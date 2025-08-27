# app/core/migration_history.py

import json
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Type
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from app.models import (
    LGMigrationStaging, LGRecord, LgStatus, LgStatusEnum, LgType, MigrationBatch,
    MigrationRecordStatusEnum, LGChangeLog
)
from app.schemas.all_schemas import LGRecordCreate
from app.schemas.migration_history_schemas import (
    MigrationHistoryPreviewOut,
    StagingSnapshotOut,
    MigrationBatchOut,
)
from app.constants import AUDIT_ACTION_TYPE_LG_AMENDED, AUDIT_ACTION_TYPE_CREATE
import logging

logger = logging.getLogger(__name__)

class MigrationHistoryService:
    def __init__(self):
        pass

    def _get_snapshot_sort_key(self, snapshot: LGMigrationStaging) -> Tuple:
        """Determines the sorting key for a snapshot based on defined rules."""
        sort_key_components = []
        source_data = snapshot.source_data_json
        
        # 1. history_sequence (if present)
        sort_key_components.append(snapshot.history_sequence if snapshot.history_sequence is not None else float('inf'))
        
        # 2. history_timestamp (if present)
        sort_key_components.append(snapshot.history_timestamp if snapshot.history_timestamp is not None else datetime.min)
        
        # 3. issuance_date (if present in source_data)
        issuance_date = source_data.get('issuance_date')
        if isinstance(issuance_date, str):
            try:
                sort_key_components.append(datetime.strptime(issuance_date, "%Y-%m-%d"))
            except (ValueError, TypeError):
                sort_key_components.append(datetime.min)
        else:
            sort_key_components.append(datetime.min)

        # 4. created_at (as a last resort)
        sort_key_components.append(snapshot.created_at)

        return tuple(sort_key_components)

    def _get_diff(self, old_snapshot: Dict[str, Any], new_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """Computes a field-level diff between two snapshots."""
        diff = {}
        all_keys = set(old_snapshot.keys()) | set(new_snapshot.keys())
        for key in sorted(list(all_keys)):
            old_val = old_snapshot.get(key)
            new_val = new_snapshot.get(key)
            if old_val != new_val:
                diff[key] = {'old': old_val, 'new': new_val}
        return diff

    async def preview_history(self, db: Session, customer_id: int, lg_number: Optional[str] = None, only_ready: bool = True) -> List[MigrationHistoryPreviewOut]:
        """
        Generates a preview of the historical import for a given LG number or all staged LGs.
        """
        # LATE IMPORT to break circular dependency
        from app.crud.crud import crud_lg_migration
        
        query = db.query(LGMigrationStaging).filter(
            LGMigrationStaging.customer_id == customer_id
        )

        if lg_number:
            query = query.filter(func.lower(LGMigrationStaging.source_data_json['lg_number'].astext) == func.lower(lg_number))
        
        if only_ready:
            query = query.filter(LGMigrationStaging.record_status == MigrationRecordStatusEnum.READY_FOR_IMPORT)

        all_snapshots = query.all()
        
        if not all_snapshots:
            return []
        
        lg_groups = {}
        for snapshot in all_snapshots:
            lg_num = snapshot.source_data_json.get('lg_number')
            if lg_num not in lg_groups:
                lg_groups[lg_num] = []
            lg_groups[lg_num].append(snapshot)
        
        results = []
        for lg_num, snapshots in lg_groups.items():
            # Sort snapshots according to the defined timeline ordering logic
            sorted_snapshots = sorted(snapshots, key=self._get_snapshot_sort_key)
            
            preview_snapshots = []
            conflict_flag = False
            
            for i, snapshot in enumerate(sorted_snapshots):
                snapshot_data = snapshot.source_data_json
                
                # Check for conflicts
                conflicts = {}
                if i > 0:
                    prev_snapshot = sorted_snapshots[i-1]
                    prev_snapshot_data = prev_snapshot.source_data_json
                    if self._get_snapshot_sort_key(snapshot) == self._get_snapshot_sort_key(prev_snapshot):
                        for key, new_val in snapshot_data.items():
                            old_val = prev_snapshot_data.get(key)
                            if old_val != new_val:
                                conflicts[key] = {'old': old_val, 'new': new_val}

                if conflicts:
                    conflict_flag = True
                
                diff = self._get_diff(sorted_snapshots[i-1].source_data_json, snapshot_data) if i > 0 else {}
                
                preview_snapshots.append(StagingSnapshotOut(
                    id=snapshot.id,
                    lg_number=lg_num,
                    issuance_date=snapshot_data.get('issuance_date', ''),
                    expiry_date=snapshot_data.get('expiry_date', ''),
                    diff=diff,
                    conflicts=conflicts,
                    migration_timestamp=snapshot.created_at
                ))
            
            results.append(MigrationHistoryPreviewOut(
                lg_number=lg_num,
                snapshots=preview_snapshots,
                conflict_flag=conflict_flag
            ))
            
        return results

    async def import_history(self, db: Session, customer_id: int, user_id: int, lg_numbers: Optional[List[str]] = None, batch_note: Optional[str] = None):
        """
        Imports one or more historical LG records into production tables.
        This is the core historical reconstruction function.
        """
        # LATE IMPORTS to break circular dependency
        from app.crud.crud import crud_lg_migration, crud_lg_record, crud_migration_batch, crud_lg_change_log, log_action
        from app.api.v1.endpoints.migration import _apply_defaults_and_autofill
        from app.core.lg_validation_service import lg_validation_service
        
        logger.info(f"Historical import process started for customer {customer_id}.")
        
        query = db.query(LGMigrationStaging).filter(LGMigrationStaging.customer_id == customer_id)
        if lg_numbers:
            query = query.filter(func.lower(LGMigrationStaging.source_data_json['lg_number'].astext).in_([ln.lower() for ln in lg_numbers]))
        
        query = query.filter(LGMigrationStaging.record_status.in_([MigrationRecordStatusEnum.READY_FOR_IMPORT, MigrationRecordStatusEnum.PENDING, MigrationRecordStatusEnum.ERROR]))
        
        all_snapshots = query.all()
        if not all_snapshots:
            return {"message": "No eligible records found for historical import.", "imported_count": 0, "failed_count": 0}

        lg_groups = {}
        for snapshot in all_snapshots:
            lg_num = snapshot.source_data_json.get('lg_number')
            if lg_num not in lg_groups:
                lg_groups[lg_num] = []
            lg_groups[lg_num].append(snapshot)
            
        batch = MigrationBatch(
            user_id=user_id,
            notes=batch_note,
            source_files=[snap.file_name for snap in all_snapshots]
        )
        db.add(batch)
        db.flush()
        
        batch_results = {'imported': 0, 'updated': 0, 'failed': 0, 'skipped_exists': 0, 'ambiguous_history': 0}
        
        for lg_num, snapshots in lg_groups.items():
            # A single transaction for each LG group to ensure all snapshots are applied or none are
            with db.begin_nested() as nested_session:
                try:
                    sorted_snapshots = sorted(snapshots, key=self._get_snapshot_sort_key)
                    first_snapshot = sorted_snapshots[0]
                    
                    # Proactive check against production table
                    existing_lg_in_prod = crud_lg_record.get_by_lg_number(db, lg_num)
                    if existing_lg_in_prod:
                        logger.warning(f"LG number '{lg_num}' already exists in production. Skipping import.")
                        for snap in snapshots:
                            snap.record_status = MigrationRecordStatusEnum.ERROR
                            snap.validation_log['import_error'] = 'LG already exists in production table. Skipping.'
                        batch_results['skipped_exists'] += 1
                        continue # Skip to the next LG group
                        
                    # Check for conflicts
                    conflict_found = False
                    for i in range(1, len(sorted_snapshots)):
                        if self._get_snapshot_sort_key(sorted_snapshots[i]) == self._get_snapshot_sort_key(sorted_snapshots[i-1]):
                            conflict_diff = self._get_diff(sorted_snapshots[i-1].source_data_json, sorted_snapshots[i].source_data_json)
                            if conflict_diff:
                                conflict_found = True
                                break

                    if conflict_found:
                        logger.warning(f"Ambiguous history detected for LG '{lg_num}'. Flagging for review.")
                        for snap in snapshots:
                            snap.record_status = MigrationRecordStatusEnum.ERROR
                            snap.validation_log['import_error'] = 'Ambiguous history detected. Two snapshots share the same timeline key but have different values.'
                        batch_results['ambiguous_history'] += 1
                        continue # Skip to the next LG group

                    # Step 1: Create base LGRecord from the first snapshot
                    lg_record_data = first_snapshot.source_data_json
                    
                    # Apply final validation and autofill again just in case
                    enhanced_data = _apply_defaults_and_autofill(db, lg_record_data, customer_id)
                    final_validation_errors = lg_validation_service.validate_lg_data(enhanced_data, context='migration', db=db, customer_id=customer_id)
                    if final_validation_errors:
                        raise ValueError(f"Final validation failed on base snapshot {first_snapshot.id}: {final_validation_errors}")

                    lg_record_create_payload = LGRecordCreate(**enhanced_data)

                    # Determine final status from the last snapshot
                    final_snapshot_data = sorted_snapshots[-1].source_data_json
                    final_status_id = LgStatusEnum.VALID.value
                    if datetime.strptime(final_snapshot_data.get('expiry_date'), "%Y-%m-%d").date() < date.today():
                        final_status_id = LgStatusEnum.EXPIRED.value
                    
                    new_lg = await crud_lg_record.create_from_migration(
                        db=db,
                        obj_in=lg_record_create_payload,
                        customer_id=customer_id,
                        user_id=user_id,
                        migration_source='LEGACY',
                        migrated_from_staging_id=first_snapshot.id,
                        final_lg_status_id=final_status_id
                    )

                    # Step 2: Apply subsequent snapshots as amendments
                    if len(sorted_snapshots) > 1:
                        for i in range(1, len(sorted_snapshots)):
                            prev_snapshot_data = sorted_snapshots[i-1].source_data_json
                            current_snapshot = sorted_snapshots[i]
                            current_snapshot_data = current_snapshot.source_data_json
                            
                            diff = self._get_diff(prev_snapshot_data, current_snapshot_data)
                            if diff:
                                # This requires a new method on crud_lg_record to apply amendments
                                updated_lg = await crud_lg_record.apply_migration_amendment(
                                    db, new_lg.id, diff, user_id
                                )
                                log_action(
                                    db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_LG_AMENDED,
                                    entity_type="LGRecord", entity_id=new_lg.id,
                                    details={"diff": diff, "snapshot_id": current_snapshot.id, "batch_id": batch.id},
                                    customer_id=customer_id, lg_record_id=new_lg.id
                                )

                    # All snapshots for this LG are processed, update staging status
                    for snap in snapshots:
                        snap.record_status = MigrationRecordStatusEnum.IMPORTED
                        snap.production_lg_id = new_lg.id
                    batch_results['imported'] += 1
                
                except Exception as e:
                    nested_session.rollback()
                    logger.error(f"Failed to import LG '{lg_num}' during historical import: {e}", exc_info=True)
                    for snap in snapshots:
                        snap.record_status = MigrationRecordStatusEnum.ERROR
                        snap.validation_log['import_error'] = str(e)
                    batch_results['failed'] += 1

        batch.totals = batch_results
        batch.finished_at = func.now()
        db.add(batch)
        db.commit()
        
        return {
            "message": "Historical migration process completed.",
            "totals": batch_results,
            "batch_id": batch.id
        }