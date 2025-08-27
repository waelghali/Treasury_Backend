# app/crud/crud_migration.py

from typing import Any, Dict, List, Optional, Type
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from fastapi import HTTPException, status
import app.models as models
from app.crud.base import CRUDBase, log_action
from app.schemas.migration_schemas import (
    LGMigrationStagingIn,
    LGMigrationStagingOut,
    MigrationRecordStatusEnum,
    MigrationReportSummary,
    ProcessingSummaryOut,
    MigrationTypeEnum,
)
from app.models import LGMigrationStaging, LGRecord
from datetime import date, datetime
import pandas as pd
import numpy as np
from app.crud.base import log_action
from sqlalchemy.orm.attributes import flag_modified

# NEW: Import the LG validation service
from app.core.lg_validation_service import lg_validation_service
# IMPORTANT: Import the autofill function from the migration service
from app.core.migration_service import _apply_defaults_and_autofill
from app.schemas.all_schemas import LGRecordBase
import logging
logger = logging.getLogger(__name__)


class CRUDLGMigration(CRUDBase):
    def __init__(self, model: Type[LGMigrationStaging]):
        super().__init__(model)

    def _create_enhanced_validation_log(self, record_data: Dict[str, Any], raw_errors: Dict[str, Any], final_status: str) -> Dict[str, Any]:
        """
        Creates a structured validation log in a format suitable for frontend table rendering.
        """
        log = {}
        all_lg_fields = [
            'lg_number', 'lg_amount', 'lg_currency_id', 'lg_payable_currency_id',
            'issuance_date', 'expiry_date', 'auto_renewal', 'lg_type_id',
            'lg_operational_status_id', 'payment_conditions', 'description_purpose',
            'issuer_name', 'issuer_id', 'beneficiary_corporate_id', 'issuing_bank_id',
            'issuing_bank_address', 'issuing_bank_phone', 'issuing_bank_fax',
            'issuing_method_id', 'applicable_rule_id', 'applicable_rules_text',
            'other_conditions', 'internal_owner_email', 'internal_owner_phone',
            'internal_owner_id', 'internal_owner_manager_email',
            'lg_category_id', 'additional_field_values', 'internal_contract_project_id',
            'notes',
        ]
        
        for field in all_lg_fields:
            value = record_data.get(field)
            error_message = raw_errors.get(field)
            
            if error_message:
                log[field] = {
                    "value": value if value is not None else "N/A",
                    "status": "ERROR",
                    "message": lg_validation_service._get_enhanced_error(field, error_message)
                }
            elif field == "expiry_date" and final_status == MigrationRecordStatusEnum.EXPIRED.value:
                log[field] = {
                    "value": value,
                    "status": "EXPIRED",
                    "message": "LG expiry date is in the past."
                }
            elif field == "lg_number" and final_status == MigrationRecordStatusEnum.DUPLICATE.value:
                 log[field] = {
                    "value": value,
                    "status": "DUPLICATE",
                    "message": "Duplicate entry. A newer version with the same LG number exists."
                }
            else:
                log[field] = {
                    "value": value if value is not None else "N/A",
                    "status": "Valid",
                    "message": "Field is valid."
                }
        
        return log

    def create_staging_record(self, db: Session, obj_in: LGMigrationStagingIn, customer_id: int, user_id: int, record_status: Optional[MigrationRecordStatusEnum] = None, validation_log: Optional[Dict[str, Any]] = None) -> LGMigrationStaging:
        # CRITICAL FIX: The source_data_json is already updated by the autofill process before
        # this is called. Ensure we use the final data.
        db_obj = self.model(
            file_name=obj_in.file_name,
            source_data_json=obj_in.source_data_json,
            record_status=record_status if record_status else MigrationRecordStatusEnum.PENDING,
            validation_log=validation_log,
            customer_id=customer_id,
            migration_type=obj_in.migration_type
        )
        db.add(db_obj)
        db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="MIGRATION_UPLOAD",
            entity_type="LGMigrationStaging",
            entity_id=db_obj.id,
            details={"file_name": obj_in.file_name, "status": db_obj.record_status.value, "migration_type": db_obj.migration_type.value},
            customer_id=customer_id,
        )

        db.refresh(db_obj)
        return db_obj

    def get_staging_records(
        self,
        db: Session,
        customer_id: int,
        status_filter: Optional[MigrationRecordStatusEnum] = None,
        lg_number_filter: Optional[str] = None, # NEW
        file_name_filter: Optional[str] = None, # NEW
        skip: int = 0,
        limit: int = 100,
    ) -> List[LGMigrationStaging]:
        query = db.query(self.model).filter(self.model.customer_id == customer_id)
        if status_filter:
            query = query.filter(self.model.record_status == status_filter)
        # Add new filtering logic for LG Number and file name
        if lg_number_filter:
            query = query.filter(func.lower(self.model.source_data_json['lg_number'].astext).like(func.lower(f"%{lg_number_filter}%")))
        if file_name_filter:
            query = query.filter(func.lower(self.model.file_name).like(func.lower(f"%{file_name_filter}%")))

        return query.order_by(self.model.created_at.desc()).offset(skip).limit(limit).all()

    def get_migration_report(self, db: Session, customer_id: int) -> MigrationReportSummary:
        results = db.query(
            self.model.record_status,
            func.count(self.model.id).label('count')
        ).filter(
            self.model.customer_id == customer_id
        ).group_by(self.model.record_status).all()

        report_data = {status_enum.value: 0 for status_enum in MigrationRecordStatusEnum}
        for status_val, count in results:
            report_data[status_val] = count

        return MigrationReportSummary(
            total_records=sum(report_data.values()),
            pending_count=report_data.get(MigrationRecordStatusEnum.PENDING.value, 0),
            needs_review_count=report_data.get(MigrationRecordStatusEnum.NEEDS_REVIEW.value, 0),
            ready_for_import_count=report_data.get(MigrationRecordStatusEnum.READY_FOR_IMPORT.value, 0),
            imported_count=report_data.get(MigrationRecordStatusEnum.IMPORTED.value, 0),
            rejected_count=report_data.get(MigrationRecordStatusEnum.REJECTED.value, 0),
            error_count=report_data.get(MigrationRecordStatusEnum.ERROR.value, 0),
            duplicates=report_data.get(MigrationRecordStatusEnum.DUPLICATE.value, 0),
            expired_count=report_data.get(MigrationRecordStatusEnum.EXPIRED.value, 0) # Corrected to expired_count
        )

    def update_and_revalidate_staging_record(self, db: Session, record_id: int, customer_id: int, complete_record_data: Dict[str, Any], user_id: int) -> LGMigrationStaging:
        db_record = self.get(db, record_id)
        if not db_record or db_record.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration record not found or not accessible.")
        
        # Store the old data for logging purposes
        old_data = db_record.source_data_json or {}
        
        # Replace the entire source_data_json with the new complete data
        db_record.source_data_json = complete_record_data
        
        # Run comprehensive validation on the new data
        validation_errors = lg_validation_service.validate_lg_data(db_record.source_data_json, context='migration', db=db, customer_id=customer_id)
        
        # Check for duplicates and expiry as post-validation business rules
        lg_number = db_record.source_data_json.get("lg_number")
        is_duplicate = False
        if not validation_errors and lg_number:
            # CHECK 1: Check against production LGRecord table
            existing_lg_in_prod = db.query(LGRecord).filter(
                func.lower(LGRecord.lg_number) == func.lower(lg_number),
                LGRecord.customer_id == customer_id
            ).first()
            if existing_lg_in_prod:
                is_duplicate = True
                validation_errors['lg_number'] = lg_validation_service._get_enhanced_error("lg_number", "LG number already exists in production.")
            
            # CHECK 2: Check for duplicates across ALL staged records with same LG number, excluding itself
            if not is_duplicate:
                latest_record = db.query(self.model).filter(
                    self.model.customer_id == customer_id,
                    func.lower(self.model.source_data_json['lg_number'].astext) == func.lower(lg_number),
                    self.model.id != db_record.id,
                    self.model.record_status.in_([
                        MigrationRecordStatusEnum.READY_FOR_IMPORT,
                        MigrationRecordStatusEnum.PENDING,
                    ])
                ).order_by(self.model.created_at.desc()).first()
                if latest_record:
                    is_duplicate = True
                    validation_errors['lg_number'] = lg_validation_service._get_enhanced_error("lg_number", "Duplicate entry. A newer version with the same LG number exists in staging.")
        
        is_expired = False
        expiry_date_str = db_record.source_data_json.get("expiry_date")
        try:
            if expiry_date_str and datetime.strptime(expiry_date_str, "%Y-%m-%d").date() < date.today():
                is_expired = True
                validation_errors['expiry_date'] = lg_validation_service._get_enhanced_error("expiry_date", "LG expiry date is in the past.")
        except (ValueError, TypeError):
            # Already handled by core validation, but ensures status is set correctly
            pass
            
        # Set final status based on validation and business rules
        if is_expired:
            db_record.record_status = MigrationRecordStatusEnum.EXPIRED
        elif is_duplicate:
            db_record.record_status = MigrationRecordStatusEnum.DUPLICATE
        elif validation_errors:
            db_record.record_status = MigrationRecordStatusEnum.ERROR
        else:
            db_record.record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT
            
        db_record.validation_log = validation_errors
        db.add(db_record)
        db.flush()
            
        log_action(
            db=db,
            user_id=user_id,
            action_type="MIGRATION_UPDATE",
            entity_type="LGMigrationStaging",
            entity_id=db_record.id,
            details={"old_data": old_data, "new_data": complete_record_data, "new_status": db_record.record_status.value},
            customer_id=customer_id,
        )
        db.refresh(db_record)
        return db_record

    def re_validate_record(self, db: Session, record_id: int, customer_id: int, user_id: int) -> LGMigrationStaging:
        """
        Re-validates a specific record and updates its status.
        """
        db_record = self.get(db, record_id)
        if not db_record or db_record.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration record not found or not accessible.")
        
        # Get the latest data from the record
        record_data = db_record.source_data_json

        # Re-apply defaults and autofill to ensure all lookup IDs are correct
        # CRITICAL FIX: The _apply_defaults_and_autofill function returns a NEW dict.
        # We must explicitly assign it back to the database object before validating.
        from app.crud.crud import crud_internal_owner_contact, crud_lg_type, crud_issuing_method, crud_rule, crud_bank, crud_customer_entity, crud_lg_category, crud_currency
        
        enhanced_record_data = _apply_defaults_and_autofill(
            db, 
            record_data, 
            customer_id,
            crud_internal_owner_contact,
            crud_lg_type,
            crud_issuing_method,
            crud_rule,
            crud_bank,
            crud_customer_entity,
            crud_lg_category,
            crud_currency
        )
        
        db_record.source_data_json = enhanced_record_data
        
        # Add this line to explicitly mark the JSON field as modified
        flag_modified(db_record, "source_data_json")

        logger.info(f"Re-validation: Autofilled data for record {record_id} has been applied.")
        
        # Re-run the core validation logic, now considering the record type
        if db_record.migration_type == MigrationTypeEnum.RECORD:
            validation_errors = lg_validation_service.validate_lg_data(db_record.source_data_json, context='migration', db=db, customer_id=customer_id)
        else: # Is an instruction
            validation_errors = lg_validation_service.validate_lg_instruction_data(db_record.source_data_json, context='migration', db=db, customer_id=customer_id)
        
        # Update status and validation log based on the result
        db_record.validation_log = validation_errors
        db_record.record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT if not validation_errors else MigrationRecordStatusEnum.ERROR
        db.add(db_record)
        db.flush()
        db.refresh(db_record)
        return db_record

    def re_validate_multiple_records(self, db: Session, customer_id: int, record_ids: List[int], user_id: int) -> List[LGMigrationStaging]:
        """
        Re-validates multiple staged records and updates their statuses.
        Uses a nested transaction to ensure each record is handled independently.
        """
        updated_records = []
        for record_id in record_ids:
            # Use a nested transaction to handle each record's validation atomically
            with db.begin_nested():
                try:
                    updated_record = self.re_validate_record(db, record_id, customer_id, user_id)
                    updated_records.append(updated_record)
                except HTTPException as e:
                    # Log the error but continue to the next record
                    db.rollback()
                    log_action(db, user_id=user_id, action_type="MIGRATION_REVALIDATION_FAILED", entity_type="LGMigrationStaging", entity_id=record_id, details={"reason": str(e.detail)}, customer_id=customer_id)
                except Exception as e:
                    db.rollback()
                    log_action(db, user_id=user_id, action_type="MIGRATION_REVALIDATION_FAILED", entity_type="LGMigrationStaging", entity_id=record_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=customer_id)
        
        # The main transaction (managed by the endpoint) will commit all nested transactions at the end.
        return updated_records

    def process_and_validate_staged_records(self, db: Session, customer_id: int) -> ProcessingSummaryOut:
        """
        Processes all PENDING staged records for a customer, applying validation rules
        and updating their status accordingly.
        """
        records = db.query(self.model).filter(
            self.model.customer_id == customer_id,
            self.model.record_status.in_([MigrationRecordStatusEnum.PENDING, MigrationRecordStatusEnum.ERROR]),
        ).all()
        
        if not records:
            return ProcessingSummaryOut(
                processed_count=0,
                ready_for_import=[],
                duplicates=[],
                expired=[],
                errors=[],
            )

        processed_ids = {
            "ready_for_import": [],
            "duplicates": [],
            "expired": [],
            "errors": [],
        }
        
        lg_numbers_map = {}
        today = date.today()

        # Pre-flight check for duplicates against live production LGs
        existing_lg_numbers = {
            lg.lg_number.lower()
            for lg in db.query(models.LGRecord.lg_number).all()
        }
        
        # Pre-populate map with errors for records that clash with production
        for record in records:
            lg_number = record.source_data_json.get("lg_number")
            if lg_number and lg_number.lower() in existing_lg_numbers and record.migration_type == MigrationTypeEnum.RECORD:
                record.record_status = MigrationRecordStatusEnum.DUPLICATE
                record.validation_log = {"lg_number": lg_validation_service._get_enhanced_error("lg_number", "LG already exists in production table. Skipping.")}
                processed_ids["duplicates"].append(record.id)
                db.add(record)
                
        # Filter out records already handled as duplicates against production
        records = [rec for rec in records if rec.id not in processed_ids["duplicates"]]

        # Re-run a fresh query or loop through the filtered list
        # Let's loop through the filtered list for simplicity and efficiency

        for record in records:
            # Step 1: Run the comprehensive validation
            if record.migration_type == MigrationTypeEnum.RECORD:
                validation_errors = lg_validation_service.validate_lg_data(record.source_data_json, context='migration')
            else:
                validation_errors = lg_validation_service.validate_lg_instruction_data(record.source_data_json, context='migration')
            
            # --- NEW FIX: Calculate lg_period_months and handle potential errors here ---
            if record.migration_type == MigrationTypeEnum.RECORD:
                try:
                    issuance_date_str = record.source_data_json.get('issuance_date')
                    expiry_date_str = record.source_data_json.get('expiry_date')
                    
                    if issuance_date_str and expiry_date_str:
                        issuance_date = datetime.strptime(str(issuance_date_str), "%Y-%m-%d").date()
                        expiry_date = datetime.strptime(str(expiry_date_str), "%Y-%m-%d").date()
                        
                        # Call the centralized function for a consistent calculation
                        from app.api.v1.endpoints.migration import calculate_lg_period_months
                        delta_months = calculate_lg_period_months(issuance_date, expiry_date)
                        
                        # Update the source data with the calculated value
                        record.source_data_json['lg_period_months'] = delta_months
                
                except (ValueError, TypeError) as e:
                    # If date parsing fails, log it and add an error to the validation log
                    # This ensures the record status becomes 'ERROR'
                    validation_errors['lg_period_months'] = f"Failed to calculate LG period months due to invalid dates: {e}"
            
            record.validation_log = validation_errors

            # Step 2: Check for expiry and format errors first
            expiry_date_str = record.source_data_json.get("expiry_date")
            is_expired = False
            try:
                if expiry_date_str:
                    expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
                    if expiry_date < today:
                        is_expired = True
            except (ValueError, TypeError):
                pass
            
            # Set status based on validation results
            if is_expired:
                record.record_status = MigrationRecordStatusEnum.EXPIRED
                record.validation_log = {"expiry_date": lg_validation_service._get_enhanced_error("expiry_date", "LG expiry date is in the past.")}
                db.add(record)
                processed_ids["expired"].append(record.id)
            elif validation_errors:
                record.record_status = MigrationRecordStatusEnum.ERROR
                db.add(record)
                processed_ids["errors"].append(record.id)
            else:
                # Step 3: Handle duplicates for valid records only
                lg_number = record.source_data_json.get("lg_number")
                if lg_number:
                    if lg_number not in lg_numbers_map:
                        lg_numbers_map[lg_number] = []
                    lg_numbers_map[lg_number].append(record)

        # Process duplicates for groups with more than one record
        for lg_number, related_records in lg_numbers_map.items():
            if len(related_records) > 1:
                latest_record = max(related_records, key=lambda rec: datetime.strptime(rec.source_data_json.get("expiry_date", date.min.isoformat()), "%Y-%m-%d").date())
                
                for record in related_records:
                    if record.id == latest_record.id:
                        record.record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT
                        processed_ids["ready_for_import"].append(record.id)
                    else:
                        record.record_status = MigrationRecordStatusEnum.DUPLICATE
                        record.validation_log = {"lg_number": "Duplicate entry. A newer version with the same LG number exists."}
                        processed_ids["duplicates"].append(record.id)
                    db.add(record)
            else:
                record = related_records[0]
                record.record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT
                processed_ids["ready_for_import"].append(record.id)
                db.add(record)
        
        db.flush()
        db.commit()
        
        return ProcessingSummaryOut(
            processed_count=len(records),
            ready_for_import=processed_ids["ready_for_import"],
            duplicates=processed_ids["duplicates"],
            expired=processed_ids["expired"],
            errors=processed_ids["errors"],
        )

    def delete_staging_record(self, db: Session, record_id: int, customer_id: int, user_id: int):
        """
        Deletes a specific staged record for the current customer.
        """
        db_record = db.query(self.model).filter(
            self.model.id == record_id,
            self.model.customer_id == customer_id
        ).first()

        if not db_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Migration record not found or not accessible.")

        db.delete(db_record)
        db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="MIGRATION_DELETE",
            entity_type="LGMigrationStaging",
            entity_id=record_id,
            details={"reason": "Record permanently deleted from staging."},
            customer_id=customer_id,
        )

    def delete_multiple_records(
        self, db: Session, customer_id: int, record_ids: List[int], user_id: int
    ) -> int:
        """
        Deletes multiple staged migration records by ID for a specific customer.
        It includes a failsafe to prevent deletion of records already imported.
        Returns the number of records deleted.
        """
        if not record_ids:
            return 0

        # Failsafe: only delete if the record is NOT referenced by a production LG.
        # This is a more robust check than relying solely on the record_status.
        # NOTE: This check ensures that even if the status update fails,
        # we don't cause a database integrity error.
        
        records_to_delete = db.query(self.model).filter(
            self.model.id.in_(record_ids),
            self.model.customer_id == customer_id
        ).all()
        
        # Filter out records that are already imported and referenced
        filtered_ids = [
            r.id for r in records_to_delete if not r.production_lg_id
        ]
        
        if not filtered_ids:
            return 0
            
        deleted_count = (
            db.query(self.model)
            .filter(
                self.model.id.in_(filtered_ids),
                self.model.customer_id == customer_id
            )
            .delete(synchronize_session=False)
        )
        
        log_action(
            db,
            user_id=user_id,
            action_type="MIGRATION_RECORDS_DELETED",
            entity_type="LGMigrationStaging",
            entity_id=None,
            details={"deleted_count": deleted_count, "deleted_ids": filtered_ids},
            customer_id=customer_id,
        )
        return deleted_count