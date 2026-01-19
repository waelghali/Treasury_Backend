# c:\Grow\app\core\migration_service.py

import os
import sys
import io
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Type
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from pydantic import ValidationError
import logging
import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, or_, cast
from sqlalchemy.dialects.postgresql import JSONB

from app.schemas.all_schemas import LGRecordCreate, LGDocumentCreate, LGInstructionCreate, LGRecordAmendRequest
from app.schemas.migration_schemas import MigrationRecordStatusEnum, MigrationTypeEnum

from app.models import LGRecord, LGInstruction, Bank, IssuingMethod, Rule, CustomerEntity, LgType, LgOperationalStatus, LGCategory, Currency, InternalOwnerContact

from app.constants import (
    DOCUMENT_TYPE_ORIGINAL_LG,
    DOCUMENT_TYPE_INTERNAL_SUPPORTING,
    ACTION_TYPE_LG_AMEND,
    InstructionTypeCode,
    SubInstructionCode,
)

logger = logging.getLogger(__name__)

# =====================================================================================
# MOVED FROM endpoints/migration.py TO BREAK CIRCULAR IMPORT
# =====================================================================================
def _get_id_by_name(db: Session, model: Any, name: str) -> Optional[int]:
    """Finds an ID by a unique name, short name, or former name in a given table."""
    if model == Bank:
        obj = db.query(model).filter(
            or_(
                func.lower(model.name) == func.lower(name),
                func.lower(model.short_name) == func.lower(name),
                cast(model.former_names, JSONB).op('?')(name)
            )
        ).first()
        return obj.id if obj else None
        
    obj = db.query(model).filter(func.lower(model.name) == func.lower(name)).first()
    return obj.id if obj else None
    
def calculate_lg_period_months(issuance_date: date, expiry_date: date) -> Optional[int]:
    """
    Calculates the LG period in months based on business rules.
    """
    if not issuance_date or not expiry_date:
        return None

    delta = relativedelta(expiry_date, issuance_date)
    total_months = delta.years * 12 + delta.months
    if delta.days > 0:
        total_months += 1

    rounded_months = int(round(total_months / 3)) * 3

    clamped_months = max(3, min(12, rounded_months))

    return clamped_months

def _apply_defaults_and_autofill(
    db: Session, 
    record_data: Dict[str, Any], 
    customer_id: int,
    # Inject dependencies here
    crud_internal_owner_contact: Any,
    crud_lg_type: Any,
    crud_issuing_method: Any,
    crud_rule: Any,
    crud_bank: Any,
    crud_customer_entity: Any,
    crud_lg_category: Any,
    crud_currency: Any,
) -> Dict[str, Any]:
    """
    Applies defaults and attempts to autofill data for a single record.
    Returns the updated record_data dictionary.
    """
    logger.debug(f"Autofill started for record: {record_data.get('lg_number', 'N/A')}")
    
    # --- Step 1: Autofill Internal Owner Contact (ROBUST FIX) ---
    existing_owner_id = record_data.get("internal_owner_contact_id")
    email_input = record_data.get("internal_owner_email")
    
    # If ID is a string (e.g. from Excel), or missing, attempt lookup by email
    if not isinstance(existing_owner_id, int) and email_input:
        clean_email = str(email_input).strip()
        owner = crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, clean_email)
        if owner:
            record_data["internal_owner_contact_id"] = owner.id
            record_data["internal_owner_email"] = owner.email 
            logger.debug(f"Autofilled internal_owner_contact_id: {owner.id}")

    # --- Step 2: Resolve Operational Status (NEW FIX) ---
    op_status_val = record_data.get("lg_operational_status_id")
    if isinstance(op_status_val, str):
        # Map common string names to your specific database IDs
        # Update these integer IDs (1, 2, 3...) to match your LGOperationalStatus table
        status_map = {
            "OPERATIVE": 1,
            "CLOSED": 2,
            "EXPIRED": 3,
            "CANCELLED": 4,
            "PENDING": 5
        }
        clean_status = op_status_val.strip().upper()
        if clean_status in status_map:
            record_data["lg_operational_status_id"] = status_map[clean_status]
            logger.debug(f"Resolved operational status '{op_status_val}' to ID: {status_map[clean_status]}")
        else:
            # Default to Operative (ID 1) if unknown string provided
            record_data["lg_operational_status_id"] = 1

    # --- Step 3: Generic Lookups (LG Type, Issuing Method, Rules) ---
    lookup_map = {
        "lg_type_id": (crud_lg_type, "lg_type"),
        "issuing_method_id": (crud_issuing_method, "issuing_method"),
        "applicable_rule_id": (crud_rule, "applicable_rule"),
    }
    
    for id_field, (crud_obj, name_field) in lookup_map.items():
        val = record_data.get(id_field)
        # If ID is missing or is a string name, look it up
        if not isinstance(val, int):
            lookup_val = val if val else record_data.get(name_field)
            if lookup_val and isinstance(lookup_val, str):
                obj = crud_obj.get_by_name(db, lookup_val.strip())
                if obj:
                    record_data[id_field] = obj.id

    # --- Step 4: Handle Issuing Bank (Fuzzy/Cleaned lookup) ---
    issuing_bank_val = record_data.get("issuing_bank_id")
    if isinstance(issuing_bank_val, str):
        # This uses the helper you likely have in migration_service or core
        from app.models import Bank
        from sqlalchemy import func, or_, cast
        from sqlalchemy.dialects.postgresql import JSONB

        bank_name_clean = issuing_bank_val.strip()
        bank = db.query(Bank).filter(
            or_(
                func.lower(Bank.name) == func.lower(bank_name_clean),
                func.lower(Bank.short_name) == func.lower(bank_name_clean),
                cast(Bank.former_names, JSONB).op('?')(bank_name_clean)
            )
        ).first()
        
        if bank:
            record_data["issuing_bank_id"] = bank.id
            # Fill secondary bank details
            record_data["issuing_bank_address"] = record_data.get("issuing_bank_address") or bank.address
            record_data["issuing_bank_phone"] = record_data.get("issuing_bank_phone") or bank.phone_number
            record_data["issuing_bank_fax"] = record_data.get("issuing_bank_fax") or bank.fax
        else:
            record_data["issuing_bank_id"] = None

    # --- Step 5: Handle Beneficiary Corporate (Customer Specific) ---
    beneficiary_val = record_data.get("beneficiary_corporate_id")
    if isinstance(beneficiary_val, str):
        entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, beneficiary_val.strip())
        if entity:
            record_data["beneficiary_corporate_id"] = entity.id
        else:
            # Fallback: if name lookup fails, check if customer has exactly one entity
            from app.models import CustomerEntity
            entities = db.query(CustomerEntity).filter(
                CustomerEntity.customer_id == customer_id, 
                CustomerEntity.is_deleted == False
            ).all()
            if len(entities) == 1:
                record_data["beneficiary_corporate_id"] = entities[0].id

    # --- Step 6: LG Category (Customer Specific vs Universal) ---
    cat_val = record_data.get("lg_category_id")
    if isinstance(cat_val, str):
        clean_cat = cat_val.strip()
        # Try Customer Category -> Try Universal Category
        cat_obj = crud_lg_category.get_by_name(db, clean_cat, customer_id) or \
                  crud_lg_category.get_by_name(db, clean_cat, None)
        
        if cat_obj:
            record_data["lg_category_id"] = cat_obj.id
        else:
            # Force Default Category (usually ID 1)
            default_cat = crud_lg_category.get_default_category(db, None)
            record_data["lg_category_id"] = default_cat.id if default_cat else None

    # --- Step 7: Currencies ---
    for cur_field in ["lg_currency_id", "lg_payable_currency_id"]:
        cur_val = record_data.get(cur_field)
        if isinstance(cur_val, str):
            currency = crud_currency.get_by_iso_code(db, cur_val.strip().upper())
            if currency:
                record_data[cur_field] = currency.id

    # --- Step 8: Dynamic Calculations (Dates and Periods) ---
    issuance_date_str = record_data.get("issuance_date")
    expiry_date_str = record_data.get("expiry_date")
    if issuance_date_str and expiry_date_str:
        try:
            from datetime import datetime
            # Ensure we are working with date objects for the calculation
            i_date = datetime.strptime(str(issuance_date_str).split(' ')[0], "%Y-%m-%d").date()
            e_date = datetime.strptime(str(expiry_date_str).split(' ')[0], "%Y-%m-%d").date()
            
            # Assuming calculate_lg_period_months is imported or available in scope
            from app.api.v1.endpoints.migration import calculate_lg_period_months
            record_data['lg_period_months'] = calculate_lg_period_months(i_date, e_date)
        except Exception as e:
            logger.warning(f"Period calculation failed: {e}")

    # Set hard defaults if still missing
    record_data["auto_renewal"] = record_data.get("auto_renewal", True)
    
    # Ensure payable currency matches if not specified
    if not record_data.get("lg_payable_currency_id"):
        record_data["lg_payable_currency_id"] = record_data.get("lg_currency_id")

    return record_data
    
class MigrationService:
    def __init__(self):
        pass

    async def _fetch_file_content(self, url: str) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Fetches file content from Local Disk. 
        If not found, falls back to Google Cloud Storage (GCS).
        """
        import mimetypes
        import os
        
        # 1. Clean the input path
        clean_url = str(url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
        file_name = os.path.basename(clean_url)

        # --- STRATEGY 1: Check Local Disk (Fastest) ---
        if os.path.exists(clean_url):
            final_path = clean_url
        elif os.path.exists(file_name):
            final_path = file_name
        else:
            final_path = None

        if final_path:
            try:
                mime_type, _ = mimetypes.guess_type(final_path)
                with open(final_path, "rb") as f:
                    return f.read(), (mime_type or "application/pdf")
            except Exception as e:
                logger.warning(f"Local read failed: {e}")

        # --- STRATEGY 2: Check Google Cloud Storage (GCS) ---
        # This runs if local file is missing
        try:
            from google.cloud import storage
            bucket_name = os.getenv("GCS_BUCKET_NAME") # Make sure this is in your .env
            
            if bucket_name:
                # We use a synchronous client inside a thread to avoid blocking async loop
                def fetch_from_gcs():
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    
                    # Try 1: Exact Filename at Root
                    blob = bucket.blob(file_name)
                    if blob.exists():
                        print(f"[DEBUG] Found '{file_name}' in GCS Root.")
                        return blob.download_as_bytes()
                    
                    # Try 2: Inside a 'migration' folder (Common practice)
                    blob_folder = bucket.blob(f"migration/{file_name}")
                    if blob_folder.exists():
                        print(f"[DEBUG] Found '{file_name}' in GCS 'migration/' folder.")
                        return blob_folder.download_as_bytes()
                        
                    return None

                # Run GCS fetch in a separate thread
                import asyncio
                content = await asyncio.to_thread(fetch_from_gcs)
                
                if content:
                    mime_type, _ = mimetypes.guess_type(file_name)
                    return content, (mime_type or "application/pdf")
                    
        except ImportError:
            logger.error("google-cloud-storage library not installed.")
        except Exception as e:
            logger.error(f"GCS Fetch Error: {e}")

        logger.warning(f"File not found locally or in GCS: {file_name}")
        return None, None
        
    async def _create_document_from_url(
        self,
        db: Session,
        lg_record_id: int,
        url: str,
        document_type: str,
        uploaded_by_user_id: int,
        original_instruction_serial: Optional[str] = None
    ) -> Optional[int]:
        """
        Fetches file content from a URL and calls the existing crud_lg_document.create_document
        to store it and create a database entry.
        """
        try:
            from app.crud.crud import crud_lg_document
            file_content, mime_type = await self._fetch_file_content(url)
            if not file_content:
                logger.error(f"Failed to fetch file content from URL: {url}")
                return None

            file_name = os.path.basename(url)

            doc_metadata = LGDocumentCreate(
                document_type=document_type,
                file_name=file_name,
                mime_type=mime_type,
                file_path=url,
                lg_instruction_id=None
            )

            db_document = await crud_lg_document.create_document(
                db=db,
                obj_in=doc_metadata,
                file_content=file_content,
                lg_record_id=lg_record_id,
                uploaded_by_user_id=uploaded_by_user_id,
                original_instruction_serial=original_instruction_serial
            )
            return db_document.id
        except Exception as e:
            logger.error(f"Error creating document from URL '{url}': {e}", exc_info=True)
            return None

    async def _apply_migration_amendment(self, db: Session, lg_record_id: int, updates: Dict[str, Any], user_id: int, staged_record_id: int):
        """
        Applies a single amendment to an existing LG record and logs the change.
        """
        try:
            from app.crud.crud import crud_lg_record, crud_lg_change_log
            from app.models import LGRecord, LGChangeLog
            from sqlalchemy import func
            from app.constants import DOCUMENT_TYPE_INTERNAL_SUPPORTING

            # --- Flatten the diff dictionary ---
            updates_to_apply = {}
            for key, val in updates.items():
                if isinstance(val, dict) and 'new' in val:
                    updates_to_apply[key] = val['new']
                else:
                    updates_to_apply[key] = val
            
            # --- Intercept Attachment URL ---
            if "attachment_url" in updates_to_apply:
                new_url = updates_to_apply["attachment_url"]
                
                if new_url:
                    clean_url = str(new_url).strip().strip("'").strip('"')
                    await self._create_document_from_url(
                        db=db,
                        lg_record_id=lg_record_id,
                        url=clean_url,
                        document_type=DOCUMENT_TYPE_INTERNAL_SUPPORTING,
                        uploaded_by_user_id=user_id,
                        original_instruction_serial=f"MIG-HIST-{staged_record_id}"
                    )
                    logger.info(f"Created amendment document from {clean_url}")

                del updates_to_apply["attachment_url"]

            lg_obj = db.query(LGRecord).filter(LGRecord.id == lg_record_id).first()
            if not lg_obj:
                raise ValueError(f"LG Record {lg_record_id} not found.")

            updated_lg_record = lg_obj
            if updates_to_apply:
                updated_lg_record = await crud_lg_record.amend_lg(
                    db=db,
                    lg_record_id=lg_record_id,
                    amendment_letter_file=None, 
                    amendment_document_metadata=None,
                    amendment_details=updates_to_apply,
                    user_id=user_id,
                    customer_id=lg_obj.customer_id,
                    approval_request_id=None,
                )

            # Calculate Next Change Index
            max_index = db.query(func.max(LGChangeLog.change_index))\
                          .filter(LGChangeLog.lg_id == lg_record_id)\
                          .scalar()
            next_index = (max_index or 0) + 1

            crud_lg_change_log.create_change_log_entry(
                db=db,
                lg_id=updated_lg_record.id,
                staging_id=staged_record_id,
                change_index=next_index,
                diff_json=updates, 
                note="Applied historical amendment"
            )

        except Exception as e:
            logger.error(f"Failed to apply historical amendment to LG {lg_record_id} from staged record {staged_record_id}: {e}", exc_info=True)
            raise

    async def migrate_record(
        self,
        db: Session,
        staged_record: Dict[str, Any],
        user_id: int,
        customer_id: int,
    ) -> LGRecord:
        """
        Migrates a single staged LG record into the production LGRecord table.
        """
        from app.crud.crud import crud_lg_record, crud_internal_owner_contact
        from app.schemas.all_schemas import LGRecordCreate 
        import json 
        
        # --- DEBUG PRINT ---
        print("\n\n============================================")
        print(">>> ENTERING MIGRATE_RECORD FUNCTION <<<")
        source_data = staged_record.source_data_json or {}
        raw_url = source_data.get("attachment_url")
        print(f">>> RAW ATTACHMENT URL: {raw_url}")
        print("============================================\n\n")

        lg_number = source_data.get("lg_number")

        if lg_number and crud_lg_record.get_by_lg_number(db, lg_number):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"LG number '{lg_number}' already exists in production."
            )

        owner_id = source_data.get("internal_owner_contact_id")
        owner_phone = source_data.get("internal_owner_phone")
        manager_email = source_data.get("manager_email")
        owner_email = source_data.get("internal_owner_email")
        
        if owner_id and (not owner_phone or not manager_email):
            owner_obj = None
            if isinstance(owner_id, int):
                owner_obj = crud_internal_owner_contact.get(db, id=owner_id)
            elif isinstance(owner_id, str):
                clean_owner_email = owner_id.strip()
                if clean_owner_email:
                    owner_obj = crud_internal_owner_contact.get_by_email_for_customer(
                        db, 
                        customer_id=staged_record.customer_id, 
                        email=clean_owner_email
                    )
            if owner_obj:
                owner_phone = owner_obj.phone_number
                manager_email = owner_obj.manager_email
                if not owner_email:
                    owner_email = owner_obj.email

        add_fields = source_data.get("additional_field_values")
        if isinstance(add_fields, str):
            if add_fields.strip().upper() in ['N/A', '0', '']:
                add_fields = None 
            else:
                try:
                    add_fields = json.loads(add_fields)
                except:
                    add_fields = None 

        try:
            payload_data = {
                "beneficiary_corporate_id": source_data.get("beneficiary_corporate_id"),
                "issuer_name": source_data.get("issuer_name"),
                "lg_number": lg_number,
                "lg_amount": source_data.get("lg_amount"),
                "lg_currency_id": source_data.get("lg_currency_id"),
                "lg_payable_currency_id": source_data.get("lg_payable_currency_id"),
                "issuance_date": source_data.get("issuance_date"),
                "expiry_date": source_data.get("expiry_date"),
                "auto_renewal": source_data.get("auto_renewal", True),
                "lg_type_id": source_data.get("lg_type_id"),
                "lg_operational_status_id": source_data.get("lg_operational_status_id"),
                "payment_conditions": source_data.get("payment_conditions"),
                "description_purpose": source_data.get("description_purpose"),
                "issuing_bank_id": source_data.get("issuing_bank_id"),
                "issuing_bank_address": source_data.get("issuing_bank_address"),
                "issuing_bank_phone": source_data.get("issuing_bank_phone"),
                "issuing_bank_fax": source_data.get("issuing_bank_fax"),
                "issuing_method_id": source_data.get("issuing_method_id"),
                "applicable_rule_id": source_data.get("applicable_rule_id"),
                "applicable_rules_text": source_data.get("applicable_rules_text"),
                "other_conditions": source_data.get("other_conditions"),
                "internal_owner_contact_id": owner_id,
                "internal_owner_phone": owner_phone, 
                "internal_owner_email": owner_email,
                "manager_email": manager_email,
                "additional_field_values": add_fields,
                "lg_category_id": source_data.get("lg_category_id"),
                "internal_contract_project_id": source_data.get("internal_contract_project_id"),
                "notes": source_data.get("notes"),
                "ai_scan_file": None,
                "internal_supporting_document_file": None,
            }
            lg_record_create_payload = LGRecordCreate(**payload_data)
            
            new_lg_record = await crud_lg_record.create(
                db=db,
                obj_in=lg_record_create_payload,
                customer_id=customer_id,
                user_id=user_id,
                ai_scan_file_content=None,
                internal_supporting_document_file_content=None,
            )
            
            new_lg_record.migration_source = 'LEGACY'
            new_lg_record.migrated_from_staging_id = staged_record.id
            db.add(new_lg_record)
            db.flush() 
            
        except Exception as e:
            logger.error(f"Error creating LG Record: {e}", exc_info=True)
            raise

        if raw_url:
            clean_url = str(raw_url).strip().strip("'").strip('"')
            print(f">>> UPLOADING INITIAL DOCUMENT: {clean_url}")
            try:
                doc_id = await self._create_document_from_url(
                    db=db,
                    lg_record_id=new_lg_record.id,
                    url=clean_url,
                    document_type=DOCUMENT_TYPE_ORIGINAL_LG,
                    uploaded_by_user_id=user_id
                )
                if doc_id:
                    logger.info(f"Successfully attached document ID {doc_id} to {lg_number}")
                    print(f">>> UPLOAD SUCCESS. ID: {doc_id}")
                else:
                    logger.error(f"Failed to attach document to {lg_number}. _create_document_from_url returned None.")
                    print(">>> UPLOAD RETURNED NONE")
            except Exception as e:
                logger.error(f"Exception during initial attachment upload for {lg_number}: {e}", exc_info=True)
                print(f">>> UPLOAD EXCEPTION: {e}")
        else:
             print(">>> NO URL FOUND IN RAW DATA")
        
        db.refresh(new_lg_record)
        return new_lg_record

    async def migrate_instruction(
        self,
        db: Session,
        staged_instruction: Dict[str, Any],
        user_id: int,
        customer_id: int,
    ) -> LGInstruction:
        """
        Migrates a single staged LG instruction into the production LGInstruction table.
        """
        from app.crud.crud import crud_lg_record, crud_lg_instruction, crud_internal_owner_contact
        source_data = staged_record.source_data_json or {}

        lg_number = source_data.get("lg_number")
        if not lg_number:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="LG Number missing for instruction record.")

        production_lg_record = crud_lg_record.get_by_lg_number(db, lg_number)
        if not production_lg_record or production_lg_record.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"LG Record '{lg_number}' not found in production for instruction import.")

        try:
            instruction_type = source_data.get('instruction_type', 'LG_AMEND')
            maker_user_email = source_data.get('maker_user_email') or 'migration_system_user@example.com'
            maker_user = crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, maker_user_email)
            if not maker_user:
                maker_id = user_id
            else:
                maker_id = maker_user.id
            
            instruction_type_code_enum = InstructionTypeCode.AMEND
            sub_instruction_code_enum = SubInstructionCode.AMENDMENT
            
            payload_data = {
                "lg_record_id": production_lg_record.id,
                "instruction_type": instruction_type,
                "template_id": source_data.get("template_id"),
                "status": source_data.get("status", "Instruction Issued"),
                "details": source_data.get("details"),
                "maker_user_id": maker_id,
            }
            instruction_create_payload = LGInstructionCreate(**payload_data)
        except ValidationError as e:
            logger.error(f"Pydantic validation failed for staged instruction {staged_instruction.get('id')}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Data validation error for instruction: {e}")

        try:
            new_instruction = await crud_lg_instruction.create(
                db=db,
                obj_in=instruction_create_payload,
                entity_code=production_lg_record.beneficiary_corporate.code,
                lg_category_code=production_lg_record.lg_category.code,
                lg_sequence_number_str=str(production_lg_record.lg_sequence_number).zfill(4),
                instruction_type_code_enum=instruction_type_code_enum,
                sub_instruction_code_enum=sub_instruction_code_enum
            )
        except HTTPException as e:
            logger.error(f"Failed to create new LG instruction from staged data {staged_instruction.get('id')}: {e.detail}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error creating new LG instruction from staged data {staged_instruction.get('id')}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during instruction migration.")

        attachment_url = source_data.get("attachment_url")
        if attachment_url:
            await self._create_document_from_url(
                db=db,
                lg_record_id=production_lg_record.id,
                url=attachment_url,
                document_type=DOCUMENT_TYPE_INTERNAL_SUPPORTING,
                uploaded_by_user_id=user_id,
                original_instruction_serial=new_instruction.serial_number
            )
            
        db.refresh(new_instruction)
        return new_instruction
    
    def _normalize_for_comparison(self, val: Any) -> str:
        """Helper to cleanup strings for loose comparison."""
        if val is None:
            return ""
        if isinstance(val, (date, datetime)):
            return val.strftime("%Y-%m-%d")
        return str(val).strip().upper().replace(",", "").replace("_", " ")

    async def audit_staged_records(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Audits the FIRST and LAST record for each LG using AI.
        Returns a report of discrepancies.
        """
        from app.models import LGMigrationStaging, Currency, CustomerEntity, Bank
        from app.core.ai_integration import process_lg_document_with_ai, process_amendment_with_ai
        from sqlalchemy import func
        
        print("\n\n=======================================================")
        print("STARTING ENHANCED AI AUDIT")
        print("=======================================================")

        # 1. DEBUG: Print counts of ALL statuses to see where records went
        status_counts = db.query(
            LGMigrationStaging.record_status, func.count(LGMigrationStaging.id)
        ).filter(
            LGMigrationStaging.customer_id == customer_id
        ).group_by(LGMigrationStaging.record_status).all()
        
        print("-> Current Record Statuses in DB:")
        for status_val, count in status_counts:
            print(f"   - {status_val}: {count}")

        # 2. Fetch Eligible Records
        records = db.query(LGMigrationStaging).filter(
            LGMigrationStaging.customer_id == customer_id,
            LGMigrationStaging.record_status.in_([
                MigrationRecordStatusEnum.READY_FOR_IMPORT,
                MigrationRecordStatusEnum.PENDING,
                MigrationRecordStatusEnum.NEEDS_REVIEW
            ])
        ).order_by(LGMigrationStaging.id).all()

        if not records:
            print("STOPPING: No eligible records found (Check statuses above).")
            return {"status": "skipped", "message": "No staged records found."}

        # --- PRE-FETCH CACHES ---
        currency_map = {c.id: c.iso_code for c in db.query(Currency).all()}
        bank_map = {b.id: b.name for b in db.query(Bank).all()}
        
        # Entity Map (Safe Name Lookup)
        entities = db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer_id).all()
        entity_map = {}
        for e in entities:
            e_name = getattr(e, 'name', getattr(e, 'entity_name', getattr(e, 'company_name', 'Unknown')))
            entity_map[e.id] = e_name

        grouped_records = {}
        for r in records:
            data = r.source_data_json or {}
            lg_num = data.get('lg_number')
            if not lg_num: continue
            if lg_num not in grouped_records: grouped_records[lg_num] = []
            grouped_records[lg_num].append(r)

        audit_report = []

        # Helpers
        def clean_date(val):
            if not val: return ""
            s = str(val).strip()
            if "T" in s: return s.split("T")[0]
            return s

        def norm(val):
            return str(val).strip().upper().replace(",", "").replace(".", "").replace("  ", " ")

        # Fuzzy string match (returns True if enough words overlap)
        def is_fuzzy_match(str1, str2):
            s1 = set(norm(str1).split())
            s2 = set(norm(str2).split())
            # If 50% of words overlap, call it a match (for Purpose/Desc)
            overlap = s1.intersection(s2)
            if not s1 or not s2: return False
            return len(overlap) / max(len(s1), len(s2)) > 0.4

        for lg_num, group in grouped_records.items():
            print(f"\n[AUDIT] Checking LG: {lg_num}")
            group.sort(key=lambda x: x.id)
            first_rec = group[0]
            last_rec = group[-1]
            discrepancies = []

            # --- 1. AUDIT CREATION (First Record) ---
            # --- FIX: Try multiple keys to find the PDF path ---
            json_data = first_rec.source_data_json or {}
            
            # 1. Try JSON 'attachment_url'
            first_url = json_data.get('attachment_url')
            
            # 2. Fallback: Try JSON 'file_path' (Common in CSV/Excel imports)
            if not first_url:
                first_url = json_data.get('file_path')

            # 3. Fallback: Try JSON 'file_name'
            if not first_url:
                first_url = json_data.get('file_name')
                
            # 4. Fallback: Use the DB record's file_name column
            if not first_url and first_rec.file_name:
                first_url = first_rec.file_name

            # DEBUG: Print what we found so you can see it in the console
            print(f"[DEBUG] Record {first_rec.id} | LG: {lg_num}")
            print(f"   -> JSON Keys Available: {list(json_data.keys())}")
            print(f"   -> Final Resolved Path: '{first_url}'")

            if first_url:
                clean_url = str(first_url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
                
                # FIX: Fetch directly (removes the "exists" check that blocked Cloud files)
                print(f"   -> Fetching content for: '{clean_url}'")
                content, mime = await self._fetch_file_content(clean_url)
                
                if content:
                    print(f"   -> AI Scanning Original Document...")
                    ai_data, _ = await process_lg_document_with_ai(content, mime, lg_number_hint=lg_num)
                    
                    if ai_data:
                        src = first_rec.source_data_json
                        
                        # --- START VALIDATION LOGIC ---
                        # A. LG Number
                        ai_lg_num = ai_data.get('lgNumber', '')
                        if norm(lg_num) != norm(ai_lg_num):
                            err = f"LG Number Mismatch: Excel='{lg_num}' vs AI='{ai_lg_num}'"
                            discrepancies.append(err)
                            print(f"      ❌ {err}")

                        # B. Amount
                        try:
                            val_excel = float(src.get('lg_amount', 0))
                            val_ai = float(ai_data.get('lgAmount', 0))
                            if abs(val_excel - val_ai) > 1.0:
                                err = f"Amount Mismatch: Excel='{val_excel:,.2f}' vs AI='{val_ai:,.2f}'"
                                discrepancies.append(err)
                                print(f"      ❌ {err}")
                        except: pass

                        # C. Currency
                        excel_curr_id = src.get('lg_currency_id')
                        excel_curr_code = currency_map.get(excel_curr_id, "Unknown")
                        ai_curr = ai_data.get('currency', '')
                        if norm(excel_curr_code) != norm(ai_curr):
                            err = f"Currency Mismatch: Excel='{excel_curr_code}' vs AI='{ai_curr}'"
                            discrepancies.append(err)
                            print(f"      ❌ {err}")

                        # D. Issuing Bank
                        excel_bank_id = src.get('issuing_bank_id')
                        excel_bank_name = bank_map.get(excel_bank_id, "Unknown")
                        ai_bank = ai_data.get('issuingBankName', '')
                        if excel_bank_name != "Unknown":
                            if norm(excel_bank_name) not in norm(ai_bank) and norm(ai_bank) not in norm(excel_bank_name):
                                err = f"Bank Mismatch: Excel='{excel_bank_name}' vs AI='{ai_bank}'"
                                discrepancies.append(err)
                                print(f"      ❌ {err}")
                                
                        # E. Dates (Issuance & Expiry)
                        excel_date = clean_date(src.get('issuance_date'))
                        ai_date = clean_date(ai_data.get('issuanceDate'))
                        if excel_date != ai_date:
                            err = f"Issuance Date Mismatch: Excel='{excel_date}' vs AI='{ai_date}'"
                            discrepancies.append(err)
                            print(f"      ❌ {err}")
                        
                        excel_exp = clean_date(src.get('expiry_date'))
                        ai_exp = clean_date(ai_data.get('expiryDate'))
                        if excel_exp != ai_exp:
                            err = f"Expiry Date Mismatch: Excel='{excel_exp}' vs AI='{ai_exp}'"
                            discrepancies.append(err)
                            print(f"      ❌ {err}")
                        # --- END VALIDATION LOGIC ---
                        
                    else:
                        print("   ⚠️ AI returned NO data.")
                else:
                    print(f"   ❌ File not found (Checked Local & Cloud): {clean_url}")

            # --- 2. AUDIT LATEST STATUS (Last Record) ---
            if last_rec.id != first_rec.id:
                last_url = last_rec.source_data_json.get('attachment_url')
                if last_url:
                    clean_url_last = str(last_url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
                    
                    print(f"   -> Fetching Amendment content for: '{clean_url_last}'")
                    content, mime = await self._fetch_file_content(clean_url_last)
                    
                    if content:
                        print(f"   -> AI Scanning Amendment...")
                        context = {"lg_record_details": {"lgNumber": lg_num}}
                        ai_amend, _ = await process_amendment_with_ai(content, mime, context)
                        
                        if ai_amend and ai_amend.get('is_relevant_amendment'):
                            changes = ai_amend.get('amendedFields', {})
                            src = last_rec.source_data_json
                            
                            if "expiryDate" in changes:
                                val_excel = clean_date(src.get('expiry_date'))
                                val_ai = clean_date(changes['expiryDate'])
                                if val_excel != val_ai:
                                    err = f"Latest Expiry Mismatch: Excel='{val_excel}' vs AI='{val_ai}'"
                                    discrepancies.append(err)
                                    print(f"      ❌ {err}")

                            if "lgAmount" in changes:
                                try:
                                    val_excel = float(src.get('lg_amount', 0))
                                    val_ai = float(changes['lgAmount'])
                                    if abs(val_excel - val_ai) > 1.0:
                                        err = f"Latest Amount Mismatch: Excel='{val_excel}' vs AI='{val_ai}'"
                                        discrepancies.append(err)
                                        print(f"      ❌ {err}")
                                except: pass

            if discrepancies:
                audit_report.append({"lg_number": lg_num, "issues": discrepancies})
                first_rec.validation_log = first_rec.validation_log or {}
                first_rec.validation_log['ai_warning'] = " | ".join(discrepancies)
                first_rec.record_status = MigrationRecordStatusEnum.NEEDS_REVIEW
                db.add(first_rec)
        
        db.commit()
        print(f"=======================================================")
        print(f"🕵️ AUDIT FINISHED. Issues found: {len(audit_report)}")
        print(f"=======================================================\n\n")
        return {"audit_summary": audit_report, "records_checked": len(grouped_records)}

# Initialize the service instance
migration_service = MigrationService()