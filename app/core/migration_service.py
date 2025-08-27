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
    logger.debug(f"Initial record data: {record_data}")
    
    # --- Step 1: Autofill Internal Owner Contact by Email ---
    internal_owner_email = record_data.pop("internal_owner_email", None)
    if internal_owner_email:
        owner = crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, internal_owner_email)
        if owner:
            record_data["internal_owner_contact_id"] = owner.id
            logger.debug(f"Autofilled internal_owner_contact_id from email: {owner.id}")
        else:
            record_data["internal_owner_contact_id"] = None
            logger.warning(f"Internal owner not found for email: {internal_owner_email}")

    # --- Step 2: Autofill other IDs from names/codes ---
    # Simplified lookup map
    lookup_map = {
        "lg_type_id": crud_lg_type,
        "issuing_method_id": crud_issuing_method,
        "applicable_rule_id": crud_rule,
    }
    
    for field, crud_obj in lookup_map.items():
        value = record_data.get(field)
        if isinstance(value, str):
            lookup_obj = crud_obj.get_by_name(db, value)
            if lookup_obj:
                record_data[field] = lookup_obj.id
                logger.debug(f"Autofilled {field} ID from name '{value}' to: {lookup_obj.id}")
            else:
                record_data[field] = None
                logger.warning(f"Could not find a matching ID for {field} with value: '{value}'")
                
    # Handle Issuing Bank (uses fuzzy matching)
    issuing_bank_name = record_data.get("issuing_bank_id")
    if isinstance(issuing_bank_name, str):
        bank = crud_bank.get_by_name(db, issuing_bank_name)
        if bank:
            record_data["issuing_bank_id"] = bank.id
            logger.debug(f"Autofilled issuing_bank_id from name '{issuing_bank_name}' to: {bank.id}")
        else:
            record_data["issuing_bank_id"] = None
            logger.warning(f"Could not find a matching ID for issuing_bank_id with value: '{issuing_bank_name}'")
    
    # Handle Beneficiary Corporate (customer-specific lookup)
    beneficiary_name = record_data.get("beneficiary_corporate_id")
    if isinstance(beneficiary_name, str):
        entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, beneficiary_name)
        if entity:
            record_data["beneficiary_corporate_id"] = entity.id
            logger.debug(f"Autofilled beneficiary_corporate_id from name '{beneficiary_name}' to: {entity.id}")
        else:
            record_data["beneficiary_corporate_id"] = None
            logger.warning(f"Could not find a matching ID for beneficiary_corporate_id with value: '{beneficiary_name}'")

    # NEW: Consolidated LG Category Autofill Logic (Unified)
    lg_category_input = record_data.get("lg_category_id")
    resolved_category = None

    if lg_category_input:
        if isinstance(lg_category_input, str):
            # Check for customer-specific category first
            resolved_category = crud_lg_category.get_by_code(db, lg_category_input, customer_id)
            if not resolved_category:
                resolved_category = crud_lg_category.get_by_name(db, lg_category_input, customer_id)
            
            # If not found, fall back to universal categories (customer_id=None)
            if not resolved_category:
                resolved_category = crud_lg_category.get_by_code(db, lg_category_input, None)
            if not resolved_category:
                resolved_category = crud_lg_category.get_by_name(db, lg_category_input, None)
        elif isinstance(lg_category_input, int):
            # If it's an ID, just check the single table
            resolved_category = crud_lg_category.get(db, lg_category_input)

    if resolved_category:
        record_data["lg_category_id"] = resolved_category.id
        logger.debug(f"Autofilled lg_category_id from input '{lg_category_input}' to: {resolved_category.id}")
    else:
        # Fallback to the default universal category if no input or no match
        default_category = crud_lg_category.get_default_category(db, None)
        if default_category:
            record_data["lg_category_id"] = default_category.id
            logger.warning(f"Could not find a matching ID for lg_category_id with value: '{lg_category_input}'. Defaulting to universal category ID {default_category.id}.")
        else:
            # This case indicates a critical system configuration error.
            logger.error("No default universal category configured. Cannot autofill.")
            record_data["lg_category_id"] = None

    # Handle Currencies (by ISO code)
    for currency_field in ["lg_currency_id", "lg_payable_currency_id"]:
        currency_code = record_data.get(currency_field)
        if isinstance(currency_code, str):
            currency = crud_currency.get_by_iso_code(db, currency_code)
            if currency:
                record_data[currency_field] = currency.id
                logger.debug(f"Autofilled {currency_field} from ISO code '{currency_code}' to: {currency.id}")
            else:
                record_data[currency_field] = None
                logger.warning(f"Could not find a matching ID for {currency_field} with value: '{currency_code}'")

    # --- Step 3: Autofill secondary bank details based on ID lookup ---
    bank_id = record_data.get("issuing_bank_id")
    if isinstance(bank_id, int):
        bank = crud_bank.get(db, bank_id)
        if bank:
            if not record_data.get("issuing_bank_address"):
                record_data["issuing_bank_address"] = bank.address
                logger.debug(f"Autofilled issuing_bank_address: {bank.address}")
            if not record_data.get("issuing_bank_phone"):
                record_data["issuing_bank_phone"] = bank.phone_number
                logger.debug(f"Autofilled issuing_bank_phone: {bank.phone_number}")
            if not record_data.get("issuing_bank_fax"):
                record_data["issuing_bank_fax"] = bank.fax
                logger.debug(f"Autofilled issuing_bank_fax: {bank.fax}")
        else:
            logger.warning(f"Bank object not found in database for ID: {bank_id}. Cannot autofill secondary details.")
            
    # --- Step 4: Apply defaults and calculate dynamic fields ---
    
    issuance_date_str = record_data.get("issuance_date")
    expiry_date_str = record_data.get("expiry_date")
    if issuance_date_str and expiry_date_str:
        try:
            issuance_date = datetime.strptime(str(issuance_date_str), "%Y-%m-%d").date()
            expiry_date = datetime.strptime(str(expiry_date_str), "%Y-%m-%d").date()
            record_data['lg_period_months'] = calculate_lg_period_months(issuance_date, expiry_date)
            logger.debug(f"Calculated lg_period_months: {record_data['lg_period_months']}")
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to calculate LG period months: {e}")
            pass
            
    if not record_data.get("lg_payable_currency_id") and isinstance(record_data.get("lg_currency_id"), int):
        record_data["lg_payable_currency_id"] = record_data["lg_currency_id"]
    
    manual_delivery_method = db.query(models.IssuingMethod).filter(models.IssuingMethod.name == "Manual Delivery").first()
    if manual_delivery_method and not record_data.get("issuing_method_id"):
        record_data["issuing_method_id"] = manual_delivery_method.id
    
    urdg_rule = db.query(models.Rule).filter(models.Rule.name == "URDG 758").first()
    if urdg_rule and not record_data.get("applicable_rule_id"):
        record_data["applicable_rule_id"] = urdg_rule.id
    
    record_data["auto_renewal"] = record_data.get("auto_renewal", True)
    
    logger.debug(f"Autofill process complete. Final data: {record_data}")
    return record_data

    # Handle Currencies (by ISO code)
    for currency_field in ["lg_currency_id", "lg_payable_currency_id"]:
        currency_code = record_data.get(currency_field)
        if isinstance(currency_code, str):
            currency = crud_currency.get_by_iso_code(db, currency_code)
            if currency:
                record_data[currency_field] = currency.id
                logger.debug(f"Autofilled {currency_field} from ISO code '{currency_code}' to: {currency.id}")
            else:
                record_data[currency_field] = None
                logger.warning(f"Could not find a matching ID for {currency_field} with value: '{currency_code}'")

    # --- Step 3: Autofill secondary bank details based on ID lookup ---
    bank_id = record_data.get("issuing_bank_id")
    if isinstance(bank_id, int):
        bank = crud_bank.get(db, bank_id)
        if bank:
            if not record_data.get("issuing_bank_address"):
                record_data["issuing_bank_address"] = bank.address
                logger.debug(f"Autofilled issuing_bank_address: {bank.address}")
            if not record_data.get("issuing_bank_phone"):
                record_data["issuing_bank_phone"] = bank.phone_number
                logger.debug(f"Autofilled issuing_bank_phone: {bank.phone_number}")
            if not record_data.get("issuing_bank_fax"):
                record_data["issuing_bank_fax"] = bank.fax
                logger.debug(f"Autofilled issuing_bank_fax: {bank.fax}")
        else:
            logger.warning(f"Bank object not found in database for ID: {bank_id}. Cannot autofill secondary details.")
            
    # --- Step 4: Apply defaults and calculate dynamic fields ---
    
    issuance_date_str = record_data.get("issuance_date")
    expiry_date_str = record_data.get("expiry_date")
    if issuance_date_str and expiry_date_str:
        try:
            issuance_date = datetime.strptime(str(issuance_date_str), "%Y-%m-%d").date()
            expiry_date = datetime.strptime(str(expiry_date_str), "%Y-%m-%d").date()
            record_data['lg_period_months'] = calculate_lg_period_months(issuance_date, expiry_date)
            logger.debug(f"Calculated lg_period_months: {record_data['lg_period_months']}")
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to calculate LG period months: {e}")
            pass
            
    if not record_data.get("lg_payable_currency_id") and isinstance(record_data.get("lg_currency_id"), int):
        record_data["lg_payable_currency_id"] = record_data["lg_currency_id"]
    
    manual_delivery_method = db.query(IssuingMethod).filter(IssuingMethod.name == "Manual Delivery").first()
    if manual_delivery_method and not record_data.get("issuing_method_id"):
        record_data["issuing_method_id"] = manual_delivery_method.id
    
    urdg_rule = db.query(Rule).filter(Rule.name == "URDG 758").first()
    if urdg_rule and not record_data.get("applicable_rule_id"):
        record_data["applicable_rule_id"] = urdg_rule.id
    
    record_data["auto_renewal"] = record_data.get("auto_renewal", True)
    
    logger.debug(f"Autofill process complete. Final data: {record_data}")
    return record_data

# =====================================================================================
# END OF MOVED FUNCTIONS
# =====================================================================================


class MigrationService:
    def __init__(self):
        pass

    async def _fetch_file_content(self, url: str) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Simulates fetching file content from a URL.
        """
        logger.warning(f"Simulating file fetch for URL: {url}. This is a placeholder.")
        # Simulating file content and a mime type
        return b"This is placeholder content for a migrated document.", "application/pdf"

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
            updated_lg_record = await crud_lg_record.amend_lg(
                db=db,
                lg_record_id=lg_record_id,
                amendment_letter_file=None, # No file for historical import
                amendment_document_metadata=None,
                amendment_details=updates,
                user_id=user_id,
                customer_id=db.query(LGRecord).filter(LGRecord.id == lg_record_id).first().customer_id,
                approval_request_id=None,
            )

            crud_lg_change_log.create_change_log_entry(
                db=db,
                lg_id=updated_lg_record.id,
                staging_id=staged_record_id,
                change_index=1,
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
        Migrates a single staged LG record into the production LGRecord table,
        reusing the core business logic from crud_lg_record.
        """
        from app.crud.crud import crud_lg_record
        source_data = staged_record.get('source_data_json', {})
        lg_number = source_data.get("lg_number")

        # CRITICAL FIX: Pre-check against production database to avoid IntegrityError.
        if lg_number and crud_lg_record.get_by_lg_number(db, lg_number):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"LG number '{lg_number}' already exists in production. Skipping import of staged record {staged_record.get('id')}."
            )

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
                "internal_owner_contact_id": source_data.get("internal_owner_contact_id"),
                "lg_category_id": source_data.get("lg_category_id"),
                "additional_field_values": source_data.get("additional_field_values"),
                "internal_contract_project_id": source_data.get("internal_contract_project_id"),
                "notes": source_data.get("notes"),
                "ai_scan_file": None,
                "internal_supporting_document_file": None,
            }
            lg_record_create_payload = LGRecordCreate(**payload_data)
        except ValidationError as e:
            logger.error(f"Pydantic validation failed for staged record {staged_record.get('id')}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Data validation error: {e}")

        try:
            new_lg_record = await crud_lg_record.create(
                db=db,
                obj_in=lg_record_create_payload,
                customer_id=customer_id,
                user_id=user_id,
                ai_scan_file_content=None,
                internal_supporting_document_file_content=None,
            )
            
            new_lg_record.migration_source = 'LEGACY'
            new_lg_record.migrated_from_staging_id = staged_record.get('id')
            db.add(new_lg_record)
            db.flush()
        except HTTPException as e:
            logger.error(f"Failed to create new LG record from staged data {staged_record.get('id')}: {e.detail}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error creating new LG record from staged data {staged_record.get('id')}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during record migration.")
        
        attachment_url = source_data.get("attachment_url")
        if attachment_url:
            await self._create_document_from_url(
                db=db,
                lg_record_id=new_lg_record.id,
                url=attachment_url,
                document_type=DOCUMENT_TYPE_ORIGINAL_LG,
                uploaded_by_user_id=user_id
            )
        
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
        Migrates a single staged LG instruction into the production LGInstruction table,
        reusing the core business logic from crud_lg_instruction.
        """
        from app.crud.crud import crud_lg_record, crud_lg_instruction, crud_internal_owner_contact
        source_data = staged_instruction.get('source_data_json', {})

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

# Initialize the service instance
migration_service = MigrationService()