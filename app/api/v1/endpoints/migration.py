# app/api/v1/endpoints/migration.py
import csv
import io
import json
import pandas as pd
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Query
from sqlalchemy.orm import Session
import numpy as np
from sqlalchemy import func, or_, cast
from pydantic import BaseModel
from dateutil.relativedelta import relativedelta
import hashlib
from sqlalchemy.dialects.postgresql import JSONB
from app.database import get_db
from app.core.security import get_current_corporate_admin_context, TokenData
from app.schemas.migration_schemas import (
    LGMigrationStagingOut,
    LGMigrationStagingIn,
    MigrationReportSummary,
    MigrationRecordStatusEnum,
    ProcessingSummaryOut,
    MigrationUploadResponse,
    RevalidateRecordsIn,
    MigrationTypeEnum,
)
import app.models as models

# UPDATED IMPORT: get services from crud.py
from app.crud.crud import (
    crud_lg_migration, crud_internal_owner_contact, crud_lg_category, crud_bank,
    crud_issuing_method, crud_rule, crud_customer_entity,
    crud_lg_type, crud_lg_record, crud_migration_batch, crud_lg_change_log, migration_history_service, log_action
)
from app.core.lg_validation_service import lg_validation_service
from app.core.migration_service import migration_service
from app.models import (
    InternalOwnerContact, LGCategory, Bank, IssuingMethod, Rule, LgType , Currency, CustomerEntity, LGRecord
)
from app.schemas.all_schemas import LGRecordCreate
from app.schemas.migration_history_schemas import (
    ImportHistoryIn,
    MigrationHistoryPreviewOut,
    MigrationBatchOut,
    MigrationReportOut
)
from datetime import date, datetime


import logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/migration",
    tags=["Migration"],
    dependencies=[Depends(get_current_corporate_admin_context)],
)

column_mapping = {
    # Core Fields
    "LG_ID": "lg_number",
    "LG No": "lg_number",
    "Guarantee No": "lg_number",
    "Guarantee_ID": "lg_number",
    "LG Number": "lg_number",
    "ATTACHMENT": "attachment_url",

    "Amount": "lg_amount",
    "LG Amount": "lg_amount",
    "Value": "lg_amount",
    "Guarantee Amount": "lg_amount",
    "Amt": "lg_amount",

    "Currency": "lg_currency_id",
    "Curr": "lg_currency_id",
    "LG Currency": "lg_currency_id",
    "Guarantee Currency": "lg_currency_id",
    "LG_Currency": "lg_currency_id",

    "Payable_Currency": "lg_payable_currency_id",
    "Pay Currency": "lg_payable_currency_id",
    "Settlement Currency": "lg_payable_currency_id",
    "Payout Currency": "lg_payable_currency_id",

    "Issuance_Date": "issuance_date",
    "issue_date": "issuance_date",
    "Issue Date": "issuance_date",
    "Start Date": "issuance_date",
    "Opening Date": "issuance_date",
    "Issued Date": "issuance_date",

    "Expiry_Date": "expiry_date",
    "Maturity Date": "expiry_date",
    "End Date": "expiry_date",
    "Valid Until": "expiry_date",
    "Expiry": "expiry_date",
    "Expiration Date": "expiry_date",
    "Exp Date": "expiry_date",

    "Auto_Renewal": "auto_renewal",
    "Renewal": "auto_renewal",
    "Auto Renewal": "auto_renewal",
    "Renewable": "auto_renewal",

    "Type": "lg_type_id",
    "LG Type": "lg_type_id",
    "Guarantee Type": "lg_type_id",
    "Type of LG": "lg_type_id",
    "Guarantee_Type": "lg_type_id",

    "Operational_Status": "lg_operational_status_id",
    "Op Status": "lg_operational_status_id",
    "LG O.Status": "lg_operational_status_id",

    "Payment_Conditions": "payment_conditions",
    "Payment Terms": "payment_conditions",
    "Conditions of Payment": "payment_conditions",
    "Payment_Terms": "payment_conditions",

    "Description_Purpose": "description_purpose",
    "Purpose": "description_purpose",
    "Desc": "description_purpose",
    "Guarantee Purpose": "description_purpose",
    "description": "description_purpose",
    "Description": "description_purpose",
    "LG_Purpose": "description_purpose",

    "Beneficiary": "beneficiary_corporate_id",
    "beneficiary_name": "beneficiary_corporate_id",
    "Beneficiary Name": "beneficiary_corporate_id",
    "Company Name": "beneficiary_corporate_id",
    "Beneficiary_Name": "beneficiary_corporate_id",

    # Bank Fields
    "Issuing_Bank_Name": "issuing_bank_id",
    "Bank": "issuing_bank_id",
    "Issuer Bank": "issuing_bank_id",
    "Issuing Bank": "issuing_bank_id",
    "Bank Name": "issuing_bank_id",

    "Bank_Address": "issuing_bank_address",
    "Bank Addr": "issuing_bank_address",
    "Bank Location": "issuing_bank_address",
    "Branch Address": "issuing_bank_address",

    "Bank_Phone": "issuing_bank_phone",
    "Tel": "issuing_bank_phone",
    "Telephone": "issuing_bank_phone",
    "Bank Tel": "issuing_bank_phone",
    "Phone": "issuing_bank_phone",

    "Bank_Fax": "issuing_bank_fax",
    "Fax": "issuing_bank_fax",
    "Bank Fax No": "issuing_bank_fax",
    "Fax Number": "issuing_bank_fax",

    "Issuing_Method": "issuing_method_id",
    "Delivery Method": "issuing_method_id",
    "Issuance Method": "issuing_method_id",
    "Method": "issuing_method_id",

    "Applicable_Rule": "applicable_rule_id",
    "Rules": "applicable_rule_id",
    "Applicable Rules": "applicable_rule_id",
    "Rule": "applicable_rule_id",
    "Governing Rules": "applicable_rule_id",

    "Rule_Text": "applicable_rules_text",
    "Rule Notes": "applicable_rules_text",
    "Rule Description": "applicable_rules_text",

    "Other_Conditions": "other_conditions",
    "Other Terms": "other_conditions",
    "Additional Conditions": "other_conditions",

    # Internal Fields
    "Internal_Owner_Email": "internal_owner_email",
    "Owner Email": "internal_owner_email",
    "Contact Email": "internal_owner_email",
    "Internal Contact": "internal_owner_email",

    "Category": "lg_category_id",
    "LG Category": "lg_category_id",
    "Category ID": "lg_category_id",
    "LG_Category": "lg_category_id",

    "Contract_ID": "internal_contract_project_id",
    "Project": "internal_contract_project_id",
    "Contract": "internal_contract_project_id",
    "Contract Ref": "internal_contract_project_id",
    "Project ID": "internal_contract_project_id",
    "Project_ID": "internal_contract_project_id",

    "Notes": "notes",
    "Remarks": "notes",
    "Comments": "notes",
    "Additional Notes": "notes",

    "Issuer_Name": "issuer_name",
    "Issuer": "issuer_name",
    "Supplier": "supplier_name",
    "Contractor": "contractor_name",
    "Issuer Company": "issuer_name",
}


def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converts DataFrame columns to snake_case for consistency."""
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    return df
    
def _get_id_from_code(db: Session, model: Any, code: str, code_column: str = 'iso_code') -> Optional[int]:
    """Helper to find an ID by a unique code/name."""
    obj = db.query(model).filter(func.lower(getattr(model, code_column)) == func.lower(code)).first()
    return obj.id if obj else None

def _get_id_by_name(db: Session, model: Any, name: str) -> Optional[int]:
    """Finds an ID by a unique name, short name, or former name in a given table."""
    # Special handling for Bank model to check multiple name fields
    if model == Bank:
        obj = db.query(model).filter(
            or_(
                func.lower(model.name) == func.lower(name),
                func.lower(model.short_name) == func.lower(name),
                cast(model.former_names, JSONB).op('?')(name)
            )
        ).first()
        return obj.id if obj else None
        
    # Generic lookup for other models
    obj = db.query(model).filter(func.lower(model.name) == func.lower(name)).first()
    return obj.id if obj else None
    if isinstance(record_data.get("lg_type_id"), str):
        lg_type_name = record_data.pop("lg_type_id")
        lg_type_obj = _get_id_by_name(db, models.LgType, lg_type_name)
        if lg_type_obj:
            record_data["lg_type_id"] = lg_type_obj
        else:
            record_data["lg_type_id"] = lg_type_name

    if isinstance(record_data.get("lg_operational_status_id"), str):
        operational_status_name = record_data.pop("lg_operational_status_id")
        operational_status_obj = _get_id_by_name(db, models.LgOperationalStatus, operational_status_name)
        if operational_status_obj:
            record_data["lg_operational_status_id"] = operational_status_obj
        else:
            record_data["lg_operational_status_id"] = operational_status_name
        
    # Generic lookup for other models
    obj = db.query(model).filter(func.lower(model.name) == func.lower(name)).first()
    return obj.id if obj else None

def _get_entity_id_from_name(db: Session, customer_id: int, entity_name: str) -> Optional[int]:
    """Finds a customer entity ID by its name for a given customer."""
    entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, entity_name)
    return entity.id if entity else None


def _apply_defaults_and_autofill(db: Session, record_data: Dict[str, Any], customer_id: int) -> Dict[str, Any]:
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
    # This block is the core of the fix. It uses a single, consistent loop
    # to handle all lookups and avoids redundant/conflicting logic.
    lookup_map = {
        "lg_type_id": (crud_lg_type, "name"),
        "issuing_method_id": (crud_issuing_method, "name"),
        "applicable_rule_id": (crud_rule, "name"),
    }
    
    for field, (crud_obj, lookup_attr) in lookup_map.items():
        value = record_data.get(field)
        if isinstance(value, str):
            lookup_obj = crud_obj.get_by_name(db, value)
            if lookup_obj:
                record_data[field] = lookup_obj.id
                logger.debug(f"Autofilled {field} ID from name '{value}' to: {lookup_obj.id}")
            else:
                logger.warning(f"Could not find a matching ID for {field} with value: '{value}'")
                
    # Specific lookups that require more complex logic
    
    # Handle Issuing Bank (uses fuzzy matching)
    issuing_bank_name = record_data.get("issuing_bank_id")
    if isinstance(issuing_bank_name, str):
        bank = crud_bank.get_by_name(db, issuing_bank_name)
        if bank:
            record_data["issuing_bank_id"] = bank.id
            logger.debug(f"Autofilled issuing_bank_id from name '{issuing_bank_name}' to: {bank.id}")
        else:
            logger.warning(f"Could not find a matching ID for issuing_bank_id with value: '{issuing_bank_name}'")
    
    # Handle Beneficiary Corporate (customer-specific lookup)
    beneficiary_name = record_data.get("beneficiary_corporate_id")
    if isinstance(beneficiary_name, str):
        entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, beneficiary_name)
        if entity:
            record_data["beneficiary_corporate_id"] = entity.id
            logger.debug(f"Autofilled beneficiary_corporate_id from name '{beneficiary_name}' to: {entity.id}")
        else:
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


@router.post("/upload-structured", status_code=status.HTTP_201_CREATED, response_model=MigrationUploadResponse)
async def upload_structured_file_for_staging(
    file: UploadFile = File(...),
    migration_type: MigrationTypeEnum = Query(MigrationTypeEnum.RECORD),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Structured upload endpoint hit for file: {file.filename}, type: {file.content_type}, for customer {current_user.customer_id}.")
    
    file_content = await file.read()
    records_to_stage = []
    
    file_hash = hashlib.sha256(file_content).hexdigest()
    existing_batch = crud_migration_batch.get_by_file_hash(db, file_hash)
    if existing_batch:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"This file has already been uploaded and processed. Batch ID: {existing_batch.id}."
        )

    normalized_mapping = {v: k for k, v in column_mapping.items()}
    def norm_keys(d):
        return {
            normalized_mapping.get(k.replace(" ", "_").lower(), k.replace(" ", "_").lower()): v
            for k, v in d.items()
        }

    if file.content_type == "application/json":
        try:
            data = json.loads(file_content)
            if not isinstance(data, list):
                data = [data]
            records_to_stage = [norm_keys(item) for item in data]
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid JSON format: {e}")
            
    elif file.content_type in [
        "text/csv", 
        "application/vnd.ms-excel", 
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ]:
        try:
            file_stream = io.BytesIO(file_content)
            if file.content_type == "text/csv":
                df = pd.read_csv(file_stream, dtype=str)
            else:
                df = pd.read_excel(file_stream, engine='openpyxl')
            
            df = _normalize_df_columns(df)

            df.rename(columns={k.lower(): v for k, v in column_mapping.items() if k.lower() in df.columns}, inplace=True)
            
            for col in ['issuance_date', 'expiry_date']:
                if col in df.columns:
                    parsed_dates = pd.to_datetime(df[col], errors='coerce').dt.date
                    df[col] = parsed_dates.where(pd.notna(parsed_dates), None)

            df = df.replace({np.nan: None})
            records_to_stage = df.to_dict('records')
            
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Failed to parse CSV/Excel file: {e}")

    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported file type. Please upload a JSON, CSV, or Excel file.")

    for record in records_to_stage:
        for key, value in record.items():
            if isinstance(value, date):
                record[key] = value.isoformat()
            
    
    staged_records = []
    try:
        for record_data in records_to_stage:
            if migration_type == MigrationTypeEnum.RECORD:
                enhanced_record_data = _apply_defaults_and_autofill(db, record_data, current_user.customer_id)
                validation_errors = lg_validation_service.validate_lg_data(enhanced_record_data, context='migration', db=db, customer_id=current_user.customer_id)
            else:
                enhanced_record_data = record_data
                validation_errors = lg_validation_service.validate_lg_instruction_data(enhanced_record_data, context='migration', db=db, customer_id=current_user.customer_id)
            
            if validation_errors:
                record_status = MigrationRecordStatusEnum.ERROR
            else:
                is_expired = False
                try:
                    expiry_date_str = enhanced_record_data.get("expiry_date")
                    if expiry_date_str and datetime.strptime(expiry_date_str, "%Y-%m-%d").date() < date.today():
                        is_expired = True
                except (ValueError, TypeError):
                    pass

                is_duplicate = False
                lg_number = enhanced_record_data.get("lg_number")
                if lg_number:
                    if migration_type == MigrationTypeEnum.RECORD:
                        latest_record = db.query(models.LGMigrationStaging).filter(
                            models.LGMigrationStaging.customer_id == current_user.customer_id,
                            func.lower(models.LGMigrationStaging.source_data_json['lg_number'].astext) == func.lower(lg_number),
                            models.LGMigrationStaging.record_status.in_([
                                MigrationRecordStatusEnum.READY_FOR_IMPORT,
                                MigrationRecordStatusEnum.PENDING,
                                MigrationRecordStatusEnum.ERROR,
                                MigrationRecordStatusEnum.NEEDS_REVIEW
                            ])
                        ).order_by(models.LGMigrationStaging.created_at.desc()).first()
                        if latest_record:
                            is_duplicate = True
                        
                if is_expired:
                    record_status = MigrationRecordStatusEnum.EXPIRED
                    validation_errors = {"expiry_date": lg_validation_service._get_enhanced_error("expiry_date", "LG expiry date is in the past.")}
                elif is_duplicate:
                    record_status = MigrationRecordStatusEnum.DUPLICATE
                    validation_errors = {"lg_number": lg_validation_service._get_enhanced_error("lg_number", "Duplicate LG number found in staged records. This record will be ignored in favor of the most recent one.")}
                else:
                    record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT

            new_record = crud_lg_migration.create_staging_record(
                db=db,
                obj_in=LGMigrationStagingIn(
                    file_name=file.filename,
                    source_data_json=enhanced_record_data,
                    migration_type=migration_type,
                ),
                customer_id=current_user.customer_id,
                user_id=current_user.user_id,
                record_status=record_status,
                validation_log=validation_errors
            )
            staged_records.append(new_record)
        
        total_records = len(staged_records)
        total_errors = sum(1 for rec in staged_records if rec.record_status in [MigrationRecordStatusEnum.ERROR, MigrationRecordStatusEnum.EXPIRED])
        total_duplicates = sum(1 for rec in staged_records if rec.record_status == MigrationRecordStatusEnum.DUPLICATE)

        return {
            "message": f"Successfully staged and processed {total_records} records.",
            "imported_count": sum(1 for rec in staged_records if rec.record_status == MigrationRecordStatusEnum.READY_FOR_IMPORT),
            "failed_count": total_errors,
            "duplicate_count": total_duplicates,
            "staged_records": [LGMigrationStagingOut.model_validate(rec) for rec in staged_records]
        }

    except HTTPException as e:
        db.rollback()
        logger.error(f"Failed to create structured staging records due to a HTTP error: {e.detail}", exc_info=True)
        raise e
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create structured staging records: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to create structured staging records: {e}")


@router.post("/manual-entry", status_code=status.HTTP_201_CREATED, response_model=LGMigrationStagingOut)
async def manual_entry_staged_record(
    record_in: LGMigrationStagingIn,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Manual record entry endpoint hit for customer {current_user.customer_id} by user {current_user.email}.")
    try:
        normalized_record_data = {
            k.replace(" ", "_").lower(): v for k, v in record_in.source_data_json.items()
        }
        
        if record_in.migration_type == MigrationTypeEnum.RECORD:
            enhanced_record_data = _apply_defaults_and_autofill(db, normalized_record_data, current_user.customer_id)
            validation_errors = lg_validation_service.validate_lg_data(enhanced_record_data, context='migration', db=db, customer_id=current_user.customer_id)
        else:
            enhanced_record_data = normalized_record_data
            validation_errors = lg_validation_service.validate_lg_instruction_data(enhanced_record_data, context='migration', db=db, customer_id=current_user.customer_id)
            
        if validation_errors:
            record_status = MigrationRecordStatusEnum.ERROR
        else:
            is_expired = False
            try:
                expiry_date_str = enhanced_record_data.get("expiry_date")
                if expiry_date_str and datetime.strptime(expiry_date_str, "%Y-%m-%d").date() < date.today():
                    is_expired = True
            except (ValueError, TypeError):
                pass
            
            is_duplicate = False
            lg_number = enhanced_record_data.get("lg_number")
            if lg_number:
                if record_in.migration_type == MigrationTypeEnum.RECORD:
                    latest_record = db.query(models.LGMigrationStaging).filter(
                        models.LGMigrationStaging.customer_id == current_user.customer_id,
                        func.lower(models.LGMigrationStaging.source_data_json['lg_number'].astext) == func.lower(lg_number),
                        models.LGMigrationStaging.record_status.in_([
                            MigrationRecordStatusEnum.READY_FOR_IMPORT,
                            MigrationRecordStatusEnum.PENDING,
                            MigrationRecordStatusEnum.ERROR,
                            MigrationRecordStatusEnum.NEEDS_REVIEW,
                            MigrationRecordStatusEnum.DUPLICATE
                        ])
                    ).order_by(models.LGMigrationStaging.created_at.desc()).first()
                    if latest_record:
                        is_duplicate = True

            if is_expired:
                record_status = MigrationRecordStatusEnum.EXPIRED
                validation_errors = {"expiry_date": lg_validation_service._get_enhanced_error("expiry_date", "LG expiry date is in the past.")}
            elif is_duplicate:
                record_status = MigrationRecordStatusEnum.DUPLICATE
                validation_errors = {"lg_number": lg_validation_service._get_enhanced_error("lg_number", "Duplicate LG number found in staged records. This record will be ignored in favor of the most recent one.")}
            else:
                record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT
                
        new_record = crud_lg_migration.create_staging_record(
            db=db,
            obj_in=LGMigrationStagingIn(
                file_name=record_in.file_name,
                source_data_json=enhanced_record_data,
                migration_type=record_in.migration_type,
            ),
            customer_id=current_user.customer_id,
            user_id=current_user.user_id,
            record_status=record_status,
            validation_log=validation_errors
        )
        db.commit()
        return new_record
    
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create manual staged record: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to create manual staged record: {e}")

@router.post("/process-staged", response_model=ProcessingSummaryOut, status_code=status.HTTP_200_OK)
async def process_staged_records_endpoint(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    raise HTTPException(
        status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        detail="This endpoint is deprecated. Records are now processed automatically upon upload or manual entry."
    )

@router.post("/staged/{record_id}/re-validate", response_model=LGMigrationStagingOut, status_code=status.HTTP_200_OK)
async def re_validate_staged_record(
    record_id: int,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Re-validating staged record {record_id} for customer {current_user.customer_id}.")
    
    db_record = crud_lg_migration.get(db, record_id)
    if not db_record or db_record.customer_id != current_user.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Staged record not found or not accessible.")
        
    try:
        updated_record = crud_lg_migration.re_validate_record(db, record_id, current_user.customer_id, current_user.user_id)
        db.commit()
        return updated_record
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to re-validate staged record {record_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Re-validation failed: {e}")

@router.post("/staged/re-validate-multiple", status_code=status.HTTP_200_OK, response_model=Dict[str, Any])
async def re_validate_multiple_staged_records(
    revalidate_in: RevalidateRecordsIn,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Re-validating multiple staged records for customer {current_user.customer_id}.")
    results = {
        "success_count": 0,
        "failed_count": 0,
        "results": []
    }
    
    for record_id in revalidate_in.ids:
        try:
            db_record = crud_lg_migration.get(db, record_id)
            if not db_record or db_record.customer_id != current_user.customer_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Record not found or not accessible.")

            updated_record = crud_lg_migration.re_validate_record(db, record_id, current_user.customer_id, current_user.user_id)
            results["success_count"] += 1
            results["results"].append({
                "id": record_id,
                "status": "success",
                "new_status": updated_record.record_status.value
            })
        except Exception as e:
            db.rollback()
            results["failed_count"] += 1
            results["results"].append({
                "id": record_id,
                "status": "failed",
                "error": str(e)
            })
    
    db.commit()
    return results

@router.post("/preview-history", response_model=List[MigrationHistoryPreviewOut], status_code=status.HTTP_200_OK)
async def preview_historical_reconstruction(
    lg_number: Optional[str] = None,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Historical preview requested for customer {current_user.customer_id} for LG: {lg_number or 'all eligible'}")
    try:
        preview_data = await migration_history_service.preview_history(db, current_user.customer_id, lg_number)
        return preview_data
    except Exception as e:
        logger.error(f"Failed to generate history preview: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate history preview: {e}")

@router.post("/import-history", status_code=status.HTTP_200_OK)
async def import_historical_records(
    import_in: ImportHistoryIn,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Historical import started for customer {current_user.customer_id} by user {current_user.email}.")
    
    query = db.query(models.LGMigrationStaging).filter(models.LGMigrationStaging.customer_id == current_user.customer_id)
    if import_in.lg_numbers:
        query = query.filter(func.lower(models.LGMigrationStaging.source_data_json['lg_number'].astext).in_([ln.lower() for ln in import_in.lg_numbers]))
    
    all_snapshots = query.filter(models.LGMigrationStaging.record_status.in_([
        MigrationRecordStatusEnum.READY_FOR_IMPORT,
        MigrationRecordStatusEnum.PENDING,
        MigrationRecordStatusEnum.ERROR
    ])).all()
    
    if not all_snapshots:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No eligible records found for historical import.")

    lg_groups = {}
    for snapshot in all_snapshots:
        lg_num = snapshot.source_data_json.get('lg_number')
        if lg_num not in lg_groups:
            lg_groups[lg_num] = []
        lg_groups[lg_num].append(snapshot)
        
    batch = models.MigrationBatch(
        user_id=current_user.user_id,
        notes=import_in.batch_note,
        source_files=[s.file_name for s in all_snapshots]
    )
    db.add(batch)
    db.flush()
    
    batch_results = {'imported': 0, 'updated': 0, 'failed': 0, 'skipped_exists': 0}
    
    for lg_num, snapshots in lg_groups.items():
        with db.begin_nested() as nested_session:
            try:
                sorted_snapshots = sorted(snapshots, key=lambda s: migration_history_service._get_snapshot_sort_key(s))
                first_snapshot = sorted_snapshots[0]
                
                existing_lg_in_prod = crud_lg_record.get_by_lg_number(db, lg_num)
                if existing_lg_in_prod:
                    logger.warning(f"LG number '{lg_num}' already exists in production. Skipping import.")
                    for snap in snapshots:
                        snap.record_status = MigrationRecordStatusEnum.ERROR
                        snap.validation_log = snap.validation_log or {}
                        snap.validation_log['import_error'] = 'LG already exists in production table. Skipping.'
                    batch_results['skipped_exists'] += 1
                    continue
                
                first_snapshot_data = first_snapshot.source_data_json
                first_snapshot_data.pop('history_sequence', None)
                first_snapshot_data.pop('history_timestamp', None)
                
                lg_record_create_payload = LGRecordCreate(**first_snapshot_data)
                
                new_lg = await crud_lg_record.create_from_migration(
                    db=db,
                    obj_in=lg_record_create_payload,
                    customer_id=current_user.customer_id,
                    user_id=current_user.user_id,
                    migration_source='LEGACY',
                    migrated_from_staging_id=first_snapshot.id
                )
                
                if len(sorted_snapshots) > 1:
                    for i in range(1, len(sorted_snapshots)):
                        prev_snapshot_data = sorted_snapshots[i-1].source_data_json
                        current_snapshot = sorted_snapshots[i]
                        current_snapshot_data = current_snapshot.source_data_json
                        
                        diff = migration_history_service._get_diff(prev_snapshot_data, current_snapshot_data)
                        
                        if diff:
                            await migration_service._apply_migration_amendment(
                                db, new_lg.id, diff, current_user.user_id, current_snapshot.id
                            )

                for snap in snapshots:
                    snap.record_status = MigrationRecordStatusEnum.IMPORTED
                    snap.production_lg_id = new_lg.id
                
                batch_results['imported'] += 1
            
            except Exception as e:
                nested_session.rollback()
                logger.error(f"Failed to import LG '{lg_num}' during historical import: {e}", exc_info=True)
                for snap in snapshots:
                    snap.record_status = MigrationRecordStatusEnum.ERROR
                    snap.validation_log = snap.validation_log or {}
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

@router.get("/report", response_model=MigrationReportOut, status_code=status.HTTP_200_OK)
async def get_migration_report(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    logger.info(f"Migration report requested for customer {current_user.customer_id}.")
    try:
        staged_summary = crud_lg_migration.get_migration_report(db, current_user.customer_id)
        last_batch = crud_migration_batch.get_batches(db, limit=1)
        
        last_batch_out = MigrationBatchOut.model_validate(last_batch[0]) if last_batch else None
        
        return MigrationReportOut(
            total_staged_records=staged_summary.total_records,
            summary_by_status={
                "PENDING": staged_summary.pending_count,
                "READY_FOR_IMPORT": staged_summary.ready_for_import_count,
                "IMPORTED": staged_summary.imported_count,
                "ERROR": staged_summary.error_count,
                "EXPIRED": staged_summary.expired_count,
                "DUPLICATE": staged_summary.duplicates,
            },
            last_batch=last_batch_out,
        )
    except Exception as e:
        logger.error(f"Failed to generate migration report: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate migration report: {e}")

@router.get("/batches", response_model=List[MigrationBatchOut], status_code=status.HTTP_200_OK)
async def get_migration_batches(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    logger.info(f"Migration batches requested for customer {current_user.customer_id}.")
    try:
        batches = crud_migration_batch.get_batches(db, skip=skip, limit=limit)
        return [MigrationBatchOut.model_validate(batch) for batch in batches]
    except Exception as e:
        logger.error(f"Failed to retrieve migration batches: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve migration batches: {e}")


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


@router.post("/import-ready", status_code=status.HTTP_200_OK)
async def import_ready_records(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Finds all records with status READY_FOR_IMPORT and migrates them
    into the main LG records table using the new migration service.
    """
    logger.info(f"Final import process started for customer {current_user.customer_id} by user {current_user.email}.")

    ready_records = crud_lg_migration.get_staging_records(
        db=db,
        customer_id=current_user.customer_id,
        status_filter=MigrationRecordStatusEnum.READY_FOR_IMPORT
    )

    imported_count = 0
    failed_count = 0
    failed_records_details = []

    # Use a set to track LG numbers imported in this batch to prevent duplicates within the same run.
    newly_imported_lg_numbers = set()

    for record in ready_records:
        lg_number = record.source_data_json.get("lg_number")
        
        # Check against the set of newly imported LG numbers in this batch
        if lg_number and lg_number in newly_imported_lg_numbers:
            logger.warning(f"LG number '{lg_number}' is a duplicate within the same batch. Skipping staged record {record.id}.")
            record.record_status = MigrationRecordStatusEnum.DUPLICATE
            record.validation_log = record.validation_log or {}
            record.validation_log['import_error'] = 'Duplicate found within the same import batch.'
            db.add(record)
            failed_count += 1
            failed_records_details.append({
                "record_id": record.id,
                "lg_number": lg_number,
                "status": "DUPLICATE_IN_BATCH",
                "message": "Duplicate found within the same import batch."
            })
            continue

        with db.begin_nested():
            try:
                if record.migration_type == MigrationTypeEnum.RECORD:
                    new_lg_record = await migration_service.migrate_record(
                        db=db,
                        staged_record=record,
                        user_id=current_user.user_id,
                        customer_id=current_user.customer_id,
                    )
                    
                    record.record_status = MigrationRecordStatusEnum.IMPORTED
                    record.production_lg_id = new_lg_record.id
                    db.add(record)
                    
                    newly_imported_lg_numbers.add(lg_number)

                    imported_count += 1
                    logger.info(f"Successfully imported LG {new_lg_record.lg_number} (ID: {new_lg_record.id}) from staged record {record.id}.")
                    
                elif record.migration_type == MigrationTypeEnum.INSTRUCTION:
                    new_instruction = await migration_service.migrate_instruction(
                        db=db,
                        staged_instruction=record,
                        user_id=current_user.user_id,
                        customer_id=current_user.customer_id,
                    )

                    record.record_status = MigrationRecordStatusEnum.IMPORTED
                    record.production_lg_id = new_instruction.lg_record_id
                    db.add(record)
                    
                    imported_count += 1
                    logger.info(f"Successfully imported instruction {new_instruction.serial_number} from staged record {record.id}.")

            except HTTPException as e:
                db.rollback()
                logger.warning(f"Migration service for record {record.id} raised HTTPException: {e.detail}")
                
                record.record_status = MigrationRecordStatusEnum.ERROR
                record.validation_log = record.validation_log or {}
                record.validation_log['import_error'] = e.detail
                db.add(record)
                
                failed_count += 1
                failed_records_details.append({"record_id": record.id, "error": e.detail})

            except Exception as e:
                db.rollback()
                logger.error(f"Failed to import staged record {record.id}: {e}", exc_info=True)
                
                record.record_status = MigrationRecordStatusEnum.ERROR
                record.validation_log = record.validation_log or {}
                record.validation_log['import_error'] = str(e)
                db.add(record)
                
                failed_count += 1
                failed_records_details.append({"record_id": record.id, "error": str(e)})

    db.commit()

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="MIGRATION_FINALIZED",
        entity_type="Customer",
        entity_id=current_user.customer_id,
        details={
            "imported_count": imported_count,
            "failed_count": failed_count,
            "failed_records": failed_records_details,
        },
        customer_id=current_user.customer_id,
    )

    return {
        "message": "Migration process completed.",
        "imported": imported_count,
        "failed": failed_count,
        "errors": [item.get('error') or item.get('message') for item in failed_records_details],
        "details": failed_records_details,
    }


# NEW ENDPOINT: Delete multiple staged records
class DeleteRecordsIn(BaseModel):
    ids: List[int]

@router.delete("/staged/{record_id}", status_code=status.HTTP_200_OK, response_model=Dict[str, str])
async def delete_staged_record(
    record_id: int,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Deletes a single staged record for the current customer.
    """
    logger.info(f"Delete staged record {record_id} endpoint hit for customer {current_user.customer_id}.")
    crud_lg_migration.delete_staging_record(
        db=db,
        record_id=record_id,
        customer_id=current_user.customer_id,
        user_id=current_user.user_id
    )
    db.commit()
    return {"message": f"Staged record {record_id} deleted successfully."}


@router.post("/staged/delete-multiple", status_code=status.HTTP_200_OK, response_model=Dict[str, int])
async def delete_multiple_staged_records(
    delete_in: DeleteRecordsIn,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Deletes multiple staged records for the current customer by their IDs.
    """
    logger.info(f"Delete multiple staged records endpoint hit for customer {current_user.customer_id}.")
    deleted_count = crud_lg_migration.delete_multiple_records(db, current_user.customer_id, delete_in.ids, current_user.user_id)
    db.commit()
    return {"deleted_count": deleted_count}

@router.post("/upload-unstructured", status_code=status.HTTP_201_CREATED, response_model=Dict[str, Any])
async def upload_unstructured_file_for_staging(
    file: UploadFile = File(...),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """
    Stub for uploading an unstructured file (e.g., PDF, image) for AI-powered data extraction and staging.
    """
    logger.info(f"Unstructured upload endpoint hit for customer {current_user.customer_id}. This feature is not yet fully implemented.")
    return {"message": "Unstructured upload not implemented yet."}


@router.get("/staged", status_code=status.HTTP_200_OK, response_model=List[LGMigrationStagingOut])
async def get_staged_records(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
    status_filter: Optional[MigrationRecordStatusEnum] = None,
    lg_number: Optional[str] = None,
    file_name: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
):
    logger.info(f"Get staged records endpoint hit for customer {current_user.customer_id}.")
    records = crud_lg_migration.get_staging_records(
        db=db,
        customer_id=current_user.customer_id,
        status_filter=status_filter,
        lg_number_filter=lg_number,
        file_name_filter=file_name,
        skip=skip,
        limit=limit
    )
    return records

@router.put("/staged/{record_id}", status_code=status.HTTP_200_OK, response_model=LGMigrationStagingOut)
async def update_staged_record(
    record_id: int,
    complete_record_data: Dict[str, Any],
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Updates a specific staged record for the current customer with complete record data and re-validates it.
    """
    logger.info(f"Update staged record {record_id} endpoint hit for customer {current_user.customer_id} with complete data: {complete_record_data}")
    record = crud_lg_migration.update_and_revalidate_staging_record(
        db=db,
        record_id=record_id,
        customer_id=current_user.customer_id,
        complete_record_data=complete_record_data,
        user_id=current_user.user_id
    )
    db.commit()
    return record

@router.post("/import/{record_id}", status_code=status.HTTP_201_CREATED, response_model=Dict[str, str])
async def import_staged_record(
    record_id: int,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """
    Stub for importing a staged record into the main LGRecords table.
    """
    logger.info(f"Import staged record {record_id} endpoint hit for customer {current_user.customer_id}. This feature is not yet fully implemented.")
    return {"message": f"Import of record {record_id} not implemented yet."}