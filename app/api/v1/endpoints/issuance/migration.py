# app/api/v1/endpoints/issuance/migration.py
"""
Issuance Migration Hub — Backend Endpoints
Handles migration of legacy LG records into the IssuedLGRecord table.
Supports: Excel/CSV upload, AI mass-scan upload, validation, bulk edit,
          historical reconstruction, and import.
"""

import json
import hashlib
import logging
import os
import io
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation

import pandas as pd
import numpy as np

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.orm.attributes import flag_modified
from pydantic import BaseModel

from app.database import get_db
from app.core.security import get_current_corporate_admin_context, TokenData
from app.crud.base import log_action
from app.models.models import LGMigrationStaging, MigrationBatch
from app.models.models_issuance import (
    IssuedLGRecord, IssuanceFacility, IssuanceFacilitySubLimit,
    IssuanceMaintenanceAction, IssuanceExposureEntry,
)
from app.models import Bank, Currency, LgType
from app.schemas.migration_schemas import (
    MigrationRecordStatusEnum, MigrationTypeEnum,
    LGMigrationStagingIn, LGMigrationStagingOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/migration", tags=["Issuance Migration"])

# ==============================================================================
# COLUMN MAPPING — Maps user-facing Excel headers to internal field names
# Comprehensive: covers ALL IssuedLGRecord fields relevant for migration
# ==============================================================================

ISSUANCE_COLUMN_MAPPING = {
    # === Core (Hard Gate) — English ===
    "lg number": "bank_lg_number",
    "bank lg number": "bank_lg_number",
    "lg no": "bank_lg_number",
    "lg #": "bank_lg_number",
    "guarantee number": "bank_lg_number",
    "beneficiary": "beneficiary_name",
    "beneficiary name": "beneficiary_name",
    "amount": "current_amount",
    "lg amount": "current_amount",
    "guarantee amount": "current_amount",
    "currency": "currency_code",
    "ccy": "currency_code",
    "bank": "bank_name",
    "bank name": "bank_name",
    "issuing bank": "bank_name",
    "status": "status",
    "lg status": "status",

    # === Soft Gate — English ===
    "issue date": "issue_date",
    "issuance date": "issue_date",
    "date of issue": "issue_date",
    "expiry date": "expiry_date",
    "maturity date": "expiry_date",
    "expiration date": "expiry_date",
    "lg type": "lg_type_name",
    "type": "lg_type_name",
    "guarantee type": "lg_type_name",
    "entity": "entity_name",
    "entity name": "entity_name",
    "issuing entity": "entity_name",
    "department": "department",
    "dept": "department",

    # === Ownership / Internal Owner ===
    "owner": "internal_owner_email",
    "internal owner": "internal_owner_email",
    "requestor": "internal_owner_email",
    "requestor email": "internal_owner_email",
    "owner email": "internal_owner_email",
    "assigned to": "internal_owner_email",
    "responsible": "internal_owner_email",

    # === Facility & Sub-Limit Linking ===
    "facility": "facility_name",
    "facility name": "facility_name",
    "sub limit": "sub_limit_name",
    "sub-limit": "sub_limit_name",
    "sublimit": "sub_limit_name",
    "sub limit name": "sub_limit_name",

    # === Reference / Contract ===
    "reference number": "reference_number",
    "ref number": "reference_number",
    "contract number": "reference_number",
    "po number": "reference_number",
    "reference type": "reference_type",
    "ref type": "reference_type",
    "contract type": "reference_type",
    "reference amount": "reference_amount",
    "contract amount": "reference_amount",
    "reference currency": "reference_currency_code",
    "contract currency": "reference_currency_code",
    "reference start date": "reference_start_date",
    "contract start date": "reference_start_date",
    "reference end date": "reference_end_date",
    "contract end date": "reference_end_date",
    "project": "project_name",
    "project name": "project_name",

    # === Operational Details ===
    "operational status": "operational_status",
    "operative status": "operational_status",
    "applicable rules": "applicable_rules",
    "rules": "applicable_rules",
    "urdg": "applicable_rules",
    "lg purpose": "lg_purpose",
    "purpose": "lg_purpose",
    "purpose of guarantee": "lg_purpose",
    "lg language": "lg_language",
    "language": "lg_language",
    "payable currency": "payable_currency_code",
    "payment currency": "payable_currency_code",
    "cross border": "is_cross_border",
    "third party": "is_third_party",

    # === Beneficiary Extended ===
    "beneficiary address": "beneficiary_address",
    "beneficiary country": "beneficiary_country",
    "country": "beneficiary_country",
    "beneficiary contact": "beneficiary_contact_person",
    "contact person": "beneficiary_contact_person",
    "beneficiary phone": "beneficiary_phone",
    "beneficiary email": "beneficiary_email",

    # === Auto-Reducing ===
    "auto reducing": "is_auto_reducing",
    "auto-reducing": "is_auto_reducing",
    "reduction trigger": "reduction_trigger",

    # === Notes ===
    "notes": "notes",
    "remarks": "notes",

    # === Core (Hard Gate) — Arabic ===
    "رقم خطاب الضمان": "bank_lg_number",
    "رقم الضمان": "bank_lg_number",
    "المستفيد": "beneficiary_name",
    "اسم المستفيد": "beneficiary_name",
    "المبلغ": "current_amount",
    "قيمة الضمان": "current_amount",
    "العملة": "currency_code",
    "البنك": "bank_name",
    "اسم البنك": "bank_name",
    "البنك المصدر": "bank_name",
    "الحالة": "status",

    # === Soft Gate — Arabic ===
    "تاريخ الإصدار": "issue_date",
    "تاريخ الانتهاء": "expiry_date",
    "تاريخ الاستحقاق": "expiry_date",
    "القسم": "department",
    "نوع الضمان": "lg_type_name",
    "الجهة": "entity_name",
    "رقم المرجع": "reference_number",
    "رقم العقد": "reference_number",
    "نوع المرجع": "reference_type",
    "قيمة العقد": "reference_amount",
    "المالك الداخلي": "internal_owner_email",
    "مقدم الطلب": "internal_owner_email",
    "الغرض": "lg_purpose",
    "الحالة التشغيلية": "operational_status",
    "القواعد المطبقة": "applicable_rules",
    "المنشأة": "facility_name",
    "الحد الفرعي": "sub_limit_name",
    "المشروع": "project_name",
    "ملاحظات": "notes",
}

# Fields required for import (Hard Gate)
HARD_GATE_FIELDS = {"bank_lg_number", "beneficiary_name", "current_amount", "currency_code", "bank_name", "status"}

# Fields recommended but not required (Soft Gate)
SOFT_GATE_FIELDS = {"issue_date", "expiry_date", "lg_type_name", "entity_name", "department", "reference_number", "internal_owner_email"}

# All known internal field names for issuance migration
ALL_ISSUANCE_FIELDS = [
    # Core (Hard Gate)
    "bank_lg_number", "beneficiary_name", "current_amount", "currency_code",
    "bank_name", "status",
    # Dates
    "issue_date", "expiry_date",
    # Classification
    "lg_type_name", "entity_name", "department",
    # Internal Owner (requestor)
    "internal_owner_email",
    # Facility & Sub-Limit
    "facility_name", "sub_limit_name",
    # Reference / Contract
    "reference_type", "reference_number", "reference_amount",
    "reference_currency_code", "reference_start_date", "reference_end_date",
    # Project
    "project_name",
    # Operational
    "operational_status", "applicable_rules", "lg_purpose", "lg_language",
    "payable_currency_code",
    # Flags
    "is_cross_border", "is_third_party",
    # Beneficiary Extended
    "beneficiary_address", "beneficiary_country",
    "beneficiary_contact_person", "beneficiary_phone", "beneficiary_email",
    # Auto-Reducing
    "is_auto_reducing", "reduction_trigger",
    # Notes
    "notes",
]

# Valid statuses for imported records
VALID_STATUSES = {
    "active": "ACTIVE", "operative": "ACTIVE", "فعال": "ACTIVE",
    "expired": "EXPIRED", "منتهي": "EXPIRED", "منتهية": "EXPIRED",
    "returned": "RETURNED", "مرتجع": "RETURNED",
    "cancelled": "CANCELLED", "canceled": "CANCELLED", "ملغي": "CANCELLED", "ملغى": "CANCELLED",
    "released": "RELEASED", "محرر": "RELEASED",
    "suspended": "SUSPENDED", "معلق": "SUSPENDED",
    "pending": "PENDING", "قيد الانتظار": "PENDING",
    "non-operative": "NON_OPERATIVE", "غير فعال": "NON_OPERATIVE",
    "closed": "RETURNED",
}

# Valid operational statuses
VALID_OPERATIONAL_STATUSES = {"OPERATIVE", "NON_OPERATIVE", "NON-OPERATIVE"}

# Valid applicable rules
VALID_APPLICABLE_RULES = {"URDG_758", "ISP_98", "LOCAL_LAW"}

# Valid reference types
VALID_REFERENCE_TYPES = {"CONTRACT", "PROJECT", "PURCHASE_ORDER", "TENDER", "OTHER"}


# ==============================================================================
# PYDANTIC MODELS for request/response
# ==============================================================================

class BulkEditRequest(BaseModel):
    ids: List[int]
    updates: Dict[str, Any]

class DeleteRecordsRequest(BaseModel):
    ids: List[int]

class RevalidateRequest(BaseModel):
    ids: List[int]

class HistoryPreviewRequest(BaseModel):
    bank_lg_number: str


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map Excel column headers to internal field names."""
    mapped_columns = {}
    for col in df.columns:
        normalized = col.strip().lower()
        if normalized in ISSUANCE_COLUMN_MAPPING:
            mapped_columns[col] = ISSUANCE_COLUMN_MAPPING[normalized]
        else:
            mapped_columns[col] = normalized.replace(" ", "_")
    df = df.rename(columns=mapped_columns)
    return df


def _clean_value(val):
    """Clean a cell value for JSON serialization."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, str):
        val = val.strip()
        if val.upper() in ("N/A", "NA", "NONE", "NULL", "-", ""):
            return None
        return val
    return val


def _parse_amount(val) -> Optional[float]:
    """Parse an amount value handling commas, currency symbols, etc."""
    if val is None:
        return None
    if isinstance(val, (int, float, Decimal)):
        return float(val)
    if isinstance(val, str):
        # Remove currency symbols, commas, spaces
        cleaned = val.strip().replace(",", "").replace(" ", "")
        for sym in ["$", "€", "£", "¥", "ر.س", "د.إ", "ج.م", "SAR", "AED", "EGP", "USD", "EUR", "GBP"]:
            cleaned = cleaned.replace(sym, "")
        cleaned = cleaned.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except (ValueError, InvalidOperation):
            return None
    return None


def _parse_date(val) -> Optional[str]:
    """Parse a date value to YYYY-MM-DD string."""
    if val is None:
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, str):
        val = val.strip()
        if not val or val.upper() in ("N/A", "NA", "NONE", "NULL"):
            return None
        # Try common date formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y/%m/%d"]:
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None
    return None


def _normalize_status(val) -> Optional[str]:
    """Normalize status string to valid enum value."""
    if val is None:
        return None
    normalized = str(val).strip().lower()
    return VALID_STATUSES.get(normalized, val.strip().upper())


def _parse_boolean(val) -> Optional[bool]:
    """Parse a boolean value from various representations."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("yes", "true", "1", "y", "نعم"):
        return True
    if s in ("no", "false", "0", "n", "لا"):
        return False
    return None


def _validate_issuance_record(record_data: Dict[str, Any], db: Session, customer_id: int) -> Dict[str, Any]:
    """
    Validate a single issuance migration record.
    Returns a validation log dict (empty = valid).
    Uses two-gate validation: hard gate (required) + soft gate (recommended).
    """
    errors = {}
    warnings = {}

    # --- HARD GATE: Required fields ---
    for field in HARD_GATE_FIELDS:
        val = record_data.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            errors[field] = {
                "status": "ERROR",
                "message": f"Required field '{field}' is missing or empty.",
                "value": val
            }

    # Validate amount is numeric
    amount = record_data.get("current_amount")
    if amount is not None:
        parsed = _parse_amount(amount)
        if parsed is None:
            errors["current_amount"] = {
                "status": "ERROR",
                "message": f"Amount '{amount}' is not a valid number.",
                "value": amount
            }
        elif parsed <= 0:
            errors["current_amount"] = {
                "status": "ERROR",
                "message": f"Amount must be positive. Got: {parsed}",
                "value": amount
            }

    # Validate bank exists
    bank_name = record_data.get("bank_name")
    if bank_name and "bank_name" not in errors:
        bank = db.query(Bank).filter(
            func.lower(Bank.name).like(func.lower(f"%{bank_name.strip()}%")),
        ).first()
        if not bank:
            errors["bank_name"] = {
                "status": "ERROR",
                "message": f"Bank '{bank_name}' not found in the system.",
                "value": bank_name
            }

    # Validate currency exists
    currency_code = record_data.get("currency_code")
    if currency_code and "currency_code" not in errors:
        currency = db.query(Currency).filter(
            func.lower(Currency.code) == func.lower(currency_code.strip())
        ).first()
        if not currency:
            errors["currency_code"] = {
                "status": "ERROR",
                "message": f"Currency '{currency_code}' not found in the system.",
                "value": currency_code
            }

    # --- SOFT GATE: Recommended fields ---
    for field in SOFT_GATE_FIELDS:
        val = record_data.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            warnings[field] = {
                "status": "WARNING",
                "message": f"Recommended field '{field}' is missing. Record can still be imported but may need manual update.",
                "value": val
            }

    # Validate dates if provided
    for date_field in ["issue_date", "expiry_date", "reference_start_date", "reference_end_date"]:
        date_val = record_data.get(date_field)
        if date_val is not None:
            parsed = _parse_date(date_val)
            if parsed is None:
                errors[date_field] = {
                    "status": "ERROR",
                    "message": f"Date '{date_val}' could not be parsed. Use YYYY-MM-DD format.",
                    "value": date_val
                }

    # Validate internal owner email — must be a registered user
    owner_email = record_data.get("internal_owner_email")
    if owner_email and isinstance(owner_email, str) and owner_email.strip():
        from app.models import User
        user = db.query(User).filter(
            func.lower(User.email) == func.lower(owner_email.strip()),
            User.customer_id == customer_id,
            User.is_deleted == False,
        ).first()
        if not user:
            errors["internal_owner_email"] = {
                "status": "ERROR",
                "message": f"User '{owner_email}' is not a registered user. Please add them first.",
                "value": owner_email
            }

    # Validate reference amount if provided
    ref_amount = record_data.get("reference_amount")
    if ref_amount is not None:
        parsed = _parse_amount(ref_amount)
        if parsed is None:
            warnings["reference_amount"] = {
                "status": "WARNING",
                "message": f"Reference amount '{ref_amount}' could not be parsed as a number.",
                "value": ref_amount
            }

    # Validate reference currency if provided
    ref_currency = record_data.get("reference_currency_code")
    if ref_currency and isinstance(ref_currency, str) and ref_currency.strip():
        currency = db.query(Currency).filter(
            func.lower(Currency.code) == func.lower(ref_currency.strip())
        ).first()
        if not currency:
            warnings["reference_currency_code"] = {
                "status": "WARNING",
                "message": f"Reference currency '{ref_currency}' not found in the system.",
                "value": ref_currency
            }

    # Validate payable currency if provided
    pay_currency = record_data.get("payable_currency_code")
    if pay_currency and isinstance(pay_currency, str) and pay_currency.strip():
        currency = db.query(Currency).filter(
            func.lower(Currency.code) == func.lower(pay_currency.strip())
        ).first()
        if not currency:
            warnings["payable_currency_code"] = {
                "status": "WARNING",
                "message": f"Payable currency '{pay_currency}' not found in the system.",
                "value": pay_currency
            }

    # Validate reference type if provided
    ref_type = record_data.get("reference_type")
    if ref_type and isinstance(ref_type, str) and ref_type.strip():
        normalized_ref = ref_type.strip().upper().replace(" ", "_")
        if normalized_ref not in VALID_REFERENCE_TYPES:
            warnings["reference_type"] = {
                "status": "WARNING",
                "message": f"Reference type '{ref_type}' is non-standard. Expected: {', '.join(VALID_REFERENCE_TYPES)}.",
                "value": ref_type
            }

    # Validate operational status if provided
    op_status = record_data.get("operational_status")
    if op_status and isinstance(op_status, str) and op_status.strip():
        normalized_op = op_status.strip().upper().replace("-", "_")
        if normalized_op not in {"OPERATIVE", "NON_OPERATIVE"}:
            warnings["operational_status"] = {
                "status": "WARNING",
                "message": f"Operational status '{op_status}' is non-standard. Expected: OPERATIVE or NON_OPERATIVE.",
                "value": op_status
            }

    # Validate applicable rules if provided
    rules = record_data.get("applicable_rules")
    if rules and isinstance(rules, str) and rules.strip():
        normalized_rules = rules.strip().upper().replace(" ", "_")
        if normalized_rules not in VALID_APPLICABLE_RULES:
            warnings["applicable_rules"] = {
                "status": "WARNING",
                "message": f"Applicable rules '{rules}' is non-standard. Expected: {', '.join(VALID_APPLICABLE_RULES)}.",
                "value": rules
            }

    # Validate facility/sub-limit names if provided
    facility_name = record_data.get("facility_name")
    if facility_name and isinstance(facility_name, str) and facility_name.strip():
        facility = db.query(IssuanceFacility).filter(
            IssuanceFacility.customer_id == customer_id,
            func.lower(IssuanceFacility.facility_name).like(func.lower(f"%{facility_name.strip()}%")),
            IssuanceFacility.is_deleted == False,
        ).first()
        if not facility:
            warnings["facility_name"] = {
                "status": "WARNING",
                "message": f"Facility '{facility_name}' not found. You can link it after import.",
                "value": facility_name
            }

    sub_limit_name = record_data.get("sub_limit_name")
    if sub_limit_name and isinstance(sub_limit_name, str) and sub_limit_name.strip():
        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            func.lower(IssuanceFacilitySubLimit.limit_name).like(func.lower(f"%{sub_limit_name.strip()}%")),
        ).first()
        if not sub_limit:
            warnings["sub_limit_name"] = {
                "status": "WARNING",
                "message": f"Sub-limit '{sub_limit_name}' not found. You can link it after import.",
                "value": sub_limit_name
            }

    # Combine: errors take priority over warnings
    validation_log = {}
    for field in ALL_ISSUANCE_FIELDS:
        if field in errors:
            validation_log[field] = errors[field]
        elif field in warnings:
            validation_log[field] = warnings[field]
        else:
            val = record_data.get(field)
            validation_log[field] = {
                "status": "Valid",
                "message": "Field is valid.",
                "value": val if val is not None else "N/A"
            }

    return {"errors": errors, "warnings": warnings, "validation_log": validation_log}


def _determine_record_status(
    validation_result: Dict,
    record_data: Dict,
    is_duplicate: bool = False,
    is_conflict: bool = False,
) -> MigrationRecordStatusEnum:
    """Determine the final status of a staged record."""
    if is_conflict:
        return MigrationRecordStatusEnum.CONFLICT
    if is_duplicate:
        return MigrationRecordStatusEnum.DUPLICATE

    has_errors = bool(validation_result.get("errors"))

    # Check if expired
    expiry_str = record_data.get("expiry_date")
    if expiry_str:
        parsed = _parse_date(expiry_str)
        if parsed:
            try:
                expiry = datetime.strptime(parsed, "%Y-%m-%d").date()
                if expiry < date.today():
                    return MigrationRecordStatusEnum.EXPIRED
            except (ValueError, TypeError):
                pass

    if has_errors:
        return MigrationRecordStatusEnum.ERROR

    has_warnings = bool(validation_result.get("warnings"))
    if has_warnings:
        return MigrationRecordStatusEnum.NEEDS_REVIEW

    return MigrationRecordStatusEnum.READY_FOR_IMPORT


# ==============================================================================
# ENDPOINTS
# ==============================================================================

@router.post("/upload", status_code=status.HTTP_201_CREATED)
async def upload_issuance_migration_file(
    file: UploadFile = File(...),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Upload an Excel/CSV file for issuance migration.
    Parses columns, maps to internal fields, validates, and stages records.
    """
    logger.info(f"Issuance migration upload by user {current_user.email} for customer {current_user.customer_id}")

    # Validate file type
    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("xlsx", "xls", "csv"):
        raise HTTPException(status_code=400, detail="Only Excel (.xlsx, .xls) and CSV (.csv) files are supported.")

    # Read file content
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # Calculate hash for dedup
    file_hash = hashlib.sha256(content).hexdigest()

    # Parse file
    try:
        if ext == "csv":
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=400, detail="File contains no data rows.")

    # Remove completely empty rows
    df = df.dropna(how="all").reset_index(drop=True)
    if df.empty:
        raise HTTPException(status_code=400, detail="File contains no valid data rows after cleanup.")

    # Map columns
    df = _map_columns(df)

    # Check we have at least some recognized columns
    recognized = set(df.columns) & set(ALL_ISSUANCE_FIELDS)
    if not recognized:
        raise HTTPException(
            status_code=400,
            detail=f"No recognized columns found. Expected columns like: LG Number, Beneficiary, Amount, Currency, Bank, Status. Found: {list(df.columns)}"
        )

    # Get existing LG numbers for dedup
    existing_lg_numbers_in_staging = set()
    existing_staging = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
        LGMigrationStaging.record_status.notin_([
            MigrationRecordStatusEnum.IMPORTED,
            MigrationRecordStatusEnum.REJECTED,
        ])
    ).all()
    for rec in existing_staging:
        lg_num = (rec.source_data_json or {}).get("bank_lg_number", "")
        if lg_num:
            existing_lg_numbers_in_staging.add(str(lg_num).strip().lower())

    existing_lg_numbers_in_prod = set()
    existing_prod = db.query(IssuedLGRecord.bank_lg_number).filter(
        IssuedLGRecord.customer_id == current_user.customer_id
    ).all()
    for (lg_num,) in existing_prod:
        if lg_num:
            existing_lg_numbers_in_prod.add(str(lg_num).strip().lower())

    # Process each row
    results = {"staged": 0, "duplicates": 0, "errors": 0, "warnings": 0, "records": []}
    seen_in_batch = {}

    for idx, row in df.iterrows():
        record_data = {}
        for field in ALL_ISSUANCE_FIELDS:
            if field in df.columns:
                record_data[field] = _clean_value(row.get(field))

        # Clean specific fields
        if "current_amount" in record_data:
            record_data["current_amount"] = _parse_amount(record_data["current_amount"])
        if "reference_amount" in record_data:
            record_data["reference_amount"] = _parse_amount(record_data["reference_amount"])
        for date_f in ["issue_date", "expiry_date", "reference_start_date", "reference_end_date"]:
            if date_f in record_data:
                record_data[date_f] = _parse_date(record_data[date_f])
        if "status" in record_data and record_data["status"]:
            record_data["status"] = _normalize_status(record_data["status"])
        # Normalize operational status
        if "operational_status" in record_data and record_data["operational_status"]:
            record_data["operational_status"] = str(record_data["operational_status"]).strip().upper().replace("-", "_")
        # Normalize applicable rules
        if "applicable_rules" in record_data and record_data["applicable_rules"]:
            record_data["applicable_rules"] = str(record_data["applicable_rules"]).strip().upper().replace(" ", "_")
        # Normalize reference type
        if "reference_type" in record_data and record_data["reference_type"]:
            record_data["reference_type"] = str(record_data["reference_type"]).strip().upper().replace(" ", "_")
        # Parse boolean fields
        for bool_f in ["is_cross_border", "is_third_party", "is_auto_reducing"]:
            if bool_f in record_data:
                record_data[bool_f] = _parse_boolean(record_data[bool_f])

        # Add source metadata
        record_data["_source_file"] = filename
        record_data["_source_type"] = "EXCEL"
        record_data["_source_row"] = int(idx) + 2  # +2 for header row + 0-index

        # Duplicate detection
        lg_num = str(record_data.get("bank_lg_number", "") or "").strip().lower()
        is_duplicate = False
        is_conflict = False

        if lg_num:
            # Check production
            if lg_num in existing_lg_numbers_in_prod:
                is_duplicate = True
            # Check staging
            elif lg_num in existing_lg_numbers_in_staging:
                # Check if data differs → conflict
                existing_rec = None
                for rec in existing_staging:
                    if str((rec.source_data_json or {}).get("bank_lg_number", "")).strip().lower() == lg_num:
                        existing_rec = rec
                        break
                if existing_rec:
                    ex_data = existing_rec.source_data_json or {}
                    ex_source = ex_data.get("_source_type", "")
                    # If from different source, check for data conflict
                    if ex_source != "EXCEL":
                        ex_amount = ex_data.get("current_amount")
                        new_amount = record_data.get("current_amount")
                        if ex_amount != new_amount:
                            is_conflict = True
                            record_data["_conflict_with_id"] = existing_rec.id
                            record_data["_conflict_details"] = {
                                "source_a": {"file": ex_data.get("_source_file"), "amount": ex_amount},
                                "source_b": {"file": filename, "amount": new_amount}
                            }
                        else:
                            is_duplicate = True
                    else:
                        is_duplicate = True
            # Check within this batch
            elif lg_num in seen_in_batch:
                is_duplicate = True

            seen_in_batch[lg_num] = True

        # Validate
        validation_result = _validate_issuance_record(record_data, db, current_user.customer_id)
        record_status = _determine_record_status(validation_result, record_data, is_duplicate, is_conflict)

        # Stage the record
        staging_obj = LGMigrationStaging(
            file_name=filename,
            source_data_json=record_data,
            record_status=record_status,
            validation_log=validation_result["validation_log"],
            customer_id=current_user.customer_id,
            migration_type=MigrationTypeEnum.ISSUANCE_RECORD,
            file_content_hash=file_hash,
        )
        db.add(staging_obj)
        db.flush()

        if record_status == MigrationRecordStatusEnum.DUPLICATE:
            results["duplicates"] += 1
        elif record_status == MigrationRecordStatusEnum.ERROR:
            results["errors"] += 1
        elif record_status == MigrationRecordStatusEnum.NEEDS_REVIEW:
            results["warnings"] += 1
        results["staged"] += 1

        results["records"].append({
            "id": staging_obj.id,
            "row": int(idx) + 2,
            "lg_number": record_data.get("bank_lg_number"),
            "status": record_status.value,
        })

    db.commit()

    log_action(
        db, user_id=current_user.user_id,
        action_type="ISSUANCE_MIGRATION_UPLOAD",
        entity_type="LGMigrationStaging",
        entity_id=None,
        details={"file_name": filename, "total_rows": results["staged"], "errors": results["errors"], "duplicates": results["duplicates"]},
        customer_id=current_user.customer_id,
    )

    return {
        "message": f"Successfully processed {results['staged']} records from '{filename}'.",
        "summary": {
            "total": results["staged"],
            "ready": results["staged"] - results["errors"] - results["duplicates"] - results["warnings"],
            "errors": results["errors"],
            "duplicates": results["duplicates"],
            "needs_review": results["warnings"],
        },
        "records": results["records"],
        "mapped_columns": list(recognized),
    }


@router.post("/upload-scan", status_code=status.HTTP_201_CREATED)
async def upload_scan_for_ai_extraction(
    files: List[UploadFile] = File(...),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Upload scanned LG documents (PDF/images) for AI-powered data extraction.
    Each file is processed through Document AI + Gemini to extract structured data.
    """
    logger.info(f"Issuance migration scan upload: {len(files)} files by user {current_user.email}")

    results = {"processed": 0, "staged": 0, "errors": [], "records": []}

    for uploaded_file in files:
        fname = uploaded_file.filename or "unknown"
        try:
            content = await uploaded_file.read()
            if not content:
                results["errors"].append({"file": fname, "error": "Empty file"})
                continue

            # Save the original scan to uploads directory
            upload_dir = os.path.join("uploads", "issuance", "migration", str(current_user.customer_id))
            os.makedirs(upload_dir, exist_ok=True)
            scan_path = os.path.join(upload_dir, fname)
            with open(scan_path, "wb") as f:
                f.write(content)

            # Call AI extraction
            extracted_data = await _extract_data_from_scan(content, fname)

            if not extracted_data:
                results["errors"].append({"file": fname, "error": "AI extraction returned no data"})
                continue

            # Process each extracted record (a single PDF might have multiple LGs)
            records_from_scan = extracted_data if isinstance(extracted_data, list) else [extracted_data]

            for rec_data in records_from_scan:
                # Add source metadata
                rec_data["_source_file"] = fname
                rec_data["_source_type"] = "SCAN"
                rec_data["_scan_file_path"] = scan_path

                # Clean fields
                if "current_amount" in rec_data:
                    rec_data["current_amount"] = _parse_amount(rec_data["current_amount"])
                if "reference_amount" in rec_data:
                    rec_data["reference_amount"] = _parse_amount(rec_data["reference_amount"])
                for date_f in ["issue_date", "expiry_date"]:
                    if date_f in rec_data:
                        rec_data[date_f] = _parse_date(rec_data[date_f])
                if "status" in rec_data and rec_data["status"]:
                    rec_data["status"] = _normalize_status(rec_data["status"])

                # Validate
                validation_result = _validate_issuance_record(rec_data, db, current_user.customer_id)
                record_status = _determine_record_status(validation_result, rec_data)

                staging_obj = LGMigrationStaging(
                    file_name=fname,
                    source_data_json=rec_data,
                    record_status=record_status,
                    validation_log=validation_result["validation_log"],
                    customer_id=current_user.customer_id,
                    migration_type=MigrationTypeEnum.ISSUANCE_RECORD,
                )
                db.add(staging_obj)
                db.flush()

                results["staged"] += 1
                results["records"].append({
                    "id": staging_obj.id,
                    "file": fname,
                    "lg_number": rec_data.get("bank_lg_number"),
                    "status": record_status.value,
                })

            results["processed"] += 1

        except Exception as e:
            logger.error(f"Failed to process scan file '{fname}': {e}", exc_info=True)
            results["errors"].append({"file": fname, "error": str(e)})

    db.commit()

    return {
        "message": f"Processed {results['processed']} files, staged {results['staged']} records.",
        "summary": results,
    }


async def _extract_data_from_scan(content: bytes, filename: str) -> Optional[Any]:
    """Extract structured data from a scanned document using AI."""
    try:
        from app.core.ai_integration import gemini_service

        extraction_prompt = """You are a data extraction specialist. Extract LG (Letter of Guarantee) information from this document.

Return a JSON object (or array of objects if multiple LGs are found) with these fields:

CORE FIELDS:
- bank_lg_number: The LG reference number assigned by the bank
- beneficiary_name: The beneficiary of the guarantee
- current_amount: The guarantee amount (numeric only, no currency symbols)
- currency_code: The 3-letter currency code (e.g., SAR, USD, AED, EGP)
- bank_name: The issuing bank name
- status: The LG status (ACTIVE, EXPIRED, CANCELLED, RETURNED, RELEASED, SUSPENDED, PENDING)
- issue_date: The issue date in YYYY-MM-DD format
- expiry_date: The expiry date in YYYY-MM-DD format

CLASSIFICATION:
- lg_type_name: The type of guarantee (e.g., Bid Bond, Performance Bond, Advance Payment, Payment Guarantee)
- entity_name: The issuing entity/company name
- department: Department that requested the LG

REFERENCE / CONTRACT:
- reference_type: Type of reference (CONTRACT, PROJECT, PURCHASE_ORDER, TENDER, OTHER)
- reference_number: Any reference/contract/PO number mentioned
- reference_amount: The underlying contract/reference amount (numeric only)
- reference_currency_code: Currency of the reference amount
- reference_start_date: Contract start date in YYYY-MM-DD format
- reference_end_date: Contract end date in YYYY-MM-DD format
- project_name: Associated project name if mentioned

OPERATIONAL:
- operational_status: Whether LG is OPERATIVE or NON_OPERATIVE
- applicable_rules: Governing rules (URDG_758, ISP_98, or LOCAL_LAW)
- lg_purpose: Description of the guarantee purpose
- lg_language: Language of the LG (AR or EN)
- payable_currency_code: Payable currency if different from LG currency

BENEFICIARY DETAILS:
- beneficiary_address: The beneficiary's address
- beneficiary_country: The beneficiary's country
- beneficiary_contact_person: Beneficiary's contact person name
- beneficiary_phone: Beneficiary's phone number
- beneficiary_email: Beneficiary's email address

FLAGS:
- is_cross_border: Whether the LG is cross-border (true/false)
- is_third_party: Whether this is a third-party issuance (true/false)
- is_auto_reducing: Whether LG auto-reduces on milestones (true/false)
- reduction_trigger: Description of reduction trigger condition if auto-reducing

OTHER:
- notes: Any additional relevant notes

If a field is not found in the document, set it to null.
Return ONLY the JSON, no markdown formatting or explanation."""

        # Use the existing Gemini service to process the document
        import base64
        encoded = base64.b64encode(content).decode("utf-8")

        # Determine MIME type
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        mime_types = {
            "pdf": "application/pdf",
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "tiff": "image/tiff",
            "tif": "image/tiff",
        }
        mime_type = mime_types.get(ext, "application/octet-stream")

        result = await gemini_service.generate_with_image(
            prompt=extraction_prompt,
            image_data=encoded,
            mime_type=mime_type,
        )

        if result:
            # Parse the JSON response
            # Clean potential markdown formatting
            text = result.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()

            return json.loads(text)

    except ImportError:
        logger.warning("Gemini service not available. Scan extraction requires AI integration.")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse AI extraction result: {e}")
        return None
    except Exception as e:
        logger.error(f"AI extraction failed: {e}", exc_info=True)
        return None


# ==============================================================================
# STAGED RECORDS CRUD
# ==============================================================================

@router.get("/staged", status_code=status.HTTP_200_OK)
async def get_staged_issuance_records(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = None,
    search: Optional[str] = None,
    file_name: Optional[str] = None,
    skip: int = 0,
    limit: int = 200,
):
    """List staged issuance migration records with optional filters."""
    query = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    )

    if status_filter:
        try:
            status_enum = MigrationRecordStatusEnum(status_filter)
            query = query.filter(LGMigrationStaging.record_status == status_enum)
        except ValueError:
            pass

    if search:
        query = query.filter(
            func.lower(LGMigrationStaging.source_data_json['bank_lg_number'].astext).like(func.lower(f"%{search}%"))
        )

    if file_name:
        query = query.filter(
            func.lower(LGMigrationStaging.file_name).like(func.lower(f"%{file_name}%"))
        )

    total = query.count()
    records = query.order_by(LGMigrationStaging.created_at.desc()).offset(skip).limit(limit).all()

    return {
        "total": total,
        "records": [
            {
                "id": r.id,
                "file_name": r.file_name,
                "record_status": r.record_status.value if r.record_status else None,
                "validation_log": r.validation_log,
                "source_data_json": r.source_data_json,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
            for r in records
        ],
    }


@router.put("/staged/{record_id}", status_code=status.HTTP_200_OK)
async def update_staged_issuance_record(
    record_id: int,
    updated_data: Dict[str, Any],
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Update a staged record and re-validate it."""
    record = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.id == record_id,
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    ).first()

    if not record:
        raise HTTPException(status_code=404, detail="Record not found.")

    # Merge updates into existing data
    existing_data = record.source_data_json or {}
    for key, val in updated_data.items():
        if key.startswith("_"):
            continue  # Don't allow modifying internal fields
        existing_data[key] = _clean_value(val)

    # Re-clean specific fields
    if "current_amount" in updated_data:
        existing_data["current_amount"] = _parse_amount(existing_data.get("current_amount"))
    if "reference_amount" in updated_data:
        existing_data["reference_amount"] = _parse_amount(existing_data.get("reference_amount"))
    for date_f in ["issue_date", "expiry_date"]:
        if date_f in updated_data:
            existing_data[date_f] = _parse_date(existing_data.get(date_f))
    if "status" in updated_data and existing_data.get("status"):
        existing_data["status"] = _normalize_status(existing_data["status"])

    record.source_data_json = existing_data
    flag_modified(record, "source_data_json")

    # Re-validate
    validation_result = _validate_issuance_record(existing_data, db, current_user.customer_id)
    record.record_status = _determine_record_status(validation_result, existing_data)
    record.validation_log = validation_result["validation_log"]

    db.add(record)
    db.commit()
    db.refresh(record)

    log_action(db, user_id=current_user.user_id, action_type="ISSUANCE_MIGRATION_UPDATE",
               entity_type="LGMigrationStaging", entity_id=record.id,
               details={"updated_fields": list(updated_data.keys()), "new_status": record.record_status.value},
               customer_id=current_user.customer_id)

    return {
        "id": record.id,
        "record_status": record.record_status.value,
        "validation_log": record.validation_log,
        "source_data_json": record.source_data_json,
    }


@router.delete("/staged/{record_id}", status_code=status.HTTP_200_OK)
async def delete_staged_issuance_record(
    record_id: int,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Delete a single staged issuance record."""
    record = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.id == record_id,
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    ).first()

    if not record:
        raise HTTPException(status_code=404, detail="Record not found.")

    db.delete(record)
    db.commit()

    return {"message": f"Record {record_id} deleted."}


@router.post("/staged/delete-multiple", status_code=status.HTTP_200_OK)
async def delete_multiple_staged_issuance_records(
    body: DeleteRecordsRequest,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Bulk delete staged issuance records."""
    deleted = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.id.in_(body.ids),
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
        LGMigrationStaging.production_lg_id.is_(None),  # Don't delete imported records
    ).delete(synchronize_session=False)

    db.commit()
    return {"deleted_count": deleted}


@router.post("/staged/re-validate-multiple", status_code=status.HTTP_200_OK)
async def revalidate_multiple_issuance_records(
    body: RevalidateRequest,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Bulk re-validate staged issuance records."""
    records = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.id.in_(body.ids),
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    ).all()

    updated = []
    for record in records:
        data = record.source_data_json or {}
        validation_result = _validate_issuance_record(data, db, current_user.customer_id)
        record.record_status = _determine_record_status(validation_result, data)
        record.validation_log = validation_result["validation_log"]
        db.add(record)
        updated.append({"id": record.id, "new_status": record.record_status.value})

    db.commit()
    return {"revalidated": len(updated), "records": updated}


@router.post("/staged/bulk-edit", status_code=status.HTTP_200_OK)
async def bulk_edit_staged_issuance_records(
    body: BulkEditRequest,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Apply the same field updates to multiple staged records.
    After applying, each record is re-validated.
    """
    records = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.id.in_(body.ids),
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    ).all()

    if not records:
        raise HTTPException(status_code=404, detail="No matching records found.")

    updated = []
    for record in records:
        data = record.source_data_json or {}
        for key, val in body.updates.items():
            if key.startswith("_"):
                continue
            data[key] = _clean_value(val)

        record.source_data_json = data
        flag_modified(record, "source_data_json")

        # Re-validate
        validation_result = _validate_issuance_record(data, db, current_user.customer_id)
        record.record_status = _determine_record_status(validation_result, data)
        record.validation_log = validation_result["validation_log"]
        db.add(record)

        updated.append({"id": record.id, "new_status": record.record_status.value})

    db.commit()

    log_action(db, user_id=current_user.user_id, action_type="ISSUANCE_MIGRATION_BULK_EDIT",
               entity_type="LGMigrationStaging", entity_id=None,
               details={"updated_count": len(updated), "fields": list(body.updates.keys())},
               customer_id=current_user.customer_id)

    return {"updated_count": len(updated), "records": updated}


# ==============================================================================
# HISTORICAL RECONSTRUCTION
# ==============================================================================

@router.post("/preview-history", status_code=status.HTTP_200_OK)
async def preview_history(
    body: HistoryPreviewRequest,
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Preview the timeline for a specific LG number before historical import."""
    records = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
        func.lower(LGMigrationStaging.source_data_json['bank_lg_number'].astext) == func.lower(body.bank_lg_number),
    ).order_by(LGMigrationStaging.created_at.asc()).all()

    if not records:
        raise HTTPException(status_code=404, detail="No records found for this LG number.")

    # Sort by history_timestamp or sequence if available
    def sort_key(r):
        data = r.source_data_json or {}
        ts = data.get("_history_timestamp") or data.get("issue_date") or "9999-12-31"
        return ts

    sorted_records = sorted(records, key=sort_key)

    timeline = []
    for i, rec in enumerate(sorted_records):
        data = rec.source_data_json or {}
        entry = {
            "sequence": i + 1,
            "record_id": rec.id,
            "timestamp": data.get("_history_timestamp") or data.get("issue_date"),
            "bank_lg_number": data.get("bank_lg_number"),
            "current_amount": data.get("current_amount"),
            "status": data.get("status"),
            "expiry_date": data.get("expiry_date"),
            "source_file": data.get("_source_file"),
        }

        # Calculate diff from previous
        if i > 0:
            prev_data = sorted_records[i - 1].source_data_json or {}
            diff = {}
            for field in ["current_amount", "status", "expiry_date", "beneficiary_name"]:
                old_val = prev_data.get(field)
                new_val = data.get(field)
                if old_val != new_val:
                    diff[field] = {"old": old_val, "new": new_val}
            entry["diff"] = diff

            # Infer action type from diff
            if "current_amount" in diff:
                old_amt = _parse_amount(diff["current_amount"]["old"]) or 0
                new_amt = _parse_amount(diff["current_amount"]["new"]) or 0
                entry["inferred_action"] = "INCREASE_AMOUNT" if new_amt > old_amt else "DECREASE_AMOUNT"
            elif "expiry_date" in diff:
                entry["inferred_action"] = "EXTENSION"
            elif "status" in diff:
                entry["inferred_action"] = "STATUS_CHANGE"
            else:
                entry["inferred_action"] = "AMENDMENT"
        else:
            entry["inferred_action"] = "INITIAL_RECORD"

        timeline.append(entry)

    return {"bank_lg_number": body.bank_lg_number, "total_snapshots": len(timeline), "timeline": timeline}


# ==============================================================================
# IMPORT INTO IssuedLGRecord
# ==============================================================================

@router.post("/import-ready", status_code=status.HTTP_200_OK)
async def import_ready_issuance_records(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """
    Import all READY_FOR_IMPORT and NEEDS_REVIEW and EXPIRED issuance records
    into the IssuedLGRecord table.
    """
    logger.info(f"Issuance migration import started by {current_user.email}")

    importable_statuses = [
        MigrationRecordStatusEnum.READY_FOR_IMPORT,
        MigrationRecordStatusEnum.NEEDS_REVIEW,
        MigrationRecordStatusEnum.EXPIRED,
    ]

    records = db.query(LGMigrationStaging).filter(
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
        LGMigrationStaging.record_status.in_(importable_statuses),
    ).all()

    if not records:
        return {"message": "No records ready for import.", "imported": 0, "failed": 0}

    # Group by bank_lg_number for historical reconstruction
    lg_groups = {}
    for record in records:
        lg_num = (record.source_data_json or {}).get("bank_lg_number", f"_unknown_{record.id}")
        if lg_num not in lg_groups:
            lg_groups[lg_num] = []
        lg_groups[lg_num].append(record)

    imported = 0
    failed = 0
    failed_details = []
    newly_imported = set()

    for lg_num, group_records in lg_groups.items():
        # Sort by timestamp for historical reconstruction
        def sort_key(r):
            data = r.source_data_json or {}
            return data.get("_history_timestamp") or data.get("issue_date") or "9999-12-31"
        sorted_group = sorted(group_records, key=sort_key)

        # Skip if already imported in this batch
        if lg_num.lower() in newly_imported:
            for rec in sorted_group:
                rec.record_status = MigrationRecordStatusEnum.DUPLICATE
                rec.validation_log = rec.validation_log or {}
                rec.validation_log["_import_error"] = "Duplicate within same import batch."
                db.add(rec)
            failed += 1
            continue

        with db.begin_nested():
            try:
                first = sorted_group[0]
                data = first.source_data_json or {}

                # Resolve foreign keys
                bank_id = _resolve_bank(db, data.get("bank_name"))
                currency_id = _resolve_currency(db, data.get("currency_code"))
                entity_id = _resolve_entity(db, current_user.customer_id, data.get("entity_name"))
                lg_type_id = _resolve_lg_type(db, current_user.customer_id, data.get("lg_type_name"))
                facility_sub_limit_id = _resolve_facility(db, current_user.customer_id, data.get("facility_name"), data.get("sub_limit_name"))

                # Parse values
                amount = _parse_amount(data.get("current_amount")) or 0
                issue_date = _safe_parse_date(data.get("issue_date"))
                expiry_date = _safe_parse_date(data.get("expiry_date"))
                status_val = data.get("status", "ACTIVE")

                # Generate migration ref number
                seq = db.query(func.count(IssuedLGRecord.id)).filter(
                    IssuedLGRecord.customer_id == current_user.customer_id
                ).scalar() or 0
                lg_ref = f"MIG-{current_user.customer_id}-{seq + 1:04d}"

                # Build action history
                action_history = [{
                    "action_type": "MIGRATION",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_file": data.get("_source_file"),
                    "source_type": data.get("_source_type", "UNKNOWN"),
                    "migrated_by": current_user.email,
                }]

                # Create the IssuedLGRecord
                new_lg = IssuedLGRecord(
                    customer_id=current_user.customer_id,
                    lg_ref_number=lg_ref,
                    bank_lg_number=data.get("bank_lg_number"),
                    beneficiary_name=data.get("beneficiary_name"),
                    current_amount=amount,
                    original_amount=amount,
                    currency_id=currency_id,
                    bank_id=bank_id,
                    status=status_val,
                    issue_date=issue_date,
                    expiry_date=expiry_date,
                    lg_type_id=lg_type_id,
                    issuing_entity_id=entity_id,
                    department=data.get("department"),
                    sub_limit_id=facility_sub_limit_id,
                    reference_number=data.get("reference_number"),
                    reference_amount=_parse_amount(data.get("reference_amount")),
                    beneficiary_address=data.get("beneficiary_address"),
                    beneficiary_country=data.get("beneficiary_country"),
                    notes=data.get("notes"),
                    action_history=action_history,
                    soft_copy_path=data.get("_scan_file_path"),
                )
                db.add(new_lg)
                db.flush()

                # Handle historical reconstruction (if multiple snapshots for same LG)
                if len(sorted_group) > 1:
                    for i in range(1, len(sorted_group)):
                        prev_data = sorted_group[i - 1].source_data_json or {}
                        curr_data = sorted_group[i].source_data_json or {}

                        # Calculate diff
                        diff = {}
                        for field in ["current_amount", "status", "expiry_date", "beneficiary_name"]:
                            old_v = prev_data.get(field)
                            new_v = curr_data.get(field)
                            if old_v != new_v:
                                diff[field] = {"old": old_v, "new": new_v}

                        if diff:
                            # Infer action type
                            action_type = "AMENDMENT"
                            action_data = {}

                            if "current_amount" in diff:
                                old_amt = _parse_amount(diff["current_amount"]["old"]) or 0
                                new_amt = _parse_amount(diff["current_amount"]["new"]) or 0
                                action_type = "INCREASE_AMOUNT" if new_amt > old_amt else "DECREASE_AMOUNT"
                                action_data["new_amount"] = new_amt
                                # Update the record's current amount
                                new_lg.current_amount = new_amt

                            if "expiry_date" in diff:
                                action_type = "EXTEND" if "current_amount" not in diff else action_type
                                new_exp = _safe_parse_date(diff["expiry_date"]["new"])
                                action_data["new_expiry_date"] = diff["expiry_date"]["new"]
                                if new_exp:
                                    new_lg.expiry_date = new_exp

                            if "status" in diff:
                                if action_type == "AMENDMENT":
                                    action_type = "STATUS_CHANGE"
                                new_lg.status = curr_data.get("status", new_lg.status)

                            # Create maintenance action
                            maintenance = IssuanceMaintenanceAction(
                                issued_lg_id=new_lg.id,
                                action_type=action_type,
                                status="EXECUTED",
                                action_data=action_data,
                            )
                            db.add(maintenance)

                            # Append to action history
                            new_lg.action_history.append({
                                "action_type": action_type,
                                "timestamp": curr_data.get("_history_timestamp") or curr_data.get("issue_date") or datetime.utcnow().isoformat(),
                                "diff": diff,
                                "source_file": curr_data.get("_source_file"),
                            })
                            flag_modified(new_lg, "action_history")

                    db.add(new_lg)
                    db.flush()

                # Mark all staging records as imported
                for rec in sorted_group:
                    rec.record_status = MigrationRecordStatusEnum.IMPORTED
                    rec.production_lg_id = new_lg.id
                    db.add(rec)

                newly_imported.add(lg_num.lower())
                imported += 1

            except Exception as e:
                logger.error(f"Failed to import issuance LG '{lg_num}': {e}", exc_info=True)
                for rec in sorted_group:
                    rec.record_status = MigrationRecordStatusEnum.ERROR
                    rec.validation_log = rec.validation_log or {}
                    rec.validation_log["_import_error"] = str(e)
                    db.add(rec)
                failed += 1
                failed_details.append({"lg_number": lg_num, "error": str(e)})

    db.commit()

    log_action(db, user_id=current_user.user_id, action_type="ISSUANCE_MIGRATION_IMPORT",
               entity_type="IssuedLGRecord", entity_id=None,
               details={"imported": imported, "failed": failed, "failed_details": failed_details},
               customer_id=current_user.customer_id)

    return {
        "message": "Issuance migration import completed.",
        "imported": imported,
        "failed": failed,
        "failed_details": failed_details,
    }


# ==============================================================================
# REPORT
# ==============================================================================

@router.get("/report", status_code=status.HTTP_200_OK)
async def get_issuance_migration_report(
    current_user: TokenData = Depends(get_current_corporate_admin_context),
    db: Session = Depends(get_db),
):
    """Get summary counts by status for issuance migration."""
    results = db.query(
        LGMigrationStaging.record_status,
        func.count(LGMigrationStaging.id).label("count")
    ).filter(
        LGMigrationStaging.customer_id == current_user.customer_id,
        LGMigrationStaging.migration_type == MigrationTypeEnum.ISSUANCE_RECORD,
    ).group_by(LGMigrationStaging.record_status).all()

    summary = {s.value: 0 for s in MigrationRecordStatusEnum}
    for status_val, count in results:
        if hasattr(status_val, 'value'):
            summary[status_val.value] = count
        else:
            summary[status_val] = count

    return {
        "total": sum(summary.values()),
        "summary": summary,
    }


# ==============================================================================
# RESOLUTION HELPERS (foreign key lookups)
# ==============================================================================

def _resolve_bank(db: Session, bank_name: Optional[str]) -> Optional[int]:
    if not bank_name:
        return None
    bank = db.query(Bank).filter(
        func.lower(Bank.name).like(func.lower(f"%{bank_name.strip()}%"))
    ).first()
    return bank.id if bank else None


def _resolve_currency(db: Session, code: Optional[str]) -> Optional[int]:
    if not code:
        return None
    currency = db.query(Currency).filter(
        func.lower(Currency.code) == func.lower(code.strip())
    ).first()
    return currency.id if currency else None


def _resolve_entity(db: Session, customer_id: int, name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    from app.models.models import CustomerEntity
    entity = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == customer_id,
        func.lower(CustomerEntity.name).like(func.lower(f"%{name.strip()}%"))
    ).first()
    return entity.id if entity else None


def _resolve_lg_type(db: Session, customer_id: int, name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    lg_type = db.query(LgType).filter(
        func.lower(LgType.name).like(func.lower(f"%{name.strip()}%"))
    ).first()
    return lg_type.id if lg_type else None


def _resolve_facility(db: Session, customer_id: int, facility_name: Optional[str], sub_limit_name: Optional[str]) -> Optional[int]:
    """Resolve facility/sub-limit name to sub_limit_id."""
    if not facility_name:
        return None

    facility = db.query(IssuanceFacility).filter(
        IssuanceFacility.customer_id == customer_id,
        func.lower(IssuanceFacility.facility_name).like(func.lower(f"%{facility_name.strip()}%")),
        IssuanceFacility.is_deleted == False,
    ).first()

    if not facility:
        return None

    if sub_limit_name:
        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.facility_id == facility.id,
            func.lower(IssuanceFacilitySubLimit.limit_name).like(func.lower(f"%{sub_limit_name.strip()}%"))
        ).first()
        return sub_limit.id if sub_limit else None

    # If no sub-limit name, return first sub-limit of the facility
    first_sub = db.query(IssuanceFacilitySubLimit).filter(
        IssuanceFacilitySubLimit.facility_id == facility.id
    ).first()
    return first_sub.id if first_sub else None


def _safe_parse_date(val) -> Optional[date]:
    """Parse a date string to a date object."""
    parsed = _parse_date(val)
    if parsed:
        try:
            return datetime.strptime(parsed, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    return None
