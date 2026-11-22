# app/api/v1/endpoints/field_provenance_endpoints.py
from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import asc
from sqlalchemy.orm import Session

from app.api.v1.endpoints.end_user import (
    _get_accessible_lg_record,
    get_current_end_user_context,
)
from app.constants import (
    AUDIT_ACTION_TYPE_LG_AMENDED,
    AUDIT_ACTION_TYPE_LG_AUTO_RENEWAL_TOGGLED,
    AUDIT_ACTION_TYPE_LG_BULK_CATEGORY_CHANGED,
    AUDIT_ACTION_TYPE_LG_SINGLE_CATEGORY_CHANGED,
    DOCUMENT_TYPE_ORIGINAL_LG,
)
from app.core.security import (
    HasPermission,
    TokenData,
    check_for_read_only_mode,
    check_subscription_status,
)
from app.database import get_db
import app.models as models
from app.models.models_field_provenance import LGFieldProvenance

logger = logging.getLogger(__name__)

router = APIRouter()

TRACKED_PROVENANCE_FIELDS = {
    # LG Core fields
    "beneficiary_corporate_id",
    "issuer_name",
    "lg_number",
    "lg_amount",
    "lg_currency_id",
    "lg_payable_currency_id",
    "issuance_date",
    "expiry_date",
    "auto_renewal",
    "lg_type_id",
    "lg_operational_status_id",
    "payment_conditions",
    "description_purpose",
    # Bank & Rule fields
    "issuing_bank_id",
    "issuing_bank_address",
    "issuing_bank_phone",
    "issuing_bank_fax",
    "foreign_bank_name",
    "foreign_bank_country",
    "foreign_bank_address",
    "foreign_bank_swift_code",
    "advising_status",
    "communication_bank_id",
    "issuing_method_id",
    "applicable_rule_id",
    "applicable_rules_text",
    "other_conditions",
    "mandatory_claim_statement",
    # System-defaulted classification field
    "lg_category_id",
}

ALLOWED_SOURCE_STATUSES = {
    "AI_SCAN",
    "SYSTEM_DEFAULT",
    "USER_MANUAL",
    "USER_EDITED_AI",
    "USER_EDITED_DEFAULT",
    "USER_EDITED",
}


class FieldProvenanceSaveIn(BaseModel):
    record_origin: str = Field(
        "MANUAL",
        description="Creation mode: 'AI_SCAN' when populated via OCR scan, 'MANUAL' otherwise.",
    )
    field_sources: Dict[str, str] = Field(
        default_factory=dict,
        description="Per-field provenance map captured on Record New LG.",
    )


class FieldProvenanceOut(BaseModel):
    lg_record_id: int
    tracking_mode: str = Field(
        ...,
        description="EXACT (tracked at creation), LEGACY_CORRELATED, LEGACY_MIGRATED, or TRIAL_SAMPLE",
    )
    record_origin: str
    field_sources: Dict[str, str] = Field(default_factory=dict)


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _correlate_legacy_lg_origin(
    db: Session,
    lg_record: models.LGRecord,
) -> tuple[str, str, Dict[str, str]]:
    """
    Read-only DB correlation for historical records created prior to LGFieldProvenance:
    - Trial sandbox sample LGs & Migration Hub imports keep initial fields unmarked.
    - LGs with a matching AI_SCAN_SUCCESS / AIUsageLog event keep initial OCR fields unmarked.
    - LGs with NO AI scan match are known manual entries -> mark populated LG Core & Bank/Rule fields.
    """
    mig_src = (getattr(lg_record, "migration_source", None) or "").strip().upper()
    if mig_src == "TRIAL_SAMPLE":
        return ("TRIAL_SAMPLE", "TRIAL_SAMPLE", {})
    if mig_src == "LEGACY" or getattr(lg_record, "migrated_from_staging_id", None) is not None:
        return ("LEGACY_MIGRATED", "MIGRATED", {})

    created_utc = _as_utc(getattr(lg_record, "created_at", None))
    customer_id = lg_record.customer_id

    # Gather initial ORIGINAL_LG_DOCUMENT file names uploaded around record creation time
    initial_doc_filenames = set()
    creator_user_id = None
    for doc in getattr(lg_record, "documents", None) or []:
        if getattr(doc, "is_deleted", False):
            continue
        if doc.document_type == DOCUMENT_TYPE_ORIGINAL_LG:
            doc_created = _as_utc(getattr(doc, "created_at", None))
            if (
                created_utc is None
                or doc_created is None
                or abs((doc_created - created_utc).total_seconds()) <= 300
            ):
                if doc.file_name:
                    initial_doc_filenames.add(doc.file_name.strip().lower())
                if creator_user_id is None and getattr(doc, "uploaded_by_user_id", None):
                    creator_user_id = doc.uploaded_by_user_id

    if creator_user_id is None:
        create_log = (
            db.query(models.AuditLog.user_id)
            .filter(
                models.AuditLog.customer_id == customer_id,
                models.AuditLog.lg_record_id == lg_record.id,
                models.AuditLog.action_type == "CREATE",
            )
            .first()
        )
        if create_log and create_log[0]:
            creator_user_id = create_log[0]

    has_ai_scan = False
    if created_utc is not None:
        window_start = created_utc - timedelta(hours=12)
        window_end = created_utc + timedelta(minutes=2)
        close_window_start = created_utc - timedelta(minutes=45)

        scan_logs = (
            db.query(models.AuditLog)
            .filter(
                models.AuditLog.customer_id == customer_id,
                models.AuditLog.action_type == "AI_SCAN_SUCCESS",
                models.AuditLog.timestamp >= window_start,
                models.AuditLog.timestamp <= window_end,
            )
            .all()
        )
        for slog in scan_logs:
            details = slog.details if isinstance(slog.details, dict) else {}
            scanned_fname = str(details.get("file_name") or "").strip().lower()
            slog_ts = _as_utc(slog.timestamp)
            if scanned_fname and scanned_fname in initial_doc_filenames:
                has_ai_scan = True
                break
            if (
                creator_user_id is not None
                and slog.user_id == creator_user_id
                and slog_ts is not None
                and slog_ts >= close_window_start
                and (not initial_doc_filenames or not scanned_fname)
            ):
                has_ai_scan = True
                break

        if not has_ai_scan:
            ai_usage_rows = (
                db.query(models.AIUsageLog)
                .filter(
                    models.AIUsageLog.customer_id == customer_id,
                    models.AIUsageLog.created_at >= window_start,
                    models.AIUsageLog.created_at <= window_end,
                )
                .all()
            )
            for urow in ai_usage_rows:
                doc_name = str(urow.doc_name or "").strip().lower()
                urow_ts = _as_utc(urow.created_at)
                if doc_name and doc_name in initial_doc_filenames:
                    has_ai_scan = True
                    break
                if (
                    creator_user_id is not None
                    and urow.user_id == creator_user_id
                    and urow_ts is not None
                    and urow_ts >= close_window_start
                    and not initial_doc_filenames
                ):
                    has_ai_scan = True
                    break

    if has_ai_scan:
        # AI-assisted legacy record: keep initial OCR fields unmarked unless later amended
        return ("LEGACY_CORRELATED", "AI_SCAN_LEGACY", {})

    # Unscanned manual legacy record: mark populated LG Core & Bank/Rule user-entered fields
    inferred: Dict[str, str] = {}
    core_always = [
        "beneficiary_corporate_id",
        "issuer_name",
        "lg_number",
        "lg_amount",
        "lg_currency_id",
        "issuance_date",
        "expiry_date",
        "lg_type_id",
        "description_purpose",
        "issuing_bank_id",
        "applicable_rule_id",
    ]
    for f in core_always:
        if getattr(lg_record, f, None) is not None:
            inferred[f] = "USER_MANUAL"

    for opt_field in (
        "lg_operational_status_id",
        "payment_conditions",
        "applicable_rules_text",
        "other_conditions",
        "mandatory_claim_statement",
    ):
        val = getattr(lg_record, opt_field, None)
        if val is not None and str(val).strip() != "":
            inferred[opt_field] = "USER_MANUAL"

    is_foreign = bool(
        getattr(lg_record, "issuing_bank", None)
        and (lg_record.issuing_bank.name or "").strip().lower() == "foreign bank"
    )
    if is_foreign:
        for fb_field in (
            "foreign_bank_name",
            "foreign_bank_country",
            "foreign_bank_address",
            "foreign_bank_swift_code",
            "communication_bank_id",
        ):
            val = getattr(lg_record, fb_field, None)
            if val is not None and str(val).strip() != "":
                inferred[fb_field] = "USER_MANUAL"
        adv_val = getattr(lg_record, "advising_status", None)
        adv_str = adv_val.value if hasattr(adv_val, "value") else str(adv_val or "").strip()
        if adv_str and adv_str != "Not Advised":
            inferred["advising_status"] = "USER_EDITED_DEFAULT"

    # System defaults: only flag when user diverged from the platform default
    if getattr(lg_record, "auto_renewal", True) is False:
        inferred["auto_renewal"] = "USER_EDITED_DEFAULT"

    issuing_method_obj = getattr(lg_record, "issuing_method", None)
    if issuing_method_obj and (issuing_method_obj.name or "").strip() != "Manual Delivery":
        inferred["issuing_method_id"] = "USER_EDITED_DEFAULT"

    lg_cat_obj = getattr(lg_record, "lg_category", None)
    if lg_cat_obj and not getattr(lg_cat_obj, "is_default", False):
        inferred["lg_category_id"] = "USER_EDITED_DEFAULT"

    return ("LEGACY_CORRELATED", "MANUAL_LEGACY", inferred)


def _overlay_post_creation_edits(
    db: Session,
    lg_record: models.LGRecord,
    base_fields: Dict[str, str],
) -> Dict[str, str]:
    """
    Overlays post-recording user amendments / category changes / auto-renewal toggles
    from AuditLog without touching any existing CRUD methods.
    """
    merged = dict(base_fields)
    relevant_actions = [
        AUDIT_ACTION_TYPE_LG_AMENDED,
        AUDIT_ACTION_TYPE_LG_SINGLE_CATEGORY_CHANGED,
        AUDIT_ACTION_TYPE_LG_BULK_CATEGORY_CHANGED,
        AUDIT_ACTION_TYPE_LG_AUTO_RENEWAL_TOGGLED,
    ]
    logs = (
        db.query(models.AuditLog)
        .filter(
            models.AuditLog.customer_id == lg_record.customer_id,
            models.AuditLog.lg_record_id == lg_record.id,
            models.AuditLog.action_type.in_(relevant_actions),
        )
        .order_by(asc(models.AuditLog.timestamp), asc(models.AuditLog.id))
        .all()
    )

    for log in logs:
        details = log.details if isinstance(log.details, dict) else {}
        if log.action_type == AUDIT_ACTION_TYPE_LG_AMENDED:
            # Ignore historical migration snapshot replay entries
            if "snapshot_id" in details or "batch_id" in details:
                continue
            amended_fields = details.get("amended_fields")
            if isinstance(amended_fields, dict):
                for field_key in amended_fields.keys():
                    if field_key in TRACKED_PROVENANCE_FIELDS:
                        merged[field_key] = "USER_AMENDED"
        elif log.action_type in (
            AUDIT_ACTION_TYPE_LG_SINGLE_CATEGORY_CHANGED,
            AUDIT_ACTION_TYPE_LG_BULK_CATEGORY_CHANGED,
        ):
            merged["lg_category_id"] = "USER_AMENDED"
        elif log.action_type == AUDIT_ACTION_TYPE_LG_AUTO_RENEWAL_TOGGLED:
            merged["auto_renewal"] = "USER_AMENDED"

    return merged


@router.post(
    "/lg-records/{lg_record_id}",
    response_model=FieldProvenanceOut,
    status_code=status.HTTP_200_OK,
    dependencies=[
        Depends(HasPermission("lg_record:create")),
        Depends(check_for_read_only_mode),
    ],
    summary="Record additive field-level origin metadata for a newly created LG",
)
def save_lg_field_provenance(
    lg_record_id: int,
    payload: FieldProvenanceSaveIn,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    lg_record = _get_accessible_lg_record(db, lg_record_id, end_user_context)
    if not lg_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="LG Record not found or not accessible.",
        )

    clean_origin = (
        "AI_SCAN"
        if (payload.record_origin or "").strip().upper() == "AI_SCAN"
        else "MANUAL"
    )
    sanitized_sources: Dict[str, str] = {}
    for raw_key, raw_status in (payload.field_sources or {}).items():
        key = str(raw_key or "").strip()
        st = str(raw_status or "").strip().upper()
        if key in TRACKED_PROVENANCE_FIELDS and st in ALLOWED_SOURCE_STATUSES:
            sanitized_sources[key] = st

    existing = (
        db.query(LGFieldProvenance)
        .filter(LGFieldProvenance.lg_record_id == lg_record.id)
        .first()
    )
    if existing:
        created_utc = _as_utc(existing.created_at)
        now_utc = datetime.now(timezone.utc)
        is_recent_retry = (
            existing.created_by_user_id == end_user_context.user_id
            and created_utc is not None
            and (now_utc - created_utc).total_seconds() <= 900
        )
        if is_recent_retry:
            existing.record_origin = clean_origin
            existing.field_sources_json = sanitized_sources
            db.add(existing)
            db.commit()
            db.refresh(existing)
        merged = _overlay_post_creation_edits(
            db, lg_record, existing.field_sources_json or {}
        )
        return FieldProvenanceOut(
            lg_record_id=lg_record.id,
            tracking_mode="EXACT",
            record_origin=existing.record_origin,
            field_sources=merged,
        )

    row = LGFieldProvenance(
        lg_record_id=lg_record.id,
        customer_id=lg_record.customer_id,
        created_by_user_id=end_user_context.user_id,
        record_origin=clean_origin,
        field_sources_json=sanitized_sources,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    merged = _overlay_post_creation_edits(db, lg_record, row.field_sources_json or {})
    return FieldProvenanceOut(
        lg_record_id=lg_record.id,
        tracking_mode="EXACT",
        record_origin=row.record_origin,
        field_sources=merged,
    )


@router.get(
    "/lg-records/{lg_record_id}",
    response_model=FieldProvenanceOut,
    dependencies=[
        Depends(HasPermission("lg_record:view_own")),
        Depends(check_subscription_status),
    ],
    summary="Retrieve field-level origin metadata for an LG record (with smart legacy correlation)",
)
def get_lg_field_provenance(
    lg_record_id: int,
    db: Session = Depends(get_db),
    current_user_context: TokenData = Depends(get_current_end_user_context),
):
    lg_record = _get_accessible_lg_record(db, lg_record_id, current_user_context)
    if not lg_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="LG Record not found or not accessible.",
        )

    row = (
        db.query(LGFieldProvenance)
        .filter(LGFieldProvenance.lg_record_id == lg_record.id)
        .first()
    )
    if row:
        merged = _overlay_post_creation_edits(
            db, lg_record, row.field_sources_json or {}
        )
        return FieldProvenanceOut(
            lg_record_id=lg_record.id,
            tracking_mode="EXACT",
            record_origin=row.record_origin,
            field_sources=merged,
        )

    tracking_mode, record_origin, legacy_fields = _correlate_legacy_lg_origin(
        db, lg_record
    )
    merged = _overlay_post_creation_edits(db, lg_record, legacy_fields)
    return FieldProvenanceOut(
        lg_record_id=lg_record.id,
        tracking_mode=tracking_mode,
        record_origin=record_origin,
        field_sources=merged,
    )
