from typing import List, Any, Optional, Dict
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, Body, Request, UploadFile, File, Form
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from datetime import date
import io
import json
import logging

logger = logging.getLogger(__name__)

from app.database import get_db
from app.core.security import get_current_corporate_admin_context, get_current_approver_context, get_current_treasury_context, get_issuance_read_context, check_subscription_status, TokenData
from app.core.document_generator import generate_pdf_from_html
from app.core.encryption import encrypt_data 

# Models
from app.models.models import Bank, Currency 
from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility, IssuanceWorkflowPolicy, CustomerFormConfiguration, IssuanceRequestSnapshot, IssuanceRequestVersion, AdminChangeRequest, BankFormIssueReport
# NOTE: Ensure you created app/models/models_reconciliation.py first!
from app.models.models_reconciliation import BankPositionBatch, BankPositionRow 
# Schemas
from app.schemas.all_schemas import BankOut, CurrencyOut 
from app.schemas.schemas_issuance import (
    IssuanceRequestCreate, IssuanceRequestOut, IssuanceRequestUpdate, IssuanceRequestDraftCreate,
    CustomerFormConfigurationCreateUpdate, IssuanceRequestVersionOut,
    IssuanceFacilityCreate, IssuanceFacilityOut, SuitableFacilityOut, 
    IssuanceRequestContentUpdate, IssuanceFacilityUpdate,
    IssuedLGRecordOut, IssuedLGRecordDetailOut,
    IssuanceExecuteRequest, IssuanceCancelRequest,
    ReconciliationRequest, ReconciliationResult,
    IssuanceWorkflowPolicyCreate, IssuanceWorkflowPolicyOut,
    BankIssuanceOptionOut, BankIssuanceOptionCreateUpdate,
    AdminChangeRequestCreate, AdminChangeRequestOut, AdminChangeRequestAction,
    BankFormIssueReportCreate, BankFormIssueReportOut, BankFormIssueReportUpdate
)
from app.services.issuance_service import issuance_service

# CRUD
from app.crud.crud_issuance import crud_issuance_request
from app.crud.crud_facility import crud_facility
from app.crud.crud_bank_methods import crud_bank_methods
from fastapi.responses import StreamingResponse

router = APIRouter()

from .base import *
from .base import _read_bank_form_pdf_bytes, _send_edit_notifications, _detect_coverage_gaps, _make_doc_filename, _get_lg_copy_docs, _send_requestor_status_notification, _serialize_action, _serialize_recon_session, _serialize_recon_result, _apply_admin_change, _create_governed_change

@router.get("/issued-lgs/export-fields")
def get_export_fields(
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Returns available columns and presets for the custom export builder."""
    from app.services.issuance_export_service import IssuanceExportService, PRESET_SUMMARY_COLS, PRESET_FINANCIAL_COLS, PRESET_OPERATIONS_COLS, PRESET_DETAILED_COLS

    fields = IssuanceExportService.get_available_columns()
    presets = {
        "summary": PRESET_SUMMARY_COLS,
        "financial": PRESET_FINANCIAL_COLS,
        "operations": PRESET_OPERATIONS_COLS,
        "detailed": PRESET_DETAILED_COLS,
    }
    return {"fields": fields, "presets": presets}


@router.get("/issued-lgs/export")
def export_issued_lgs(
    export_type: str = Query("summary", description="summary | detailed | full_audit | custom"),
    status_filter: Optional[str] = Query(None, description="Filter by status, e.g. ACTIVE"),
    search: Optional[str] = Query(None, description="Search LG ref or beneficiary"),
    columns: Optional[str] = Query(None, description="Comma-separated column keys for custom export"),
    include_event_log: bool = Query(False, description="Include dedicated Lifecycle Event Log tab"),
    include_approvals: bool = Query(False, description="Include dedicated Approvals Audit tab"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Export Issued LGs to professional formatted XLSX with full lifecycle history. Supports custom column selection and arranging."""
    from app.services.issuance_export_service import IssuanceExportService
    from fastapi.responses import StreamingResponse

    custom_cols_list = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    buf = IssuanceExportService.generate_excel_export(
        db=db,
        customer_id=current_user.customer_id,
        user_email=getattr(current_user, "email", "Treasury User"),
        export_type=export_type,
        status_filter=status_filter,
        search=search,
        custom_columns=custom_cols_list,
        include_event_log=include_event_log,
        include_approvals=include_approvals,
    )

    filename = f"issued_lgs_{export_type}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/dashboard-stats")
def get_dashboard_stats(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    4.2: Treasury dashboard KPI endpoint.
    Returns aggregated stats: pending requests, approvals, SLA breaches,
    expiring LGs, facility utilization, and recent activity.
    """
    from datetime import datetime, timedelta, date, timezone
    from sqlalchemy import func as sqla_func

    cust_id = current_user.customer_id
    today = date.today()
    d7 = today + timedelta(days=7)
    d30 = today + timedelta(days=30)

    # --- Pending Requests ---
    pending_requests = db.query(sqla_func.count(IssuanceRequest.id)).filter(
        IssuanceRequest.customer_id == cust_id,
        IssuanceRequest.status.in_(["SUBMITTED", "PENDING_APPROVAL", "REVISION_REQUIRED"])
    ).scalar() or 0

    # --- Pending Approvals (for this user) ---
    pending_approvals_q = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == cust_id,
        IssuanceRequest.status == "PENDING_APPROVAL",
    ).all()
    pending_approvals = sum(
        1 for r in pending_approvals_q
        if r.pending_approver_users and str(current_user.user_id) in [str(u) for u in (r.pending_approver_users or [])]
    )

    # --- Pending Bank Replies ---
    pending_bank = db.query(sqla_func.count(IssuedLGRecord.id)).filter(
        IssuedLGRecord.customer_id == cust_id,
        IssuedLGRecord.status == "INTERNAL_PROCESSING",
    ).scalar() or 0

    # --- SLA Breaches (requests pending > sla_agreement_days on their facility) ---
    sla_breaches = 0
    pending_reqs = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == cust_id,
        IssuanceRequest.status.in_(["PENDING_APPROVAL", "SUBMITTED", "APPROVED_INTERNAL", "FACILITY_RESERVED"])
    ).all()
    for req in pending_reqs:
        if req.created_at:
            age_days = (datetime.now(timezone.utc) - req.created_at).days
            if age_days > 7:  # Default SLA threshold
                sla_breaches += 1

    # --- Expiring LGs ---
    expiring_7d = db.query(sqla_func.count(IssuedLGRecord.id)).filter(
        IssuedLGRecord.customer_id == cust_id,
        IssuedLGRecord.status == "ACTIVE",
        IssuedLGRecord.expiry_date != None,
        IssuedLGRecord.expiry_date <= d7,
        IssuedLGRecord.expiry_date >= today,
    ).scalar() or 0

    expiring_30d = db.query(sqla_func.count(IssuedLGRecord.id)).filter(
        IssuedLGRecord.customer_id == cust_id,
        IssuedLGRecord.status == "ACTIVE",
        IssuedLGRecord.expiry_date != None,
        IssuedLGRecord.expiry_date <= d30,
        IssuedLGRecord.expiry_date >= today,
    ).scalar() or 0

    # --- Active LGs totals ---
    active_stats = db.query(
        sqla_func.count(IssuedLGRecord.id),
        sqla_func.coalesce(sqla_func.sum(IssuedLGRecord.current_amount), 0)
    ).filter(
        IssuedLGRecord.customer_id == cust_id,
        IssuedLGRecord.status == "ACTIVE",
    ).first()

    total_active_lgs = active_stats[0] or 0
    total_active_amount = float(active_stats[1] or 0)

    # --- Facility Utilization per Bank ---
    facilities = db.query(IssuanceFacility).filter(
        IssuanceFacility.customer_id == cust_id,
        IssuanceFacility.status == "ACTIVE",
        IssuanceFacility.is_deleted == False,
    ).all()

    facility_utilization = []
    for fac in facilities:
        total_limit = float(fac.total_limit_amount or 0)
        if total_limit <= 0:
            continue
        # Sum utilized amount from active LGs under this facility's sub-limits
        sub_limit_ids = [sl.id for sl in (fac.sub_limits or [])]
        utilized = 0
        if sub_limit_ids:
            utilized = float(db.query(
                sqla_func.coalesce(sqla_func.sum(IssuedLGRecord.current_amount), 0)
            ).filter(
                IssuedLGRecord.facility_sub_limit_id.in_(sub_limit_ids),
                IssuedLGRecord.status.in_(["ACTIVE", "INTERNAL_PROCESSING"]),
            ).scalar() or 0)
            # Add initial utilization from sub-limits
            for sl in fac.sub_limits:
                utilized += float(sl.initial_utilization or 0)

        used_pct = round((utilized / total_limit) * 100, 1) if total_limit > 0 else 0
        bank_name = fac.bank.name if fac.bank else f"Bank #{fac.bank_id}"
        facility_utilization.append({
            "facility_id": fac.id,
            "facility_name": fac.facility_name,
            "bank": bank_name,
            "total_limit": total_limit,
            "utilized": utilized,
            "available": max(total_limit - utilized, 0),
            "used_pct": used_pct,
            "currency": fac.currency.iso_code if fac.currency else "N/A",
        })

    # --- Expiring LGs list (for table) ---
    expiring_lgs_list = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == cust_id,
        IssuedLGRecord.status == "ACTIVE",
        IssuedLGRecord.expiry_date != None,
        IssuedLGRecord.expiry_date <= d30,
        IssuedLGRecord.expiry_date >= today,
    ).order_by(IssuedLGRecord.expiry_date.asc()).limit(10).all()

    expiring_lgs = []
    for lg in expiring_lgs_list:
        days_remaining = (lg.expiry_date - today).days
        expiring_lgs.append({
            "id": lg.id,
            "ref": lg.lg_ref_number,
            "beneficiary": lg.beneficiary_name,
            "amount": float(lg.current_amount or 0),
            "currency": lg.currency.iso_code if lg.currency else "",
            "expiry_date": lg.expiry_date.isoformat() if lg.expiry_date else None,
            "days_remaining": days_remaining,
            "bank": lg.bank.name if lg.bank else "",
        })

    # --- Recent Activity (latest 10 audit trail entries) ---
    recent_lgs = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == cust_id,
    ).order_by(IssuedLGRecord.created_at.desc()).limit(5).all()

    recent_actions = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == cust_id,
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).limit(5).all()

    recent_activity = []
    for lg in recent_lgs:
        recent_activity.append({
            "type": "ISSUED",
            "ref": lg.lg_ref_number,
            "description": f"LG issued to {lg.beneficiary_name}",
            "timestamp": lg.created_at.isoformat() if lg.created_at else None,
        })
    for action in recent_actions:
        lg = action.issued_lg
        recent_activity.append({
            "type": f"MAINTENANCE_{action.action_type}",
            "ref": lg.lg_ref_number if lg else f"LG#{action.issued_lg_id}",
            "description": f"{action.action_type.replace('_', ' ').title()} — {action.status}",
            "timestamp": action.created_at.isoformat() if action.created_at else None,
        })
    # Sort combined by timestamp desc
    recent_activity.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    recent_activity = recent_activity[:10]

    return {
        "pending_requests": pending_requests,
        "pending_approvals": pending_approvals,
        "pending_bank_replies": pending_bank,
        "sla_breaches": sla_breaches,
        "expiring_lgs_7d": expiring_7d,
        "expiring_lgs_30d": expiring_30d,
        "total_active_lgs": total_active_lgs,
        "total_active_amount": total_active_amount,
        "facility_utilization": facility_utilization,
        "expiring_lgs": expiring_lgs,
        "recent_activity": recent_activity,
    }


# ==============================================================================
# H2: Supporting Document Analysis During Request
# ==============================================================================

