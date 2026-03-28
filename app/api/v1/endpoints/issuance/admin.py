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

@router.get("/admin/change-requests", response_model=List[AdminChangeRequestOut])
def list_change_requests(
    status_filter: Optional[str] = Query(None, description="PENDING, APPROVED, REJECTED"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """List all admin change requests for this customer."""
    query = db.query(AdminChangeRequest).filter(
        AdminChangeRequest.customer_id == current_user.customer_id,
    )
    if status_filter:
        query = query.filter(AdminChangeRequest.status == status_filter.upper())
    requests = query.order_by(AdminChangeRequest.created_at.desc()).all()
    
    results = []
    for req in requests:
        out = AdminChangeRequestOut.model_validate(req)
        out.requested_by_email = req.requested_by.email if req.requested_by else None
        out.approved_by_email = req.approved_by.email if req.approved_by else None
        results.append(out)
    
    # Enrich APPROVAL_MATRIX_UPDATE: resolve user IDs to emails in approver_values
    from app.models import User as UserModel
    for out in results:
        if out.change_type == "APPROVAL_MATRIX_UPDATE" and out.change_payload:
            new_value = out.change_payload.get("new_value")
            if not new_value:
                continue
            steps = new_value if isinstance(new_value, list) else list(new_value.values()) if isinstance(new_value, dict) else []
            for step in steps:
                if not isinstance(step, dict):
                    continue
                approver_type = step.get("approver_type", "")
                approver_vals = step.get("approver_values", [])
                if not approver_vals:
                    continue
                if approver_type == "USERS":
                    # Resolve numeric IDs to emails
                    int_ids = [v for v in approver_vals if isinstance(v, int) or (isinstance(v, str) and v.isdigit())]
                    if int_ids:
                        users = db.query(UserModel).filter(UserModel.id.in_([int(i) for i in int_ids])).all()
                        id_to_email = {u.id: u.email for u in users}
                        step["approver_values"] = [id_to_email.get(int(v), str(v)) if (isinstance(v, int) or (isinstance(v, str) and v.isdigit())) else v for v in approver_vals]
    
    return results


@router.post("/admin/change-requests", response_model=AdminChangeRequestOut, status_code=201)
def create_change_request(
    payload: AdminChangeRequestCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Submit a new admin change request requiring dual-control approval."""
    new_req = AdminChangeRequest(
        customer_id=current_user.customer_id,
        requested_by_user_id=current_user.user_id,
        change_type=payload.change_type,
        change_payload=payload.change_payload,
        status="PENDING"
    )
    db.add(new_req)
    db.commit()
    db.refresh(new_req)
    
    out = AdminChangeRequestOut.model_validate(new_req)
    out.requested_by_email = new_req.requested_by.email if new_req.requested_by else None
    return out


# ---------------------------------------------------------------------------
# K1: Governance — Auto‑Apply Logic & Helpers
# ---------------------------------------------------------------------------

@router.post("/admin/change-requests/{request_id}/action", response_model=AdminChangeRequestOut)
def action_change_request(
    request_id: int,
    action_payload: AdminChangeRequestAction,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Approve or reject an admin change request (dual-control)."""
    change_req = db.query(AdminChangeRequest).filter(
        AdminChangeRequest.id == request_id,
        AdminChangeRequest.customer_id == current_user.customer_id
    ).first()
    if not change_req:
        raise HTTPException(status_code=404, detail="Change request not found")
    if change_req.status != "PENDING":
        raise HTTPException(status_code=400, detail=f"Request is already {change_req.status}")
    
    # Dual-control: approver must be different from requester
    if change_req.requested_by_user_id == current_user.user_id:
        raise HTTPException(status_code=403, detail="Cannot approve/reject your own change request")
    
    action = action_payload.action.upper()
    if action == "APPROVE":
        change_req.status = "APPROVED"
        change_req.approved_by_user_id = current_user.user_id
        from datetime import datetime
        change_req.applied_at = datetime.utcnow()
        # K1: Apply the change automatically on approval
        _apply_admin_change(db, change_req)
    elif action == "REJECT":
        change_req.status = "REJECTED"
        change_req.approved_by_user_id = current_user.user_id
        change_req.rejection_reason = action_payload.rejection_reason
    else:
        raise HTTPException(status_code=400, detail="Action must be APPROVE or REJECT")
    
    db.commit()
    db.refresh(change_req)
    
    out = AdminChangeRequestOut.model_validate(change_req)
    out.requested_by_email = change_req.requested_by.email if change_req.requested_by else None
    out.approved_by_email = change_req.approved_by.email if change_req.approved_by else None
    return out


# ==============================================================================
# BANK FORM ISSUE REPORTING ENDPOINTS
# ==============================================================================

@router.get("/bank-form-issues", response_model=List[BankFormIssueReportOut])
def list_bank_form_issues(
    status_filter: Optional[str] = Query(None),
    bank_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """List bank form issue reports for this customer."""
    query = db.query(BankFormIssueReport).filter(
        BankFormIssueReport.customer_id == current_user.customer_id,
    )
    if status_filter:
        query = query.filter(BankFormIssueReport.status == status_filter.upper())
    if bank_id:
        query = query.filter(BankFormIssueReport.bank_id == bank_id)
    reports = query.order_by(BankFormIssueReport.created_at.desc()).all()
    
    results = []
    for report in reports:
        out = BankFormIssueReportOut.model_validate(report)
        out.reported_by_email = report.reported_by.email if report.reported_by else None
        out.bank_name = report.bank.name if report.bank else None
        results.append(out)
    return results


@router.post("/bank-form-issues", response_model=BankFormIssueReportOut, status_code=201)
def create_bank_form_issue(
    bank_id: int = Form(...),
    issue_type: str = Form(...),
    description: str = Form(...),
    field_name: Opt[str] = Form(None),
    severity: str = Form("MEDIUM"),
    form_config_id: Opt[int] = Form(None),
    attachment: Opt[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Report an issue with a bank form, with optional file attachment."""
    attachment_path = None
    if attachment and attachment.filename:
        try:
            import asyncio, re as _re
            from datetime import date as _date
            from app.core.ai_integration import _upload_to_gcs
            file_bytes = attachment.file.read()
            ext = attachment.filename.rsplit('.', 1)[-1] if '.' in attachment.filename else 'bin'
            import uuid
            _today = _date.today().strftime('%Y%m%d')
            _safe_orig = _re.sub(r'[^\w\-]', '_', attachment.filename.rsplit('.', 1)[0] if '.' in attachment.filename else attachment.filename)[:40]
            blob_name = f"customer_{current_user.customer_id}/form_issues/FORM_ISSUE_{issue_type}_BANK-{bank_id}_{_today}_{_safe_orig}_{uuid.uuid4().hex[:6]}.{ext}"
            loop = asyncio.new_event_loop()
            try:
                gcs_url = loop.run_until_complete(_upload_to_gcs(file_bytes, blob_name, attachment.content_type or 'application/octet-stream'))
                attachment_path = gcs_url
            finally:
                loop.close()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to upload form issue attachment: {e}")

    new_report = BankFormIssueReport(
        customer_id=current_user.customer_id,
        reported_by_user_id=current_user.user_id,
        bank_id=bank_id,
        form_config_id=form_config_id,
        issue_type=issue_type,
        description=description,
        field_name=field_name,
        severity=severity,
        status="OPEN",
        attachment_path=attachment_path,
    )
    db.add(new_report)
    db.commit()
    db.refresh(new_report)
    
    out = BankFormIssueReportOut.model_validate(new_report)
    out.reported_by_email = new_report.reported_by.email if new_report.reported_by else None
    out.bank_name = new_report.bank.name if new_report.bank else None
    return out


@router.patch("/bank-form-issues/{issue_id}", response_model=BankFormIssueReportOut)
def update_bank_form_issue(
    issue_id: int,
    payload: BankFormIssueReportUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Update a bank form issue report (admin only)."""
    report = db.query(BankFormIssueReport).filter(
        BankFormIssueReport.id == issue_id,
        BankFormIssueReport.customer_id == current_user.customer_id,
    ).first()
    if not report:
        raise HTTPException(status_code=404, detail="Issue report not found")
    
    if payload.status:
        report.status = payload.status
        if payload.status in ("RESOLVED", "CLOSED"):
            from datetime import datetime
            report.resolved_at = datetime.utcnow()
    if payload.resolution_notes is not None:
        report.resolution_notes = payload.resolution_notes
    if payload.severity:
        report.severity = payload.severity
    
    db.commit()
    db.refresh(report)

    # Notify the reporter about the status change
    if payload.status and report.reported_by_user_id:
        try:
            from app.services.notification_service import notify
            bank_name = report.bank.name if report.bank else "Unknown Bank"
            issue_label = (report.issue_type or "").replace("_", " ").title()
            status_label = payload.status.replace("_", " ").title()
            status_emoji = {"RESOLVED": "✅", "CLOSED": "🔒", "IN_PROGRESS": "🔄", "WONT_FIX": "⛔"}.get(payload.status, "📋")
            
            message = f"Your report about '{issue_label}' for {bank_name} has been updated to {status_label}."
            if payload.resolution_notes:
                message += f" Notes: {payload.resolution_notes}"

            notify(
                db, user_ids=[report.reported_by_user_id], module="ISSUANCE",
                event_type="BANK_FORM_ISSUE_UPDATED",
                title=f"{status_emoji} Bank Form Issue — {status_label}",
                message=message,
                link="/end-user/issuance/issued-lgs",
                actor_user_id=current_user.user_id,
                reference_id=report.id,
            )
        except Exception:
            pass  # Non-fatal: don't block the update if notification fails
    
    out = BankFormIssueReportOut.model_validate(report)
    out.reported_by_email = report.reported_by.email if report.reported_by else None
    out.bank_name = report.bank.name if report.bank else None
    return out


# ==============================================================================
# 3.2 BANK FORM GAP DETECTION
# ==============================================================================

@router.post("/bank-forms/{form_id}/gap-analysis/{request_id}")
def bank_form_gap_analysis(
    form_id: int,
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    3.2: After auto-fill, compare request fields vs mapped form fields.
    Alert user about unmapped critical details and offer supplementary letter option.
    """
    from sqlalchemy.orm import selectinload
    from app.models.models_issuance import BankFormTemplate

    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    if not form_template.field_mapping:
        raise HTTPException(400, "Form has no field mapping. Run AI analysis first.")

    # Load request with relationships
    request = db.query(IssuanceRequest).options(
        selectinload(IssuanceRequest.currency),
        selectinload(IssuanceRequest.lg_type),
        selectinload(IssuanceRequest.issuing_entity),
        selectinload(IssuanceRequest.customer),
        selectinload(IssuanceRequest.project),
    ).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id,
    ).first()
    if not request:
        raise HTTPException(404, "Issuance request not found.")

    # Build the data dict the same way fill_bank_form does
    from app.core.pdf_form_filler import build_request_data_dict
    request_data = build_request_data_dict(request, db, bank_id=form_template.bank_id)

    # Analyze gaps: which form fields couldn't get filled?
    field_mapping = form_template.field_mapping  # Could be list of {pdf_field_name, mapped_to} or dict
    # Normalize: if stored as list (from AI analysis), convert to {form_field: data_key} dict
    if isinstance(field_mapping, list):
        field_mapping = {
            entry.get("pdf_field_name", entry.get("form_field", f"field_{i}")): entry.get("mapped_to", entry.get("data_key", ""))
            for i, entry in enumerate(field_mapping)
            if isinstance(entry, dict)
        }
    filled_fields = []
    empty_fields = []
    unmapped_request_fields = []

    # Critical request fields that should ideally be on the form
    # Keys must match those produced by build_request_data_dict()
    critical_fields = {
        "beneficiary_name", "amount", "currency_code", "expiry_date",
        "requested_issue_date", "lg_purpose", "beneficiary_address",
        "reference_number", "applicable_rules",
    }
    
    # Alias normalization: some fields have multiple names that point to the same data
    # e.g. AI may map to "requested_expiry_date" or "expiry_date" — both are valid
    FIELD_ALIASES = {
        "requested_expiry_date": "expiry_date",
        "requested_issue_date": "requested_issue_date",  # canonical
        "issue_date": "requested_issue_date",
        "current_date": "requested_issue_date",
    }

    # Check what the form maps vs what data we have
    mapped_request_keys = set()
    for form_field, data_key in field_mapping.items():
        # Normalize the data_key through aliases before adding
        canonical_key = FIELD_ALIASES.get(data_key, data_key)
        mapped_request_keys.add(canonical_key)
        mapped_request_keys.add(data_key)  # Also keep original
        value = request_data.get(data_key)
        if value and str(value).strip():
            filled_fields.append({
                "form_field": form_field,
                "data_key": data_key,
                "value": str(value)[:100],
            })
        else:
            empty_fields.append({
                "form_field": form_field,
                "data_key": data_key,
                "reason": "Request data is empty or missing for this field",
            })

    # Find critical request fields NOT mapped to any form field
    for crit_field in critical_fields:
        if crit_field not in mapped_request_keys:
            value = request_data.get(crit_field)
            if value and str(value).strip():
                unmapped_request_fields.append({
                    "field": crit_field,
                    "value": str(value)[:100],
                    "severity": "HIGH",
                    "suggestion": f"This critical field '{crit_field}' has data but no corresponding form field.",
                })

    has_gaps = bool(empty_fields or unmapped_request_fields)

    # Build supplementary letter suggestion if gaps exist
    supplementary_letter = None
    if unmapped_request_fields:
        letter_lines = [f"- {f['field']}: {f['value']}" for f in unmapped_request_fields]
        supplementary_letter = {
            "suggested": True,
            "reason": f"{len(unmapped_request_fields)} critical field(s) have data but no form field.",
            "content_preview": "\n".join(letter_lines),
        }

    return {
        "form_id": form_id,
        "form_name": form_template.name,
        "request_id": request_id,
        "serial_number": request.serial_number,
        "has_gaps": has_gaps,
        "summary": {
            "total_form_fields": len(field_mapping),
            "filled": len(filled_fields),
            "empty": len(empty_fields),
            "unmapped_critical": len(unmapped_request_fields),
        },
        "filled_fields": filled_fields,
        "empty_fields": empty_fields,
        "unmapped_critical_fields": unmapped_request_fields,
        "supplementary_letter": supplementary_letter,
    }


# ==============================================================================
# 3.3 RECONCILIATION HEADER DRIFT DETECTION
# ==============================================================================

from app.services.reconciliation_service import reconciliation_service as recon_service

