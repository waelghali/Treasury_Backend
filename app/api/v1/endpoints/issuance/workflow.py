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

@router.get("/workflow-policies", response_model=List[IssuanceWorkflowPolicyOut])
def list_workflow_policies(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ List all active approval rules for this customer in sequential order. """
    return db.query(IssuanceWorkflowPolicy).filter(
        IssuanceWorkflowPolicy.customer_id == current_user.customer_id
    ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()

@router.put("/workflow-policies")
def update_workflow_policies(
    policies_in: List[IssuanceWorkflowPolicyCreate],
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ Bulk replace all approval rules. Wipes old rules and sets the new sequence.
    Multi-admin: requires dual-control approval via AdminChangeRequest.
    Returns saved policies + coverage gap warnings. """
    
    amount_based_types = {"AMOUNT_OVER", "AMOUNT_RANGE"}
    amount_currencies = set()
    has_dept_match = False
    
    for p in policies_in:
        if p.condition_type in amount_based_types and p.currency_id:
            amount_currencies.add(p.currency_id)
        if p.condition_type == "DEPT_MATCH":
            has_dept_match = True
            
    if len(amount_currencies) > 1:
        raise HTTPException(
            status_code=400,
            detail="All amount-based approval steps must use the same currency. "
                   "Please set the same currency for all Amount Over and Amount Range conditions."
        )



    # Serialize policies for the change payload
    new_val = [p.model_dump() for p in policies_in]

    change_req, auto_approved = _create_governed_change(
        db, current_user.customer_id, current_user.user_id,
        "APPROVAL_MATRIX_UPDATE", {"new_value": new_val}
    )

    if auto_approved:
        # _apply_admin_change already applied the policies — reload them
        new_policies = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == current_user.customer_id
        ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()
        warnings = _detect_coverage_gaps(policies_in)
        return {"policies": new_policies, "warnings": warnings}

    # Multi-admin: return 202 — change is pending
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=202,
        content={
            "message": "Approval matrix change submitted for approval by a second administrator.",
            "change_request_id": change_req.id,
            "status": "PENDING"
        }
    )

# ==============================================================================
# 6. APPROVAL ACTIONS
# ==============================================================================


@router.get("/requests/{request_id}/approval-roadmap")
def get_approval_roadmap(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Returns the full approval lifecycle roadmap for a request."""
    return issuance_service.get_approval_roadmap(db, request_id)

@router.get("/my-pending-approvals", response_model=List[IssuanceRequestOut])
def get_my_pending_approvals(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """
    Returns issuance requests where the current user is a designated approver.
    Queries pending_approver_users JSONB field using proper containment.
    """
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy import cast, text
    
    # Use PostgreSQL JSONB @> operator to check if array contains user_id
    # This avoids substring false positives (e.g., user 2 matching "[42]")
    requests = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.status == "PENDING_APPROVAL",
        IssuanceRequest.is_deleted == False,
        IssuanceRequest.pending_approver_users.cast(JSONB).contains([current_user.user_id])
    ).order_by(IssuanceRequest.created_at.desc()).all()
    
    print(f"[DEBUG APPROVAL] my-pending-approvals: user_id={current_user.user_id}, found {len(requests)} requests: {[(r.id, r.pending_approver_users) for r in requests]}")
    
    return requests


@router.get("/my-approval-history", response_model=List[IssuanceRequestOut])
def get_my_approval_history(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """
    Returns issuance requests relevant to this approver:
    - PENDING_APPROVAL: only if user is in pending_approver_users
    - All other statuses: only if user has acted on them (in approval_chain_audit)
    """
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy.dialects.postgresql import JSONB

    requests = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.is_deleted == False,
        IssuanceRequest.status.notin_(["DRAFT"]),
    ).order_by(IssuanceRequest.created_at.desc()).all()

    uid = current_user.user_id
    filtered = []
    for req in requests:
        if req.status == "PENDING_APPROVAL":
            # Only show if this user is a designated approver for the current step
            approvers = [int(x) for x in (req.pending_approver_users or [])]
            if uid not in approvers:
                continue
        else:
            # For completed/rejected: only show if user participated in the audit trail
            audit = req.approval_chain_audit or []
            participated = any(
                entry.get("user_id") == uid
                for entry in audit
                if isinstance(entry, dict) and entry.get("action") in (
                    "APPROVED_STEP", "REJECTED", "REVISION_REQUIRED"
                )
            )
            if not participated:
                continue
        filtered.append(req)

    return filtered

@router.post("/requests/{request_id}/submit", response_model=IssuanceRequestOut)
async def submit_request_for_approval(
    request_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """ User submits DRAFT -> PENDING_APPROVAL """
    result = issuance_service.submit_for_approval(db, request_id, current_user.user_id)
    
    print(f"[DEBUG EMAIL] internal_submit: status={result.status}, approvers={result.pending_approver_users}")
    
    # --- Notification (matching corporate_admin.py create_user pattern exactly) ---
    if result.status == "PENDING_APPROVAL" and result.pending_approver_users:
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails
        from app.models import User
        import os
        
        email_settings, _ = get_customer_email_settings(db, result.customer_id)
        submitter = db.query(User).filter(User.id == current_user.user_id).first()
        currency = result.currency.iso_code if result.currency else "N/A"
        approver_ids = [int(uid) for uid in result.pending_approver_users]
        approver_emails = _get_user_emails(db, approver_ids)
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
        
        print(f"[DEBUG EMAIL] internal_submit: approver_ids={approver_ids}, emails={approver_emails}, host={email_settings.smtp_host}")
        
        if approver_emails:
            submitter_email = submitter.email if submitter else result.requestor_email
            subject = f"ACTION REQUIRED: LG Request {result.serial_number} Awaiting Approval"
            body = f"""
            <html>
            <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <h2 style="color: #1a56db; margin-top: 0;">🔔 New Approval Request</h2>
                    <p>A new LG issuance request has been submitted and requires your approval.</p>
                    <div style="background: #f8fafc; border-left: 4px solid #1a56db; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{result.serial_number}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {result.amount}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{result.beneficiary_name}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Submitted by:</td><td style="padding: 4px 0;">{submitter_email or 'External Requestor'}</td></tr>
                        </table>
                    </div>
                    <div style="text-align: center; margin: 25px 0;">
                        <a href="{frontend_url}/corporate-admin/approval-inbox" style="padding: 12px 30px; background: #1a56db; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Review Request</a>
                    </div>
                    <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                    <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
                </div>
            </body>
            </html>
            """
            background_tasks.add_task(
                send_email,
                db,
                approver_emails,
                subject,
                body,
                {},
                email_settings,
            )
            print(f"[DEBUG EMAIL] internal_submit: background_task added for {approver_emails}")
    
    # --- In-App Notification (additive — does not replace email) ---
    if result.status == "PENDING_APPROVAL" and result.pending_approver_users:
        from app.services.notification_service import notify_request_submitted
        from app.models import User
        submitter = db.query(User).filter(User.id == current_user.user_id).first()
        submitter_name = submitter.email if submitter else "External Requestor"
        approver_ids = [int(uid) for uid in result.pending_approver_users]
        notify_request_submitted(
            db, approver_ids, result.serial_number,
            submitter_name, current_user.user_id, request_id
        )
    
    return result

@router.post("/requests/{request_id}/approve", response_model=IssuanceRequestOut)
def approve_request_action(
    request_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """ 
    Approver (Manager) approves the request. 
    Moves to Next Step OR 'APPROVED_INTERNAL'.
    """
    result = issuance_service.approve_request(db, request_id, current_user.user_id)
    currency = result.currency.iso_code if result.currency else "N/A"
    
    # --- Resolve email data NOW while DB session is active (not in background task) ---
    from app.core.email_service import send_email, get_customer_email_settings
    from app.services.issuance_notifications import _get_user_email, _get_user_emails, _base_url
    
    email_settings, _ = get_customer_email_settings(db, result.customer_id)
    
    # --- Notification 1: Tell submitter their request was approved ---
    to_emails = []
    submitter_email = _get_user_email(db, result.requestor_user_id)
    if submitter_email:
        to_emails.append(submitter_email)
    if result.requestor_email and result.requestor_email not in to_emails:
        to_emails.append(result.requestor_email)
    
    if to_emails:
        is_final = result.status == "APPROVED_INTERNAL"
        status_label = "Fully Approved ✅" if is_final else "Step Approved — Proceeding to Next Approver"
        link = f"{_base_url()}/corporate-admin/issuance/requests"
        subject = f"LG Request {result.serial_number} — {status_label}"
        body = f"""
        <html>
        <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #16a34a; margin-top: 0;">✅ Request Approved</h2>
                <p>Your LG issuance request has been <strong>{status_label.lower()}</strong>.</p>
                <div style="background: #f0fdf4; border-left: 4px solid #16a34a; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{result.serial_number}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {result.amount}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{result.beneficiary_name}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Status:</td><td style="padding: 4px 0; font-weight: bold; color: #16a34a;">{result.status}</td></tr>
                    </table>
                </div>
                {"<p>Your request is now ready for issuance execution.</p>" if is_final else "<p>The request is moving to the next approval step.</p>"}
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{link}" style="padding: 12px 30px; background: #16a34a; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">View Status</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
            </div>
        </body>
        </html>
        """
        background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings)
    
    # --- Notification 2: If still pending, notify next set of approvers ---
    if result.status == "PENDING_APPROVAL" and result.pending_approver_users:
        approver_ids = [int(uid) for uid in result.pending_approver_users]
        approver_emails = _get_user_emails(db, approver_ids)
        if approver_emails:
            link2 = f"{_base_url()}/corporate-admin/approval-inbox"
            subject2 = f"ACTION REQUIRED: LG Issuance Request {result.serial_number} Awaiting Your Approval"
            body2 = f"""
            <html>
            <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <h2 style="color: #1a56db; margin-top: 0;">🔔 New Approval Request</h2>
                    <p>An LG issuance request requires your approval.</p>
                    <div style="background: #f8fafc; border-left: 4px solid #1a56db; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{result.serial_number}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {result.amount}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{result.beneficiary_name}</td></tr>
                        </table>
                    </div>
                    <div style="text-align: center; margin: 25px 0;">
                        <a href="{link2}" style="padding: 12px 30px; background: #1a56db; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Review Request</a>
                    </div>
                    <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                    <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
                </div>
            </body>
            </html>
            """
            background_tasks.add_task(send_email, db, approver_emails, subject2, body2, {}, email_settings)
    
    # --- In-App Notification (additive — does not replace email) ---
    from app.services.notification_service import notify_request_approved
    from app.models import User
    is_final = result.status == "APPROVED_INTERNAL"
    approver = db.query(User).filter(User.id == current_user.user_id).first()
    approver_name = approver.email if approver else "Approver"
    
    # Notify the submitter
    notify_recipients = []
    if result.requestor_user_id:
        notify_recipients.append(result.requestor_user_id)
    # If still pending, also notify next approvers
    if not is_final and result.pending_approver_users:
        notify_recipients.extend([int(uid) for uid in result.pending_approver_users])
    
    notify_request_approved(
        db, notify_recipients, result.serial_number,
        step_number=0, approver_name=approver_name,
        actor_user_id=current_user.user_id, request_id=request_id,
        is_fully_approved=is_final
    )
    
    # --- Notify external requestor ---
    if result.requestor_email:
        event = "APPROVED_INTERNAL" if is_final else "APPROVED_STEP"
        _send_requestor_status_notification(db, background_tasks, result, event)
    
    return result

@router.post("/requests/{request_id}/reject", response_model=IssuanceRequestOut)
def reject_request_action(
    request_id: int,
    background_tasks: BackgroundTasks,
    body: dict = Body(default={}),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """ Approver rejects the request. """
    rejection_reason = body.get("rejection_reason") if body else None
    result = issuance_service.reject_request(db, request_id, current_user.user_id, rejection_reason=rejection_reason)
    
    # --- Resolve email data NOW while DB session is active ---
    from app.core.email_service import send_email, get_customer_email_settings
    from app.services.issuance_notifications import _get_user_email, _base_url
    from app.models import User
    
    email_settings, _ = get_customer_email_settings(db, result.customer_id)
    rejector = db.query(User).filter(User.id == current_user.user_id).first()
    rejector_email = rejector.email if rejector else "Admin"
    currency = result.currency.iso_code if result.currency else "N/A"
    
    to_emails = []
    submitter_email = _get_user_email(db, result.requestor_user_id)
    if submitter_email:
        to_emails.append(submitter_email)
    if result.requestor_email and result.requestor_email not in to_emails:
        to_emails.append(result.requestor_email)
    
    if to_emails:
        link = f"{_base_url()}/corporate-admin/issuance/requests"
        subject = f"LG Request {result.serial_number} — Rejected ❌"
        body = f"""
        <html>
        <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #dc2626; margin-top: 0;">❌ Request Rejected</h2>
                <p>Your LG issuance request has been rejected by an approver.</p>
                <div style="background: #fef2f2; border-left: 4px solid #dc2626; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{result.serial_number}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {result.amount}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{result.beneficiary_name}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Rejected by:</td><td style="padding: 4px 0;">{rejector_email}</td></tr>
                    </table>
                </div>
                <p>Please review the request details and contact the approver if clarification is needed.</p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{link}" style="padding: 12px 30px; background: #dc2626; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">View Request</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
            </div>
        </body>
        </html>
        """
        background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings)
    
    # --- In-App Notification (additive — does not replace email) ---
    from app.services.notification_service import notify_request_rejected
    notify_recipients = []
    if result.requestor_user_id:
        notify_recipients.append(result.requestor_user_id)
    
    notify_request_rejected(
        db, notify_recipients, result.serial_number,
        rejector_name=rejector_email, reason="See request details",
        actor_user_id=current_user.user_id, request_id=request_id
    )
    
    # --- Notify external requestor ---
    if result.requestor_email:
        _send_requestor_status_notification(db, background_tasks, result, "REQUEST_REJECTED")
    
    return result

# ── Cancellation Request Workflow ──────────────────────────────────────

@router.post("/requests/{request_id}/request-cancellation", response_model=IssuanceRequestOut)
def request_cancellation_action(
    request_id: int,
    body: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """End user or requestor submits a cancellation request with a reason."""
    reason = body.get("reason", "").strip()
    if not reason or len(reason) < 5:
        raise HTTPException(status_code=400, detail="A cancellation reason is required (min 5 characters).")
    return issuance_service.request_cancellation(db, request_id, current_user.user_id, reason)

@router.post("/requests/{request_id}/resolve-cancellation", response_model=IssuanceRequestOut)
def resolve_cancellation_action(
    request_id: int,
    body: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """Admin approves or rejects a cancellation request."""
    approved = body.get("approved", False)
    note = body.get("note", "")
    return issuance_service.resolve_cancellation(db, request_id, current_user.user_id, approved, note)


@router.post("/requests/{request_id}/return-for-revision", response_model=IssuanceRequestOut)
def return_for_revision_action(
    request_id: int,
    payload: ReturnForRevisionPayload,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context)
):
    """
    Approver returns the request for revision instead of rejecting.
    Requestor can edit and re-submit — approval resumes from the same step.
    """
    result = issuance_service.return_for_revision(
        db, request_id, current_user.user_id, payload.revision_notes
    )
    
    # --- Email notification to requestor ---
    from app.core.email_service import send_email, get_customer_email_settings
    from app.services.issuance_notifications import _get_user_email, _base_url
    from app.models import User
    
    email_settings, _ = get_customer_email_settings(db, result.customer_id)
    returner = db.query(User).filter(User.id == current_user.user_id).first()
    returner_email = returner.email if returner else "Approver"
    currency = result.currency.iso_code if result.currency else "N/A"
    
    to_emails = []
    submitter_email = _get_user_email(db, result.requestor_user_id)
    if submitter_email:
        to_emails.append(submitter_email)
    if result.requestor_email and result.requestor_email not in to_emails:
        to_emails.append(result.requestor_email)
    
    if to_emails:
        link = f"{_base_url()}/corporate-admin/issuance/requests"
        notes_html = f"<p><strong>Revision Notes:</strong> {payload.revision_notes}</p>" if payload.revision_notes else ""
        subject = f"LG Request {result.serial_number} — Returned for Revision 🔄"
        body = f"""
        <html>
        <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #f59e0b; margin-top: 0;">🔄 Returned for Revision</h2>
                <p>Your LG issuance request has been returned for revision by an approver. Please review the notes, make corrections, and re-submit.</p>
                <div style="background: #fffbeb; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{result.serial_number}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {result.amount}</td></tr>
                        <tr><td style="padding: 4px 0; color: #666;">Returned by:</td><td style="padding: 4px 0;">{returner_email}</td></tr>
                    </table>
                </div>
                {notes_html}
                <p>Once you have made the required changes, please re-submit the request. <strong>Approval will resume from the step that returned it.</strong></p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{link}" style="padding: 12px 30px; background: #f59e0b; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Edit & Resubmit</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
            </div>
        </body>
        </html>
        """
        background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings)
    
    # --- Notify external requestor ---
    if result.requestor_email:
        _send_requestor_status_notification(db, background_tasks, result, "REVISION_REQUIRED")
    
    return result

from app.schemas.schemas_issuance import SuitableFacilityOut

@router.patch("/lg-records/{lg_id}/record-delivery")
@router.post("/lg-records/{lg_id}/record-delivery")
async def record_delivery(
    lg_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Step 5.5a: Record delivery of bank form to bank.
    Optionally requires delivery proof document (configurable per customer).
    Accepts JSON (PATCH/POST) or multipart FormData with optional delivery_proof file (POST).
    Only END_USER (treasury officer) can execute this.
    """
    from app.constants import UserRole
    if current_user.role not in (UserRole.END_USER, UserRole.END_USER.value):
        raise HTTPException(status_code=403, detail="Only treasury end users can record delivery.")
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequestDocument
    from app.crud.crud import log_action
    from app.crud import crud_customer_configuration
    from app.constants import GlobalConfigKey
    import json
    import os

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.delivery_date:
        raise HTTPException(status_code=400, detail="Delivery already recorded.")

    # Block delivery for terminal statuses
    terminal_statuses = ("CANCELLED", "EXPIRED", "RELEASED", "CANCELLED_BY_BANK")
    if lg.status in terminal_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot record delivery — LG is in terminal status '{lg.status}'. Only active LGs can be delivered."
        )

    # Parse payload from either JSON or FormData
    content_type = request.headers.get("content-type", "")
    delivery_proof_file = None
    if "multipart/form-data" in content_type:
        form = await request.form()
        data_str = form.get("data", "{}")
        payload = json.loads(data_str)
        delivery_proof_file = form.get("delivery_proof")
    else:
        payload = await request.json()

    # If file was provided, upload it and create IssuanceRequestDocument
    if delivery_proof_file and lg.request_id:
        try:
            from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
            from app.models.models_issuance import IssuanceRequestDocument
            import uuid
            
            from app.api.v1.endpoints.issuance.base import _make_doc_filename
            delivery_proof_display_name = _make_doc_filename(
                "DELIVERY_PROOF",
                lg.lg_ref_number or f"LG-{lg_id}",
                delivery_proof_file.filename or "proof"
            )
            
            bucket_name = os.environ.get("GCS_BUCKET_NAME") or GCS_BUCKET_NAME or "lg_custody_bucket"
            file_ext = delivery_proof_file.filename.rsplit(".", 1)[-1] if "." in delivery_proof_file.filename else "pdf"
            
            # Organized path: customer / requests / req_id / lg / doc_type_slug / file
            from app.crud.crud_lg_document import _slugify_doc_type
            doc_type_slug = _slugify_doc_type("DELIVERY_PROOF")
            req_folder = lg.request_id if lg.request_id else "unlinked"
            gcs_path = f"customer_{current_user.customer_id}/requests/{req_folder}/lg_{lg_id}/{doc_type_slug}/{delivery_proof_display_name}"
            file_content = await delivery_proof_file.read()
            stored_uri = await _upload_to_gcs(bucket_name, gcs_path, file_content, delivery_proof_file.content_type)

            if stored_uri:
                new_doc = IssuanceRequestDocument(
                    request_id=lg.request_id,
                    document_type="DELIVERY_PROOF",
                    file_name=delivery_proof_display_name,
                    file_path=stored_uri,
                    uploaded_by=current_user.user_id,
                )
                db.add(new_doc)
                db.flush()
        except Exception as e:
            logger.warning(f"Failed to upload delivery proof: {e}")

    # Check if delivery proof is mandatory
    proof_required_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_DELIVERY
    )
    proof_required = (proof_required_config or {}).get("effective_value", "false").lower() == "true"

    if proof_required:
        # Check if a DELIVERY_PROOF document exists for the related request
        request_id = lg.request_id
        if request_id:
            proof_doc = db.query(IssuanceRequestDocument).filter(
                IssuanceRequestDocument.request_id == request_id,
                IssuanceRequestDocument.document_type == "DELIVERY_PROOF",
                IssuanceRequestDocument.is_deleted == False
            ).first()
            if not proof_doc:
                raise HTTPException(
                    status_code=400,
                    detail="Delivery proof document is required before recording delivery. Please upload a scanned copy with bank receiving stamp."
                )

    from datetime import date
    lg.delivery_date = payload.get("delivery_date") or date.today().isoformat()
    lg.delivery_method = payload.get("delivery_method", "HAND_DELIVERY")
    lg.delivery_notes = payload.get("delivery_notes")
    lg.status = "DELIVERED_TO_BANK"

    log_action(db, current_user.user_id, "ISSUANCE_DELIVERY_RECORDED", "IssuedLGRecord", lg.id,
               {"delivery_method": lg.delivery_method, "delivery_date": str(lg.delivery_date)},
               current_user.customer_id)
               
    db.commit()

    return {
        "message": "Delivery recorded successfully.",
        "id": lg.id,
        "status": lg.status,
        "delivery_date": str(lg.delivery_date),
        "delivery_method": lg.delivery_method
    }


@router.patch("/lg-records/{lg_id}/record-bank-reply")
async def record_bank_reply(
    lg_id: int,
    background_tasks: BackgroundTasks,
    bank_reply_type: Opt[str] = Form(None),
    bank_reply_date: Opt[str] = Form(None),
    bank_reply_notes: Opt[str] = Form(None),
    bank_lg_number: Opt[str] = Form(None),
    bank_lg_amount: Opt[str] = Form(None),
    bank_lg_issue_date: Opt[str] = Form(None),
    bank_lg_expiry_date: Opt[str] = Form(None),
    issue_cancellation_letter: Opt[str] = Form(None),
    bank_reply_file: Opt[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Step 5.5b: Record bank's reply to issuance request.
    Reply types: LG_ISSUED, INQUIRY, REJECTED, NO_RESPONSE
    """
    from app.constants import UserRole
    if current_user.role not in (UserRole.END_USER, UserRole.END_USER.value):
        raise HTTPException(status_code=403, detail="Only treasury end users can record bank replies.")
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceRequestDocument
    from app.models.models import Bank, Currency, Customer, CustomerEntity
    from app.crud.crud import log_action
    from datetime import date, datetime
    import os
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    import uuid

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    reply_type = bank_reply_type
    if reply_type not in ("LG_ISSUED", "INQUIRY", "REJECTED", "NO_RESPONSE", "CANCELLED_BY_USER"):
        raise HTTPException(status_code=400, detail="Invalid reply type. Must be: LG_ISSUED, INQUIRY, REJECTED, NO_RESPONSE, CANCELLED_BY_USER")

    # If a file was uploaded (e.g., REJECTED attachment, or Issued copy), save it to the request documents
    if bank_reply_file and bank_reply_file.filename and lg.request_id:
        file_bytes = await bank_reply_file.read()
        file_ext = bank_reply_file.filename.split(".")[-1] if "." in bank_reply_file.filename else "pdf"
        sanitized_orig = "".join([c if c.isalnum() else "_" for c in bank_reply_file.filename.split(".")[0]])
        safe_ref = (lg.lg_ref_number or "REQ").replace("/", "-").replace(" ", "_")
        date_str = date.today().strftime("%Y%m%d")
        
        # Determine the appropriate document type based on the reply
        doc_type = "BANK_REPLY"
        if reply_type == "REJECTED":
            doc_type = "BANK_REJECTION_NOTICE"
        elif reply_type == "LG_ISSUED":
            doc_type = "BANK_LG_COPY"
        elif reply_type == "INQUIRY":
            doc_type = "BANK_INQUIRY"
        
        # Format: DOC_TYPE_LG-REF-123_20260325_filename.pdf
        new_filename = f"{doc_type}_{safe_ref}_{date_str}_{sanitized_orig}.{file_ext}"
        
        from app.crud.crud_lg_document import _slugify_doc_type
        doc_type_slug = _slugify_doc_type(doc_type)
        gcs_path = f"customer_{current_user.customer_id}/requests/{lg.request_id}/lg_{lg.id}/{doc_type_slug}/{new_filename}"
        
        try:
            gcs_url = await _upload_to_gcs(GCS_BUCKET_NAME, gcs_path, file_bytes, bank_reply_file.content_type)
            if gcs_url:
                doc = IssuanceRequestDocument(
                    request_id=lg.request_id,
                    document_type=doc_type,
                    file_name=new_filename,
                    file_path=gcs_url,
                    uploaded_by=current_user.user_id,
                )
                db.add(doc)
                db.flush()
        except Exception as e:
            # Non-fatal if GCS upload fails, could log it
            pass

    # ── INQUIRY: append note only, keep step open ──
    if reply_type == "INQUIRY":
        inquiry_entry = {
            "date": bank_reply_date or date.today().isoformat(),
            "notes": bank_reply_notes or "",
            "type": "INQUIRY",
            "logged_by_user_id": current_user.user_id,
            "created_at": datetime.utcnow().isoformat(),
        }
        current_log = list(lg.bank_inquiry_log or [])
        current_log.append(inquiry_entry)
        lg.bank_inquiry_log = current_log
        # Do NOT set bank_reply_type — keeps bank reply selector open
        # Status stays as whatever it was (PENDING_BANK_REPLY or similar)

        log_action(db, current_user.user_id, "ISSUANCE_BANK_INQUIRY_NOTED", "IssuedLGRecord", lg.id,
                   {"inquiry_notes": inquiry_entry["notes"], "inquiry_date": inquiry_entry["date"]},
                   current_user.customer_id)

        return {
            "message": "Bank inquiry noted. You can continue to record the final bank reply when ready.",
            "id": lg.id,
            "status": lg.status,
            "bank_reply_type": None,  # Not finalized
            "inquiry_count": len(current_log),
        }

    # ── LG_ISSUED: normal flow → pending verification ──
    lg.bank_reply_type = reply_type
    lg.bank_reply_date = bank_reply_date or date.today().isoformat()
    lg.bank_reply_notes = bank_reply_notes

    if reply_type == "LG_ISSUED":
        def _clean_date(d_str):
            if not d_str:
                return None
            s = str(d_str).strip().replace('"', '').replace("'", "")
            if s.startswith("0000") or s.lower() in ("null", "none"):
                return None
            return s

        lg.bank_lg_number = bank_lg_number
        lg.bank_lg_issue_date = _clean_date(bank_lg_issue_date)
        lg.bank_lg_expiry_date = _clean_date(bank_lg_expiry_date)
        lg.bank_lg_amount = bank_lg_amount or None
        # D2: Populate issue_date from bank's confirmed issue date
        lg.issue_date = lg.bank_lg_issue_date or date.today()
        lg.status = "LG_ISSUED"
        lg.verification_status = "PENDING"
        # Also set the request to COMPLETED since bank confirmed issuance
        if lg.request_id:
            request_obj = db.query(IssuanceRequest).get(lg.request_id)
            if request_obj and request_obj.status == "INTERNAL_PROCESSING":
                request_obj.status = "COMPLETED"
                request_obj.locked_for_issuance = False

    # ── REJECTED / NO_RESPONSE / CANCELLED_BY_USER: close this LG, reopen request ──
    elif reply_type in ("REJECTED", "NO_RESPONSE", "CANCELLED_BY_USER"):
        status_map = {"REJECTED": "BANK_REJECTED", "NO_RESPONSE": "SLA_EXCEEDED", "CANCELLED_BY_USER": "CANCELLED"}
        lg.status = status_map[reply_type]

        # Reopen the original IssuanceRequest for reprocessing
        if lg.request_id:
            request_obj = db.query(IssuanceRequest).get(lg.request_id)
            if request_obj:
                previous_status = request_obj.status
                request_obj.status = "APPROVED_INTERNAL"  # Back to "Ready for Bank"
                request_obj.lg_record_id = None   # Unlink from this failed LG

                # Release facility exposure — free the held capacity
                from app.models.models_issuance import IssuanceExposureEntry
                db.query(IssuanceExposureEntry).filter(
                    IssuanceExposureEntry.request_id == lg.request_id,
                    IssuanceExposureEntry.is_active == True
                ).update({"is_active": False})
                request_obj.selected_sub_limit_id = None
                request_obj.locked_for_issuance = False


                # Add audit trail entry
                import json as _json
                audit = list(request_obj.approval_chain_audit or [])
                audit.append({
                    "action": {"REJECTED": "BANK_REJECTED", "NO_RESPONSE": "BANK_NO_RESPONSE", "CANCELLED_BY_USER": "USER_CANCELLED"}.get(reply_type, reply_type),
                    "user_id": current_user.user_id,
                    "note": f"Bank {reply_type.lower().replace('_', ' ')}: {bank_reply_notes or ''}. Request reopened for reprocessing.",
                    "previous_status": previous_status,
                    "lg_record_id": lg.id,
                    "timestamp": datetime.utcnow().isoformat()
                })
                request_obj.approval_chain_audit = audit

    # ── Generate cancellation notice if requested (NO_RESPONSE or CANCELLED_BY_USER) ──
    cancellation_letter_generated = False
    is_cancellation_requested = str(issue_cancellation_letter).lower() == 'true' if issue_cancellation_letter else False
    if reply_type in ("NO_RESPONSE", "CANCELLED_BY_USER") and is_cancellation_requested:
        try:
            from app.core.document_generator import generate_pdf_from_html
            from app.models.models import Template as TemplateModel

            cancel_template = db.query(TemplateModel).filter(
                TemplateModel.action_type == "ISSUANCE_NO_RESPONSE_CANCELLATION",
                TemplateModel.is_global == True,
                TemplateModel.is_deleted == False,
                TemplateModel.is_notification_template == False,
            ).first()

            if cancel_template:
                # Gather request details
                request_obj_for_letter = db.query(IssuanceRequest).get(lg.request_id) if lg.request_id else None
                bank = db.query(Bank).get(lg.bank_id) if lg.bank_id else None
                currency = db.query(Currency).get(lg.currency_id) if lg.currency_id else None
                entity = db.query(CustomerEntity).get(lg.issuing_entity_id) if lg.issuing_entity_id else None
                customer = db.query(Customer).get(lg.customer_id)

                template_data = {
                    "bank_name": bank.name if bank else "N/A",
                    "bank_address": bank.address if bank else "N/A",
                    "lg_ref": lg.lg_ref_number or "N/A",
                    "amount": f"{float(lg.current_amount):,.2f}" if lg.current_amount else "N/A",
                    "currency": currency.iso_code if currency else "N/A",
                    "currency_symbol": currency.symbol if currency else "",
                    "beneficiary": lg.beneficiary_name or "N/A",
                    "beneficiary_address": lg.beneficiary_address or "",
                    "requested_issue_date": str(lg.requested_issue_date) if lg.requested_issue_date else "N/A",
                    "expiry_date": str(lg.expiry_date) if lg.expiry_date else "N/A",
                    "company_name": customer.name if customer else "N/A",
                    "company_address": (entity.address if entity and entity.address else customer.address) if customer else "N/A",
                    "entity_name": entity.entity_name if entity else "N/A",
                    "current_date": date.today().strftime("%Y-%m-%d"),
                    "delivery_date": str(lg.delivery_date) if lg.delivery_date else "N/A",
                    "original_notes": bank_reply_notes or "",
                }

                generated_html = cancel_template.content
                for key, value in template_data.items():
                    generated_html = generated_html.replace(f"{{{{{key}}}}}", str(value) if value else "")

                pdf_filename = f"cancellation_notice_{lg.lg_ref_number}_{date.today().isoformat()}"
                generated_pdf_bytes = await generate_pdf_from_html(generated_html, pdf_filename)

                # Upload to GCS; fall back to local tempdir if GCS is unavailable
                from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
                gcs_path = f"issuance/{lg.customer_id}/cancellation_notices/{pdf_filename}.pdf"
                pdf_path = None
                try:
                    gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, gcs_path, generated_pdf_bytes, "application/pdf")
                    if gcs_uri:
                        pdf_path = gcs_uri
                    else:
                        raise Exception("GCS upload returned None")
                except Exception as gcs_err:
                    import os, tempfile
                    logger.warning(f"GCS upload failed for cancellation notice, saving locally: {gcs_err}")
                    output_dir = os.path.join(tempfile.gettempdir(), "Output", "Cancellation_Notices")
                    os.makedirs(output_dir, exist_ok=True)
                    pdf_path = os.path.join(output_dir, f"{pdf_filename}.pdf")
                    with open(pdf_path, "wb") as f:
                        f.write(generated_pdf_bytes)

                lg.cancellation_notice = {
                    "generated_at": datetime.utcnow().isoformat(),
                    "pdf_path": pdf_path,
                    "template_id": cancel_template.id,
                    "generated_by_user_id": current_user.user_id,
                    "bank_name": bank.name if bank else None,
                    "delivery_date": None,
                    "delivery_method": None,
                    "delivery_notes": None,
                    "bank_reply_date": None,
                    "bank_reply_notes": None,
                }
                cancellation_letter_generated = True

                log_action(db, current_user.user_id, "ISSUANCE_CANCELLATION_NOTICE_GENERATED",
                           "IssuedLGRecord", lg.id,
                           {"lg_ref": lg.lg_ref_number, "bank": bank.name if bank else None,
                            "pdf_path": pdf_path},
                           current_user.customer_id)
                logger.info(f"Cancellation notice generated for LG {lg.id} ({lg.lg_ref_number})")
            else:
                logger.warning("No ISSUANCE_NO_RESPONSE_CANCELLATION template found in DB. Skipping letter generation.")
        except Exception as e:
            logger.error(f"Failed to generate cancellation notice for LG {lg.id}: {e}", exc_info=True)
            # Don't fail the whole request — the NO_RESPONSE recording still succeeds

    log_action(db, current_user.user_id, "ISSUANCE_BANK_REPLY_RECORDED", "IssuedLGRecord", lg.id,
               {"reply_type": reply_type, "bank_lg_number": lg.bank_lg_number},
               current_user.customer_id)

    # Notify requestor (all reply types except INQUIRY get email)
    if lg.request_id:
        request = db.query(IssuanceRequest).get(lg.request_id)
        if request and request.requestor_email:
            _send_requestor_status_notification(
                db, background_tasks, request, reply_type, lg
            )

    return {
        "message": f"Bank reply recorded: {reply_type}",
        "id": lg.id,
        "status": lg.status,
        "bank_reply_type": reply_type,
        "bank_lg_number": lg.bank_lg_number,
        "request_reopened": reply_type in ("REJECTED", "NO_RESPONSE"),
        "cancellation_letter_generated": cancellation_letter_generated,
        "cancellation_notice_download_url": f"/api/v1/issuance/lg-records/{lg.id}/cancellation-notice-pdf" if cancellation_letter_generated else None,
    }

# ── Cancellation Notice Endpoints ─────────────────────────────────────

@router.get("/lg-records/{lg_id}/cancellation-notice-pdf")
async def download_cancellation_notice_pdf(
    lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Download the generated cancellation notice PDF (GCS signed URL or local FileResponse)."""
    from app.models.models_issuance import IssuedLGRecord
    from fastapi.responses import FileResponse, RedirectResponse
    import os

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")
    if not lg.cancellation_notice or not lg.cancellation_notice.get("pdf_path"):
        raise HTTPException(status_code=404, detail="No cancellation notice has been generated for this LG.")

    pdf_path = lg.cancellation_notice["pdf_path"]
    filename = f"cancellation_notice_{lg.lg_ref_number}.pdf"

    # GCS path — generate a signed URL
    if pdf_path.startswith("gs://"):
        from app.core.ai_integration import generate_signed_gcs_url
        signed_url = await generate_signed_gcs_url(pdf_path, expiration=3600)
        if not signed_url:
            raise HTTPException(status_code=500, detail="Could not generate download link.")
        return {"download_url": signed_url, "filename": filename}

    # Local fallback
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="Cancellation notice PDF file not found on disk.")
    return FileResponse(path=pdf_path, media_type="application/pdf", filename=filename)



@router.patch("/lg-records/{lg_id}/cancellation-notice-delivery")
def record_cancellation_notice_delivery(
    lg_id: int,
    payload: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Record delivery of the cancellation notice to the bank."""
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action
    from datetime import date, datetime

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")
    if not lg.cancellation_notice:
        raise HTTPException(status_code=400, detail="No cancellation notice exists for this LG.")

    notice = dict(lg.cancellation_notice)
    notice["delivery_date"] = payload.get("delivery_date") or date.today().isoformat()
    notice["delivery_method"] = payload.get("delivery_method", "HAND_DELIVERY")
    notice["delivery_notes"] = payload.get("delivery_notes", "")
    notice["delivered_by_user_id"] = current_user.user_id
    lg.cancellation_notice = notice

    log_action(db, current_user.user_id, "ISSUANCE_CANCELLATION_NOTICE_DELIVERED",
               "IssuedLGRecord", lg.id,
               {"lg_ref": lg.lg_ref_number, "delivery_date": notice["delivery_date"],
                "delivery_method": notice["delivery_method"]},
               current_user.customer_id)

    return {
        "message": "Cancellation notice delivery recorded.",
        "id": lg.id,
        "cancellation_notice": lg.cancellation_notice,
    }


@router.patch("/lg-records/{lg_id}/cancellation-notice-reply")
def record_cancellation_notice_reply(
    lg_id: int,
    payload: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Record bank reply to the cancellation notice."""
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action
    from datetime import date, datetime

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")
    if not lg.cancellation_notice:
        raise HTTPException(status_code=400, detail="No cancellation notice exists for this LG.")

    notice = dict(lg.cancellation_notice)
    notice["bank_reply_date"] = payload.get("bank_reply_date") or date.today().isoformat()
    notice["bank_reply_notes"] = payload.get("bank_reply_notes", "")
    notice["replied_by_user_id"] = current_user.user_id
    lg.cancellation_notice = notice

    log_action(db, current_user.user_id, "ISSUANCE_CANCELLATION_NOTICE_BANK_REPLY",
               "IssuedLGRecord", lg.id,
               {"lg_ref": lg.lg_ref_number, "bank_reply_date": notice["bank_reply_date"],
                "bank_reply_notes": notice["bank_reply_notes"]},
               current_user.customer_id)

    return {
        "message": "Bank reply to cancellation notice recorded.",
        "id": lg.id,
        "cancellation_notice": lg.cancellation_notice,
    }


# ── Cancel & Reopen (Approval-Gated) ─────────────────────────────────────

@router.post("/lg-records/{lg_id}/request-cancellation")
def request_lg_cancellation(
    lg_id: int,
    payload: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    End user requests cancellation of a bank request.
    Sets LG status to CANCEL_REQUESTED and notifies corp admin for approval.
    """
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action
    from datetime import datetime

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    allowed_statuses = ("INTERNAL_PROCESSING", "DELIVERED_TO_BANK")
    if lg.status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot request cancellation for LG in status '{lg.status}'. Allowed: {allowed_statuses}"
        )

    reason = payload.get("cancel_reason", "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="Cancellation reason is required.")

    # Save metadata for later resolution
    meta = dict(lg.custody_transfer_log or []) if isinstance(lg.custody_transfer_log, dict) else {}
    cancel_meta = {
        "cancel_reason": reason,
        "issue_cancellation_letter": payload.get("issue_cancellation_letter", True),
        "requested_by_user_id": current_user.user_id,
        "requested_at": datetime.utcnow().isoformat(),
        "previous_status": lg.status,
    }

    # Store in metadata_json on the linked IssuanceRequest (IssuedLGRecord has no metadata_json column)
    linked_request = db.query(IssuanceRequest).filter(IssuanceRequest.lg_record_id == lg.id).first()
    if linked_request:
        existing_meta = dict(linked_request.metadata_json or {})
        existing_meta["pending_cancellation"] = cancel_meta
        linked_request.metadata_json = existing_meta

    lg.status = "CANCEL_REQUESTED"

    # Audit trail
    ctl = list(lg.custody_transfer_log or [])
    ctl.append({
        "action": "CANCEL_REQUESTED",
        "user_id": current_user.user_id,
        "reason": reason,
        "previous_status": cancel_meta["previous_status"],
        "timestamp": datetime.utcnow().isoformat(),
    })
    lg.custody_transfer_log = ctl

    log_action(db, current_user.user_id, "LG_CANCEL_REQUESTED",
               "IssuedLGRecord", lg.id,
               {"reason": reason, "previous_status": cancel_meta["previous_status"],
                "issue_letter": cancel_meta["issue_cancellation_letter"]},
               current_user.customer_id)

    # Notify corp admins
    try:
        from app.schemas.all_schemas import SystemNotificationCreate
        from app.crud.crud import crud_notification
        from app.models import models as base_models
        _now = datetime.utcnow()

        admins = db.query(base_models.User).filter(
            base_models.User.customer_id == current_user.customer_id,
            base_models.User.role == "corporate_admin",
            base_models.User.is_deleted == False,
        ).all()

        for admin in admins:
            notif = SystemNotificationCreate(
                content=f"Cancellation requested for LG {lg.lg_ref_number}: {reason}",
                notification_type="LG_CANCEL_REQUEST",
                start_date=_now,
                end_date=_now + __import__('datetime').timedelta(days=30),
                target_user_ids=[admin.id],
                target_customer_ids=[current_user.customer_id],
                link="/corporate-admin/issuance/issued-lgs",
            )
            crud_notification.create_notification(db, obj_in=notif)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to send cancel request notification: {e}")

    db.commit()

    return {
        "message": "Cancellation request submitted for admin approval.",
        "id": lg.id,
        "status": lg.status,
    }


