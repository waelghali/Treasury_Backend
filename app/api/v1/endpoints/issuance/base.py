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
def _read_bank_form_pdf_bytes(form_template) -> bytes:
    """Read bank form template PDF bytes from GCS or local disk."""
    import os
    if not form_template.file_path:
        raise HTTPException(400, "No PDF file associated with this form.")

    if form_template.file_path.startswith("gs://"):
        try:
            from app.core.ai_integration import _get_gcs_client
            path_parts = form_template.file_path[5:].split('/', 1)
            bucket_name, blob_name = path_parts[0], path_parts[1]
            client = _get_gcs_client()
            if not client:
                raise HTTPException(500, "GCS client not available.")
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.download_as_bytes()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Failed to download PDF from cloud storage: {e}")
    elif os.path.exists(form_template.file_path):
        with open(form_template.file_path, "rb") as f:
            return f.read()
    else:
        raise HTTPException(404, "PDF file not found.")

def _send_edit_notifications(db, request, editor: TokenData, metadata: dict):
    """
    Sends FYI notifications after a post-submission edit.
    - Safe edits: log audit entry tagging requestor + prior approvers.
    - Risky edits (re-approval): same, plus a warning that approval chain was reset.
    Uses the existing audit log system which feeds the notification banner.
    """
    from app.crud.crud import log_action
    
    # Gather recipients: requestor + anyone who already approved
    notify_user_ids = set()
    if request.requestor_user_id:
        notify_user_ids.add(request.requestor_user_id)
    
    # Prior approvers from approval_chain_audit
    if request.approval_chain_audit:
        for step in request.approval_chain_audit:
            if step.get('user_id'):
                notify_user_ids.add(step['user_id'])
    
    # Don't notify the editor themselves
    notify_user_ids.discard(editor.user_id)
    
    if not notify_user_ids:
        return  # No one to notify
    
    re_approval = metadata.get('re_approval_triggered', False)
    changed_fields = metadata.get('risky_fields_changed', []) + metadata.get('safe_fields_changed', [])
    reason = metadata.get('change_reason', '')
    
    action_type = "EDIT_RE_APPROVAL_NOTICE" if re_approval else "EDIT_FYI_NOTICE"
    
    log_action(
        db,
        user_id=editor.user_id,
        action_type=action_type,
        entity_type="IssuanceRequest",
        entity_id=request.id,
        details={
            "serial_number": request.serial_number,
            "editor_role": editor.role if hasattr(editor, 'role') else "unknown",
            "changed_fields": changed_fields,
            "reason": reason,
            "re_approval_triggered": re_approval,
            "notify_user_ids": list(notify_user_ids),
        },
        customer_id=editor.customer_id
    )


# ==============================================================================
# REQUESTS MANAGEMENT (CORE)
# ==============================================================================

def _detect_coverage_gaps(policies_in) -> list:
    """
    Analyzes a set of workflow policies for coverage gaps.
    Returns a list of warning strings for the admin.
    """
    from decimal import Decimal, InvalidOperation

    warnings = []
    has_always = False
    amount_ranges = []  # [(min, max)] — max=None means open-ended
    amount_over_thresholds = []

    for p in policies_in:
        ct = p.condition_type
        if ct in ("ALWAYS", "ANY_DEPARTMENT"):
            has_always = True
        elif ct == "AMOUNT_RANGE" and p.condition_value:
            try:
                raw = str(p.condition_value).strip().strip("()")
                if "," in raw:
                    parts = raw.split(",")
                else:
                    parts = raw.split("-")
                min_val = Decimal(parts[0].strip()) if parts[0].strip() else Decimal("0")
                max_val = Decimal(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else None
                amount_ranges.append((min_val, max_val))
            except (InvalidOperation, IndexError):
                pass
        elif ct == "AMOUNT_OVER" and p.condition_value:
            try:
                threshold = Decimal(str(p.condition_value))
                amount_over_thresholds.append(threshold)
            except (InvalidOperation, ValueError):
                pass

    # Only analyze if there are amount-based policies but no catch-all
    if (amount_ranges or amount_over_thresholds) and not has_always:
        # Check if there's an open-ended upper range
        has_open_upper = any(max_val is None for _, max_val in amount_ranges)
        has_amount_over = len(amount_over_thresholds) > 0

        if amount_ranges and not has_open_upper and not has_amount_over:
            # Find the highest upper bound
            max_upper = max(
                (max_val for _, max_val in amount_ranges if max_val is not None),
                default=Decimal("0")
            )
            warnings.append(
                f"Coverage gap detected: Requests with amounts above {max_upper:,.0f} "
                f"do not match any approval rule and will be blocked. "
                f"Consider adding an 'Amount Greater Than {max_upper:,.0f}' rule "
                f"or an 'Always' rule to cover all amounts."
            )

        # Check for gaps between ranges (e.g., 0-50K and 100K-200K → gap at 50K-100K)
        if len(amount_ranges) > 1:
            sorted_ranges = sorted(amount_ranges, key=lambda x: x[0])
            for i in range(len(sorted_ranges) - 1):
                _, curr_max = sorted_ranges[i]
                next_min, _ = sorted_ranges[i + 1]
                if curr_max is not None and next_min > curr_max:
                    warnings.append(
                        f"Coverage gap detected: Requests with amounts between "
                        f"{curr_max:,.0f} and {next_min:,.0f} do not match any rule "
                        f"and will be blocked."
                    )

    # Edge case: policies exist but ALL are conditional (no ALWAYS rule)
    if policies_in and not has_always:
        conditional_only = all(
            p.condition_type not in ("ALWAYS", "ANY_DEPARTMENT")
            for p in policies_in
        )
        if conditional_only:
            warnings.append(
                "All approval rules have conditions. Requests that don't match "
                "any condition will be blocked. Consider adding an 'Always' rule "
                "as a catch-all to ensure all requests have an approval path."
            )

    return warnings

class ReturnForRevisionPayload(BaseModel):
    revision_notes: Optional[str] = None

def _make_doc_filename(doc_type: str, ref: str, original_filename: str) -> str:
    """Build a meaningful, standardized display filename for stored issuance documents.
    Format: {DOC_TYPE}_{REF}_{YYYYMMDD}_{sanitized_original}.{ext}
    Example: DELIVERY_PROOF_LG-TEMP-2026-0004-R3_20260325_bank_receipt.pdf
    """
    import re as _re
    from datetime import date as _date
    today = _date.today().strftime('%Y%m%d')
    # Get extension
    if original_filename and '.' in original_filename:
        base, ext = original_filename.rsplit('.', 1)
    else:
        base = original_filename or 'document'
        ext = 'pdf'
    # Sanitize: keep alphanumeric, underscore, hyphen; truncate to 40 chars
    clean_base = _re.sub(r'[^\w\-]', '_', base.strip())[:40]
    clean_ref = _re.sub(r'[^\w\-]', '-', str(ref).strip())
    doc_slug = doc_type.upper().replace(' ', '_')
    return f"{doc_slug}_{clean_ref}_{today}_{clean_base}.{ext.lower()}"

@router.get("/requests/{request_id}/documents/{document_id}/download")
async def download_request_document(
    request_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Returns the document — as a signed GCS URL redirect or a direct file stream for local storage."""
    import os
    from app.models.models_issuance import IssuanceRequestDocument
    from fastapi.responses import FileResponse

    doc = db.query(IssuanceRequestDocument).filter(
        IssuanceRequestDocument.id == document_id,
        IssuanceRequestDocument.request_id == request_id,
        IssuanceRequestDocument.is_deleted == False
    ).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    file_path = doc.file_path or ""

    if file_path.startswith("gs://"):
        # Cloud Storage path — generate a signed URL
        from app.core.ai_integration import generate_signed_gcs_url
        signed_url = await generate_signed_gcs_url(file_path, expiration=3600)
        if not signed_url:
            raise HTTPException(status_code=500, detail="Could not generate download link. File may not be accessible.")
        return {
            "file_name": doc.file_name,
            "document_type": doc.document_type,
            "download_url": signed_url
        }
    else:
        # Local file path — serve directly
        # Support both absolute and relative paths
        abs_path = file_path if os.path.isabs(file_path) else os.path.join(os.getcwd(), file_path)
        if not os.path.exists(abs_path):
            raise HTTPException(status_code=404, detail="File not found on server storage.")

        # Determine media type
        ext = abs_path.rsplit(".", 1)[-1].lower() if "." in abs_path else ""
        media_type_map = {
            "pdf": "application/pdf",
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "tiff": "image/tiff",
            "webp": "image/webp",
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }
        media_type = media_type_map.get(ext, "application/octet-stream")

        return FileResponse(
            path=abs_path,
            filename=doc.file_name or os.path.basename(abs_path),
            media_type=media_type,
        )



# ==============================================================================
# ISSUED LGs — List, Reprint, Bank Options
# ==============================================================================


def _get_lg_copy_docs(db, request_id):
    """Get BANK_LG_COPY documents for a given request."""
    from app.models.models_issuance import IssuanceRequestDocument
    docs = db.query(IssuanceRequestDocument).filter(
        IssuanceRequestDocument.request_id == request_id,
        IssuanceRequestDocument.document_type == "BANK_LG_COPY"
    ).order_by(IssuanceRequestDocument.created_at.desc()).limit(5).all()
    return [{
        "id": d.id,
        "file_name": d.file_name,
        "file_path": d.file_path,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    } for d in docs]


@router.patch("/lg-records/{lg_id}/record-handover")
@router.post("/lg-records/{lg_id}/record-handover")
async def record_handover(
    lg_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Record LG handover to recipient. Default = requestor from original request.
    Accepts JSON (PATCH) or multipart FormData with optional signed_copy file (POST).
    Only END_USER (treasury officer) can execute this.
    """
    from app.constants import UserRole
    if current_user.role not in (UserRole.END_USER, UserRole.END_USER.value):
        raise HTTPException(status_code=403, detail="Only treasury end users can record handover.")
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    from app.core.email_service import send_email, get_customer_email_settings
    from app.crud import crud_customer_configuration
    from app.constants import GlobalConfigKey
    from datetime import date as date_type, datetime
    import os, json

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(404, "Issued LG record not found.")

    # Must be verified first
    if lg.verification_status not in ("MATCHED", "ACCEPTED"):
        raise HTTPException(400, "LG must be verified before handover. Complete verification first.")

    if lg.handover_date:
        raise HTTPException(400, "Handover already recorded for this LG.")

    # Block handover for terminal statuses
    terminal_statuses = ("CANCELLED", "EXPIRED", "RELEASED", "CANCELLED_BY_BANK")
    if lg.status in terminal_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot record handover — LG is in terminal status '{lg.status}'. Only active LGs can be handed over."
        )

    # Parse payload from either JSON or FormData
    content_type = request.headers.get("content-type", "")
    signed_copy_file = None
    if "multipart/form-data" in content_type:
        form = await request.form()
        data_str = form.get("data", "{}")
        payload = json.loads(data_str)
        signed_copy_file = form.get("signed_copy")
    else:
        payload = await request.json()

    # Validate required fields
    recipient_name = payload.get("recipient_name")
    recipient_email = payload.get("recipient_email")
    if not recipient_name or not recipient_email:
        raise HTTPException(400, "Recipient name and email are required.")

    handover_date = payload.get("handover_date", str(date_type.today()))

    # Set handover fields
    lg.handover_date = handover_date
    lg.handover_notes = payload.get("handover_notes")
    lg.handover_by_user_id = current_user.user_id
    lg.recipient_name = recipient_name
    lg.recipient_email = recipient_email
    lg.recipient_department = payload.get("recipient_department")
    lg.recipient_job_title = payload.get("recipient_job_title")
    lg.recipient_phone = payload.get("recipient_phone")
    lg.recipient_employee_id = payload.get("recipient_employee_id")
    lg.recipient_manager_email = payload.get("recipient_manager_email")
    lg.recipient_second_line_manager_email = payload.get("recipient_second_line_manager_email")

    # Upload signed receiving copy if provided
    if signed_copy_file:
        try:
            from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
            from app.models.models_issuance import IssuanceRequestDocument as _IReqDoc
            import uuid
            bucket_name = os.environ.get("GCS_BUCKET_NAME") or GCS_BUCKET_NAME or "lg_custody_bucket"
            file_ext = signed_copy_file.filename.rsplit(".", 1)[-1] if "." in signed_copy_file.filename else "pdf"
            
            # Generate the display name before using it
            handover_display_name = _make_doc_filename(
                "HANDOVER_SIGNED_COPY",
                lg.lg_ref_number or f"LG-{lg_id}",
                signed_copy_file.filename or "signed_copy"
            )
            
            # Organized path: customer / requests / req_id / lg / doc_type_slug / file
            req_id_folder = f"{lg.request_id}" if lg.request_id else "unlinked"
            doc_type_slug = "handover_signed_copy"
            gcs_path = f"customer_{current_user.customer_id}/requests/{req_id_folder}/lg_{lg_id}/{doc_type_slug}/{handover_display_name}"
            file_content = await signed_copy_file.read()
            stored_uri = await _upload_to_gcs(bucket_name, gcs_path, file_content, signed_copy_file.content_type)
            if stored_uri:
                lg.handover_signed_copy_path = stored_uri
                # Also save as an IssuanceRequestDocument for Documents tab visibility
                if lg.request_id:
                    _signed_doc = _IReqDoc(
                        request_id=lg.request_id,
                        document_type="HANDOVER_SIGNED_COPY",
                        file_name=handover_display_name,
                        file_path=stored_uri,
                        uploaded_by=current_user.user_id,
                    )
                    db.add(_signed_doc)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to upload signed copy: {e}")

    lg.status = "ACTIVE"
    db.commit()

    # --- Send email notifications ---
    orig_request = db.query(IssuanceRequest).get(lg.request_id) if lg.request_id else None
    email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
    frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")

    # Collect all email recipients
    email_recipients = set()
    email_recipients.add(recipient_email)  # Always notify recipient
    if orig_request:
        email_recipients.add(orig_request.requestor_email)
        if orig_request.manager_email:
            email_recipients.add(orig_request.manager_email)
    if lg.recipient_manager_email:
        email_recipients.add(lg.recipient_manager_email)
    # Add corporate admins
    from app.models import User
    from app.constants import UserRole
    admins = db.query(User).filter(
        User.customer_id == current_user.customer_id,
        User.role == UserRole.CORPORATE_ADMIN,
        User.is_deleted == False
    ).all()
    for admin in admins:
        email_recipients.add(admin.email)
    # Add delivering user
    delivering_user = db.query(User).get(current_user.user_id)
    if delivering_user:
        email_recipients.add(delivering_user.email)

    email_recipients = [e for e in email_recipients if e]  # Remove None/empty

    subject = f"📦 LG Handover Confirmed — {lg.lg_ref_number}"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #059669; margin-top: 0;">📦 LG Handover Confirmed</h2>
            <div style="background: #f0fdf4; border-left: 4px solid #059669; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;">LG Ref:</td><td style="padding: 4px 0; font-weight: bold;">{lg.lg_ref_number}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Bank LG #:</td><td style="padding: 4px 0;">{lg.bank_lg_number or 'N/A'}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Delivered to:</td><td style="padding: 4px 0; font-weight: bold;">{recipient_name}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Recipient Email:</td><td style="padding: 4px 0;">{recipient_email}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Handover Date:</td><td style="padding: 4px 0;">{handover_date}</td></tr>
                </table>
            </div>
            <p>The Letter of Guarantee has been handed over to the recipient above.</p>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
        </div>
    </body>
    </html>
    """

    try:
        from starlette.background import BackgroundTasks
        bg_tasks = BackgroundTasks()
        bg_tasks.add_task(send_email, db, email_recipients, subject, body, {}, email_settings)
    except Exception:
        pass  # Non-blocking email failure

    return {"message": "LG handover recorded successfully.", "status": lg.status}


# --- Helper: Send requestor status notification ---
def _send_requestor_status_notification(db, background_tasks, request, event_type, lg=None):
    """Send email to requestor about status change. Covers both approval and post-issuance events."""
    from app.core.email_service import send_email, get_customer_email_settings
    import os

    # Derive customer_id from the request or lg object
    customer_id = getattr(request, 'customer_id', None) or (getattr(lg, 'customer_id', None) if lg else None)
    if customer_id:
        email_settings, _ = get_customer_email_settings(db, customer_id)
    else:
        from app.core.email_service import get_global_email_settings
        email_settings = get_global_email_settings()
    frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")

    # Bank reply / post-issuance events (require lg)
    bank_events = {}
    if lg:
        bank_events = {
            "LG_ISSUED": {"emoji": "✅", "title": "Your LG Has Been Issued!", "color": "#10b981",
                           "body": f"The bank has issued your Letter of Guarantee.<br><strong>LG Number:</strong> {lg.bank_lg_number or 'Pending'}"},
            "INQUIRY": {"emoji": "❓", "title": "Bank Inquiry on Your LG Request", "color": "#f59e0b",
                         "body": f"The bank has requested additional information.<br><strong>Details:</strong> {lg.bank_reply_notes or 'Please contact your treasury team.'}"},
            "REJECTED": {"emoji": "❌", "title": "LG Issuance Request Declined", "color": "#ef4444",
                          "body": f"Unfortunately, the bank has declined this issuance request.<br><strong>Reason:</strong> {lg.bank_reply_notes or 'No reason provided.'}"},
            "NO_RESPONSE": {"emoji": "⏰", "title": "Bank SLA Exceeded", "color": "#6b7280",
                             "body": "The bank has not responded within the expected timeframe. Your treasury team is following up."},
            "VERIFIED": {"emoji": "🎉", "title": "LG Verified & Confirmed", "color": "#059669",
                          "body": f"Your Letter of Guarantee has been verified and confirmed.<br><strong>LG Number:</strong> {lg.bank_lg_number or 'N/A'}"},
        }

    # Approval lifecycle events (no lg needed)
    approval_events = {
        "APPROVED_STEP": {"emoji": "👍", "title": "Your LG Request Advanced", "color": "#3b82f6",
                           "body": "Your issuance request has passed an approval step and is moving to the next reviewer."},
        "APPROVED_INTERNAL": {"emoji": "✅", "title": "Your LG Request Has Been Fully Approved!", "color": "#10b981",
                               "body": "Great news! Your issuance request has been fully approved and is now ready for issuance execution by the treasury team."},
        "REQUEST_REJECTED": {"emoji": "❌", "title": "Your LG Request Has Been Rejected", "color": "#ef4444",
                              "body": f"Your issuance request has been rejected by an approver.<br><strong>Notes:</strong> {getattr(request, 'revision_notes', '') or 'Please contact the treasury team for details.'}"},
        "REVISION_REQUIRED": {"emoji": "🔄", "title": "Your LG Request Needs Revision", "color": "#f59e0b",
                                   "body": f"An approver has returned your request for revision. Please review and re-submit.<br>"
                                           f"<strong>Notes:</strong> {getattr(request, 'revision_notes', '') or 'No specific notes provided.'}<br><br>"
                                           f'<a href="{frontend_url}/portal/issuance" style="padding: 10px 25px; background: #f59e0b; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Visit Portal to Edit &amp; Resubmit</a>'},
    }

    all_events = {**bank_events, **approval_events}
    status_info = all_events.get(event_type)

    if not status_info:
        return

    subject = f"{status_info['emoji']} {status_info['title']} — {request.serial_number}"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: {status_info['color']}; margin-top: 0;">{status_info['emoji']} {status_info['title']}</h2>
            <div style="background: #f8fafc; border-left: 4px solid {status_info['color']}; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;">Request:</td><td style="padding: 4px 0; font-weight: bold;">{request.serial_number}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{request.beneficiary_name}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{request.amount}</td></tr>
                </table>
            </div>
            <p>{status_info['body']}</p>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
        </div>
    </body>
    </html>
    """
    background_tasks.add_task(
        send_email, db, [request.requestor_email], subject, body, {}, email_settings
    )


# ==============================================================================
# 9. AI-POWERED LG COPY EXTRACTION
# ==============================================================================

@router.post("/lg-records/{lg_id}/extract-lg-copy")
async def extract_lg_copy(
    lg_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Upload a scanned LG copy (PDF or image). AI extracts LG number, amount,
    dates, beneficiary, etc. Returns extracted fields + comparison with original request.
    """
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(404, "Issued LG record not found")

    # Validate file
    allowed_types = ["application/pdf", "image/jpeg", "image/png", "image/tiff", "image/webp"]
    if file.content_type not in allowed_types:
        raise HTTPException(400, f"Unsupported file type: {file.content_type}. Accepted: PDF, JPEG, PNG, TIFF, WebP")

    file_bytes = await file.read()
    if len(file_bytes) > 10 * 1024 * 1024:  # 10 MB limit
        raise HTTPException(400, "File too large (max 10 MB)")

    # Run AI extraction
    from app.core.ai_integration import process_lg_document_with_ai
    extracted_data, usage_meta = await process_lg_document_with_ai(
        file_bytes=file_bytes,
        mime_type=file.content_type,
        lg_number_hint=lg.lg_ref_number or f"lg_{lg_id}",
        db=db,
        current_user=current_user,
        file_name=file.filename or "lg_copy_scan",
    )

    if not extracted_data:
        raise HTTPException(422, "AI could not extract data from the uploaded document. Please ensure the scan is clear and readable.")

    # Save the uploaded LG copy as a document so admin can view it during review
    if lg.request_id:
        try:
            from app.models.models_issuance import IssuanceRequestDocument
            bank_lg_copy_display_name = _make_doc_filename(
                "BANK_LG_COPY",
                lg.lg_ref_number or f"LG-{lg_id}",
                file.filename or "scan"
            )

            # Upload to GCS
            from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
            gcs_path = f"issuance/{current_user.customer_id}/{lg.request_id}/lg_{lg_id}/bank_lg_copy/{bank_lg_copy_display_name}"
            file_path = None
            try:
                gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, gcs_path, file_bytes, file.content_type or "application/octet-stream")
                if gcs_uri:
                    file_path = gcs_uri
                else:
                    raise Exception("GCS upload returned None")
            except Exception as gcs_err:
                import os
                logger.warning(f"GCS upload failed for BANK_LG_COPY, saving locally: {gcs_err}")
                upload_dir = os.path.join("uploads", "issuance", str(current_user.customer_id), str(lg.request_id), f"lg_{lg_id}", "bank_lg_copy")
                os.makedirs(upload_dir, exist_ok=True)
                file_path = os.path.join(upload_dir, bank_lg_copy_display_name)
                with open(file_path, "wb") as f_out:
                    f_out.write(file_bytes)

            doc = IssuanceRequestDocument(
                request_id=lg.request_id,
                document_type="BANK_LG_COPY",
                file_name=bank_lg_copy_display_name,
                file_path=file_path,
                uploaded_by=current_user.user_id,
            )
            db.add(doc)
            db.commit()
        except Exception as doc_err:
            logger.warning(f"Could not save LG copy document: {doc_err}")


    # Get original request for comparison
    request_obj = None
    if lg.request_id:
        request_obj = db.query(IssuanceRequest).filter(
            IssuanceRequest.id == lg.request_id,
        ).first()

    # Build comparison
    from difflib import SequenceMatcher

    comparison = {"fields": [], "has_discrepancy": False}
    request_amount = float(request_obj.amount) if request_obj and request_obj.amount else None
    request_expiry = str(request_obj.requested_expiry_date) if request_obj and request_obj.requested_expiry_date else None
    request_beneficiary = request_obj.beneficiary_name if request_obj else None

    extracted_amount = extracted_data.get("lgAmount")
    extracted_expiry = extracted_data.get("expiryDate")
    extracted_beneficiary = extracted_data.get("beneficiaryName")

    # Resolve FK fields for comparison
    request_currency = None
    request_lg_type = None
    request_address = request_obj.beneficiary_address if request_obj else None
    request_purpose = request_obj.lg_purpose if request_obj else None
    request_op_status = request_obj.operational_status if request_obj else None

    if request_obj:
        # Resolve currency from currency_id
        if request_obj.currency_id:
            from app.models import Currency
            currency_obj = db.query(Currency).get(request_obj.currency_id)
            request_currency = currency_obj.iso_code if currency_obj else None
        # Resolve LG type from lg_type_id
        if request_obj.lg_type_id:
            from app.models.models_issuance import IssuanceFacility  # lg_types table
            from sqlalchemy import text as sa_text
            lg_type_row = db.execute(sa_text("SELECT name FROM lg_types WHERE id = :id"), {"id": request_obj.lg_type_id}).first()
            request_lg_type = lg_type_row[0] if lg_type_row else None

    extracted_currency = extracted_data.get("currency", "")
    extracted_lg_type = extracted_data.get("lgType", "")
    extracted_address = extracted_data.get("beneficiaryAddress", "")
    extracted_purpose = extracted_data.get("purpose", "")
    extracted_op_status = extracted_data.get("operationalStatus", "")

    def _normalize_date(val):
        """Extract just the YYYY-MM-DD portion from any date/datetime string."""
        if val is None:
            return None
        s = str(val).strip()
        # Try to extract YYYY-MM-DD from the start
        import re as _re
        m = _re.match(r'(\d{4}-\d{2}-\d{2})', s)
        return m.group(1) if m else s

    def _compare(label, requested, extracted_val, severity="HIGH"):
        match = True
        if requested is not None and extracted_val is not None:
            if isinstance(requested, (int, float)):
                match = abs(float(requested) - float(extracted_val)) < 0.01
            else:
                # Normalize dates before comparing to avoid format-only mismatches
                r = _normalize_date(requested)
                e = _normalize_date(extracted_val)
                match = r.lower() == e.lower()
        return {
            "field": label,
            "requested": str(requested) if requested else None,
            "extracted": str(extracted_val) if extracted_val else None,
            "match": match,
            "severity": severity,
        }

    def _name_compare(label, requested, extracted_val, threshold=0.80, severity="HIGH"):
        """Smart name comparison: substring containment OR fuzzy match.
        Handles cases where bank includes a longer/expanded version of the name."""
        match = True
        ratio = None
        if requested and extracted_val:
            r = str(requested).strip().lower()
            e = str(extracted_val).strip().lower()
            # Consider a match if either name is fully contained in the other
            if r in e or e in r:
                match = True
                ratio = 1.0
            else:
                ratio = SequenceMatcher(None, r, e).ratio()
                match = ratio >= threshold
        return {
            "field": label,
            "requested": str(requested) if requested else None,
            "extracted": str(extracted_val) if extracted_val else None,
            "match": match,
            "match_pct": round(ratio * 100, 1) if ratio is not None else None,
            "severity": severity,
        }

    def _fuzzy_compare(label, requested, extracted_val, threshold=0.90, severity="MEDIUM"):
        """Compare using fuzzy matching with SequenceMatcher. Match passes if ratio >= threshold."""
        match = True
        ratio = None
        if requested and extracted_val:
            r = str(requested).strip().lower()
            e = str(extracted_val).strip().lower()
            # Substring containment is always a match (bank may paraphrase or expand)
            if r in e or e in r:
                match = True
                ratio = 1.0
            else:
                ratio = SequenceMatcher(None, r, e).ratio()
                match = ratio >= threshold
        return {
            "field": label,
            "requested": str(requested) if requested else None,
            "extracted": str(extracted_val) if extracted_val else None,
            "match": match,
            "match_pct": round(ratio * 100, 1) if ratio is not None else None,
            "severity": severity,
        }

    # Core comparisons
    comparison["fields"].append(_compare("Amount", request_amount, extracted_amount, "HIGH"))
    comparison["fields"].append(_compare("Expiry Date", request_expiry, extracted_expiry, "HIGH"))
    comparison["fields"].append(_name_compare("Beneficiary Name", request_beneficiary, extracted_beneficiary, 0.80, "HIGH"))

    # Additional comparisons
    comparison["fields"].append(_compare("Currency", request_currency, extracted_currency, "HIGH"))
    comparison["fields"].append(_name_compare("LG Type", request_lg_type, extracted_lg_type, 0.85, "HIGH"))
    comparison["fields"].append(_name_compare("Beneficiary Address", request_address, extracted_address, 0.80, "MEDIUM"))
    # Purpose: threshold 0.50 — banks often wrap the core purpose in formal legal language
    comparison["fields"].append(_fuzzy_compare("Purpose / Description", request_purpose, extracted_purpose, 0.50, "MEDIUM"))

    # Operational status — only compare for advance payment LGs
    if request_lg_type and "advance" in (request_lg_type or "").lower():
        comparison["fields"].append(_compare("Operational Status", request_op_status, extracted_op_status, "HIGH"))

    comparison["has_discrepancy"] = any(not f["match"] for f in comparison["fields"])

    return {
        "extracted": {
            "bank_lg_number": extracted_data.get("lgNumber", ""),
            "bank_lg_amount": extracted_amount,
            "bank_lg_issue_date": extracted_data.get("issuanceDate", ""),
            "bank_lg_expiry_date": extracted_expiry,
            "bank_beneficiary_name": extracted_beneficiary,
            "issuing_bank_name": extracted_data.get("issuingBankName", ""),
            "currency": extracted_currency,
            "lg_type": extracted_lg_type,
            "purpose": extracted_purpose,
            "beneficiary_address": extracted_address,
            "operational_status": extracted_op_status,
        },
        "comparison": comparison,
        "raw_extracted": extracted_data,
        "usage": usage_meta,
    }


# ==============================================================================
# 10. CORRECTION REQUEST LETTER
# ==============================================================================

@router.post("/lg-records/{lg_id}/generate-correction-letter")
async def generate_correction_letter(
    lg_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Generate a formal correction request letter to the bank listing discrepancies
    found during LG verification. Returns a downloadable PDF.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    from app.models import User, Customer
    from app.crud.base import log_action
    from datetime import date as date_type
    from starlette.responses import StreamingResponse
    import io

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(404, "Issued LG record not found.")

    payload = await request.json()
    discrepancies = payload.get("discrepancies", [])
    if not discrepancies:
        raise HTTPException(400, "No discrepancies provided.")

    # Get context
    request_obj = db.query(IssuanceRequest).get(lg.request_id) if lg.request_id else None
    customer = db.query(Customer).get(current_user.customer_id)
    requesting_user = db.query(User).get(current_user.user_id)

    # Get entity name
    entity_name = customer.name if customer else "Our Company"
    if request_obj and request_obj.issuing_entity_id:
        from app.models import CustomerEntity
        entity = db.query(CustomerEntity).get(request_obj.issuing_entity_id)
        if entity:
            entity_name = entity.entity_name

    # Get bank name
    bank_name = "The Bank"
    if request_obj:
        from sqlalchemy import text as sa_text
        if hasattr(request_obj, 'selected_sub_limit_id') and request_obj.selected_sub_limit_id:
            from app.models.models_issuance import IssuanceFacilitySubLimit, IssuanceFacility
            sub = db.query(IssuanceFacilitySubLimit).get(request_obj.selected_sub_limit_id)
            if sub:
                fac = db.query(IssuanceFacility).get(sub.facility_id)
                if fac and fac.bank_id:
                    from app.models import Bank
                    bank = db.query(Bank).get(fac.bank_id)
                    if bank:
                        bank_name = bank.name

    today_str = date_type.today().strftime("%B %d, %Y")

    # Build discrepancy rows
    disc_rows = ""
    for d in discrepancies:
        severity_badge = "&#128308;" if d.get("severity") == "HIGH" else "&#128992;"
        match_info = ""
        if d.get("match_pct") is not None:
            match_info = f' <span style="color:#888;font-size:11px;">({d["match_pct"]}% match)</span>'
        disc_rows += f"""
        <tr>
            <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;font-weight:600;">{severity_badge} {d.get("field", "")}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;color:#059669;">{d.get("requested", "N/A")}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;color:#dc2626;">{d.get("extracted", "N/A")}{match_info}</td>
        </tr>"""

    user_fullname = (request_obj.requestor_name if request_obj and request_obj.requestor_name
                      else requesting_user.email if requesting_user
                      else "Authorized Signatory")

    html = f"""
    <html>
    <head>
        <style>
            @page {{ size: A4; margin: 2.5cm; }}
            body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #1e293b; line-height: 1.7; font-size: 13px; }}
            .header {{ border-bottom: 3px solid #1e40af; padding-bottom: 15px; margin-bottom: 30px; }}
            .header h1 {{ color: #1e40af; font-size: 18px; margin: 0; }}
            .header p {{ color: #64748b; font-size: 12px; margin: 3px 0; }}
            .meta-block {{ font-size: 12px; color: #475569; margin-bottom: 20px; }}
            .meta-block strong {{ color: #1e293b; }}
            h2 {{ color: #1e40af; font-size: 15px; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 12px; }}
            th {{ background: #f1f5f9; padding: 10px 12px; text-align: left; font-weight: 700; color: #334155; border-bottom: 2px solid #cbd5e1; }}
            .ref {{ background: #eff6ff; padding: 12px 16px; border-radius: 8px; border-left: 4px solid #3b82f6; margin: 20px 0; }}
            .footer {{ margin-top: 50px; padding-top: 15px; border-top: 1px solid #e5e7eb; font-size: 11px; color: #94a3b8; }}
            .signature {{ margin-top: 60px; }}
            .signature .line {{ border-top: 1px solid #1e293b; width: 200px; margin-top: 40px; padding-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{entity_name}</h1>
            <p>Letter of Guarantee &mdash; Correction Request</p>
        </div>

        <div class="meta-block">
            <strong>To:</strong> {bank_name}<br>
            <strong>Date:</strong> {today_str}<br>
            <strong>LG Ref:</strong> {lg.lg_ref_number or "N/A"} &nbsp;|&nbsp;
            <strong>Bank LG No:</strong> {lg.bank_lg_number or "N/A"} &nbsp;|&nbsp;
            <strong>Request:</strong> {request_obj.serial_number if request_obj else "N/A"}
        </div>

        <div class="ref">
            <strong>Subject:</strong> Request for Correction of Letter of Guarantee &mdash; {lg.lg_ref_number or "N/A"}
        </div>

        <p>Dear Sir/Madam,</p>
        <p>
            We refer to the above-mentioned Letter of Guarantee issued by your esteemed bank.
            Upon reviewing the issued LG document, we have identified the following discrepancies
            between our original request and the LG as issued:
        </p>

        <h2>Discrepancy Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Field</th>
                    <th>As Requested</th>
                    <th>As Issued</th>
                </tr>
            </thead>
            <tbody>
                {disc_rows}
            </tbody>
        </table>

        <p>
            We kindly request that you review the above discrepancies and issue a corrected
            Letter of Guarantee at your earliest convenience to reflect the originally requested terms.
        </p>
        <p>
            Please do not hesitate to contact us should you require any clarification or additional documentation.
        </p>

        <p>Thank you for your prompt attention to this matter.</p>

        <div class="signature">
            <p>Yours faithfully,</p>
            <div class="line">
                <strong>{user_fullname}</strong><br>
                <span style="color:#64748b;font-size:12px;">{entity_name}</span>
            </div>
        </div>

        <div class="footer">
            Generated by Treasury Management System on {today_str}. Document reference: CORR-{lg.lg_ref_number or lg_id}
        </div>
    </body>
    </html>
    """

    # Generate PDF
    try:
        from weasyprint import HTML as WeasyHTML
        pdf_bytes = WeasyHTML(string=html).write_pdf()
    except ImportError:
        # Fallback: return HTML directly if weasyprint not available
        return StreamingResponse(
            io.BytesIO(html.encode("utf-8")),
            media_type="text/html",
            headers={"Content-Disposition": f'inline; filename="correction_request_{lg.lg_ref_number or lg_id}.html"'}
        )

    # Log the action
    log_action(db, current_user.user_id, "ISSUANCE_CORRECTION_REQUESTED", "IssuedLGRecord", lg.id,
               {"discrepancies": discrepancies, "bank": bank_name},
               current_user.customer_id)

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="correction_request_{lg.lg_ref_number or lg_id}.pdf"'}
    )


# ==============================================================================
# 20. ISSUANCE LG MAINTENANCE ACTIONS
# ==============================================================================

from app.services.issuance_maintenance_service import maintenance_service
from app.models.models_issuance import IssuanceMaintenanceAction
from pydantic import BaseModel as PydanticBase
from typing import Optional as Opt

class MaintenanceActionCreate(PydanticBase):
    action_type: str  # EXTEND, INCREASE_AMOUNT, CLOSE, LIQUIDATION, AMENDMENT, ACTIVATE
    action_data: dict  # {new_expiry_date, new_amount, amendment_text, ...}
    notes: Opt[str] = None

class DeliveryRecord(PydanticBase):
    delivery_method: str  # HAND_DELIVERY, COURIER, EMAIL
    delivery_notes: Opt[str] = None

class BankReplyRecord(PydanticBase):
    bank_reply_notes: Opt[str] = None

class RejectPayload(PydanticBase):
    reason: Opt[str] = None

class ResubmitDiscrepancyPayload(PydanticBase):
    notes: Opt[str] = None

def _serialize_action(action: IssuanceMaintenanceAction, lg=None, db=None) -> dict:
    """Serialize maintenance action to dict for API response."""
    # Resolve initiator profile
    initiated_by_email = None
    initiated_by_name = None
    initiated_by_department = None
    initiated_by_job_title = None
    if db and action.initiated_by_user_id:
        from app.models.models import User
        initiator = db.query(User).filter(User.id == action.initiated_by_user_id).first()
        if initiator:
            initiated_by_email = initiator.email
            initiated_by_name = initiator.full_name if hasattr(initiator, 'full_name') else None
            initiated_by_department = initiator.department if hasattr(initiator, 'department') else None
            initiated_by_job_title = initiator.job_title if hasattr(initiator, 'job_title') else None

    # Fallback: external requestor email from action_data
    if not initiated_by_email and action.action_data:
        initiated_by_email = action.action_data.get("requestor_email")
        if initiated_by_email and not initiated_by_name:
            initiated_by_name = f"External Requestor ({action.initiation_source or 'PORTAL'})"

    # Resolve currency code
    lg_currency_code = None
    if lg and lg.currency_id and db:
        from app.models.models import Currency
        curr = db.query(Currency.iso_code).filter(Currency.id == lg.currency_id).first()
        lg_currency_code = curr.iso_code if curr else None

    # Resolve bank name
    lg_bank_name = None
    if lg and lg.bank_id and db:
        from app.models.models import Bank
        bank = db.query(Bank.name).filter(Bank.id == lg.bank_id).first()
        lg_bank_name = bank.name if bank else None

    return {
        "id": action.id,
        "issued_lg_id": action.issued_lg_id,
        "lg_ref_number": lg.lg_ref_number if lg else None,
        "lg_beneficiary": lg.beneficiary_name if lg else None,
        "lg_current_amount": str(lg.current_amount) if lg else None,
        "lg_currency_code": lg_currency_code,
        "lg_expiry_date": str(lg.expiry_date) if lg else None,
        "lg_issue_date": str(lg.issue_date) if lg and lg.issue_date else None,
        "lg_status": lg.status if lg else None,
        "lg_operational_status": lg.operational_status if lg and hasattr(lg, "operational_status") else None,
        "lg_bank_name": lg_bank_name,
        "lg_issuance_method": lg.issuance_method if lg else None,
        "lg_bank_lg_number": lg.bank_lg_number if lg else None,
        "action_type": action.action_type,
        "status": action.status,
        "action_data": action.action_data,
        "pending_approver_users": action.pending_approver_users,
        "current_step_number": action.current_step_number,
        "approval_history": action.approval_history,
        "instruction_status": action.instruction_status,
        "letter_serial_number": action.letter_serial_number,
        "is_printed": action.is_printed,
        "delivery_date": str(action.delivery_date) if action.delivery_date else None,
        "delivery_method": action.delivery_method,
        "delivery_notes": action.delivery_notes,
        "delivery_document_path": action.delivery_document_path,
        "bank_reply_date": str(action.bank_reply_date) if action.bank_reply_date else None,
        "bank_reply_notes": action.bank_reply_notes,
        "bank_reply_document_path": action.bank_reply_document_path,
        "initiation_source": action.initiation_source,
        "initiated_by_user_id": action.initiated_by_user_id,
        "initiated_by_email": initiated_by_email,
        "initiated_by_name": initiated_by_name,
        "initiated_by_department": initiated_by_department,
        "initiated_by_job_title": initiated_by_job_title,
        "supporting_documents": (action.action_data or {}).get("supporting_documents", []),
        "executed_by_user_id": action.executed_by_user_id,
        "notes": action.notes,
        "letter_generated_path": action.letter_generated_path,
        "created_at": str(action.created_at) if action.created_at else None,
        "updated_at": str(action.updated_at) if action.updated_at else None,
    }


# ==============================================================================
# MAINTENANCE DOCUMENT ACCESS
# ==============================================================================

def _serialize_recon_session(session, db: Session, brief: bool = False) -> dict:
    bank = db.query(Bank).filter(Bank.id == session.bank_id).first()
    result = {
        "id": session.id,
        "bank_id": session.bank_id,
        "bank_name": bank.name if bank else None,
        "position_date": str(session.position_date),
        "status": session.status,
        "file_format": session.file_format,
        "original_file_name": session.original_file_name,
        "total_bank_records": session.total_bank_records,
        "matched_count": session.matched_count,
        "mismatched_count": session.mismatched_count,
        "bank_only_count": session.bank_only_count,
        "system_only_count": session.system_only_count,
        "notes": session.notes,
        "created_at": str(session.created_at) if session.created_at else None,
    }
    if not brief:
        result.update({
            "parsing_method": session.parsing_method,
            "bank_reported_total": str(session.bank_reported_total) if session.bank_reported_total else None,
            "bank_reported_count": session.bank_reported_count,
            "completeness_status": session.completeness_status,
            "completeness_note": session.completeness_note,
            "error_message": session.error_message,
            "reviewed_at": str(session.reviewed_at) if session.reviewed_at else None,
        })
    return result


def _serialize_recon_result(result, db: Session) -> dict:
    from app.models.models_issuance import IssuedLGRecord, ReconciliationBankRow as ReconBankRow
    lg = None
    if result.issued_lg_id:
        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == result.issued_lg_id).first()
    bank_row = None
    if result.bank_row_id:
        bank_row = db.query(ReconBankRow).filter(
            ReconBankRow.id == result.bank_row_id
        ).first()

    return {
        "id": result.id,
        "session_id": result.session_id,
        "mismatch_type": result.mismatch_type,
        "severity": result.severity,
        "field_name": result.field_name,
        "bank_value": result.bank_value,
        "system_value": result.system_value,
        "user_resolution": result.user_resolution,
        "resolution_notes": result.resolution_notes,
        "approval_status": result.approval_status,
        "record_updated": result.record_updated,
        # Context
        "lg_ref_number": lg.lg_ref_number if lg else None,
        "lg_bank_number": lg.bank_lg_number if lg else None,
        "lg_beneficiary": lg.beneficiary_name if lg else None,
        "lg_status": lg.status if lg else None,
        "bank_row_lg_number": bank_row.bank_lg_number if bank_row else None,
        "bank_row_beneficiary": bank_row.beneficiary_name if bank_row else None,
        "bank_row_amount": str(bank_row.amount) if bank_row and bank_row.amount else None,
    }


# ==============================================================================
# ADMIN DUAL-CONTROL ENDPOINTS
# ==============================================================================

def _apply_admin_change(db: Session, change_req: AdminChangeRequest):
    """Auto-apply an approved AdminChangeRequest based on its change_type."""
    from datetime import datetime as _dt
    payload = change_req.change_payload or {}
    ct = change_req.change_type

    if ct == "FORM_CONFIG_UPDATE":
        config = db.query(CustomerFormConfiguration).filter(
            CustomerFormConfiguration.customer_id == change_req.customer_id
        ).first()
        if not config:
            config = CustomerFormConfiguration(customer_id=change_req.customer_id)
            db.add(config)
        new_val = payload.get("new_value", {})
        if "field_configurations" in new_val:
            config.field_configurations = new_val["field_configurations"]
        if "custom_field_1_config" in new_val:
            config.custom_field_1_config = new_val["custom_field_1_config"]
        if "custom_field_2_config" in new_val:
            config.custom_field_2_config = new_val["custom_field_2_config"]
        if "mandatory_document_types" in new_val:
            config.mandatory_document_types = new_val["mandatory_document_types"]
        if "reference_types" in new_val:
            config.reference_types = new_val["reference_types"]
        if "document_config" in new_val:
            config.document_config = new_val["document_config"]

    elif ct == "APPROVAL_MATRIX_UPDATE":
        # Bulk-replace workflow policies
        db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == change_req.customer_id
        ).delete()
        amount_types = {"AMOUNT_OVER", "AMOUNT_RANGE"}
        has_dept_match = False
        for idx, p in enumerate(payload.get("new_value", [])):
            if p.get("condition_type") == "DEPT_MATCH":
                has_dept_match = True
            db_obj = IssuanceWorkflowPolicy(
                customer_id=change_req.customer_id,
                step_sequence=idx + 1,
                condition_type=p.get("condition_type", "ALWAYS"),
                condition_value=str(p["condition_value"]) if p.get("condition_value") else None,
                currency_id=p.get("currency_id") if p.get("condition_type") in amount_types else None,
                approver_type=p.get("approver_type", "ROLE"),
                approver_values=p.get("approver_values", []),
                required_signatures=p.get("required_signatures", 1),
                is_active=True
            )
            db.add(db_obj)
            
        if has_dept_match:
            form_config = db.query(CustomerFormConfiguration).filter(
                CustomerFormConfiguration.customer_id == change_req.customer_id
            ).first()
            if not form_config:
                form_config = CustomerFormConfiguration(
                    customer_id=change_req.customer_id,
                    field_configurations={"department": {"is_visible": True, "is_mandatory": True}}
                )
                db.add(form_config)
            else:
                import copy
                fc = copy.deepcopy(form_config.field_configurations or {})
                dept_config = fc.get("department", {})
                dept_config["is_visible"] = True
                dept_config["is_mandatory"] = True
                fc["department"] = dept_config
                form_config.field_configurations = fc

    elif ct == "DEPARTMENT_CREATE":
        from app.crud.crud_org import crud_department
        from app.schemas.all_schemas import DepartmentCreate
        dept_data = DepartmentCreate(**payload.get("new_value", {}))
        crud_department.create_dept(db, dept_data, change_req.customer_id, change_req.requested_by_user_id)

    elif ct == "DEPARTMENT_UPDATE":
        from app.crud.crud_org import crud_department
        from app.schemas.all_schemas import DepartmentUpdate
        dept = crud_department.get(db, id=payload.get("entity_id"))
        if dept and dept.customer_id == change_req.customer_id:
            update_data = DepartmentUpdate(**payload.get("new_value", {}))
            crud_department.update_dept(db, dept, update_data, change_req.requested_by_user_id)

    elif ct == "GROUP_CREATE":
        from app.crud.crud_org import crud_approval_group
        from app.schemas.all_schemas import ApprovalGroupCreate
        grp_data = ApprovalGroupCreate(**payload.get("new_value", {}))
        crud_approval_group.create_group(db, grp_data, change_req.customer_id, change_req.requested_by_user_id)

    elif ct == "GROUP_UPDATE":
        from app.crud.crud_org import crud_approval_group
        from app.schemas.all_schemas import ApprovalGroupUpdate
        grp = crud_approval_group.get(db, id=payload.get("entity_id"))
        if grp and grp.customer_id == change_req.customer_id:
            update_data = ApprovalGroupUpdate(**payload.get("new_value", {}))
            crud_approval_group.update_group(db, grp, update_data, change_req.requested_by_user_id)

    elif ct == "CUSTOMER_CONFIG_UPDATE":
        from app.crud.crud import crud_customer_configuration
        crud_customer_configuration.set_customer_config(
            db,
            customer_id=change_req.customer_id,
            global_config_id=payload.get("global_config_id"),
            configured_value=payload.get("configured_value"),
            user_id=change_req.requested_by_user_id,
        )

    elif ct == "EMAIL_SETTINGS_UPDATE":
        from app.crud.crud import crud_customer_email_setting
        db_settings = crud_customer_email_setting.get(db, payload.get("setting_id"))
        if db_settings and db_settings.customer_id == change_req.customer_id:
            # Apply only non-password fields (password was applied immediately at request time)
            for field in ("smtp_host", "smtp_port", "smtp_username", "sender_email", "sender_display_name", "is_active"):
                if field in payload.get("new_value", {}):
                    setattr(db_settings, field, payload["new_value"][field])
            db.add(db_settings)

    elif ct == "EMAIL_SETTINGS_CREATE":
        from app.crud.crud import crud_customer_email_setting
        from app.models import CustomerEmailSetting
        nv = payload.get("new_value", {})
        # Check if a record already exists (e.g. password was pre-applied)
        existing = crud_customer_email_setting.get_by_customer_id(db, change_req.customer_id)
        if existing:
            # Update the existing record with the approved non-password fields
            for field in ("smtp_host", "smtp_port", "smtp_username", "sender_email", "sender_display_name", "is_active"):
                if field in nv:
                    setattr(existing, field, nv[field])
            existing.is_deleted = False
            db.add(existing)
        else:
            # Create fresh (password would need to be re-set after approval)
            db_settings = CustomerEmailSetting(
                customer_id=change_req.customer_id,
                smtp_host=nv.get("smtp_host", ""),
                smtp_port=nv.get("smtp_port", 587),
                smtp_username=nv.get("smtp_username", ""),
                sender_email=nv.get("sender_email", ""),
                sender_display_name=nv.get("sender_display_name"),
                is_active=nv.get("is_active", True),
            )
            db.add(db_settings)

    elif ct == "EMAIL_SETTINGS_DELETE":
        from app.crud.crud import crud_customer_email_setting
        db_settings = crud_customer_email_setting.get(db, payload.get("setting_id"))
        if db_settings and db_settings.customer_id == change_req.customer_id:
            db_settings.soft_delete()
            db.add(db_settings)

    db.flush()


def _create_governed_change(
    db: Session, customer_id: int, user_id: int,
    change_type: str, change_payload: dict
) -> tuple:
    """
    Thin wrapper — delegates to shared governance module,
    supplying the issuance-specific _apply_admin_change as callback.
    """
    from app.core.governance import create_governed_change
    return create_governed_change(
        db, customer_id, user_id,
        change_type, change_payload,
        apply_fn=_apply_admin_change,
    )
