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

@router.post("/requests/", response_model=IssuanceRequestOut)
def create_issuance_request_internal(
    request_in: IssuanceRequestCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Internal: Creates a DRAFT request (all required fields must be present)."""
    return crud_issuance_request.create_request(
        db, 
        obj_in=request_in, 
        customer_id=current_user.customer_id, 
        user_id=current_user.user_id 
    )

@router.post("/requests/draft", response_model=IssuanceRequestOut)
def save_draft_request_internal(
    request_in: IssuanceRequestDraftCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Internal: Saves an incomplete draft — all fields are optional."""
    return crud_issuance_request.create_request(
        db, 
        obj_in=request_in, 
        customer_id=current_user.customer_id, 
        user_id=current_user.user_id 
    )

@router.get("/requests/", response_model=List[IssuanceRequestOut])
def get_issuance_requests(
    skip: int = 0, limit: int = 100,
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_issuance_read_context)
):
    return crud_issuance_request.get_by_customer(db, customer_id=current_user.customer_id, skip=skip, limit=limit)

@router.get("/requests/{request_id}", response_model=IssuanceRequestOut)
def get_single_issuance_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Get a single issuance request by ID."""
    return crud_issuance_request.get_single(db, request_id, current_user.customer_id)

@router.delete("/requests/{request_id}")
def delete_draft_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Delete a DRAFT request. Only drafts can be deleted."""
    return crud_issuance_request.delete_draft(db, request_id, current_user.customer_id, current_user.user_id)

@router.put("/requests/{request_id}", response_model=IssuanceRequestOut)
def edit_issuance_request(
    request_id: int,
    request_in: IssuanceRequestUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Edit a request (corporate admin or end user).
    - If blacklisted fields changed on a post-submission request → re-approval triggered.
    - If only safe fields changed → edit requires admin approval (EDIT_REQUESTED).
    - DRAFT edits have no governance restrictions.
    """
    updated = crud_issuance_request.update_request(
        db, request_id, request_in, current_user.customer_id, current_user.user_id
    )

    # Send notifications for post-submission edits
    metadata = getattr(updated, '_edit_metadata', None)
    if metadata:
        if metadata.get('re_approval_triggered') or metadata.get('safe_fields_changed'):
            try:
                _send_edit_notifications(db, updated, current_user, metadata)
            except Exception:
                import logging
                logging.getLogger(__name__).warning("Failed to send edit notifications", exc_info=True)

        if metadata.get('edit_pending_approval'):
            # Notify corp admins about pending edit
            try:
                from app.schemas.all_schemas import SystemNotificationCreate
                from app.crud.crud import crud_notification
                from app.models import models as base_models
                from datetime import datetime, timedelta

                _now = datetime.utcnow()
                admins = db.query(base_models.User).filter(
                    base_models.User.customer_id == current_user.customer_id,
                    base_models.User.role == "corporate_admin",
                    base_models.User.is_deleted == False,
                ).all()
                for admin in admins:
                    notif = SystemNotificationCreate(
                        content=f"Edit approval needed for request {updated.serial_number}: "
                                f"{', '.join(metadata.get('safe_fields_changed', []))}",
                        notification_type="ISSUANCE_EDIT_REQUEST",
                        start_date=_now,
                        end_date=_now + timedelta(days=30),
                        target_user_ids=[admin.id],
                        target_customer_ids=[current_user.customer_id],
                        link="/corporate-admin/approval-center",
                    )
                    crud_notification.create_notification(db, obj_in=notif)
            except Exception:
                import logging
                logging.getLogger(__name__).warning("Failed to send edit request notification", exc_info=True)

    return updated


@router.post("/requests/{request_id}/resolve-edit")
def resolve_issuance_edit(
    request_id: int,
    payload: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Admin approves or rejects a pending edit request.
    Approve: apply stored changes, restore previous status.
    Reject: discard changes, restore previous status.
    """
    from app.crud.crud import log_action
    from datetime import datetime
    from sqlalchemy.orm.attributes import flag_modified

    req = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id
    ).first()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found.")

    if req.status != "EDIT_REQUESTED":
        raise HTTPException(status_code=400, detail=f"Request is not pending edit approval. Status: {req.status}")

    meta = dict(req.metadata_json or {})
    edit_meta = meta.get("pending_edit")
    if not edit_meta:
        raise HTTPException(status_code=400, detail="No pending edit data found.")

    approved = payload.get("approved", False)
    admin_note = payload.get("note", "")
    previous_status = edit_meta.get("previous_status", "APPROVED_INTERNAL")

    # Audit trail
    audit = list(req.approval_chain_audit or [])
    audit.append({
        "action": "EDIT_RESOLVED",
        "decision": "APPROVED" if approved else "REJECTED",
        "admin_user_id": current_user.user_id,
        "admin_note": admin_note,
        "timestamp": datetime.utcnow().isoformat(),
        "changed_fields": list(edit_meta.get("diff", {}).keys()),
    })
    req.approval_chain_audit = audit
    flag_modified(req, 'approval_chain_audit')

    # Clean up
    del meta["pending_edit"]
    req.metadata_json = meta

    if approved:
        # Apply the stored changes
        changes = edit_meta.get("changes", {})
        for field, new_value in changes.items():
            setattr(req, field, new_value)

        req.status = previous_status

        log_action(db, current_user.user_id, "ISSUANCE_EDIT_APPROVED",
                   "IssuanceRequest", req.id,
                   {"applied_fields": list(changes.keys()), "admin_note": admin_note},
                   current_user.customer_id)

        # Notify requestor
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            requestor_id = edit_meta.get("requested_by_user_id")
            if requestor_id:
                _now = datetime.utcnow()
                notif = SystemNotificationCreate(
                    content=f"Your edit request for {req.serial_number} has been approved and applied.",
                    notification_type="ISSUANCE_EDIT_APPROVED",
                    start_date=_now,
                    end_date=_now + __import__('datetime').timedelta(days=30),
                    target_user_ids=[requestor_id],
                    target_customer_ids=[current_user.customer_id],
                )
                crud_notification.create_notification(db, obj_in=notif)
        except Exception:
            pass
    else:
        # Discard changes, restore status
        req.status = previous_status

        log_action(db, current_user.user_id, "ISSUANCE_EDIT_REJECTED",
                   "IssuanceRequest", req.id,
                   {"rejected_fields": list(edit_meta.get("diff", {}).keys()), "admin_note": admin_note},
                   current_user.customer_id)

        # Notify requestor
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            requestor_id = edit_meta.get("requested_by_user_id")
            if requestor_id:
                _now = datetime.utcnow()
                notif = SystemNotificationCreate(
                    content=f"Your edit request for {req.serial_number} was rejected. "
                            f"Reason: {admin_note or 'No reason provided.'}",
                    notification_type="ISSUANCE_EDIT_REJECTED",
                    start_date=_now,
                    end_date=_now + __import__('datetime').timedelta(days=30),
                    target_user_ids=[requestor_id],
                    target_customer_ids=[current_user.customer_id],
                )
                crud_notification.create_notification(db, obj_in=notif)
        except Exception:
            pass

    db.commit()

    return {
        "message": f"Edit {'approved and applied' if approved else 'rejected'}.",
        "id": req.id,
        "serial_number": req.serial_number,
        "status": req.status,
    }

# ==============================================================================
# GOVERNANCE: VERSIONS & SNAPSHOTS
# ==============================================================================

@router.get("/requests/{request_id}/snapshot")
def get_request_snapshot(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Retrieves the Immutable V1 Submission Snapshot."""
    snapshot = db.query(IssuanceRequestSnapshot).join(IssuanceRequestSnapshot.request).filter(
        IssuanceRequestSnapshot.request_id == request_id,
        crud_issuance_request.model.customer_id == current_user.customer_id
    ).first()
    
    if not snapshot:
        raise HTTPException(404, "Snapshot not found. Has this request been submitted?")
    return snapshot.snapshot_data

@router.get("/requests/{request_id}/versions", response_model=List[IssuanceRequestVersionOut])
def get_request_versions(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Retrieves linear edit history and diffs."""
    # Ensure ownership
    req = crud_issuance_request.get(db, id=request_id)
    if not req or req.customer_id != current_user.customer_id:
        raise HTTPException(404, "Request not found")
        
    versions = db.query(IssuanceRequestVersion).filter(
        IssuanceRequestVersion.request_id == request_id
    ).order_by(IssuanceRequestVersion.version_number.desc()).all()
    
    return versions

# ==============================================================================
# 4. INTELLIGENT DECISION SUPPORT
# ==============================================================================

@router.get("/requests/{request_id}/suitable-facilities", response_model=List[SuitableFacilityOut])
def get_suitable_facilities(
    request_id: int, 
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Returns facilities with utilization, cost calculations, and recommendation tags.
    Delegates to the service's Smart Engine v2.
    """
    return issuance_service.get_suitable_facilities(db, request_id)

@router.post("/requests/{request_id}/reserve", response_model=IssuanceRequestOut)
def reserve_facility_for_request(
    request_id: int,
    sub_limit_id: int = Query(..., description="The sub-limit to reserve capacity on"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Reserve facility capacity without issuing the LG. APPROVED_INTERNAL → FACILITY_RESERVED."""
    return issuance_service.reserve_facility(db, request_id, current_user.user_id, sub_limit_id)

@router.post("/requests/{request_id}/release-reservation", response_model=IssuanceRequestOut)
def release_facility_reservation(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Release a facility reservation, freeing capacity. FACILITY_RESERVED → APPROVED_INTERNAL."""
    return issuance_service.release_reservation(db, request_id, current_user.user_id)

# C5: Pre-execution checks (FX drift warning)
@router.get("/requests/{request_id}/pre-execution-check")
def pre_execution_check(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Runs pre-execution checks before issuing an LG.
    Returns warnings about FX rate drift since reservation.
    Frontend should call this before showing the execute dialog.
    """
    return issuance_service.pre_execution_check(db, request_id)

@router.post("/requests/{request_id}/issue", response_model=IssuedLGRecordDetailOut)
async def issue_lg(
    request_id: int,
    body: IssuanceExecuteRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Unified Issuance Endpoint.
    Validates approval, acquires atomic lock, checks facility,
    creates exposure entry, creates LG record, and transitions status.
    """
    new_lg = await issuance_service.issue_lg(
        db=db,
        request_id=request_id,
        user_id=current_user.user_id,
        sub_limit_id=body.sub_limit_id,
        issued_ref_number=body.issued_ref_number,
        issue_date=body.issue_date,
        expiry_date=body.expiry_date,
        issuance_method=body.issuance_method,
        bank_method_id=body.bank_method_id,
        bank_id=body.bank_id,
        manual_pricing=body.manual_pricing
    )
    return new_lg


@router.post("/requests/{request_id}/cancel", response_model=IssuanceRequestOut)
def cancel_issuance_request(
    request_id: int,
    body: IssuanceCancelRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """
    Cancel a request before bank confirmation.
    Releases any facility reservation and unlocks the request.
    """
    return issuance_service.cancel_request(
        db=db,
        request_id=request_id,
        user_id=current_user.user_id,
        reason=body.reason
    )

# ==============================================================================
# 5. UTILITIES (PDF, SECURITY)
# ==============================================================================

@router.get("/requests/{request_id}/print-form")
async def print_issuance_application_form(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    request = crud_issuance_request.get(db, id=request_id)
    if not request: raise HTTPException(404, "Request not found")

    html_content = f"""
    <html><body>
        <h1>LG Issuance Application</h1>
        <p><strong>Ref:</strong> {request.id}</p>
        <p><strong>Beneficiary:</strong> {request.beneficiary_name}</p>
        <p><strong>Amount:</strong> {request.amount}</p>
    </body></html>
    """
    pdf_bytes = await generate_pdf_from_html(html_content, f"application_{request.id}")
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=application_{request.id}.pdf"}
    )

@router.get("/requests/{request_id}/generate-letter")
async def generate_issuance_letter(
    request_id: int,
    additional_text: str = Query("", description="Extra free text instructions to include in the letter"),
    use_special_wording: bool = Query(False, description="Override to use special wording instead of bank standard"),
    field_overrides: str = Query("", description="JSON dict of placeholder overrides from missing fields panel"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Generates a signed company letter PDF for an issuance request using the template system.
    Uses customer-specific template if available, otherwise falls back to the global default.
    Available placeholders are defined under action_type 'LG_ISSUANCE_REQUEST'.
    """
    import json
    overrides = {}
    if field_overrides:
        try:
            overrides = json.loads(field_overrides)
        except (json.JSONDecodeError, TypeError):
            pass
    result = await issuance_service.generate_issuance_letter(
        db=db,
        request_id=request_id,
        customer_id=current_user.customer_id,
        additional_text=additional_text,
        use_special_wording=use_special_wording,
        field_overrides=overrides,
    )
    headers = {
        'Content-Disposition': f'inline; filename="{result["filename"]}"'
    }
    return StreamingResponse(
        io.BytesIO(result["pdf_bytes"]),
        media_type="application/pdf",
        headers=headers,
    )

@router.get("/requests/{request_id}/letter-fields-check")
async def check_letter_fields(
    request_id: int,
    bank_id: Optional[int] = Query(None, description="Selected bank for resolving bank account info"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Pre-checks which company letter placeholder fields are empty or missing.
    Returns missing_fields in the same shape as bank form auto-fill so the frontend
    can reuse the same missing-fields UI panel.
    """
    from sqlalchemy.orm import selectinload
    from app.models.models_issuance import (
        IssuanceFacilitySubLimit, IssuanceFacility, CustomerBankAccount,
        CustomerFormConfiguration,
    )

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
        raise HTTPException(status_code=404, detail="Issuance request not found.")

    # Resolve bank_id from query param → locked_bank_id in metadata → sub_limit
    meta = dict(request.metadata_json or {})
    resolved_bank_id = bank_id or (int(meta["locked_bank_id"]) if meta.get("locked_bank_id") else None)

    # Resolve bank name and bank account
    bank_name = "N/A"
    bank_account = None
    if request.selected_sub_limit_id:
        sub_limit = db.query(IssuanceFacilitySubLimit).options(
            selectinload(IssuanceFacilitySubLimit.facility).selectinload(IssuanceFacility.bank),
            selectinload(IssuanceFacilitySubLimit.facility).selectinload(IssuanceFacility.bank_account),
        ).filter(IssuanceFacilitySubLimit.id == request.selected_sub_limit_id).first()
        if sub_limit and sub_limit.facility:
            if sub_limit.facility.bank:
                bank_name = sub_limit.facility.bank.name
                if not resolved_bank_id:
                    resolved_bank_id = sub_limit.facility.bank_id
            if sub_limit.facility.bank_account:
                bank_account = sub_limit.facility.bank_account

    # If still no bank name, resolve directly from Bank table using resolved_bank_id
    if bank_name == "N/A" and resolved_bank_id:
        bank_obj = db.query(Bank).filter(Bank.id == resolved_bank_id).first()
        if bank_obj:
            bank_name = bank_obj.name

    # Fallback bank account lookup using resolved_bank_id
    if not bank_account and resolved_bank_id:
        bank_account = db.query(CustomerBankAccount).filter(
            CustomerBankAccount.customer_id == current_user.customer_id,
            CustomerBankAccount.bank_id == resolved_bank_id,
            CustomerBankAccount.is_default == True,
            CustomerBankAccount.is_deleted == False,
        ).first()
        if not bank_account:
            bank_account = db.query(CustomerBankAccount).filter(
                CustomerBankAccount.customer_id == current_user.customer_id,
                CustomerBankAccount.bank_id == resolved_bank_id,
                CustomerBankAccount.is_active == True,
                CustomerBankAccount.is_deleted == False,
            ).first()

    # Build the same placeholder data as generate_issuance_letter
    from datetime import date
    amount_val = float(request.amount) if request.amount else 0
    currency_code = request.currency.iso_code if request.currency else "N/A"

    # Human-readable labels for each field
    FIELD_LABELS = {
        "beneficiary_name": "Beneficiary Name",
        "beneficiary_address": "Beneficiary Address",
        "amount": "Amount",
        "currency_code": "Currency",
        "lg_type": "LG Type",
        "expiry_date": "Expiry Date",
        "issue_date": "Issue Date",
        "purpose": "LG Purpose",
        "reference_type": "Reference Type",
        "reference_number": "Reference Number",
        "bank_name": "Bank Name",
        "account_name": "Bank Account Name",
        "account_number": "Bank Account Number",
        "iban": "IBAN",
        "branch_name": "Branch Name",
        "customer_number": "Customer Number at Bank",
        "customer_name": "Company Name",
        "entity_name": "Entity Name",
        "customer_address": "Entity Address",
        "requestor_name": "Requestor Name",
    }

    # Build values dict (subset of full placeholder_data - only user-relevant fields)
    field_values = {
        "beneficiary_name": request.beneficiary_name or "",
        "beneficiary_address": request.beneficiary_address or "",
        "amount": f"{amount_val:,.2f}" if amount_val > 0 else "",
        "currency_code": currency_code if currency_code != "N/A" else "",
        "lg_type": request.lg_type.name if request.lg_type else "",
        "expiry_date": request.requested_expiry_date.strftime("%d-%b-%Y") if request.requested_expiry_date else "",
        "issue_date": request.requested_issue_date.strftime("%d-%b-%Y") if request.requested_issue_date else date.today().strftime("%d-%b-%Y"),
        "purpose": request.lg_purpose or "",
        "reference_type": request.reference_type or "",
        "reference_number": request.reference_number or "",
        "bank_name": bank_name if bank_name != "N/A" else "",
        "account_name": bank_account.account_name if bank_account else "",
        "account_number": bank_account.account_number if bank_account else "",
        "iban": bank_account.iban if bank_account else "",
        "branch_name": bank_account.branch_name if bank_account else "",
        "customer_number": bank_account.customer_number if bank_account else "",
        "customer_name": request.customer.name if request.customer else "",
        "entity_name": request.issuing_entity.entity_name if request.issuing_entity else "",
        "customer_address": request.issuing_entity.address if request.issuing_entity and hasattr(request.issuing_entity, 'address') else "",
        "requestor_name": request.requestor_name or "",
    }

    # Identify missing (empty) fields
    missing_fields = []
    filled_count = 0
    for key, value in field_values.items():
        val = str(value).strip() if value else ""
        if val and val not in ("N/A", "None"):
            filled_count += 1
        else:
            missing_fields.append({
                "pdf_field_name": key,  # Reuse same key name as bank form for frontend compat
                "label": FIELD_LABELS.get(key, key),
                "mapped_to": key,
                "current_value": val,
                "saved_value": "",
            })

    return {
        "status": "missing_fields" if missing_fields else "complete",
        "missing_fields": missing_fields,
        "total_fields": len(field_values),
        "auto_filled_fields": filled_count,
        "lg_language": getattr(request, 'lg_language', 'EN') or 'EN',
    }

@router.get("/generate-portal-link")
def generate_portal_link(
    recipient_email: str = Query(...),
    department: str = Query(...),
    hours_valid: int = Query(24),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Revised: Generates an encrypted link for a specific guest/employee.
    Format: customer_id|department|email|expiry_timestamp
    """
    from datetime import datetime, timedelta, timezone
    from app.crud.crud import log_action # Ensure this is imported

    # 1. Calculate Expiry
    expiry = (datetime.now(timezone.utc) + timedelta(hours=hours_valid)).isoformat()
    
    # 2. Build Payload (using | as a separator for clarity)
    payload = f"{current_user.customer_id}|{department}|{recipient_email}|{expiry}"
    
    # 3. Encrypt using your core encryption
    token = encrypt_data(payload)

    # 4. Record the action in the System Log
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="EXTERNAL_INVITE_GENERATED",
        entity_type="IssuanceRequest",
        entity_id=None,
        details={"recipient": recipient_email, "dept": department, "expiry": expiry},
        customer_id=current_user.customer_id
    )

    return {"token": token, "link": f"/public/request?token={token}"}

# ==============================================================================
# 6. RECONCILIATION ENGINE (NEW)
# ==============================================================================

@router.get("/requests/{request_id}/recommendations", response_model=List[SuitableFacilityOut])
def get_facility_recommendations(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Returns a list of facilities that CAN issue this LG, sorted by SLA.
    """
    return issuance_service.get_suitable_facilities(db, request_id)


# ==============================================================================
# C3. TREASURY ENRICHMENT (Technical Input by Treasury)
# ==============================================================================

@router.patch("/requests/{request_id}/enrich")
def enrich_request(
    request_id: int,
    enrichment_data: Dict[str, Any] = Body(..., description="Treasury enrichment fields to add/update"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """
    Treasury team adds technical details to a request (applicable rules,
    advising bank, margin instructions, internal notes, etc.).
    Available at any time after submission — before, during, or after approval.
    Merges into the existing treasury_enrichment JSONB field.
    """
    from app.models.models_issuance import IssuanceRequest
    from datetime import datetime as dt

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.is_deleted == False,
    ).first()
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if request.status == "DRAFT":
        raise HTTPException(status_code=400, detail="Cannot enrich a draft — request must be submitted first")

    # Merge enrichment data
    existing = request.treasury_enrichment or {}
    existing.update(enrichment_data)
    existing["enriched_by_user_id"] = current_user.user_id
    existing["enriched_at"] = dt.utcnow().isoformat()

    # Use flag_modified to ensure JSONB change is detected
    from sqlalchemy.orm.attributes import flag_modified
    request.treasury_enrichment = existing
    flag_modified(request, "treasury_enrichment")

    # Also update applicable_rules on the request if provided in enrichment
    if "applicable_rules" in enrichment_data:
        request.applicable_rules = enrichment_data["applicable_rules"]

    # Update cross_border_details if provided
    if "cross_border_details" in enrichment_data and isinstance(enrichment_data["cross_border_details"], dict):
        cbd = request.cross_border_details or {}
        cbd.update(enrichment_data["cross_border_details"])
        request.cross_border_details = cbd
        flag_modified(request, "cross_border_details")

    db.flush()
    db.refresh(request)

    return {
        "message": "Request enriched successfully",
        "request_id": request.id,
        "treasury_enrichment": request.treasury_enrichment,
        "applicable_rules": request.applicable_rules,
        "cross_border_details": request.cross_border_details,
    }


# ==============================================================================
# 8. BENEFICIARY LOOKUP (Smart Auto-Fill)
# ==============================================================================

@router.get("/beneficiary-lookup")
def beneficiary_lookup(
    id_number: str = Query(..., description="Beneficiary ID/Number to look up"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Returns the most recent beneficiary data matching the given ID/number.
    Searches across all issuance requests for this customer.
    """
    from app.models.models_issuance import IssuanceRequest
    
    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.beneficiary_id_number == id_number,
        IssuanceRequest.is_deleted == False
    ).order_by(IssuanceRequest.created_at.desc()).first()
    
    if not request:
        return {"found": False}
    
    return {
        "found": True,
        "beneficiary_name": request.beneficiary_name,
        "beneficiary_country": request.beneficiary_country,
        "beneficiary_address": request.beneficiary_address,
        "beneficiary_contact_person": request.beneficiary_contact_person,
        "beneficiary_phone": request.beneficiary_phone,
        "beneficiary_email": request.beneficiary_email,
    }


@router.get("/beneficiary-suggest")
def beneficiary_suggest(
    name: str = Query(..., min_length=3, description="Partial beneficiary name to search"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Fuzzy match: returns previous beneficiary names similar to the input.
    Searches across all issuance requests for this customer.
    Uses combined ILIKE substring + Python-level fuzzy matching for typo tolerance.
    """
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy import func as sa_func
    from difflib import SequenceMatcher
    
    # Step 1: Get all distinct beneficiary names for this customer (limited to recent 200)
    all_names = db.query(
        IssuanceRequest.beneficiary_name,
        IssuanceRequest.beneficiary_id_number,
        IssuanceRequest.beneficiary_country,
        IssuanceRequest.beneficiary_address,
        IssuanceRequest.beneficiary_contact_person,
        IssuanceRequest.beneficiary_phone,
        IssuanceRequest.beneficiary_email,
        sa_func.max(IssuanceRequest.created_at).label('latest')
    ).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.beneficiary_name.isnot(None),
        IssuanceRequest.beneficiary_name != '',
        IssuanceRequest.is_deleted == False
    ).group_by(
        IssuanceRequest.beneficiary_name,
        IssuanceRequest.beneficiary_id_number,
        IssuanceRequest.beneficiary_country,
        IssuanceRequest.beneficiary_address,
        IssuanceRequest.beneficiary_contact_person,
        IssuanceRequest.beneficiary_phone,
        IssuanceRequest.beneficiary_email,
    ).order_by(sa_func.max(IssuanceRequest.created_at).desc()).limit(200).all()
    
    # Step 2: Score each name using fuzzy similarity
    name_lower = name.lower()
    scored = []
    for m in all_names:
        ben_name = m.beneficiary_name or ''
        ben_lower = ben_name.lower()
        
        # ILIKE-style substring check gives high score
        if name_lower in ben_lower or ben_lower in name_lower:
            score = 95
        else:
            # Fuzzy similarity (SequenceMatcher handles typos, transpositions, etc.)
            score = int(SequenceMatcher(None, name_lower, ben_lower).ratio() * 100)
        
        # Lowered from 80 to 60 because SequenceMatcher ratio drops significantly
        # when the string lengths differ (e.g., "Aple" vs "Apple Inc").
        if score >= 60:
            scored.append((score, m))
    
    # Step 3: Sort by score descending, take top 5
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:5]
    
    return [
        {
            "beneficiary_name": m.beneficiary_name,
            "beneficiary_id_number": m.beneficiary_id_number,
            "beneficiary_country": m.beneficiary_country,
            "beneficiary_address": m.beneficiary_address,
            "beneficiary_contact_person": m.beneficiary_contact_person,
            "beneficiary_phone": m.beneficiary_phone,
            "beneficiary_email": m.beneficiary_email,
            "similarity_score": score,
        }
        for score, m in top
    ]


@router.get("/beneficiary-nearmatch")
def beneficiary_nearmatch(
    name: str = Query(..., min_length=2, description="Beneficiary name to check for near matches"),
    threshold: float = Query(0.85, ge=0.5, le=1.0, description="Minimum similarity ratio (0.85 = 85%)"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Checks if a beneficiary name closely matches any existing beneficiary
    in the customer's history. Returns matches ≥ threshold similarity.
    Used for the side-note warning in the approval/execution modal.
    """
    from app.models.models_issuance import IssuanceRequest
    from difflib import SequenceMatcher

    # Get unique beneficiary names for this customer
    beneficiaries = db.query(
        IssuanceRequest.beneficiary_name,
        IssuanceRequest.serial_number,
        IssuanceRequest.beneficiary_id_number,
    ).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.beneficiary_name != None,
        IssuanceRequest.is_deleted == False,
        IssuanceRequest.status != "DRAFT",
    ).order_by(IssuanceRequest.created_at.desc()).all()

    # Deduplicate by (name, id_number) combo — prioritize most recent
    seen = {}
    for b in beneficiaries:
        normalized = b.beneficiary_name.strip().lower()
        dedup_key = (normalized, (b.beneficiary_id_number or '').strip().lower())
        if dedup_key not in seen:
            seen[dedup_key] = b

    # Compare
    input_normalized = name.strip().lower()
    near_matches = []
    for dedup_key, record in seen.items():
        if dedup_key[0] == input_normalized:
            continue  # Skip exact match
        ratio = SequenceMatcher(None, input_normalized, dedup_key[0]).ratio()
        if ratio >= threshold:
            near_matches.append({
                "beneficiary_name": record.beneficiary_name,
                "beneficiary_id_number": record.beneficiary_id_number,
                "similarity": round(ratio * 100, 1),
                "last_seen_request": record.serial_number,
            })

    # Sort by similarity descending
    near_matches.sort(key=lambda x: x["similarity"], reverse=True)
    return near_matches[:5]


# ==============================================================================
# 9. DOCUMENT MANAGEMENT (Issuance Request Documents)
# ==============================================================================

from fastapi import UploadFile, File

@router.post("/requests/{request_id}/documents")
async def upload_request_document(
    request_id: int,
    document_type: str = Query(..., description="CONTRACT, PURCHASE_ORDER, THIRD_PARTY, SPECIAL_WORDING, OTHER"),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context)
):
    """Upload a document to an issuance request. Uses customer-specific GCS bucket."""
    from app.models.models_issuance import IssuanceRequest, IssuanceRequestDocument
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    from app.crud.crud_lg_document import _slugify_doc_type
    import uuid
    from datetime import datetime as dt
    
    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id
    ).first()
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    
    # Read file
    file_content = await file.read()
    file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
    
    # Generate unique filename
    unique_filename = f"{document_type}_{dt.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_extension}"
    
    # Construct GCS path: customer / request / lg (if known) / doc_type / file
    doc_type_slug = _slugify_doc_type(document_type)
    lg_folder = f"lg_{request.issued_lg_id}" if hasattr(request, 'issued_lg_id') and request.issued_lg_id else "request_docs"
    blob_path = f"customer_{current_user.customer_id}/requests/{request_id}/{lg_folder}/{doc_type_slug}/{unique_filename}"
    
    # Get customer-specific bucket or fallback
    from app.crud import crud_customer_configuration
    bucket_name = GCS_BUCKET_NAME
    bucket_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, "STORAGE_BUCKET_NAME"
    )
    if bucket_config and bucket_config.get('effective_value'):
        bucket_name = bucket_config['effective_value']
    
    # Upload
    stored_uri = await _upload_to_gcs(bucket_name, blob_path, file_content, file.content_type)
    if not stored_uri:
        raise HTTPException(status_code=500, detail="Failed to upload document")
    
    # Save metadata — use systematic filename for discoverability
    doc_display_name = _make_doc_filename(document_type, f"REQ-{request_id}", file.filename)
    doc = IssuanceRequestDocument(
        request_id=request_id,
        document_type=document_type,
        file_name=doc_display_name,
        file_path=stored_uri,
        uploaded_by=current_user.user_id
    )
    db.add(doc)
    db.flush()
    db.refresh(doc)
    
    return {
        "id": doc.id,
        "document_type": doc.document_type,
        "file_name": doc.file_name,
        "created_at": str(doc.created_at) if doc.created_at else None
    }


@router.get("/requests/{request_id}/documents")
def list_request_documents(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """List all documents attached to an issuance request."""
    from app.models.models_issuance import IssuanceRequestDocument
    
    docs = db.query(IssuanceRequestDocument).filter(
        IssuanceRequestDocument.request_id == request_id,
        IssuanceRequestDocument.is_deleted == False
    ).all()
    
    return [
        {
            "id": d.id,
            "document_type": d.document_type,
            "file_name": d.file_name,
            "created_at": str(d.created_at) if d.created_at else None,
            "ai_verification_result": d.ai_verification_result
        }
        for d in docs
    ]


