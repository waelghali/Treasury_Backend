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

@router.get("/requests/{request_id}/similarity-check")
def check_similarity_against_issued_lgs(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Scores the given request against all issued LGs and active requests for this customer.
    Weights: ref_type+number 30%, beneficiary_name 25%, amount 20%, lg_type 15%, expiry 10%.
    Returns matches ≥70%.
    """
    from app.models.models_issuance import IssuanceRequest
    from app.services.issuance_service import issuance_service
    
    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id
    ).first()
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
        
    result = issuance_service.get_similarity_matches(
        db=db,
        customer_id=current_user.customer_id,
        reference_type=request.reference_type,
        reference_number=request.reference_number,
        beneficiary_name=request.beneficiary_name,
        amount=float(request.amount) if request.amount else None,
        lg_type_id=request.lg_type_id,
        requested_expiry_date=request.requested_expiry_date,
        exclude_request_id=request_id
    )
    
    return {
        "request_id": request_id,
        "total_issued_compared": result.get("total_issued_compared", 0),
        "total_requests_compared": result.get("total_requests_compared", 0),
        "matches": result.get("matches", [])[:10]  # Top 10 at most
    }



# ==============================================================================
# EDIT NOTIFICATION HELPER
# ==============================================================================

@router.get("/issued-lgs")
def list_issued_lgs(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """List all Issued LG records for the customer, with comprehensive details."""
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    from app.models import Bank, Currency, User

    records = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == current_user.customer_id
    ).order_by(IssuedLGRecord.created_at.desc()).all()

    result = []
    for r in records:
        # Resolve bank: direct relationship first, then via facility chain
        bank_name = "N/A"
        resolved_bank_id = None
        facility_name = None
        facility_ref = None
        sub_limit_name = None
        
        # Priority 1: Direct bank_id on the record
        if r.bank_id and r.bank:
            bank_name = r.bank.name
            resolved_bank_id = r.bank_id
        
        # Priority 2: Through sub_limit → facility → bank
        if r.sub_limit and r.sub_limit.facility:
            f = r.sub_limit.facility
            if resolved_bank_id is None and f.bank:
                bank_name = f.bank.name
                resolved_bank_id = f.bank_id
            facility_name = f.facility_name
            facility_ref = f.reference_number
            sub_limit_name = r.sub_limit.limit_name

        currency_code = r.currency.iso_code if r.currency else "N/A"

        # Get linked request details
        request_data = None
        if r.request_id:
            req = db.query(IssuanceRequest).filter(IssuanceRequest.id == r.request_id).first()
            if req:
                request_data = {
                    "id": req.id,
                    "serial_number": req.serial_number,
                    "requestor_name": req.requestor_name,
                    "requestor_email": req.requestor_email,
                    "lg_type": req.lg_type.name if req.lg_type else None,
                    "lg_purpose": req.lg_purpose,
                    "beneficiary_name": req.beneficiary_name,
                    "beneficiary_address": req.beneficiary_address,
                    "reference_type": req.reference_type,
                    "reference_number": req.reference_number,
                    "project_name": req.project.name if req.project else None,
                    "department": req.department,
                    "amount": float(req.amount) if req.amount else 0,
                    "status": req.status,
                    "requested_expiry_date": str(req.requested_expiry_date) if req.requested_expiry_date else None,
                    "requires_special_wording": req.requires_special_wording,
                    "other_conditions": req.other_conditions,
                    "is_cross_border": req.is_cross_border or False,
                    "is_third_party": req.is_third_party or False,
                    "submitted_at": req.submitted_at.isoformat() if getattr(req, 'submitted_at', None) else None,
                    "submitted_by_user_id": getattr(req, 'submitted_by_user_id', None),
                    "applicable_rules": req.applicable_rules,
                    "cross_border_details": req.cross_border_details,
                    "treasury_enrichment": req.treasury_enrichment,
                    "reference_end_date": str(req.reference_end_date) if req.reference_end_date else None,
                    "created_at": req.created_at.isoformat() if req.created_at else None,
                    "approval_chain_audit": req.approval_chain_audit or [],
                    "metadata_json": req.metadata_json,
                    "cancellation_reason": req.cancellation_reason,
                }

        # Issued-by user
        issued_by_name = None
        if r.issued_by_user_id:
            user = db.query(User).filter(User.id == r.issued_by_user_id).first()
            if user:
                issued_by_name = f"{user.first_name} {user.last_name}" if hasattr(user, 'first_name') else user.email

        # Current owner name
        current_owner_name = None
        if getattr(r, 'current_owner_user_id', None):
            owner = db.query(User).filter(User.id == r.current_owner_user_id).first()
            if owner:
                current_owner_name = owner.email

        result.append({
            "id": r.id,
            "lg_ref_number": r.lg_ref_number,
            "internal_serial": r.internal_serial,
            "beneficiary_name": r.beneficiary_name,
            "current_amount": float(r.current_amount),
            "currency_code": currency_code,
            "currency_id": r.currency_id,
            "issue_date": str(r.issue_date) if r.issue_date else None,
            "expiry_date": str(r.expiry_date) if r.expiry_date else None,
            "status": r.status,
            "issuance_method": r.issuance_method,
            # Bank & Facility
            "bank_name": bank_name,
            "bank_id": resolved_bank_id,
            "facility_name": facility_name,
            "facility_ref": facility_ref,
            "sub_limit_name": sub_limit_name,
            # Bank Confirmation
            "bank_confirmation_ref": r.bank_confirmation_ref,
            "bank_confirmation_date": str(r.bank_confirmation_date) if r.bank_confirmation_date else None,
            # Delivery Tracking
            "delivery_date": str(r.delivery_date) if r.delivery_date else None,
            "delivery_method": r.delivery_method,
            "delivery_notes": r.delivery_notes,
            # Bank Reply Tracking
            "bank_reply_type": r.bank_reply_type,
            "bank_reply_date": str(r.bank_reply_date) if r.bank_reply_date else None,
            "bank_reply_notes": r.bank_reply_notes,
            "bank_lg_number": r.bank_lg_number,
            # Verification
            "verification_status": r.verification_status,
            "verification_notes": r.verification_notes,
            "verified_at": r.verified_at.isoformat() if r.verified_at else None,
            "verified_by_user_id": r.verified_by_user_id,
            # Handover
            "handover_date": str(r.handover_date) if r.handover_date else None,
            "handover_notes": r.handover_notes,
            "handover_by_user_id": r.handover_by_user_id,
            "recipient_name": r.recipient_name,
            # Custody
            "original_copy_collected_by": r.original_copy_collected_by,
            "original_copy_collected_date": str(r.original_copy_collected_date) if r.original_copy_collected_date else None,
            "soft_copy_path": r.soft_copy_path,
            "custody_holder": r.custody_holder,
            "custody_transfer_log": r.custody_transfer_log or [],
            "action_history": r.action_history or [],
            # Accountability
            "issued_by_user_id": r.issued_by_user_id,
            "issued_by_name": issued_by_name,
            # Phase A new fields
            "reference_validity_flag": getattr(r, 'reference_validity_flag', None),
            "current_owner_user_id": getattr(r, 'current_owner_user_id', None),
            "current_owner_name": current_owner_name,
            # Bank LG fields for comparison
            "bank_lg_amount": float(r.bank_lg_amount) if r.bank_lg_amount else None,
            "bank_lg_issue_date": str(r.bank_lg_issue_date) if r.bank_lg_issue_date else None,
            "bank_lg_expiry_date": str(r.bank_lg_expiry_date) if r.bank_lg_expiry_date else None,
            # LG Copy Documents (for admin review)
            "lg_copy_documents": _get_lg_copy_docs(db, r.request_id) if r.request_id else [],
            # Linked Request
            "request": request_data,
            # Pricing: facility-based or manual
            "sub_limit_id": r.facility_sub_limit_id,
            "manual_pricing": r.manual_pricing,
            "facility_pricing": {
                "commission_rate": float(r.sub_limit.default_commission_rate) if r.sub_limit and r.sub_limit.default_commission_rate else None,
                "min_commission": float(r.sub_limit.default_min_commission) if r.sub_limit and r.sub_limit.default_min_commission else None,
                "flat_fee": float(r.sub_limit.default_flat_fee) if r.sub_limit and r.sub_limit.default_flat_fee else None,
                "margin_pct": float(r.sub_limit.default_cash_margin_pct) if r.sub_limit and r.sub_limit.default_cash_margin_pct else None,
            } if r.facility_sub_limit_id and r.sub_limit else None,
            # Cancellation notice tracking
            "cancellation_notice": r.cancellation_notice,
            # Cancellation metadata (from linked request — IssuedLGRecord has no metadata_json)
            "metadata_json": request_data.get("metadata_json") if request_data else None,
            # Timestamps
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        })

    return result


@router.post("/lg-records/{lg_id}/resolve-cancellation")
async def resolve_lg_cancellation(
    lg_id: int,
    payload: dict,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Admin approves or rejects a pending LG cancellation request.
    Approve: cancel LG, reopen request, generate letter if requested (+ MaintenanceAction for tracking).
    Reject: restore previous status.
    """
    from app.models.models_issuance import (
        IssuedLGRecord, IssuanceRequest, IssuanceExposureEntry,
        IssuanceMaintenanceAction
    )
    from app.crud.crud import log_action
    from datetime import datetime, date

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.status != "CANCEL_REQUESTED":
        raise HTTPException(status_code=400, detail=f"LG is not pending cancellation. Current status: {lg.status}")

    # Read cancel metadata from the linked IssuanceRequest (not from LG — it has no metadata_json)
    linked_request = db.query(IssuanceRequest).filter(IssuanceRequest.lg_record_id == lg.id).first()
    meta = dict(linked_request.metadata_json or {}) if linked_request and linked_request.metadata_json else {}
    cancel_meta = meta.get("pending_cancellation")
    # Fallback: try custody_transfer_log for previously-queued cancellations
    if not cancel_meta:
        for entry in reversed(lg.custody_transfer_log or []):
            if entry.get("action") == "CANCEL_REQUESTED":
                cancel_meta = {
                    "cancel_reason": entry.get("reason", ""),
                    "previous_status": entry.get("previous_status", "INTERNAL_PROCESSING"),
                    "requested_by_user_id": entry.get("user_id"),
                    "requested_at": entry.get("timestamp"),
                    "issue_cancellation_letter": True,
                }
                break
    if not cancel_meta:
        raise HTTPException(status_code=400, detail="No pending cancellation data found.")

    approved = payload.get("approved", False)
    admin_note = payload.get("note", "")
    previous_status = cancel_meta.get("previous_status", "INTERNAL_PROCESSING")

    # Audit trail
    ctl = list(lg.custody_transfer_log or [])
    ctl.append({
        "action": "CANCEL_RESOLVED",
        "decision": "APPROVED" if approved else "REJECTED",
        "admin_user_id": current_user.user_id,
        "admin_note": admin_note,
        "timestamp": datetime.utcnow().isoformat(),
    })
    lg.custody_transfer_log = ctl

    meta.pop("pending_cancellation", None)
    if linked_request:
        linked_request.metadata_json = meta

    cancellation_letter_generated = False
    maintenance_action_id = None

    if approved:
        # === EXECUTE CANCELLATION (same logic as CANCELLED_BY_USER in record-bank-reply) ===
        lg.status = "CANCELLED"
        lg.bank_reply_type = "CANCELLED_BY_USER"
        lg.bank_reply_date = date.today().isoformat()
        lg.bank_reply_notes = cancel_meta.get("cancel_reason", "")

        # Reopen the original IssuanceRequest for reprocessing
        if lg.request_id:
            request_obj = db.query(IssuanceRequest).get(lg.request_id)
            if request_obj:
                request_obj.status = "APPROVED_INTERNAL"
                request_obj.lg_record_id = None
                db.query(IssuanceExposureEntry).filter(
                    IssuanceExposureEntry.request_id == lg.request_id,
                    IssuanceExposureEntry.is_active == True
                ).update({"is_active": False})
                request_obj.selected_sub_limit_id = None
                request_obj.locked_for_issuance = False

                # Audit on request
                audit = list(request_obj.approval_chain_audit or [])
                audit.append({
                    "action": "USER_CANCELLED_APPROVED",
                    "admin_user_id": current_user.user_id,
                    "note": f"Cancellation approved by admin. Reason: {cancel_meta.get('cancel_reason', '')}",
                    "previous_status": previous_status,
                    "lg_record_id": lg.id,
                    "timestamp": datetime.utcnow().isoformat()
                })
                request_obj.approval_chain_audit = audit

        # Generate cancellation letter if requested
        if cancel_meta.get("issue_cancellation_letter"):
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
                        "original_notes": cancel_meta.get("cancel_reason", ""),
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
                        import logging as _log
                        _log.getLogger(__name__).warning(f"GCS upload failed for cancellation notice, saving locally: {gcs_err}")
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
                        "approved_by_user_id": current_user.user_id,
                        "bank_name": bank.name if bank else None,
                        "delivery_date": None,
                        "delivery_method": None,
                    }
                    cancellation_letter_generated = True

                    # Create IssuanceMaintenanceAction for letter lifecycle tracking
                    # (print → remind → escalate pipeline)
                    serial_prefix = "ISS-CANCEL"
                    from sqlalchemy import func as sa_func
                    max_serial = db.query(sa_func.count(IssuanceMaintenanceAction.id)).filter(
                        IssuanceMaintenanceAction.action_type == "CANCEL_BANK_REQUEST"
                    ).scalar() or 0
                    serial_number = f"{serial_prefix}-{date.today().year}-{(max_serial + 1):04d}"

                    maint_action = IssuanceMaintenanceAction(
                        issued_lg_id=lg.id,
                        action_type="CANCEL_BANK_REQUEST",
                        status="EXECUTED",
                        action_data={
                            "cancel_reason": cancel_meta.get("cancel_reason", ""),
                            "approved_by": current_user.user_id,
                        },
                        initiated_by_user_id=cancel_meta.get("requested_by_user_id"),
                        notes=cancel_meta.get("cancel_reason", ""),
                        letter_generated_path=pdf_path,
                        letter_serial_number=serial_number,
                        instruction_status="Instruction Issued",
                        is_printed=False,
                    )
                    db.add(maint_action)
                    db.flush()
                    maintenance_action_id = maint_action.id

            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Failed to generate cancellation letter: {e}", exc_info=True)

        log_action(db, current_user.user_id, "LG_CANCEL_APPROVED",
                   "IssuedLGRecord", lg.id,
                   {"reason": cancel_meta.get("cancel_reason"), "letter_generated": cancellation_letter_generated},
                   current_user.customer_id)

        # Notify the requestor
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            requestor_id = cancel_meta.get("requested_by_user_id")
            if requestor_id:
                _now = datetime.utcnow()
                notif = SystemNotificationCreate(
                    content=f"Your cancellation request for LG {lg.lg_ref_number} has been approved."
                            + (" A cancellation letter has been generated." if cancellation_letter_generated else ""),
                    notification_type="LG_CANCEL_APPROVED",
                    start_date=_now,
                    end_date=_now + __import__('datetime').timedelta(days=30),
                    target_user_ids=[requestor_id],
                    target_customer_ids=[current_user.customer_id],
                    link="/end-user/issuance/issued-lgs",
                )
                crud_notification.create_notification(db, obj_in=notif)
        except Exception:
            pass

    else:
        # === REJECTED: restore previous status ===
        lg.status = previous_status

        log_action(db, current_user.user_id, "LG_CANCEL_REJECTED",
                   "IssuedLGRecord", lg.id,
                   {"reason": cancel_meta.get("cancel_reason"), "admin_note": admin_note,
                    "restored_status": previous_status},
                   current_user.customer_id)

        # Notify the requestor
        try:
            from app.schemas.all_schemas import SystemNotificationCreate
            from app.crud.crud import crud_notification
            requestor_id = cancel_meta.get("requested_by_user_id")
            if requestor_id:
                _now = datetime.utcnow()
                notif = SystemNotificationCreate(
                    content=f"Your cancellation request for LG {lg.lg_ref_number} was rejected. "
                            f"Reason: {admin_note or 'No reason provided.'}",
                    notification_type="LG_CANCEL_REJECTED",
                    start_date=_now,
                    end_date=_now + __import__('datetime').timedelta(days=30),
                    target_user_ids=[requestor_id],
                    target_customer_ids=[current_user.customer_id],
                    link="/end-user/issuance/issued-lgs",
                )
                crud_notification.create_notification(db, obj_in=notif)
        except Exception:
            pass

    db.commit()

    return {
        "message": f"Cancellation {'approved' if approved else 'rejected'}.",
        "id": lg.id,
        "status": lg.status,
        "cancellation_letter_generated": cancellation_letter_generated,
        "cancellation_notice_download_url": f"/api/v1/issuance/lg-records/{lg.id}/cancellation-notice-pdf" if cancellation_letter_generated else None,
        "maintenance_action_id": maintenance_action_id,
    }

@router.patch("/lg-records/{lg_id}/verify")
def verify_lg_copy(
    lg_id: int,
    payload: dict,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Step 5.6: Verify issued LG copy against original request.
    Auto-compares bank-confirmed values with request values.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    from app.crud.crud import log_action
    from datetime import datetime

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.verification_status not in (None, "PENDING", "DISCREPANCY", "DISCREPANCY_REJECTED"):
        raise HTTPException(status_code=400, detail=f"Cannot verify — current status: {lg.verification_status}")

    request = db.query(IssuanceRequest).get(lg.request_id) if lg.request_id else None

    # Allow user to update bank values if provided
    if payload.get("bank_lg_number"):
        lg.bank_lg_number = payload["bank_lg_number"]
    if payload.get("bank_lg_amount"):
        lg.bank_lg_amount = payload["bank_lg_amount"]
    if payload.get("bank_lg_issue_date"):
        lg.bank_lg_issue_date = payload["bank_lg_issue_date"]
    if payload.get("bank_lg_expiry_date"):
        lg.bank_lg_expiry_date = payload["bank_lg_expiry_date"]

    # Compliance auto-check
    discrepancies = []
    if request:
        # Amount check
        if lg.bank_lg_amount is not None and request.amount is not None:
            from decimal import Decimal
            bank_amt = Decimal(str(lg.bank_lg_amount))
            req_amt = Decimal(str(request.amount))
            if bank_amt != req_amt:
                discrepancies.append({
                    "field": "amount",
                    "requested": str(req_amt),
                    "bank_confirmed": str(bank_amt),
                    "severity": "HIGH"
                })

        # Expiry date check — normalize both to YYYY-MM-DD to avoid format mismatches
        if lg.bank_lg_expiry_date and request.requested_expiry_date:
            import re as _re
            def _to_date_str(v):
                m = _re.match(r'(\d{4}-\d{2}-\d{2})', str(v).strip())
                return m.group(1) if m else str(v)
            if _to_date_str(lg.bank_lg_expiry_date) != _to_date_str(request.requested_expiry_date):
                discrepancies.append({
                    "field": "expiry_date",
                    "requested": str(request.requested_expiry_date),
                    "bank_confirmed": str(lg.bank_lg_expiry_date),
                    "severity": "MEDIUM"
                })

        # Beneficiary name — allow substring containment (bank may use full legal name)
        bank_beneficiary = payload.get("bank_beneficiary_name", "")
        if bank_beneficiary and request.beneficiary_name:
            r = request.beneficiary_name.strip().lower()
            e = bank_beneficiary.strip().lower()
            if r not in e and e not in r:
                from difflib import SequenceMatcher as _SM
                ratio = _SM(None, r, e).ratio()
                if ratio < 0.80:
                    discrepancies.append({
                        "field": "beneficiary_name",
                        "requested": request.beneficiary_name,
                        "bank_confirmed": bank_beneficiary,
                        "severity": "MEDIUM"
                    })

        # Currency check
        bank_currency = payload.get("bank_currency_id") or payload.get("bank_currency")
        if bank_currency and request.currency_id is not None:
            # Support both currency_id (int) and currency code (string)
            if isinstance(bank_currency, int) or (isinstance(bank_currency, str) and bank_currency.isdigit()):
                currency_match = int(bank_currency) == request.currency_id
            else:
                # Lookup by currency code
                from app.models.models import Currency as CurrencyModel
                req_currency = db.query(CurrencyModel).filter(CurrencyModel.id == request.currency_id).first()
                currency_match = req_currency and req_currency.iso_code.upper() == str(bank_currency).upper()
            if not currency_match:
                req_currency_obj = db.query(CurrencyModel).filter(CurrencyModel.id == request.currency_id).first() if 'req_currency' not in dir() else req_currency
                discrepancies.append({
                    "field": "currency",
                    "requested": req_currency_obj.iso_code if req_currency_obj else str(request.currency_id),
                    "bank_confirmed": str(bank_currency),
                    "severity": "HIGH"
                })

        # LG Type check
        bank_lg_type = payload.get("bank_lg_type_id") or payload.get("bank_lg_type")
        if bank_lg_type and request.lg_type_id is not None:
            if isinstance(bank_lg_type, int) or (isinstance(bank_lg_type, str) and bank_lg_type.isdigit()):
                lg_type_match = int(bank_lg_type) == request.lg_type_id
            else:
                from app.models.models import LgType
                req_lg_type = db.query(LgType).filter(LgType.id == request.lg_type_id).first()
                lg_type_match = req_lg_type and req_lg_type.name.strip().lower() == str(bank_lg_type).strip().lower()
            if not lg_type_match:
                discrepancies.append({
                    "field": "lg_type",
                    "requested": str(request.lg_type_id),
                    "bank_confirmed": str(bank_lg_type),
                    "severity": "MEDIUM"
                })

        # Purpose — allow substring containment (bank often expands purpose text)
        bank_purpose = payload.get("bank_lg_purpose", "")
        if bank_purpose and request.lg_purpose:
            r = request.lg_purpose.strip().lower()
            e = bank_purpose.strip().lower()
            if r not in e and e not in r:
                from difflib import SequenceMatcher as _SM
                ratio = _SM(None, r, e).ratio()
                if ratio < 0.50:
                    discrepancies.append({
                        "field": "purpose",
                        "requested": request.lg_purpose,
                        "bank_confirmed": bank_purpose,
                        "severity": "MEDIUM"
                    })

        # Operational Status check (particularly for Advance Payment LGs)
        bank_operational_status = payload.get("bank_operational_status", "")
        if bank_operational_status and request.operational_status:
            if bank_operational_status.strip().lower() != request.operational_status.strip().lower():
                discrepancies.append({
                    "field": "operational_status",
                    "requested": request.operational_status,
                    "bank_confirmed": bank_operational_status,
                    "severity": "MEDIUM"
                })

    # Build full comparison (ALL fields, matched + mismatched) for the admin review
    all_comparisons = []
    if request:
        from decimal import Decimal
        import re as _re2
        def _norm_date(v):
            m = _re2.match(r'(\d{4}-\d{2}-\d{2})', str(v).strip())
            return m.group(1) if m else str(v)
        def _name_match(a, b):
            if not a or not b: return True
            al, bl = a.strip().lower(), b.strip().lower()
            if al in bl or bl in al: return True
            from difflib import SequenceMatcher as _SM2
            return _SM2(None, al, bl).ratio() >= 0.80
        # Amount
        if lg.bank_lg_amount is not None and request.amount is not None:
            bank_amt = Decimal(str(lg.bank_lg_amount))
            req_amt = Decimal(str(request.amount))
            all_comparisons.append({"field": "Amount", "requested": str(req_amt), "bank_confirmed": str(bank_amt), "severity": "HIGH" if bank_amt != req_amt else "OK", "match": bank_amt == req_amt})
        # Expiry Date — normalize to YYYY-MM-DD
        if request.requested_expiry_date:
            bank_exp = _norm_date(lg.bank_lg_expiry_date) if lg.bank_lg_expiry_date else "—"
            req_exp = _norm_date(request.requested_expiry_date)
            match_d = bank_exp == "—" or bank_exp == req_exp
            all_comparisons.append({"field": "Expiry Date", "requested": req_exp, "bank_confirmed": bank_exp, "severity": "MEDIUM" if not match_d else "OK", "match": match_d})
        # Beneficiary — substring containment
        bank_beneficiary = payload.get("bank_beneficiary_name", "")
        if request.beneficiary_name:
            match_b = not bank_beneficiary or _name_match(bank_beneficiary, request.beneficiary_name)
            all_comparisons.append({"field": "Beneficiary", "requested": request.beneficiary_name, "bank_confirmed": bank_beneficiary or "—", "severity": "MEDIUM" if not match_b else "OK", "match": match_b})
        # Currency
        bank_currency_val = payload.get("bank_currency_id") or payload.get("bank_currency")
        if request.currency_id:
            from app.models.models import Currency as CurrencyModel
            req_curr_obj = db.query(CurrencyModel).filter(CurrencyModel.id == request.currency_id).first()
            req_curr_name = req_curr_obj.iso_code if req_curr_obj else str(request.currency_id)
            match_c = not bank_currency_val or req_curr_name.upper() == str(bank_currency_val).upper()
            all_comparisons.append({"field": "Currency", "requested": req_curr_name, "bank_confirmed": str(bank_currency_val) if bank_currency_val else "—", "severity": "HIGH" if not match_c else "OK", "match": match_c})

        # LG Type — substring containment
        bank_lg_type_val = payload.get("bank_lg_type_id") or payload.get("bank_lg_type")
        if request.lg_type_id:
            from sqlalchemy import text as sa_text
            lg_type_row = db.execute(sa_text("SELECT name FROM lg_types WHERE id = :id"), {"id": request.lg_type_id}).first()
            req_lg_type_name = lg_type_row[0] if lg_type_row else str(request.lg_type_id)
            match_t = not bank_lg_type_val or _name_match(req_lg_type_name, str(bank_lg_type_val))
            all_comparisons.append({"field": "LG Type", "requested": req_lg_type_name, "bank_confirmed": str(bank_lg_type_val) if bank_lg_type_val else "—", "severity": "MEDIUM" if not match_t else "OK", "match": match_t})
        # Purpose — substring containment + 50% fuzzy
        bank_purpose_val = payload.get("bank_lg_purpose", "")
        if request.lg_purpose:
            r_p, e_p = request.lg_purpose.strip().lower(), bank_purpose_val.strip().lower()
            if not bank_purpose_val:
                match_p = True
            elif r_p in e_p or e_p in r_p:
                match_p = True
            else:
                from difflib import SequenceMatcher as _SM3
                match_p = _SM3(None, r_p, e_p).ratio() >= 0.50
            all_comparisons.append({"field": "Purpose", "requested": request.lg_purpose, "bank_confirmed": bank_purpose_val or "—", "severity": "MEDIUM" if not match_p else "OK", "match": match_p})

    # Determine result
    force_accept = payload.get("force_accept", False)

    if not discrepancies:
        # D4: Enforce bank_lg_number before allowing MATCHED status
        if not lg.bank_lg_number and not payload.get("force_no_number", False):
            raise HTTPException(
                status_code=400,
                detail="Bank LG number is required before confirmation. "
                       "Set 'force_no_number' to true if the bank did not assign a number."
            )
        lg.verification_status = "MATCHED"
        lg.status = "LG_ISSUED"
    elif force_accept:
        # Only corporate_admin or checker can force-accept discrepancies
        from app.constants import UserRole
        if current_user.role not in (UserRole.CORPORATE_ADMIN, UserRole.CORPORATE_ADMIN.value, UserRole.CHECKER, UserRole.CHECKER.value):
            raise HTTPException(
                status_code=403,
                detail="Only Corporate Admin or Checker can accept discrepancies. Please submit for review."
            )
        # D4: Enforce bank_lg_number before allowing ACCEPTED status
        if not lg.bank_lg_number and not payload.get("force_no_number", False):
            raise HTTPException(
                status_code=400,
                detail="Bank LG number is required before confirmation. "
                       "Set 'force_no_number' to true if the bank did not assign a number."
            )
        lg.verification_status = "ACCEPTED"
        lg.verification_notes = payload.get("verification_notes", "Discrepancies manually accepted")
        lg.status = "LG_ISSUED"
    else:
        lg.verification_status = "DISCREPANCY"
        
        # Build readable text summary of actual discrepancies
        disc_text = "System detected the following discrepancies:\n"
        actual_discrepancies = [c for c in all_comparisons if not c['match']]
        for c in actual_discrepancies:
            disc_text += f"- {c['field'].title()}: Requested '{c['requested']}', Bank Confirmed '{c['bank_confirmed']}' (Severity: {c['severity']})\n"
        
        user_notes = payload.get("verification_notes", "").strip()
        if user_notes:
            disc_text += f"\nRequestor Comment:\n{user_notes}"
            
        lg.verification_notes = disc_text
        # Status stays LG_ISSUED — verification is advisory, not blocking

    lg.verified_by_user_id = current_user.user_id
    lg.verified_at = datetime.utcnow()

    log_action(db, current_user.user_id, "ISSUANCE_LG_VERIFIED", "IssuedLGRecord", lg.id,
               {"verification_status": lg.verification_status, "discrepancies": discrepancies},
               current_user.customer_id)

    # Notify requestor on confirmation
    if lg.status == "LG_ISSUED" and request and request.requestor_email:
        _send_requestor_status_notification(
            db, background_tasks, request, "VERIFIED", lg
        )

    return {
        "message": f"Verification complete: {lg.verification_status}",
        "id": lg.id,
        "status": lg.status,
        "verification_status": lg.verification_status,
        "discrepancies": discrepancies
    }


@router.patch("/lg-records/{lg_id}/manual-pricing")
def update_manual_pricing(
    lg_id: int,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    D3: Update manual pricing on an IssuedLGRecord.
    Only allowed for LGs issued without a facility (sub_limit_id is NULL).
    """
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.facility_sub_limit_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Cannot set manual pricing on a facility-backed LG. Pricing is derived from the facility terms."
        )

    # Build clean pricing object
    pricing = {
        "commission_rate": payload.get("commission_rate"),
        "flat_fee": payload.get("flat_fee"),
        "margin_pct": payload.get("margin_pct"),
        "notes": payload.get("notes"),
    }
    # Remove None values for clean storage
    pricing = {k: v for k, v in pricing.items() if v is not None}

    lg.manual_pricing = pricing if pricing else None
    db.flush()

    log_action(db, current_user.user_id, "ISSUANCE_MANUAL_PRICING_UPDATED", "IssuedLGRecord", lg.id,
               {"manual_pricing": pricing}, current_user.customer_id)

    return {
        "message": "Manual pricing updated successfully.",
        "id": lg.id,
        "manual_pricing": lg.manual_pricing
    }


@router.post("/lg-records/{lg_id}/reject-discrepancy")
def reject_discrepancy(
    lg_id: int,
    payload: dict = Body({}),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Corporate Admin rejects discrepancies — resets verification_status to PENDING
    so the end user can re-upload a corrected LG copy.
    Bank reply data is kept intact (the bank DID reply).
    """
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action

    # Only corporate_admin or checker can reject
    from app.constants import UserRole as _UserRole
    if current_user.role not in (_UserRole.CORPORATE_ADMIN, _UserRole.CORPORATE_ADMIN.value, _UserRole.CHECKER, _UserRole.CHECKER.value):
        raise HTTPException(status_code=403, detail="Only corporate admins or checkers can reject discrepancies.")

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.verification_status != "DISCREPANCY":
        raise HTTPException(status_code=400, detail=f"LG is not in DISCREPANCY status (current: {lg.verification_status})")

    notes = payload.get("notes", "Discrepancies rejected by corporate admin — re-upload required.")
    from datetime import datetime

    # Log rejection before resetting
    log_action(
        db, current_user.user_id, "reject_discrepancy", "issuance",
        lg.id,
        {"notes": notes, "previous_status": "DISCREPANCY",
         "previous_verification_notes": lg.verification_notes},
        current_user.customer_id,
    )

    # Append rejection note to verification_notes (preserve the discrepancy details)
    existing_notes = lg.verification_notes or ""
    rejection_stamp = f"\n--- REJECTED by Admin ({datetime.utcnow().strftime('%Y-%m-%d %H:%M')}) ---\n{notes}"
    lg.verification_notes = existing_notes + rejection_stamp

    # Set DISCREPANCY_REJECTED — this keeps the item visible in the admin's history
    # while also signalling to the end user that they must re-upload a corrected LG copy.
    # The verify endpoint now accepts DISCREPANCY_REJECTED as a re-verifiable status.
    lg.verification_status = "DISCREPANCY_REJECTED"
    lg.status = "LG_ISSUED"
    # DO NOT clear bank_reply_type, bank_reply_date, bank_lg_number, bank_lg_amount, etc.
    # The bank DID reply — those values stay. Only the verification needs to be redone.

    db.commit()

    return {
        "message": "Discrepancy rejected — end user can now re-upload a corrected LG copy.",
        "id": lg.id,
        "verification_status": lg.verification_status,
        "status": lg.status,
    }

@router.get("/lg-records/{lg_id}/post-issuance-status")
def get_post_issuance_status(
    lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Returns the full post-issuance timeline for a given LG record.
    Used by the PostIssuanceTracker frontend component.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceRequestDocument, IssuanceFacility
    from app.crud import crud_customer_configuration
    from app.constants import GlobalConfigKey
    from datetime import date, timedelta

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    request = db.query(IssuanceRequest).get(lg.request_id) if lg.request_id else None

    # Get documents
    docs = []
    if lg.request_id:
        doc_records = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == lg.request_id,
            IssuanceRequestDocument.document_type.in_(["DELIVERY_PROOF", "BANK_REPLY", "BANK_LG_COPY"])
        ).all()
        docs = [{"id": d.id, "type": d.document_type, "file_name": d.file_name,
                 "created_at": d.created_at.isoformat() if d.created_at else None} for d in doc_records]

    # Compute SLA info
    delivery_sla_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE
    )
    delivery_sla_days = int((delivery_sla_config or {}).get("effective_value", 3))

    # Bank reply SLA: use facility SLA if available, else fallback to 5
    bank_reply_sla_days = 5
    if request and request.selected_sub_limit_id:
        from sqlalchemy.orm import joinedload
        sub_limit = db.query(IssuanceFacilitySubLimit).options(
            joinedload(IssuanceFacilitySubLimit.facility)
        ).get(request.selected_sub_limit_id)
        if sub_limit and sub_limit.facility and sub_limit.facility.sla_agreement_days:
            bank_reply_sla_days = sub_limit.facility.sla_agreement_days

    # Delivery proof requirement
    proof_required_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_DELIVERY
    )
    delivery_proof_required = (proof_required_config or {}).get("effective_value", "false").lower() == "true"

    # SLA breach checks
    today = date.today()
    delivery_sla_breached = (
        lg.status in ("ISSUED", "INTERNAL_PROCESSING") and
        not lg.delivery_date and
        lg.created_at and
        (today - lg.created_at.date()).days > delivery_sla_days
    )
    # Bank reply SLA: starts from delivery_date if recorded, else from created_at
    bank_reply_start_date = lg.delivery_date or (lg.created_at.date() if lg.created_at else None)
    bank_reply_sla_breached = (
        bank_reply_start_date and
        not lg.bank_reply_type and
        (today - bank_reply_start_date).days > bank_reply_sla_days
    )

    # Build timeline steps
    steps = [
        {
            "step": "ISSUED",
            "label": "Request Issued to Bank",
            "status": "completed",
            "date": lg.created_at.isoformat() if lg.created_at else None,
            "details": {"lg_ref": lg.lg_ref_number, "method": lg.issuance_method}
        },
        {
            "step": "DELIVERY",
            "label": "Delivered to Bank",
            "status": "completed" if lg.delivery_date else ("sla_breach" if delivery_sla_breached else "pending"),
            "date": str(lg.delivery_date) if lg.delivery_date else None,
            "details": {
                "method": lg.delivery_method,
                "notes": lg.delivery_notes,
                "proof_required": delivery_proof_required,
                "sla_days": delivery_sla_days,
            }
        },
        {
            "step": "BANK_REPLY",
            "label": "Bank Reply",
            "status": (
                "completed" if lg.bank_reply_type else
                ("sla_breach" if bank_reply_sla_breached else "pending")
            ),
            "date": str(lg.bank_reply_date) if lg.bank_reply_date else None,
            "details": {
                "reply_type": lg.bank_reply_type,
                "notes": lg.bank_reply_notes,
                "bank_lg_number": lg.bank_lg_number,
                "bank_lg_amount": str(lg.bank_lg_amount) if lg.bank_lg_amount else None,
                "bank_lg_expiry_date": str(lg.bank_lg_expiry_date) if lg.bank_lg_expiry_date else None,
                "sla_days": bank_reply_sla_days,
                "inquiry_log": lg.bank_inquiry_log or [],
            }
        },
    ]

    # Insert CANCELLATION_NOTICE step after BANK_REPLY (only for NO_RESPONSE)
    if lg.bank_reply_type == "NO_RESPONSE":
        cn = lg.cancellation_notice or {}
        cn_delivery_done = bool(cn.get("delivery_date"))
        cn_reply_done = bool(cn.get("bank_reply_date"))

        if cn:
            # Determine step status
            if cn_reply_done:
                cn_status = "completed"
            elif cn_delivery_done:
                cn_status = "pending_reply"
            elif cn.get("generated_at"):
                cn_status = "pending_delivery"
            else:
                cn_status = "pending"

            steps.append({
                "step": "CANCELLATION_NOTICE",
                "label": "Cancellation Notice to Bank",
                "status": cn_status,
                "date": cn.get("generated_at"),
                "details": {
                    "generated_at": cn.get("generated_at"),
                    "bank_name": cn.get("bank_name"),
                    "delivery_date": cn.get("delivery_date"),
                    "delivery_method": cn.get("delivery_method"),
                    "delivery_notes": cn.get("delivery_notes"),
                    "bank_reply_date": cn.get("bank_reply_date"),
                    "bank_reply_notes": cn.get("bank_reply_notes"),
                    "has_pdf": bool(cn.get("pdf_path")),
                }
            })
        else:
            # No notice generated — show as suggestion
            steps.append({
                "step": "CANCELLATION_NOTICE",
                "label": "Cancellation Notice to Bank",
                "status": "not_generated",
                "date": None,
                "details": {"has_pdf": False}
            })

    # Continue with remaining steps (only for LG_ISSUED flow)
    steps.extend([
        {
            "step": "VERIFICATION",
            "label": "LG Copy Verification",
            "status": (
                "completed" if lg.verification_status in ("MATCHED", "ACCEPTED") else
                ("rejected" if lg.verification_status == "DISCREPANCY_REJECTED" else
                 ("discrepancy" if lg.verification_status == "DISCREPANCY" else
                  ("pending" if lg.bank_reply_type == "LG_ISSUED" else "future")))
            ),
            "date": lg.verified_at.isoformat() if lg.verified_at else None,
            "details": {
                "verification_status": lg.verification_status,
                "notes": lg.verification_notes,
                "verified_by": lg.verified_by_user_id,
                "request_amount": str(request.amount) if request and request.amount else None,
                "request_expiry": str(request.requested_expiry_date) if request else None,
                "request_beneficiary": request.beneficiary_name if request else None,
            }
        },
        {
            "step": "HANDOVER",
            "label": "LG Handover",
            "status": (
                "completed" if lg.handover_date else
                ("pending" if lg.verification_status in ("MATCHED", "ACCEPTED") else "future")
            ),
            "date": str(lg.handover_date) if lg.handover_date else None,
            "details": {
                "recipient_name": lg.recipient_name,
                "recipient_email": lg.recipient_email,
                "recipient_department": lg.recipient_department,
                "recipient_job_title": lg.recipient_job_title,
                "recipient_phone": lg.recipient_phone,
                "recipient_employee_id": lg.recipient_employee_id,
                "recipient_manager_email": lg.recipient_manager_email,
                "notes": lg.handover_notes,
                # Pre-fill defaults from requestor
                "requestor_defaults": {
                    "name": request.requestor_name if request else None,
                    "email": request.requestor_email if request else None,
                    "department": request.department if request else None,
                    "job_title": request.job_title if request else None,
                    "phone": request.phone_number if request else None,
                    "employee_id": request.employee_id if request else None,
                    "manager_email": request.manager_email if request else None,
                    "second_line_manager_email": request.second_line_manager_email if request else None,
                } if request else None,
            }
        }
    ])

    # Get recipient field config
    recipient_field_config = {}
    form_config = db.query(CustomerFormConfiguration).filter(
        CustomerFormConfiguration.customer_id == current_user.customer_id
    ).first()
    if form_config and hasattr(form_config, 'recipient_field_configurations') and form_config.recipient_field_configurations:
        recipient_field_config = form_config.recipient_field_configurations

    # Get handover signed copy requirement
    handover_doc_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, GlobalConfigKey.DOC_MANDATORY_LG_HANDOVER
    )
    handover_signed_copy_required = (handover_doc_config or {}).get("effective_value", "false").lower() == "true"

    # Build expected values for client-side comparison (manual entry path)
    expected_values = None
    if request:
        from app.models.models import Currency as CurrencyModel
        req_currency_obj = db.query(CurrencyModel).filter(CurrencyModel.id == request.currency_id).first() if request.currency_id else None
        expected_values = {
            "amount": str(request.amount) if request.amount else None,
            "expiry_date": str(request.requested_expiry_date) if request.requested_expiry_date else None,
            "beneficiary_name": request.beneficiary_name,
            "currency": req_currency_obj.iso_code if req_currency_obj else None,
        }

    return {
        "lg_id": lg.id,
        "lg_ref": lg.lg_ref_number,
        "overall_status": lg.status,
        "steps": steps,
        "documents": docs,
        "recipient_field_config": recipient_field_config,
        "handover_signed_copy_required": handover_signed_copy_required,
        "expected_values": expected_values,
    }


# ── Lifecycle History (for Issuance LGs) ──────────────────────────────────────
@router.get("/lg-records/{lg_id}/lifecycle-history")
def get_issuance_lifecycle_history(
    lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
):
    """
    Retrieves the chronological list of all audit log events associated with
    an Issued LG record (issuance module).
    """
    from app.models.models_issuance import IssuedLGRecord
    from app.models import AuditLog, User

    # Verify the LG belongs to the user's customer
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    # Query audit logs by entity_type + entity_id (issuance module pattern)
    from sqlalchemy.orm import selectinload as sl
    from sqlalchemy import desc

    query = db.query(AuditLog).filter(
        AuditLog.customer_id == current_user.customer_id,
        or_(
            # Match by entity reference (most issuance log_action calls)
            (AuditLog.entity_type == "IssuedLGRecord") & (AuditLog.entity_id == lg_id),
            # Also match by lg_record_id if populated
            AuditLog.lg_record_id == lg_id,
        ),
    )
    if action_type:
        query = query.filter(AuditLog.action_type == action_type)

    events = query.options(sl(AuditLog.user)).order_by(desc(AuditLog.timestamp)).all()

    return [
        {
            "id": e.id,
            "timestamp": e.timestamp.isoformat() if e.timestamp else None,
            "action_type": e.action_type,
            "user_email": e.user.email if e.user else None,
            "details": e.details or {},
        }
        for e in events
    ]


# ── Available Maintenance Actions ──────────────────────────────────────────────
@router.get("/issued-lgs/{lg_id}/available-actions")
def get_available_actions(
    lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Returns which maintenance actions are currently valid for this LG.
    Frontend uses this to decide which buttons to render — no guessing.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceMaintenanceAction
    from app.constants import LgTypeEnum, LgOperationalStatusEnum

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    actions = []

    # Standard maintenance actions — only available when LG is ACTIVE
    if lg.status == "ACTIVE":
        actions.append({"type": "EXTEND", "label": "Extend Expiry"})
        actions.append({"type": "INCREASE_AMOUNT", "label": "Increase Amount"})
        actions.append({"type": "AMENDMENT", "label": "Amend LG"})
        actions.append({"type": "CLOSE", "label": "Close / Return"})
        actions.append({"type": "LIQUIDATION", "label": "Record Liquidation"})
        actions.append({"type": "CHANGE_OWNERSHIP", "label": "Change Owner"})

        # ACTIVATE — only for Advance Payment LGs with Non-Operative status, one-time only
        is_advance_payment = (lg.lg_type_id == LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE)

        # Check operational_status: prefer lg.operational_status, fallback to request join
        op_status = lg.operational_status
        if not op_status and lg.request_id:
            request_obj = db.query(IssuanceRequest.operational_status).filter(
                IssuanceRequest.id == lg.request_id
            ).first()
            op_status = request_obj.operational_status if request_obj else None

        is_non_operative = False
        if op_status:
            is_non_operative = op_status.strip().lower() in (
                "non-operative", "none operative", "non_operative"
            )

        # Check no prior approved/executed ACTIVATE action exists
        already_activated = False
        if is_advance_payment and is_non_operative:
            already_activated = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == lg.id,
                IssuanceMaintenanceAction.action_type == "ACTIVATE",
                IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED", "COMPLETED"]),
            ).first() is not None

        if is_advance_payment and is_non_operative and not already_activated:
            actions.append({"type": "ACTIVATE", "label": "Activate Non-Op"})

    return {"lg_id": lg.id, "lg_status": lg.status, "available_actions": actions}


@router.patch("/lg-records/{lg_id}/resubmit-discrepancy")
def resubmit_discrepancy(
    lg_id: int,
    payload: ResubmitDiscrepancyPayload = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """
    Resubmit an LG discrepancy that was previously rejected by an admin.
    This moves the verification_status from DISCREPANCY_REJECTED back to DISCREPANCY,
    putting it back in the admin's approval queue without requiring a new document upload.
    """
    from app.models.models_issuance import IssuedLGRecord
    from app.crud.crud import log_action

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    
    if not lg:
        raise HTTPException(status_code=404, detail="LG record not found.")

    if lg.verification_status != "DISCREPANCY_REJECTED":
        raise HTTPException(
            status_code=400, 
            detail=f"Only LGs with DISCREPANCY_REJECTED status can be resubmitted. Current status: {lg.verification_status}"
        )

    # Move status back to pending discrepancy review
    lg.verification_status = "DISCREPANCY"
    
    # Append the new notes if provided
    if payload.notes:
        existing_notes = lg.bank_reply_notes or ""
        prefix = "\n\n" if existing_notes else ""
        lg.bank_reply_notes = f"{existing_notes}{prefix}[Re-submission Note]: {payload.notes}"

    # Log the action
    log_action(db, current_user.user_id, "ISSUANCE_DISCREPANCY_RESUBMITTED", "IssuedLGRecord", lg.id,
               {"notes": payload.notes}, current_user.customer_id)

    db.commit()
    db.refresh(lg)

    return {"message": "Discrepancy resubmitted successfully.", "verification_status": lg.verification_status}


@router.post("/issued-lgs/{issued_lg_id}/maintenance")
def create_maintenance_action(
    issued_lg_id: int,
    payload: MaintenanceActionCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Create a maintenance action on an issued LG.
    
    Open to: Treasury end users, Corporate admins, and the original requestor
    of the LG (verified via the linked IssuanceRequest.requestor_user_id).
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    from app.constants import UserRole

    # Verify the LG exists and belongs to the customer
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == issued_lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="Issued LG record not found.")

    # Authorization: only end users (treasury officers) or the original requestor can create maintenance actions
    # Corporate admins supervise and approve — they do not initiate maintenance
    is_treasury = current_user.role in (UserRole.END_USER, UserRole.END_USER.value)
    is_requestor = False
    if lg.request_id:
        orig_request = db.query(IssuanceRequest).filter(
            IssuanceRequest.id == lg.request_id,
        ).first()
        if orig_request and orig_request.requestor_user_id == current_user.user_id:
            is_requestor = True

    if not is_treasury and not is_requestor:
        raise HTTPException(
            status_code=403,
            detail="Only treasury users, corporate admins, or the original requestor can raise maintenance actions."
        )
    action = maintenance_service.create_action(
        db, issued_lg_id, payload.action_type, payload.action_data,
        current_user.user_id, current_user.customer_id, payload.notes,
        initiation_source="INTERNAL_USER"
    )

    # --- Email + In-App Notification ---
    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(issued_lg_id)

    if action.status == "PENDING_APPROVAL" and action.pending_approver_users:
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails, _base_url
        from app.services.notification_service import notify

        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        approver_ids = [int(uid) for uid in action.pending_approver_users]
        approver_emails = _get_user_emails(db, approver_ids)

        if approver_emails:
            subject = f"ACTION REQUIRED: LG {ref} — {payload.action_type} Request"
            body = f"""
            <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #1a56db;">🔔 LG Maintenance Request</h2>
                <p>A <strong>{payload.action_type}</strong> action on LG <strong>{ref}</strong> requires your approval.</p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/approval-inbox" style="padding: 12px 30px; background: #1a56db; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">Review Request</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, approver_emails, subject, body, {}, email_settings)

        # In-App Notification
        notify(
            db, user_ids=approver_ids, module="ISSUANCE",
            event_type=f"MAINTENANCE_{payload.action_type}_PENDING",
            title=f"LG {ref} — {payload.action_type} Pending",
            message=f"A {payload.action_type.lower().replace('_', ' ')} action on LG {ref} requires your approval.",
            link="/corporate-admin/approval-inbox",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

    return _serialize_action(action, lg)


@router.get("/issued-lgs/{issued_lg_id}/maintenance")
def list_maintenance_actions(
    issued_lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """List all maintenance actions for an issued LG."""
    actions = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.issued_lg_id == issued_lg_id
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == issued_lg_id).first()
    return [_serialize_action(a, lg, db) for a in actions]


@router.get("/issued-lgs/{issued_lg_id}/documents")
def list_issued_lg_documents(
    issued_lg_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Aggregate all documents related to an issued LG from multiple sources."""
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceRequestDocument, IssuanceMaintenanceAction

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == issued_lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="Issued LG not found")

    documents = []

    # Source 1: Request documents (contracts, special wording, formal requests)
    if lg.request_id:
        req_docs = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == lg.request_id
        ).all()
        for doc in req_docs:
            documents.append({
                "id": f"req-{doc.id}",
                "document_id": doc.id,
                "request_id": lg.request_id,
                "file_name": doc.file_name,
                "document_type": doc.document_type,
                "source": "Request",
                "download_type": "request_doc",
                "created_at": doc.created_at.isoformat() if doc.created_at else None,
            })

    # Source 2: LG soft copy (scanned original)
    if lg.soft_copy_path:
        documents.append({
            "id": f"lg-soft-{lg.id}",
            "lg_id": lg.id,
            "file_name": lg.soft_copy_path.split("/")[-1] if "/" in (lg.soft_copy_path or "") else "LG Soft Copy",
            "document_type": "LG_SOFT_COPY",
            "source": "LG Record",
            "download_type": "lg_reprint",
            "created_at": lg.created_at.isoformat() if lg.created_at else None,
        })

    # Source 3: Issuance instruction letter (virtual — generated via reprint endpoint)
    # Always present as a downloadable action for the issued LG
    issuance_method = getattr(lg, 'issuance_method', 'COMPANY_LETTER') or 'COMPANY_LETTER'
    documents.append({
        "id": f"lg-instruction-{lg.id}",
        "lg_id": lg.id,
        "file_name": f"Issuance Instruction — {issuance_method.replace('_', ' ').title()}",
        "document_type": "ISSUANCE_INSTRUCTION",
        "source": f"Issuance ({issuance_method.replace('_', ' ').title()})",
        "download_type": "lg_reprint",
        "created_at": lg.created_at.isoformat() if lg.created_at else None,
    })

    # Source 4: Maintenance action letters (generated PDFs)
    maint_actions = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.issued_lg_id == issued_lg_id,
        IssuanceMaintenanceAction.is_deleted == False,
    ).all()
    for ma in maint_actions:
        # 3a: Generated instruction letter
        if ma.letter_generated_path:
            path = ma.letter_generated_path
            documents.append({
                "id": f"maint-{ma.id}",
                "action_id": ma.id,
                "file_name": path.split("/")[-1] if "/" in (path or "") else f"{ma.action_type} Letter",
                "document_type": f"MAINTENANCE_{ma.action_type}",
                "source": f"Maintenance — {ma.action_type.replace('_', ' ').title()}",
                "download_type": "maintenance_letter",
                "serial": ma.letter_serial_number,
                "created_at": ma.created_at.isoformat() if ma.created_at else None,
            })

        # 3b: Delivery proof document
        if ma.delivery_document_path:
            documents.append({
                "id": f"maint-delivery-{ma.id}",
                "action_id": ma.id,
                "file_name": f"Delivery Proof — {ma.action_type.replace('_', ' ').title()}",
                "document_type": "DELIVERY_PROOF",
                "source": f"Maintenance — {ma.action_type.replace('_', ' ').title()}",
                "download_type": "maintenance_doc",
                "doc_type": "delivery",
                "created_at": str(ma.delivery_date) if ma.delivery_date else (ma.created_at.isoformat() if ma.created_at else None),
            })

        # 3c: Bank reply document
        if ma.bank_reply_document_path:
            documents.append({
                "id": f"maint-bankreply-{ma.id}",
                "action_id": ma.id,
                "file_name": f"Bank Reply — {ma.action_type.replace('_', ' ').title()}",
                "document_type": "BANK_REPLY",
                "source": f"Maintenance — {ma.action_type.replace('_', ' ').title()}",
                "download_type": "maintenance_doc",
                "doc_type": "bank_reply",
                "created_at": str(ma.bank_reply_date) if ma.bank_reply_date else (ma.created_at.isoformat() if ma.created_at else None),
            })

    return documents


# ==============================================================================
# ISSUANCE ACTION CENTER (End-User operational tasks)
# ==============================================================================

@router.post("/maintenance/{action_id}/approve")
def approve_maintenance_action(
    action_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    """Approve a maintenance action step."""
    action = maintenance_service.approve_action(db, action_id, current_user.user_id, current_user.customer_id)

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # Notify initiator
    from app.services.notification_service import notify
    if action.initiated_by_user_id:
        status_label = "Fully Approved ✅" if action.status in ("APPROVED", "EXECUTED") else "Step Approved"
        notify(
            db, user_ids=[action.initiated_by_user_id], module="ISSUANCE",
            event_type=f"MAINTENANCE_{action.action_type}_APPROVED",
            title=f"LG {ref} — {action.action_type} {status_label}",
            message=f"Your {action.action_type.lower().replace('_', ' ')} request has been {status_label.lower()}.",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

        # Email notification
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails, _base_url, get_common_communication_emails
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        initiator_emails = _get_user_emails(db, [action.initiated_by_user_id])
        cc_emails = get_common_communication_emails(db, current_user.customer_id)
        if initiator_emails:
            subject = f"LG {ref} — {action.action_type.replace('_', ' ')} {status_label}"
            body = f"""
            <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #16a34a;">✅ Maintenance Action Approved</h2>
                <p>Your <strong>{action.action_type.replace('_', ' ')}</strong> request on LG <strong>{ref}</strong> has been <strong>{status_label.lower()}</strong>.</p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: #16a34a; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View LG</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, initiator_emails, subject, body, {}, email_settings, cc_emails=cc_emails)

    # If action was executed and instruction letter issued, send print notification
    if action.status == "EXECUTED" and action.instruction_status == "Instruction Issued":
        from app.services.issuance_notifications import notify_maintenance_action_executed
        data = action.action_data or {}
        # Build action-specific detail rows for the email
        detail_rows = ""
        if action.action_type == "EXTEND":
            detail_rows = f'<tr><td style="padding: 4px 0; color: #666;">New Expiry:</td><td style="padding: 4px 0;">{data.get("new_expiry_date", "N/A")}</td></tr>'
        elif action.action_type == "INCREASE_AMOUNT":
            detail_rows = f'<tr><td style="padding: 4px 0; color: #666;">New Amount:</td><td style="padding: 4px 0;">{data.get("new_amount", "N/A")}</td></tr>'
        elif action.action_type == "AMENDMENT":
            parts = []
            if data.get("new_beneficiary_name"): parts.append(f"Beneficiary: {data['new_beneficiary_name']}")
            if data.get("new_lg_purpose"): parts.append(f"Purpose: {data['new_lg_purpose']}")
            detail_rows = f'<tr><td style="padding: 4px 0; color: #666;">Changes:</td><td style="padding: 4px 0;">{", ".join(parts) or "See letter"}</td></tr>'

        currency = lg.currency.iso_code if lg and hasattr(lg, 'currency') and lg.currency else ""
        amount_str = f"{currency} {float(lg.current_amount):,.2f}" if lg and lg.current_amount else "N/A"

        background_tasks.add_task(
            notify_maintenance_action_executed,
            db, action.action_type, ref,
            lg.bank.name if lg and lg.bank else "N/A",
            lg.beneficiary_name or "N/A",
            amount_str,
            action.letter_serial_number or "",
            detail_rows,
            action.initiated_by_user_id,
            current_user.customer_id,
        )

    return _serialize_action(action, lg)


@router.post("/maintenance/{action_id}/reject")
def reject_maintenance_action(
    action_id: int,
    payload: RejectPayload,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    """Reject a maintenance action."""
    action = maintenance_service.reject_action(db, action_id, current_user.user_id, current_user.customer_id, payload.reason)

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # Notify initiator
    from app.services.notification_service import notify
    if action.initiated_by_user_id:
        notify(
            db, user_ids=[action.initiated_by_user_id], module="ISSUANCE",
            event_type=f"MAINTENANCE_{action.action_type}_REJECTED",
            title=f"LG {ref} — {action.action_type} Rejected ❌",
            message=f"Your {action.action_type.lower().replace('_', ' ')} request was rejected. Reason: {payload.reason or 'Not specified'}",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

        # Email notification
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails, _base_url, get_common_communication_emails
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        initiator_emails = _get_user_emails(db, [action.initiated_by_user_id])
        cc_emails = get_common_communication_emails(db, current_user.customer_id)
        if initiator_emails:
            reason_text = payload.reason or "Not specified"
            subject = f"LG {ref} — {action.action_type.replace('_', ' ')} Rejected ❌"
            body = f"""
            <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #dc2626;">❌ Maintenance Action Rejected</h2>
                <p>Your <strong>{action.action_type.replace('_', ' ')}</strong> request on LG <strong>{ref}</strong> has been rejected.</p>
                <div style="background: #fef2f2; border-left: 4px solid #dc2626; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p style="margin: 0;"><strong>Reason:</strong> {reason_text}</p>
                </div>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: #dc2626; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View LG</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, initiator_emails, subject, body, {}, email_settings, cc_emails=cc_emails)

    return _serialize_action(action, lg)


@router.post("/maintenance/{action_id}/delivery")
def record_maintenance_delivery(
    action_id: int,
    delivery_method: Opt[str] = Form("UNSPECIFIED"),
    delivery_notes: Opt[str] = Form(None),
    delivery_date: Opt[str] = Form(None),
    delivery_document: Opt[UploadFile] = File(None),
    delivery_document_file: Opt[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Record letter delivery to bank — with date and optional proof document."""
    # Accept file from either field name (delivery_document or delivery_document_file)
    doc_file = delivery_document or delivery_document_file
    doc_bytes = None
    doc_mime = None
    if doc_file and doc_file.filename:
        doc_bytes = doc_file.file.read()
        doc_mime = doc_file.content_type

    action = maintenance_service.record_delivery(
        db, action_id, current_user.user_id,
        delivery_method or "UNSPECIFIED", delivery_notes,
        delivery_date_str=delivery_date,
        delivery_document_bytes=doc_bytes,
        delivery_document_mime_type=doc_mime,
        customer_id=current_user.customer_id,
    )
    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    return _serialize_action(action, lg)


@router.post("/maintenance/{action_id}/bank-reply")
def record_maintenance_bank_reply(
    action_id: int,
    background_tasks: BackgroundTasks,
    bank_reply_notes: Opt[str] = Form(None),
    reply_details: Opt[str] = Form(None),
    bank_reply_date: Opt[str] = Form(None),
    bank_reply_file: Opt[UploadFile] = File(None),
    bank_reply_document_file: Opt[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Record bank reply and apply confirmed changes to LG.
    F3: Optionally accepts a bank reply document for AI verification."""
    # Accept notes from either field name
    notes = bank_reply_notes or reply_details
    # Accept file from either field name
    reply_file = bank_reply_file or bank_reply_document_file
    file_bytes = None
    mime_type = None
    if reply_file and reply_file.filename:
        file_bytes = reply_file.file.read()
        mime_type = reply_file.content_type

    action = maintenance_service.record_bank_reply(
        db, action_id, current_user.user_id, current_user.customer_id,
        notes, file_bytes, mime_type,
        bank_reply_date_str=bank_reply_date,
    )

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # Notify initiator (only when changes were applied, not when awaiting confirmation)
    from app.services.notification_service import notify
    if action.initiated_by_user_id and action.instruction_status == "Confirmed by Bank":
        notify(
            db, user_ids=[action.initiated_by_user_id], module="ISSUANCE",
            event_type=f"MAINTENANCE_{action.action_type}_BANK_CONFIRMED",
            title=f"LG {ref} — Bank Confirmed {action.action_type}",
            message=f"The bank has confirmed the {action.action_type.lower().replace('_', ' ')} on LG {ref}.",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

        # Email notification — bank confirmed
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails, _base_url, get_common_communication_emails
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        to_emails = _get_user_emails(db, [action.initiated_by_user_id])
        cc_emails = get_common_communication_emails(db, current_user.customer_id)

        is_liquidation = action.action_type in ("LIQUIDATION",)
        color = "#dc2626" if is_liquidation else "#16a34a"
        icon = "🚨" if is_liquidation else "🏦"
        alert = "HIGH ALERT: " if is_liquidation else ""

        if to_emails:
            subject = f"{alert}LG {ref} — Bank Confirmed {action.action_type.replace('_', ' ')}"
            body = f"""
            <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);{' border: 2px solid #dc2626;' if is_liquidation else ''}">
                <h2 style="color: {color};">{icon} Bank Reply Confirmed — {action.action_type.replace('_', ' ')}</h2>
                <p>The bank has confirmed the <strong>{action.action_type.replace('_', ' ')}</strong> action on LG <strong>{ref}</strong>. Changes have been applied.</p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: {color}; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View LG</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)

    return _serialize_action(action, lg)


@router.post("/maintenance/{action_id}/cancel")
def cancel_maintenance_action(
    action_id: int,
    payload: RejectPayload,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Cancel a recently executed maintenance action within the cancellation window.
    Only the most recent action on an LG can be cancelled, and only while instruction_status = 'Instruction Issued'."""
    action = maintenance_service.cancel_action(
        db, action_id, current_user.user_id, current_user.customer_id,
        reason=payload.reason or ""
    )

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # In-app notification to initiator
    from app.services.notification_service import notify
    if action.initiated_by_user_id:
        notify(
            db, user_ids=[action.initiated_by_user_id], module="ISSUANCE",
            event_type=f"MAINTENANCE_{action.action_type}_CANCELLED",
            title=f"LG {ref} — {action.action_type} Cancelled",
            message=f"The {action.action_type.lower().replace('_', ' ')} action on LG {ref} has been cancelled. Reason: {payload.reason or 'Not specified'}",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

        # Email notification
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.issuance_notifications import _get_user_emails, _base_url, get_common_communication_emails
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        to_emails = _get_user_emails(db, [action.initiated_by_user_id])
        cc_emails = get_common_communication_emails(db, current_user.customer_id)
        if to_emails:
            subject = f"LG {ref} — {action.action_type.replace('_', ' ')} Cancelled"
            body = f"""
            <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                <h2 style="color: #b45309;">⚠️ Maintenance Action Cancelled</h2>
                <p>The <strong>{action.action_type.replace('_', ' ')}</strong> action on LG <strong>{ref}</strong> has been cancelled.</p>
                <div style="background: #fffbeb; border-left: 4px solid #b45309; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p style="margin: 0;"><strong>Reason:</strong> {payload.reason or 'Not specified'}</p>
                </div>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: #b45309; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View LG</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)

    return _serialize_action(action, lg)


@router.get("/maintenance/pending")
def get_pending_maintenance_actions(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    """Get all maintenance actions pending the current user's approval."""
    from sqlalchemy import cast, String
    actions = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.status == "PENDING_APPROVAL"
    ).all()

    # Filter to those where current user is in pending_approver_users
    pending = []
    for a in actions:
        approvers = [int(uid) for uid in (a.pending_approver_users or [])]
        if current_user.user_id in approvers:
            lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == a.issued_lg_id).first()
            pending.append(_serialize_action(a, lg, db))

    return pending


@router.post("/maintenance/upload-document")
async def upload_maintenance_document(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Upload a supporting document for a maintenance action. Returns a URI to include in action_data."""
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    from app.crud.crud_lg_document import _slugify_doc_type
    from app.crud import crud_customer_configuration
    import uuid
    from datetime import datetime as dt

    file_content = await file.read()
    if len(file_content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File size exceeds 10MB limit")

    file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
    unique_name = f"MAINT_DOC_{dt.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_extension}"
    blob_path = f"customer_{current_user.customer_id}/maintenance_docs/{unique_name}"

    bucket_name = GCS_BUCKET_NAME
    bucket_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, current_user.customer_id, "STORAGE_BUCKET_NAME"
    )
    if bucket_config and bucket_config.get('effective_value'):
        bucket_name = bucket_config['effective_value']

    stored_uri = await _upload_to_gcs(bucket_name, blob_path, file_content, file.content_type)
    if not stored_uri:
        raise HTTPException(status_code=500, detail="Failed to upload document")

    return {
        "uri": stored_uri,
        "file_name": file.filename,
        "size_bytes": len(file_content),
    }


@router.get("/maintenance/approval-history")
def get_maintenance_approval_history(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    """Get maintenance actions relevant to this approver:
    - PENDING_APPROVAL: only if user is in pending_approver_users
    - All other statuses: only if user previously acted on it (in approval_history)
    """
    # Get all issued LGs for this customer first
    customer_lg_ids = [
        lg_id for (lg_id,) in db.query(IssuedLGRecord.id).filter(
            IssuedLGRecord.customer_id == current_user.customer_id
        ).all()
    ]
    if not customer_lg_ids:
        return []

    actions = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.issued_lg_id.in_(customer_lg_ids)
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    result = []
    uid = current_user.user_id
    for a in actions:
        if a.status == "PENDING_APPROVAL":
            # Only show if this user is a designated approver for the current step
            approvers = [int(x) for x in (a.pending_approver_users or [])]
            if uid not in approvers:
                continue
        else:
            # For completed/rejected/executed: only show if user participated
            history = a.approval_history or []
            participated = any(
                entry.get("user_id") == uid
                for entry in history
                if isinstance(entry, dict)
            )
            if not participated:
                continue

        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == a.issued_lg_id).first()
        result.append(_serialize_action(a, lg, db))

    return result


@router.get("/maintenance/pending-print")
def get_maintenance_pending_print(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """
    Returns maintenance actions with instruction letters pending print.
    These are actions with status=EXECUTED and instruction_status='Instruction Issued'
    (meaning the letter has been generated but not yet printed/delivered).
    """
    from sqlalchemy.orm import selectinload

    actions = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.status == "EXECUTED",
        IssuanceMaintenanceAction.instruction_status == "Instruction Issued",
        IssuanceMaintenanceAction.is_deleted == False,
    ).options(
        selectinload(IssuanceMaintenanceAction.issued_lg),
    ).order_by(IssuanceMaintenanceAction.created_at.asc()).all()

    results = []
    for a in actions:
        lg = a.issued_lg
        results.append({
            "id": a.id,
            "action_type": a.action_type,
            "letter_serial_number": a.letter_serial_number,
            "instruction_status": a.instruction_status,
            "created_at": str(a.created_at) if a.created_at else None,
            "executed_at": str(a.updated_at) if a.updated_at else None,
            "issued_lg_id": a.issued_lg_id,
            "lg_ref_number": lg.lg_ref_number if lg else None,
            "lg_beneficiary": lg.beneficiary_name if lg else None,
            "initiated_by_user_id": a.initiated_by_user_id,
            # Normalized fields for action center display
            "source": "issuance_maintenance",
        })

    return results


@router.post("/maintenance/{action_id}/mark-printed")
def mark_maintenance_printed(
    action_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Mark a maintenance instruction as printed (advances instruction_status)."""
    action = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuanceMaintenanceAction.id == action_id,
        IssuedLGRecord.customer_id == current_user.customer_id,
    ).first()

    if not action:
        raise HTTPException(status_code=404, detail="Maintenance action not found")

    if action.instruction_status != "Instruction Issued":
        raise HTTPException(status_code=400, detail="Action is not in 'Instruction Issued' status")

    # Advance to next status (printed but not yet delivered)
    action.instruction_status = "Printed"
    db.commit()
    db.refresh(action)

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    return _serialize_action(action, lg)


@router.post("/requests/{request_id}/analyze-document")
async def analyze_supporting_document_endpoint(
    request_id: int,
    doc_type: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    H2: Upload a supporting document (Contract, PO, Formal Request) for AI analysis.
    Cross-references extracted fields against the issuance request.
    ADVISORY only — highlights potential gaps, never blocks the user.
    """
    from app.core.ai_integration import analyze_supporting_document, AI_DOC_MAX_SIZE_BYTES

    # Validate request
    request_obj = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id,
    ).first()
    if not request_obj:
        raise HTTPException(404, "Issuance request not found")

    pdf_bytes = await file.read()

    # File size guard (friendly message, not an error)
    if len(pdf_bytes) > AI_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document is too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Maximum is {AI_DOC_MAX_SIZE_BYTES // (1024*1024)} MB.",
            "comparison": None,
        }

    # Build request_data dict with the 9 fields for AI-driven comparison
    request_data = {
        "contract_value": float(request_obj.reference_amount) if request_obj.reference_amount else None,
        "currency": getattr(request_obj.currency, 'iso_code', None) if request_obj.currency else None,
        "beneficiary_name": request_obj.beneficiary_name,
        "beneficiary_address": request_obj.beneficiary_address,
        "lg_type": getattr(request_obj.lg_type, 'name', None) if hasattr(request_obj, 'lg_type') and request_obj.lg_type else None,
        "lg_value": float(request_obj.amount) if request_obj.amount else None,
        "lg_currency": getattr(request_obj.currency, 'iso_code', None) if request_obj.currency else None,
        "lg_expiry_date": str(request_obj.requested_expiry_date) if request_obj.requested_expiry_date else None,
        "lg_purpose": request_obj.lg_purpose,
    }

    # Run AI-driven verification (comparison is done by AI, not rule-based)
    result = await analyze_supporting_document(
        pdf_bytes, doc_type.upper(), file.filename,
        request_data=request_data,
        db=db, customer_id=current_user.customer_id, user_id=current_user.user_id,
    )

    # Persist the verification result on the matching document
    if result.get("status") == "OK":
        from app.models.models_issuance import IssuanceRequestDocument
        doc_record = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == request_id,
            IssuanceRequestDocument.document_type == doc_type.upper(),
            IssuanceRequestDocument.is_deleted == False
        ).order_by(IssuanceRequestDocument.created_at.desc()).first()
        if doc_record and hasattr(doc_record, 'ai_verification_result'):
            doc_record.ai_verification_result = result
            db.commit()

    return result


# ==============================================================================
# MAINTENANCE ACTIONS LISTING (End-User Dashboard)
# ==============================================================================

@router.get("/my-maintenance-actions")
def get_my_maintenance_actions(
    db: Session = Depends(get_db),
    context: TokenData = Depends(get_issuance_read_context),
):
    """
    Returns all issuance maintenance actions for the current customer,
    for display in the End-User Dashboard 'My Actions' tab.
    """
    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord
    from sqlalchemy.orm import joinedload

    actions = db.query(IssuanceMaintenanceAction).options(
        joinedload(IssuanceMaintenanceAction.issued_lg),
    ).join(
        IssuedLGRecord, IssuanceMaintenanceAction.issued_lg_id == IssuedLGRecord.id
    ).filter(
        IssuedLGRecord.customer_id == context.customer_id,
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    return {
        "total": len(actions),
        "actions": [
            {
                "id": a.id,
                "action_type": a.action_type,
                "status": a.status,
                "instruction_status": a.instruction_status,
                "action_data": a.action_data,
                "notes": a.notes,
                "lg_ref": a.issued_lg.lg_ref_number if a.issued_lg else None,
                "lg_beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
                "letter_serial_number": a.letter_serial_number,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in actions
        ]
    }