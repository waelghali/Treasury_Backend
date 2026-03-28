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


# ==============================================================================
# 1. DICTIONARIES (Banks & Currencies)
# ==============================================================================

@router.get("/banks", response_model=List[BankOut])
def get_issuance_banks(db: Session = Depends(get_db)):
    """Fetch all banks for dropdowns."""
    return db.query(Bank).all()

@router.get("/currencies", response_model=List[CurrencyOut])
def get_issuance_currencies(db: Session = Depends(get_db)):
    """Fetch all currencies for dropdowns."""
    return db.query(Currency).all()

# ==============================================================================
# BANK ISSUANCE METHODS (LIBRARY)
# ==============================================================================

@router.get("/bank-methods", response_model=List[BankIssuanceOptionOut])
def list_bank_methods(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """List all issuance methods for a specific bank."""
    return crud_bank_methods.get_by_bank(db, bank_id=bank_id, only_active=False)

@router.post("/bank-methods", response_model=BankIssuanceOptionOut)
def create_bank_method(
    bank_id: int,
    method_in: BankIssuanceOptionCreateUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Create a new issuance method for a bank."""
    return crud_bank_methods.create_method(db, bank_id=bank_id, obj_in=method_in.model_dump(), user_id=current_user.user_id)

@router.put("/bank-methods/{method_id}", response_model=BankIssuanceOptionOut)
def update_bank_method(
    method_id: int,
    method_in: BankIssuanceOptionCreateUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Update an existing bank issuance method."""
    return crud_bank_methods.update_method(db, method_id=method_id, obj_in=method_in.model_dump(), user_id=current_user.user_id)

@router.delete("/bank-methods/{method_id}", response_model=BankIssuanceOptionOut)
def deactivate_bank_method(
    method_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Deactivate a bank issuance method."""
    return crud_bank_methods.update_method(db, method_id=method_id, obj_in={"is_active": False}, user_id=current_user.user_id)

# ==============================================================================
# LG TYPES (read-only for all authenticated users)
# ==============================================================================

@router.get("/lg-types")
def list_lg_types(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Return all active LG types for dropdowns."""
    from app.models import LgType
    types = db.query(LgType).filter(LgType.is_deleted == False).order_by(LgType.name).all()
    return [{"id": t.id, "name": t.name} for t in types]

# ==============================================================================
# BANK FORM TEMPLATES (Upload, AI Analysis, Fill)
# ==============================================================================

from fastapi import File, UploadFile
from app.models.models_issuance import BankFormTemplate


@router.post("/bank-forms/{form_id}/fill/{request_id}")
async def fill_bank_form(
    form_id: int,
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Fill a bank's PDF form with data from an issuance request.
    Uses the cached field mapping (no AI call). Returns the filled PDF.
    """
    from sqlalchemy.orm import selectinload
    
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    
    if not form_template.field_mapping:
        raise HTTPException(400, "Form has no field mapping. Run AI analysis first.")
    
    # Read blank form (GCS or local)
    template_pdf_bytes = _read_bank_form_pdf_bytes(form_template)
    
    # Load the request with relationships
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
    
    # Build data dict
    from app.core.pdf_form_filler import fill_pdf_form, build_request_data_dict
    request_data = build_request_data_dict(request, db, bank_id=form_template.bank_id)
    
    # Fill the form
    filled_pdf = fill_pdf_form(
        template_pdf_bytes=template_pdf_bytes,
        field_mapping=form_template.field_mapping,
        request_data=request_data,
    )
    
    filename = f"Filled_{form_template.name}_{request.serial_number}.pdf"
    
    return StreamingResponse(
        io.BytesIO(filled_pdf),
        media_type="application/pdf",
        headers={'Content-Disposition': f'inline; filename="{filename}"'}
    )

# ==============================================================================
# CUSTOMER BANK ACCOUNTS
# ==============================================================================

from app.models.models_issuance import CustomerBankAccount

@router.get("/bank-accounts")
def list_bank_accounts(
    bank_id: int = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """List all bank accounts for this customer, optionally filtered by bank."""
    query = db.query(CustomerBankAccount).filter(
        CustomerBankAccount.customer_id == current_user.customer_id,
        CustomerBankAccount.is_deleted == False,
    )
    if bank_id:
        query = query.filter(CustomerBankAccount.bank_id == bank_id)
    
    accounts = query.order_by(CustomerBankAccount.bank_id, CustomerBankAccount.is_default.desc()).all()
    
    return [
        {
            "id": a.id,
            "bank_id": a.bank_id,
            "bank_name": a.bank.name if a.bank else "Unknown",
            "entity_id": a.entity_id,
            "entity_name": a.entity.entity_name if a.entity else None,
            "account_name": a.account_name,
            "account_number": a.account_number,
            "customer_number": a.customer_number,
            "branch_name": a.branch_name,
            "iban": a.iban,
            "is_default": a.is_default,
            "is_active": a.is_active,
        }
        for a in accounts
    ]


@router.post("/bank-accounts")
def create_bank_account(
    body: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Create a new bank account for the customer."""
    # If setting as default, unset any existing default for same customer+bank
    if body.get("is_default"):
        existing_defaults = db.query(CustomerBankAccount).filter(
            CustomerBankAccount.customer_id == current_user.customer_id,
            CustomerBankAccount.bank_id == body["bank_id"],
            CustomerBankAccount.is_default == True,
            CustomerBankAccount.is_deleted == False,
        ).all()
        for d in existing_defaults:
            d.is_default = False
    
    account = CustomerBankAccount(
        customer_id=current_user.customer_id,
        bank_id=body["bank_id"],
        entity_id=body.get("entity_id"),
        account_name=body["account_name"],
        account_number=body["account_number"],
        customer_number=body.get("customer_number"),
        branch_name=body.get("branch_name"),
        iban=body.get("iban"),
        is_default=body.get("is_default", False),
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    
    return {
        "id": account.id,
        "message": "Bank account created successfully",
    }


@router.put("/bank-accounts/{account_id}")
def update_bank_account(
    account_id: int,
    body: dict,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Update a bank account."""
    account = db.query(CustomerBankAccount).filter(
        CustomerBankAccount.id == account_id,
        CustomerBankAccount.customer_id == current_user.customer_id,
        CustomerBankAccount.is_deleted == False,
    ).first()
    
    if not account:
        raise HTTPException(404, "Bank account not found.")
    
    # If setting as default, unset existing
    if body.get("is_default") and not account.is_default:
        existing_defaults = db.query(CustomerBankAccount).filter(
            CustomerBankAccount.customer_id == current_user.customer_id,
            CustomerBankAccount.bank_id == account.bank_id,
            CustomerBankAccount.is_default == True,
            CustomerBankAccount.is_deleted == False,
            CustomerBankAccount.id != account_id,
        ).all()
        for d in existing_defaults:
            d.is_default = False
    
    for field in ["account_name", "account_number", "customer_number", "branch_name", "iban", "is_default", "entity_id", "is_active"]:
        if field in body:
            setattr(account, field, body[field])
    
    db.commit()
    return {"message": "Bank account updated"}


@router.delete("/bank-accounts/{account_id}")
def delete_bank_account(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Soft-delete a bank account."""
    account = db.query(CustomerBankAccount).filter(
        CustomerBankAccount.id == account_id,
        CustomerBankAccount.customer_id == current_user.customer_id,
        CustomerBankAccount.is_deleted == False,
    ).first()
    
    if not account:
        raise HTTPException(404, "Bank account not found.")
    
    account.is_deleted = True
    account.is_active = False
    db.commit()
    return {"message": "Bank account deleted"}


# ==============================================================================
# FORM DICTIONARY (accessible to all roles for the issuance form)
# ==============================================================================

@router.get("/check-duplicate-reference")
def check_duplicate_reference(
    reference_type: str = Query(...),
    reference_number: str = Query(...),
    exclude_request_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Check if a request with the same reference type + number exists for this customer."""
    from app.models.models_issuance import IssuanceRequest, IssuedLGRecord
    from sqlalchemy import func
    
    query = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        func.lower(IssuanceRequest.reference_type) == reference_type.lower(),
        func.lower(IssuanceRequest.reference_number) == reference_number.strip().lower(),
        IssuanceRequest.is_deleted == False
    )
    if exclude_request_id:
        query = query.filter(IssuanceRequest.id != exclude_request_id)
    
    request_matches = query.order_by(IssuanceRequest.created_at.desc()).limit(3).all()
    
    # Also check against issued LGs (via their linked request's reference)
    lg_query = db.query(IssuedLGRecord).join(
        IssuanceRequest, IssuedLGRecord.request_id == IssuanceRequest.id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        func.lower(IssuanceRequest.reference_type) == reference_type.lower(),
        func.lower(IssuanceRequest.reference_number) == reference_number.strip().lower(),
    ).limit(3).all()

    all_matches = []
    for m in request_matches:
        all_matches.append({
            "id": m.id,
            "serial_number": m.serial_number,
            "status": m.status,
            "amount": str(m.amount) if m.amount else None,
            "beneficiary_name": m.beneficiary_name,
            "created_at": str(m.created_at) if m.created_at else None,
            "type": "request"
        })
    for lg in lg_query:
        all_matches.append({
            "id": lg.id,
            "serial_number": lg.lg_ref_number,
            "status": f"ISSUED ({lg.status})",
            "amount": str(lg.current_amount) if lg.current_amount else None,
            "beneficiary_name": lg.beneficiary_name,
            "created_at": str(lg.created_at) if lg.created_at else None,
            "type": "issued_lg"
        })
    
    # Build recall data from the most recent request match
    recall_data = None
    if request_matches:
        latest = request_matches[0]  # already ordered by created_at desc
        recall_data = {
            "reference_amount": str(latest.reference_amount) if latest.reference_amount else None,
            "reference_currency_id": latest.reference_currency_id,
            "reference_start_date": str(latest.reference_start_date) if latest.reference_start_date else None,
            "reference_end_date": str(latest.reference_end_date) if latest.reference_end_date else None,
            "project_id": latest.project_id,
        }

    if not all_matches:
        return {"found": False, "matches": [], "recall_data": None}
    
    return {"found": True, "matches": all_matches, "recall_data": recall_data}


@router.post("/issued-lgs/{lg_id}/reprint")
async def reprint_issued_lg(
    lg_id: int,
    additional_text: str = Query("", description="Extra instructions for the letter"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Smart reprint: serves the right document based on issuance_method.
    Priority: uploaded doc → regenerate (company letter or bank form).
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceRequestDocument
    from fastapi.responses import StreamingResponse, RedirectResponse
    from starlette.responses import Response
    import io, os

    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == current_user.customer_id
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="Issued LG not found.")

    # ── Log the reprint action ──
    from datetime import datetime as dt
    reprint_log = lg.custody_transfer_log or []
    reprint_log.append({
        "action": "REPRINT",
        "user_id": current_user.user_id,
        "timestamp": dt.now().isoformat(),
        "method": lg.issuance_method,
    })
    lg.custody_transfer_log = reprint_log
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(lg, "custody_transfer_log")
    db.commit()

    # ── PRIORITY 1: Serve uploaded soft copy ──
    if lg.soft_copy_path:
        if lg.soft_copy_path.startswith("gs://"):
            try:
                from app.core.ai_integration import storage_client
                import datetime as _dt
                parts = lg.soft_copy_path.replace("gs://", "").split("/", 1)
                bucket = storage_client.bucket(parts[0])
                blob = bucket.blob(parts[1] if len(parts) > 1 else "")
                signed_url = blob.generate_signed_url(version="v4", expiration=_dt.timedelta(minutes=15), method="GET")
                return RedirectResponse(url=signed_url)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Failed to serve soft_copy_path: {e}")
        elif os.path.exists(lg.soft_copy_path):
            with open(lg.soft_copy_path, "rb") as f:
                content = f.read()
            ext = lg.soft_copy_path.rsplit(".", 1)[-1].lower()
            media_types = {"pdf": "application/pdf", "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}
            return StreamingResponse(
                io.BytesIO(content),
                media_type=media_types.get(ext, "application/octet-stream"),
                headers={"Content-Disposition": f'inline; filename="lg_{lg.lg_ref_number}.{ext}"'}
            )

    # ── PRIORITY 2: Serve BANK_LG_COPY document ──
    if lg.request_id:
        doc = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == lg.request_id,
            IssuanceRequestDocument.document_type == "BANK_LG_COPY"
        ).order_by(IssuanceRequestDocument.created_at.desc()).first()
        if doc and doc.file_path:
            file_path = doc.file_path
            if file_path.startswith("gs://"):
                try:
                    from app.core.ai_integration import storage_client
                    import datetime as _dt
                    parts = file_path.replace("gs://", "").split("/", 1)
                    bucket = storage_client.bucket(parts[0])
                    blob = bucket.blob(parts[1] if len(parts) > 1 else "")
                    signed_url = blob.generate_signed_url(version="v4", expiration=_dt.timedelta(minutes=15), method="GET")
                    return RedirectResponse(url=signed_url)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"Failed to serve BANK_LG_COPY: {e}")
            elif os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    content = f.read()
                ext = file_path.rsplit(".", 1)[-1].lower()
                media_types = {"pdf": "application/pdf", "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}
                return StreamingResponse(
                    io.BytesIO(content),
                    media_type=media_types.get(ext, "application/octet-stream"),
                    headers={"Content-Disposition": f'inline; filename="lg_{lg.lg_ref_number}.{ext}"'}
                )

    # ── FALLBACK: Regenerate based on issuance_method ──
    method = (lg.issuance_method or "").upper()

    if method == "COMPANY_LETTER":
        if not lg.request_id:
            raise HTTPException(status_code=400, detail="No linked request — cannot regenerate company letter.")
        result = await issuance_service.generate_issuance_letter(
            db=db, request_id=lg.request_id,
            customer_id=current_user.customer_id,
            additional_text=additional_text,
        )
        return Response(
            content=result["pdf_bytes"],
            media_type="application/pdf",
            headers={'Content-Disposition': f'inline; filename="{result["filename"]}"'}
        )

    elif method in ("FILLABLE_PDF", "PHYSICAL_OVERLAY", "SCANNED_FILL", "BANK_FORM"):
        # Re-fill the bank form from the template
        if not lg.request_id or not lg.bank_id:
            raise HTTPException(status_code=400, detail="No linked request/bank — cannot regenerate bank form.")
        from app.models.models_issuance import BankFormTemplate
        form_template = db.query(BankFormTemplate).filter(
            BankFormTemplate.bank_id == lg.bank_id,
            BankFormTemplate.is_active == True,
            BankFormTemplate.is_deleted == False,
        ).order_by(BankFormTemplate.priority.desc()).first()
        if not form_template:
            raise HTTPException(status_code=404, detail="No active bank form template found for this bank. Cannot regenerate.")
        # Build request data and fill
        request_obj = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
        if not request_obj:
            raise HTTPException(status_code=404, detail="Linked request not found.")
        from app.core.pdf_form_filler import build_request_data_dict
        request_data = build_request_data_dict(request_obj, db, bank_id=lg.bank_id)
        from app.core.pdf_form_filler import fill_pdf_form, generate_overlay_pdf, generate_scanned_fill_pdf
        # Read the template PDF file into bytes (GCS or local)
        template_bytes = _read_bank_form_pdf_bytes(form_template)
        form_type = (form_template.form_type or "FILLABLE_PDF").upper()
        if form_type == "PHYSICAL_OVERLAY":
            filled_pdf = generate_overlay_pdf(template_bytes, form_template.field_mapping or {}, request_data)
        elif form_type == "SCANNED_FILL":
            filled_pdf = generate_scanned_fill_pdf(template_bytes, form_template.field_mapping or {}, request_data)
        else:
            filled_pdf = fill_pdf_form(template_bytes, form_template.field_mapping or {}, request_data)
        return StreamingResponse(
            io.BytesIO(filled_pdf),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="bank_form_{lg.lg_ref_number}.pdf"'}
        )

    raise HTTPException(
        status_code=404,
        detail=f"No document found and cannot regenerate for issuance method '{method}'."
    )


@router.get("/banks/{bank_id}/issuance-options")
def get_bank_issuance_options(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Return available issuance methods for a bank (company letter, bank form, API)."""
    from app.models.models_issuance import BankIssuanceOption, BankFormTemplate

    options = db.query(BankIssuanceOption).filter(
        BankIssuanceOption.bank_id == bank_id,
        BankIssuanceOption.is_active == True
    ).all()

    # Also check if this bank has any active form templates
    has_form_templates = db.query(BankFormTemplate).filter(
        BankFormTemplate.bank_id == bank_id,
        BankFormTemplate.is_active == True,
        BankFormTemplate.is_deleted == False
    ).first() is not None

    # Build response — always include "Company Letter" as a base option
    methods = []

    # Company Letter is always available
    methods.append({
        "id": "COMPANY_LETTER",
        "strategy_code": "COMPANY_LETTER",
        "display_name": "Company Letter",
        "description": "Generate a signed company letter to the bank requesting LG issuance",
        "available": True,
    })

    # Bank Form if templates exist
    methods.append({
        "id": "BANK_FORM",
        "strategy_code": "BANK_FORM",
        "display_name": "Fill Bank Form",
        "description": "Auto-fill the bank's official PDF application form",
        "available": has_form_templates,
    })

    # Add any custom options from BankIssuanceOption table
    for opt in options:
        if opt.strategy_code not in ["COMPANY_LETTER", "BANK_FORM"]:
            methods.append({
                "id": str(opt.id),
                "strategy_code": opt.strategy_code,
                "display_name": opt.display_name,
                "description": opt.configuration.get("description", ""),
                "available": True,
            })

    # API placeholder
    methods.append({
        "id": "BANK_API",
        "strategy_code": "BANK_API",
        "display_name": "Bank API",
        "description": "Submit directly via bank's API integration (coming soon)",
        "available": False,
    })

    return methods


@router.post("/maintenance/{action_id}/confirm-bank-reply")
def confirm_bank_reply(
    action_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Phase 2: User reviewed AI verification and chose to proceed.
    Applies the previously paused changes to the LG record."""
    action = maintenance_service.confirm_bank_reply(
        db, action_id, current_user.user_id, current_user.customer_id
    )

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # Notify initiator that bank confirmed (Phase 2)
    from app.services.notification_service import notify
    if action.initiated_by_user_id:
        notify(
            db, user_ids=[action.initiated_by_user_id], module="ISSUANCE",
            event_type=f"MAINTENANCE_{action.action_type}_BANK_CONFIRMED",
            title=f"LG {ref} — Bank Confirmed {action.action_type}",
            message=f"The bank has confirmed the {action.action_type.lower().replace('_', ' ')} on LG {ref}. Changes applied.",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

        # Email notification — bank confirmed after user review
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
                <p style="color: #b45309; font-weight: bold;">⚠️ Note: AI verification flagged discrepancies, but user approved proceeding.</p>
                <div style="text-align: center; margin: 25px 0;">
                    <a href="{_base_url()}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: {color}; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View LG</a>
                </div>
                <hr style="border: none; border-top: 1px solid #eee;" />
                <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
            </div></body></html>
            """
            background_tasks.add_task(send_email, db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)

    return _serialize_action(action, lg)


@router.post("/maintenance/{action_id}/cancel-bank-reply")
def cancel_pending_bank_reply(
    action_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """User reviewed AI verification and chose NOT to proceed.
    Reverts the action so user can re-upload a corrected document."""
    action = maintenance_service.cancel_pending_bank_reply(
        db, action_id, current_user.user_id, current_user.customer_id
    )
    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    return _serialize_action(action, lg)


@router.post("/issued-lgs/{issued_lg_id}/bank-initiated-change")
def process_bank_initiated_change(
    issued_lg_id: int,
    bank_letter: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Upload a bank letter → AI extracts what changed → returns diff for user review.
    Does NOT apply changes yet."""
    file_bytes = bank_letter.file.read()
    mime_type = bank_letter.content_type

    result = maintenance_service.process_bank_initiated_change(
        db, issued_lg_id, file_bytes, mime_type,
        current_user.user_id, current_user.customer_id,
    )
    return result


@router.post("/maintenance/{action_id}/confirm-bank-change")
def confirm_bank_initiated_change(
    action_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """User reviewed AI-detected bank changes and confirms. Applies changes to LG record."""
    action = maintenance_service.confirm_bank_initiated_change(
        db, action_id, current_user.user_id, current_user.customer_id,
    )

    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

    # In-app + Email notification
    from app.services.notification_service import notify
    from app.core.email_service import send_email, get_customer_email_settings
    from app.services.issuance_notifications import _get_user_emails, _base_url, get_common_communication_emails

    # Notify all end users + admins for this customer
    users = db.query(User).filter(
        User.customer_id == current_user.customer_id,
        User.is_deleted == False,
        User.role.in_(["corporate_admin", "end_user"])
    ).all()
    user_ids = [u.id for u in users]

    is_liquidation = action.action_type in ("LIQUIDATION",)
    icon = "🚨" if is_liquidation else "🏦"
    alert = "HIGH ALERT: " if is_liquidation else ""

    if user_ids:
        notify(
            db, user_ids=user_ids, module="ISSUANCE",
            event_type=f"BANK_INITIATED_{action.action_type}",
            title=f"{alert}LG {ref} — Bank-Initiated {action.action_type.replace('_', ' ')}",
            message=f"The bank has initiated a {action.action_type.lower().replace('_', ' ')} on LG {ref}. Changes have been applied.",
            link=f"/corporate-admin/issuance/issued-lgs",
            actor_user_id=current_user.user_id,
            reference_id=action.id
        )

    return _serialize_action(action, lg)

class PreSubmitSimilarityPayload(BaseModel):
    reference_type: Optional[str] = None
    reference_number: Optional[str] = None
    beneficiary_name: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    lg_type_id: Optional[int] = None
    requested_expiry_date: Optional[date] = None
    exclude_request_id: Optional[int] = None

@router.post("/pre-submit-similarity")
def pre_submit_similarity_check(
    payload: PreSubmitSimilarityPayload,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Realtime check against issued LGs based on form fields."""
    from app.services.issuance_service import issuance_service
    
    return issuance_service.get_similarity_matches(
        db=db,
        customer_id=current_user.customer_id,
        reference_type=payload.reference_type,
        reference_number=payload.reference_number,
        beneficiary_name=payload.beneficiary_name,
        amount=payload.amount,
        currency=payload.currency,
        lg_type_id=payload.lg_type_id,
        requested_expiry_date=payload.requested_expiry_date,
        exclude_request_id=payload.exclude_request_id
    )


