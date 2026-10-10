from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, UploadFile, File, Response, Request, Query
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from typing import List, Any, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
import secrets
import logging
import json
import csv
import io
import os
import uuid
import shutil
import threading
import asyncio

from app.database import get_db
from app.core.security import get_current_active_user, TokenData
from app.crud.crud import log_action
from app.core.email_service import send_email, get_global_email_settings, get_customer_email_settings
from app.services.unified_email_builder import build_transaction_email_html, build_standard_email_html
from app.constants import UserRole


from app.schemas.schemas_quotation import (
    QuotationBankCreate, QuotationBankOut,
    QuotationContactInvitationCreate, QuotationContactInvitationOut,
    QuotationRequestCreate, QuotationRequestOut,
    QuotationResultsOut, QuotationResultItem,
    ReTenderRequest, QuotationResubmitRequest,
    QuotationCancellationRequest, QuotationRescheduleRequest,
    QuotationDelegateRequest
)
from app.crud.crud_quotation import crud_quotation, get_bank_leg_signature
from app.models.models_quotation import (
    QuotationRequest, QuotationBankAssignment, QuotationOffer, 
    QuotationTBillOffer, QuotationBank, QuotationBankContactInvitation, QuotationAnalytics, QuotationAccessOTP,
    QuotationLeg, QuotationBankLegConfig
)

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/upload-documents")
async def upload_quotation_documents(
    files: List[UploadFile] = File(...),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Uploads supporting documents for a quotation request to GCS bucket (or local storage fallback) in parallel."""
    import asyncio
    from app.core.ai_integration import _upload_to_gcs, generate_signed_gcs_url, GCS_BUCKET_NAME
    from app.core.storage_service import build_customer_blob_path
    
    async def _process_single_file(file: UploadFile):
        file_bytes = await file.read()
        safe_filename = f"{uuid.uuid4().hex[:8]}_{file.filename.replace(' ', '_')}"
        blob_path = build_customer_blob_path(current_user.customer_id, "quotations", f"rfq_docs/{safe_filename}")
        
        gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, blob_path, file_bytes, file.content_type or "application/octet-stream")
        if not gcs_uri:
            raise HTTPException(status_code=500, detail=f"Failed to upload document {file.filename} to cloud storage")
        
        # Fresh upload is guaranteed to exist: skip remote existence check for sub-millisecond HMAC signing
        signed = await generate_signed_gcs_url(gcs_uri, expiration=604800, skip_existence_check=True)
        doc_url = signed or gcs_uri

        return {
            "name": file.filename,
            "path": doc_url
        }

    # Parallelize file reading, uploading, and signing
    uploaded_files = await asyncio.gather(*[_process_single_file(f) for f in files])
    return {"documents": list(uploaded_files)}


@router.get("/document-url")
async def get_quotation_document_url(
    gcs_uri: str = Query(..., description="GCS URI to generate signed link for"),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Generates an authorized secure signed URL for viewing an RFQ document."""
    if not gcs_uri or not str(gcs_uri).startswith("gs://"):
        raise HTTPException(status_code=400, detail="Invalid GCS URI")
    
    # Customer tenant security check
    expected_customer_segment = f"customer_{current_user.customer_id}/"
    user_role = (getattr(current_user, 'role', '') or '').lower()
    is_system_admin = user_role in ('system_owner', 'super_admin') or getattr(current_user, 'is_superuser', False)
    if expected_customer_segment not in gcs_uri and not is_system_admin:
        raise HTTPException(status_code=403, detail="Unauthorized access to document")
    
    from app.core.ai_integration import generate_signed_gcs_url
    signed = await generate_signed_gcs_url(gcs_uri, expiration=3600)
    if not signed:
        raise HTTPException(status_code=500, detail="Could not generate signed URL")
    
    return {"url": signed}


def _resolve_entities_for_invitation(db: Session, customer_id: int, qb = None, fallback_entity_ids: list = None):
    """Resolves active legal entities for email notifications and handshake pages."""
    from app.models.models import CustomerEntity
    all_ents = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == customer_id,
        CustomerEntity.is_active == True,
        CustomerEntity.is_deleted == False
    ).order_by(CustomerEntity.entity_name.asc()).all()

    scope = getattr(qb, "entity_scope", "ALL_ENTITIES") or "ALL_ENTITIES"
    if scope == "ALL_ENTITIES" or not qb:
        return [{"id": e.id, "name": e.entity_name, "code": e.code} for e in all_ents], "ALL_ENTITIES"

    assigned_ids = [assoc.entity_id for assoc in qb.entity_associations] if getattr(qb, "entity_associations", None) else (fallback_entity_ids or [])
    ents = [{"id": e.id, "name": e.entity_name, "code": e.code} for e in all_ents if e.id in assigned_ids]
    if not ents:
        return [{"id": e.id, "name": e.entity_name, "code": e.code} for e in all_ents], "ALL_ENTITIES"
    return ents, "SPECIFIC_ENTITIES"


@router.post("/banks", response_model=QuotationBankOut)
def create_quotation_bank(
    bank_in: QuotationBankCreate,
    background_tasks: BackgroundTasks,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Adds or updates a Bank in a specific Customer's Quotation Roster."""
    # RBAC Guard: Strictly restricted to Corporate Administrators and System Owners
    if current_user.role not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER, "corporate_admin", "super_admin", "system_owner"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Corporate Administrators are authorized to configure bank counterparty rosters."
        )

    # Capture previous state for structured role audit
    existing_bank = None
    target_id = getattr(bank_in, 'id', None)
    if target_id:
        existing_bank = db.query(QuotationBank).filter(
            QuotationBank.id == target_id,
            QuotationBank.customer_id == current_user.customer_id
        ).first()
    elif (bank_in.entity_scope or "ALL_ENTITIES") == "ALL_ENTITIES":
        existing_bank = db.query(QuotationBank).filter(
            QuotationBank.customer_id == current_user.customer_id,
            QuotationBank.bank_id == bank_in.bank_id,
            QuotationBank.entity_scope == "ALL_ENTITIES",
            QuotationBank.trade_type == (bank_in.trade_type or "BOTH")
        ).first()

    old_contacts = list(existing_bank.contacts) if (existing_bank and existing_bank.contacts) else []
    old_auth_email = getattr(existing_bank, 'authorized_contact_email', None) if existing_bank else None

    bank = crud_quotation.create_quotation_bank(
        db,
        customer_id=current_user.customer_id,
        obj_in=bank_in,
        current_user_email=current_user.email
    )
    
    # Structured Audit Log: Track additions, removals, and role elevations
    new_contacts = list(bank.contacts) if bank.contacts else []
    old_map = {c.get("email", "").strip().lower(): c for c in old_contacts if c.get("email")}
    new_map = {c.get("email", "").strip().lower(): c for c in new_contacts if c.get("email")}

    added_contacts = [
        {"email": email, "name": data.get("name", ""), "role": data.get("role", "EXECUTION")}
        for email, data in new_map.items() if email not in old_map
    ]
    removed_contacts = [
        {"email": email, "name": data.get("name", ""), "role": data.get("role", "EXECUTION")}
        for email, data in old_map.items() if email not in new_map
    ]
    role_changes = []
    for email, new_data in new_map.items():
        if email in old_map:
            old_role = old_map[email].get("role", "EXECUTION")
            new_role = new_data.get("role", "EXECUTION")
            if old_role != new_role:
                role_changes.append({
                    "email": email,
                    "name": new_data.get("name", ""),
                    "from_role": old_role,
                    "to_role": new_role
                })

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_ROSTER_UPDATED" if existing_bank else "QUOTATION_BANK_ADDED",
        entity_type="QuotationBank",
        entity_id=bank.id,
        details={
            "bank_id": bank_in.bank_id,
            "bank_name": bank.bank.name if bank.bank else f"Bank {bank_in.bank_id}",
            "trade_type": bank.trade_type,
            "added_contacts": added_contacts,
            "removed_contacts": removed_contacts,
            "role_changes": role_changes,
            "total_contacts": len(new_contacts)
        },
        customer_id=current_user.customer_id
    )

    # Dispatch dealer invitation handshake email for all newly added contacts (first onboarding or roster additions)
    if added_contacts:
        import secrets
        from datetime import datetime, timezone, timedelta
        from app.core.email_service import send_email, get_customer_email_settings
        from app.services.unified_email_builder import build_dealer_invitation_email
        from app.core.routing import get_frontend_base_url
        from app.models.models import Customer, Bank

        cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
        bank_obj = bank.bank if bank.bank else db.query(Bank).filter(Bank.id == bank_in.bank_id).first()
        bank_name_str = bank_obj.name if bank_obj else f"Bank {bank_in.bank_id}"

        for contact in added_contacts:
            c_email = (contact.get("email") or "").strip().lower()
            if not c_email:
                continue

            existing_inv = db.query(QuotationBankContactInvitation).filter(
                QuotationBankContactInvitation.customer_id == current_user.customer_id,
                QuotationBankContactInvitation.bank_id == bank_in.bank_id,
                QuotationBankContactInvitation.email == c_email,
                QuotationBankContactInvitation.status.in_(["PENDING", "ACCEPTED"])
            ).first()

            if not existing_inv:
                inv_token = secrets.token_urlsafe(32)
                inv_expires_at = datetime.now(timezone.utc) + timedelta(days=14)

                invitation = QuotationBankContactInvitation(
                    customer_id=current_user.customer_id,
                    bank_id=bank_in.bank_id,
                    quotation_bank_id=bank.id,
                    email=c_email,
                    title=contact.get("name") or None,
                    role=(contact.get("role") or "EXECUTION").upper(),
                    token=inv_token,
                    status="PENDING",
                    invited_by_user_id=current_user.user_id,
                    expires_at=inv_expires_at
                )
                db.add(invitation)
                db.commit()
                db.refresh(invitation)

                try:
                    frontend_base = get_frontend_base_url(request)
                    handshake_link = f"{frontend_base}/public/dealer-handshake/{inv_token}"
                    ents_for_email, scope_str = _resolve_entities_for_invitation(db, current_user.customer_id, bank, bank_in.entity_ids)
                    subj, email_html = build_dealer_invitation_email(
                        customer_branding=cust.name if cust else "Corporate Treasury",
                        bank_name=bank_name_str,
                        dealer_email=invitation.email,
                        dealer_title=invitation.title,
                        role=invitation.role,
                        handshake_link=handshake_link,
                        entities=ents_for_email,
                        entity_scope=scope_str
                    )
                    email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
                    background_tasks.add_task(
                        send_email,
                        db,
                        [invitation.email],
                        subj,
                        email_html,
                        {},
                        email_settings
                    )
                except Exception as email_err:
                    logger.warning(f"Could not dispatch dealer handshake invitation email on onboarding: {email_err}")

    return bank


@router.post("/banks/{bank_config_id}/send-roster-report")
def send_bank_roster_report(
    bank_config_id: int,
    background_tasks: BackgroundTasks,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Dispatches an on-demand official trading roster audit report to the bank's Authorized Governance Contact."""
    if current_user.role not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER, "corporate_admin", "super_admin", "system_owner"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Corporate Administrators can send counterparty roster audit reports."
        )

    bank = db.query(QuotationBank).filter(
        QuotationBank.id == bank_config_id,
        QuotationBank.customer_id == current_user.customer_id
    ).first()

    if not bank:
        raise HTTPException(status_code=404, detail="Bank counterparty configuration not found.")

    if not bank.authorized_contact_email:
        raise HTTPException(
            status_code=400,
            detail="No Authorized Bank Governance Officer email is configured for this bank. Please provide and save an authorized email first."
        )

    from app.models.models import Customer
    from app.services.unified_email_builder import build_bank_roster_governance_email
    from app.core.email_service import get_customer_email_settings
    
    cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
    customer_name = cust.name if cust else "Corporate Treasury"
    bank_name = bank.bank.name if bank.bank else f"Bank {bank.bank_id}"
    email_settings, _ = get_customer_email_settings(db, current_user.customer_id)

    contacts_list = list(bank.contacts) if bank.contacts else []
    subj, body = build_bank_roster_governance_email(
        customer_branding=customer_name,
        bank_name=bank_name,
        authorized_contact_email=bank.authorized_contact_email,
        authorized_contact_name=bank.authorized_contact_name,
        contacts=contacts_list,
        email_purpose="ROSTER_AUDIT"
    )

    background_tasks.add_task(
        send_email,
        db,
        [bank.authorized_contact_email],
        subj,
        body,
        {},
        email_settings
    )


# --- Staged Dealer Contact Invitations & Handshake Management ---

@router.get("/banks/{bank_id}/invitations", response_model=List[QuotationContactInvitationOut])
def get_bank_contact_invitations(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Retrieves all pending staged invitations for a given bank partner."""
    invitations = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.customer_id == current_user.customer_id,
        QuotationBankContactInvitation.bank_id == bank_id,
        QuotationBankContactInvitation.status == "PENDING"
    ).order_by(QuotationBankContactInvitation.created_at.desc()).all()
    return invitations


@router.post("/banks/{bank_id}/invite-contact", response_model=QuotationContactInvitationOut)
def invite_bank_contact(
    bank_id: int,
    body: QuotationContactInvitationCreate,
    background_tasks: BackgroundTasks,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Stages a new trading representative in the invitation table and dispatches
    a personal tokenized welcome handshake email to the dealer.
    Enforces all counterparty controls (bank domain, public provider blocks, anti-collusion, duplicates).
    """
    from app.models.models import Bank, Customer
    from app.core.bank_validation import validate_bank_contact_email
    from app.services.unified_email_builder import build_dealer_invitation_email
    from app.core.routing import get_frontend_base_url
    from app.core.email_service import get_customer_email_settings

    bank = db.query(Bank).filter(Bank.id == bank_id).first()
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found.")

    cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
    clean_email = body.email.strip().lower()

    # 1. Enforce Counterparty Integrity & Domain Validation Controls
    is_valid, err_msg = validate_bank_contact_email(
        email=clean_email,
        bank=bank,
        customer=cust,
        current_user_email=current_user.email
    )
    if not is_valid:
        raise HTTPException(status_code=400, detail=err_msg)

    # 2. Check if already active in quotation_banks.contacts
    qb = db.query(QuotationBank).filter(
        QuotationBank.customer_id == current_user.customer_id,
        QuotationBank.bank_id == bank_id
    ).first()
    if qb and qb.contacts:
        active_emails = [(c.get("email") or "").strip().lower() for c in qb.contacts]
        if clean_email in active_emails:
            raise HTTPException(
                status_code=400,
                detail=f"{clean_email} is already an active registered trading representative for this bank."
            )

    # 3. Check if already pending an invitation
    existing_inv = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.customer_id == current_user.customer_id,
        QuotationBankContactInvitation.bank_id == bank_id,
        QuotationBankContactInvitation.email == clean_email,
        QuotationBankContactInvitation.status == "PENDING"
    ).first()
    if existing_inv:
        raise HTTPException(
            status_code=400,
            detail=f"An invitation is already pending for {clean_email}. Use the resend option to refresh it."
        )

    # 4. Create staged invitation record
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(days=14)

    invitation = QuotationBankContactInvitation(
        customer_id=current_user.customer_id,
        bank_id=bank_id,
        quotation_bank_id=qb.id if qb else None,
        email=clean_email,
        title=body.title.strip() if body.title else None,
        role=(body.role or "EXECUTION").upper(),
        token=token,
        status="PENDING",
        invited_by_user_id=current_user.user_id,
        expires_at=expires_at
    )
    db.add(invitation)
    db.commit()
    db.refresh(invitation)

    # 5. Dispatch dealer invitation handshake email
    try:
        frontend_base = get_frontend_base_url(request)
        handshake_link = f"{frontend_base}/public/dealer-handshake/{token}"
        qb = db.query(QuotationBank).filter(
            QuotationBank.customer_id == current_user.customer_id,
            QuotationBank.bank_id == bank_id
        ).first()
        ents_for_email, scope_str = _resolve_entities_for_invitation(db, current_user.customer_id, qb)
        subj, email_html = build_dealer_invitation_email(
            customer_branding=cust.name if cust else "Corporate Treasury",
            bank_name=bank.name,
            dealer_email=invitation.email,
            dealer_title=invitation.title,
            role=invitation.role,
            handshake_link=handshake_link,
            entities=ents_for_email,
            entity_scope=scope_str
        )
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        background_tasks.add_task(
            send_email,
            db,
            [invitation.email],
            subj,
            email_html,
            {},
            email_settings
        )
    except Exception as email_err:
        logger.warning(f"Could not dispatch dealer handshake invitation email: {email_err}")

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_DEALER_INVITED",
        entity_type="QuotationBankContactInvitation",
        entity_id=invitation.id,
        details={
            "bank_id": bank_id,
            "bank_name": bank.name,
            "email": invitation.email,
            "title": invitation.title,
            "role": invitation.role
        },
        customer_id=current_user.customer_id
    )

    return invitation


@router.post("/banks/invitations/{invitation_id}/resend")
def resend_contact_invitation(
    invitation_id: int,
    background_tasks: BackgroundTasks,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Refreshes the token and resends the invitation email to the pending dealer."""
    from app.models.models import Bank, Customer
    from app.services.unified_email_builder import build_dealer_invitation_email
    from app.core.routing import get_frontend_base_url
    from app.core.email_service import get_customer_email_settings

    inv = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.id == invitation_id,
        QuotationBankContactInvitation.customer_id == current_user.customer_id
    ).first()
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found.")
    if inv.status != "PENDING":
        raise HTTPException(status_code=400, detail=f"Cannot resend invitation with status '{inv.status}'.")

    # Refresh token and expiration
    inv.token = secrets.token_urlsafe(32)
    inv.expires_at = datetime.now(timezone.utc) + timedelta(days=14)
    db.commit()
    db.refresh(inv)

    cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
    bank = db.query(Bank).filter(Bank.id == inv.bank_id).first()
    frontend_base = get_frontend_base_url(request)
    handshake_link = f"{frontend_base}/public/dealer-handshake/{inv.token}"

    qb = None
    if inv.quotation_bank_id:
        from app.models.models_quotation import QuotationBank
        qb = db.query(QuotationBank).filter(QuotationBank.id == inv.quotation_bank_id).first()
    if not qb:
        from app.models.models_quotation import QuotationBank
        qb = db.query(QuotationBank).filter(
            QuotationBank.customer_id == current_user.customer_id,
            QuotationBank.bank_id == inv.bank_id
        ).first()

    ents_for_email, scope_str = _resolve_entities_for_invitation(db, current_user.customer_id, qb)

    subj, email_html = build_dealer_invitation_email(
        customer_branding=cust.name if cust else "Corporate Treasury",
        bank_name=bank.name if bank else f"Bank {inv.bank_id}",
        dealer_email=inv.email,
        dealer_title=inv.title,
        role=inv.role,
        handshake_link=handshake_link,
        entities=ents_for_email,
        entity_scope=scope_str
    )
    email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
    background_tasks.add_task(
        send_email,
        db,
        [inv.email],
        subj,
        email_html,
        {},
        email_settings
    )

    return {"status": "success", "message": f"Invitation resent to {inv.email}."}


@router.delete("/banks/invitations/{invitation_id}")
def revoke_contact_invitation(
    invitation_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Revokes / deletes a pending contact invitation."""
    inv = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.id == invitation_id,
        QuotationBankContactInvitation.customer_id == current_user.customer_id
    ).first()
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found.")

    db.delete(inv)
    db.commit()
    return {"status": "success", "message": "Invitation cancelled."}

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_ROSTER_REPORT_SENT",
        entity_type="QuotationBank",
        entity_id=bank.id,
        details={
            "bank_id": bank.bank_id,
            "bank_name": bank_name,
            "recipient_email": bank.authorized_contact_email,
            "total_contacts": len(contacts_list)
        },
        customer_id=current_user.customer_id
    )

    return {
        "status": "success",
        "message": f"Official roster audit report successfully sent to {bank.authorized_contact_email}.",
        "recipient": bank.authorized_contact_email
    }

@router.delete("/banks/{bank_id}")
def delete_quotation_bank(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Removes a Bank from the Customer's Quotation Roster."""
    # RBAC Guard: Strictly restricted to Corporate Administrators and System Owners
    if current_user.role not in [UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER, "corporate_admin", "super_admin", "system_owner"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Corporate Administrators are authorized to remove bank counterparties."
        )

    success = crud_quotation.delete_quotation_bank(db, customer_id=current_user.customer_id, bank_id=bank_id)
    if not success:
        raise HTTPException(status_code=404, detail="Bank configuration not found.")
    
    # Audit log
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_REMOVED",
        entity_type="QuotationBank",
        entity_id=bank_id,
        details={"bank_id": bank_id},
        customer_id=current_user.customer_id
    )
    return {"message": "Bank configuration removed."}

@router.get("/entities")
def get_user_accessible_quotation_entities(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Returns active customer entities accessible by current user for creating or filtering quotations.
    """
    from app.models.models import CustomerEntity, User
    user = db.query(User).filter(User.id == current_user.user_id).first()
    is_admin = current_user.role in ["corporate_admin", "super_admin"]
    has_all = getattr(user, "has_all_entity_access", False) if user else False

    if is_admin or has_all:
        entities = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == current_user.customer_id,
            CustomerEntity.is_active == True,
            CustomerEntity.is_deleted == False
        ).order_by(CustomerEntity.entity_name.asc()).all()
    else:
        allowed_ids = [assoc.customer_entity_id for assoc in user.entity_associations] if user else []
        entities = db.query(CustomerEntity).filter(
            CustomerEntity.id.in_(allowed_ids),
            CustomerEntity.customer_id == current_user.customer_id,
            CustomerEntity.is_active == True,
            CustomerEntity.is_deleted == False
        ).order_by(CustomerEntity.entity_name.asc()).all()

    return [
        {
            "id": e.id,
            "entity_name": e.entity_name,
            "name": e.entity_name,
            "code": e.code,
            "tax_id": e.tax_id,
            "commercial_register_number": e.commercial_register_number,
            "address": e.address
        }
        for e in entities
    ]

@router.get("/banks", response_model=List[QuotationBankOut])
def get_quotation_banks(
    trade_type: str = None,
    entity_id: int = None,
    exclude_all_pending: bool = False,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    return crud_quotation.get_quotation_banks(
        db,
        customer_id=current_user.customer_id,
        trade_type=trade_type,
        entity_id=entity_id,
        exclude_all_pending=exclude_all_pending
    )

@router.get("/banks/latest-costs")
def get_latest_bank_costs(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Retrieves cost settings from the most recent RFQ for a given bank that actually had non-zero cost parameters."""
    q_banks = db.query(QuotationBank).filter(
        QuotationBank.customer_id == current_user.customer_id,
        QuotationBank.bank_id == bank_id
    ).all()
    q_bank_ids = [qb.id for qb in q_banks]
    
    if not q_bank_ids:
        return {"cost_min": 0.0, "cost_percent": 0.0, "cost_max": 0.0, "cost_flat": 0.0}
        
    # First, search QuotationBankAssignment for most recent non-zero cost
    latest_assignment = db.query(QuotationBankAssignment).join(
        QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
    ).filter(
        QuotationBankAssignment.quotation_bank_id.in_(q_bank_ids),
        QuotationRequest.customer_id == current_user.customer_id,
        QuotationRequest.status.in_(['PENDING', 'EVALUATING', 'COMPLETED']),
        or_(
            QuotationBankAssignment.cost_min > 0,
            QuotationBankAssignment.cost_percent > 0,
            QuotationBankAssignment.cost_max > 0,
            QuotationBankAssignment.cost_flat > 0
        )
    ).order_by(QuotationRequest.created_at.desc()).first()
    
    if latest_assignment:
        return {
            "cost_min": latest_assignment.cost_min or 0.0,
            "cost_percent": latest_assignment.cost_percent or 0.0,
            "cost_max": latest_assignment.cost_max or 0.0,
            "cost_flat": latest_assignment.cost_flat or 0.0,
            "quotation_base": latest_assignment.quotation_base
        }
        
    # Also search QuotationBankLegConfig in case costs were configured per currency leg
    latest_leg_cfg = db.query(QuotationBankLegConfig).join(
        QuotationBankAssignment, QuotationBankLegConfig.assignment_id == QuotationBankAssignment.id
    ).join(
        QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
    ).filter(
        QuotationBankAssignment.quotation_bank_id.in_(q_bank_ids),
        QuotationRequest.customer_id == current_user.customer_id,
        QuotationRequest.status.in_(['PENDING', 'EVALUATING', 'COMPLETED']),
        or_(
            QuotationBankLegConfig.cost_min > 0,
            QuotationBankLegConfig.cost_percent > 0,
            QuotationBankLegConfig.cost_max > 0,
            QuotationBankLegConfig.cost_flat > 0
        )
    ).order_by(QuotationRequest.created_at.desc()).first()

    if latest_leg_cfg:
        return {
            "cost_min": latest_leg_cfg.cost_min or 0.0,
            "cost_percent": latest_leg_cfg.cost_percent or 0.0,
            "cost_max": latest_leg_cfg.cost_max or 0.0,
            "cost_flat": latest_leg_cfg.cost_flat or 0.0,
            "quotation_base": latest_leg_cfg.quotation_base
        }

    return {"cost_min": 0.0, "cost_percent": 0.0, "cost_max": 0.0, "cost_flat": 0.0}

@router.get("/recommendations")
def get_bank_recommendations(
    trade_type: str = "FX_SPOT",
    buy_currency: str = None,
    sell_currency: str = None,
    amount: float = None,
    quotation_base: str = "Execution",
    currency_pairs: str = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Multi-dimensional Smart Counterparty Recommendation Engine.
    Evaluates:
    1. Direct Pair Win Rate & Currency Specialization (Affinity).
    2. Competitive Proximity / Runner-Up Index (Metric A: Finished Top 2 / within tight spread).
    3. Ticket Size Appetite (Metric B: Win and quote rate on large tickets >= $1M).
    4. Pure Participation Rate (Drop response-time penalty; measure commitments delivered).
    5. Multi-Leg Basket Coverage for complex multi-pair portfolios.
    """
    from app.models.models_quotation import QuotationBank, QuotationRequest, QuotationBankAssignment, QuotationOffer

    q = db.query(QuotationBank).filter(
        QuotationBank.customer_id == current_user.customer_id
    )
    if trade_type and trade_type != 'BOTH':
        q = q.filter(QuotationBank.trade_type.in_([trade_type, 'BOTH']))
    all_customer_banks = q.all()

    # Exclude banks where all registered contacts are pending dealer handshake
    from app.models.models_quotation import QuotationBankContactInvitation
    pending_invs = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.customer_id == current_user.customer_id,
        QuotationBankContactInvitation.status == "PENDING"
    ).all()
    pending_by_bank = {}
    for pinv in pending_invs:
        pending_by_bank.setdefault(pinv.bank_id, set()).add((pinv.email or "").strip().lower())

    customer_banks = []
    for qb in all_customer_banks:
        q_contacts = list(qb.contacts) if qb.contacts else []
        pending_set = pending_by_bank.get(qb.bank_id, set())
        has_active = any((c.get("email") or "").strip().lower() not in pending_set for c in q_contacts)
        if q_contacts and has_active:
            customer_banks.append(qb)

    if not customer_banks:
        return {"recommended_bank_ids": [], "recommendations": [], "all_bank_analytics": {}}

    target_pairs = set()
    if buy_currency and sell_currency:
        target_pairs.add(f"{buy_currency.upper()}/{sell_currency.upper()}")
    if currency_pairs:
        for p in currency_pairs.split(','):
            p_clean = p.strip().upper()
            if '/' in p_clean:
                target_pairs.add(p_clean)

    is_large_ticket_request = bool(amount and amount >= 1000000.0)
    bank_stats = []

    for qb in customer_banks:
        b_name = qb.bank.name if qb.bank else f"Bank {qb.bank_id}"

        # All closed assignments
        assignments = db.query(QuotationBankAssignment).join(
            QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
        ).filter(
            QuotationBankAssignment.quotation_bank_id == qb.id,
            QuotationRequest.status.in_(['COMPLETED', 'EXPIRED', 'REJECTED'])
        ).all()

        total_invited = len(assignments)
        if total_invited == 0:
            bank_stats.append({
                "bank_id": qb.bank_id,
                "quotation_bank_id": qb.id,
                "bank_name": b_name,
                "score": 50.0,
                "participation_rate": 100.0,
                "global_win_rate": 0.0,
                "pair_win_rate": 0.0,
                "top_2_rate": 0.0,
                "total_won": 0,
                "total_participated": 0,
                "total_invited": 0,
                "highlight": "Roster Bank • Ready to Quote",
                "badges": ["✨ New Roster Bank"]
            })
            continue

        total_responded = 0
        total_legs_quoted = 0
        total_legs_won = 0

        pair_legs_quoted = 0
        pair_legs_won = 0

        top_2_count = 0

        large_ticket_quoted = 0
        large_ticket_won = 0

        multi_leg_packages_invited = 0
        multi_leg_packages_fully_quoted = 0

        for a in assignments:
            rfq = a.rfq
            offers = a.offers or []
            if offers:
                total_responded += 1

            rfq_legs = rfq.legs or []
            if len(rfq_legs) > 1:
                multi_leg_packages_invited += 1
                legs_with_quotes = set(o.leg_id for o in offers if o.leg_id)
                if len(legs_with_quotes) >= len(rfq_legs):
                    multi_leg_packages_fully_quoted += 1

            is_large_rfq = ((rfq.amount or 0) >= 1000000.0) or any((l.amount or 0) >= 1000000.0 for l in rfq_legs)
            if is_large_rfq and offers:
                large_ticket_quoted += 1

            for leg in rfq_legs:
                leg_offers = [o for o in offers if (o.leg_id == leg.id or (not o.leg_id and len(rfq_legs) == 1))]
                if not leg_offers:
                    continue

                total_legs_quoted += 1
                leg_pair_str = f"{(leg.buy_currency or '').upper()}/{(leg.sell_currency or '').upper()}"
                is_pair_match = leg_pair_str in target_pairs if target_pairs else False

                if is_pair_match:
                    pair_legs_quoted += 1

                is_leg_won = (leg.winner_bank_id == qb.bank_id)
                if is_leg_won:
                    total_legs_won += 1
                    if is_pair_match:
                        pair_legs_won += 1
                    if is_large_rfq:
                        large_ticket_won += 1

                # Competitive Proximity (Top 2 Rank in this leg - Metric A)
                all_comp_quotes = []
                from app.services.tenant_key_service import tenant_key_service
                tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, current_user.customer_id)
                for other_a in rfq.assignments:
                    o_offers = [o for o in other_a.offers if (o.leg_id == leg.id or (not o.leg_id and len(rfq_legs) == 1))]
                    if o_offers:
                        best_p = min(tenant_key_service.resolve_offer_price(o, tenant_dek) for o in o_offers) if (leg.direction or 'Buy').lower() == 'buy' else max(tenant_key_service.resolve_offer_price(o, tenant_dek) for o in o_offers)
                        all_comp_quotes.append({
                            "bank_id": other_a.quotation_bank.bank_id,
                            "price": best_p
                        })

                if (leg.direction or 'Buy').lower() == 'buy':
                    all_comp_quotes.sort(key=lambda x: x['price'])
                else:
                    all_comp_quotes.sort(key=lambda x: x['price'], reverse=True)

                top_2_ids = [item['bank_id'] for item in all_comp_quotes[:2]]
                if qb.bank_id in top_2_ids:
                    top_2_count += 1

        participation_rate = (total_responded / total_invited * 100) if total_invited > 0 else 0.0
        global_win_rate = (total_legs_won / total_legs_quoted * 100) if total_legs_quoted > 0 else 0.0
        pair_win_rate = (pair_legs_won / pair_legs_quoted * 100) if pair_legs_quoted > 0 else 0.0
        top_2_rate = (top_2_count / total_legs_quoted * 100) if total_legs_quoted > 0 else 0.0

        badges = []

        # 1. Pair Specialist Badge (Affinity)
        active_pair_label = list(target_pairs)[0] if target_pairs else ""
        if pair_legs_won >= 2 and pair_win_rate >= 40.0:
            badges.append(f"⭐ {active_pair_label} Specialist ({pair_legs_won} of {pair_legs_quoted} won)")
        elif pair_legs_won >= 1 and pair_win_rate >= 40.0:
            badges.append(f"⭐ Proven in {active_pair_label}")

        # 2. Competitive Proximity Badge (Metric A)
        if top_2_count >= 2 and top_2_rate >= 50.0:
            badges.append(f"🎯 Tight Competitor (Top 2 in {top_2_rate:.0f}% of quotes)")

        # 3. Ticket Size Appetite Badge (Metric B)
        if is_large_ticket_request and large_ticket_won >= 1:
            badges.append(f"🏛️ Mega-Ticket Dominance ({large_ticket_won} deals > $1M)")
        elif large_ticket_won >= 2:
            badges.append(f"🏛️ Large-Ticket Proven ({large_ticket_won} deals > $1M)")

        # 4. Multi-Leg Basket Badge
        if len(target_pairs) > 1 and multi_leg_packages_fully_quoted >= 2:
            badges.append("📦 Full Basket Quoter")

        # 5. Pure Participation Badge (No response-time penalty)
        if participation_rate >= 85.0:
            badges.append(f"⚡ Highly Active ({participation_rate:.0f}% participation)")
        elif participation_rate < 40.0 and total_invited >= 3:
            badges.append(f"⚠️ Low Response Rate ({total_responded}/{total_invited} quoted)")

        # Transparent Scoring Model (Out of 100)
        eff_win = pair_win_rate if pair_legs_quoted >= 2 else global_win_rate
        score = (eff_win * 0.40) + (top_2_rate * 0.35) + (participation_rate * 0.25)
        if is_large_ticket_request and large_ticket_won >= 1:
            score += 10.0
        if participation_rate < 40.0 and total_invited >= 3:
            score = max(0.0, score - 15.0)

        primary_highlight = badges[0] if badges else (f"{global_win_rate:.0f}% Win Rate" if total_legs_won > 0 else "Active Counterparty")

        bank_stats.append({
            "bank_id": qb.bank_id,
            "quotation_bank_id": qb.id,
            "bank_name": b_name,
            "score": round(score, 1),
            "participation_rate": round(participation_rate, 1),
            "global_win_rate": round(global_win_rate, 1),
            "pair_win_rate": round(pair_win_rate, 1),
            "top_2_rate": round(top_2_rate, 1),
            "total_won": total_legs_won,
            "total_participated": total_responded,
            "total_invited": total_invited,
            "highlight": primary_highlight,
            "badges": badges
        })

    bank_stats.sort(key=lambda x: x["score"], reverse=True)
    top_recommendations = bank_stats[:3]
    rec_bank_ids = [r["bank_id"] for r in top_recommendations]

    for b in bank_stats:
        b["is_top_recommended"] = b["bank_id"] in rec_bank_ids

    return {
        "recommended_bank_ids": rec_bank_ids,
        "recommendations": top_recommendations,
        "all_bank_analytics": {r["bank_id"]: r for r in bank_stats}
    }

@router.get("/evaluation-rate")
def get_quotation_evaluation_rate(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Returns CBE corridor rates, customer margin, and effective evaluation rate (mid + margin).
    Used to pre-fill the Evaluation Interest Rate field in T-Bills and alternative value date quotations.
    """
    from app.core.background_tasks import get_quotation_eval_rate_details
    return get_quotation_eval_rate_details(db, customer_id=current_user.customer_id)

def _dispatch_quotation_submission_email(
    db: Session,
    background_tasks: BackgroundTasks,
    rfq: Any,
    current_user: Any,
    requires_approval: bool,
    request: Optional[Request] = None,
    is_retender: bool = False
):
    try:
        from app.models import User, UserRole
        from app.services.issuance_notifications import get_common_communication_emails
        from app.services.unified_email_builder import build_transaction_email_html
        from app.core.email_service import get_customer_email_settings, send_email
        from app.core.routing import get_frontend_base_url

        admins = db.query(User).filter(
            User.customer_id == current_user.customer_id,
            User.role == UserRole.CORPORATE_ADMIN,
            User.is_deleted == False
        ).all()
        admin_emails = list(dict.fromkeys(
            a.email.strip() for a in admins if a.email and "@" in a.email
        ))
        cc_list = get_common_communication_emails(db, current_user.customer_id)
        admin_set = {e.lower() for e in admin_emails}
        cc_emails = [e for e in cc_list if e.lower() not in admin_set]

        to_recipients = admin_emails if admin_emails else cc_emails
        cc_recipients = cc_emails if admin_emails else []

        if not to_recipients:
            logger.info(f"No recipients found for quotation submission email (RFQ {getattr(rfq, 'ref_no', '')}).")
            return

        base_url = get_frontend_base_url(request=request)
        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)

        customer_display_name = (rfq.customer.name if getattr(rfq, "customer", None) and rfq.customer.name else "Corporate Treasury")
        entity_display_name = (rfq.entity.entity_name if getattr(rfq, "entity", None) and rfq.entity.entity_name else None)
        submitter_display = getattr(current_user, "email", None) or f"User #{getattr(current_user, 'user_id', '')}"

        kv = {
            "Quotation Reference": rfq.ref_no,
            "Type": "FX Spot" if rfq.type == "FX_SPOT" else "Treasury Bill (T-Bill)",
            "Direction": rfq.direction or "N/A",
            "Submitted By": submitter_display,
        }
        legs_list = getattr(rfq, "legs", []) or []
        if rfq.type == "FX_SPOT":
            if len(legs_list) > 1:
                leg_bases = list(set((l.quotation_base or "Execution").capitalize() for l in legs_list))
                is_mixed = len(leg_bases) > 1
                mixed_str = " &bull; Mixed Execution &amp; Indicative" if is_mixed else ""
                kv["Package Structure"] = f"Multi-Currency Package ({len(legs_list)} Pairs{mixed_str})"
                for idx, leg in enumerate(legs_list, 1):
                    leg_dir = leg.direction or "Buy"
                    leg_amt = f"{leg.amount:,.2f} {leg.buy_currency}" if leg.amount else "N/A"
                    leg_pair = leg.currency_pair or f"{leg.buy_currency}/{leg.sell_currency}"
                    leg_val = str(leg.value_date) if leg.value_date else "Standard"
                    leg_base = leg.quotation_base or "Execution"
                    kv[f"Leg #{idx}: {leg_pair}"] = f"{leg_dir} {leg_amt} | Value: {leg_val} | Base: {leg_base}"
            else:
                if rfq.amount:
                    curr = rfq.buy_currency if rfq.direction == "Buy" else rfq.sell_currency
                    kv["Amount"] = f"{rfq.amount:,.2f} {curr or ''}".strip()
                if rfq.buy_currency and rfq.sell_currency:
                    kv["Currency Pair"] = f"{rfq.buy_currency} / {rfq.sell_currency}"
                if rfq.value_date:
                    kv["Value Date"] = str(rfq.value_date)
                if rfq.quotation_base:
                    kv["Quotation Base"] = rfq.quotation_base
        else:
            if rfq.amount:
                kv["Face Value"] = f"{rfq.amount:,.2f} EGP"
            if rfq.maturity_date_start:
                kv["Maturity"] = (
                    f"{rfq.maturity_date_start} to {rfq.maturity_date_end}"
                    if rfq.maturity_date_end and rfq.maturity_date_end != rfq.maturity_date_start
                    else str(rfq.maturity_date_start)
                )

        if entity_display_name:
            kv["Entity"] = entity_display_name

        if rfq.window_start or rfq.window_end:
            try:
                from zoneinfo import ZoneInfo
                cairo_tz = ZoneInfo("Africa/Cairo")
                if rfq.window_start:
                    ws = rfq.window_start
                    if hasattr(ws, "tzinfo") and ws.tzinfo is None:
                        from datetime import timezone as _tz
                        ws = ws.replace(tzinfo=_tz.utc)
                    ws_cairo = ws.astimezone(cairo_tz)
                    kv["Window Opens"] = ws_cairo.strftime("%A, %d %b %Y at %H:%M %Z")
                if rfq.window_end:
                    we = rfq.window_end
                    if hasattr(we, "tzinfo") and we.tzinfo is None:
                        from datetime import timezone as _tz
                        we = we.replace(tzinfo=_tz.utc)
                    we_cairo = we.astimezone(cairo_tz)
                    kv["Submission Deadline"] = we_cairo.strftime("%A, %d %b %Y at %H:%M %Z")
            except Exception:
                # Fallback: show raw values if timezone conversion fails
                if rfq.window_start:
                    kv["Window Opens"] = str(rfq.window_start)
                if rfq.window_end:
                    kv["Submission Deadline"] = str(rfq.window_end)

        # Expected Results Window duration based on Customer Configuration
        admin_timeout_sec = 120 if rfq.type == "TBILL" else 30
        try:
            from app.crud.crud_config import crud_customer_configuration
            from app.constants import GlobalConfigKey
            cfg_key = GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_TBILL if rfq.type == "TBILL" else GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_FX_SPOT
            cfg = crud_customer_configuration.get_customer_config_or_global_fallback(db, current_user.customer_id, cfg_key)
            if cfg and cfg.get("effective_value"):
                admin_timeout_sec = int(cfg["effective_value"])
        except Exception:
            pass

        if admin_timeout_sec < 60:
            res_dur_str = f"{admin_timeout_sec} seconds"
        elif admin_timeout_sec == 60:
            res_dur_str = "1 minute (60 seconds)"
        elif admin_timeout_sec % 60 == 0:
            res_dur_str = f"{admin_timeout_sec // 60} minutes ({admin_timeout_sec} seconds)"
        else:
            res_dur_str = f"{admin_timeout_sec // 60} min {admin_timeout_sec % 60} sec ({admin_timeout_sec}s)"

        kv["Expected Results Window"] = f"Within {res_dur_str} after deadline"

        action_label = "Re-Tender" if is_retender else "Quotation"
        multi_label = f" ({len(legs_list)} Pairs)" if len(legs_list) > 1 else ""
        if requires_approval:
            subject = f"ACTION REQUIRED: {action_label} Request {rfq.ref_no}{multi_label} Awaiting Approval"
            title = f"🔔 {action_label} Awaiting Approval"
            summary_text = (
                f"A new {action_label.lower()} request ({rfq.ref_no}){f' for {len(legs_list)} currency pairs' if len(legs_list) > 1 else ''} has been submitted by {submitter_display} "
                f"and is awaiting your review and approval before release to counterparties."
            )
            cta_text = "Review & Approve Quotation"
            cta_url = f"{base_url}/corporate-admin/quotations/history?rfq_id={rfq.id}"
        else:
            subject = f"NOTIFICATION: New {action_label} Request {rfq.ref_no}{multi_label} Submitted"
            title = f"{action_label} Request Submitted"
            summary_text = (
                f"A new {action_label.lower()} request ({rfq.ref_no}){f' for {len(legs_list)} currency pairs' if len(legs_list) > 1 else ''} has been submitted by {submitter_display} "
                f"and released to counterparties."
            )
            cta_text = "View Quotation"
            cta_url = f"{base_url}/corporate-admin/quotations/history?rfq_id={rfq.id}"

        body = build_transaction_email_html(
            customer_name=customer_display_name,
            title=title,
            transaction_ref=rfq.ref_no,
            transaction_type=f"{action_label} Request",
            key_value_dict=kv,
            summary_text=summary_text,
            cta_text=cta_text,
            cta_url=cta_url,
            recipient_name="Corporate Admin",
            platform_name="Grow Treasury Platform"
        )

        background_tasks.add_task(
            send_email,
            db,
            to_recipients,
            subject,
            body,
            {},
            email_settings,
            cc_emails=cc_recipients
        )
        logger.info(f"Queued quotation notification email for {to_recipients} (CC: {cc_recipients}) for RFQ {rfq.ref_no}")
    except Exception as e:
        logger.error(f"Error queueing quotation submission email for RFQ {getattr(rfq, 'ref_no', '')}: {e}", exc_info=True)

@router.post("/", response_model=Any)
def create_rfq(
    rfq_in: QuotationRequestCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Creates a new RFQ and generates secure tokens for external Banks."""
    # Add file path parsing here if files are uploaded.
    # For now, it accepts JSON. If files are needed, this endpoint will need to use Form/File Fastapi constructs.
    
    try:
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        
        config = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED
        )
        requires_approval = False
        if config and config.get("effective_value"):
            requires_approval = str(config.get("effective_value")).lower() == 'true'

        # --- T-Bill Directional Validation ---
        if rfq_in.type == 'TBILL':
            if rfq_in.direction == 'Sell':
                # No ranges allowed for Sell
                if (rfq_in.settlementDateEnd and rfq_in.settlementDateEnd != rfq_in.settlementDateStart) or \
                   (rfq_in.maturityDateEnd and rfq_in.maturityDateEnd != rfq_in.maturityDateStart):
                    raise HTTPException(status_code=400, detail="Date ranges are not allowed for T-Bill Sell quotations.")
                # Eval rate not allowed / irrelevant for Sell
                if rfq_in.evalRate is not None:
                     rfq_in.evalRate = None # Silently clear or could raise error. Let's clear it.
            elif rfq_in.direction == 'Buy':
                # Eval rate required if ranges are present
                has_range = (rfq_in.settlementDateEnd and rfq_in.settlementDateEnd != rfq_in.settlementDateStart) or \
                            (rfq_in.maturityDateEnd and rfq_in.maturityDateEnd != rfq_in.maturityDateStart)
                if has_range and (rfq_in.evalRate is None or rfq_in.evalRate <= 0):
                    raise HTTPException(status_code=400, detail="Evaluation Interest Rate (%) is required for T-Bill Buy quotations with date ranges.")

        # --- Strict Positive Amount Validation ---
        if rfq_in.type == 'FX_SPOT':
            legs_list = rfq_in.legs or rfq_in.pairs
            if legs_list:
                for idx, leg in enumerate(legs_list):
                    if leg.amount is None or leg.amount <= 0:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Trade amount for Pair #{idx + 1} ({leg.buyCurrency or 'USD'}/{leg.sellCurrency or 'EGP'}) must be a positive number strictly greater than 0."
                        )
            else:
                if rfq_in.amount is None or rfq_in.amount <= 0:
                    raise HTTPException(status_code=400, detail="Trade amount must be a positive number strictly greater than 0.")
        elif rfq_in.type == 'TBILL':
            if rfq_in.amount is None or rfq_in.amount <= 0:
                raise HTTPException(status_code=400, detail="Total amount must be a positive number strictly greater than 0.")

        # --- Entity Scope & Access Validation ---
        from app.models.models import CustomerEntity, User
        cust_entities = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == current_user.customer_id,
            CustomerEntity.is_active == True,
            CustomerEntity.is_deleted == False
        ).all()

        if cust_entities:
            if len(cust_entities) == 1 and not rfq_in.entity_id:
                rfq_in.entity_id = cust_entities[0].id
            elif not rfq_in.entity_id:
                raise HTTPException(status_code=400, detail="Please select the requesting Legal Entity.")

            user = db.query(User).filter(User.id == current_user.user_id).first()
            is_admin = current_user.role in ["corporate_admin", "super_admin"]
            has_all = getattr(user, "has_all_entity_access", False) if user else False
            if not is_admin and not has_all:
                user_ids = [assoc.customer_entity_id for assoc in user.entity_associations] if user else []
                if rfq_in.entity_id not in user_ids:
                    raise HTTPException(status_code=403, detail="You do not have permission to create quotations for this entity.")

        rfq, assignments = crud_quotation.create_request(
            db, 
            customer_id=current_user.customer_id, 
            user_id=current_user.user_id, 
            requires_approval=requires_approval,
            obj_in=rfq_in
        )
        
        log_action(
            db,
            user_id=current_user.user_id,
            action_type="QUOTATION_RFQ_CREATED",
            entity_type="QuotationRequest",
            entity_id=None, # UUID string cannot fit into Integer column
            details={
                "rfq_id": rfq.id,
                "ref_no": rfq.ref_no,
                "type": rfq.type,
                "entity_id": rfq.entity_id,
                "quotation_base": rfq.quotation_base,
                "legal_disclaimer_accepted": bool(rfq_in.legal_disclaimer_accepted or rfq_in.legalDisclaimerAccepted)
            },
            customer_id=current_user.customer_id
        )
        
        # Trigger immediate email dispatch if not requiring corporate-level approval
        if not requires_approval:
            # Schedule 15m prior reminder if window_start - now >= 60 min
            try:
                from app.services.quotation_reminder_service import schedule_rfq_15m_reminder
                schedule_rfq_15m_reminder(
                    rfq_id=rfq.id,
                    window_start=rfq.window_start,
                    release_time=rfq.created_at or datetime.now(timezone.utc)
                )
            except Exception as rem_err:
                logger.warning(f"Failed to schedule 15m reminder for RFQ {rfq.id}: {rem_err}")

            email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
            from app.core.routing import get_frontend_base_url
            base_url = get_frontend_base_url(request=request)
            entity_display_name = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else 'Corporate Treasury')
            
            for assignment in assignments:
                q_bank_id = assignment.get("quotation_bank_id")
                bank_row = db.query(QuotationBank).filter(QuotationBank.id == q_bank_id).first() if q_bank_id else None
                if not bank_row:
                    continue
                    
                contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
                if not contacts and bank_row.emails:
                    contacts = [{"email": e.strip(), "name": "", "role": "EXECUTION"} for e in bank_row.emails.split(',') if e.strip()]
                if not contacts:
                    continue
                
                bank_name = bank_row.bank.name if bank_row.bank else 'Bank Partner'
                link = f"{base_url}/public-quotation/{assignment['token']}"
                
                # Collect contacts by role
                all_bank_emails = list(dict.fromkeys(
                    c.get("email", "").strip() for c in contacts if c.get("email")
                ))
                approver_emails = list(dict.fromkeys(
                    c.get("email", "").strip() for c in contacts 
                    if c.get("role") == "APPROVER" and c.get("email")
                ))
                approver_set = {e.lower() for e in approver_emails}
                non_approver_emails = [e for e in all_bank_emails if e.lower() not in approver_set]

                db_assignment = None
                if assignment.get("id"):
                    db_assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == assignment["id"]).first()
                elif assignment.get("token"):
                    db_assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == assignment["token"]).first()

                leg_cfgs = getattr(db_assignment, "leg_configs", []) if db_assignment else []
                if leg_cfgs:
                    has_exec_leg = any((c.quotation_base or "").lower() == "execution" for c in leg_cfgs)
                else:
                    ass_base = (assignment.get("quotation_base") or getattr(rfq, "quotation_base", "") or "Execution").lower()
                    has_exec_leg = ass_base in ("execution", "mixed")
                if db_assignment and getattr(db_assignment, "is_cross_entity", False):
                    has_exec_leg = False

                is_indicative = not has_exec_leg
                has_approver = len(approver_emails) > 0
                has_execution = any(c.get("role") == "EXECUTION" for c in contacts)

                if assignment.get("approval_status") == "PENDING" and not is_indicative and has_approver and has_execution:
                    # Phase 1a: Email APPROVER contacts with review link requiring 2FA OTP verification
                    for app_email in approver_emails:
                        approver_link = f"{base_url}/public-quotation/{assignment['token']}?email={app_email}"
                        from app.services.unified_email_builder import build_quotation_rfq_bank_email
                        subject, body = build_quotation_rfq_bank_email(
                            rfq=rfq,
                            assignment=db_assignment,
                            bank_name=bank_name,
                            customer_branding=entity_display_name,
                            link=approver_link,
                            email_purpose="BANK_APPROVAL_REQUIRED"
                        )
                        background_tasks.add_task(send_email, db, [app_email], subject, body, {}, email_settings)

                    # Phase 1b: Email EXECUTION + VIEW_ONLY contacts with heads-up (NO link) ALL TOGETHER in ONE email
                    if non_approver_emails:
                        from app.services.unified_email_builder import build_quotation_rfq_bank_email
                        subject, body = build_quotation_rfq_bank_email(
                            rfq=rfq,
                            assignment=db_assignment,
                            bank_name=bank_name,
                            customer_branding=entity_display_name,
                            link="",
                            email_purpose="BANK_HEADS_UP"
                        )
                        background_tasks.add_task(send_email, db, non_approver_emails, subject, body, {}, email_settings)
                else:
                    # --- STANDARD FLOW (No bank-level approval needed: Indicative, or No Approver, or Approver without Execution role) ---
                    if db_assignment and db_assignment.approval_status == 'PENDING':
                        db_assignment.approval_status = None
                        db.commit()

                    # Send standard invitation email with portal link to ALL contacts from the same bank ALL TOGETHER in the SAME email
                    if all_bank_emails:
                        from app.services.unified_email_builder import build_quotation_rfq_bank_email
                        subject, body = build_quotation_rfq_bank_email(
                            rfq=rfq,
                            assignment=db_assignment,
                            bank_name=bank_name,
                            customer_branding=entity_display_name,
                            link=link,
                            email_purpose="INVITATION"
                        )
                        background_tasks.add_task(
                            send_email,
                            db,
                            all_bank_emails,
                            subject,
                            body,
                            {},
                            email_settings,
                        )

        
        # Dispatch submission email to Corporate Admins and Common Communication List
        _dispatch_quotation_submission_email(
            db=db,
            background_tasks=background_tasks,
            rfq=rfq,
            current_user=current_user,
            requires_approval=requires_approval,
            request=request,
            is_retender=False
        )

        return {"rfq_id": rfq.id, "ref_no": rfq.ref_no, "assignments": assignments}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

def compute_rfq_standings(rfq: QuotationRequest, db: Session, dispatch_emails: bool = False) -> dict:
    """Institutional RFQ evaluation engine: calculates normalized standings, TVM adjustments, tie-breakers, and winners."""
    now = datetime.now(timezone.utc)

    def _to_utc_dt(dt):
        if not dt:
            return None
        if isinstance(dt, str):
            try:
                from dateutil import parser
                dt = parser.parse(dt)
            except Exception:
                return None
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    w_start = _to_utc_dt(rfq.window_start)
    w_end = _to_utc_dt(rfq.window_end)

    is_scheduled = bool(w_start and now < w_start)
    # The moment window_end is reached, the tender window is closed for execution evaluation
    is_closed = bool(w_end and now >= w_end) and not is_scheduled

    # Resolve Acceptance Timeout and Default Action from Customer Configuration
    try:
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        cfg_key = GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_TBILL if rfq.type == "TBILL" else GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_FX_SPOT
        cfg = crud_customer_configuration.get_customer_config_or_global_fallback(db, rfq.customer_id, cfg_key)
        effective_timeout = int(cfg["effective_value"]) if (cfg and cfg.get("effective_value")) else (120 if rfq.type == "TBILL" else 30)
        if rfq.acceptance_timeout_seconds is None or (rfq.acceptance_status is None and rfq.acceptance_timeout_seconds != effective_timeout):
            rfq.acceptance_timeout_seconds = effective_timeout
    except Exception:
        if rfq.acceptance_timeout_seconds is None:
            rfq.acceptance_timeout_seconds = 120 if rfq.type == "TBILL" else 30

    if rfq.acceptance_timeout_action is None or rfq.acceptance_status is None:
        try:
            from app.crud.crud_config import crud_customer_configuration
            from app.constants import GlobalConfigKey
            cfg_act = crud_customer_configuration.get_customer_config_or_global_fallback(db, rfq.customer_id, GlobalConfigKey.QUOTATION_ACCEPTANCE_DEFAULT_ACTION)
            raw_act = (cfg_act.get("effective_value") if cfg_act else None) or "AUTO_REJECT"
            rfq.acceptance_timeout_action = "AUTO_ACCEPT" if str(raw_act).strip().upper() in ("AUTO_ACCEPT", "ACCEPT", "TRUE", "1") else "AUTO_REJECT"
        except Exception:
            rfq.acceptance_timeout_action = "AUTO_REJECT"

    if w_end and (rfq.acceptance_deadline is None or (rfq.acceptance_status is None and rfq.acceptance_deadline != w_end + timedelta(seconds=rfq.acceptance_timeout_seconds))):
        rfq.acceptance_deadline = w_end + timedelta(seconds=rfq.acceptance_timeout_seconds)

    acc_deadline = _to_utc_dt(rfq.acceptance_deadline) or (w_end + timedelta(seconds=rfq.acceptance_timeout_seconds) if w_end else None)

    q_base_str = (rfq.quotation_base or "Execution").lower()
    is_indicative_only = q_base_str == "indicative" and not getattr(rfq, "legs", [])

    if is_closed:
        if is_indicative_only:
            rfq.status = 'COMPLETED'
            rfq.acceptance_status = 'INDICATIVE_COMPLETED'
            db.commit()
        elif rfq.acceptance_status in ('ACCEPTED', 'AUTO_ACCEPTED'):
            rfq.status = 'COMPLETED'
            db.commit()
        elif rfq.acceptance_status in ('REJECTED', 'AUTO_REJECTED') or rfq.status == 'REJECTED':
            rfq.status = 'REJECTED'
            if not rfq.acceptance_status:
                rfq.acceptance_status = 'REJECTED'
            db.commit()
        else:
            # Acceptance decision pending — will be evaluated after computing standings & uncontested status
            if rfq.status not in ('REJECTED', 'CANCELLED', 'COMPLETED'):
                rfq.status = 'EVALUATING'
            if not rfq.acceptance_status:
                rfq.acceptance_status = 'PENDING'
            db.commit()
        
    assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).all()
    
    # Gate live market benchmark evaluation: only run when live tender bidding or acceptance window is open
    is_live_bidding = bool(not is_closed and not is_scheduled and rfq.status in ('PENDING', 'OPEN', 'EVALUATING'))
    is_acceptance_open = bool(rfq.acceptance_status == 'PENDING' and acc_deadline and now < acc_deadline)
    can_fetch_live_market = bool(is_live_bidding or is_acceptance_open)
    
    results = []
    winner_bank_id = None
    is_inconclusive = False
    inconclusive_reason = None
    best_indicative_rate = None
    best_execution_rate = None
    deviation_percent = None
    has_execution_banks = False
    is_uncontested = False
    uncontested_reason = None
    
    from app.services.tenant_key_service import tenant_key_service
    tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, rfq.customer_id)

    if rfq.type == 'TBILL':
        all_tbill_offers = []
        for a in assignments:
            offers_db = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).all()
            q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
            for o in offers_db:
                all_tbill_offers.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "settlement_date": o.settlement_date,
                    "maturity_date": o.maturity_date,
                    "discount_rate": tenant_key_service.resolve_tbill_discount_rate(o, tenant_dek),
                    "max_amount": o.max_amount,
                    "submitted_at": o.submitted_at
                })

        if not all_tbill_offers:
            for a in assignments:
                q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
                results.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "quotation_bank_id": a.quotation_bank_id,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "offers": [],
                    "best_score": None,
                    "token": a.token,
                    "quotation_base": a.quotation_base or rfq.quotation_base,
                    "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                    "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                    "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                    "approval_status": a.approval_status,
                    "approved_by_email": a.approved_by_email,
                    "approved_at": a.approved_at,
                    "approval_notes": a.approval_notes,
                    "cost_min": a.cost_min or 0.0,
                    "cost_percent": a.cost_percent or 0.0,
                    "cost_max": a.cost_max or 0.0,
                    "cost_flat": a.cost_flat or 0.0
                })
            rfq.winner_bank_name = None
            rfq.winner_rate = None
            rfq.saved_vs_avg = None
            return {
                "rfq": rfq,
                "results": results,
                "winner_bank_id": None,
                "is_inconclusive": is_closed and not is_scheduled,
                "inconclusive_reason": "Quotation window closed without receiving any offers from counterparties." if (is_closed and not is_scheduled) else None,
                "best_indicative_rate": None,
                "best_execution_rate": None,
                "deviation_percent": None,
                "has_execution_banks": True
            }

        # --- T-Bill Normalization Logic ---
        is_buy = (rfq.direction and rfq.direction.lower() == 'buy')
        eval_rate = (rfq.eval_rate or 0) / 100.0

        s_min = None
        m_max = None
        
        parsed_offers = []
        for o in all_tbill_offers:
            try:
                s_dt = datetime.strptime(o['settlement_date'], "%Y-%m-%d")
                m_dt = datetime.strptime(o['maturity_date'], "%Y-%m-%d")
                o['s_dt'] = s_dt
                o['m_dt'] = m_dt
                parsed_offers.append(o)
                
                if s_min is None or s_dt < s_min: s_min = s_dt
                if m_max is None or m_dt > m_max: m_max = m_dt
            except Exception:
                continue

        # Calculate scores
        for o in parsed_offers:
            days = (o['m_dt'] - o['s_dt']).days
            price = 100.0 * (1.0 - (o['discount_rate'] / 100.0) * (days / 360.0))
            
            if is_buy:
                delta_s = (o['s_dt'] - s_min).days
                delta_m = (m_max - o['m_dt']).days
                m_accrual_factor = 1.0 + (eval_rate * (delta_m / 360.0))
                scaled_price = price / m_accrual_factor
                s_discount_factor = 1.0 - (eval_rate * (delta_s / 360.0))
                normalized_price = scaled_price * s_discount_factor
                o['score'] = normalized_price
            else:
                o['score'] = o['discount_rate']

        # Group by bank and take the best offer
        bank_best = {}
        for o in parsed_offers:
            bid = o['bank_id']
            if bid not in bank_best or o['score'] < bank_best[bid]['score']:
                bank_best[bid] = o

        # Format Final Results
        for a in assignments:
            q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
            bank_id = q_bank.bank_id if q_bank else 0
            
            bank_offers = [o for o in parsed_offers if o['bank_id'] == bank_id]
            best_offer = bank_best.get(bank_id)
            
            results.append({
                "bank_id": bank_id,
                "quotation_bank_id": a.quotation_bank_id,
                "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                "bank_emails": q_bank.emails if q_bank else "",
                "offers": bank_offers,
                "best_score": best_offer['score'] if best_offer else None,
                "submitted_by_email": best_offer.get('submitted_by_email') if best_offer else (bank_offers[0].get('submitted_by_email') if bank_offers else None),
                "notes": best_offer.get('notes') if best_offer else (bank_offers[0].get('notes') if bank_offers else None),
                "token": a.token,
                "quotation_base": a.quotation_base or rfq.quotation_base,
                "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                "approval_status": a.approval_status,
                "approved_by_email": a.approved_by_email,
                "approved_at": a.approved_at,
                "approval_notes": a.approval_notes,
                "cost_min": a.cost_min or 0.0,
                "cost_percent": a.cost_percent or 0.0,
                "cost_max": a.cost_max or 0.0,
                "cost_flat": a.cost_flat or 0.0
            })

        results.sort(key=lambda x: (x['best_score'] is None, x['best_score']))
        exec_results = [r for r in results if (r.get('quotation_base') or 'Execution').lower() == 'execution' and not r.get('is_cross_entity')]
        valid_exec_tbills = [r for r in exec_results if r.get('best_score') is not None]
        if len(valid_exec_tbills) == 1:
            is_uncontested = True
            uncontested_reason = "Only 1 bank counterparty provided a quote on this tender. No competing offers were received to establish market spread."
        if exec_results and exec_results[0].get('best_score') is not None:
            winner_bank_id = exec_results[0]['bank_id']

    else:
        # FX_SPOT: Evaluate per currency pair leg
        rfq_legs = rfq.legs if (rfq.legs and len(rfq.legs) > 0) else []

        if not rfq_legs:
            from app.models.models_quotation import QuotationLeg
            virtual_leg = QuotationLeg(
                id=f"{rfq.id}-leg-1",
                rfq_id=rfq.id,
                leg_index=1,
                type=rfq.type or "FX_SPOT",
                direction=rfq.direction or "Buy",
                buy_currency=rfq.buy_currency or "USD",
                sell_currency=rfq.sell_currency or "EGP",
                amount=rfq.amount,
                value_date=rfq.value_date,
                allow_alternative_value_date=rfq.allow_alternative_value_date,
                quotation_base=rfq.quotation_base,
                max_tolerance_percent=rfq.max_tolerance_percent,
                status=rfq.status
            )
            rfq_legs = [virtual_leg]

        legs_data = []

        for leg in rfq_legs:
            leg_dir_str = (leg.direction or rfq.direction or "Buy").lower()
            is_sell = (leg_dir_str == 'sell')
            leg_amount = float((leg.amount if leg.amount is not None else rfq.amount) or 1.0)
            leg_target_val_date = leg.value_date or rfq.value_date
            leg_base = (leg.quotation_base or rfq.quotation_base or 'Execution').lower()
            leg_tol = leg.max_tolerance_percent if leg.max_tolerance_percent is not None else (rfq.max_tolerance_percent or 0.0)

            leg_results = []
            for a in assignments:
                cfg = a.get_config_for_leg(leg.id)
                q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
                if getattr(cfg, 'is_invited', True) is False:
                    leg_results.append({
                        "bank_id": q_bank.bank_id if q_bank else 0,
                        "quotation_bank_id": a.quotation_bank_id,
                        "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                        "bank_emails": q_bank.emails if q_bank else "",
                        "price": None,
                        "finalPrice": None,
                        "normalized_price": None,
                        "assigned_value_date": None,
                        "offered_value_date": None,
                        "allow_alternative_value_date": False,
                        "is_alternative_value_date": False,
                        "is_custom_value_date": False,
                        "time_value_adjustment": 0.0,
                        "notes": None,
                        "submitted_at": None,
                        "submitted_by_email": None,
                        "token": a.token,
                        "quotation_base": a.quotation_base or rfq.quotation_base or "Execution",
                        "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                        "is_excluded": True,
                        "is_invited": False,
                        "is_passed": False,
                        "is_document_visible": False,
                        "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                        "approval_status": a.approval_status,
                        "cost_min": 0.0,
                        "cost_percent": 0.0,
                        "cost_max": 0.0,
                        "cost_flat": 0.0
                    })
                    continue

                assigned_val_date = cfg.value_date or a.value_date or leg_target_val_date
                assigned_base = cfg.quotation_base or a.quotation_base or leg.quotation_base or rfq.quotation_base or "Execution"
                allow_alt_val = cfg.allow_alternative_value_date if cfg.allow_alternative_value_date is not None else (
                    a.allow_alternative_value_date if a.allow_alternative_value_date is not None else (leg.allow_alternative_value_date or False)
                )

                assigned_val_str = str(assigned_val_date).split('T')[0] if assigned_val_date is not None else None
                is_custom_date = bool(cfg.value_date and str(cfg.value_date).split('T')[0] != (str(leg_target_val_date).split('T')[0] if leg_target_val_date else ''))

                is_leg_passed = bool(getattr(cfg, 'is_passed', False)) if cfg else False
                if is_leg_passed:
                    leg_results.append({
                        "bank_id": q_bank.bank_id if q_bank else 0,
                        "quotation_bank_id": a.quotation_bank_id,
                        "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                        "bank_emails": q_bank.emails if q_bank else "",
                        "price": None,
                        "finalPrice": None,
                        "normalized_price": None,
                        "assigned_value_date": assigned_val_str,
                        "offered_value_date": None,
                        "allow_alternative_value_date": allow_alt_val,
                        "is_alternative_value_date": False,
                        "is_custom_value_date": is_custom_date,
                        "time_value_adjustment": 0.0,
                        "notes": None,
                        "submitted_at": None,
                        "submitted_by_email": None,
                        "token": a.token,
                        "quotation_base": assigned_base,
                        "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                        "is_passed": True,
                        "is_document_visible": cfg.is_document_visible if hasattr(cfg, 'is_document_visible') else True,
                        "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                        "approval_status": a.approval_status,
                        "approved_by_email": a.approved_by_email,
                        "approved_at": a.approved_at,
                        "approval_notes": a.approval_notes,
                        "cost_min": cfg.cost_min or 0.0,
                        "cost_percent": cfg.cost_percent or 0.0,
                        "cost_max": cfg.cost_max or 0.0,
                        "cost_flat": cfg.cost_flat or 0.0
                    })
                    continue

                offer_db = db.query(QuotationOffer).filter(
                    QuotationOffer.assignment_id == a.id,
                    (QuotationOffer.leg_id == leg.id) | (QuotationOffer.leg_id.is_(None) if len(rfq_legs) == 1 else False)
                ).order_by(QuotationOffer.submitted_at.desc()).first()

                if not offer_db:
                    leg_results.append({
                        "bank_id": q_bank.bank_id if q_bank else 0,
                        "quotation_bank_id": a.quotation_bank_id,
                        "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                        "bank_emails": q_bank.emails if q_bank else "",
                        "price": None,
                        "finalPrice": None,
                        "normalized_price": None,
                        "assigned_value_date": assigned_val_str,
                        "offered_value_date": None,
                        "allow_alternative_value_date": allow_alt_val,
                        "is_alternative_value_date": False,
                        "is_custom_value_date": is_custom_date,
                        "time_value_adjustment": 0.0,
                        "notes": None,
                        "submitted_at": None,
                        "submitted_by_email": None,
                        "token": a.token,
                        "quotation_base": assigned_base,
                        "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                        "is_passed": False,
                        "is_document_visible": cfg.is_document_visible if hasattr(cfg, 'is_document_visible') else True,
                        "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                        "approval_status": a.approval_status,
                        "approved_by_email": a.approved_by_email,
                        "approved_at": a.approved_at,
                        "approval_notes": a.approval_notes,
                        "cost_min": cfg.cost_min or 0.0,
                        "cost_percent": cfg.cost_percent or 0.0,
                        "cost_max": cfg.cost_max or 0.0,
                        "cost_flat": cfg.cost_flat or 0.0
                    })
                    continue

                price = tenant_key_service.resolve_offer_price(offer_db, tenant_dek)
                base_deal_volume = leg_amount * price
                raw_fee = (base_deal_volume * (float(cfg.cost_percent or 0) / 100.0)) + float(cfg.cost_flat or 0)
                clamped_fee = raw_fee
                if cfg.cost_min and cfg.cost_min > 0:
                    clamped_fee = max(clamped_fee, float(cfg.cost_min))
                if cfg.cost_max and cfg.cost_max > 0:
                    clamped_fee = min(clamped_fee, float(cfg.cost_max))

                fee_per_unit = clamped_fee / leg_amount if leg_amount > 0 else 0.0
                final_all_in_price = round((price - fee_per_unit) if is_sell else (price + fee_per_unit), 5)

                effective_val_date = offer_db.offered_value_date or assigned_val_date or leg_target_val_date
                normalized_price = final_all_in_price
                tvm_adjustment = 0.0
                is_alt_date = False

                if leg_target_val_date and effective_val_date:
                    try:
                        target_dt = datetime.strptime(str(leg_target_val_date).split('T')[0], "%Y-%m-%d").date()
                        offered_dt = datetime.strptime(str(effective_val_date).split('T')[0], "%Y-%m-%d").date()
                        delta_days = (offered_dt - target_dt).days
                        if delta_days != 0:
                            is_alt_date = True
                            r_eval = (rfq.eval_rate or 20.25) / 100.0
                            normalized_price = round(final_all_in_price * (1.0 - (r_eval * (delta_days / 365.0))), 5)
                            tvm_adjustment = round(normalized_price - final_all_in_price, 5)
                    except Exception as tvm_err:
                        logger.warning(f"Error computing TVM adjustment: {tvm_err}")

                offered_val_str = str(effective_val_date).split('T')[0] if effective_val_date is not None else None

                leg_results.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "quotation_bank_id": a.quotation_bank_id,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "price": price,
                    "finalPrice": final_all_in_price,
                    "normalized_price": normalized_price,
                    "assigned_value_date": assigned_val_str,
                    "offered_value_date": offered_val_str,
                    "allow_alternative_value_date": allow_alt_val,
                    "is_alternative_value_date": is_alt_date,
                    "is_custom_value_date": is_custom_date,
                    "time_value_adjustment": tvm_adjustment,
                    "bank_fee_total": clamped_fee,
                    "fee_per_unit": fee_per_unit,
                    "notes": offer_db.notes,
                    "submitted_at": offer_db.submitted_at,
                    "submitted_by_email": offer_db.submitted_by_email,
                    "token": a.token,
                    "quotation_base": assigned_base,
                    "is_cross_entity": bool(getattr(a, 'is_cross_entity', False)),
                    "is_passed": False,
                    "is_document_visible": cfg.is_document_visible if hasattr(cfg, 'is_document_visible') else True,
                    "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                    "approval_status": a.approval_status,
                    "approved_by_email": a.approved_by_email,
                    "approved_at": a.approved_at,
                    "approval_notes": a.approval_notes,
                    "cost_min": cfg.cost_min or 0.0,
                    "cost_percent": cfg.cost_percent or 0.0,
                    "cost_max": cfg.cost_max or 0.0,
                    "cost_flat": cfg.cost_flat or 0.0
                })

            valid_leg_results = [r for r in leg_results if r.get('finalPrice') is not None]

            def _sort_ts(r):
                ts = r.get('submitted_at')
                if ts and hasattr(ts, 'timestamp'):
                    return ts.timestamp()
                return float('inf')

            if is_sell:
                valid_leg_results.sort(key=lambda x: (
                    -(x.get('normalized_price') if x.get('normalized_price') is not None else x['finalPrice']),
                    _sort_ts(x)
                ))
            else:
                valid_leg_results.sort(key=lambda x: (
                    (x.get('normalized_price') if x.get('normalized_price') is not None else x['finalPrice']),
                    _sort_ts(x)
                ))
            leg_results = valid_leg_results + [r for r in leg_results if r.get('finalPrice') is None]

            leg_has_execution = any((r.get('quotation_base') or 'Execution').lower() == 'execution' and not r.get('is_cross_entity') for r in leg_results)
            leg_winner_bank_id = None
            leg_is_inconclusive = False
            leg_inconclusive_reason = None
            leg_best_indicative = None
            leg_best_execution = None
            leg_deviation_pct = None
            execution_bids = []
            indicative_bids = []

            if not valid_leg_results:
                if is_closed and not is_scheduled:
                    leg_is_inconclusive = True
                    leg_inconclusive_reason = "Quotation window closed without receiving any quotes for this currency pair."
            elif not leg_has_execution:
                leg_is_inconclusive = True
                leg_inconclusive_reason = "All counterparties were requested on an Indicative basis for this currency pair."
            else:
                indicative_bids = [r for r in valid_leg_results if (r.get('quotation_base') or 'Execution').lower() == 'indicative' or r.get('is_cross_entity')]
                execution_bids = [r for r in valid_leg_results if (r.get('quotation_base') or 'Execution').lower() == 'execution' and not r.get('is_cross_entity')]

                if not execution_bids and is_closed:
                    leg_is_inconclusive = True
                    leg_inconclusive_reason = "No Execution quotes were submitted before the window closed for this currency pair."

                if indicative_bids:
                    leg_best_indicative = indicative_bids[0].get('normalized_price') or indicative_bids[0]['finalPrice']
                if execution_bids:
                    leg_best_execution = execution_bids[0].get('normalized_price') or execution_bids[0]['finalPrice']

                if execution_bids:
                    best_exec_item = execution_bids[0]
                    if leg_best_indicative is not None and leg_best_execution is not None:
                        if is_sell:
                            leg_deviation_pct = ((leg_best_indicative - leg_best_execution) / leg_best_indicative) * 100.0
                        else:
                            leg_deviation_pct = ((leg_best_execution - leg_best_indicative) / leg_best_indicative) * 100.0

                        if leg_deviation_pct <= 0:
                            leg_winner_bank_id = best_exec_item['bank_id']
                        else:
                            if leg_deviation_pct > leg_tol:
                                leg_is_inconclusive = True
                                leg_inconclusive_reason = (
                                    f"The best Execution rate ({leg_best_execution:.4f}) exceeded the Indicative benchmark "
                                    f"({leg_best_indicative:.4f}) by {leg_deviation_pct:.2f}%, which is higher than the allowed tolerance of {leg_tol:.2f}%."
                                )
                            else:
                                leg_winner_bank_id = best_exec_item['bank_id']
                    else:
                        leg_winner_bank_id = best_exec_item['bank_id']

            leg_savings_summary = None
            if leg_winner_bank_id and not leg_is_inconclusive and valid_leg_results:
                winner_res = next((r for r in valid_leg_results if r['bank_id'] == leg_winner_bank_id), None)
                if winner_res:
                    rates = [r['finalPrice'] for r in valid_leg_results if r.get('finalPrice') is not None]
                    if len(rates) >= 1:
                        win_rate = winner_res.get('finalPrice') or rates[0]
                        avg_rate = sum(rates) / len(rates)
                        worst_rate = max(rates) if not is_sell else min(rates)
                        if not is_sell:
                            s_vs_avg = max(0.0, (avg_rate - win_rate) * leg_amount)
                            s_vs_worst = max(0.0, (worst_rate - win_rate) * leg_amount)
                        else:
                            s_vs_avg = max(0.0, (win_rate - avg_rate) * leg_amount)
                            s_vs_worst = max(0.0, (win_rate - worst_rate) * leg_amount)

                        leg_savings_summary = {
                            "winner_bank_name": winner_res.get('bank_name'),
                            "winner_rate": round(win_rate, 4),
                            "avg_rate": round(avg_rate, 4),
                            "worst_rate": round(worst_rate, 4),
                            "currency": leg.sell_currency,
                            "saved_vs_avg": round(s_vs_avg, 2),
                            "saved_vs_worst": round(s_vs_worst, 2),
                            "total_quotes": len(rates)
                        }

            leg_status_val = getattr(leg, 'status', None)
            is_leg_declined = leg_status_val in ('REJECTED', 'CANCELLED')
            if is_leg_declined:
                leg_winner_bank_id = None
                leg_savings_summary = None

            if hasattr(leg, 'id') and leg_savings_summary and not leg_is_inconclusive and not is_leg_declined:
                leg.winner_bank_name = leg_savings_summary.get("winner_bank_name")
                leg.winner_bank_id = leg_winner_bank_id
                tenant_key_service.apply_encrypted_leg_winner(
                    leg,
                    win_rate=leg_savings_summary.get("winner_rate"),
                    saved_vs_avg=leg_savings_summary.get("saved_vs_avg"),
                    dek=tenant_dek
                )
            elif hasattr(leg, 'id'):
                leg.winner_bank_name = None
                leg.winner_bank_id = None
                tenant_key_service.apply_encrypted_leg_winner(leg, None, None, tenant_dek)

            if is_closed and hasattr(leg, 'id'):
                if leg_is_inconclusive:
                    if leg.status not in ('ACCEPTED', 'REJECTED', 'CANCELLED'):
                        leg.status = 'INCONCLUSIVE'
                elif leg.status in ('PENDING', 'PENDING_APPROVAL', 'EVALUATING', 'APPROVED_SCHEDULED'):
                    leg.status = 'COMPLETED'

            leg_is_uncontested = bool(len(execution_bids) == 1)
            leg_uncontested_reason = (
                "Only 1 bank counterparty provided a quote on this currency pair. No competing offers were received to establish market spread."
                if leg_is_uncontested else None
            )

            # Phase 6.6: Use Frozen Snapshot if deal accepted, else live benchmark
            leg_market_bm = leg.market_benchmark_snapshot if getattr(leg, 'market_benchmark_snapshot', None) else None
            if not leg_market_bm and can_fetch_live_market and (getattr(leg, 'type', None) or rfq.type) == 'FX_SPOT' and leg.buy_currency and leg.sell_currency:
                try:
                    from app.services.live_market_service import live_market_service
                    leg_market_bm = live_market_service.get_empirical_reference(
                        db,
                        customer_id=rfq.customer_id,
                        from_code=leg.buy_currency,
                        to_code=leg.sell_currency,
                        direction=leg.direction or rfq.direction or 'Buy',
                        amount=leg.amount
                    )
                    win_rate_val = leg_savings_summary.get("winner_rate") if leg_savings_summary else None
                    if win_rate_val and leg_market_bm:
                        leg_market_bm["quote_evaluation"] = live_market_service.evaluate_quote_spread(
                            float(win_rate_val),
                            leg_market_bm,
                            direction=leg.direction or rfq.direction or 'Buy'
                        )
                except Exception as bm_err:
                    logger.warning(f"Error computing live market benchmark for leg {leg.id}: {bm_err}")

            legs_data.append({
                "leg_id": leg.id,
                "leg_index": leg.leg_index,
                "type": leg.type or "FX_SPOT",
                "direction": leg.direction or "Buy",
                "buy_currency": leg.buy_currency,
                "sell_currency": leg.sell_currency,
                "pair_name": f"{leg.buy_currency}/{leg.sell_currency}",
                "currency_pair": f"{leg.buy_currency}/{leg.sell_currency}",
                "amount": leg.amount,
                "value_date": str(leg.value_date).split('T')[0] if leg.value_date else None,
                "quotation_base": leg.quotation_base or "Execution",
                "max_tolerance_percent": leg.max_tolerance_percent,
                "status": leg.status,
                "results": leg_results,
                "ladder": leg_results,
                "winner_bank_id": leg_winner_bank_id,
                "winner_bank_name": leg_savings_summary.get("winner_bank_name") if leg_savings_summary else None,
                "winner_rate": leg_savings_summary.get("winner_rate") if leg_savings_summary else None,
                "saved_vs_avg": leg_savings_summary.get("saved_vs_avg") if leg_savings_summary else None,
                "savings_amount": leg_savings_summary.get("saved_vs_avg", 0.0) if leg_savings_summary else 0.0,
                "savings_percent": round((leg_savings_summary.get("saved_vs_avg", 0.0) / (leg.amount * leg_savings_summary.get("avg_rate", 1.0))) * 100, 2) if (leg_savings_summary and leg.amount and leg_savings_summary.get("avg_rate")) else 0.0,
                "is_inconclusive": leg_is_inconclusive,
                "inconclusive_reason": leg_inconclusive_reason,
                "is_uncontested": leg_is_uncontested,
                "uncontested_reason": leg_uncontested_reason,
                "best_indicative_rate": leg_best_indicative,
                "best_execution_rate": leg_best_execution,
                "deviation_percent": leg_deviation_pct,
                "has_execution_banks": leg_has_execution,
                "savings_summary": leg_savings_summary,
                "market_benchmark": leg_market_bm
            })

    # --- Live Trading Floor Presence Telemetry ---
    total_invited = len(assignments)
    desks_active = 0
    quotes_locked = 0
    approvals_pending = 0
    approvals_cleared = 0

    for a in assignments:
        if a.approval_status == 'PENDING':
            approvals_pending += 1
        elif a.approval_status == 'APPROVED':
            approvals_cleared += 1

        otp_exists = db.query(QuotationAccessOTP).filter(QuotationAccessOTP.assignment_id == a.id).first()
        if otp_exists:
            desks_active += 1

        if rfq.type == 'TBILL':
            has_q = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).first()
        else:
            has_q = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).first()
        if has_q:
            quotes_locked += 1

    live_telemetry = {
        "total_invited": total_invited,
        "desks_active": desks_active,
        "quotes_locked": quotes_locked,
        "approvals_pending": approvals_pending,
        "approvals_cleared": approvals_cleared,
        "summary_text": f"{desks_active} of {total_invited} Desks Active • {quotes_locked} Quote{'s' if quotes_locked != 1 else ''} Locked In"
    }

    # --- Savings Summary ---
    savings_summary = None
    if winner_bank_id and not is_inconclusive:
        winner_res = next((r for r in results if r['bank_id'] == winner_bank_id), None)
        if winner_res:
            if rfq.type == 'FX_SPOT' and valid_results:
                rates = [r['finalPrice'] for r in valid_results if r.get('finalPrice') is not None]
                if len(rates) >= 1:
                    win_rate = winner_res.get('finalPrice') or rates[0]
                    avg_rate = sum(rates) / len(rates)
                    worst_rate = max(rates) if not is_sell else min(rates)
                    amount = float(rfq.amount or 1.0)
                    
                    if not is_sell:
                        saved_vs_avg = max(0.0, (avg_rate - win_rate) * amount)
                        saved_vs_worst = max(0.0, (worst_rate - win_rate) * amount)
                    else:
                        saved_vs_avg = max(0.0, (win_rate - avg_rate) * amount)
                        saved_vs_worst = max(0.0, (win_rate - worst_rate) * amount)

                    savings_summary = {
                        "winner_bank_name": winner_res.get('bank_name'),
                        "winner_rate": round(win_rate, 4),
                        "avg_rate": round(avg_rate, 4),
                        "worst_rate": round(worst_rate, 4),
                        "currency": rfq.sell_currency,
                        "saved_vs_avg": round(saved_vs_avg, 2),
                        "saved_vs_worst": round(saved_vs_worst, 2),
                        "total_quotes": len(rates)
                    }
            elif rfq.type == 'TBILL' and valid_results:
                scores = [r['best_score'] for r in valid_results if r.get('best_score') is not None]
                if len(scores) >= 1:
                    win_score = winner_res.get('best_score') or scores[0]
                    avg_score = sum(scores) / len(scores)
                    savings_summary = {
                        "winner_bank_name": winner_res.get('bank_name'),
                        "winner_rate": round(win_score, 4),
                        "avg_rate": round(avg_score, 4),
                        "worst_rate": round(max(scores) if is_buy else min(scores), 4),
                        "currency": "EGP",
                        "saved_vs_avg": round(abs(avg_score - win_score) * 1000, 2),
                        "saved_vs_worst": round(abs(max(scores) - min(scores)) * 1000, 2),
                        "total_quotes": len(scores)
                    }

    # Backward compatibility: populate root fields from primary leg if multi-pair
    primary_leg = legs_data[0] if legs_data else None
    if primary_leg:
        results = primary_leg["results"]
        winner_bank_id = primary_leg["winner_bank_id"]
        is_inconclusive = all(l["is_inconclusive"] for l in legs_data)
        inconclusive_reason = primary_leg["inconclusive_reason"]
        best_indicative_rate = primary_leg["best_indicative_rate"]
        best_execution_rate = primary_leg["best_execution_rate"]
        deviation_percent = primary_leg["deviation_percent"]
        has_execution_banks = any(l["has_execution_banks"] for l in legs_data)
        savings_summary = primary_leg["savings_summary"]
        is_uncontested = any(l.get("is_uncontested", False) for l in legs_data)
        uncontested_reason = primary_leg.get("uncontested_reason")

    # Attach winner and rate attributes to RFQ object
    if savings_summary and not is_inconclusive:
        rfq.winner_bank_name = savings_summary.get("winner_bank_name")
        tenant_key_service.apply_encrypted_rfq_winner(
            rfq,
            win_rate=savings_summary.get("winner_rate"),
            saved_vs_avg=savings_summary.get("saved_vs_avg"),
            dek=tenant_dek
        )
    else:
        rfq.winner_bank_name = None
        tenant_key_service.apply_encrypted_rfq_winner(rfq, None, None, tenant_dek)

    # Phase 6.4: Acceptance Window Expiration & Governance Engine
    if is_closed and not is_indicative_only:
        if rfq.acceptance_status in ('ACCEPTED', 'AUTO_ACCEPTED'):
            rfq.status = 'COMPLETED'
        elif rfq.acceptance_status in ('REJECTED', 'AUTO_REJECTED') or rfq.status == 'REJECTED':
            rfq.status = 'REJECTED'
            if not rfq.acceptance_status:
                rfq.acceptance_status = 'REJECTED'
        elif not is_inconclusive:
            # Acceptance decision is still pending. Check if acceptance window has expired!
            if acc_deadline and now > acc_deadline:
                from app.crud.crud import log_action
                if rfq.acceptance_timeout_action == "AUTO_ACCEPT":
                    # Check AUTO_ACCEPT_SINGLE_QUOTE governance guardrail!
                    cfg_single = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, rfq.customer_id, GlobalConfigKey.AUTO_ACCEPT_SINGLE_QUOTE
                    )
                    allow_single_auto_accept = (
                        str(cfg_single.get("effective_value") if cfg_single else "false").strip().lower() in ("true", "1")
                    )

                    if is_uncontested and not allow_single_auto_accept:
                        # HALT automated execution for uncontested single quote!
                        # Governance policy requires manual corporate treasury approval
                        rfq.acceptance_status = 'PENDING'
                        rfq.admin_revision_notes = (
                            "Automated execution halted: sole-source uncontested quote received. "
                            "Corporate treasury manual approval required by governance policy."
                        )
                        log_action(
                            db=db,
                            user_id=None,
                            action_type="QUOTATION_AUTO_ACCEPT_HALTED_SINGLE_QUOTE",
                            entity_type="QuotationRequest",
                            entity_id=rfq.id,
                            details={
                                "rfq_id": rfq.id,
                                "ref_no": rfq.ref_no,
                                "reason": "Automated execution halted for uncontested single quote. Corporate treasury manual approval required."
                            },
                            customer_id=rfq.customer_id
                        )
                    else:
                        # Competitive quotes OR customer has explicitly consented to auto-accepting single quote!
                        rfq.status = 'COMPLETED'
                        rfq.acceptance_status = 'AUTO_ACCEPTED'
                        rfq.acceptance_resolved_at = now
                        for leg in (rfq.legs or []):
                            if leg.winner_bank_id and leg.status not in ('REJECTED', 'CANCELLED'):
                                leg.status = 'ACCEPTED'
                            elif leg.status in ('PENDING', 'PENDING_APPROVAL', 'EVALUATING', 'APPROVED_SCHEDULED'):
                                leg.status = 'INCONCLUSIVE' if not leg.winner_bank_id else 'COMPLETED'
                        log_action(
                            db=db,
                            user_id=None,
                            action_type="QUOTATION_DEAL_AUTO_ACCEPTED",
                            entity_type="QuotationRequest",
                            entity_id=rfq.id,
                            details={
                                "rfq_id": rfq.id,
                                "ref_no": rfq.ref_no,
                                "is_uncontested": is_uncontested,
                                "reason": "Acceptance window expired with policy AUTO_ACCEPT"
                            },
                            customer_id=rfq.customer_id
                        )
                else:
                    # Policy is AUTO_REJECT (user's configuration)
                    rfq.status = 'REJECTED'
                    rfq.acceptance_status = 'AUTO_REJECTED'
                    rfq.admin_revision_notes = "Quotation auto-rejected: corporate acceptance window expired with default action AUTO_REJECT."
                    rfq.acceptance_resolved_at = now
                    for leg in (rfq.legs or []):
                        leg.status = 'REJECTED'
                        leg.rejection_reason = "Auto-rejected on acceptance timeout"
                    log_action(
                        db=db,
                        user_id=None,
                        action_type="QUOTATION_DEAL_AUTO_REJECTED",
                        entity_type="QuotationRequest",
                        entity_id=rfq.id,
                        details={"rfq_id": rfq.id, "ref_no": rfq.ref_no, "reason": "Acceptance window expired with policy AUTO_REJECT"},
                        customer_id=rfq.customer_id
                    )
                    trigger_auto_dispatch_results(rfq.id)
            else:
                # Acceptance window is still active! Awaiting Corporate Admin manual decision
                rfq.acceptance_status = 'PENDING'
                if rfq.status not in ('REJECTED', 'CANCELLED'):
                    rfq.status = 'EVALUATING'

    # Auto-dispatch result emails for automated deal confirmations OR auto-rejections
    is_auto_deal_concluded = (rfq.acceptance_status in ('AUTO_ACCEPTED', 'AUTO_REJECTED')) or (rfq.status == 'COMPLETED' and is_indicative_only)
    if dispatch_emails and is_closed and is_auto_deal_concluded and has_execution_banks:
        trigger_auto_dispatch_results(rfq.id)

    db.commit()

    # Phase 6.6: Derive Root Market Benchmark for Single-Pair / Overall RFQ (Frozen Snapshot or Live)
    root_market_bm = rfq.market_benchmark_snapshot if getattr(rfq, 'market_benchmark_snapshot', None) else None
    if not root_market_bm:
        root_market_bm = primary_leg.get("market_benchmark") if primary_leg else None
    if not root_market_bm and can_fetch_live_market and rfq.type == 'FX_SPOT' and rfq.buy_currency and rfq.sell_currency:
        try:
            from app.services.live_market_service import live_market_service
            root_market_bm = live_market_service.get_empirical_reference(
                db,
                customer_id=rfq.customer_id,
                from_code=rfq.buy_currency,
                to_code=rfq.sell_currency,
                direction=rfq.direction or 'Buy',
                amount=rfq.amount
            )
            if savings_summary and savings_summary.get("winner_rate") and root_market_bm:
                root_market_bm["quote_evaluation"] = live_market_service.evaluate_quote_spread(
                    float(savings_summary["winner_rate"]),
                    root_market_bm,
                    direction=rfq.direction or 'Buy'
                )
        except Exception as root_bm_err:
            logger.warning(f"Error computing root market benchmark: {root_bm_err}")

    return {
        "rfq": rfq,
        "legs": legs_data,
        "results": results,
        "winner_bank_id": winner_bank_id,
        "is_inconclusive": is_inconclusive,
        "inconclusive_reason": inconclusive_reason,
        "best_indicative_rate": best_indicative_rate,
        "best_execution_rate": best_execution_rate,
        "deviation_percent": deviation_percent,
        "has_execution_banks": has_execution_banks,
        "is_uncontested": is_uncontested,
        "uncontested_reason": uncontested_reason,
        "live_telemetry": live_telemetry,
        "savings_summary": savings_summary,
        "market_benchmark": root_market_bm
    }

@router.get("/", response_model=List[QuotationRequestOut])
def get_rfq_history(
    entity_id: int = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns the history of quotations for this customer, filtered by user entity access."""
    from app.models.models import User
    user = db.query(User).filter(User.id == current_user.user_id).first()
    is_admin = current_user.role in ["corporate_admin", "super_admin"]
    has_all = getattr(user, "has_all_entity_access", False) if user else False

    if is_admin or has_all:
        allowed_entity_ids = [entity_id] if entity_id else None
    else:
        user_ids = [assoc.customer_entity_id for assoc in user.entity_associations] if user else []
        if entity_id:
            allowed_entity_ids = [entity_id] if entity_id in user_ids else []
        else:
            allowed_entity_ids = user_ids

    reqs = crud_quotation.get_requests(db, customer_id=current_user.customer_id, allowed_entity_ids=allowed_entity_ids, user_id=current_user.user_id)
    now = datetime.now(timezone.utc)
    changed = False
    
    # 1. Batch fetch parent RFQ references & re-tender counts in 2 queries instead of 2 * N queries
    req_ids = [r.id for r in reqs]
    parent_ids = list({r.parent_rfq_id for r in reqs if r.parent_rfq_id})

    parent_map = {}
    if parent_ids:
        parent_rows = db.query(QuotationRequest.id, QuotationRequest.ref_no).filter(QuotationRequest.id.in_(parent_ids)).all()
        parent_map = {row[0]: row[1] for row in parent_rows}

    retender_counts = {}
    if req_ids:
        retender_rows = db.query(QuotationRequest.parent_rfq_id, func.count(QuotationRequest.id)).filter(
            QuotationRequest.parent_rfq_id.in_(req_ids)
        ).group_by(QuotationRequest.parent_rfq_id).all()
        retender_counts = {row[0]: row[1] for row in retender_rows}

    # 2. Batch fetch approval audit logs in 1 single query instead of 2 * N queries
    approval_map = {}
    if reqs and current_user.customer_id:
        from app.models.models import AuditLog
        from sqlalchemy.orm import joinedload
        audit_logs = db.query(AuditLog).options(joinedload(AuditLog.user)).filter(
            AuditLog.action_type.in_(["QUOTATION_RFQ_APPROVED", "QUOTATION_RFQ_APPROVED_SCHEDULED"]),
            AuditLog.customer_id == current_user.customer_id
        ).order_by(AuditLog.id.desc()).all()
        for al in audit_logs:
            det = al.details or {}
            rfq_key = str(det.get("rfq_id"))
            if rfq_key and rfq_key not in approval_map:
                u = al.user
                approver_name = None
                if u:
                    f_name = getattr(u, 'first_name', '') or ''
                    l_name = getattr(u, 'last_name', '') or ''
                    approver_name = f"{f_name} {l_name}".strip() or u.email
                approver_email = det.get("approved_by_email") or (u.email if u else None)
                approval_map[rfq_key] = (approver_name, approver_email)

    for r in reqs:
        try:
            w_end_val = r.window_end
            if w_end_val and w_end_val.tzinfo is None:
                w_end_val = w_end_val.replace(tzinfo=timezone.utc)
            is_closed = bool(w_end_val and now > (w_end_val + timedelta(seconds=2)))
        except Exception:
            is_closed = False
            
        status_changed = False
        if is_closed:
            if r.status in ('PENDING', 'OPEN'):
                r.status = 'COMPLETED'
                changed = True
                status_changed = True
                for leg in (r.legs or []):
                    if leg.status not in ('ACCEPTED', 'REJECTED', 'CANCELLED', 'INCONCLUSIVE', 'COMPLETED'):
                        leg.status = 'INCONCLUSIVE' if not leg.winner_bank_id else 'COMPLETED'
            elif r.status == 'PENDING_APPROVAL':
                r.status = 'REJECTED'
                changed = True
                status_changed = True
                for leg in (r.legs or []):
                    if leg.status not in ('REJECTED', 'CANCELLED'):
                        leg.status = 'REJECTED'
            elif r.status == 'CANCEL_REQUESTED':
                r.status = 'CANCELLED'
                changed = True
                status_changed = True
                for leg in (r.legs or []):
                    if leg.status not in ('CANCELLED',):
                        leg.status = 'CANCELLED'
            
            # Auto-expire any bank-level approvals that were still PENDING when window closed
            if status_changed:
                pending_assignments = db.query(QuotationBankAssignment).filter(
                    QuotationBankAssignment.rfq_id == r.id,
                    QuotationBankAssignment.approval_status == 'PENDING'
                ).all()
                for pa in pending_assignments:
                    pa.approval_status = 'EXPIRED'
        
        # Attach winner and rate data:
        # For concluded RFQs, winner info is already persisted on QuotationLeg! Read directly.
        if hasattr(r, 'legs') and r.legs:
            winning_legs = [l for l in r.legs if l.winner_bank_name is not None]
            if len(r.legs) > 1:
                if len(winning_legs) == len(r.legs) and len(set(l.winner_bank_name for l in winning_legs)) == 1:
                    r.winner_bank_name = winning_legs[0].winner_bank_name
                    r.winner_rate = None
                    r.saved_vs_avg = sum(l.saved_vs_avg or 0.0 for l in r.legs)
                elif winning_legs:
                    unique_winners = list(set(l.winner_bank_name for l in winning_legs))
                    if len(unique_winners) == 1:
                        r.winner_bank_name = f"{unique_winners[0]} ({len(winning_legs)}/{len(r.legs)} Legs)"
                    else:
                        r.winner_bank_name = f"Split Award ({len(winning_legs)}/{len(r.legs)} Legs)"
                    r.winner_rate = None
                    r.saved_vs_avg = sum(l.saved_vs_avg or 0.0 for l in r.legs)
                else:
                    r.winner_bank_name = None
                    r.winner_rate = None
                    r.saved_vs_avg = None
            else:
                first_leg = r.legs[0]
                r.winner_bank_name = first_leg.winner_bank_name
                r.winner_rate = first_leg.winner_rate
                r.saved_vs_avg = first_leg.saved_vs_avg
        elif is_closed and r.status in ('PENDING', 'OPEN') and r.acceptance_status is None:
            # Deal tender window closed within the last few seconds: finalize standings
            compute_rfq_standings(r, db, dispatch_emails=False)
        else:
            r.winner_bank_name = getattr(r, 'winner_bank_name', None)
            r.winner_rate = getattr(r, 'winner_rate', None)
            r.saved_vs_avg = getattr(r, 'saved_vs_avg', None)
            
        r.parent_rfq_ref = parent_map.get(r.parent_rfq_id)
        r.re_tender_count = retender_counts.get(r.id, 0)
        app_name, app_email = approval_map.get(str(r.id), (None, None))
        r.approved_by_name = app_name
        r.approved_by_email = app_email
            
    if changed:
        db.commit()
        
    return reqs

@router.get("/market-benchmarks")
def get_rfq_market_benchmarks(
    currency_pair: str = "USD/EGP",
    trade_type: str = "FX_SPOT",
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns K-anonymity privacy-protected market spread benchmarks (0% confidentiality risk)."""
    from app.services.quotation_benchmark_service import get_market_benchmarks
    return get_market_benchmarks(db, currency_pair=currency_pair, trade_type=trade_type, k_threshold=3)

@router.get("/timing-recommendations")
def get_rfq_timing_recommendations(
    trade_type: str = "FX_SPOT",
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns optimal liquidity timing windows across historical tenders."""
    from app.services.quotation_benchmark_service import get_timing_recommendations
    return get_timing_recommendations(db, trade_type=trade_type)

@router.post("/{rfq_id}/request-cancellation")
def request_rfq_cancellation(
    rfq_id: str,
    payload: QuotationCancellationRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Allows the End User (Maker) to request cancellation of an active or pending quotation.
    - If PENDING_APPROVAL: Can be immediately cancelled (zero counterparty bank exposure).
    - If PENDING: Allowed only if current time is before the cutoff window (default 15 minutes before window_start).
      Transitions status to CANCEL_REQUESTED for Corporate Admin review.
    - If window is open, evaluating, completed, or already cancelled: Rejected.
    """
    rfq = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    if rfq.status in ('CANCELLED', 'REJECTED', 'COMPLETED'):
        raise HTTPException(status_code=400, detail=f"Cannot cancel quotation with status {rfq.status}.")

    if rfq.status == 'CANCEL_REQUESTED':
        raise HTTPException(status_code=400, detail="A cancellation request is already pending corporate admin approval.")

    now = datetime.now(timezone.utc)
    w_start = rfq.window_start
    if w_start and w_start.tzinfo is None:
        w_start = w_start.replace(tzinfo=timezone.utc)

    # Case 1: PENDING_APPROVAL or APPROVED_SCHEDULED - Internal draft or unsent schedule only, never sent to banks! Immediate cancellation.
    if rfq.status in ('PENDING_APPROVAL', 'APPROVED_SCHEDULED'):
        rfq.status = 'CANCELLED'
        rfq.cancellation_reason = payload.reason
        rfq.cancellation_notes = payload.notes
        rfq.cancellation_requested_by = current_user.user_id
        rfq.cancellation_requested_at = now
        rfq.cancelled_at = now
        rfq.scheduled_release_at = None
        rfq.scheduled_release_job_id = None
        for leg in (rfq.legs or []):
            if leg.status not in ('CANCELLED',):
                leg.status = 'CANCELLED'
        db.commit()

        # Cancel scheduled release if registered
        try:
            from app.services.quotation_release_scheduler import cancel_scheduled_rfq_release
            cancel_scheduled_rfq_release(rfq_id=rfq.id)
        except Exception as rel_err:
            logger.warning(f"Failed to cancel scheduled release for RFQ {rfq.id}: {rel_err}")

        # Cancel scheduled 15m reminder if registered
        try:
            from app.services.quotation_reminder_service import cancel_rfq_15m_reminder
            cancel_rfq_15m_reminder(rfq_id=rfq.id)
        except Exception as rem_err:
            logger.warning(f"Failed to cancel 15m reminder for RFQ {rfq.id}: {rem_err}")

        log_action(
            db=db,
            user_id=current_user.user_id,
            action_type="QUOTATION_CANCELLED_INTERNAL",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": rfq.id,
                "ref_no": rfq.ref_no,
                "reason": payload.reason,
                "notes": payload.notes,
                "message": f"Draft/scheduled RFQ {rfq.ref_no} cancelled prior to bank dispatch."
            },
            customer_id=current_user.customer_id
        )
        return {"message": "Quotation cancelled successfully before bank dispatch.", "status": "CANCELLED", "rfq_id": rfq.id}

    # Case 2: PENDING (Released to banks, scheduled to open in the future)
    if rfq.status == 'PENDING':
        # Retrieve cutoff limit (default 15 minutes)
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        cutoff_val = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.QUOTATION_CANCELLATION_CUTOFF_MINUTES
        )
        cutoff_minutes = 15
        try:
            if cutoff_val is not None:
                cutoff_minutes = int(float(str(cutoff_val)))
        except (ValueError, TypeError):
            cutoff_minutes = 15

        if w_start:
            seconds_remaining = (w_start - now).total_seconds()
            if seconds_remaining < (cutoff_minutes * 60):
                raise HTTPException(
                    status_code=400,
                    detail=f"Quotation cannot be cancelled within {cutoff_minutes} minutes of the bidding window opening."
                )

        rfq.status = 'CANCEL_REQUESTED'
        rfq.cancellation_reason = payload.reason
        rfq.cancellation_notes = payload.notes
        rfq.cancellation_requested_by = current_user.user_id
        rfq.cancellation_requested_at = now
        db.commit()


        log_action(
            db=db,
            user_id=current_user.user_id,
            action_type="QUOTATION_CANCELLATION_REQUESTED",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": rfq.id,
                "ref_no": rfq.ref_no,
                "reason": payload.reason,
                "notes": payload.notes,
                "message": f"Cancellation requested for RFQ {rfq.ref_no}."
            },
            customer_id=current_user.customer_id
        )
        return {
            "message": "Cancellation request submitted for Corporate Admin approval.",
            "status": "CANCEL_REQUESTED",
            "rfq_id": rfq.id
        }

    # For any other status (OPEN, EVALUATING, etc.)
    raise HTTPException(status_code=400, detail=f"Cannot cancel quotation while in status {rfq.status}.")


@router.post("/{rfq_id}/reschedule-release")
def reschedule_rfq_release_maker(
    rfq_id: str,
    payload: QuotationRescheduleRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Allows Maker or Admin to reschedule or immediately release an RFQ in APPROVED_SCHEDULED status."""
    rfq = crud_quotation.get_request(db, rfq_id=rfq_id, customer_id=current_user.customer_id)
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    if rfq.status != 'APPROVED_SCHEDULED' or rfq.is_dispatched:
        raise HTTPException(status_code=400, detail="Only scheduled quotations awaiting dispatch can be rescheduled.")

    from app.services.quotation_release_scheduler import (
        cancel_scheduled_rfq_release, schedule_rfq_bank_release, broadcast_rfq_to_banks
    )
    now_utc = datetime.now(timezone.utc)
    cancel_scheduled_rfq_release(rfq.id)

    if payload.release_now:
        rfq.status = 'PENDING'
        rfq.scheduled_release_at = None
        rfq.scheduled_release_job_id = None
        db.commit()

        from app.core.routing import get_frontend_base_url
        base_url = get_frontend_base_url(request=request)
        background_tasks.add_task(broadcast_rfq_to_banks, rfq_id=rfq.id, base_url=base_url)

        return {"message": "Quotation released to banks immediately.", "rfq_id": rfq.id, "status": rfq.status}

    if not payload.scheduled_release_at:
        raise HTTPException(status_code=400, detail="New scheduled release time or release_now=true is required.")

    new_time = payload.scheduled_release_at
    if new_time.tzinfo is None:
        new_time = new_time.replace(tzinfo=timezone.utc)
    else:
        new_time = new_time.astimezone(timezone.utc)

    if new_time <= now_utc:
        raise HTTPException(status_code=400, detail="New scheduled release time must be in the future.")

    w_end = rfq.window_end
    if w_end and w_end.tzinfo is None:
        w_end = w_end.replace(tzinfo=timezone.utc)
    if w_end and new_time >= w_end:
        raise HTTPException(status_code=400, detail="New scheduled release time must be earlier than the quotation window close time.")

    job_id = schedule_rfq_bank_release(rfq_id=rfq.id, release_at=new_time)
    rfq.scheduled_release_at = new_time
    rfq.scheduled_release_job_id = job_id
    db.commit()

    return {
        "message": f"Bank release rescheduled for {new_time.strftime('%Y-%m-%d %H:%M UTC')}.",
        "rfq_id": rfq.id,
        "status": rfq.status,
        "scheduled_release_at": rfq.scheduled_release_at
    }


@router.post("/{rfq_id}/cancel-scheduled-release")
def cancel_rfq_release_maker(
    rfq_id: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Allows Maker or Admin to cancel a scheduled release and revert RFQ to PENDING_APPROVAL."""
    rfq = crud_quotation.get_request(db, rfq_id=rfq_id, customer_id=current_user.customer_id)
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    if rfq.status != 'APPROVED_SCHEDULED' or rfq.is_dispatched:
        raise HTTPException(status_code=400, detail="Only scheduled quotations awaiting dispatch can be cancelled.")

    from app.services.quotation_release_scheduler import cancel_scheduled_rfq_release
    cancel_scheduled_rfq_release(rfq.id)

    rfq.status = 'PENDING_APPROVAL'
    rfq.scheduled_release_at = None
    rfq.scheduled_release_job_id = None
    db.commit()

    return {"message": "Scheduled release cancelled. RFQ returned to pending approval.", "rfq_id": rfq.id, "status": rfq.status}


@router.post("/{rfq_id}/re-tender")
def retender_quotation(
    rfq_id: str,
    payload: ReTenderRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Clones an existing quotation (e.g. Inconclusive, Expired, or Completed) into a clean, audited re-tender."""
    parent = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not parent:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    # Calculate unique sequential re-tender suffix
    new_ref_no, root_parent_id = crud_quotation.get_unique_retender_ref_no(db, parent)

    # Determine window times
    now = datetime.now(timezone.utc)
    w_start = payload.window_start if payload.window_start else now
    w_end = payload.window_end

    if w_end <= w_start:
        raise HTTPException(status_code=400, detail="Window close time must be after the opening time.")

    # Check customer approval policy
    from app.crud.crud_config import crud_customer_configuration
    from app.constants import GlobalConfigKey
    config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED
    )
    requires_approval = False
    if config and config.get("effective_value"):
        requires_approval = str(config.get("effective_value")).lower() == 'true'

    initial_status = "PENDING_APPROVAL" if requires_approval else "PENDING"

    new_id = str(uuid.uuid4())
    new_rfq = QuotationRequest(
        id=new_id,
        ref_no=new_ref_no,
        customer_id=parent.customer_id,
        created_by_user_id=current_user.user_id,
        type=parent.type,
        direction=parent.direction,
        value_date=parent.value_date,
        amount=payload.amount if payload.amount is not None else parent.amount,
        min_ticket_amount=parent.min_ticket_amount,
        buy_currency=parent.buy_currency,
        sell_currency=parent.sell_currency,
        settlement_date_start=parent.settlement_date_start,
        settlement_date_end=parent.settlement_date_end,
        maturity_date_start=parent.maturity_date_start,
        maturity_date_end=parent.maturity_date_end,
        eval_rate=parent.eval_rate,
        window_start=w_start,
        window_end=w_end,
        quotation_base=parent.quotation_base,
        max_tolerance_percent=parent.max_tolerance_percent,
        allow_alternative_value_date=parent.allow_alternative_value_date or False,
        document_path=parent.document_path,
        status=initial_status,
        acceptance_timeout_seconds=parent.acceptance_timeout_seconds,
        acceptance_timeout_action=parent.acceptance_timeout_action,
        token_validity_hours=payload.token_validity_hours or parent.token_validity_hours or 24,
        parent_rfq_id=root_parent_id,
        entity_id=getattr(payload, 'entity_id', None) or parent.entity_id,
        comments_to_banks=parent.comments_to_banks
    )
    db.add(new_rfq)
    db.flush()

    # Replicate Bank Assignments
    parent_assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == parent.id).all()
    assigned_records = []
    
    for pa in parent_assignments:
        if payload.selected_bank_ids is not None and pa.quotation_bank.bank_id not in payload.selected_bank_ids:
            continue
        
        token = str(uuid.uuid4())
        bank_row = pa.quotation_bank
        bank_contacts = bank_row.contacts if (bank_row and isinstance(bank_row.contacts, list)) else []
        has_approvers = any(c.get("role") == "APPROVER" for c in bank_contacts)
        has_execution = any(c.get("role") == "EXECUTION" for c in bank_contacts)
        has_exec_retender = (pa.quotation_base or parent.quotation_base or "Execution").lower() in ("execution", "mixed")
        if getattr(pa, "is_cross_entity", False):
            has_exec_retender = False
        assignment_approval_status = "PENDING" if (has_approvers and has_execution and has_exec_retender) else None

        new_assignment = QuotationBankAssignment(
            id=str(uuid.uuid4()),
            rfq_id=new_rfq.id,
            quotation_bank_id=pa.quotation_bank_id,
            token=token,
            cost_min=pa.cost_min,
            cost_percent=pa.cost_percent,
            cost_max=pa.cost_max,
            cost_flat=pa.cost_flat,
            quotation_base=pa.quotation_base,
            is_document_visible=pa.is_document_visible,
            value_date=pa.value_date,
            allow_alternative_value_date=pa.allow_alternative_value_date,
            approval_status=assignment_approval_status
        )
        db.add(new_assignment)
        assigned_records.append({"assignment": new_assignment, "bank_row": bank_row, "token": token})

    db.commit()

    # If no internal approval required, dispatch bank notification emails
    if not requires_approval:
        # Schedule 15m prior reminder if window_start - now >= 60 min
        try:
            from app.services.quotation_reminder_service import schedule_rfq_15m_reminder
            schedule_rfq_15m_reminder(
                rfq_id=new_rfq.id,
                window_start=new_rfq.window_start,
                release_time=new_rfq.created_at or datetime.now(timezone.utc)
            )
        except Exception as rem_err:
            logger.warning(f"Failed to schedule 15m reminder for RFQ {new_rfq.id}: {rem_err}")

        email_settings, _ = get_customer_email_settings(db, current_user.customer_id)
        from app.core.routing import get_frontend_base_url
        base_url = get_frontend_base_url(request=request)
        customer_branding = (new_rfq.entity.entity_name if new_rfq.entity else None) or (new_rfq.customer.name if new_rfq.customer else "Corporate Treasury")

        for item in assigned_records:
            bank_row = item["bank_row"]
            if not bank_row: continue
            contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
            if not contacts and bank_row.emails:
                contacts = [{"email": e.strip(), "name": "", "role": "EXECUTION"} for e in bank_row.emails.split(',') if e.strip()]
            if not contacts:
                continue

            new_assignment = item.get("assignment")
            bank_display_name = bank_row.bank.name if bank_row.bank else "Bank Partner"
            link = f"{base_url}/public-quotation/{item['token']}"

            # Collect contacts by role
            all_bank_emails = list(dict.fromkeys(
                c.get("email", "").strip() for c in contacts if c.get("email")
            ))
            approver_emails = list(dict.fromkeys(
                c.get("email", "").strip() for c in contacts 
                if c.get("role") == "APPROVER" and c.get("email")
            ))
            approver_set = {e.lower() for e in approver_emails}
            non_approver_emails = [e for e in all_bank_emails if e.lower() not in approver_set]

            leg_cfgs = getattr(new_assignment, "leg_configs", []) if new_assignment else []
            if leg_cfgs:
                has_exec_leg = any((c.quotation_base or "").lower() == "execution" for c in leg_cfgs)
            else:
                ass_base = (getattr(new_assignment, "quotation_base", "") or getattr(new_rfq, "quotation_base", "") or "Execution").lower()
                has_exec_leg = ass_base in ("execution", "mixed")
            if new_assignment and getattr(new_assignment, "is_cross_entity", False):
                has_exec_leg = False

            is_indicative = not has_exec_leg
            has_approver = len(approver_emails) > 0
            has_execution = any(c.get("role") == "EXECUTION" for c in contacts)

            if new_assignment and getattr(new_assignment, 'approval_status', None) == 'PENDING' and not is_indicative and has_approver and has_execution:
                # --- BANK APPROVAL FLOW ---
                # Phase 1a: APPROVER contacts with review link requiring 2FA OTP verification
                for app_email in approver_emails:
                    approver_link = f"{base_url}/public-quotation/{item['token']}?email={app_email}"
                    from app.services.unified_email_builder import build_quotation_rfq_bank_email
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=new_rfq,
                        assignment=new_assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=approver_link,
                        email_purpose="BANK_APPROVAL_REQUIRED"
                    )
                    background_tasks.add_task(send_email, db, [app_email], subject, body, {}, email_settings)

                # Phase 1b: EXECUTION + VIEW_ONLY with heads-up (NO link) ALL TOGETHER in ONE email
                if non_approver_emails:
                    from app.services.unified_email_builder import build_quotation_rfq_bank_email
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=new_rfq,
                        assignment=new_assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link="",
                        email_purpose="BANK_HEADS_UP"
                    )
                    background_tasks.add_task(send_email, db, non_approver_emails, subject, body, {}, email_settings)
            else:
                # --- STANDARD FLOW (no bank-level approval needed, e.g. Indicative or Execution without Approver) ---
                if new_assignment and getattr(new_assignment, 'approval_status', None) == 'PENDING':
                    new_assignment.approval_status = None
                    db.commit()

                # Send standard invitation email with portal link to ALL contacts from the same bank ALL TOGETHER in the SAME email
                if all_bank_emails:
                    from app.services.unified_email_builder import build_quotation_rfq_bank_email
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=new_rfq,
                        assignment=new_assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=link,
                        email_purpose="RE_TENDER"
                    )
                    background_tasks.add_task(
                        send_email,
                        db,
                        all_bank_emails,
                        subject,
                        body,
                        {},
                        email_settings,
                    )


    # Dispatch submission email to Corporate Admins and Common Communication List
    _dispatch_quotation_submission_email(
        db=db,
        background_tasks=background_tasks,
        rfq=new_rfq,
        current_user=current_user,
        requires_approval=requires_approval,
        request=request,
        is_retender=True
    )

    return {
        "message": "Quotation re-tendered successfully.",
        "rfq_id": new_rfq.id,
        "ref_no": new_rfq.ref_no,
        "parent_ref_no": parent.ref_no,
        "status": new_rfq.status
    }

@router.post("/{rfq_id}/resubmit")
def resubmit_quotation(
    rfq_id: str,
    payload: QuotationResubmitRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Allows the maker to update an RFQ that was returned with status NEEDS_REVISION or is still PENDING_APPROVAL and resubmit for approval."""
    rfq = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    if rfq.status not in ('NEEDS_REVISION', 'PENDING_APPROVAL'):
        raise HTTPException(status_code=400, detail=f"Quotation is in {rfq.status} status and cannot be edited or resubmitted.")

    # Validate date consistency: settlement/value date cannot precede quotation trade date
    eff_w_start = payload.window_start or rfq.window_start
    w_date = None
    if eff_w_start:
        if hasattr(eff_w_start, 'date'):
            w_date = eff_w_start.date()
        else:
            try:
                w_date = datetime.strptime(str(eff_w_start).strip().split('T')[0], "%Y-%m-%d").date()
            except Exception:
                pass

    def _parse_d(v):
        if not v:
            return None
        try:
            return datetime.strptime(str(v).strip().split('T')[0], "%Y-%m-%d").date()
        except Exception:
            return None

    eff_type = payload.type or rfq.type
    eff_val_date = payload.value_date if payload.value_date is not None else rfq.value_date
    if w_date:
        if (eff_type == 'FX_SPOT' or not eff_type) and eff_val_date:
            val_d = _parse_d(eff_val_date)
            if val_d and val_d < w_date:
                raise HTTPException(
                    status_code=400,
                    detail=f"Master Value Date ({val_d}) cannot be earlier than quotation window date ({w_date}). Settlement date can be the same day or later, but never earlier."
                )
        elif eff_type == 'TBILL':
            eff_settle = payload.settlement_date_start if payload.settlement_date_start is not None else rfq.settlement_date_start
            if eff_settle:
                s_d = _parse_d(eff_settle)
                if s_d and s_d < w_date:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Settlement Date ({s_d}) cannot be earlier than quotation window date ({w_date})."
                    )

    if payload.type is not None:
        rfq.type = payload.type
    if payload.direction is not None:
        rfq.direction = payload.direction
    if payload.value_date is not None:
        rfq.value_date = payload.value_date
    if payload.amount is not None:
        rfq.amount = payload.amount
    if payload.min_ticket_amount is not None:
        rfq.min_ticket_amount = payload.min_ticket_amount
    if payload.buy_currency is not None:
        rfq.buy_currency = payload.buy_currency
    if payload.sell_currency is not None:
        rfq.sell_currency = payload.sell_currency
    if payload.settlement_date_start is not None:
        rfq.settlement_date_start = payload.settlement_date_start
    if payload.settlement_date_end is not None:
        rfq.settlement_date_end = payload.settlement_date_end
    if payload.maturity_date_start is not None:
        rfq.maturity_date_start = payload.maturity_date_start
    if payload.maturity_date_end is not None:
        rfq.maturity_date_end = payload.maturity_date_end
    if payload.eval_rate is not None:
        rfq.eval_rate = payload.eval_rate
    if payload.window_start is not None:
        rfq.window_start = payload.window_start
    if payload.window_end is not None:
        rfq.window_end = payload.window_end
    if payload.quotation_base is not None:
        rfq.quotation_base = payload.quotation_base
    if payload.max_tolerance_percent is not None:
        rfq.max_tolerance_percent = payload.max_tolerance_percent
    if payload.document_path is not None:
        rfq.document_path = payload.document_path
    if payload.token_validity_hours is not None:
        rfq.token_validity_hours = payload.token_validity_hours
    if payload.allow_alternative_value_date is not None:
        rfq.allow_alternative_value_date = payload.allow_alternative_value_date
    if getattr(payload, 'internal_notes', None) is not None:
        rfq.internal_notes = payload.internal_notes
    elif getattr(payload, 'internalNotes', None) is not None:
        rfq.internal_notes = payload.internalNotes
    if getattr(payload, 'comments_to_banks', None) is not None:
        rfq.comments_to_banks = payload.comments_to_banks
    elif getattr(payload, 'commentsToBanks', None) is not None:
        rfq.comments_to_banks = payload.commentsToBanks
    if getattr(payload, 'user_notes', None) is not None:
        rfq.user_revision_notes = payload.user_notes.strip() if payload.user_notes else None

    # Handle multi-pair legs re-creation if provided
    resubmit_pairs = getattr(payload, 'pairs', None) or getattr(payload, 'legs', None)
    if resubmit_pairs:
        if len(resubmit_pairs) > 4:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A maximum of 4 currency pairs can be submitted in a single quotation request."
            )

        seen_pair_keys = set()
        for idx, p_item in enumerate(resubmit_pairs, start=1):
            b_curr = (getattr(p_item, 'buyCurrency', None) or getattr(p_item, 'buy_currency', None) or rfq.buy_currency or 'USD').strip().upper()
            s_curr = (getattr(p_item, 'sellCurrency', None) or getattr(p_item, 'sell_currency', None) or rfq.sell_currency or 'EGP').strip().upper()
            v_date = str(getattr(p_item, 'valueDate', None) or getattr(p_item, 'value_date', None) or rfq.value_date or '').strip()
            q_b = (getattr(p_item, 'quotationBase', None) or getattr(p_item, 'quotation_base', None) or rfq.quotation_base or 'Execution').strip().lower()

            pair_key = (frozenset([b_curr, s_curr]), v_date, q_b)
            if pair_key in seen_pair_keys:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Duplicate currency pair detected: Multiple legs requested for {b_curr}/{s_curr} with the same settlement date ({v_date}) and quotation base ({q_b.capitalize()}). Similar pairs with matching settlement date and quotation base are not permitted."
                )
            seen_pair_keys.add(pair_key)

        db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq.id).delete()
        created_legs_with_source = []
        for idx, p_item in enumerate(resubmit_pairs, start=1):
            leg_id = f"{rfq.id}-leg-{idx}"
            leg_val_d = getattr(p_item, 'valueDate', None)
            leg_obj = QuotationLeg(
                id=leg_id,
                rfq_id=rfq.id,
                leg_index=idx,
                type=rfq.type,
                direction=getattr(p_item, 'direction', None) or rfq.direction or "Buy",
                buy_currency=getattr(p_item, 'buyCurrency', None) or rfq.buy_currency or "USD",
                sell_currency=getattr(p_item, 'sellCurrency', None) or rfq.sell_currency or "EGP",
                amount=getattr(p_item, 'amount', None),
                min_ticket_amount=getattr(p_item, 'minTicketAmount', None),
                value_date=str(leg_val_d) if leg_val_d else None,
                allow_alternative_value_date=bool(getattr(p_item, 'allowAlternativeValueDate', False)),
                quotation_base=getattr(p_item, 'quotationBase', None) or rfq.quotation_base,
                max_tolerance_percent=getattr(p_item, 'maxTolerancePercent', None) or rfq.max_tolerance_percent,
                document_path=json.dumps(rfq.get_documents_for_leg(leg_index=idx, pair=f"{getattr(p_item, 'buyCurrency', None) or rfq.buy_currency or 'USD'}/{getattr(p_item, 'sellCurrency', None) or rfq.sell_currency or 'EGP'}")) if rfq.get_documents_for_leg(leg_index=idx, pair=f"{getattr(p_item, 'buyCurrency', None) or rfq.buy_currency or 'USD'}/{getattr(p_item, 'sellCurrency', None) or rfq.sell_currency or 'EGP'}") else None,
                status='PENDING_APPROVAL',
                entity_id=rfq.entity_id
            )
            db.add(leg_obj)
            created_legs_with_source.append((leg_obj, p_item))
        db.flush()

    # If new selected banks provided from the builder, re-sync bank assignments
    if payload.selected_banks:
        try:
            banks_data = json.loads(payload.selected_banks) if isinstance(payload.selected_banks, str) else (payload.selected_banks or [])
            # Preserve existing assignment tokens so counterparty tabs/links do not get decommissioned on revision
            existing_tokens = {
                a.quotation_bank_id: a.token
                for a in db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).all()
                if a.quotation_bank_id and a.token
            }
            # Remove previous unsubmitted assignments
            db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).delete()
            current_legs = db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq.id).order_by(QuotationLeg.leg_index.asc()).all()

            # Pre-scan legs per bank to determine order-independent assignment parameters and per-leg bank data
            bank_legs_catalog = {} # raw_bank_id -> list of dicts: {'leg_obj': leg_obj, 'b_data': b_data}
            legs_to_process = created_legs_with_source if 'created_legs_with_source' in locals() and created_legs_with_source else [(l, None) for l in current_legs]

            for leg_obj, p_source in legs_to_process:
                p_banks_raw = (getattr(p_source, 'selectedBanks', None) or getattr(p_source, 'selected_banks', None)) if p_source else None
                if p_banks_raw:
                    leg_banks = json.loads(p_banks_raw) if isinstance(p_banks_raw, str) else p_banks_raw
                    if isinstance(leg_banks, list):
                        leg_banks = [b.dict() if hasattr(b, 'dict') else b for b in leg_banks]
                    else:
                        leg_banks = banks_data
                else:
                    leg_banks = banks_data

                for b_data in leg_banks:
                    bid = b_data.get('id')
                    if not bid:
                        continue
                    if bid not in bank_legs_catalog:
                        bank_legs_catalog[bid] = []
                    bank_legs_catalog[bid].append({'leg_obj': leg_obj, 'b_data': b_data})

            # Also ensure all banks in root banks_data are cataloged
            for b_data in banks_data:
                bid = b_data.get('id')
                if bid and bid not in bank_legs_catalog:
                    bank_legs_catalog[bid] = [{'leg_obj': l, 'b_data': b_data} for l in current_legs]

            all_created_leg_cfgs = []
            for raw_bank_id, leg_items in bank_legs_catalog.items():
                root_bank_info = next((rb for rb in banks_data if str(rb.get('id')) == str(raw_bank_id)), leg_items[0]['b_data'])
                q_bank = db.query(QuotationBank).filter(
                    QuotationBank.customer_id == current_user.customer_id,
                    QuotationBank.bank_id == raw_bank_id,
                    QuotationBank.trade_type.in_([rfq.type, "BOTH"])
                ).first()
                if not q_bank:
                    continue

                token = existing_tokens.get(q_bank.id) or str(uuid.uuid4())
                assignment_id = str(uuid.uuid4())

                # Check if bank is cross-entity for this RFQ's entity
                is_cross_bank = False
                if rfq.entity_id and q_bank.entity_scope != 'ALL_ENTITIES':
                    assoc_eids = [assoc.entity_id for assoc in q_bank.entity_associations] if q_bank.entity_associations else []
                    if rfq.entity_id not in assoc_eids:
                        is_cross_bank = True

                if is_cross_bank:
                    from app.crud.crud_config import crud_customer_configuration
                    from app.constants import GlobalConfigKey
                    cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.ALLOW_CROSS_ENTITY_INDICATIVE_QUOTES
                    )
                    allow_cross_cfg = False
                    if cfg and cfg.get("effective_value"):
                        allow_cross_cfg = str(cfg["effective_value"]).strip().lower() in ("true", "1", "yes")
                    if not allow_cross_cfg:
                        b_name = root_bank_info.get('name') or (q_bank.bank.name if (q_bank and getattr(q_bank, 'bank', None)) else f"Bank #{raw_bank_id}")
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Cross-entity quotation is disabled. Bank {b_name} does not belong to the selected legal entity."
                        )

                # Compute order-independent quotation base across ALL active legs for this bank
                if is_cross_bank:
                    bank_overall_base = "Indicative"
                    has_any_exec_leg = False
                else:
                    leg_bases = []
                    for item in leg_items:
                        l_b_data = item['b_data']
                        l_leg_obj = item['leg_obj']
                        l_q_base = l_b_data.get('quotationBase') or root_bank_info.get('quotationBase') or l_leg_obj.quotation_base or rfq.quotation_base or 'Execution'
                        is_l_invited = l_b_data.get('isInvited', l_b_data.get('is_invited', True)) is not False
                        if str(l_q_base).strip().lower() not in ['invisible', 'skipped', 'excluded'] and is_l_invited:
                            leg_bases.append((l_q_base or 'Execution').strip().capitalize())

                    if not leg_bases:
                        leg_bases = [(root_bank_info.get('quotationBase') or rfq.quotation_base or 'Execution').strip().capitalize()]

                    has_exec = any(b.lower() == 'execution' for b in leg_bases)
                    has_indic = any(b.lower() == 'indicative' for b in leg_bases)
                    has_any_exec_leg = has_exec
                    if has_exec and has_indic:
                        bank_overall_base = "Mixed"
                    elif has_indic:
                        bank_overall_base = "Indicative"
                    else:
                        bank_overall_base = "Execution"

                contacts = q_bank.contacts if isinstance(q_bank.contacts, list) else []
                has_approver = any(c.get('role') == 'APPROVER' for c in contacts)
                has_execution = any(c.get('role') == 'EXECUTION' for c in contacts)
                bank_approval_status = 'PENDING' if (has_approver and has_execution and has_any_exec_leg and not is_cross_bank) else None

                bank_value_date = root_bank_info.get('valueDate') or rfq.value_date
                if w_date and (rfq.type == 'FX_SPOT' or not rfq.type) and bank_value_date:
                    b_val_d = _parse_d(bank_value_date)
                    if b_val_d and b_val_d < w_date:
                        bank_label = q_bank.bank.name if (q_bank and q_bank.bank) else f"Bank #{raw_bank_id}"
                        raise HTTPException(
                            status_code=400,
                            detail=f"Value Date ({b_val_d}) for {bank_label} cannot be earlier than quotation window date ({w_date}). Value date must be on or after the quotation trade date."
                        )

                bank_allow_alt = root_bank_info.get('allowAlternativeValueDate')

                db_assignment = QuotationBankAssignment(
                    id=assignment_id,
                    rfq_id=rfq.id,
                    quotation_bank_id=q_bank.id,
                    token=token,
                    cost_min=root_bank_info.get('costMin', 0.0),
                    cost_percent=root_bank_info.get('costPercent', 0.0),
                    cost_max=root_bank_info.get('costMax', 0.0),
                    cost_flat=root_bank_info.get('costFlat', 0.0),
                    quotation_base=bank_overall_base,
                    is_document_visible=root_bank_info.get('isDocumentVisible', True) if not is_cross_bank else False,
                    value_date=bank_value_date,
                    allow_alternative_value_date=bank_allow_alt,
                    approval_status=bank_approval_status
                )
                db.add(db_assignment)
                db.flush()

                bank_seen_signatures = set()
                for item in leg_items:
                    leg_obj = item['leg_obj']
                    b_data = item['b_data']
                    cfg_id = str(uuid.uuid4())
                    leg_cfg_val_d = _parse_d(b_data.get('valueDate') or leg_obj.value_date)
                    leg_q_base = "Indicative" if is_cross_bank else (b_data.get('quotationBase') or leg_obj.quotation_base or 'Execution')
                    is_leg_invited = b_data.get('isInvited', b_data.get('is_invited', True)) is not False
                    if str(leg_q_base).strip().lower() in ['invisible', 'skipped', 'excluded']:
                        is_leg_invited = False
                        leg_q_base = "Invisible"

                    if (rfq.type == 'FX_SPOT' or not rfq.type) and is_leg_invited:
                        sig = get_bank_leg_signature(
                            leg_obj.buy_currency,
                            leg_obj.sell_currency,
                            leg_obj.direction,
                            leg_cfg_val_d,
                            leg_q_base
                        )
                        if sig in bank_seen_signatures:
                            b_name = b_data.get('name') or (q_bank.bank.name if (q_bank and getattr(q_bank, 'bank', None)) else f"Bank #{raw_bank_id}")
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Counterparty conflict for {b_name}: Multiple legs requested for {leg_obj.buy_currency}/{leg_obj.sell_currency} with matching effective settlement date ({leg_cfg_val_d}) and quotation base ({str(leg_q_base).capitalize()}). A bank cannot receive identical quote requests."
                            )
                        bank_seen_signatures.add(sig)

                    leg_allow_alt = bool(b_data.get('allowAlternativeValueDate')) if b_data.get('allowAlternativeValueDate') is not None else bool(leg_obj.allow_alternative_value_date)

                    leg_bank_cfg = QuotationBankLegConfig(
                        id=cfg_id,
                        assignment_id=db_assignment.id,
                        leg_id=leg_obj.id,
                        is_invited=is_leg_invited,
                        cost_min=b_data.get('costMin', 0.0),
                        cost_percent=b_data.get('costPercent', 0.0),
                        cost_max=b_data.get('costMax', 0.0),
                        cost_flat=b_data.get('costFlat', 0.0),
                        quotation_base=leg_q_base,
                        is_document_visible=b_data.get('isDocumentVisible', True) if not is_cross_bank else False,
                        value_date=leg_cfg_val_d,
                        allow_alternative_value_date=leg_allow_alt
                    )
                    db.add(leg_bank_cfg)
                    all_created_leg_cfgs.append(leg_bank_cfg)

            # Re-evaluate RFQ-level quotation_base based on active invited counterparties
            active_bases = [
                c.quotation_base.lower() for c in all_created_leg_cfgs
                if c.is_invited and c.quotation_base and c.quotation_base.lower() != 'invisible'
            ]
            if active_bases:
                has_rfq_exec = any(b == 'execution' for b in active_bases)
                has_rfq_indic = any(b == 'indicative' for b in active_bases)
                if has_rfq_exec and has_rfq_indic:
                    rfq.quotation_base = "Mixed"
                elif has_rfq_indic:
                    rfq.quotation_base = "Indicative"
                else:
                    rfq.quotation_base = "Execution"
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to update bank assignments on resubmit: {e}")

    rfq.status = 'PENDING_APPROVAL'
    db.commit()


    # Step 3: Dispatch Email to Corporate Admins
    from app.services.quotation_approval_notifications import dispatch_rfq_resubmitted_email
    dispatch_rfq_resubmitted_email(
        db=db,
        background_tasks=background_tasks,
        rfq=rfq,
        user_notes=getattr(payload, 'user_notes', None),
        submitter_email=current_user.email or f"User #{current_user.user_id}",
        request=request
    )

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_RFQ_RESUBMITTED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": rfq.id,
            "ref_no": rfq.ref_no,
            "type": rfq.type,
            "entity_id": rfq.entity_id,
            "quotation_base": rfq.quotation_base,
            "legal_disclaimer_accepted": bool(payload.legal_disclaimer_accepted or payload.legalDisclaimerAccepted)
        },
        customer_id=current_user.customer_id
    )
    db.commit()

    return {"message": "Quotation revised and resubmitted for approval.", "rfq_id": rfq.id, "status": rfq.status}

@router.get("/stats")
def get_quotation_stats(
    trade_type: str = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Calculates bank performance statistics for the Market Insights dashboard."""
    query = db.query(QuotationRequest).filter(
        QuotationRequest.customer_id == current_user.customer_id,
        QuotationRequest.status == 'COMPLETED'
    )
    if trade_type:
        query = query.filter(QuotationRequest.type == trade_type)
    reqs = query.all()
    
    bank_stats = {}
    
    for r in reqs:
        standings = compute_rfq_standings(r, db, dispatch_emails=False)
        if standings.get("is_inconclusive"):
            continue

        valid_bids = [res for res in standings.get("results", []) if (res.get("finalPrice") is not None or res.get("best_score") is not None)]
        if not valid_bids:
            continue

        for i, offer in enumerate(valid_bids):
            bid_id = offer['quotation_bank_id']
            if bid_id not in bank_stats:
                bank_stats[bid_id] = {
                    'bank_id': bid_id,
                    'bank_name': offer['bank_name'],
                    'total_participated': 0,
                    'total_won': 0,
                    'ranks': {1: 0, 2: 0, 3: 0},
                    'total_spread': 0.0,
                    'spread_count': 0
                }
            
            stats = bank_stats[bid_id]
            stats['total_participated'] += 1
            rank = i + 1
            if rank <= 3:
                stats['ranks'][rank] += 1
            if rank == 1:
                stats['total_won'] += 1
            
            winner_price = valid_bids[0].get('normalized_price') or valid_bids[0].get('finalPrice') or valid_bids[0].get('best_score') or 0
            curr_price = offer.get('normalized_price') or offer.get('finalPrice') or offer.get('best_score') or 0
            if winner_price > 0 and curr_price:
                spread = abs(curr_price - winner_price) / winner_price * 100
                stats['total_spread'] += spread
                stats['spread_count'] += 1

    results = []
    for bid, s in bank_stats.items():
        results.append({
            'bank_id': s['bank_id'],
            'bank_name': s['bank_name'],
            'win_rate': (s['total_won'] / s['total_participated'] * 100) if s['total_participated'] > 0 else 0,
            'total_won': s['total_won'],
            'total_participated': s['total_participated'],
            'ranks': s['ranks'],
            'avg_spread': (s['total_spread'] / s['spread_count']) if s['spread_count'] > 0 else 0
        })
        
    return sorted(results, key=lambda x: x['win_rate'], reverse=True)

@router.get("/{rfq_id}/results", response_model=QuotationResultsOut)
def get_rfq_results(
    rfq_id: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Calculates active Quotation standings/results for a given RFQ."""
    if current_user and hasattr(current_user, 'customer_id'):
        rfq = crud_quotation.get_request(db, rfq_id=rfq_id, customer_id=current_user.customer_id)
    else:
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()

    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")
        
    standings = compute_rfq_standings(rfq, db, dispatch_emails=True)

    # Pre-sign any GCS document paths for instantaneous client-side viewing
    if rfq.document_path and "gs://" in rfq.document_path:
        try:
            import json
            from app.core.ai_integration import generate_signed_gcs_url_sync
            loaded = json.loads(rfq.document_path)
            docs_to_sign = []
            if isinstance(loaded, dict) and "documents" in loaded:
                docs_to_sign = loaded.get("documents", [])
            elif isinstance(loaded, list):
                docs_to_sign = loaded

            modified = False
            for d in docs_to_sign:
                p_val = d.get("path")
                if p_val and str(p_val).startswith("gs://"):
                    signed = generate_signed_gcs_url_sync(p_val, expiration=604800)
                    if signed:
                        d["path"] = signed
                        modified = True

            if modified:
                rfq_out = QuotationRequestOut.model_validate(rfq)
                rfq_out.document_path = json.dumps(loaded)
                standings["rfq"] = rfq_out
        except Exception as e:
            logger.warning(f"Failed to pre-sign documents in get_rfq_results: {e}")

    return standings

_DISPATCHING_RFQS = set()
_DISPATCH_LOCK = threading.Lock()

async def dispatch_rfq_result_emails(rfq_id: str, db: Session, force: bool = False) -> dict:
    """Sends winner & regret emails to assigned Execution banks for a completed RFQ."""
    with _DISPATCH_LOCK:
        if rfq_id in _DISPATCHING_RFQS:
            logger.info(f"RFQ {rfq_id} result emails are already being dispatched by an active process. Skipping duplicate.")
            return {"status": "skipped", "detail": "Result emails are currently being dispatched for this RFQ"}
        _DISPATCHING_RFQS.add(rfq_id)

    try:
        return await _execute_dispatch_rfq_result_emails(rfq_id, db, force)
    finally:
        with _DISPATCH_LOCK:
            _DISPATCHING_RFQS.discard(rfq_id)

async def _execute_dispatch_rfq_result_emails(rfq_id: str, db: Session, force: bool = False) -> dict:
    from app.models.models import AuditLog
    from app.crud.crud import log_action

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        return {"status": "error", "detail": "RFQ not found"}

    # Idempotency check: only send once automatically unless manually forced by admin
    already_sent = db.query(AuditLog).filter(
        AuditLog.action_type == "QUOTATION_RESULTS_SENT",
        AuditLog.entity_type == "QuotationRequest"
    ).filter(
        AuditLog.details["rfq_id"].astext == str(rfq.id)
    ).first()

    if already_sent and not force:
        return {"status": "skipped", "detail": "Result emails already dispatched for this RFQ"}

    # Debounce safeguard even when forced: prevent rapid duplicate clicks (<15s)
    if force and already_sent and already_sent.timestamp:
        now_utc = datetime.now(timezone.utc)
        ts = already_sent.timestamp.replace(tzinfo=timezone.utc) if already_sent.timestamp.tzinfo is None else already_sent.timestamp
        if (now_utc - ts).total_seconds() < 15:
            return {"status": "skipped", "detail": "Result emails were recently sent. Please wait before retrying."}

    # Evaluate results using central evaluation engine WITHOUT recursive dispatch
    res_data = compute_rfq_standings(rfq, db, dispatch_emails=False)
    results = res_data.get("results", [])
    is_inconclusive = res_data.get("is_inconclusive", False)
    inconclusive_reason = res_data.get("inconclusive_reason")
    winner_bank_id = res_data.get("winner_bank_id")
    legs_data = res_data.get("legs", [])
    is_multi_leg = bool(legs_data and len(legs_data) > 1)

    # Pre-claim in AuditLog to guarantee atomic DB-level idempotency across concurrent workers
    audit_entry = None
    if not force:
        audit_entry = log_action(
            db,
            user_id=rfq.created_by_user_id,
            action_type="QUOTATION_RESULTS_SENT",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "ref_no": rfq.ref_no,
                "winner_bank_id": winner_bank_id,
                "emails_count": 0,
                "is_multi_leg": is_multi_leg,
                "status": "in_progress",
                "dispatched_at": datetime.now(timezone.utc).isoformat()
            },
            customer_id=rfq.customer_id
        )
        db.commit()

    # Verify if there are any submitted execution quotes to notify
    has_any_execution_quotes = any(
        (r.get('quotation_base') or rfq.quotation_base or 'Execution').lower() == 'execution' and r.get('price') is not None
        for r in results
    ) if not is_multi_leg else any(
        any((r.get('quotation_base') or l.get('quotation_base') or 'Execution').lower() == 'execution' and r.get('price') is not None for r in l.get('results', []))
        for l in legs_data
    )
    if not has_any_execution_quotes:
        return {"status": "skipped", "detail": "No Execution quotes received to dispatch outcome emails."}

    from app.core.email_service import get_customer_email_settings, send_email
    from app.services.unified_email_builder import build_transaction_email_html
    email_settings, source = get_customer_email_settings(db, rfq.customer_id)

    customer_name = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else "Treasury Client")
    ref_no = rfq.ref_no
    emails_dispatched = 0
    sender_name = "Treasury Quotations" if source != "customer_specific" else email_settings.sender_display_name

    if is_multi_leg:
        # Multi-Leg: Group participants across all legs
        banks_map = {}
        for leg in legs_data:
            for r in leg.get("results", []):
                q_base = (r.get('quotation_base') or leg.get('quotation_base') or rfq.quotation_base or 'Execution').lower()
                if q_base == 'indicative':
                    continue
                b_id = r.get("bank_id")
                if b_id and b_id not in banks_map:
                    bank_emails = []
                    if r.get('contacts') and isinstance(r['contacts'], list):
                        bank_emails = [c.get('email', '').strip() for c in r['contacts'] if c.get('email')]
                    if not bank_emails and r.get('bank_emails'):
                        bank_emails = [e.strip() for e in r['bank_emails'].split(',') if e.strip()]
                    banks_map[b_id] = {
                        "bank_id": b_id,
                        "bank_name": r.get("bank_name"),
                        "bank_emails": bank_emails,
                        "submitted_by_email": r.get("submitted_by_email")
                    }

        for b_id, b_info in banks_map.items():
            if not b_info["bank_emails"]:
                continue

            won_legs = [l for l in legs_data if l.get("winner_bank_id") == b_id and not l.get("is_inconclusive") and rfq.status not in ('REJECTED', 'CANCELLED')]
            lost_legs = [l for l in legs_data if l not in won_legs and any(r.get("bank_id") == b_id for r in l.get("results", []))]

            if won_legs:
                is_partial = len(won_legs) < len(legs_data)
                if is_partial:
                    inconclusive_lost = [l for l in lost_legs if l.get("is_inconclusive") or not l.get("winner_bank_id")]
                    awarded_other_lost = [l for l in lost_legs if not l.get("is_inconclusive") and l.get("winner_bank_id")]
                    if inconclusive_lost and awarded_other_lost:
                        remain_str = f"The remaining {len(lost_legs)} unselected pair(s) were concluded ({len(awarded_other_lost)} awarded to competing counterparties, {len(inconclusive_lost)} closed without execution)."
                    elif inconclusive_lost:
                        remain_str = f"The remaining {len(lost_legs)} unselected pair(s) closed without execution (inconclusive / exceeded tolerance limits)."
                    else:
                        remain_str = f"The remaining {len(lost_legs)} unselected pair(s) were concluded and awarded to competing counterparties."
                    subject = f"TRADE EXECUTION (PARTIAL): RFQ {ref_no} ({customer_name}) - Awarded {len(won_legs)} of {len(legs_data)} Legs"
                    alloc_str = f"<span style='color: #16a34a; font-weight: 700;'>Partially Awarded ({len(won_legs)} of {len(legs_data)} Currency Pairs)</span>"
                    summary_text = f"We are pleased to confirm the execution of <strong>{len(won_legs)} of {len(legs_data)} currency pairs</strong> with <strong>{customer_name}</strong> based on your winning quotes. {remain_str}"
                else:
                    subject = f"TRADE EXECUTION CONFIRMED: RFQ {ref_no} ({customer_name}) - Full Multi-Currency Package ({len(won_legs)} Legs)"
                    alloc_str = f"<span style='color: #16a34a; font-weight: 700;'>100% Package Awarded ({len(won_legs)} of {len(legs_data)} Pairs)</span>"
                    summary_text = f"We are pleased to confirm the execution of all {len(won_legs)} currency pair trades in this package with <strong>{customer_name}</strong> based on your winning quotes."

                key_vals = {
                    "RFQ Reference": ref_no,
                    "Requesting Legal Entity": customer_name,
                    "Package Allocation": alloc_str,
                }
                for i, leg in enumerate(won_legs):
                    b_res = next((r for r in leg.get("results", []) if r.get("bank_id") == b_id), {})
                    p_str = f"{b_res.get('price', 0):.5f}" if b_res.get('price') is not None else "N/A"
                    val_date_str = str(b_res.get('offered_value_date') or b_res.get('assigned_value_date') or leg.get('value_date') or 'Standard Spot')
                    pair_label = leg.get('currency_pair') or f"{leg.get('buy_currency')}/{leg.get('sell_currency')}"
                    key_vals[f"Awarded Leg #{i+1} ({pair_label})"] = (
                        f"🏆 Client {leg.get('direction', 'BUY')} {leg.get('amount', 0):,.2f} {leg.get('buy_currency', '')} "
                        f"@ <strong style='color: #16a34a;'>{p_str}</strong> (Value Date: {val_date_str})"
                    )

                from app.core.otp_security import generate_scoped_deal_receipt
                exec_legs_data = []
                for leg in won_legs:
                    b_res = next((r for r in leg.get("results", []) if r.get("bank_id") == b_id), {})
                    exec_legs_data.append({
                        "leg_id": str(leg.get("id", "") or leg.get("leg_id", "")),
                        "pair": leg.get('currency_pair') or f"{leg.get('buy_currency')}/{leg.get('sell_currency')}",
                        "direction": leg.get('direction', 'BUY'),
                        "amount": float(leg.get('amount', 0)),
                        "currency": leg.get('buy_currency', ''),
                        "rate": float(b_res.get('price', 0)) if b_res.get('price') is not None else 0.0,
                        "value_date": str(b_res.get('offered_value_date') or b_res.get('assigned_value_date') or leg.get('value_date') or 'Standard Spot')
                    })
                exec_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                receipt_info = generate_scoped_deal_receipt(
                    rfq_id=str(rfq.id),
                    ref_no=ref_no,
                    customer_name=customer_name,
                    bank_id=b_id,
                    bank_name=b_info['bank_name'],
                    executed_legs=exec_legs_data,
                    executed_at=exec_timestamp
                )
                key_vals["Deal Execution Receipt"] = f"<span style='font-family: monospace; font-size: 11px; background: #f1f5f9; padding: 2px 6px; border-radius: 4px; color: #0f172a; font-weight: 700;'>{receipt_info['receipt_id']}</span>"
                key_vals["Cryptographic Signature"] = f"<span style='font-family: monospace; font-size: 10px; color: #334155; word-break: break-all;'>{receipt_info['signature_hash']}</span>"

                # Attached Supporting Documents for Won Legs
                won_docs = []
                seen_doc_paths = set()
                from app.core.ai_integration import generate_signed_gcs_url
                for leg in won_legs:
                    l_idx = leg.get("leg_index")
                    l_id = str(leg.get("id") or leg.get("leg_id") or "")
                    l_pair = leg.get("currency_pair") or f"{leg.get('buy_currency')}/{leg.get('sell_currency')}"
                    l_docs = rfq.get_documents_for_leg(leg_index=l_idx, leg_id=l_id, pair=l_pair)
                    for d in l_docs:
                        p_val = d.get("path")
                        if p_val and p_val not in seen_doc_paths:
                            seen_doc_paths.add(p_val)
                            p_signed = p_val
                            if str(p_val).startswith("gs://"):
                                try:
                                    signed = await generate_signed_gcs_url(p_val, expiration=604800)
                                    p_signed = signed or p_val
                                except Exception:
                                    pass
                            won_docs.append({
                                "name": d.get("name") or "Document",
                                "path": p_signed,
                                "pair": d.get("pair") or leg.get("currency_pair")
                            })
                if won_docs:
                    doc_html_links = []
                    for wd in won_docs:
                        pair_str = f" <span style='color: #64748b; font-size: 11px;'>({wd['pair']})</span>" if wd.get('pair') else ""
                        doc_html_links.append(f"<a href='{wd['path']}' target='_blank' style='color: #0284c7; text-decoration: underline; font-weight: 600;'>📄 {wd['name']}</a>{pair_str}")
                    key_vals["Trade Supporting Documents"] = "<br/>".join(doc_html_links)

                if is_partial and lost_legs:
                    for i, leg in enumerate(lost_legs):
                        b_res = next((r for r in leg.get("results", []) if r.get("bank_id") == b_id), {})
                        p_str = f"{b_res.get('price', 0):.5f}" if b_res.get('price') is not None else "No Quote"
                        pair_label = leg.get('currency_pair') or f"{leg.get('buy_currency')}/{leg.get('sell_currency')}"
                        key_vals[f"Unselected Leg ({pair_label})"] = (
                            f"<span style='color: #64748b;'>Your Quote: {p_str} &bull; Concluded &bull; Not Selected</span>"
                        )

                body = build_transaction_email_html(
                    customer_name=customer_name,
                    title="📈 Multi-Currency Trade Execution Confirmation",
                    transaction_ref=ref_no,
                    transaction_type="RFQ Multi-Leg Execution",
                    key_value_dict=key_vals,
                    summary_text=summary_text,
                    recipient_name=f"{b_info['bank_name']} Treasury Desk"
                )
            elif lost_legs:
                # Regret notification email
                subject = f"RFQ Outcome Notification: RFQ {ref_no} ({customer_name}) - Multi-Currency Package"
                key_vals = {
                    "RFQ Reference": ref_no,
                    "Requesting Legal Entity": customer_name,
                    "Participating Package Legs": f"{len(lost_legs)} Currency Pairs",
                    "Deal Status": "<span style='color: #64748b; font-weight: 700;'>Concluded &bull; Not Selected</span>"
                }
                for i, leg in enumerate(lost_legs):
                    b_res = next((r for r in leg.get("results", []) if r.get("bank_id") == b_id), {})
                    p_str = f"{b_res.get('price', 0):.5f}" if b_res.get('price') is not None else "No Quote Submitted"
                    pair_label = leg.get('currency_pair') or f"{leg.get('buy_currency')}/{leg.get('sell_currency')}"
                    key_vals[f"Leg #{i+1} ({pair_label})"] = f"Your Quote: {p_str} &bull; Concluded"

                body = build_transaction_email_html(
                    customer_name=customer_name,
                    title="RFQ Concluded - Trade Outcome Notification",
                    transaction_ref=ref_no,
                    transaction_type="RFQ Outcome",
                    key_value_dict=key_vals,
                    summary_text=f"Thank you for submitting your quotation for RFQ <strong>{ref_no}</strong> with <strong>{customer_name}</strong>. We are writing to inform you that this quotation request has concluded and your offer was not selected for trade execution on this occasion. We appreciate your prompt participation and look forward to collaborating on future transactions.",
                    recipient_name=f"{b_info['bank_name']} Treasury Desk"
                )
            else:
                continue

            await send_email(
                db=db,
                to_emails=b_info["bank_emails"],
                subject_template=subject,
                body_template=body,
                template_data={},
                email_settings=email_settings,
                sender_name=sender_name
            )
            emails_dispatched += 1

    else:
        # Single-Ticket execution
        for bank_res in results:
            q_base = (bank_res.get('quotation_base') or rfq.quotation_base or 'Execution').lower()
            if q_base == 'indicative':
                continue

            bank_emails = []
            if bank_res.get('contacts') and isinstance(bank_res['contacts'], list):
                bank_emails = [c.get('email', '').strip() for c in bank_res['contacts'] if c.get('email')]
            if not bank_emails and bank_res.get('bank_emails'):
                bank_emails = [e.strip() for e in bank_res['bank_emails'].split(',') if e.strip()]

            if not bank_emails:
                continue

            is_winner = (bank_res['bank_id'] == winner_bank_id) and (rfq.status not in ('REJECTED', 'CANCELLED'))
            dealer_identity = bank_res.get('submitted_by_email') or "Authorized Execution Dealer"
            sub_time_str = "N/A"
            if bank_res.get('submitted_at'):
                try:
                    sub_time_str = bank_res['submitted_at'].strftime("%d %b %Y, %H:%M:%S UTC")
                except Exception:
                    sub_time_str = str(bank_res['submitted_at'])

            executed_val_date = bank_res.get('offered_value_date') or bank_res.get('assigned_value_date') or str(rfq.value_date) or 'Standard Spot'

            if is_winner:
                from app.core.otp_security import generate_scoped_deal_receipt
                single_leg_data = [{
                    "pair": f"{rfq.buy_currency}/{rfq.sell_currency}",
                    "direction": rfq.direction,
                    "amount": float(rfq.amount),
                    "currency": rfq.buy_currency,
                    "rate": float(bank_res['price']),
                    "value_date": executed_val_date
                }]
                exec_timestamp = sub_time_str if sub_time_str != "N/A" else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                receipt_info = generate_scoped_deal_receipt(
                    rfq_id=str(rfq.id),
                    ref_no=ref_no,
                    customer_name=customer_name,
                    bank_id=bank_res['bank_id'],
                    bank_name=bank_res['bank_name'],
                    executed_legs=single_leg_data,
                    executed_at=exec_timestamp
                )
                single_key_vals = {
                    "RFQ Reference": ref_no,
                    "Requesting Legal Entity": customer_name,
                    "Pair": f"{rfq.buy_currency}/{rfq.sell_currency}",
                    "Direction": rfq.direction,
                    "Amount": f"{rfq.amount:,.2f} {rfq.buy_currency}",
                    "Executed Rate": f"<span style='color: #16a34a; font-weight: 700;'>{bank_res['price']:.5f}</span>",
                    "All-In Effective Rate": f"{bank_res['finalPrice']:.5f}",
                    "Settlement Value Date": executed_val_date,
                    "Confirmed / Executed By": f"<span style='color: #0f172a; font-weight: 700;'>{dealer_identity}</span>",
                    "Execution Timestamp": sub_time_str,
                    "Deal Execution Receipt": f"<span style='font-family: monospace; font-size: 11px; background: #f1f5f9; padding: 2px 6px; border-radius: 4px; color: #0f172a; font-weight: 700;'>{receipt_info['receipt_id']}</span>",
                    "Cryptographic Signature": f"<span style='font-family: monospace; font-size: 10px; color: #334155; word-break: break-all;'>{receipt_info['signature_hash']}</span>",
                    "Outcome Status": "<span style='color: #16a34a; font-weight: 700;'>🏆 Awarded &amp; Executed</span>"
                }

                single_docs = rfq.get_parsed_documents()
                if single_docs:
                    from app.core.ai_integration import generate_signed_gcs_url
                    doc_html_links = []
                    for sd in single_docs:
                        p_val = sd.get("path")
                        p_signed = p_val
                        if str(p_val).startswith("gs://"):
                            try:
                                signed = await generate_signed_gcs_url(p_val, expiration=604800)
                                p_signed = signed or p_val
                            except Exception:
                                pass
                        doc_html_links.append(f"<a href='{p_signed}' target='_blank' style='color: #0284c7; text-decoration: underline; font-weight: 600;'>📄 {sd.get('name') or 'Document'}</a>")
                    single_key_vals["Trade Supporting Documents"] = "<br/>".join(doc_html_links)

                subject = f"TRADE EXECUTION CONFIRMED: RFQ {ref_no} ({customer_name}) - {rfq.buy_currency}/{rfq.sell_currency}"
                body = build_transaction_email_html(
                    customer_name=customer_name,
                    title="📈 Trade Execution Confirmation",
                    transaction_ref=ref_no,
                    transaction_type="RFQ Execution",
                    key_value_dict=single_key_vals,
                    summary_text=f"We are pleased to confirm the execution of the trade with <strong>{customer_name}</strong> based on your winning quote.",
                    recipient_name=f"{bank_res['bank_name']} Treasury Desk"
                )
            else:
                subject = f"RFQ Outcome Notification: RFQ {ref_no} ({customer_name}) - {rfq.buy_currency}/{rfq.sell_currency}"
                quote_display = f"{bank_res['price']:.5f}" if bank_res.get('price') is not None else "No Quote Submitted"
                body = build_transaction_email_html(
                    customer_name=customer_name,
                    title="RFQ Concluded - Trade Outcome Notification",
                    transaction_ref=ref_no,
                    transaction_type="RFQ Outcome",
                    key_value_dict={
                        "RFQ Reference": ref_no,
                        "Requesting Legal Entity": customer_name,
                        "Pair": f"{rfq.buy_currency}/{rfq.sell_currency}",
                        "Direction": rfq.direction,
                        "Amount": f"{rfq.amount:,.2f} {rfq.buy_currency}",
                        "Target Value Date": executed_val_date,
                        "Your Submitted Quote": quote_display,
                        "Deal Status": "<span style='color: #64748b; font-weight: 700;'>Concluded &bull; Not Selected</span>"
                    },
                    summary_text=f"Thank you for submitting your quotation for RFQ <strong>{ref_no}</strong> ({rfq.buy_currency}/{rfq.sell_currency}) with <strong>{customer_name}</strong>. We are writing to inform you that this quotation request has concluded and your offer was not selected for trade execution on this occasion. We appreciate your prompt participation and look forward to collaborating on future transactions.",
                    recipient_name=f"{bank_res['bank_name']} Treasury Desk"
                )

            await send_email(
                db=db,
                to_emails=bank_emails,
                subject_template=subject,
                body_template=body,
                template_data={},
                email_settings=email_settings,
                sender_name=sender_name
            )
            emails_dispatched += 1

    # Finalize or record audit log for idempotency
    if audit_entry:
        audit_entry.details = {
            "rfq_id": str(rfq.id),
            "ref_no": rfq.ref_no,
            "winner_bank_id": winner_bank_id,
            "emails_count": emails_dispatched,
            "is_multi_leg": is_multi_leg,
            "status": "completed",
            "dispatched_at": datetime.now(timezone.utc).isoformat()
        }
        db.commit()
    else:
        log_action(
            db,
            user_id=rfq.created_by_user_id,
            action_type="QUOTATION_RESULTS_SENT",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "ref_no": rfq.ref_no,
                "winner_bank_id": winner_bank_id,
                "emails_count": emails_dispatched,
                "is_multi_leg": is_multi_leg,
                "status": "completed",
                "dispatched_at": datetime.now(timezone.utc).isoformat()
            },
            customer_id=rfq.customer_id
        )
        db.commit()

    return {"status": "sent", "dispatched_count": emails_dispatched, "winner_bank_id": winner_bank_id}

def _run_auto_dispatch(rfq_id: str):
    from app.database import SessionLocal
    db_local = SessionLocal()
    try:
        from app.models.models import AuditLog
        already_sent = db_local.query(AuditLog).filter(
            AuditLog.action_type == "QUOTATION_RESULTS_SENT",
            AuditLog.entity_type == "QuotationRequest"
        ).filter(
            AuditLog.details["rfq_id"].astext == str(rfq_id)
        ).first()
        if already_sent:
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(dispatch_rfq_result_emails(rfq_id, db_local, force=False))
        loop.close()
    except Exception as err:
        logger.warning(f"Auto-dispatching result emails failed for RFQ {rfq_id}: {err}")
    finally:
        db_local.close()

def trigger_auto_dispatch_results(rfq_id: str):
    with _DISPATCH_LOCK:
        if rfq_id in _DISPATCHING_RFQS:
            return
    thread = threading.Thread(target=_run_auto_dispatch, args=(rfq_id,))
    thread.daemon = True
    thread.start()

@router.post("/{rfq_id}/send-results")
async def send_rfq_results(
    rfq_id: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Sends or resends winner/regret emails to assigned Execution banks for a completed RFQ."""
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")

    res = await dispatch_rfq_result_emails(rfq_id, db, force=True)
    if res.get("status") == "error":
        raise HTTPException(status_code=400, detail=res.get("detail", "Error sending results"))
    if res.get("status") == "skipped":
        raise HTTPException(status_code=400, detail=res.get("detail", "Cannot send deal result emails: No conclusive winner found."))

    return {"message": f"Result emails sent to participating banks ({res.get('dispatched_count', 0)} emails sent)."}

@router.post("/{rfq_id}/resend-invite/{quotation_bank_id}")
async def resend_rfq_bank_invite(
    rfq_id: str,
    quotation_bank_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Resends the secure invite email to a specific bank for an RFQ."""
    rfq = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")
        
    assignment = db.query(QuotationBankAssignment).filter(
        QuotationBankAssignment.rfq_id == rfq.id,
        QuotationBankAssignment.quotation_bank_id == quotation_bank_id
    ).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Bank assignment not found for this RFQ")
        
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == quotation_bank_id).first()
    if not q_bank or not q_bank.emails:
        raise HTTPException(status_code=400, detail="No email address configured for this bank")
        
    email_settings, _ = get_customer_email_settings(db, rfq.customer_id)
    from app.core.routing import get_frontend_base_url
    base_url = get_frontend_base_url(request=request)
    bank_emails = [e.strip() for e in q_bank.emails.split(',') if e.strip()]
    link = f"{base_url}/public-quotation/{assignment.token}"
    
    from app.services.unified_email_builder import build_quotation_rfq_bank_email
    customer_branding = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else "Treasury Customer")
    bank_display_name = q_bank.bank.name if q_bank.bank else "Bank Partner"
    
    subject, body = build_quotation_rfq_bank_email(
        rfq=rfq,
        assignment=assignment,
        bank_name=bank_display_name,
        customer_branding=customer_branding,
        link=link,
        email_purpose="REMINDER"
    )
    background_tasks.add_task(
        send_email,
        db,
        bank_emails,
        subject,
        body,
        {},
        email_settings,
    )
    return {"message": f"Invitation email resent to {bank_display_name}"}

@router.get("/notifications")
def get_my_notifications(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Fetches the 20 most recent notifications for the logged-in user (decommissioned)."""
    return []

@router.patch("/notifications/{notification_id}/read")
def mark_notification_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks a specific notification as read (decommissioned)."""
    return {"message": "Notification marked as read"}

@router.get("/export-csv")
def export_quotations_csv(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Exports a detailed CSV report containing all RFQs and every bank quote submitted."""
    rfqs = crud_quotation.get_requests(db, customer_id=current_user.customer_id)
    
    output = io.BytesIO()
    output.write(b'\xef\xbb\xbf')
    text_wrapper = io.TextIOWrapper(output, encoding='utf-8', newline='')
    writer = csv.writer(text_wrapper, lineterminator='\r\n')
    
    # Write detailed headers
    writer.writerow([
        "RFQ Reference", "Type", "Direction", "Amount", "Buy Currency", "Sell Currency",
        "Value Date", "RFQ Status", "Master Quotation Base", "Max Tolerance %",
        "Bank Name", "Bank Base Type", "Document Visible", "Submitted Base Rate",
        "Bank Fee Total", "All-In Effective Rate", "Submission Time", "Is Winner",
        "Created By", "Created At"
    ])
    
    for rfq in rfqs:
        try:
            res_data = get_rfq_results(rfq.id, db, current_user)
            results = res_data.get("results", [])
            winner_bank_id = res_data.get("winner_bank_id")
            creator_name = (rfq.creator.email.split('@')[0] if (rfq.creator and rfq.creator.email) else getattr(rfq, 'creator_name', 'End User')) or "End User"
            
            created_at_str = rfq.created_at
            if created_at_str and hasattr(created_at_str, 'isoformat'):
                created_at_str = created_at_str.isoformat()
            else:
                created_at_str = str(created_at_str or "")

            legs_data = res_data.get("legs", [])
            if legs_data and len(legs_data) > 1:
                for leg in legs_data:
                    leg_results = leg.get("results", [])
                    leg_winner_id = leg.get("winner_bank_id")
                    if not leg_results:
                        writer.writerow([
                            rfq.ref_no, f"FX_PORTFOLIO (Leg {leg.get('leg_index', 1)})", leg.get("direction") or rfq.direction or "", leg.get("amount") or 0,
                            leg.get("buy_currency") or "", leg.get("sell_currency") or "", leg.get("value_date") or "",
                            leg.get("status") or rfq.status, leg.get("quotation_base") or rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                            "No Quotes", "", "", "", "", "", "", "NO",
                            creator_name, created_at_str
                        ])
                    else:
                        for b in leg_results:
                            is_win = "YES" if (leg_winner_id and b.get("bank_id") == leg_winner_id) else "NO"
                            sub_time = b.get("submitted_at")
                            sub_time_str = sub_time.isoformat() if (sub_time and hasattr(sub_time, 'isoformat')) else str(sub_time or "")
                            writer.writerow([
                                rfq.ref_no, f"FX_PORTFOLIO (Leg {leg.get('leg_index', 1)})", leg.get("direction") or rfq.direction or "", leg.get("amount") or 0,
                                leg.get("buy_currency") or "", leg.get("sell_currency") or "", leg.get("value_date") or "",
                                leg.get("status") or rfq.status, leg.get("quotation_base") or rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                                b.get("bank_name", ""), b.get("quotation_base", ""),
                                "YES" if b.get("is_document_visible") else "NO",
                                b.get("price") if b.get("price") is not None else "No Quote",
                                b.get("bank_fee_total") if b.get("bank_fee_total") is not None else "",
                                b.get("finalPrice") if b.get("finalPrice") is not None else "",
                                sub_time_str,
                                is_win, creator_name, created_at_str
                            ])
            elif not results:
                writer.writerow([
                    rfq.ref_no, rfq.type, rfq.direction or "", rfq.amount or 0,
                    rfq.buy_currency or "", rfq.sell_currency or "", rfq.value_date or "",
                    rfq.status, rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                    "No Banks Assigned", "", "", "", "", "", "", "NO",
                    creator_name, created_at_str
                ])
            else:
                for b in results:
                    is_win = "YES" if (winner_bank_id and b.get("bank_id") == winner_bank_id) else "NO"
                    sub_time = b.get("submitted_at")
                    if sub_time and hasattr(sub_time, 'isoformat'):
                        sub_time = sub_time.isoformat()
                    else:
                        sub_time = str(sub_time or "")

                    writer.writerow([
                        rfq.ref_no, rfq.type, rfq.direction or "", rfq.amount or 0,
                        rfq.buy_currency or "", rfq.sell_currency or "", rfq.value_date or "",
                        rfq.status, rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                        b.get("bank_name", ""), b.get("quotation_base", ""),
                        "YES" if b.get("is_document_visible") else "NO",
                        b.get("price") if b.get("price") is not None else "No Quote",
                        b.get("bank_fee_total") if b.get("bank_fee_total") is not None else "",
                        b.get("finalPrice") if b.get("finalPrice") is not None else "",
                        sub_time,
                        is_win, creator_name, created_at_str
                    ])
        except Exception as rfq_err:
            logger.warning(f"Skipping RFQ {rfq.id} in CSV export due to error: {rfq_err}")
            continue

    text_wrapper.flush()
    filename = f"quotation_detailed_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )


@router.get("/delegation-colleagues")
def get_delegation_colleagues(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns active corporate colleagues under the same customer available for deal acceptance delegation.
    Corporate Admins are excluded as they already possess global authority to accept/decline deals."""
    from app.models.models import User
    users = db.query(User).filter(
        User.customer_id == current_user.customer_id,
        User.is_deleted == False
    ).all()

    valid_colleagues = []
    for u in users:
        u_role = (u.role.value if hasattr(u.role, 'value') else str(u.role or '')).lower()
        if u_role in ('corporate_admin', 'super_admin'):
            continue
        if u.id == current_user.user_id:
            continue
        valid_colleagues.append({
            "id": u.id,
            "email": u.email,
            "role": u_role,
            "display_name": f"{getattr(u, 'first_name', '') or ''} {getattr(u, 'last_name', '') or ''}".strip() or u.email
        })

    return valid_colleagues


@router.patch("/{rfq_id}/delegate")
def delegate_deal_acceptance(
    rfq_id: str,
    payload: QuotationDelegateRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Delegates deal acceptance authority for an RFQ to another corporate colleague.
    Only the Maker or a Corporate Admin can delegate.
    """
    from app.services.deal_acceptance_service import execute_deal_delegation
    return execute_deal_delegation(
        rfq_id=rfq_id,
        delegator_user_id=current_user.user_id,
        delegatee_user_id=payload.delegated_to_user_id,
        db=db
    )


@router.post("/{rfq_id}/accept-deal")
async def accept_quotation_deal_enduser(
    rfq_id: str,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Accepts winning quotation deal.
    Accessible by Corporate Admin, Maker (RFQ creator), or designated Delegated Colleague.
    """
    from app.services.deal_acceptance_service import execute_shared_deal_acceptance
    body = {}
    if request:
        try:
            body = await request.json()
        except Exception:
            body = {}

    return await execute_shared_deal_acceptance(
        rfq_id=rfq_id,
        user_id=current_user.user_id,
        accepted_leg_ids=body.get("accepted_leg_ids"),
        declined_leg_ids=body.get("declined_leg_ids"),
        db=db
    )


@router.post("/{rfq_id}/decline-deal")
async def decline_quotation_deal_enduser(
    rfq_id: str,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Declines quotation deal outcome.
    Accessible by Corporate Admin, Maker (RFQ creator), or designated Delegated Colleague.
    """
    from app.services.deal_acceptance_service import execute_shared_deal_decline
    body = {}
    if request:
        try:
            body = await request.json()
        except Exception:
            body = {}

    reason = body.get("reason", "Declined by corporate treasury desk")
    return await execute_shared_deal_decline(
        rfq_id=rfq_id,
        user_id=current_user.user_id,
        reason=reason,
        db=db
    )


@router.get("/active-acceptance-alert")
def get_active_acceptance_alert_enduser(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Polls for active RFQ deals currently awaiting binding corporate acceptance
    for the authenticated end user (Maker or designated Delegate).
    """
    from app.services.deal_acceptance_service import get_active_deal_awaiting_acceptance
    user_role = current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role)
    return get_active_deal_awaiting_acceptance(
        user_id=current_user.user_id,
        customer_id=current_user.customer_id,
        user_role=user_role,
        db=db
    )


