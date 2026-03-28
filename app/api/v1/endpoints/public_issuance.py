# app/api/v1/endpoints/public_issuance.py

from typing import Any, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, UploadFile, File, Body
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.database import get_db
from app.core.encryption import decrypt_data, encrypt_data
from app.crud.crud_issuance import crud_issuance_request
from app.schemas.schemas_issuance import IssuanceRequestCreate, IssuanceRequestOut, CustomerFormConfigurationCreateUpdate, IssuanceRequestDraftCreate
from datetime import datetime, timezone, timedelta
import secrets
import string
import os

# Models
from app.models import Customer, ExternalOTP, CustomerEntity, Currency, LgType, Department
from app.models.models_issuance import CustomerFormConfiguration
from app.crud.crud import log_action, crud_customer_configuration
from app.constants import GlobalConfigKey

router = APIRouter()

class InviteRequest(BaseModel):
    email: str
    department: str = "General"

# --- SECURITY HELPER FOR PORTAL ---

def _check_customer_issuance_module(db: Session, customer_id: int):
    """Verifies that the customer's subscription includes the LG Issuance module."""
    customer = db.query(Customer).filter(Customer.id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found.")
    if customer.subscription_plan and not customer.subscription_plan.has_issuance_module:
        raise HTTPException(
            status_code=403,
            detail="Your organization's subscription does not include the LG Issuance module."
        )
def verify_portal_token(token: str) -> dict:
    try:
        # FastAPI might auto-decode URL characters, but we handle it just in case
        token = token.replace(" ", "+") 
        decrypted_str = decrypt_data(token)
        
        separator = "|" if "|" in decrypted_str else ":"
        parts = decrypted_str.split(separator)
        
        if "PublicDiscovery" in parts:
            # OTP-based token: customer_id|PublicDiscovery|email|expiry_iso
            expiry_str = parts[3] if len(parts) > 3 else None
            if expiry_str:
                expiry_dt = datetime.fromisoformat(expiry_str)
                if expiry_dt < datetime.now(timezone.utc):
                    raise HTTPException(status_code=403, detail="Session has expired. Please verify again.")
            return {
                "customer_id": int(parts[0]),
                "department": "Public Request", 
                "email": parts[2]
            }
        
        # Invite-based token: customer_id|department|email|expiry or customer_id:department:expiry
        expiry_str = parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else None)
        if expiry_str:
            try:
                # Try ISO format first (from generate_portal_link)
                expiry_dt = datetime.fromisoformat(expiry_str)
                if expiry_dt.tzinfo is None:
                    expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
                if expiry_dt < datetime.now(timezone.utc):
                    raise HTTPException(status_code=403, detail="Invite link has expired.")
            except ValueError:
                # Try date-only format (from generate_invite_link: YYYY-MM-DD)
                from datetime import date as date_type
                expiry_date = date_type.fromisoformat(expiry_str)
                if expiry_date < date_type.today():
                    raise HTTPException(status_code=403, detail="Invite link has expired.")

        # Determine email (may be in parts[2] for 4-part tokens)
        email = parts[2] if len(parts) > 3 else None

        return {
            "customer_id": int(parts[0]),
            "department": parts[1],
            "email": email,
            "expiry": expiry_str
        }
        
    except HTTPException:
        raise  # Re-raise our own 403 errors
    except Exception as e:
        print(f"Token Decryption Error: {str(e)}")
        raise HTTPException(status_code=403, detail="Invalid session token.")

# --- ENDPOINTS ---

@router.get("/validate-access")
def validate_portal_access(token: str = Query(...), db: Session = Depends(get_db)):
    data = verify_portal_token(token)
    _check_customer_issuance_module(db, data["customer_id"])
    return {"valid": True, "department": data["department"], "message": "Access granted"}

@router.post("/verify-domain")
def public_verify_domain(
    payload: dict,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    email = payload.get("email", "").lower()
    if "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email format")
    
    domain = email.split("@")[-1]
    # Check if domain is in the customer's domains JSON array
    # Use cast to JSONB + @> containment operator (contains() on JSON columns generates LIKE which fails)
    from sqlalchemy import cast, type_coerce
    from sqlalchemy.dialects.postgresql import JSONB
    customer = db.query(Customer).filter(
        cast(Customer.domains, JSONB).contains([domain]),
        Customer.is_deleted == False
    ).first()
    if not customer:
        log_action(db, None, "PUBLIC_DOMAIN_REJECTED", "PublicAccess", None, {"email": email})
        raise HTTPException(status_code=404, detail="Organization not recognized.")

    otp_code = "".join(secrets.choice(string.digits) for _ in range(6))
    
    new_otp = ExternalOTP(
        email=email,
        otp_code=otp_code,
        customer_id=customer.id,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10)
    )
    db.add(new_otp)
    
    # --- Send OTP email (matching corporate_admin.py pattern exactly) ---
    from app.core.email_service import send_email, get_global_email_settings
    email_settings = get_global_email_settings()
    
    subject = "Your LG Issuance Portal Verification Code"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #1a56db; margin-top: 0;">🔐 Verification Code</h2>
            <p>Your verification code for the LG Issuance Request Portal is:</p>
            <div style="text-align: center; margin: 25px 0;">
                <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #1a56db; background: #f0f4ff; padding: 15px 30px; border-radius: 8px; display: inline-block;">{otp_code}</span>
            </div>
            <p>This code will expire in <strong>10 minutes</strong>.</p>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">If you did not request this code, please ignore this email.</p>
        </div>
    </body>
    </html>
    """
    background_tasks.add_task(
        send_email,
        db,
        [email],
        subject,
        body,
        {},
        email_settings,
    )
    print(f"[DEBUG EMAIL] verify-domain: OTP email queued for {email}")
    
    log_action(db, None, "EXTERNAL_OTP_SENT", "OTP", None, {"email": email}, customer.id)
    return {"message": "Verification code sent."}

@router.post("/verify-otp")
async def public_verify_otp(payload: dict, db: Session = Depends(get_db)):
    email = payload.get("email", "").lower()
    otp_code = payload.get("otp", "")

    # --- OTP Rate Limiting (brute-force protection) ---
    # Configurable thresholds via GlobalConfig (defaults: 5 attempts, 15 min lockout)
    max_attempts = 5
    lockout_minutes = 15
    try:
        from app.models import GlobalConfiguration
        max_cfg = db.query(GlobalConfiguration).filter(
            GlobalConfiguration.key == GlobalConfigKey.OTP_MAX_FAILED_ATTEMPTS
        ).first()
        if max_cfg and max_cfg.value_default:
            max_attempts = int(max_cfg.value_default)
        lock_cfg = db.query(GlobalConfiguration).filter(
            GlobalConfiguration.key == GlobalConfigKey.OTP_LOCKOUT_DURATION_MINUTES
        ).first()
        if lock_cfg and lock_cfg.value_default:
            lockout_minutes = int(lock_cfg.value_default)
    except Exception:
        db.rollback()

    lockout_window = datetime.now(timezone.utc) - timedelta(minutes=lockout_minutes)

    # Count recent failed OTP attempts for this email (verified=False and created within window)
    recent_failures = db.query(func.count(ExternalOTP.id)).filter(
        ExternalOTP.email == email,
        ExternalOTP.is_verified == False,
        ExternalOTP.created_at >= lockout_window,
    ).scalar() or 0

    if recent_failures >= max_attempts:
        log_action(db, None, "OTP_RATE_LIMITED", "OTP", None, {"email": email, "attempts": recent_failures})
        raise HTTPException(
            status_code=429,
            detail=f"Too many failed attempts. Please try again in {lockout_minutes} minutes."
        )

    record = db.query(ExternalOTP).filter(
        ExternalOTP.email == email,
        ExternalOTP.otp_code == otp_code,
        ExternalOTP.expires_at > datetime.now(timezone.utc),
        ExternalOTP.is_verified == False
    ).first()

    if not record:
        log_action(db, None, "OTP_FAILED", "OTP", None, {"email": email})
        raise HTTPException(status_code=400, detail="Invalid or expired code.")

    record.is_verified = True
    db.commit()

    # Configurable session expiry (default 60 minutes)
    session_minutes = 60
    try:
        cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, record.customer_id, GlobalConfigKey.PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES
        )
        if cfg and cfg.get('effective_value'):
            session_minutes = int(cfg['effective_value'])
    except Exception:
        # A failed DB query leaves the session in a broken state;
        # rollback so subsequent operations (audit log) can proceed.
        db.rollback()
    expiry = (datetime.now(timezone.utc) + timedelta(minutes=session_minutes)).isoformat()
    payload_str = f"{record.customer_id}|PublicDiscovery|{email}|{expiry}"
    token = encrypt_data(payload_str)

    log_action(db, None, "OTP_SUCCESS", "OTP", record.id, {"email": email}, record.customer_id)
    return {"token": token, "message": "Access granted"}

@router.post("/generate-invite")
def generate_invite_link(
    payload: InviteRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    email = payload.email
    department = payload.department or "General"
    
    full_domain = email.split("@")[-1]
    from sqlalchemy import cast
    from sqlalchemy.dialects.postgresql import JSONB
    customer = db.query(Customer).filter(
        cast(Customer.domains, JSONB).contains([full_domain]),
        Customer.is_deleted == False
    ).first()
    
    if not customer:
        raise HTTPException(status_code=404, detail="Customer domain not found")

    # Configurable invite link expiry (default 168 hours = 7 days)
    invite_hours = 168
    try:
        cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer.id, GlobalConfigKey.INVITE_LINK_EXPIRY_HOURS
        )
        if cfg and cfg.get('effective_value'):
            invite_hours = int(cfg['effective_value'])
    except Exception:
        pass
    expiry = (datetime.now() + timedelta(hours=invite_hours)).strftime("%Y-%m-%d")
    raw_token = f"{customer.id}|{department}|{email}|{expiry}"
    encrypted_token = encrypt_data(raw_token)
    
    frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")
    invite_link = f"{frontend_url}/public-issuance/form?token={encrypted_token}"
    
    # --- Send invite email (matching corporate_admin.py create_user pattern exactly) ---
    from app.core.email_service import send_email, get_global_email_settings
    email_settings = get_global_email_settings()
    print(f"[DEBUG EMAIL] generate_invite: to={email}, host={email_settings.smtp_host}, port={email_settings.smtp_port}")
    
    subject = "LG Issuance Request — Your Access Link"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #1a56db; margin-top: 0;">📋 LG Issuance Request Portal</h2>
            <p>You have been invited to submit a Letter of Guarantee issuance request.</p>
            <p><strong>Department:</strong> {department}</p>
            <p>Please click the button below to access the request form. This link is valid for 7 days.</p>
            <div style="text-align: center; margin: 25px 0;">
                <a href="{invite_link}" style="padding: 14px 35px; background: #1a56db; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block; font-size: 16px;">Open Request Form</a>
            </div>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated invitation from your Treasury LG Issuance system. If you did not expect this email, please ignore it.</p>
        </div>
    </body>
    </html>
    """
    background_tasks.add_task(
        send_email,
        db,
        [email],
        subject,
        body,
        {},
        email_settings,
    )
    print(f"[DEBUG EMAIL] generate_invite: background_task added for {email}")
    
    return {"invite_link": invite_link}

@router.post("/submit", response_model=IssuanceRequestOut)
def public_submit_request(
    request_in: IssuanceRequestCreate,
    token: str = Query(...),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
):
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    # Only use token department as fallback — respect the user's choice from the form
    if not request_in.department:
        request_in.department = access_data.get("department", "General")

    request = crud_issuance_request.create_request(
        db, obj_in=request_in, customer_id=customer_id, user_id=None
    )
    
    # Use unified submit flow: creates V1 snapshot + runs approval matrix
    from app.services.issuance_service import issuance_service
    submitted_request = issuance_service.submit_for_approval(
        db, request.id, user_id=None
    )
    
    print(f"[DEBUG EMAIL] public_submit: status={submitted_request.status}, approvers={submitted_request.pending_approver_users}, bg_tasks={background_tasks is not None}")
    
    # --- Send email to approvers (matching corporate_admin.py create_user pattern exactly) ---
    if background_tasks and submitted_request.status == "PENDING_APPROVAL" and submitted_request.pending_approver_users:
        from app.core.email_service import send_email, get_global_email_settings
        from app.services.issuance_notifications import _get_user_emails
        
        email_settings = get_global_email_settings()
        approver_ids = [int(uid) for uid in submitted_request.pending_approver_users]
        approver_emails = _get_user_emails(db, approver_ids)
        currency = submitted_request.currency.iso_code if submitted_request.currency else "N/A"
        frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")
        
        print(f"[DEBUG EMAIL] public_submit: approver_ids={approver_ids}, emails={approver_emails}, host={email_settings.smtp_host}")
        
        if approver_emails:
            subject = f"ACTION REQUIRED: LG Request {submitted_request.serial_number} Awaiting Approval"
            body = f"""
            <html>
            <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <h2 style="color: #1a56db; margin-top: 0;">🔔 New Approval Request</h2>
                    <p>A new LG issuance request has been submitted and requires your approval.</p>
                    <div style="background: #f8fafc; border-left: 4px solid #1a56db; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{submitted_request.serial_number}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {submitted_request.amount}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{submitted_request.beneficiary_name}</td></tr>
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
            print(f"[DEBUG EMAIL] public_submit: background_task added for {approver_emails}")
    
    return submitted_request


@router.post("/save-draft", response_model=IssuanceRequestOut)
def public_save_draft(
    request_in: IssuanceRequestDraftCreate,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Save an issuance request as DRAFT without submitting for approval."""
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    # Only use token department as fallback — respect the user's choice from the form
    if not request_in.department:
        request_in.department = access_data.get("department", "General")

    draft = crud_issuance_request.create_request(
        db, obj_in=request_in, customer_id=customer_id, user_id=None
    )
    # No submission — stays as DRAFT
    return draft


@router.get("/requests/{request_id}")
def public_get_draft(
    request_id: int,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Public: Get a single request by ID (for editing drafts)."""
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy.orm import joinedload

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    request = db.query(IssuanceRequest).options(
        joinedload(IssuanceRequest.currency),
        joinedload(IssuanceRequest.lg_type),
        joinedload(IssuanceRequest.issuing_entity),
    ).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.is_deleted == False,
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    
    # Only the original requestor can view their own request
    if email and request.requestor_email and request.requestor_email.lower() != email.lower():
        raise HTTPException(status_code=403, detail="Not authorized to view this request")

    return request


@router.put("/requests/{request_id}")
def public_update_draft(
    request_id: int,
    request_in: dict,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Public: Update an existing DRAFT request. Accepts raw dict to avoid
    validation errors when partially saving incomplete drafts."""
    from app.models.models_issuance import IssuanceRequest

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.is_deleted == False,
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    if request.status not in ("DRAFT", "REVISION_REQUIRED"):
        raise HTTPException(status_code=400, detail="Only DRAFT or REVISION_REQUIRED requests can be updated")
    if email and request.requestor_email and request.requestor_email.lower() != email.lower():
        raise HTTPException(status_code=403, detail="Not authorized to update this request")

    # Accept raw dict — skip Pydantic validation for drafts (no business rules enforced)
    ALLOWED_FIELDS = {
        'issuing_entity_id', 'requestor_name', 'requestor_email', 'department',
        'job_title', 'phone_number', 'employee_id', 'manager_email', 'second_line_manager_email',
        'reference_type', 'reference_number', 'reference_amount', 'reference_currency_id',
        'reference_start_date', 'reference_end_date', 'project_id',
        'lg_type_id', 'lg_purpose', 'amount', 'currency_id', 'payable_currency_id',
        'requested_issue_date', 'requested_expiry_date', 'operational_status',
        'lg_language', 'is_auto_reducing', 'reduction_trigger', 'other_conditions',
        'beneficiary_id_number', 'beneficiary_name', 'beneficiary_address',
        'beneficiary_contact_person', 'beneficiary_phone', 'beneficiary_email',
        'beneficiary_country', 'is_third_party', 'third_party_name', 'third_party_address',
        'third_party_relationship', 'is_cross_border', 'issuance_country', 'applicable_rules',
        'cross_border_details', 'requires_special_wording',
        'is_urgent', 'urgency_justification', 'comments',
        'custom_field_1_value', 'custom_field_2_value'
    }
    for key, value in request_in.items():
        if key in ALLOWED_FIELDS and hasattr(request, key):
            setattr(request, key, value)

    # Capture change_reason for RETURNED_FOR_REVISION requests
    # Store it on the request itself so submit_for_approval can read it.
    # We store it in a dedicated JSONB field on the request's approval_chain_audit
    # as a pending_change_reason entry that submit_for_approval will consume.
    change_reason = request_in.get("change_reason")
    if change_reason and request.status == "REVISION_REQUIRED":
        from sqlalchemy.orm.attributes import flag_modified
        current_audit = list(request.approval_chain_audit or [])
        current_audit.append({
            "action": "EDIT_REASON_PENDING",
            "reason": change_reason,
            "user_name": request.requestor_name or request.requestor_email,
            "timestamp": str(date.today())
        })
        request.approval_chain_audit = list(current_audit)
        flag_modified(request, 'approval_chain_audit')

    db.commit()
    db.refresh(request)
    return request


@router.post("/requests/{request_id}/submit")
def public_submit_existing_draft(
    request_id: int,
    token: str = Query(...),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
):
    """Public: Submit an existing DRAFT request for approval (instead of creating a new one)."""
    from app.models.models_issuance import IssuanceRequest

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.is_deleted == False,
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    if request.status not in ("DRAFT", "REVISION_REQUIRED"):
        raise HTTPException(status_code=400, detail="Only DRAFT or REVISION_REQUIRED requests can be submitted")
    if email and request.requestor_email and request.requestor_email.lower() != email.lower():
        raise HTTPException(status_code=403, detail="Not authorized to submit this request")

    # Use unified submit flow: creates V1 snapshot + runs approval matrix
    from app.services.issuance_service import issuance_service
    submitted_request = issuance_service.submit_for_approval(
        db, request.id, user_id=None
    )

    # --- Send email to approvers ---
    if background_tasks and submitted_request.status == "PENDING_APPROVAL" and submitted_request.pending_approver_users:
        from app.core.email_service import send_email, get_global_email_settings
        from app.services.issuance_notifications import _get_user_emails

        email_settings = get_global_email_settings()
        approver_ids = [int(uid) for uid in submitted_request.pending_approver_users]
        approver_emails = _get_user_emails(db, approver_ids)
        currency = submitted_request.currency.iso_code if submitted_request.currency else "N/A"
        frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")

        if approver_emails:
            subject = f"ACTION REQUIRED: LG Request {submitted_request.serial_number} Awaiting Approval"
            body = f"""
            <html>
            <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <h2 style="color: #1a56db; margin-top: 0;">🔔 New Approval Request</h2>
                    <p>A new LG issuance request has been submitted and requires your approval.</p>
                    <div style="background: #f8fafc; border-left: 4px solid #1a56db; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{submitted_request.serial_number}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency} {submitted_request.amount}</td></tr>
                            <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{submitted_request.beneficiary_name}</td></tr>
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
            background_tasks.add_task(send_email, db, approver_emails, subject, body, {}, email_settings)

    return submitted_request


@router.delete("/requests/{request_id}")
def public_delete_draft(
    request_id: int,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Public: Delete a DRAFT request."""
    from app.models.models_issuance import IssuanceRequest

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.is_deleted == False,
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    if request.status != "DRAFT":
        raise HTTPException(status_code=400, detail="Only DRAFT requests can be deleted")
    if email and request.requestor_email and request.requestor_email.lower() != email.lower():
        raise HTTPException(status_code=403, detail="Not authorized to delete this request")

    # Soft delete (preserves audit trail)
    request.is_deleted = True
    db.commit()
    return {"message": "Draft deleted"}


@router.get("/previous-requestor")
def public_get_previous_requestor(
    token: str = Query(...),
    email: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Looks up the most recent issuance request from a given email.
    Returns common auto-fill data (job_title, employee_id, phone, etc.).
    """
    from app.models.models_issuance import IssuanceRequest
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]

    prev = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.requestor_email == email.lower()
    ).order_by(IssuanceRequest.id.desc()).first()

    if not prev:
        return {"found": False}

    return {
        "found": True,
        "requestor_name": prev.requestor_name,
        "department": prev.department,
        "job_title": prev.job_title,
        "phone_number": prev.phone_number,
        "employee_id": prev.employee_id,
        "manager_email": prev.manager_email,
        "second_line_manager_email": prev.second_line_manager_email,
        "issuing_entity_id": prev.issuing_entity_id
    }


# --- REQUESTOR DASHBOARD: My Requests ---

@router.get("/my-requests")
def public_get_my_requests(
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Returns all issuance requests submitted by this requestor (identified by email from token)."""
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy.orm import joinedload
    
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")
    
    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")
    
    requests = db.query(IssuanceRequest).options(
        joinedload(IssuanceRequest.lg_type),
        joinedload(IssuanceRequest.currency),
        joinedload(IssuanceRequest.issuing_entity),
        joinedload(IssuanceRequest.lg_record),
    ).filter(
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.requestor_email == email.lower(),
        IssuanceRequest.is_deleted == False,
    ).order_by(IssuanceRequest.created_at.desc()).all()
    
    return {
        "email": email,
        "total": len(requests),
        "requests": [
            {
                "id": r.id,
                "serial_number": r.serial_number,
                "status": r.status,
                "beneficiary_name": r.beneficiary_name,
                "amount": str(r.amount) if r.amount else None,
                "currency": r.currency.iso_code if r.currency else None,
                "lg_type": r.lg_type.name if r.lg_type else None,
                "entity_name": r.issuing_entity.entity_name if r.issuing_entity else None,
                "requested_issue_date": str(r.requested_issue_date) if r.requested_issue_date else None,
                "requested_expiry_date": str(r.requested_expiry_date) if r.requested_expiry_date else None,
                "is_urgent": r.is_urgent,
                "revision_notes": r.revision_notes,
                "lg_ref_number": r.lg_record.lg_ref_number if r.lg_record else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
            for r in requests
        ]
    }


# --- NEW ENDPOINTS FOR THE DYNAMIC FORM ---

@router.get("/form-config", response_model=CustomerFormConfigurationCreateUpdate)
def public_get_form_config(token: str = Query(...), db: Session = Depends(get_db)):
    """Allows the public form to safely render the customer's specific layout."""
    access_data = verify_portal_token(token)
    
    config = db.query(CustomerFormConfiguration).filter(
        CustomerFormConfiguration.customer_id == access_data["customer_id"]
    ).first()
    
    if not config:
        return CustomerFormConfigurationCreateUpdate()
        
    return CustomerFormConfigurationCreateUpdate(
        field_configurations=config.field_configurations,
        custom_field_1_config=config.custom_field_1_config,
        custom_field_2_config=config.custom_field_2_config,
        mandatory_document_types=config.mandatory_document_types,
        reference_types=config.reference_types,
        document_config=config.document_config
    )

@router.get("/dictionaries")
def public_get_dictionaries(token: str = Query(...), db: Session = Depends(get_db)):
    """Bundles entities, currencies, LG types, departments, and email for the public form."""
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    entities = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == customer_id, 
        CustomerEntity.is_active == True, 
        CustomerEntity.is_deleted == False
    ).all()
    
    currencies = db.query(Currency).all()
    lg_types = db.query(LgType).all()
    
    departments = db.query(Department).filter(
        Department.customer_id == customer_id,
        Department.is_deleted == False
    ).all()

    from app.models.models_issuance import CorporateProject
    projects = db.query(CorporateProject).filter(
        CorporateProject.customer_id == customer_id,
        CorporateProject.status == 'ACTIVE'
    ).order_by(CorporateProject.name).all()
    
    return {
        "entities": [{"id": e.id, "name": e.entity_name} for e in entities],
        "currencies": [{"id": c.id, "name": c.name, "iso_code": c.iso_code} for c in currencies],
        "lgTypes": [{"id": t.id, "name": t.name} for t in lg_types],
        "departments": [{"id": d.name, "name": d.name} for d in departments],
        "projects": [{"id": p.id, "name": p.name, "project_type": p.project_type, "reference_number": p.reference_number, "status": p.status} for p in projects],
        "department": access_data["department"],
        "email": access_data.get("email")
    }


# --- BENEFICIARY LOOKUP (PUBLIC PORTAL) ---

@router.get("/beneficiary-lookup")
def public_beneficiary_lookup(
    token: str = Query(...),
    id_number: str = Query(...),
    db: Session = Depends(get_db)
):
    """Lookup beneficiary by ID/number for public portal users."""
    from app.models.models_issuance import IssuanceRequest
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == customer_id,
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


@router.get("/check-duplicate-reference")
def public_check_duplicate_reference(
    token: str = Query(...),
    reference_type: str = Query(...),
    reference_number: str = Query(...),
    exclude_id: Optional[int] = Query(None, description="Request ID to exclude from duplicate check (self)"),
    db: Session = Depends(get_db)
):
    """Check if a request with the same reference type + number already exists (public portal)."""
    from app.models.models_issuance import IssuanceRequest
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    # Check existing requests (case-insensitive)
    query = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == customer_id,
        func.lower(IssuanceRequest.reference_type) == reference_type.lower(),
        func.lower(IssuanceRequest.reference_number) == reference_number.strip().lower(),
        IssuanceRequest.is_deleted == False
    )
    # Exclude the current request when editing a draft
    if exclude_id:
        query = query.filter(IssuanceRequest.id != exclude_id)
    
    request_matches = query.order_by(IssuanceRequest.created_at.desc()).limit(3).all()
    
    # Also check against issued LGs (via their linked request's reference)
    # Exclude LGs whose parent request is already in request_matches (avoid double-counting)
    from app.models.models_issuance import IssuedLGRecord
    matched_request_ids = {m.id for m in request_matches}
    lg_query = db.query(IssuedLGRecord).join(
        IssuanceRequest, IssuedLGRecord.request_id == IssuanceRequest.id
    ).filter(
        IssuedLGRecord.customer_id == customer_id,
        func.lower(IssuanceRequest.reference_type) == reference_type.lower(),
        func.lower(IssuanceRequest.reference_number) == reference_number.strip().lower(),
    )
    if matched_request_ids:
        lg_query = lg_query.filter(IssuedLGRecord.request_id.notin_(matched_request_ids))
    lg_results = lg_query.limit(3).all()

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
    for lg in lg_results:
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

class PublicPreSubmitSimilarityPayload(BaseModel):
    token: str
    reference_type: Optional[str] = None
    reference_number: Optional[str] = None
    beneficiary_name: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    lg_type_id: Optional[int] = None
    requested_expiry_date: Optional[date] = None
    exclude_request_id: Optional[int] = None

@router.post("/pre-submit-similarity")
def public_pre_submit_similarity_check(
    payload: PublicPreSubmitSimilarityPayload,
    db: Session = Depends(get_db)
):
    """Realtime check against issued LGs and active requests based on form fields (public portal)."""
    access_data = verify_portal_token(payload.token)
    customer_id = access_data["customer_id"]
    
    from app.services.issuance_service import issuance_service
    
    return issuance_service.get_similarity_matches(
        db=db,
        customer_id=customer_id,
        reference_type=payload.reference_type,
        reference_number=payload.reference_number,
        beneficiary_name=payload.beneficiary_name,
        amount=payload.amount,
        currency=payload.currency,
        lg_type_id=payload.lg_type_id,
        requested_expiry_date=payload.requested_expiry_date,
        exclude_request_id=payload.exclude_request_id
    )


@router.get("/beneficiary-suggest")
def public_beneficiary_suggest(
    token: str = Query(...),
    name: str = Query(..., min_length=3),
    db: Session = Depends(get_db)
):
    """Fuzzy match beneficiary names for public portal users."""
    from app.models.models_issuance import IssuanceRequest
    from sqlalchemy import func as sa_func
    from difflib import SequenceMatcher
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    
    # Get all distinct beneficiary names for this customer (limited to recent 200)
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
        IssuanceRequest.customer_id == customer_id,
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
    
    # Score each name using fuzzy similarity
    name_lower = name.lower()
    scored = []
    for m in all_names:
        ben_name = m.beneficiary_name or ''
        ben_lower = ben_name.lower()
        
        if name_lower in ben_lower or ben_lower in name_lower:
            score = 95
        else:
            score = int(SequenceMatcher(None, name_lower, ben_lower).ratio() * 100)
        
        # Lowered to 60 to prevent SequenceMatcher length penalties from dropping near matches
        if score >= 60:
            scored.append((score, m))
    
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


# --- DOCUMENT UPLOAD (PUBLIC PORTAL) ---

@router.post("/requests/{request_id}/documents")
async def public_upload_document(
    request_id: int,
    token: str = Query(...),
    document_type: str = Query(..., description="CONTRACT, PURCHASE_ORDER, THIRD_PARTY, SPECIAL_WORDING, OTHER"),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload a document to a public issuance request using portal token auth."""
    from app.models.models_issuance import IssuanceRequest, IssuanceRequestDocument
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    from app.crud.crud_lg_document import _slugify_doc_type
    import uuid
    from datetime import datetime as dt

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id
    ).first()
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Validate file size (max 10 MB)
    file_content = await file.read()
    if len(file_content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File size exceeds 10MB limit")

    file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
    unique_filename = f"REQ-{request_id}_{document_type}_{dt.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_extension}"

    doc_type_slug = _slugify_doc_type(document_type)
    blob_path = f"customer_{customer_id}/issuance_req_{request_id}/{doc_type_slug}/{unique_filename}"

    # Get customer-specific bucket or fallback
    from app.crud import crud_customer_configuration
    bucket_name = GCS_BUCKET_NAME
    bucket_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, "STORAGE_BUCKET_NAME"
    )
    if bucket_config and bucket_config.get('effective_value'):
        bucket_name = bucket_config['effective_value']

    # Upload
    stored_uri = await _upload_to_gcs(bucket_name, blob_path, file_content, file.content_type)
    if not stored_uri:
        raise HTTPException(status_code=500, detail="Failed to upload document")

    # Save metadata
    doc = IssuanceRequestDocument(
        request_id=request_id,
        document_type=document_type,
        file_name=file.filename,
        file_path=stored_uri,
        uploaded_by=None  # Public user (no user_id)
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


@router.post("/requests/{request_id}/analyze-document")
async def public_analyze_document(
    request_id: int,
    token: str = Query(...),
    doc_type: str = Query(default="CONTRACT"),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Public: AI-analyze a reference document against the issuance request.
    Extracts structured fields from the document and compares them to the request.
    Only fields actually found in the document are compared — missing = pass.
    """
    from app.models.models_issuance import IssuanceRequest
    from app.core.ai_integration import analyze_supporting_document, AI_DOC_MAX_SIZE_BYTES
    from sqlalchemy.orm import joinedload
    from decimal import Decimal

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]

    request_obj = db.query(IssuanceRequest).options(
        joinedload(IssuanceRequest.currency),
        joinedload(IssuanceRequest.payable_currency),
        joinedload(IssuanceRequest.lg_type),
    ).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == customer_id,
    ).first()
    if not request_obj:
        raise HTTPException(404, "Request not found")

    pdf_bytes = await file.read()

    if len(pdf_bytes) > AI_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Max 5 MB.",
            "comparison": None,
        }

    # Run AI extraction
    ai_result = await analyze_supporting_document(
        pdf_bytes, doc_type.upper(), file.filename,
        db=db, customer_id=customer_id, user_id=None,
    )

    if ai_result["status"] != "OK":
        return ai_result

    extracted = ai_result["extracted_fields"]

    # Build comparison
    comparison = []

    def _compare(field, request_val, doc_val, label):
        if doc_val is None:
            return  # Field not found in document = auto-pass
        match = False
        if request_val is not None:
            if isinstance(request_val, (int, float, Decimal)):
                try:
                    match = abs(float(request_val) - float(doc_val)) < 0.01
                except (ValueError, TypeError):
                    match = False
            else:
                match = str(request_val).strip().lower() == str(doc_val).strip().lower()
        comparison.append({
            "field": field, "label": label,
            "request_value": str(request_val) if request_val is not None else None,
            "document_value": str(doc_val),
            "match": match,
            "severity": "info" if match else ("warning" if request_val is not None else "suggestion"),
        })

    # Amount
    amount_key = {"PURCHASE_ORDER": "po_value", "FORMAL_REQUEST": "requested_amount"}.get(doc_type.upper(), "contract_value")
    _compare("amount", request_obj.amount, extracted.get(amount_key), "Document Amount vs Request Amount")

    # Beneficiary name
    ben_key = {"PURCHASE_ORDER": "vendor_name", "FORMAL_REQUEST": "requested_beneficiary"}.get(doc_type.upper(), "beneficiary_name")
    _compare("beneficiary_name", request_obj.beneficiary_name,
             extracted.get(ben_key) or extracted.get("beneficiary_name"), "Beneficiary Name")

    # Beneficiary address
    _compare("beneficiary_address", request_obj.beneficiary_address,
             extracted.get("beneficiary_address"), "Beneficiary Address")

    # Reference number
    _compare("reference_number", getattr(request_obj, 'reference_number', None),
             extracted.get("reference_number"), "Reference Number")

    # LG Type — bidirectional fuzzy word match
    if extracted.get("lg_type_hint") and request_obj.lg_type:
        lg_name = getattr(request_obj.lg_type, 'name', '')
        doc_type_val = extracted.get("lg_type_hint", '')
        # Tokenize both, ignore very short words like "of", "a", "the"
        req_words = set(w.lower() for w in lg_name.split() if len(w) > 2)
        doc_words = set(w.lower() for w in doc_type_val.split() if len(w) > 2)
        # Jaccard-like: require significant overlap in both directions
        if req_words and doc_words:
            overlap = len(req_words & doc_words)
            union = len(req_words | doc_words)
            lg_match = (overlap / union) >= 0.5 if union > 0 else False
        else:
            # Fallback: direct substring containment (either direction)
            lg_match = lg_name.lower() in doc_type_val.lower() or doc_type_val.lower() in lg_name.lower()
        comparison.append({"field": "lg_type", "label": "LG Type",
                           "request_value": lg_name, "document_value": doc_type_val,
                           "match": lg_match, "severity": "info" if lg_match else "warning"})

    # Currency
    if extracted.get("currency_code") and request_obj.currency:
        c_match = getattr(request_obj.currency, 'iso_code', '') == extracted.get("currency_code")
        comparison.append({"field": "currency", "label": "Currency",
                           "request_value": getattr(request_obj.currency, 'iso_code', None),
                           "document_value": extracted.get("currency_code"),
                           "match": c_match, "severity": "info" if c_match else "warning"})

    # Payable currency
    if extracted.get("payable_currency") and request_obj.payable_currency:
        p_match = getattr(request_obj.payable_currency, 'iso_code', '') == extracted.get("payable_currency")
        comparison.append({"field": "payable_currency", "label": "Payable Currency",
                           "request_value": getattr(request_obj.payable_currency, 'iso_code', None),
                           "document_value": extracted.get("payable_currency"),
                           "match": p_match, "severity": "info" if p_match else "warning"})

    # Maturity / Expiry Date — smart comparison with duration parsing
    doc_maturity = extracted.get("maturity_date")
    req_expiry = request_obj.requested_expiry_date
    if doc_maturity and req_expiry:
        import re
        from datetime import date, timedelta
        expiry_match = False
        doc_display = doc_maturity
        req_display = str(req_expiry)

        # Try to parse duration patterns like "6 months", "valid for 1 year", "180 days"
        duration_pattern = re.search(r'(\d+)\s*(month|year|day|week)s?', doc_maturity, re.IGNORECASE)
        if duration_pattern:
            num = int(duration_pattern.group(1))
            unit = duration_pattern.group(2).lower()
            today = date.today()
            if unit in ('month',):
                expected_expiry = today + timedelta(days=num * 30)
            elif unit in ('year',):
                expected_expiry = today + timedelta(days=num * 365)
            elif unit in ('week',):
                expected_expiry = today + timedelta(weeks=num)
            else:
                expected_expiry = today + timedelta(days=num)

            delta = abs((req_expiry - expected_expiry).days)
            expiry_match = delta <= 15  # ±15 day tolerance
            doc_display = f"{doc_maturity} (≈ {expected_expiry.isoformat()}, {delta}d diff)"
        else:
            # Try to parse exact date from document
            try:
                from dateutil import parser as dateparser
                doc_date = dateparser.parse(doc_maturity, dayfirst=True).date()
                delta = abs((req_expiry - doc_date).days)
                expiry_match = delta <= 7  # ±7 day tolerance for exact dates
                doc_display = f"{doc_maturity} ({doc_date.isoformat()}, {delta}d diff)"
            except Exception:
                # Fallback: simple string match
                expiry_match = str(req_expiry) in doc_maturity or doc_maturity in str(req_expiry)

        comparison.append({"field": "requested_expiry_date", "label": "Maturity / Expiry Date",
                           "request_value": req_display, "document_value": doc_display,
                           "match": expiry_match,
                           "severity": "info" if expiry_match else "warning"})

    # Purpose
    if extracted.get("purpose") and request_obj.lg_purpose:
        req_w = set(w.lower() for w in (request_obj.lg_purpose or '').split() if len(w) > 3)
        doc_w = set(w.lower() for w in extracted.get("purpose", '').split() if len(w) > 3)
        p_match = len(req_w & doc_w) >= min(2, len(req_w)) if req_w else False
        comparison.append({"field": "purpose", "label": "LG Purpose",
                           "request_value": request_obj.lg_purpose,
                           "document_value": extracted.get("purpose"),
                           "match": p_match, "severity": "info" if p_match else "suggestion"})

    # Special conditions
    if extracted.get("special_conditions"):
        comparison.append({"field": "special_conditions", "label": "Special Conditions in Document",
                           "request_value": request_obj.other_conditions or "None specified",
                           "document_value": "; ".join(extracted["special_conditions"]),
                           "match": True, "severity": "info"})

    result = {
        "status": "OK",
        "message": None,
        "doc_type": doc_type.upper(),
        "summary": extracted.get("summary"),
        "comparison": comparison,
        "mismatches": len([c for c in comparison if not c["match"]]),
        "total_fields_compared": len(comparison),
    }

    # Store result on the document for later viewing
    try:
        from app.models.models_issuance import IssuanceRequestDocument
        doc = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == request_id,
            IssuanceRequestDocument.document_type == doc_type.upper(),
        ).order_by(IssuanceRequestDocument.id.desc()).first()
        if doc:
            doc.ai_verification_result = result
            db.commit()
    except Exception:
        pass  # Non-critical

    return result


# ==============================================================================
# 4.1 REQUESTOR MAINTENANCE PORTAL
# ==============================================================================

@router.get("/my-issued-lgs")
def public_get_my_issued_lgs(
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Returns issued LGs linked to the requestor's email.
    Joins IssuedLGRecord → IssuanceRequest → requestor_email.
    """
    from app.models.models_issuance import IssuanceRequest, IssuedLGRecord
    from sqlalchemy.orm import joinedload

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    # Find all request IDs by this requestor
    request_rows = db.query(IssuanceRequest.id, IssuanceRequest.serial_number).filter(
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.requestor_email == email.lower(),
        IssuanceRequest.is_deleted == False,
    ).all()
    request_ids = [r.id for r in request_rows]
    request_serial_map = {r.id: r.serial_number for r in request_rows}

    if not request_ids:
        lgs = []
    else:
        lgs = db.query(IssuedLGRecord).options(
            joinedload(IssuedLGRecord.bank),
            joinedload(IssuedLGRecord.currency),
        ).filter(
            IssuedLGRecord.customer_id == customer_id,
            IssuedLGRecord.request_id.in_(request_ids),
        ).order_by(IssuedLGRecord.created_at.desc()).all()

    # Query incoming handovers (pending acceptance for this email)
    from sqlalchemy.sql.expression import cast
    import sqlalchemy
    
    # We use cast to String to handle astext properly across different SQL dialects if needed
    incoming = db.query(IssuedLGRecord).options(
        joinedload(IssuedLGRecord.bank),
        joinedload(IssuedLGRecord.currency),
    ).filter(
        IssuedLGRecord.customer_id == customer_id,
        IssuedLGRecord.handover_state == "PENDING_ACCEPTANCE",
        IssuedLGRecord.pending_handover_data['email'].astext.ilike(email)
    ).all()

    def format_lg(lg, is_incoming=False):
        return {
            "id": lg.id,
            "lg_ref_number": lg.lg_ref_number,
            "beneficiary_name": lg.beneficiary_name,
            "current_amount": float(lg.current_amount or 0),
            "currency": lg.currency.iso_code if lg.currency else "",
            "bank_name": lg.bank.name if lg.bank else "",
            "issue_date": str(lg.issue_date) if lg.issue_date else None,
            "expiry_date": str(lg.expiry_date) if lg.expiry_date else None,
            "status": lg.status,
            "bank_lg_number": lg.bank_lg_number,
            "request_serial_number": request_serial_map.get(lg.request_id) if not is_incoming else None,
            "created_at": lg.created_at.isoformat() if lg.created_at else None,
            "handover_state": lg.handover_state,
            "pending_handover_data": lg.pending_handover_data,
        }

    return {
        "email": email,
        "total": len(lgs),
        "issued_lgs": [format_lg(lg) for lg in lgs],
        "incoming_handovers": [format_lg(lg, is_incoming=True) for lg in incoming],
    }



@router.post("/maintenance/upload-document")
async def public_upload_maintenance_document(
    token: str = Query(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Public portal: upload a supporting document for a maintenance action."""
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    from app.crud import crud_customer_configuration
    import uuid
    from datetime import datetime as dt

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]

    file_content = await file.read()
    if len(file_content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File size exceeds 10MB limit")

    file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'bin'
    unique_name = f"MAINT_DOC_{dt.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_extension}"
    blob_path = f"customer_{customer_id}/maintenance_docs/{unique_name}"

    bucket_name = GCS_BUCKET_NAME
    bucket_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, "STORAGE_BUCKET_NAME"
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


@router.post("/issued-lgs/{lg_id}/maintenance")
def public_create_maintenance_action(
    lg_id: int,
    payload: dict,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Requestor creates a maintenance action: EXTEND, INCREASE_AMOUNT, CLOSE, AMENDMENT.
    Uses the same maintenance_service as the internal flow.
    """
    from app.models.models_issuance import IssuanceRequest, IssuedLGRecord, IssuanceMaintenanceAction
    from app.services.issuance_maintenance_service import maintenance_service

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    # Verify the LG belongs to this requestor
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="Issued LG not found")

    # Check requestor owns this LG via request
    if lg.request_id:
        req = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
        if req and req.requestor_email and req.requestor_email.lower() != email.lower():
            raise HTTPException(status_code=403, detail="Not authorized to modify this LG")

    action_type = payload.get("action_type", "").upper()
    allowed_types = ["EXTEND", "INCREASE_AMOUNT", "AMENDMENT"]
    if action_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"action_type must be one of: {', '.join(allowed_types)}")

    action_data = payload.get("action_data", {})
    notes = payload.get("notes")

    # Store requestor email in action_data for traceability (no internal user ID)
    action_data["requestor_email"] = email
    action = maintenance_service.create_action(
        db, lg_id, action_type, action_data,
        user_id=None,  # Public requestor — no internal user
        customer_id=customer_id,
        notes=notes,
        initiation_source="REQUESTOR_PORTAL"
    )

    return {
        "id": action.id,
        "action_type": action.action_type,
        "status": action.status,
        "action_data": action.action_data,
        "notes": action.notes,
        "created_at": action.created_at.isoformat() if action.created_at else None,
    }


@router.get("/my-maintenance-actions")
def public_get_my_maintenance_actions(
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Returns all maintenance actions on LGs linked to this requestor.
    """
    from app.models.models_issuance import IssuanceRequest, IssuedLGRecord, IssuanceMaintenanceAction
    from sqlalchemy.orm import joinedload

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    # Find LG IDs linked to this requestor
    request_ids = [r.id for r in db.query(IssuanceRequest.id).filter(
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.requestor_email == email.lower(),
        IssuanceRequest.is_deleted == False,
    ).all()]

    if not request_ids:
        return {"email": email, "total": 0, "actions": []}

    lg_ids = [lg.id for lg in db.query(IssuedLGRecord.id).filter(
        IssuedLGRecord.customer_id == customer_id,
        IssuedLGRecord.request_id.in_(request_ids),
    ).all()]

    if not lg_ids:
        return {"email": email, "total": 0, "actions": []}

    actions = db.query(IssuanceMaintenanceAction).options(
        joinedload(IssuanceMaintenanceAction.issued_lg),
    ).filter(
        IssuanceMaintenanceAction.issued_lg_id.in_(lg_ids),
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    return {
        "email": email,
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
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in actions
        ]
    }


@router.post("/issued-lgs/{lg_id}/maintenance")
def public_submit_maintenance_action(
    lg_id: int,
    payload: dict = Body(...),
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    External requestor submits a maintenance action on their issued LG.
    Allowed actions: EXTEND, INCREASE_AMOUNT, CLOSE, AMENDMENT.
    """
    from app.models.models_issuance import IssuanceRequest, IssuedLGRecord
    from app.services.issuance_maintenance_service import IssuanceMaintenanceService

    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")

    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    # Verify the LG belongs to a request made by this email
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG not found")

    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == lg.request_id,
    ).first()
    if not request or (request.requestor_email or "").lower() != email.lower():
        raise HTTPException(status_code=403, detail="You can only manage LGs from your own requests")

    # Validate allowed action types for external requestors
    allowed_types = {"EXTEND", "INCREASE_AMOUNT", "AMENDMENT"}
    action_type = payload.get("action_type")
    if action_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"Action type '{action_type}' is not available from the portal")

    svc = IssuanceMaintenanceService()
    ext_action_data = payload.get("action_data", {})
    ext_action_data["requestor_email"] = email
    action = svc.create_action(
        db=db,
        issued_lg_id=lg_id,
        action_type=action_type,
        action_data=ext_action_data,
        user_id=None,  # No internal user — external requestor
        customer_id=customer_id,
        notes=payload.get("notes"),
        initiation_source="EXTERNAL_REQUESTOR",
    )
    return {"id": action.id, "status": action.status, "action_type": action.action_type}

# ==============================================================================
# OWNERSHIP HANDOVER ENDPOINTS
# ==============================================================================

@router.post("/handover/initiate")
def public_initiate_handover(
    payload: dict = Body(...),
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Requestor initiates handing over their LG to another Requestor profile.
    payload: {
        lg_id: int,
        new_requestor: { email, name, department, ... }
    }
    """
    from app.crud.crud_issuance_owner import initiate_peer_handover
    from app.schemas.schemas_issuance import RequestorProfile
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    lg_id = payload.get("lg_id")
    new_req_data = payload.get("new_requestor")
    if not lg_id or not new_req_data:
        raise HTTPException(status_code=400, detail="lg_id and new_requestor are required.")

    # Guard: ensure LG belongs to this requestor
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == customer_id,
    ).first()
    if not lg:
        raise HTTPException(status_code=404, detail="LG not found")

    request = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
    if not request or (request.requestor_email or "").lower() != email.lower():
        raise HTTPException(status_code=403, detail="You can only hand over LGs from your own requests")

    profile = RequestorProfile(**new_req_data)
    lg = initiate_peer_handover(db, customer_id, lg_id, profile, email)
    return {"message": "Handover initiated successfully.", "lg_id": lg.id, "handover_state": lg.handover_state}


@router.post("/handover/resolve")
def public_resolve_handover(
    payload: dict = Body(...),
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    The target Requestor accepts or rejects the handover.
    payload: {
        lg_id: int,
        action: "ACCEPT" | "REJECT"
    }
    """
    from app.crud.crud_issuance_owner import resolve_peer_handover
    
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    email = access_data.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email not available in token.")

    lg_id = payload.get("lg_id")
    action = payload.get("action")
    if not lg_id or not action or action not in ["ACCEPT", "REJECT"]:
        raise HTTPException(status_code=400, detail="lg_id and valid action (ACCEPT/REJECT) are required.")

    lg = resolve_peer_handover(db, customer_id, lg_id, action, email)
    return {"message": f"Handover {action.lower()}ed successfully.", "lg_id": lg.id}