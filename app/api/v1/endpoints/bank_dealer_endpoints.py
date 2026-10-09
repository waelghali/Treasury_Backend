# app/api/v1/endpoints/bank_dealer_endpoints.py
"""
Permanent Bank Dealer Trading Desk Endpoints
Provides institutional authentication, RFC 6238 TOTP 2-Factor enrolment,
Microsoft / Google Authenticator QR binding, and dealer profile management.
"""

import os
import secrets
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, HTTPException, status, Request, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, cast, Text, String

from app.database import get_db
from app.models.models import Bank
from app.models.models_quotation import QuotationBankDealer, QuotationBank
from app.services.dealer_auth_service import (
    generate_totp_secret,
    generate_totp_uri,
    generate_qr_code_base64,
    verify_totp_code,
    create_dealer_access_token,
    decode_dealer_access_token,
    create_enrollment_handover_token,
    decode_enrollment_handover_token,
    verify_password,
    get_password_hash,
    MAX_FAILED_LOGIN_ATTEMPTS,
    LOCKOUT_DURATION_MINUTES,
    EMAIL_OTP_TTL_MINUTES
)
from app.core.otp_security import hash_otp_code, verify_otp_code
from app.core.email_service import send_email, get_customer_email_settings

logger = logging.getLogger(__name__)

router = APIRouter()


# ==============================================================================
# SCHEMAS
# ==============================================================================

class DealerEnrollInitiateRequest(BaseModel):
    email: EmailStr = Field(..., description="Official corporate bank email (@bank.com)")
    full_name: str = Field(..., min_length=2, max_length=255, description="Full Name of the Trader / Dealer")
    bank_id: Optional[int] = Field(None, description="Bank ID if known, otherwise auto-detected via email domain")
    phone_number: Optional[str] = Field(None, max_length=50)
    title: Optional[str] = Field(None, max_length=100, description="e.g. Senior FX Trader, Treasury Sales")
    role: Optional[str] = Field("EXECUTION", description="'EXECUTION', 'APPROVER', or 'VIEW_ONLY'")


class DealerEnrollVerifyEmailRequest(BaseModel):
    email: EmailStr = Field(..., description="Corporate email address")
    otp_code: str = Field(..., min_length=6, max_length=6, description="6-digit verification code from email")


class DealerEnrollConfirmTotpRequest(BaseModel):
    enrollment_token: str = Field(..., description="Gate 1 to Gate 2 cryptographic handover token")
    totp_code: str = Field(..., min_length=6, max_length=6, description="6-digit rolling code from Microsoft/Google Authenticator")
    password: str = Field(..., min_length=8, max_length=128, description="Permanent dealer password (min 8 characters)")


class DealerLoginRequest(BaseModel):
    email: EmailStr = Field(..., description="Official corporate bank email")
    password: str = Field(..., min_length=1, description="Permanent dealer password")
    totp_code: str = Field(..., min_length=6, max_length=6, description="6-digit rolling code from Microsoft/Google Authenticator")


class DealerRecoveryInitiateRequest(BaseModel):
    email: EmailStr = Field(..., description="Official corporate bank email (@bank.com)")


class DealerRecoveryVerifyEmailRequest(BaseModel):
    email: EmailStr = Field(..., description="Corporate email address")
    otp_code: str = Field(..., min_length=6, max_length=6, description="6-digit recovery code from email")


# ==============================================================================
# AUTH DEPENDENCY
# ==============================================================================

async def get_current_dealer(
    request: Request,
    db: Session = Depends(get_db)
) -> QuotationBankDealer:
    """
    Dependency that extracts and validates the Bank Dealer JWT token from
    Authorization: Bearer <token> or ?token=<token>.
    """
    auth_header = request.headers.get("Authorization")
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1].strip()
    elif "token" in request.query_params:
        token = request.query_params["token"].strip()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing dealer authentication credentials."
        )

    try:
        payload = decode_dealer_access_token(token)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )

    dealer_id = payload.get("dealer_id") or payload.get("sub")
    if not dealer_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid dealer credentials payload."
        )

    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.id == int(dealer_id),
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bank dealer account not found."
        )

    if not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bank dealer account has been deactivated by administrator."
        )

    if dealer.bank and not dealer.bank.portal_access_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Trading portal access for this bank has been disabled by platform administration."
        )

    return dealer


# ==============================================================================
# 2-FACTOR ENROLMENT ENDPOINTS (THE HANDSHAKE CEREMONY)
# ==============================================================================

@router.post("/auth/enroll/initiate")
async def initiate_dealer_enrollment(
    req: DealerEnrollInitiateRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Gate 1 (Corporate Domain Proof - Initiation):
    Verifies bank corporate domain, provisions or locates dealer record,
    and dispatches a 6-digit verification code to the dealer's corporate email.
    """
    clean_email = req.email.strip().lower()
    domain = clean_email.split("@")[1]

    # Resolve target Bank
    bank: Optional[Bank] = None
    if req.bank_id:
        bank = db.query(Bank).filter(Bank.id == req.bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise HTTPException(status_code=400, detail=f"Bank with ID {req.bank_id} does not exist.")
        # If bank has an email domain configured, verify email matches
        if bank.email_domain and domain != bank.email_domain.strip().lower():
            raise HTTPException(
                status_code=400,
                detail=f"Email domain '@{domain}' does not match official domain '@{bank.email_domain}' for {bank.name}."
            )
    else:
        # Auto-detect Bank via domain
        bank = db.query(Bank).filter(
            func.lower(Bank.email_domain) == domain,
            Bank.is_deleted == False
        ).first()

        # Fallback: check if email is listed in any active QuotationBank roster
        if not bank:
            q_bank = db.query(QuotationBank).filter(
                or_(
                    func.lower(QuotationBank.emails).like(f"%{clean_email}%"),
                    cast(QuotationBank.contacts, Text).like(f"%{clean_email}%")
                ),
                QuotationBank.is_deleted == False
            ).first()
            if q_bank and q_bank.bank:
                bank = q_bank.bank

        if not bank:
            raise HTTPException(
                status_code=400,
                detail=f"The email domain '@{domain}' is not registered with any partner bank. Please select your bank or contact platform support."
            )

    if not bank.portal_access_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Trading portal access for {bank.name} has been disabled by platform administration."
        )

    # Check existing dealer record
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()

    if dealer and not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your bank trading desk account has been deactivated. Please contact platform administration."
        )

    if dealer and dealer.is_totp_enrolled and dealer.hashed_password:
        return {
            "success": True,
            "already_enrolled": True,
            "message": "Your trading desk account is already enrolled. Please log in directly with your password and Authenticator code.",
            "email": clean_email,
            "bank_id": bank.id,
            "bank_name": bank.name
        }

    now_utc = datetime.now(timezone.utc)

    if not dealer:
        dealer = QuotationBankDealer(
            bank_id=bank.id,
            email=clean_email,
            full_name=req.full_name.strip(),
            phone_number=req.phone_number.strip() if req.phone_number else None,
            title=req.title.strip() if req.title else None,
            role=req.role.strip().upper() if req.role else "EXECUTION",
            is_totp_enrolled=False,
            is_active=True
        )
        db.add(dealer)
    else:
        # Update details if re-initiating pending enrollment
        dealer.full_name = req.full_name.strip()
        dealer.bank_id = bank.id
        if req.phone_number:
            dealer.phone_number = req.phone_number.strip()
        if req.title:
            dealer.title = req.title.strip()
        if req.role:
            dealer.role = req.role.strip().upper()

    # Generate 6-digit email OTP
    otp_code = f"{secrets.randbelow(900000) + 100000}"
    hashed_otp = hash_otp_code(otp_code)
    expires_at = now_utc + timedelta(minutes=EMAIL_OTP_TTL_MINUTES)

    dealer.pending_email_otp = hashed_otp
    dealer.pending_email_otp_expires_at = expires_at
    dealer.pending_otp_failed_attempts = 0
    dealer.enrollment_token = None
    dealer.enrollment_token_expires_at = None

    db.commit()
    db.refresh(dealer)

    # Dispatch Verification Email (SendOnly - save_copy=False)
    subject = f"Grow Treasury - Bank Dealer 2FA Enrolment Code ({bank.name})"
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #0f172a; margin: 0; padding: 24px; color: #f8fafc;">
        <div style="max-width: 520px; margin: 0 auto; background: #1e293b; border-radius: 16px; border: 1px solid #334155; overflow: hidden; box-shadow: 0 10px 25px rgba(0,0,0,0.4);">
            <div style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 28px 32px; border-bottom: 1px solid #334155; text-align: center;">
                <div style="display: inline-block; background: #059669; color: #ffffff; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; padding: 4px 12px; border-radius: 20px; margin-bottom: 12px;">
                    Institutional Counterparty Desk
                </div>
                <h1 style="color: #ffffff; font-size: 20px; font-weight: 700; margin: 0; letter-spacing: -0.025em;">Authenticator 2-Factor Enrolment</h1>
                <p style="color: #94a3b8; font-size: 13px; margin: 6px 0 0 0;">{bank.name}</p>
            </div>
            
            <div style="padding: 32px;">
                <p style="margin: 0 0 16px 0; font-size: 15px; line-height: 1.5; color: #e2e8f0;">Dear <strong>{dealer.full_name}</strong>,</p>
                <p style="margin: 0 0 20px 0; font-size: 13px; line-height: 1.6; color: #94a3b8;">
                    You are setting up permanent access to the <strong>Grow Treasury Unified Trading Desk</strong> for <strong>{bank.name}</strong>. Enter the 6-digit verification code below to verify your corporate email and bind your Authenticator app.
                </p>

                <div style="background: #0f172a; border: 1px solid #334155; border-radius: 12px; padding: 24px; text-align: center; margin-bottom: 24px;">
                    <span style="display: block; font-size: 11px; text-transform: uppercase; font-weight: 700; color: #64748b; letter-spacing: 0.05em; margin-bottom: 8px;">Corporate Email Verification Code</span>
                    <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 38px; font-weight: 800; letter-spacing: 0.25em; color: #34d399; margin-left: 0.25em;">{otp_code}</span>
                    <span style="display: block; font-size: 11px; color: #64748b; margin-top: 10px;">Expires in {EMAIL_OTP_TTL_MINUTES} minutes &bull; Single Use</span>
                </div>

                <div style="background: #1e293b; border-left: 3px solid #3b82f6; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px;">
                    <p style="margin: 0; font-size: 12px; color: #cbd5e1; line-height: 1.4;">
                        <strong>Next Step:</strong> Once verified, you will scan a QR code using <strong>Microsoft Authenticator</strong> or <strong>Google Authenticator</strong> to complete permanent activation.
                    </p>
                </div>

                <div style="border-top: 1px solid #334155; padding-top: 16px; font-size: 11px; color: #64748b;">
                    If you did not initiate this enrolment request, please notify your bank's Treasury Desk Head immediately.
                </div>
            </div>
            
            <div style="background: #0f172a; padding: 14px 32px; border-top: 1px solid #334155; font-size: 11px; color: #64748b; text-align: center;">
                Grow Treasury Platform &bull; Institutional Trading Desk Security
            </div>
        </div>
    </body>
    </html>
    """

    email_settings, source = get_customer_email_settings(db, None)
    background_tasks.add_task(
        send_email,
        db=db,
        to_emails=[clean_email],
        subject_template=subject,
        body_template=body,
        template_data={},
        email_settings=email_settings,
        sender_name="Grow Treasury Trading Desk",
        save_copy=False
    )

    return {
        "success": True,
        "already_enrolled": False,
        "message": f"Verification code dispatched to {clean_email}.",
        "email": clean_email,
        "bank_id": bank.id,
        "bank_name": bank.name,
        "requires_email_verification": True
    }


@router.post("/auth/enroll/verify-email")
async def verify_dealer_email_otp(
    req: DealerEnrollVerifyEmailRequest,
    db: Session = Depends(get_db)
):
    """
    Gate 1 (Corporate Domain Proof - Verification):
    Validates the 6-digit email OTP in constant time.
    Upon verification, generates a fresh RFC 6238 TOTP secret, renders
    a high-resolution QR code for Microsoft Authenticator, and returns
    a short-lived cryptographic handover token for Gate 2.
    """
    clean_email = req.email.strip().lower()
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        raise HTTPException(status_code=404, detail="No pending enrolment found for this email.")

    if dealer.is_totp_enrolled and dealer.hashed_password:
        return {
            "success": True,
            "already_enrolled": True,
            "message": "Account is already enrolled. Please log in directly."
        }

    now_utc = datetime.now(timezone.utc)

    # Check OTP expiration
    if not dealer.pending_email_otp or not dealer.pending_email_otp_expires_at or now_utc > dealer.pending_email_otp_expires_at:
        raise HTTPException(
            status_code=400,
            detail="Verification code has expired. Please request a new code."
        )

    # Check brute-force threshold
    if dealer.pending_otp_failed_attempts >= 3:
        dealer.pending_email_otp = None
        dealer.pending_email_otp_expires_at = None
        db.commit()
        raise HTTPException(
            status_code=400,
            detail="Too many incorrect attempts. For security, this code has been invalidated. Please request a fresh code."
        )

    # Verify OTP constant time
    if not verify_otp_code(req.otp_code, dealer.pending_email_otp):
        dealer.pending_otp_failed_attempts += 1
        db.commit()
        rem = 3 - dealer.pending_otp_failed_attempts
        raise HTTPException(
            status_code=400,
            detail=f"Invalid verification code. {rem} attempt(s) remaining."
        )

    # Email verified! Clear pending email OTP
    dealer.pending_email_otp = None
    dealer.pending_email_otp_expires_at = None
    dealer.pending_otp_failed_attempts = 0
    dealer.email_verified_at = now_utc

    # Generate fresh RFC 6238 TOTP secret
    totp_secret = generate_totp_secret()
    dealer.totp_secret = totp_secret

    bank_name = dealer.bank.name if dealer.bank else "Partner Bank"
    otpauth_uri = generate_totp_uri(totp_secret, dealer.email, bank_name)
    qr_code_base64 = generate_qr_code_base64(otpauth_uri)

    # Issue Gate 1 -> Gate 2 Handover Token
    handover_token = create_enrollment_handover_token(dealer.id, dealer.email, dealer.bank_id)
    dealer.enrollment_token = handover_token
    dealer.enrollment_token_expires_at = now_utc + timedelta(minutes=15)

    db.commit()

    return {
        "success": True,
        "message": "Email verified successfully. Scan the QR code with Microsoft Authenticator or Google Authenticator.",
        "enrollment_token": handover_token,
        "totp_secret": totp_secret,
        "otpauth_uri": otpauth_uri,
        "qr_code_base64": qr_code_base64,
        "email": dealer.email,
        "bank_name": bank_name,
        "full_name": dealer.full_name
    }


@router.post("/auth/enroll/confirm-totp")
async def confirm_dealer_totp_and_activate(
    req: DealerEnrollConfirmTotpRequest,
    db: Session = Depends(get_db)
):
    """
    Gate 2 (Authenticator App Binding - Cryptographic Handshake Proof):
    Validates the 6-digit rolling code generated on the dealer's physical phone.
    Permanently seals the account, stores password hash, sets is_totp_enrolled = True,
    and issues an active trading session JWT.
    """
    try:
        payload = decode_enrollment_handover_token(req.enrollment_token)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    dealer_id = int(payload["sub"])
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.id == dealer_id,
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        raise HTTPException(status_code=404, detail="Dealer account not found.")

    if not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your bank trading desk account has been deactivated. Contact your administrator."
        )

    if dealer.bank and not dealer.bank.portal_access_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Trading portal access for {dealer.bank.name} has been disabled by platform administration."
        )

    if not dealer.totp_secret:
        raise HTTPException(status_code=400, detail="No pending TOTP secret found. Please restart enrolment.")

    now_utc = datetime.now(timezone.utc)
    if dealer.enrollment_token != req.enrollment_token or (dealer.enrollment_token_expires_at and now_utc > dealer.enrollment_token_expires_at):
        raise HTTPException(status_code=400, detail="Enrolment session expired. Please restart enrolment.")

    # Cryptographic Handshake Proof: verify rolling TOTP code
    is_valid = verify_totp_code(dealer.totp_secret, req.totp_code)
    if not is_valid:
        raise HTTPException(
            status_code=400,
            detail="Invalid 6-digit code. Check your Microsoft Authenticator app and ensure the code has not expired."
        )

    # Validate password strength (min 8 chars)
    if len(req.password) < 8:
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters long."
        )

    # Seal and activate account
    dealer.is_totp_enrolled = True
    dealer.hashed_password = get_password_hash(req.password)
    dealer.last_login_at = now_utc
    dealer.failed_login_attempts = 0
    dealer.locked_until = None
    dealer.enrollment_token = None
    dealer.enrollment_token_expires_at = None

    db.commit()
    db.refresh(dealer)

    bank_name = dealer.bank.name if dealer.bank else "Partner Bank"

    # Issue full institutional session JWT
    access_token = create_dealer_access_token(
        dealer_id=dealer.id,
        email=dealer.email,
        bank_id=dealer.bank_id,
        bank_name=bank_name,
        full_name=dealer.full_name,
        role=dealer.role
    )

    return {
        "success": True,
        "message": "Authenticator successfully bound and trading desk account activated!",
        "access_token": access_token,
        "token_type": "bearer",
        "dealer": dealer.to_dict()
    }


# ==============================================================================
# DAY 1+ ACCOUNT RECOVERY / CHANGED DEVICE / PASSWORD RESET
# ==============================================================================

@router.post("/auth/recovery/initiate")
async def initiate_dealer_recovery(
    req: DealerRecoveryInitiateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Day 1+ Account Recovery & 2FA Device Migration:
    Dispatches a single-use 6-digit cryptographic security code to the dealer's
    registered corporate bank email to allow password reset and Authenticator re-binding
    (e.g., when the trader gets a new phone or forgot their password).
    """
    clean_email = req.email.strip().lower()
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        # Standard security response to prevent user enumeration
        return {
            "success": True,
            "message": "If an account exists for this corporate email, a recovery verification code has been dispatched."
        }

    if not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your bank trading desk account has been deactivated. Please contact platform administration."
        )

    if dealer.bank and not dealer.bank.portal_access_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Trading portal access for {dealer.bank.name} has been disabled by platform administration."
        )

    now_utc = datetime.now(timezone.utc)

    # Generate 6-digit email OTP
    otp_code = f"{secrets.randbelow(900000) + 100000}"
    hashed_otp = hash_otp_code(otp_code)
    expires_at = now_utc + timedelta(minutes=EMAIL_OTP_TTL_MINUTES)

    dealer.pending_email_otp = hashed_otp
    dealer.pending_email_otp_expires_at = expires_at
    dealer.pending_otp_failed_attempts = 0
    dealer.enrollment_token = None
    dealer.enrollment_token_expires_at = None

    db.commit()
    db.refresh(dealer)

    bank_name = dealer.bank.name if dealer.bank else "Partner Bank"

    # Dispatch Recovery Verification Email
    subject = f"Grow Treasury - Trading Desk Account Recovery & 2FA Reset ({bank_name})"
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #0f172a; margin: 0; padding: 24px; color: #f8fafc;">
        <div style="max-width: 520px; margin: 0 auto; background: #1e293b; border-radius: 16px; border: 1px solid #334155; overflow: hidden; box-shadow: 0 10px 25px rgba(0,0,0,0.4);">
            <div style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 28px 32px; border-bottom: 1px solid #334155; text-align: center;">
                <div style="display: inline-block; background: #0284c7; color: #ffffff; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; padding: 4px 12px; border-radius: 20px; margin-bottom: 12px;">
                    Desk Account Recovery
                </div>
                <h1 style="color: #ffffff; font-size: 20px; font-weight: 700; margin: 0; letter-spacing: -0.025em;">Password & 2FA Reset Code</h1>
                <p style="color: #94a3b8; font-size: 13px; margin: 6px 0 0 0;">{bank_name}</p>
            </div>
            
            <div style="padding: 32px;">
                <p style="margin: 0 0 16px 0; font-size: 15px; line-height: 1.5; color: #e2e8f0;">Dear <strong>{dealer.full_name}</strong>,</p>
                <p style="margin: 0 0 20px 0; font-size: 13px; line-height: 1.6; color: #94a3b8;">
                    We received a request to reset your password or re-bind Microsoft Authenticator on a new phone for your <strong>Grow Treasury Trading Desk</strong> account.
                </p>

                <div style="background: #0f172a; border: 1px solid #334155; border-radius: 12px; padding: 24px; text-align: center; margin-bottom: 24px;">
                    <span style="display: block; font-size: 11px; text-transform: uppercase; font-weight: 700; color: #64748b; letter-spacing: 0.05em; margin-bottom: 8px;">Security Recovery Code</span>
                    <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 38px; font-weight: 800; letter-spacing: 0.25em; color: #38bdf8; margin-left: 0.25em;">{otp_code}</span>
                    <span style="display: block; font-size: 11px; color: #64748b; margin-top: 10px;">Expires in {EMAIL_OTP_TTL_MINUTES} minutes &bull; Single Use</span>
                </div>

                <div style="background: #1e293b; border-left: 3px solid #0284c7; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px;">
                    <p style="margin: 0; font-size: 12px; color: #cbd5e1; line-height: 1.4;">
                        <strong>Next Step:</strong> After entering this code on the trading desk portal, you will be prompted to scan a new QR code with <strong>Microsoft Authenticator</strong> on your phone and choose a new password.
                    </p>
                </div>

                <div style="border-top: 1px solid #334155; padding-top: 16px; font-size: 11px; color: #64748b;">
                    If you did not request this recovery, please notify your bank's Treasury Desk Head immediately.
                </div>
            </div>
            
            <div style="background: #0f172a; padding: 14px 32px; border-top: 1px solid #334155; font-size: 11px; color: #64748b; text-align: center;">
                Grow Treasury Platform &bull; Institutional Trading Desk Security
            </div>
        </div>
    </body>
    </html>
    """

    email_settings, source = get_customer_email_settings(db, None)
    background_tasks.add_task(
        send_email,
        db=db,
        to_emails=[clean_email],
        subject_template=subject,
        body_template=body,
        template_data={},
        email_settings=email_settings,
        sender_name="Grow Treasury Trading Desk",
        save_copy=False
    )

    return {
        "success": True,
        "message": f"Security recovery code dispatched to {clean_email}.",
        "email": clean_email,
        "bank_name": bank_name
    }


@router.post("/auth/recovery/verify-email")
async def verify_dealer_recovery_otp(
    req: DealerRecoveryVerifyEmailRequest,
    db: Session = Depends(get_db)
):
    """
    Verifies the 6-digit recovery OTP, unlocks any temporary lockout,
    and generates a fresh RFC 6238 TOTP secret and QR code so the dealer can
    bind their new phone / Authenticator and set a new password.
    """
    clean_email = req.email.strip().lower()
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        raise HTTPException(status_code=404, detail="No registered account found for this email.")

    if not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your bank trading desk account has been deactivated. Contact your administrator."
        )

    now_utc = datetime.now(timezone.utc)

    # Check OTP expiration
    if not dealer.pending_email_otp or not dealer.pending_email_otp_expires_at or now_utc > dealer.pending_email_otp_expires_at:
        raise HTTPException(
            status_code=400,
            detail="The verification code has expired. Please request a new recovery code."
        )

    # Brute-force rate limiting
    if dealer.pending_otp_failed_attempts >= 3:
        dealer.pending_email_otp = None
        dealer.pending_email_otp_expires_at = None
        db.commit()
        raise HTTPException(
            status_code=400,
            detail="Too many incorrect attempts. For security, this code has been invalidated. Please request a fresh code."
        )

    # Verify OTP constant time
    if not verify_otp_code(req.otp_code, dealer.pending_email_otp):
        dealer.pending_otp_failed_attempts += 1
        db.commit()
        rem = 3 - dealer.pending_otp_failed_attempts
        raise HTTPException(
            status_code=400,
            detail=f"Invalid recovery code. {rem} attempt(s) remaining."
        )

    # Recovery Code Verified! Clear pending email OTP and reset lockout
    dealer.pending_email_otp = None
    dealer.pending_email_otp_expires_at = None
    dealer.pending_otp_failed_attempts = 0
    dealer.email_verified_at = now_utc
    dealer.locked_until = None
    dealer.failed_login_attempts = 0

    # Generate fresh RFC 6238 TOTP secret
    totp_secret = generate_totp_secret()
    dealer.totp_secret = totp_secret

    bank_name = dealer.bank.name if dealer.bank else "Partner Bank"
    otpauth_uri = generate_totp_uri(totp_secret, dealer.email, bank_name)
    qr_code_base64 = generate_qr_code_base64(otpauth_uri)

    # Issue Gate 1 -> Gate 2 Handover Token
    handover_token = create_enrollment_handover_token(dealer.id, dealer.email, dealer.bank_id)
    dealer.enrollment_token = handover_token
    dealer.enrollment_token_expires_at = now_utc + timedelta(minutes=15)

    db.commit()

    return {
        "success": True,
        "message": "Recovery verified. Scan the new QR code on your phone and choose a new password.",
        "enrollment_token": handover_token,
        "totp_secret": totp_secret,
        "otpauth_uri": otpauth_uri,
        "qr_code_base64": qr_code_base64,
        "email": dealer.email,
        "bank_name": bank_name,
        "full_name": dealer.full_name
    }


# ==============================================================================
# DAY 1+ INSTANT LOGIN & DESK SESSION
# ==============================================================================

@router.post("/auth/login")
async def dealer_login(
    req: DealerLoginRequest,
    db: Session = Depends(get_db)
):
    """
    Day 1+ Instant Authentication:
    Authenticates in 3 seconds using corporate email, password, and 6-digit
    rolling code from Microsoft / Google Authenticator.
    Zero dependency on external bank email servers, junk filters, or expiring links.
    """
    clean_email = req.email.strip().lower()
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()

    if not dealer:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid corporate email or password."
        )

    if not dealer.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your bank trading desk account has been deactivated. Contact your administrator."
        )

    if dealer.bank and not dealer.bank.portal_access_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Trading portal access for {dealer.bank.name} has been disabled by platform administration."
        )

    now_utc = datetime.now(timezone.utc)

    # Check lockout
    if dealer.locked_until and now_utc < dealer.locked_until:
        remaining_sec = int((dealer.locked_until - now_utc).total_seconds())
        remaining_min = max(1, remaining_sec // 60)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account temporarily locked due to multiple failed login attempts. Please try again in {remaining_min} minute(s)."
        )

    # Check if enrolled
    if not dealer.is_totp_enrolled or not dealer.totp_secret or not dealer.hashed_password:
        raise HTTPException(
            status_code=400,
            detail="Your account has not completed Authenticator 2FA enrolment. Please complete enrolment first."
        )

    # Step 1: Verify Password
    if not verify_password(req.password, dealer.hashed_password):
        dealer.failed_login_attempts += 1
        if dealer.failed_login_attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
            dealer.locked_until = now_utc + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
            db.commit()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Account locked for {LOCKOUT_DURATION_MINUTES} minutes due to repeated failed attempts."
            )
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid corporate email or password."
        )

    # Step 2: Verify TOTP Code
    if not verify_totp_code(dealer.totp_secret, req.totp_code):
        dealer.failed_login_attempts += 1
        if dealer.failed_login_attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
            dealer.locked_until = now_utc + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
            db.commit()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Account locked for {LOCKOUT_DURATION_MINUTES} minutes due to repeated failed attempts."
            )
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid 6-digit Authenticator code. Please check Microsoft Authenticator and try again."
        )

    # Authentication Successful! Reset counters
    dealer.failed_login_attempts = 0
    dealer.locked_until = None
    dealer.last_login_at = now_utc
    db.commit()
    db.refresh(dealer)

    bank_name = dealer.bank.name if dealer.bank else "Partner Bank"

    access_token = create_dealer_access_token(
        dealer_id=dealer.id,
        email=dealer.email,
        bank_id=dealer.bank_id,
        bank_name=bank_name,
        full_name=dealer.full_name,
        role=dealer.role
    )

    return {
        "success": True,
        "access_token": access_token,
        "token_type": "bearer",
        "dealer": dealer.to_dict()
    }


# ==============================================================================
# PROFILE & SESSION INSPECTION
# ==============================================================================

@router.get("/auth/me")
async def get_dealer_profile(
    current_dealer: QuotationBankDealer = Depends(get_current_dealer)
):
    """
    Returns the authenticated dealer's identity, role, and institutional bank profile.
    """
    return {
        "success": True,
        "dealer": current_dealer.to_dict()
    }


# ==============================================================================
# UNIFIED MULTI-CUSTOMER TRADING BLOTTER ENDPOINTS
# ==============================================================================

class ClaimDeskRequest(BaseModel):
    assignment_id: str = Field(..., description="Assignment UUID of the target RFQ desk")


@router.get("/blotter/live-rfqs")
async def get_live_rfq_blotter(
    current_dealer: QuotationBankDealer = Depends(get_current_dealer),
    db: Session = Depends(get_db)
):
    """
    Unified Multi-Customer Live RFQ Blotter:
    Returns all live, open, and upcoming tenders dispatched to the dealer's bank
    across ALL corporate clients on the platform.
    Features:
    - Real-time countdowns & urgency sorting
    - Desk concurrency lock awareness (desk_session_service)
    - Multi-leg privacy filtering (invisible legs cleanly hidden)
    - Quoted / Passed state tracking
    """
    from app.models.models_quotation import (
        QuotationBankAssignment, QuotationRequest, QuotationLeg,
        QuotationOffer, QuotationTBillOffer, QuotationBankLegConfig
    )
    from app.services.desk_session_service import desk_session_service

    now_utc = datetime.now(timezone.utc)

    # Query all active assignments for the dealer's bank
    assignments = db.query(QuotationBankAssignment).join(
        QuotationBank, QuotationBankAssignment.quotation_bank_id == QuotationBank.id
    ).join(
        QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
    ).filter(
        QuotationBank.bank_id == current_dealer.bank_id,
        QuotationRequest.is_deleted == False,
        QuotationRequest.status.in_(["OPEN", "EVALUATING", "PENDING"])
    ).all()

    live_tickets = []

    for asgn in assignments:
        rfq = asgn.rfq
        if not rfq:
            continue

        w_start = rfq.window_start.replace(tzinfo=timezone.utc) if (rfq.window_start and rfq.window_start.tzinfo is None) else rfq.window_start
        w_end = rfq.window_end.replace(tzinfo=timezone.utc) if (rfq.window_end and rfq.window_end.tzinfo is None) else rfq.window_end

        # Token validity cutoff
        validity_hours = getattr(rfq, 'token_validity_hours', 24) or 24
        cutoff = w_end + timedelta(hours=validity_hours) if w_end else None
        if cutoff and now_utc > cutoff:
            continue

        # Status categorization
        if w_start and now_utc < w_start:
            timing_status = "SCHEDULED"
            sec_remaining = max(0, int((w_start - now_utc).total_seconds()))
        elif w_end and now_utc <= w_end:
            timing_status = "LIVE_OPEN"
            sec_remaining = max(0, int((w_end - now_utc).total_seconds()))
        else:
            timing_status = "EVALUATING"
            sec_remaining = 0

        # Multi-leg vs Single-leg inspection
        is_multi_leg = bool(getattr(rfq, 'legs', None) and len(rfq.legs) > 0)
        visible_legs = []

        if is_multi_leg:
            for leg in rfq.legs:
                l_id = str(leg.id)
                cfg = asgn.get_config_for_leg(l_id) if hasattr(asgn, 'get_config_for_leg') else None
                # Privacy Shield: do not leak invisible legs to this bank
                if cfg and cfg.is_invited is False:
                    continue

                # Check if this leg has been quoted or passed
                leg_offer = next((o for o in asgn.offers if str(o.leg_id or '') == l_id and not o.is_deleted), None)
                is_passed = bool(cfg and cfg.is_passed)

                pair_str = leg.currency_pair or f"{leg.buy_currency}/{leg.sell_currency}"
                visible_legs.append({
                    "leg_id": l_id,
                    "currency_pair": pair_str,
                    "direction": leg.direction or "BUY",
                    "amount": float(leg.amount or 0),
                    "currency": leg.buy_currency,
                    "value_date": getattr(cfg, 'value_date', None) or leg.value_date or "Spot (T+2)",
                    "allow_alternative_value_date": bool(getattr(cfg, 'allow_alternative_value_date', None) if (cfg and getattr(cfg, 'allow_alternative_value_date', None) is not None) else leg.allow_alternative_value_date),
                    "has_quote": leg_offer is not None,
                    "is_passed": is_passed
                })

            if len(visible_legs) == 0:
                # Bank was hidden from all legs
                continue

            summary_pair = f"{len(visible_legs)} Legs Package" if len(visible_legs) > 1 else visible_legs[0]["currency_pair"]
            distinct_currencies = list(dict.fromkeys(l["currency"] for l in visible_legs if l.get("currency")))
            if len(distinct_currencies) == 1 and len(visible_legs) > 0:
                summary_amount = sum(l["amount"] for l in visible_legs)
                summary_currency = distinct_currencies[0]
            else:
                summary_amount = None
                summary_currency = None

            summary_direction = visible_legs[0]["direction"] if len(visible_legs) == 1 else "PACKAGE"
            summary_value_date = visible_legs[0]["value_date"] if len(visible_legs) == 1 else "Multiple Dates"
            all_quoted = all(l["has_quote"] or l["is_passed"] for l in visible_legs) and any(l["has_quote"] for l in visible_legs)
            all_passed = all(l["is_passed"] for l in visible_legs)
        else:
            # Single-leg RFQ
            is_tbill = (rfq.type or "FX_SPOT").upper() == "TBILL"
            has_offer = False
            if is_tbill:
                has_offer = any(not o.is_deleted for o in asgn.tbill_offers)
            else:
                has_offer = any(not o.is_deleted for o in asgn.offers)

            summary_pair = f"{rfq.buy_currency}/{rfq.sell_currency}" if (rfq.buy_currency and rfq.sell_currency) else (rfq.type or "FX_SPOT")
            summary_amount = float(rfq.amount or 0)
            summary_direction = rfq.direction or "BUY"
            summary_value_date = rfq.value_date or "Spot (T+2)"
            all_quoted = has_offer
            all_passed = False

        # Desk Concurrency Lock from desk_session_service
        desk = desk_session_service._desks.get(asgn.id)
        if desk and desk.active_trader_email:
            now_check = datetime.now(timezone.utc)
            desk.check_and_release_expired_lock(now_check)

        if desk and desk.active_trader_email:
            is_you = (desk.active_trader_email.strip().lower() == current_dealer.email.strip().lower())
            desk_lock = {
                "is_locked": True,
                "locked_by_email": desk.active_trader_email,
                "locked_by_name": desk.active_trader_name or desk.active_trader_email.split("@")[0],
                "is_you": is_you,
                "spectators_count": len(desk.spectators)
            }
        else:
            desk_lock = {
                "is_locked": False,
                "locked_by_email": None,
                "locked_by_name": None,
                "is_you": False,
                "spectators_count": 0
            }

        customer_name = rfq.customer.name if rfq.customer else "Corporate Client"
        entity_name = getattr(rfq.entity, 'entity_name', getattr(rfq.entity, 'name', None)) if getattr(rfq, 'entity', None) else None

        live_tickets.append({
            "assignment_id": asgn.id,
            "token": asgn.token,
            "rfq_id": str(rfq.id),
            "ref_no": rfq.ref_no,
            "type": rfq.type or "FX_SPOT",
            "quotation_base": rfq.quotation_base or "Execution",
            "customer_name": customer_name,
            "entity_name": entity_name,
            "status": timing_status,
            "window_start": w_start.isoformat() if w_start else None,
            "window_end": w_end.isoformat() if w_end else None,
            "seconds_remaining": sec_remaining,
            "is_multi_leg": is_multi_leg,
            "visible_legs_count": len(visible_legs) if is_multi_leg else 1,
            "visible_legs": visible_legs,
            "summary_pair": summary_pair,
            "summary_amount": summary_amount,
            "summary_direction": summary_direction,
            "summary_value_date": summary_value_date,
            "allow_alternative_value_date": bool(rfq.allow_alternative_value_date),
            "has_quoted": all_quoted,
            "has_passed": all_passed,
            "desk_lock": desk_lock,
            "created_at": rfq.created_at.isoformat() if rfq.created_at else None
        })

    # Sort tickets: LIVE_OPEN first (by seconds_remaining asc), then SCHEDULED, then EVALUATING
    def sort_key(t):
        order = {"LIVE_OPEN": 0, "SCHEDULED": 1, "EVALUATING": 2}
        return (order.get(t["status"], 3), t["seconds_remaining"] if t["status"] == "LIVE_OPEN" else -t["seconds_remaining"])

    live_tickets.sort(key=sort_key)

    return {
        "success": True,
        "bank_name": current_dealer.bank.name if current_dealer.bank else "Partner Bank",
        "total_active_rfqs": len(live_tickets),
        "live_open_count": sum(1 for t in live_tickets if t["status"] == "LIVE_OPEN"),
        "scheduled_count": sum(1 for t in live_tickets if t["status"] == "SCHEDULED"),
        "evaluating_count": sum(1 for t in live_tickets if t["status"] == "EVALUATING"),
        "tickets": live_tickets
    }


@router.get("/blotter/history")
async def get_historical_trades_blotter(
    current_dealer: QuotationBankDealer = Depends(get_current_dealer),
    db: Session = Depends(get_db)
):
    """
    Historical Trades Blotter & Won Execution Archive:
    Returns full history of concluded trades for the dealer's bank.
    Includes:
    - Won firm trades with HMAC-SHA256 Cryptographic Deal Execution Receipts
    - Concluded rates, ticket amounts, and value dates
    - Clean Sweep vs Partial execution breakdown
    - Lost and passed historical records
    """
    from app.models.models_quotation import (
        QuotationBankAssignment, QuotationRequest, QuotationLeg,
        QuotationOffer, QuotationTBillOffer, QuotationAnalytics
    )
    from app.core.otp_security import generate_scoped_deal_receipt

    assignments = db.query(QuotationBankAssignment).join(
        QuotationBank, QuotationBankAssignment.quotation_bank_id == QuotationBank.id
    ).join(
        QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
    ).filter(
        QuotationBank.bank_id == current_dealer.bank_id,
        QuotationRequest.is_deleted == False,
        QuotationRequest.status.in_(["COMPLETED", "REJECTED", "CANCELLED", "EXPIRED", "INCONCLUSIVE"])
    ).order_by(QuotationRequest.updated_at.desc()).limit(100).all()

    history_records = []

    for asgn in assignments:
        rfq = asgn.rfq
        if not rfq:
            continue

        customer_name = rfq.customer.name if rfq.customer else "Corporate Client"
        bank_name = current_dealer.bank.name if current_dealer.bank else "Partner Bank"

        # Multi-leg vs Single-leg
        is_multi_leg = bool(getattr(rfq, 'legs', None) and len(rfq.legs) > 0)
        
        # Summary trading attributes
        if is_multi_leg:
            valid_legs = [l for l in rfq.legs if not l.is_deleted]
            summary_pair = f"{len(valid_legs)} Legs Package" if len(valid_legs) > 1 else (valid_legs[0].currency_pair or "FX Package")
            summary_direction = "PACKAGE"
            distinct_currencies = list(dict.fromkeys(l.buy_currency for l in valid_legs if l.buy_currency))
            if len(distinct_currencies) == 1 and valid_legs:
                summary_amount = sum(float(l.amount or 0) for l in valid_legs)
                summary_currency = distinct_currencies[0]
            else:
                summary_amount = None
                summary_currency = None
            summary_value_date = "Multiple Dates" if len(set(l.value_date for l in valid_legs if l.value_date)) > 1 else (valid_legs[0].value_date or "Spot (T+2)")
        else:
            distinct_pairs = [f"{rfq.buy_currency}/{rfq.sell_currency}" if (rfq.buy_currency and rfq.sell_currency) else (rfq.type or "FX_SPOT")]
            summary_pair = distinct_pairs[0]
            summary_direction = rfq.direction or "BUY"
            summary_amount = float(rfq.amount or 0)
            summary_currency = rfq.buy_currency or "USD"
            summary_value_date = rfq.value_date or "Spot (T+2)"

        # Check Dealer's own submitted quote on this RFQ
        dealer_offers = [o for o in asgn.offers if not o.is_deleted]
        dealer_tbills = [o for o in asgn.tbill_offers if not o.is_deleted]
        has_quoted = len(dealer_offers) > 0 or len(dealer_tbills) > 0

        clean_my_email = (current_dealer.email or "").strip().lower()
        my_offers = [o for o in dealer_offers if (o.submitted_by_email or "").strip().lower() == clean_my_email]
        my_tbills = [o for o in dealer_tbills if (o.submitted_by_email or "").strip().lower() == clean_my_email]
        quoted_by_me = len(my_offers) > 0 or len(my_tbills) > 0

        from app.services.tenant_key_service import tenant_key_service
        tenant_dek = None
        if getattr(rfq, 'customer_id', None):
            try:
                tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, rfq.customer_id)
            except Exception:
                tenant_dek = None

        dealer_rate = None
        dealer_submitted_at = None
        dealer_submitted_by = None
        dealer_notes = None

        if dealer_offers:
            latest_offer = sorted(dealer_offers, key=lambda x: x.submitted_at or datetime.min, reverse=True)[0]
            dealer_rate = tenant_key_service.resolve_offer_price(latest_offer, tenant_dek) if latest_offer else None
            dealer_submitted_at = latest_offer.submitted_at.isoformat() if latest_offer.submitted_at else None
            dealer_submitted_by = latest_offer.submitted_by_email
            dealer_notes = latest_offer.notes
        elif dealer_tbills:
            latest_tbill = sorted(dealer_tbills, key=lambda x: x.submitted_at or datetime.min, reverse=True)[0]
            dealer_rate = tenant_key_service.resolve_tbill_discount_rate(latest_tbill, tenant_dek) if latest_tbill else None
            dealer_submitted_at = latest_tbill.submitted_at.isoformat() if latest_tbill.submitted_at else None
            dealer_submitted_by = latest_tbill.submitted_by_email
            dealer_notes = latest_tbill.notes

        # Determine winning status & details
        won_legs = []
        all_legs_detail = []
        is_won = False
        winning_rate = None
        winning_bank_name = None

        if is_multi_leg:
            for idx, leg in enumerate(rfq.legs, 1):
                if leg.is_deleted:
                    continue
                pair_str = leg.currency_pair or f"{leg.buy_currency}/{leg.sell_currency}"
                leg_offer = next((o for o in dealer_offers if str(o.leg_id or '') == str(leg.id)), None)
                my_leg_rate = tenant_key_service.resolve_offer_price(leg_offer, tenant_dek) if leg_offer else None
                
                leg_is_won = (leg.winner_bank_id == current_dealer.bank_id)
                leg_win_rate = float(leg.winner_rate or leg.eval_rate or 0) if (leg.winner_rate or leg.eval_rate) else None
                
                leg_data = {
                    "leg_index": idx,
                    "leg_id": str(leg.id),
                    "pair": pair_str,
                    "direction": leg.direction or "BUY",
                    "amount": float(leg.amount or 0),
                    "currency": leg.buy_currency or "USD",
                    "value_date": leg.value_date or "Spot (T+2)",
                    "my_rate": my_leg_rate,
                    "winning_rate": leg_win_rate,
                    "winning_bank_name": (current_dealer.bank.name if current_dealer.bank else "This Bank") if leg_is_won else leg.winner_bank_name,
                    "is_won": leg_is_won,
                    "status": "WON" if leg_is_won else ("LOST" if leg.winner_bank_id else "UNAWARDED")
                }
                all_legs_detail.append(leg_data)
                
                if leg_is_won:
                    won_legs.append({
                        "leg_index": idx,
                        "leg_id": str(leg.id),
                        "pair": pair_str,
                        "direction": leg.direction or "BUY",
                        "amount": float(leg.amount or 0),
                        "currency": leg.buy_currency or "USD",
                        "rate": leg_win_rate or my_leg_rate or 0.0,
                        "value_date": leg.value_date or "Spot (T+2)"
                    })

            is_won = len(won_legs) > 0
            is_clean_sweep = is_won and (len(won_legs) == len(all_legs_detail))
            outcome_badge = "WON_CLEAN_SWEEP" if is_clean_sweep else (f"WON_{len(won_legs)}_OF_{len(all_legs_detail)}" if is_won else ("CANCELLED" if rfq.status in ["CANCELLED", "INCONCLUSIVE"] else ("EXPIRED" if rfq.status == "EXPIRED" else ("UNQUOTED" if not has_quoted else "LOST"))))
            winning_rate = won_legs[0]["rate"] if won_legs else (all_legs_detail[0]["winning_rate"] if all_legs_detail else None)
        else:
            # Single-leg RFQ
            primary_leg = rfq.legs[0] if (rfq.legs and len(rfq.legs) > 0) else None
            if primary_leg and primary_leg.winner_bank_id:
                is_won = bool(primary_leg.winner_bank_id == current_dealer.bank_id and rfq.status == "COMPLETED")
                winning_rate = float(primary_leg.winner_rate or primary_leg.eval_rate or 0) if (primary_leg.winner_rate or primary_leg.eval_rate) else None
                winning_bank_name = primary_leg.winner_bank_name
            elif rfq.eval_rate:
                winning_rate = float(rfq.eval_rate)
                if dealer_rate and abs(dealer_rate - winning_rate) < 0.00001 and rfq.status == "COMPLETED":
                    is_won = True

            outcome_badge = "WON" if is_won else ("CANCELLED" if rfq.status in ["CANCELLED", "INCONCLUSIVE"] else ("EXPIRED" if rfq.status == "EXPIRED" else ("UNQUOTED" if not has_quoted else "LOST")))

            if is_won:
                won_legs.append({
                    "leg_id": str(rfq.id),
                    "pair": summary_pair,
                    "direction": summary_direction,
                    "amount": summary_amount,
                    "currency": summary_currency,
                    "rate": winning_rate or dealer_rate or 0.0,
                    "value_date": summary_value_date
                })

        # Determine if this won deal was won by the authenticated dealer specifically
        is_won_by_me = False
        if is_won:
            if is_multi_leg:
                for leg in rfq.legs:
                    if leg.is_deleted or leg.winner_bank_id != current_dealer.bank_id:
                        continue
                    leg_my_offer = next((o for o in my_offers if str(o.leg_id or '') == str(leg.id)), None)
                    if leg_my_offer:
                        is_won_by_me = True
                        break
            else:
                is_won_by_me = quoted_by_me

        # Calculate Market Rank and Pricing Delta
        all_rfq_offers = db.query(QuotationOffer).join(
            QuotationBankAssignment, QuotationOffer.assignment_id == QuotationBankAssignment.id
        ).filter(
            QuotationBankAssignment.rfq_id == rfq.id,
            QuotationOffer.is_deleted == False
        ).all()

        all_prices = []
        for o in all_rfq_offers:
            p = tenant_key_service.resolve_offer_price(o, tenant_dek)
            if p and p > 0:
                all_prices.append(p)
        total_bidders = len(set(o.assignment_id for o in all_rfq_offers))
        dealer_rank = None
        spread_delta = None

        if dealer_rate is not None and len(all_prices) > 0:
            is_buy = (summary_direction or "BUY").upper() == "BUY"
            sorted_unique = sorted(list(set(all_prices)), reverse=(not is_buy))
            best_rate = sorted_unique[0]
            if winning_rate is None:
                winning_rate = best_rate
            try:
                dealer_rank = sorted_unique.index(dealer_rate) + 1
            except ValueError:
                dealer_rank = None

            if winning_rate is not None:
                spread_delta = round(dealer_rate - winning_rate, 4)

        # Generate Cryptographic Scoped Receipt if won
        receipt_data = None
        if is_won and won_legs:
            exec_time = rfq.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC") if rfq.updated_at else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            try:
                receipt_data = generate_scoped_deal_receipt(
                    rfq_id=str(rfq.id),
                    ref_no=rfq.ref_no,
                    customer_name=customer_name,
                    bank_id=current_dealer.bank_id,
                    bank_name=bank_name,
                    executed_legs=won_legs,
                    executed_at=exec_time
                )
            except Exception as e:
                logger.warning(f"Could not generate deal receipt: {e}")

        history_records.append({
            "assignment_id": asgn.id,
            "token": asgn.token,
            "rfq_id": str(rfq.id),
            "ref_no": rfq.ref_no,
            "customer_name": customer_name,
            "type": rfq.type or "FX_SPOT",
            "quotation_base": rfq.quotation_base or "Execution",
            "outcome": outcome_badge,
            "is_won": is_won,
            "is_won_by_me": is_won_by_me,
            "quoted_by_me": quoted_by_me,
            "has_quoted": has_quoted,
            "summary_pair": summary_pair,
            "summary_direction": summary_direction,
            "summary_amount": summary_amount,
            "summary_currency": summary_currency,
            "summary_value_date": summary_value_date,
            "dealer_rate": dealer_rate,
            "dealer_submitted_at": dealer_submitted_at,
            "dealer_submitted_by": dealer_submitted_by,
            "dealer_notes": dealer_notes,
            "winning_rate": winning_rate,
            "winning_bank_name": winning_bank_name,
            "dealer_rank": dealer_rank,
            "total_bidders": total_bidders,
            "spread_delta": spread_delta,
            "won_legs_count": len(won_legs),
            "total_legs_count": len(rfq.legs) if is_multi_leg else 1,
            "executed_legs": won_legs,
            "all_legs_detail": all_legs_detail if is_multi_leg else [],
            "receipt": receipt_data,
            "concluded_at": rfq.updated_at.isoformat() if rfq.updated_at else rfq.created_at.isoformat()
        })

    won_records = [h for h in history_records if h["is_won"]]
    quoted_records = [h for h in history_records if h["dealer_rate"] is not None]
    my_won_records = [h for h in won_records if h.get("is_won_by_me")]
    my_quoted_records = [h for h in quoted_records if h.get("quoted_by_me")]
    
    # Calculate won volume broken down by currency without ever mixing currencies
    won_volume_by_currency = {}
    for h in won_records:
        if h.get("all_legs_detail"):
            for leg in h["all_legs_detail"]:
                if leg.get("is_won"):
                    c = leg.get("currency") or "USD"
                    won_volume_by_currency[c] = won_volume_by_currency.get(c, 0.0) + float(leg.get("amount") or 0.0)
        elif h.get("summary_amount") and h.get("summary_currency"):
            c = h["summary_currency"]
            won_volume_by_currency[c] = won_volume_by_currency.get(c, 0.0) + float(h["summary_amount"] or 0.0)

    win_rate = round((len(won_records) / len(quoted_records) * 100), 1) if quoted_records else 0.0
    my_win_rate = round((len(my_won_records) / len(my_quoted_records) * 100), 1) if my_quoted_records else 0.0
    ranks = [h["dealer_rank"] for h in quoted_records if h["dealer_rank"] is not None]
    avg_rank = round(sum(ranks) / len(ranks), 1) if ranks else None

    return {
        "success": True,
        "bank_name": current_dealer.bank.name if current_dealer.bank else "Partner Bank",
        "total_deals": len(history_records),
        "won_deals_count": len(won_records),
        "my_won_deals_count": len(my_won_records),
        "quoted_deals_count": len(quoted_records),
        "my_quoted_deals_count": len(my_quoted_records),
        "my_win_rate_percent": my_win_rate,
        "won_volume_by_currency": won_volume_by_currency,
        "total_volume_won": won_volume_by_currency.get("USD", 0.0),
        "win_rate_percent": win_rate,
        "avg_dealer_rank": avg_rank,
        "records": history_records
    }


@router.post("/blotter/claim-desk")
async def claim_rfq_desk_lock(
    req: ClaimDeskRequest,
    current_dealer: QuotationBankDealer = Depends(get_current_dealer),
    db: Session = Depends(get_db)
):
    """
    Claims active exclusive trader control of an RFQ quotation desk ticket.
    Integrates directly with desk_session_service concurrency manager.
    """
    from app.services.desk_session_service import desk_session_service

    status_data = desk_session_service.get_or_claim_desk(
        assignment_id=req.assignment_id,
        email=current_dealer.email,
        name=current_dealer.full_name,
        role=current_dealer.role
    )

    return {
        "success": True,
        "desk_status": status_data
    }


@router.get("/achievements")
async def get_dealer_desk_achievements(
    current_dealer: QuotationBankDealer = Depends(get_current_dealer),
    db: Session = Depends(get_db)
):
    """
    Returns authentic institutional trophies, streaks, and gamification tiers
    for the authenticated bank trader and their institution's trading desk.
    """
    from app.services.dealer_achievement_service import dealer_achievement_service

    bank_id = current_dealer.bank_id
    bank_name = current_dealer.bank.name if current_dealer.bank else "Partner Bank"

    achievements = dealer_achievement_service.get_dealer_achievements(
        db,
        dealer_email=current_dealer.email,
        bank_id=bank_id,
        bank_name=bank_name,
        include_bank_desk=True
    )

    return {
        "success": True,
        "achievements": achievements
    }

