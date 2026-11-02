# app/auth_v2/routers.py
import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

# Core security and database dependencies
from app.database import get_db
import app.core.security as security
# NEW: Import the custom token creation function with a specific name
from app.core.security import create_access_token as create_fresh_access_token 
# NEW: Import the new centralized IP resolution function
from app.core.security import get_client_ip

# Auth_v2 specific schemas and services
from app.schemas.all_schemas import (
    LoginRequest, ChangePasswordRequest, ForgotPasswordRequest,
    ResetPasswordRequest, UserAccountOut, AdminUserUpdate, Token,
    UserLegalAcceptanceRequest, VerifyMFARequest
)
from app.auth_v2.services import auth_service # Import the instantiated service

# Constants for roles and permissions
from app.constants import UserRole
from app.core.honeypot import verify_honeypot

logger = logging.getLogger(__name__)

router = APIRouter()

# app/auth_v2/routers.py

@router.post("/login",  status_code=status.HTTP_200_OK)
async def login_for_access_token(
    request: Request,
    login_data: LoginRequest, # Change from OAuth2PasswordRequestForm to LoginRequest
    db: Session = Depends(get_db)
):
    """
    Authenticate user and return JWT or MFA requirement.
    """
    # Perimeter Anti-Bot Honeypot Defense
    await verify_honeypot(
        login_data.website_url,
        request=request,
        context="login",
        legitimate_values=[login_data.email]
    )

    try:
        # Pass the fields from login_data to the service
        auth_response = await auth_service.authenticate_user(
            db=db,
            email=login_data.email,
            password=login_data.password,
            request_ip=get_client_ip(request),
            device_id=login_data.device_id,  # This fixes the missing argument error
            remember_me=login_data.remember_me
        )

        if not auth_response:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        return auth_response

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Login failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during login."
        )

@router.post("/change-password", response_model=Dict[str, Any], status_code=status.HTTP_200_OK)
async def change_password(
    request: Request,
    response: Response,
    request_body: ChangePasswordRequest,
    current_user: security.TokenData = Depends(security.get_current_user),
    db: Session = Depends(get_db)
):
    """
    Allows an authenticated user to change their password.
    Clears the must_change_password flag on success.
    """
    if current_user.must_change_password:
        is_first_login_change = True
    else:
        is_first_login_change = False

    try:
        new_token_data = await auth_service.change_password(
            db,
            current_user,
            request_body,
            get_client_ip(request),
            is_first_login_change=is_first_login_change
        )
        return new_token_data
    except HTTPException as e:
        raise e
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Password change failed for user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during password change."
        )

@router.post("/forgot-password", status_code=status.HTTP_202_ACCEPTED)
async def forgot_password(
    request: Request,
    request_body: ForgotPasswordRequest,
    db: Session = Depends(get_db)
):
    """
    Initiates the password reset process by sending a reset link to the user's email.
    """
    # Perimeter Anti-Bot Honeypot Defense
    await verify_honeypot(
        request_body.website_url,
        request=request,
        context="forgot_password",
        legitimate_values=[request_body.email]
    )

    try:
        await auth_service.initiate_password_reset(
            db, request_body.email, get_client_ip(request), request=request
        )
        return {"message": "If an account with that email exists, a password reset link has been sent."}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Forgot password initiation failed for email {request_body.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during password reset initiation."
        )

@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    request: Request,
    request_body: ResetPasswordRequest,
    db: Session = Depends(get_db)
):
    """
    Resets the user's password using a valid reset token.
    """
    try:
        # MODIFIED: Pass request_body object and request.client.host correctly
        await auth_service.reset_password(
            db,
            request_body, # Pass the entire request_body object
            get_client_ip(request) # Pass the IP address
        )
        return {"message": "Password has been successfully reset."}
    except HTTPException as e:
        raise e
    except ValueError as e: # Catch validation errors from schemas or service
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Password reset failed for token: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during password reset."
        )

@router.get("/account/me", response_model=UserAccountOut, status_code=status.HTTP_200_OK)
async def get_my_account_info(
    current_user: security.TokenData = Depends(security.get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Retrieves the currently authenticated user's account information.
    """
    try:
        user_info = await auth_service.get_user_account_info(db, current_user)
        return user_info
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to retrieve account info for user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching account information."
        )

@router.patch("/admin/users/{user_id}/password-reset", response_model=UserAccountOut, status_code=status.HTTP_200_OK)
async def admin_set_user_password(
    user_id: int,
    request_body: AdminUserUpdate,
    request: Request,
    admin_user: security.TokenData = Depends(security.get_current_user), # Can be SO or CA
    db: Session = Depends(get_db)
):
    """
    Allows System Owners or Corporate Admins to set/reset a user's password.
    Adheres to the role hierarchy.
    """
    # Ensure the admin user has privilege to perform this action
    if admin_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only System Owners or Corporate Admins can perform this action."
        )

    try:
        updated_user = await auth_service.admin_set_user_password(
            db,
            user_id,
            request_body,
            admin_user,
            get_client_ip(request)
        )
        return updated_user
    except HTTPException as e:
        raise e
    except ValueError as e: # Catch validation errors from schemas or service
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Admin password reset failed for target user {user_id} by admin {admin_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during admin password reset."
        )

@router.get("/admin/audit-logs/authentication", status_code=status.HTTP_200_OK)
async def admin_view_auth_logs(
    admin_user: security.TokenData = Depends(security.get_current_user), # Can be SO or CA
    db: Session = Depends(get_db),
    user_id: Optional[int] = None,
    email: Optional[str] = None,
    action_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
):
    """
    Allows System Owners or Corporate Admins to view authentication-related audit logs.
    System Owners can view all logs. Corporate Admins can view logs for their own customer.
    """
    # Ensure the admin user has privilege to perform this action
    if admin_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have privileges to view audit logs."
        )
    
    # Specific permission check for viewing audit logs.
    # A System Owner would typically have "audit_log:view_all"
    # A Corporate Admin might have "audit_log:view_auth" for their customer.
    # The `auth_service.admin_view_auth_logs` already handles the customer scope.
    if "audit_log:view_all" not in admin_user.permissions and "audit_log:view_auth" not in admin_user.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have the necessary permission (audit_log:view_auth or audit_log:view_all) to view audit logs."
        )


    try:
        logs_data = await auth_service.admin_view_auth_logs(
            db, admin_user, user_id, email, action_type, skip, limit
        )
        return logs_data
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to retrieve authentication audit logs for admin {admin_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching audit logs."
        )

@router.post("/policies/accept", status_code=status.HTTP_200_OK)
async def accept_policies(
    request: Request,
    request_body: UserLegalAcceptanceRequest,  # <-- CRITICAL FIX: Add the request body parameter
    current_user: security.TokenData = Depends(security.get_current_user),
    db: Session = Depends(get_db)
):
    """
    Allows an authenticated user to record their acceptance of the latest legal policies.
    """
    try:
        # Pass the request body and user data to the service layer for processing
        await auth_service.accept_legal_policies(
            db, 
            current_user,
            request_body,
            get_client_ip(request)
        )
        
        return {"message": "Legal policies accepted successfully."}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to accept policies for user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while accepting policies."
        )


@router.post("/refresh-token", response_model=Token)
async def refresh_token(
    current_user: security.TokenData = Depends(security.get_current_active_user)
):
    """
    Generates a new JWT for an already authenticated user to extend their session.
    """
    # Map the current TokenData back to a dictionary for encoding
    new_data = {
        "sub": current_user.email,
        "user_id": current_user.user_id,
        "role": current_user.role.value if hasattr(current_user.role, 'value') else str(current_user.role),
        "permissions": current_user.permissions,
        "customer_id": current_user.customer_id,
        "customer_name": getattr(current_user, 'customer_name', None),
        "subscription_status": current_user.subscription_status.value if current_user.subscription_status else None,
        "subscription_end_date": getattr(current_user, 'subscription_end_date', None),
        "has_all_entity_access": current_user.has_all_entity_access,
        "entity_ids": current_user.entity_ids,
        "must_change_password": current_user.must_change_password,
        "must_accept_policies": current_user.must_accept_policies,
        "last_accepted_legal_version": current_user.last_accepted_legal_version,
        "has_custody_module": current_user.has_custody_module,
        "has_issuance_module": current_user.has_issuance_module,
        "has_quotation_module": current_user.has_quotation_module,
        "has_reconciliation_module": current_user.has_reconciliation_module,
        "can_email_inbox": getattr(current_user, 'can_email_inbox', True),
    }
    
    # create_fresh_access_token is the alias for create_access_token
    new_token = create_fresh_access_token(data=new_data)
    
    return Token(
        access_token=new_token,
        token_type="bearer",
        must_accept_policies=current_user.must_accept_policies
    )

@router.post("/verify-mfa", response_model=Token)
async def verify_mfa(
    request: Request,
    verify_data: VerifyMFARequest,
    db: Session = Depends(get_db)
):
    """
    Verifies the 6-digit email code. If valid, trusts the device 
    and returns a full access token.
    """
    # 1. Decode the session token to ensure it's a valid MFA attempt
    
    try:
        payload = security.jwt.decode(
            verify_data.mfa_session_token, 
            security.SECRET_KEY, 
            algorithms=[security.ALGORITHM]
        )
        if payload.get("is_mfa_verified") is not True: 
            # Note: We set this to False in Phase 2 for MFA tokens
            pass 
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid MFA session.")

    # 2. Call the service to validate the code and trust the device
    user_data = await auth_service.verify_mfa_code(
        db, 
        email=verify_data.email, 
        code=verify_data.mfa_code,
        device_id=verify_data.device_id,
        request_ip=get_client_ip(request),
        remember_me=verify_data.remember_me
    )

    if not user_data:
        raise HTTPException(status_code=400, detail="Invalid or expired verification code.")

    return user_data

@router.post("/resend-mfa")
async def resend_mfa_code(
    request: Request,
    verify_data: VerifyMFARequest,
    db: Session = Depends(get_db)
):
    """Generates and sends a new MFA code for an active session."""
    # SURGICAL FIX: Ensure User is imported
    from app.models import User 

    # 1. Validate the session token
    try:
        security.jwt.decode(verify_data.mfa_session_token, security.SECRET_KEY, algorithms=[security.ALGORITHM])
    except Exception:
        raise HTTPException(status_code=401, detail="Session expired. Please log in again.")

    # 2. Fetch the user 
    user = db.query(User).filter(User.email == verify_data.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    # 3. Trigger new flow
    await auth_service.trigger_mfa_flow(db, user)
    
    return {"message": "A new verification code has been sent to your email."}


# ── WebAuthn Biometric Authentication (Face ID / Touch ID / Fingerprint) ───────
# For Permanent Platform Users (End Users, Corporate Admins, Checkers, System Owners).

from datetime import datetime, timezone
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import selectinload
from app.models.models_push import UserWebAuthnCredential
from app.core.webauthn_service import (
    build_registration_options,
    build_authentication_options,
    verify_registration_response,
    verify_authentication_assertion,
    resolve_rp_id,
)
from app.crud.crud import log_action
from app.constants import AUDIT_ACTION_TYPE_LOGIN_SUCCESS, AUDIT_ACTION_TYPE_UPDATE


class WebAuthnRegisterOptionsIn(BaseModel):
    rp_id: Optional[str] = Field(None, max_length=255)


class WebAuthnRegisterVerifyIn(BaseModel):
    challenge_token: str
    credential_id: str = Field(..., max_length=512)
    client_data_json: str
    attestation_object: Optional[str] = None
    authenticator_data: Optional[str] = None
    public_key_spki: Optional[str] = None
    public_key_alg: Optional[int] = -7
    device_id: Optional[str] = Field(None, max_length=64)
    device_label: Optional[str] = Field(None, max_length=120)
    platform: Optional[str] = Field(None, max_length=20)


class WebAuthnAuthOptionsIn(BaseModel):
    email: Optional[str] = Field(None, max_length=255)
    credential_id: Optional[str] = Field(None, max_length=512)
    device_id: Optional[str] = Field(None, max_length=64)
    rp_id: Optional[str] = Field(None, max_length=255)


class WebAuthnAuthVerifyIn(BaseModel):
    challenge_token: str
    credential_id: str = Field(..., max_length=512)
    client_data_json: str
    authenticator_data: str
    signature: str
    device_id: Optional[str] = Field(None, max_length=64)


@router.post("/webauthn/register/options", status_code=status.HTTP_200_OK)
async def webauthn_register_options(
    request: Request,
    payload: WebAuthnRegisterOptionsIn,
    current_user: security.TokenData = Depends(security.get_current_active_user),
    db: Session = Depends(get_db),
):
    """Generates WebAuthn registration options for an authenticated permanent user."""
    from app.models import User
    user = db.query(User).filter(User.id == current_user.user_id, User.is_deleted == False).first()
    if not user:
        raise HTTPException(status_code=404, detail="User account not found.")

    rp_id = resolve_rp_id(request, payload.rp_id)
    existing_creds = [
        c.credential_id
        for c in db.query(UserWebAuthnCredential).filter(
            UserWebAuthnCredential.user_id == user.id,
            UserWebAuthnCredential.is_active == True,
            UserWebAuthnCredential.is_deleted == False,
        ).all()
    ]
    full_name = getattr(user, "full_name", None) or getattr(user, "name", None) or user.email
    return build_registration_options(
        user_id=user.id,
        user_email=user.email,
        user_display_name=full_name,
        rp_id=rp_id,
        existing_credential_ids=existing_creds,
    )


@router.post("/webauthn/register/verify", status_code=status.HTTP_200_OK)
async def webauthn_register_verify(
    request: Request,
    payload: WebAuthnRegisterVerifyIn,
    db: Session = Depends(get_db),
):
    """
    Verifies and stores a newly created WebAuthn biometric credential (Face ID / Fingerprint).
    Authorized by the signed challenge_token issued during QR pairing or authenticated setup.
    """
    from app.models import User, UserDevice

    verified = verify_registration_response(
        challenge_token=payload.challenge_token,
        credential_id=payload.credential_id,
        client_data_json_b64=payload.client_data_json,
        attestation_object_b64=payload.attestation_object,
        authenticator_data_b64=payload.authenticator_data,
        public_key_spki_b64=payload.public_key_spki,
        public_key_alg=payload.public_key_alg,
    )

    user = db.query(User).filter(User.id == verified["user_id"], User.is_deleted == False).first()
    if not user:
        raise HTTPException(status_code=404, detail="User account not found.")

    cred_id = verified["credential_id"]
    cred = db.query(UserWebAuthnCredential).filter(
        UserWebAuthnCredential.credential_id == cred_id
    ).first()

    platform = (payload.platform or "mobile").lower()[:20]
    label = payload.device_label or "Smartphone Biometrics (Face ID / Fingerprint)"

    if cred is None:
        cred = UserWebAuthnCredential(credential_id=cred_id, user_id=user.id, public_key_spki=verified["public_key_spki"])
        db.add(cred)

    cred.user_id = user.id
    cred.customer_id = user.customer_id
    cred.public_key_spki = verified["public_key_spki"]
    cred.algorithm = verified["algorithm"]
    cred.sign_count = verified["sign_count"]
    cred.device_id = payload.device_id
    cred.device_label = label
    cred.platform = platform
    cred.is_active = True
    cred.is_deleted = False
    cred.last_used_at = datetime.now(timezone.utc)

    # Ensure the device is marked as trusted for MFA
    if payload.device_id:
        device_rec = db.query(UserDevice).filter(
            UserDevice.user_id == user.id,
            UserDevice.device_id == payload.device_id,
        ).first()
        if not device_rec:
            device_rec = UserDevice(
                user_id=user.id,
                device_id=payload.device_id,
                device_name=label,
                is_trusted=True,
                last_ip=get_client_ip(request),
            )
            db.add(device_rec)
        else:
            device_rec.is_trusted = True
            device_rec.last_ip = get_client_ip(request)

    db.commit()

    log_action(
        db,
        user_id=user.id,
        action_type=AUDIT_ACTION_TYPE_UPDATE,
        entity_type="UserWebAuthnCredential",
        entity_id=cred.id,
        details={"email": user.email, "action": "Biometric credential enrolled", "device_label": label},
        ip_address=get_client_ip(request),
        customer_id=user.customer_id,
    )

    return {
        "success": True,
        "credential_id": cred_id,
        "user_email": user.email,
        "device_label": label,
    }


@router.post("/webauthn/authenticate/options", status_code=status.HTTP_200_OK)
async def webauthn_authenticate_options(
    request: Request,
    payload: WebAuthnAuthOptionsIn,
    db: Session = Depends(get_db),
):
    """
    Returns a WebAuthn assertion challenge for Face ID / Fingerprint login on LoginPage.
    Matches credentials by credential_id, device_id, or email, and also supports discoverable passkeys.
    """
    from app.models import User

    rp_id = resolve_rp_id(request, payload.rp_id)
    allowed_ids: List[str] = []
    matched_email: Optional[str] = None

    base_q = db.query(UserWebAuthnCredential).join(
        User, User.id == UserWebAuthnCredential.user_id
    ).filter(
        UserWebAuthnCredential.is_active == True,
        UserWebAuthnCredential.is_deleted == False,
        User.is_deleted == False,
    )

    if payload.credential_id:
        cred = base_q.filter(UserWebAuthnCredential.credential_id == payload.credential_id.strip()).first()
        if cred:
            allowed_ids.append(cred.credential_id)
            if cred.user:
                matched_email = cred.user.email

    if not allowed_ids and payload.device_id:
        creds = base_q.filter(UserWebAuthnCredential.device_id == payload.device_id.strip()).all()
        for c in creds:
            if c.credential_id not in allowed_ids:
                allowed_ids.append(c.credential_id)
            if not matched_email and c.user:
                matched_email = c.user.email

    if payload.email and not allowed_ids:
        creds = base_q.filter(func.lower(User.email) == payload.email.strip().lower()).all()
        for c in creds:
            if c.credential_id not in allowed_ids:
                allowed_ids.append(c.credential_id)
            if not matched_email and c.user:
                matched_email = c.user.email

    options = build_authentication_options(rp_id=rp_id, allowed_credential_ids=allowed_ids)
    options["has_registered_credentials"] = len(allowed_ids) > 0
    options["user_email"] = matched_email
    return options


@router.post("/webauthn/authenticate/verify", status_code=status.HTTP_200_OK)
async def webauthn_authenticate_verify(
    request: Request,
    payload: WebAuthnAuthVerifyIn,
    db: Session = Depends(get_db),
):
    """
    Verifies a Face ID / Touch ID / Fingerprint assertion and issues a full JWT access token
    for permanent users (End Users, Corporate Admins, Checkers, System Owners).
    """
    from app.models import User, Customer, UserDevice

    cred = db.query(UserWebAuthnCredential).filter(
        UserWebAuthnCredential.credential_id == payload.credential_id.strip(),
        UserWebAuthnCredential.is_active == True,
        UserWebAuthnCredential.is_deleted == False,
    ).first()
    if not cred:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="This biometric credential is not recognized or has been revoked. Please sign in with your password.",
        )

    user = db.query(User).options(
        selectinload(User.customer).selectinload(Customer.subscription_plan),
        selectinload(User.entity_associations),
    ).filter(
        User.id == cred.user_id,
        User.is_deleted == False,
    ).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User account is inactive or deleted.")

    now = datetime.now(timezone.utc)
    if user.locked_until and user.locked_until > now:
        time_remaining = user.locked_until - now
        minutes, seconds = divmod(time_remaining.total_seconds(), 60)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account is locked. Please try again after {int(minutes)} minutes and {int(seconds)} seconds.",
        )

    new_sign_count = verify_authentication_assertion(
        challenge_token=payload.challenge_token,
        client_data_json_b64=payload.client_data_json,
        authenticator_data_b64=payload.authenticator_data,
        signature_b64=payload.signature,
        public_key_spki_b64=cred.public_key_spki,
        stored_sign_count=cred.sign_count or 0,
    )

    cred.sign_count = new_sign_count
    cred.last_used_at = now
    if payload.device_id and not cred.device_id:
        cred.device_id = payload.device_id

    user.failed_login_attempts = 0
    user.locked_until = None

    eff_device_id = payload.device_id or cred.device_id
    if eff_device_id:
        device_rec = db.query(UserDevice).filter(
            UserDevice.user_id == user.id,
            UserDevice.device_id == eff_device_id,
        ).first()
        if not device_rec:
            device_rec = UserDevice(
                user_id=user.id,
                device_id=eff_device_id,
                device_name=cred.device_label or "Biometric Mobile Device",
                is_trusted=True,
                last_ip=get_client_ip(request),
            )
            db.add(device_rec)
        else:
            device_rec.is_trusted = True
            device_rec.last_ip = get_client_ip(request)

    db.commit()

    token_data = await auth_service.build_user_token_data(db, user)
    access_token = create_fresh_access_token(data=token_data)

    log_action(
        db,
        user_id=user.id,
        action_type=AUDIT_ACTION_TYPE_LOGIN_SUCCESS,
        entity_type="User",
        entity_id=user.id,
        details={"email": user.email, "method": "biometric_webauthn", "device_label": cred.device_label},
        ip_address=get_client_ip(request),
        customer_id=user.customer_id,
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "must_accept_policies": token_data.get("must_accept_policies", False),
        "user": {
            "id": user.id,
            "email": user.email,
            "role": token_data["role"],
            "customer_id": user.customer_id,
            "subscription_status": token_data.get("subscription_status"),
            "subscription_end_date": token_data.get("subscription_end_date"),
        },
    }
