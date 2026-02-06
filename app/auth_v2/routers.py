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
    try:
        await auth_service.initiate_password_reset(
            db, request_body.email, get_client_ip(request)
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
        "role": current_user.role.value,
        "customer_id": current_user.customer_id,
        "subscription_status": current_user.subscription_status.value if current_user.subscription_status else None,
        "has_all_entity_access": current_user.has_all_entity_access,
        "entity_ids": current_user.entity_ids,
        "must_change_password": current_user.must_change_password,
        "must_accept_policies": current_user.must_accept_policies,
        "last_accepted_legal_version": current_user.last_accepted_legal_version
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