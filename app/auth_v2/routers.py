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

# Auth_v2 specific schemas and services
from app.schemas.all_schemas import (
    LoginRequest, ChangePasswordRequest, ForgotPasswordRequest,
    ResetPasswordRequest, UserAccountOut, AdminUserUpdate, Token
)
from app.auth_v2.services import auth_service # Import the instantiated service

# Constants for roles and permissions
from app.constants import UserRole

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/login", response_model=Token, status_code=status.HTTP_200_OK)
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    Authenticate user and return JWT token.
    Redirects to change-password if must_change_password = True.
    """
    try:
        # Call service to authenticate user and get a fresh token
        # The authenticate_user function in the service layer will be updated to include subscription status in the payload.
        auth_response = await auth_service.authenticate_user(
            db, form_data.username, form_data.password, request.client.host
        )

        if auth_response.get("must_change_password"):
            # If must_change_password is true, return a 307 redirect status
            # or a specific header for the frontend to handle.
            # Returning 200 OK with must_change_password flag is a common pattern.
            # The frontend should interpret this and redirect accordingly.
            return Token(
                access_token=auth_response["access_token"],
                token_type=auth_response["token_type"]
            )

        return Token(
            access_token=auth_response["access_token"],
            token_type=auth_response["token_type"]
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Login failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during login."
        )

@router.post("/change-password", response_model=Token, status_code=status.HTTP_200_OK)
async def change_password(
    request: Request,
    response: Response, # NEW: Add response parameter for the sliding expiration
    request_body: ChangePasswordRequest,
    current_user: security.TokenData = Depends(security.get_current_user), # Use get_current_user to allow password change even if must_change_password is true
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
        new_token = await auth_service.change_password(
            db,
            current_user,
            request_body,
            request.client.host,
            is_first_login_change=is_first_login_change
        )
        # Note: The `get_current_user` dependency already added a new token to the response header.
        return new_token
    except HTTPException as e:
        raise e
    except ValueError as e: # Catch validation errors from schemas or service
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
            db, request_body.email, request.client.host
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
            request.client.host # Pass the IP address
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
            request.client.host
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
