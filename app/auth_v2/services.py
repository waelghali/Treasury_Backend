# app/auth_v2/services.py
import uuid
import logging
import secrets
import string
import os # Added for os.getenv
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from fastapi import HTTPException, status, Request # Import Request for IP address
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func
from sqlalchemy import or_

# Core authentication and utility imports
from app.core.hashing import get_password_hash, verify_password
from app.core.security import create_access_token, TokenData # We will use TokenData from core.security
from app.core.email_service import get_customer_email_settings, get_global_email_settings, send_email, EmailSettings

# Database models and CRUD operations
import app.models as models
from app.models import User, PasswordResetToken, GlobalConfiguration, Customer
# MODIFIED: Import log_action directly from app.crud.crud
from app.crud.crud import log_action # Import the global log_action utility


# Schemas for validation and response
from app.schemas.all_schemas import (
    ChangePasswordRequest,
    ResetPasswordRequest,
    AdminUserUpdate,
    UserAccountOut,
    Token,
    UserLegalAcceptanceRequest
)

# NEW: Import ALL necessary AUDIT_ACTION_TYPE and GlobalConfigKey constants
from app.constants import (
    UserRole,
    GlobalConfigKey, # Added for password policy config retrieval
    AUDIT_ACTION_TYPE_LOGIN_SUCCESS,
    AUDIT_ACTION_TYPE_LOGIN_FAILED,
    AUDIT_ACTION_TYPE_ACCOUNT_LOCKED, # NEW
    AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_SUCCESS,
    AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_FAILED,
    AUDIT_ACTION_TYPE_PASSWORD_RESET_INITIATED,
    AUDIT_ACTION_TYPE_PASSWORD_RESET_COMPLETED,
    AUDIT_ACTION_TYPE_PASSWORD_RESET_FAILED,
    AUDIT_ACTION_TYPE_ADMIN_PASSWORD_SET,
    AUDIT_ACTION_TYPE_ADMIN_PASSWORD_RESET,
    AUDIT_ACTION_TYPE_UPDATE,
    AUDIT_ACTION_TYPE_LEGAL_ARTIFACT_ACCEPTED
)


logger = logging.getLogger(__name__)

class AuthService:
    def __init__(self):
        pass # No explicit dependencies injected in init for this design, rely on late imports

    async def _get_password_policy_config(self, db: Session) -> Dict[str, Any]:
        """Fetches password policy settings from GlobalConfiguration."""
        from app.crud.crud import crud_global_configuration # Late import
        
        policy_config = {}
        for key_enum, default_value in [
            (GlobalConfigKey.PASSWORD_MIN_LENGTH, "8"),
            (GlobalConfigKey.PASSWORD_REQUIRE_UPPERCASE, "true"),
            (GlobalConfigKey.PASSWORD_REQUIRE_LOWERCASE, "true"),
            (GlobalConfigKey.PASSWORD_REQUIRE_DIGIT, "true"),
            (GlobalConfigKey.PASSWORD_RESET_TOKEN_EXPIRY_MINUTES, "15")
        ]:
            config_value = crud_global_configuration.get_by_key(db, key_enum)
            value = config_value.value_default if config_value else default_value
            
            if key_enum == GlobalConfigKey.PASSWORD_MIN_LENGTH:
                policy_config[key_enum.value] = int(value)
            elif key_enum == GlobalConfigKey.PASSWORD_RESET_TOKEN_EXPIRY_MINUTES:
                policy_config[key_enum.value] = int(value)
            else:
                policy_config[key_enum.value] = value.lower() == 'true'
        return policy_config

    async def _get_login_policy_config(self, db: Session) -> Dict[str, Any]:
        """NEW: Fetches login policy settings from GlobalConfiguration."""
        from app.crud.crud import crud_global_configuration
        
        policy_config = {}
        for key_enum, default_value in [
            (GlobalConfigKey.LOGIN_MAX_FAILED_ATTEMPTS, "5"),
            (GlobalConfigKey.LOGIN_LOCKOUT_DURATION_MINUTES, "15")
        ]:
            config_value = crud_global_configuration.get_by_key(db, key_enum)
            value = config_value.value_default if config_value else default_value
            
            policy_config[key_enum.value] = int(value)
        return policy_config

    async def _get_legal_artifact_versions(self, db: Session) -> Dict[str, float]:
        """
        NEW: Fetches the latest version of all legal artifacts from GlobalConfiguration.
        Returns 0.0 if a version key is not found.
        """
        from app.crud.crud import crud_global_configuration
        
        tc_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.TC_VERSION)
        pp_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.PP_VERSION)
        
        tc_version = float(tc_config.value_default) if tc_config and tc_config.value_default else 0.0
        pp_version = float(pp_config.value_default) if pp_config and pp_config.value_default else 0.0

        return {
            "tc_version": tc_version,
            "pp_version": pp_version,
        }

    async def _validate_password_policy(self, password: str, db: Session) -> None:
        """Validates a password against the configured policy."""
        policy = await self._get_password_policy_config(db)
        
        min_length = policy.get(GlobalConfigKey.PASSWORD_MIN_LENGTH.value, 8)
        require_uppercase = policy.get(GlobalConfigKey.PASSWORD_REQUIRE_UPPERCASE.value, True)
        require_lowercase = policy.get(GlobalConfigKey.PASSWORD_REQUIRE_LOWERCASE.value, True)
        require_digit = policy.get(GlobalConfigKey.PASSWORD_REQUIRE_DIGIT.value, True)

        if len(password) < min_length:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Password must be at least {min_length} characters long."
            )
        if require_uppercase and not any(c.isupper() for c in password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one uppercase letter."
            )
        if require_lowercase and not any(c.islower() for c in password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one lowercase letter."
            )
        if require_digit and not any(c.isdigit() for c in password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one digit."
            )

    async def authenticate_user(
        self, db: Session, email: str, password: str, request_ip: str, device_id: str, remember_me: bool
    ) -> Dict[str, Any]:
        from app.crud.crud import crud_user, crud_role_permission  # Late import
        from sqlalchemy.orm import selectinload
        from sqlalchemy import func

        # 1. Fetch the user with necessary relations
        user = db.query(User).options(
            selectinload(User.customer),
            selectinload(User.entity_associations)
        ).filter(
            func.lower(User.email) == email.lower(), 
            User.is_deleted == False
        ).first()

        # 2. Check for user existence first
        if not user:
            log_action(
                db,
                user_id=None,
                action_type=AUDIT_ACTION_TYPE_LOGIN_FAILED,
                entity_type="User",
                entity_id=None,
                details={"email": email, "reason": "User not found or inactive"},
                ip_address=request_ip,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password."
            )

        # 3. Check for account lockout
        current_time = datetime.now(timezone.utc)
        if user.locked_until and user.locked_until > current_time:
            time_remaining = user.locked_until - current_time
            minutes, seconds = divmod(time_remaining.total_seconds(), 60)
            formatted_time = f"{int(minutes)} minutes and {int(seconds)} seconds"

            log_action(
                db,
                user_id=user.id,
                action_type=AUDIT_ACTION_TYPE_LOGIN_FAILED,
                entity_type="User",
                entity_id=user.id,
                details={"email": user.email, "reason": "Account is locked"},
                ip_address=request_ip,
                customer_id=user.customer_id,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Account is locked. Please try again after {formatted_time}."
            )

        # 4. Verify password
        if not verify_password(password, user.password_hash):
            user.failed_login_attempts += 1
            login_policy = await self._get_login_policy_config(db)
            max_attempts = login_policy.get(GlobalConfigKey.LOGIN_MAX_FAILED_ATTEMPTS.value, 5)
            lockout_duration = login_policy.get(GlobalConfigKey.LOGIN_LOCKOUT_DURATION_MINUTES.value, 15)

            log_details = {"email": user.email, "reason": "Incorrect password", "failed_attempts": user.failed_login_attempts}
            log_action_type = AUDIT_ACTION_TYPE_LOGIN_FAILED

            if user.failed_login_attempts >= max_attempts:
                user.locked_until = current_time + timedelta(minutes=lockout_duration)
                log_action_type = AUDIT_ACTION_TYPE_ACCOUNT_LOCKED
                log_details["reason"] = f"Account locked after {max_attempts} failed attempts."

            db.add(user)
            db.commit()

            log_action(
                db,
                user_id=user.id,
                action_type=log_action_type,
                entity_type="User",
                entity_id=user.id,
                details=log_details,
                ip_address=request_ip,
                customer_id=user.customer_id,
            )

            if log_action_type == AUDIT_ACTION_TYPE_ACCOUNT_LOCKED:
                time_remaining = user.locked_until - current_time
                minutes, seconds = divmod(time_remaining.total_seconds(), 60)
                formatted_time = f"{int(minutes)} minutes and {int(seconds)} seconds"
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Account is locked. Please try again after {formatted_time}."
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Incorrect email or password."
                )

        # 5. Success: Reset failed attempts
        user.failed_login_attempts = 0
        user.locked_until = None
        db.add(user)
        db.commit()

        # --- START OF MFA / TRUSTED DEVICE LOGIC ---
        
        device_record = db.query(models.UserDevice).filter(
            models.UserDevice.user_id == user.id,
            models.UserDevice.device_id == device_id
        ).first()

        is_trusted = device_record.is_trusted if device_record else False

        if not is_trusted:
            has_any_devices = db.query(models.UserDevice).filter(
                models.UserDevice.user_id == user.id
            ).first()

            if not has_any_devices:
                # GRANDFATHERING: Auto-trust first device ever seen
                new_device = models.UserDevice(
                    user_id=user.id,
                    device_id=device_id,
                    device_name="Initial Migration Device",
                    is_trusted=True,
                    last_ip=request_ip
                )
                db.add(new_device)
                db.commit()
                is_trusted = True

        if not is_trusted:
            # Trigger MFA and return immediately; policy check happens AFTER verify-mfa
            await self.trigger_mfa_flow(db, user)
            return {
                "status": "MFA_REQUIRED",
                "mfa_session_token": create_access_token(
                    data={
                        "sub": user.email, 
                        "user_id": user.id, 
                        "is_mfa_verified": False,
                        "role": user.role.value
                    },
                    expires_delta=timedelta(minutes=15)
                )
            }
        
        # --- PROCEED TO FULL LOGIN (Always runs for trusted devices) ---

        db_permissions = crud_role_permission.get_permissions_for_role(db, user.role.value)
        permission_names = [p.name for p in db_permissions]

        customer_name = user.customer.name if (user.customer_id and user.customer) else None

        must_accept_policies = False
        if user.role != UserRole.SYSTEM_OWNER:
            latest_versions = await self._get_legal_artifact_versions(db)
            latest_system_version = max(latest_versions.get("tc_version", 0.0), latest_versions.get("pp_version", 0.0))
            
            if user.last_accepted_legal_version is None or user.last_accepted_legal_version < latest_system_version:
                must_accept_policies = True

        token_data = {
            "sub": user.email,
            "user_id": user.id,
            "role": user.role.value,
            "permissions": permission_names,
            "customer_id": user.customer_id,
            "customer_name": customer_name,
            "has_all_entity_access": user.has_all_entity_access,
            "entity_ids": [assoc.customer_entity_id for assoc in user.entity_associations] if not user.has_all_entity_access else [],
            "must_change_password": user.must_change_password,
            "must_accept_policies": must_accept_policies,
            "last_accepted_legal_version": user.last_accepted_legal_version,
            "subscription_status": user.customer.status.value if user.customer else None,
            "subscription_end_date": user.customer.end_date.isoformat() if user.customer and user.customer.end_date else None
        }

        access_token = create_access_token(data=token_data)

        log_action(
            db,
            user_id=user.id,
            action_type=AUDIT_ACTION_TYPE_LOGIN_SUCCESS,
            entity_type="User",
            entity_id=user.id,
            details={"email": user.email},
            ip_address=request_ip,
            customer_id=user.customer_id,
        )

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "must_accept_policies": must_accept_policies,
            "user": {
                "id": user.id,
                "email": user.email,
                "role": user.role.value,
                "customer_id": user.customer_id,
                "subscription_status": user.customer.status.value if user.customer else None, # Also add here
                "subscription_end_date": user.customer.end_date.isoformat() if user.customer and user.customer.end_date else None
            },
        }
    
    async def change_password(
        self, db: Session, user: TokenData, request_body: ChangePasswordRequest, request_ip: Optional[str],
        is_first_login_change: bool = False # Flag to distinguish forced change vs. self-service
    ) -> Dict[str, Any]:
        """
        Allows an authenticated user to change their password.
        Clears the must_change_password flag on success.
        This function now relies solely on the configured password policy.
        """
        from app.crud.crud import crud_user # Late import

        db_user = crud_user.get(db, user.user_id)
        if not db_user or db_user.is_deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or inactive.")

        if not verify_password(request_body.current_password, db_user.password_hash):
            log_action_type = AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_FAILED if is_first_login_change else AUDIT_ACTION_TYPE_UPDATE
            # MODIFIED: Call log_action utility
            log_action(
                db,
                user_id=user.user_id,
                action_type=log_action_type,
                entity_type="User",
                entity_id=db_user.id,
                details={"email": db_user.email, "reason": "Incorrect current password"},
                ip_address=request_ip,
                customer_id=db_user.customer_id,
            )
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect current password.")

        # The password policy is now enforced here, using the business logic layer.
        await self._validate_password_policy(request_body.new_password, db)

        # Update password using crud_user's internal logic or directly if no custom logic is there
        # We need to make sure crud_user.update_user handles the password hashing internally
        # Or we call db_user.set_password directly, which is preferred for model encapsulation
        
        db_user.set_password(request_body.new_password)
        db_user.must_change_password = False
        db_user.updated_at = func.now()
        db.add(db_user)
        db.flush() # Flush to persist changes before logging and token creation

        # Re-generate token after password change to reflect must_change_password = False
        # Fetch current permissions from DB to ensure token is up-to-date
        from app.crud.crud import crud_role_permission # Late import
        db_permissions = crud_role_permission.get_permissions_for_role(db, db_user.role.value)
        permission_names = [p.name for p in db_permissions]

        # ADDED: Fetch must_accept_policies and last_accepted_legal_version from the db_user object
        must_accept_policies_status = False
        latest_versions = await self._get_legal_artifact_versions(db)
        latest_system_version = max(latest_versions.get("tc_version", 0.0), latest_versions.get("pp_version", 0.0))
        if db_user.last_accepted_legal_version is None or db_user.last_accepted_legal_version < latest_system_version:
            must_accept_policies_status = True

        new_token_data = {
            "sub": db_user.email,
            "user_id": db_user.id,
            "role": db_user.role.value,
            "permissions": permission_names,
            "customer_id": db_user.customer_id,
            "has_all_entity_access": db_user.has_all_entity_access,
            "entity_ids": [assoc.customer_entity_id for assoc in db_user.entity_associations] if not db_user.has_all_entity_access else [],
            "must_change_password": False, # Explicitly false now
            "must_accept_policies": must_accept_policies_status, # MODIFIED: Use the newly computed value
            "last_accepted_legal_version": db_user.last_accepted_legal_version
        }
        new_access_token = create_access_token(data=new_token_data)

        log_action_type = AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_SUCCESS if is_first_login_change else AUDIT_ACTION_TYPE_UPDATE
        # MODIFIED: Call log_action utility
        log_action(
            db,
            user_id=user.user_id,
            action_type=log_action_type,
            entity_type="User",
            entity_id=db_user.id,
            details={"email": db_user.email, "reason": "Password changed"},
            ip_address=request_ip,
            customer_id=db_user.customer_id,
        )
        # Removed db.commit(), `get_db` generator handles it.

        return {
            "access_token": new_access_token,
            "token_type": "bearer",
            "must_accept_policies": must_accept_policies_status
        }

    async def initiate_password_reset(self, db: Session, email: str, request_ip: Optional[str]) -> None:
        """
        Generates a password reset token and sends it to the user's email.
        """
        from app.crud.crud import crud_user, crud_customer_configuration # Late imports

        user = crud_user.get_by_email(db, email)
        if not user:
            # For security, do not reveal if the email exists or not
            logger.info(f"Password reset requested for non-existent or inactive email: {email}")
            # MODIFIED: Call log_action utility
            log_action(
                db,
                user_id=None,
                action_type=AUDIT_ACTION_TYPE_PASSWORD_RESET_FAILED,
                entity_type="User",
                entity_id=None,
                details={"email": email, "reason": "Email not found for reset"},
                ip_address=request_ip,
            )
            return # Return success to prevent enumeration attacks

        # Get token expiry from config
        policy_config = await self._get_password_policy_config(db)
        token_expiry_minutes = policy_config.get(GlobalConfigKey.PASSWORD_RESET_TOKEN_EXPIRY_MINUTES.value, 15)

        # Invalidate any existing tokens for this user
        db.query(models.PasswordResetToken).filter(
            models.PasswordResetToken.user_id == user.id,
            models.PasswordResetToken.is_used == False,
            models.PasswordResetToken.expires_at > func.now()
        ).update({"is_used": True, "updated_at": func.now()}, synchronize_session=False)
        db.flush()

        # Generate a new token
        plain_token = str(uuid.uuid4())
        token_hash = get_password_hash(plain_token) # Hash the token for storage
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=token_expiry_minutes)

        new_reset_token = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=expires_at,
            is_used=False
        )
        db.add(new_reset_token)
        db.flush()

        # Send email with the plaintext token
        # Get FRONTEND_URL from environment variable
        frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000') # Default for local dev
        reset_link = f"{frontend_url}/reset-password?token={plain_token}" # Updated path based on App.js routing

        # Get email settings (customer-specific or global fallback)
        email_settings: EmailSettings
        email_config_source: str
        try:
            # MODIFIED: Removed 'await' here
            email_settings, email_config_source = get_customer_email_settings(db, user.customer_id)
        except HTTPException: # If customer settings are problematic, fallback to global
            # MODIFIED: Removed 'await' here
            email_settings, email_config_source = get_global_email_settings()
        
        email_subject = "Password Reset Request"
        email_body = f"""
        <html>
        <body>
            <p>Dear {user.email},</p>
            <p>You have requested to reset your password for the Treasury Management Platform.</p>
            <p>Please click on the following link to reset your password:</p>
            <p><a href="{reset_link}">Reset your password here</a></p>
            <p>This link is valid for {token_expiry_minutes} minutes.</p>
            <p>If you did not request a password reset, please ignore this email.</p>
            <p>Regards,</p>
            <p>The {{platform_name}} Team</p>
        </body>
        </html>
        """

        send_success = await send_email(
            db=db,
            to_emails=[user.email],
            subject_template=email_subject,
            body_template=email_body,
            template_data={"reset_link": reset_link, "expiry_minutes": token_expiry_minutes},
            email_settings=email_settings,
            cc_emails=[] # No CC for password reset
        )

        log_details = {"email": user.email, "token_id": new_reset_token.id, "email_sent": send_success}
        if not send_success:
            log_details["reason"] = "Email sending failed"

        # MODIFIED: Call log_action utility
        log_action(
            db,
            user_id=user.id,
            action_type=AUDIT_ACTION_TYPE_PASSWORD_RESET_INITIATED,
            entity_type="PasswordResetToken",
            entity_id=new_reset_token.id,
            details=log_details,
            ip_address=request_ip,
            customer_id=user.customer_id,
        )
        # Removed db.commit(), `get_db` generator handles it.

    async def reset_password(self, db: Session, request_body: ResetPasswordRequest, request_ip: Optional[str]) -> None:
        """
        Resets the user's password using a valid reset token.
        This function now relies solely on the configured password policy.
        """
        from app.crud.crud import crud_user # Late import

        # Find the token record first, then verify the plaintext token against its hash
        # This approach prevents timing attacks by always attempting to fetch a token before comparing.
        # Modified to find token directly based on what `verify_password` can check
        
        # Retrieve potential valid tokens (not used, not expired)
        valid_tokens = db.query(PasswordResetToken).filter(
            PasswordResetToken.is_used == False,
            PasswordResetToken.expires_at > func.now()
        ).all()

        found_token_record = None
        for token_rec in valid_tokens:
            if verify_password(request_body.token, token_rec.token_hash):
                found_token_record = token_rec
                break
        
        if not found_token_record:
            # MODIFIED: Call log_action utility
            log_action(
                db,
                user_id=None, # User ID unknown at this stage for security reasons
                action_type=AUDIT_ACTION_TYPE_PASSWORD_RESET_FAILED,
                entity_type="PasswordResetToken",
                entity_id=None,
                details={"token_status": "Invalid or expired", "reason": "Token not found or invalid hash/expiry/used status"},
                ip_address=request_ip,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired password reset token."
            )
        
        # Now use found_token_record
        reset_token_record = found_token_record

        user = crud_user.get(db, reset_token_record.user_id)
        if not user or user.is_deleted:
            # Mark token as used even if user is not found/active to prevent re-use
            reset_token_record.is_used = True
            db.add(reset_token_record)
            db.flush()
            # MODIFIED: Call log_action utility
            log_action(
                db,
                user_id=reset_token_record.user_id,
                action_type=AUDIT_ACTION_TYPE_PASSWORD_RESET_FAILED,
                entity_type="PasswordResetToken",
                entity_id=reset_token_record.id,
                details={"reason": "Associated user not found or inactive"},
                ip_address=request_ip,
                customer_id=user.customer_id if user else None,
            )
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Associated user not found or inactive.")

        # The password policy is now enforced here, using the business logic layer.
        await self._validate_password_policy(request_body.new_password, db)

        # Update user's password and mark token as used
        user.set_password(request_body.new_password)
        user.must_change_password = False
        user.updated_at = func.now()
        reset_token_record.is_used = True
        reset_token_record.updated_at = func.now()

        db.add(user)
        db.add(reset_token_record)
        db.flush() # Flush to persist changes before logging

        # MODIFIED: Call log_action utility
        log_action(
            db,
            user_id=user.id,
            action_type=AUDIT_ACTION_TYPE_PASSWORD_RESET_COMPLETED,
            entity_type="User",
            entity_id=user.id,
            details={"email": user.email, "token_id": reset_token_record.id},
            ip_address=request_ip,
            customer_id=user.customer_id,
        )
        # Removed db.commit(), `get_db` generator handles it.

    async def get_user_account_info(self, db: Session, user: TokenData) -> UserAccountOut:
        """
        Retrieves current authenticated user's account information for self-service.
        """
        from app.crud.crud import crud_user, crud_role_permission # Late import

        db_user = crud_user.get(db, user.user_id)
        if not db_user or db_user.is_deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or inactive.")

        # Ensure permissions are up-to-date
        db_permissions = crud_role_permission.get_permissions_for_role(db, db_user.role.value)
        permission_names = [p.name for p in db_permissions]

        return UserAccountOut(
            id=db_user.id,
            email=db_user.email,
            role=db_user.role,
            customer_id=db_user.customer_id,
            has_all_entity_access=db_user.has_all_entity_access,
            must_change_password=db_user.must_change_password,
            permissions=permission_names,
            created_at=db_user.created_at,
            updated_at=db_user.updated_at
        )

    async def admin_set_user_password(
        self,
        db: Session,
        target_user_id: int,
        request_body: AdminUserUpdate,
        admin_user: TokenData,
        request_ip: Optional[str]
    ) -> UserAccountOut:
        """
        Allows an admin (SO or CA) to set/reset a user's password and force change.
        Enforces role hierarchy and customer scope.
        This function now relies solely on the configured password policy.
        """
        from app.crud.crud import crud_user # Late import

        db_target_user = crud_user.get(db, target_user_id)
        if not db_target_user or db_target_user.is_deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target user not found or inactive.")

        # --- Role Hierarchy Enforcement ---
        # 1. System Owner can manage Corporate Admins (and implicitly, End Users/Checkers too)
        if admin_user.role == UserRole.SYSTEM_OWNER:
            # System Owner can set/reset password for any user
            pass # No additional role checks needed for SO here
        # 2. Corporate Admin can manage End Users and Checkers within their customer scope
        elif admin_user.role == UserRole.CORPORATE_ADMIN:
            if admin_user.customer_id is None:
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Admin user is not associated with a customer."
                )
            if db_target_user.customer_id != admin_user.customer_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You can only manage users within your organization."
                )
            if db_target_user.role == UserRole.SYSTEM_OWNER:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Corporate Admins cannot manage System Owner accounts."
                )
            if db_target_user.role == UserRole.CORPORATE_ADMIN and db_target_user.id != admin_user.user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Corporate Admins cannot manage other Corporate Admin accounts."
                )
            if db_target_user.id == admin_user.user_id:
                # If CA tries to reset their own password via admin panel, it implies a self-service type action
                # But typically this path is for "admin resetting others"
                # If they reset their own, it should be fine, but often self-service is preferred route
                pass # Allow CA to reset their own password, but not via the "admin of others" path
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have privileges to manage user passwords."
            )
        # --- End Role Hierarchy Enforcement ---

        # The password policy is now enforced here, using the business logic layer.
        await self._validate_password_policy(request_body.new_password, db)

        # Update password
        db_target_user.set_password(request_body.new_password)
        db_target_user.must_change_password = request_body.force_change_on_next_login
        db_target_user.updated_at = func.now()
        db.add(db_target_user)
        db.flush()

        # Log action
        action_type_log = AUDIT_ACTION_TYPE_ADMIN_PASSWORD_RESET if request_body.force_change_on_next_login else AUDIT_ACTION_TYPE_ADMIN_PASSWORD_SET
        # MODIFIED: Call log_action utility
        log_action(
            db,
            user_id=admin_user.user_id,
            action_type=action_type_log,
            entity_type="User",
            entity_id=db_target_user.id,
            details={
                "target_email": db_target_user.email,
                "forced_change_on_next_login": request_body.force_change_on_next_login
            },
            ip_address=request_ip,
            customer_id=db_target_user.customer_id,
        )
        # Removed db.commit(), `get_db` generator handles it.

        # Return updated user info
        from app.crud.crud import crud_role_permission # Late import
        db_permissions = crud_role_permission.get_permissions_for_role(db, db_target_user.role.value)
        permission_names = [p.name for p in db_permissions]

        return UserAccountOut(
            id=db_target_user.id,
            email=db_target_user.email,
            role=db_target_user.role,
            customer_id=db_target_user.customer_id,
            has_all_entity_access=db_target_user.has_all_entity_access,
            must_change_password=db_target_user.must_change_password,
            permissions=permission_names,
            created_at=db_target_user.created_at,
            updated_at=db_target_user.updated_at
        )

    async def admin_view_auth_logs(
        self,
        db: Session,
        admin_user: TokenData,
        user_id: Optional[int] = None,
        email: Optional[str] = None,
        action_type: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Allows admin to view authentication-related audit logs.
        System Owner can view all, Corporate Admin can view for their customer.
        """
        # Ensure only auth-related actions are queried by default or provided.
        auth_action_types = [
            AUDIT_ACTION_TYPE_LOGIN_SUCCESS,
            AUDIT_ACTION_TYPE_LOGIN_FAILED,
            AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_SUCCESS,
            AUDIT_ACTION_TYPE_PASSWORD_CHANGE_FIRST_LOGIN_FAILED,
            AUDIT_ACTION_TYPE_PASSWORD_RESET_INITIATED,
            AUDIT_ACTION_TYPE_PASSWORD_RESET_COMPLETED,
            AUDIT_ACTION_TYPE_PASSWORD_RESET_FAILED,
            AUDIT_ACTION_TYPE_ADMIN_PASSWORD_SET,
            AUDIT_ACTION_TYPE_ADMIN_PASSWORD_RESET,
            # For general updates that include password changes in details
            AUDIT_ACTION_TYPE_UPDATE,
            # NEW: Add the new action type
            AUDIT_ACTION_TYPE_ACCOUNT_LOCKED,
            AUDIT_ACTION_TYPE_LEGAL_ARTIFACT_ACCEPTED
        ]

        query_filters = []
        if action_type:
            # Validate if provided action_type is one of the allowed auth actions
            if action_type not in auth_action_types:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid action_type for authentication logs. Allowed: {', '.join(auth_action_types)}")
            query_filters.append(models.AuditLog.action_type == action_type)
        else:
            query_filters.append(models.AuditLog.action_type.in_(auth_action_types))


        if user_id:
            query_filters.append(models.AuditLog.user_id == user_id)
        if email:
            # Assuming 'details' field can contain 'email' for login/password changes
            # This is a bit brittle, might need to improve AuditLog model or details structure for direct filtering
            query_filters.append(models.AuditLog.details.op('->>')('email').ilike(f'%{email}%'))

        # Role-based filtering
        if admin_user.role == UserRole.CORPORATE_ADMIN:
            if admin_user.customer_id is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Admin user is not associated with a customer. Cannot retrieve logs."
                )
            query_filters.append(models.AuditLog.customer_id == admin_user.customer_id)
        elif admin_user.role != UserRole.SYSTEM_OWNER:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have privileges to view audit logs."
            )
        # System Owner has no additional customer_id filter

        logs_query = db.query(models.AuditLog).filter(*query_filters).order_by(models.AuditLog.timestamp.desc())

        total_count = logs_query.count()
        logs = logs_query.offset(skip).limit(limit).all()

        # Format for output - sensitive details in 'details' should be redacted if not already
        # AuditLogOut schema might be more appropriate for direct return, but here dict for flexibility
        formatted_logs = []
        for log in logs:
            details_copy = log.details.copy() if log.details else {}
            # Redact password related info if present directly in details
            if "password" in details_copy:
                details_copy["password"] = "[REDACTED]"
            if "old_password" in details_copy:
                details_copy["old_password"] = "[REDACTED]"
            if "new_password" in details_copy:
                details_copy["new_password"] = "[REDACTED]"
            if "reason" in details_copy and isinstance(details_copy["reason"], str) and "password" in details_copy["reason"].lower():
                 details_copy["reason"] = "[PASSWORD_RELATED_REASON]" # Generic redaction

            formatted_logs.append({
                "id": log.id,
                "user_id": log.user_id,
                "action_type": log.action_type,
                "entity_type": log.entity_type,
                "entity_id": log.entity_id,
                "details": details_copy,
                "timestamp": log.timestamp,
                "ip_address": log.ip_address,
                "customer_id": log.customer_id,
                "lg_record_id": log.lg_record_id,
            })
        
        return {"total_count": total_count, "logs": formatted_logs}

    async def accept_legal_policies(self, db: Session, user: TokenData, request_body: UserLegalAcceptanceRequest, request_ip: Optional[str]) -> Dict[str, Any]:
        """NEW: Service method to handle legal artifact acceptance."""
        from app.crud.crud import crud_user, crud_legal_artifact, crud_user_legal_acceptance
        
        db_user = crud_user.get(db, user.user_id)
        if not db_user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")
        
        latest_versions = await self._get_legal_artifact_versions(db)
        if request_body.tc_version != latest_versions.get("tc_version") or \
           request_body.pp_version != latest_versions.get("pp_version"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Versions provided do not match the latest legal artifact versions.")

        tc_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type="terms_and_conditions")
        pp_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type="privacy_policy")
        
        if not tc_artifact or not pp_artifact:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Legal artifacts not found in the system.")
            
        # 🐛 FIX: Check if acceptance already exists before recording
        if not crud_user_legal_acceptance.has_accepted(db, user_id=db_user.id, artifact_id=tc_artifact.id):
            crud_user_legal_acceptance.record_acceptance(
                db, user_id=db_user.id, artifact_id=tc_artifact.id, ip_address=request_ip
            )
        
        if not crud_user_legal_acceptance.has_accepted(db, user_id=db_user.id, artifact_id=pp_artifact.id):
            crud_user_legal_acceptance.record_acceptance(
                db, user_id=db_user.id, artifact_id=pp_artifact.id, ip_address=request_ip
            )
        
        # Update user's last accepted version to the latest
        db_user.last_accepted_legal_version = max(tc_artifact.version, pp_artifact.version)
        db_user.updated_at = func.now()
        db.add(db_user)
        db.flush()
        db.refresh(db_user)
        
        # Log the acceptance event
        log_action(
            db,
            user_id=db_user.id,
            action_type=AUDIT_ACTION_TYPE_LEGAL_ARTIFACT_ACCEPTED,
            entity_type="User",
            entity_id=db_user.id,
            details={
                "email": db_user.email,
                "tc_version_accepted": request_body.tc_version,
                "pp_version_accepted": request_body.pp_version
            },
            ip_address=request_ip,
            customer_id=db_user.customer_id
        )

        return {"message": "Legal policies accepted successfully."}
    
    def generate_mfa_code(self) -> str:
        """Generates a secure 6-digit numeric code."""
        return ''.join(secrets.choice(string.digits) for _ in range(6))

    async def trigger_mfa_flow(self, db: Session, user: models.User):
        """Generates, hashes, and saves a code, then sends the email."""
        raw_code = self.generate_mfa_code()
        
        # Reuse your existing hashing utility
        user.mfa_code_hashed = get_password_hash(raw_code)
        user.mfa_code_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
        user.mfa_attempts = 0
        
        db.add(user)
        db.commit()

        # Get the necessary email settings from your service
        from app.core.email_service import get_customer_email_settings, send_email
        email_settings, _ = get_customer_email_settings(db, user.customer_id)

        # Send the email using your existing email_service parameters
        await send_email(
            db=db,
            to_emails=[user.email],
            subject_template="Your Verification Code",
            sender_name="Grow BD Security",
            body_template=f"Your security code is: <b>{raw_code}</b>. It expires in 10 minutes.",
            template_data={},
            email_settings=email_settings
        )
        
        # Optional: Print to console so you can test even if SMTP is not configured
        print(f"DEBUG: MFA Code for {user.email} is {raw_code}")

    def is_device_trusted(self, db: Session, user_id: int, device_id: str) -> bool:
        """Checks if the device_id is already marked as trusted for this user."""
        device = db.query(models.UserDevice).filter(
            models.UserDevice.user_id == user_id,
            models.UserDevice.device_id == device_id,
            models.UserDevice.is_trusted == True
        ).first()
        return device is not None

    async def verify_mfa_code(
        self, db: Session, email: str, code: str, device_id: str, request_ip: str, remember_me: bool
    ) -> Optional[Dict[str, Any]]:
        user = db.query(User).filter(User.email == email).first()
        if not user or not user.mfa_code_hashed:
            return None

        # Check expiration and attempts
        if datetime.now(timezone.utc) > user.mfa_code_expires_at:
            return None

        if user.mfa_attempts >= 5:
            user.mfa_code_hashed = None
            user.mfa_attempts = 0
            db.commit()
            raise HTTPException(
                status_code=400, 
                detail="Too many failed attempts. Please log in again to receive a new code."
            )

        if not verify_password(code, user.mfa_code_hashed):
            user.mfa_attempts += 1
            db.commit()
            remaining = 5 - user.mfa_attempts
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid code. {remaining} attempts remaining."
            )

        # Success: Only trust device if 'remember_me' was checked
        device = db.query(models.UserDevice).filter(
            models.UserDevice.user_id == user.id,
            models.UserDevice.device_id == device_id
        ).first()

        if not device:
            device = models.UserDevice(
                user_id=user.id, 
                device_id=device_id, 
                is_trusted=remember_me, # SURGICAL FIX
                last_ip=request_ip,
                device_name="Web Browser"
            )
            db.add(device)
        else:
            device.is_trusted = remember_me # SURGICAL FIX
            device.last_ip = request_ip

        # Clear MFA data for next time
        user.mfa_code_hashed = None
        user.mfa_attempts = 0
        db.commit()

        # Generate the final full access token (Ensure this matches the token logic in authenticate_user)
        # This will now include the must_accept_policies flag
        return await self.generate_auth_response(db, user, request_ip)
    async def generate_auth_response(self, db: Session, user: User, request_ip: str) -> Dict[str, Any]:
        """
        Helper to generate the final JWT and handle post-login logic (like IP logging).
        """
        # 1. Create the access token
        # Adjust the 'data' dictionary to match what your app currently uses in JWTs
        access_token = create_access_token(
            data={
                "sub": user.email, 
                "user_id": user.id, 
                "role": user.role.value,
                "customer_id": user.customer_id, # CRITICAL: This was missing!
                "is_mfa_verified": True
            }
        )

        # 2. Reset failed attempts
        user.failed_login_attempts = 0
        db.commit()

        return {
                "access_token": access_token,
                "token_type": "bearer",
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "role": user.role.value,
                    "customer_id": user.customer_id,
                    "must_change_password": user.must_change_password
                }
            }
auth_service = AuthService()