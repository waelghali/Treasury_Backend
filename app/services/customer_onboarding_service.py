# app/services/customer_onboarding_service.py
"""
Customer Onboarding Service — Zero-Touch Activation & Key Envelope Initialization.
Provides:
1. Secure single-use 24-hour activation token generation (hashed in DB).
2. Confidential 'SendOnly' email delivery (save_copy=False, never saved to Sent Items).
3. Resend invitation pipeline for expired or missing activation links.
4. Eager Tenant DEK envelope provisioning.
"""

import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.models import User, PasswordResetToken
from app.core.hashing import get_password_hash
from app.core.email_service import send_email, get_customer_email_settings, get_global_email_settings
from app.services.unified_email_builder import build_security_email_html
from app.core.routing import get_frontend_base_url
from app.crud.crud import log_action

logger = logging.getLogger(__name__)


async def send_corporate_admin_activation_email(
    db: Session,
    user: User,
    customer_name: str,
    request: Optional[object] = None
) -> Tuple[bool, Optional[str]]:
    """
    Issues a single-use 24-hour activation token for a Corporate Admin
    and dispatches a private 'SendOnly' email (save_copy=False) with the activation link.
    Guarantees that the System Owner has zero knowledge of the link or credentials.
    """
    try:
        # 1. Invalidate any existing active tokens for this user
        db.query(PasswordResetToken).filter(
            PasswordResetToken.user_id == user.id,
            PasswordResetToken.is_used == False,
            PasswordResetToken.expires_at > func.now()
        ).update({"is_used": True, "updated_at": func.now()}, synchronize_session=False)
        db.flush()

        # 2. Generate secure single-use token (only token_hash is stored in DB)
        plain_token = str(uuid.uuid4())
        token_hash = get_password_hash(plain_token)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

        token_rec = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=expires_at,
            is_used=False
        )
        db.add(token_rec)
        db.flush()

        # 3. Format activation link with mode=activation
        frontend_url = get_frontend_base_url(request=request)
        activation_link = f"{frontend_url}/reset-password?token={plain_token}&mode=activation"

        # 4. Resolve email settings (customer-specific or global fallback)
        try:
            email_settings, _ = get_customer_email_settings(db, user.customer_id)
        except Exception:
            email_settings, _ = get_global_email_settings()

        # 5. Build high-aesthetic security email
        subject = f"Welcome to Grow Treasury — Activate Your Account ({customer_name})"
        body = build_security_email_html(
            title="Welcome to Grow Treasury",
            user_email=user.email,
            message=(
                f"Your organization <strong>{customer_name}</strong> has been onboarded to Grow Treasury. "
                f"You have been designated as the primary <strong>Corporate Administrator</strong>.<br><br>"
                f"Please click below to activate your account and establish your private, confidential password. "
                f"For institutional privacy, this activation link is strictly confidential and neither the System Owner "
                f"nor Grow platform administrators have access to your credentials.<br><br>"
                f"<em>This link is valid for 24 hours.</em>"
            ),
            action_url=activation_link,
            action_text="Activate Account & Set Password",
            platform_name="Grow Treasury Onboarding"
        )

        # 6. Send email with save_copy=False (SendOnly - Never saved to Sent Items!)
        success, err = await send_email(
            db=db,
            to_emails=[user.email],
            subject_template=subject,
            body_template=body,
            template_data={"activation_link": activation_link},
            email_settings=email_settings,
            save_copy=False  # SendOnly: Link never recorded in email Sent folder
        )

        log_action(
            db,
            user_id=user.id,
            action_type="USER_ACTIVATION_LINK_DISPATCHED",
            entity_type="User",
            entity_id=user.id,
            details={
                "email": user.email,
                "customer_name": customer_name,
                "email_sent": success,
                "save_copy": False
            },
            customer_id=user.customer_id
        )

        return success, err

    except Exception as e:
        logger.error(f"Error dispatching activation email to {user.email}: {e}", exc_info=True)
        return False, str(e)
