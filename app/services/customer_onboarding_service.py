# app/services/customer_onboarding_service.py
"""
Customer Onboarding Service — Zero-Touch Activation & Key Envelope Initialization.
Provides:
1. Secure single-use 24-hour activation token generation (hashed in DB).
2. Confidential 'SendOnly' email delivery (save_copy=False, never saved to Sent Items).
3. Resend invitation pipeline for expired or missing activation links.
4. Eager Tenant DEK envelope provisioning.
"""

import os
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.models import User, PasswordResetToken
from app.constants import UserRole
from app.core.hashing import get_password_hash
from app.core.email_service import (
    EmailAttachment,
    send_email,
    get_customer_email_settings,
    get_global_email_settings,
)
from app.services.unified_email_builder import build_security_email_html
from app.core.routing import get_frontend_base_url
from app.crud.crud import log_action

logger = logging.getLogger(__name__)


def _load_quick_start_guide_attachment() -> Optional[EmailAttachment]:
    """Loads the illustrated Full-Suite Quick Start Guide PDF for onboarding emails."""
    app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    pdf_path = os.path.join(app_root, "templates", "quick_start_guide.pdf")
    try:
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        if pdf_bytes:
            return EmailAttachment("Grow_Treasury_Quick_Start_Guide.pdf", pdf_bytes, "application/pdf")
    except FileNotFoundError:
        logger.warning(f"Quick start guide PDF not found at: {pdf_path}; sending activation email without attachment.")
    except Exception as exc:
        logger.warning(f"Could not attach quick start guide PDF ({pdf_path}): {exc}")
    return None


async def send_corporate_admin_activation_email(
    db: Session,
    user: User,
    customer_name: str,
    request: Optional[object] = None,
    custom_role_label: Optional[str] = None,
    is_trial: bool = False,
    recipient_name: Optional[str] = None,
    module_labels: Optional[list] = None,
    attach_guide: bool = True,
) -> Tuple[bool, Optional[str]]:
    """
    Issues a single-use 24-hour activation token for a Corporate Admin or Customer User
    and dispatches a private 'SendOnly' email (save_copy=False) with the activation link.
    Guarantees that administrators (System Owner and Corporate Admins) have zero knowledge of the link or credentials.
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
        valid_hours = 48 if is_trial else 24
        expires_at = datetime.now(timezone.utc) + timedelta(hours=valid_hours)

        token_rec = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=expires_at,
            is_used=False
        )
        db.add(token_rec)
        db.commit()

        # 3. Format activation link with mode=activation
        frontend_url = get_frontend_base_url(request=request)
        activation_link = f"{frontend_url}/reset-password?token={plain_token}&mode=activation"

        # 4. Resolve email settings (customer-specific or global fallback)
        try:
            email_settings, _ = get_customer_email_settings(db, user.customer_id)
        except Exception:
            email_settings = get_global_email_settings()

        # 5. Build high-aesthetic security email
        role_label = custom_role_label
        if not role_label:
            if user.role == UserRole.CORPORATE_ADMIN:
                role_label = "Corporate Administrator"
            elif user.role == UserRole.CHECKER:
                role_label = "Checker / Approver"
            elif user.role == UserRole.VIEWER:
                role_label = "Viewer"
            elif user.role == UserRole.END_USER:
                role_label = "User / Maker"
            else:
                role_label = str(user.role.value).replace('_', ' ').title()

        if is_trial:
            greeting_line = f"Hello <strong>{recipient_name}</strong>,<br><br>" if recipient_name else ""
            modules_html = ""
            if module_labels:
                items = "".join(f"<li style='margin-bottom:4px;'>{m}</li>" for m in module_labels)
                modules_html = (
                    f"<div style='margin: 14px 0; padding: 12px 16px; background-color: #f8fafc; "
                    f"border-left: 3px solid #0284c7; border-radius: 6px;'>"
                    f"<strong style='font-size:12px; color:#0f172a; text-transform:uppercase; letter-spacing:0.5px;'>"
                    f"Unlocked Modules in Your Workspace:</strong>"
                    f"<ul style='margin:8px 0 0 0; padding-left:18px; color:#334155; font-size:13px;'>{items}</ul>"
                    f"</div>"
                )
            subject = f"Welcome to Grow Treasury — Activate Your 14-Day Free Trial ({customer_name})"
            body = build_security_email_html(
                title="Your 14-Day Free Trial is Ready",
                user_email=user.email,
                message=(
                    f"{greeting_line}"
                    f"Your private Free Trial account for <strong>{customer_name}</strong> is ready on "
                    f"<strong>Grow Corporate Treasury Platform</strong>."
                    f"{modules_html}"
                    f"We already loaded sample Letters of Guarantee, 3 bank credit lines, and sample test files "
                    f"so you can start testing in 2 minutes — or upload your own files safely (encrypted for your company only).<br><br>"
                    f"<strong>Full-Suite Quick Start Guide:</strong> Attached as a PDF covering all 4 modules (LG Custody, Outbound Issuance, FX &amp; T-Bill Quotations, and Bank Auto-Reconciliation).<br><br>"
                    f"<strong>Note:</strong> Your 14-day trial timer starts only after you set your password and sign in.<br><br>"
                    f"<em>This private activation link is valid for {valid_hours} hours.</em>"
                ),
                action_url=activation_link,
                action_text="Set Password & Start Free Trial",
                platform_name="Grow Treasury Platform",
            )
        else:
            subject = f"Welcome to Grow Treasury — Activate Your Account ({customer_name})"
            body = build_security_email_html(
                title="Welcome to Grow Treasury",
                user_email=user.email,
                message=(
                    f"Your account has been configured under organization <strong>{customer_name}</strong> in Grow Treasury "
                    f"with the role of <strong>{role_label}</strong>.<br><br>"
                    f"Please click below to activate your account and establish your private, confidential password. "
                    f"For institutional privacy, this activation link is strictly confidential and neither platform administrators "
                    f"nor your organization's administrators have access to your credentials.<br><br>"
                    f"<strong>Full-Suite Quick Start Guide:</strong> Attached as a PDF covering Day-1 setup and all 4 treasury modules (LG Custody, Outbound Issuance, FX &amp; T-Bill Quotations, and Bank Auto-Reconciliation).<br><br>"
                    f"<em>This link is valid for {valid_hours} hours.</em>"
                ),
                action_url=activation_link,
                action_text="Activate Account & Set Password",
                platform_name="Grow Treasury Onboarding"
            )

        guide_attachment = _load_quick_start_guide_attachment() if attach_guide else None

        # 6. Send email with save_copy=False (SendOnly - Never saved to Sent Items!)
        success, err = await send_email(
            db=db,
            to_emails=[user.email],
            subject_template=subject,
            body_template=body,
            template_data={"activation_link": activation_link},
            email_settings=email_settings,
            attachments=[guide_attachment] if guide_attachment else None,
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
                "guide_attached": bool(guide_attachment),
                "save_copy": False
            },
            customer_id=user.customer_id
        )

        return success, err

    except Exception as e:
        logger.error(f"Error dispatching activation email to {user.email}: {e}", exc_info=True)
        return False, str(e)


# Alias for generalized user activation
send_user_activation_email = send_corporate_admin_activation_email
