# app/core/email_service.py

import os
import smtplib
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from sqlalchemy.orm import Session, selectinload

# App Imports
from app.core.encryption import decrypt_data
from app.models import Customer

logger = logging.getLogger(__name__)

# --- Data Structures ---

@dataclass
class EmailSettings:
    """Holds SMTP configuration details."""
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    sender_email: str
    sender_display_name: Optional[str] = None

@dataclass
class EmailAttachment:
    """Holds attachment data."""
    filename: str
    content: bytes
    mime_type: str


# --- Configuration Retrievers ---

def get_global_email_settings() -> EmailSettings:
    """Retrieves global email settings from environment variables."""
    sender_email = os.getenv("EMAIL_SENDER_EMAIL")
    smtp_host = os.getenv("EMAIL_SMTP_HOST")
    smtp_username = os.getenv("EMAIL_SMTP_USERNAME")
    smtp_password = os.getenv("EMAIL_SMTP_PASSWORD")
    
    # Default values
    smtp_port = int(os.getenv("EMAIL_SMTP_PORT", 587))
    display_name = os.getenv("GLOBAL_SENDER_DISPLAY_NAME", "Treasury Platform Notifications")

    if not all([sender_email, smtp_host, smtp_username, smtp_password]):
        logger.warning("Missing global email env vars. Using dummy fallback.")
        return EmailSettings(
            smtp_host="", smtp_port=587, smtp_username="", smtp_password="",
            sender_email="no-reply@example.com", sender_display_name="System Notifications"
        )

    return EmailSettings(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_username=smtp_username,
        smtp_password=smtp_password,
        sender_email=sender_email,
        sender_display_name=display_name
    )

def get_customer_email_settings(db: Session, customer_id: int) -> Tuple[EmailSettings, str]:
    """
    Retrieves customer-specific email settings. 
    Returns: (EmailSettings, source_description_string)
    """
    customer = db.query(Customer).options(
        selectinload(Customer.customer_email_settings)
    ).filter(Customer.id == customer_id).first()

    # Guard Clause: Check if settings exist and are active
    if not (customer and customer.customer_email_settings and customer.customer_email_settings.is_active):
        logger.info(f"Customer {customer_id}: Custom email settings not found/inactive. Using Global.")
        return get_global_email_settings(), "global"

    settings = customer.customer_email_settings

    # Guard Clause: Check for incomplete data
    if not all([settings.smtp_host, settings.smtp_username, settings.smtp_password_encrypted, settings.sender_email]):
        logger.warning(f"Customer {customer_id}: Custom settings incomplete. Fallback to Global.")
        return get_global_email_settings(), "global_fallback_incomplete"

    try:
        decrypted_password = decrypt_data(settings.smtp_password_encrypted)
        logger.info(f"Customer {customer_id}: Using custom email settings.")
        
        return EmailSettings(
            smtp_host=settings.smtp_host,
            smtp_port=settings.smtp_port,
            smtp_username=settings.smtp_username,
            smtp_password=decrypted_password,
            sender_email=settings.sender_email,
            sender_display_name=settings.sender_display_name,
        ), "customer_specific"

    except Exception as e:
        logger.error(f"Customer {customer_id}: decryption failed ({e}). Fallback to Global.", exc_info=True)
        return get_global_email_settings(), "global_fallback_error"


# --- Core Sending Logic ---

async def send_email(
    db: Session,
    to_emails: List[str],
    subject_template: str,
    body_template: str,
    template_data: Dict[str, Any],
    email_settings: EmailSettings,
    cc_emails: Optional[List[str]] = None,
    sender_name: Optional[str] = None,
    attachments: Optional[List[EmailAttachment]] = None
) -> bool:
    """
    Sends an email using provided settings.
    Note: `template_data` is kept for API compatibility but the caller 
    is expected to have already formatted the subject/body strings.
    """
    if not to_emails:
        logger.warning("Email attempt aborted: No recipients provided.")
        return False

    # Logic to handle display name override
    display_name = sender_name if sender_name else email_settings.sender_display_name
    sender_header = f"{display_name} <{email_settings.sender_email}>" if display_name else email_settings.sender_email

    try:
        # 1. Build Message
        msg = MIMEMultipart('mixed')
        msg['From'] = sender_header
        msg['To'] = ", ".join(to_emails)
        msg['Subject'] = subject_template
        if cc_emails:
            msg['Cc'] = ", ".join(cc_emails)

        msg.attach(MIMEText(body_template, 'html'))

        # 2. Handle Attachments
        if attachments:
            for att in attachments:
                main_type, sub_type = att.mime_type.split('/', 1)
                part = MIMEBase(main_type, sub_type)
                part.set_payload(att.content)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{att.filename}"')
                msg.attach(part)

        # 3. Consolidate Recipients
        all_recipients = list(set(to_emails + (cc_emails or [])))

        # 4. Send via SMTP
        logger.debug(f"Connecting to SMTP: {email_settings.smtp_host}:{email_settings.smtp_port}")
        
        # Select Connection Type
        if email_settings.smtp_port == 465:
            server = smtplib.SMTP_SSL(email_settings.smtp_host, email_settings.smtp_port)
        else:
            server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port)
            server.starttls()

        server.login(email_settings.smtp_username, email_settings.smtp_password)
        server.send_message(msg, from_addr=email_settings.sender_email, to_addrs=all_recipients)
        server.quit()

        logger.info(f"Email sent to {to_emails} via {email_settings.smtp_host}")
        return True

    except smtplib.SMTPConnectError as e:
        logger.error(f"SMTP Connect Error ({email_settings.smtp_host}): {e}")
        return False
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Auth Error ({email_settings.smtp_username}): Check credentials.")
        return False
    except Exception as e:
        logger.error(f"Email send failed: {e}", exc_info=True)
        return False