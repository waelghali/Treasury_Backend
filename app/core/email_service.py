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
from email.header import Header
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
    display_name = os.getenv("GLOBAL_SENDER_DISPLAY_NAME", "Treasury Quotations")

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

    # Guard Clause: Check if settings exist, are active, and not soft-deleted
    if not (customer and customer.customer_email_settings and customer.customer_email_settings.is_active and not customer.customer_email_settings.is_deleted):
        logger.info(f"Customer {customer_id}: Custom email settings not found/inactive/deleted. Using Global.")
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


# --- Test Domain Filtering ---

TEST_DOMAINS = ("@acmecorp.com", "@example.com", "@email.com", "@test.com", ".test", "test.com")

def process_recipients(recipient_list: Optional[List[str]]) -> List[str]:
    """Filters out dummy/test domain email addresses."""
    if not recipient_list:
        return []
    clean_list = []
    for email_addr in recipient_list:
        if not email_addr or not isinstance(email_addr, str):
            continue
        lower_addr = email_addr.lower().strip()
        if any(lower_addr.endswith(domain) or f"@{domain}" in lower_addr for domain in ("acmecorp.com", "example.com", "email.com", "test.com", "cibegtest.com")) or lower_addr.endswith(".test") or "test@" in lower_addr:
            continue
        clean_list.append(email_addr)
    return clean_list


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
) -> Tuple[bool, Optional[str]]:
    """
    Sends an email using provided settings.
    Filters out dummy test domain addresses before sending.
    """
    to_emails = process_recipients(to_emails)
    cc_emails = process_recipients(cc_emails or [])

    if not to_emails and not cc_emails:
        logger.info("Email delivery suppressed: All recipients belong to dummy/test domains.")
        return True, None


    # Logic to handle display name override
    display_name = sender_name if sender_name else email_settings.sender_display_name
    if display_name:
        # This encodes Arabic names into a format like =?utf-8?b?...?= 
        # which SMTP servers accept as valid ASCII.
        encoded_name = Header(display_name, 'utf-8').encode()
        sender_header = f"{encoded_name} <{email_settings.sender_email}>"
    else:
        sender_header = email_settings.sender_email

    # 1. Build Message
    msg = MIMEMultipart('mixed')
    msg['From'] = sender_header
    msg['To'] = ", ".join(to_emails)
    msg['Subject'] = subject_template
    if cc_emails:
        msg['Cc'] = ", ".join(cc_emails)

    # Ensure all outgoing emails have high-aesthetic corporate SaaS styling
    body_to_send = body_template or ""
    body_lower = body_to_send.strip().lower()
    if "<!doctype html" not in body_lower and "<html" not in body_lower:
        from app.services.unified_email_builder import build_standard_email_html
        cust_name = sender_name or getattr(email_settings, 'sender_display_name', None) or "Grow Treasury"
        
        # Format plain-text / newlines if not already using HTML block elements
        if "<p" not in body_lower and "<div" not in body_lower and "<table" not in body_lower:
            paragraphs = [f"<p style='margin: 0 0 14px 0; line-height: 1.6; color: #334155;'>{p.strip().replace(chr(10), '<br/>')}</p>" for p in body_to_send.split("\n\n") if p.strip()]
            formatted_body = "".join(paragraphs) if paragraphs else f"<p style='margin: 0; line-height: 1.6; color: #334155;'>{body_to_send.replace(chr(10), '<br/>')}</p>"
        else:
            formatted_body = body_to_send

        body_to_send = build_standard_email_html(
            customer_name=cust_name,
            title=subject_template,
            content_html=formatted_body,
            platform_name="Grow BD Treasury Platform"
        )

    msg.attach(MIMEText(body_to_send, 'html'))

    # 2. Handle Attachments
    if attachments:
        for att in attachments:
            mime_type = att.mime_type or "application/octet-stream"
            if '/' in mime_type:
                main_type, sub_type = mime_type.split('/', 1)
            else:
                main_type, sub_type = "application", "octet-stream"
            part = MIMEBase(main_type, sub_type)
            part.set_payload(att.content)
            encoders.encode_base64(part)
            try:
                att.filename.encode('ascii')
                part.add_header('Content-Disposition', 'attachment', filename=att.filename)
            except (UnicodeEncodeError, AttributeError):
                part.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', att.filename or "attachment.pdf"))
            msg.attach(part)

    # 3. Consolidate Recipients
    all_recipients = list(set(to_emails + (cc_emails or [])))
    if not all_recipients:
        logger.info("Email delivery suppressed: No valid recipients found after filtering.")
        return True, None

    # 4. Send via SMTP in background worker thread with 10s socket timeout
    logger.debug(f"Connecting to SMTP: {email_settings.smtp_host}:{email_settings.smtp_port}")

    def _send_smtp():
        if email_settings.smtp_port == 465:
            server = smtplib.SMTP_SSL(email_settings.smtp_host, email_settings.smtp_port, timeout=10)
        else:
            server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port, timeout=10)
            server.starttls()

        server.login(email_settings.smtp_username, email_settings.smtp_password)
        server.send_message(msg, from_addr=email_settings.sender_email, to_addrs=all_recipients)
        server.quit()

    try:
        import asyncio
        await asyncio.to_thread(_send_smtp)
        logger.info(f"Email sent to {all_recipients} via {email_settings.smtp_host}")
        return True, None
    except Exception as primary_err:
        logger.warning(
            f"Custom SMTP ({email_settings.smtp_host}:{email_settings.smtp_port}) delivery failed: {primary_err}. "
            f"Initiating automatic fallback to generic global email service..."
        )

        # --- AUTOMATIC FALLBACK TO GENERIC / GLOBAL EMAIL ---
        try:
            global_settings = get_global_email_settings()
            if global_settings.smtp_host and (
                global_settings.smtp_host != email_settings.smtp_host or
                global_settings.smtp_username != email_settings.smtp_username
            ):
                def _send_global_fallback():
                    # Update From header for global delivery
                    from_name = sender_name or global_settings.sender_display_name or "Treasury Notifications"
                    encoded_name = Header(from_name, 'utf-8').encode()
                    from_header = f"{encoded_name} <{global_settings.sender_email}>"
                    if 'From' in msg:
                        del msg['From']
                    msg['From'] = from_header
                    if 'Sender' in msg:
                        del msg['Sender']
                    msg['Sender'] = global_settings.sender_email
                    if 'Reply-To' in msg:
                        del msg['Reply-To']
                    msg['Reply-To'] = global_settings.sender_email

                    if global_settings.smtp_port == 465:
                        fb_server = smtplib.SMTP_SSL(global_settings.smtp_host, global_settings.smtp_port, timeout=10)
                    else:
                        fb_server = smtplib.SMTP(global_settings.smtp_host, global_settings.smtp_port, timeout=10)
                        fb_server.starttls()

                    fb_server.login(global_settings.smtp_username, global_settings.smtp_password)
                    fb_server.send_message(msg, from_addr=global_settings.sender_email, to_addrs=all_recipients)
                    fb_server.quit()

                import asyncio
                await asyncio.to_thread(_send_global_fallback)
                logger.info(f"Email successfully delivered to {all_recipients} via generic global fallback ({global_settings.smtp_host})!")
                return True, None
        except Exception as fb_err:
            logger.error(f"Global email fallback also failed: {fb_err}")
            return False, f"Custom SMTP failed: {primary_err} | Generic fallback failed: {fb_err}"

        return False, f"SMTP Error ({email_settings.smtp_host}): {primary_err}"


# --- Account Lockout Protection ---

class AccountLockoutTracker:
    """
    In-memory rate limiter to protect corporate mail accounts (Active Directory / LDAP / Exchange)
    from being locked by Windows Domain lockout policies due to repeated failed authentication attempts.
    """
    _failures: Dict[str, List[Any]] = {}
    MAX_CONSECUTIVE_FAILURES = 3
    LOCKOUT_WINDOW_MINUTES = 15

    @classmethod
    def _clean_old_failures(cls, key: str):
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(minutes=cls.LOCKOUT_WINDOW_MINUTES)
        if key in cls._failures:
            cls._failures[key] = [t for t in cls._failures[key] if t > cutoff]
            if not cls._failures[key]:
                del cls._failures[key]

    @classmethod
    def is_locked(cls, account_key: str) -> Tuple[bool, Optional[int]]:
        """Returns (is_locked, remaining_minutes)"""
        from datetime import datetime
        key = (account_key or "").strip().lower()
        if not key:
            return False, None
        cls._clean_old_failures(key)
        failures = cls._failures.get(key, [])
        if len(failures) >= cls.MAX_CONSECUTIVE_FAILURES:
            oldest_relevant = failures[0]
            elapsed = (datetime.utcnow() - oldest_relevant).total_seconds()
            remaining_seconds = max(1, int((cls.LOCKOUT_WINDOW_MINUTES * 60) - elapsed))
            remaining_mins = max(1, (remaining_seconds + 59) // 60)
            return True, remaining_mins
        return False, None

    @classmethod
    def record_failure(cls, account_key: str):
        from datetime import datetime
        key = (account_key or "").strip().lower()
        if not key:
            return
        cls._clean_old_failures(key)
        if key not in cls._failures:
            cls._failures[key] = []
        cls._failures[key].append(datetime.utcnow())
        logger.warning(
            f"AccountLockoutTracker: Recorded auth failure for '{key}'. "
            f"Total recent failures: {len(cls._failures[key])}/{cls.MAX_CONSECUTIVE_FAILURES}"
        )

    @classmethod
    def record_success(cls, account_key: str):
        key = (account_key or "").strip().lower()
        if key and key in cls._failures:
            del cls._failures[key]
            logger.info(f"AccountLockoutTracker: Cleared auth failures for '{key}' on successful login.")


def verify_smtp_connection(
    smtp_host: str,
    smtp_port: int,
    smtp_username: str,
    smtp_password: str,
    customer_id: Optional[int] = None,
    timeout: int = 10
) -> Tuple[bool, str, Optional[str]]:
    """
    Tests connection and authentication to an SMTP server with Account Lockout Protection.
    Returns: (is_success, error_category, error_message)
    error_category: "SUCCESS" | "AUTH_ERROR" | "LOCKED" | "NETWORK_ERROR"
    """
    import socket
    if not smtp_host:
        return False, "NETWORK_ERROR", "SMTP Host is required."
    if not smtp_username or not smtp_password:
        return False, "AUTH_ERROR", "SMTP Username and Password are required."

    account_key = f"{customer_id or 'global'}:{smtp_username}"
    is_locked, remaining_mins = AccountLockoutTracker.is_locked(account_key)
    if is_locked:
        return False, "LOCKED", (
            f"Account Lockout Protection Active: {AccountLockoutTracker.MAX_CONSECUTIVE_FAILURES} consecutive login failures detected for '{smtp_username}'. "
            f"To protect your corporate account from being locked by your company's Active Directory security policy, testing is paused for approximately {remaining_mins} more minute(s)."
        )

    server = None
    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_host, int(smtp_port), timeout=timeout)
        else:
            server = smtplib.SMTP(smtp_host, int(smtp_port), timeout=timeout)
            server.ehlo()
            server.starttls()
            server.ehlo()
        server.login(smtp_username, smtp_password)
        server.quit()
        AccountLockoutTracker.record_success(account_key)
        return True, "SUCCESS", None
    except smtplib.SMTPAuthenticationError as e:
        AccountLockoutTracker.record_failure(account_key)
        msg = e.smtp_error.decode("utf-8", errors="replace") if isinstance(e.smtp_error, bytes) else str(e.smtp_error or e)
        locked_now, rem = AccountLockoutTracker.is_locked(account_key)
        lock_warn = f" [Account testing locked for {rem} min to protect corporate Active Directory]" if locked_now else ""
        return False, "AUTH_ERROR", f"SMTP Authentication failed for '{smtp_username}': {msg}{lock_warn}"
    except (smtplib.SMTPConnectError, socket.error, OSError) as e:
        return False, "NETWORK_ERROR", f"Could not connect to SMTP server {smtp_host}:{smtp_port} (Connection timed out or firewalled from cloud network): {e}"
    except Exception as e:
        err_str = str(e)
        if "auth" in err_str.lower() or "credentials" in err_str.lower():
            AccountLockoutTracker.record_failure(account_key)
            return False, "AUTH_ERROR", f"SMTP Authentication failed: {err_str}"
        return False, "NETWORK_ERROR", f"SMTP Connection error ({smtp_host}:{smtp_port}): {err_str}"
    finally:
        if server:
            try:
                server.close()
            except Exception:
                pass


def verify_imap_connection(
    imap_host: str,
    imap_port: int,
    imap_username: str,
    imap_password: str,
    use_ssl: bool = True,
    customer_id: Optional[int] = None,
    timeout: int = 10
) -> Tuple[bool, str, Optional[str]]:
    """
    Tests connection and authentication to an IMAP server with Account Lockout Protection.
    Returns: (is_success, error_category, error_message)
    error_category: "SUCCESS" | "AUTH_ERROR" | "LOCKED" | "NETWORK_ERROR"
    """
    import imaplib
    import socket
    if not imap_host:
        return False, "NETWORK_ERROR", "IMAP Host is required."
    if not imap_username or not imap_password:
        return False, "AUTH_ERROR", "IMAP Username and Password are required."

    account_key = f"{customer_id or 'global'}:{imap_username}"
    is_locked, remaining_mins = AccountLockoutTracker.is_locked(account_key)
    if is_locked:
        return False, "LOCKED", (
            f"Account Lockout Protection Active: consecutive login failures detected for '{imap_username}'. "
            f"Testing is paused for {remaining_mins} minute(s) to protect your corporate account."
        )

    port = int(imap_port) if imap_port else (993 if use_ssl else 143)
    mail = None
    try:
        if use_ssl:
            mail = imaplib.IMAP4_SSL(imap_host, port, timeout=timeout)
        else:
            mail = imaplib.IMAP4(imap_host, port, timeout=timeout)
        mail.login(imap_username, imap_password)
        mail.logout()
        AccountLockoutTracker.record_success(account_key)
        return True, "SUCCESS", None
    except imaplib.IMAP4.error as e:
        AccountLockoutTracker.record_failure(account_key)
        return False, "AUTH_ERROR", f"IMAP Authentication failed for '{imap_username}': {e}"
    except (socket.error, OSError) as e:
        return False, "NETWORK_ERROR", f"Could not connect to IMAP server {imap_host}:{port} (Connection timed out or firewalled from cloud network): {e}"
    except Exception as e:
        err_str = str(e)
        if "auth" in err_str.lower():
            AccountLockoutTracker.record_failure(account_key)
            return False, "AUTH_ERROR", f"IMAP Authentication failed: {err_str}"
        return False, "NETWORK_ERROR", f"IMAP connection error ({imap_host}:{port}): {err_str}"
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass
