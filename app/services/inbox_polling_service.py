# app/services/inbox_polling_service.py
"""
IMAP Polling Engine for Smart Inbox
Fetches unread/unprocessed emails from customer mailboxes, extracts metadata
and attachments, securely encrypts body text, and initiates classification.
"""

import imaplib
import email
from email.header import decode_header
import hashlib
import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.models.models import CustomerEmailSetting, Customer, SubscriptionPlan
from app.models.models_inbox import InboxItem, InboxAttachment
from app.core.encryption import encrypt_data, decrypt_data
from app.services.inbox_classification_service import inbox_classification_service

logger = logging.getLogger("app.inbox_polling")

ALLOWED_ATTACHMENT_EXTENSIONS = {".xlsx", ".xls", ".csv", ".pdf"}
STORAGE_BASE_DIR = os.path.join("uploads", "inbox")


class InboxPollingService:
    """
    Service responsible for connecting to customer IMAP accounts,
    downloading emails & attachments, and creating InboxItem records.
    """

    def _decode_mime_header(self, header_value: Optional[str]) -> str:
        """Safely decode RFC 2047 encoded email headers."""
        if not header_value:
            return ""
        decoded_fragments = []
        for fragment, encoding in decode_header(header_value):
            if isinstance(fragment, bytes):
                try:
                    decoded_fragments.append(fragment.decode(encoding or "utf-8", errors="replace"))
                except Exception:
                    decoded_fragments.append(fragment.decode("latin-1", errors="replace"))
            else:
                decoded_fragments.append(str(fragment))
        return "".join(decoded_fragments).strip()

    def _extract_email_address(self, raw_header: str) -> Tuple[str, Optional[str]]:
        """Extracts clean email address and domain from 'Name <email@domain.com>' format."""
        match = re.search(r'[\w\.-]+@[\w\.-]+', raw_header)
        if match:
            clean_email = match.group(0).lower().strip()
            domain = clean_email.split('@')[-1] if '@' in clean_email else None
            return clean_email, domain
        return raw_header.lower().strip(), None

    def _extract_clean_text_from_html(self, html_content: str) -> str:
        """Removes style, script, head blocks and HTML tags to extract clean human-readable text."""
        if not html_content:
            return ""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, "html.parser")
            for tag in soup(["style", "script", "head", "meta", "link"]):
                tag.decompose()
            text = soup.get_text(separator=" ")
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception:
            clean = re.sub(r'<(style|script|head)[^>]*>[\s\S]*?</\1>', ' ', html_content, flags=re.IGNORECASE)
            clean = re.sub(r'<[^>]+>', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean)
            return clean.strip()

    def _get_email_body(self, msg: email.message.Message) -> str:
        """Extracts plain text body from email message, cleanly handling HTML and multipart."""
        body = ""
        if msg.is_multipart():
            # First pass: look for text/plain
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))
                if content_type == "text/plain" and "attachment" not in content_disposition:
                    charset = part.get_content_charset() or "utf-8"
                    payload = part.get_payload(decode=True)
                    if payload:
                        try:
                            body = payload.decode(charset, errors="replace")
                        except Exception:
                            body = payload.decode("latin-1", errors="replace")
                    if body and not body.strip().startswith("<!DOCTYPE") and not "<html" in body.lower():
                        break
                    else:
                        body = self._extract_clean_text_from_html(body)
                        if body:
                            break

            # If still empty or no plain text found, extract from text/html
            if not body:
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        charset = part.get_content_charset() or "utf-8"
                        payload = part.get_payload(decode=True)
                        if payload:
                            try:
                                raw_html = payload.decode(charset, errors="replace")
                            except Exception:
                                raw_html = payload.decode("latin-1", errors="replace")
                            body = self._extract_clean_text_from_html(raw_html)
                            if body:
                                break
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                try:
                    raw_text = payload.decode(charset, errors="replace")
                except Exception:
                    raw_text = payload.decode("latin-1", errors="replace")
                
                if msg.get_content_type() == "text/html" or "<html" in raw_text.lower() or "<style" in raw_text.lower():
                    body = self._extract_clean_text_from_html(raw_text)
                else:
                    body = raw_text
        return body.strip()

    def _save_attachment(
        self,
        customer_id: int,
        message_id_clean: str,
        filename: str,
        content: bytes
    ) -> Tuple[str, str, int]:
        """Saves attachment to local storage directory and computes SHA-256 hash."""
        # Sanitize filename
        safe_filename = re.sub(r'[^\w\.-]', '_', filename)
        dest_dir = os.path.join(STORAGE_BASE_DIR, str(customer_id), message_id_clean)
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, safe_filename)

        with open(dest_path, "wb") as f:
            f.write(content)

        file_hash = hashlib.sha256(content).hexdigest()
        file_size = len(content)
        return dest_path, file_hash, file_size

    def _connect_imap(self, settings: CustomerEmailSetting) -> Optional[imaplib.IMAP4]:
        """Establishes authenticated IMAP connection."""
        if not settings.imap_host or not settings.imap_username or not settings.imap_password_encrypted:
            logger.warning(f"Customer {settings.customer_id} missing IMAP host/username/password.")
            return None

        try:
            password = decrypt_data(settings.imap_password_encrypted)
        except Exception as e:
            logger.error(f"Failed to decrypt IMAP password for customer {settings.customer_id}: {e}")
            return None

        port = settings.imap_port or (993 if settings.imap_use_ssl else 143)
        for attempt in range(2):
            mail = None
            try:
                if settings.imap_use_ssl:
                    mail = imaplib.IMAP4_SSL(settings.imap_host, port, timeout=15)
                else:
                    mail = imaplib.IMAP4(settings.imap_host, port, timeout=15)
                
                mail.login(settings.imap_username, password)
                return mail
            except Exception as e:
                if mail:
                    try:
                        mail.logout()
                    except Exception:
                        pass
                if attempt == 0:
                    time.sleep(1.0)
                    continue
                logger.error(f"IMAP login failed for customer {settings.customer_id} on {settings.imap_host}:{port}: {e}")
                return None

    def poll_customer_mailbox(self, db: Session, customer_id: int) -> List[InboxItem]:
        """
        Polls a single customer's mailbox for new unread messages.
        Returns list of newly created InboxItem records.
        """
        settings = db.query(CustomerEmailSetting).filter(
            CustomerEmailSetting.customer_id == customer_id,
            CustomerEmailSetting.imap_is_active == True,
            CustomerEmailSetting.is_deleted == False
        ).first()

        if not settings:
            return []

        mail = self._connect_imap(settings)
        if not mail:
            return []

        inbox_folder = settings.imap_inbox_folder or "INBOX"
        processed_folder = settings.imap_processed_folder or "Processed"
        new_items = []

        try:
            status, _ = mail.select(inbox_folder)
            if status != "OK":
                logger.error(f"Could not select folder '{inbox_folder}' for customer {customer_id}")
                return []

            # Search for messages (ALL messages, with DB-level Message-ID dedup to catch read/unread alike)
            search_status, message_nums = mail.search(None, "ALL")
            if search_status != "OK" or not message_nums[0]:
                return []

            msg_ids = message_nums[0].split()
            # Process up to 50 newest messages per cycle
            for msg_id in msg_ids[-50:]:
                try:
                    # 1. Ultra-fast header check: Fetch ONLY Message-ID header (takes ~1ms per email)
                    header_status, header_data = mail.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
                    if header_status == "OK" and header_data and header_data[0] and isinstance(header_data[0], tuple):
                        raw_hdr = header_data[0][1]
                        hdr_msg = email.message_from_bytes(raw_hdr)
                        msg_id_hdr = hdr_msg.get("Message-ID", "").strip("<>")
                        if msg_id_hdr:
                            existing = db.query(InboxItem.id).filter(
                                InboxItem.customer_id == customer_id,
                                InboxItem.message_id == msg_id_hdr,
                                InboxItem.is_deleted == False
                            ).first()
                            if existing:
                                # Already ingested - skip without downloading large body/attachments!
                                continue

                    # 2. Only fetch full RFC822 for genuinely NEW uningested messages
                    fetch_status, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if fetch_status != "OK" or not msg_data:
                        continue

                    raw_email = msg_data[0][1]
                    email_message = email.message_from_bytes(raw_email)

                    # Extract Message-ID
                    message_id_header = email_message.get("Message-ID", "")
                    if not message_id_header:
                        # Fallback unique hash
                        message_id_header = f"gen-{hashlib.md5(raw_email[:500]).hexdigest()}@grow"
                    else:
                        message_id_header = message_id_header.strip("<>")

                    # Dedup Check: Skip if message_id already exists for this customer
                    existing = db.query(InboxItem.id).filter(
                        InboxItem.customer_id == customer_id,
                        InboxItem.message_id == message_id_header,
                        InboxItem.is_deleted == False
                    ).first()
                    if existing:
                        continue

                    # Extract headers
                    sender_raw = self._decode_mime_header(email_message.get("From", ""))
                    sender_email, sender_domain = self._extract_email_address(sender_raw)
                    subject = self._decode_mime_header(email_message.get("Subject", ""))
                    in_reply_to = email_message.get("In-Reply-To", "").strip("<>") or None

                    # Parse Date
                    date_header = email_message.get("Date")
                    try:
                        received_at = email.utils.parsedate_to_datetime(date_header)
                        if received_at.tzinfo is None:
                            received_at = received_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        received_at = datetime.now(timezone.utc)

                    # Body
                    body_text = self._get_email_body(email_message)
                    body_encrypted = None
                    if body_text:
                        try:
                            body_encrypted = encrypt_data(body_text[:10000])  # store up to 10k chars encrypted
                        except Exception:
                            body_encrypted = None

                    # Process Attachments
                    clean_msg_id = re.sub(r'[^\w-]', '_', message_id_header)[:40]
                    attachments_data = []
                    primary_att_path = None
                    primary_att_name = None
                    primary_att_type = None
                    primary_att_hash = None

                    if email_message.is_multipart():
                        for part in email_message.walk():
                            if part.get_content_maintype() == "multipart":
                                continue
                            if part.get("Content-Disposition") is None:
                                continue

                            filename = part.get_filename()
                            if filename:
                                filename = self._decode_mime_header(filename)
                                ext = os.path.splitext(filename)[1].lower()
                                if ext in ALLOWED_ATTACHMENT_EXTENSIONS:
                                    payload = part.get_payload(decode=True)
                                    if payload:
                                        file_hash = hashlib.sha256(payload).hexdigest()
                                        file_size = len(payload)

                                        # Save file to disk under customer directory
                                        cust_dir = os.path.join(STORAGE_BASE_DIR, str(customer_id))
                                        os.makedirs(cust_dir, exist_ok=True)
                                        safe_name = f"{clean_msg_id}_{filename}"
                                        file_path = os.path.join(cust_dir, safe_name)

                                        with open(file_path, "wb") as f:
                                            f.write(payload)

                                        att_obj = {
                                            "filename": filename,
                                            "file_path": file_path,
                                            "file_type": ext.lstrip(".").upper(),
                                            "file_size": file_size,
                                            "file_hash": file_hash
                                        }
                                        attachments_data.append(att_obj)

                                        # Set first attachment as primary
                                        if not primary_att_path:
                                            primary_att_path = file_path
                                            primary_att_name = filename
                                            primary_att_type = ext.lstrip(".").upper()
                                            primary_att_hash = file_hash

                    # Create InboxItem record
                    item = InboxItem(
                        customer_id=customer_id,
                        message_id=message_id_header,
                        in_reply_to=in_reply_to,
                        sender_email=sender_email,
                        sender_domain=sender_domain,
                        subject=subject,
                        body_text_encrypted=body_encrypted,
                        has_attachment=len(attachments_data) > 0,
                        attachment_count=len(attachments_data),
                        primary_attachment_path=primary_att_path,
                        primary_attachment_name=primary_att_name,
                        primary_attachment_type=primary_att_type,
                        attachment_content_hash=primary_att_hash,
                        received_at=received_at,
                        status="RECEIVED"
                    )
                    db.add(item)
                    db.flush()

                    # Save Attachments to DB
                    for att_info in attachments_data:
                        att_rec = InboxAttachment(
                            inbox_item_id=item.id,
                            file_name=att_info["filename"],
                            file_type=att_info["file_type"],
                            file_size_bytes=att_info["file_size"],
                            storage_path=att_info["file_path"],
                            content_hash=att_info["file_hash"],
                            is_primary=(att_info["file_path"] == primary_att_path)
                        )
                        db.add(att_rec)

                    db.commit()
                    db.refresh(item)

                    # Trigger classification engine
                    classified_item = inbox_classification_service.classify(db, item)
                    new_items.append(classified_item)

                except (TimeoutError, OSError, imaplib.IMAP4.abort, imaplib.IMAP4.error) as sock_err:
                    logger.warning(f"IMAP connection interrupted for customer {customer_id} at msg {msg_id}: {sock_err}")
                    db.rollback()
                    break
                except Exception as inner_e:
                    logger.error(f"Error processing message {msg_id} for customer {customer_id}: {inner_e}", exc_info=True)
                    db.rollback()

            # Attempt to mark processed folder (optional / safe)
            try:
                # Create processed folder if not exists
                mail.create(processed_folder)
            except Exception:
                pass

        finally:
            try:
                mail.close()
                mail.logout()
            except Exception:
                pass

        return new_items

    def poll_all_active_customers(self, db: Session) -> Dict[int, int]:
        """
        Scans all customers with Smart Inbox enabled and active IMAP settings.
        Returns map of customer_id -> number of ingested emails.
        """
        # Find all customers with plan flag enabled
        active_settings = db.query(CustomerEmailSetting).join(Customer).join(SubscriptionPlan).filter(
            SubscriptionPlan.can_email_inbox == True,
            CustomerEmailSetting.imap_is_active == True,
            CustomerEmailSetting.is_deleted == False
        ).all()

        results = {}
        for setting in active_settings:
            try:
                ingested = self.poll_customer_mailbox(db, setting.customer_id)
                results[setting.customer_id] = len(ingested)
                if ingested:
                    logger.info(f"Ingested {len(ingested)} new emails for customer {setting.customer_id}")
            except Exception as e:
                logger.error(f"Failed polling customer {setting.customer_id}: {e}", exc_info=True)
                results[setting.customer_id] = 0

        return results


inbox_polling_service = InboxPollingService()
