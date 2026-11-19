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
STORAGE_BASE_DIR = os.path.join("private_storage", "inbox")
IMAP_SOCKET_TIMEOUT = 30
MAX_NEW_EMAILS_PER_CYCLE = 15


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

    @staticmethod
    def _normalize_message_id(raw_val: Optional[str]) -> Tuple[str, List[str]]:
        """Normalizes a Message-ID header and returns (canonical_id, lookup_variants)."""
        raw = raw_val or ""
        norm = raw.strip().strip("<>").strip()
        legacy = raw.strip("<>")
        variants = list(dict.fromkeys(v for v in (norm, legacy) if v))
        return norm, variants

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
        account_key = f"{settings.customer_id}:{settings.imap_username}"

        from app.core.email_service import AccountLockoutTracker
        is_locked, remaining_mins = AccountLockoutTracker.is_locked(account_key)
        if is_locked:
            logger.warning(
                f"Customer {settings.customer_id}: IMAP polling paused for {remaining_mins} min "
                f"due to Account Lockout Protection for '{settings.imap_username}'."
            )
            return None

        for attempt in range(2):
            mail = None
            try:
                if settings.imap_use_ssl:
                    mail = imaplib.IMAP4_SSL(settings.imap_host, port, timeout=IMAP_SOCKET_TIMEOUT)
                else:
                    mail = imaplib.IMAP4(settings.imap_host, port, timeout=IMAP_SOCKET_TIMEOUT)
                
                mail.login(settings.imap_username, password)
                AccountLockoutTracker.record_success(account_key)
                return mail
            except imaplib.IMAP4.error as auth_err:
                # Do NOT retry authentication failures - protect Active Directory from lockout!
                AccountLockoutTracker.record_failure(account_key)
                logger.error(f"IMAP auth failed for customer {settings.customer_id} on {settings.imap_host}: {auth_err}. Aborting retry to prevent account lockout.")
                if mail:
                    try:
                        mail.logout()
                    except Exception:
                        pass
                return None
            except Exception as e:
                if mail:
                    try:
                        mail.logout()
                    except Exception:
                        pass
                if attempt == 0:
                    time.sleep(1.0)
                    continue
                logger.error(f"IMAP connection failed for customer {settings.customer_id} on {settings.imap_host}:{port}: {e}")
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
            recent_msg_ids = msg_ids[-50:]
            if not recent_msg_ids:
                return []

            # 1. Batch-fetch Message-ID headers (1 IMAP roundtrip + 1 DB query instead of 50+50)
            seq_to_variants: Dict[bytes, List[str]] = {}
            all_variants: List[str] = []
            try:
                batch_seq = b",".join(m if isinstance(m, bytes) else str(m).encode() for m in recent_msg_ids)
                header_status, header_data = mail.fetch(batch_seq, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
                if header_status == "OK" and header_data:
                    for entry in header_data:
                        if isinstance(entry, tuple) and len(entry) >= 2:
                            meta_bytes, raw_hdr = entry[0], entry[1]
                            if not meta_bytes or not raw_hdr:
                                continue
                            meta_parts = (meta_bytes if isinstance(meta_bytes, bytes) else str(meta_bytes).encode()).split()
                            if not meta_parts:
                                continue
                            seq_id = meta_parts[1] if meta_parts[0] == b"*" and len(meta_parts) >= 2 else meta_parts[0]
                            raw_hdr_bytes = raw_hdr if isinstance(raw_hdr, bytes) else str(raw_hdr).encode("utf-8", errors="replace")
                            hdr_msg = email.message_from_bytes(raw_hdr_bytes)
                            _, variants = self._normalize_message_id(hdr_msg.get("Message-ID"))
                            if variants:
                                seq_to_variants[seq_id] = variants
                                all_variants.extend(variants)
            except (TimeoutError, OSError, imaplib.IMAP4.abort, imaplib.IMAP4.error) as batch_err:
                logger.warning(f"IMAP connection interrupted during batch header fetch for customer {customer_id}: {batch_err}")
                return []

            existing_mids: set = set()
            if all_variants:
                unique_variants = list(dict.fromkeys(all_variants))
                rows = db.query(InboxItem.message_id).filter(
                    InboxItem.customer_id == customer_id,
                    InboxItem.message_id.in_(unique_variants),
                    InboxItem.is_deleted == False
                ).all()
                existing_mids = {row[0] for row in rows if row and row[0]}

            unseen_msg_ids = []
            for mid in recent_msg_ids:
                mid_bytes = mid if isinstance(mid, bytes) else str(mid).encode()
                variants = seq_to_variants.get(mid_bytes, [])
                if variants and any(v in existing_mids for v in variants):
                    continue
                unseen_msg_ids.append(mid)

            if not unseen_msg_ids:
                return []

            target_unseen = unseen_msg_ids[:MAX_NEW_EMAILS_PER_CYCLE]

            for msg_id in target_unseen:
                try:
                    # 2. Fetch full RFC822 for genuinely NEW uningested messages, with single reconnect on transient EOF/timeout
                    try:
                        fetch_status, msg_data = mail.fetch(msg_id, "(RFC822)")
                    except (TimeoutError, OSError, imaplib.IMAP4.abort) as fetch_err:
                        logger.info(
                            f"IMAP connection interrupted while fetching msg {msg_id} for customer {customer_id} "
                            f"({fetch_err}); attempting single reconnect."
                        )
                        try:
                            if mail:
                                mail.logout()
                        except Exception:
                            pass
                        mail = self._connect_imap(settings)
                        if not mail:
                            raise fetch_err
                        sel_status, _ = mail.select(inbox_folder)
                        if sel_status != "OK":
                            raise imaplib.IMAP4.abort(f"Could not re-select folder '{inbox_folder}'")
                        fetch_status, msg_data = mail.fetch(msg_id, "(RFC822)")

                    if fetch_status != "OK" or not msg_data or not msg_data[0] or not isinstance(msg_data[0], tuple):
                        continue

                    raw_email = msg_data[0][1]
                    email_message = email.message_from_bytes(raw_email)

                    # Extract and normalize Message-ID
                    norm_msg_id, lookup_variants = self._normalize_message_id(email_message.get("Message-ID"))
                    if not norm_msg_id:
                        # Fallback unique hash
                        message_id_header = f"gen-{hashlib.md5(raw_email[:500]).hexdigest()}@grow"
                        lookup_variants = [message_id_header]
                    else:
                        message_id_header = norm_msg_id

                    # Dedup Check: Skip if message_id already exists for this customer
                    existing = db.query(InboxItem.id).filter(
                        InboxItem.customer_id == customer_id,
                        InboxItem.message_id.in_(lookup_variants),
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

                                        # Upload attachment directly to GCS
                                        from app.core.storage_service import build_customer_blob_path, upload_bytes_to_gcs_sync
                                        blob_path = build_customer_blob_path(customer_id, "inbox", f"msg_{clean_msg_id}/{filename}")
                                        mime_type = "application/pdf" if ext == ".pdf" else "application/octet-stream"
                                        try:
                                            file_path = upload_bytes_to_gcs_sync(
                                                blob_path,
                                                payload,
                                                content_type=mime_type,
                                                timeout=25,
                                            )
                                        except Exception as upload_err:
                                            logger.error(f"Failed to upload inbox attachment {filename} to GCS: {upload_err}")
                                            file_path = f"gs://lg_custody_bucket/{blob_path}"

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
            if mail:
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
