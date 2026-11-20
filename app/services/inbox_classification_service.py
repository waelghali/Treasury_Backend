# app/services/inbox_classification_service.py
"""
Multi-Signal Classification Engine for Smart Inbox
Scores emails based on thread tracking, domain mapping, attachment structure,
historical user corrections, and subject keywords.
"""

import io
import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, desc

from app.models.models import Bank
from app.models.models_inbox import (
    InboxItem, InboxOutboundRequest, EmailClassificationHistory
)
from app.services.reconciliation_service import COLUMN_KEYWORDS as LG_COLUMN_KEYWORDS
from app.services.banking_standards_ontology import banking_standards_ontology
from app.services.pdf_statement_parser import (
    get_candidate_pdf_passwords,
    unlock_pdf_bytes,
    extract_cash_balances_from_text,
    extract_statement_from_pdf,
)
from app.core.storage_service import read_attachment_bytes
from app.core.encryption import decrypt_data

logger = logging.getLogger("app.inbox_classification")

GENERIC_DOMAINS = {
    "gmail.com", "outlook.com", "hotmail.com", "yahoo.com",
    "icloud.com", "aol.com", "protonmail.com", "mail.com"
}

STATEMENT_KEYWORDS = [
    "statement", "bank statement", "account statement", "transactions",
    "debit", "credit", "balance", "كشف حساب", "معاملات", "رصيد"
]

CASH_POSITION_KEYWORDS = [
    "cash position", "daily balance", "available balance", "ledger balance",
    "account balance", "balance summary", "balances as of",
    "موقف السيولة", "الرصيد المتاح", "الرصيد الدفتري", "بيان الأرصدة", "أرصدة الحسابات"
]

LG_POSITION_KEYWORDS = [
    "lg position", "position report", "guarantee position", "active lgs",
    "letter of guarantee", "letters of guarantee", "بيان الضمانات", "خطابات الضمان",
    "موقف الضمانات", "تقرير الضمانات"
]

PROGRESS_KEYWORDS = [
    "progress", "milestone", "delivery complete", "handover", "completion",
    "تسليم", "انتهاء المرحلة", "تقرير إنجاز", "محضر استلام"
]

IRRELEVANT_KEYWORDS = [
    "survey", "feedback", "newsletter", "subscription", "shipped", "tracking",
    "delivery", "amazon", "playstation", "order confirmation", "purchase",
    "hotel", "booking", "receipt", "promo", "promotions", "marketing", "unsubscribe",
    "marriott", "guestsurveys", "airbnb", "uber", "careem", "talabat", "terms",
    "استبيان", "طلب شراء", "تأكيد الشحن", "تتبع الشحنة", "تأكيد الحجز", "عرض خاص"
]

NIL_POSITION_PATTERNS = [
    r'\b(?:nil\s+position|zero\s+position|no\s+outstanding|no\s+active\s+lgs?|no\s+active\s+guarantees?|nil\s+balance|zero\s+balance|0(?:\.00)?\s+outstanding|no\s+guarantees?\s+issued|no\s+facilities\s+or\s+guarantees)\b',
    r'(?:لا\s+يوجد\s+خطابات\s+ضمان|لا\s+توجد\s+خطابات\s+ضمان|لا\s+يوجد\s+موقف|الرصيد\s+صفر|لا\s+توجد\s+ضمانات\s+قائمة|لا\s+توجد\s+كفالات|الموقف\s+صفر|موقف\s+صفر|لا\s+توجد\s+أية\s+خطابات\s+ضمان|لا\s+توجد\s+أي\s+خطابات\s+ضمان|لا\s+يوجد\s+رصيد\s+قائم)'
]


class InboxClassificationService:
    """
    Classifies ingested emails into actionable categories:
    - LG_POSITION_REPORT
    - BANK_STATEMENT
    - CASH_POSITION_SNAPSHOT
    - PROGRESS_REPORT
    - UNCLASSIFIED
    """

    def _inspect_dataframe_headers(self, df: Any) -> Tuple[Optional[str], List[str]]:
        """Inspects a pandas DataFrame header/top row for LG vs Bank Statement structure."""
        if df is None or df.empty:
            return None, []

        headers_str = " ".join([str(c).lower() for c in df.columns])
        row_0_str = ""
        try:
            row_0_str = " ".join([str(v).lower() for v in df.iloc[0].values])
        except Exception:
            pass
        combined_text = f"{headers_str} {row_0_str}"

        # 1. Check LG Position columns
        lg_matched_keys = []
        for field, keywords in LG_COLUMN_KEYWORDS.items():
            if any(kw in combined_text for kw in keywords):
                lg_matched_keys.append(field)

        if len(lg_matched_keys) >= 2 or "bank_lg_number" in lg_matched_keys:
            return "LG_POSITION_REPORT", lg_matched_keys

        # 2. Check Statement keywords (debit, credit, balance)
        statement_matches = [kw for kw in STATEMENT_KEYWORDS if kw in combined_text]
        if len(statement_matches) >= 2 or (
            "balance" in statement_matches
            and ("debit" in statement_matches or "credit" in statement_matches)
        ):
            return "BANK_STATEMENT", statement_matches

        return None, []

    def _inspect_attachment_headers(
        self,
        file_path: str,
        file_type: Optional[str],
        db: Optional[Session] = None,
        customer_id: Optional[int] = None,
        bank_id: Optional[int] = None,
    ) -> Tuple[Optional[str], List[str], Optional[List[Dict[str, Any]]]]:
        """
        Reads attachment bytes from GCS or local disk and inspects structure across:
        - Excel / CSV
        - SWIFT MT940 / ISO 20022 CAMT.053
        - PDF (unlocking using saved/CIF passwords if encrypted)
        Returns (detected_category, list_of_matched_columns, extracted_summary_balances).
        """
        raw_bytes = read_attachment_bytes(file_path)
        if not raw_bytes:
            return None, [], None

        ft = (file_type or "").lower().lstrip(".")

        if ft in ("mt940", "sta", "940", "txt", "xml"):
            if banking_standards_ontology.is_mt940_content(raw_bytes) or banking_standards_ontology.is_camt053_content(raw_bytes):
                return "BANK_STATEMENT", ["SWIFT_MT940_OR_CAMT053"], None

        if ft in ("xlsx", "xls", "xlsb", "csv"):
            df = None
            try:
                import pandas as pd
                if ft in ("xlsx", "xls", "xlsb"):
                    df = pd.read_excel(io.BytesIO(raw_bytes), nrows=15)
                else:
                    df = pd.read_csv(io.BytesIO(raw_bytes), nrows=15)
            except Exception as e:
                logger.warning(f"Could not read attachment {file_path} for header inspection: {e}")
                return None, [], None
            cat, cols = self._inspect_dataframe_headers(df)
            return cat, cols, None

        if ft == "pdf":
            try:
                passwords = get_candidate_pdf_passwords(db, customer_id, bank_id)
                unlocked_bytes, was_encrypted, is_unlocked = unlock_pdf_bytes(raw_bytes, passwords)
                if was_encrypted and not is_unlocked:
                    return "LOCKED_PDF", ["ENCRYPTED_PDF"], None
                df, _, summary_balances = extract_statement_from_pdf(unlocked_bytes)
                if df is not None and not df.empty:
                    cat, cols = self._inspect_dataframe_headers(df)
                    if cat:
                        return cat, cols, summary_balances or None
                    if len(df) >= 2 and df.shape[1] >= 3:
                        return "BANK_STATEMENT", ["PDF_TABLE"], summary_balances or None
                if summary_balances:
                    return "CASH_POSITION_SNAPSHOT", ["PDF_BALANCE_SUMMARY"], summary_balances
            except Exception as e:
                logger.warning(f"Could not inspect PDF attachment {file_path}: {e}")
                return None, [], None

        return None, [], None

    def classify(self, db: Session, item: InboxItem) -> InboxItem:
        """
        Runs multi-signal scoring to classify an InboxItem.
        Updates item classification, confidence, signals breakdown, and action summary.
        """
        signals = {}
        score = 0
        detected_category = "UNCLASSIFIED"
        matched_bank = None
        is_nil_detected = False
        locked_pdf_detected = False

        # Decrypt plain text body for inspection
        body_text = ""
        if item.body_text_encrypted:
            try:
                body_text = decrypt_data(item.body_text_encrypted) or ""
            except Exception:
                pass

        # ----------------------------------------------------------------------
        # SIGNAL 1: Reference Number Exact Match in Subject or Body
        # ----------------------------------------------------------------------
        ref_match = re.search(
            r'(?:POS|STM|BAL)-\d{8}-[A-Za-z0-9]+',
            f"{item.subject or ''} {body_text}",
            flags=re.IGNORECASE,
        )
        if ref_match:
            found_ref = ref_match.group(0).upper()
            outbound_req = db.query(InboxOutboundRequest).filter(
                InboxOutboundRequest.customer_id == item.customer_id,
                InboxOutboundRequest.request_reference == found_ref,
                InboxOutboundRequest.is_deleted == False
            ).first()

            if outbound_req:
                item.outbound_request_id = outbound_req.id
                outbound_req.is_replied = True
                outbound_req.reply_received_at = datetime.now(timezone.utc)
                score += 6
                signals["reference_match"] = {
                    "points": 6,
                    "reference": found_ref,
                    "outbound_id": outbound_req.id,
                    "request_type": outbound_req.request_type,
                    "subject": outbound_req.subject
                }
                if outbound_req.bank_id and not item.matched_bank_id:
                    item.matched_bank_id = outbound_req.bank_id
                    matched_bank = outbound_req.bank

                if outbound_req.request_type == "LG_POSITION":
                    detected_category = "LG_POSITION_REPORT"
                elif outbound_req.request_type == "BANK_STATEMENT":
                    detected_category = "BANK_STATEMENT"
                elif outbound_req.request_type == "CASH_POSITION":
                    detected_category = "CASH_POSITION_SNAPSHOT"
                elif outbound_req.request_type == "PROGRESS":
                    detected_category = "PROGRESS_REPORT"

        # ----------------------------------------------------------------------
        # SIGNAL 2: In-Reply-To Thread Header Match
        # ----------------------------------------------------------------------
        if item.in_reply_to and "reference_match" not in signals:
            outbound_req = db.query(InboxOutboundRequest).filter(
                InboxOutboundRequest.customer_id == item.customer_id,
                InboxOutboundRequest.message_id == item.in_reply_to,
                InboxOutboundRequest.is_deleted == False
            ).first()

            if outbound_req:
                item.outbound_request_id = outbound_req.id
                outbound_req.is_replied = True
                outbound_req.reply_received_at = datetime.now(timezone.utc)
                score += 5
                signals["thread_match"] = {
                    "points": 5,
                    "outbound_id": outbound_req.id,
                    "request_type": outbound_req.request_type,
                    "subject": outbound_req.subject
                }
                if outbound_req.bank_id and not item.matched_bank_id:
                    item.matched_bank_id = outbound_req.bank_id
                    matched_bank = outbound_req.bank

                if outbound_req.request_type == "LG_POSITION":
                    detected_category = "LG_POSITION_REPORT"
                elif outbound_req.request_type == "BANK_STATEMENT":
                    detected_category = "BANK_STATEMENT"
                elif outbound_req.request_type == "CASH_POSITION":
                    detected_category = "CASH_POSITION_SNAPSHOT"
                elif outbound_req.request_type == "PROGRESS":
                    detected_category = "PROGRESS_REPORT"

        # ----------------------------------------------------------------------
        # SIGNAL 3: Sender Domain & Bank Association
        # ----------------------------------------------------------------------
        if item.matched_bank_id and matched_bank is None:
            matched_bank = db.query(Bank).filter(
                Bank.id == item.matched_bank_id,
                Bank.is_deleted == False,
            ).first()
        if item.sender_domain:
            if item.sender_domain.lower() in GENERIC_DOMAINS:
                item.is_trusted_sender = False
                score -= 1
                signals["generic_domain"] = {
                    "points": -1,
                    "reason": "Sender is using a generic email provider (e.g. Gmail/Yahoo)"
                }
            else:
                bank = db.query(Bank).filter(
                    Bank.email_domain.ilike(item.sender_domain.strip().lower()),
                    Bank.is_deleted == False
                ).first()
                if bank:
                    item.matched_bank_id = bank.id
                    matched_bank = bank
                    score += 4
                    signals["bank_domain_match"] = {
                        "points": 4,
                        "bank_id": bank.id,
                        "bank_name": bank.name,
                        "domain": item.sender_domain
                    }

        # ----------------------------------------------------------------------
        # SIGNAL 4: NIL / Zero Position Detection (Text Statement in Body/Subject)
        # ----------------------------------------------------------------------
        full_text_to_search = f"{item.subject or ''} {body_text}"
        for pattern in NIL_POSITION_PATTERNS:
            m = re.search(pattern, full_text_to_search, flags=re.IGNORECASE)
            if m:
                is_nil_detected = True
                item.is_nil_position = True
                score += 6
                detected_category = "LG_POSITION_REPORT"
                signals["nil_position_detected"] = {
                    "points": 6,
                    "statement": m.group(0).strip()
                }
                break

        # ----------------------------------------------------------------------
        # SIGNAL 5: Attachment Structure Analysis (GCS + Local + PDF Unlock)
        # ----------------------------------------------------------------------
        has_structured_statement_or_lg_att = False
        if item.has_attachment and item.primary_attachment_path:
            att_category, matched_cols, att_balances = self._inspect_attachment_headers(
                item.primary_attachment_path,
                item.primary_attachment_type,
                db=db,
                customer_id=item.customer_id,
                bank_id=item.matched_bank_id,
            )
            if att_category == "LOCKED_PDF":
                locked_pdf_detected = True
                score += 3
                signals["pdf_password_required"] = {
                    "points": 3,
                    "reason": "Password-protected PDF attachment — enter statement password or configure on Bank Account",
                }
                if detected_category == "UNCLASSIFIED" and (
                    matched_bank
                    or any(
                        kw in f"{item.subject or ''} {item.primary_attachment_name or ''}".lower()
                        for kw in STATEMENT_KEYWORDS + CASH_POSITION_KEYWORDS
                    )
                ):
                    detected_category = "BANK_STATEMENT"
            elif att_category == "CASH_POSITION_SNAPSHOT":
                score += 5
                if detected_category == "UNCLASSIFIED":
                    detected_category = "CASH_POSITION_SNAPSHOT"
                if att_balances:
                    item.extracted_balances_json = att_balances
                signals["attachment_inspection"] = {
                    "points": 5,
                    "detected_type": att_category,
                    "matched_columns": matched_cols,
                }
            elif att_category:
                has_structured_statement_or_lg_att = True
                score += 5
                detected_category = att_category
                signals["attachment_inspection"] = {
                    "points": 5,
                    "detected_type": att_category,
                    "matched_columns": matched_cols,
                }
        elif detected_category == "LG_POSITION_REPORT" and not item.has_attachment and not is_nil_detected:
            # LG position without attachment and without NIL text gets a minor penalty
            score -= 1
            signals["missing_attachment_penalty"] = -1

        # ----------------------------------------------------------------------
        # SIGNAL 5b: Inline Bilingual Cash Balance Extraction from Email Body
        # ----------------------------------------------------------------------
        inline_balances = extract_cash_balances_from_text(f"{item.subject or ''}\n{body_text}")
        if inline_balances:
            if not item.extracted_balances_json:
                item.extracted_balances_json = inline_balances
            if not has_structured_statement_or_lg_att and not is_nil_detected:
                if detected_category in ("UNCLASSIFIED", "CASH_POSITION_SNAPSHOT"):
                    detected_category = "CASH_POSITION_SNAPSHOT"
                score += 6
                signals["inline_balance_detected"] = {
                    "points": 6,
                    "accounts_found": len(inline_balances),
                    "currencies": sorted({b["currency"] for b in inline_balances}),
                }

        # ----------------------------------------------------------------------
        # SIGNAL 6: Subject & Body Keywords Match
        # ----------------------------------------------------------------------
        subj_lower = (item.subject or "").lower()
        has_treasury_keyword = False

        if any(kw in subj_lower for kw in LG_POSITION_KEYWORDS):
            has_treasury_keyword = True
            score += 4
            if detected_category == "UNCLASSIFIED":
                detected_category = "LG_POSITION_REPORT"
            signals["subject_keyword_match"] = {"points": 4, "category": "LG_POSITION_REPORT"}
        elif any(kw in subj_lower for kw in CASH_POSITION_KEYWORDS):
            has_treasury_keyword = True
            score += 4
            if detected_category == "UNCLASSIFIED":
                detected_category = "CASH_POSITION_SNAPSHOT"
            signals["subject_keyword_match"] = {"points": 4, "category": "CASH_POSITION_SNAPSHOT"}
        elif any(kw in subj_lower for kw in STATEMENT_KEYWORDS):
            has_treasury_keyword = True
            score += 4
            if detected_category == "UNCLASSIFIED":
                detected_category = "BANK_STATEMENT"
            signals["subject_keyword_match"] = {"points": 4, "category": "BANK_STATEMENT"}
        elif any(kw in subj_lower for kw in PROGRESS_KEYWORDS):
            has_treasury_keyword = True
            score += 4
            if detected_category == "UNCLASSIFIED":
                detected_category = "PROGRESS_REPORT"
            signals["subject_keyword_match"] = {"points": 4, "category": "PROGRESS_REPORT"}

        # ----------------------------------------------------------------------
        # SIGNAL 7: Learning from Classification History
        # ----------------------------------------------------------------------
        history = db.query(EmailClassificationHistory).filter(
            EmailClassificationHistory.customer_id == item.customer_id,
            or_(
                EmailClassificationHistory.sender_email == item.sender_email,
                EmailClassificationHistory.sender_domain == item.sender_domain
            )
        ).order_by(desc(EmailClassificationHistory.id)).first()

        if history:
            score += 3
            if detected_category == "UNCLASSIFIED":
                detected_category = history.classification
            signals["history_match"] = {
                "points": 3,
                "previous_classification": history.classification,
                "corrected_before": history.was_user_corrected
            }

        # ----------------------------------------------------------------------
        # SIGNAL 8: Irrelevant / Promotional / Non-Treasury Detection
        # ----------------------------------------------------------------------
        # Guard: Never mark as IRRELEVANT if treasury keywords or reply matches exist
        if detected_category == "UNCLASSIFIED" and not has_treasury_keyword and not inline_balances:
            combined_search_text = f"{item.subject or ''} {body_text}".lower()
            sender_lower = (item.sender_email or "").lower()
            irrelevant_hits = [
                kw for kw in IRRELEVANT_KEYWORDS 
                if kw in combined_search_text or kw in sender_lower
            ]
            if irrelevant_hits:
                detected_category = "IRRELEVANT"
                confidence = "HIGH"
                signals["irrelevant_detection"] = {
                    "matched_keywords": irrelevant_hits,
                    "reason": "Non-treasury or promotional email"
                }
            elif score <= 0 and not item.has_attachment:
                # Untrusted generic domain with no positive treasury signals or attachments
                detected_category = "IRRELEVANT"
                confidence = "MEDIUM"
                signals["irrelevant_detection"] = {
                    "reason": "Generic sender domain with no treasury keywords or attachments"
                }

        # ----------------------------------------------------------------------
        # Final Confidence Assessment
        # ----------------------------------------------------------------------
        if detected_category == "IRRELEVANT":
            confidence = signals.get("irrelevant_detection", {}).get("confidence", "HIGH")
        elif score >= 6 and detected_category != "UNCLASSIFIED":
            confidence = "HIGH"
        elif score >= 3 and detected_category != "UNCLASSIFIED":
            confidence = "MEDIUM"
        elif detected_category != "UNCLASSIFIED":
            confidence = "LOW"
        else:
            confidence = "LOW"
            detected_category = "UNCLASSIFIED"

        # Action Summary Text
        bank_label = matched_bank.name if matched_bank else (item.sender_domain or "Bank")
        action_summary = None
        if detected_category == "LG_POSITION_REPORT":
            if item.is_nil_position:
                action_summary = f"Bank confirmed NIL position (Zero / No active guarantees as of requested date) from {bank_label}. Confirm to reconcile all active system LGs against a zero bank position."
            else:
                action_summary = f"LG Position Report from {bank_label}. Confirm to create a Reconciliation Session and run position matching."
        elif detected_category == "BANK_STATEMENT":
            if locked_pdf_detected:
                action_summary = f"Password-protected PDF Statement from {bank_label}. Confirm (and enter PDF password if not saved on Bank Account) to unlock and ingest."
            else:
                action_summary = f"Bank Statement from {bank_label}. Confirm to ingest transactions, update live cash position, and run auto-matching."
        elif detected_category == "CASH_POSITION_SNAPSHOT":
            balances_list = item.extracted_balances_json or []
            ccys = ", ".join(sorted({b.get("currency", "EGP") for b in balances_list})) if balances_list else "EGP"
            action_summary = (
                f"Cash Position Snapshot from {bank_label} "
                f"({len(balances_list)} balance(s) detected: {ccys}). Confirm to update Live Cash Positions."
            )
        elif detected_category == "PROGRESS_REPORT":
            action_summary = f"Project progress report from {item.sender_email}. Confirm to review milestone actions."
        elif detected_category == "IRRELEVANT":
            action_summary = "Non-treasury or promotional message. No downstream action required."
        else:
            action_summary = "Unclassified email. Please review the attachments/content and select a classification."

        # Update Item
        item.classification = detected_category
        item.classification_confidence = confidence
        item.confidence_score = score
        item.classification_signals = signals
        item.action_summary = action_summary
        item.status = "CLASSIFIED"

        db.commit()
        db.refresh(item)
        return item

    def reclassify(
        self,
        db: Session,
        inbox_item_id: int,
        new_classification: str,
        user_id: int
    ) -> InboxItem:
        """
        Allows user to override system classification.
        Saves user choice to learning history and updates item.
        """
        item = db.query(InboxItem).filter(
            InboxItem.id == inbox_item_id,
            InboxItem.is_deleted == False
        ).first()

        if not item:
            raise ValueError(f"InboxItem {inbox_item_id} not found")

        original_class = item.classification
        item.user_override_classification = new_classification
        item.classification = new_classification
        item.classification_confidence = "HIGH"  # User verified

        # Update action summary
        bank_label = item.matched_bank.name if item.matched_bank else (item.sender_domain or "Bank")
        if new_classification == "LG_POSITION_REPORT":
            item.action_summary = f"LG Position Report from {bank_label}. Ready to reconcile."
        elif new_classification == "BANK_STATEMENT":
            item.action_summary = f"Bank Statement from {bank_label}. Ready to ingest and reconcile."
        elif new_classification == "CASH_POSITION_SNAPSHOT":
            item.action_summary = f"Cash Position Snapshot from {bank_label}. Ready to update Live Cash Positions."
        elif new_classification == "PROGRESS_REPORT":
            item.action_summary = f"Progress update from {item.sender_email}. Ready to review."
        else:
            item.action_summary = "Unclassified item."

        # Record in Learning History
        history = EmailClassificationHistory(
            customer_id=item.customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            classification=new_classification,
            was_user_corrected=(new_classification != original_class),
            original_classification=original_class,
            confidence_score=item.confidence_score,
            confirmed_by_user_id=user_id
        )
        db.add(history)
        db.commit()
        db.refresh(item)
        return item


inbox_classification_service = InboxClassificationService()
