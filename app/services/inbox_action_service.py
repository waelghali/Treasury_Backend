# app/services/inbox_action_service.py
"""
Action Execution Service for Smart Inbox
Executes confirmed inbox items by bridging them into the appropriate target module:
- LG_POSITION_REPORT -> ReconciliationSession creation + file parsing + matching
- BANK_STATEMENT -> BankStatement ingestion + PDF unlock + auto-matching + CashBalanceSnapshot
- CASH_POSITION_SNAPSHOT -> Multi-currency live CashBalanceSnapshot updates
- PROGRESS_REPORT -> Milestone action review
"""

import logging
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_data, encrypt_data
from app.core.storage_service import read_attachment_bytes
from app.crud.crud import log_action
from app.models.models import Bank, User
from app.models.models_inbox import (
    CashBalanceSnapshot,
    EmailClassificationHistory,
    InboxItem,
)
from app.models.models_issuance import CustomerBankAccount
from app.models.models_reconciliation_v2 import BankStatement, BankTransaction
from app.services.bank_reconciliation_service import bank_reconcile_service
from app.services.pdf_statement_parser import (
    extract_cash_balances_from_text,
    extract_statement_from_pdf,
    get_candidate_pdf_passwords,
    unlock_pdf_bytes,
)
from app.services.reconciliation_service import reconciliation_service

logger = logging.getLogger("app.inbox_action")


def _normalize_acct_key(raw: Optional[str]) -> str:
    if not raw:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(raw)).upper()


def _parse_as_of_datetime(
    raw_date: Optional[Any],
    fallback_dt: Optional[datetime] = None,
) -> datetime:
    if isinstance(raw_date, datetime):
        return raw_date if raw_date.tzinfo else raw_date.replace(tzinfo=timezone.utc)
    if isinstance(raw_date, date):
        return datetime.combine(raw_date, datetime.min.time(), tzinfo=timezone.utc)
    if isinstance(raw_date, str) and raw_date.strip():
        cleaned = raw_date.strip()
        is_iso = bool(re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[T\s].*)?$", cleaned))
        if is_iso:
            try:
                iso_candidate = cleaned.replace("Z", "+00:00")
                if "/" in iso_candidate[:10]:
                    iso_candidate = iso_candidate[:10].replace("/", "-") + iso_candidate[10:]
                py_dt = datetime.fromisoformat(iso_candidate)
                return py_dt if py_dt.tzinfo else py_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
        try:
            import pandas as pd
            dt = pd.to_datetime(cleaned, dayfirst=not is_iso, errors="coerce")
            if dt is not None and not pd.isna(dt):
                py_dt = dt.to_pydatetime()
                return py_dt if py_dt.tzinfo else py_dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    if fallback_dt:
        return fallback_dt if fallback_dt.tzinfo else fallback_dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


class InboxActionService:
    """
    Executes confirmed (and eligible zero-touch) inbox items, records cash balance snapshots,
    and aggregates live multi-bank cash positions.
    """

    def _match_customer_bank_account(
        self,
        db: Session,
        customer_id: int,
        bank_id: int,
        account_number: Optional[str] = None,
        currency_code: Optional[str] = None,
    ) -> Optional[CustomerBankAccount]:
        accounts = (
            db.query(CustomerBankAccount)
            .filter(
                CustomerBankAccount.customer_id == customer_id,
                CustomerBankAccount.bank_id == bank_id,
                CustomerBankAccount.is_deleted == False,
                CustomerBankAccount.is_active == True,
            )
            .order_by(CustomerBankAccount.is_default.desc(), CustomerBankAccount.id.asc())
            .all()
        )
        if not accounts:
            return None

        target_ccy = (
            str(currency_code).strip().upper()[:3]
            if currency_code and str(currency_code).strip()
            else None
        )

        def _pick_by_currency(candidates: List[CustomerBankAccount]) -> Optional[CustomerBankAccount]:
            if not candidates:
                return None
            if target_ccy:
                ccy_matches = [c for c in candidates if (c.currency_code or "").upper() == target_ccy]
                if len(ccy_matches) == 1:
                    return ccy_matches[0]
                if ccy_matches:
                    for c in ccy_matches:
                        if c.is_default:
                            return c
                    return None
            if len(candidates) == 1:
                return candidates[0]
            for c in candidates:
                if c.is_default:
                    return c
            return candidates[0]

        target_norm = _normalize_acct_key(account_number)
        if target_norm:
            exact_matches = []
            for a in accounts:
                acct_norm = _normalize_acct_key(a.account_number)
                iban_norm = _normalize_acct_key(a.iban)
                if acct_norm and (
                    acct_norm == target_norm
                    or (min(len(acct_norm), len(target_norm)) >= 6 and (acct_norm.endswith(target_norm) or target_norm.endswith(acct_norm)))
                ):
                    exact_matches.append(a)
                elif iban_norm and (
                    iban_norm == target_norm
                    or (len(target_norm) >= 6 and iban_norm.endswith(target_norm))
                ):
                    exact_matches.append(a)

            if exact_matches:
                picked = _pick_by_currency(exact_matches)
                if picked:
                    return picked

            if len(target_norm) >= 6:
                tail6 = target_norm[-6:]
                tail_matches = [
                    a for a in accounts
                    if _normalize_acct_key(a.account_number).endswith(tail6)
                    or _normalize_acct_key(a.iban).endswith(tail6)
                ]
                if len(tail_matches) == 1:
                    return tail_matches[0]
                if tail_matches and target_ccy:
                    ccy_tail = [a for a in tail_matches if (a.currency_code or "").upper() == target_ccy]
                    if len(ccy_tail) == 1:
                        return ccy_tail[0]

            return None

        if target_ccy:
            ccy_accounts = [a for a in accounts if (a.currency_code or "").upper() == target_ccy]
            if not ccy_accounts:
                return None
            if len(ccy_accounts) == 1:
                return ccy_accounts[0]
            for a in ccy_accounts:
                if a.is_default:
                    return a
            return ccy_accounts[0]

        if len(accounts) == 1:
            return accounts[0]

        for a in accounts:
            if a.is_default:
                return a
        return accounts[0]

    @staticmethod
    def _parse_finite_decimal(val: Any, field_name: str, default_zero: bool = False) -> Decimal:
        if val is None:
            if default_zero:
                return Decimal("0")
            raise HTTPException(status_code=400, detail=f"Missing numeric value for {field_name}.")
        if isinstance(val, bool):
            raise HTTPException(status_code=400, detail=f"Invalid numeric value for {field_name}.")
        try:
            s = str(val).strip()
            if not s or "," in s:
                raise HTTPException(status_code=400, detail=f"Invalid numeric value for {field_name}.")
            dec = Decimal(s)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=f"Invalid numeric value for {field_name}.") from exc
        if not dec.is_finite():
            raise HTTPException(status_code=400, detail=f"Non-finite numeric value for {field_name}.")
        return dec

    def record_cash_balance_snapshot(
        self,
        db: Session,
        customer_id: int,
        bank_id: int,
        available_balance: Any,
        currency_code: str = "EGP",
        ledger_balance: Optional[Any] = None,
        account_number: Optional[str] = None,
        as_of_datetime: Optional[datetime] = None,
        source_channel: str = "EMAIL_STATEMENT",
        inbox_item_id: Optional[int] = None,
        statement_id: Optional[int] = None,
        continuity_status: str = "VALIDATED",
        notes: Optional[str] = None,
        auto_commit: bool = False,
    ) -> CashBalanceSnapshot:
        """Creates and persists a point-in-time CashBalanceSnapshot linked to CustomerBankAccount when available."""
        raw_acct = str(account_number).strip()[:64] if account_number and str(account_number).strip() else None
        matched_acct = self._match_customer_bank_account(
            db, customer_id, bank_id, raw_acct, currency_code=currency_code
        )
        effective_acct_num = raw_acct or (
            matched_acct.account_number[:64] if matched_acct and matched_acct.account_number else None
        )
        effective_ccy = (
            str(currency_code).strip().upper()[:3]
            if currency_code and str(currency_code).strip()
            else (
                matched_acct.currency_code.strip().upper()[:3]
                if matched_acct and matched_acct.currency_code
                else "EGP"
            )
        )
        avail_dec = self._parse_finite_decimal(available_balance, "available_balance", default_zero=True)
        ledger_dec = (
            self._parse_finite_decimal(ledger_balance, "ledger_balance")
            if ledger_balance is not None
            else avail_dec
        )
        effective_dt = _parse_as_of_datetime(as_of_datetime)
        effective_source = (str(source_channel or "EMAIL_STATEMENT").strip()[:40] or "EMAIL_STATEMENT")
        effective_continuity = (str(continuity_status or "VALIDATED").strip()[:40] or "VALIDATED")

        snapshot = CashBalanceSnapshot(
            customer_id=customer_id,
            bank_id=bank_id,
            bank_account_id=matched_acct.id if matched_acct else None,
            account_number=effective_acct_num,
            currency_code=effective_ccy,
            available_balance=avail_dec,
            ledger_balance=ledger_dec,
            as_of_datetime=effective_dt,
            source_channel=effective_source,
            inbox_item_id=inbox_item_id,
            statement_id=statement_id,
            continuity_status=effective_continuity,
            notes=notes,
        )
        db.add(snapshot)
        db.flush()
        if auto_commit:
            db.commit()
            db.refresh(snapshot)
        return snapshot

    async def confirm_and_execute(
        self,
        db: Session,
        inbox_item_id: int,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None,
        pdf_password: Optional[str] = None,
        save_pdf_password: bool = False,
        override_balances: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Confirms an InboxItem and triggers its respective downstream action."""
        item = db.query(InboxItem).filter(
            InboxItem.id == inbox_item_id,
            InboxItem.customer_id == customer_id,
            InboxItem.is_deleted == False
        ).first()

        if not item:
            raise HTTPException(status_code=404, detail="Inbox item not found")

        if item.status in ("ACTIONED", "CONFIRMED"):
            raise HTTPException(
                status_code=400,
                detail=f"Item has already been actioned (Ref: {item.action_reference_type} #{item.action_reference_id})"
            )

        if item.classification == "LG_POSITION_REPORT":
            return await self._execute_lg_position(
                db, item, user_id, customer_id, override_bank_id, override_position_date
            )

        elif item.classification == "BANK_STATEMENT":
            return self._execute_bank_statement_sync(
                db=db,
                item=item,
                user_id=user_id,
                customer_id=customer_id,
                override_bank_id=override_bank_id,
                override_position_date=override_position_date,
                pdf_password=pdf_password,
                save_pdf_password=save_pdf_password,
            )

        elif item.classification == "CASH_POSITION_SNAPSHOT":
            return self._execute_cash_position_snapshot_sync(
                db=db,
                item=item,
                user_id=user_id,
                customer_id=customer_id,
                override_bank_id=override_bank_id,
                override_position_date=override_position_date,
                override_balances=override_balances,
                pdf_password=pdf_password,
                save_pdf_password=save_pdf_password,
            )

        elif item.classification == "PROGRESS_REPORT":
            item.status = "CONFIRMED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "ProgressReport"
            db.commit()
            return {
                "status": "CONFIRMED",
                "message": "Progress report confirmed and saved.",
                "item_id": item.id
            }

        elif item.classification == "IRRELEVANT":
            item.status = "ARCHIVED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "Archived"
            db.commit()
            return {
                "status": "ARCHIVED",
                "message": "Item marked as irrelevant and archived.",
                "item_id": item.id
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Item must be classified as a valid actionable category before confirmation."
            )

    async def _execute_lg_position(
        self,
        db: Session,
        item: InboxItem,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Executes LG Position Report:
        1. Validates Bank and Position Date.
        2. Creates ReconciliationSession.
        3. Parses file (GCS or local) into bank rows.
        4. Runs matching engine against active IssuedLGRecords.
        """
        bank_id = override_bank_id or item.matched_bank_id
        if not bank_id:
            raise HTTPException(
                status_code=400,
                detail="Please specify the Bank associated with this LG Position Report."
            )

        bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise HTTPException(status_code=400, detail=f"Bank ID {bank_id} not found.")

        pos_date = override_position_date or (item.received_at.date() if item.received_at else date.today())

        # Handle NIL / Zero Position Reconciliation
        if item.is_nil_position or not item.has_attachment:
            session = reconciliation_service.create_session(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                position_date=pos_date,
                user_id=user_id,
                file_name="NIL_POSITION_CONFIRMATION",
                notes=f"NIL/Zero position confirmed via Smart Inbox email: '{item.subject}' (Sender: {item.sender_email})"
            )
            session.status = "PARSED"
            session.parsing_method = "NIL_EMAIL"
            session.total_bank_records = 0
            db.flush()

            try:
                session = reconciliation_service.run_matching(
                    db=db,
                    session_id=session.id,
                    customer_id=customer_id,
                    user_id=user_id
                )
            except Exception as match_err:
                logger.warning(f"Reconciliation matching warning for nil-position InboxItem {item.id}: {match_err}")

            item.status = "ACTIONED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "ReconciliationSession"
            item.action_reference_id = session.id
            item.matched_bank_id = bank_id
            db.commit()

            return {
                "status": "ACTIONED",
                "message": f"NIL Position Reconciliation Session #{session.id} created and matched against system records.",
                "session_id": session.id,
                "item_id": item.id
            }

        file_bytes = read_attachment_bytes(item.primary_attachment_path)
        if not file_bytes:
            raise HTTPException(
                status_code=400,
                detail="No readable attachment file found for this position report."
            )

        filename = item.primary_attachment_name or os.path.basename(item.primary_attachment_path or "position.xlsx")

        session = reconciliation_service.create_session(
            db=db,
            customer_id=customer_id,
            bank_id=bank_id,
            position_date=pos_date,
            user_id=user_id,
            file_name=filename,
            notes=f"Auto-generated via Smart Inbox from email: '{item.subject}' (Sender: {item.sender_email})"
        )

        try:
            session = await reconciliation_service.parse_file(
                db=db,
                session_id=session.id,
                file_bytes=file_bytes,
                file_name=filename,
                customer_id=customer_id,
                user_id=user_id
            )
        except Exception as parse_err:
            logger.error(f"Reconciliation parse failed for InboxItem {item.id}: {parse_err}")
            item.status = "PARSE_ERROR"
            item.error_message = str(parse_err)
            db.commit()
            raise HTTPException(status_code=400, detail=f"File parsing error: {str(parse_err)}")

        try:
            session = reconciliation_service.run_matching(
                db=db,
                session_id=session.id,
                customer_id=customer_id,
                user_id=user_id
            )
        except Exception as match_err:
            logger.warning(f"Reconciliation matching warning for InboxItem {item.id}: {match_err}")

        item.status = "ACTIONED"
        item.actioned_at = datetime.now(timezone.utc)
        item.actioned_by_user_id = user_id
        item.action_reference_type = "ReconciliationSession"
        item.action_reference_id = session.id
        item.matched_bank_id = bank_id

        history = EmailClassificationHistory(
            customer_id=customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            classification="LG_POSITION_REPORT",
            was_user_corrected=(item.user_override_classification is not None),
            original_classification=item.classification,
            confidence_score=item.confidence_score,
            confirmed_by_user_id=user_id
        )
        db.add(history)

        log_action(
            db=db,
            user_id=user_id,
            action_type="INBOX_ITEM_ACTIONED",
            entity_type="InboxItem",
            entity_id=item.id,
            details={
                "classification": "LG_POSITION_REPORT",
                "session_id": session.id,
                "bank_id": bank_id,
                "position_date": pos_date.isoformat(),
                "total_records": session.total_bank_records,
                "matched_count": session.matched_count,
                "mismatched_count": session.mismatched_count
            },
            customer_id=customer_id
        )

        db.commit()
        db.refresh(item)

        return {
            "status": "SUCCESS",
            "message": f"Reconciliation Session #{session.id} successfully created and matched with {session.total_bank_records} bank records.",
            "session_id": session.id,
            "bank_id": bank_id,
            "bank_name": bank.name,
            "position_date": pos_date.isoformat(),
            "total_records": session.total_bank_records,
            "matched_count": session.matched_count,
            "mismatched_count": session.mismatched_count,
            "item_id": item.id
        }

    def _execute_bank_statement_sync(
        self,
        db: Session,
        item: InboxItem,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None,
        pdf_password: Optional[str] = None,
        save_pdf_password: bool = False,
        is_auto_executed: bool = False,
    ) -> Dict[str, Any]:
        """
        Executes BANK_STATEMENT ingestion into Bank Reconciliation v2:
        - Reads attachment bytes from GCS or local storage
        - Unlocks encrypted PDFs (and optionally persists the static PDF password)
        - Falls back to CashBalanceSnapshot if PDF only contains summary balances
        - Ingests transactions, runs auto-matching, and records CashBalanceSnapshot
        """
        bank_id = override_bank_id or item.matched_bank_id
        if not bank_id:
            raise HTTPException(
                status_code=400,
                detail="Please specify the Bank associated with this Bank Statement."
            )

        bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise HTTPException(status_code=400, detail=f"Bank ID {bank_id} not found.")

        if item.status in ("ACTIONED", "CONFIRMED") or (
            item.action_reference_type == "BankStatement" and item.action_reference_id
        ):
            if item.action_reference_id:
                existing_stmt = (
                    db.query(BankStatement)
                    .filter(
                        BankStatement.id == item.action_reference_id,
                        BankStatement.customer_id == customer_id,
                    )
                    .first()
                )
                if existing_stmt:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Inbox item has already been ingested into Bank Statement #{existing_stmt.id}.",
                    )
            raise HTTPException(status_code=400, detail="Inbox item has already been actioned.")

        file_bytes = read_attachment_bytes(item.primary_attachment_path)
        if not file_bytes:
            if item.extracted_balances_json:
                return self._execute_cash_position_snapshot_sync(
                    db=db,
                    item=item,
                    user_id=user_id,
                    customer_id=customer_id,
                    override_bank_id=bank_id,
                    override_position_date=override_position_date,
                    is_auto_executed=is_auto_executed,
                )
            raise HTTPException(
                status_code=400,
                detail="No readable statement attachment found for this inbox item."
            )

        filename = item.primary_attachment_name or os.path.basename(item.primary_attachment_path or "statement.xlsx")
        ext = os.path.splitext(filename)[1].lower().lstrip(".") or (item.primary_attachment_type or "").lower()

        pdf_meta: Optional[Dict[str, Any]] = None
        should_save_pdf_pw = False

        if ext == "pdf":
            passwords = get_candidate_pdf_passwords(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                explicit_password=pdf_password,
            )
            unlocked_bytes, was_encrypted, is_unlocked = unlock_pdf_bytes(file_bytes, passwords)
            if was_encrypted and not is_unlocked:
                # Raise cleanly WITHOUT setting PARSE_ERROR so user can enter password and retry
                raise HTTPException(
                    status_code=400,
                    detail="PDF_PASSWORD_REQUIRED: Statement PDF is password-protected. Please enter the PDF password to unlock.",
                )
            if not is_unlocked:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid or unreadable PDF statement file.",
                )

            should_save_pdf_pw = bool(
                save_pdf_password and pdf_password and pdf_password.strip() and was_encrypted and is_unlocked
            )

            try:
                df_pdf, pdf_meta, summary_balances = extract_statement_from_pdf(unlocked_bytes)
            except ValueError as pdf_err:
                raise HTTPException(status_code=400, detail=str(pdf_err)) from pdf_err
            if (df_pdf is None or df_pdf.empty) and summary_balances:
                item.extracted_balances_json = summary_balances
                return self._execute_cash_position_snapshot_sync(
                    db=db,
                    item=item,
                    user_id=user_id,
                    customer_id=customer_id,
                    override_bank_id=bank_id,
                    override_position_date=override_position_date,
                    pdf_password=pdf_password,
                    save_pdf_password=should_save_pdf_pw,
                    pdf_already_unlocked=should_save_pdf_pw,
                    source_channel="EMAIL_PDF_SUMMARY",
                    is_auto_executed=is_auto_executed,
                )

        overrides: Dict[str, Any] = {
            "bank_id": bank_id,
            "file_name": filename,
            "pdf_password": pdf_password,
        }

        try:
            db_stmt = bank_reconcile_service.process_ingestion(
                db,
                file_bytes,
                ext,
                company_id=customer_id,
                user_id=user_id,
                overrides=overrides,
            )
        except HTTPException:
            db.rollback()
            raise
        except Exception as ingest_err:
            logger.error(f"Statement ingestion failed for InboxItem {item.id}: {ingest_err}")
            db.rollback()
            if not is_auto_executed:
                refreshed_item = db.query(InboxItem).filter(InboxItem.id == item.id).first()
                if refreshed_item:
                    refreshed_item.status = "PARSE_ERROR"
                    refreshed_item.error_message = str(ingest_err)[:1000]
                    db.commit()
            raise HTTPException(status_code=400, detail=f"Statement parsing error: {str(ingest_err)}")

        match_res = None
        try:
            match_res = bank_reconcile_service.run_matching_engine(
                db,
                customer_id=customer_id,
                user_id=user_id,
                statement_id=db_stmt.id,
            )
        except Exception as match_err:
            logger.warning(f"Auto-matching warning for statement #{db_stmt.id}: {match_err}")

        # Determine currency from parsed transactions, PDF metadata, or linked bank account
        first_txn = (
            db.query(BankTransaction.currency)
            .filter(BankTransaction.statement_id == db_stmt.id)
            .first()
        )
        pdf_ccy = pdf_meta.get("currency") if isinstance(pdf_meta, dict) else None
        inferred_ccy = (first_txn[0] if first_txn and first_txn[0] else None) or pdf_ccy
        matched_acct = self._match_customer_bank_account(
            db,
            customer_id,
            bank_id,
            db_stmt.account_number,
            currency_code=inferred_ccy,
        )
        stmt_currency = (
            inferred_ccy
            or (matched_acct.currency_code if matched_acct and matched_acct.currency_code else None)
            or "EGP"
        ).upper()[:3]

        if should_save_pdf_pw and matched_acct:
            matched_acct.statement_pdf_password_encrypted = encrypt_data(pdf_password.strip())
            db.flush()

        if item.ingestion_channel == "WATCHED_FOLDER":
            src_channel = "WATCHED_FOLDER"
        elif ext in ("mt940", "sta", "940", "xml"):
            src_channel = "MT940_FEED"
        else:
            src_channel = "EMAIL_STATEMENT"

        snapshot = self.record_cash_balance_snapshot(
            db=db,
            customer_id=customer_id,
            bank_id=bank_id,
            available_balance=db_stmt.closing_balance,
            ledger_balance=db_stmt.closing_balance,
            currency_code=stmt_currency,
            account_number=db_stmt.account_number,
            as_of_datetime=db_stmt.statement_end_date,
            source_channel=src_channel,
            inbox_item_id=item.id,
            statement_id=db_stmt.id,
            continuity_status=db_stmt.balance_continuity_status or "VALIDATED",
            notes=f"Auto-captured from {'watched-folder ' if src_channel == 'WATCHED_FOLDER' else ''}statement '{filename}'",
        )

        item.status = "ACTIONED"
        item.actioned_at = datetime.now(timezone.utc)
        item.actioned_by_user_id = user_id
        item.action_reference_type = "BankStatement"
        item.action_reference_id = db_stmt.id
        item.matched_bank_id = bank_id
        item.error_message = None

        history = EmailClassificationHistory(
            customer_id=customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            classification="BANK_STATEMENT",
            was_user_corrected=(item.user_override_classification is not None),
            original_classification=item.classification,
            confidence_score=item.confidence_score,
            confirmed_by_user_id=user_id,
        )
        db.add(history)

        log_action(
            db=db,
            user_id=user_id,
            action_type="INBOX_STATEMENT_INGESTED",
            entity_type="InboxItem",
            entity_id=item.id,
            details={
                "statement_id": db_stmt.id,
                "snapshot_id": snapshot.id,
                "bank_id": bank_id,
                "closing_balance": float(db_stmt.closing_balance or 0),
                "currency": stmt_currency,
                "auto_executed": is_auto_executed,
            },
            customer_id=customer_id,
        )

        db.commit()
        db.refresh(item)

        return {
            "status": "ACTIONED",
            "message": f"Statement #{db_stmt.id} ingested ({stmt_currency} {float(db_stmt.closing_balance or 0):,.2f}) and live cash position updated.",
            "statement_id": db_stmt.id,
            "snapshot_id": snapshot.id,
            "bank_id": bank_id,
            "bank_name": bank.name,
            "closing_balance": float(db_stmt.closing_balance or 0),
            "currency": stmt_currency,
            "continuity_status": db_stmt.balance_continuity_status,
            "match_result": match_res,
            "item_id": item.id,
        }

    def _execute_cash_position_snapshot_sync(
        self,
        db: Session,
        item: InboxItem,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None,
        override_balances: Optional[List[Any]] = None,
        pdf_password: Optional[str] = None,
        save_pdf_password: bool = False,
        pdf_already_unlocked: bool = False,
        source_channel: Optional[str] = None,
        is_auto_executed: bool = False,
    ) -> Dict[str, Any]:
        """Executes CASH_POSITION_SNAPSHOT by recording one or more CashBalanceSnapshot rows."""
        bank_id = override_bank_id or item.matched_bank_id
        if not bank_id:
            raise HTTPException(
                status_code=400,
                detail="Please specify the Bank associated with this Cash Position snapshot."
            )

        bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise HTTPException(status_code=400, detail=f"Bank ID {bank_id} not found.")

        if item.status in ("ACTIONED", "CONFIRMED"):
            raise HTTPException(
                status_code=400,
                detail=f"Item has already been actioned (Ref: {item.action_reference_type} #{item.action_reference_id})",
            )

        raw_balances = override_balances if override_balances else (item.extracted_balances_json or [])
        balances: List[Dict[str, Any]] = [
            b.model_dump() if hasattr(b, "model_dump") else dict(b)
            for b in raw_balances
            if isinstance(b, dict) or hasattr(b, "model_dump")
        ]
        effective_source = source_channel or "EMAIL_BODY"
        was_pdf_unlocked = bool(pdf_already_unlocked)

        if not balances and item.has_attachment and (item.primary_attachment_type or "").lower() == "pdf":
            file_bytes = read_attachment_bytes(item.primary_attachment_path)
            if file_bytes:
                passwords = get_candidate_pdf_passwords(db, customer_id, bank_id, explicit_password=pdf_password)
                unlocked_bytes, was_encrypted, is_unlocked = unlock_pdf_bytes(file_bytes, passwords)
                if was_encrypted and not is_unlocked:
                    raise HTTPException(
                        status_code=400,
                        detail="PDF_PASSWORD_REQUIRED: Attachment PDF is password-protected. Please enter the PDF password.",
                    )
                if not is_unlocked:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid or unreadable PDF attachment file.",
                    )
                was_pdf_unlocked = bool(was_encrypted and is_unlocked)
                try:
                    _, _, balances = extract_statement_from_pdf(unlocked_bytes)
                except ValueError as pdf_err:
                    raise HTTPException(status_code=400, detail=str(pdf_err)) from pdf_err
                effective_source = "EMAIL_PDF_SUMMARY"

        if not balances and item.body_text_encrypted:
            try:
                body_text = decrypt_data(item.body_text_encrypted) or ""
                balances = extract_cash_balances_from_text(f"{item.subject or ''}\n{body_text}")
            except Exception:
                balances = []

        if not balances:
            raise HTTPException(
                status_code=400,
                detail="No cash balance figures could be extracted. Please enter or verify balances before confirming."
            )

        snapshots: List[CashBalanceSnapshot] = []
        serialized_balances: List[Dict[str, Any]] = []
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            avail = entry.get("available_balance")
            ledger = entry.get("ledger_balance")
            if avail is None and ledger is None:
                continue
            as_of_dt = _parse_as_of_datetime(
                override_position_date or entry.get("as_of_date"),
                fallback_dt=item.received_at,
            )
            snap = self.record_cash_balance_snapshot(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                available_balance=avail if avail is not None else ledger,
                ledger_balance=ledger if ledger is not None else avail,
                currency_code=entry.get("currency") or "EGP",
                account_number=entry.get("account_number"),
                as_of_datetime=as_of_dt,
                source_channel=effective_source,
                inbox_item_id=item.id,
                continuity_status="SNAPSHOT",
                notes=f"Extracted from Smart Inbox item #{item.id}: '{item.subject or ''}'",
            )
            snapshots.append(snap)
            serialized_balances.append({
                "snapshot_id": snap.id,
                "account_number": snap.account_number,
                "currency": snap.currency_code,
                "available_balance": float(snap.available_balance),
                "ledger_balance": float(snap.ledger_balance) if snap.ledger_balance is not None else None,
            })

        if not snapshots:
            raise HTTPException(status_code=400, detail="No valid numeric balances provided.")

        if save_pdf_password and pdf_password and pdf_password.strip() and was_pdf_unlocked:
            first_snap = snapshots[0]
            acct_to_save = (
                db.query(CustomerBankAccount).filter(CustomerBankAccount.id == first_snap.bank_account_id).first()
                if first_snap.bank_account_id
                else self._match_customer_bank_account(
                    db, customer_id, bank_id, first_snap.account_number, currency_code=first_snap.currency_code
                )
            )
            if acct_to_save:
                acct_to_save.statement_pdf_password_encrypted = encrypt_data(pdf_password.strip())
                db.flush()

        item.extracted_balances_json = balances
        item.status = "ACTIONED"
        item.actioned_at = datetime.now(timezone.utc)
        item.actioned_by_user_id = user_id
        item.action_reference_type = "CashBalanceSnapshot"
        item.action_reference_id = snapshots[0].id
        item.matched_bank_id = bank_id
        item.error_message = None

        history = EmailClassificationHistory(
            customer_id=customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            classification="CASH_POSITION_SNAPSHOT",
            was_user_corrected=(item.user_override_classification is not None),
            original_classification=item.classification,
            confidence_score=item.confidence_score,
            confirmed_by_user_id=user_id,
        )
        db.add(history)

        log_action(
            db=db,
            user_id=user_id,
            action_type="INBOX_CASH_POSITION_RECORDED",
            entity_type="InboxItem",
            entity_id=item.id,
            details={
                "bank_id": bank_id,
                "snapshots_created": len(snapshots),
                "currencies": sorted({s.currency_code for s in snapshots}),
                "auto_executed": is_auto_executed,
            },
            customer_id=customer_id,
        )

        db.commit()
        db.refresh(item)

        return {
            "status": "ACTIONED",
            "message": f"Updated {len(snapshots)} live cash balance snapshot(s) for {bank.name}.",
            "snapshots_created": len(snapshots),
            "balances": serialized_balances,
            "bank_id": bank_id,
            "bank_name": bank.name,
            "item_id": item.id,
        }

    def try_auto_execute_item(self, db: Session, item: InboxItem) -> bool:
        """
        Zero-Touch Auto-Ingest:
        Automatically executes HIGH-confidence trusted bank statements or cash balances
        when the customer has enabled `auto_ingest_statements` on an account at that bank.
        Leaves item safely in CLASSIFIED state if any error (e.g. missing PDF password) occurs.
        """
        if (
            not item
            or item.status != "CLASSIFIED"
            or item.classification not in ("BANK_STATEMENT", "CASH_POSITION_SNAPSHOT")
            or item.classification_confidence != "HIGH"
            or not item.is_trusted_sender
            or not item.matched_bank_id
        ):
            return False

        has_auto_acct = (
            db.query(CustomerBankAccount.id)
            .filter(
                CustomerBankAccount.customer_id == item.customer_id,
                CustomerBankAccount.bank_id == item.matched_bank_id,
                CustomerBankAccount.auto_ingest_statements == True,
                CustomerBankAccount.is_active == True,
                CustomerBankAccount.is_deleted == False,
            )
            .first()
        )
        if not has_auto_acct:
            return False

        actor = (
            db.query(User)
            .filter(
                User.customer_id == item.customer_id,
                User.is_deleted == False,
            )
            .order_by(User.id.asc())
            .first()
        )
        if not actor:
            return False

        try:
            if item.classification == "BANK_STATEMENT":
                self._execute_bank_statement_sync(
                    db=db,
                    item=item,
                    user_id=actor.id,
                    customer_id=item.customer_id,
                    is_auto_executed=True,
                )
            else:
                self._execute_cash_position_snapshot_sync(
                    db=db,
                    item=item,
                    user_id=actor.id,
                    customer_id=item.customer_id,
                    is_auto_executed=True,
                )
            logger.info(f"Zero-touch auto-ingested InboxItem #{item.id} ({item.classification}) for customer {item.customer_id}")
            return True
        except Exception as auto_err:
            db.rollback()
            logger.info(f"Zero-touch auto-ingest deferred for InboxItem #{item.id}: {auto_err}")
            return False

    def get_live_cash_positions(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Aggregates the latest known balance per (bank, account, currency) across
        CashBalanceSnapshot records and ingested BankStatements.
        """
        now_utc = datetime.now(timezone.utc)

        banks = {
            b.id: b.name
            for b in db.query(Bank).filter(Bank.is_deleted == False).all()
        }
        acct_rows = (
            db.query(CustomerBankAccount)
            .filter(
                CustomerBankAccount.customer_id == customer_id,
                CustomerBankAccount.is_deleted == False,
                CustomerBankAccount.is_active == True,
            )
            .all()
        )
        acct_by_id = {a.id: a for a in acct_rows}

        snapshots = (
            db.query(CashBalanceSnapshot)
            .filter(
                CashBalanceSnapshot.customer_id == customer_id,
                CashBalanceSnapshot.is_deleted == False,
            )
            .order_by(desc(CashBalanceSnapshot.as_of_datetime), desc(CashBalanceSnapshot.id))
            .all()
        )

        statements = (
            db.query(BankStatement)
            .filter(
                BankStatement.company_id == customer_id,
                BankStatement.is_deleted == False,
            )
            .order_by(desc(BankStatement.statement_end_date), desc(BankStatement.id))
            .all()
        )

        # Batch fetch currencies for statements
        stmt_ids = [s.id for s in statements]
        stmt_ccy_map: Dict[int, str] = {}
        if stmt_ids:
            txn_ccy_rows = (
                db.query(BankTransaction.statement_id, BankTransaction.currency)
                .filter(BankTransaction.statement_id.in_(stmt_ids))
                .distinct()
                .all()
            )
            for sid, ccy in txn_ccy_rows:
                if sid and ccy and sid not in stmt_ccy_map:
                    stmt_ccy_map[sid] = ccy.upper()

        latest_map: Dict[tuple, Dict[str, Any]] = {}
        covered_stmt_ids = set()

        def _consider_candidate(cand: Dict[str, Any]) -> None:
            key = (cand["bank_id"], cand["_norm_key"], cand["currency"])
            existing = latest_map.get(key)
            if existing is None or cand["_sort_ts"] > existing["_sort_ts"]:
                latest_map[key] = cand

        for snap in snapshots:
            if snap.statement_id:
                covered_stmt_ids.add(snap.statement_id)
            linked_acct = acct_by_id.get(snap.bank_account_id) if snap.bank_account_id else None
            acct_num = snap.account_number or (linked_acct.account_number if linked_acct else None)
            ccy = (snap.currency_code or "EGP").upper()
            norm_key = _normalize_acct_key(acct_num) or f"BANK_{snap.bank_id}_{ccy}"
            as_of = _parse_as_of_datetime(snap.as_of_datetime, snap.created_at)
            created_ts = _parse_as_of_datetime(snap.created_at, as_of)
            _consider_candidate({
                "bank_id": snap.bank_id,
                "bank_name": banks.get(snap.bank_id, f"Bank #{snap.bank_id}"),
                "bank_account_id": snap.bank_account_id,
                "account_name": linked_acct.account_name if linked_acct else None,
                "account_number": acct_num or "Primary Account",
                "currency": ccy,
                "available_balance": float(snap.available_balance or 0),
                "ledger_balance": float(snap.ledger_balance) if snap.ledger_balance is not None else float(snap.available_balance or 0),
                "as_of_datetime": as_of.isoformat(),
                "source_channel": snap.source_channel,
                "continuity_status": snap.continuity_status or "VALIDATED",
                "statement_id": snap.statement_id,
                "inbox_item_id": snap.inbox_item_id,
                "_norm_key": norm_key,
                "_sort_ts": (as_of, created_ts, snap.id),
            })

        for st in statements:
            if st.id in covered_stmt_ids:
                continue
            stmt_ccy = stmt_ccy_map.get(st.id)
            matched_acct = self._match_customer_bank_account(
                db, customer_id, st.bank_id, st.account_number, currency_code=stmt_ccy
            )
            ccy = (
                stmt_ccy
                or (matched_acct.currency_code if matched_acct and matched_acct.currency_code else None)
                or "EGP"
            ).upper()
            acct_num = st.account_number or (matched_acct.account_number if matched_acct else None)
            norm_key = _normalize_acct_key(acct_num) or f"BANK_{st.bank_id}_{ccy}"
            as_of = _parse_as_of_datetime(st.statement_end_date, st.created_at)
            created_ts = _parse_as_of_datetime(st.created_at, as_of)
            _consider_candidate({
                "bank_id": st.bank_id,
                "bank_name": banks.get(st.bank_id, f"Bank #{st.bank_id}"),
                "bank_account_id": matched_acct.id if matched_acct else None,
                "account_name": matched_acct.account_name if matched_acct else None,
                "account_number": acct_num or "Statement Account",
                "currency": ccy,
                "available_balance": float(st.closing_balance or 0),
                "ledger_balance": float(st.closing_balance or 0),
                "as_of_datetime": as_of.isoformat(),
                "source_channel": "BANK_STATEMENT",
                "continuity_status": st.balance_continuity_status or "VALIDATED",
                "statement_id": st.id,
                "inbox_item_id": None,
                "_norm_key": norm_key,
                "_sort_ts": (as_of, created_ts, st.id),
            })

        accounts_out: List[Dict[str, Any]] = []
        totals_by_currency: Dict[str, Dict[str, Any]] = {}
        stale_count = 0
        banks_covered = set()

        for cand in sorted(latest_map.values(), key=lambda x: (x["bank_name"], x["currency"], x["account_number"])):
            as_of_dt = cand["_sort_ts"][0]
            age_hours = max(0.0, (now_utc - as_of_dt).total_seconds() / 3600.0)
            is_stale = age_hours > 24.0
            if is_stale:
                stale_count += 1
            banks_covered.add(cand["bank_id"])

            ccy = cand["currency"]
            tot = totals_by_currency.setdefault(
                ccy,
                {"currency": ccy, "available_balance": 0.0, "ledger_balance": 0.0, "account_count": 0},
            )
            tot["available_balance"] = round(tot["available_balance"] + cand["available_balance"], 2)
            tot["ledger_balance"] = round(tot["ledger_balance"] + cand["ledger_balance"], 2)
            tot["account_count"] += 1

            clean_entry = {k: v for k, v in cand.items() if not k.startswith("_")}
            clean_entry["freshness_hours"] = round(age_hours, 1)
            clean_entry["is_stale"] = is_stale
            accounts_out.append(clean_entry)

        return {
            "as_of": now_utc.isoformat(),
            "accounts": accounts_out,
            "totals_by_currency": totals_by_currency,
            "banks_covered_count": len(banks_covered),
            "total_accounts_count": len(accounts_out),
            "stale_accounts_count": stale_count,
        }


inbox_action_service = InboxActionService()
