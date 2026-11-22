"""
Phase 2 Service: Official Month-End Bank Reconciliation Statement (BRS) Audit Pack
& 1-Click Departmental Exception / Action Packs.

Provides:
1. Formal Two-Way Proof-of-Cash Bridge (Bank Closing <-> Adjusted Cash <-> ERP Book Balance)
   with itemized Deposits in Transit, Unpresented Disbursements, Unbooked Bank Credits/Debits,
   and Paired Ghost Reversal Netting.
2. Period Sign-Off & Locking with Cryptographic SHA-256 WORM Audit Chain stamping (`brs_worm_hash`).
3. Multi-Page A4 Audit Certificate PDF (via WeasyPrint) + Multi-Sheet Excel Audit Workbook (`.xlsx`).
4. Four Departmental Action & Exception Packs:
   - `AR_COLLECTIONS_AND_MYSTERY_CASH` (Sales, Credit Control & AR)
   - `WHT_TAX_CERTIFICATES_REGISTER` (Tax & Statutory Compliance — Form 41 WHT + Bank Stamp/Tax)
   - `AP_STALE_PAYMENTS_AND_BOUNCED_RISK` (AP, Procurement & Risk Control)
   - `BANK_FEES_AND_FLOAT_AUDIT` (Corporate Treasury — Bank Tariff & Value-Date Float Leakage)
"""

import csv
import html
import io
import logging
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, noload

from app.models.models_reconciliation_v2 import (
    BankStatement,
    BankTransaction,
    InternalLedgerRecord,
    ReconciliationMatch,
)
from app.crud.crud_reconciliation_v2 import crud_bank_transaction
from app.crud.crud import log_action

logger = logging.getLogger(__name__)


def _has_table(db: Session, table_name: str) -> bool:
    try:
        return bool(sa_inspect(db.connection()).has_table(table_name))
    except Exception:
        return False


def sanitize_spreadsheet_cell(val: Any) -> Any:
    """Neutralizes spreadsheet formula injection (=, +, @, non-numeric -, leading tab/CR)."""
    if not isinstance(val, str) or not val:
        return val
    stripped = val.lstrip(" ")
    if not stripped:
        return val
    first_ch = stripped[0]
    if first_ch in ("=", "+", "@") or val[0] in ("\t", "\r"):
        return f"'{val}"
    if first_ch == "-":
        num_probe = stripped[1:].replace(",", "").strip()
        try:
            float(num_probe)
            return val
        except ValueError:
            return f"'{val}"
    return val


def _fmt_money(val: Any, ccy: str = "") -> str:
    try:
        num = float(val or 0)
    except Exception:
        num = 0.0
    txt = f"{num:,.2f}"
    return f"{ccy} {txt}".strip() if ccy else txt


def _fmt_dt(dt_val: Any) -> str:
    if not dt_val:
        return ""
    if isinstance(dt_val, (datetime, date)):
        return dt_val.strftime("%Y-%m-%d")
    return str(dt_val)[:10]


def _days_between(d_earlier: Any, d_later: Any) -> int:
    if not d_earlier or not d_later:
        return 0
    d1 = d_earlier.date() if isinstance(d_earlier, datetime) else d_earlier
    d2 = d_later.date() if isinstance(d_later, datetime) else d_later
    try:
        return max(0, (d2 - d1).days)
    except Exception:
        return 0


class ReconciliationBRSAndPacksService:
    """Computes Month-End BRS Audit Certificates and Departmental Action Packs."""

    # ==================================================================================
    # 1. OFFICIAL MONTH-END BANK RECONCILIATION STATEMENT (BRS) ENGINE
    # ==================================================================================

    def build_statement_brs_report(
        self,
        db: Session,
        customer_id: int,
        statement_id: int,
        erp_book_balance_override: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        stmt = (
            db.query(BankStatement)
            .options(noload(BankStatement.bank), noload(BankStatement.currency))
            .filter(
                BankStatement.id == statement_id,
                BankStatement.company_id == customer_id,
                BankStatement.is_deleted == False,
            )
            .first()
        )
        if not stmt:
            raise HTTPException(status_code=404, detail="Bank statement not found")

        raw_txns = (
            db.query(BankTransaction)
            .options(noload("*"))
            .filter(
                BankTransaction.statement_id == stmt.id,
                BankTransaction.is_deleted == False,
            )
            .order_by(BankTransaction.booking_date.asc(), BankTransaction.id.asc())
            .all()
        )
        txns = (
            crud_bank_transaction._resolve_confidential_fields(
                db, raw_txns, customer_id=customer_id
            )
            if _has_table(db, "quotation_tenant_keys")
            else raw_txns
        )

        stmt_ccy = txns[0].currency.upper() if txns and txns[0].currency else "EGP"
        bank_name = f"Bank #{stmt.bank_id}"
        stmt_end = stmt.statement_end_date

        # Resolve optional tables safely (works seamlessly in PostgreSQL & isolated SQLite unit tests)
        company_name = f"Company #{customer_id}"
        signer_email = None
        try:
            from app.models.models import Bank, Currency, Customer, User
            if stmt.bank_id and _has_table(db, "banks"):
                b_obj = db.query(Bank).filter(Bank.id == stmt.bank_id).first()
                if b_obj and getattr(b_obj, "name", None):
                    bank_name = b_obj.name
            if stmt.currency_id and _has_table(db, "currencies"):
                c_obj = db.query(Currency).filter(Currency.id == stmt.currency_id).first()
                if c_obj and getattr(c_obj, "iso_code", None):
                    stmt_ccy = c_obj.iso_code.upper()
            if _has_table(db, "customers"):
                cust = db.query(Customer).filter(Customer.id == customer_id).first()
                if cust and getattr(cust, "name", None):
                    company_name = cust.name
            if stmt.brs_signed_by and _has_table(db, "users"):
                u = db.query(User).filter(User.id == stmt.brs_signed_by).first()
                if u:
                    signer_email = u.email
        except Exception:
            pass

        # Load matches on this statement's transactions
        txn_ids = [t.id for t in txns]
        matches_by_txn: Dict[int, List[ReconciliationMatch]] = {}
        erp_map: Dict[int, InternalLedgerRecord] = {}
        if txn_ids:
            m_rows = (
                db.query(ReconciliationMatch)
                .filter(
                    ReconciliationMatch.bank_txn_id.in_(txn_ids),
                    ReconciliationMatch.is_deleted == False,
                )
                .all()
            )
            erp_ids = [
                m.source_record_id
                for m in m_rows
                if m.source_record_id and (m.source_type or "").upper() != "TREASURY (LG)"
            ]
            if erp_ids:
                for e in (
                    db.query(InternalLedgerRecord)
                    .filter(
                        InternalLedgerRecord.id.in_(erp_ids),
                        InternalLedgerRecord.company_id == customer_id,
                        InternalLedgerRecord.is_deleted == False,
                    )
                    .all()
                ):
                    erp_map[e.id] = e
            for m in m_rows:
                matches_by_txn.setdefault(m.bank_txn_id, []).append(m)

        # Check how many distinct banks have statements in this currency for smart unassigned-ERP scoping
        same_ccy_banks = {
            r[0]
            for r in db.query(BankStatement.bank_id)
            .filter(BankStatement.company_id == customer_id, BankStatement.is_deleted == False)
            .all()
        }
        is_single_bank_tenant = len(same_ccy_banks) <= 1

        # Load OPEN ERP Ledger items as of statement_end_date (Deposits in Transit & Unpresented Payments)
        open_erps = (
            db.query(InternalLedgerRecord)
            .filter(
                InternalLedgerRecord.company_id == customer_id,
                InternalLedgerRecord.status == "OPEN",
                InternalLedgerRecord.is_deleted == False,
            )
            .order_by(InternalLedgerRecord.record_date.asc())
            .all()
        )

        deposits_in_transit: List[Dict[str, Any]] = []
        unpresented_payments: List[Dict[str, Any]] = []
        unassigned_open_erp_info: List[Dict[str, Any]] = []

        stmt_acct_norm = (stmt.account_number or "").strip().upper()
        stmt_end_naive = stmt_end.replace(tzinfo=None) if stmt_end else None

        for e in open_erps:
            e_ccy = (e.currency or "EGP").strip().upper()
            if e_ccy != stmt_ccy:
                continue
            e_dt_naive = e.record_date.replace(tzinfo=None) if e.record_date else None
            if stmt_end_naive and e_dt_naive and e_dt_naive > stmt_end_naive:
                continue

            e_acct_norm = (e.bank_account_number or "").strip().upper()
            belongs_to_stmt = False
            if stmt_acct_norm and e_acct_norm:
                belongs_to_stmt = e_acct_norm == stmt_acct_norm
            elif e.bank_id is not None:
                belongs_to_stmt = e.bank_id == stmt.bank_id
            elif is_single_bank_tenant:
                belongs_to_stmt = True

            e_amt = Decimal(str(e.amount or "0.00"))
            item_dict = {
                "id": e.id,
                "record_date": _fmt_dt(e.record_date),
                "age_days": _days_between(e.record_date, stmt_end or datetime.utcnow()),
                "record_type": e.record_type,
                "reference_number": e.reference_number,
                "entity_name": e.entity_name,
                "amount": float(abs(e_amt)),
                "signed_amount": float(e_amt),
                "currency": e_ccy,
                "gl_account": e.gl_account or "",
            }

            if not belongs_to_stmt:
                unassigned_open_erp_info.append(item_dict)
                continue

            if e_amt > Decimal("0.00"):
                deposits_in_transit.append(item_dict)
            elif e_amt < Decimal("0.00"):
                unpresented_payments.append(item_dict)

        # Partition Bank Statement Lines into:
        # 1) Cleared against ERP (or LG)
        # 2) Paired Same-Day Error Reversals (net 0.00)
        # 3) Unbooked Bank Credits (Interest, Sweeps In, Unidentified Mystery Credits)
        # 4) Unbooked Bank Debits (Bank Charges, Stamp/WHT Taxes, Sweeps Out, Bounced Cheques, Unrecorded Wires)
        cleared_bank_items: List[Dict[str, Any]] = []
        paired_reversals: List[Dict[str, Any]] = []
        unbooked_bank_credits: List[Dict[str, Any]] = []
        unbooked_bank_debits: List[Dict[str, Any]] = []

        # Net fee/WHT variance on matched items not yet posted to GL
        # (On AR Credit: bank received 99,000 vs 100,000 invoice -> 1,000 deduction lowered bank cash relative to gross AR;
        #  On AP Debit: bank debited 50,150 vs 50,000 bill -> 150 wire fee lowered bank cash relative to net AP bill)
        total_match_deductions = Decimal("0.00")

        for t in txns:
            cr = Decimal(str(t.credit_amount or "0.00"))
            dr = Decimal(str(t.debit_amount or "0.00"))
            is_cr = cr > Decimal("0.00")
            abs_amt = cr if is_cr else dr

            if t.is_reversal and t.linked_txn_id:
                paired_reversals.append({
                    "id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "description": t.raw_description,
                    "debit": float(dr),
                    "credit": float(cr),
                    "linked_txn_id": t.linked_txn_id,
                })
                continue

            t_matches = matches_by_txn.get(t.id) or []
            if t_matches:
                erp_refs = []
                erp_gross_total = Decimal("0.00")
                has_erp_rec = False
                for m in t_matches:
                    if m.source_record_id in erp_map:
                        has_erp_rec = True
                        eo = erp_map[m.source_record_id]
                        erp_refs.append(eo.reference_number)
                        erp_gross_total += abs(Decimal(str(eo.amount or "0.00")))
                    elif (m.source_type or "").upper() == "TREASURY (LG)":
                        erp_refs.append(f"LG#{m.source_record_id}")

                var_amt = Decimal(str(t.variance_amount or "0.00"))
                if has_erp_rec and erp_gross_total > Decimal("0.00") and not t.is_exported:
                    # Signed cash impact of variance = signed_bank - signed_erp
                    signed_bank = cr - dr
                    signed_erp = erp_gross_total if is_cr else -erp_gross_total
                    # Usually negative (bank fee / WHT reduced cash relative to invoice/bill)
                    total_match_deductions += (signed_erp - signed_bank)

                cleared_bank_items.append({
                    "id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "value_date": _fmt_dt(t.value_date),
                    "description": t.raw_description,
                    "counterparty": t.counterparty_name or "",
                    "debit": float(dr),
                    "credit": float(cr),
                    "matched_refs": ", ".join(erp_refs),
                    "variance_amount": float(var_amt),
                    "is_exported": bool(t.is_exported),
                })
            else:
                entry = {
                    "id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "value_date": _fmt_dt(t.value_date),
                    "age_days": _days_between(t.booking_date, stmt_end or datetime.utcnow()),
                    "description": t.raw_description,
                    "counterparty": t.counterparty_name or "",
                    "category": t.classification_category or "UNCLASSIFIED",
                    "gl_account": t.internal_category or "1010950 - Unapplied Suspense",
                    "amount": float(abs_amt),
                    "is_classified": bool(t.is_classified),
                    "is_exported": bool(t.is_exported),
                }
                if is_cr:
                    unbooked_bank_credits.append(entry)
                elif dr > Decimal("0.00"):
                    unbooked_bank_debits.append(entry)

        # ── Formal Two-Way BRS Mathematical Bridge ──
        bank_closing = Decimal(str(stmt.closing_balance or "0.00"))
        bank_opening = Decimal(str(stmt.opening_balance or "0.00"))
        sum_dit = sum(Decimal(str(x["amount"])) for x in deposits_in_transit)
        sum_up = sum(Decimal(str(x["amount"])) for x in unpresented_payments)

        # Adjusted True Cash Position = Bank Closing + Deposits in Transit - Unpresented Payments
        adjusted_cash_balance = bank_closing + sum_dit - sum_up

        # Unbooked Bank Items (only those NOT yet exported to ERP GL)
        unexported_cr = sum(
            Decimal(str(x["amount"])) for x in unbooked_bank_credits if not x["is_exported"]
        )
        unexported_dr = sum(
            Decimal(str(x["amount"])) for x in unbooked_bank_debits if not x["is_exported"]
        )
        all_unbooked_cr = sum(Decimal(str(x["amount"])) for x in unbooked_bank_credits)
        all_unbooked_dr = sum(Decimal(str(x["amount"])) for x in unbooked_bank_debits)

        # Implied Pre-Export ERP Book Balance:
        # Book_Balance + Unbooked_Credits - Unbooked_Debits - Match_Fee_Deductions == Adjusted_Cash_Balance
        # => Implied_Book_Balance = Adjusted_Cash_Balance - Unbooked_Credits + Unbooked_Debits + Match_Fee_Deductions
        implied_erp_book_balance = (
            adjusted_cash_balance - unexported_cr + unexported_dr + total_match_deductions
        )

        effective_book_override = (
            erp_book_balance_override
            if erp_book_balance_override is not None
            else stmt.erp_book_closing_balance
        )
        has_explicit_book = effective_book_override is not None
        reported_erp_book = (
            Decimal(str(effective_book_override))
            if has_explicit_book
            else implied_erp_book_balance
        )

        # Book-side adjusted cash = Reported ERP Book + Unbooked Credits - Unbooked Debits - Match Deductions
        book_adjusted_cash = (
            reported_erp_book + unexported_cr - unexported_dr - total_match_deductions
        )
        unexplained_variance = adjusted_cash_balance - book_adjusted_cash

        total_txns = len(txns)
        reconciled_cnt = sum(1 for t in txns if t.is_reconciled)
        classified_cnt = sum(1 for t in txns if t.is_classified)

        return {
            "statement": {
                "id": stmt.id,
                "company_id": customer_id,
                "company_name": company_name,
                "bank_id": stmt.bank_id,
                "bank_name": bank_name,
                "account_number": stmt.account_number or "—",
                "currency": stmt_ccy,
                "file_name": stmt.file_name,
                "period_start": _fmt_dt(stmt.statement_start_date),
                "period_end": _fmt_dt(stmt.statement_end_date),
                "opening_balance": float(bank_opening),
                "closing_balance": float(bank_closing),
                "continuity_status": stmt.balance_continuity_status or "VALIDATED",
                "continuity_variance": float(stmt.balance_variance_amount or 0),
                "status": stmt.status,
                "is_signed_off": bool(stmt.brs_signed_at or (stmt.status or "").upper() == "RECONCILED"),
                "brs_signed_by_email": signer_email,
                "brs_signed_at": stmt.brs_signed_at.isoformat() if stmt.brs_signed_at else None,
                "brs_signoff_notes": stmt.brs_signoff_notes or "",
                "brs_worm_hash": stmt.brs_worm_hash or "",
                "total_transactions": total_txns,
                "reconciled_count": reconciled_cnt,
                "classified_count": classified_cnt,
            },
            "bridge": {
                "bank_closing_balance": float(bank_closing),
                "plus_deposits_in_transit": float(sum_dit),
                "deposits_in_transit_count": len(deposits_in_transit),
                "less_unpresented_payments": float(sum_up),
                "unpresented_payments_count": len(unpresented_payments),
                "adjusted_cash_balance": float(adjusted_cash_balance),
                "has_explicit_erp_book_balance": has_explicit_book,
                "erp_book_closing_balance": float(reported_erp_book),
                "implied_erp_book_balance": float(implied_erp_book_balance),
                "plus_unbooked_bank_credits": float(unexported_cr),
                "all_unbooked_bank_credits": float(all_unbooked_cr),
                "unbooked_bank_credits_count": len(unbooked_bank_credits),
                "less_unbooked_bank_debits": float(unexported_dr),
                "all_unbooked_bank_debits": float(all_unbooked_dr),
                "unbooked_bank_debits_count": len(unbooked_bank_debits),
                "less_unposted_match_variances": float(total_match_deductions),
                "book_side_adjusted_cash": float(book_adjusted_cash),
                "unexplained_variance": float(round(unexplained_variance, 2)),
                "is_proof_of_cash_balanced": abs(unexplained_variance) < Decimal("0.01"),
                "paired_reversals_netted_count": len(paired_reversals),
            },
            "schedules": {
                "deposits_in_transit": deposits_in_transit,
                "unpresented_payments": unpresented_payments,
                "unbooked_bank_credits": unbooked_bank_credits,
                "unbooked_bank_debits": unbooked_bank_debits,
                "cleared_matches": cleared_bank_items,
                "paired_reversals": paired_reversals,
                "unassigned_open_erp_info": unassigned_open_erp_info,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def sign_off_statement_brs(
        self,
        db: Session,
        customer_id: int,
        statement_id: int,
        user_id: int,
        erp_book_closing_balance: Optional[Decimal] = None,
        notes: Optional[str] = None,
        lock_transactions: bool = True,
    ) -> Dict[str, Any]:
        stmt = (
            db.query(BankStatement)
            .options(noload(BankStatement.bank), noload(BankStatement.currency))
            .filter(
                BankStatement.id == statement_id,
                BankStatement.company_id == customer_id,
                BankStatement.is_deleted == False,
            )
            .first()
        )
        if not stmt:
            raise HTTPException(status_code=404, detail="Bank statement not found")

        # Compute BRS snapshot metrics first for cryptographic WORM audit seal
        quantized_book = (
            Decimal(str(erp_book_closing_balance)).quantize(Decimal("0.01"))
            if erp_book_closing_balance is not None
            else None
        )
        brs_preview = self.build_statement_brs_report(
            db,
            customer_id=customer_id,
            statement_id=stmt.id,
            erp_book_balance_override=quantized_book,
        )
        bridge = brs_preview["bridge"]
        now_utc = datetime.now(timezone.utc)

        worm_hash = None
        if _has_table(db, "audit_logs"):
            try:
                log_action(
                    db=db,
                    user_id=user_id,
                    action_type="BANK_STATEMENT_BRS_SIGNED_OFF",
                    entity_type="BankStatement",
                    entity_id=stmt.id,
                    details={
                        "file_name": stmt.file_name,
                        "account_number": stmt.account_number,
                        "bank_closing_balance": bridge["bank_closing_balance"],
                        "adjusted_cash_balance": bridge["adjusted_cash_balance"],
                        "erp_book_closing_balance": bridge["erp_book_closing_balance"],
                        "unexplained_variance": bridge["unexplained_variance"],
                        "lock_transactions": lock_transactions,
                    },
                    customer_id=customer_id,
                )
                from app.models.models import AuditLog
                latest_log = (
                    db.query(AuditLog)
                    .filter(
                        AuditLog.customer_id == customer_id,
                        AuditLog.entity_type == "BankStatement",
                        AuditLog.entity_id == stmt.id,
                    )
                    .order_by(AuditLog.id.desc())
                    .first()
                )
                if latest_log and latest_log.entry_hash:
                    worm_hash = latest_log.entry_hash
            except Exception as audit_err:
                logger.debug(f"WORM hash fallback in sign_off_statement_brs: {audit_err}")

        if not worm_hash:
            import hashlib
            raw_sig = f"{customer_id}:{stmt.id}:{now_utc.isoformat()}:{bridge['adjusted_cash_balance']}"
            worm_hash = hashlib.sha256(raw_sig.encode("utf-8")).hexdigest().upper()

        if quantized_book is not None:
            stmt.erp_book_closing_balance = quantized_book
        stmt.status = "RECONCILED"
        stmt.brs_signed_by = user_id
        stmt.brs_signed_at = now_utc
        if notes is not None:
            stmt.brs_signoff_notes = str(notes).strip()
        stmt.brs_worm_hash = worm_hash

        if lock_transactions:
            db.query(BankTransaction).filter(
                BankTransaction.statement_id == stmt.id,
                BankTransaction.is_deleted == False,
            ).update({"is_locked": True}, synchronize_session=False)

        db.commit()
        return self.build_statement_brs_report(db, customer_id=customer_id, statement_id=stmt.id)

    def reopen_statement_brs(
        self,
        db: Session,
        customer_id: int,
        statement_id: int,
        user_id: int,
    ) -> Dict[str, Any]:
        stmt = (
            db.query(BankStatement)
            .options(noload(BankStatement.bank), noload(BankStatement.currency))
            .filter(
                BankStatement.id == statement_id,
                BankStatement.company_id == customer_id,
                BankStatement.is_deleted == False,
            )
            .first()
        )
        if not stmt:
            raise HTTPException(status_code=404, detail="Bank statement not found")

        stmt.status = stmt.balance_continuity_status or "VALIDATED"
        stmt.brs_signed_by = None
        stmt.brs_signed_at = None
        stmt.brs_worm_hash = None

        # Unlock non-exported transactions so adjustments can be made
        db.query(BankTransaction).filter(
            BankTransaction.statement_id == stmt.id,
            BankTransaction.is_exported != True,
            BankTransaction.is_deleted == False,
        ).update({"is_locked": False}, synchronize_session=False)

        if _has_table(db, "audit_logs"):
            try:
                log_action(
                    db=db,
                    user_id=user_id,
                    action_type="BANK_STATEMENT_BRS_REOPENED",
                    entity_type="BankStatement",
                    entity_id=stmt.id,
                    details={"file_name": stmt.file_name},
                    customer_id=customer_id,
                )
            except Exception:
                pass

        db.commit()
        return self.build_statement_brs_report(db, customer_id=customer_id, statement_id=stmt.id)

    def render_brs_certificate_html(self, brs: Dict[str, Any]) -> str:
        s = brs["statement"]
        b = brs["bridge"]
        sched = brs["schedules"]
        ccy = html.escape(s["currency"])

        status_badge = (
            f'<span style="background:#dcfce7;color:#166534;padding:3px 9px;border-radius:12px;font-weight:700;font-size:8.5pt;">'
            f'SIGNED &amp; WORM SEALED</span>'
            if s["is_signed_off"]
            else '<span style="background:#fef3c7;color:#92400e;padding:3px 9px;border-radius:12px;font-weight:700;font-size:8.5pt;">DRAFT / WORKING BRS</span>'
        )
        proof_badge = (
            '<span style="color:#15803d;font-weight:800;">RECONCILED (0.00 UNEXPLAINED VARIANCE)</span>'
            if b["is_proof_of_cash_balanced"]
            else f'<span style="color:#b91c1c;font-weight:800;">UNEXPLAINED VARIANCE: {_fmt_money(b["unexplained_variance"], ccy)}</span>'
        )

        def _render_rows(items: List[Dict[str, Any]], cols: List[Tuple[str, str, bool]], empty_msg: str) -> str:
            if not items:
                return f'<tr><td colspan="{len(cols)}" style="text-align:center;color:#94a3b8;font-style:italic;padding:8px;">{html.escape(empty_msg)}</td></tr>'
            out = []
            for it in items[:150]:
                tds = []
                for key, _, is_num in cols:
                    raw = it.get(key, "")
                    txt = _fmt_money(raw) if is_num else html.escape(str(raw or "—"))
                    align = "right" if is_num else "left"
                    mono = "font-family:monospace;font-weight:600;" if is_num else ""
                    tds.append(f'<td style="text-align:{align};{mono}">{txt}</td>')
                out.append(f"<tr>{''.join(tds)}</tr>")
            return "\n".join(out)

        dit_rows = _render_rows(
            sched["deposits_in_transit"],
            [("record_date", "Date", False), ("reference_number", "ERP Ref", False), ("entity_name", "Counterparty", False), ("age_days", "Age (d)", False), ("amount", f"Amount ({ccy})", True)],
            "No deposits in transit as of statement closing date.",
        )
        up_rows = _render_rows(
            sched["unpresented_payments"],
            [("record_date", "Date", False), ("reference_number", "ERP Ref", False), ("entity_name", "Beneficiary", False), ("age_days", "Age (d)", False), ("amount", f"Amount ({ccy})", True)],
            "No unpresented cheques or uncleared outward wires.",
        )
        ub_cr_rows = _render_rows(
            sched["unbooked_bank_credits"],
            [("booking_date", "Date", False), ("category", "Category", False), ("description", "Narrative", False), ("gl_account", "Target GL", False), ("amount", f"Amount ({ccy})", True)],
            "No unbooked bank credits.",
        )
        ub_dr_rows = _render_rows(
            sched["unbooked_bank_debits"],
            [("booking_date", "Date", False), ("category", "Category", False), ("description", "Narrative", False), ("gl_account", "Target GL", False), ("amount", f"Amount ({ccy})", True)],
            "No unbooked bank debits or fees.",
        )

        worm_line = (
            f'<div style="margin-top:8px;padding:7px 10px;background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;font-size:7.8pt;color:#334155;">'
            f'<strong>WORM Cryptographic Audit Hash (SHA-256):</strong> <span style="font-family:monospace;">{html.escape(s["brs_worm_hash"])}</span>'
            f' &nbsp;|&nbsp; <strong>Signed By:</strong> {html.escape(s["brs_signed_by_email"] or "Authorized User")} on {html.escape(_fmt_dt(s["brs_signed_at"]))}'
            f'</div>'
            if s["brs_worm_hash"]
            else ""
        )

        notes_block = (
            f'<div style="margin-top:8px;font-size:8.3pt;color:#334155;"><strong>Sign-Off Notes:</strong> {html.escape(s["brs_signoff_notes"])}</div>'
            if s["brs_signoff_notes"]
            else ""
        )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Bank Reconciliation Statement — {html.escape(s["bank_name"])}</title>
<style>
  @page {{
    size: A4;
    margin: 13mm 14mm 16mm 14mm;
    @bottom-left {{
      content: "Grow Corporate Treasury · Official Bank Reconciliation Audit Pack";
      font-size: 7.5pt;
      color: #64748b;
    }}
    @bottom-right {{
      content: "Page " counter(page) " of " counter(pages);
      font-size: 7.5pt;
      color: #64748b;
    }}
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    font-size: 8.6pt;
    color: #0f172a;
    line-height: 1.38;
    margin: 0;
  }}
  .masthead {{
    background: #0f172a;
    color: #ffffff;
    padding: 13px 16px;
    border-radius: 8px;
    margin-bottom: 12px;
  }}
  .masthead h1 {{ margin: 0; font-size: 13pt; letter-spacing: -0.2px; }}
  .masthead .sub {{ color: #94a3b8; font-size: 8.2pt; margin-top: 3px; }}
  .meta-grid {{
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 12px;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
  }}
  .meta-grid td {{ padding: 6px 10px; border: 1px solid #e2e8f0; font-size: 8.3pt; }}
  .bridge-table {{ width: 100%; border-collapse: collapse; margin-bottom: 14px; }}
  .bridge-table th {{
    background: #eff6ff;
    color: #1e3a8a;
    text-align: left;
    padding: 6px 10px;
    border: 1px solid #bfdbfe;
    font-size: 8.5pt;
  }}
  .bridge-table td {{ padding: 5.5px 10px; border: 1px solid #e2e8f0; }}
  .bridge-total {{ background: #f0fdf4; font-weight: 800; color: #14532d; }}
  h3 {{
    font-size: 9.5pt;
    margin: 12px 0 5px 0;
    color: #0f172a;
    border-bottom: 1.5px solid #cbd5e1;
    padding-bottom: 3px;
    page-break-after: avoid;
  }}
  .sched {{ width: 100%; border-collapse: collapse; margin-bottom: 10px; font-size: 8pt; }}
  .sched th {{ background: #f1f5f9; color: #334155; padding: 5px 7px; border: 1px solid #cbd5e1; text-align: left; }}
  .sched td {{ padding: 4.5px 7px; border: 1px solid #e2e8f0; }}
  .sig-grid {{ width: 100%; margin-top: 18px; border-collapse: collapse; page-break-inside: avoid; }}
  .sig-grid td {{ width: 33%; padding: 24px 10px 6px 10px; border-top: 1px solid #94a3b8; text-align: center; font-size: 8pt; color: #475569; }}
</style>
</head>
<body>
  <div class="masthead">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div>
        <h1>Official Bank Reconciliation Statement (Proof of Cash)</h1>
        <div class="sub">{html.escape(s["company_name"])} &nbsp;·&nbsp; {html.escape(s["bank_name"])} &nbsp;·&nbsp; Account: {html.escape(s["account_number"])}</div>
      </div>
      <div>{status_badge}</div>
    </div>
  </div>

  <table class="meta-grid">
    <tr>
      <td><strong>Statement Period:</strong> {html.escape(s["period_start"])} to {html.escape(s["period_end"])}</td>
      <td><strong>Currency:</strong> {ccy}</td>
      <td><strong>Statement Continuity:</strong> {html.escape(s["continuity_status"])}</td>
    </tr>
    <tr>
      <td><strong>Opening Bank Balance:</strong> {_fmt_money(s["opening_balance"], ccy)}</td>
      <td><strong>Closing Bank Balance:</strong> {_fmt_money(s["closing_balance"], ccy)}</td>
      <td><strong>Proof Status:</strong> {proof_badge}</td>
    </tr>
  </table>

  <table class="bridge-table">
    <thead>
      <tr>
        <th style="width:72%;">Section A — Bank Statement to Adjusted True Cash Bridge</th>
        <th style="width:28%;text-align:right;">Amount ({ccy})</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>1. Closing Balance per Bank Statement</strong> ({html.escape(s["file_name"])})</td>
        <td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(b["bank_closing_balance"])}</td>
      </tr>
      <tr>
        <td>(+) Add: Deposits in Transit ({b["deposits_in_transit_count"]} open ERP receipt item(s) not yet credited by bank)</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(b["plus_deposits_in_transit"])}</td>
      </tr>
      <tr>
        <td>(-) Less: Unpresented Cheques &amp; Uncleared Outward Wires ({b["unpresented_payments_count"]} open ERP disbursement(s))</td>
        <td style="text-align:right;font-family:monospace;">({_fmt_money(b["less_unpresented_payments"])})</td>
      </tr>
      <tr class="bridge-total">
        <td>= Adjusted True Cash Position (Per Bank Side)</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(b["adjusted_cash_balance"], ccy)}</td>
      </tr>
    </tbody>
  </table>

  <table class="bridge-table">
    <thead>
      <tr>
        <th style="width:72%;">Section B — ERP General Ledger Book to Adjusted Cash Bridge</th>
        <th style="width:28%;text-align:right;">Amount ({ccy})</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>1. Closing Cash Balance per ERP Books</strong> ({'Reported Trial Balance' if b['has_explicit_erp_book_balance'] else 'Implied Pre-Posting Book Balance'})</td>
        <td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(b["erp_book_closing_balance"])}</td>
      </tr>
      <tr>
        <td>(+) Add: Unexported Bank Credits ({b["unbooked_bank_credits_count"]} interest / unidentified credit item(s) on statement)</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(b["plus_unbooked_bank_credits"])}</td>
      </tr>
      <tr>
        <td>(-) Less: Unexported Bank Debits ({b["unbooked_bank_debits_count"]} bank fee, tax &amp; direct debit item(s) on statement)</td>
        <td style="text-align:right;font-family:monospace;">({_fmt_money(b["less_unbooked_bank_debits"])})</td>
      </tr>
      <tr>
        <td>(-) Less: Unexported Match Fee / WHT Deductions on Cleared Invoices</td>
        <td style="text-align:right;font-family:monospace;">({_fmt_money(b["less_unposted_match_variances"])})</td>
      </tr>
      <tr class="bridge-total">
        <td>= Adjusted Cash Position (Per ERP Book Side) &nbsp;|&nbsp; Unexplained Variance: {_fmt_money(b["unexplained_variance"], ccy)}</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(b["book_side_adjusted_cash"], ccy)}</td>
      </tr>
    </tbody>
  </table>

  {notes_block}
  {worm_line}

  <h3>Schedule 1 — Deposits in Transit ({len(sched["deposits_in_transit"])})</h3>
  <table class="sched">
    <thead><tr><th>Date</th><th>ERP Reference</th><th>Counterparty</th><th>Age (d)</th><th style="text-align:right;">Amount ({ccy})</th></tr></thead>
    <tbody>{dit_rows}</tbody>
  </table>

  <h3>Schedule 2 — Unpresented Cheques &amp; Uncleared Outward Wires ({len(sched["unpresented_payments"])})</h3>
  <table class="sched">
    <thead><tr><th>Date</th><th>ERP Reference</th><th>Beneficiary</th><th>Age (d)</th><th style="text-align:right;">Amount ({ccy})</th></tr></thead>
    <tbody>{up_rows}</tbody>
  </table>

  <h3>Schedule 3 — Unbooked Bank Statement Credits ({len(sched["unbooked_bank_credits"])})</h3>
  <table class="sched">
    <thead><tr><th>Date</th><th>Category</th><th>Narrative</th><th>Target GL</th><th style="text-align:right;">Amount ({ccy})</th></tr></thead>
    <tbody>{ub_cr_rows}</tbody>
  </table>

  <h3>Schedule 4 — Unbooked Bank Statement Debits, Fees &amp; Taxes ({len(sched["unbooked_bank_debits"])})</h3>
  <table class="sched">
    <thead><tr><th>Date</th><th>Category</th><th>Narrative</th><th>Target GL</th><th style="text-align:right;">Amount ({ccy})</th></tr></thead>
    <tbody>{ub_dr_rows}</tbody>
  </table>

  <table class="sig-grid">
    <tr>
      <td>Prepared By (Treasury / GL Maker)</td>
      <td>Reviewed By (Chief Accountant)</td>
      <td>Approved By (CFO / Corporate Admin)</td>
    </tr>
  </table>
</body>
</html>"""

    def render_brs_excel_bytes(self, brs: Dict[str, Any]) -> bytes:
        import pandas as pd

        s = brs["statement"]
        b = brs["bridge"]
        sched = brs["schedules"]
        ccy = s["currency"]

        summary_rows = [
            {"Section": "Header", "Line Item": "Company", "Count": "", "Amount": s["company_name"], "Currency": ccy},
            {"Section": "Header", "Line Item": "Bank & Account", "Count": "", "Amount": f"{s['bank_name']} ({s['account_number']})", "Currency": ccy},
            {"Section": "Header", "Line Item": "Period", "Count": "", "Amount": f"{s['period_start']} to {s['period_end']}", "Currency": ccy},
            {"Section": "Header", "Line Item": "Sign-Off Status", "Count": "", "Amount": "SIGNED & LOCKED" if s["is_signed_off"] else "DRAFT", "Currency": ccy},
            {"Section": "Header", "Line Item": "WORM Audit Hash", "Count": "", "Amount": s["brs_worm_hash"] or "Pending Sign-Off", "Currency": ccy},
            {"Section": "Bank Side", "Line Item": "Closing Balance per Bank Statement", "Count": s["total_transactions"], "Amount": b["bank_closing_balance"], "Currency": ccy},
            {"Section": "Bank Side", "Line Item": "(+) Deposits in Transit", "Count": b["deposits_in_transit_count"], "Amount": b["plus_deposits_in_transit"], "Currency": ccy},
            {"Section": "Bank Side", "Line Item": "(-) Unpresented Cheques & Wires", "Count": b["unpresented_payments_count"], "Amount": -b["less_unpresented_payments"], "Currency": ccy},
            {"Section": "Bank Side", "Line Item": "= Adjusted True Cash Balance", "Count": "", "Amount": b["adjusted_cash_balance"], "Currency": ccy},
            {"Section": "Book Side", "Line Item": "Closing Cash Balance per ERP Books", "Count": "", "Amount": b["erp_book_closing_balance"], "Currency": ccy},
            {"Section": "Book Side", "Line Item": "(+) Unexported Bank Credits", "Count": b["unbooked_bank_credits_count"], "Amount": b["plus_unbooked_bank_credits"], "Currency": ccy},
            {"Section": "Book Side", "Line Item": "(-) Unexported Bank Debits & Fees", "Count": b["unbooked_bank_debits_count"], "Amount": -b["less_unbooked_bank_debits"], "Currency": ccy},
            {"Section": "Book Side", "Line Item": "(-) Unexported Match Variances", "Count": "", "Amount": -b["less_unposted_match_variances"], "Currency": ccy},
            {"Section": "Book Side", "Line Item": "= Adjusted Cash (Book Side)", "Count": "", "Amount": b["book_side_adjusted_cash"], "Currency": ccy},
            {"Section": "Proof", "Line Item": "Unexplained Proof-of-Cash Variance", "Count": "", "Amount": b["unexplained_variance"], "Currency": ccy},
        ]

        def _safe_df(rows_list: List[Dict[str, Any]]) -> Any:
            if not rows_list:
                return pd.DataFrame([{"Info": "No items in this schedule"}])
            cleaned = [{k: sanitize_spreadsheet_cell(v) for k, v in r.items()} for r in rows_list]
            return pd.DataFrame(cleaned)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            _safe_df(summary_rows).to_excel(writer, index=False, sheet_name="1_BRS_Proof_of_Cash")
            _safe_df(sched["deposits_in_transit"]).to_excel(writer, index=False, sheet_name="2_Deposits_In_Transit")
            _safe_df(sched["unpresented_payments"]).to_excel(writer, index=False, sheet_name="3_Unpresented_Payments")
            _safe_df(sched["unbooked_bank_credits"] + sched["unbooked_bank_debits"]).to_excel(writer, index=False, sheet_name="4_Unbooked_Bank_Lines")
            _safe_df(sched["cleared_matches"]).to_excel(writer, index=False, sheet_name="5_Cleared_ERP_Matches")
        buf.seek(0)
        return buf.getvalue()

    # ==================================================================================
    # 2. DEPARTMENTAL EXCEPTION & ACTION PACKS ENGINE (4 OPERATIONAL PACKS)
    # ==================================================================================

    PACK_DEFINITIONS = {
        "AR_COLLECTIONS_AND_MYSTERY_CASH": {
            "title": "Sales & AR: Verified Collections + Mystery Cash",
            "audience": "Sales, Credit Control & Accounts Receivable",
            "description": "Morning customer credit-release feed of verified collections plus unidentified incoming deposits awaiting customer claim.",
        },
        "WHT_TAX_CERTIFICATES_REGISTER": {
            "title": "Tax Team: WHT (Form 41) & Bank Tax Deductions",
            "audience": "Tax & Statutory Compliance",
            "description": "Withholding tax deducted at source on customer collections (requiring tax certificates) plus direct bank tax & stamp duty debits.",
        },
        "AP_STALE_PAYMENTS_AND_BOUNCED_RISK": {
            "title": "AP & Risk: Bounced Cheques & Stale Uncleared Payments",
            "audience": "Accounts Payable, Procurement & Risk Control",
            "description": "Immediate alerts on reversed/bounced collections, aged uncleared ERP payments, and unbooked bank outflows.",
        },
        "BANK_FEES_AND_FLOAT_AUDIT": {
            "title": "Treasury: Bank Fee Leakage & Clearing Float Audit",
            "audience": "Corporate Treasurer & CFO",
            "description": "Itemized audit of deducted bank charges, sweep fees, wire variances, and value-date clearing float delays by bank.",
        },
    }

    def build_action_packs(
        self,
        db: Session,
        customer_id: int,
        selected_pack: Optional[str] = None,
        bank_id: Optional[int] = None,
        statement_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        banks_map = {}
        if _has_table(db, "banks"):
            try:
                from app.models.models import Bank
                banks_map = {b.id: (b.short_name or b.name) for b in db.query(Bank).all()}
            except Exception:
                pass

        stmt_q = (
            db.query(BankStatement)
            .options(noload(BankStatement.bank), noload(BankStatement.currency))
            .filter(
                BankStatement.company_id == customer_id,
                BankStatement.is_deleted == False,
            )
        )
        if bank_id:
            stmt_q = stmt_q.filter(BankStatement.bank_id == bank_id)
        if statement_id:
            stmt_q = stmt_q.filter(BankStatement.id == statement_id)

        stmts_by_id = {s.id: s for s in stmt_q.all()}
        if stmts_by_id:
            txns = (
                db.query(BankTransaction)
                .options(noload("*"))
                .filter(
                    BankTransaction.statement_id.in_(list(stmts_by_id.keys())),
                    BankTransaction.is_deleted == False,
                )
                .order_by(BankTransaction.booking_date.desc(), BankTransaction.id.desc())
                .limit(3000)
                .all()
            )
            if _has_table(db, "quotation_tenant_keys"):
                txns = crud_bank_transaction._resolve_confidential_fields(db, txns, customer_id=customer_id)
        else:
            txns = []

        txn_ids = [t.id for t in txns]
        matches_by_txn: Dict[int, List[ReconciliationMatch]] = {}
        erp_map: Dict[int, InternalLedgerRecord] = {}
        if txn_ids:
            m_rows = (
                db.query(ReconciliationMatch)
                .filter(
                    ReconciliationMatch.bank_txn_id.in_(txn_ids),
                    ReconciliationMatch.is_deleted == False,
                )
                .all()
            )
            erp_ids = [
                m.source_record_id
                for m in m_rows
                if m.source_record_id and (m.source_type or "").upper() != "TREASURY (LG)"
            ]
            if erp_ids:
                for e in (
                    db.query(InternalLedgerRecord)
                    .filter(
                        InternalLedgerRecord.id.in_(erp_ids),
                        InternalLedgerRecord.company_id == customer_id,
                        InternalLedgerRecord.is_deleted == False,
                    )
                    .all()
                ):
                    erp_map[e.id] = e
            for m in m_rows:
                matches_by_txn.setdefault(m.bank_txn_id, []).append(m)

        open_ap_q = db.query(InternalLedgerRecord).filter(
            InternalLedgerRecord.company_id == customer_id,
            InternalLedgerRecord.status == "OPEN",
            InternalLedgerRecord.amount < 0,
            InternalLedgerRecord.is_deleted == False,
        )
        if bank_id:
            open_ap_q = open_ap_q.filter(InternalLedgerRecord.bank_id == bank_id)
        open_ap_erps = open_ap_q.order_by(InternalLedgerRecord.record_date.asc()).limit(1000).all()

        now_dt = datetime.utcnow()

        # ── Build Pack 1: Sales & AR (Verified Collections + Mystery Cash) ──
        verified_collections: List[Dict[str, Any]] = []
        mystery_cash: List[Dict[str, Any]] = []

        # ── Build Pack 2: WHT Certificates & Direct Bank Tax Deductions ──
        wht_rows: List[Dict[str, Any]] = []

        # ── Build Pack 3: Bounced Cheques, Aged Uncleared AP & Unbooked Outflows ──
        bounced_and_ap_rows: List[Dict[str, Any]] = []

        # ── Build Pack 4: Bank Fees & Float Audit ──
        fee_and_float_rows: List[Dict[str, Any]] = []

        for t in txns:
            if t.is_reversal and t.linked_txn_id:
                continue

            st_obj = stmts_by_id.get(t.statement_id)
            b_name = banks_map.get(st_obj.bank_id, f"Bank #{st_obj.bank_id}") if st_obj else "Bank"
            acct_no = t.account_number or (st_obj.account_number if st_obj else "") or ""
            cr = Decimal(str(t.credit_amount or "0.00"))
            dr = Decimal(str(t.debit_amount or "0.00"))
            is_cr = cr > Decimal("0.00")
            ccy = (t.currency or "EGP").upper()
            cat_up = (t.classification_category or "").upper()
            int_up = (t.internal_category or "").upper()
            is_sweep = cat_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER") or int_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER") or bool(t.linked_txn_id)
            t_matches = matches_by_txn.get(t.id) or []
            erp_pairs = [(m, erp_map[m.source_record_id]) for m in t_matches if m.source_record_id in erp_map]
            var_amt = Decimal(str(t.variance_amount or "0.00"))

            # Float lag check (Value Date later than Booking Date on inflows, or earlier on outflows)
            float_lag_days = 0
            if t.booking_date and t.value_date:
                float_lag_days = abs((t.value_date.date() - t.booking_date.date()).days)

            # 1) AR Verified Collections vs Mystery Credits
            if is_cr and not is_sweep:
                if erp_pairs or cat_up == "COLLECTION_FROM_CUSTOMER":
                    refs = ", ".join(e.reference_number for _, e in erp_pairs) or (t.e2e_id or "")
                    cust_name = (erp_pairs[0][1].entity_name if erp_pairs else None) or t.counterparty_name or "Verified Customer"
                    gross_amt = sum(abs(Decimal(str(e.amount or 0))) for _, e in erp_pairs) if erp_pairs else cr
                    verified_collections.append({
                        "Section": "VERIFIED_COLLECTION",
                        "Date": _fmt_dt(t.booking_date),
                        "Value_Date": _fmt_dt(t.value_date),
                        "Bank": b_name,
                        "Account": acct_no,
                        "Customer": cust_name,
                        "Matched_Invoice_Ref": refs,
                        "Gross_Invoice_Amount": float(gross_amt),
                        "Net_Cash_Received": float(cr),
                        "Deducted_Fee_or_WHT": float(var_amt),
                        "Currency": ccy,
                        "Action_Required": "Release Credit Limit / Order",
                    })
                elif cat_up not in ("BANK_INTEREST",):
                    mystery_cash.append({
                        "Section": "UNIDENTIFIED_MYSTERY_CREDIT",
                        "Date": _fmt_dt(t.booking_date),
                        "Value_Date": _fmt_dt(t.value_date),
                        "Age_Days": _days_between(t.booking_date, now_dt),
                        "Bank": b_name,
                        "Account": acct_no,
                        "Customer": t.counterparty_name or "UNIDENTIFIED SENDER",
                        "Matched_Invoice_Ref": "",
                        "Gross_Invoice_Amount": "",
                        "Net_Cash_Received": float(cr),
                        "Deducted_Fee_or_WHT": 0.0,
                        "Currency": ccy,
                        "Raw_Bank_Narrative": t.raw_description,
                        "Action_Required": "Sales / AR to Claim & Provide Invoice #",
                    })

            # 2) WHT Tax Certificates & Direct Bank Tax Deductions
            if erp_pairs and var_amt > Decimal("0.00"):
                m0, e0 = erp_pairs[0]
                disp_up = (m0.variance_disposition or "").upper()
                desc_up = (t.raw_description or "").upper()
                is_wht = disp_up in ("WHT", "WHT_DEDUCTION", "WITHHOLDING_TAX", "TAX_DEDUCTION") or any(
                    k in desc_up for k in ("WHT", "WITHHOLDING", "خصم", "ضريبة خصم")
                )
                if is_wht:
                    gross_inv = sum(abs(Decimal(str(e.amount or 0))) for _, e in erp_pairs)
                    pct = round(float((var_amt / gross_inv) * 100), 2) if gross_inv > 0 else 0.0
                    wht_rows.append({
                        "Tax_Source": "CUSTOMER_WHT_FORM_41",
                        "Date": _fmt_dt(t.booking_date),
                        "Bank": b_name,
                        "Counterparty": e0.entity_name or t.counterparty_name or "",
                        "Invoice_Reference": ", ".join(e.reference_number for _, e in erp_pairs),
                        "Gross_Amount": float(gross_inv),
                        "Net_Bank_Amount": float(cr if is_cr else dr),
                        "Tax_Deducted_Amount": float(var_amt),
                        "Implied_Tax_Pct": f"{pct}%",
                        "Currency": ccy,
                        "Certificate_Status": "PENDING_FORM_41_COLLECTION",
                    })

            if dr > Decimal("0.00") and cat_up == "TAX_DEDUCTION":
                wht_rows.append({
                    "Tax_Source": "BANK_DIRECT_TAX_OR_STAMP",
                    "Date": _fmt_dt(t.booking_date),
                    "Bank": b_name,
                    "Counterparty": b_name,
                    "Invoice_Reference": t.e2e_id or t.back_office_ref or "BANK_STATEMENT",
                    "Gross_Amount": float(dr),
                    "Net_Bank_Amount": float(dr),
                    "Tax_Deducted_Amount": float(dr),
                    "Implied_Tax_Pct": "Direct",
                    "Currency": ccy,
                    "Certificate_Status": "BOOK_TO_TAX_LEDGER",
                })

            # 3) Bounced Cheques / Reversed Collections & Unbooked Commercial Debits
            if dr > Decimal("0.00"):
                if cat_up == "REVERSED_COLLECTION":
                    bounced_and_ap_rows.append({
                        "Alert_Type": "CRITICAL_BOUNCED_COLLECTION",
                        "Date": _fmt_dt(t.booking_date),
                        "Age_Days": _days_between(t.booking_date, now_dt),
                        "Bank": b_name,
                        "Party": t.counterparty_name or "Customer",
                        "Reference": t.cheque_number or t.e2e_id or "",
                        "Amount": float(dr),
                        "Currency": ccy,
                        "Description": t.raw_description,
                        "Required_Action": "HOLD DELIVERY & NOTIFY LEGAL / AR IMMEDIATELY",
                    })
                elif not erp_pairs and not is_sweep and cat_up not in ("BANK_CHARGES", "TAX_DEDUCTION", "BANK_INTEREST"):
                    bounced_and_ap_rows.append({
                        "Alert_Type": "UNBOOKED_BANK_DISBURSEMENT",
                        "Date": _fmt_dt(t.booking_date),
                        "Age_Days": _days_between(t.booking_date, now_dt),
                        "Bank": b_name,
                        "Party": t.counterparty_name or "Unmatched Payee",
                        "Reference": t.e2e_id or t.back_office_ref or "",
                        "Amount": float(dr),
                        "Currency": ccy,
                        "Description": t.raw_description,
                        "Required_Action": "AP to Post Missing Vendor Bill / Payment Voucher",
                    })

            # 4) Bank Fees, Sweep/Wire Variances & Value-Date Float Audit
            is_fee_line = dr > Decimal("0.00") and cat_up == "BANK_CHARGES"
            has_fee_var = var_amt > Decimal("0.00") and (
                is_sweep or (erp_pairs and (erp_pairs[0][0].variance_disposition or "BANK_FEE_WRITEOFF") == "BANK_FEE_WRITEOFF")
            )
            if is_fee_line or has_fee_var or float_lag_days >= 2:
                fee_val = float(dr) if is_fee_line else (float(var_amt) if has_fee_var else 0.0)
                fee_and_float_rows.append({
                    "Finding_Type": (
                        "DIRECT_BANK_CHARGE"
                        if is_fee_line
                        else ("EMBEDDED_WIRE_OR_SWEEP_FEE" if has_fee_var else "CLEARING_FLOAT_DELAY")
                    ),
                    "Booking_Date": _fmt_dt(t.booking_date),
                    "Value_Date": _fmt_dt(t.value_date),
                    "Float_Days": float_lag_days,
                    "Bank": b_name,
                    "Account": acct_no,
                    "Transaction_Amount": float(cr if is_cr else dr),
                    "Fee_Leakage_Amount": fee_val,
                    "Currency": ccy,
                    "Narrative": t.raw_description,
                })

        # Append Open/Stale ERP AP Payments to Pack 3
        for e in open_ap_erps:
            age_d = _days_between(e.record_date, now_dt)
            bounced_and_ap_rows.append({
                "Alert_Type": "STALE_UNCLEARED_ERP_PAYMENT" if age_d >= 7 else "OPEN_ERP_PAYABLE_IN_TRANSIT",
                "Date": _fmt_dt(e.record_date),
                "Age_Days": age_d,
                "Bank": e.bank_name or (banks_map.get(e.bank_id, "Unassigned") if e.bank_id else "Any"),
                "Party": e.entity_name,
                "Reference": e.reference_number,
                "Amount": float(abs(Decimal(str(e.amount or 0)))),
                "Currency": (e.currency or "EGP").upper(),
                "Description": f"{e.record_type} — Still OPEN in ERP (not yet cleared on bank statement)",
                "Required_Action": "Verify Wire Execution / IBAN or Track Unpresented Cheque" if age_d >= 7 else "Awaiting Bank Clearing",
            })

        ar_all_rows = mystery_cash + verified_collections

        packs_summary = [
            {
                "pack_key": "AR_COLLECTIONS_AND_MYSTERY_CASH",
                **self.PACK_DEFINITIONS["AR_COLLECTIONS_AND_MYSTERY_CASH"],
                "total_rows": len(ar_all_rows),
                "primary_metric_label": f"{len(mystery_cash)} Mystery Credits · {len(verified_collections)} Verified",
                "total_amount": round(
                    sum(r["Net_Cash_Received"] for r in ar_all_rows), 2
                ),
            },
            {
                "pack_key": "WHT_TAX_CERTIFICATES_REGISTER",
                **self.PACK_DEFINITIONS["WHT_TAX_CERTIFICATES_REGISTER"],
                "total_rows": len(wht_rows),
                "primary_metric_label": f"{sum(1 for r in wht_rows if r['Tax_Source'] == 'CUSTOMER_WHT_FORM_41')} Form-41 WHT Claims",
                "total_amount": round(sum(r["Tax_Deducted_Amount"] for r in wht_rows), 2),
            },
            {
                "pack_key": "AP_STALE_PAYMENTS_AND_BOUNCED_RISK",
                **self.PACK_DEFINITIONS["AP_STALE_PAYMENTS_AND_BOUNCED_RISK"],
                "total_rows": len(bounced_and_ap_rows),
                "primary_metric_label": (
                    f"{sum(1 for r in bounced_and_ap_rows if r['Alert_Type'] == 'CRITICAL_BOUNCED_COLLECTION')} Bounced · "
                    f"{sum(1 for r in bounced_and_ap_rows if r['Alert_Type'] == 'STALE_UNCLEARED_ERP_PAYMENT')} Stale AP"
                ),
                "total_amount": round(sum(r["Amount"] for r in bounced_and_ap_rows), 2),
            },
            {
                "pack_key": "BANK_FEES_AND_FLOAT_AUDIT",
                **self.PACK_DEFINITIONS["BANK_FEES_AND_FLOAT_AUDIT"],
                "total_rows": len(fee_and_float_rows),
                "primary_metric_label": (
                    f"Fees: {_fmt_money(sum(r['Fee_Leakage_Amount'] for r in fee_and_float_rows))} · "
                    f"{sum(1 for r in fee_and_float_rows if r['Float_Days'] >= 2)} Float Lags"
                ),
                "total_amount": round(sum(r["Fee_Leakage_Amount"] for r in fee_and_float_rows), 2),
            },
        ]

        rows_by_pack = {
            "AR_COLLECTIONS_AND_MYSTERY_CASH": ar_all_rows,
            "WHT_TAX_CERTIFICATES_REGISTER": wht_rows,
            "AP_STALE_PAYMENTS_AND_BOUNCED_RISK": bounced_and_ap_rows,
            "BANK_FEES_AND_FLOAT_AUDIT": fee_and_float_rows,
        }

        active_key = (selected_pack or "AR_COLLECTIONS_AND_MYSTERY_CASH").strip().upper()
        if active_key not in rows_by_pack:
            raise HTTPException(status_code=400, detail=f"Unknown action pack '{active_key}'")

        active_rows = rows_by_pack[active_key]
        active_headers = list(active_rows[0].keys()) if active_rows else []

        return {
            "packs": packs_summary,
            "selected_pack": active_key,
            "selected_meta": self.PACK_DEFINITIONS[active_key],
            "headers": active_headers,
            "rows": active_rows,
        }

    def export_action_pack_bytes(
        self,
        pack_data: Dict[str, Any],
        file_format: str = "XLSX",
    ) -> Tuple[bytes, str, str]:
        fmt = (file_format or "XLSX").strip().upper()
        pack_key = pack_data["selected_pack"].lower()
        meta = pack_data["selected_meta"]
        rows = pack_data["rows"]
        headers = pack_data["headers"] or ["Status"]
        safe_rows = (
            [{k: sanitize_spreadsheet_cell(v) for k, v in r.items()} for r in rows]
            if rows
            else [{"Status": "No exception or action items found in current scope"}]
        )
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if fmt == "CSV":
            s_buf = io.StringIO()
            s_buf.write("\ufeff")
            writer = csv.DictWriter(s_buf, fieldnames=list(safe_rows[0].keys()), extrasaction="ignore")
            writer.writeheader()
            for r in safe_rows:
                writer.writerow(r)
            return (
                s_buf.getvalue().encode("utf-8"),
                "text/csv; charset=utf-8",
                f"grow_{pack_key}_{ts}.csv",
            )

        if fmt == "XLSX":
            import pandas as pd

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                if pack_data["selected_pack"] == "AR_COLLECTIONS_AND_MYSTERY_CASH" and rows:
                    mystery = [r for r in safe_rows if r.get("Section") == "UNIDENTIFIED_MYSTERY_CREDIT"]
                    verified = [r for r in safe_rows if r.get("Section") == "VERIFIED_COLLECTION"]
                    pd.DataFrame(mystery or [{"Info": "Zero unidentified mystery credits"}]).to_excel(
                        writer, index=False, sheet_name="1_Mystery_Cash_To_Claim"
                    )
                    pd.DataFrame(verified or [{"Info": "Zero verified collections"}]).to_excel(
                        writer, index=False, sheet_name="2_Verified_AR_Collections"
                    )
                else:
                    pd.DataFrame(safe_rows).to_excel(writer, index=False, sheet_name="Action_Pack")
            buf.seek(0)
            return (
                buf.getvalue(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                f"grow_{pack_key}_{ts}.xlsx",
            )

        raise HTTPException(status_code=400, detail=f"Unsupported export format '{fmt}'")

    def render_action_pack_html(self, pack_data: Dict[str, Any]) -> str:
        meta = pack_data["selected_meta"]
        rows = pack_data["rows"][:200]
        headers = pack_data["headers"]

        if not rows or not headers:
            table_html = '<p style="color:#64748b;font-style:italic;padding:14px 0;">No items currently require action in this pack.</p>'
        else:
            ths = "".join(f"<th>{html.escape(h.replace('_', ' '))}</th>" for h in headers)
            trs = []
            for r in rows:
                tds = []
                for h in headers:
                    v = r.get(h, "")
                    if isinstance(v, float):
                        tds.append(f'<td style="text-align:right;font-family:monospace;">{v:,.2f}</td>')
                    else:
                        tds.append(f"<td>{html.escape(str(v if v is not None else ''))}</td>")
                trs.append(f"<tr>{''.join(tds)}</tr>")
            table_html = f"<table><thead><tr>{ths}</tr></thead><tbody>{''.join(trs)}</tbody></table>"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{html.escape(meta["title"])}</title>
<style>
  @page {{ size: A4 landscape; margin: 12mm 14mm 14mm 14mm; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; font-size: 8pt; color: #0f172a; }}
  .hdr {{ background: #0f172a; color: #fff; padding: 11px 14px; border-radius: 7px; margin-bottom: 10px; }}
  .hdr h1 {{ margin: 0; font-size: 12pt; }}
  .hdr p {{ margin: 3px 0 0 0; color: #cbd5e1; font-size: 8pt; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ background: #f1f5f9; border: 1px solid #cbd5e1; padding: 5px 6px; text-align: left; font-size: 7.5pt; }}
  td {{ border: 1px solid #e2e8f0; padding: 4px 6px; font-size: 7.5pt; }}
</style>
</head>
<body>
  <div class="hdr">
    <h1>{html.escape(meta["title"])}</h1>
    <p>Target Team: {html.escape(meta["audience"])} &nbsp;·&nbsp; {html.escape(meta["description"])}</p>
  </div>
  {table_html}
</body>
</html>"""


reconciliation_brs_service = ReconciliationBRSAndPacksService()
