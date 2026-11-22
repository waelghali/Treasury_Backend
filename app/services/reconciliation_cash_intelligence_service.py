"""
Phase 3 Service: CFO Direct-Method Cash Flow (IAS 7), 7/14/30-Day Rolling Cash Liquidity Forecast
(with Learned Counterparty Payment Lag Intelligence), and Bank Fee & Clearing Float Scorecard.

100% additive & zero-regression:
- Reads verified `BankStatement`, `BankTransaction`, `InternalLedgerRecord`, `ReconciliationMatch`,
  `ClassificationTaxonomy`, and optional `CashBalanceSnapshot` / `Bank` / `Currency` / `Customer` tables.
- Isolates cross-bank sweeps (`INTER_BANK_SWEEP` / `INTERNAL_TRANSFER`) and nets paired error reversals
  so operating cash flow is never distorted by internal liquidity transfers or bank booking errors.
- Learns counterparty-specific payment lag (`booking_date - record_date`) from historical reconciled
  ERP matches to predict realistic clearing dates for currently `OPEN` AR/AP items.
- Benchmarks banks on direct fees, embedded wire/sweep variance deductions, effective fee drag in
  basis points (bps), and value-date clearing float cost.
"""

import html
import io
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, noload

from app.crud.crud_reconciliation_v2 import crud_bank_transaction
from app.models.models_reconciliation_v2 import (
    BankStatement,
    BankTransaction,
    ClassificationTaxonomy,
    InternalLedgerRecord,
    ReconciliationMatch,
)
from app.services.reconciliation_brs_and_packs_service import sanitize_spreadsheet_cell

logger = logging.getLogger(__name__)


def _has_table(db: Session, table_name: str) -> bool:
    try:
        return bool(sa_inspect(db.connection()).has_table(table_name))
    except Exception:
        return False


def _naive_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None)
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    if isinstance(val, str) and val.strip():
        s = val.strip().replace("Z", "")
        for fmt, cut in (
            ("%Y-%m-%dT%H:%M:%S", 19),
            ("%Y-%m-%d %H:%M:%S", 19),
            ("%Y-%m-%d", 10),
            ("%d/%m/%Y", 10),
            ("%d-%m-%Y", 10),
        ):
            try:
                return datetime.strptime(s[:cut], fmt)
            except Exception:
                continue
    return None


def _fmt_dt(val: Any) -> str:
    dt = _naive_dt(val)
    return dt.strftime("%Y-%m-%d") if dt else ""


def _fmt_money(val: Any, ccy: str = "") -> str:
    try:
        num = float(val or 0)
    except Exception:
        num = 0.0
    txt = f"{num:,.2f}"
    return f"{ccy} {txt}".strip() if ccy else txt


def _norm_acct(raw: Optional[str]) -> str:
    if not raw:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(raw)).upper()


def _norm_entity(raw: Optional[str]) -> str:
    if not raw:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s\u0600-\u06FF]", " ", str(raw).upper())).strip()


class ReconciliationCashIntelligenceService:
    """Computes Direct-Method Cash Flow, 30-Day Rolling Liquidity Forecast, and Bank Fee/Float Scorecard."""

    INVESTING_KEYWORDS = (
        "CAPEX", "FIXED_ASSET", "EQUIPMENT", "MACHINERY", "TIME_DEPOSIT",
        "TERM_DEPOSIT", "TREASURY_BILL", "T_BILL", "INVESTMENT", "SUBSIDIARY_ACQ",
        "ودائع لأجل", "أصول ثابتة", "أذون خزانة",
    )
    FINANCING_KEYWORDS = (
        "LOAN", "FACILITY_DRAWDOWN", "FACILITY_REPAYMENT", "PRINCIPAL_REPAYMENT",
        "OVERDRAFT_SETTLEMENT", "DIVIDEND", "SHARE_CAPITAL", "EQUITY", "FINANCING",
        "قرض", "توزيعات أرباح", "تمويل", "سداد أصل",
    )

    def _classify_ias7_activity(
        self,
        is_credit: bool,
        cat_code: str,
        internal_gl: str,
        erp_types: List[str],
        has_lg_match: bool,
    ) -> Tuple[str, str]:
        """
        Maps a non-sweep, non-reversal bank transaction to `(ias7_section, line_label)`
        where `ias7_section` in `('OPERATING', 'INVESTING', 'FINANCING')`.
        """
        combined = f"{cat_code} {internal_gl}".upper()

        # Check Investing first
        if any(k in combined for k in self.INVESTING_KEYWORDS):
            label = (
                "Proceeds / Maturity of Investments & Deposits"
                if is_credit
                else "Capital Expenditure & Time Deposit Placements"
            )
            return "INVESTING", label

        # Check Financing
        if any(k in combined for k in self.FINANCING_KEYWORDS):
            label = (
                "Loan / Facility Drawdowns & Equity Inflows"
                if is_credit
                else "Debt Principal Repayments, Financing & Dividends"
            )
            return "FINANCING", label

        # Default to Operating Cash Flow with rich Treasury line-item granularity
        erp_up = {t.upper() for t in erp_types if t}
        if is_credit:
            if "AR_INVOICE" in erp_up or "COLLECTION" in combined or "RECEIVABLE" in combined or "POS" in combined:
                return "OPERATING", "Customer & Milestone Collections (AR)"
            if "INTEREST" in combined or "عائد" in combined or "فوائد دائنة" in combined:
                return "OPERATING", "Bank Interest & Yield Received"
            if "TAX" in combined or "REFUND" in combined:
                return "OPERATING", "Tax & Statutory Refunds"
            if cat_code in ("", "UNCLASSIFIED"):
                return "OPERATING", "Unclassified / Unclaimed Operating Inflows"
            return "OPERATING", f"Other Operating Receipts ({cat_code.replace('_', ' ').title()})"
        else:
            if has_lg_match or "LG_COMMISSION" in erp_up or "LG" in cat_code or "GUARANTEE" in combined:
                return "OPERATING", "LG Commissions, Cash Margins & Trade Finance"
            if "PAYROLL_BATCH" in erp_up or "PAYROLL" in combined or "SALARY" in combined or "رواتب" in combined:
                return "OPERATING", "Payroll, Salaries & Employee Benefits"
            if "AP_BILL" in erp_up or "SUPPLIER" in combined or "VENDOR" in combined or "PAYABLE" in combined:
                return "OPERATING", "Supplier & Subcontractor Settlements (AP)"
            if "BANK_CHARGES" in cat_code or "COMMISSION" in combined or "FEE" in combined or "عمولة" in combined:
                return "OPERATING", "Direct Bank Charges & Corridor Fees"
            if "TAX" in combined or "CUSTOMS" in combined or "VAT" in combined or "ضرائب" in combined or "جمارك" in combined:
                return "OPERATING", "Taxes, WHT, VAT & Statutory Duties"
            if cat_code == "REVERSED_COLLECTION":
                return "OPERATING", "Returned / Bounced Customer Collections"
            if cat_code in ("", "UNCLASSIFIED"):
                return "OPERATING", "Unclassified Operating Disbursements"
            return "OPERATING", f"Operating Expenses ({cat_code.replace('_', ' ').title()})"

    def build_cash_intelligence_report(
        self,
        db: Session,
        customer_id: int,
        currency: Optional[str] = None,
        bank_id: Optional[int] = None,
        days_back: Optional[int] = None,
        minimum_cash_buffer: float = 0.0,
        benchmark_annual_rate_pct: float = 20.0,
        as_of_override: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        # 1. Resolve optional metadata tables safely
        company_name = f"Company #{customer_id}"
        banks_map: Dict[int, str] = {}
        ccy_id_map: Dict[int, str] = {}
        try:
            from app.models.models import Bank, Currency, Customer
            if _has_table(db, "customers"):
                c_obj = db.query(Customer).filter(Customer.id == customer_id).first()
                if c_obj and getattr(c_obj, "name", None):
                    company_name = c_obj.name
            if _has_table(db, "banks"):
                for b in db.query(Bank).all():
                    banks_map[b.id] = getattr(b, "short_name", None) or b.name or f"Bank #{b.id}"
            if _has_table(db, "currencies"):
                for c in db.query(Currency).all():
                    if getattr(c, "iso_code", None):
                        ccy_id_map[c.id] = c.iso_code.upper()
        except Exception:
            pass

        # 2. Load Tenant Statements
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
        all_stmts = stmt_q.order_by(BankStatement.statement_end_date.asc(), BankStatement.id.asc()).all()
        stmts_by_id = {s.id: s for s in all_stmts}

        # 3. Load Tenant Bank Transactions
        has_dek_table = _has_table(db, "quotation_tenant_keys")
        all_txns: List[BankTransaction] = []
        if stmts_by_id:
            tx_q = (
                db.query(BankTransaction)
                .options(noload("*"))
                .filter(
                    BankTransaction.statement_id.in_(list(stmts_by_id.keys())),
                    BankTransaction.is_deleted == False,
                )
                .order_by(BankTransaction.booking_date.asc(), BankTransaction.id.asc())
            )
            all_txns = tx_q.all()
            if has_dek_table and all_txns:
                all_txns = crud_bank_transaction._resolve_confidential_fields(
                    db, all_txns, customer_id=customer_id
                )

        # 4. Load CashBalanceSnapshots (if Smart Inbox table exists)
        snapshots: List[Any] = []
        if _has_table(db, "cash_balance_snapshots"):
            try:
                from app.models.models_inbox import CashBalanceSnapshot
                snap_q = (
                    db.query(CashBalanceSnapshot)
                    .options(noload("*"))
                    .filter(
                        CashBalanceSnapshot.customer_id == customer_id,
                        CashBalanceSnapshot.is_deleted == False,
                    )
                )
                if bank_id:
                    snap_q = snap_q.filter(CashBalanceSnapshot.bank_id == bank_id)
                snapshots = snap_q.order_by(CashBalanceSnapshot.as_of_datetime.asc(), CashBalanceSnapshot.id.asc()).all()
            except Exception:
                snapshots = []

        # 5. Load InternalLedgerRecords (both OPEN and RECONCILED for lag learning!)
        erp_q = db.query(InternalLedgerRecord).options(noload("*")).filter(
            InternalLedgerRecord.company_id == customer_id,
            InternalLedgerRecord.is_deleted == False,
        )
        if bank_id:
            erp_q = erp_q.filter(
                (InternalLedgerRecord.bank_id == bank_id) | (InternalLedgerRecord.bank_id == None)
            )
        all_erps = erp_q.all()
        erp_by_id = {e.id: e for e in all_erps}

        # 6. Load ReconciliationMatches across tenant transactions
        txn_by_id = {t.id: t for t in all_txns}
        matches_by_txn: Dict[int, List[ReconciliationMatch]] = defaultdict(list)
        if txn_by_id:
            m_rows = (
                db.query(ReconciliationMatch)
                .options(noload("*"))
                .filter(
                    ReconciliationMatch.bank_txn_id.in_(list(txn_by_id.keys())),
                    ReconciliationMatch.is_deleted == False,
                )
                .all()
            )
            for m in m_rows:
                matches_by_txn[m.bank_txn_id].append(m)

        # 7. Discover Currencies & Latest Verified Cash Positions Across All Currencies
        # Map statement -> currency
        stmt_ccy_resolved: Dict[int, str] = {}
        first_txn_ccy_by_stmt: Dict[int, str] = {}
        for t in all_txns:
            if t.statement_id not in first_txn_ccy_by_stmt and t.currency:
                first_txn_ccy_by_stmt[t.statement_id] = t.currency.strip().upper()
        for s in all_stmts:
            c_code = (
                ccy_id_map.get(s.currency_id)
                or first_txn_ccy_by_stmt.get(s.id)
                or "EGP"
            )
            stmt_ccy_resolved[s.id] = c_code

        # Consolidate latest verified cash per `(bank_id, norm_acct, ccy)`
        account_latest: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
        for s in all_stmts:
            c_code = stmt_ccy_resolved[s.id]
            acct_key = _norm_acct(s.account_number) or f"STMTACCT{s.bank_id}"
            key = (s.bank_id, acct_key, c_code)
            as_of = _naive_dt(s.statement_end_date) or _naive_dt(s.created_at) or datetime(2026, 1, 1)
            prev = account_latest.get(key)
            if prev is None or as_of >= prev["_as_of_dt"]:
                account_latest[key] = {
                    "bank_id": s.bank_id,
                    "bank_name": banks_map.get(s.bank_id, f"Bank #{s.bank_id}"),
                    "account_number": s.account_number or f"Account #{s.bank_id}",
                    "currency": c_code,
                    "balance": Decimal(str(s.closing_balance or "0.00")),
                    "as_of_date": _fmt_dt(as_of),
                    "_as_of_dt": as_of,
                    "source": "VALIDATED_STATEMENT" if (s.balance_continuity_status or "VALIDATED") == "VALIDATED" else "STATEMENT",
                    "is_brs_signed": bool(s.brs_signed_at or (s.status or "").upper() == "RECONCILED"),
                    "statement_id": s.id,
                }

        for snap in snapshots:
            c_code = (snap.currency_code or "EGP").strip().upper()
            acct_key = _norm_acct(snap.account_number) or f"STMTACCT{snap.bank_id}"
            key = (snap.bank_id, acct_key, c_code)
            as_of = _naive_dt(snap.as_of_datetime) or _naive_dt(snap.created_at) or datetime(2026, 1, 1)
            prev = account_latest.get(key)
            # Newer snapshot supersedes older statement balance (e.g. intra-day Smart Inbox feed)
            if prev is None or as_of > prev["_as_of_dt"]:
                account_latest[key] = {
                    "bank_id": snap.bank_id,
                    "bank_name": banks_map.get(snap.bank_id, f"Bank #{snap.bank_id}"),
                    "account_number": snap.account_number or (prev["account_number"] if prev else f"Account #{snap.bank_id}"),
                    "currency": c_code,
                    "balance": Decimal(str(snap.available_balance or "0.00")),
                    "as_of_date": _fmt_dt(as_of),
                    "_as_of_dt": as_of,
                    "source": f"LIVE_{snap.source_channel or 'SNAPSHOT'}",
                    "is_brs_signed": bool(prev["is_brs_signed"]) if prev else False,
                    "statement_id": snap.statement_id or (prev["statement_id"] if prev else None),
                }

        ccy_totals: Dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
        ccy_acct_counts: Dict[str, int] = defaultdict(int)
        for (_, _, c_code), info in account_latest.items():
            ccy_totals[c_code] += info["balance"]
            ccy_acct_counts[c_code] += 1

        for t in all_txns:
            if t.currency:
                ccy_totals.setdefault(t.currency.strip().upper(), Decimal("0.00"))
        for e in all_erps:
            if e.currency:
                ccy_totals.setdefault(e.currency.strip().upper(), Decimal("0.00"))

        if not ccy_totals:
            ccy_totals["EGP"] = Decimal("0.00")

        available_currencies = sorted(
            ccy_totals.keys(),
            key=lambda c: (0 if c == "EGP" else 1, -float(abs(ccy_totals[c])), c),
        )
        selected_ccy = (
            currency.strip().upper()
            if currency and currency.strip().upper() in ccy_totals
            else (currency.strip().upper() if currency and currency.strip() else available_currencies[0])
        )

        multi_currency_strip = [
            {
                "currency": c,
                "verified_cash_balance": float(round(ccy_totals.get(c, Decimal("0.00")), 2)),
                "accounts_count": ccy_acct_counts.get(c, 0),
            }
            for c in available_currencies
        ]

        # 8. Filter transactions, accounts & ERP records to `selected_ccy`
        cutoff_dt: Optional[datetime] = None
        if days_back and days_back > 0:
            anchor_probe = _naive_dt(as_of_override) or max(
                ([_naive_dt(t.booking_date) for t in all_txns if _naive_dt(t.booking_date)] or [datetime.utcnow()])
            )
            cutoff_dt = anchor_probe - timedelta(days=days_back)

        ccy_txns: List[BankTransaction] = []
        for t in all_txns:
            t_ccy = (t.currency or stmt_ccy_resolved.get(t.statement_id, "EGP")).strip().upper()
            if t_ccy != selected_ccy:
                continue
            b_dt = _naive_dt(t.booking_date)
            if cutoff_dt and b_dt and b_dt < cutoff_dt:
                continue
            ccy_txns.append(t)

        # ==============================================================================
        # PILLAR 1: IAS 7 DIRECT-METHOD CASH FLOW ENGINE
        # ==============================================================================
        inflow_lines: Dict[str, Dict[str, Any]] = {}
        outflow_lines: Dict[str, Dict[str, Any]] = {}
        investing_lines: Dict[str, Dict[str, Any]] = {}
        financing_lines: Dict[str, Dict[str, Any]] = {}

        period_buckets: Dict[str, Dict[str, Decimal]] = defaultdict(
            lambda: {
                "operating_inflow": Decimal("0.00"),
                "operating_outflow": Decimal("0.00"),
                "investing_net": Decimal("0.00"),
                "financing_net": Decimal("0.00"),
                "sweeps_in": Decimal("0.00"),
                "sweeps_out": Decimal("0.00"),
            }
        )

        excluded_reversals_count = 0
        excluded_reversals_gross = Decimal("0.00")
        sweeps_in_total = Decimal("0.00")
        sweeps_out_total = Decimal("0.00")
        sweeps_count = 0

        min_txn_dt: Optional[datetime] = None
        max_txn_dt: Optional[datetime] = None

        def _add_cf_line(target_dict: Dict[str, Dict[str, Any]], label: str, amt: Decimal, is_inflow: bool):
            rec = target_dict.setdefault(
                label,
                {"label": label, "amount": Decimal("0.00"), "count": 0, "direction": "INFLOW" if is_inflow else "OUTFLOW"},
            )
            rec["amount"] += amt
            rec["count"] += 1

        for t in ccy_txns:
            cr = Decimal(str(t.credit_amount or "0.00"))
            dr = Decimal(str(t.debit_amount or "0.00"))
            is_cr = cr > Decimal("0.00")
            amt = cr if is_cr else dr
            if amt <= Decimal("0.00"):
                continue

            b_dt = _naive_dt(t.booking_date)
            if b_dt:
                if min_txn_dt is None or b_dt < min_txn_dt:
                    min_txn_dt = b_dt
                if max_txn_dt is None or b_dt > max_txn_dt:
                    max_txn_dt = b_dt
            period_key = b_dt.strftime("%Y-%m") if b_dt else "Undated"

            # 1) Ghost Reversal Netting -> strictly excluded from Cash Flow totals (both legs)
            linked_peer = txn_by_id.get(t.linked_txn_id) if t.linked_txn_id else None
            if t.linked_txn_id and (t.is_reversal or (linked_peer and linked_peer.is_reversal)):
                excluded_reversals_count += 1
                excluded_reversals_gross += amt
                continue

            cat_up = (t.classification_category or "").strip().upper()
            int_up = (t.internal_category or "").strip().upper()
            is_sweep = (
                cat_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")
                or int_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")
                or (bool(t.linked_txn_id) and not t.is_reversal)
            )

            # 2) Cross-Bank Liquidity Sweeps -> isolated from Operating Cash Flow!
            #    (Any embedded corridor wire fee on a sweep debit IS recognized as Operating Bank Charges)
            if is_sweep:
                sweeps_count += 1
                var_fee = Decimal(str(t.variance_amount or "0.00"))
                if not is_cr and var_fee > Decimal("0.00") and var_fee < amt:
                    principal_sweep = amt - var_fee
                    sweeps_out_total += principal_sweep
                    period_buckets[period_key]["sweeps_out"] += principal_sweep
                    _add_cf_line(outflow_lines, "Direct Bank Charges & Corridor Fees", var_fee, False)
                    period_buckets[period_key]["operating_outflow"] += var_fee
                elif is_cr:
                    sweeps_in_total += amt
                    period_buckets[period_key]["sweeps_in"] += amt
                else:
                    sweeps_out_total += amt
                    period_buckets[period_key]["sweeps_out"] += amt
                continue

            # 3) Standard IAS 7 Classification (Operating / Investing / Financing)
            t_matches = matches_by_txn.get(t.id, [])
            erp_types = [
                erp_by_id[m.source_record_id].record_type
                for m in t_matches
                if m.source_record_id in erp_by_id
            ]
            has_lg_match = any((m.source_type or "").upper() == "TREASURY (LG)" for m in t_matches)

            section, line_label = self._classify_ias7_activity(
                is_credit=is_cr,
                cat_code=cat_up,
                internal_gl=int_up,
                erp_types=erp_types,
                has_lg_match=has_lg_match,
            )

            if section == "OPERATING":
                if is_cr:
                    _add_cf_line(inflow_lines, line_label, amt, True)
                    period_buckets[period_key]["operating_inflow"] += amt
                else:
                    _add_cf_line(outflow_lines, line_label, amt, False)
                    period_buckets[period_key]["operating_outflow"] += amt
            elif section == "INVESTING":
                _add_cf_line(investing_lines, line_label, amt if is_cr else -amt, is_cr)
                period_buckets[period_key]["investing_net"] += (amt if is_cr else -amt)
            else:
                _add_cf_line(financing_lines, line_label, amt if is_cr else -amt, is_cr)
                period_buckets[period_key]["financing_net"] += (amt if is_cr else -amt)

        op_inflow_total = sum((v["amount"] for v in inflow_lines.values()), Decimal("0.00"))
        op_outflow_total = sum((v["amount"] for v in outflow_lines.values()), Decimal("0.00"))
        net_operating_cf = op_inflow_total - op_outflow_total
        net_investing_cf = sum((v["amount"] for v in investing_lines.values()), Decimal("0.00"))
        net_financing_cf = sum((v["amount"] for v in financing_lines.values()), Decimal("0.00"))
        net_sweeps_cf = sweeps_in_total - sweeps_out_total
        net_total_cash_movement = net_operating_cf + net_investing_cf + net_financing_cf + net_sweeps_cf

        observed_span_days = max(1, (max_txn_dt - min_txn_dt).days + 1) if (min_txn_dt and max_txn_dt) else 30
        avg_daily_inflow = op_inflow_total / Decimal(str(observed_span_days))
        avg_daily_outflow = op_outflow_total / Decimal(str(observed_span_days))
        ocf_margin_pct = (
            float(round((net_operating_cf / op_inflow_total) * Decimal("100"), 2))
            if op_inflow_total > Decimal("0.00")
            else 0.0
        )

        def _serialize_lines(d: Dict[str, Dict[str, Any]], denom: Decimal) -> List[Dict[str, Any]]:
            rows = sorted(d.values(), key=lambda r: abs(r["amount"]), reverse=True)
            return [
                {
                    "label": r["label"],
                    "amount": float(round(r["amount"], 2)),
                    "count": r["count"],
                    "direction": r["direction"],
                    "share_pct": float(round((abs(r["amount"]) / denom) * Decimal("100"), 1)) if denom > 0 else 0.0,
                }
                for r in rows
            ]

        period_series = []
        for p_key in sorted(period_buckets.keys()):
            pb = period_buckets[p_key]
            net_op = pb["operating_inflow"] - pb["operating_outflow"]
            period_series.append({
                "period": p_key,
                "operating_inflow": float(round(pb["operating_inflow"], 2)),
                "operating_outflow": float(round(pb["operating_outflow"], 2)),
                "net_operating_cf": float(round(net_op, 2)),
                "investing_net": float(round(pb["investing_net"], 2)),
                "financing_net": float(round(pb["financing_net"], 2)),
                "net_cash_change": float(round(net_op + pb["investing_net"] + pb["financing_net"], 2)),
            })

        direct_cash_flow = {
            "currency": selected_ccy,
            "period_start": _fmt_dt(min_txn_dt),
            "period_end": _fmt_dt(max_txn_dt),
            "observed_days": observed_span_days,
            "operating_inflows_total": float(round(op_inflow_total, 2)),
            "operating_outflows_total": float(round(op_outflow_total, 2)),
            "net_operating_cash_flow": float(round(net_operating_cf, 2)),
            "operating_cash_margin_pct": ocf_margin_pct,
            "net_investing_cash_flow": float(round(net_investing_cf, 2)),
            "net_financing_cash_flow": float(round(net_financing_cf, 2)),
            "free_cash_flow_proxy": float(round(net_operating_cf + net_investing_cf, 2)),
            "isolated_sweeps_in": float(round(sweeps_in_total, 2)),
            "isolated_sweeps_out": float(round(sweeps_out_total, 2)),
            "net_interbank_sweeps": float(round(net_sweeps_cf, 2)),
            "isolated_sweeps_count": sweeps_count,
            "excluded_paired_reversals_count": excluded_reversals_count,
            "excluded_paired_reversals_gross": float(round(excluded_reversals_gross, 2)),
            "net_total_cash_movement": float(round(net_total_cash_movement, 2)),
            "avg_daily_operating_inflow": float(round(avg_daily_inflow, 2)),
            "avg_daily_operating_outflow": float(round(avg_daily_outflow, 2)),
            "operating_inflow_breakdown": _serialize_lines(inflow_lines, op_inflow_total),
            "operating_outflow_breakdown": _serialize_lines(outflow_lines, op_outflow_total),
            "investing_breakdown": _serialize_lines(investing_lines, max(abs(net_investing_cf), Decimal("1.00"))),
            "financing_breakdown": _serialize_lines(financing_lines, max(abs(net_financing_cf), Decimal("1.00"))),
            "period_series": period_series,
        }

        # ==============================================================================
        # PILLAR 2: 7 / 14 / 30-DAY ROLLING LIQUIDITY FORECAST & COUNTERPARTY LAG AI
        # ==============================================================================
        # Step A: Learn Historical Counterparty Clearing Lag (`booking_date - record_date`)
        entity_lags: Dict[str, List[int]] = defaultdict(list)
        entity_display_names: Dict[str, str] = {}
        for b_txn_id, m_list in matches_by_txn.items():
            b_txn = txn_by_id.get(b_txn_id)
            if not b_txn:
                continue
            b_dt = _naive_dt(b_txn.booking_date)
            if not b_dt:
                continue
            for m in m_list:
                erp_rec = erp_by_id.get(m.source_record_id)
                if not erp_rec:
                    continue
                r_dt = _naive_dt(erp_rec.record_date)
                if not r_dt:
                    continue
                norm_e = _norm_entity(erp_rec.entity_name)
                if not norm_e:
                    continue
                lag_d = max(0, (b_dt.date() - r_dt.date()).days)
                entity_lags[norm_e].append(lag_d)
                entity_display_names[norm_e] = erp_rec.entity_name

        counterparty_lag_profiles = []
        avg_lag_by_entity: Dict[str, int] = {}
        for norm_e, lags in entity_lags.items():
            avg_l = int(round(sum(lags) / len(lags)))
            avg_lag_by_entity[norm_e] = avg_l
            spread = max(lags) - min(lags)
            reliability = max(55, min(99, 98 - spread * 2 + min(len(lags) * 3, 10)))
            counterparty_lag_profiles.append({
                "entity_name": entity_display_names[norm_e],
                "historical_matches": len(lags),
                "avg_clearing_lag_days": avg_l,
                "min_lag_days": min(lags),
                "max_lag_days": max(lags),
                "reliability_score_pct": reliability,
            })
        counterparty_lag_profiles.sort(key=lambda x: (-x["historical_matches"], x["avg_clearing_lag_days"]))

        # Step B: Verified Starting Cash Anchor in `selected_ccy`
        ccy_accounts = [
            {k: v for k, v in info.items() if k != "_as_of_dt"}
            for (_, _, c_code), info in sorted(
                account_latest.items(), key=lambda kv: (-float(kv[1]["balance"]), kv[1]["account_number"])
            )
            if c_code == selected_ccy
        ]
        for a in ccy_accounts:
            a["balance"] = float(round(Decimal(str(a["balance"])), 2))

        starting_cash = sum((Decimal(str(a["balance"])) for a in ccy_accounts), Decimal("0.00"))

        # Determine forecast Day-0 reference date:
        # Either `as_of_override`, or the latest statement/snapshot date in this currency (or today UTC)
        latest_cash_dates = [
            info["_as_of_dt"]
            for (_, _, c_code), info in account_latest.items()
            if c_code == selected_ccy and info.get("_as_of_dt")
        ]
        forecast_anchor_dt = (
            _naive_dt(as_of_override)
            or (max(latest_cash_dates) if latest_cash_dates else None)
            or max_txn_dt
            or datetime.utcnow()
        )
        anchor_date = forecast_anchor_dt.date()

        # Step C: Schedule OPEN InternalLedgerRecords across Day +1 .. Day +30
        open_ccy_erps = [
            e for e in all_erps
            if (e.status or "OPEN").upper() == "OPEN"
            and (e.currency or "EGP").strip().upper() == selected_ccy
        ]

        daily_inflows_base: Dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
        daily_inflows_stress: Dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
        daily_outflows_base: Dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
        daily_outflows_stress: Dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))

        upcoming_receivables: List[Dict[str, Any]] = []
        upcoming_payables: List[Dict[str, Any]] = []

        for e in open_ccy_erps:
            amt_signed = Decimal(str(e.amount or "0.00"))
            if amt_signed == Decimal("0.00"):
                continue
            is_inflow = amt_signed > Decimal("0.00")
            abs_amt = abs(amt_signed)
            r_dt = _naive_dt(e.record_date) or forecast_anchor_dt
            r_date = r_dt.date()
            age_days = max(0, (anchor_date - r_date).days)
            norm_e = _norm_entity(e.entity_name)

            # Check custom_fields_json for explicit ERP due_date / expected_date
            explicit_due: Optional[date] = None
            cf = e.custom_fields_json if isinstance(e.custom_fields_json, dict) else {}
            for dk in ("due_date", "DueDate", "expected_date", "expected_clearing_date", "maturity_date"):
                if cf.get(dk):
                    parsed_d = _naive_dt(cf.get(dk))
                    if parsed_d:
                        explicit_due = parsed_d.date()
                        break

            is_overdue_risk = False
            if explicit_due:
                raw_offset = (explicit_due - anchor_date).days
                if raw_offset < 1:
                    is_overdue_risk = is_inflow and raw_offset < -7
                    day_offset = 7 if is_overdue_risk else 2
                    prediction_basis = "OVERDUE_ERP_DUE_DATE" if is_overdue_risk else "ERP_DUE_DATE"
                else:
                    day_offset = raw_offset
                    prediction_basis = "ERP_DUE_DATE"
                expected_lag_days = max(0, (explicit_due - r_date).days)
            elif norm_e in avg_lag_by_entity:
                learned_lag = avg_lag_by_entity[norm_e]
                predicted_date = r_date + timedelta(days=learned_lag)
                raw_offset = (predicted_date - anchor_date).days
                if raw_offset < 1:
                    is_overdue_risk = is_inflow and age_days > max(30, learned_lag + 14)
                    day_offset = 10 if is_overdue_risk else 3
                    prediction_basis = "OVERDUE_AT_RISK" if is_overdue_risk else "LEARNED_COUNTERPARTY_LAG"
                else:
                    day_offset = raw_offset
                    prediction_basis = "LEARNED_COUNTERPARTY_LAG"
                expected_lag_days = learned_lag
            else:
                default_lag = 5 if is_inflow else 3
                expected_lag_days = default_lag
                if age_days > 30:
                    is_overdue_risk = is_inflow
                    day_offset = 14 if is_inflow else 3
                    prediction_basis = "OVERDUE_AT_RISK" if is_inflow else "AGED_PAYABLE_IMMINENT"
                else:
                    predicted_date = r_date + timedelta(days=default_lag)
                    day_offset = max(1, (predicted_date - anchor_date).days)
                    prediction_basis = "STANDARD_CLEARING_WINDOW"

            expected_clear_date = anchor_date + timedelta(days=day_offset)

            item_payload = {
                "id": e.id,
                "reference_number": e.reference_number,
                "entity_name": e.entity_name,
                "record_type": e.record_type,
                "record_date": _fmt_dt(r_date),
                "age_days": age_days,
                "learned_or_expected_lag_days": expected_lag_days,
                "expected_clearing_date": expected_clear_date.strftime("%Y-%m-%d"),
                "days_from_now": day_offset,
                "amount": float(round(abs_amt, 2)),
                "currency": selected_ccy,
                "prediction_basis": prediction_basis,
                "is_overdue_at_risk": is_overdue_risk,
            }

            if is_inflow:
                upcoming_receivables.append(item_payload)
                if 1 <= day_offset <= 30:
                    # Apply 50% conservative haircut to >30d overdue AR even in baseline, 0% in stress!
                    base_weight = Decimal("0.50") if is_overdue_risk else Decimal("1.00")
                    stress_weight = Decimal("0.00") if is_overdue_risk else Decimal("0.85")
                    daily_inflows_base[day_offset] += (abs_amt * base_weight)
                    daily_inflows_stress[day_offset] += (abs_amt * stress_weight)
            else:
                upcoming_payables.append(item_payload)
                if 1 <= day_offset <= 30:
                    daily_outflows_base[day_offset] += abs_amt
                    # Stress test assumes 100% of payables hit without delay (pulled forward if aged)
                    stress_day = max(1, min(day_offset, 5)) if age_days > 14 else day_offset
                    daily_outflows_stress[stress_day] += abs_amt

        upcoming_receivables.sort(key=lambda r: (r["days_from_now"], -r["amount"]))
        upcoming_payables.sort(key=lambda r: (r["days_from_now"], -r["amount"]))

        # Build 30-Day Daily Trajectory
        min_buffer_dec = Decimal(str(minimum_cash_buffer or 0))
        running_base = starting_cash
        running_stress = starting_cash
        lowest_base_cash = starting_cash
        lowest_base_day = anchor_date.strftime("%Y-%m-%d")
        lowest_stress_cash = starting_cash
        lowest_stress_day = anchor_date.strftime("%Y-%m-%d")

        daily_projection = [
            {
                "day_index": 0,
                "date": anchor_date.strftime("%Y-%m-%d"),
                "expected_inflow": 0.0,
                "committed_outflow": 0.0,
                "baseline_cash": float(round(starting_cash, 2)),
                "stress_cash": float(round(starting_cash, 2)),
                "minimum_buffer": float(round(min_buffer_dec, 2)),
            }
        ]

        horizons: Dict[str, Dict[str, Any]] = {}
        cum_in_b = Decimal("0.00")
        cum_out_b = Decimal("0.00")

        for d_idx in range(1, 31):
            d_date = (anchor_date + timedelta(days=d_idx)).strftime("%Y-%m-%d")
            in_b = daily_inflows_base[d_idx]
            out_b = daily_outflows_base[d_idx]
            in_s = daily_inflows_stress[d_idx]
            out_s = daily_outflows_stress[d_idx]

            cum_in_b += in_b
            cum_out_b += out_b
            running_base = running_base + in_b - out_b
            running_stress = running_stress + in_s - out_s

            if running_base < lowest_base_cash:
                lowest_base_cash = running_base
                lowest_base_day = d_date
            if running_stress < lowest_stress_cash:
                lowest_stress_cash = running_stress
                lowest_stress_day = d_date

            daily_projection.append({
                "day_index": d_idx,
                "date": d_date,
                "expected_inflow": float(round(in_b, 2)),
                "committed_outflow": float(round(out_b, 2)),
                "baseline_cash": float(round(running_base, 2)),
                "stress_cash": float(round(running_stress, 2)),
                "minimum_buffer": float(round(min_buffer_dec, 2)),
            })

            if d_idx in (7, 14, 30):
                horizons[f"{d_idx}d"] = {
                    "horizon_days": d_idx,
                    "target_date": d_date,
                    "expected_inflows": float(round(cum_in_b, 2)),
                    "committed_outflows": float(round(cum_out_b, 2)),
                    "net_cash_flow": float(round(cum_in_b - cum_out_b, 2)),
                    "projected_ending_cash": float(round(running_base, 2)),
                    "stress_ending_cash": float(round(running_stress, 2)),
                    "buffer_headroom": float(round(running_base - min_buffer_dec, 2)),
                }

        liquidity_status = "HEALTHY"
        if lowest_base_cash < Decimal("0.00"):
            liquidity_status = "CRITICAL_CASH_DEFICIT"
        elif lowest_base_cash < min_buffer_dec:
            liquidity_status = "BUFFER_BREACH_WARNING"
        elif lowest_stress_cash < min_buffer_dec:
            liquidity_status = "STRESS_BUFFER_WATCH"

        # Idle Excess Cash detection (if 30-day lowest stress cash exceeds buffer by > 0)
        deployable_idle_cash = max(Decimal("0.00"), lowest_stress_cash - min_buffer_dec)

        liquidity_forecast = {
            "currency": selected_ccy,
            "anchor_date": anchor_date.strftime("%Y-%m-%d"),
            "verified_starting_cash": float(round(starting_cash, 2)),
            "minimum_cash_buffer": float(round(min_buffer_dec, 2)),
            "liquidity_status": liquidity_status,
            "lowest_baseline_cash": float(round(lowest_base_cash, 2)),
            "lowest_baseline_date": lowest_base_day,
            "lowest_stress_cash": float(round(lowest_stress_cash, 2)),
            "lowest_stress_date": lowest_stress_day,
            "deployable_excess_cash_30d": float(round(deployable_idle_cash, 2)),
            "horizons": horizons,
            "account_cash_positions": ccy_accounts,
            "counterparty_lag_profiles": counterparty_lag_profiles[:25],
            "upcoming_receivables": upcoming_receivables[:50],
            "upcoming_payables": upcoming_payables[:50],
            "open_receivables_total_count": len(upcoming_receivables),
            "open_payables_total_count": len(upcoming_payables),
            "daily_projection": daily_projection,
        }

        # ==============================================================================
        # PILLAR 3: BANK FEE LEAKAGE & CLEARING FLOAT SCORECARD
        # ==============================================================================
        bank_stats: Dict[int, Dict[str, Any]] = {}
        annual_rate = Decimal(str(benchmark_annual_rate_pct or 20.0)) / Decimal("100.0")

        for t in ccy_txns:
            linked_peer = txn_by_id.get(t.linked_txn_id) if t.linked_txn_id else None
            if t.linked_txn_id and (t.is_reversal or (linked_peer and linked_peer.is_reversal)):
                continue
            st_obj = stmts_by_id.get(t.statement_id)
            b_id = st_obj.bank_id if st_obj else 0
            b_name = banks_map.get(b_id, f"Bank #{b_id}")

            bs = bank_stats.setdefault(
                b_id,
                {
                    "bank_id": b_id,
                    "bank_name": b_name,
                    "txns_count": 0,
                    "gross_volume": Decimal("0.00"),
                    "inflow_volume": Decimal("0.00"),
                    "inflow_count": 0,
                    "direct_fees_amount": Decimal("0.00"),
                    "direct_fees_count": 0,
                    "embedded_fees_amount": Decimal("0.00"),
                    "embedded_fees_count": 0,
                    "float_lag_days_sum": 0,
                    "delayed_inflows_count": 0,
                    "max_float_days": 0,
                    "imputed_float_cost": Decimal("0.00"),
                    "continuity_warnings": 0,
                },
            )

            cr = Decimal(str(t.credit_amount or "0.00"))
            dr = Decimal(str(t.debit_amount or "0.00"))
            is_cr = cr > Decimal("0.00")
            amt = cr if is_cr else dr
            cat_up = (t.classification_category or "").strip().upper()
            int_up = (t.internal_category or "").strip().upper()
            is_sweep = (
                cat_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")
                or int_up in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")
                or bool(t.linked_txn_id)
            )

            bs["txns_count"] += 1
            bs["gross_volume"] += amt

            if is_cr and not is_sweep:
                bs["inflow_volume"] += cr
                bs["inflow_count"] += 1
                b_dt = _naive_dt(t.booking_date)
                v_dt = _naive_dt(t.value_date)
                if b_dt and v_dt:
                    f_days = max(0, (v_dt.date() - b_dt.date()).days)
                    bs["float_lag_days_sum"] += f_days
                    if f_days > bs["max_float_days"]:
                        bs["max_float_days"] = f_days
                    if f_days >= 1:
                        bs["delayed_inflows_count"] += 1
                        bs["imputed_float_cost"] += (
                            cr * (Decimal(str(f_days)) / Decimal("365.0")) * annual_rate
                        )

            # Direct fee lines
            if dr > Decimal("0.00") and cat_up == "BANK_CHARGES":
                bs["direct_fees_amount"] += dr
                bs["direct_fees_count"] += 1

            # Embedded corridor/match deduction fees (excluding tax WHT)
            var_amt = Decimal(str(t.variance_amount or "0.00"))
            if var_amt > Decimal("0.00"):
                t_matches = matches_by_txn.get(t.id, [])
                disp_up = (t_matches[0].variance_disposition or "BANK_FEE_WRITEOFF").upper() if t_matches else "BANK_FEE_WRITEOFF"
                is_wht = disp_up in ("WHT", "WHT_DEDUCTION", "WITHHOLDING_TAX", "TAX_DEDUCTION")
                if is_sweep or (t_matches and not is_wht):
                    bs["embedded_fees_amount"] += var_amt
                    bs["embedded_fees_count"] += 1

        # Count statement continuity warnings per bank
        for s in all_stmts:
            if stmt_ccy_resolved.get(s.id) == selected_ccy and s.bank_id in bank_stats:
                if (s.balance_continuity_status or "VALIDATED").upper() != "VALIDATED":
                    bank_stats[s.bank_id]["continuity_warnings"] += 1

        scorecard_rows = []
        total_direct_fees = Decimal("0.00")
        total_embedded_fees = Decimal("0.00")
        total_imputed_float = Decimal("0.00")

        for b_id, bs in bank_stats.items():
            tot_fees = bs["direct_fees_amount"] + bs["embedded_fees_amount"]
            tot_economic_drag = tot_fees + bs["imputed_float_cost"]
            vol = bs["gross_volume"]
            fee_bps = float(round((tot_fees / vol) * Decimal("10000"), 2)) if vol > 0 else 0.0
            all_in_bps = float(round((tot_economic_drag / vol) * Decimal("10000"), 2)) if vol > 0 else 0.0
            avg_float = (
                round(bs["float_lag_days_sum"] / bs["inflow_count"], 2)
                if bs["inflow_count"] > 0
                else 0.0
            )

            # Efficiency score (0..100): penalize high fee bps, value-date float lag, and continuity errors
            penalty = min(45.0, fee_bps * 0.9) + min(35.0, avg_float * 10.0) + (bs["continuity_warnings"] * 10.0)
            score = int(round(max(25.0, 100.0 - penalty)))
            grade = "A" if score >= 88 else ("B" if score >= 75 else ("C" if score >= 60 else "D"))

            total_direct_fees += bs["direct_fees_amount"]
            total_embedded_fees += bs["embedded_fees_amount"]
            total_imputed_float += bs["imputed_float_cost"]

            scorecard_rows.append({
                "bank_id": b_id,
                "bank_name": bs["bank_name"],
                "transactions_count": bs["txns_count"],
                "gross_volume": float(round(vol, 2)),
                "direct_fees_amount": float(round(bs["direct_fees_amount"], 2)),
                "direct_fees_count": bs["direct_fees_count"],
                "embedded_fees_amount": float(round(bs["embedded_fees_amount"], 2)),
                "embedded_fees_count": bs["embedded_fees_count"],
                "total_fees_amount": float(round(tot_fees, 2)),
                "effective_fee_bps": fee_bps,
                "avg_inflow_float_days": avg_float,
                "max_float_days": bs["max_float_days"],
                "delayed_inflows_count": bs["delayed_inflows_count"],
                "imputed_float_cost": float(round(bs["imputed_float_cost"], 2)),
                "total_economic_drag": float(round(tot_economic_drag, 2)),
                "all_in_drag_bps": all_in_bps,
                "efficiency_score": score,
                "efficiency_grade": grade,
            })

        scorecard_rows.sort(key=lambda r: (-r["total_economic_drag"], -r["gross_volume"]))

        bank_scorecard = {
            "currency": selected_ccy,
            "benchmark_annual_rate_pct": float(benchmark_annual_rate_pct or 20.0),
            "total_direct_fees": float(round(total_direct_fees, 2)),
            "total_embedded_fees": float(round(total_embedded_fees, 2)),
            "total_explicit_fees": float(round(total_direct_fees + total_embedded_fees, 2)),
            "total_imputed_float_cost": float(round(total_imputed_float, 2)),
            "total_economic_bank_drag": float(round(total_direct_fees + total_embedded_fees + total_imputed_float, 2)),
            "banks": scorecard_rows,
        }

        return {
            "company_id": customer_id,
            "company_name": company_name,
            "selected_currency": selected_ccy,
            "available_currencies": available_currencies,
            "multi_currency_cash_strip": multi_currency_strip,
            "direct_cash_flow": direct_cash_flow,
            "liquidity_forecast": liquidity_forecast,
            "bank_scorecard": bank_scorecard,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ==============================================================================
    # 4-SHEET EXCEL WORKBOOK & WEASYPRINT A4 CFO EXECUTIVE BRIEFING EXPORTERS
    # ==============================================================================

    def render_excel_bytes(self, report: Dict[str, Any]) -> bytes:
        import pandas as pd

        ccy = report["selected_currency"]
        cf = report["direct_cash_flow"]
        lf = report["liquidity_forecast"]
        bs = report["bank_scorecard"]

        # Sheet 1: IAS 7 Direct Cash Flow Statement
        cf_rows = [
            {"Section": "SUMMARY", "Line Item": "Operating Cash Inflows", "Count": "", "Amount": cf["operating_inflows_total"], "Currency": ccy},
            {"Section": "SUMMARY", "Line Item": "Operating Cash Outflows", "Count": "", "Amount": -cf["operating_outflows_total"], "Currency": ccy},
            {"Section": "SUMMARY", "Line Item": "= Net Operating Cash Flow (OCF)", "Count": "", "Amount": cf["net_operating_cash_flow"], "Currency": ccy},
            {"Section": "SUMMARY", "Line Item": "Net Investing Cash Flow", "Count": "", "Amount": cf["net_investing_cash_flow"], "Currency": ccy},
            {"Section": "SUMMARY", "Line Item": "Net Financing Cash Flow", "Count": "", "Amount": cf["net_financing_cash_flow"], "Currency": ccy},
            {"Section": "NON-OPERATING", "Line Item": "Isolated Inter-Bank Sweeps (Net)", "Count": cf["isolated_sweeps_count"], "Amount": cf["net_interbank_sweeps"], "Currency": ccy},
            {"Section": "AUDIT_CONTROL", "Line Item": "Excluded Same-Day Paired Reversals (0 Net)", "Count": cf["excluded_paired_reversals_count"], "Amount": 0.0, "Currency": ccy},
        ]
        for r in cf["operating_inflow_breakdown"]:
            cf_rows.append({"Section": "OPERATING_INFLOW", "Line Item": r["label"], "Count": r["count"], "Amount": r["amount"], "Currency": ccy})
        for r in cf["operating_outflow_breakdown"]:
            cf_rows.append({"Section": "OPERATING_OUTFLOW", "Line Item": r["label"], "Count": r["count"], "Amount": -r["amount"], "Currency": ccy})
        for r in cf["investing_breakdown"]:
            cf_rows.append({"Section": "INVESTING", "Line Item": r["label"], "Count": r["count"], "Amount": r["amount"], "Currency": ccy})
        for r in cf["financing_breakdown"]:
            cf_rows.append({"Section": "FINANCING", "Line Item": r["label"], "Count": r["count"], "Amount": r["amount"], "Currency": ccy})

        # Sheet 2: 30-Day Daily Liquidity Trajectory
        traj_rows = lf["daily_projection"]

        # Sheet 3: Upcoming Open Receivables & Payables + Learned Lag
        queue_rows = (
            [{"Direction": "INFLOW_RECEIVABLE", **r} for r in lf["upcoming_receivables"]]
            + [{"Direction": "OUTFLOW_PAYABLE", **r} for r in lf["upcoming_payables"]]
        )

        # Sheet 4: Bank Fee & Float Scorecard
        bank_rows = bs["banks"]

        def _safe_df(rows_list: List[Dict[str, Any]]) -> Any:
            if not rows_list:
                return pd.DataFrame([{"Info": "No records in current scope"}])
            cleaned = [{k: sanitize_spreadsheet_cell(v) for k, v in r.items()} for r in rows_list]
            return pd.DataFrame(cleaned)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            _safe_df(cf_rows).to_excel(writer, index=False, sheet_name="1_Direct_Cash_Flow_IAS7")
            _safe_df(traj_rows).to_excel(writer, index=False, sheet_name="2_30Day_Liquidity_Forecast")
            _safe_df(queue_rows).to_excel(writer, index=False, sheet_name="3_Upcoming_AR_AP_Lags")
            _safe_df(bank_rows).to_excel(writer, index=False, sheet_name="4_Bank_Fee_Float_Scorecard")
        buf.seek(0)
        return buf.getvalue()

    def render_pdf_html(self, report: Dict[str, Any]) -> str:
        ccy = html.escape(report["selected_currency"])
        comp = html.escape(report["company_name"])
        cf = report["direct_cash_flow"]
        lf = report["liquidity_forecast"]
        bs = report["bank_scorecard"]
        h7 = lf["horizons"].get("7d", {})
        h14 = lf["horizons"].get("14d", {})
        h30 = lf["horizons"].get("30d", {})

        def _cf_trs(items: List[Dict[str, Any]], sign_prefix: str = "") -> str:
            if not items:
                return '<tr><td colspan="3" style="color:#94a3b8;font-style:italic;text-align:center;">None in period</td></tr>'
            return "".join(
                f'<tr><td>{html.escape(r["label"])}</td>'
                f'<td style="text-align:center;">{r["count"]}</td>'
                f'<td style="text-align:right;font-family:monospace;font-weight:600;">{sign_prefix}{_fmt_money(r["amount"])}</td></tr>'
                for r in items
            )

        bank_trs = (
            "".join(
                f'<tr>'
                f'<td><strong>{html.escape(b["bank_name"])}</strong></td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(b["gross_volume"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(b["direct_fees_amount"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(b["embedded_fees_amount"])}</td>'
                f'<td style="text-align:right;font-family:monospace;font-weight:700;">{b["effective_fee_bps"]:.2f} bps</td>'
                f'<td style="text-align:center;">{b["avg_inflow_float_days"]:.1f}d</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(b["imputed_float_cost"])}</td>'
                f'<td style="text-align:center;font-weight:800;">{html.escape(b["efficiency_grade"])} ({b["efficiency_score"]})</td>'
                f'</tr>'
                for b in bs["banks"]
            )
            if bs["banks"]
            else '<tr><td colspan="8" style="text-align:center;color:#94a3b8;font-style:italic;">No bank statement activity in selected currency.</td></tr>'
        )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>CFO Cash Flow &amp; Liquidity Intelligence — {comp}</title>
<style>
  @page {{
    size: A4;
    margin: 12mm 13mm 15mm 13mm;
    @bottom-left {{ content: "Grow Corporate Treasury · CFO Cash Flow, Rolling Liquidity & Bank Scorecard"; font-size: 7.5pt; color: #64748b; }}
    @bottom-right {{ content: "Page " counter(page) " of " counter(pages); font-size: 7.5pt; color: #64748b; }}
  }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; font-size: 8.4pt; color: #0f172a; line-height: 1.36; margin: 0; }}
  .hdr {{ background: #0f172a; color: #fff; padding: 13px 16px; border-radius: 8px; margin-bottom: 11px; }}
  .hdr h1 {{ margin: 0; font-size: 13pt; }}
  .hdr p {{ margin: 3px 0 0 0; color: #94a3b8; font-size: 8.2pt; }}
  .kpi-row {{ width: 100%; border-collapse: collapse; margin-bottom: 12px; }}
  .kpi-row td {{ width: 25%; border: 1px solid #cbd5e1; background: #f8fafc; padding: 7px 9px; }}
  .kpi-lbl {{ font-size: 7.2pt; text-transform: uppercase; color: #64748b; font-weight: 700; }}
  .kpi-val {{ font-size: 10.5pt; font-weight: 800; font-family: monospace; margin-top: 2px; color: #0f172a; }}
  h3 {{ font-size: 9.4pt; margin: 11px 0 5px 0; border-bottom: 1.5px solid #cbd5e1; padding-bottom: 3px; color: #0f172a; page-break-after: avoid; }}
  table.grid {{ width: 100%; border-collapse: collapse; margin-bottom: 10px; font-size: 8pt; }}
  table.grid th {{ background: #eff6ff; color: #1e3a8a; border: 1px solid #bfdbfe; padding: 5px 7px; text-align: left; }}
  table.grid td {{ border: 1px solid #e2e8f0; padding: 4.5px 7px; }}
  .tot {{ background: #f0fdf4; font-weight: 800; color: #14532d; }}
</style>
</head>
<body>
  <div class="hdr">
    <h1>CFO Direct Cash Flow, 30-Day Liquidity Forecast &amp; Bank Scorecard</h1>
    <p>{comp} &nbsp;·&nbsp; Reporting Currency: {ccy} &nbsp;·&nbsp; Forecast Anchor: {html.escape(lf["anchor_date"])} &nbsp;·&nbsp; Status: {html.escape(lf["liquidity_status"])}</p>
  </div>

  <table class="kpi-row">
    <tr>
      <td><div class="kpi-lbl">Verified Starting Cash</div><div class="kpi-val">{_fmt_money(lf["verified_starting_cash"], ccy)}</div></td>
      <td><div class="kpi-lbl">Net Operating Cash Flow</div><div class="kpi-val">{_fmt_money(cf["net_operating_cash_flow"], ccy)}</div></td>
      <td><div class="kpi-lbl">+30d Baseline Ending Cash</div><div class="kpi-val">{_fmt_money(h30.get("projected_ending_cash", 0), ccy)}</div></td>
      <td><div class="kpi-lbl">Bank Fee &amp; Float Drag</div><div class="kpi-val">{_fmt_money(bs["total_economic_bank_drag"], ccy)}</div></td>
    </tr>
  </table>

  <h3>1. IAS 7 Direct-Method Cash Flow Statement (Sweeps &amp; Reversals Isolated)</h3>
  <table class="grid">
    <thead><tr><th>Operating Inflow Category</th><th style="text-align:center;width:12%;">Txns</th><th style="text-align:right;width:26%;">Amount ({ccy})</th></tr></thead>
    <tbody>
      {_cf_trs(cf["operating_inflow_breakdown"], "+")}
      <tr class="tot"><td>Total Verified Operating Inflows</td><td style="text-align:center;">—</td><td style="text-align:right;font-family:monospace;">+{_fmt_money(cf["operating_inflows_total"])}</td></tr>
    </tbody>
  </table>

  <table class="grid">
    <thead><tr><th>Operating Outflow Category</th><th style="text-align:center;width:12%;">Txns</th><th style="text-align:right;width:26%;">Amount ({ccy})</th></tr></thead>
    <tbody>
      {_cf_trs(cf["operating_outflow_breakdown"], "-")}
      <tr class="tot"><td>Net Operating Cash Flow (OCF) &nbsp;|&nbsp; Cash Margin: {cf["operating_cash_margin_pct"]}%</td><td style="text-align:center;">—</td><td style="text-align:right;font-family:monospace;">{_fmt_money(cf["net_operating_cash_flow"], ccy)}</td></tr>
    </tbody>
  </table>

  <p style="font-size:7.7pt;color:#475569;margin:2px 0 9px 0;">
    <strong>Audit Hygiene:</strong> Isolated {cf["isolated_sweeps_count"]} inter-bank liquidity sweep line(s) (Net: {_fmt_money(cf["net_interbank_sweeps"], ccy)}) and netted {cf["excluded_paired_reversals_count"]} paired error reversal(s) out of operating P&amp;L cash flow.
  </p>

  <h3>2. Rolling 7 / 14 / 30-Day Cash Liquidity Forecast (Behavioral Lag Adjusted)</h3>
  <table class="grid">
    <thead>
      <tr>
        <th>Horizon Window</th>
        <th style="text-align:right;">Expected Inflows</th>
        <th style="text-align:right;">Committed Outflows</th>
        <th style="text-align:right;">Baseline Ending Cash</th>
        <th style="text-align:right;">Conservative Stress Cash</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>Day +7</strong> ({html.escape(h7.get("target_date", ""))})</td>
        <td style="text-align:right;font-family:monospace;">+{_fmt_money(h7.get("expected_inflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;">-{_fmt_money(h7.get("committed_outflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(h7.get("projected_ending_cash", 0))}</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(h7.get("stress_ending_cash", 0))}</td>
      </tr>
      <tr>
        <td><strong>Day +14</strong> ({html.escape(h14.get("target_date", ""))})</td>
        <td style="text-align:right;font-family:monospace;">+{_fmt_money(h14.get("expected_inflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;">-{_fmt_money(h14.get("committed_outflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(h14.get("projected_ending_cash", 0))}</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(h14.get("stress_ending_cash", 0))}</td>
      </tr>
      <tr class="tot">
        <td><strong>Day +30</strong> ({html.escape(h30.get("target_date", ""))})</td>
        <td style="text-align:right;font-family:monospace;">+{_fmt_money(h30.get("expected_inflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;">-{_fmt_money(h30.get("committed_outflows", 0))}</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(h30.get("projected_ending_cash", 0), ccy)}</td>
        <td style="text-align:right;font-family:monospace;">{_fmt_money(h30.get("stress_ending_cash", 0), ccy)}</td>
      </tr>
    </tbody>
  </table>

  <h3>3. Bank Fee Leakage &amp; Value-Date Clearing Float Scorecard</h3>
  <table class="grid">
    <thead>
      <tr>
        <th>Bank</th>
        <th style="text-align:right;">Volume</th>
        <th style="text-align:right;">Direct Fees</th>
        <th style="text-align:right;">Hidden Ded.</th>
        <th style="text-align:right;">Fee Drag</th>
        <th style="text-align:center;">Avg Float</th>
        <th style="text-align:right;">Float Cost</th>
        <th style="text-align:center;">Grade</th>
      </tr>
    </thead>
    <tbody>{bank_trs}</tbody>
  </table>
</body>
</html>"""


reconciliation_cash_intelligence_service = ReconciliationCashIntelligenceService()
