"""
Phase 4 Service: Closed-Loop Cross-Module Treasury Integration
Bridges Bank Statement Reconciliation directly with:
1. LG Issuance, Facility Tariffs & LG Custody (`IssuedLGRecord`, `IssuanceFacilitySubLimit`, `IssuanceFacility`, `LGRecord`):
   - Audits actual bank LG commission debits against contractual Facility commission rates, flat fees & minimums.
   - Audits actual bank LG cash-margin holds against contractual Facility cash-margin % (`EXCESS_CASH_MARGIN_HELD`),
     and tracks incoming bank cash-margin releases (`CASH_MARGIN_RELEASED`).
   - Detects `SHADOW_LG_UNREGISTERED` when a bank statement debits LG fees/margins for a guarantee reference
     not registered anywhere in Grow's Issuance or Custody registers.
2. RFQ Quotations Module (`QuotationRequest`, `QuotationLeg`):
   - Verifies bank statement settlement of awarded FX Spot and T-Bill / Money Market deals.
   - Flags `SETTLED_RATE_OR_FEE_DISCREPANCY` when bank settled at an off-market rate worse than the agreed
     winning quote, `SETTLED_VALUE_DATE_DELAY` on late value-dating, and `OVERDUE_UNSETTLED_DEAL` when value date passed.
3. Closed-Loop Idle Cash -> 1-Click Pre-Filled T-Bill / Money Market RFQ Opportunity Generator.
"""

import html
import io
import logging
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, noload

from app.crud.crud_reconciliation_v2 import crud_bank_transaction
from app.models.models_reconciliation_v2 import (
    BankStatement,
    BankTransaction,
    ReconciliationMatch,
)
from app.services.reconciliation_brs_and_packs_service import sanitize_spreadsheet_cell
from app.services.reconciliation_cash_intelligence_service import (
    reconciliation_cash_intelligence_service,
)

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


def _norm_ref(raw: Optional[str]) -> str:
    if not raw:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(raw)).upper()


class ReconciliationTreasuryBridgeService:
    """Closed-loop audit & action bridge across Reconciliation, LG/Facilities, and RFQ Quotations."""

    LG_SIGNAL_KEYWORDS = (
        "LG-", "LG/", "LG#", "LETTER OF GUARANTEE", "BANK GUARANTEE",
        "LG COMMISSION", "LG ISSUANCE", "LG MARGIN", "CASH MARGIN LG",
        "GUARANTEE COMMISSION", "GUARANTEE MARGIN",
        "خطاب ضمان", "عمولة ضمان", "عمولة خطاب ضمان", "غطاء نقدي", "غطاء خطاب ضمان", "تأمين خطاب ضمان",
    )
    MARGIN_KEYWORDS = (
        "MARGIN", "CASH COVER", "COVER HOLD", "CASH HOLD", "COLLATERAL",
        "غطاء", "تأمين نقدي", "هامش نقدي", "حجز غطاء", "رد غطاء", "إفراج غطاء",
    )
    RELEASE_KEYWORDS = (
        "RELEASE", "REFUND", "RETURN", "UNBLOCK", "رد غطاء", "إفراج", "استرداد",
    )

    def _extract_suspected_lg_ref(self, text: str) -> str:
        if not text:
            return "UNREFERENCED-LG"
        m = re.search(
            r"\b((?:LG|GUA|BG)[-/#][A-Z0-9\-/#]{2,24}|(?:LG|GUA|BG)\d[A-Z0-9\-/#]{2,24})\b",
            text.upper(),
        )
        return m.group(1) if m else "UNREFERENCED-LG"

    def build_treasury_bridge_report(
        self,
        db: Session,
        customer_id: int,
        currency: Optional[str] = None,
        bank_id: Optional[int] = None,
        minimum_cash_buffer: float = 0.0,
        benchmark_annual_rate_pct: float = 20.0,
    ) -> Dict[str, Any]:
        # 1. Resolve optional lookup tables safely
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

        # 2. Load Tenant Bank Statements & Transactions
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

        latest_stmt_end_by_bank: Dict[int, datetime] = {}
        for s in all_stmts:
            s_end = _naive_dt(s.statement_end_date)
            if s_end:
                prev = latest_stmt_end_by_bank.get(s.bank_id)
                if prev is None or s_end > prev:
                    latest_stmt_end_by_bank[s.bank_id] = s_end

        all_txns: List[BankTransaction] = []
        if stmts_by_id:
            all_txns = (
                db.query(BankTransaction)
                .options(noload("*"))
                .filter(
                    BankTransaction.statement_id.in_(list(stmts_by_id.keys())),
                    BankTransaction.is_deleted == False,
                )
                .order_by(BankTransaction.booking_date.asc(), BankTransaction.id.asc())
                .all()
            )
            if _has_table(db, "quotation_tenant_keys") and all_txns:
                all_txns = crud_bank_transaction._resolve_confidential_fields(
                    db, all_txns, customer_id=customer_id
                )

        txn_by_id = {t.id: t for t in all_txns}
        matches_by_txn: Dict[int, List[ReconciliationMatch]] = {}
        if txn_by_id:
            for m in (
                db.query(ReconciliationMatch)
                .options(noload("*"))
                .filter(
                    ReconciliationMatch.bank_txn_id.in_(list(txn_by_id.keys())),
                    ReconciliationMatch.is_deleted == False,
                )
                .all()
            ):
                matches_by_txn.setdefault(m.bank_txn_id, []).append(m)

        target_ccy = currency.strip().upper() if currency and currency.strip() else None

        # ==============================================================================
        # ENGINE 1: LG FACILITY TARIFF, CASH MARGIN & SHADOW-LG AUDIT
        # ==============================================================================
        issued_lgs: Dict[int, Any] = {}
        sublimits_by_id: Dict[int, Any] = {}
        facilities_by_id: Dict[int, Any] = {}
        custody_lgs: List[Any] = []

        if _has_table(db, "issued_lg_records"):
            try:
                from app.models.models_issuance import (
                    IssuanceFacility,
                    IssuanceFacilitySubLimit,
                    IssuedLGRecord,
                )
                lg_q = db.query(IssuedLGRecord).options(noload("*")).filter(
                    IssuedLGRecord.customer_id == customer_id
                )
                if bank_id:
                    lg_q = lg_q.filter(IssuedLGRecord.bank_id == bank_id)
                for lg in lg_q.all():
                    issued_lgs[lg.id] = lg

                if _has_table(db, "facilities"):
                    for f in (
                        db.query(IssuanceFacility)
                        .options(noload("*"))
                        .filter(
                            IssuanceFacility.customer_id == customer_id,
                            IssuanceFacility.is_deleted == False,
                        )
                        .all()
                    ):
                        facilities_by_id[f.id] = f

                if _has_table(db, "issuance_facility_sub_limits") and facilities_by_id:
                    for sl in (
                        db.query(IssuanceFacilitySubLimit)
                        .options(noload("*"))
                        .filter(IssuanceFacilitySubLimit.facility_id.in_(list(facilities_by_id.keys())))
                        .all()
                    ):
                        sublimits_by_id[sl.id] = sl
            except Exception as lg_err:
                logger.debug(f"LG Issuance lookup skipped: {lg_err}")

        if _has_table(db, "lg_records"):
            try:
                from app.models.models import LGRecord
                c_q = db.query(LGRecord).options(noload("*")).filter(
                    LGRecord.customer_id == customer_id,
                    LGRecord.is_deleted == False,
                )
                if bank_id:
                    c_q = c_q.filter(LGRecord.issuing_bank_id == bank_id)
                custody_lgs = c_q.all()
            except Exception:
                custody_lgs = []

        # Build fast reference index -> `("ISSUED", lg_obj)` or `("CUSTODY", lg_obj)`
        ref_index: List[Tuple[str, str, Any]] = []
        for lg in issued_lgs.values():
            for raw_r in (lg.lg_ref_number, lg.bank_lg_number, lg.internal_serial):
                nr = _norm_ref(raw_r)
                if len(nr) >= 5:
                    ref_index.append((nr, "ISSUED", lg))
        for clg in custody_lgs:
            nr = _norm_ref(clg.lg_number)
            if len(nr) >= 5:
                ref_index.append((nr, "CUSTODY", clg))
        # Longest refs first to avoid partial prefix collisions
        ref_index.sort(key=lambda x: len(x[0]), reverse=True)

        lg_audit_rows: List[Dict[str, Any]] = []
        shadow_lg_rows: List[Dict[str, Any]] = []
        total_lg_claimable = Decimal("0.00")

        for t in all_txns:
            linked_peer = txn_by_id.get(t.linked_txn_id) if t.linked_txn_id else None
            if t.linked_txn_id and (t.is_reversal or (linked_peer and linked_peer.is_reversal)):
                continue

            t_ccy = (t.currency or "EGP").strip().upper()
            if target_ccy and t_ccy != target_ccy:
                continue

            st_obj = stmts_by_id.get(t.statement_id)
            b_id = st_obj.bank_id if st_obj else 0
            b_name = banks_map.get(b_id, f"Bank #{b_id}")

            cr = Decimal(str(t.credit_amount or "0.00"))
            dr = Decimal(str(t.debit_amount or "0.00"))
            is_cr = cr > Decimal("0.00")
            amt = cr if is_cr else dr
            if amt <= Decimal("0.00"):
                continue

            haystack_raw = f"{t.raw_description or ''} {t.e2e_id or ''} {t.back_office_ref or ''}"
            haystack_up = haystack_raw.upper()
            haystack_norm = _norm_ref(haystack_raw)
            cat_up = (t.classification_category or "").strip().upper()

            # 1) Check explicit ReconciliationMatch(source_type="TREASURY (LG)")
            matched_kind: Optional[str] = None
            matched_lg: Any = None
            for m in matches_by_txn.get(t.id, []):
                if (m.source_type or "").upper() == "TREASURY (LG)" and m.source_record_id in issued_lgs:
                    matched_kind = "ISSUED"
                    matched_lg = issued_lgs[m.source_record_id]
                    break

            # 2) Check reference substring match against Issued & Custody LGs
            if matched_lg is None and haystack_norm:
                for norm_r, kind, lg_obj in ref_index:
                    lg_b_id = lg_obj.bank_id if kind == "ISSUED" else lg_obj.issuing_bank_id
                    if lg_b_id and b_id and lg_b_id != b_id:
                        continue
                    if norm_r in haystack_norm:
                        matched_kind = kind
                        matched_lg = lg_obj
                        break

            is_lg_narrative = (
                matched_lg is not None
                or cat_up in ("LG_COMMISSION", "LG_CASH_MARGIN", "TRADE_FINANCE_LG")
                or any(kw in haystack_up for kw in self.LG_SIGNAL_KEYWORDS)
            )
            if not is_lg_narrative:
                continue

            # Shadow / Unregistered LG Check!
            if matched_lg is None:
                suspected_ref = self._extract_suspected_lg_ref(haystack_raw)
                shadow_lg_rows.append({
                    "txn_id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "bank_id": b_id,
                    "bank_name": b_name,
                    "account_number": t.account_number or (st_obj.account_number if st_obj else "") or "—",
                    "suspected_lg_ref": suspected_ref,
                    "direction": "CREDIT" if is_cr else "DEBIT",
                    "amount": float(round(amt, 2)),
                    "currency": t_ccy,
                    "raw_description": t.raw_description,
                    "finding_status": "SHADOW_LG_UNREGISTERED",
                    "recommended_action": "Verify against bank LG position report — register missing LG or claim refund if expired/cancelled",
                })
                continue

            # Registered LG Tariff & Margin Audit!
            if matched_kind == "CUSTODY":
                lg_ref = matched_lg.lg_number
                lg_principal = Decimal(str(matched_lg.lg_amount or "0.00"))
                lg_audit_rows.append({
                    "txn_id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "bank_name": b_name,
                    "lg_ref_number": lg_ref,
                    "facility_name": "Incoming Custody LG",
                    "lg_principal_amount": float(round(lg_principal, 2)),
                    "event_type": "CUSTODY_LG_MOVEMENT",
                    "bank_amount": float(round(amt, 2)),
                    "contractual_expected_amount": float(round(amt, 2)),
                    "claimable_variance": 0.0,
                    "currency": t_ccy,
                    "audit_status": "CUSTODY_VERIFIED",
                    "audit_note": f"Linked to Custody LG {lg_ref} ({matched_lg.issuer_name})",
                })
                continue

            # `matched_kind == "ISSUED"` -> Full Facility Tariff & Cash Margin Audit!
            lg_ref = matched_lg.bank_lg_number or matched_lg.lg_ref_number
            lg_principal = Decimal(str(matched_lg.current_amount or "0.00"))
            sl = sublimits_by_id.get(matched_lg.facility_sub_limit_id) if matched_lg.facility_sub_limit_id else None
            fac = facilities_by_id.get(sl.facility_id) if sl else None
            mp = matched_lg.manual_pricing if isinstance(matched_lg.manual_pricing, dict) else {}

            fac_label = (
                f"{fac.facility_name} ({sl.limit_name})"
                if (fac and sl)
                else ("Manual Bank Pricing" if mp else "Unlinked Facility")
            )

            # Resolve contractual pricing terms
            raw_comm_rate = (
                Decimal(str(sl.default_commission_rate))
                if (sl and sl.default_commission_rate is not None)
                else (Decimal(str(mp["commission_rate"])) if mp.get("commission_rate") is not None else None)
            )
            flat_fee = (
                Decimal(str(sl.default_flat_fee or 0))
                if sl
                else Decimal(str(mp.get("flat_fee") or 0))
            )
            min_comm = (
                Decimal(str(sl.default_min_commission or 0))
                if sl
                else Decimal("0.00")
            )
            agreed_margin_pct = (
                Decimal(str(sl.default_cash_margin_pct))
                if (sl and sl.default_cash_margin_pct is not None)
                else (
                    Decimal(str(fac.facility_default_margin_pct))
                    if (fac and fac.facility_default_margin_pct is not None)
                    else (Decimal(str(mp["margin_pct"])) if mp.get("margin_pct") is not None else None)
                )
            )

            is_margin_line = any(mk in haystack_up for mk in self.MARGIN_KEYWORDS) or cat_up == "LG_CASH_MARGIN"
            is_release_line = is_cr and (is_margin_line or any(rk in haystack_up for rk in self.RELEASE_KEYWORDS))

            if is_release_line:
                lg_audit_rows.append({
                    "txn_id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "bank_name": b_name,
                    "lg_ref_number": lg_ref,
                    "facility_name": fac_label,
                    "lg_principal_amount": float(round(lg_principal, 2)),
                    "event_type": "CASH_MARGIN_RELEASE",
                    "bank_amount": float(round(cr, 2)),
                    "contractual_expected_amount": float(round(cr, 2)),
                    "claimable_variance": 0.0,
                    "currency": t_ccy,
                    "audit_status": "CASH_MARGIN_RELEASED",
                    "audit_note": f"Cash margin credit (+{_fmt_money(cr, t_ccy)}) released back to operating liquidity.",
                })
            elif is_margin_line and not is_cr:
                if agreed_margin_pct is not None and lg_principal > 0:
                    expected_margin = (lg_principal * agreed_margin_pct) / Decimal("100.0")
                    actual_pct = (dr / lg_principal) * Decimal("100.0")
                    diff = dr - expected_margin
                    tolerance = max(Decimal("50.00"), lg_principal * Decimal("0.005"))
                    if diff > tolerance:
                        total_lg_claimable += diff
                        status_code = "EXCESS_CASH_MARGIN_HELD"
                        note = (
                            f"Bank held {actual_pct:.2f}% cash margin vs. agreed {agreed_margin_pct:.2f}% "
                            f"Facility cap — excess cash locked: {_fmt_money(diff, t_ccy)}."
                        )
                        claim_val = float(round(diff, 2))
                    else:
                        status_code = "TARIFF_COMPLIANT"
                        note = f"Cash margin hold ({actual_pct:.2f}%) matches agreed {agreed_margin_pct:.2f}% Facility term."
                        claim_val = 0.0
                else:
                    expected_margin = dr
                    status_code = "NO_TARIFF_BENCHMARK"
                    note = "Cash margin debit recorded (no margin % configured on sub-limit)."
                    claim_val = 0.0

                lg_audit_rows.append({
                    "txn_id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "bank_name": b_name,
                    "lg_ref_number": lg_ref,
                    "facility_name": fac_label,
                    "lg_principal_amount": float(round(lg_principal, 2)),
                    "event_type": "CASH_MARGIN_HOLD",
                    "bank_amount": float(round(dr, 2)),
                    "contractual_expected_amount": float(round(expected_margin, 2)),
                    "claimable_variance": claim_val,
                    "currency": t_ccy,
                    "audit_status": status_code,
                    "audit_note": note,
                })
            elif not is_cr:
                # LG Issuance / Commission Debit Audit
                if raw_comm_rate is not None and lg_principal > 0:
                    annual_rate = raw_comm_rate / Decimal("100.0") if raw_comm_rate > Decimal("0.25") else raw_comm_rate
                    is_quarterly_nar = any(q in haystack_up for q in ("QUARTER", "QTR", "Q1", "Q2", "Q3", "Q4", "ربع"))
                    if is_quarterly_nar:
                        period_frac = Decimal("0.25")
                        basis_desc = "quarterly"
                    elif matched_lg.issue_date and matched_lg.expiry_date:
                        d_span = max(1, (matched_lg.expiry_date - matched_lg.issue_date).days)
                        period_frac = Decimal(str(d_span)) / Decimal("365.0")
                        basis_desc = f"{d_span}d tenor"
                    else:
                        period_frac = Decimal("0.25")
                        basis_desc = "standard quarterly"

                    expected_comm = max(min_comm, (lg_principal * annual_rate * period_frac) + flat_fee)
                    diff = dr - expected_comm
                    tolerance = max(Decimal("25.00"), expected_comm * Decimal("0.03"))
                    if diff > tolerance:
                        total_lg_claimable += diff
                        status_code = "COMMISSION_OVERCHARGE_CLAIM"
                        note = (
                            f"Debited {_fmt_money(dr, t_ccy)} vs. contractual {_fmt_money(expected_comm, t_ccy)} "
                            f"({float(annual_rate * 100):.2f}% p.a., {basis_desc} + {_fmt_money(flat_fee)} flat) — "
                            f"claimable overcharge: {_fmt_money(diff, t_ccy)}."
                        )
                        claim_val = float(round(diff, 2))
                    else:
                        status_code = "TARIFF_COMPLIANT"
                        note = f"Commission matches contractual Facility tariff ({float(annual_rate * 100):.2f}% p.a., {basis_desc})."
                        claim_val = 0.0
                else:
                    expected_comm = dr
                    status_code = "NO_TARIFF_BENCHMARK"
                    note = "LG commission debit linked to LG (configure sub-limit rate to audit overcharges)."
                    claim_val = 0.0

                lg_audit_rows.append({
                    "txn_id": t.id,
                    "booking_date": _fmt_dt(t.booking_date),
                    "bank_name": b_name,
                    "lg_ref_number": lg_ref,
                    "facility_name": fac_label,
                    "lg_principal_amount": float(round(lg_principal, 2)),
                    "event_type": "LG_COMMISSION_FEE",
                    "bank_amount": float(round(dr, 2)),
                    "contractual_expected_amount": float(round(expected_comm, 2)),
                    "claimable_variance": claim_val,
                    "currency": t_ccy,
                    "audit_status": status_code,
                    "audit_note": note,
                })

        # ==============================================================================
        # ENGINE 2: RFQ QUOTATION AWARDED DEAL SETTLEMENT VERIFICATION
        # ==============================================================================
        deal_audit_rows: List[Dict[str, Any]] = []
        total_deal_slippage = Decimal("0.00")

        if _has_table(db, "quotation_rfqs"):
            try:
                from app.models.models_quotation import QuotationLeg, QuotationRequest

                rfqs = (
                    db.query(QuotationRequest)
                    .options(noload("*"))
                    .filter(
                        QuotationRequest.customer_id == customer_id,
                        QuotationRequest.is_deleted == False,
                    )
                    .order_by(QuotationRequest.created_at.desc())
                    .limit(300)
                    .all()
                )
                rfq_by_id = {r.id: r for r in rfqs}
                legs_by_rfq: Dict[str, List[Any]] = {}
                if rfq_by_id and _has_table(db, "quotation_legs"):
                    for lg in (
                        db.query(QuotationLeg)
                        .options(noload("*"))
                        .filter(
                            QuotationLeg.rfq_id.in_(list(rfq_by_id.keys())),
                            QuotationLeg.is_deleted == False,
                        )
                        .order_by(QuotationLeg.leg_index.asc())
                        .all()
                    ):
                        legs_by_rfq.setdefault(lg.rfq_id, []).append(lg)

                def _safe_rate(obj: Any) -> Optional[Decimal]:
                    raw = getattr(obj, "_db_winner_rate", None)
                    if raw is None and _has_table(db, "quotation_tenant_keys"):
                        try:
                            raw = obj.winner_rate
                        except Exception:
                            raw = None
                    return Decimal(str(raw)) if raw is not None else None

                # Build unified list of awarded executable trade items (leg-level or RFQ-level)
                awarded_items: List[Dict[str, Any]] = []
                for rfq in rfqs:
                    if (rfq.quotation_base or "Execution").upper() == "INDICATIVE":
                        continue
                    r_legs = legs_by_rfq.get(rfq.id, [])
                    if r_legs:
                        for leg in r_legs:
                            w_bank = leg.winner_bank_id or rfq.winner_bank_id
                            is_won = bool(w_bank) and (
                                (leg.status or "").upper() in ("ACCEPTED", "COMPLETED")
                                or (rfq.acceptance_status or "").upper() in ("ACCEPTED", "AUTO_ACCEPTED")
                            )
                            if not is_won:
                                continue
                            if bank_id and w_bank != bank_id:
                                continue
                            awarded_items.append({
                                "rfq_id": rfq.id,
                                "ref_no": rfq.ref_no,
                                "exec_ref": leg.execution_reference or "",
                                "trade_type": (leg.type or rfq.type or "FX_SPOT").upper(),
                                "direction": (leg.direction or rfq.direction or "Buy").capitalize(),
                                "buy_ccy": (leg.buy_currency or rfq.buy_currency or "USD").upper(),
                                "sell_ccy": (leg.sell_currency or rfq.sell_currency or "EGP").upper(),
                                "amount": Decimal(str(leg.amount or rfq.amount or 0)),
                                "winner_bank_id": w_bank,
                                "winner_bank_name": leg.winner_bank_name or rfq.winner_bank_name or banks_map.get(w_bank, f"Bank #{w_bank}"),
                                "winner_rate": _safe_rate(leg) or _safe_rate(rfq),
                                "value_date": leg.value_date or leg.settlement_date_start or rfq.value_date or rfq.settlement_date_start,
                                "maturity_date": leg.maturity_date_start or rfq.maturity_date_start,
                            })
                    else:
                        w_bank = rfq.winner_bank_id
                        is_won = bool(w_bank) and (
                            (rfq.acceptance_status or "").upper() in ("ACCEPTED", "AUTO_ACCEPTED")
                            or (rfq.status or "").upper() == "COMPLETED"
                        )
                        if is_won and (not bank_id or w_bank == bank_id):
                            awarded_items.append({
                                "rfq_id": rfq.id,
                                "ref_no": rfq.ref_no,
                                "exec_ref": "",
                                "trade_type": (rfq.type or "FX_SPOT").upper(),
                                "direction": (rfq.direction or "Buy").capitalize(),
                                "buy_ccy": (rfq.buy_currency or "USD").upper(),
                                "sell_ccy": (rfq.sell_currency or "EGP").upper(),
                                "amount": Decimal(str(rfq.amount or 0)),
                                "winner_bank_id": w_bank,
                                "winner_bank_name": rfq.winner_bank_name or banks_map.get(w_bank, f"Bank #{w_bank}"),
                                "winner_rate": _safe_rate(rfq),
                                "value_date": rfq.value_date or rfq.settlement_date_start,
                                "maturity_date": rfq.maturity_date_start,
                            })

                used_txn_ids = set()
                for deal in awarded_items:
                    if deal["amount"] <= 0:
                        continue
                    t_type = deal["trade_type"]
                    w_bank = deal["winner_bank_id"]
                    val_dt = _naive_dt(deal["value_date"])
                    ref_norm = _norm_ref(deal["ref_no"])
                    exec_norm = _norm_ref(deal["exec_ref"])
                    w_rate = deal["winner_rate"]

                    # Candidate bank transactions at `winner_bank_id`
                    bank_txns = [
                        t for t in all_txns
                        if t.id not in used_txn_ids
                        and stmts_by_id.get(t.statement_id)
                        and stmts_by_id[t.statement_id].bank_id == w_bank
                    ]

                    matched_txn: Optional[BankTransaction] = None
                    expected_leg_amt = deal["amount"]
                    expected_leg_ccy = deal["sell_ccy"] if t_type == "FX_SPOT" else (target_ccy or "EGP")

                    if t_type == "FX_SPOT":
                        # Expected counter-currency settlement = amount * winner_rate
                        counter_expected = (
                            (deal["amount"] * w_rate).quantize(Decimal("0.01"))
                            if w_rate is not None
                            else None
                        )
                        # Pass 1: Explicit RFQ ref_no or execution_reference match in bank narrative
                        for bt in bank_txns:
                            bt_norm = _norm_ref(f"{bt.raw_description or ''} {bt.e2e_id or ''} {bt.back_office_ref or ''}")
                            if (ref_norm and len(ref_norm) >= 5 and ref_norm in bt_norm) or (
                                exec_norm and len(exec_norm) >= 5 and exec_norm in bt_norm
                            ):
                                matched_txn = bt
                                break

                        # Pass 2: Date window + Counter-amount (within 2% rate slippage) or Base-amount match
                        if matched_txn is None:
                            for bt in bank_txns:
                                b_dt = _naive_dt(bt.booking_date)
                                if val_dt and b_dt and abs((b_dt.date() - val_dt.date()).days) > 7:
                                    continue
                                bt_ccy = (bt.currency or "EGP").upper()
                                bt_cr = Decimal(str(bt.credit_amount or 0))
                                bt_dr = Decimal(str(bt.debit_amount or 0))
                                if deal["direction"] == "Buy":
                                    if bt_ccy == deal["sell_ccy"] and counter_expected and bt_dr > 0:
                                        if abs(bt_dr - counter_expected) <= counter_expected * Decimal("0.025"):
                                            matched_txn = bt
                                            break
                                    if bt_ccy == deal["buy_ccy"] and bt_cr > 0:
                                        if abs(bt_cr - deal["amount"]) <= Decimal("100.00"):
                                            matched_txn = bt
                                            break
                                else:
                                    if bt_ccy == deal["sell_ccy"] and counter_expected and bt_cr > 0:
                                        if abs(bt_cr - counter_expected) <= counter_expected * Decimal("0.025"):
                                            matched_txn = bt
                                            break

                        if matched_txn is not None:
                            used_txn_ids.add(matched_txn.id)
                            bt_ccy = (matched_txn.currency or "EGP").upper()
                            bt_amt = Decimal(str(matched_txn.debit_amount or 0)) or Decimal(str(matched_txn.credit_amount or 0))
                            expected_cmp = counter_expected if (counter_expected and bt_ccy == deal["sell_ccy"]) else deal["amount"]
                            expected_leg_amt = expected_cmp
                            expected_leg_ccy = bt_ccy

                            # Evaluate rate/fee slippage!
                            if deal["direction"] == "Buy" and bt_ccy == deal["sell_ccy"] and counter_expected:
                                slippage = bt_amt - counter_expected  # Paid more sell_ccy than agreed!
                            elif deal["direction"] == "Sell" and bt_ccy == deal["sell_ccy"] and counter_expected:
                                slippage = counter_expected - bt_amt  # Received less sell_ccy than agreed!
                            else:
                                slippage = abs(bt_amt - expected_cmp)

                            v_dt = _naive_dt(matched_txn.value_date) or _naive_dt(matched_txn.booking_date)
                            lag_d = max(0, (v_dt.date() - val_dt.date()).days) if (v_dt and val_dt) else 0

                            if slippage > Decimal("10.00"):
                                total_deal_slippage += slippage
                                s_status = "SETTLED_RATE_OR_FEE_DISCREPANCY"
                                implied_rate = float(round(bt_amt / deal["amount"], 6)) if deal["amount"] > 0 else 0.0
                                s_note = (
                                    f"Settled {_fmt_money(bt_amt, bt_ccy)} (implied {implied_rate:.4f}) vs. agreed "
                                    f"{float(w_rate or 0):.4f} ({_fmt_money(expected_cmp, bt_ccy)}) — "
                                    f"claimable slippage: {_fmt_money(slippage, bt_ccy)}."
                                )
                                slip_val = float(round(slippage, 2))
                            elif lag_d >= 1:
                                s_status = "SETTLED_VALUE_DATE_DELAY"
                                s_note = f"Principal & rate verified, but bank value-dated {lag_d}d after agreed {_fmt_dt(val_dt)}."
                                slip_val = 0.0
                            else:
                                s_status = "SETTLED_VERIFIED"
                                s_note = f"Exact FX settlement verified on bank statement ({_fmt_money(bt_amt, bt_ccy)})."
                                slip_val = 0.0

                            deal_audit_rows.append({
                                "rfq_id": deal["rfq_id"],
                                "ref_no": deal["ref_no"],
                                "trade_type": f"FX_SPOT ({deal['direction']} {deal['buy_ccy']}/{deal['sell_ccy']})",
                                "winner_bank_name": deal["winner_bank_name"],
                                "agreed_value_date": _fmt_dt(val_dt),
                                "actual_settlement_date": _fmt_dt(v_dt),
                                "agreed_rate": float(w_rate) if w_rate is not None else None,
                                "expected_settlement_amount": float(round(expected_leg_amt, 2)),
                                "actual_bank_amount": float(round(bt_amt, 2)),
                                "slippage_or_discrepancy": slip_val,
                                "currency": expected_leg_ccy,
                                "settlement_status": s_status,
                                "audit_note": s_note,
                            })
                            continue
                    else:
                        # TBILL / Money Market Placement Settlement Check
                        for bt in bank_txns:
                            bt_norm = _norm_ref(f"{bt.raw_description or ''} {bt.e2e_id or ''} {bt.back_office_ref or ''}")
                            bt_dr = Decimal(str(bt.debit_amount or 0))
                            b_dt = _naive_dt(bt.booking_date)
                            if (ref_norm and len(ref_norm) >= 5 and ref_norm in bt_norm) or (
                                bt_dr > 0
                                and (not val_dt or not b_dt or abs((b_dt.date() - val_dt.date()).days) <= 7)
                                and bt_dr >= deal["amount"] * Decimal("0.75")
                                and bt_dr <= deal["amount"] * Decimal("1.02")
                            ):
                                matched_txn = bt
                                break

                        if matched_txn is not None:
                            used_txn_ids.add(matched_txn.id)
                            bt_ccy = (matched_txn.currency or "EGP").upper()
                            bt_amt = Decimal(str(matched_txn.debit_amount or 0)) or Decimal(str(matched_txn.credit_amount or 0))
                            v_dt = _naive_dt(matched_txn.value_date) or _naive_dt(matched_txn.booking_date)
                            deal_audit_rows.append({
                                "rfq_id": deal["rfq_id"],
                                "ref_no": deal["ref_no"],
                                "trade_type": "TBILL / MONEY_MARKET",
                                "winner_bank_name": deal["winner_bank_name"],
                                "agreed_value_date": _fmt_dt(val_dt),
                                "actual_settlement_date": _fmt_dt(v_dt),
                                "agreed_rate": float(w_rate) if w_rate is not None else None,
                                "expected_settlement_amount": float(round(deal["amount"], 2)),
                                "actual_bank_amount": float(round(bt_amt, 2)),
                                "slippage_or_discrepancy": 0.0,
                                "currency": bt_ccy,
                                "settlement_status": "SETTLED_VERIFIED",
                                "audit_note": f"T-Bill / MM placement debit ({_fmt_money(bt_amt, bt_ccy)}) verified on statement.",
                            })
                            continue

                    # Unmatched Awarded Deal -> Determine if OVERDUE vs AWAITING STATEMENT
                    latest_stmt_dt = latest_stmt_end_by_bank.get(w_bank)
                    is_overdue = bool(val_dt and latest_stmt_dt and latest_stmt_dt.date() > val_dt.date())
                    deal_audit_rows.append({
                        "rfq_id": deal["rfq_id"],
                        "ref_no": deal["ref_no"],
                        "trade_type": f"{t_type} ({deal['buy_ccy']}/{deal['sell_ccy']})" if t_type == "FX_SPOT" else "TBILL / MONEY_MARKET",
                        "winner_bank_name": deal["winner_bank_name"],
                        "agreed_value_date": _fmt_dt(val_dt),
                        "actual_settlement_date": "—",
                        "agreed_rate": float(w_rate) if w_rate is not None else None,
                        "expected_settlement_amount": float(round(expected_leg_amt, 2)),
                        "actual_bank_amount": 0.0,
                        "slippage_or_discrepancy": 0.0,
                        "currency": expected_leg_ccy,
                        "settlement_status": "OVERDUE_UNSETTLED_DEAL" if is_overdue else "AWAITING_STATEMENT_OR_VALUE_DATE",
                        "audit_note": (
                            f"Awarded deal value date ({_fmt_dt(val_dt)}) passed before statement end ({_fmt_dt(latest_stmt_dt)}) with NO settlement found!"
                            if is_overdue
                            else "Awaiting settlement value date or next bank statement upload."
                        ),
                    })
            except Exception as q_err:
                logger.debug(f"Quotation settlement audit skipped: {q_err}")

        # ==============================================================================
        # ENGINE 3: IDLE CASH -> 1-CLICK PRE-FILLED T-BILL / MM RFQ OPPORTUNITIES
        # ==============================================================================
        cash_intel = reconciliation_cash_intelligence_service.build_cash_intelligence_report(
            db=db,
            customer_id=customer_id,
            currency=target_ccy,
            bank_id=bank_id,
            minimum_cash_buffer=minimum_cash_buffer,
            benchmark_annual_rate_pct=benchmark_annual_rate_pct,
        )
        lf = cash_intel["liquidity_forecast"]
        sel_ccy = cash_intel["selected_currency"]
        excess_30d = Decimal(str(lf.get("deployable_excess_cash_30d") or 0))
        annual_rate = Decimal(str(benchmark_annual_rate_pct or 20.0)) / Decimal("100.0")

        idle_cash_opportunities: List[Dict[str, Any]] = []
        if excess_30d > Decimal("0.00"):
            # Round down to clean 10,000 tranche when >= 50,000, else exact
            tranche = (
                (excess_30d // Decimal("10000")) * Decimal("10000")
                if excess_30d >= Decimal("50000")
                else excess_30d
            )
            yield_30d = tranche * annual_rate * (Decimal("30") / Decimal("365"))
            yield_91d = tranche * annual_rate * (Decimal("91") / Decimal("365"))
            today_d = datetime.now(timezone.utc).date()
            settle_d = (today_d + timedelta(days=1)).strftime("%Y-%m-%d")
            mat_91d = (today_d + timedelta(days=92)).strftime("%Y-%m-%d")

            idle_cash_opportunities.append({
                "currency": sel_ccy,
                "verified_30d_stress_floor_above_buffer": float(round(excess_30d, 2)),
                "recommended_rfq_tranche": float(round(tranche, 2)),
                "benchmark_yield_rate_pct": float(benchmark_annual_rate_pct or 20.0),
                "estimated_30d_interest_gain": float(round(yield_30d, 2)),
                "estimated_91d_interest_gain": float(round(yield_91d, 2)),
                "suggested_settlement_date": settle_d,
                "suggested_maturity_date": mat_91d,
                "prefill_url_query": (
                    f"prefill_type=TBILL&prefill_amount={int(tranche)}&prefill_currency={sel_ccy}"
                    f"&prefill_settlement={settle_d}&prefill_maturity={mat_91d}"
                ),
            })

        overcharge_cnt = sum(
            1 for r in lg_audit_rows if r["audit_status"] in ("COMMISSION_OVERCHARGE_CLAIM", "EXCESS_CASH_MARGIN_HELD")
        )
        unsettled_or_slip_cnt = sum(
            1 for r in deal_audit_rows
            if r["settlement_status"] in ("SETTLED_RATE_OR_FEE_DISCREPANCY", "OVERDUE_UNSETTLED_DEAL", "SETTLED_VALUE_DATE_DELAY")
        )
        total_recoverable = total_lg_claimable + total_deal_slippage

        return {
            "company_id": customer_id,
            "company_name": company_name,
            "selected_currency": sel_ccy,
            "summary_kpis": {
                "lg_movements_audited": len(lg_audit_rows),
                "lg_tariff_or_margin_breaches": overcharge_cnt,
                "shadow_unregistered_lgs_count": len(shadow_lg_rows),
                "awarded_rfq_deals_audited": len(deal_audit_rows),
                "rfq_settlement_exceptions": unsettled_or_slip_cnt,
                "total_claimable_bank_recovery": float(round(total_recoverable, 2)),
                "deployable_idle_cash_tranche": (
                    idle_cash_opportunities[0]["recommended_rfq_tranche"] if idle_cash_opportunities else 0.0
                ),
                "estimated_91d_yield_opportunity": (
                    idle_cash_opportunities[0]["estimated_91d_interest_gain"] if idle_cash_opportunities else 0.0
                ),
            },
            "lg_facility_audit": lg_audit_rows,
            "shadow_lgs": shadow_lg_rows,
            "quotation_settlement_audit": deal_audit_rows,
            "idle_cash_opportunities": idle_cash_opportunities,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def render_excel_bytes(self, report: Dict[str, Any]) -> bytes:
        import pandas as pd

        def _safe_df(rows_list: List[Dict[str, Any]], empty_msg: str) -> Any:
            if not rows_list:
                return pd.DataFrame([{"Status": empty_msg}])
            cleaned = [{k: sanitize_spreadsheet_cell(v) for k, v in r.items()} for r in rows_list]
            return pd.DataFrame(cleaned)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            _safe_df(
                report["lg_facility_audit"], "No LG commission or cash-margin statement lines in scope"
            ).to_excel(writer, index=False, sheet_name="1_LG_Tariff_Margin_Audit")
            _safe_df(
                report["shadow_lgs"], "Zero unregistered shadow LGs detected on bank statements"
            ).to_excel(writer, index=False, sheet_name="2_Shadow_Unregistered_LGs")
            _safe_df(
                report["quotation_settlement_audit"], "No awarded FX / T-Bill quotation deals in scope"
            ).to_excel(writer, index=False, sheet_name="3_RFQ_Deal_Settlement_Audit")
        buf.seek(0)
        return buf.getvalue()

    def render_pdf_html(self, report: Dict[str, Any]) -> str:
        comp = html.escape(report["company_name"])
        ccy = html.escape(report["selected_currency"])
        k = report["summary_kpis"]

        lg_trs = (
            "".join(
                f'<tr>'
                f'<td>{html.escape(r["booking_date"])}</td>'
                f'<td>{html.escape(r["bank_name"])}</td>'
                f'<td><strong>{html.escape(r["lg_ref_number"])}</strong></td>'
                f'<td>{html.escape(r["event_type"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(r["bank_amount"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(r["contractual_expected_amount"])}</td>'
                f'<td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(r["claimable_variance"])}</td>'
                f'<td>{html.escape(r["audit_status"])}</td>'
                f'</tr>'
                for r in report["lg_facility_audit"]
            )
            if report["lg_facility_audit"]
            else '<tr><td colspan="8" style="text-align:center;color:#94a3b8;font-style:italic;">No registered LG fee or margin movements in scope.</td></tr>'
        )

        sh_trs = (
            "".join(
                f'<tr>'
                f'<td>{html.escape(r["booking_date"])}</td>'
                f'<td>{html.escape(r["bank_name"])}</td>'
                f'<td><strong>{html.escape(r["suspected_lg_ref"])}</strong></td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(r["amount"], r["currency"])}</td>'
                f'<td>{html.escape(r["raw_description"])}</td>'
                f'</tr>'
                for r in report["shadow_lgs"]
            )
            if report["shadow_lgs"]
            else '<tr><td colspan="5" style="text-align:center;color:#94a3b8;font-style:italic;">Zero shadow / unregistered LG debits detected.</td></tr>'
        )

        rfq_trs = (
            "".join(
                f'<tr>'
                f'<td><strong>{html.escape(r["ref_no"])}</strong></td>'
                f'<td>{html.escape(r["trade_type"])}</td>'
                f'<td>{html.escape(r["winner_bank_name"])}</td>'
                f'<td>{html.escape(r["agreed_value_date"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(r["expected_settlement_amount"], r["currency"])}</td>'
                f'<td style="text-align:right;font-family:monospace;">{_fmt_money(r["actual_bank_amount"], r["currency"])}</td>'
                f'<td style="text-align:right;font-family:monospace;font-weight:700;">{_fmt_money(r["slippage_or_discrepancy"], r["currency"])}</td>'
                f'<td>{html.escape(r["settlement_status"])}</td>'
                f'</tr>'
                for r in report["quotation_settlement_audit"]
            )
            if report["quotation_settlement_audit"]
            else '<tr><td colspan="8" style="text-align:center;color:#94a3b8;font-style:italic;">No awarded RFQ deals in scope.</td></tr>'
        )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Closed-Loop Treasury Audit &amp; Bank Claim Pack — {comp}</title>
<style>
  @page {{
    size: A4 landscape;
    margin: 12mm 13mm 14mm 13mm;
    @bottom-left {{ content: "Grow Corporate Treasury · Closed-Loop LG Tariff & RFQ Deal Settlement Audit"; font-size: 7.5pt; color: #64748b; }}
    @bottom-right {{ content: "Page " counter(page) " of " counter(pages); font-size: 7.5pt; color: #64748b; }}
  }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; font-size: 8pt; color: #0f172a; margin: 0; }}
  .hdr {{ background: #0f172a; color: #fff; padding: 12px 15px; border-radius: 7px; margin-bottom: 10px; }}
  .hdr h1 {{ margin: 0; font-size: 12.5pt; }}
  .hdr p {{ margin: 3px 0 0 0; color: #94a3b8; font-size: 8pt; }}
  .kpi {{ width: 100%; border-collapse: collapse; margin-bottom: 11px; }}
  .kpi td {{ width: 25%; border: 1px solid #cbd5e1; background: #f8fafc; padding: 6px 9px; }}
  .lbl {{ font-size: 7pt; text-transform: uppercase; color: #64748b; font-weight: 700; }}
  .val {{ font-size: 10pt; font-weight: 800; font-family: monospace; margin-top: 2px; }}
  h3 {{ font-size: 9pt; margin: 10px 0 4px 0; border-bottom: 1.5px solid #cbd5e1; padding-bottom: 2px; page-break-after: avoid; }}
  table.grid {{ width: 100%; border-collapse: collapse; margin-bottom: 9px; font-size: 7.6pt; }}
  table.grid th {{ background: #eff6ff; color: #1e3a8a; border: 1px solid #bfdbfe; padding: 4.5px 6px; text-align: left; }}
  table.grid td {{ border: 1px solid #e2e8f0; padding: 4px 6px; }}
</style>
</head>
<body>
  <div class="hdr">
    <h1>Closed-Loop Treasury Audit &amp; Bank Tariff Claim Pack</h1>
    <p>{comp} &nbsp;·&nbsp; Currency Scope: {ccy} &nbsp;·&nbsp; LG Facility Tariffs, Shadow Guarantees &amp; RFQ Deal Settlement Verification</p>
  </div>

  <table class="kpi">
    <tr>
      <td><div class="lbl">Claimable Bank Recovery</div><div class="val">{_fmt_money(k["total_claimable_bank_recovery"], ccy)}</div></td>
      <td><div class="lbl">LG Tariff / Margin Breaches</div><div class="val">{k["lg_tariff_or_margin_breaches"]} of {k["lg_movements_audited"]}</div></td>
      <td><div class="lbl">Shadow Unregistered LGs</div><div class="val">{k["shadow_unregistered_lgs_count"]}</div></td>
      <td><div class="lbl">RFQ Settlement Exceptions</div><div class="val">{k["rfq_settlement_exceptions"]} of {k["awarded_rfq_deals_audited"]}</div></td>
    </tr>
  </table>

  <h3>1. LG Facility Commission &amp; Cash-Margin Tariff Audit</h3>
  <table class="grid">
    <thead><tr><th>Date</th><th>Bank</th><th>LG Ref</th><th>Event</th><th style="text-align:right;">Bank Amount</th><th style="text-align:right;">Contractual</th><th style="text-align:right;">Claimable</th><th>Status</th></tr></thead>
    <tbody>{lg_trs}</tbody>
  </table>

  <h3>2. Shadow / Unregistered LG Debits Detected on Bank Statements</h3>
  <table class="grid">
    <thead><tr><th>Date</th><th>Bank</th><th>Suspected LG Ref</th><th style="text-align:right;">Amount</th><th>Raw Statement Narrative</th></tr></thead>
    <tbody>{sh_trs}</tbody>
  </table>

  <h3>3. Awarded FX &amp; T-Bill RFQ Deal Settlement Verification</h3>
  <table class="grid">
    <thead><tr><th>RFQ Ref</th><th>Deal Type</th><th>Winning Bank</th><th>Value Date</th><th style="text-align:right;">Expected</th><th style="text-align:right;">Actual Bank</th><th style="text-align:right;">Discrepancy</th><th>Status</th></tr></thead>
    <tbody>{rfq_trs}</tbody>
  </table>
</body>
</html>"""


reconciliation_treasury_bridge_service = ReconciliationTreasuryBridgeService()
