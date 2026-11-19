from typing import List, Any, Optional, Dict, Tuple
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, Body, Request, UploadFile, File, Form
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from datetime import date, datetime
import io
import json
import logging

logger = logging.getLogger(__name__)

from app.database import get_db
from app.core.security import get_current_corporate_admin_context, get_current_approver_context, get_current_treasury_context, get_issuance_read_context, check_subscription_status, TokenData
from app.core.document_generator import generate_pdf_from_html
from app.core.encryption import encrypt_data 

# Models
from app.models.models import Bank, Currency 
from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility, IssuanceWorkflowPolicy, CustomerFormConfiguration, IssuanceRequestSnapshot, IssuanceRequestVersion, AdminChangeRequest, BankFormIssueReport
# NOTE: Ensure you created app/models/models_reconciliation.py first!
from app.models.models_reconciliation import BankPositionBatch, BankPositionRow 
# Schemas
from app.schemas.all_schemas import BankOut, CurrencyOut 
from app.schemas.schemas_issuance import (
    IssuanceRequestCreate, IssuanceRequestOut, IssuanceRequestUpdate, IssuanceRequestDraftCreate,
    CustomerFormConfigurationCreateUpdate, IssuanceRequestVersionOut,
    IssuanceFacilityCreate, IssuanceFacilityOut, SuitableFacilityOut, 
    IssuanceRequestContentUpdate, IssuanceFacilityUpdate,
    IssuedLGRecordOut, IssuedLGRecordDetailOut,
    IssuanceExecuteRequest, IssuanceCancelRequest,
    ReconciliationRequest, ReconciliationResult,
    IssuanceWorkflowPolicyCreate, IssuanceWorkflowPolicyOut,
    BankIssuanceOptionOut, BankIssuanceOptionCreateUpdate,
    AdminChangeRequestCreate, AdminChangeRequestOut, AdminChangeRequestAction,
    BankFormIssueReportCreate, BankFormIssueReportOut, BankFormIssueReportUpdate
)
from app.services.issuance_service import issuance_service

# CRUD
from app.crud.crud_issuance import crud_issuance_request
from app.crud.crud_facility import crud_facility
from app.crud.crud_bank_methods import crud_bank_methods
from fastapi.responses import StreamingResponse

router = APIRouter()

from .base import *
from .base import _read_bank_form_pdf_bytes, _send_edit_notifications, _detect_coverage_gaps, _make_doc_filename, _get_lg_copy_docs, _send_requestor_status_notification, _serialize_action, _serialize_recon_session, _serialize_recon_result, _apply_admin_change, _create_governed_change

@router.get("/issued-lgs/export-fields")
def get_export_fields(
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Returns available columns and presets for the custom export builder."""
    from app.services.issuance_export_service import IssuanceExportService, PRESET_SUMMARY_COLS, PRESET_FINANCIAL_COLS, PRESET_OPERATIONS_COLS, PRESET_DETAILED_COLS

    fields = IssuanceExportService.get_available_columns()
    presets = {
        "summary": PRESET_SUMMARY_COLS,
        "financial": PRESET_FINANCIAL_COLS,
        "operations": PRESET_OPERATIONS_COLS,
        "detailed": PRESET_DETAILED_COLS,
    }
    return {"fields": fields, "presets": presets}


@router.get("/issued-lgs/export")
def export_issued_lgs(
    export_type: str = Query("summary", description="summary | detailed | full_audit | custom"),
    status_filter: Optional[str] = Query(None, description="Filter by status, e.g. ACTIVE"),
    search: Optional[str] = Query(None, description="Search LG ref or beneficiary"),
    columns: Optional[str] = Query(None, description="Comma-separated column keys for custom export"),
    include_event_log: bool = Query(False, description="Include dedicated Lifecycle Event Log tab"),
    include_approvals: bool = Query(False, description="Include dedicated Approvals Audit tab"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """Export Issued LGs to professional formatted XLSX with full lifecycle history. Supports custom column selection and arranging."""
    from app.services.issuance_export_service import IssuanceExportService
    from fastapi.responses import StreamingResponse

    custom_cols_list = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    buf = IssuanceExportService.generate_excel_export(
        db=db,
        customer_id=current_user.customer_id,
        user_email=getattr(current_user, "email", "Treasury User"),
        export_type=export_type,
        status_filter=status_filter,
        search=search,
        custom_columns=custom_cols_list,
        include_event_log=include_event_log,
        include_approvals=include_approvals,
    )

    filename = f"issued_lgs_{export_type}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


def _safe_date(val: Any) -> Optional[date]:
    """Safely normalize datetime/date/ISO-string to a date object."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return datetime.strptime(val[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _build_issuance_fx_converter(
    db: Session,
    facilities: List[IssuanceFacility],
    lgs: List[IssuedLGRecord],
    requested_base_code: Optional[str] = None,
):
    """
    Determines the reporting base currency from actual tenant facilities/LGs and builds a fast
    cached converter using CBE daily rates (allow_ai=False for deterministic, sub-millisecond reads).
    """
    from decimal import Decimal
    from collections import Counter
    from app.services.fx_service import fx_service

    ccy_map: Dict[int, Currency] = {}
    for c in db.query(Currency).filter(Currency.is_deleted == False).all():
        ccy_map[c.id] = c
    code_to_ccy: Dict[str, Currency] = {c.iso_code.upper(): c for c in ccy_map.values() if c.iso_code}

    portfolio_codes_counter: Counter = Counter()
    for f in facilities:
        if f.currency_id and f.currency_id in ccy_map:
            portfolio_codes_counter[ccy_map[f.currency_id].iso_code.upper()] += 2
    for lg in lgs:
        if lg.currency_id and lg.currency_id in ccy_map:
            portfolio_codes_counter[ccy_map[lg.currency_id].iso_code.upper()] += 1

    available_currencies = sorted(portfolio_codes_counter.keys())
    if "EGP" in code_to_ccy and "EGP" not in available_currencies and len(available_currencies) > 1:
        available_currencies.insert(0, "EGP")
    elif not available_currencies:
        available_currencies = ["EGP"] if "EGP" in code_to_ccy else (list(code_to_ccy.keys())[:1] or ["EGP"])

    chosen_ccy: Optional[Currency] = None
    if requested_base_code and requested_base_code.strip().upper() in code_to_ccy:
        chosen_ccy = code_to_ccy[requested_base_code.strip().upper()]
    elif len(portfolio_codes_counter) == 1:
        only_code = next(iter(portfolio_codes_counter.keys()))
        chosen_ccy = code_to_ccy.get(only_code)
    elif "EGP" in portfolio_codes_counter and "EGP" in code_to_ccy:
        chosen_ccy = code_to_ccy["EGP"]
    elif portfolio_codes_counter:
        top_code = portfolio_codes_counter.most_common(1)[0][0]
        chosen_ccy = code_to_ccy.get(top_code)
    else:
        chosen_ccy = code_to_ccy.get("EGP") or (next(iter(ccy_map.values())) if ccy_map else None)

    base_ccy_id = chosen_ccy.id if chosen_ccy else None
    base_ccy_code = chosen_ccy.iso_code.upper() if chosen_ccy and chosen_ccy.iso_code else "EGP"

    rate_cache: Dict[int, Optional[Decimal]] = {}
    fx_rates_applied: Dict[str, float] = {}
    unconverted_codes: set = set()

    def to_base(amount_val: Any, from_ccy_id: Optional[int]) -> float:
        amt = float(amount_val or 0)
        if amt == 0 or not from_ccy_id or not base_ccy_id or from_ccy_id == base_ccy_id:
            return amt
        from_c = ccy_map.get(from_ccy_id)
        from_code = from_c.iso_code.upper() if from_c and from_c.iso_code else f"CCY#{from_ccy_id}"
        if from_code == base_ccy_code:
            return amt
        if from_ccy_id not in rate_cache:
            rate_cache[from_ccy_id] = fx_service.get_rate(
                db, from_ccy_id, base_ccy_id, allow_ai=False
            )
        r = rate_cache[from_ccy_id]
        if r is not None:
            r_float = float(r)
            fx_rates_applied[from_code] = round(r_float, 6)
            return amt * r_float
        unconverted_codes.add(from_code)
        return amt

    return {
        "base_currency": base_ccy_code,
        "base_currency_id": base_ccy_id,
        "available_currencies": available_currencies,
        "ccy_map": ccy_map,
        "to_base": to_base,
        "fx_rates_applied": fx_rates_applied,
        "unconverted_codes": unconverted_codes,
    }


def _extract_lg_pricing_and_margin(lg: IssuedLGRecord) -> Dict[str, Any]:
    """
    Extracts strictly factual cash margin & commission pricing for an IssuedLGRecord
    from either manual_pricing (for non-facility issuances) or the linked facility sub-limit.
    Never invents rates when none are configured.
    """
    amt = float(lg.current_amount or lg.bank_lg_amount or 0)
    manual = lg.manual_pricing if isinstance(lg.manual_pricing, dict) else {}
    sub = lg.sub_limit
    fac = sub.facility if sub else None

    margin_pct: Optional[float] = None
    margin_amt_native = 0.0
    if manual.get("margin_amount") is not None and str(manual.get("margin_amount")).strip() != "":
        try:
            margin_amt_native = max(0.0, float(manual["margin_amount"]))
            margin_pct = round((margin_amt_native / amt) * 100.0, 2) if amt > 0 else 0.0
        except (ValueError, TypeError):
            margin_amt_native = 0.0
    elif manual.get("margin_pct") is not None and str(manual.get("margin_pct")).strip() != "":
        try:
            margin_pct = max(0.0, float(manual["margin_pct"]))
            margin_amt_native = amt * (margin_pct / 100.0)
        except (ValueError, TypeError):
            margin_pct = None
    elif sub is not None and sub.default_cash_margin_pct is not None:
        margin_pct = max(0.0, float(sub.default_cash_margin_pct))
        margin_amt_native = amt * (margin_pct / 100.0)
    elif fac is not None and fac.facility_default_margin_pct is not None:
        margin_pct = max(0.0, float(fac.facility_default_margin_pct))
        margin_amt_native = amt * (margin_pct / 100.0)

    comm_rate_pct: Optional[float] = None
    min_comm_fac = 0.0
    flat_fee_fac = 0.0
    pricing_ccy_id = lg.currency_id
    has_commission_pricing = False

    if manual.get("commission_rate") is not None and str(manual.get("commission_rate")).strip() != "":
        try:
            comm_rate_pct = max(0.0, float(manual["commission_rate"]))
            flat_fee_fac = float(manual.get("flat_fee") or 0)
            has_commission_pricing = (comm_rate_pct > 0 or flat_fee_fac > 0)
        except (ValueError, TypeError):
            comm_rate_pct = None
    elif sub is not None and (
        (sub.default_commission_rate is not None and float(sub.default_commission_rate) > 0)
        or (sub.default_flat_fee is not None and float(sub.default_flat_fee) > 0)
    ):
        comm_rate_pct = max(0.0, float(sub.default_commission_rate or 0))
        min_comm_fac = max(0.0, float(sub.default_min_commission or 0))
        flat_fee_fac = max(0.0, float(sub.default_flat_fee or 0))
        pricing_ccy_id = fac.currency_id if fac and fac.currency_id else lg.currency_id
        has_commission_pricing = True

    return {
        "amount_native": amt,
        "margin_pct": margin_pct,
        "margin_amount_native": margin_amt_native,
        "is_cash_backed": margin_amt_native > 0.005,
        "has_commission_pricing": has_commission_pricing,
        "comm_rate_pct": comm_rate_pct,
        "min_comm_fac": min_comm_fac,
        "flat_fee_fac": flat_fee_fac,
        "pricing_ccy_id": pricing_ccy_id,
    }


@router.get("/dashboard-stats")
def get_dashboard_stats(
    base_currency: Optional[str] = Query(None, description="Optional reporting currency ISO code (e.g. EGP, USD)"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Executive LG Issuance Dashboard KPI & Telemetry Endpoint.
    Computes multi-currency normalized exposure, accurate facility & sub-limit utilization,
    locked cash margin collateral, real-time operational pipeline, concentration breakdowns,
    maturity horizon, and prioritized governance exceptions strictly from factual database records.
    """
    from datetime import datetime, timedelta, date as date_cls
    from decimal import Decimal
    from sqlalchemy.orm import selectinload, joinedload
    from app.models.models_issuance import (
        IssuanceMaintenanceAction,
        IssuanceExposureEntry,
        ReconciliationSession,
        ReconciliationResult,
        CorporateProject,
    )
    from app.services.sla_actualization_service import sla_actualization_service
    from app.services.business_calendar_service import business_calendar_service
    from app.crud.crud_config import crud_customer_configuration
    from app.constants import GlobalConfigKey, UserRole

    cust_id = current_user.customer_id
    today = date_cls.today()
    d7 = today + timedelta(days=7)
    d30 = today + timedelta(days=30)
    d60 = today + timedelta(days=60)
    d90 = today + timedelta(days=90)

    CONFIRMED_LIVE_STATUSES = {"ACTIVE", "LG_ISSUED", "PENDING_CLOSE", "CANCEL_REQUESTED"}
    IN_FLIGHT_STATUSES = {"INTERNAL_PROCESSING", "DELIVERED_TO_BANK", "SLA_EXCEEDED", "ISSUED"}

    # Delivery SLA config (same source of truth as post_issuance.py)
    delivery_sla_cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, cust_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE
    )
    delivery_sla_days = int((delivery_sla_cfg or {}).get("effective_value", 3) or 3)

    # Load all active facilities with sub_limits, entities, bank, currency
    has_full_entities = (
        current_user.role in (UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER)
        or getattr(current_user, "has_all_entity_access", True) is not False
    )
    allowed_entity_ids = set(current_user.entity_ids or []) if not has_full_entities else None

    facilities: List[IssuanceFacility] = (
        db.query(IssuanceFacility)
        .options(
            selectinload(IssuanceFacility.sub_limits),
            selectinload(IssuanceFacility.entities),
            joinedload(IssuanceFacility.bank),
            joinedload(IssuanceFacility.currency),
        )
        .filter(
            IssuanceFacility.customer_id == cust_id,
            IssuanceFacility.is_deleted == False,
            IssuanceFacility.deleted_at == None,
            IssuanceFacility.status.in_(["ACTIVE", "SUSPENDED", "FX_SUSPENDED"]),
        )
        .all()
    )
    if allowed_entity_ids is not None:
        facilities = [
            f for f in facilities
            if not f.entities or any(e.id in allowed_entity_ids for e in f.entities)
        ]

    # Load all IssuedLGRecords for this customer with eager relationships
    lg_query = (
        db.query(IssuedLGRecord)
        .options(
            joinedload(IssuedLGRecord.bank),
            joinedload(IssuedLGRecord.currency),
            joinedload(IssuedLGRecord.lg_type),
            joinedload(IssuedLGRecord.issuing_entity),
            joinedload(IssuedLGRecord.sub_limit).joinedload(IssuanceFacilitySubLimit.facility),
        )
        .filter(IssuedLGRecord.customer_id == cust_id)
    )
    if allowed_entity_ids is not None:
        lg_query = lg_query.filter(IssuedLGRecord.issuing_entity_id.in_(allowed_entity_ids))
    all_lgs: List[IssuedLGRecord] = lg_query.order_by(IssuedLGRecord.created_at.desc()).all()

    # Load non-deleted IssuanceRequests for this customer
    req_query = (
        db.query(IssuanceRequest)
        .options(
            joinedload(IssuanceRequest.currency),
            joinedload(IssuanceRequest.lg_type),
            joinedload(IssuanceRequest.issuing_entity),
        )
        .filter(
            IssuanceRequest.customer_id == cust_id,
            IssuanceRequest.is_deleted == False,
        )
    )
    if allowed_entity_ids is not None:
        req_query = req_query.filter(IssuanceRequest.issuing_entity_id.in_(allowed_entity_ids))
    all_requests: List[IssuanceRequest] = req_query.all()

    # Load non-deleted Maintenance Actions for this customer
    maint_query = (
        db.query(IssuanceMaintenanceAction)
        .join(IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id)
        .options(joinedload(IssuanceMaintenanceAction.issued_lg))
        .filter(
            IssuedLGRecord.customer_id == cust_id,
            IssuanceMaintenanceAction.is_deleted == False,
        )
    )
    if allowed_entity_ids is not None:
        maint_query = maint_query.filter(IssuedLGRecord.issuing_entity_id.in_(allowed_entity_ids))
    all_maint_actions: List[IssuanceMaintenanceAction] = (
        maint_query.order_by(IssuanceMaintenanceAction.created_at.desc()).all()
    )

    # Build FX converter
    fx_ctx = _build_issuance_fx_converter(db, facilities, all_lgs, base_currency)
    base_ccy = fx_ctx["base_currency"]
    ccy_map = fx_ctx["ccy_map"]
    to_base = fx_ctx["to_base"]

    # Business calendar pre-fetch for accurate bank SLA breach checks
    weekend_days = business_calendar_service.get_weekend_days(db)
    min_deliv_yr = min(
        [lg.delivery_date.year for lg in all_lgs if lg.delivery_date],
        default=today.year - 1,
    )
    holidays_set = business_calendar_service.get_holiday_dates(
        db, min(min_deliv_yr, today.year - 1), today.year + 1
    )

    def calc_biz_days(sd: Optional[date], ed: Optional[date]) -> int:
        if not sd or not ed or ed <= sd:
            return 0
        cur = sd
        b_days = 0
        while cur < ed:
            cur += timedelta(days=1)
            if cur.weekday() not in weekend_days and cur not in holidays_set:
                b_days += 1
        return b_days

    # Partition LGs
    live_lgs = [lg for lg in all_lgs if (lg.status or "").upper() in CONFIRMED_LIVE_STATUSES]
    in_flight_lgs = [lg for lg in all_lgs if (lg.status or "").upper() in IN_FLIGHT_STATUSES]

    # --- 1. Exposure & Collateral Summary ---
    total_active_lgs = len(live_lgs)
    total_active_base = 0.0
    ccy_buckets: Dict[str, Dict[str, Any]] = {}

    locked_margin_base = 0.0
    margin_ccy_buckets: Dict[str, float] = {}
    cash_backed_count = 0
    unsecured_count = 0
    cross_border_count = 0
    non_operative_count = 0
    third_party_count = 0

    for lg in live_lgs:
        amt = float(lg.current_amount or lg.bank_lg_amount or 0)
        ccy_code = lg.currency.iso_code.upper() if lg.currency and lg.currency.iso_code else "EGP"
        base_eqv = to_base(amt, lg.currency_id)
        total_active_base += base_eqv

        b = ccy_buckets.setdefault(ccy_code, {"currency": ccy_code, "amount": 0.0, "base_equivalent": 0.0, "count": 0})
        b["amount"] += amt
        b["base_equivalent"] += base_eqv
        b["count"] += 1

        pm = _extract_lg_pricing_and_margin(lg)
        if pm["is_cash_backed"]:
            cash_backed_count += 1
            m_base = to_base(pm["margin_amount_native"], lg.currency_id)
            locked_margin_base += m_base
            margin_ccy_buckets[ccy_code] = margin_ccy_buckets.get(ccy_code, 0.0) + pm["margin_amount_native"]
        else:
            unsecured_count += 1

        if lg.is_cross_border:
            cross_border_count += 1
        if (lg.operational_status or "").strip().lower() in ("non-operative", "non_operative", "non operative"):
            non_operative_count += 1
        if lg.is_third_party:
            third_party_count += 1

    by_currency_list = []
    for code, b in sorted(ccy_buckets.items(), key=lambda kv: kv[1]["base_equivalent"], reverse=True):
        by_currency_list.append({
            "currency": code,
            "amount": round(b["amount"], 2),
            "base_equivalent": round(b["base_equivalent"], 2),
            "count": b["count"],
            "share_pct": round((b["base_equivalent"] / total_active_base) * 100.0, 1) if total_active_base > 0 else 0.0,
        })

    in_flight_base = sum(
        to_base(float(lg.current_amount or 0), lg.currency_id) for lg in in_flight_lgs
    )

    weighted_avg_margin_pct = round((locked_margin_base / total_active_base) * 100.0, 2) if total_active_base > 0 else 0.0
    clean_credit_base = max(0.0, total_active_base - locked_margin_base)

    # --- 2. Facility Utilization & Capacity Summary (strictly in facility currency via engine) ---
    facility_utilization = []
    total_limit_base = 0.0
    total_utilized_base = 0.0
    total_reserved_base = 0.0
    high_util_count = 0
    fac_expiring_soon_count = 0
    active_fac_banks = set()

    # Preload active RESERVATION entries per sub_limit
    res_rows = (
        db.query(
            IssuanceExposureEntry.sub_limit_id,
            func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0),
        )
        .join(IssuanceFacility, IssuanceFacility.id == IssuanceExposureEntry.facility_id)
        .filter(
            IssuanceFacility.customer_id == cust_id,
            IssuanceExposureEntry.entry_type == "RESERVATION",
            IssuanceExposureEntry.is_active == True,
        )
        .group_by(IssuanceExposureEntry.sub_limit_id)
        .all()
    )
    reserved_by_sl: Dict[int, float] = {int(r[0]): float(r[1] or 0) for r in res_rows if r[0] is not None}

    active_facilities_only = [f for f in facilities if (f.status or "").upper() == "ACTIVE"]
    for fac in active_facilities_only:
        total_limit = float(fac.total_limit_amount or 0)
        if total_limit <= 0:
            continue
        active_fac_banks.add(fac.bank_id)
        fac_ccy_code = fac.currency.iso_code.upper() if fac.currency and fac.currency.iso_code else base_ccy

        utilized_fac_ccy = 0.0
        reserved_fac_ccy = 0.0
        sl_margins = []
        for sl in (fac.sub_limits or []):
            sl_used = float(issuance_service.compute_sub_limit_used_in_facility_ccy(db, fac, sl))
            utilized_fac_ccy += sl_used
            reserved_fac_ccy += reserved_by_sl.get(sl.id, 0.0)
            if sl.default_cash_margin_pct is not None:
                sl_margins.append(float(sl.default_cash_margin_pct))

        issued_only_fac_ccy = max(0.0, utilized_fac_ccy - reserved_fac_ccy)
        available_fac_ccy = max(0.0, total_limit - utilized_fac_ccy)
        used_pct = round((utilized_fac_ccy / total_limit) * 100.0, 1) if total_limit > 0 else 0.0
        if used_pct >= 80.0:
            high_util_count += 1

        days_to_fac_expiry = (fac.expiry_date - today).days if fac.expiry_date else None
        days_to_review = (fac.review_date - today).days if fac.review_date else None
        review_or_expiry_soon = bool(
            fac.review_required_flag
            or (days_to_fac_expiry is not None and days_to_fac_expiry <= 30)
            or (days_to_review is not None and days_to_review <= 30)
        )
        if review_or_expiry_soon:
            fac_expiring_soon_count += 1

        sla_meta = sla_actualization_service.compute_effective_sla(fac, fac.bank)
        eff_margin_pct = (
            float(fac.facility_default_margin_pct)
            if fac.facility_default_margin_pct is not None
            else (round(sum(sl_margins) / len(sl_margins), 1) if sl_margins else 0.0)
        )

        total_limit_base += to_base(total_limit, fac.currency_id)
        total_utilized_base += to_base(utilized_fac_ccy, fac.currency_id)
        total_reserved_base += to_base(reserved_fac_ccy, fac.currency_id)

        bank_name = fac.bank.name if fac.bank else f"Bank #{fac.bank_id}"
        bank_short = (fac.bank.short_name if fac.bank and fac.bank.short_name else bank_name)
        facility_utilization.append({
            "facility_id": fac.id,
            "facility_name": fac.facility_name,
            "bank": bank_name,
            "bank_short": bank_short,
            "total_limit": round(total_limit, 2),
            "utilized": round(utilized_fac_ccy, 2),
            "issued_utilized": round(issued_only_fac_ccy, 2),
            "reserved_utilized": round(reserved_fac_ccy, 2),
            "available": round(available_fac_ccy, 2),
            "used_pct": used_pct,
            "currency": fac_ccy_code,
            "sub_limits_count": len(fac.sub_limits or []),
            "agreed_sla_days": sla_meta["agreed_sla_days"],
            "actual_sla_days": sla_meta["actual_avg_sla_days"] if sla_meta["sample_size"] > 0 else None,
            "sla_commitment_pct": sla_meta["sla_commitment_pct"] if sla_meta["sample_size"] > 0 else None,
            "sla_sample_size": sla_meta["sample_size"],
            "slippage_risk": sla_meta["slippage_risk"],
            "default_margin_pct": eff_margin_pct,
            "expiry_date": fac.expiry_date.isoformat() if fac.expiry_date else None,
            "days_to_expiry": days_to_fac_expiry,
            "review_or_expiry_soon": review_or_expiry_soon,
        })

    facility_utilization.sort(key=lambda x: x["used_pct"], reverse=True)
    total_headroom_base = max(0.0, total_limit_base - total_utilized_base)
    overall_util_pct = round((total_utilized_base / total_limit_base) * 100.0, 1) if total_limit_base > 0 else 0.0

    # --- 3. Requests, Approvals, SLA Breaches & Pipeline Stages ---
    user_id_str = str(current_user.user_id)
    user_email_lower = (getattr(current_user, "email", "") or "").lower()

    req_pending_approval_list = [
        r for r in all_requests
        if (r.status or "").upper() in ("SUBMITTED", "PENDING_APPROVAL", "REVISION_REQUIRED", "EDIT_REQUESTED", "CANCELLATION_REQUESTED")
    ]
    pending_requests = len(req_pending_approval_list)

    my_req_approvals = sum(
        1 for r in all_requests
        if (r.status or "").upper() in ("PENDING_APPROVAL", "EDIT_REQUESTED", "CANCELLATION_REQUESTED")
        and (r.requestor_email or "").lower() != user_email_lower
        and (
            not r.pending_approver_users
            or user_id_str in [str(u) for u in (r.pending_approver_users or [])]
        )
    )
    my_maint_approvals = sum(
        1 for m in all_maint_actions
        if (m.status or "").upper() == "PENDING_APPROVAL"
        and m.initiated_by_user_id != current_user.user_id
        and (
            not m.pending_approver_users
            or user_id_str in [str(u) for u in (m.pending_approver_users or [])]
        )
    )
    my_admin_change_approvals = 0
    if current_user.role == UserRole.CORPORATE_ADMIN:
        my_admin_change_approvals = (
            db.query(func.count(AdminChangeRequest.id))
            .filter(
                AdminChangeRequest.customer_id == cust_id,
                AdminChangeRequest.status == "PENDING",
                AdminChangeRequest.requested_by_user_id != current_user.user_id,
                AdminChangeRequest.is_deleted == False,
            )
            .scalar()
            or 0
        )
    cancel_req_lgs = [lg for lg in all_lgs if (lg.status or "").upper() == "CANCEL_REQUESTED"]
    pending_approvals = my_req_approvals + my_maint_approvals + my_admin_change_approvals + len(cancel_req_lgs)

    org_pending_approvals = (
        sum(1 for r in all_requests if (r.status or "").upper() in ("PENDING_APPROVAL", "EDIT_REQUESTED", "CANCELLATION_REQUESTED"))
        + sum(1 for m in all_maint_actions if (m.status or "").upper() == "PENDING_APPROVAL")
        + len(cancel_req_lgs)
    )
    urgent_open_requests = sum(
        1 for r in all_requests
        if r.is_urgent and (r.status or "").upper() in ("SUBMITTED", "PENDING_APPROVAL", "APPROVED_INTERNAL", "FACILITY_RESERVED")
    )

    ready_to_execute_reqs = [
        r for r in all_requests
        if (r.status or "").upper() in ("APPROVED_INTERNAL", "FACILITY_RESERVED") and r.lg_record_id is None
    ]

    lgs_pending_delivery = [
        lg for lg in in_flight_lgs
        if lg.delivery_date is None and not lg.bank_reply_type
    ]
    maint_pending_delivery = [
        m for m in all_maint_actions
        if (m.status or "").upper() in ("APPROVED", "EXECUTED")
        and m.delivery_date is None
        and (m.letter_generated_path is not None or m.is_printed)
    ]

    lgs_at_bank = [
        lg for lg in in_flight_lgs
        if lg.delivery_date is not None and lg.bank_reply_date is None and (lg.bank_reply_type or "") != "LG_ISSUED"
    ]
    maint_at_bank = [
        m for m in all_maint_actions
        if (m.status or "").upper() in ("APPROVED", "EXECUTED")
        and m.delivery_date is not None
        and m.bank_reply_date is None
    ]
    pending_bank = len(lgs_at_bank) + len(lgs_pending_delivery)

    lgs_pending_verif_or_handover = [
        lg for lg in live_lgs
        if (lg.verification_status or "PENDING").upper() in ("PENDING", "DISCREPANCY", "DISCREPANCY_REJECTED")
        or lg.handover_date is None
    ]
    lgs_active_verified = [
        lg for lg in live_lgs
        if (lg.verification_status or "").upper() in ("MATCHED", "ACCEPTED") and lg.handover_date is not None
    ]

    role_prefix = "/end-user" if current_user.role == UserRole.END_USER else "/corporate-admin"
    approval_path = f"{role_prefix}/issuance/requests" if current_user.role == UserRole.END_USER else "/corporate-admin/approval-requests"

    pipeline_stages = [
        {
            "key": "pending_approval",
            "label": "Pending Approval",
            "sublabel": "Requests in governance",
            "count": len(req_pending_approval_list),
            "base_amount": round(sum(to_base(float(r.amount or 0), r.currency_id) for r in req_pending_approval_list), 2),
            "path": approval_path,
        },
        {
            "key": "ready_to_execute",
            "label": "Ready to Issue",
            "sublabel": "Approved, awaiting execution",
            "count": len(ready_to_execute_reqs),
            "base_amount": round(sum(to_base(float(r.amount or 0), r.currency_id) for r in ready_to_execute_reqs), 2),
            "path": f"{role_prefix}/issuance/requests",
        },
        {
            "key": "pending_delivery",
            "label": "Pending Delivery",
            "sublabel": f"{len(lgs_pending_delivery)} LGs · {len(maint_pending_delivery)} Maint.",
            "count": len(lgs_pending_delivery) + len(maint_pending_delivery),
            "base_amount": round(sum(to_base(float(lg.current_amount or 0), lg.currency_id) for lg in lgs_pending_delivery), 2),
            "path": f"{role_prefix}/action-center",
        },
        {
            "key": "at_bank",
            "label": "At Bank",
            "sublabel": "Awaiting bank confirmation",
            "count": len(lgs_at_bank) + len(maint_at_bank),
            "base_amount": round(sum(to_base(float(lg.current_amount or 0), lg.currency_id) for lg in lgs_at_bank), 2),
            "path": f"{role_prefix}/action-center",
        },
        {
            "key": "post_issuance",
            "label": "Verify & Handover",
            "sublabel": "Scan check / recipient sign-off",
            "count": len(lgs_pending_verif_or_handover),
            "base_amount": round(sum(to_base(float(lg.current_amount or 0), lg.currency_id) for lg in lgs_pending_verif_or_handover), 2),
            "path": f"{role_prefix}/issuance/issued-lgs",
        },
        {
            "key": "active_verified",
            "label": "Completed & Active",
            "sublabel": "Verified & handed over",
            "count": len(lgs_active_verified),
            "base_amount": round(sum(to_base(float(lg.current_amount or 0), lg.currency_id) for lg in lgs_active_verified), 2),
            "path": f"{role_prefix}/issuance/issued-lgs",
        },
    ]

    # --- 4. SLA Breaches & Exceptions Feed ---
    exceptions_feed: List[Dict[str, Any]] = []
    sla_breaches = 0

    # Check in-flight LG delivery & bank SLA breaches
    for lg in in_flight_lgs:
        created_d = _safe_date(lg.created_at)
        fac = lg.sub_limit.facility if (lg.sub_limit and lg.sub_limit.facility) else None
        agreed_bank_sla = int(fac.sla_agreement_days) if (fac and fac.sla_agreement_days) else 5
        bank_label = lg.bank.short_name or lg.bank.name if lg.bank else "Bank"

        if not lg.delivery_date and created_d:
            days_undelivered = (today - created_d).days
            if days_undelivered > delivery_sla_days:
                sla_breaches += 1
                exceptions_feed.append({
                    "severity": "HIGH" if days_undelivered > delivery_sla_days + 3 else "MEDIUM",
                    "category": "DELIVERY_SLA",
                    "title": f"{lg.lg_ref_number} — Undelivered to {bank_label}",
                    "detail": f"Executed {days_undelivered}d ago (SLA: {delivery_sla_days}d) · {lg.beneficiary_name}",
                    "days": days_undelivered,
                    "path": f"{role_prefix}/issuance/issued-lgs",
                })
        elif lg.delivery_date and not lg.bank_reply_date:
            biz_wait = calc_biz_days(lg.delivery_date, today)
            if (lg.status or "").upper() == "SLA_EXCEEDED" or biz_wait > agreed_bank_sla:
                sla_breaches += 1
                exceptions_feed.append({
                    "severity": "CRITICAL" if biz_wait > agreed_bank_sla + 3 else "HIGH",
                    "category": "BANK_SLA",
                    "title": f"{lg.lg_ref_number} — {bank_label} SLA Exceeded",
                    "detail": f"{biz_wait} business days at bank (Agreed SLA: {agreed_bank_sla}d)",
                    "days": biz_wait,
                    "path": f"{role_prefix}/issuance/issued-lgs",
                })
            elif (lg.bank_reply_type or "").upper() == "INQUIRY":
                exceptions_feed.append({
                    "severity": "HIGH",
                    "category": "BANK_INQUIRY",
                    "title": f"{lg.lg_ref_number} — Bank Inquiry Pending",
                    "detail": f"{bank_label} raised an inquiry requiring response",
                    "days": biz_wait,
                    "path": f"{role_prefix}/issuance/issued-lgs",
                })

    # Stalled internal approval requests (> 5 calendar days)
    for req in req_pending_approval_list:
        req_d = _safe_date(req.created_at)
        if req_d:
            age_d = (today - req_d).days
            if age_d > 5:
                sla_breaches += 1
                exceptions_feed.append({
                    "severity": "HIGH" if req.is_urgent or age_d > 10 else "MEDIUM",
                    "category": "APPROVAL_SLA",
                    "title": f"{req.serial_number} — Approval Stalled ({age_d}d)",
                    "detail": f"{'URGENT · ' if req.is_urgent else ''}{req.beneficiary_name or 'Request'} ({req.status.replace('_', ' ').title()})",
                    "days": age_d,
                    "path": approval_path,
                })

    # Verification discrepancies, expired-unreleased LGs, contract overruns
    discrepancy_count = 0
    expired_unreleased_count = 0
    expired_unreleased_base = 0.0
    contract_overrun_count = 0

    # Maturity buckets
    mat_buckets = {
        "expired_unclosed": {"key": "expired_unclosed", "label": "Expired (Unreleased)", "count": 0, "base_amount": 0.0},
        "critical_7d": {"key": "critical_7d", "label": "0–7 Days", "count": 0, "base_amount": 0.0},
        "near_30d": {"key": "near_30d", "label": "8–30 Days", "count": 0, "base_amount": 0.0},
        "mid_60d": {"key": "mid_60d", "label": "31–60 Days", "count": 0, "base_amount": 0.0},
        "quarter_90d": {"key": "quarter_90d", "label": "61–90 Days", "count": 0, "base_amount": 0.0},
        "beyond_90d": {"key": "beyond_90d", "label": "> 90 Days", "count": 0, "base_amount": 0.0},
        "open_ended": {"key": "open_ended", "label": "Open-Ended", "count": 0, "base_amount": 0.0},
    }

    expiring_candidates: List[Tuple[int, IssuedLGRecord, date]] = []

    for lg in live_lgs:
        amt_base = to_base(float(lg.current_amount or lg.bank_lg_amount or 0), lg.currency_id)
        eff_exp = lg.bank_lg_expiry_date or lg.expiry_date
        bank_label = lg.bank.short_name or lg.bank.name if lg.bank else "Bank"

        if (lg.verification_status or "").upper() in ("DISCREPANCY", "DISCREPANCY_REJECTED"):
            discrepancy_count += 1
            exceptions_feed.append({
                "severity": "CRITICAL",
                "category": "DISCREPANCY",
                "title": f"{lg.bank_lg_number or lg.lg_ref_number} — Scan Discrepancy",
                "detail": f"Issued LG copy mismatch at {bank_label} · {lg.beneficiary_name}",
                "days": 0,
                "path": f"{role_prefix}/issuance/issued-lgs",
            })

        is_overrun = (
            (lg.reference_validity_flag or "").upper() == "EXCEEDED"
            or (eff_exp is not None and lg.reference_end_date is not None and eff_exp > lg.reference_end_date)
        )
        if is_overrun:
            contract_overrun_count += 1
            exceptions_feed.append({
                "severity": "MEDIUM",
                "category": "CONTRACT_OVERRUN",
                "title": f"{lg.bank_lg_number or lg.lg_ref_number} — Exceeds Contract End",
                "detail": f"LG expiry ({eff_exp}) extends past underlying reference ({lg.reference_end_date})",
                "days": (eff_exp - lg.reference_end_date).days if (eff_exp and lg.reference_end_date) else 0,
                "path": f"{role_prefix}/issuance/issued-lgs",
            })

        if lg.is_open_ended or not eff_exp:
            mat_buckets["open_ended"]["count"] += 1
            mat_buckets["open_ended"]["base_amount"] += amt_base
        else:
            days_rem = (eff_exp - today).days
            if days_rem < 0:
                expired_unreleased_count += 1
                expired_unreleased_base += amt_base
                mat_buckets["expired_unclosed"]["count"] += 1
                mat_buckets["expired_unclosed"]["base_amount"] += amt_base
                exceptions_feed.append({
                    "severity": "HIGH",
                    "category": "STALE_EXPOSURE",
                    "title": f"{lg.bank_lg_number or lg.lg_ref_number} — Expired {abs(days_rem)}d Ago (Unreleased)",
                    "detail": f"Still blocking {lg.currency.iso_code if lg.currency else ''} {float(lg.current_amount or 0):,.0f} at {bank_label}",
                    "days": abs(days_rem),
                    "path": f"{role_prefix}/action-center",
                })
            elif days_rem <= 7:
                mat_buckets["critical_7d"]["count"] += 1
                mat_buckets["critical_7d"]["base_amount"] += amt_base
                expiring_candidates.append((days_rem, lg, eff_exp))
            elif days_rem <= 30:
                mat_buckets["near_30d"]["count"] += 1
                mat_buckets["near_30d"]["base_amount"] += amt_base
                expiring_candidates.append((days_rem, lg, eff_exp))
            elif days_rem <= 60:
                mat_buckets["mid_60d"]["count"] += 1
                mat_buckets["mid_60d"]["base_amount"] += amt_base
                expiring_candidates.append((days_rem, lg, eff_exp))
            elif days_rem <= 90:
                mat_buckets["quarter_90d"]["count"] += 1
                mat_buckets["quarter_90d"]["base_amount"] += amt_base
                expiring_candidates.append((days_rem, lg, eff_exp))
            else:
                mat_buckets["beyond_90d"]["count"] += 1
                mat_buckets["beyond_90d"]["base_amount"] += amt_base

    expiring_7d = mat_buckets["critical_7d"]["count"]
    expiring_30d = mat_buckets["critical_7d"]["count"] + mat_buckets["near_30d"]["count"]

    expiring_candidates.sort(key=lambda t: t[0])
    expiring_lgs = []
    for days_rem, lg, eff_exp in expiring_candidates[:12]:
        expiring_lgs.append({
            "id": lg.id,
            "ref": lg.bank_lg_number or lg.lg_ref_number,
            "internal_ref": lg.lg_ref_number,
            "beneficiary": lg.beneficiary_name,
            "amount": float(lg.current_amount or 0),
            "currency": lg.currency.iso_code if lg.currency else "",
            "expiry_date": eff_exp.isoformat(),
            "days_remaining": days_rem,
            "bank": lg.bank.short_name or lg.bank.name if lg.bank else "",
            "lg_type": lg.lg_type.name if lg.lg_type else "Guarantee",
            "entity": lg.issuing_entity.code if lg.issuing_entity else "",
            "reference_exceeded": (lg.reference_validity_flag or "").upper() == "EXCEEDED",
        })

    # Open Position Reconciliation Variances
    unreconciled_variances_count = (
        db.query(func.count(ReconciliationResult.id))
        .join(ReconciliationSession, ReconciliationSession.id == ReconciliationResult.session_id)
        .filter(
            ReconciliationSession.customer_id == cust_id,
            ReconciliationSession.is_deleted == False,
            or_(
                ReconciliationResult.user_resolution == None,
                ReconciliationResult.approval_status == "PENDING_APPROVAL",
            ),
        )
        .scalar()
        or 0
    )
    if unreconciled_variances_count > 0:
        exceptions_feed.append({
            "severity": "HIGH",
            "category": "RECON_VARIANCE",
            "title": f"{unreconciled_variances_count} Unresolved Position Variance{'s' if unreconciled_variances_count != 1 else ''}",
            "detail": "Bank position statement mismatches awaiting resolution or admin sign-off",
            "days": 0,
            "path": f"{role_prefix}/issuance/reconciliation",
        })

    # Sort exceptions by severity (CRITICAL -> HIGH -> MEDIUM)
    sev_rank = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    exceptions_feed.sort(key=lambda e: (sev_rank.get(e["severity"], 9), -e.get("days", 0)))

    # --- 5. Concentration Breakdowns (LG Type, Entity, Department) ---
    def build_concentration(key_fn, code_fn=None):
        agg: Dict[str, Dict[str, Any]] = {}
        for lg in live_lgs:
            label = key_fn(lg) or "Unassigned"
            code = code_fn(lg) if code_fn else None
            amt_b = to_base(float(lg.current_amount or lg.bank_lg_amount or 0), lg.currency_id)
            rec = agg.setdefault(label, {"name": label, "code": code, "count": 0, "base_amount": 0.0})
            rec["count"] += 1
            rec["base_amount"] += amt_b
        items = sorted(agg.values(), key=lambda x: x["base_amount"], reverse=True)
        for it in items:
            it["base_amount"] = round(it["base_amount"], 2)
            it["share_pct"] = round((it["base_amount"] / total_active_base) * 100.0, 1) if total_active_base > 0 else 0.0
        return items

    by_lg_type = build_concentration(lambda lg: lg.lg_type.name if lg.lg_type else "Other Guarantee")
    by_entity = build_concentration(
        lambda lg: lg.issuing_entity.entity_name if lg.issuing_entity else "Corporate HQ",
        lambda lg: lg.issuing_entity.code if lg.issuing_entity else "HQ",
    )
    by_department = build_concentration(lambda lg: (lg.department or "General").strip() or "General")

    # --- 6. Recent Activity (latest 10 issuance + maintenance events) ---
    recent_activity = []
    for lg in all_lgs[:8]:
        ccy_code = lg.currency.iso_code if lg.currency else ""
        recent_activity.append({
            "type": "ISSUED" if (lg.status or "").upper() in CONFIRMED_LIVE_STATUSES else "EXECUTION",
            "ref": lg.bank_lg_number or lg.lg_ref_number,
            "description": f"{lg.beneficiary_name} · {lg.bank.short_name or lg.bank.name if lg.bank else 'Bank'}",
            "amount": float(lg.current_amount or 0),
            "currency": ccy_code,
            "status": lg.status,
            "timestamp": lg.created_at.isoformat() if lg.created_at else None,
        })
    for action in all_maint_actions[:8]:
        lg = action.issued_lg
        recent_activity.append({
            "type": f"MAINTENANCE_{action.action_type}",
            "ref": (lg.bank_lg_number or lg.lg_ref_number) if lg else f"LG#{action.issued_lg_id}",
            "description": f"{action.action_type.replace('_', ' ').title()} ({action.status.replace('_', ' ').title()})",
            "amount": float(lg.current_amount or 0) if lg else None,
            "currency": lg.currency.iso_code if (lg and lg.currency) else "",
            "status": action.status,
            "timestamp": action.created_at.isoformat() if action.created_at else None,
        })
    recent_activity.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    recent_activity = recent_activity[:10]

    for b in mat_buckets.values():
        b["base_amount"] = round(b["base_amount"], 2)

    return {
        # Legacy top-level keys preserved for full backward compatibility
        "pending_requests": pending_requests,
        "pending_approvals": pending_approvals,
        "pending_bank_replies": pending_bank,
        "sla_breaches": sla_breaches,
        "expiring_lgs_7d": expiring_7d,
        "expiring_lgs_30d": expiring_30d,
        "total_active_lgs": total_active_lgs,
        "total_active_amount": round(total_active_base, 2),
        "facility_utilization": facility_utilization,
        "expiring_lgs": expiring_lgs,
        "recent_activity": recent_activity,

        # Enriched Executive Intelligence Sections
        "as_of": datetime.utcnow().isoformat(),
        "base_currency": base_ccy,
        "available_currencies": fx_ctx["available_currencies"],
        "fx_rates_applied": fx_ctx["fx_rates_applied"],
        "fx_unconverted_currencies": sorted(fx_ctx["unconverted_codes"]),
        "exposure_summary": {
            "total_active_lgs": total_active_lgs,
            "total_active_base_amount": round(total_active_base, 2),
            "by_currency": by_currency_list,
            "in_flight_lgs_count": len(in_flight_lgs),
            "in_flight_base_amount": round(in_flight_base, 2),
            "cross_border_count": cross_border_count,
            "cross_border_pct": round((cross_border_count / total_active_lgs) * 100.0, 1) if total_active_lgs > 0 else 0.0,
            "non_operative_count": non_operative_count,
            "third_party_count": third_party_count,
        },
        "capacity_summary": {
            "active_facilities_count": len(active_facilities_only),
            "banks_count": len(active_fac_banks),
            "total_limit_base": round(total_limit_base, 2),
            "total_utilized_base": round(total_utilized_base, 2),
            "total_reserved_base": round(total_reserved_base, 2),
            "total_headroom_base": round(total_headroom_base, 2),
            "overall_utilization_pct": overall_util_pct,
            "high_utilization_count": high_util_count,
            "facilities_expiring_soon_count": fac_expiring_soon_count,
        },
        "collateral_summary": {
            "locked_cash_margin_base": round(locked_margin_base, 2),
            "clean_credit_exposure_base": round(clean_credit_base, 2),
            "weighted_avg_margin_pct": weighted_avg_margin_pct,
            "cash_backed_lgs_count": cash_backed_count,
            "unsecured_lgs_count": unsecured_count,
            "margin_by_currency": {k: round(v, 2) for k, v in margin_ccy_buckets.items()},
        },
        "governance_queue": {
            "my_pending_approvals": pending_approvals,
            "my_request_approvals": my_req_approvals,
            "my_maintenance_approvals": my_maint_approvals,
            "my_admin_change_approvals": my_admin_change_approvals,
            "org_pending_approvals": org_pending_approvals,
            "urgent_requests_count": urgent_open_requests,
            "discrepancy_count": discrepancy_count,
            "unreconciled_variances_count": unreconciled_variances_count,
            "expired_unreleased_count": expired_unreleased_count,
            "expired_unreleased_base": round(expired_unreleased_base, 2),
            "contract_overrun_count": contract_overrun_count,
        },
        "pipeline_stages": pipeline_stages,
        "concentration": {
            "by_lg_type": by_lg_type,
            "by_entity": by_entity,
            "by_department": by_department,
        },
        "maturity_horizon": list(mat_buckets.values()),
        "exceptions_feed": exceptions_feed[:15],
    }


@router.get("/analytics-report")
def get_issuance_analytics_report(
    start_date: Optional[date] = Query(None, description="Period start date for operational flow metrics"),
    end_date: Optional[date] = Query(None, description="Period end date for operational flow metrics"),
    entity_id: Optional[int] = Query(None, description="Filter portfolio & flow by Issuing Entity ID"),
    bank_id: Optional[int] = Query(None, description="Filter portfolio & flow by Bank ID"),
    base_currency: Optional[str] = Query(None, description="Reporting base currency ISO code (e.g. EGP, USD)"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Comprehensive Corporate Admin LG Issuance Reporting & Analytics Engine.
    Provides factual Executive Portfolio & Pricing metrics, Maturity Ladder,
    Entity/Department/Project/Beneficiary allocations, Bank SLA & Wallet-Share Scorecard,
    and Period-filtered Operational Velocity & Lifecycle Governance metrics.
    """
    from datetime import datetime, timedelta, date as date_cls
    from dateutil.relativedelta import relativedelta
    from sqlalchemy.orm import selectinload, joinedload
    from app.models.models import CustomerEntity
    from app.models.models_issuance import (
        IssuanceMaintenanceAction,
        IssuanceExposureEntry,
        ReconciliationSession,
        ReconciliationResult,
        CorporateProject,
    )
    from app.services.business_calendar_service import business_calendar_service
    from app.constants import UserRole

    cust_id = current_user.customer_id
    today = date_cls.today()
    eff_end = end_date or today
    eff_start = start_date or (eff_end - timedelta(days=90))
    if eff_start > eff_end:
        eff_start, eff_end = eff_end, eff_start

    CONFIRMED_LIVE_STATUSES = {"ACTIVE", "LG_ISSUED", "PENDING_CLOSE", "CANCEL_REQUESTED"}
    IN_FLIGHT_STATUSES = {"INTERNAL_PROCESSING", "DELIVERED_TO_BANK", "SLA_EXCEEDED", "ISSUED"}

    has_full_entities = (
        current_user.role in (UserRole.CORPORATE_ADMIN, UserRole.SYSTEM_OWNER)
        or getattr(current_user, "has_all_entity_access", True) is not False
    )
    allowed_entity_ids = set(current_user.entity_ids or []) if not has_full_entities else None

    # 1. Load filter options (all active entities & relationship banks for this customer)
    ent_q = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == cust_id,
        CustomerEntity.is_deleted == False,
    )
    if allowed_entity_ids is not None:
        ent_q = ent_q.filter(CustomerEntity.id.in_(allowed_entity_ids))
    tenant_entities = ent_q.order_by(CustomerEntity.entity_name.asc()).all()

    all_facilities_unfiltered: List[IssuanceFacility] = (
        db.query(IssuanceFacility)
        .options(
            selectinload(IssuanceFacility.sub_limits),
            selectinload(IssuanceFacility.entities),
            joinedload(IssuanceFacility.bank),
            joinedload(IssuanceFacility.currency),
        )
        .filter(
            IssuanceFacility.customer_id == cust_id,
            IssuanceFacility.is_deleted == False,
            IssuanceFacility.deleted_at == None,
        )
        .all()
    )
    if allowed_entity_ids is not None:
        all_facilities_unfiltered = [
            f for f in all_facilities_unfiltered
            if not f.entities or any(e.id in allowed_entity_ids for e in f.entities)
        ]

    lg_unfiltered_q = (
        db.query(IssuedLGRecord)
        .options(
            joinedload(IssuedLGRecord.bank),
            joinedload(IssuedLGRecord.currency),
            joinedload(IssuedLGRecord.lg_type),
            joinedload(IssuedLGRecord.issuing_entity),
            joinedload(IssuedLGRecord.project),
            joinedload(IssuedLGRecord.sub_limit).joinedload(IssuanceFacilitySubLimit.facility),
        )
        .filter(IssuedLGRecord.customer_id == cust_id)
    )
    if allowed_entity_ids is not None:
        lg_unfiltered_q = lg_unfiltered_q.filter(IssuedLGRecord.issuing_entity_id.in_(allowed_entity_ids))
    all_lgs_unfiltered: List[IssuedLGRecord] = lg_unfiltered_q.order_by(IssuedLGRecord.created_at.desc()).all()

    # Distinct relationship banks across facilities & LGs
    bank_lookup: Dict[int, Dict[str, Any]] = {}
    for f in all_facilities_unfiltered:
        if f.bank_id and f.bank:
            bank_lookup[f.bank_id] = {
                "id": f.bank_id,
                "name": f.bank.name,
                "short_name": f.bank.short_name or f.bank.name,
            }
    for lg in all_lgs_unfiltered:
        if lg.bank_id and lg.bank and lg.bank_id not in bank_lookup:
            bank_lookup[lg.bank_id] = {
                "id": lg.bank_id,
                "name": lg.bank.name,
                "short_name": lg.bank.short_name or lg.bank.name,
            }

    # Build FX converter using full tenant context
    fx_ctx = _build_issuance_fx_converter(db, all_facilities_unfiltered, all_lgs_unfiltered, base_currency)
    base_ccy = fx_ctx["base_currency"]
    to_base = fx_ctx["to_base"]

    # Apply optional entity_id / bank_id filters
    lgs = all_lgs_unfiltered
    if entity_id:
        lgs = [lg for lg in lgs if lg.issuing_entity_id == entity_id]
    if bank_id:
        lgs = [lg for lg in lgs if lg.bank_id == bank_id]

    facilities = [f for f in all_facilities_unfiltered if (f.status or "").upper() == "ACTIVE"]
    if bank_id:
        facilities = [f for f in facilities if f.bank_id == bank_id]
    if entity_id:
        facilities = [
            f for f in facilities
            if not f.entities or any(e.id == entity_id for e in f.entities)
        ]

    req_q = (
        db.query(IssuanceRequest)
        .options(
            joinedload(IssuanceRequest.currency),
            joinedload(IssuanceRequest.lg_type),
            joinedload(IssuanceRequest.issuing_entity),
            joinedload(IssuanceRequest.selected_sub_limit).joinedload(IssuanceFacilitySubLimit.facility),
        )
        .filter(
            IssuanceRequest.customer_id == cust_id,
            IssuanceRequest.is_deleted == False,
        )
    )
    if allowed_entity_ids is not None:
        req_q = req_q.filter(IssuanceRequest.issuing_entity_id.in_(allowed_entity_ids))
    if entity_id:
        req_q = req_q.filter(IssuanceRequest.issuing_entity_id == entity_id)
    requests_list: List[IssuanceRequest] = req_q.all()
    if bank_id:
        lg_ids_for_bank = {lg.id for lg in lgs}
        requests_list = [
            r for r in requests_list
            if (r.lg_record_id and r.lg_record_id in lg_ids_for_bank)
            or (
                r.selected_sub_limit
                and r.selected_sub_limit.facility
                and r.selected_sub_limit.facility.bank_id == bank_id
            )
        ]

    lg_id_set = {lg.id for lg in lgs}
    all_maint = (
        db.query(IssuanceMaintenanceAction)
        .join(IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id)
        .filter(
            IssuedLGRecord.customer_id == cust_id,
            IssuanceMaintenanceAction.is_deleted == False,
        )
        .all()
    )
    maint_list = (
        [m for m in all_maint if m.issued_lg_id in lg_id_set]
        if (entity_id or bank_id or allowed_entity_ids is not None)
        else all_maint
    )

    # Business calendar pre-fetch
    weekend_days = business_calendar_service.get_weekend_days(db)
    min_rep_deliv_yr = min(
        [lg.delivery_date.year for lg in lgs if lg.delivery_date],
        default=today.year - 2,
    )
    holidays_set = business_calendar_service.get_holiday_dates(
        db, min(min_rep_deliv_yr, today.year - 2), today.year + 1
    )

    def calc_biz_turnaround(sd: Optional[date], ed: Optional[date]) -> float:
        if not sd or not ed:
            return 1.0
        if ed <= sd:
            return 1.0
        cur = sd
        b_days = 0
        while cur < ed:
            cur += timedelta(days=1)
            if cur.weekday() not in weekend_days and cur not in holidays_set:
                b_days += 1
        return max(1.0, float(b_days))

    live_lgs = [lg for lg in lgs if (lg.status or "").upper() in CONFIRMED_LIVE_STATUSES]
    in_flight_lgs = [lg for lg in lgs if (lg.status or "").upper() in IN_FLIGHT_STATUSES]

    # --- SECTION 1: Executive Portfolio, Margin & Factual Commission KPIs ---
    active_exposure_base = 0.0
    locked_margin_base = 0.0
    cash_backed_exposure_base = 0.0
    annualized_comm_base = 0.0
    priced_exposure_base = 0.0
    priced_lgs_count = 0
    unpriced_lgs_count = 0
    expired_unreleased_count = 0
    expired_unreleased_base = 0.0
    ccy_summary: Dict[str, Dict[str, Any]] = {}

    for lg in live_lgs:
        amt = float(lg.current_amount or lg.bank_lg_amount or 0)
        amt_base = to_base(amt, lg.currency_id)
        active_exposure_base += amt_base

        ccy_code = lg.currency.iso_code.upper() if lg.currency and lg.currency.iso_code else base_ccy
        cb = ccy_summary.setdefault(ccy_code, {"currency": ccy_code, "count": 0, "native_amount": 0.0, "base_amount": 0.0})
        cb["count"] += 1
        cb["native_amount"] += amt
        cb["base_amount"] += amt_base

        pm = _extract_lg_pricing_and_margin(lg)
        if pm["is_cash_backed"]:
            m_base = to_base(pm["margin_amount_native"], lg.currency_id)
            locked_margin_base += m_base
            cash_backed_exposure_base += amt_base

        if pm["has_commission_pricing"]:
            priced_lgs_count += 1
            priced_exposure_base += amt_base
            pct_comm_base = amt_base * ((pm["comm_rate_pct"] or 0.0) / 100.0)
            min_comm_base = to_base(pm["min_comm_fac"], pm["pricing_ccy_id"])
            flat_fee_base = to_base(pm["flat_fee_fac"], pm["pricing_ccy_id"])
            annualized_comm_base += max(pct_comm_base, min_comm_base) + flat_fee_base
        else:
            unpriced_lgs_count += 1

        eff_exp = lg.bank_lg_expiry_date or lg.expiry_date
        if not lg.is_open_ended and eff_exp and eff_exp < today:
            expired_unreleased_count += 1
            expired_unreleased_base += amt_base

    # Facility capacity totals (for filtered facilities)
    total_fac_limit_base = 0.0
    total_fac_utilized_base = 0.0
    fac_util_by_bank: Dict[int, Dict[str, float]] = {}

    for fac in facilities:
        lim = float(fac.total_limit_amount or 0)
        if lim <= 0:
            continue
        used = sum(
            float(issuance_service.compute_sub_limit_used_in_facility_ccy(db, fac, sl))
            for sl in (fac.sub_limits or [])
        )
        lim_b = to_base(lim, fac.currency_id)
        used_b = to_base(used, fac.currency_id)
        total_fac_limit_base += lim_b
        total_fac_utilized_base += used_b

        fb = fac_util_by_bank.setdefault(fac.bank_id, {"facilities_count": 0, "limit_base": 0.0, "used_base": 0.0, "sla_days": []})
        fb["facilities_count"] += 1
        fb["limit_base"] += lim_b
        fb["used_base"] += used_b
        if fac.sla_agreement_days:
            fb["sla_days"].append(float(fac.sla_agreement_days))

    headroom_base = max(0.0, total_fac_limit_base - total_fac_utilized_base)
    util_pct = round((total_fac_utilized_base / total_fac_limit_base) * 100.0, 1) if total_fac_limit_base > 0 else 0.0
    weighted_margin_pct = round((locked_margin_base / active_exposure_base) * 100.0, 2) if active_exposure_base > 0 else 0.0
    cash_backed_share_pct = round((cash_backed_exposure_base / active_exposure_base) * 100.0, 1) if active_exposure_base > 0 else 0.0
    weighted_comm_rate_pct = round((annualized_comm_base / priced_exposure_base) * 100.0, 3) if priced_exposure_base > 0 else None

    # --- SECTION 2: Maturity Ladder & Upcoming / Overdue Register ---
    ladder_defs = [
        ("expired_unclosed", "Expired (Unclosed)"),
        ("d0_30", "0–30 Days"),
        ("d31_60", "31–60 Days"),
        ("d61_90", "61–90 Days"),
        ("d91_180", "91–180 Days"),
        ("d181_365", "181–365 Days"),
        ("gt_365", "> 1 Year"),
        ("open_ended", "Open-Ended"),
    ]
    ladder_map = {k: {"key": k, "bucket": lbl, "count": 0, "base_amount": 0.0} for k, lbl in ladder_defs}
    maturity_watch_items = []

    for lg in live_lgs:
        amt = float(lg.current_amount or lg.bank_lg_amount or 0)
        amt_base = to_base(amt, lg.currency_id)
        eff_exp = lg.bank_lg_expiry_date or lg.expiry_date
        if lg.is_open_ended or not eff_exp:
            b_key = "open_ended"
            days_rem = None
        else:
            days_rem = (eff_exp - today).days
            if days_rem < 0:
                b_key = "expired_unclosed"
            elif days_rem <= 30:
                b_key = "d0_30"
            elif days_rem <= 60:
                b_key = "d31_60"
            elif days_rem <= 90:
                b_key = "d61_90"
            elif days_rem <= 180:
                b_key = "d91_180"
            elif days_rem <= 365:
                b_key = "d181_365"
            else:
                b_key = "gt_365"

            if days_rem <= 90:
                maturity_watch_items.append({
                    "id": lg.id,
                    "ref": lg.bank_lg_number or lg.lg_ref_number,
                    "internal_serial": lg.internal_serial or lg.lg_ref_number,
                    "beneficiary": lg.beneficiary_name,
                    "bank": lg.bank.short_name or lg.bank.name if lg.bank else "N/A",
                    "entity": lg.issuing_entity.entity_name if lg.issuing_entity else "Corporate HQ",
                    "lg_type": lg.lg_type.name if lg.lg_type else "Guarantee",
                    "amount": round(amt, 2),
                    "currency": lg.currency.iso_code if lg.currency else base_ccy,
                    "base_amount": round(amt_base, 2),
                    "expiry_date": eff_exp.isoformat(),
                    "days_remaining": days_rem,
                    "is_expired_unclosed": days_rem < 0,
                    "reference_exceeded": (lg.reference_validity_flag or "").upper() == "EXCEEDED",
                })

        ladder_map[b_key]["count"] += 1
        ladder_map[b_key]["base_amount"] += amt_base

    maturity_ladder = []
    for k, _ in ladder_defs:
        item = ladder_map[k]
        item["base_amount"] = round(item["base_amount"], 2)
        item["share_pct"] = round((item["base_amount"] / active_exposure_base) * 100.0, 1) if active_exposure_base > 0 else 0.0
        maturity_ladder.append(item)

    maturity_watch_items.sort(key=lambda x: x["days_remaining"])

    # --- SECTION 3: Multi-Dimensional Allocations (Entity, Dept, Project, LG Type, Top Beneficiaries) ---
    def aggregate_dimension(dim_key_fn, extra_fn=None):
        groups: Dict[str, Dict[str, Any]] = {}
        for lg in live_lgs:
            k_label = dim_key_fn(lg) or "Unassigned"
            amt_b = to_base(float(lg.current_amount or lg.bank_lg_amount or 0), lg.currency_id)
            pm = _extract_lg_pricing_and_margin(lg)
            marg_b = to_base(pm["margin_amount_native"], lg.currency_id) if pm["is_cash_backed"] else 0.0
            eff_exp = lg.bank_lg_expiry_date or lg.expiry_date
            eff_iss = lg.bank_lg_issue_date or lg.issue_date or _safe_date(lg.created_at)
            tenor_d = (eff_exp - eff_iss).days if (eff_exp and eff_iss and eff_exp >= eff_iss) else None
            is_stale = bool(not lg.is_open_ended and eff_exp and eff_exp < today)

            g = groups.setdefault(k_label, {
                "name": k_label,
                "extra": extra_fn(lg) if extra_fn else None,
                "active_count": 0,
                "base_amount": 0.0,
                "locked_margin_base": 0.0,
                "cross_border_count": 0,
                "stale_count": 0,
                "_tenors": [],
            })
            g["active_count"] += 1
            g["base_amount"] += amt_b
            g["locked_margin_base"] += marg_b
            if lg.is_cross_border:
                g["cross_border_count"] += 1
            if is_stale:
                g["stale_count"] += 1
            if tenor_d is not None:
                g["_tenors"].append(tenor_d)

        out = []
        for g in sorted(groups.values(), key=lambda x: x["base_amount"], reverse=True):
            tenors = g.pop("_tenors")
            cnt = g["active_count"]
            b_amt = g["base_amount"]
            g["base_amount"] = round(b_amt, 2)
            g["locked_margin_base"] = round(g["locked_margin_base"], 2)
            g["avg_lg_base"] = round(b_amt / cnt, 2) if cnt > 0 else 0.0
            g["share_pct"] = round((b_amt / active_exposure_base) * 100.0, 1) if active_exposure_base > 0 else 0.0
            g["avg_tenor_days"] = round(sum(tenors) / len(tenors), 0) if tenors else None
            out.append(g)
        return out

    alloc_by_entity = aggregate_dimension(
        lambda lg: lg.issuing_entity.entity_name if lg.issuing_entity else "Corporate HQ",
        lambda lg: lg.issuing_entity.code if lg.issuing_entity else "HQ",
    )
    alloc_by_dept = aggregate_dimension(lambda lg: (lg.department or "General").strip() or "General")
    alloc_by_project = aggregate_dimension(
        lambda lg: lg.project.name if lg.project else "General / Non-Project",
        lambda lg: lg.project.project_type if lg.project else "GENERAL",
    )
    alloc_by_lg_type = aggregate_dimension(lambda lg: lg.lg_type.name if lg.lg_type else "Other Guarantee")
    alloc_by_beneficiary = aggregate_dimension(
        lambda lg: lg.beneficiary_name or "Unknown Beneficiary",
        lambda lg: lg.beneficiary_country or "",
    )[:10]

    # --- SECTION 4: Banking Partner SLA, Pricing & Wallet-Share Scorecard ---
    # Latest reconciliation stats per bank
    recon_sessions = (
        db.query(ReconciliationSession)
        .filter(
            ReconciliationSession.customer_id == cust_id,
            ReconciliationSession.is_deleted == False,
        )
        .order_by(ReconciliationSession.position_date.desc())
        .all()
    )
    latest_recon_by_bank: Dict[int, str] = {}
    for rs in recon_sessions:
        if rs.bank_id and rs.bank_id not in latest_recon_by_bank and rs.position_date:
            latest_recon_by_bank[rs.bank_id] = rs.position_date.isoformat()

    open_recon_rows = (
        db.query(ReconciliationSession.bank_id, func.count(ReconciliationResult.id))
        .join(ReconciliationResult, ReconciliationResult.session_id == ReconciliationSession.id)
        .filter(
            ReconciliationSession.customer_id == cust_id,
            ReconciliationSession.is_deleted == False,
            or_(
                ReconciliationResult.user_resolution == None,
                ReconciliationResult.approval_status == "PENDING_APPROVAL",
            ),
        )
        .group_by(ReconciliationSession.bank_id)
        .all()
    )
    open_recon_by_bank = {int(r[0]): int(r[1]) for r in open_recon_rows if r[0] is not None}

    bank_ids_in_scope = set(fac_util_by_bank.keys()) | {lg.bank_id for lg in lgs if lg.bank_id}
    bank_scorecard = []

    for b_id in bank_ids_in_scope:
        b_meta = bank_lookup.get(b_id, {"id": b_id, "name": f"Bank #{b_id}", "short_name": f"Bank #{b_id}"})
        b_all_lgs = [lg for lg in lgs if lg.bank_id == b_id]
        b_live_lgs = [lg for lg in live_lgs if lg.bank_id == b_id]
        b_inflight_lgs = [lg for lg in in_flight_lgs if lg.bank_id == b_id]
        fb = fac_util_by_bank.get(b_id, {"facilities_count": 0, "limit_base": 0.0, "used_base": 0.0, "sla_days": []})

        b_exp_base = 0.0
        b_margin_base = 0.0
        b_comm_rates = []
        b_margin_pcts = []
        for lg in b_live_lgs:
            amt_b = to_base(float(lg.current_amount or lg.bank_lg_amount or 0), lg.currency_id)
            b_exp_base += amt_b
            pm = _extract_lg_pricing_and_margin(lg)
            if pm["is_cash_backed"]:
                b_margin_base += to_base(pm["margin_amount_native"], lg.currency_id)
            if pm["margin_pct"] is not None:
                b_margin_pcts.append(pm["margin_pct"])
            if pm["comm_rate_pct"] is not None and pm["comm_rate_pct"] > 0:
                b_comm_rates.append(pm["comm_rate_pct"])

        # Also inspect facility sub-limit configured pricing if no live LGs yet on this bank
        if not b_comm_rates or not b_margin_pcts:
            for fac in [f for f in facilities if f.bank_id == b_id]:
                for sl in (fac.sub_limits or []):
                    if not b_comm_rates and sl.default_commission_rate is not None and float(sl.default_commission_rate) > 0:
                        b_comm_rates.append(float(sl.default_commission_rate))
                    if not b_margin_pcts and sl.default_cash_margin_pct is not None:
                        b_margin_pcts.append(float(sl.default_cash_margin_pct))

        agreed_sla = round(sum(fb["sla_days"]) / len(fb["sla_days"]), 1) if fb["sla_days"] else 5.0

        # Compute tenant-specific empirical bank turnaround & SLA compliance
        turnarounds = []
        on_time_count = 0
        for lg in b_all_lgs:
            rep_d = lg.bank_reply_date or lg.bank_lg_issue_date or lg.issue_date
            if lg.delivery_date and rep_d and rep_d >= lg.delivery_date:
                fac = lg.sub_limit.facility if (lg.sub_limit and lg.sub_limit.facility) else None
                lg_sla = float(fac.sla_agreement_days) if (fac and fac.sla_agreement_days) else agreed_sla
                t_days = calc_biz_turnaround(lg.delivery_date, rep_d)
                turnarounds.append(t_days)
                if t_days <= lg_sla:
                    on_time_count += 1

        if turnarounds:
            actual_turnaround = round(sum(turnarounds) / len(turnarounds), 1)
            sla_compliance_pct = round((on_time_count / len(turnarounds)) * 100.0, 1)
        else:
            # Fallback to facility actual_avg_sla_days if seeded/recorded on customer's facilities
            fac_emas = [
                float(f.actual_avg_sla_days) for f in facilities
                if f.bank_id == b_id and f.actual_avg_sla_days is not None and (f.total_completed_issuances or 0) > 0
            ]
            fac_comms = [
                float(f.sla_commitment_pct) for f in facilities
                if f.bank_id == b_id and f.sla_commitment_pct is not None and (f.total_completed_issuances or 0) > 0
            ]
            actual_turnaround = round(sum(fac_emas) / len(fac_emas), 1) if fac_emas else None
            sla_compliance_pct = round(sum(fac_comms) / len(fac_comms), 1) if fac_comms else None

        inquiries_cnt = sum(
            1 for lg in b_all_lgs
            if (lg.bank_reply_type or "").upper() == "INQUIRY" or (isinstance(lg.bank_inquiry_log, list) and len(lg.bank_inquiry_log) > 0)
        )
        rejections_cnt = sum(
            1 for lg in b_all_lgs
            if (lg.status or "").upper() == "BANK_REJECTED" or (lg.bank_reply_type or "").upper() == "REJECTED"
        )
        discrepancies_cnt = sum(
            1 for lg in b_all_lgs
            if (lg.verification_status or "").upper() in ("DISCREPANCY", "DISCREPANCY_REJECTED", "ACCEPTED")
        )
        verified_total = sum(
            1 for lg in b_all_lgs
            if (lg.verification_status or "").upper() in ("MATCHED", "DISCREPANCY", "DISCREPANCY_REJECTED", "ACCEPTED")
        )
        matched_clean = sum(1 for lg in b_all_lgs if (lg.verification_status or "").upper() == "MATCHED")
        match_rate_pct = round((matched_clean / verified_total) * 100.0, 1) if verified_total > 0 else None

        lim_b = fb["limit_base"]
        used_b = fb["used_base"]
        bank_scorecard.append({
            "bank_id": b_id,
            "bank_name": b_meta["name"],
            "short_name": b_meta["short_name"],
            "active_lgs_count": len(b_live_lgs),
            "in_flight_count": len(b_inflight_lgs),
            "active_exposure_base": round(b_exp_base, 2),
            "wallet_share_pct": round((b_exp_base / active_exposure_base) * 100.0, 1) if active_exposure_base > 0 else 0.0,
            "facilities_count": fb["facilities_count"],
            "total_limit_base": round(lim_b, 2),
            "utilized_base": round(used_b, 2),
            "headroom_base": round(max(0.0, lim_b - used_b), 2),
            "utilization_pct": round((used_b / lim_b) * 100.0, 1) if lim_b > 0 else 0.0,
            "agreed_sla_days": agreed_sla,
            "actual_turnaround_days": actual_turnaround,
            "sla_compliance_pct": sla_compliance_pct,
            "completed_sample_size": len(turnarounds),
            "inquiries_count": inquiries_cnt,
            "rejections_count": rejections_cnt,
            "discrepancies_count": discrepancies_cnt,
            "verification_match_rate_pct": match_rate_pct,
            "avg_commission_rate_pct": round(sum(b_comm_rates) / len(b_comm_rates), 3) if b_comm_rates else None,
            "avg_cash_margin_pct": round(sum(b_margin_pcts) / len(b_margin_pcts), 1) if b_margin_pcts else None,
            "locked_margin_base": round(b_margin_base, 2),
            "last_recon_date": latest_recon_by_bank.get(b_id),
            "open_recon_variances": open_recon_by_bank.get(b_id, 0),
        })

    bank_scorecard.sort(key=lambda x: (x["active_exposure_base"], x["total_limit_base"]), reverse=True)

    # --- SECTION 5: Operational Velocity, Turnaround & 6-Month Trend ---
    req_by_id: Dict[int, IssuanceRequest] = {r.id: r for r in requests_list}

    def in_period(d: Optional[date]) -> bool:
        return bool(d and eff_start <= d <= eff_end)

    # 5a. Turnaround speedometers (Approval, Execution->Delivery, Bank Issuance, End-to-End)
    approval_days_period, approval_days_life = [], []
    for r in requests_list:
        sub_d = _safe_date(r.created_at)
        if not sub_d or not isinstance(r.approval_chain_audit, list):
            continue
        appr_step = next(
            (s for s in reversed(r.approval_chain_audit) if isinstance(s, dict) and "APPROVED" in str(s.get("action", "")).upper()),
            None,
        )
        if appr_step:
            appr_d = _safe_date(appr_step.get("timestamp"))
            if appr_d and appr_d >= sub_d:
                delta_d = float((appr_d - sub_d).days)
                approval_days_life.append(delta_d)
                if in_period(appr_d):
                    approval_days_period.append(delta_d)

    delivery_days_period, delivery_days_life = [], []
    bank_days_period, bank_days_life = [], []
    e2e_days_period, e2e_days_life = [], []

    for lg in lgs:
        exec_d = _safe_date(lg.created_at)
        deliv_d = lg.delivery_date
        rep_d = lg.bank_reply_date or lg.bank_lg_issue_date or lg.issue_date

        if exec_d and deliv_d and deliv_d >= exec_d:
            dd = float((deliv_d - exec_d).days)
            delivery_days_life.append(dd)
            if in_period(deliv_d):
                delivery_days_period.append(dd)

        if deliv_d and rep_d and rep_d >= deliv_d:
            bd = calc_biz_turnaround(deliv_d, rep_d)
            bank_days_life.append(bd)
            if in_period(rep_d):
                bank_days_period.append(bd)

        linked_req = req_by_id.get(lg.request_id) if lg.request_id else None
        orig_d = _safe_date(linked_req.created_at) if linked_req else exec_d
        if orig_d and rep_d and rep_d >= orig_d:
            ed = float((rep_d - orig_d).days)
            e2e_days_life.append(ed)
            if in_period(rep_d):
                e2e_days_period.append(ed)

    def avg_or_none(vals: List[float]) -> Optional[float]:
        return round(sum(vals) / len(vals), 1) if vals else None

    turnaround_metrics = {
        "approval_days_period": avg_or_none(approval_days_period),
        "approval_days_lifetime": avg_or_none(approval_days_life),
        "approval_sample_count": len(approval_days_period) or len(approval_days_life),
        "delivery_days_period": avg_or_none(delivery_days_period),
        "delivery_days_lifetime": avg_or_none(delivery_days_life),
        "delivery_sample_count": len(delivery_days_period) or len(delivery_days_life),
        "bank_biz_days_period": avg_or_none(bank_days_period),
        "bank_biz_days_lifetime": avg_or_none(bank_days_life),
        "bank_sample_count": len(bank_days_period) or len(bank_days_life),
        "end_to_end_days_period": avg_or_none(e2e_days_period),
        "end_to_end_days_lifetime": avg_or_none(e2e_days_life),
        "end_to_end_sample_count": len(e2e_days_period) or len(e2e_days_life),
    }

    # 5b. Period Funnel
    period_reqs = [r for r in requests_list if in_period(_safe_date(r.created_at))]
    period_lgs_exec = [lg for lg in lgs if in_period(_safe_date(lg.created_at))]
    period_lgs_deliv = [lg for lg in lgs if in_period(lg.delivery_date)]
    period_lgs_conf = [
        lg for lg in lgs
        if (lg.status or "").upper() in CONFIRMED_LIVE_STATUSES
        and in_period(lg.bank_reply_date or lg.bank_lg_issue_date or lg.issue_date or _safe_date(lg.created_at))
    ]
    period_lgs_handover = [lg for lg in lgs if in_period(lg.handover_date)]

    period_funnel = {
        "requests_submitted": len([r for r in period_reqs if (r.status or "").upper() != "DRAFT"]),
        "requests_approved": len([
            r for r in period_reqs
            if (r.status or "").upper() in ("APPROVED_INTERNAL", "FACILITY_RESERVED", "INTERNAL_PROCESSING", "COMPLETED", "LG_ISSUED")
            or r.lg_record_id is not None
        ]),
        "requests_revision_required": len([
            r for r in period_reqs
            if (r.status or "").upper() == "REVISION_REQUIRED" or r.returned_from_step is not None
        ]),
        "requests_rejected_or_cancelled": len([
            r for r in period_reqs
            if (r.status or "").upper() in ("REJECTED", "CANCELLED")
        ]),
        "lgs_executed": len(period_lgs_exec),
        "lgs_delivered_to_bank": len(period_lgs_deliv),
        "lgs_confirmed_by_bank": len(period_lgs_conf),
        "lgs_handed_over": len(period_lgs_handover),
        "confirmed_volume_base": round(
            sum(to_base(float(lg.current_amount or lg.bank_lg_amount or 0), lg.currency_id) for lg in period_lgs_conf),
            2,
        ),
    }

    # 5c. Maintenance Actions Flow in Period
    maint_types_order = ["EXTEND", "INCREASE_AMOUNT", "AMENDMENT", "CLOSE", "LIQUIDATION", "ACTIVATE"]
    maint_by_type = {t: 0 for t in maint_types_order}
    maint_by_status = {"PENDING_APPROVAL": 0, "APPROVED": 0, "EXECUTED": 0, "REJECTED": 0}
    period_maint = [m for m in maint_list if in_period(_safe_date(m.created_at))]
    for m in period_maint:
        at = (m.action_type or "OTHER").upper()
        maint_by_type[at] = maint_by_type.get(at, 0) + 1
        st = (m.status or "OTHER").upper()
        if st in maint_by_status:
            maint_by_status[st] += 1

    # 5d. 6-Month Chronological Issuance & Maintenance Trend
    monthly_trend = []
    anchor_month = date_cls(eff_end.year, eff_end.month, 1)
    for idx in range(5, -1, -1):
        m_start = anchor_month - relativedelta(months=idx)
        m_yr, m_mo = m_start.year, m_start.month
        m_label = m_start.strftime("%b %Y")

        m_reqs = [
            r for r in requests_list
            if (r.status or "").upper() != "DRAFT"
            and _safe_date(r.created_at)
            and _safe_date(r.created_at).year == m_yr
            and _safe_date(r.created_at).month == m_mo
        ]
        m_lgs = [
            lg for lg in lgs
            if _safe_date(lg.created_at)
            and _safe_date(lg.created_at).year == m_yr
            and _safe_date(lg.created_at).month == m_mo
        ]
        m_maint = [
            m for m in maint_list
            if _safe_date(m.created_at)
            and _safe_date(m.created_at).year == m_yr
            and _safe_date(m.created_at).month == m_mo
        ]

        monthly_trend.append({
            "month": m_label,
            "requests_submitted": len(m_reqs),
            "new_lgs_issued": len(m_lgs),
            "issued_volume_base": round(
                sum(to_base(float(lg.current_amount or 0), lg.currency_id) for lg in m_lgs), 2
            ),
            "extensions": sum(1 for m in m_maint if (m.action_type or "").upper() == "EXTEND"),
            "closures": sum(1 for m in m_maint if (m.action_type or "").upper() == "CLOSE"),
            "other_maintenance": sum(1 for m in m_maint if (m.action_type or "").upper() not in ("EXTEND", "CLOSE")),
        })

    # 5e. Verification & Custody Governance Quality (Live Portfolio)
    verif_quality = {
        "matched": sum(1 for lg in live_lgs if (lg.verification_status or "").upper() == "MATCHED"),
        "accepted_with_discrepancy": sum(1 for lg in live_lgs if (lg.verification_status or "").upper() == "ACCEPTED"),
        "discrepancy_open": sum(1 for lg in live_lgs if (lg.verification_status or "").upper() in ("DISCREPANCY", "DISCREPANCY_REJECTED")),
        "pending_verification": sum(1 for lg in live_lgs if not lg.verification_status or (lg.verification_status or "").upper() == "PENDING"),
        "handed_over": sum(1 for lg in live_lgs if lg.handover_date is not None),
        "pending_handover": sum(1 for lg in live_lgs if lg.handover_date is None),
    }

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "period": {
            "start_date": eff_start.isoformat(),
            "end_date": eff_end.isoformat(),
        },
        "base_currency": base_ccy,
        "available_currencies": fx_ctx["available_currencies"],
        "fx_rates_applied": fx_ctx["fx_rates_applied"],
        "fx_unconverted_currencies": sorted(fx_ctx["unconverted_codes"]),
        "filter_options": {
            "entities": [
                {"id": e.id, "name": e.entity_name, "code": e.code}
                for e in tenant_entities
            ],
            "banks": sorted(bank_lookup.values(), key=lambda b: b["name"]),
        },
        "executive_kpis": {
            "active_lgs_count": len(live_lgs),
            "in_flight_lgs_count": len(in_flight_lgs),
            "active_exposure_base": round(active_exposure_base, 2),
            "by_currency": [
                {
                    "currency": v["currency"],
                    "count": v["count"],
                    "native_amount": round(v["native_amount"], 2),
                    "base_amount": round(v["base_amount"], 2),
                    "share_pct": round((v["base_amount"] / active_exposure_base) * 100.0, 1) if active_exposure_base > 0 else 0.0,
                }
                for v in sorted(ccy_summary.values(), key=lambda x: x["base_amount"], reverse=True)
            ],
            "total_facility_limit_base": round(total_fac_limit_base, 2),
            "utilized_facility_base": round(total_fac_utilized_base, 2),
            "headroom_base": round(headroom_base, 2),
            "utilization_pct": util_pct,
            "locked_cash_margin_base": round(locked_margin_base, 2),
            "weighted_margin_pct": weighted_margin_pct,
            "cash_backed_share_pct": cash_backed_share_pct,
            "annualized_commission_base": round(annualized_comm_base, 2),
            "weighted_commission_rate_pct": weighted_comm_rate_pct,
            "priced_lgs_count": priced_lgs_count,
            "unpriced_lgs_count": unpriced_lgs_count,
            "expired_unreleased_count": expired_unreleased_count,
            "expired_unreleased_base": round(expired_unreleased_base, 2),
        },
        "maturity_ladder": maturity_ladder,
        "maturity_watch_items": maturity_watch_items[:25],
        "allocations": {
            "by_entity": alloc_by_entity,
            "by_department": alloc_by_dept,
            "by_project": alloc_by_project,
            "by_lg_type": alloc_by_lg_type,
            "by_beneficiary": alloc_by_beneficiary,
        },
        "bank_scorecard": bank_scorecard,
        "operational_velocity": {
            "turnaround": turnaround_metrics,
            "period_funnel": period_funnel,
            "maintenance_by_type": maint_by_type,
            "maintenance_by_status": maint_by_status,
            "maintenance_total_in_period": len(period_maint),
            "monthly_trend": monthly_trend,
            "verification_quality": verif_quality,
        },
    }


# ==============================================================================
# H2: Supporting Document Analysis During Request
# ==============================================================================

