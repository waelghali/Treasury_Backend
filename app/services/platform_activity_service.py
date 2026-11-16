# app/services/platform_activity_service.py
"""
Unified Platform Activity & Control Verification Service for Corporate Admin / Signatory Oversight.

Synthesizes clean, human-readable business activity across all 6 platform domains:
  1. LG Custody (LGRecord, LGInstruction, ApprovalRequest, MigrationBatch)
  2. LG Issuance & Facilities (IssuanceRequest, IssuedLGRecord, IssuanceMaintenanceAction,
     IssuanceFacilityAuditLog, ReconciliationSession)
  3. RFQ Quotations (QuotationRequest, QuotationOffer, QuotationTBillOffer)
  4. Bank Reconciliation (BankStatement, ReconciliationMatch, ClassificationRule)
  5. Smart Inbox (InboxItem, InboxOutboundRequest)
  6. Governance & Admin (AdminChangeRequest, User, CustomerEntity, Department,
     ApprovalGroup, LGCategory, CustomerConfiguration)

Also powers the Pre-Bank Signatory Verification Desk:
  - Pending Bank Letters Queue (Custody + Issuance unified, with risk/anomaly badges)
  - Expiry Cohort Completeness Check (including LGs recently extended out of the window)
  - Handed-List Reconciliation / Verifier (paste LG numbers or Instruction Serials)
  - Cross-Module Control Exceptions Radar
"""

import logging
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session, selectinload

from app.constants import LgStatusEnum
from app.models.models import (
    ApprovalGroup,
    ApprovalRequest,
    AuditLog,
    Bank,
    Currency,
    CustomerConfiguration,
    CustomerEntity,
    Department,
    InternalOwnerContact,
    LGCategory,
    LGInstruction,
    LGRecord,
    MigrationBatch,
    User,
)
from app.models.models_inbox import InboxItem, InboxOutboundRequest
from app.models.models_issuance import (
    AdminChangeRequest,
    CustomerBankAccount,
    IssuanceExposureEntry,
    IssuanceFacility,
    IssuanceFacilityAuditLog,
    IssuanceMaintenanceAction,
    IssuanceRequest,
    IssuedLGRecord,
    ReconciliationSession,
)
from app.models.models_quotation import QuotationRequest
from app.models.models_reconciliation_v2 import (
    BankStatement,
    BankTransaction,
    ClassificationRule,
    ReconciliationMatch,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Date / Time & Formatting Helpers (100% naive/aware safe)
# ──────────────────────────────────────────────────────────────────────────────

def _to_utc_datetime(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    if isinstance(val, date):
        return datetime.combine(val, time.min, tzinfo=timezone.utc)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            try:
                d = datetime.strptime(s[:10], "%Y-%m-%d")
                return d.replace(tzinfo=timezone.utc)
            except Exception:
                return None
    return None


def _to_iso(val: Any) -> Optional[str]:
    dt = _to_utc_datetime(val)
    return dt.isoformat() if dt else None


def _to_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return datetime.strptime(val.strip()[:10], "%Y-%m-%d").date()
        except Exception:
            dt = _to_utc_datetime(val)
            return dt.date() if dt else None
    return None


def _fmt_date_human(val: Any) -> str:
    d = _to_date(val)
    if not d:
        return "N/A"
    return d.strftime("%d-%b-%Y")


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def _user_label(user: Optional[User], fallback_id: Optional[int] = None, default: str = "System") -> str:
    if user is not None:
        email = getattr(user, "email", None)
        full_name = getattr(user, "full_name", None)
        if full_name and email:
            return f"{full_name} ({email})"
        if email:
            return email
        if full_name:
            return full_name
    if fallback_id:
        return f"User #{fallback_id}"
    return default


def _custody_bank_name(lg: Optional[LGRecord]) -> Optional[str]:
    if not lg:
        return None
    if getattr(lg, "foreign_bank_name", None):
        if getattr(lg, "communication_bank", None) and getattr(lg.communication_bank, "name", None):
            return f"{lg.foreign_bank_name} (via {lg.communication_bank.name})"
        return lg.foreign_bank_name
    if getattr(lg, "issuing_bank", None) and getattr(lg.issuing_bank, "name", None):
        return lg.issuing_bank.name
    return None


def _custody_instruction_lifecycle(inst: LGInstruction, now_utc: datetime) -> Tuple[str, str, List[str]]:
    """
    Returns (lifecycle_stage, lifecycle_label, warning_badges) for a Custody LGInstruction.
    """
    warnings: List[str] = []
    status_str = (inst.status or "").strip()
    details = inst.details if isinstance(inst.details, dict) else {}

    # Check bank terms discrepancy flag
    discrepancy_info = details.get("bank_terms_admin_approval")
    if isinstance(discrepancy_info, dict):
        disc_status = discrepancy_info.get("status")
        if disc_status == "PENDING_ADMIN":
            warnings.append("Bank Reply Discrepancy Pending")
        elif disc_status == "REJECTED_BY_ADMIN":
            warnings.append("Bank Terms Rejected")

    inst_dt = _to_utc_datetime(inst.instruction_date or inst.created_at)
    days_since_issue = (now_utc - inst_dt).days if inst_dt else 0

    if status_str.lower() in ("canceled", "cancelled"):
        return "CANCELED", "Canceled", warnings

    if inst.bank_reply_date or status_str == "Confirmed by Bank":
        return "BANK_CONFIRMED", "Confirmed by Bank", warnings

    if inst.delivery_date or status_str == "Instruction Delivered":
        deliv_dt = _to_utc_datetime(inst.delivery_date)
        days_waiting_reply = (now_utc - deliv_dt).days if deliv_dt else 0
        if days_waiting_reply > 10:
            warnings.append(f"No Bank Reply ({days_waiting_reply}d)")
        return "DELIVERED_AWAITING_REPLY", "Delivered · Awaiting Bank Reply", warnings

    if days_since_issue > 5:
        warnings.append(f"Undelivered ({days_since_issue}d)")

    if inst.is_printed:
        return "PRINTED_PENDING_DELIVERY", "Printed · Awaiting Sign / Delivery", warnings

    return "ISSUED_PENDING_PRINT", "Issued · Awaiting Print & Sign", warnings


def _map_custody_instruction_category(inst_type: str) -> Tuple[str, str]:
    t = (inst_type or "").upper()
    if "EXTEND" in t or "EXTENSION" in t:
        return "RENEWAL", "LG Renewal / Extension"
    if "RELEASE" in t:
        return "RELEASE", "LG Release"
    if "LIQUIDAT" in t:
        return "LIQUIDATION", "LG Liquidation"
    if "DECREASE" in t:
        return "AMENDMENT", "LG Amount Decrease"
    if "AMEND" in t:
        return "AMENDMENT", "LG Amendment"
    if "ACTIVAT" in t:
        return "AMENDMENT", "Activate Non-Operative LG"
    if "REMINDER" in t:
        return "LOGISTICS", "Bank Reminder Letter"
    if "CANCEL" in t:
        return "RELEASE", "Cancel Instruction"
    return "AMENDMENT", inst_type.replace("LG_", "").replace("_", " ").title()


def _summarize_custody_instruction(inst: LGInstruction) -> Tuple[str, Optional[str], Optional[str]]:
    details = inst.details if isinstance(inst.details, dict) else {}
    t = (inst.instruction_type or "").upper()
    ccy = details.get("lg_currency") or (
        inst.lg_record.lg_currency.iso_code if (inst.lg_record and inst.lg_record.lg_currency) else ""
    )

    if "EXTEND" in t or "EXTENSION" in t:
        old_exp = details.get("old_expiry_date")
        new_exp = details.get("new_expiry_date")
        period = details.get("extension_period")
        period_suffix = f" ({period})" if period else ""
        return (
            f"Extended expiry {_fmt_date_human(old_exp)} → {_fmt_date_human(new_exp)}{period_suffix}",
            old_exp,
            new_exp,
        )
    if "DECREASE" in t:
        dec = _safe_float(details.get("decrease_amount"))
        new_amt = _safe_float(details.get("new_lg_amount") or details.get("new_amount"))
        old_amt = _safe_float(details.get("original_lg_amount") or details.get("old_amount"))
        dec_str = f"{ccy} {dec:,.2f}".strip() if dec is not None else "amount"
        new_str = f"{ccy} {new_amt:,.2f}".strip() if new_amt is not None else "N/A"
        return (
            f"Decreased by {dec_str} → New balance {new_str}",
            f"{old_amt:,.2f}" if old_amt is not None else None,
            f"{new_amt:,.2f}" if new_amt is not None else None,
        )
    if "LIQUIDAT" in t:
        liq_type = details.get("liquidation_type", "Full/Partial")
        liq_amt = _safe_float(details.get("liquidation_amount"))
        amt_str = f" ({ccy} {liq_amt:,.2f})" if liq_amt is not None else ""
        return (f"Liquidation instruction issued — {liq_type}{amt_str}", None, "Liquidated")
    if "RELEASE" in t:
        return ("Official bank release instruction issued to return/close LG", "Valid", "Released")
    if "AMEND" in t:
        reason = details.get("reason") or details.get("amendment_details") or "Terms updated"
        return (f"Amendment instruction issued ({str(reason)[:90]})", None, None)
    if "ACTIVAT" in t:
        return ("Activation instruction issued for non-operative LG", "Non-Operative", "Operative")
    return (f"Instruction {inst.serial_number} issued", None, None)


def _issuance_maint_lifecycle(act: IssuanceMaintenanceAction, now_utc: datetime) -> Tuple[str, str, List[str]]:
    warnings: List[str] = []
    st = (act.status or "").upper()
    inst_st = (act.instruction_status or "").strip()

    if st == "PENDING_APPROVAL":
        return "PENDING_APPROVAL", f"Pending Approval (Step {act.current_step_number or 1})", warnings
    if st == "REJECTED":
        return "REJECTED", "Rejected in Approval", warnings
    if st in ("CANCELLED", "CANCELED"):
        return "CANCELED", "Canceled", warnings

    if act.bank_reply_date or inst_st == "Confirmed by Bank":
        return "BANK_CONFIRMED", "Confirmed by Bank", warnings
    if act.delivery_date or inst_st == "Instruction Delivered":
        deliv_dt = _to_utc_datetime(act.delivery_date)
        days_w = (now_utc - deliv_dt).days if deliv_dt else 0
        if days_w > 10:
            warnings.append(f"No Bank Reply ({days_w}d)")
        return "DELIVERED_AWAITING_REPLY", "Delivered · Awaiting Bank Reply", warnings

    created_dt = _to_utc_datetime(act.updated_at or act.created_at)
    days_since = (now_utc - created_dt).days if created_dt else 0
    if days_since > 5 and act.letter_serial_number:
        warnings.append(f"Undelivered ({days_since}d)")

    if act.is_printed:
        return "PRINTED_PENDING_DELIVERY", "Printed · Awaiting Sign / Delivery", warnings
    if act.letter_serial_number or inst_st == "Instruction Issued":
        return "ISSUED_PENDING_PRINT", "Approved · Letter Ready to Sign", warnings
    return "COMPLETED", st.replace("_", " ").title(), warnings


# ──────────────────────────────────────────────────────────────────────────────
# Main Platform Activity Service
# ──────────────────────────────────────────────────────────────────────────────

class PlatformActivityService:

    def get_platform_activity_feed(
        self,
        db: Session,
        customer_id: int,
        module_filter: str = "ALL",
        category_filter: str = "ALL",
        user_id_filter: Optional[int] = None,
        actor_key_filter: Optional[str] = None,
        entity_id_filter: Optional[int] = None,
        bank_filter: Optional[str] = None,
        lifecycle_filter: str = "ALL",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 250,
    ) -> Dict[str, Any]:
        """
        Builds a unified, noise-free business activity stream across all 6 platform modules,
        with true cross-filtered dynamic facets so the UI only shows filter options that actually exist.
        """
        now_utc = datetime.now(timezone.utc)
        dt_start = _to_utc_datetime(start_date) if start_date else None
        dt_end = None
        if end_date:
            d_end = _to_date(end_date)
            if d_end:
                dt_end = datetime.combine(d_end, time(23, 59, 59), tzinfo=timezone.utc)
            else:
                dt_end = _to_utc_datetime(end_date)

        # Preload customer users & entities for fast, N+1-free resolution
        users_by_id: Dict[int, User] = {
            u.id: u for u in db.query(User).filter(User.customer_id == customer_id).all()
        }
        entities_by_id: Dict[int, CustomerEntity] = {
            e.id: e for e in db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer_id).all()
        }
        banks_by_id: Dict[int, str] = {
            b.id: b.name for b in db.query(Bank).all()
        }
        currencies_by_id: Dict[int, str] = {
            c.id: c.iso_code for c in db.query(Currency).all()
        }

        events: List[Dict[str, Any]] = []
        want_mod = (module_filter or "ALL").upper()

        # Collect all 6 modules so cross-filtered facets always reflect live available options
        try:
            events.extend(self._collect_custody_events(db, customer_id, now_utc, users_by_id, entities_by_id))
        except Exception as e:
            logger.warning(f"Activity feed custody collector error: {e}", exc_info=True)

        try:
            events.extend(
                self._collect_issuance_events(
                    db, customer_id, now_utc, users_by_id, entities_by_id, banks_by_id, currencies_by_id
                )
            )
        except Exception as e:
            logger.warning(f"Activity feed issuance collector error: {e}", exc_info=True)

        try:
            events.extend(self._collect_quotation_events(db, customer_id, users_by_id, entities_by_id))
        except Exception as e:
            logger.warning(f"Activity feed quotations collector error: {e}", exc_info=True)

        try:
            events.extend(
                self._collect_reconciliation_events(db, customer_id, users_by_id, banks_by_id, currencies_by_id)
            )
        except Exception as e:
            logger.warning(f"Activity feed reconciliation collector error: {e}", exc_info=True)

        try:
            events.extend(self._collect_inbox_events(db, customer_id, users_by_id, banks_by_id))
        except Exception as e:
            logger.warning(f"Activity feed smart inbox collector error: {e}", exc_info=True)

        try:
            events.extend(self._collect_governance_events(db, customer_id, users_by_id, entities_by_id))
        except Exception as e:
            logger.warning(f"Activity feed governance collector error: {e}", exc_info=True)

        # ── Step 1: Apply Base Window (Date Range + Search Text) ──────────────
        q_lower = search.strip().lower() if search and search.strip() else None
        base_events: List[Dict[str, Any]] = []

        for ev in events:
            ev_dt = _to_utc_datetime(ev.get("timestamp"))
            if dt_start and (not ev_dt or ev_dt < dt_start):
                continue
            if dt_end and (not ev_dt or ev_dt > dt_end):
                continue
            if q_lower:
                haystack = " ".join(
                    str(x)
                    for x in [
                        ev.get("action_label"),
                        ev.get("actor_name"),
                        ev.get("approver_name"),
                        ev.get("entity_name"),
                        ev.get("bank_name"),
                        ev.get("reference"),
                        ev.get("secondary_ref"),
                        ev.get("beneficiary_name"),
                        ev.get("summary"),
                        " ".join(
                            f"{c.get('reference', '')} {c.get('secondary_ref', '')} {c.get('bank_name', '')}"
                            for c in (ev.get("children") or [])
                        ),
                    ]
                    if x
                ).lower()
                if q_lower not in haystack:
                    continue

            # Assign deterministic actor_key so both internal users and external/system actors are filterable
            if ev.get("actor_id"):
                ev["actor_key"] = f"id:{ev['actor_id']}"
            else:
                ev["actor_key"] = f"name:{ev.get('actor_name') or 'System'}"
            base_events.append(ev)

        # ── Step 2: Per-Dimension Predicates for Cross-Filtered Facets ────────
        cat_upper = (category_filter or "ALL").upper()
        life_upper = (lifecycle_filter or "ALL").upper()
        eff_actor_key = (
            actor_key_filter
            if (actor_key_filter and actor_key_filter != "ALL")
            else (f"id:{user_id_filter}" if user_id_filter else None)
        )
        eff_bank = bank_filter if (bank_filter and bank_filter != "ALL") else None

        def _match_mod(ev: Dict[str, Any]) -> bool:
            return want_mod == "ALL" or ev.get("module") == want_mod

        def _match_cat(ev: Dict[str, Any]) -> bool:
            return cat_upper == "ALL" or (ev.get("category") or "").upper() == cat_upper

        def _match_life_bucket(ev: Dict[str, Any], target_life: str) -> bool:
            if target_life == "ALL":
                return True
            st = (ev.get("lifecycle_stage") or "").upper()
            if target_life == "NEEDS_SIGN_OR_DELIVERY":
                return st in ("ISSUED_PENDING_PRINT", "PRINTED_PENDING_DELIVERY")
            if target_life == "AWAITING_BANK_REPLY":
                return st == "DELIVERED_AWAITING_REPLY"
            if target_life == "PENDING_APPROVAL":
                return st == "PENDING_APPROVAL"
            if target_life == "FLAGGED":
                return bool(ev.get("has_warning"))
            if target_life == "COMPLETED":
                return st in ("BANK_CONFIRMED", "COMPLETED", "APPROVED")
            return st == target_life

        def _match_life(ev: Dict[str, Any]) -> bool:
            return _match_life_bucket(ev, life_upper)

        def _ev_actor_keys(ev: Dict[str, Any]) -> Set[str]:
            keys = {ev["actor_key"]}
            for c in (ev.get("children") or []):
                if c.get("actor_id"):
                    keys.add(f"id:{c['actor_id']}")
                elif c.get("actor_name"):
                    keys.add(f"name:{c['actor_name']}")
            return keys

        def _match_actor(ev: Dict[str, Any]) -> bool:
            if not eff_actor_key:
                return True
            return eff_actor_key in _ev_actor_keys(ev)

        def _ev_entities(ev: Dict[str, Any]) -> List[Tuple[int, str]]:
            pairs: Dict[int, str] = {}
            if ev.get("entity_id") and ev.get("entity_name") and ev.get("entity_name") != "Multiple Entities":
                pairs[int(ev["entity_id"])] = str(ev["entity_name"])
            for c in (ev.get("children") or []):
                if c.get("entity_id") and c.get("entity_name"):
                    pairs[int(c["entity_id"])] = str(c["entity_name"])
            return list(pairs.items())

        def _match_entity(ev: Dict[str, Any]) -> bool:
            if not entity_id_filter:
                return True
            return any(eid == entity_id_filter for eid, _ in _ev_entities(ev))

        def _ev_banks(ev: Dict[str, Any]) -> Set[str]:
            banks: Set[str] = set()
            b = ev.get("bank_name")
            if b and not b.endswith(" Banks") and b != "All Banks":
                banks.add(str(b))
            for c in (ev.get("children") or []):
                if c.get("bank_name"):
                    banks.add(str(c["bank_name"]))
            return banks

        def _match_bank(ev: Dict[str, Any]) -> bool:
            if not eff_bank:
                return True
            return eff_bank in _ev_banks(ev)

        # ── Step 3: Compute Dynamic Cross-Filtered Facets (Only > 0 Counts!) ──
        module_counts: Dict[str, int] = defaultdict(int)
        category_counts: Dict[str, int] = defaultdict(int)
        actor_counts: Dict[str, Dict[str, Any]] = {}
        entity_counts: Dict[int, Dict[str, Any]] = {}
        bank_counts: Dict[str, int] = defaultdict(int)
        lifecycle_counts: Dict[str, int] = {
            "ALL": 0,
            "NEEDS_SIGN_OR_DELIVERY": 0,
            "AWAITING_BANK_REPLY": 0,
            "PENDING_APPROVAL": 0,
            "FLAGGED": 0,
            "COMPLETED": 0,
        }

        filtered: List[Dict[str, Any]] = []

        for ev in base_events:
            m_mod = _match_mod(ev)
            m_cat = _match_cat(ev)
            m_life = _match_life(ev)
            m_act = _match_actor(ev)
            m_ent = _match_entity(ev)
            m_bnk = _match_bank(ev)

            # Module facet (all other filters applied)
            if m_cat and m_life and m_act and m_ent and m_bnk:
                module_counts[ev["module"]] += 1

            # Category facet (all other filters applied)
            if m_mod and m_life and m_act and m_ent and m_bnk:
                cat_val = (ev.get("category") or "OTHER").upper()
                category_counts[cat_val] += 1

            # Actor facet (all other filters applied)
            if m_mod and m_cat and m_life and m_ent and m_bnk:
                akey = ev["actor_key"]
                if akey not in actor_counts:
                    actor_counts[akey] = {
                        "value": akey,
                        "label": ev.get("actor_name") or "System",
                        "actor_type": ev.get("actor_type") or "INTERNAL",
                        "count": 0,
                    }
                actor_counts[akey]["count"] += 1

            # Entity facet (all other filters applied)
            if m_mod and m_cat and m_life and m_act and m_bnk:
                for eid, ename in _ev_entities(ev):
                    if eid not in entity_counts:
                        entity_counts[eid] = {"id": eid, "name": ename, "count": 0}
                    entity_counts[eid]["count"] += 1

            # Bank facet (all other filters applied)
            if m_mod and m_cat and m_life and m_act and m_ent:
                for bname in _ev_banks(ev):
                    bank_counts[bname] += 1

            # Lifecycle KPI facet (all other filters applied)
            if m_mod and m_cat and m_act and m_ent and m_bnk:
                lifecycle_counts["ALL"] += 1
                for lkey in ("NEEDS_SIGN_OR_DELIVERY", "AWAITING_BANK_REPLY", "PENDING_APPROVAL", "FLAGGED", "COMPLETED"):
                    if _match_life_bucket(ev, lkey):
                        lifecycle_counts[lkey] += 1

            if m_mod and m_cat and m_life and m_act and m_ent and m_bnk:
                filtered.append(ev)

        # Sort newest first
        filtered.sort(
            key=lambda item: _to_utc_datetime(item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

        total_matching = len(filtered)
        sliced = filtered[:limit]

        facets = {
            "modules": {k: v for k, v in module_counts.items() if v > 0},
            "categories": [
                {"value": k, "count": v}
                for k, v in sorted(category_counts.items(), key=lambda x: (-x[1], x[0]))
                if v > 0
            ],
            "actors": sorted(actor_counts.values(), key=lambda x: (-x["count"], x["label"])),
            "entities": sorted(entity_counts.values(), key=lambda x: (-x["count"], x["name"])),
            "banks": [
                {"name": k, "count": v}
                for k, v in sorted(bank_counts.items(), key=lambda x: (-x[1], x[0]))
                if v > 0
            ],
            "lifecycles": lifecycle_counts,
        }

        return {
            "total": total_matching,
            "stats": {
                "total_activities": lifecycle_counts["ALL"],
                "pending_sign_or_delivery": lifecycle_counts["NEEDS_SIGN_OR_DELIVERY"],
                "awaiting_bank_reply": lifecycle_counts["AWAITING_BANK_REPLY"],
                "flagged_items": lifecycle_counts["FLAGGED"],
                "module_counts": facets["modules"],
            },
            "facets": facets,
            "items": sliced,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Module Collectors
    # ──────────────────────────────────────────────────────────────────────────

    def _collect_custody_events(
        self,
        db: Session,
        customer_id: int,
        now_utc: datetime,
        users_by_id: Dict[int, User],
        entities_by_id: Dict[int, CustomerEntity],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # A. Custody Instructions (Extensions, Releases, Liquidations, Decreases, Amendments, Activations)
        instructions = (
            db.query(LGInstruction)
            .join(LGRecord, LGInstruction.lg_record_id == LGRecord.id)
            .options(
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.issuing_bank),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.communication_bank),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.lg_currency),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.beneficiary_corporate),
            )
            .filter(
                LGRecord.customer_id == customer_id,
                LGInstruction.is_deleted == False,
            )
            .order_by(LGInstruction.id.desc())
            .limit(500)
            .all()
        )

        bulk_batches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for inst in instructions:
            lg = inst.lg_record
            if not lg:
                continue
            details = inst.details if isinstance(inst.details, dict) else {}
            cat, label = _map_custody_instruction_category(inst.instruction_type)
            summary, chg_from, chg_to = _summarize_custody_instruction(inst)
            stage, stage_label, warnings = _custody_instruction_lifecycle(inst, now_utc)

            if cat == "RENEWAL" and lg.auto_renewal is False:
                warnings.append("Auto-Renew is OFF")

            maker = users_by_id.get(inst.maker_user_id)
            checker = users_by_id.get(inst.checker_user_id) if inst.checker_user_id else None
            entity_name = lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else (
                entities_by_id[lg.beneficiary_corporate_id].entity_name
                if lg.beneficiary_corporate_id in entities_by_id
                else None
            )

            item = {
                "id": f"custody-inst-{inst.id}",
                "timestamp": _to_iso(inst.instruction_date or inst.created_at),
                "module": "CUSTODY",
                "category": cat,
                "action_code": inst.instruction_type,
                "action_label": label,
                "actor_name": _user_label(maker, inst.maker_user_id),
                "actor_id": inst.maker_user_id,
                "actor_type": "INTERNAL",
                "approver_name": _user_label(checker, inst.checker_user_id, default="") or None,
                "entity_id": lg.beneficiary_corporate_id,
                "entity_name": entity_name,
                "bank_name": _custody_bank_name(lg),
                "reference": lg.lg_number,
                "secondary_ref": inst.serial_number,
                "beneficiary_name": lg.issuer_name,
                "amount": _safe_float(lg.lg_amount),
                "currency": lg.lg_currency.iso_code if lg.lg_currency else None,
                "summary": summary,
                "change_from": chg_from,
                "change_to": chg_to,
                "lifecycle_stage": stage,
                "lifecycle_label": stage_label,
                "has_warning": len(warnings) > 0,
                "warning_badges": warnings,
                "links": {
                    "lg_record_id": lg.id,
                    "instruction_id": inst.id,
                    "navigate_to": f"/corporate-admin/lg-records/{lg.id}",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            }

            batch_id = details.get("bulk_renewal_batch_id") if details.get("is_bulk_renewal") else None
            if batch_id:
                bulk_batches[str(batch_id)].append(item)
            else:
                out.append(item)

        # Collapse Bulk Renewal Batches into 1 clean parent row with expandable children
        for batch_id, batch_items in bulk_batches.items():
            if len(batch_items) == 1:
                out.append(batch_items[0])
                continue

            first = batch_items[0]
            printed_cnt = sum(1 for x in batch_items if x["lifecycle_stage"] != "ISSUED_PENDING_PRINT")
            deliv_cnt = sum(
                1 for x in batch_items if x["lifecycle_stage"] in ("DELIVERED_AWAITING_REPLY", "BANK_CONFIRMED")
            )
            conf_cnt = sum(1 for x in batch_items if x["lifecycle_stage"] == "BANK_CONFIRMED")
            warn_cnt = sum(1 for x in batch_items if x["has_warning"])

            # Aggregate stage of the batch
            if conf_cnt == len(batch_items):
                b_stage, b_label = "BANK_CONFIRMED", f"All {len(batch_items)} Confirmed by Bank"
            elif deliv_cnt == len(batch_items):
                b_stage, b_label = "DELIVERED_AWAITING_REPLY", f"{deliv_cnt}/{len(batch_items)} Delivered · {conf_cnt} Confirmed"
            elif deliv_cnt > 0:
                b_stage, b_label = "PRINTED_PENDING_DELIVERY", f"{deliv_cnt}/{len(batch_items)} Delivered · {len(batch_items) - deliv_cnt} Pending"
            elif printed_cnt > 0:
                b_stage, b_label = "PRINTED_PENDING_DELIVERY", f"{printed_cnt}/{len(batch_items)} Printed · Awaiting Delivery"
            else:
                b_stage, b_label = "ISSUED_PENDING_PRINT", f"{len(batch_items)} Letters Issued · Ready to Sign"

            totals_by_ccy: Dict[str, float] = defaultdict(float)
            for x in batch_items:
                if x.get("amount") and x.get("currency"):
                    totals_by_ccy[x["currency"]] += float(x["amount"])
            totals_str = " + ".join(f"{ccy} {amt:,.0f}" for ccy, amt in totals_by_ccy.items())
            single_ccy = list(totals_by_ccy.keys())[0] if len(totals_by_ccy) == 1 else None
            single_amt = totals_by_ccy[single_ccy] if single_ccy else None

            batch_warnings = []
            if warn_cnt > 0:
                batch_warnings.append(f"{warn_cnt} LG(s) Flagged")

            out.append({
                "id": f"custody-batch-{batch_id}",
                "timestamp": first["timestamp"],
                "module": "CUSTODY",
                "category": "RENEWAL",
                "action_code": "LG_BULK_RENEWAL",
                "action_label": f"Bulk Renewal Run ({len(batch_items)} LGs)",
                "actor_name": first["actor_name"],
                "actor_id": first["actor_id"],
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": first["entity_id"] if len({x["entity_id"] for x in batch_items}) == 1 else None,
                "entity_name": first["entity_name"] if len({x["entity_name"] for x in batch_items}) == 1 else "Multiple Entities",
                "bank_name": first["bank_name"] if len({x["bank_name"] for x in batch_items}) == 1 else f"{len({x['bank_name'] for x in batch_items if x.get('bank_name')})} Banks",
                "reference": batch_id,
                "secondary_ref": f"{len(batch_items)} Instructions",
                "beneficiary_name": None,
                "amount": single_amt,
                "currency": single_ccy,
                "summary": f"Renewed {len(batch_items)} Custody LGs ({totals_str or 'Mixed Currencies'}) — expand to inspect each letter",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": b_stage,
                "lifecycle_label": b_label,
                "has_warning": warn_cnt > 0,
                "warning_badges": batch_warnings,
                "links": {"navigate_to": "/corporate-admin/action-center"},
                "is_batch": True,
                "batch_count": len(batch_items),
                "children": batch_items,
            })

        # B. Newly Recorded Custody LGs (excluding legacy bulk migration rows which are grouped via MigrationBatch)
        recent_lgs = (
            db.query(LGRecord)
            .options(
                selectinload(LGRecord.issuing_bank),
                selectinload(LGRecord.communication_bank),
                selectinload(LGRecord.lg_currency),
                selectinload(LGRecord.beneficiary_corporate),
                selectinload(LGRecord.lg_status),
            )
            .filter(LGRecord.customer_id == customer_id, LGRecord.is_deleted == False)
            .order_by(LGRecord.id.desc())
            .limit(150)
            .all()
        )
        # Map LGRecord CREATE actor from AuditLog if present
        lg_ids = [lg.id for lg in recent_lgs]
        creator_by_lg: Dict[int, Optional[int]] = {}
        if lg_ids:
            create_logs = (
                db.query(AuditLog.entity_id, AuditLog.user_id)
                .filter(
                    AuditLog.customer_id == customer_id,
                    AuditLog.entity_type == "LGRecord",
                    AuditLog.action_type.in_(["CREATE", "LG_CREATED", "MIGRATION_IMPORT_RECORD"]),
                    AuditLog.entity_id.in_(lg_ids),
                )
                .all()
            )
            for eid, uid in create_logs:
                if eid and eid not in creator_by_lg:
                    creator_by_lg[eid] = uid

        for lg in recent_lgs:
            if lg.migration_source == "LEGACY":
                continue
            uid = creator_by_lg.get(lg.id)
            actor = users_by_id.get(uid) if uid else None
            status_name = lg.lg_status.name if lg.lg_status else "Valid"
            out.append({
                "id": f"custody-lg-new-{lg.id}",
                "timestamp": _to_iso(lg.created_at or lg.issuance_date),
                "module": "CUSTODY",
                "category": "NEW_LG",
                "action_code": "LG_RECORDED",
                "action_label": "New Custody LG Recorded",
                "actor_name": _user_label(actor, uid, default="Treasury User"),
                "actor_id": uid,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": lg.beneficiary_corporate_id,
                "entity_name": lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else None,
                "bank_name": _custody_bank_name(lg),
                "reference": lg.lg_number,
                "secondary_ref": None,
                "beneficiary_name": lg.issuer_name,
                "amount": _safe_float(lg.lg_amount),
                "currency": lg.lg_currency.iso_code if lg.lg_currency else None,
                "summary": f"Recorded incoming LG from {lg.issuer_name} · Expires {_fmt_date_human(lg.expiry_date)}",
                "change_from": _to_date(lg.issuance_date).isoformat() if _to_date(lg.issuance_date) else None,
                "change_to": _to_date(lg.expiry_date).isoformat() if _to_date(lg.expiry_date) else None,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": f"Custody · {status_name}",
                "has_warning": False,
                "warning_badges": [],
                "links": {
                    "lg_record_id": lg.id,
                    "navigate_to": f"/corporate-admin/lg-records/{lg.id}",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # C. Custody Approval Requests that are PENDING / REJECTED / WITHDRAWN (Approved ones already produced an LGInstruction above!)
        approvals = (
            db.query(ApprovalRequest)
            .options(selectinload(ApprovalRequest.lg_record).selectinload(LGRecord.lg_currency))
            .filter(ApprovalRequest.customer_id == customer_id)
            .order_by(ApprovalRequest.id.desc())
            .limit(150)
            .all()
        )
        for req in approvals:
            st_val = req.status.value if hasattr(req.status, "value") else str(req.status)
            if st_val == "APPROVED" and req.related_instruction_id:
                continue  # Already represented cleanly by the generated LGInstruction!
            maker = users_by_id.get(req.maker_user_id)
            checker = users_by_id.get(req.checker_user_id) if req.checker_user_id else None
            lg = req.lg_record
            cat, base_lbl = _map_custody_instruction_category(req.action_type)
            stage_map = {
                "PENDING": ("PENDING_APPROVAL", "Pending Maker-Checker Approval"),
                "REJECTED": ("REJECTED", "Rejected by Approver"),
                "WITHDRAWN": ("CANCELED", "Withdrawn by Maker"),
                "AUTO_REJECTED_EXPIRED": ("REJECTED", "Auto-Expired (Approval Timeout)"),
            }
            st_code, st_lbl = stage_map.get(st_val, ("COMPLETED", st_val.replace("_", " ").title()))
            out.append({
                "id": f"custody-apr-{req.id}",
                "timestamp": _to_iso(req.updated_at or req.created_at),
                "module": "CUSTODY",
                "category": "APPROVAL" if st_val != "PENDING" else cat,
                "action_code": f"APPROVAL_{req.action_type}",
                "action_label": f"{base_lbl} Request ({st_val.title()})",
                "actor_name": _user_label(maker, req.maker_user_id),
                "actor_id": req.maker_user_id,
                "actor_type": "INTERNAL",
                "approver_name": _user_label(checker, req.checker_user_id, default="") or None,
                "entity_id": lg.beneficiary_corporate_id if lg else None,
                "entity_name": (
                    entities_by_id[lg.beneficiary_corporate_id].entity_name
                    if (lg and lg.beneficiary_corporate_id in entities_by_id)
                    else None
                ),
                "bank_name": _custody_bank_name(lg) if lg else None,
                "reference": lg.lg_number if lg else f"Req #{req.id}",
                "secondary_ref": f"APR-{req.id}",
                "beneficiary_name": lg.issuer_name if lg else None,
                "amount": _safe_float(lg.lg_amount) if lg else None,
                "currency": lg.lg_currency.iso_code if (lg and lg.lg_currency) else None,
                "summary": f"{base_lbl} submitted for approval" + (f" · Reason: {req.reason}" if req.reason else ""),
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": st_code,
                "lifecycle_label": st_lbl,
                "has_warning": st_val in ("REJECTED", "AUTO_REJECTED_EXPIRED"),
                "warning_badges": ["Rejected"] if st_val == "REJECTED" else [],
                "links": {
                    "lg_record_id": lg.id if lg else None,
                    "navigate_to": "/corporate-admin/approval-requests",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # D. Auto-Renewal Toggle & Owner Changes (meaningful policy/custody changes without instructions)
        policy_logs = (
            db.query(AuditLog)
            .options(selectinload(AuditLog.lg_record))
            .filter(
                AuditLog.customer_id == customer_id,
                AuditLog.action_type.in_([
                    "LG_AUTO_RENEWAL_TOGGLED",
                    "LG_TOGGLE_AUTO_RENEWAL",
                    "LG_SINGLE_OWNER_CHANGED",
                    "LG_BULK_OWNER_CHANGED",
                ]),
            )
            .order_by(AuditLog.id.desc())
            .limit(100)
            .all()
        )
        for log in policy_logs:
            det = log.details if isinstance(log.details, dict) else {}
            actor = users_by_id.get(log.user_id) if log.user_id else None
            lg = log.lg_record
            if "AUTO_RENEW" in (log.action_type or ""):
                new_state = det.get("new_auto_renewal_status")
                state_str = "ON" if new_state else "OFF"
                lbl = f"Auto-Renewal Switched {state_str}"
                summ = f"Set Auto-Renewal to {state_str}" + (f" · Reason: {det.get('reason')}" if det.get("reason") else "")
                warns = ["Auto-Renew Turned OFF"] if new_state is False else []
            else:
                lbl = "Internal LG Owner Reassigned"
                summ = f"Owner reassigned to {det.get('new_owner_email') or 'new owner'}"
                warns = []

            out.append({
                "id": f"custody-log-{log.id}",
                "timestamp": _to_iso(log.timestamp),
                "module": "CUSTODY",
                "category": "AMENDMENT",
                "action_code": log.action_type,
                "action_label": lbl,
                "actor_name": _user_label(actor, log.user_id),
                "actor_id": log.user_id,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": lg.beneficiary_corporate_id if lg else None,
                "entity_name": (
                    entities_by_id[lg.beneficiary_corporate_id].entity_name
                    if (lg and lg.beneficiary_corporate_id in entities_by_id)
                    else None
                ),
                "bank_name": None,
                "reference": (lg.lg_number if lg else det.get("lg_number")) or f"Log #{log.id}",
                "secondary_ref": None,
                "beneficiary_name": lg.issuer_name if lg else None,
                "amount": _safe_float(lg.lg_amount) if lg else None,
                "currency": None,
                "summary": summ,
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": "Updated",
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {
                    "lg_record_id": lg.id if lg else None,
                    "navigate_to": f"/corporate-admin/lg-records/{lg.id}" if lg else "/corporate-admin/lg-records",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        return out

    def _collect_issuance_events(
        self,
        db: Session,
        customer_id: int,
        now_utc: datetime,
        users_by_id: Dict[int, User],
        entities_by_id: Dict[int, CustomerEntity],
        banks_by_id: Dict[int, str],
        currencies_by_id: Dict[int, str],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # A. Issuance Requests (Internal & External Portal Requestors)
        requests = (
            db.query(IssuanceRequest)
            .filter(IssuanceRequest.customer_id == customer_id, IssuanceRequest.is_deleted == False)
            .order_by(IssuanceRequest.id.desc())
            .limit(200)
            .all()
        )
        for req in requests:
            st = (req.status or "DRAFT").upper()
            if st == "DRAFT":
                continue  # Skip unfinished drafts
            is_external = req.requestor_user_id is None
            actor_name = (
                _user_label(users_by_id.get(req.requestor_user_id), req.requestor_user_id)
                if req.requestor_user_id
                else f"{req.requestor_name or 'External'} ({req.requestor_email or 'Portal'})"
            )
            ccy = currencies_by_id.get(req.currency_id)
            ent_name = entities_by_id[req.issuing_entity_id].entity_name if req.issuing_entity_id in entities_by_id else None

            stage_map = {
                "PENDING_APPROVAL": ("PENDING_APPROVAL", f"Pending Approval (Step {req.current_approval_step or 1})"),
                "APPROVED_INTERNAL": ("APPROVED", "Approved · Ready for Treasury Execution"),
                "FACILITY_RESERVED": ("APPROVED", "Facility Reserved · Ready to Issue"),
                "ISSUED": ("COMPLETED", "LG Issued"),
                "COMPLETED": ("COMPLETED", "Issued & Completed"),
                "REJECTED": ("REJECTED", "Request Rejected"),
                "REVISION_REQUIRED": ("PENDING_APPROVAL", "Returned for Revision"),
                "CANCELLED": ("CANCELED", "Request Cancelled"),
                "CANCELLATION_REQUESTED": ("PENDING_APPROVAL", "Cancellation Requested"),
            }
            l_stage, l_label = stage_map.get(st, ("COMPLETED", st.replace("_", " ").title()))
            warns = []
            if req.is_urgent:
                warns.append("Urgent Request")
            if req.requires_special_wording:
                warns.append("Special Wording")

            out.append({
                "id": f"iss-req-{req.id}",
                "timestamp": _to_iso(req.updated_at or req.created_at),
                "module": "ISSUANCE",
                "category": "NEW_LG" if st not in ("REJECTED", "CANCELLED") else "APPROVAL",
                "action_code": f"ISSUANCE_REQUEST_{st}",
                "action_label": f"LG Issuance Request ({st.replace('_', ' ').title()})",
                "actor_name": actor_name,
                "actor_id": req.requestor_user_id,
                "actor_type": "EXTERNAL_REQUESTOR" if is_external else "INTERNAL",
                "approver_name": None,
                "entity_id": req.issuing_entity_id,
                "entity_name": ent_name,
                "bank_name": None,
                "reference": req.serial_number,
                "secondary_ref": req.reference_number,
                "beneficiary_name": req.beneficiary_name,
                "amount": _safe_float(req.amount),
                "currency": ccy,
                "summary": f"Beneficiary: {req.beneficiary_name or 'N/A'} · Target Expiry: {_fmt_date_human(req.requested_expiry_date)}",
                "change_from": None,
                "change_to": _to_date(req.requested_expiry_date).isoformat() if _to_date(req.requested_expiry_date) else None,
                "lifecycle_stage": l_stage,
                "lifecycle_label": l_label,
                "has_warning": len(warns) > 0 or st == "REJECTED",
                "warning_badges": warns,
                "links": {"navigate_to": "/corporate-admin/issuance/requests"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # B. Issued LG Records (Execution, Delivery, Bank Reply, Discrepancy Verification)
        issued_lgs = (
            db.query(IssuedLGRecord)
            .filter(IssuedLGRecord.customer_id == customer_id)
            .order_by(IssuedLGRecord.id.desc())
            .limit(200)
            .all()
        )
        issued_map: Dict[int, IssuedLGRecord] = {lg.id: lg for lg in issued_lgs}

        for lg in issued_lgs:
            st = (lg.status or "").upper()
            bank_name = banks_by_id.get(lg.bank_id)
            ccy = currencies_by_id.get(lg.currency_id)
            ent_name = entities_by_id[lg.issuing_entity_id].entity_name if lg.issuing_entity_id in entities_by_id else None
            issuer_user = users_by_id.get(lg.issued_by_user_id) if lg.issued_by_user_id else None

            warns: List[str] = []
            if lg.verification_status == "DISCREPANCY":
                warns.append("Bank Copy Discrepancy")
            elif lg.verification_status == "ACCEPTED" and lg.verification_notes:
                warns.append("Discrepancy Accepted")
            if lg.reference_validity_flag == "EXCEEDED":
                warns.append("Exceeds Contract End Date")

            if st == "INTERNAL_PROCESSING" and not lg.delivery_date:
                l_stage, l_label = "ISSUED_PENDING_PRINT", "Bank Form Generated · Awaiting Delivery"
            elif st == "DELIVERED_TO_BANK" or (lg.delivery_date and not lg.bank_reply_date):
                l_stage, l_label = "DELIVERED_AWAITING_REPLY", "Delivered to Bank · Awaiting LG Copy"
            elif st in ("ACTIVE", "LG_ISSUED"):
                ver_lbl = f" · {lg.verification_status.title()}" if lg.verification_status else ""
                l_stage, l_label = "BANK_CONFIRMED", f"Active Bank LG{ver_lbl}"
            elif st in ("RETURNED", "CANCELLED"):
                l_stage, l_label = "CANCELED", st.title()
            else:
                l_stage, l_label = "COMPLETED", st.replace("_", " ").title()

            out.append({
                "id": f"iss-lg-{lg.id}",
                "timestamp": _to_iso(lg.updated_at or lg.created_at),
                "module": "ISSUANCE",
                "category": "NEW_LG",
                "action_code": "ISSUANCE_LG_ISSUED",
                "action_label": "Outgoing Bank LG Executed",
                "actor_name": _user_label(issuer_user, lg.issued_by_user_id, default="Treasury Officer"),
                "actor_id": lg.issued_by_user_id,
                "actor_type": "INTERNAL",
                "approver_name": (
                    _user_label(users_by_id.get(lg.verified_by_user_id), lg.verified_by_user_id, default="")
                    if lg.verified_by_user_id
                    else None
                ),
                "entity_id": lg.issuing_entity_id,
                "entity_name": ent_name,
                "bank_name": bank_name,
                "reference": lg.bank_lg_number or lg.lg_ref_number,
                "secondary_ref": lg.internal_serial or lg.lg_ref_number,
                "beneficiary_name": lg.beneficiary_name,
                "amount": _safe_float(lg.current_amount),
                "currency": ccy,
                "summary": f"Issued to {lg.beneficiary_name} via {bank_name or 'Bank'} · Expires {_fmt_date_human(lg.expiry_date)}",
                "change_from": _to_date(lg.issue_date).isoformat() if _to_date(lg.issue_date) else None,
                "change_to": _to_date(lg.expiry_date).isoformat() if _to_date(lg.expiry_date) else None,
                "lifecycle_stage": l_stage,
                "lifecycle_label": l_label,
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {
                    "issued_lg_id": lg.id,
                    "navigate_to": "/corporate-admin/issuance/issued-lgs",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # C. Issuance Maintenance Actions (EXTEND, INCREASE_AMOUNT, CLOSE, LIQUIDATION, AMENDMENT, ACTIVATE)
        maint_actions = (
            db.query(IssuanceMaintenanceAction)
            .join(IssuedLGRecord, IssuanceMaintenanceAction.issued_lg_id == IssuedLGRecord.id)
            .filter(
                IssuedLGRecord.customer_id == customer_id,
                IssuanceMaintenanceAction.is_deleted == False,
            )
            .order_by(IssuanceMaintenanceAction.id.desc())
            .limit(250)
            .all()
        )
        for act in maint_actions:
            lg = issued_map.get(act.issued_lg_id) or act.issued_lg
            if not lg:
                continue
            adata = act.action_data if isinstance(act.action_data, dict) else {}
            atype = (act.action_type or "").upper()
            cat_map = {
                "EXTEND": ("RENEWAL", "Issued LG Extension"),
                "INCREASE_AMOUNT": ("AMENDMENT", "Issued LG Amount Increase"),
                "CLOSE": ("RELEASE", "Issued LG Closure / Return"),
                "LIQUIDATION": ("LIQUIDATION", "Issued LG Liquidation"),
                "AMENDMENT": ("AMENDMENT", "Issued LG Amendment"),
                "ACTIVATE": ("AMENDMENT", "Issued LG Activation"),
                "CHANGE_OWNERSHIP": ("AMENDMENT", "Issued LG Owner Transfer"),
                "CANCEL_LIQUIDATION": ("AMENDMENT", "Cancel Liquidation"),
            }
            cat, lbl = cat_map.get(atype, ("AMENDMENT", f"Issuance {atype.title()}"))
            l_stage, l_label, warns = _issuance_maint_lifecycle(act, now_utc)
            if act.initiation_source == "BANK_INITIATED":
                warns.append("Bank-Initiated")

            chg_from, chg_to = None, None
            if atype == "EXTEND":
                old_e = adata.get("old_expiry_date") or adata.get("previous_expiry_date")
                new_e = adata.get("new_expiry_date")
                summ = f"Extend Issued LG expiry {_fmt_date_human(old_e) if old_e else ''} → {_fmt_date_human(new_e)}".strip()
                chg_from, chg_to = old_e, new_e
            elif atype == "INCREASE_AMOUNT":
                new_a = _safe_float(adata.get("new_amount"))
                ccy_s = currencies_by_id.get(lg.currency_id) or ""
                summ = f"Increase LG amount to {ccy_s} {new_a:,.2f}" if new_a is not None else "Increase LG amount"
            else:
                summ = act.notes or f"{lbl} on {lg.bank_lg_number or lg.lg_ref_number}"

            initiator = users_by_id.get(act.initiated_by_user_id) if act.initiated_by_user_id else None
            out.append({
                "id": f"iss-maint-{act.id}",
                "timestamp": _to_iso(act.updated_at or act.created_at),
                "module": "ISSUANCE",
                "category": cat,
                "action_code": f"ISSUANCE_MAINT_{atype}",
                "action_label": lbl,
                "actor_name": _user_label(initiator, act.initiated_by_user_id, default=act.initiation_source or "User"),
                "actor_id": act.initiated_by_user_id,
                "actor_type": "INTERNAL" if act.initiation_source == "INTERNAL_USER" else "EXTERNAL_REQUESTOR",
                "approver_name": None,
                "entity_id": lg.issuing_entity_id,
                "entity_name": entities_by_id[lg.issuing_entity_id].entity_name if lg.issuing_entity_id in entities_by_id else None,
                "bank_name": banks_by_id.get(lg.bank_id),
                "reference": lg.bank_lg_number or lg.lg_ref_number,
                "secondary_ref": act.letter_serial_number,
                "beneficiary_name": lg.beneficiary_name,
                "amount": _safe_float(lg.current_amount),
                "currency": currencies_by_id.get(lg.currency_id),
                "summary": summ,
                "change_from": chg_from,
                "change_to": chg_to,
                "lifecycle_stage": l_stage,
                "lifecycle_label": l_label,
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {
                    "issued_lg_id": lg.id,
                    "maintenance_action_id": act.id,
                    "navigate_to": "/corporate-admin/issuance/issued-lgs",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # D. Bank Facility Audit Logs (Previously invisible in AuditLog!)
        fac_logs = (
            db.query(IssuanceFacilityAuditLog, IssuanceFacility)
            .join(IssuanceFacility, IssuanceFacilityAuditLog.facility_id == IssuanceFacility.id)
            .filter(IssuanceFacility.customer_id == customer_id)
            .order_by(IssuanceFacilityAuditLog.id.desc())
            .limit(80)
            .all()
        )
        for flog, fac in fac_logs:
            actor = users_by_id.get(flog.performed_by_user_id)
            bank_name = fac.foreign_bank_name or banks_by_id.get(fac.bank_id)
            ccy = currencies_by_id.get(fac.currency_id)
            out.append({
                "id": f"iss-faclog-{flog.id}",
                "timestamp": _to_iso(flog.timestamp or flog.created_at),
                "module": "ISSUANCE",
                "category": "ADMIN",
                "action_code": f"FACILITY_{flog.action}",
                "action_label": f"Bank Facility {flog.action.title()}",
                "actor_name": _user_label(actor, flog.performed_by_user_id),
                "actor_id": flog.performed_by_user_id,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": bank_name,
                "reference": fac.facility_name,
                "secondary_ref": fac.reference_number,
                "beneficiary_name": None,
                "amount": _safe_float(fac.total_limit_amount),
                "currency": ccy,
                "summary": f"Facility '{fac.facility_name}' ({bank_name or 'Bank'}) — {flog.action.lower()}d",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": fac.status,
                "has_warning": flog.action in ("SUSPEND", "DELETE"),
                "warning_badges": [flog.action.title()] if flog.action in ("SUSPEND", "DELETE") else [],
                "links": {"navigate_to": "/corporate-admin/issuance/facilities"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # E. LG Position Reconciliation Sessions
        recon_sessions = (
            db.query(ReconciliationSession)
            .filter(ReconciliationSession.customer_id == customer_id, ReconciliationSession.is_deleted == False)
            .order_by(ReconciliationSession.id.desc())
            .limit(60)
            .all()
        )
        for sess in recon_sessions:
            actor = users_by_id.get(sess.created_by_user_id) if sess.created_by_user_id else None
            bank_name = banks_by_id.get(sess.bank_id)
            mismatches = (sess.mismatched_count or 0) + (sess.bank_only_count or 0) + (sess.system_only_count or 0)
            warns = [f"{mismatches} Variance(s)"] if mismatches > 0 else []
            out.append({
                "id": f"iss-posrecon-{sess.id}",
                "timestamp": _to_iso(sess.reviewed_at or sess.updated_at or sess.created_at),
                "module": "ISSUANCE",
                "category": "RECON",
                "action_code": "LG_POSITION_RECONCILIATION",
                "action_label": "LG Bank Position Reconciliation",
                "actor_name": _user_label(actor, sess.created_by_user_id),
                "actor_id": sess.created_by_user_id,
                "actor_type": "INTERNAL",
                "approver_name": (
                    _user_label(users_by_id.get(sess.reviewed_by_user_id), sess.reviewed_by_user_id, default="")
                    if sess.reviewed_by_user_id
                    else None
                ),
                "entity_id": None,
                "entity_name": None,
                "bank_name": bank_name,
                "reference": sess.original_file_name or f"Position {_fmt_date_human(sess.position_date)}",
                "secondary_ref": f"Snapshot {_fmt_date_human(sess.position_date)}",
                "beneficiary_name": None,
                "amount": _safe_float(sess.bank_reported_total),
                "currency": None,
                "summary": f"{sess.matched_count or 0} matched · {mismatches} variance(s) out of {sess.total_bank_records or 0} bank rows",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED" if sess.status == "COMPLETED" else "PENDING_APPROVAL",
                "lifecycle_label": f"Position · {sess.status}",
                "has_warning": mismatches > 0,
                "warning_badges": warns,
                "links": {"navigate_to": "/corporate-admin/issuance/reconciliation"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        return out

    def _collect_quotation_events(
        self,
        db: Session,
        customer_id: int,
        users_by_id: Dict[int, User],
        entities_by_id: Dict[int, CustomerEntity],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        rfqs = (
            db.query(QuotationRequest)
            .options(selectinload(QuotationRequest.assignments))
            .filter(QuotationRequest.customer_id == customer_id, QuotationRequest.is_deleted == False)
            .order_by(QuotationRequest.created_at.desc())
            .limit(150)
            .all()
        )
        for rfq in rfqs:
            creator = users_by_id.get(rfq.created_by_user_id)
            resolver = users_by_id.get(rfq.acceptance_resolved_by_user_id) if rfq.acceptance_resolved_by_user_id else None
            ent_name = entities_by_id[rfq.entity_id].entity_name if rfq.entity_id in entities_by_id else None
            invited_cnt = len(rfq.assignments or [])

            rfq_type = (rfq.type or "FX_SPOT").upper()
            pair_desc = (
                f"{rfq.direction or ''} {rfq.buy_currency or ''}/{rfq.sell_currency or ''}".strip(" /")
                if rfq_type == "FX_SPOT"
                else rfq_type.replace("_", " ")
            )

            acc_st = (rfq.acceptance_status or "").upper()
            st = (rfq.status or "").upper()
            warns: List[str] = []

            if acc_st in ("ACCEPTED", "AUTO_ACCEPTED"):
                l_stage = "COMPLETED"
                l_label = "Auto-Accepted Deal" if acc_st == "AUTO_ACCEPTED" else f"Deal Won · {rfq.winner_bank_name or 'Bank'}"
                if acc_st == "AUTO_ACCEPTED":
                    warns.append("Auto-Accepted on Timeout")
            elif acc_st in ("REJECTED", "AUTO_REJECTED"):
                l_stage = "REJECTED"
                l_label = "Deal Declined" if acc_st == "REJECTED" else "Expired Unaccepted"
                warns.append(l_label)
            elif st == "PENDING_APPROVAL":
                l_stage, l_label = "PENDING_APPROVAL", "Pending Admin Approval"
            elif st == "OPEN":
                l_stage, l_label = "DELIVERED_AWAITING_REPLY", "Live Bidding Window"
            elif st in ("CANCELLED", "REJECTED"):
                l_stage, l_label = "CANCELED", st.title()
            else:
                l_stage, l_label = "COMPLETED", st.replace("_", " ").title()

            win_rate = rfq.winner_rate
            win_str = f" · Won by {rfq.winner_bank_name} @ {win_rate}" if (rfq.winner_bank_name and win_rate is not None) else (
                f" · Winner: {rfq.winner_bank_name}" if rfq.winner_bank_name else ""
            )
            summ = f"{pair_desc} ({rfq.quotation_base or 'Execution'}) · {invited_cnt} bank(s) invited{win_str}"

            out.append({
                "id": f"rfq-{rfq.id}",
                "timestamp": _to_iso(rfq.acceptance_resolved_at or rfq.dispatched_at or rfq.updated_at or rfq.created_at),
                "module": "QUOTATIONS",
                "category": "RFQ_DEAL",
                "action_code": f"RFQ_{acc_st or st}",
                "action_label": f"RFQ {rfq_type.replace('_', ' ')} ({pair_desc or rfq.ref_no})",
                "actor_name": _user_label(creator, rfq.created_by_user_id),
                "actor_id": rfq.created_by_user_id,
                "actor_type": "INTERNAL",
                "approver_name": _user_label(resolver, rfq.acceptance_resolved_by_user_id, default="") if resolver else None,
                "entity_id": rfq.entity_id,
                "entity_name": ent_name,
                "bank_name": rfq.winner_bank_name,
                "reference": rfq.ref_no,
                "secondary_ref": rfq.quotation_base,
                "beneficiary_name": None,
                "amount": _safe_float(rfq.amount),
                "currency": rfq.buy_currency or rfq.sell_currency,
                "summary": summ,
                "change_from": None,
                "change_to": str(win_rate) if win_rate is not None else None,
                "lifecycle_stage": l_stage,
                "lifecycle_label": l_label,
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {
                    "rfq_id": rfq.id,
                    "navigate_to": f"/corporate-admin/quotations/results/{rfq.id}" if st == "COMPLETED" else "/corporate-admin/quotations",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })
        return out

    def _collect_reconciliation_events(
        self,
        db: Session,
        customer_id: int,
        users_by_id: Dict[int, User],
        banks_by_id: Dict[int, str],
        currencies_by_id: Dict[int, str],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # A. Bank Statements Uploaded & Reconciled
        statements = (
            db.query(BankStatement)
            .filter(BankStatement.company_id == customer_id, BankStatement.is_deleted == False)
            .order_by(BankStatement.id.desc())
            .limit(80)
            .all()
        )
        for stmt in statements:
            actor = users_by_id.get(stmt.created_by) if stmt.created_by else None
            bank_name = banks_by_id.get(stmt.bank_id)
            ccy = currencies_by_id.get(stmt.currency_id)
            warns = []
            if stmt.balance_continuity_status == "CONTINUITY_WARNING" or stmt.status == "CONTINUITY_WARNING":
                warns.append("Balance Continuity Gap")
            if stmt.status == "ERROR":
                warns.append("Parse Error")

            out.append({
                "id": f"recon-stmt-{stmt.id}",
                "timestamp": _to_iso(stmt.updated_at or stmt.created_at),
                "module": "RECONCILIATION",
                "category": "RECON",
                "action_code": "BANK_STATEMENT_IMPORTED",
                "action_label": "Bank Statement Imported",
                "actor_name": _user_label(actor, stmt.created_by),
                "actor_id": stmt.created_by,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": bank_name,
                "reference": stmt.file_name,
                "secondary_ref": stmt.account_number,
                "beneficiary_name": None,
                "amount": _safe_float(stmt.closing_balance),
                "currency": ccy,
                "summary": f"Statement {_fmt_date_human(stmt.statement_start_date)} → {_fmt_date_human(stmt.statement_end_date)} · Closing Bal: {ccy or ''} {float(stmt.closing_balance or 0):,.2f}",
                "change_from": _to_date(stmt.statement_start_date).isoformat() if _to_date(stmt.statement_start_date) else None,
                "change_to": _to_date(stmt.statement_end_date).isoformat() if _to_date(stmt.statement_end_date) else None,
                "lifecycle_stage": "COMPLETED" if stmt.status == "RECONCILED" else "APPROVED",
                "lifecycle_label": f"Statement · {stmt.status}",
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {
                    "statement_id": stmt.id,
                    "navigate_to": f"/corporate-admin/reconciliation/workspace/{stmt.id}",
                },
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # B. Reconciliation Matches (Grouped by User + Statement + Day to avoid flooding)
        matches = (
            db.query(ReconciliationMatch, BankTransaction.statement_id)
            .join(BankTransaction, ReconciliationMatch.bank_txn_id == BankTransaction.id)
            .join(BankStatement, BankTransaction.statement_id == BankStatement.id)
            .filter(BankStatement.company_id == customer_id, ReconciliationMatch.is_deleted == False)
            .order_by(ReconciliationMatch.id.desc())
            .limit(300)
            .all()
        )
        stmt_map = {s.id: s for s in statements}
        grouped_matches: Dict[Tuple[int, int, str], List[ReconciliationMatch]] = defaultdict(list)
        for m, sid in matches:
            day_key = (_to_date(m.created_at) or date.today()).isoformat()
            grouped_matches[(m.created_by or 0, sid, day_key)].append(m)

        for (uid, sid, day_key), m_list in grouped_matches.items():
            stmt = stmt_map.get(sid)
            actor = users_by_id.get(uid) if uid else None
            manual_cnt = sum(1 for x in m_list if x.match_logic in ("MANUAL", "TOLERANCE"))
            writeoff_cnt = sum(1 for x in m_list if (x.variance_disposition or "NONE") != "NONE")
            warns = []
            if writeoff_cnt > 0:
                warns.append(f"{writeoff_cnt} Variance Write-Off(s)")
            out.append({
                "id": f"recon-matchgrp-{sid}-{uid}-{day_key}",
                "timestamp": _to_iso(m_list[0].created_at),
                "module": "RECONCILIATION",
                "category": "RECON",
                "action_code": "RECON_TRANSACTIONS_MATCHED",
                "action_label": f"Reconciled {len(m_list)} Bank Transaction(s)",
                "actor_name": _user_label(actor, uid),
                "actor_id": uid or None,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": banks_by_id.get(stmt.bank_id) if stmt else None,
                "reference": stmt.file_name if stmt else f"Statement #{sid}",
                "secondary_ref": f"{manual_cnt} manual / {len(m_list) - manual_cnt} auto",
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": f"Matched {len(m_list)} transaction(s) ({manual_cnt} manual/tolerance, {writeoff_cnt} variance disposition)",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": "Matched",
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {"navigate_to": f"/corporate-admin/reconciliation/workspace/{sid}"},
                "is_batch": len(m_list) > 1,
                "batch_count": len(m_list),
                "children": [],
            })

        # C. Classification Rules Created / Promoted
        rules = (
            db.query(ClassificationRule)
            .filter(
                ClassificationRule.company_id == customer_id,
                ClassificationRule.is_deleted == False,
                ClassificationRule.rule_origin == "MANUAL",
            )
            .order_by(ClassificationRule.id.desc())
            .limit(40)
            .all()
        )
        for r in rules:
            actor = users_by_id.get(r.created_by) if r.created_by else None
            out.append({
                "id": f"recon-rule-{r.id}",
                "timestamp": _to_iso(r.updated_at or r.created_at),
                "module": "RECONCILIATION",
                "category": "ADMIN",
                "action_code": "RECON_RULE_CONFIGURED",
                "action_label": "GL Classification Rule Configured",
                "actor_name": _user_label(actor, r.created_by),
                "actor_id": r.created_by,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": banks_by_id.get(r.bank_id) if r.bank_id else "All Banks",
                "reference": r.rule_name or f"Rule #{r.id}",
                "secondary_ref": f"GL {r.assigned_gl_account}",
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": f"Maps to GL {r.assigned_gl_account} ({r.assigned_category or 'Uncategorized'}) · State: {r.lifecycle_state}",
                "change_from": None,
                "change_to": r.assigned_gl_account,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": r.lifecycle_state,
                "has_warning": False,
                "warning_badges": [],
                "links": {"navigate_to": "/corporate-admin/reconciliation/rules"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        return out

    def _collect_inbox_events(
        self,
        db: Session,
        customer_id: int,
        users_by_id: Dict[int, User],
        banks_by_id: Dict[int, str],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # A. Actioned / Confirmed Smart Inbox Emails
        items = (
            db.query(InboxItem)
            .filter(
                InboxItem.customer_id == customer_id,
                InboxItem.is_deleted == False,
                InboxItem.status.in_(["CONFIRMED", "ACTIONED", "PARSE_ERROR"]),
            )
            .order_by(InboxItem.id.desc())
            .limit(80)
            .all()
        )
        for it in items:
            actor = users_by_id.get(it.actioned_by_user_id) if it.actioned_by_user_id else None
            bank_name = banks_by_id.get(it.matched_bank_id) if it.matched_bank_id else it.sender_domain
            eff_class = it.user_override_classification or it.classification
            warns = []
            if it.status == "PARSE_ERROR":
                warns.append("Parse Error")
            if not it.is_trusted_sender:
                warns.append("Untrusted Sender Domain")

            out.append({
                "id": f"inbox-item-{it.id}",
                "timestamp": _to_iso(it.actioned_at or it.updated_at or it.received_at),
                "module": "SMART_INBOX",
                "category": "INBOX",
                "action_code": f"INBOX_{it.status}",
                "action_label": f"Bank Email {it.status.replace('_', ' ').title()}",
                "actor_name": _user_label(actor, it.actioned_by_user_id, default="Smart Inbox Engine"),
                "actor_id": it.actioned_by_user_id,
                "actor_type": "INTERNAL" if it.actioned_by_user_id else "SYSTEM",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": bank_name,
                "reference": eff_class.replace("_", " ").title(),
                "secondary_ref": it.primary_attachment_name,
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": f"From {it.sender_email}: \"{it.subject or 'No Subject'}\" · {it.action_summary or eff_class}",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED" if it.status == "ACTIONED" else "APPROVED",
                "lifecycle_label": it.status.title(),
                "has_warning": len(warns) > 0,
                "warning_badges": warns,
                "links": {"navigate_to": "/corporate-admin/inbox"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # B. Outbound Position / Statement Requests to Banks
        outbound = (
            db.query(InboxOutboundRequest)
            .filter(InboxOutboundRequest.customer_id == customer_id, InboxOutboundRequest.is_deleted == False)
            .order_by(InboxOutboundRequest.id.desc())
            .limit(60)
            .all()
        )
        for req in outbound:
            actor = users_by_id.get(req.sent_by_user_id) if req.sent_by_user_id else None
            bank_name = banks_by_id.get(req.bank_id)
            out.append({
                "id": f"inbox-out-{req.id}",
                "timestamp": _to_iso(req.reply_received_at or req.sent_at),
                "module": "SMART_INBOX",
                "category": "INBOX",
                "action_code": "OUTBOUND_BANK_REQUEST",
                "action_label": f"Outbound {req.request_type.replace('_', ' ').title()} Request",
                "actor_name": _user_label(actor, req.sent_by_user_id, default="Scheduled Automation" if req.is_scheduled else "System"),
                "actor_id": req.sent_by_user_id,
                "actor_type": "SYSTEM" if req.is_scheduled else "INTERNAL",
                "approver_name": None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": bank_name,
                "reference": req.request_reference or f"OUT-{req.id}",
                "secondary_ref": "Replied" if req.is_replied else "Awaiting Bank Reply",
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": f"Sent '{req.subject}' to {', '.join(req.sent_to_emails or [])[:80]}",
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "BANK_CONFIRMED" if req.is_replied else "DELIVERED_AWAITING_REPLY",
                "lifecycle_label": "Bank Replied" if req.is_replied else "Sent · Awaiting Bank Email",
                "has_warning": False,
                "warning_badges": [],
                "links": {"navigate_to": "/corporate-admin/inbox"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        return out

    def _collect_governance_events(
        self,
        db: Session,
        customer_id: int,
        users_by_id: Dict[int, User],
        entities_by_id: Dict[int, CustomerEntity],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # A. Dual-Control Admin Change Requests
        admin_reqs = (
            db.query(AdminChangeRequest)
            .filter(AdminChangeRequest.customer_id == customer_id)
            .order_by(AdminChangeRequest.id.desc())
            .limit(80)
            .all()
        )
        for cr in admin_reqs:
            maker = users_by_id.get(cr.requested_by_user_id)
            approver = users_by_id.get(cr.approved_by_user_id) if cr.approved_by_user_id else None
            payload = cr.change_payload if isinstance(cr.change_payload, dict) else {}
            cfg_key = payload.get("config_key") or payload.get("name") or cr.change_type
            st = (cr.status or "PENDING").upper()
            l_map = {
                "PENDING": ("PENDING_APPROVAL", "Pending 2nd Admin Approval"),
                "APPROVED": ("COMPLETED", "Approved & Applied"),
                "REJECTED": ("REJECTED", "Rejected by 2nd Admin"),
            }
            l_stage, l_label = l_map.get(st, ("COMPLETED", st.title()))
            out.append({
                "id": f"gov-adminchg-{cr.id}",
                "timestamp": _to_iso(cr.applied_at or cr.created_at),
                "module": "GOVERNANCE",
                "category": "ADMIN",
                "action_code": f"DUAL_CONTROL_{cr.change_type}",
                "action_label": f"Governance Change: {cr.change_type.replace('_', ' ').title()}",
                "actor_name": _user_label(maker, cr.requested_by_user_id),
                "actor_id": cr.requested_by_user_id,
                "actor_type": "INTERNAL",
                "approver_name": _user_label(approver, cr.approved_by_user_id, default="") if approver else None,
                "entity_id": None,
                "entity_name": None,
                "bank_name": None,
                "reference": str(cfg_key),
                "secondary_ref": f"CHG-{cr.id}",
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": f"Dual-control change ({cr.change_type.replace('_', ' ').lower()})"
                + (f": {payload.get('old_value')} → {payload.get('new_value')}" if "new_value" in payload else ""),
                "change_from": str(payload.get("old_value")) if "old_value" in payload else None,
                "change_to": str(payload.get("new_value")) if "new_value" in payload else None,
                "lifecycle_stage": l_stage,
                "lifecycle_label": l_label,
                "has_warning": st == "REJECTED",
                "warning_badges": ["Rejected"] if st == "REJECTED" else [],
                "links": {"navigate_to": "/corporate-admin/approval-requests"},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        # B. Meaningful Governance AuditLog entries (Users, Entities, Categories, Configs, Migrations — NO login/email noise!)
        gov_logs = (
            db.query(AuditLog)
            .filter(
                AuditLog.customer_id == customer_id,
                AuditLog.entity_type.in_([
                    "User",
                    "CustomerEntity",
                    "LGCategory",
                    "CustomerConfiguration",
                    "Department",
                    "ApprovalGroup",
                    "CustomerBankAccount",
                    "CorporateProject",
                    "MigrationBatch",
                ]),
                ~AuditLog.action_type.in_([
                    "LOGIN_SUCCESS",
                    "LOGIN_FAILED",
                    "LOGIN_FAILURE",
                    "LOGOUT",
                    "NOTIFICATION_SENT",
                    "NOTIFICATION_FAILED",
                    "LEGAL_ARTIFACT_ACCEPTED",
                ]),
            )
            .order_by(AuditLog.id.desc())
            .limit(120)
            .all()
        )
        for log in gov_logs:
            actor = users_by_id.get(log.user_id) if log.user_id else None
            det = log.details if isinstance(log.details, dict) else {}
            etype = log.entity_type or "Setting"
            act_raw = (log.action_type or "UPDATE").replace("_BY_CA", "")

            if etype == "User":
                target_u = users_by_id.get(log.entity_id) if log.entity_id else None
                ref = (target_u.email if target_u else det.get("email")) or f"User #{log.entity_id}"
                role_info = f" ({det.get('role')})" if det.get("role") else ""
                lbl = f"User Account {act_raw.replace('_', ' ').title()}"
                summ = f"{lbl}: {ref}{role_info}"
                nav = "/corporate-admin/users"
            elif etype == "CustomerEntity":
                ce = entities_by_id.get(log.entity_id) if log.entity_id else None
                ref = (ce.entity_name if ce else det.get("entity_name")) or f"Entity #{log.entity_id}"
                lbl = f"Subsidiary / Entity {act_raw.replace('_', ' ').title()}"
                summ = f"{lbl}: {ref}"
                nav = "/corporate-admin/module-configs"
            elif etype == "CustomerConfiguration":
                ref = det.get("config_key") or det.get("key") or f"Config #{log.entity_id}"
                lbl = "Policy / Module Configuration Updated"
                summ = f"Setting {ref} updated" + (f" to '{det.get('configured_value')}'" if "configured_value" in det else "")
                nav = "/corporate-admin/module-configs"
            elif etype == "LGCategory":
                ref = det.get("name") or det.get("category_name") or f"Category #{log.entity_id}"
                lbl = f"LG Category {act_raw.replace('_', ' ').title()}"
                summ = f"{lbl}: {ref}"
                nav = "/corporate-admin/lg-categories"
            else:
                ref = det.get("name") or det.get("account_name") or f"{etype} #{log.entity_id or ''}"
                lbl = f"{etype} {act_raw.replace('_', ' ').title()}"
                summ = f"{lbl}: {ref}"
                nav = "/corporate-admin/module-configs"

            is_del = "DELETE" in act_raw or "LOCKED" in act_raw
            out.append({
                "id": f"gov-log-{log.id}",
                "timestamp": _to_iso(log.timestamp),
                "module": "GOVERNANCE",
                "category": "ADMIN",
                "action_code": log.action_type,
                "action_label": lbl,
                "actor_name": _user_label(actor, log.user_id),
                "actor_id": log.user_id,
                "actor_type": "INTERNAL",
                "approver_name": None,
                "entity_id": log.entity_id if etype == "CustomerEntity" else None,
                "entity_name": entities_by_id[log.entity_id].entity_name if (etype == "CustomerEntity" and log.entity_id in entities_by_id) else None,
                "bank_name": None,
                "reference": str(ref),
                "secondary_ref": etype,
                "beneficiary_name": None,
                "amount": None,
                "currency": None,
                "summary": summ,
                "change_from": None,
                "change_to": None,
                "lifecycle_stage": "COMPLETED",
                "lifecycle_label": act_raw.replace("_", " ").title(),
                "has_warning": is_del,
                "warning_badges": ["Deleted / Locked"] if is_del else [],
                "links": {"navigate_to": nav},
                "is_batch": False,
                "batch_count": 1,
                "children": [],
            })

        return out

    # ──────────────────────────────────────────────────────────────────────────
    # TAB 2: Control & Pre-Sign Verification Workspace
    # ──────────────────────────────────────────────────────────────────────────

    def get_control_verification_workspace(
        self,
        db: Session,
        customer_id: int,
        window_days: int = 45,
        cohort_start_date: Optional[str] = None,
        cohort_end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Builds the Signatory & Reviewer Control Workspace:
          1. pending_bank_letters: Every newly generated Custody & Issuance letter awaiting signature / bank delivery
          2. expiry_cohort: 100% of Custody + Issuance LGs due in the expiry window (including recently renewed ones!)
          3. cross_module_exceptions: Platform-wide control exceptions across all 6 modules
        """
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()

        win_start = _to_date(cohort_start_date) or (today - timedelta(days=7))
        win_end = _to_date(cohort_end_date) or (today + timedelta(days=max(1, window_days)))

        users_by_id: Dict[int, User] = {
            u.id: u for u in db.query(User).filter(User.customer_id == customer_id).all()
        }
        entities_by_id: Dict[int, CustomerEntity] = {
            e.id: e for e in db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer_id).all()
        }
        banks_by_id: Dict[int, str] = {b.id: b.name for b in db.query(Bank).all()}
        currencies_by_id: Dict[int, str] = {c.id: c.iso_code for c in db.query(Currency).all()}

        pending_bank_letters: List[Dict[str, Any]] = []
        expiry_cohort: List[Dict[str, Any]] = []
        cross_module_exceptions: List[Dict[str, Any]] = []

        # ── 1. PENDING BANK LETTERS (Custody Instructions + Issuance Letters Awaiting Delivery) ──
        try:
            undelivered_custody = (
                db.query(LGInstruction)
                .join(LGRecord, LGInstruction.lg_record_id == LGRecord.id)
                .options(
                    selectinload(LGInstruction.lg_record).selectinload(LGRecord.issuing_bank),
                    selectinload(LGInstruction.lg_record).selectinload(LGRecord.communication_bank),
                    selectinload(LGInstruction.lg_record).selectinload(LGRecord.lg_currency),
                    selectinload(LGInstruction.lg_record).selectinload(LGRecord.beneficiary_corporate),
                    selectinload(LGInstruction.lg_record).selectinload(LGRecord.internal_owner_contact),
                )
                .filter(
                    LGRecord.customer_id == customer_id,
                    LGInstruction.is_deleted == False,
                    LGInstruction.delivery_date.is_(None),
                    ~LGInstruction.status.in_(["Canceled", "Cancelled", "Confirmed by Bank", "Instruction Delivered"]),
                )
                .order_by(LGInstruction.id.desc())
                .all()
            )
            for inst in undelivered_custody:
                lg = inst.lg_record
                if not lg:
                    continue
                det = inst.details if isinstance(inst.details, dict) else {}
                cat, label = _map_custody_instruction_category(inst.instruction_type)
                summary, chg_from, chg_to = _summarize_custody_instruction(inst)
                _, _, flags = _custody_instruction_lifecycle(inst, now_utc)

                if cat == "RENEWAL":
                    if lg.auto_renewal is False:
                        flags.append("Auto-Renew Policy is OFF")
                    old_d = _to_date(chg_from)
                    new_d = _to_date(chg_to)
                    if old_d and new_d and (new_d - old_d).days > 370:
                        flags.append(f"Extended > 1 Year ({(new_d - old_d).days}d)")

                inst_dt = _to_utc_datetime(inst.instruction_date or inst.created_at)
                days_waiting = (now_utc - inst_dt).days if inst_dt else 0

                pending_bank_letters.append({
                    "id": f"sign-cust-{inst.id}",
                    "module": "CUSTODY",
                    "category": cat,
                    "action_label": label,
                    "lg_number": lg.lg_number,
                    "serial_number": inst.serial_number,
                    "batch_id": det.get("bulk_renewal_batch_id") if det.get("is_bulk_renewal") else None,
                    "bank_name": _custody_bank_name(lg),
                    "entity_name": lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else None,
                    "beneficiary_name": lg.issuer_name,
                    "internal_owner": lg.internal_owner_contact.email if lg.internal_owner_contact else None,
                    "amount": _safe_float(lg.lg_amount),
                    "currency": lg.lg_currency.iso_code if lg.lg_currency else None,
                    "change_summary": summary,
                    "old_expiry_date": chg_from,
                    "new_expiry_date": chg_to,
                    "maker_name": _user_label(users_by_id.get(inst.maker_user_id), inst.maker_user_id),
                    "maker_id": inst.maker_user_id,
                    "checker_name": _user_label(users_by_id.get(inst.checker_user_id), inst.checker_user_id, default="") if inst.checker_user_id else None,
                    "created_at": _to_iso(inst.instruction_date or inst.created_at),
                    "days_waiting": days_waiting,
                    "is_printed": bool(inst.is_printed),
                    "control_flags": flags,
                    "instruction_id": inst.id,
                    "maintenance_action_id": None,
                    "navigate_to": f"/corporate-admin/lg-records/{lg.id}",
                })

                if days_waiting > 5:
                    cross_module_exceptions.append({
                        "id": f"exc-stuck-cust-{inst.id}",
                        "module": "CUSTODY",
                        "severity": "HIGH" if days_waiting > 10 else "MEDIUM",
                        "exception_type": "Stuck Bank Letter (Undelivered)",
                        "reference": lg.lg_number,
                        "secondary_ref": inst.serial_number,
                        "bank_name": _custody_bank_name(lg),
                        "actor_name": _user_label(users_by_id.get(inst.maker_user_id), inst.maker_user_id),
                        "detail": f"{label} generated {days_waiting} days ago — still not delivered to bank.",
                        "timestamp": _to_iso(inst.instruction_date or inst.created_at),
                        "navigate_to": f"/corporate-admin/lg-records/{lg.id}",
                    })
        except Exception as e:
            logger.warning(f"Control workspace custody pending letters error: {e}", exc_info=True)

        # Issuance Maintenance Letters Awaiting Delivery
        try:
            undelivered_maint = (
                db.query(IssuanceMaintenanceAction)
                .join(IssuedLGRecord, IssuanceMaintenanceAction.issued_lg_id == IssuedLGRecord.id)
                .filter(
                    IssuedLGRecord.customer_id == customer_id,
                    IssuanceMaintenanceAction.is_deleted == False,
                    IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED"]),
                    IssuanceMaintenanceAction.delivery_date.is_(None),
                    IssuanceMaintenanceAction.letter_serial_number.isnot(None),
                )
                .order_by(IssuanceMaintenanceAction.id.desc())
                .all()
            )
            for act in undelivered_maint:
                lg = act.issued_lg
                if not lg:
                    continue
                adata = act.action_data if isinstance(act.action_data, dict) else {}
                atype = (act.action_type or "").upper()
                flags: List[str] = []
                if lg.reference_validity_flag == "EXCEEDED":
                    flags.append("Exceeds Contract End Date")
                if lg.verification_status == "DISCREPANCY":
                    flags.append("Unresolved Copy Discrepancy")

                act_dt = _to_utc_datetime(act.updated_at or act.created_at)
                days_w = (now_utc - act_dt).days if act_dt else 0
                if days_w > 5:
                    flags.append(f"Undelivered ({days_w}d)")

                old_e = adata.get("old_expiry_date") or adata.get("previous_expiry_date")
                new_e = adata.get("new_expiry_date")
                summ = (
                    f"Extend {_fmt_date_human(old_e) if old_e else ''} → {_fmt_date_human(new_e)}".strip()
                    if atype == "EXTEND"
                    else (act.notes or f"Issuance {atype.replace('_', ' ').title()}")
                )

                pending_bank_letters.append({
                    "id": f"sign-issmaint-{act.id}",
                    "module": "ISSUANCE",
                    "category": "RENEWAL" if atype == "EXTEND" else ("RELEASE" if atype == "CLOSE" else "AMENDMENT"),
                    "action_label": f"Issued LG {atype.replace('_', ' ').title()}",
                    "lg_number": lg.bank_lg_number or lg.lg_ref_number,
                    "serial_number": act.letter_serial_number,
                    "batch_id": None,
                    "bank_name": banks_by_id.get(lg.bank_id),
                    "entity_name": entities_by_id[lg.issuing_entity_id].entity_name if lg.issuing_entity_id in entities_by_id else None,
                    "beneficiary_name": lg.beneficiary_name,
                    "internal_owner": _user_label(users_by_id.get(lg.current_owner_user_id), lg.current_owner_user_id, default="") if lg.current_owner_user_id else None,
                    "amount": _safe_float(lg.current_amount),
                    "currency": currencies_by_id.get(lg.currency_id),
                    "change_summary": summ,
                    "old_expiry_date": old_e,
                    "new_expiry_date": new_e,
                    "maker_name": _user_label(users_by_id.get(act.initiated_by_user_id), act.initiated_by_user_id),
                    "maker_id": act.initiated_by_user_id,
                    "checker_name": None,
                    "created_at": _to_iso(act.updated_at or act.created_at),
                    "days_waiting": days_w,
                    "is_printed": bool(act.is_printed),
                    "control_flags": flags,
                    "instruction_id": None,
                    "maintenance_action_id": act.id,
                    "navigate_to": "/corporate-admin/issuance/issued-lgs",
                })
        except Exception as e:
            logger.warning(f"Control workspace issuance pending letters error: {e}", exc_info=True)

        # ── 2. EXPIRY COHORT COMPLETENESS (Custody + Issuance Unified) ────────
        # Reconstructs 100% of LGs originally expiring in [win_start, win_end], INCLUDING LGs
        # whose expiry_date was already pushed forward by a recent extension!
        try:
            all_custody_lgs = (
                db.query(LGRecord)
                .options(
                    selectinload(LGRecord.issuing_bank),
                    selectinload(LGRecord.communication_bank),
                    selectinload(LGRecord.lg_currency),
                    selectinload(LGRecord.beneficiary_corporate),
                    selectinload(LGRecord.internal_owner_contact),
                    selectinload(LGRecord.lg_status),
                    selectinload(LGRecord.instructions),
                )
                .filter(LGRecord.customer_id == customer_id, LGRecord.is_deleted == False)
                .all()
            )
            for lg in all_custody_lgs:
                cur_exp = _to_date(lg.expiry_date)
                active_insts = [
                    i for i in (lg.instructions or [])
                    if not i.is_deleted and (i.status or "").lower() not in ("canceled", "cancelled")
                ]
                active_insts.sort(key=lambda x: x.id, reverse=True)
                latest_inst = active_insts[0] if active_insts else None

                # Check if an extension moved its old_expiry_date out of [win_start, win_end]
                ext_inst_in_window: Optional[LGInstruction] = None
                for inst in active_insts:
                    if "EXTEND" in (inst.instruction_type or "").upper() or "EXTENSION" in (inst.instruction_type or "").upper():
                        det = inst.details if isinstance(inst.details, dict) else {}
                        old_e = _to_date(det.get("old_expiry_date"))
                        if old_e and win_start <= old_e <= win_end:
                            ext_inst_in_window = inst
                            break

                in_current_window = bool(cur_exp and win_start <= cur_exp <= win_end)
                if not in_current_window and not ext_inst_in_window:
                    continue

                status_id = lg.lg_status_id
                if ext_inst_in_window:
                    det = ext_inst_in_window.details if isinstance(ext_inst_in_window.details, dict) else {}
                    orig_exp = _to_date(det.get("old_expiry_date")) or cur_exp
                    new_exp = _to_date(det.get("new_expiry_date")) or cur_exp
                    if ext_inst_in_window.bank_reply_date or ext_inst_in_window.status == "Confirmed by Bank":
                        bucket = "DELIVERED_OR_CONFIRMED"
                        bucket_label = "Renewed · Bank Confirmed"
                    elif ext_inst_in_window.delivery_date or ext_inst_in_window.status == "Instruction Delivered":
                        bucket = "DELIVERED_OR_CONFIRMED"
                        bucket_label = "Renewed · Delivered to Bank"
                    else:
                        bucket = "READY_TO_SIGN"
                        bucket_label = "In Signatory Pile · Awaiting Delivery"
                    maker_lbl = _user_label(users_by_id.get(ext_inst_in_window.maker_user_id), ext_inst_in_window.maker_user_id)
                    serial_ref = ext_inst_in_window.serial_number
                    inst_id_ref = ext_inst_in_window.id
                elif status_id in (LgStatusEnum.RELEASED.value, LgStatusEnum.LIQUIDATED.value) or (
                    latest_inst and any(k in (latest_inst.instruction_type or "").upper() for k in ("RELEASE", "LIQUIDAT"))
                ):
                    orig_exp = cur_exp
                    new_exp = None
                    bucket = "RELEASING_OR_CLOSED"
                    bucket_label = lg.lg_status.name if lg.lg_status else "Releasing / Closed"
                    maker_lbl = _user_label(users_by_id.get(latest_inst.maker_user_id), latest_inst.maker_user_id) if latest_inst else None
                    serial_ref = latest_inst.serial_number if latest_inst else None
                    inst_id_ref = latest_inst.id if latest_inst else None
                else:
                    orig_exp = cur_exp
                    new_exp = None
                    bucket = "UNACTIONED_AT_RISK"
                    days_left = (cur_exp - today).days if cur_exp else 0
                    bucket_label = f"Unactioned · {'Expired' if days_left < 0 else f'{days_left}d left'}"
                    maker_lbl = None
                    serial_ref = None
                    inst_id_ref = None

                days_to_orig = (orig_exp - today).days if orig_exp else None
                expiry_cohort.append({
                    "id": f"cohort-cust-{lg.id}",
                    "module": "CUSTODY",
                    "lg_number": lg.lg_number,
                    "bank_name": _custody_bank_name(lg),
                    "entity_name": lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else None,
                    "beneficiary_name": lg.issuer_name,
                    "internal_owner": lg.internal_owner_contact.email if lg.internal_owner_contact else None,
                    "auto_renewal": bool(lg.auto_renewal),
                    "amount": _safe_float(lg.lg_amount),
                    "currency": lg.lg_currency.iso_code if lg.lg_currency else None,
                    "cohort_expiry_date": orig_exp.isoformat() if orig_exp else None,
                    "new_expiry_date": new_exp.isoformat() if new_exp else None,
                    "days_remaining": days_to_orig,
                    "bucket": bucket,
                    "bucket_label": bucket_label,
                    "actor_name": maker_lbl,
                    "serial_number": serial_ref,
                    "instruction_id": inst_id_ref,
                    "maintenance_action_id": None,
                    "navigate_to": f"/corporate-admin/lg-records/{lg.id}",
                })
        except Exception as e:
            logger.warning(f"Cohort custody error: {e}", exc_info=True)

        # Issuance LGs in Expiry Cohort
        try:
            all_issued_lgs = (
                db.query(IssuedLGRecord)
                .options(selectinload(IssuedLGRecord.maintenance_actions))
                .filter(IssuedLGRecord.customer_id == customer_id)
                .all()
            )
            for lg in all_issued_lgs:
                cur_exp = _to_date(lg.expiry_date)
                m_acts = [
                    m for m in (lg.maintenance_actions or [])
                    if not m.is_deleted and (m.status or "").upper() not in ("REJECTED", "CANCELLED", "CANCELED")
                ]
                m_acts.sort(key=lambda x: x.id, reverse=True)

                ext_act_in_win: Optional[IssuanceMaintenanceAction] = None
                for m in m_acts:
                    if (m.action_type or "").upper() == "EXTEND":
                        adata = m.action_data if isinstance(m.action_data, dict) else {}
                        old_e = _to_date(adata.get("old_expiry_date") or adata.get("previous_expiry_date"))
                        if old_e and win_start <= old_e <= win_end:
                            ext_act_in_win = m
                            break

                in_cur_win = bool(cur_exp and win_start <= cur_exp <= win_end)
                if not in_cur_win and not ext_act_in_win:
                    continue

                st = (lg.status or "").upper()
                if ext_act_in_win:
                    adata = ext_act_in_win.action_data if isinstance(ext_act_in_win.action_data, dict) else {}
                    orig_exp = _to_date(adata.get("old_expiry_date") or adata.get("previous_expiry_date")) or cur_exp
                    new_exp = _to_date(adata.get("new_expiry_date")) or cur_exp
                    if (ext_act_in_win.status or "").upper() == "PENDING_APPROVAL":
                        bucket = "READY_TO_SIGN"
                        bucket_label = "Extension Pending Approval"
                    elif ext_act_in_win.delivery_date or ext_act_in_win.bank_reply_date:
                        bucket = "DELIVERED_OR_CONFIRMED"
                        bucket_label = "Extended · Delivered / Confirmed"
                    else:
                        bucket = "READY_TO_SIGN"
                        bucket_label = "In Signatory Pile · Awaiting Delivery"
                    actor_lbl = _user_label(users_by_id.get(ext_act_in_win.initiated_by_user_id), ext_act_in_win.initiated_by_user_id)
                    serial_ref = ext_act_in_win.letter_serial_number
                    maint_id = ext_act_in_win.id
                elif st in ("RETURNED", "CANCELLED", "LIQUIDATED") or any(
                    (m.action_type or "").upper() in ("CLOSE", "LIQUIDATION") for m in m_acts
                ):
                    orig_exp = cur_exp
                    new_exp = None
                    bucket = "RELEASING_OR_CLOSED"
                    bucket_label = f"Closing / {st.title()}"
                    actor_lbl = None
                    serial_ref = None
                    maint_id = None
                else:
                    orig_exp = cur_exp
                    new_exp = None
                    bucket = "UNACTIONED_AT_RISK"
                    days_left = (cur_exp - today).days if cur_exp else 0
                    bucket_label = f"Unactioned · {'Expired' if days_left < 0 else f'{days_left}d left'}"
                    actor_lbl = None
                    serial_ref = None
                    maint_id = None

                expiry_cohort.append({
                    "id": f"cohort-iss-{lg.id}",
                    "module": "ISSUANCE",
                    "lg_number": lg.bank_lg_number or lg.lg_ref_number,
                    "bank_name": banks_by_id.get(lg.bank_id),
                    "entity_name": entities_by_id[lg.issuing_entity_id].entity_name if lg.issuing_entity_id in entities_by_id else None,
                    "beneficiary_name": lg.beneficiary_name,
                    "internal_owner": _user_label(users_by_id.get(lg.current_owner_user_id), lg.current_owner_user_id, default="") if lg.current_owner_user_id else None,
                    "auto_renewal": False,
                    "amount": _safe_float(lg.current_amount),
                    "currency": currencies_by_id.get(lg.currency_id),
                    "cohort_expiry_date": orig_exp.isoformat() if orig_exp else None,
                    "new_expiry_date": new_exp.isoformat() if new_exp else None,
                    "days_remaining": (orig_exp - today).days if orig_exp else None,
                    "bucket": bucket,
                    "bucket_label": bucket_label,
                    "actor_name": actor_lbl,
                    "serial_number": serial_ref,
                    "instruction_id": None,
                    "maintenance_action_id": maint_id,
                    "navigate_to": "/corporate-admin/issuance/issued-lgs",
                })

                # Also collect Issuance Discrepancy / Contract Overrun exceptions
                if lg.verification_status == "DISCREPANCY":
                    cross_module_exceptions.append({
                        "id": f"exc-iss-disc-{lg.id}",
                        "module": "ISSUANCE",
                        "severity": "HIGH",
                        "exception_type": "Bank LG Copy Discrepancy",
                        "reference": lg.bank_lg_number or lg.lg_ref_number,
                        "secondary_ref": lg.internal_serial,
                        "bank_name": banks_by_id.get(lg.bank_id),
                        "actor_name": _user_label(users_by_id.get(lg.issued_by_user_id), lg.issued_by_user_id),
                        "detail": lg.verification_notes or "Bank LG scan has unresolved terms discrepancy.",
                        "timestamp": _to_iso(lg.verified_at or lg.updated_at or lg.created_at),
                        "navigate_to": "/corporate-admin/approval-requests",
                    })
                if lg.reference_validity_flag == "EXCEEDED" and st == "ACTIVE":
                    cross_module_exceptions.append({
                        "id": f"exc-iss-ref-{lg.id}",
                        "module": "ISSUANCE",
                        "severity": "MEDIUM",
                        "exception_type": "LG Expiry Exceeds Contract End Date",
                        "reference": lg.bank_lg_number or lg.lg_ref_number,
                        "secondary_ref": lg.reference_number,
                        "bank_name": banks_by_id.get(lg.bank_id),
                        "actor_name": "Contract Watchdog",
                        "detail": f"LG expiry ({_fmt_date_human(lg.expiry_date)}) exceeds underlying {lg.reference_type or 'Contract'} end date ({_fmt_date_human(lg.reference_end_date)}).",
                        "timestamp": _to_iso(lg.updated_at or lg.created_at),
                        "navigate_to": "/corporate-admin/issuance/issued-lgs",
                    })
        except Exception as e:
            logger.warning(f"Cohort issuance error: {e}", exc_info=True)

        # Additional Cross-Module Exceptions (Quotations, Reconciliation, Smart Inbox)
        try:
            # Quotations: Auto-accepted / Auto-rejected / Declined deals
            exc_rfqs = (
                db.query(QuotationRequest)
                .filter(
                    QuotationRequest.customer_id == customer_id,
                    QuotationRequest.is_deleted == False,
                    QuotationRequest.acceptance_status.in_(["REJECTED", "AUTO_REJECTED", "AUTO_ACCEPTED"]),
                )
                .order_by(QuotationRequest.updated_at.desc())
                .limit(20)
                .all()
            )
            for rfq in exc_rfqs:
                acc = (rfq.acceptance_status or "").upper()
                cross_module_exceptions.append({
                    "id": f"exc-rfq-{rfq.id}",
                    "module": "QUOTATIONS",
                    "severity": "MEDIUM" if acc == "AUTO_ACCEPTED" else "HIGH",
                    "exception_type": f"RFQ Deal {acc.replace('_', ' ').title()}",
                    "reference": rfq.ref_no,
                    "secondary_ref": rfq.winner_bank_name,
                    "bank_name": rfq.winner_bank_name,
                    "actor_name": _user_label(users_by_id.get(rfq.acceptance_resolved_by_user_id or rfq.created_by_user_id)),
                    "detail": f"{rfq.type} RFQ ({rfq.buy_currency or ''}/{rfq.sell_currency or ''}) finished with status {acc.replace('_', ' ')}.",
                    "timestamp": _to_iso(rfq.acceptance_resolved_at or rfq.updated_at or rfq.created_at),
                    "navigate_to": f"/corporate-admin/quotations/results/{rfq.id}",
                })
        except Exception as e:
            logger.debug(f"Cross-module RFQ check skipped: {e}")

        try:
            # Reconciliation: Statement balance continuity warnings
            warn_stmts = (
                db.query(BankStatement)
                .filter(
                    BankStatement.company_id == customer_id,
                    BankStatement.is_deleted == False,
                    BankStatement.balance_continuity_status == "CONTINUITY_WARNING",
                )
                .order_by(BankStatement.id.desc())
                .limit(15)
                .all()
            )
            for st in warn_stmts:
                cross_module_exceptions.append({
                    "id": f"exc-recon-stmt-{st.id}",
                    "module": "RECONCILIATION",
                    "severity": "HIGH",
                    "exception_type": "Statement Balance Continuity Gap",
                    "reference": st.file_name,
                    "secondary_ref": st.account_number,
                    "bank_name": banks_by_id.get(st.bank_id),
                    "actor_name": _user_label(users_by_id.get(st.created_by), st.created_by),
                    "detail": f"Opening/closing balance variance of {float(st.balance_variance_amount or 0):,.2f} detected on statement.",
                    "timestamp": _to_iso(st.created_at),
                    "navigate_to": f"/corporate-admin/reconciliation/workspace/{st.id}",
                })
        except Exception as e:
            logger.debug(f"Cross-module Recon check skipped: {e}")

        try:
            # Smart Inbox: Parse errors or untrusted bank emails
            err_inbox = (
                db.query(InboxItem)
                .filter(
                    InboxItem.customer_id == customer_id,
                    InboxItem.is_deleted == False,
                    InboxItem.status == "PARSE_ERROR",
                )
                .order_by(InboxItem.id.desc())
                .limit(15)
                .all()
            )
            for it in err_inbox:
                cross_module_exceptions.append({
                    "id": f"exc-inbox-{it.id}",
                    "module": "SMART_INBOX",
                    "severity": "MEDIUM",
                    "exception_type": "Bank Email Attachment Parse Error",
                    "reference": it.primary_attachment_name or it.subject or f"Email #{it.id}",
                    "secondary_ref": it.sender_email,
                    "bank_name": banks_by_id.get(it.matched_bank_id) if it.matched_bank_id else it.sender_domain,
                    "actor_name": "Smart Inbox",
                    "detail": it.error_message or f"Failed to parse incoming {it.classification} email from {it.sender_email}.",
                    "timestamp": _to_iso(it.received_at),
                    "navigate_to": "/corporate-admin/inbox",
                })
        except Exception as e:
            logger.debug(f"Cross-module Inbox check skipped: {e}")

        # Sort Cohort: UNACTIONED_AT_RISK first, then READY_TO_SIGN, then DELIVERED_OR_CONFIRMED, then RELEASING_OR_CLOSED
        bucket_rank = {
            "UNACTIONED_AT_RISK": 0,
            "READY_TO_SIGN": 1,
            "DELIVERED_OR_CONFIRMED": 2,
            "RELEASING_OR_CLOSED": 3,
        }
        expiry_cohort.sort(
            key=lambda c: (
                bucket_rank.get(c["bucket"], 9),
                c["days_remaining"] if c["days_remaining"] is not None else 9999,
            )
        )

        return {
            "window": {
                "start_date": win_start.isoformat(),
                "end_date": win_end.isoformat(),
                "window_days": window_days,
            },
            "summary": {
                "pending_sign_or_delivery_count": len(pending_bank_letters),
                "pending_renewals_to_sign_count": sum(1 for x in pending_bank_letters if x["category"] == "RENEWAL"),
                "flagged_letters_count": sum(1 for x in pending_bank_letters if len(x.get("control_flags") or []) > 0),
                "cohort_total": len(expiry_cohort),
                "cohort_ready_to_sign": sum(1 for c in expiry_cohort if c["bucket"] == "READY_TO_SIGN"),
                "cohort_delivered_or_confirmed": sum(1 for c in expiry_cohort if c["bucket"] == "DELIVERED_OR_CONFIRMED"),
                "cohort_releasing_or_closed": sum(1 for c in expiry_cohort if c["bucket"] == "RELEASING_OR_CLOSED"),
                "cohort_unactioned_at_risk": sum(1 for c in expiry_cohort if c["bucket"] == "UNACTIONED_AT_RISK"),
                "cross_module_exceptions_count": len(cross_module_exceptions),
            },
            "pending_bank_letters": pending_bank_letters,
            "expiry_cohort": expiry_cohort,
            "cross_module_exceptions": cross_module_exceptions,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Handed-List Verifier ("A team member handed me a list of renewals...")
    # ──────────────────────────────────────────────────────────────────────────

    def verify_handed_list(
        self,
        db: Session,
        customer_id: int,
        raw_text: str,
        window_days: int = 45,
        cohort_start_date: Optional[str] = None,
        cohort_end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Cross-checks a pasted list of LG numbers / Instruction Serials / Internal Serials
        against both Custody and Issuance records + the active expiry cohort.
        """
        # 1. Extract unique candidate tokens from pasted text (supports newlines, commas, semicolons, tabs)
        raw_lines = [ln.strip() for ln in re.split(r"[\r\n,;]+", raw_text or "") if ln.strip()]
        tokens: List[str] = []
        seen_norm: Set[str] = set()

        for line in raw_lines:
            # If user pasted multi-column Excel rows (tab-separated), check cells; otherwise keep whole trimmed token
            parts = [p.strip() for p in line.split("\t") if p.strip()]
            chosen = parts[0] if len(parts) == 1 else None
            if chosen is None:
                # Pick first cell that looks like an LG/serial identifier (contains digits)
                for cell in parts:
                    if any(ch.isdigit() for ch in cell) and len(cell) >= 3:
                        chosen = cell
                        break
                if chosen is None and parts:
                    chosen = parts[0]
            if not chosen:
                continue
            # Skip obvious header labels
            if chosen.lower() in ("lg", "lg number", "lg_number", "lg #", "lg no", "serial", "serial number", "reference", "ref"):
                continue
            norm = re.sub(r"\s+", "", chosen).upper()
            if norm and norm not in seen_norm:
                seen_norm.add(norm)
                tokens.append(chosen.strip())

        workspace = self.get_control_verification_workspace(
            db,
            customer_id=customer_id,
            window_days=window_days,
            cohort_start_date=cohort_start_date,
            cohort_end_date=cohort_end_date,
        )

        users_by_id: Dict[int, User] = {
            u.id: u for u in db.query(User).filter(User.customer_id == customer_id).all()
        }
        banks_by_id: Dict[int, str] = {b.id: b.name for b in db.query(Bank).all()}
        currencies_by_id: Dict[int, str] = {c.id: c.iso_code for c in db.query(Currency).all()}
        entities_by_id: Dict[int, CustomerEntity] = {
            e.id: e for e in db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer_id).all()
        }
        now_utc = datetime.now(timezone.utc)

        # Build fast lookup maps across ALL Custody LGs & Instructions + Issuance LGs & Maintenance Actions
        custody_lgs = (
            db.query(LGRecord)
            .options(
                selectinload(LGRecord.issuing_bank),
                selectinload(LGRecord.communication_bank),
                selectinload(LGRecord.lg_currency),
                selectinload(LGRecord.beneficiary_corporate),
                selectinload(LGRecord.lg_status),
                selectinload(LGRecord.instructions),
            )
            .filter(LGRecord.customer_id == customer_id, LGRecord.is_deleted == False)
            .all()
        )
        issued_lgs = (
            db.query(IssuedLGRecord)
            .options(selectinload(IssuedLGRecord.maintenance_actions))
            .filter(IssuedLGRecord.customer_id == customer_id)
            .all()
        )

        def _norm(s: Optional[str]) -> str:
            return re.sub(r"\s+", "", str(s or "")).upper()

        custody_by_key: Dict[str, LGRecord] = {}
        for lg in custody_lgs:
            if lg.lg_number:
                custody_by_key[_norm(lg.lg_number)] = lg
            for inst in (lg.instructions or []):
                if not inst.is_deleted and inst.serial_number:
                    custody_by_key[_norm(inst.serial_number)] = lg

        issued_by_key: Dict[str, IssuedLGRecord] = {}
        for ilg in issued_lgs:
            for k in (ilg.bank_lg_number, ilg.lg_ref_number, ilg.internal_serial):
                if k:
                    issued_by_key[_norm(k)] = ilg
            for m in (ilg.maintenance_actions or []):
                if not m.is_deleted and m.letter_serial_number:
                    issued_by_key[_norm(m.letter_serial_number)] = ilg

        verified_items: List[Dict[str, Any]] = []
        unactioned_matches: List[Dict[str, Any]] = []
        unknown_references: List[str] = []
        matched_lg_keys: Set[str] = set()

        for tok in tokens:
            nk = _norm(tok)
            cust_lg = custody_by_key.get(nk)
            iss_lg = issued_by_key.get(nk) if not cust_lg else None

            # Fallback partial suffix match if exact normalized match not found (e.g., last digits without leading zeros/prefix)
            if not cust_lg and not iss_lg and len(nk) >= 4:
                cust_matches = [lg for k, lg in custody_by_key.items() if k.endswith(nk) or nk in k]
                if len({x.id for x in cust_matches}) == 1:
                    cust_lg = cust_matches[0]
                else:
                    iss_matches = [lg for k, lg in issued_by_key.items() if k.endswith(nk) or nk in k]
                    if len({x.id for x in iss_matches}) == 1:
                        iss_lg = iss_matches[0]

            if cust_lg:
                matched_lg_keys.add(f"CUSTODY:{cust_lg.id}")
                active_insts = [
                    i for i in (cust_lg.instructions or [])
                    if not i.is_deleted and (i.status or "").lower() not in ("canceled", "cancelled")
                ]
                active_insts.sort(key=lambda x: x.id, reverse=True)
                latest = active_insts[0] if active_insts else None

                if latest:
                    cat, lbl = _map_custody_instruction_category(latest.instruction_type)
                    summ, old_v, new_v = _summarize_custody_instruction(latest)
                    stage, stage_lbl, warns = _custody_instruction_lifecycle(latest, now_utc)
                    if cat == "RENEWAL" and cust_lg.auto_renewal is False:
                        warns.append("Auto-Renew is OFF")
                    verified_items.append({
                        "input_token": tok,
                        "module": "CUSTODY",
                        "lg_number": cust_lg.lg_number,
                        "serial_number": latest.serial_number,
                        "bank_name": _custody_bank_name(cust_lg),
                        "entity_name": cust_lg.beneficiary_corporate.entity_name if cust_lg.beneficiary_corporate else None,
                        "beneficiary_name": cust_lg.issuer_name,
                        "amount": _safe_float(cust_lg.lg_amount),
                        "currency": cust_lg.lg_currency.iso_code if cust_lg.lg_currency else None,
                        "action_label": lbl,
                        "change_summary": summ,
                        "old_expiry_date": old_v,
                        "new_expiry_date": new_v,
                        "actor_name": _user_label(users_by_id.get(latest.maker_user_id), latest.maker_user_id),
                        "action_date": _to_iso(latest.instruction_date or latest.created_at),
                        "lifecycle_stage": stage,
                        "lifecycle_label": stage_lbl,
                        "control_flags": warns,
                        "instruction_id": latest.id,
                        "maintenance_action_id": None,
                        "navigate_to": f"/corporate-admin/lg-records/{cust_lg.id}",
                    })
                else:
                    unactioned_matches.append({
                        "input_token": tok,
                        "module": "CUSTODY",
                        "lg_number": cust_lg.lg_number,
                        "bank_name": _custody_bank_name(cust_lg),
                        "entity_name": cust_lg.beneficiary_corporate.entity_name if cust_lg.beneficiary_corporate else None,
                        "beneficiary_name": cust_lg.issuer_name,
                        "amount": _safe_float(cust_lg.lg_amount),
                        "currency": cust_lg.lg_currency.iso_code if cust_lg.lg_currency else None,
                        "current_expiry_date": _to_date(cust_lg.expiry_date).isoformat() if _to_date(cust_lg.expiry_date) else None,
                        "current_status": cust_lg.lg_status.name if cust_lg.lg_status else "Valid",
                        "auto_renewal": bool(cust_lg.auto_renewal),
                        "reason": "LG exists in Custody, but NO renewal or instruction has been generated in Grow.",
                        "navigate_to": f"/corporate-admin/lg-records/{cust_lg.id}",
                    })
            elif iss_lg:
                matched_lg_keys.add(f"ISSUANCE:{iss_lg.id}")
                m_acts = [
                    m for m in (iss_lg.maintenance_actions or [])
                    if not m.is_deleted and (m.status or "").upper() not in ("REJECTED", "CANCELLED", "CANCELED")
                ]
                m_acts.sort(key=lambda x: x.id, reverse=True)
                latest_m = m_acts[0] if m_acts else None

                if latest_m:
                    adata = latest_m.action_data if isinstance(latest_m.action_data, dict) else {}
                    atype = (latest_m.action_type or "").upper()
                    stage, stage_lbl, warns = _issuance_maint_lifecycle(latest_m, now_utc)
                    old_e = adata.get("old_expiry_date") or adata.get("previous_expiry_date")
                    new_e = adata.get("new_expiry_date")
                    summ = (
                        f"Extend {_fmt_date_human(old_e) if old_e else ''} → {_fmt_date_human(new_e)}".strip()
                        if atype == "EXTEND"
                        else (latest_m.notes or f"Issuance {atype.title()}")
                    )
                    verified_items.append({
                        "input_token": tok,
                        "module": "ISSUANCE",
                        "lg_number": iss_lg.bank_lg_number or iss_lg.lg_ref_number,
                        "serial_number": latest_m.letter_serial_number,
                        "bank_name": banks_by_id.get(iss_lg.bank_id),
                        "entity_name": entities_by_id[iss_lg.issuing_entity_id].entity_name if iss_lg.issuing_entity_id in entities_by_id else None,
                        "beneficiary_name": iss_lg.beneficiary_name,
                        "amount": _safe_float(iss_lg.current_amount),
                        "currency": currencies_by_id.get(iss_lg.currency_id),
                        "action_label": f"Issued LG {atype.replace('_', ' ').title()}",
                        "change_summary": summ,
                        "old_expiry_date": old_e,
                        "new_expiry_date": new_e,
                        "actor_name": _user_label(users_by_id.get(latest_m.initiated_by_user_id), latest_m.initiated_by_user_id),
                        "action_date": _to_iso(latest_m.updated_at or latest_m.created_at),
                        "lifecycle_stage": stage,
                        "lifecycle_label": stage_lbl,
                        "control_flags": warns,
                        "instruction_id": None,
                        "maintenance_action_id": latest_m.id,
                        "navigate_to": "/corporate-admin/issuance/issued-lgs",
                    })
                else:
                    unactioned_matches.append({
                        "input_token": tok,
                        "module": "ISSUANCE",
                        "lg_number": iss_lg.bank_lg_number or iss_lg.lg_ref_number,
                        "bank_name": banks_by_id.get(iss_lg.bank_id),
                        "entity_name": entities_by_id[iss_lg.issuing_entity_id].entity_name if iss_lg.issuing_entity_id in entities_by_id else None,
                        "beneficiary_name": iss_lg.beneficiary_name,
                        "amount": _safe_float(iss_lg.current_amount),
                        "currency": currencies_by_id.get(iss_lg.currency_id),
                        "current_expiry_date": _to_date(iss_lg.expiry_date).isoformat() if _to_date(iss_lg.expiry_date) else None,
                        "current_status": iss_lg.status,
                        "auto_renewal": False,
                        "reason": "Issued LG exists, but NO maintenance / extension action has been initiated in Grow.",
                        "navigate_to": "/corporate-admin/issuance/issued-lgs",
                    })
            else:
                unknown_references.append(tok)

        # Find Omitted Items: LGs in the Expiry Cohort (either READY_TO_SIGN or UNACTIONED_AT_RISK) that were NOT on the handed list!
        omitted_items: List[Dict[str, Any]] = []
        for c in workspace["expiry_cohort"]:
            if c["bucket"] not in ("READY_TO_SIGN", "UNACTIONED_AT_RISK"):
                continue
            cid = str(c["id"])
            key_str = (
                f"CUSTODY:{cid.replace('cohort-cust-', '')}"
                if cid.startswith("cohort-cust-")
                else f"ISSUANCE:{cid.replace('cohort-iss-', '')}"
            )
            if key_str not in matched_lg_keys:
                omitted_items.append(c)

        return {
            "submitted_count": len(tokens),
            "verified_count": len(verified_items),
            "unactioned_count": len(unactioned_matches),
            "unknown_count": len(unknown_references),
            "omitted_from_list_count": len(omitted_items),
            "verified_items": verified_items,
            "unactioned_matches": unactioned_matches,
            "unknown_references": unknown_references,
            "omitted_items": omitted_items,
        }

    def get_audit_log_facets(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Returns distinct action_types, entity_types, users, and customer_entities
        that actually exist in AuditLog for this customer (with counts).
        """
        from sqlalchemy import func as sa_func

        action_rows = (
            db.query(AuditLog.action_type, sa_func.count(AuditLog.id))
            .filter(AuditLog.customer_id == customer_id, AuditLog.action_type.isnot(None))
            .group_by(AuditLog.action_type)
            .order_by(sa_func.count(AuditLog.id).desc())
            .all()
        )
        entity_rows = (
            db.query(AuditLog.entity_type, sa_func.count(AuditLog.id))
            .filter(AuditLog.customer_id == customer_id, AuditLog.entity_type.isnot(None))
            .group_by(AuditLog.entity_type)
            .order_by(sa_func.count(AuditLog.id).desc())
            .all()
        )
        user_rows = (
            db.query(AuditLog.user_id, User.email, sa_func.count(AuditLog.id))
            .join(User, AuditLog.user_id == User.id)
            .filter(AuditLog.customer_id == customer_id, AuditLog.user_id.isnot(None))
            .group_by(AuditLog.user_id, User.email)
            .order_by(sa_func.count(AuditLog.id).desc())
            .all()
        )
        ce_rows = (
            db.query(AuditLog.entity_id, CustomerEntity.entity_name, sa_func.count(AuditLog.id))
            .join(CustomerEntity, AuditLog.entity_id == CustomerEntity.id)
            .filter(
                AuditLog.customer_id == customer_id,
                AuditLog.entity_type == "CustomerEntity",
                AuditLog.entity_id.isnot(None),
            )
            .group_by(AuditLog.entity_id, CustomerEntity.entity_name)
            .order_by(sa_func.count(AuditLog.id).desc())
            .all()
        )
        return {
            "action_types": [{"value": a, "count": int(c)} for a, c in action_rows if a and c > 0],
            "entity_types": [{"value": e, "count": int(c)} for e, c in entity_rows if e and c > 0],
            "users": [{"id": uid, "email": email or f"User #{uid}", "count": int(c)} for uid, email, c in user_rows if c > 0],
            "customer_entities": [{"id": eid, "entity_name": name, "count": int(c)} for eid, name, c in ce_rows if c > 0],
        }


platform_activity_service = PlatformActivityService()
