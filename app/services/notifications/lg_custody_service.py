# app/services/notifications/lg_custody_service.py
"""
LG Custody Smart Mobile Alerts Service.

Operates as 'Middle Management' supervising end-user guarantee custody:
1. LG_CUSTODY_INACTION_ESCALATION: Escalates to Corporate Admin when an LG reaches <= 14d and <= 5d without any renewal/release action.
2. LG_CUSTODY_UNDELIVERED_LETTER: Alerts Corporate Admin when an extension/claim letter was issued on desktop but not marked delivered to bank (<= 5d to expiry).
3. LG_CUSTODY_APPROVAL_REQUIRED: Alerts Corporate Admin when an End User requests a Release or Liquidation requiring Maker-Checker authorization.
4. LG_CUSTODY_DAY_ZERO: Morning emergency alert at Day 0 cutoff if guarantee expires today without confirmed extension/claim.
5. LG_CUSTODY_BANK_DEFECT: Alerts Corporate Admin when a bank reply contains an objection or defect.
6. LG_USER_EXPIRY_ACTION_NOTICE: 30-day advisory reminder to End User directing them to the desktop terminal to prepare paperwork.
7. LG_USER_PRE_ESCALATION_WARNING: 16-day grace warning (48h before Admin escalation) to prompt desktop action.
8. LG_USER_REQUEST_DECIDED: Alerts End User when Admin decides on their Release/Liquidation request.
9. LG_CUSTODY_PORTFOLIO_BURST_DIGEST: Anti-spam consolidation when > 3 LGs trigger the same sweep event for a customer on the same morning.

Strict institutional confidentiality: zero financial amounts, masked LG serials, masked counterparty names.
"""

import logging
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
import pytz

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import or_

import app.models as models
from app.constants import (
    LgStatusEnum,
    ApprovalRequestStatusEnum,
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
)
from app.models.models_notification import UserNotification
from app.services.notifications.catalog import EventCode, get_spec
from app.services.notifications.emit import emit_lg

logger = logging.getLogger(__name__)

CAIRO_TZ = pytz.timezone("Africa/Cairo")
BURST_THRESHOLD = 3

DELIVERABLE_CUSTODY_INSTRUCTIONS = (
    "LG_EXTENSION",
    "LG_RELEASE",
    "LG_LIQUIDATE",
    "LG_DECREASE_AMOUNT",
    "LG_ACTIVATE_NON_OPERATIVE",
    "LG_AMEND",
    "EXTENSION",
    "RELEASE",
    "LIQUIDATE",
)

TERMINAL_CUSTODY_INSTRUCTIONS = (
    "LG_RELEASE",
    "LG_LIQUIDATE",
    "RELEASE",
    "LIQUIDATE",
)

EXTENSION_INSTRUCTIONS = (
    "LG_EXTENSION",
    "EXTENSION",
)

BURST_SUMMARY_LABELS = {
    EventCode.LG_CUSTODY_DAY_ZERO: "Final Expiry Cutoff Today",
    EventCode.LG_CUSTODY_UNDELIVERED_LETTER: "Undelivered Bank Letter Risk",
    EventCode.LG_CUSTODY_INACTION_ESCALATION: "Custody Inaction Escalation",
    EventCode.LG_USER_PRE_ESCALATION_WARNING: "Pending Management Escalation (48h)",
    EventCode.LG_USER_EXPIRY_ACTION_NOTICE: "30-Day Guarantee Action Advisory",
}


def _cairo_today() -> date:
    """Returns today's calendar date in Africa/Cairo timezone to match the scheduler."""
    return datetime.now(CAIRO_TZ).date()


def was_event_sent_recently(
    db: Session,
    lg_id: int,
    event_code: EventCode,
    within_days: int = 1,
    customer_id: Optional[int] = None,
    bucket_key: Optional[str] = None,
) -> bool:
    """
    Checks if this specific event (or a portfolio burst digest covering this bucket)
    was already dispatched for the LG within N days.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=within_days)
    indiv_filter = (
        (UserNotification.reference_type == "LGRecord")
        & (UserNotification.reference_id == lg_id)
        & (UserNotification.event_type == event_code.value)
    )
    if customer_id is not None:
        b_key = bucket_key or event_code.value
        burst_ref_key = f"{customer_id}:{b_key}"
        burst_filter = (
            (UserNotification.reference_type == "LGRecord")
            & (UserNotification.event_type == EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST.value)
            & (UserNotification.reference_key == burst_ref_key)
        )
        condition = or_(indiv_filter, burst_filter)
    else:
        condition = indiv_filter

    count = db.query(UserNotification.id).filter(
        condition,
        UserNotification.created_at >= cutoff,
    ).count()
    return count > 0


def _has_active_custody_action(db: Session, lg: models.LGRecord, today: Optional[date] = None) -> bool:
    """
    Returns True if a custody action has been taken or is in progress for the CURRENT renewal cycle:
    1. A terminal AuditLog action for Release or Liquidation.
    2. A PENDING ApprovalRequest for this LGRecord (waiting on Checker/Admin).
    3. A non-canceled instruction that is:
       - A terminal instruction (Release / Liquidate), OR
       - An open/unconfirmed extension (bank_reply_date is None), OR
       - An extension issued within the current renewal window (last 45 days).
       (Historical extensions confirmed in prior years do NOT suppress current cycle alerts).
    """
    ref_today = today or _cairo_today()

    # 1. Check audit log for explicit release/liquidation
    audit_action = db.query(models.AuditLog.id).filter(
        models.AuditLog.lg_record_id == lg.id,
        models.AuditLog.action_type.in_([ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE]),
    ).first()
    if audit_action:
        return True

    # 2. Check for a pending Maker-Checker approval request on this LG
    pending_ar = db.query(models.ApprovalRequest.id).filter(
        models.ApprovalRequest.entity_type == "LGRecord",
        models.ApprovalRequest.entity_id == lg.id,
        models.ApprovalRequest.status == ApprovalRequestStatusEnum.PENDING,
    ).first()
    if pending_ar:
        return True

    # 3. Check instructions scoped to current renewal cycle
    if getattr(lg, "instructions", None):
        recent_cutoff = ref_today - timedelta(days=45)
        for inst in lg.instructions:
            if getattr(inst, "is_deleted", False) or inst.status == "Canceled":
                continue
            inst_type = (inst.instruction_type or "").upper()
            if inst_type in TERMINAL_CUSTODY_INSTRUCTIONS:
                return True
            if inst_type in EXTENSION_INSTRUCTIONS:
                if inst.bank_reply_date is None and inst.status != "Confirmed by Bank":
                    return True
                inst_dt = None
                if inst.instruction_date:
                    inst_dt = inst.instruction_date.date() if hasattr(inst.instruction_date, "date") else inst.instruction_date
                elif getattr(inst, "created_at", None):
                    inst_dt = inst.created_at.date() if hasattr(inst.created_at, "date") else inst.created_at
                if inst_dt and inst_dt >= recent_cutoff:
                    return True

    return False


def resolve_stale_custody_notifications(
    db: Session,
    lg_record_id: int,
    event_codes: Optional[List[EventCode]] = None,
) -> int:
    """
    Auto-resolves (marks read & expires) unread custody notifications for an LG
    when the End User completes the required action or withdraws an approval request.
    """
    if not lg_record_id:
        return 0
    target_codes = [c.value for c in event_codes] if event_codes else [
        EventCode.LG_CUSTODY_INACTION_ESCALATION.value,
        EventCode.LG_CUSTODY_UNDELIVERED_LETTER.value,
        EventCode.LG_CUSTODY_DAY_ZERO.value,
        EventCode.LG_USER_EXPIRY_ACTION_NOTICE.value,
        EventCode.LG_USER_PRE_ESCALATION_WARNING.value,
    ]
    now = datetime.now(timezone.utc)
    updated = db.query(UserNotification).filter(
        UserNotification.reference_type == "LGRecord",
        UserNotification.reference_id == lg_record_id,
        UserNotification.event_type.in_(target_codes),
        UserNotification.is_read == False,  # noqa: E712
    ).update(
        {"is_read": True, "expires_at": now},
        synchronize_session=False,
    )
    if updated:
        logger.info("Auto-resolved %d stale custody notification(s) for LG %s", updated, lg_record_id)
    return updated


def _dispatch_bucket_with_burst_protection(
    db: Session,
    customer_id: int,
    event_code: EventCode,
    bucket_key: str,
    items: List[Tuple[models.LGRecord, Dict[str, Any]]],
) -> int:
    """
    Dispatches queued notifications for a customer & event bucket.
    If <= BURST_THRESHOLD (3) LGs triggered the event, emits each individually.
    If > BURST_THRESHOLD LGs triggered the event on the same morning, emits the top 2
    most urgent individual alerts + 1 consolidated portfolio burst digest.
    """
    if not items:
        return 0

    staged = 0
    if len(items) <= BURST_THRESHOLD:
        for lg_rec, extra in items:
            staged += emit_lg(db, event_code, lg_rec, extra=extra)
        return staged

    # Burst protection triggered (> 3 LGs for the same event in the same morning)
    # Sort by expiry_date ascending so the most urgent LGs get the 2 individual slots
    sorted_items = sorted(
        items,
        key=lambda pair: (
            pair[0].expiry_date.date() if hasattr(pair[0].expiry_date, "date") else pair[0].expiry_date,
            pair[0].id or 0,
        ),
    )
    for lg_rec, extra in sorted_items[:2]:
        staged += emit_lg(db, event_code, lg_rec, extra=extra)

    lead_lg = sorted_items[2][0]
    spec = get_spec(event_code)
    summary_label = BURST_SUMMARY_LABELS.get(event_code, spec.title)
    staged += emit_lg(
        db=db,
        code=EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST,
        lg_record=lead_lg,
        extra={
            "count": len(sorted_items),
            "summary_type": summary_label,
        },
        audiences_override=spec.audiences,
        reference_key_override=f"{customer_id}:{bucket_key}",
    )
    logger.info(
        "Burst protection consolidated %d '%s' alerts for customer %s into 2 individual + 1 portfolio digest",
        len(sorted_items), event_code.value, customer_id,
    )
    return staged


async def run_lg_custody_mobile_alerts_sweep(db: Session) -> int:
    """
    Daily background sweep for LG Custody smart mobile notifications.
    Completely additive, isolated in its own error-handling block with per-customer commit.
    """
    logger.info("--- START: LG Custody Smart Mobile Alerts Sweep ---")
    today = _cairo_today()

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()  # noqa: E712
    if not customers:
        return 0

    staged_total = 0

    for customer in customers:
        try:
            max_lookahead = today + timedelta(days=35)
            eligible_lgs = db.query(models.LGRecord).filter(
                models.LGRecord.customer_id == customer.id,
                models.LGRecord.is_deleted == False,  # noqa: E712
                models.LGRecord.lg_status_id == LgStatusEnum.VALID.value,
                models.LGRecord.expiry_date >= today,
                models.LGRecord.expiry_date <= max_lookahead,
            ).options(
                selectinload(models.LGRecord.instructions),
            ).all()

            # Group triggered events per customer to apply Portfolio Burst Protection
            # Key: (EventCode, bucket_key) -> List[(LGRecord, extra_dict)]
            pending_buckets: Dict[Tuple[EventCode, str], List[Tuple[models.LGRecord, Dict[str, Any]]]] = defaultdict(list)

            for lg in eligible_lgs:
                lg_expiry_val = lg.expiry_date.date() if hasattr(lg.expiry_date, "date") else lg.expiry_date
                days_left = (lg_expiry_val - today).days

                # 1. Day 0 Final Cutoff Alert (Corporate Admin)
                if days_left == 0:
                    b_key = EventCode.LG_CUSTODY_DAY_ZERO.value
                    if not was_event_sent_recently(
                        db, lg.id, EventCode.LG_CUSTODY_DAY_ZERO, within_days=1,
                        customer_id=customer.id, bucket_key=b_key,
                    ):
                        pending_buckets[(EventCode.LG_CUSTODY_DAY_ZERO, b_key)].append((lg, {}))

                # 2. Dormant Undelivered Letter Check (Corporate Admin)
                # If an instruction letter was issued on desktop but not marked delivered to bank, and days_left <= 5
                if 0 <= days_left <= 5 and getattr(lg, "instructions", None):
                    for inst in lg.instructions:
                        if (
                            not getattr(inst, "is_deleted", False)
                            and inst.instruction_type in DELIVERABLE_CUSTODY_INSTRUCTIONS
                            and inst.status == "Instruction Issued"
                            and not inst.delivery_date
                            and not inst.sent_to_bank
                        ):
                            b_key = EventCode.LG_CUSTODY_UNDELIVERED_LETTER.value
                            if not was_event_sent_recently(
                                db, lg.id, EventCode.LG_CUSTODY_UNDELIVERED_LETTER, within_days=2,
                                customer_id=customer.id, bucket_key=b_key,
                            ):
                                pending_buckets[(EventCode.LG_CUSTODY_UNDELIVERED_LETTER, b_key)].append(
                                    (lg, {"days": days_left})
                                )
                            break

                # Check if End User already took action in the current renewal cycle
                has_action = _has_active_custody_action(db, lg, today=today)

                if not has_action:
                    # 3a. Inaction Escalation to Corporate Admin — 14-day window (12..14d catch-up)
                    if 12 <= days_left <= 14:
                        b_key = f"{EventCode.LG_CUSTODY_INACTION_ESCALATION.value}_14D"
                        if not was_event_sent_recently(
                            db, lg.id, EventCode.LG_CUSTODY_INACTION_ESCALATION, within_days=6,
                            customer_id=customer.id, bucket_key=b_key,
                        ):
                            pending_buckets[(EventCode.LG_CUSTODY_INACTION_ESCALATION, b_key)].append(
                                (lg, {"days": days_left})
                            )

                    # 3b. Inaction Escalation to Corporate Admin — 5-day critical window (3..5d catch-up)
                    elif 3 <= days_left <= 5:
                        b_key = f"{EventCode.LG_CUSTODY_INACTION_ESCALATION.value}_5D"
                        if not was_event_sent_recently(
                            db, lg.id, EventCode.LG_CUSTODY_INACTION_ESCALATION, within_days=5,
                            customer_id=customer.id, bucket_key=b_key,
                        ):
                            pending_buckets[(EventCode.LG_CUSTODY_INACTION_ESCALATION, b_key)].append(
                                (lg, {"days": days_left})
                            )

                    # 4. Pre-Escalation Grace Warning to End User — 16-day window (15..16d catch-up)
                    elif 15 <= days_left <= 16:
                        b_key = EventCode.LG_USER_PRE_ESCALATION_WARNING.value
                        if not was_event_sent_recently(
                            db, lg.id, EventCode.LG_USER_PRE_ESCALATION_WARNING, within_days=5,
                            customer_id=customer.id, bucket_key=b_key,
                        ):
                            pending_buckets[(EventCode.LG_USER_PRE_ESCALATION_WARNING, b_key)].append((lg, {}))

                    # 5. 30-Day Advisory Notice to End User (Desktop Paperwork Reminder) — (28..30d catch-up)
                    elif 28 <= days_left <= 30:
                        b_key = EventCode.LG_USER_EXPIRY_ACTION_NOTICE.value
                        if not was_event_sent_recently(
                            db, lg.id, EventCode.LG_USER_EXPIRY_ACTION_NOTICE, within_days=10,
                            customer_id=customer.id, bucket_key=b_key,
                        ):
                            pending_buckets[(EventCode.LG_USER_EXPIRY_ACTION_NOTICE, b_key)].append((lg, {}))

            customer_staged = 0
            for (event_code, bucket_key), items in pending_buckets.items():
                customer_staged += _dispatch_bucket_with_burst_protection(
                    db=db,
                    customer_id=customer.id,
                    event_code=event_code,
                    bucket_key=bucket_key,
                    items=items,
                )

            if customer_staged > 0:
                db.commit()
                staged_total += customer_staged

        except Exception:
            db.rollback()
            logger.exception("Error during LG Custody mobile alerts sweep for customer %s", customer.id)
            continue

    logger.info("--- FINISHED: LG Custody Smart Mobile Alerts Sweep (staged %d notifications) ---", staged_total)
    return staged_total


def notify_custody_approval_required(
    db: Session,
    lg_record: models.LGRecord,
    action_type: str,
    maker_user_id: int,
) -> int:
    """
    Notifies Corporate Admins when an End User submits a Custody request requiring approval.
    Also auto-resolves any pending inaction/expiry warnings on that LG since the user took action.
    """
    resolve_stale_custody_notifications(
        db,
        lg_record.id,
        event_codes=[
            EventCode.LG_CUSTODY_INACTION_ESCALATION,
            EventCode.LG_USER_EXPIRY_ACTION_NOTICE,
            EventCode.LG_USER_PRE_ESCALATION_WARNING,
        ],
    )
    human_action = action_type.replace("LG_", "").replace("_", " ").title()
    return emit_lg(
        db=db,
        code=EventCode.LG_CUSTODY_APPROVAL_REQUIRED,
        lg_record=lg_record,
        actor_user_id=maker_user_id,
        extra={"action_type": human_action},
    )


def notify_custody_request_decided(
    db: Session,
    lg_record: models.LGRecord,
    status: str,
    maker_user_id: int,
    checker_user_id: Optional[int] = None,
) -> int:
    """
    Notifies the End User when their Custody request is approved or rejected by Corporate Admin.
    Also auto-resolves the Corporate Admin's unread LG_CUSTODY_APPROVAL_REQUIRED notification.
    """
    resolve_stale_custody_notifications(
        db,
        lg_record.id,
        event_codes=[EventCode.LG_CUSTODY_APPROVAL_REQUIRED],
    )
    status_text = "approved" if "APPROV" in status.upper() else "rejected"
    return emit_lg(
        db=db,
        code=EventCode.LG_USER_REQUEST_DECIDED,
        lg_record=lg_record,
        actor_user_id=checker_user_id,
        target_user_id=maker_user_id,
        extra={"status": status_text},
    )


def notify_custody_bank_defect(
    db: Session,
    lg_record: models.LGRecord,
    actor_user_id: Optional[int] = None,
) -> int:
    """
    Notifies Corporate Admins when a bank reply is recorded with objections or AI defect detection.
    """
    return emit_lg(
        db=db,
        code=EventCode.LG_CUSTODY_BANK_DEFECT,
        lg_record=lg_record,
        actor_user_id=actor_user_id,
    )
