# app/services/notifications/quotation_timers.py
"""
Timed quotation alerts (Notifications Q4).

A single sweeper runs every few seconds and raises time-based events:

  RFQ_CLOSING_SOON        live window, <= 2 minutes before window_end
  RFQ_QUOTES_READY        window closed, a winning quote awaits accept/reject
  RFQ_RESULTS_READY       window closed, indicative-only RFQ concluded
  RFQ_CLOSED_NO_QUOTES    window closed without executable quotes
  RFQ_DEAL_AUTO_RESOLVED  acceptance time ran out, policy accepted/rejected

Why a sweeper (not one timer job per RFQ): it needs no hooks in every
approve / cancel / reschedule / re-tender path, survives restarts by design,
and each alert is raised at most once (an existing notification row with the
same event_type + reference_key is the idempotency guard).

RFQ window transitions are otherwise lazy (computed when someone opens the
RFQ). The sweeper runs the same evaluation (compute_rfq_standings) once at
window close and once at the acceptance deadline, so the state - and the
alert - is correct even if nobody is looking. Results emails stay on their
existing path (dispatch_emails=False).
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional, Set, Tuple

from sqlalchemy import and_, exists, or_, event
from sqlalchemy.orm import Session

from app.models.models_notification import UserNotification
from app.models.models_quotation import QuotationRequest
from app.services.notifications.catalog import EventCode
from app.services.notifications.emit import emit

logger = logging.getLogger(__name__)

SWEEP_JOB_ID = "quotation_timed_alerts_sweep"
NUDGE_JOB_ID = "quotation_timed_alerts_nudge"
IDLE_SWEEP_SECONDS = 30
ACTIVE_SWEEP_SECONDS = 2
CLOSING_SOON_LEAD = timedelta(minutes=2)
QUOTES_READY_10S_LEAD = timedelta(seconds=10)
STARTING_SOON_5M_LEAD = timedelta(minutes=5)
STARTING_SOON_1M_LEAD = timedelta(minutes=1)
MIN_ADVANCE_LEAD = timedelta(minutes=15)
SHORT_WINDOW_MAX_DURATION = timedelta(minutes=10)
CATCH_UP = timedelta(minutes=30)      # how far back a missed alert is still worth raising
APPROACHING_LEAD = CLOSING_SOON_LEAD + timedelta(seconds=IDLE_SWEEP_SECONDS)
LIVE_STATUSES = ("PENDING", "OPEN")
CLOSE_STATUSES = ("PENDING", "OPEN", "EVALUATING")
OUTCOME_CODES = (EventCode.RFQ_QUOTES_READY, EventCode.RFQ_RESULTS_READY, EventCode.RFQ_CLOSED_NO_QUOTES)

_scheduler = None
_session_factory = None
_current_interval: int = IDLE_SWEEP_SECONDS
_SESSION_FLAG = "quotation_timer_nudge_pending"
# Evaluations already run in this process: (stage, rfq_id). Prevents re-running
# compute_rfq_standings every few seconds for an RFQ whose state does not change.
_evaluated: Dict[Tuple[str, str], datetime] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _already_notified(db: Session, rfq_id: str, codes) -> bool:
    values = [c.value for c in codes]
    return db.query(exists().where(
        UserNotification.reference_key == str(rfq_id),
        UserNotification.event_type.in_(values),
    )).scalar()


def _is_indicative_only(rfq: QuotationRequest) -> bool:
    return (rfq.quotation_base or "Execution").lower() == "indicative" and not getattr(rfq, "legs", [])


def _evaluate_once(stage: str, rfq: QuotationRequest, db: Session, evaluate: Callable) -> None:
    key = (stage, str(rfq.id))
    if key in _evaluated:
        return
    _evaluated[key] = _now()
    evaluate(rfq, db)
    db.refresh(rfq)


def _prune(now: datetime) -> None:
    for key, at in list(_evaluated.items()):
        if now - at > CATCH_UP * 2:
            _evaluated.pop(key, None)


def _default_evaluate(rfq: QuotationRequest, db: Session) -> None:
    from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings
    compute_rfq_standings(rfq, db, dispatch_emails=False)


def _raise(db: Session, code: EventCode, rfq: QuotationRequest, extra=None) -> int:
    count = emit(db, code, rfq, actor_user_id=None, extra=extra)
    db.commit()
    if count:
        logger.info("Timed alert %s raised for RFQ %s (%d recipient(s))", code.value, rfq.ref_no, count)
    return count


def _collect_assignment_dealer_emails(db: Session, asgn) -> Set[str]:
    """
    Extracts all execution dealer emails associated with an invited bank assignment.
    Skips assignments pending bank approval or already declined/expired, and excludes
    VIEW_ONLY and APPROVER contacts so only dealers with execution authority are alerted.
    """
    dealer_emails: Set[str] = set()
    if getattr(asgn, "approval_status", None) in ("PENDING", "DECLINED", "EXPIRED"):
        return dealer_emails

    bank_row = getattr(asgn, "quotation_bank", None)
    if not bank_row:
        return dealer_emails

    import json as _json
    contacts = getattr(bank_row, "contacts", None)
    if isinstance(contacts, str):
        try:
            contacts = _json.loads(contacts)
        except Exception:
            contacts = []

    non_execution_emails: Set[str] = set()
    if isinstance(contacts, list):
        for c in contacts:
            if isinstance(c, dict) and c.get("email"):
                email_clean = c["email"].strip().lower()
                role = (c.get("role") or "EXECUTION").upper()
                if role == "EXECUTION":
                    dealer_emails.add(email_clean)
                else:
                    non_execution_emails.add(email_clean)

    if getattr(bank_row, "emails", None) and not contacts:
        for e in bank_row.emails.split(","):
            if e.strip():
                email_clean = e.strip().lower()
                if email_clean not in non_execution_emails:
                    dealer_emails.add(email_clean)

    for off in getattr(asgn, "offers", []) or []:
        if getattr(off, "submitted_by_email", None):
            email_clean = off.submitted_by_email.strip().lower()
            if email_clean not in non_execution_emails:
                dealer_emails.add(email_clean)

    for otp in getattr(asgn, "otps", []) or []:
        if getattr(otp, "email", None):
            email_clean = otp.email.strip().lower()
            if email_clean not in non_execution_emails:
                dealer_emails.add(email_clean)

    if getattr(bank_row, "bank_id", None):
        from app.models.models_quotation import QuotationBankDealer
        dealers = db.query(QuotationBankDealer.email).filter(
            QuotationBankDealer.bank_id == bank_row.bank_id,
            QuotationBankDealer.is_active == True,  # noqa: E712
            QuotationBankDealer.is_deleted == False,  # noqa: E712
        ).all()
        for (d_email,) in dealers:
            if d_email:
                email_clean = d_email.strip().lower()
                if email_clean not in non_execution_emails:
                    dealer_emails.add(email_clean)

    return dealer_emails


def _has_valid_execution_quote(rfq: QuotationRequest) -> bool:
    """
    Returns True if the RFQ has received at least 1 valid (non-passed) execution quote
    from an active bank assignment. Used by stage 7A (T - 10s before window_end) so we
    can alert the corporate users 10 seconds before the acceptance window opens without
    mutating or closing the live RFQ window early.
    """
    if _is_indicative_only(rfq):
        return False

    rfq_type = (getattr(rfq, "type", None) or "FX_SPOT").upper()
    rfq_default_base = (getattr(rfq, "quotation_base", None) or "Execution").lower()

    for asgn in (getattr(rfq, "assignments", None) or []):
        if getattr(asgn, "is_cross_entity", False):
            continue
        if getattr(asgn, "approval_status", None) in ("PENDING", "DECLINED", "EXPIRED"):
            continue

        asgn_base = (getattr(asgn, "quotation_base", None) or rfq_default_base).lower()

        if rfq_type == "TBILL":
            if asgn_base == "indicative":
                continue
            for t_off in (getattr(asgn, "tbill_offers", None) or []):
                if getattr(t_off, "discount_rate", None) is not None or getattr(t_off, "encrypted_discount_rate", None) is not None:
                    return True
        else:
            leg_base_map = {
                str(lc.leg_id): (lc.quotation_base or "Execution").lower()
                for lc in (getattr(asgn, "leg_configs", None) or [])
                if getattr(lc, "leg_id", None) is not None
            }
            for off in (getattr(asgn, "offers", None) or []):
                if getattr(off, "is_passed", False):
                    continue
                has_price = (getattr(off, "price", None) is not None) or (getattr(off, "encrypted_price", None) is not None)
                if not has_price:
                    continue
                leg_id_str = str(off.leg_id) if getattr(off, "leg_id", None) is not None else None
                if leg_id_str and leg_id_str in leg_base_map:
                    is_exec = leg_base_map[leg_id_str] == "execution"
                else:
                    is_exec = asgn_base in ("execution", "mixed")
                if is_exec:
                    return True

    return False


# ── Stages ──────────────────────────────────────────────────────────────────

def _starting_soon_5m(db: Session, now: datetime) -> int:
    """
    At T - 5 Minutes:
    - Sent if lead time (window_start - dispatched_at) >= 15 minutes.
    - Maker + Delegate: Sent in all cases (RFQ_STARTING_SOON).
    - Bank Dealers: For each invited bank, check if any dealer is online.
      If at least 1 dealer is online, ignore reminder for that bank desk.
      If 0 dealers are online, send DEALER_STARTING_SOON push.
    """
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(LIVE_STATUSES),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_start > now,
        QuotationRequest.window_start <= now + STARTING_SOON_5M_LEAD,
    ).all()

    from app.services.desk_session_service import desk_session_service
    from app.services.notifications.push_channel import enqueue_dealer_push

    raised = 0
    dealer_pushes_staged = 0

    for rfq in rfqs:
        w_start = _as_utc(rfq.window_start)
        disp_time = _as_utc(rfq.dispatched_at or rfq.created_at)
        if not w_start or not disp_time:
            continue

        # Only RFQs dispatched in advance (>= 15 minutes lead time)
        if (w_start - disp_time) < MIN_ADVANCE_LEAD:
            continue

        # 1. Maker + Delegate: sent in all cases (idempotent)
        if not _already_notified(db, rfq.id, [EventCode.RFQ_STARTING_SOON]):
            raised += _raise(db, EventCode.RFQ_STARTING_SOON, rfq)

        # 2. Bank Dealers: presence-aware
        for asgn in (rfq.assignments or []):
            if desk_session_service.is_desk_active(str(asgn.id)):
                # Dealer already online: ignore / suppress reminder
                continue

            # 0 dealers online: enqueue reminder to bank dealers
            dealer_emails = _collect_assignment_dealer_emails(db, asgn)
            if dealer_emails:
                bank_id = asgn.quotation_bank.bank_id if asgn.quotation_bank else None
                try:
                    dealer_pushes_staged += enqueue_dealer_push(
                        db=db,
                        assignment_id=str(asgn.id),
                        rfq_id=str(rfq.id),
                        ref_no=rfq.ref_no,
                        target_url=f"/public-quotation/{asgn.token}",
                        event_code="DEALER_STARTING_SOON",
                        title=f"RFQ {rfq.ref_no} · Starting in 5 min",
                        body="Quotation window opens in 5 minutes. Check in to prepare your quote.",
                        dealer_emails=dealer_emails,
                        bank_id=bank_id,
                        urgency="normal",
                        expires_at=rfq.window_start,
                    )
                except Exception as e:
                    logger.warning("Failed to enqueue dealer 5m starting soon push for RFQ %s: %s", rfq.ref_no, e)

    if dealer_pushes_staged:
        db.commit()

    return raised


def _starting_soon_1m(db: Session, now: datetime) -> int:
    """
    At T - 1 Minute:
    - Only applies to short quotation windows (duration <= 10m) with advance dispatch (lead time >= 15m).
    - Bank Dealers: For any bank still having 0 dealers online, send urgent DEALER_URGENT_STARTING_SOON push.
    - Maker Visibility: If all invited banks have 0 dealers online, notify Maker + Delegate (RFQ_NO_DEALERS_CHECKED_IN).
    """
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(LIVE_STATUSES),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_start > now,
        QuotationRequest.window_start <= now + STARTING_SOON_1M_LEAD,
    ).all()

    from app.services.desk_session_service import desk_session_service
    from app.services.notifications.push_channel import enqueue_dealer_push

    raised = 0
    dealer_pushes_staged = 0

    for rfq in rfqs:
        w_start = _as_utc(rfq.window_start)
        w_end = _as_utc(rfq.window_end)
        disp_time = _as_utc(rfq.dispatched_at or rfq.created_at)
        if not w_start or not w_end or not disp_time:
            continue

        # Only RFQs dispatched in advance (>= 15 minutes lead time)
        if (w_start - disp_time) < MIN_ADVANCE_LEAD:
            continue

        # Only short windows (duration <= 10 minutes)
        if (w_end - w_start) > SHORT_WINDOW_MAX_DURATION:
            continue

        assignments = rfq.assignments or []
        total_invited = len(assignments)
        checked_in_count = 0

        for asgn in assignments:
            if desk_session_service.is_desk_active(str(asgn.id)):
                checked_in_count += 1
            else:
                # Bank still absent: send urgent 1m follow-up
                dealer_emails = _collect_assignment_dealer_emails(db, asgn)
                if dealer_emails:
                    bank_id = asgn.quotation_bank.bank_id if asgn.quotation_bank else None
                    try:
                        dealer_pushes_staged += enqueue_dealer_push(
                            db=db,
                            assignment_id=str(asgn.id),
                            rfq_id=str(rfq.id),
                            ref_no=rfq.ref_no,
                            target_url=f"/public-quotation/{asgn.token}",
                            event_code="DEALER_URGENT_STARTING_SOON",
                            title=f"RFQ {rfq.ref_no} · Window opens in 1 min",
                            body="The bidding window starts in 1 minute. Check in now to submit your quote.",
                            dealer_emails=dealer_emails,
                            bank_id=bank_id,
                            urgency="high",
                            expires_at=rfq.window_start,
                        )
                    except Exception as e:
                        logger.warning("Failed to enqueue dealer 1m urgent push for RFQ %s: %s", rfq.ref_no, e)

        # Maker visibility at T - 1m: if 0 of N banks checked in, warn the maker
        if total_invited > 0 and checked_in_count == 0:
            if not _already_notified(db, rfq.id, [EventCode.RFQ_NO_DEALERS_CHECKED_IN]):
                raised += _raise(db, EventCode.RFQ_NO_DEALERS_CHECKED_IN, rfq, extra={"invited_count": str(total_invited)})

    if dealer_pushes_staged:
        db.commit()

    return raised


def _closing_soon(db: Session, now: datetime) -> int:
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(LIVE_STATUSES),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_start <= now,
        QuotationRequest.window_end > now,
        QuotationRequest.window_end <= now + CLOSING_SOON_LEAD,
    ).all()
    raised = 0
    for rfq in rfqs:
        if _is_indicative_only(rfq) or _already_notified(db, rfq.id, [EventCode.RFQ_CLOSING_SOON]):
            continue

        # If total bidding window is <= lead time (e.g. 1m or 2m test window), skip "closing soon" alert
        if rfq.window_start and rfq.window_end:
            window_duration = _as_utc(rfq.window_end) - _as_utc(rfq.window_start)
            if window_duration <= CLOSING_SOON_LEAD:
                continue

        # 1. Raise in-app / push to maker
        raised += _raise(db, EventCode.RFQ_CLOSING_SOON, rfq)

        # 2. Also enqueue push alerts to all invited bank dealers who subscribed to mobile alerts
        try:
            from app.services.notifications.push_channel import enqueue_dealer_push

            dealer_push_count = 0
            for asgn in (rfq.assignments or []):
                bank_row = asgn.quotation_bank
                if not bank_row:
                    continue
                dealer_emails = _collect_assignment_dealer_emails(db, asgn)
                if dealer_emails:
                    dealer_push_count += enqueue_dealer_push(
                        db=db,
                        assignment_id=asgn.id,
                        rfq_id=str(rfq.id),
                        ref_no=rfq.ref_no,
                        target_url=f"/public-quotation/{asgn.token}",
                        event_code="DEALER_CLOSING_SOON",
                        title=f"RFQ {rfq.ref_no} · Closing soon",
                        body="Quotation window closes in about 2 minutes. Submit or update your quote now.",
                        dealer_emails=dealer_emails,
                        bank_id=bank_row.bank_id,
                        urgency="high",
                        expires_at=rfq.window_end,
                    )
            if dealer_push_count:
                db.commit()
        except Exception as dealer_err:
            logger.warning("Failed to enqueue dealer closing soon push for RFQ %s: %s", rfq.ref_no, dealer_err)

    return raised


def _quotes_ready_10s(db: Session, now: datetime) -> int:
    """
    Stage 7A (At window_end - 10s):
    If the quotation window is within 10 seconds of closing AND at least 1 valid
    execution quote has been received, fire RFQ_QUOTES_READY early so Maker,
    Delegate, and Corporate Admins are ready when the acceptance window opens.
    """
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(LIVE_STATUSES),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_start <= now,
        QuotationRequest.window_end > now,
        QuotationRequest.window_end <= now + QUOTES_READY_10S_LEAD,
    ).all()
    raised = 0
    for rfq in rfqs:
        if _is_indicative_only(rfq) or _already_notified(db, rfq.id, OUTCOME_CODES):
            continue
        if _has_valid_execution_quote(rfq):
            raised += _raise(db, EventCode.RFQ_QUOTES_READY, rfq)
    return raised


def _window_closed(db: Session, now: datetime, evaluate: Callable) -> int:
    """
    Stage 7B & #8 (At window_end - 0s):
    Always runs compute_rfq_standings once when the window closes so the RFQ
    transitions to EVALUATING/PENDING or COMPLETED/NO_QUOTES on time.
    - If 7A already sent RFQ_QUOTES_READY at T - 10s, _already_notified is True
      and 7B is skipped (never sends both 7A and 7B).
    - If 7A was NOT sent (e.g. a quote arrived in the final 10 seconds) and at
      least 1 valid execution quote was received (acceptance_status == 'PENDING'),
      7B sends RFQ_QUOTES_READY at 0s.
    - If 0 execution quotes were received (acceptance_status == 'NO_QUOTES'),
      neither 7A nor 7B is sent; instead #8 RFQ_CLOSED_NO_QUOTES is sent.
    """
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(CLOSE_STATUSES + ("COMPLETED",)),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_end <= now,
        QuotationRequest.window_end > now - CATCH_UP,
    ).all()
    raised = 0
    for rfq in rfqs:
        if rfq.status in CLOSE_STATUSES:
            _evaluate_once("close", rfq, db, evaluate)
        if _already_notified(db, rfq.id, OUTCOME_CODES):
            continue

        if rfq.acceptance_status == "PENDING":
            deadline = _as_utc(rfq.acceptance_deadline)
            if deadline and deadline > _now():
                raised += _raise(db, EventCode.RFQ_QUOTES_READY, rfq)
        elif rfq.acceptance_status == "NO_QUOTES":
            raised += _raise(db, EventCode.RFQ_CLOSED_NO_QUOTES, rfq)
        elif rfq.acceptance_status == "INDICATIVE_COMPLETED":
            raised += _raise(db, EventCode.RFQ_RESULTS_READY, rfq)
    return raised


def _acceptance_expired(db: Session, now: datetime, evaluate: Callable) -> int:
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.acceptance_status.in_(("PENDING", "AUTO_ACCEPTED", "AUTO_REJECTED")),
        QuotationRequest.acceptance_deadline <= now,
        QuotationRequest.acceptance_deadline > now - CATCH_UP,
    ).all()
    raised = 0
    for rfq in rfqs:
        if _already_notified(db, rfq.id, [EventCode.RFQ_DEAL_AUTO_RESOLVED]):
            continue
        if rfq.acceptance_status == "PENDING":
            _evaluate_once("deadline", rfq, db, evaluate)
        if rfq.acceptance_status in ("AUTO_ACCEPTED", "AUTO_REJECTED"):
            db.query(UserNotification).filter(
                UserNotification.module == "QUOTATION",
                UserNotification.reference_key == str(rfq.id),
                UserNotification.event_type.in_(["RFQ_QUOTES_READY", "RFQ_CLOSING_SOON"]),
                UserNotification.is_read == False,
            ).update({"is_read": True, "expires_at": now}, synchronize_session=False)
        if rfq.acceptance_status == "AUTO_ACCEPTED":
            raised += _raise(db, EventCode.RFQ_DEAL_AUTO_RESOLVED, rfq, extra={"decision": "auto-accepted"})
        elif rfq.acceptance_status == "AUTO_REJECTED":
            raised += _raise(db, EventCode.RFQ_DEAL_AUTO_RESOLVED, rfq, extra={"decision": "auto-rejected"})
    return raised


def _inspect_rfq_activity(db: Session, now: datetime) -> Tuple[bool, bool]:
    """
    Fast inspection of candidate RFQs:
    Returns (has_work_now, is_active_or_approaching).
    - has_work_now: any RFQ needs one of the stages evaluated right now.
    - is_active_or_approaching: any RFQ is active or approaching its starting/closing/deadline window.
    """
    now = _as_utc(now) or _now()
    candidates = db.query(
        QuotationRequest.id,
        QuotationRequest.status,
        QuotationRequest.is_dispatched,
        QuotationRequest.dispatched_at,
        QuotationRequest.created_at,
        QuotationRequest.window_start,
        QuotationRequest.window_end,
        QuotationRequest.acceptance_status,
        QuotationRequest.acceptance_deadline,
    ).filter(
        or_(
            and_(
                QuotationRequest.is_dispatched == True,  # noqa: E712
                or_(
                    QuotationRequest.status.in_(LIVE_STATUSES),
                    QuotationRequest.window_end > now - CATCH_UP,
                ),
            ),
            and_(
                QuotationRequest.acceptance_status.in_(("PENDING", "AUTO_ACCEPTED", "AUTO_REJECTED")),
                QuotationRequest.acceptance_deadline > now - CATCH_UP,
            ),
        )
    ).all()

    if not candidates:
        return False, False

    has_work_now = False
    is_active_or_approaching = False
    approaching = now + APPROACHING_LEAD
    approaching_start_5m = now + STARTING_SOON_5M_LEAD + timedelta(seconds=IDLE_SWEEP_SECONDS)

    for r in candidates:
        w_start = _as_utc(r.window_start)
        w_end = _as_utc(r.window_end)
        acc_dl = _as_utc(r.acceptance_deadline)
        disp_time = _as_utc(r.dispatched_at or r.created_at)

        # Pre-start 5m due now?
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_start
            and disp_time
            and (w_start - disp_time) >= MIN_ADVANCE_LEAD
            and now < w_start <= now + STARTING_SOON_5M_LEAD
        ):
            has_work_now = True

        # Pre-start 1m due now?
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_start
            and w_end
            and disp_time
            and (w_start - disp_time) >= MIN_ADVANCE_LEAD
            and (w_end - w_start) <= SHORT_WINDOW_MAX_DURATION
            and now < w_start <= now + STARTING_SOON_1M_LEAD
        ):
            has_work_now = True

        # Stage 1: Closing soon due now?
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_start
            and w_end
            and w_start <= now < w_end <= now + CLOSING_SOON_LEAD
        ):
            has_work_now = True

        # Stage 2: Window closed due now?
        if (
            r.is_dispatched
            and r.status in (CLOSE_STATUSES + ("COMPLETED",))
            and w_end
            and now - CATCH_UP < w_end <= now
        ):
            if ("close", str(r.id)) not in _evaluated:
                has_work_now = True

        # Stage 3: Acceptance expired due now?
        if (
            r.acceptance_status in ("PENDING", "AUTO_ACCEPTED", "AUTO_REJECTED")
            and acc_dl
            and now - CATCH_UP < acc_dl <= now
        ):
            if ("deadline", str(r.id)) not in _evaluated:
                has_work_now = True

        # Approaching window start (within 5m + idle interval)
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_start
            and disp_time
            and (w_start - disp_time) >= MIN_ADVANCE_LEAD
            and now < w_start <= approaching_start_5m
        ):
            is_active_or_approaching = True

        # Approaching closing soon / window close
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_end
            and now < w_end <= approaching
        ):
            is_active_or_approaching = True

        # Live window open
        if (
            r.is_dispatched
            and r.status in LIVE_STATUSES
            and w_start
            and w_end
            and w_start <= now <= w_end
        ):
            if w_end <= approaching:
                is_active_or_approaching = True

        # Approaching or within acceptance deadline
        if (
            r.acceptance_status == "PENDING"
            and acc_dl
            and now < acc_dl <= approaching
        ):
            is_active_or_approaching = True

    if has_work_now:
        is_active_or_approaching = True

    return has_work_now, is_active_or_approaching


def sweep_once(db: Session, evaluate: Callable = _default_evaluate, now: Optional[datetime] = None) -> Dict[str, int]:
    """One pass over all stages. Each stage is isolated so one bad RFQ cannot block the others."""
    now = now or _now()
    stats = {
        "starting_soon_5m": 0,
        "starting_soon_1m": 0,
        "closing_soon": 0,
        "quotes_ready_10s": 0,
        "window_closed": 0,
        "auto_resolved": 0,
    }

    # Fast-path check: avoid stage queries when there is no work to do
    has_work_now, _ = _inspect_rfq_activity(db, now)
    if not has_work_now:
        _prune(now)
        return stats

    for name, stage in (("starting_soon_5m", lambda: _starting_soon_5m(db, now)),
                        ("starting_soon_1m", lambda: _starting_soon_1m(db, now)),
                        ("closing_soon", lambda: _closing_soon(db, now)),
                        ("quotes_ready_10s", lambda: _quotes_ready_10s(db, now)),
                        ("window_closed", lambda: _window_closed(db, now, evaluate)),
                        ("auto_resolved", lambda: _acceptance_expired(db, now, evaluate))):
        try:
            stats[name] = stage()
        except Exception:
            db.rollback()
            logger.exception("Timed quotation alerts: stage %s failed", name)
    _prune(now)
    return stats


def _adjust_interval(target_interval: int) -> None:
    global _current_interval
    if _scheduler is None or _current_interval == target_interval:
        return
    try:
        _scheduler.reschedule_job(SWEEP_JOB_ID, trigger="interval", seconds=target_interval)
        _current_interval = target_interval
        logger.debug("Quotation timer sweeper adjusted interval to %ss.", target_interval)
    except Exception:
        logger.debug("Could not reschedule quotation timer sweeper to %ss", target_interval)


def nudge_quotation_timers() -> None:
    """Wakes the quotation timer sweeper immediately (thread-safe, coalesced)."""
    if _scheduler is None:
        return
    try:
        _scheduler.add_job(
            run_quotation_timer_sweep, trigger="date", id=NUDGE_JOB_ID, replace_existing=True,
            misfire_grace_time=60, coalesce=True, max_instances=1,
        )
    except Exception:
        logger.debug("Could not schedule quotation timer nudge")


# ── After-commit nudge on RFQ state changes ───────────────────────────────────

@event.listens_for(QuotationRequest, "after_insert")
@event.listens_for(QuotationRequest, "after_update")
def _on_rfq_change(mapper, connection, target):
    session = Session.object_session(target)
    if session is not None and not session.info.get("_is_sweeper_session"):
        session.info[_SESSION_FLAG] = True


@event.listens_for(Session, "after_commit")
def _after_commit(session):
    if session.info.pop(_SESSION_FLAG, False):
        nudge_quotation_timers()


@event.listens_for(Session, "after_rollback")
def _after_rollback(session):
    session.info.pop(_SESSION_FLAG, None)


def _sweep_in_thread() -> Tuple[Dict[str, int], int]:
    if _session_factory is None:
        return {"closing_soon": 0, "window_closed": 0, "auto_resolved": 0}, IDLE_SWEEP_SECONDS
    db = _session_factory()
    db.info["_is_sweeper_session"] = True
    try:
        now = _now()
        has_work_now, is_active_or_approaching = _inspect_rfq_activity(db, now)
        target_interval = ACTIVE_SWEEP_SECONDS if is_active_or_approaching else IDLE_SWEEP_SECONDS
        if not has_work_now:
            _prune(now)
            return {"closing_soon": 0, "window_closed": 0, "auto_resolved": 0}, target_interval
        stats = sweep_once(db, now=now)
        return stats, target_interval
    finally:
        db.close()


async def run_quotation_timer_sweep() -> None:
    if _session_factory is None:
        return
    try:
        # compute_rfq_standings is synchronous (DB + optional market lookups): keep it off the event loop.
        stats, target_interval = await asyncio.to_thread(_sweep_in_thread)
        _adjust_interval(target_interval)
    except Exception:
        logger.exception("Timed quotation alerts sweep failed")


def init_quotation_timers(scheduler, session_factory) -> None:
    """Registers the sweeper. Starts at IDLE_SWEEP_SECONDS (30s) and adapts dynamically."""
    global _scheduler, _session_factory, _current_interval
    _scheduler = scheduler
    _session_factory = session_factory
    _current_interval = IDLE_SWEEP_SECONDS

    # Silence internal APScheduler executor tick messages
    logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

    scheduler.add_job(
        run_quotation_timer_sweep, trigger="interval", seconds=IDLE_SWEEP_SECONDS, id=SWEEP_JOB_ID,
        name="Timed quotation alerts (closing soon / quotes ready / auto-resolved)",
        replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=IDLE_SWEEP_SECONDS,
    )
    logger.info("Timed quotation alerts sweeper scheduled (adaptive idle: %ss, active: %ss).",
                IDLE_SWEEP_SECONDS, ACTIVE_SWEEP_SECONDS)
