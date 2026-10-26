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

from sqlalchemy import exists
from sqlalchemy.orm import Session

from app.models.models_notification import UserNotification
from app.models.models_quotation import QuotationRequest
from app.services.notifications.catalog import EventCode
from app.services.notifications.emit import emit

logger = logging.getLogger(__name__)

SWEEP_JOB_ID = "quotation_timed_alerts_sweep"
SWEEP_SECONDS = 5
CLOSING_SOON_LEAD = timedelta(minutes=2)
CATCH_UP = timedelta(minutes=30)      # how far back a missed alert is still worth raising
LIVE_STATUSES = ("PENDING", "OPEN")
CLOSE_STATUSES = ("PENDING", "OPEN", "EVALUATING")
OUTCOME_CODES = (EventCode.RFQ_QUOTES_READY, EventCode.RFQ_RESULTS_READY, EventCode.RFQ_CLOSED_NO_QUOTES)

_scheduler = None
_session_factory = None
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


# ── Stages ──────────────────────────────────────────────────────────────────

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
        raised += _raise(db, EventCode.RFQ_CLOSING_SOON, rfq)
    return raised


def _window_closed(db: Session, now: datetime, evaluate: Callable) -> int:
    rfqs = db.query(QuotationRequest).filter(
        QuotationRequest.status.in_(CLOSE_STATUSES + ("COMPLETED",)),
        QuotationRequest.is_dispatched == True,  # noqa: E712
        QuotationRequest.window_end <= now,
        QuotationRequest.window_end > now - CATCH_UP,
    ).all()
    raised = 0
    for rfq in rfqs:
        if _already_notified(db, rfq.id, OUTCOME_CODES):
            continue
        if rfq.status in CLOSE_STATUSES:
            _evaluate_once("close", rfq, db, evaluate)

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
        if rfq.acceptance_status == "AUTO_ACCEPTED":
            raised += _raise(db, EventCode.RFQ_DEAL_AUTO_RESOLVED, rfq, extra={"decision": "auto-accepted"})
        elif rfq.acceptance_status == "AUTO_REJECTED":
            raised += _raise(db, EventCode.RFQ_DEAL_AUTO_RESOLVED, rfq, extra={"decision": "auto-rejected"})
    return raised


def sweep_once(db: Session, evaluate: Callable = _default_evaluate, now: Optional[datetime] = None) -> Dict[str, int]:
    """One pass over all stages. Each stage is isolated so one bad RFQ cannot block the others."""
    now = now or _now()
    stats = {"closing_soon": 0, "window_closed": 0, "auto_resolved": 0}
    for name, stage in (("closing_soon", lambda: _closing_soon(db, now)),
                        ("window_closed", lambda: _window_closed(db, now, evaluate)),
                        ("auto_resolved", lambda: _acceptance_expired(db, now, evaluate))):
        try:
            stats[name] = stage()
        except Exception:
            db.rollback()
            logger.exception("Timed quotation alerts: stage %s failed", name)
    _prune(now)
    return stats


def _sweep_in_thread() -> Dict[str, int]:
    db = _session_factory()
    try:
        return sweep_once(db)
    finally:
        db.close()


async def run_quotation_timer_sweep() -> None:
    if _session_factory is None:
        return
    try:
        # compute_rfq_standings is synchronous (DB + optional market lookups): keep it off the event loop.
        await asyncio.to_thread(_sweep_in_thread)
    except Exception:
        logger.exception("Timed quotation alerts sweep failed")


def init_quotation_timers(scheduler, session_factory) -> None:
    """Registers the sweeper. Called once at startup."""
    global _scheduler, _session_factory
    _scheduler = scheduler
    _session_factory = session_factory
    scheduler.add_job(
        run_quotation_timer_sweep, trigger="interval", seconds=SWEEP_SECONDS, id=SWEEP_JOB_ID,
        name="Timed quotation alerts (closing soon / quotes ready / auto-resolved)",
        replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=SWEEP_SECONDS,
    )
    logger.info("Timed quotation alerts sweeper scheduled every %ss.", SWEEP_SECONDS)
