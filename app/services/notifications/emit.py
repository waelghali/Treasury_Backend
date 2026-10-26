# app/services/notifications/emit.py
"""
Single entry point for raising notification events.

    emit(db, EventCode.RFQ_APPROVED, rfq, actor_user_id=current_user.user_id)

Contract:
  * Rows are added to the caller's session and commit/roll back atomically
    with the business change. emit() never commits.
  * Programming errors (unknown code, bad placeholders) raise immediately so
    they surface in tests instead of failing silently in production.
  * Runtime failures while resolving recipients are logged at ERROR with the
    event code and never block the business action.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.constants import UserRole
from app.models.models_notification import UserNotification
from app.models.models_quotation import QuotationRequest
from app.services.notifications.audiences import resolve_rfq_audience
from app.services.notifications.catalog import (
    EventSpec,
    ExpiryPolicy,
    LinkTarget,
    get_spec,
)
from app.services.notifications.push_channel import enqueue_push

logger = logging.getLogger(__name__)


def _role_path(role) -> str:
    value = role.value if isinstance(role, UserRole) else str(role or "")
    return "corporate-admin" if value in ("corporate_admin", "viewer") else "end-user"


def build_link(target: LinkTarget, rfq: QuotationRequest, recipient_role) -> str:
    role_path = _role_path(recipient_role)
    if target == LinkTarget.RFQ_RESULTS:
        return f"/{role_path}/quotations/results/{rfq.id}"
    if target == LinkTarget.RFQ_APPROVALS:
        return "/corporate-admin/quotations/approvals"
    if target == LinkTarget.RFQ_ACTIVE:
        return "/corporate-admin/quotations" if role_path == "corporate-admin" else "/end-user/quotations/active"
    if target == LinkTarget.RFQ_HISTORY:
        return "/corporate-admin/quotations" if role_path == "corporate-admin" else "/end-user/quotations/history"
    raise ValueError(f"Unsupported link target: {target}")


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def compute_expiry(spec: EventSpec, rfq: QuotationRequest, now: datetime) -> datetime:
    if spec.expiry == ExpiryPolicy.UNTIL_WINDOW_START:
        w_start, w_end = _as_utc(rfq.window_start), _as_utc(rfq.window_end)
        if w_start and w_start > now:
            return w_start
        if w_end and w_end > now:
            return w_end
        return now + timedelta(hours=1)
    if spec.expiry == ExpiryPolicy.UNTIL_WINDOW_END:
        w_end = _as_utc(rfq.window_end)
        return w_end if w_end and w_end > now else now + timedelta(minutes=1)
    if spec.expiry == ExpiryPolicy.UNTIL_ACCEPTANCE_DEADLINE:
        deadline = _as_utc(rfq.acceptance_deadline)
        return deadline if deadline and deadline > now else now + timedelta(minutes=1)
    return now + timedelta(hours=24)


def render(spec: EventSpec, rfq: QuotationRequest, extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Renders title/body from the safe context only. Raises on unexpected fields."""
    extra = extra or {}
    unexpected = set(extra) - set(spec.allowed_fields)
    if unexpected:
        raise ValueError(f"{spec.code.value}: fields not allowed in notification text: {sorted(unexpected)}")
    context = {"ref_no": rfq.ref_no or "—", **{k: str(v) for k, v in extra.items()}}
    missing = [f for f in spec.allowed_fields if f not in context]
    if missing:
        raise ValueError(f"{spec.code.value}: missing render fields {missing}")
    return {"title": spec.title.format_map(context), "body": spec.body.format_map(context)}


def emit(
    db: Session,
    code,
    rfq: QuotationRequest,
    actor_user_id: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Raises a quotation notification event. Returns the number of in-app
    notifications staged in the caller's transaction.
    """
    spec = get_spec(code)              # raises ValueError for unknown codes
    text = render(spec, rfq, extra)    # raises ValueError for bad placeholders

    try:
        recipients = resolve_rfq_audience(db, rfq, spec.audiences, actor_user_id=actor_user_id)
        if not recipients:
            logger.info("Notification %s for RFQ %s: no recipients after scoping", spec.code.value, rfq.id)
            return 0

        now = datetime.now(timezone.utc)
        expires_at = compute_expiry(spec, rfq, now)
        rows = [
            UserNotification(
                user_id=user_id,
                module=spec.module,
                event_type=spec.code.value,
                title=text["title"],
                message=text["body"],
                link=build_link(spec.link, rfq, role),
                actor_user_id=actor_user_id,
                reference_type="QuotationRequest",
                reference_key=str(rfq.id),
                urgency=spec.urgency.value,
                expires_at=expires_at,
            )
            for user_id, role in recipients.items()
        ]
        db.add_all(rows)
        try:
            pushes = enqueue_push(db, rows)
        except Exception:
            pushes = 0
            logger.exception("Notification %s for RFQ %s: push enqueue failed (in-app kept)", spec.code.value, rfq.id)
        logger.info(
            "Notification %s staged for RFQ %s -> %d recipient(s), %d push delivery(ies)",
            spec.code.value, rfq.id, len(rows), pushes,
        )
        return len(rows)
    except Exception:
        logger.exception("Notification %s for RFQ %s failed during recipient resolution", spec.code.value, rfq.id)
        return 0
