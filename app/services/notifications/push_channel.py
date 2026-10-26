# app/services/notifications/push_channel.py
"""
Web Push channel (Notifications Q3).

Flow:
  emit()  --stages-->  UserNotification + NotificationDelivery(PENDING)   (same DB transaction)
  commit  --nudge--->  dispatcher runs now (plus a 30s safety sweep)
  dispatcher: claim due rows (FOR UPDATE SKIP LOCKED) -> send outside the
              transaction (worker thread) -> record SENT / retry / FAILED / EXPIRED.

Request handlers never touch the network (release gate, Rule 8).
"""

import asyncio
import base64
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional
from urllib.parse import urlparse

from sqlalchemy import event
from sqlalchemy.orm import Session

from app.models.models_notification import UserNotification
from app.models.models_push import NotificationDelivery, PushSubscription
from app.services.notifications.push_config import get_vapid_config, push_enabled

logger = logging.getLogger(__name__)

# Push services we are willing to POST to (prevents users registering an
# arbitrary URL and turning the dispatcher into an SSRF proxy).
ALLOWED_PUSH_HOST_SUFFIXES = (
    "fcm.googleapis.com",          # Chrome / Edge on Android, Chrome desktop
    "push.services.mozilla.com",   # Firefox
    "push.apple.com",              # Safari / iOS home-screen apps (web.push.apple.com)
    "notify.windows.com",          # Edge on Windows (WNS)
)

RETRY_BACKOFF = (timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=30))
MAX_ATTEMPTS = len(RETRY_BACKOFF) + 1
MAX_TTL_SECONDS = 24 * 3600
DEFAULT_TTL_SECONDS = 24 * 3600
BATCH_SIZE = 100
STUCK_SENDING_AFTER = timedelta(minutes=2)
MAX_SUBSCRIPTIONS_PER_USER = 10

SWEEP_JOB_ID = "push_dispatch_sweep"
NUDGE_JOB_ID = "push_dispatch_nudge"
_SESSION_FLAG = "push_dispatch_pending"

_scheduler = None
_session_factory = None


# ── Validation helpers ──────────────────────────────────────────────────────

def is_allowed_endpoint(endpoint: str) -> bool:
    try:
        parsed = urlparse(endpoint)
    except Exception:
        return False
    if parsed.scheme != "https" or not parsed.hostname or len(endpoint) > 1024:
        return False
    host = parsed.hostname.lower()
    return any(host == s or host.endswith("." + s) for s in ALLOWED_PUSH_HOST_SUFFIXES)


def endpoint_fingerprint(endpoint: str) -> str:
    """Short non-reversible id so the browser can recognise 'this device'."""
    return hashlib.sha256(endpoint.encode("utf-8")).hexdigest()[:16]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ── Payload ─────────────────────────────────────────────────────────────────

def build_payload(notification: UserNotification) -> dict:
    """
    Lock-screen payload. Only the already-redacted title/body (rendered from
    whitelisted catalog templates), the in-app link and expiry are sent.
    """
    expires_at = _as_utc(notification.expires_at)
    tag = f"{(notification.module or 'GROW').lower()}-{notification.reference_key or notification.id}"
    return {
        "title": notification.title,
        "body": notification.message,
        "url": notification.link or "/app",
        "tag": tag,
        "urgency": notification.urgency or "normal",
        "expires_at": expires_at.isoformat() if expires_at else None,
    }


def _topic_for(tag: str) -> str:
    # Topic header: <= 32 chars from the URL-safe base64 alphabet. Lets the push
    # service replace an undelivered older alert for the same RFQ.
    digest = hashlib.sha256(tag.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:32]


# ── Enqueue (called from emit, inside the caller's transaction) ─────────────

def enqueue_push(db: Session, notifications: Iterable[UserNotification]) -> int:
    """Stages one PENDING delivery per (notification, active subscription)."""
    if not push_enabled():
        return 0
    notifications = list(notifications)
    if not notifications:
        return 0

    user_ids = {n.user_id for n in notifications}
    subs = db.query(PushSubscription).filter(
        PushSubscription.user_id.in_(user_ids),
        PushSubscription.is_active == True,  # noqa: E712
        PushSubscription.is_deleted == False,  # noqa: E712
    ).all()
    if not subs:
        return 0

    by_user = {}
    for sub in subs:
        by_user.setdefault(sub.user_id, []).append(sub)

    now = _now()
    count = 0
    for notif in notifications:
        for sub in by_user.get(notif.user_id, []):
            db.add(NotificationDelivery(
                notification=notif,
                subscription_id=sub.id,
                user_id=notif.user_id,
                channel="PUSH",
                status="PENDING",
                attempts=0,
                next_attempt_at=now,
                expires_at=_as_utc(notif.expires_at),
            ))
            count += 1
    if count:
        db.info[_SESSION_FLAG] = True
    return count


# ── After-commit nudge ──────────────────────────────────────────────────────

@event.listens_for(Session, "after_commit")
def _after_commit(session):
    if session.info.pop(_SESSION_FLAG, False):
        nudge_dispatcher()


@event.listens_for(Session, "after_rollback")
def _after_rollback(session):
    session.info.pop(_SESSION_FLAG, None)


def nudge_dispatcher() -> None:
    """Ask the scheduler to run the dispatcher now (thread-safe, coalesced)."""
    if _scheduler is None:
        return  # tests / scripts: the 30s sweep or an explicit call handles it
    try:
        _scheduler.add_job(
            run_push_dispatch, trigger="date", id=NUDGE_JOB_ID, replace_existing=True,
            misfire_grace_time=60, coalesce=True, max_instances=1,
        )
    except Exception:
        logger.exception("Web Push: could not schedule dispatcher nudge (the 30s sweep will pick it up)")


def init_push_dispatcher(scheduler, session_factory) -> None:
    """Registers the 30s safety sweep. Called once at startup."""
    global _scheduler, _session_factory
    _scheduler = scheduler
    _session_factory = session_factory
    if not push_enabled():
        return
    scheduler.add_job(
        run_push_dispatch, trigger="interval", seconds=30, id=SWEEP_JOB_ID,
        name="Web Push dispatcher (30s sweep)", replace_existing=True,
        coalesce=True, max_instances=1, misfire_grace_time=30,
    )
    logger.info("Web Push dispatcher scheduled every 30s.")


# ── Sending ─────────────────────────────────────────────────────────────────

class SendResult:
    OK = "OK"
    GONE = "GONE"            # 404/410: subscription is dead
    PERMANENT = "PERMANENT"  # 400/403/413: our request is wrong; don't retry
    RETRY = "RETRY"          # 429/5xx/network


def send_web_push(sub: PushSubscription, payload: dict, ttl: int, urgency: str, topic: str):
    """Blocking send (run in a worker thread). Returns (SendResult, error_text)."""
    from pywebpush import WebPushException, webpush

    config = get_vapid_config()
    if config is None:
        return SendResult.RETRY, "push disabled"
    try:
        webpush(
            subscription_info={"endpoint": sub.endpoint, "keys": {"p256dh": sub.p256dh, "auth": sub.auth}},
            data=json.dumps(payload, separators=(",", ":")),
            vapid_private_key=config.private_key,
            vapid_claims={"sub": config.subject},
            ttl=ttl,
            headers={"Urgency": urgency if urgency in ("very-low", "low", "normal", "high") else "normal",
                     "Topic": topic},
            timeout=10,
        )
        return SendResult.OK, None
    except WebPushException as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        text = f"HTTP {status}: {str(exc)[:200]}"
        if status in (404, 410):
            return SendResult.GONE, text
        if status in (400, 403, 413):
            return SendResult.PERMANENT, text
        return SendResult.RETRY, text
    except Exception as exc:  # network errors, timeouts
        return SendResult.RETRY, f"{type(exc).__name__}: {str(exc)[:200]}"


def _claim_due(db: Session, now: datetime) -> List[int]:
    # Recover rows stuck in SENDING (process died mid-send).
    db.query(NotificationDelivery).filter(
        NotificationDelivery.status == "SENDING",
        NotificationDelivery.updated_at < now - STUCK_SENDING_AFTER,
    ).update({"status": "PENDING"}, synchronize_session=False)

    rows = (
        db.query(NotificationDelivery)
        .filter(NotificationDelivery.status == "PENDING", NotificationDelivery.next_attempt_at <= now)
        .order_by(NotificationDelivery.id)
        .limit(BATCH_SIZE)
        .with_for_update(skip_locked=True)
        .all()
    )
    claimed = []
    for row in rows:
        expires = _as_utc(row.expires_at)
        if expires and expires <= now:
            row.status = "EXPIRED"
            row.last_error = "expired before delivery"
            continue
        row.status = "SENDING"
        row.attempts = (row.attempts or 0) + 1
        row.updated_at = now
        claimed.append(row.id)
    db.commit()
    return claimed


def _record_result(db: Session, delivery: NotificationDelivery, result: str, error: Optional[str], now: datetime):
    sub = delivery.subscription
    if result == SendResult.OK:
        delivery.status = "SENT"
        delivery.sent_at = now
        delivery.last_error = None
        sub.last_success_at = now
        sub.failure_count = 0
        return

    delivery.last_error = (error or "")[:255]
    sub.last_failure_at = now
    sub.last_error = (error or "")[:255]
    sub.failure_count = (sub.failure_count or 0) + 1

    if result == SendResult.GONE:
        delivery.status = "FAILED"
        sub.is_active = False
        sub.revoked_reason = "GONE"
        # No point sending anything else to this endpoint.
        db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub.id,
            NotificationDelivery.status == "PENDING",
        ).update({"status": "FAILED", "last_error": "subscription gone"}, synchronize_session=False)
        logger.info("Web Push: subscription %s is gone; deactivated.", sub.id)
        return

    if result == SendResult.PERMANENT or delivery.attempts >= MAX_ATTEMPTS:
        delivery.status = "FAILED"
        logger.error("Web Push: delivery %s failed permanently: %s", delivery.id, error)
        return

    next_at = now + RETRY_BACKOFF[min(delivery.attempts, len(RETRY_BACKOFF)) - 1]
    expires = _as_utc(delivery.expires_at)
    if expires and next_at >= expires:
        delivery.status = "EXPIRED"
        return
    delivery.status = "PENDING"
    delivery.next_attempt_at = next_at
    logger.warning("Web Push: delivery %s will retry at %s (%s)", delivery.id, next_at.isoformat(), error)


async def dispatch_once(db: Session, sender=send_web_push) -> dict:
    """One dispatcher pass. `sender` is injectable for tests."""
    stats = {"claimed": 0, "sent": 0, "retry": 0, "failed": 0, "gone": 0}
    if not push_enabled():
        return stats
    now = _now()
    claimed = _claim_due(db, now)
    stats["claimed"] = len(claimed)

    for delivery_id in claimed:
        delivery = db.get(NotificationDelivery, delivery_id)
        if delivery is None:
            continue
        sub = delivery.subscription
        notif = delivery.notification
        if sub is None or not sub.is_active or notif is None:
            delivery.status = "FAILED"
            delivery.last_error = "subscription inactive"
            db.commit()
            stats["failed"] += 1
            continue

        payload = build_payload(notif)
        expires = _as_utc(delivery.expires_at)
        ttl = DEFAULT_TTL_SECONDS if not expires else max(0, min(MAX_TTL_SECONDS, int((expires - _now()).total_seconds())))
        result, error = await asyncio.to_thread(
            sender, sub, payload, ttl, payload["urgency"], _topic_for(payload["tag"])
        )
        _record_result(db, delivery, result, error, _now())
        db.commit()

        if result == SendResult.OK:
            stats["sent"] += 1
        elif result == SendResult.GONE:
            stats["gone"] += 1
        elif delivery.status == "PENDING":
            stats["retry"] += 1
        else:
            stats["failed"] += 1
    return stats


async def run_push_dispatch() -> None:
    """Scheduler entry point (sweep + nudge)."""
    if _session_factory is None:
        return
    db = _session_factory()
    try:
        stats = await dispatch_once(db)
        if stats["claimed"]:
            logger.info("Web Push dispatch: %s", stats)
    except Exception:
        db.rollback()
        logger.exception("Web Push dispatcher pass failed")
    finally:
        db.close()
