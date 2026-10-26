# app/api/v1/endpoints/notification_endpoints.py
"""
General-purpose notification API.
Serves UserNotification records for the bell icon across all modules.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.database import get_db
from app.core.security import get_current_active_user, TokenData
from app.models.models_notification import UserNotification
from app.models.models_push import PushSubscription
from app.services.notifications.push_channel import (
    MAX_SUBSCRIPTIONS_PER_USER,
    endpoint_fingerprint,
    enqueue_push,
    is_allowed_endpoint,
)
from app.services.notifications.push_config import get_vapid_config

router = APIRouter()


@router.get("/")
def get_my_notifications(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Fetches the 30 most recent notifications for the logged-in user."""
    return db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id
    ).order_by(desc(UserNotification.created_at)).limit(30).all()


@router.get("/unread-count")
def get_unread_count(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns the count of unread notifications for badge display."""
    count = db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id,
        UserNotification.is_read == False
    ).count()
    return {"count": count}


@router.patch("/{notification_id}/read")
def mark_notification_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks a specific notification as read."""
    notif = db.query(UserNotification).filter(
        UserNotification.id == notification_id,
        UserNotification.user_id == current_user.user_id
    ).first()
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found")
    notif.is_read = True
    db.commit()
    return {"message": "Notification marked as read"}


@router.patch("/mark-all-read")
def mark_all_as_read(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks all notifications as read for the current user."""
    db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id,
        UserNotification.is_read == False
    ).update({"is_read": True})
    db.commit()
    return {"message": "All notifications marked as read"}


# ── Web Push (mobile alerts) — Notifications Q3 ─────────────────────────────────
# Every endpoint below works on the caller's own subscriptions only.

_PLATFORMS = {"ios", "android", "desktop"}


class PushKeys(BaseModel):
    p256dh: str = Field(..., min_length=10, max_length=255)
    auth: str = Field(..., min_length=8, max_length=255)


class PushSubscribeIn(BaseModel):
    endpoint: str = Field(..., max_length=1024)
    keys: PushKeys
    device_id: Optional[str] = Field(None, max_length=64)
    device_label: Optional[str] = Field(None, max_length=120)
    platform: Optional[str] = Field(None, max_length=20)


class PushUnsubscribeIn(BaseModel):
    endpoint: str = Field(..., max_length=1024)


class PushTestIn(BaseModel):
    endpoint: Optional[str] = Field(None, max_length=1024, description="Limit the test to this device")


def _device_out(sub: PushSubscription) -> dict:
    return {
        "id": sub.id,
        "device_label": sub.device_label,
        "platform": sub.platform,
        "fingerprint": endpoint_fingerprint(sub.endpoint),
        "created_at": sub.created_at,
        "last_success_at": sub.last_success_at,
        "last_failure_at": sub.last_failure_at,
    }


def _active_subs_query(db: Session, user_id: int):
    return db.query(PushSubscription).filter(
        PushSubscription.user_id == user_id,
        PushSubscription.is_active == True,  # noqa: E712
        PushSubscription.is_deleted == False,  # noqa: E712
    )


@router.get("/push/public-key")
def get_push_public_key(current_user: TokenData = Depends(get_current_active_user)):
    """VAPID public key the browser needs to subscribe. enabled=false if push is off on this server."""
    config = get_vapid_config()
    return {"enabled": config is not None, "public_key": config.public_key if config else None}


@router.post("/push/subscribe")
def subscribe_push(
    payload: PushSubscribeIn,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Registers (or refreshes) this browser for the current user."""
    if get_vapid_config() is None:
        raise HTTPException(status_code=503, detail="Mobile alerts are not enabled on this server.")
    if not is_allowed_endpoint(payload.endpoint):
        raise HTTPException(status_code=400, detail="Unsupported push service.")

    platform = (payload.platform or "").lower()
    platform = platform if platform in _PLATFORMS else None
    sub = db.query(PushSubscription).filter(PushSubscription.endpoint == payload.endpoint).first()

    if sub is None:
        if _active_subs_query(db, current_user.user_id).count() >= MAX_SUBSCRIPTIONS_PER_USER:
            raise HTTPException(status_code=409, detail="Too many devices. Remove an old device first.")
        sub = PushSubscription(endpoint=payload.endpoint)
        db.add(sub)
    elif sub.user_id != current_user.user_id:
        # Same browser, different person signed in: the browser now belongs to
        # the caller; the previous owner stops receiving alerts here.
        sub.revoked_reason = "REASSIGNED"

    sub.user_id = current_user.user_id
    sub.customer_id = current_user.customer_id
    sub.p256dh = payload.keys.p256dh
    sub.auth = payload.keys.auth
    sub.device_id = payload.device_id
    sub.device_label = payload.device_label
    sub.platform = platform
    sub.device_type = platform or "web"
    sub.is_active = True
    sub.is_deleted = False
    sub.failure_count = 0
    sub.last_error = None
    db.commit()
    db.refresh(sub)
    return _device_out(sub)


@router.post("/push/unsubscribe")
def unsubscribe_push(
    payload: PushUnsubscribeIn,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Turns off alerts for this browser (used on sign-out and by the toggle)."""
    sub = _active_subs_query(db, current_user.user_id).filter(PushSubscription.endpoint == payload.endpoint).first()
    if sub:
        sub.is_active = False
        sub.revoked_reason = "SIGNED_OUT"
        db.commit()
    return {"message": "Mobile alerts turned off for this device"}


@router.get("/push/devices")
def list_push_devices(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    subs = _active_subs_query(db, current_user.user_id).order_by(desc(PushSubscription.created_at)).all()
    return [_device_out(s) for s in subs]


@router.delete("/push/devices/{device_id}")
def remove_push_device(
    device_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Removes one of the caller's devices (e.g. a lost phone)."""
    sub = _active_subs_query(db, current_user.user_id).filter(PushSubscription.id == device_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="Device not found")
    sub.is_active = False
    sub.revoked_reason = "USER_REMOVED"
    db.commit()
    return {"message": "Device removed"}


@router.post("/push/test")
def send_test_push(
    payload: PushTestIn,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Queues a test alert to the caller's devices (or just one). Sent by the dispatcher."""
    if get_vapid_config() is None:
        raise HTTPException(status_code=503, detail="Mobile alerts are not enabled on this server.")

    now = datetime.now(timezone.utc)
    recent = db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id,
        UserNotification.event_type == "PUSH_TEST",
        UserNotification.created_at >= now - timedelta(seconds=10),
    ).first()
    if recent:
        raise HTTPException(status_code=429, detail="Please wait a few seconds before sending another test.")

    subs_q = _active_subs_query(db, current_user.user_id)
    if payload.endpoint:
        subs_q = subs_q.filter(PushSubscription.endpoint == payload.endpoint)
    if subs_q.count() == 0:
        raise HTTPException(status_code=404, detail="No device with mobile alerts enabled.")

    notif = UserNotification(
        user_id=current_user.user_id,
        module="SYSTEM",
        event_type="PUSH_TEST",
        title="Grow test alert",
        message="Mobile alerts are working on this device.",
        link="/app",
        is_read=True,  # informational only; don't add to the bell badge
        reference_type="PushTest",
        reference_key=f"test-{current_user.user_id}",
        urgency="high",
        expires_at=now + timedelta(minutes=10),
    )
    db.add(notif)
    queued = enqueue_push(db, [notif], only_subscription_ids={s.id for s in subs_q.all()})
    db.commit()
    return {"queued": queued}
