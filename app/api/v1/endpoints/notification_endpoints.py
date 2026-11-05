# app/api/v1/endpoints/notification_endpoints.py
"""
General-purpose notification API.
Serves UserNotification records for the bell icon across all modules.
"""

import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, func

from app.database import get_db
from app.core.security import get_current_active_user, get_client_ip, TokenData
from app.models.models_notification import UserNotification
from app.models.models_push import PushSubscription, UserMobilePairToken, UserWebAuthnCredential
from app.core.webauthn_service import build_registration_options, resolve_rp_id
from app.services.notifications.push_channel import (
    MAX_SUBSCRIPTIONS_PER_USER,
    endpoint_fingerprint,
    is_allowed_endpoint,
)
from app.services.notifications.push_config import get_vapid_config
from app.core.routing import get_frontend_base_url
from app.core.hashing import verify_password
from app.services.dealer_auth_service import generate_qr_code_base64

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


def _device_out(sub: PushSubscription, has_biometric: bool = False) -> dict:
    return {
        "id": sub.id,
        "device_id": sub.device_id,
        "device_label": sub.device_label,
        "platform": sub.platform,
        "fingerprint": endpoint_fingerprint(sub.endpoint),
        "has_biometric": has_biometric,
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
def get_push_public_key():
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

    is_already_active = (
        sub is not None
        and sub.user_id == current_user.user_id
        and sub.is_active
        and not sub.is_deleted
    )
    if not is_already_active and _active_subs_query(db, current_user.user_id).count() >= MAX_SUBSCRIPTIONS_PER_USER:
        oldest = _active_subs_query(db, current_user.user_id).order_by(PushSubscription.created_at.asc()).first()
        if oldest:
            oldest.is_active = False
            oldest.revoked_reason = "ADMIN_REVOKED"

    if sub is None:
        sub = PushSubscription(endpoint=payload.endpoint)
        db.add(sub)
    elif sub.user_id != current_user.user_id:
        # Same browser, different person signed in: the browser now belongs to
        # the caller; the previous owner stops receiving alerts here.
        sub.revoked_reason = "REASSIGNED"

    sub.user_id = current_user.user_id
    sub.customer_id = current_user.customer_id
    sub.dealer_email = None
    sub.dealer_id = None
    sub.bank_id = None
    sub.p256dh = payload.keys.p256dh
    sub.auth = payload.keys.auth
    sub.device_id = payload.device_id
    sub.device_label = payload.device_label
    sub.platform = platform
    sub.device_type = platform or "web"
    if sub.revoked_reason == "SIGNED_OUT":
        sub.revoked_reason = None
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


class PushRotateIn(BaseModel):
    old_endpoint: str = Field(..., max_length=1024)
    new_endpoint: str = Field(..., max_length=1024)
    keys: PushKeys


@router.post("/push/rotate")
def rotate_push_subscription(
    payload: PushRotateIn,
    db: Session = Depends(get_db),
):
    """
    Self-healing background token rotation triggered by Service Worker 'pushsubscriptionchange'.
    Verifies possession of the active old_endpoint and updates it to the rotated new_endpoint + keys.
    Works for both logged-in browsers and QR-paired mobile devices.
    """
    if not is_allowed_endpoint(payload.new_endpoint):
        raise HTTPException(status_code=400, detail="Unsupported push service.")

    sub = db.query(PushSubscription).filter(
        PushSubscription.endpoint == payload.old_endpoint,
        PushSubscription.is_active == True,  # noqa: E712
        PushSubscription.is_deleted == False,  # noqa: E712
    ).first()
    if not sub:
        raise HTTPException(status_code=404, detail="Active subscription not found for rotation.")

    # Check if new_endpoint already exists to avoid unique constraint collision
    existing_new = db.query(PushSubscription).filter(
        PushSubscription.endpoint == payload.new_endpoint,
        PushSubscription.id != sub.id,
    ).first()
    if existing_new:
        db.delete(existing_new)
        db.flush()

    sub.endpoint = payload.new_endpoint
    sub.p256dh = payload.keys.p256dh
    sub.auth = payload.keys.auth
    sub.failure_count = 0
    sub.last_error = None
    db.commit()
    return {"rotated": True, "fingerprint": endpoint_fingerprint(sub.endpoint)}


@router.get("/push/devices")
def list_push_devices(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    subs = _active_subs_query(db, current_user.user_id).order_by(desc(PushSubscription.created_at)).all()
    bio_device_ids = {
        c.device_id
        for c in db.query(UserWebAuthnCredential).filter(
            UserWebAuthnCredential.user_id == current_user.user_id,
            UserWebAuthnCredential.is_active == True,  # noqa: E712
            UserWebAuthnCredential.is_deleted == False,  # noqa: E712
            UserWebAuthnCredential.device_id.isnot(None),
        ).all()
    }
    return [_device_out(s, has_biometric=bool(s.device_id and s.device_id in bio_device_ids)) for s in subs]


@router.delete("/push/devices/{device_id}")
def remove_push_device(
    device_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Removes one of the caller's devices (e.g. a lost phone) and revokes any linked biometric login."""
    sub = _active_subs_query(db, current_user.user_id).filter(PushSubscription.id == device_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="Device not found")
    sub.is_active = False
    sub.revoked_reason = "USER_REMOVED"
    if sub.device_id:
        db.query(UserWebAuthnCredential).filter(
            UserWebAuthnCredential.user_id == current_user.user_id,
            UserWebAuthnCredential.device_id == sub.device_id,
            UserWebAuthnCredential.is_active == True,  # noqa: E712
        ).update({"is_active": False}, synchronize_session=False)
    db.commit()
    return {"message": "Device removed"}


# ── Desktop-to-Mobile QR Pairing with Password Verification (Zero-Trust) ────────

class MobilePairInitiateOut(BaseModel):
    pairing_token: str
    setup_url: str
    qr_code_base64: str
    expires_in_seconds: int


class MobilePairAuthorizeIn(BaseModel):
    password: str = Field(..., min_length=1)
    endpoint: str = Field(..., max_length=1024)
    keys: PushKeys
    device_id: Optional[str] = Field(None, max_length=64)
    device_label: Optional[str] = Field(None, max_length=120)
    platform: Optional[str] = Field(None, max_length=20)
    rp_id: Optional[str] = Field(None, max_length=255)


@router.post("/push/mobile-pair/initiate", response_model=MobilePairInitiateOut)
def initiate_user_mobile_pair(
    request: Request,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """
    Generates a secure 15-minute QR pairing session for an End User or Corporate Admin.
    """
    token_str = secrets.token_hex(24)
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=15)

    pair_rec = UserMobilePairToken(
        pairing_token=token_str,
        user_id=current_user.user_id,
        expires_at=expires_at,
        is_paired=False,
    )
    db.add(pair_rec)
    db.commit()

    base_url = get_frontend_base_url(request=request).rstrip("/")
    setup_url = f"{base_url}/mobile-pair?token={token_str}"
    qr_b64 = generate_qr_code_base64(setup_url)

    return {
        "pairing_token": token_str,
        "setup_url": setup_url,
        "qr_code_base64": qr_b64,
        "expires_in_seconds": 900,
    }


@router.get("/push/mobile-pair/status/{pairing_token}")
def get_user_mobile_pair_status(
    pairing_token: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
):
    """Live status check polled by desktop QR modal while waiting for phone authorization."""
    clean_token = (pairing_token or "").strip().replace(" ", "")
    pair_rec = db.query(UserMobilePairToken).filter(
        or_(
            UserMobilePairToken.pairing_token == clean_token,
            func.lower(UserMobilePairToken.pairing_token) == func.lower(clean_token),
        ),
        UserMobilePairToken.user_id == current_user.user_id,
    ).first()
    if not pair_rec:
        raise HTTPException(status_code=404, detail="Pairing session not found.")

    return {
        "is_paired": bool(pair_rec.is_paired),
        "device_label": pair_rec.device_label,
        "platform": pair_rec.platform,
    }


@router.get("/push/mobile-pair/{pairing_token}/info")
def get_user_mobile_pair_info(
    pairing_token: str,
    db: Session = Depends(get_db),
):
    """
    Public info endpoint called by phone browser after scanning the desktop QR code.
    Identifies the user account without returning sensitive data or secrets.
    """
    clean_token = (pairing_token or "").strip().replace(" ", "")
    now = datetime.now(timezone.utc)
    pair_rec = db.query(UserMobilePairToken).filter(
        or_(
            UserMobilePairToken.pairing_token == clean_token,
            func.lower(UserMobilePairToken.pairing_token) == func.lower(clean_token),
        ),
        UserMobilePairToken.expires_at > now,
    ).first()
    if not pair_rec:
        raise HTTPException(status_code=404, detail="This pairing QR code has expired or is invalid. Please generate a fresh QR code on your computer.")

    from app.models import User
    user = db.query(User).filter(User.id == pair_rec.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User account not found.")

    company_name = user.customer.name if getattr(user, "customer", None) else "Grow Treasury"
    full_name = getattr(user, "full_name", None) or getattr(user, "name", None) or user.email
    vapid_cfg = get_vapid_config()

    return {
        "email": user.email,
        "full_name": full_name,
        "company_name": company_name,
        "is_paired": bool(pair_rec.is_paired),
        "vapid_public_key": vapid_cfg.public_key if vapid_cfg else None,
    }


@router.post("/push/mobile-pair/{pairing_token}/authorize")
def authorize_user_mobile_pair(
    pairing_token: str,
    payload: MobilePairAuthorizeIn,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public authorization endpoint called by phone when user enters password.
    Blocks the 'unlocked laptop' threat: only the actual account holder knowing the password can pair.
    Also marks the mobile device as trusted and returns WebAuthn biometric registration options.
    """
    clean_token = (pairing_token or "").strip().replace(" ", "")
    now = datetime.now(timezone.utc)
    pair_rec = db.query(UserMobilePairToken).filter(
        or_(
            UserMobilePairToken.pairing_token == clean_token,
            func.lower(UserMobilePairToken.pairing_token) == func.lower(clean_token),
        ),
        UserMobilePairToken.expires_at > now,
    ).first()
    if not pair_rec:
        raise HTTPException(status_code=404, detail="This pairing QR code has expired or is invalid. Please generate a fresh QR code on your computer.")

    if not is_allowed_endpoint(payload.endpoint):
        raise HTTPException(status_code=400, detail="Unsupported push endpoint.")

    from app.models import User, UserDevice
    user = db.query(User).filter(User.id == pair_rec.user_id).first()
    pwd_hash = getattr(user, "password_hash", None) or getattr(user, "hashed_password", None)
    if not user or not pwd_hash:
        raise HTTPException(status_code=404, detail="User account not found.")

    # PASSWORD VERIFICATION: Prevents unauthorized physical walk-up
    if not verify_password(payload.password, pwd_hash):
        raise HTTPException(status_code=401, detail="Incorrect password. Please enter your Grow corporate password to authorize this device.")

    platform = (payload.platform or "").lower()
    platform = platform if platform in _PLATFORMS else "desktop"

    # Locate existing subscription by endpoint globally (to prevent unique constraint violation)
    sub = db.query(PushSubscription).filter(PushSubscription.endpoint == payload.endpoint).first()

    # Enforce device limit per user if this subscription is not already active for this user
    is_already_active_for_user = (
        sub is not None
        and sub.user_id == user.id
        and sub.is_active
        and not sub.is_deleted
    )
    if not is_already_active_for_user:
        active_subs = _active_subs_query(db, user.id).order_by(PushSubscription.created_at.asc()).all()
        if len(active_subs) >= MAX_SUBSCRIPTIONS_PER_USER:
            oldest = active_subs[0]
            oldest.is_active = False
            oldest.revoked_reason = "ADMIN_REVOKED"

    if sub is None:
        sub = PushSubscription(endpoint=payload.endpoint)
        db.add(sub)
    elif sub.user_id != user.id:
        sub.revoked_reason = "REASSIGNED"

    sub.user_id = user.id
    sub.customer_id = user.customer_id
    sub.dealer_email = None
    sub.dealer_id = None
    sub.bank_id = None
    sub.p256dh = payload.keys.p256dh
    sub.auth = payload.keys.auth
    sub.device_id = payload.device_id
    sub.device_label = payload.device_label or f"{platform.capitalize()} Device"
    sub.platform = platform
    sub.device_type = platform
    sub.is_active = True
    sub.is_deleted = False
    sub.revoked_reason = None
    sub.failure_count = 0
    sub.last_error = None

    # Trust this mobile device for MFA since the user verified their password on the paired phone
    if payload.device_id:
        device_rec = db.query(UserDevice).filter(
            UserDevice.user_id == user.id,
            UserDevice.device_id == payload.device_id,
        ).first()
        if not device_rec:
            device_rec = UserDevice(
                user_id=user.id,
                device_id=payload.device_id,
                device_name=sub.device_label,
                is_trusted=True,
                last_ip=get_client_ip(request),
            )
            db.add(device_rec)
        else:
            device_rec.is_trusted = True
            device_rec.device_name = sub.device_label
            device_rec.last_ip = get_client_ip(request)

    pair_rec.is_paired = True
    pair_rec.paired_at = now
    pair_rec.device_label = sub.device_label
    pair_rec.platform = platform
    db.commit()

    # Pre-generate WebAuthn registration options so the phone can enroll Face ID / Fingerprint in one tap
    rp_id = resolve_rp_id(request, payload.rp_id)
    existing_creds = [
        c.credential_id
        for c in db.query(UserWebAuthnCredential).filter(
            UserWebAuthnCredential.user_id == user.id,
            UserWebAuthnCredential.is_active == True,  # noqa: E712
            UserWebAuthnCredential.is_deleted == False,  # noqa: E712
        ).all()
    ]
    full_name = getattr(user, "full_name", None) or getattr(user, "name", None) or user.email
    webauthn_options = build_registration_options(
        user_id=user.id,
        user_email=user.email,
        user_display_name=full_name,
        rp_id=rp_id,
        existing_credential_ids=existing_creds,
    )

    return {
        "success": True,
        "message": f"Device authorized successfully! Phone alerts are now active for {user.email}.",
        "user_id": user.id,
        "user_email": user.email,
        "device_label": sub.device_label,
        "webauthn_registration": webauthn_options,
    }



