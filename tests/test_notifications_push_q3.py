"""
Test Suite: Notification Programme - Phase Q3 (Web Push channel)

Verifies:
1. Only real push services are accepted as endpoints (SSRF guard).
2. emit() queues phone deliveries only for the recipients' own active devices
   (cross-customer isolation, actor excluded, inactive devices skipped).
3. Push payload is lock-screen safe and carries only the whitelisted keys.
4. Dispatcher: success -> SENT; 404/410 -> device deactivated; 5xx -> retry
   with backoff; expired alerts are never sent.
5. Endpoints are own-data only: unsubscribe/remove cannot touch another user's
   device; the same browser re-subscribed by another user moves to that user.

Runs against the local development database inside one outer transaction that
is always rolled back (dispatcher/endpoint commits become savepoints).
Run:  python tests/test_notifications_push_q3.py   (also pytest-compatible)
"""
import asyncio
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

import app.main  # noqa: F401,E402  (registers all models / mappers)
from fastapi import HTTPException  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.api.v1.endpoints import notification_endpoints as ep  # noqa: E402
from app.constants import UserRole  # noqa: E402
from app.core.security import TokenData  # noqa: E402
from app.database import engine  # noqa: E402
from app.models.models import User  # noqa: E402
from app.models.models_notification import UserNotification  # noqa: E402
from app.models.models_push import NotificationDelivery, PushSubscription  # noqa: E402
from app.models.models_quotation import QuotationRequest  # noqa: E402
from app.services.notifications import EventCode, emit  # noqa: E402
from app.services.notifications.push_channel import (  # noqa: E402
    SendResult,
    build_payload,
    dispatch_once,
    is_allowed_endpoint,
)
from app.services.notifications.push_config import push_enabled  # noqa: E402


# ─── Helpers ─────────────────────────────────────────────────────────────────

@contextmanager
def _session():
    conn = engine.connect()
    outer = conn.begin()
    db = Session(bind=conn, join_transaction_mode="create_savepoint", autoflush=False)
    try:
        yield db
    finally:
        db.close()
        outer.rollback()
        conn.close()


def _users(db, customer_id, role):
    return db.query(User).filter(
        User.customer_id == customer_id, User.role == role, User.is_deleted == False  # noqa: E712
    ).order_by(User.id).all()


def _tenants(db):
    customer_ids = [r[0] for r in db.query(User.customer_id).filter(
        User.role == UserRole.CORPORATE_ADMIN, User.is_deleted == False, User.customer_id.isnot(None)  # noqa: E712
    ).distinct().order_by(User.customer_id).all()]
    a = next((c for c in customer_ids if _users(db, c, UserRole.END_USER)), None)
    b = next((c for c in customer_ids if c != a), None)
    assert a is not None and b is not None, "Local DB needs two customers with corporate admins."
    return {
        "a": a, "b": b,
        "a_admins": _users(db, a, UserRole.CORPORATE_ADMIN),
        "a_maker": _users(db, a, UserRole.END_USER)[0],
        "b_admin": _users(db, b, UserRole.CORPORATE_ADMIN)[0],
    }


def _endpoint():
    return f"https://fcm.googleapis.com/fcm/send/test-{uuid.uuid4().hex}"


def _sub(db, user, active=True):
    sub = PushSubscription(
        user_id=user.id, customer_id=user.customer_id, endpoint=_endpoint(),
        p256dh="BTestP256dhKeyValue000000", auth="TestAuthKey0", platform="android",
        is_active=active,
    )
    db.add(sub)
    db.flush()
    return sub


def _token(user):
    return TokenData(user_id=user.id, role=user.role, customer_id=user.customer_id)


def _rfq(customer_id, maker_id):
    now = datetime.now(timezone.utc)
    return QuotationRequest(
        id=f"test-q3-{uuid.uuid4()}", ref_no=f"Q3-TEST-{uuid.uuid4().hex[:6].upper()}",
        customer_id=customer_id, created_by_user_id=maker_id,
        window_start=now + timedelta(hours=2), window_end=now + timedelta(hours=2, minutes=15),
        amount=12_500_000.0, buy_currency="USD", sell_currency="EGP",
    )


def _deliveries_for(db, rfq):
    return db.query(NotificationDelivery).join(UserNotification).filter(
        UserNotification.reference_key == rfq.id
    ).all()


def _expect_http(status, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except HTTPException as exc:
        assert exc.status_code == status, f"expected HTTP {status}, got {exc.status_code}"
        return
    raise AssertionError(f"expected HTTP {status}")


# ─── Tests ───────────────────────────────────────────────────────────────────

def test_push_is_configured_locally():
    assert push_enabled(), "Set VAPID_* in .env to run the Q3 suite"
    print("  [PASS] VAPID configuration loads and validates")


def test_endpoint_allowlist():
    good = [
        "https://fcm.googleapis.com/fcm/send/abc",
        "https://web.push.apple.com/QH4abc",
        "https://updates.push.services.mozilla.com/wpush/v2/abc",
        "https://db5p.notify.windows.com/w/?token=abc",
    ]
    bad = [
        "http://fcm.googleapis.com/fcm/send/abc",
        "https://fcm.googleapis.com.evil.com/x",
        "https://evilfcm.googleapis.com/x",
        "https://localhost/x",
        "https://169.254.169.254/latest/meta-data",
        "not a url",
    ]
    assert all(is_allowed_endpoint(u) for u in good)
    assert not any(is_allowed_endpoint(u) for u in bad)
    print("  [PASS] only known push services accepted (SSRF guard)")


def test_emit_queues_only_recipient_devices():
    with _session() as db:
        t = _tenants(db)
        admin_subs = [_sub(db, a) for a in t["a_admins"]]
        extra_admin_sub = _sub(db, t["a_admins"][0])          # second phone
        inactive = _sub(db, t["a_admins"][0], active=False)   # removed phone
        maker_sub = _sub(db, t["a_maker"])                    # actor
        other_customer_sub = _sub(db, t["b_admin"])           # other tenant

        rfq = _rfq(t["a"], t["a_maker"].id)
        emit(db, EventCode.RFQ_APPROVAL_REQUIRED, rfq, actor_user_id=t["a_maker"].id)
        db.flush()

        targeted = {d.subscription_id for d in _deliveries_for(db, rfq)}
        expected = {s.id for s in admin_subs} | {extra_admin_sub.id}
        assert expected <= targeted, "every admin device must be queued"
        # Real devices enabled in the dev DB may also be targeted - but only recipients' own:
        admin_ids = {a.id for a in t["a_admins"]}
        owners = {s.user_id for s in db.query(PushSubscription).filter(PushSubscription.id.in_(targeted))}
        assert owners <= admin_ids, "push queued for a non-recipient"
        assert inactive.id not in targeted
        assert maker_sub.id not in targeted, "actor must not get a push"
        assert other_customer_sub.id not in targeted, "cross-customer push leak!"
        for d in _deliveries_for(db, rfq):
            assert d.status == "PENDING" and d.expires_at is not None
    print("  [PASS] pushes go only to recipients' active devices (isolation, actor, inactive)")


def test_payload_is_lock_screen_safe():
    with _session() as db:
        t = _tenants(db)
        _sub(db, t["a_admins"][0])
        rfq = _rfq(t["a"], t["a_maker"].id)
        emit(db, EventCode.RFQ_APPROVAL_REQUIRED, rfq, actor_user_id=t["a_maker"].id)
        db.flush()
        notif = db.query(UserNotification).filter(UserNotification.reference_key == rfq.id).first()
        payload = build_payload(notif)
        assert set(payload) == {"title", "body", "url", "tag", "urgency", "expires_at"}
        blob = f"{payload['title']} {payload['body']}"
        for forbidden in ("USD", "EGP", "12500000", "12,500,000"):
            assert forbidden not in blob
        assert payload["url"].startswith("/")
        assert payload["tag"] == f"quotation-{rfq.id}"
    print("  [PASS] payload has only whitelisted keys and no amounts/currencies")


def _dispatch(db, result_for):
    calls = []

    def fake_sender(sub, payload, ttl, urgency, topic):
        calls.append((sub.id, payload, ttl, urgency, topic))
        return result_for(sub)

    stats = asyncio.run(dispatch_once(db, sender=fake_sender))
    return stats, calls


def test_dispatch_success_gone_retry_expired():
    with _session() as db:
        t = _tenants(db)
        ok_sub, gone_sub, flaky_sub = (_sub(db, t["a_admins"][0]) for _ in range(3))
        outcomes = {ok_sub.id: (SendResult.OK, None),
                    gone_sub.id: (SendResult.GONE, "HTTP 410"),
                    flaky_sub.id: (SendResult.RETRY, "HTTP 503")}

        rfq = _rfq(t["a"], t["a_maker"].id)
        emit(db, EventCode.RFQ_APPROVAL_REQUIRED, rfq, actor_user_id=t["a_maker"].id)
        db.flush()
        mine = {d.subscription_id: d for d in _deliveries_for(db, rfq)}

        # An alert that is already stale must never be sent (separate device keeps
        # the unique (notification, subscription) pair valid).
        spare = _sub(db, t["a_admins"][0])
        expired = NotificationDelivery(notification=mine[ok_sub.id].notification, subscription_id=spare.id,
                                       user_id=spare.user_id, status="PENDING",
                                       next_attempt_at=datetime.now(timezone.utc),
                                       expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        db.add(expired)
        db.flush()

        _, calls = _dispatch(db, lambda sub: outcomes.get(sub.id, (SendResult.OK, None)))
        for d in list(mine.values()) + [expired]:
            db.refresh(d)
        db.refresh(gone_sub)
        db.refresh(ok_sub)

        assert mine[ok_sub.id].status == "SENT" and ok_sub.last_success_at is not None
        assert mine[gone_sub.id].status == "FAILED" and gone_sub.is_active is False and gone_sub.revoked_reason == "GONE"
        assert mine[flaky_sub.id].status == "PENDING" and mine[flaky_sub.id].attempts == 1
        assert mine[flaky_sub.id].next_attempt_at > datetime.now(timezone.utc) + timedelta(seconds=30)
        assert expired.status == "EXPIRED"
        assert spare.id not in {c[0] for c in calls}, "expired alert was sent"

        sent = next(c for c in calls if c[0] == ok_sub.id)
        assert 0 < sent[2] <= 24 * 3600, "TTL must follow the alert expiry"
        assert sent[3] == "high" and len(sent[4]) <= 32
    print("  [PASS] dispatcher: SENT / 410 deactivates device / 503 retries / expired never sent")


def test_endpoints_are_own_data_only():
    with _session() as db:
        t = _tenants(db)
        alice, bob = t["a_admins"][0], t["b_admin"]
        endpoint = _endpoint()
        body = ep.PushSubscribeIn(endpoint=endpoint, keys=ep.PushKeys(p256dh="BTestP256dhKeyValue000000", auth="TestAuthKey0"),
                                  device_label="Test phone", platform="ios")

        out = ep.subscribe_push(body, db=db, current_user=_token(alice))
        assert out["fingerprint"] and "endpoint" not in out

        # Bob cannot remove or unsubscribe Alice's device.
        _expect_http(404, ep.remove_push_device, out["id"], db=db, current_user=_token(bob))
        ep.unsubscribe_push(ep.PushUnsubscribeIn(endpoint=endpoint), db=db, current_user=_token(bob))
        sub = db.get(PushSubscription, out["id"])
        assert sub.is_active and sub.user_id == alice.id
        assert all(d["id"] != out["id"] for d in ep.list_push_devices(db=db, current_user=_token(bob)))

        # Bad endpoint rejected.
        bad = ep.PushSubscribeIn(endpoint="https://169.254.169.254/x", keys=body.keys)
        _expect_http(400, ep.subscribe_push, bad, db=db, current_user=_token(alice))

        # Same browser, Bob signs in and enables alerts -> device moves to Bob.
        ep.subscribe_push(body, db=db, current_user=_token(bob))
        db.refresh(sub)
        assert sub.user_id == bob.id and sub.customer_id == bob.customer_id
        assert all(d["id"] != sub.id for d in ep.list_push_devices(db=db, current_user=_token(alice)))

        # Bob signs out -> inactive.
        ep.unsubscribe_push(ep.PushUnsubscribeIn(endpoint=endpoint), db=db, current_user=_token(bob))
        db.refresh(sub)
        assert sub.is_active is False and sub.revoked_reason == "SIGNED_OUT"
    print("  [PASS] endpoints own-data only; shared browser moves owner; sign-out deactivates")


if __name__ == "__main__":
    print("=" * 70)
    print("NOTIFICATION PROGRAMME - PHASE Q3 VERIFICATION (WEB PUSH)")
    print("=" * 70)
    test_push_is_configured_locally()
    test_endpoint_allowlist()
    test_emit_queues_only_recipient_devices()
    test_payload_is_lock_screen_safe()
    test_dispatch_success_gone_retry_expired()
    test_endpoints_are_own_data_only()
    print("\n" + "=" * 70)
    print("ALL PHASE Q3 CHECKS PASSED")
    print("=" * 70)
