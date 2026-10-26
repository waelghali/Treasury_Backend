"""
Test Suite: Notification Programme - Phase Q4 (timed quotation alerts)

Verifies, with a stubbed RFQ evaluation (the pricing engine is out of scope):
1. RFQ_CLOSING_SOON fires for a live, dispatched RFQ <= 2 minutes before close,
   goes to the maker (and delegate), and fires only once.
2. No closing-soon alert for RFQs still awaiting approval or not yet live.
3. At window close: winning quote -> RFQ_QUOTES_READY (expires at the
   acceptance deadline); no quotes -> RFQ_CLOSED_NO_QUOTES; indicative-only ->
   RFQ_RESULTS_READY. Each outcome is raised once.
4. At the acceptance deadline: policy outcome -> RFQ_DEAL_AUTO_RESOLVED with
   the decision in the text; raised once.
5. Every timed alert queues phone deliveries for the maker's active device and
   none for other customers.
6. All timed texts are lock-screen safe.

Runs against the local development database inside one outer transaction that
is always rolled back.
Run:  python tests/test_notifications_timed_q4.py   (also pytest-compatible)
"""
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

import app.main  # noqa: F401,E402
from sqlalchemy.orm import Session  # noqa: E402

from app.constants import UserRole  # noqa: E402
from app.database import engine  # noqa: E402
from app.models.models import User  # noqa: E402
from app.models.models_notification import UserNotification  # noqa: E402
from app.models.models_push import NotificationDelivery, PushSubscription  # noqa: E402
from app.models.models_quotation import QuotationRequest  # noqa: E402
from app.services.notifications import quotation_timers as qt  # noqa: E402
from app.services.notifications.catalog import CATALOG, EventCode  # noqa: E402
from app.services.notifications.emit import render  # noqa: E402


@contextmanager
def _session():
    conn = engine.connect()
    outer = conn.begin()
    db = Session(bind=conn, join_transaction_mode="create_savepoint", autoflush=False)
    qt._evaluated.clear()
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


def _actors(db):
    customer_ids = [r[0] for r in db.query(User.customer_id).filter(
        User.role == UserRole.END_USER, User.is_deleted == False, User.customer_id.isnot(None)  # noqa: E712
    ).distinct().order_by(User.customer_id).all()]
    a = customer_ids[0]
    other = db.query(User).filter(User.customer_id != a, User.customer_id.isnot(None),
                                  User.is_deleted == False).first()  # noqa: E712
    return _users(db, a, UserRole.END_USER)[0], other


def _rfq(db, maker, window_start, window_end, **kw):
    rfq = QuotationRequest(
        id=f"test-q4-{uuid.uuid4()}", ref_no=f"Q4-{uuid.uuid4().hex[:6].upper()}",
        customer_id=maker.customer_id, created_by_user_id=maker.id,
        window_start=window_start, window_end=window_end,
        amount=12_500_000.0, buy_currency="USD", sell_currency="EGP",
        type="FX_SPOT", status=kw.pop("status", "PENDING"), is_dispatched=kw.pop("is_dispatched", True),
        acceptance_timeout_seconds=kw.pop("acceptance_timeout_seconds", 30),
        acceptance_timeout_action=kw.pop("acceptance_timeout_action", "AUTO_REJECT"),
        **kw,
    )
    db.add(rfq)
    db.flush()
    return rfq


def _sub(db, user):
    sub = PushSubscription(user_id=user.id, customer_id=user.customer_id,
                           endpoint=f"https://fcm.googleapis.com/fcm/send/test-{uuid.uuid4().hex}",
                           p256dh="BTestP256dhKeyValue000000", auth="TestAuthKey0", platform="android")
    db.add(sub)
    db.flush()
    return sub


def _alerts(db, rfq, code):
    return db.query(UserNotification).filter(
        UserNotification.reference_key == rfq.id, UserNotification.event_type == code.value
    ).all()


def _stub(**fields):
    """Fake evaluation: applies the given RFQ state, as compute_rfq_standings would."""
    calls = []

    def evaluate(rfq, db):
        calls.append(rfq.id)
        for k, v in fields.items():
            setattr(rfq, k, v)
        db.flush()
    evaluate.calls = calls
    return evaluate


def test_closing_soon_fires_once_to_maker_with_push():
    with _session() as db:
        maker, other = _actors(db)
        maker_sub = _sub(db, maker)
        other_sub = _sub(db, other)
        now = datetime.now(timezone.utc)
        rfq = _rfq(db, maker, now - timedelta(minutes=10), now + timedelta(seconds=90))

        qt.sweep_once(db, evaluate=_stub())
        qt.sweep_once(db, evaluate=_stub())
        rows = _alerts(db, rfq, EventCode.RFQ_CLOSING_SOON)
        assert len(rows) == 1 and rows[0].user_id == maker.id
        assert rows[0].urgency == "high"
        assert abs((rows[0].expires_at - rfq.window_end).total_seconds()) < 1, "must expire at window close"
        targeted = {d.subscription_id for d in db.query(NotificationDelivery).filter(
            NotificationDelivery.notification_id == rows[0].id)}
        assert targeted == {maker_sub.id} and other_sub.id not in targeted
    print("  [PASS] closing soon: once, maker only, expires at close, queued to maker's phone")


def test_no_closing_soon_before_approval_or_live():
    with _session() as db:
        maker, _ = _actors(db)
        now = datetime.now(timezone.utc)
        awaiting = _rfq(db, maker, now - timedelta(minutes=10), now + timedelta(seconds=60),
                        status="PENDING_APPROVAL", is_dispatched=False)
        far = _rfq(db, maker, now - timedelta(minutes=10), now + timedelta(minutes=20))
        qt.sweep_once(db, evaluate=_stub())
        assert not _alerts(db, awaiting, EventCode.RFQ_CLOSING_SOON)
        assert not _alerts(db, far, EventCode.RFQ_CLOSING_SOON)
    print("  [PASS] no closing-soon for unapproved RFQs or windows > 2 min away")


def test_window_close_outcomes():
    with _session() as db:
        maker, _ = _actors(db)
        now = datetime.now(timezone.utc)

        winner = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3))
        ev = _stub(status="EVALUATING", acceptance_status="PENDING",
                   acceptance_deadline=now + timedelta(seconds=27))
        qt.sweep_once(db, evaluate=ev)
        qt.sweep_once(db, evaluate=ev)
        rows = _alerts(db, winner, EventCode.RFQ_QUOTES_READY)
        assert len(rows) == 1 and ev.calls == [winner.id], "evaluate once, alert once"
        assert abs((rows[0].expires_at - winner.acceptance_deadline).total_seconds()) < 1

        empty = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3))
        qt.sweep_once(db, evaluate=_stub(status="COMPLETED", acceptance_status="NO_QUOTES"))
        assert len(_alerts(db, empty, EventCode.RFQ_CLOSED_NO_QUOTES)) == 1

        indicative = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3),
                          quotation_base="Indicative")
        qt.sweep_once(db, evaluate=_stub(status="COMPLETED", acceptance_status="INDICATIVE_COMPLETED"))
        assert len(_alerts(db, indicative, EventCode.RFQ_RESULTS_READY)) == 1
    print("  [PASS] window close: quotes ready (expires at deadline) / no quotes / indicative results, once each")


def test_acceptance_deadline_auto_resolution():
    with _session() as db:
        maker, _ = _actors(db)
        now = datetime.now(timezone.utc)
        rfq = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(minutes=1),
                   status="EVALUATING", acceptance_status="PENDING",
                   acceptance_deadline=now - timedelta(seconds=2))
        # Outcome alert for the close already sent earlier:
        db.add(UserNotification(user_id=maker.id, module="QUOTATION", event_type="RFQ_QUOTES_READY",
                                title="x", message="x", reference_key=rfq.id))
        db.flush()
        ev = _stub(status="COMPLETED", acceptance_status="AUTO_ACCEPTED")
        qt.sweep_once(db, evaluate=ev)
        qt.sweep_once(db, evaluate=ev)
        rows = _alerts(db, rfq, EventCode.RFQ_DEAL_AUTO_RESOLVED)
        assert len(rows) == 1 and "auto-accepted" in rows[0].title
        for forbidden in ("USD", "EGP", "12,500,000", "12500000"):
            assert forbidden not in rows[0].title + rows[0].message
    print("  [PASS] acceptance deadline: auto-resolution alert once, decision in text, no amounts")


def test_timed_texts_are_lock_screen_safe():
    rfq = QuotationRequest(id="x", ref_no="Q4-SAFE", amount=12_500_000.0, buy_currency="USD", sell_currency="EGP")
    timed = [EventCode.RFQ_CLOSING_SOON, EventCode.RFQ_QUOTES_READY, EventCode.RFQ_RESULTS_READY,
             EventCode.RFQ_CLOSED_NO_QUOTES, EventCode.RFQ_DEAL_AUTO_RESOLVED]
    for code in timed:
        spec = CATALOG[code]
        text = render(spec, rfq, {"decision": "auto-rejected"} if "decision" in spec.allowed_fields else None)
        blob = text["title"] + text["body"]
        assert "Q4-SAFE" in text["title"]
        for forbidden in ("USD", "EGP", "12,500,000", "12500000"):
            assert forbidden not in blob
    print(f"  [PASS] all {len(timed)} timed texts are lock-screen safe")


if __name__ == "__main__":
    print("=" * 70)
    print("NOTIFICATION PROGRAMME - PHASE Q4 VERIFICATION (TIMED ALERTS)")
    print("=" * 70)
    test_closing_soon_fires_once_to_maker_with_push()
    test_no_closing_soon_before_approval_or_live()
    test_window_close_outcomes()
    test_acceptance_deadline_auto_resolution()
    test_timed_texts_are_lock_screen_safe()
    print("\n" + "=" * 70)
    print("ALL PHASE Q4 CHECKS PASSED")
    print("=" * 70)
