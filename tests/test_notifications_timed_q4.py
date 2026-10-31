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
from app.models.models import Bank, User  # noqa: E402
from app.models.models_notification import UserNotification  # noqa: E402
from app.models.models_push import NotificationDelivery, PushSubscription  # noqa: E402
from app.models.models_quotation import QuotationBank, QuotationBankAssignment, QuotationRequest  # noqa: E402
from app.services.desk_session_service import desk_session_service  # noqa: E402
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


def _dealer_sub(db, email, bank_id=None):
    sub = PushSubscription(
        dealer_email=email,
        bank_id=bank_id,
        endpoint=f"https://fcm.googleapis.com/fcm/send/dealer-{uuid.uuid4().hex}",
        p256dh="BTestP256dhKeyValue000000",
        auth="TestAuthKey0",
        platform="android",
        is_active=True,
        is_deleted=False
    )
    db.add(sub)
    db.flush()
    return sub


def _asgn(db, rfq, dealer_email, bank_id=None):
    if bank_id is None:
        bank = db.query(Bank).first()
        bank_id = bank.id if bank else 1
    qb = QuotationBank(
        customer_id=rfq.customer_id,
        bank_id=bank_id,
        emails=dealer_email,
        contacts=[{"email": dealer_email, "name": "Dealer", "role": "EXECUTION"}]
    )
    db.add(qb)
    db.flush()
    asgn = QuotationBankAssignment(
        id=f"test-asgn-{uuid.uuid4()}",
        rfq_id=rfq.id,
        quotation_bank_id=qb.id,
        token=uuid.uuid4().hex,
    )
    db.add(asgn)
    db.flush()
    db.refresh(rfq)
    return asgn, qb


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
        admins = _users(db, maker.customer_id, UserRole.CORPORATE_ADMIN)
        expected_uids = {maker.id} | {a.id for a in admins}
        maker_sub = _sub(db, maker)
        other_sub = _sub(db, other)
        now = datetime.now(timezone.utc)
        rfq = _rfq(db, maker, now - timedelta(minutes=10), now + timedelta(seconds=90))

        qt.sweep_once(db, evaluate=_stub())
        qt.sweep_once(db, evaluate=_stub())
        rows = _alerts(db, rfq, EventCode.RFQ_CLOSING_SOON)
        assert {r.user_id for r in rows} == expected_uids
        maker_row = next(r for r in rows if r.user_id == maker.id)
        assert maker_row.urgency == "high"
        assert abs((maker_row.expires_at - rfq.window_end).total_seconds()) < 1, "must expire at window close"
        targeted = {d.subscription_id for d in db.query(NotificationDelivery).filter(
            NotificationDelivery.notification_id == maker_row.id)}
        assert maker_sub.id in targeted and other_sub.id not in targeted
    print("  [PASS] closing soon: once, maker + corporate admins, expires at close, queued to phone")


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
        admins = _users(db, maker.customer_id, UserRole.CORPORATE_ADMIN)
        expected_uids = {maker.id} | {a.id for a in admins}
        now = datetime.now(timezone.utc)

        winner = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3))
        ev = _stub(status="EVALUATING", acceptance_status="PENDING",
                   acceptance_deadline=now + timedelta(seconds=27))
        qt.sweep_once(db, evaluate=ev)
        qt.sweep_once(db, evaluate=ev)
        rows = _alerts(db, winner, EventCode.RFQ_QUOTES_READY)
        assert {r.user_id for r in rows} == expected_uids and ev.calls == [winner.id], "evaluate once, alert once"
        assert abs((rows[0].expires_at - winner.acceptance_deadline).total_seconds()) < 1

        empty = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3))
        qt.sweep_once(db, evaluate=_stub(status="COMPLETED", acceptance_status="NO_QUOTES"))
        assert len(_alerts(db, empty, EventCode.RFQ_CLOSED_NO_QUOTES)) == 1

        indicative = _rfq(db, maker, now - timedelta(minutes=20), now - timedelta(seconds=3),
                          quotation_base="Indicative")
        qt.sweep_once(db, evaluate=_stub(status="COMPLETED", acceptance_status="INDICATIVE_COMPLETED"))
        assert len(_alerts(db, indicative, EventCode.RFQ_RESULTS_READY)) == 1
    print("  [PASS] window close: quotes ready (7B, expires at deadline) / no quotes / indicative results, once each")


def test_quotes_ready_7a_at_minus_10s_and_mutual_exclusivity_with_7b():
    from app.models.models_quotation import QuotationOffer
    with _session() as db:
        maker, _ = _actors(db)
        admins = _users(db, maker.customer_id, UserRole.CORPORATE_ADMIN)
        expected_uids = {maker.id} | {a.id for a in admins}
        now = datetime.now(timezone.utc)

        # Case 1: Valid execution quote already received at T - 8s -> 7A fires at T - 8s, 7B skipped at T + 2s
        rfq_7a = _rfq(db, maker, now - timedelta(minutes=5), now + timedelta(seconds=8),
                      acceptance_timeout_seconds=30)
        asgn_7a, _ = _asgn(db, rfq_7a, "exec7a@bank.com")
        db.add(QuotationOffer(
            assignment_id=asgn_7a.id,
            price=50.15,
            submitted_by_email="exec7a@bank.com",
        ))
        db.flush()
        db.refresh(rfq_7a)
        db.refresh(asgn_7a)

        qt.sweep_once(db, now=now)
        rows_7a = _alerts(db, rfq_7a, EventCode.RFQ_QUOTES_READY)
        assert {r.user_id for r in rows_7a} == expected_uids, "7A must fire at T - 10s when a quote exists"

        # Now advance past window_end (T + 2s): _window_closed still evaluates, but 7B must NOT send a duplicate
        later = now + timedelta(seconds=10)
        ev_7a = _stub(status="EVALUATING", acceptance_status="PENDING",
                      acceptance_deadline=later + timedelta(seconds=28))
        qt.sweep_once(db, evaluate=ev_7a, now=later)
        rows_after_close = _alerts(db, rfq_7a, EventCode.RFQ_QUOTES_READY)
        assert len(rows_after_close) == len(rows_7a), "7B must not fire if 7A was already sent"
        assert ev_7a.calls == [rfq_7a.id], "RFQ standings evaluation must still run at window close"

        # Case 2: 0 quotes at T - 8s -> 7A does NOT fire
        rfq_no_quote = _rfq(db, maker, now - timedelta(minutes=5), now + timedelta(seconds=8))
        _asgn(db, rfq_no_quote, "silent@bank.com")
        qt.sweep_once(db, now=now)
        assert len(_alerts(db, rfq_no_quote, EventCode.RFQ_QUOTES_READY)) == 0, "7A must not fire when 0 quotes"
    print("  [PASS] 7A (T - 10s) fires with valid quote, suppresses 7B at 0s, and skips when 0 quotes")


def test_acceptance_deadline_auto_resolution():
    with _session() as db:
        maker, _ = _actors(db)
        admins = _users(db, maker.customer_id, UserRole.CORPORATE_ADMIN)
        expected_uids = {maker.id} | {a.id for a in admins}
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
        assert {r.user_id for r in rows} == expected_uids and "auto-accepted" in rows[0].title
        for forbidden in ("USD", "EGP", "12,500,000", "12500000"):
            assert forbidden not in rows[0].title + rows[0].message
    print("  [PASS] acceptance deadline: auto-resolution alert once (maker + corporate admins), no amounts")


def test_pre_start_5m_attendance_aware():
    with _session() as db:
        maker, _ = _actors(db)
        now = datetime.now(timezone.utc)
        # RFQ with lead time 24 minutes (>= 15m) and window_start in 4 minutes (<= 5m)
        rfq = _rfq(db, maker,
                   window_start=now + timedelta(minutes=4),
                   window_end=now + timedelta(minutes=14),
                   dispatched_at=now - timedelta(minutes=20),
                   status="PENDING",
                   is_dispatched=True)

        asgn1, qb1 = _asgn(db, rfq, "dealer1@bank1.com")
        asgn2, qb2 = _asgn(db, rfq, "dealer2@bank2.com")

        sub1 = _dealer_sub(db, "dealer1@bank1.com", qb1.bank_id)
        sub2 = _dealer_sub(db, "dealer2@bank2.com", qb2.bank_id)

        # Mark Bank 1 dealer as online/active
        desk_session_service.get_or_claim_desk(str(asgn1.id), "dealer1@bank1.com", "Dealer One")

        # 3. Bank 2 has ONLY a VIEW_ONLY spectator online -> reminder STILL ENQUEUED to execution dealer
        desk_session_service.get_or_claim_desk(str(asgn2.id), "viewer2@bank2.com", "Viewer Two", role="VIEW_ONLY")

        qt.sweep_once(db, now=now)
        qt.sweep_once(db, now=now)  # idempotent

        # 1. Maker + Delegate receives RFQ_STARTING_SOON
        maker_alerts = _alerts(db, rfq, EventCode.RFQ_STARTING_SOON)
        assert len(maker_alerts) == 1, "Maker must receive RFQ_STARTING_SOON"
        assert maker_alerts[0].user_id == maker.id

        # 2. Bank 1 has dealer online -> reminder IGNORED/SUPPRESSED
        delivs_bank1 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub1.id,
            NotificationDelivery.dedupe_key.like("%DEALER_STARTING_SOON%")
        ).all()
        assert len(delivs_bank1) == 0, "Online dealer must not receive 5m reminder"

        # 3. Bank 2 has NO execution dealer online (only VIEW_ONLY) -> reminder ENQUEUED
        delivs_bank2 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub2.id,
            NotificationDelivery.dedupe_key.like("%DEALER_STARTING_SOON%")
        ).all()
        assert len(delivs_bank2) == 1, "Offline execution dealer must receive DEALER_STARTING_SOON push even if viewer is online"
    print("  [PASS] pre-start 5m: maker notified, online dealers ignored, absent dealers reminded")


def test_pre_start_1m_attendance_and_maker_visibility():
    with _session() as db:
        maker, _ = _actors(db)
        now = datetime.now(timezone.utc)
        # RFQ with lead time 20m (>= 15m), short window (duration 5m <= 10m), window_start in 45s (<= 1m)
        rfq = _rfq(db, maker,
                   window_start=now + timedelta(seconds=45),
                   window_end=now + timedelta(minutes=5),
                   dispatched_at=now - timedelta(minutes=20),
                   status="PENDING",
                   is_dispatched=True)

        asgn1, qb1 = _asgn(db, rfq, "dealer1@bank1.com")
        asgn2, qb2 = _asgn(db, rfq, "dealer2@bank2.com")

        sub1 = _dealer_sub(db, "dealer1@bank1.com", qb1.bank_id)
        sub2 = _dealer_sub(db, "dealer2@bank2.com", qb2.bank_id)

        # Case A: Both banks have 0 dealers online
        qt.sweep_once(db, now=now)

        # 1. Urgent push enqueued to both absent bank dealers
        delivs_1 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub1.id,
            NotificationDelivery.dedupe_key.like("%DEALER_URGENT_STARTING_SOON%")
        ).all()
        delivs_2 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub2.id,
            NotificationDelivery.dedupe_key.like("%DEALER_URGENT_STARTING_SOON%")
        ).all()
        assert len(delivs_1) == 1 and len(delivs_2) == 1, "Both absent dealers receive urgent 1m push"

        # 2. Maker receives low attendance warning (0 of 2 invited banks checked in)
        warnings = _alerts(db, rfq, EventCode.RFQ_NO_DEALERS_CHECKED_IN)
        assert len(warnings) == 1, "Maker must receive RFQ_NO_DEALERS_CHECKED_IN warning"
        assert "0 of 2 invited banks" in warnings[0].message
        assert warnings[0].urgency == "high"

        # Case B: If an RFQ has at least 1 bank checked in, maker warning is suppressed
        rfq2 = _rfq(db, maker,
                    window_start=now + timedelta(seconds=45),
                    window_end=now + timedelta(minutes=5),
                    dispatched_at=now - timedelta(minutes=20),
                    status="PENDING",
                    is_dispatched=True)
        asgn3, qb3 = _asgn(db, rfq2, "dealer3@bank3.com")
        asgn4, qb4 = _asgn(db, rfq2, "dealer4@bank4.com")
        sub3 = _dealer_sub(db, "dealer3@bank3.com", qb3.bank_id)
        sub4 = _dealer_sub(db, "dealer4@bank4.com", qb4.bank_id)

        # Bank 3 is active/checked in!
        desk_session_service.get_or_claim_desk(str(asgn3.id), "dealer3@bank3.com", "Dealer Three")

        qt.sweep_once(db, now=now)

        # Maker does NOT get warning because 1 bank is checked in
        warnings2 = _alerts(db, rfq2, EventCode.RFQ_NO_DEALERS_CHECKED_IN)
        assert len(warnings2) == 0, "Maker must not be warned if at least 1 bank is checked in"

        # Bank 3 (checked in) does not get urgent push; Bank 4 (absent) does
        delivs_3 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub3.id,
            NotificationDelivery.dedupe_key.like("%DEALER_URGENT_STARTING_SOON%")
        ).all()
        delivs_4 = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub4.id,
            NotificationDelivery.dedupe_key.like("%DEALER_URGENT_STARTING_SOON%")
        ).all()
        assert len(delivs_3) == 0, "Checked-in bank must not get urgent 1m push"
        assert len(delivs_4) == 1, "Absent bank must get urgent 1m push"

    print("  [PASS] pre-start 1m: urgent dealer nudges sent, maker warned if 0 banks checked in")


def test_timed_texts_are_lock_screen_safe():
    rfq = QuotationRequest(id="x", ref_no="Q4-SAFE", amount=12_500_000.0, buy_currency="USD", sell_currency="EGP")
    timed = [
        EventCode.RFQ_STARTING_SOON,
        EventCode.RFQ_NO_DEALERS_CHECKED_IN,
        EventCode.RFQ_CLOSING_SOON,
        EventCode.RFQ_QUOTES_READY,
        EventCode.RFQ_RESULTS_READY,
        EventCode.RFQ_CLOSED_NO_QUOTES,
        EventCode.RFQ_DEAL_AUTO_RESOLVED
    ]
    for code in timed:
        spec = CATALOG[code]
        extra = {}
        if "decision" in spec.allowed_fields:
            extra["decision"] = "auto-rejected"
        if "invited_count" in spec.allowed_fields:
            extra["invited_count"] = "3"
        text = render(spec, rfq, extra if extra else None)
        blob = text["title"] + text["body"]
        assert "Q4-SAFE" in text["title"]
        for forbidden in ("USD", "EGP", "12,500,000", "12500000"):
            assert forbidden not in blob
    print(f"  [PASS] all {len(timed)} timed texts are lock-screen safe")


if __name__ == "__main__":
    print("=" * 70)
    print("NOTIFICATION PROGRAMME - PHASE Q4 VERIFICATION (TIMED ALERTS)")
    print("=" * 70)
    test_pre_start_5m_attendance_aware()
    test_pre_start_1m_attendance_and_maker_visibility()
    test_closing_soon_fires_once_to_maker_with_push()
    test_no_closing_soon_before_approval_or_live()
    test_window_close_outcomes()
    test_quotes_ready_7a_at_minus_10s_and_mutual_exclusivity_with_7b()
    test_acceptance_deadline_auto_resolution()
    test_timed_texts_are_lock_screen_safe()
    print("\n" + "=" * 70)
    print("ALL PHASE Q4 CHECKS PASSED")
    print("=" * 70)
