"""
Test Suite: Notification Programme — Phase Q1 (quotation notification core, in-app channel)

Verifies:
1. Role audiences resolve only to users of the RFQ's customer (cross-customer isolation).
2. The actor is never notified about their own action.
3. RFQ_DELEGATE resolves to nobody when no delegate is set; out-of-scope delegates are dropped.
4. Unknown event codes and unexpected template fields raise immediately (no silent failure).
5. Rendered text is lock-screen safe: no amounts, currencies or rates.
6. Notifications are staged in the caller's transaction: a rollback leaves no rows.
7. Deep links are role-aware.

Runs against the local development database. Every test rolls back; nothing is committed.
Run:  python tests/test_notifications_quotation_q1.py   (also pytest-compatible)
"""
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.main  # noqa: F401,E402  (registers all models / mappers)
from app.constants import UserRole  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.models.models import User  # noqa: E402
from app.models.models_notification import UserNotification  # noqa: E402
from app.models.models_quotation import QuotationRequest  # noqa: E402
from app.services.notifications import EventCode, emit  # noqa: E402
from app.services.notifications.audiences import AudienceKey, resolve_rfq_audience  # noqa: E402
from app.services.notifications.catalog import CATALOG, get_spec  # noqa: E402
from app.services.notifications.emit import build_link, render  # noqa: E402


# ─── Helpers ─────────────────────────────────────────────────────────────────

@contextmanager
def _session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        db.close()


def _assert_raises(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type:
        return
    raise AssertionError(f"Expected {exc_type.__name__} to be raised")


def _users(db, customer_id, role):
    return db.query(User).filter(
        User.customer_id == customer_id, User.role == role, User.is_deleted == False  # noqa: E712
    ).all()


def _tenants(db):
    """Two customers that each have a corporate admin; customer A also has an end user."""
    customer_ids = [r[0] for r in db.query(User.customer_id).filter(
        User.role == UserRole.CORPORATE_ADMIN, User.is_deleted == False, User.customer_id.isnot(None)  # noqa: E712
    ).distinct().order_by(User.customer_id).all()]
    a = next((c for c in customer_ids if _users(db, c, UserRole.END_USER)), None)
    b = next((c for c in customer_ids if c != a), None)
    assert a is not None and b is not None, \
        "Local DB needs two customers with corporate admins (one with an end user)."
    return {
        "a": a,
        "b": b,
        "a_admins": _users(db, a, UserRole.CORPORATE_ADMIN),
        "a_maker": _users(db, a, UserRole.END_USER)[0],
        "b_admins": _users(db, b, UserRole.CORPORATE_ADMIN),
    }


def _transient_rfq(customer_id, maker_id, **kw):
    now = datetime.now(timezone.utc)
    return QuotationRequest(
        id=f"test-q1-{uuid.uuid4()}",
        ref_no=f"Q1-TEST-{uuid.uuid4().hex[:6].upper()}",
        customer_id=customer_id,
        created_by_user_id=maker_id,
        window_start=now + timedelta(hours=2),
        window_end=now + timedelta(hours=2, minutes=15),
        amount=12_500_000.0,
        buy_currency="USD",
        sell_currency="EGP",
        **kw,
    )


# ─── Tests ───────────────────────────────────────────────────────────────────

def test_admin_audience_is_customer_scoped():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
        recipients = resolve_rfq_audience(db, rfq, [AudienceKey.CUSTOMER_CORPORATE_ADMINS])
        assert set(recipients) == {u.id for u in t["a_admins"]}
        assert not set(recipients) & {u.id for u in t["b_admins"]}, "Cross-customer leak!"
    print("  [PASS] corporate-admin audience is scoped to the RFQ's customer")


def test_actor_is_excluded():
    with _session() as db:
        t = _tenants(db)
        admin = t["a_admins"][0]
        rfq = _transient_rfq(t["a"], admin.id)
        recipients = resolve_rfq_audience(
            db, rfq, [AudienceKey.CUSTOMER_CORPORATE_ADMINS, AudienceKey.RFQ_CREATOR], actor_user_id=admin.id
        )
        assert admin.id not in recipients
    print("  [PASS] actor never notified about their own action")


def test_delegate_empty_and_out_of_scope_dropped():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
        assert resolve_rfq_audience(db, rfq, [AudienceKey.RFQ_DELEGATE]) == {}
        # A delegate pointing at another customer's user must be dropped by the tenancy re-check.
        rfq.delegated_to_user_id = t["b_admins"][0].id
        assert resolve_rfq_audience(db, rfq, [AudienceKey.RFQ_DELEGATE]) == {}
    print("  [PASS] missing delegate -> nobody; cross-customer delegate dropped")


def test_unknown_code_raises():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
        _assert_raises(ValueError, emit, db, "NOT_A_REAL_EVENT", rfq)
    print("  [PASS] unknown event code raises ValueError")


def test_unexpected_template_field_raises():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
        _assert_raises(ValueError, emit, db, EventCode.RFQ_APPROVED, rfq, extra={"amount": "12,500,000"})
    print("  [PASS] non-whitelisted template field raises ValueError")


def test_rendered_text_is_lock_screen_safe():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
    for code, spec in CATALOG.items():
        extra = {"decision": "approved"} if "decision" in spec.allowed_fields else None
        text = render(spec, rfq, extra)
        blob = f"{text['title']} {text['body']}"
        assert rfq.ref_no in text["title"], f"{code}: title must carry the RFQ reference"
        for forbidden in ("USD", "EGP", "12500000", "12,500,000", "12.5"):
            assert forbidden not in blob, f"{code}: leaked '{forbidden}' into notification text"
    print(f"  [PASS] all {len(CATALOG)} event texts are lock-screen safe")


def test_emit_stages_rows_and_rollback_leaves_none():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_maker"].id)
        count = emit(db, EventCode.RFQ_APPROVAL_REQUIRED, rfq, actor_user_id=t["a_maker"].id)
        assert count == len(t["a_admins"])
        db.flush()
        rows = db.query(UserNotification).filter(UserNotification.reference_key == rfq.id).all()
        assert len(rows) == count
        for row in rows:
            assert row.event_type == "RFQ_APPROVAL_REQUIRED"
            assert row.module == "QUOTATION"
            assert row.urgency == "high"
            assert row.link == "/corporate-admin/quotations/approvals"
            assert row.expires_at is not None

        db.rollback()
        leftover = db.query(UserNotification).filter(UserNotification.reference_key == rfq.id).count()
        assert leftover == 0, "Rolled-back business transaction must not leave notifications behind"
    print("  [PASS] rows staged in caller's transaction; rollback leaves none")


def test_creator_events_target_maker_with_role_aware_links():
    with _session() as db:
        t = _tenants(db)
        maker, admin = t["a_maker"], t["a_admins"][0]
        rfq = _transient_rfq(t["a"], maker.id)
        rfq.cancellation_requested_by = maker.id

        expectations = {
            EventCode.RFQ_APPROVED: f"/end-user/quotations/results/{rfq.id}",
            EventCode.RFQ_REJECTED: "/end-user/quotations/history",
            EventCode.RFQ_REVISION_REQUESTED: "/end-user/quotations/active",
        }
        for code in expectations:
            assert emit(db, code, rfq, actor_user_id=admin.id) == 1
        assert emit(db, EventCode.RFQ_CANCELLATION_DECIDED, rfq, actor_user_id=admin.id,
                    extra={"decision": "approved"}) == 1
        db.flush()

        rows = db.query(UserNotification).filter(UserNotification.reference_key == rfq.id).all()
        assert {r.user_id for r in rows} == {maker.id}
        by_code = {r.event_type: r for r in rows}
        for code, link in expectations.items():
            assert by_code[code.value].link == link
        assert "approved" in by_code["RFQ_CANCELLATION_DECIDED"].title
    print("  [PASS] maker-facing events reach only the maker, with end-user links")


def test_corporate_admin_links_use_admin_routes():
    with _session() as db:
        t = _tenants(db)
        rfq = _transient_rfq(t["a"], t["a_admins"][0].id)
    spec = get_spec(EventCode.RFQ_APPROVED)
    assert build_link(spec.link, rfq, UserRole.CORPORATE_ADMIN) == f"/corporate-admin/quotations/results/{rfq.id}"
    print("  [PASS] corporate-admin recipients get corporate-admin routes")


if __name__ == "__main__":
    print("=" * 70)
    print("NOTIFICATION PROGRAMME - PHASE Q1 VERIFICATION")
    print("=" * 70)
    test_admin_audience_is_customer_scoped()
    test_actor_is_excluded()
    test_delegate_empty_and_out_of_scope_dropped()
    test_unknown_code_raises()
    test_unexpected_template_field_raises()
    test_rendered_text_is_lock_screen_safe()
    test_emit_stages_rows_and_rollback_leaves_none()
    test_creator_events_target_maker_with_role_aware_links()
    test_corporate_admin_links_use_admin_routes()
    print("\n" + "=" * 70)
    print("ALL PHASE Q1 CHECKS PASSED")
    print("=" * 70)
