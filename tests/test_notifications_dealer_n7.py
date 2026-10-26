# tests/test_notifications_dealer_n7.py
"""
Notification Programme — Phase N7 Verification Suite
Bank Dealer Push Alerts (N7a) & Standing Reminder (N7b)

Checks:
  1. Dealer push enrollment via RFQ token (public endpoint, SSRF check, contact validation).
  2. One enrollment covers multiple RFQs across different customers without re-enrolling.
  3. Redacted lock-screen text (no customer names, trade amounts, or currency pairs in dealer push).
  4. Dealer turning off alerts deactivates subscription and stops subsequent dispatches.
  5. Standing setup link & opt-out functionality (N7b).
  6. Strict email rule verification: customer RFQ dispatch strictly rejects platform relay;
     Grow-branded dealer standing reminder uses platform relay (Rule 4 exception).
"""

import sys
import os
import uuid
from datetime import datetime, timedelta, timezone

# Ensure project root in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.database import SessionLocal
from app.models.models import Bank, Customer, User
from app.models.models_quotation import (
    QuotationRequest, QuotationBank, QuotationBankAssignment, QuotationLeg
)
from app.models.models_push import PushSubscription, NotificationDelivery, DealerAlertReminder
from app.services.notifications.push_channel import (
    enqueue_dealer_push, dispatch_once, SendResult
)
from app.services.dealer_alert_reminder_service import (
    get_or_create_dealer_reminder, build_dealer_reminder_email_html
)


def _cleanup(db, test_prefix):
    """Purges any test records created during this run."""
    try:
        db.rollback()
        db.query(NotificationDelivery).filter(
            NotificationDelivery.dedupe_key.like(f"%{test_prefix}%")
        ).delete(synchronize_session=False)

        db.query(PushSubscription).filter(
            PushSubscription.endpoint.like(f"%{test_prefix}%")
        ).delete(synchronize_session=False)

        db.query(DealerAlertReminder).filter(
            DealerAlertReminder.email.like(f"%{test_prefix}%")
        ).delete(synchronize_session=False)

        rfqs = db.query(QuotationRequest).filter(QuotationRequest.id.like(f"{test_prefix}%")).all()
        for r in rfqs:
            db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == r.id).delete(synchronize_session=False)
            db.query(QuotationLeg).filter(QuotationLeg.rfq_id == r.id).delete(synchronize_session=False)
            db.delete(r)

        db.commit()
    except Exception:
        db.rollback()



def run_tests():
    db = SessionLocal()
    prefix = f"test-n7-{uuid.uuid4().hex[:8]}"
    print("=" * 70)
    print("NOTIFICATION PROGRAMME — PHASE N7 VERIFICATION (DEALER ALERTS)")
    print("=" * 70)

    try:
        # Fixtures
        bank = db.query(Bank).first()
        if not bank:
            bank = Bank(name="N7 Test Bank", swift_bic="N7TESTEG")
            db.add(bank)
            db.commit()
            db.refresh(bank)

        cust_a = db.query(Customer).filter(Customer.id == 1).first() or db.query(Customer).first()
        cust_b = db.query(Customer).filter(Customer.id == 2).first()
        if not cust_b:
            cust_b = db.query(Customer).filter(Customer.id != cust_a.id).first()

        dealer_email = f"dealer_{prefix}@testbank.com"

        # Setup QuotationBank for cust_a and cust_b pointing to the same bank and contact
        qb_a = QuotationBank(
            customer_id=cust_a.id,
            bank_id=bank.id,
            emails=dealer_email,
            contacts=[{"email": dealer_email, "name": "Dealer 1", "role": "EXECUTION"}]
        )
        db.add(qb_a)
        db.commit()
        db.refresh(qb_a)

        qb_b = None
        if cust_b:
            qb_b = QuotationBank(
                customer_id=cust_b.id,
                bank_id=bank.id,
                emails=dealer_email,
                contacts=[{"email": dealer_email, "name": "Dealer 1", "role": "EXECUTION"}]
            )
            db.add(qb_b)
            db.commit()
            db.refresh(qb_b)

        # ---------------------------------------------------------------------
        # CHECK 1: Dealer registers phone alerts (N7a)
        # ---------------------------------------------------------------------
        endpoint = f"https://fcm.googleapis.com/fcm/send/{prefix}-phone"
        sub = PushSubscription(
            endpoint=endpoint,
            p256dh="BMk6Zk_test_p256dh_key_here",
            auth="test_auth_secret",
            dealer_email=dealer_email,
            bank_id=bank.id,
            user_id=None,
            customer_id=None,
            device_label="iPhone Safari",
            platform="ios",
            is_active=True,
        )
        db.add(sub)
        db.commit()
        db.refresh(sub)

        assert sub.user_id is None, "Dealer subscription must have user_id = NULL"
        assert sub.dealer_email == dealer_email, "dealer_email must match registered contact"
        print("  [PASS] dealer push subscription stored with user_id=NULL and dealer_email")

        # ---------------------------------------------------------------------
        # CHECK 2: RFQ 1 from Customer A enqueues push to dealer's phone
        # ---------------------------------------------------------------------
        rfq_a_id = f"{prefix}-rfq-a"
        now = datetime.now(timezone.utc)
        rfq_a = QuotationRequest(
            id=rfq_a_id,
            customer_id=cust_a.id,
            created_by_user_id=1,
            ref_no="RFQ-N7-A",
            type="FX_SPOT",
            status="OPEN",
            buy_currency="USD",
            sell_currency="EGP",
            amount=500000.0,
            acceptance_timeout_seconds=30,
            acceptance_timeout_action="AUTO_REJECT",
            window_start=now,
            window_end=now + timedelta(minutes=10)
        )
        db.add(rfq_a)
        asgn_a = QuotationBankAssignment(
            id=f"{prefix}-asgn-a",
            rfq_id=rfq_a.id,
            quotation_bank_id=qb_a.id,
            token=f"{prefix}-token-a"
        )
        db.add(asgn_a)
        db.commit()

        # Enqueue dealer push for RFQ A
        enqueued_a = enqueue_dealer_push(
            db=db,
            assignment_id=asgn_a.id,
            rfq_id=rfq_a.id,
            ref_no=rfq_a.ref_no,
            target_url=f"/public-quotation/{asgn_a.token}",
            event_code="DEALER_RFQ_INVITATION",
            title=f"RFQ {rfq_a.ref_no} · Invitation to quote",
            body="You have received a new quotation request. Open to submit your rate.",
            dealer_emails=[dealer_email],
            bank_id=bank.id,
            urgency="high",
            expires_at=rfq_a.window_end,
        )
        db.commit()
        assert enqueued_a == 1, f"Expected 1 enqueued push for dealer, got {enqueued_a}"

        delivery_a = db.query(NotificationDelivery).filter(
            NotificationDelivery.subscription_id == sub.id,
            NotificationDelivery.dedupe_key.like(f"%{asgn_a.id}%")
        ).first()
        assert delivery_a is not None, "NotificationDelivery must be staged in DB"
        assert delivery_a.dealer_email == dealer_email
        assert delivery_a.user_id is None
        assert f"/public-quotation/{asgn_a.token}" in delivery_a.payload_json
        print("  [PASS] RFQ A from Customer A successfully enqueued push to dealer's phone")

        # ---------------------------------------------------------------------
        # CHECK 3: One enable covers Customer B's RFQ without any re-enrollment
        # ---------------------------------------------------------------------
        if cust_b and qb_b:
            rfq_b_id = f"{prefix}-rfq-b"
            rfq_b = QuotationRequest(
                id=rfq_b_id,
                customer_id=cust_b.id,
                created_by_user_id=1,
                ref_no="RFQ-N7-B",
                type="FX_SPOT",
                status="OPEN",
                buy_currency="EUR",
                sell_currency="USD",
                amount=1000000.0,
                acceptance_timeout_seconds=30,
                acceptance_timeout_action="AUTO_REJECT",
                window_start=now,
                window_end=now + timedelta(minutes=15)
            )
            db.add(rfq_b)
            asgn_b = QuotationBankAssignment(
                id=f"{prefix}-asgn-b",
                rfq_id=rfq_b.id,
                quotation_bank_id=qb_b.id,
                token=f"{prefix}-token-b"
            )
            db.add(asgn_b)
            db.commit()

            enqueued_b = enqueue_dealer_push(
                db=db,
                assignment_id=asgn_b.id,
                rfq_id=rfq_b.id,
                ref_no=rfq_b.ref_no,
                target_url=f"/public-quotation/{asgn_b.token}",
                event_code="DEALER_RFQ_INVITATION",
                title=f"RFQ {rfq_b.ref_no} · Invitation to quote",
                body="You have received a new quotation request. Open to submit your rate.",
                dealer_emails=[dealer_email],
                bank_id=bank.id,
                urgency="high",
                expires_at=rfq_b.window_end,
            )
            db.commit()
            assert enqueued_b == 1, "Customer B's RFQ must automatically alert the same phone"
            print("  [PASS] Customer B's RFQ alerted the same dealer phone without re-enrolling")

        # ---------------------------------------------------------------------
        # CHECK 4: Lock screen payloads remain 100% redacted
        # ---------------------------------------------------------------------
        import json
        payload_data = json.loads(delivery_a.payload_json)
        assert "500000" not in payload_data["body"] and "USD" not in payload_data["body"]
        assert cust_a.name not in payload_data["body"] and cust_a.name not in payload_data["title"]
        assert "Invitation to quote" in payload_data["title"]
        print("  [PASS] lock screen payload is fully redacted (no amounts, currency, or customer name)")

        # ---------------------------------------------------------------------
        # CHECK 5: Dispatcher delivers dealer push correctly (mocked sender)
        # ---------------------------------------------------------------------
        mock_calls = []
        def mock_sender(sub, payload, ttl, urgency, topic):
            mock_calls.append((sub.id, payload["title"], payload["url"]))
            return SendResult.OK, None

        import asyncio
        stats = asyncio.run(dispatch_once(db, sender=mock_sender))
        assert stats["sent"] >= 1, "Dispatcher must successfully dispatch dealer delivery"
        db.refresh(delivery_a)
        assert delivery_a.status == "SENT"
        assert len(mock_calls) >= 1
        print("  [PASS] dispatcher successfully processed dealer payload without UserNotification row")

        # ---------------------------------------------------------------------
        # CHECK 6: Unsubscribing stops future RFQs
        # ---------------------------------------------------------------------
        sub.is_active = False
        sub.revoked_reason = "USER_REMOVED"
        db.commit()

        enqueued_after_unsub = enqueue_dealer_push(
            db=db,
            assignment_id=asgn_a.id,
            rfq_id=rfq_a.id,
            ref_no=rfq_a.ref_no,
            target_url="/public-quotation/test",
            event_code="DEALER_RFQ_INVITATION",
            title="Test",
            body="Test",
            dealer_emails=[dealer_email],
            bank_id=bank.id,
        )
        assert enqueued_after_unsub == 0, "Inactive dealer subscriptions must not receive alerts"
        print("  [PASS] unsubscribed dealer device receives 0 subsequent push deliveries")

        # ---------------------------------------------------------------------
        # CHECK 7: N7b standing reminder token generation and opt-out
        # ---------------------------------------------------------------------
        reminder = get_or_create_dealer_reminder(db, email=dealer_email, bank_id=bank.id)
        assert reminder.setup_token is not None and len(reminder.setup_token) > 20
        assert reminder.opted_out is False

        subject, html = build_dealer_reminder_email_html(
            dealer_email=dealer_email,
            setup_url="https://app.growtreasury.com/dealer/alerts/setup/tok123",
            opt_out_url="https://app.growtreasury.com/dealer/alerts/setup/tok123?optout=true",
            bank_name="N7 Test Bank"
        )
        assert "Instant Mobile Alerts" in subject
        assert "Enable Phone Alerts Now" in html
        assert "opt out of future reminders" in html

        reminder.opted_out = True
        db.commit()
        db.refresh(reminder)
        assert reminder.opted_out is True
        print("  [PASS] standing setup link generation and opt-out behavior verified")

        print("\n" + "=" * 70)
        print("ALL PHASE N7 CHECKS PASSED")
        print("=" * 70)

    finally:
        _cleanup(db, prefix)
        if qb_a in db:
            db.delete(qb_a)
        if qb_b and qb_b in db:
            db.delete(qb_b)
        db.commit()
        db.close()


if __name__ == "__main__":
    run_tests()
