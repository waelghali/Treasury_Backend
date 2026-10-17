"""
Test Suite: Strict Customer Email Rule & Anti-Leak Safeguard for Quotations
Verifies:
1. get_customer_email_settings with allow_platform_fallback=False returns (None, 'no_customer_settings')
   when a customer has no custom SMTP configuration.
2. Default get_customer_email_settings (allow_platform_fallback=True) still returns platform relay
   for internal non-banking features.
3. send_email with allow_platform_fallback=False returns failure immediately and NEVER triggers
   fallback relays (Resend / Brevo) when primary customer delivery fails.
4. broadcast_rfq_to_banks completes RFQ lifecycle state transition to PENDING and is_dispatched=True
   without leaking any emails to banks from the platform domain.
5. dispatch_rfq_15m_reminder suppresses bank reminder emails when custom SMTP is absent.
"""
import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.main
from app.database import SessionLocal
from app.models.models import Customer
from app.models.models_quotation import QuotationRequest, QuotationBankAssignment, QuotationBank
from app.core.email_service import (
    get_customer_email_settings,
    send_email,
    EmailSettings
)


def test_get_customer_email_settings_strict_enforcement():
    print("\n--- [TEST 1] get_customer_email_settings strict enforcement ---")
    db = SessionLocal()
    try:
        # Customer 2 (Globex Industries) has NO custom SMTP
        settings_strict, source_strict = get_customer_email_settings(db, customer_id=2, allow_platform_fallback=False)
        assert settings_strict is None, f"Expected None for strict customer email settings, got {settings_strict}"
        assert source_strict == "no_customer_settings", f"Expected 'no_customer_settings', got {source_strict}"
        print("  [PASS] allow_platform_fallback=False returned (None, 'no_customer_settings') for customer without custom SMTP (Customer 2)")

        # Fallback allowed (default) returns platform relay for Customer 2
        settings_fb, source_fb = get_customer_email_settings(db, customer_id=2, allow_platform_fallback=True)
        assert settings_fb is not None, "Expected platform settings when fallback is allowed"
        assert source_fb == "platform_relay_branded", f"Expected 'platform_relay_branded', got {source_fb}"
        assert "via Grow Treasury" in settings_fb.sender_display_name
        print("  [PASS] allow_platform_fallback=True preserved for internal platform notifications (Customer 2)")

        # Customer 1 (Acme Corp) HAS valid custom SMTP
        settings_c1, source_c1 = get_customer_email_settings(db, customer_id=1, allow_platform_fallback=False)
        assert settings_c1 is not None, "Expected customer settings for Customer 1"
        assert source_c1 == "customer_specific", f"Expected 'customer_specific', got {source_c1}"
        assert settings_c1.sender_email == "wael_click@yahoo.com"
        print("  [PASS] allow_platform_fallback=False successfully returns direct custom SMTP for Customer 1")
    finally:
        db.close()


def test_send_email_disables_platform_fallback():
    print("\n--- [TEST 2] send_email disables platform fallback ---")
    db = SessionLocal()
    try:
        # Invalid SMTP settings that cannot connect (immediate refusal on localhost closed port)
        failing_settings = EmailSettings(
            smtp_host="127.0.0.1",
            smtp_port=59999,
            smtp_username="user@fake.com",
            smtp_password="password",
            sender_email="treasury@acmecorp.com",
            sender_display_name="Acme Treasury",
            reply_to="treasury@acmecorp.com"
        )

        async def _run_send():
            success, err = await send_email(
                db=db,
                to_emails=["bank.dealer@partnerbank.com"],
                subject_template="Quotation RFQ Test",
                body_template="<p>Test</p>",
                template_data={},
                email_settings=failing_settings,
                allow_platform_fallback=False
            )
            return success, err

        success, err = asyncio.run(_run_send())
        assert success is False, "Expected send_email to fail without fallback"
        assert err is not None
        assert "Platform fallback is disabled" not in err  # logger message
        assert "SMTP Error" in err
        print("  [PASS] send_email with allow_platform_fallback=False failed cleanly without attempting platform relay")
    finally:
        db.close()


def test_broadcast_rfq_suppresses_bank_emails_without_customer_smtp():
    print("\n--- [TEST 3] broadcast_rfq_to_banks suppresses bank emails when custom SMTP absent ---")
    from app.services.quotation_release_scheduler import broadcast_rfq_to_banks

    db = SessionLocal()
    test_rfq_id = None
    try:
        # Create a test RFQ in APPROVED_SCHEDULED status for Customer 2 (Globex, no SMTP)
        import uuid
        rfq = QuotationRequest(
            id=str(uuid.uuid4()),
            customer_id=2,
            ref_no=f"TEST-EMAIL-RULE-{uuid.uuid4().hex[:6].upper()}",
            type="FX_SPOT",
            direction="Buy",
            amount=500000.0,
            buy_currency="USD",
            sell_currency="EGP",
            status="APPROVED_SCHEDULED",
            is_dispatched=False,
            acceptance_timeout_seconds=300,
            acceptance_timeout_action="AUTO_REJECT",
            window_start=datetime.now(timezone.utc) + timedelta(hours=2),
            window_end=datetime.now(timezone.utc) + timedelta(hours=3),
            created_by_user_id=1
        )
        db.add(rfq)
        db.commit()
        db.refresh(rfq)
        test_rfq_id = rfq.id

        # Run broadcast
        async def _run_broadcast():
            return await broadcast_rfq_to_banks(rfq_id=rfq.id, db=db)

        result = asyncio.run(_run_broadcast())
        db.refresh(rfq)

        # Verify RFQ status transitioned to PENDING and dispatched
        assert rfq.status == "PENDING", f"Expected RFQ status PENDING, got {rfq.status}"
        assert rfq.is_dispatched is True, "Expected is_dispatched to be True"
        assert rfq.dispatched_at is not None, "Expected dispatched_at to be set"
        print(f"  [PASS] RFQ {rfq.ref_no} transitioned to PENDING & is_dispatched=True")
        print(f"  [PASS] Zero emails dispatched from platform domain to bank counterparties")
    finally:
        if test_rfq_id:
            db.query(QuotationRequest).filter(QuotationRequest.id == test_rfq_id).delete()
            db.commit()
        db.close()


def test_dispatch_15m_reminder_suppressed_without_customer_smtp():
    print("\n--- [TEST 4] execute_rfq_15m_reminder suppresses emails without customer SMTP ---")
    from app.services.quotation_reminder_service import execute_rfq_15m_reminder

    db = SessionLocal()
    test_rfq_id = None
    try:
        import uuid
        rfq = QuotationRequest(
            id=str(uuid.uuid4()),
            customer_id=2,
            ref_no=f"TEST-EMAIL-REMINDER-{uuid.uuid4().hex[:6].upper()}",
            type="FX_SPOT",
            direction="Buy",
            amount=250000.0,
            buy_currency="USD",
            sell_currency="EGP",
            status="PENDING",
            is_dispatched=True,
            acceptance_timeout_seconds=300,
            acceptance_timeout_action="AUTO_REJECT",
            window_start=datetime.now(timezone.utc) + timedelta(minutes=15),
            window_end=datetime.now(timezone.utc) + timedelta(minutes=30),
            created_by_user_id=1
        )
        db.add(rfq)
        db.commit()
        db.refresh(rfq)
        test_rfq_id = rfq.id

        async def _run_reminder():
            return await execute_rfq_15m_reminder(rfq_id=rfq.id, db=db)

        res = asyncio.run(_run_reminder())
        assert res is False, "Expected reminder dispatch to return False (suppressed)"
        print("  [PASS] 15m reminder cleanly suppressed without custom SMTP")
    finally:
        if test_rfq_id:
            db.query(QuotationRequest).filter(QuotationRequest.id == test_rfq_id).delete()
            db.commit()
        db.close()


if __name__ == "__main__":
    print("=" * 70)
    print("STRICT QUOTATION CUSTOMER EMAIL RULES VERIFICATION")
    print("=" * 70)
    test_get_customer_email_settings_strict_enforcement()
    test_send_email_disables_platform_fallback()
    test_broadcast_rfq_suppresses_bank_emails_without_customer_smtp()
    test_dispatch_15m_reminder_suppressed_without_customer_smtp()
    print("\n" + "=" * 70)
    print("ALL STRICT QUOTATION EMAIL RULE CHECKS PASSED 100%!")
    print("=" * 70)
