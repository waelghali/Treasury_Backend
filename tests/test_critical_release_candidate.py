# tests/test_critical_release_candidate.py
"""
Automated Verification Suite for Release Candidate (safe/2026-10-15-reviewed-fixes):
1. Sensitive Data Log Filter (Scrubbing JWTs, Bearer tokens, passwords, OTPs, LG numbers, emails, URL tokens)
2. Public Issuance AI Integration & Logger (no FK crash on user_id=None, logger defined, DB session health)
3. Quotation History Winner Preservation (winner_bank_id preserved on closed & completed RFQ legs)
"""

import sys
import logging
import re
from datetime import datetime, timezone, timedelta
from app.main import SensitiveDataFilter
from app.database import SessionLocal
from app.models.models import Customer, User, Bank, AIUsageLog
from app.models.models_quotation import QuotationRequest, QuotationLeg, QuotationOffer
from app.core.ai_integration import log_ai_usage_sync
from app.core.security import TokenData
from app.api.v1.endpoints.quotations_endpoints import get_rfq_history


def test_log_privacy_filter():
    print("\n--- [TEST 1] Log Privacy & Sensitive Data Filter ---")
    s_filter = SensitiveDataFilter(preserve_args=False)
    
    # 1. JWT Token
    jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0.signature_string_here_123"
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, f"User token is {jwt}", (), None)
    s_filter.filter(rec)
    assert "[JWT]" in rec.msg, f"JWT not scrubbed: {rec.msg}"
    assert jwt not in rec.msg
    print("  [PASS] JWT scrubbed to [JWT]")

    # 2. Bearer Authorization
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "Header Authorization: Bearer secret_token_xyz123", (), None)
    s_filter.filter(rec)
    assert "Bearer [REDACTED]" in rec.msg, f"Bearer not scrubbed: {rec.msg}"
    assert "secret_token_xyz123" not in rec.msg
    print("  [PASS] Authorization: Bearer scrubbed to Bearer [REDACTED]")

    # 3. Connection string credentials
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "Connecting to postgresql://grow_user:MySecretPassword99@db.grow.internal:5432/prod", (), None)
    s_filter.filter(rec)
    assert "postgresql://grow_user:[REDACTED]@db.grow.internal" in rec.msg, f"DB password not scrubbed: {rec.msg}"
    assert "MySecretPassword99" not in rec.msg
    print("  [PASS] Database connection URI credentials scrubbed")

    # 4. Sensitive Query Params (?token=... &otp=...)
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "GET /api/v1/auth/verify?token=abc123456&otp=839201&status=active", (), None)
    s_filter.filter(rec)
    assert "?token=[REDACTED]" in rec.msg, f"Token query param not scrubbed: {rec.msg}"
    assert "&otp=[REDACTED]" in rec.msg, f"OTP query param not scrubbed: {rec.msg}"
    assert "status=active" in rec.msg, f"Non-sensitive query param was wrongly scrubbed: {rec.msg}"
    print("  [PASS] Query params token & otp scrubbed, status=active preserved")

    # 5. Sensitive Dict / JSON key-values (password, raw_code, mfa_code)
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "Payload: {'password': 'TopSecretPassword', 'mfa_code': '482019'}", (), None)
    s_filter.filter(rec)
    assert "TopSecretPassword" not in rec.msg
    assert "482019" not in rec.msg
    print("  [PASS] Key-value secrets (password, mfa_code) scrubbed to [REDACTED]")

    # 6. Opaque URL Token in path
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "GET /api/v1/public/quotations/ABCDEF1234567890ABCDEF123456/live-rank HTTP/1.1", (), None)
    s_filter.filter(rec)
    assert "[TOKEN]" in rec.msg, f"URL path token not scrubbed: {rec.msg}"
    assert "ABCDEF1234567890ABCDEF123456" not in rec.msg
    print("  [PASS] URL path token scrubbed to [TOKEN]")

    # 7. LG Numbers in text
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "LG No: 5600772000008294 issued for customer", (), None)
    s_filter.filter(rec)
    assert "5600772000008294" not in rec.msg
    assert "****8294" in rec.msg
    print("  [PASS] LG number masked to last 4 digits (****8294)")

    # 8. Email Masking
    rec = logging.LogRecord("test", logging.INFO, "test.py", 10, "Dispatching to waelghali79@gmail.com and treasurer@cbe.gov.eg", (), None)
    s_filter.filter(rec)
    assert "w***@gmail.com" in rec.msg
    assert "t***@cbe.gov.eg" in rec.msg
    print("  [PASS] Email addresses masked (w***@gmail.com, t***@cbe.gov.eg)")

    # 9. Server access log preserving args
    server_filter = SensitiveDataFilter(preserve_args=True)
    access_rec = logging.LogRecord(
        "uvicorn.access", logging.INFO, "access.py", 10,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:51234", "GET", "/public/quotations/gAAAAABlM34567890123456789012345/submit", "1.1", 200),
        None
    )
    server_filter.filter(access_rec)
    formatted = access_rec.getMessage()
    assert "gAAAAABlM3" not in formatted
    print("  [PASS] Server access log tuple args scrubbed preserving formatting")
    print("ALL LOG PRIVACY CHECKS PASSED.")


def test_public_issuance_ai_and_logger():
    print("\n--- [TEST 2] Public Issuance AI Integration & Logger ---")
    
    # 1. Verify logger is imported and callable in public_issuance
    from app.api.v1.endpoints import public_issuance
    assert hasattr(public_issuance, 'logger'), "public_issuance is missing module-level logger!"
    assert public_issuance.logger is not None
    public_issuance.logger.info("Test public issuance logger message")
    print("  [PASS] public_issuance.logger exists and logs cleanly")

    # 2. Verify log_ai_usage_sync with user_id = None (public unauthenticated user)
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "At least 1 customer record required in DB"
        
        # Call log_ai_usage_sync with user_id=None
        log_ai_usage_sync(
            db=db,
            customer_id=cust.id,
            user_id=None,
            doc_name="document_verify.pdf",
            prompt_tokens=100,
            completion_tokens=50,
            call_type="public_test"
        )
        print("  [PASS] log_ai_usage_sync with user_id=None returned without error")

        # Crucial check: DB session must NOT be poisoned!
        # If an unhandled DB FK error occurred, executing a query on db would raise PendingRollbackError
        test_query = db.query(Customer).filter(Customer.id == cust.id).first()
        assert test_query is not None
        print("  [PASS] Database session remains completely healthy (no FK error / rollback poisoning)")

        # 3. Test with real user_id to ensure authenticated usage logging still functions
        user = db.query(User).filter(User.customer_id == cust.id).first()
        if user:
            initial_count = db.query(AIUsageLog).filter(AIUsageLog.user_id == user.id).count()
            log_ai_usage_sync(
                db=db,
                customer_id=cust.id,
                user_id=user.id,
                doc_name="auth_doc_verify.pdf",
                prompt_tokens=50,
                completion_tokens=25,
                call_type="auth_test"
            )
            new_count = db.query(AIUsageLog).filter(AIUsageLog.user_id == user.id).count()
            assert new_count == initial_count + 1
            print(f"  [PASS] Authenticated user AI usage logged successfully (user_id={user.id})")
    finally:
        db.close()
    print("ALL PUBLIC ISSUANCE AI CHECKS PASSED.")


def test_quotation_history_winner_preservation():
    print("\n--- [TEST 3] Quotation History Winner Preservation ---")
    db = SessionLocal()
    cleanup_rfq_ids = []
    
    try:
        cust = db.query(Customer).first()
        bank = db.query(Bank).first()
        user = db.query(User).filter(User.customer_id == cust.id).first()
        assert cust and bank and user, "Customer, Bank, and User required for test"

        # Case A: Historical COMPLETED RFQ with awarded winner
        past_time = datetime.now(timezone.utc) - timedelta(hours=2)
        import uuid
        rfq_won_id = str(uuid.uuid4())
        rfq_won = QuotationRequest(
            id=rfq_won_id,
            customer_id=cust.id,
            created_by_user_id=user.id,
            entity_id=None,
            ref_no=f"TEST-WON-{int(datetime.now().timestamp())}",
            status="COMPLETED",
            acceptance_status="ACCEPTED",
            created_at=past_time,
            window_start=past_time - timedelta(minutes=15),
            window_end=past_time + timedelta(minutes=10),
            acceptance_timeout_seconds=300,
            acceptance_timeout_action="AUTO_REJECT",
            acceptance_deadline=past_time + timedelta(minutes=30),
            winner_bank_id=bank.id,
            winner_bank_name=bank.name,
            winner_rate=1.25,
        )
        db.add(rfq_won)
        db.flush()
        cleanup_rfq_ids.append(rfq_won.id)

        leg_won = QuotationLeg(
            id=str(uuid.uuid4()),
            rfq_id=rfq_won.id,
            buy_currency="USD",
            sell_currency="EGP",
            amount=100000,
            quotation_base="Execution",
            status="ACCEPTED",
            winner_bank_id=bank.id,
            winner_bank_name=bank.name,
            winner_rate=1.25,
        )
        db.add(leg_won)
        db.commit()

        # Call get_rfq_history as this user
        token_data = TokenData(
            user_id=user.id,
            customer_id=cust.id,
            role="corporate_admin",
            permissions=[]
        )
        
        history_results = get_rfq_history(
            entity_id=None,
            db=db,
            current_user=token_data
        )

        # Refresh leg_won from DB
        db.refresh(leg_won)
        db.refresh(rfq_won)
        
        assert leg_won.winner_bank_id == bank.id, f"CRITICAL REGRESSION: leg.winner_bank_id was wiped! Got: {leg_won.winner_bank_id}"
        assert leg_won.winner_bank_name == bank.name, f"CRITICAL REGRESSION: leg.winner_bank_name was wiped!"
        assert rfq_won.winner_bank_id == bank.id, f"CRITICAL REGRESSION: rfq.winner_bank_id was wiped!"
        print(f"  [PASS] Historical won RFQ preserved winner: bank_id={leg_won.winner_bank_id} ({leg_won.winner_bank_name})")

        # Case B: Expired RFQ with NO_QUOTES
        rfq_exp_id = str(uuid.uuid4())
        rfq_expired = QuotationRequest(
            id=rfq_exp_id,
            customer_id=cust.id,
            created_by_user_id=user.id,
            entity_id=None,
            ref_no=f"TEST-EXP-{int(datetime.now().timestamp())}",
            status="OPEN",
            acceptance_status="PENDING",
            created_at=past_time,
            window_start=past_time - timedelta(minutes=10),
            window_end=past_time + timedelta(minutes=5),  # Closed 1h 55m ago
            acceptance_timeout_seconds=300,
            acceptance_timeout_action="AUTO_REJECT",
            acceptance_deadline=None,
            winner_bank_id=None,
            winner_bank_name=None,
        )
        db.add(rfq_expired)
        db.flush()
        cleanup_rfq_ids.append(rfq_expired.id)

        leg_unawarded = QuotationLeg(
            id=str(uuid.uuid4()),
            rfq_id=rfq_expired.id,
            buy_currency="EUR",
            sell_currency="EGP",
            amount=50000,
            quotation_base="Execution",
            status="OPEN",
            winner_bank_id=None,
            winner_bank_name=None,
        )
        db.add(leg_unawarded)
        db.commit()

        # Query history again to trigger auto-closure logic
        get_rfq_history(
            entity_id=None,
            db=db,
            current_user=token_data
        )

        db.refresh(rfq_expired)
        db.refresh(leg_unawarded)
        assert rfq_expired.status == "COMPLETED"
        assert rfq_expired.acceptance_status == "NO_QUOTES"
        assert leg_unawarded.status == "COMPLETED"
        assert leg_unawarded.winner_bank_id is None
        print("  [PASS] Expired RFQ cleanly transitioned to COMPLETED / NO_QUOTES without corrupting winners")

    finally:
        # Clean up test RFQs
        try:
            db.rollback()
            for rfq_id in cleanup_rfq_ids:
                db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq_id).delete()
                db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).delete()
            db.commit()
        except Exception:
            pass
        db.close()
    print("ALL QUOTATION HISTORY CHECKS PASSED.")


def test_generate_invite_security():
    print("\n--- [TEST 4] Issuance Invite Security & Domain Whitelist ---")
    from fastapi import HTTPException
    from app.api.v1.endpoints.public_issuance import generate_invite_link, InviteRequest
    from app.core.encryption import decrypt_data
    from app.core.security import TokenData

    db = SessionLocal()
    try:
        cust = db.query(Customer).filter(Customer.id == 1).first()
        assert cust is not None, "Customer 1 required for test"

        # 1. Non-admin user cannot generate invite
        regular_user = TokenData(user_id=5, customer_id=cust.id, role="end_user", permissions=[])
        try:
            generate_invite_link(
                payload=InviteRequest(email="someone@acmecorp.com", department="IT"),
                background_tasks=None,
                current_user=regular_user,
                db=db
            )
            assert False, "Should have failed for non-admin user!"
        except HTTPException as e:
            assert e.status_code == 403
            print("  [PASS] Non-admin user blocked from generating invites (HTTP 403)")

        # 2. Admin cannot invite domain outside their organization
        admin_user = TokenData(user_id=2, customer_id=cust.id, role="corporate_admin", permissions=[])
        try:
            generate_invite_link(
                payload=InviteRequest(email="hacker@pilottest.com", department="IT"),
                background_tasks=None,
                current_user=admin_user,
                db=db
            )
            assert False, "Should have failed for unlisted domain!"
        except HTTPException as e:
            assert e.status_code == 400
            assert "not authorized for your organization" in e.detail
            print("  [PASS] Admin blocked from inviting external/unauthorized domain (HTTP 400)")

        # 3. Admin successfully invites company domain
        res = generate_invite_link(
            payload=InviteRequest(email="engineer@acmecorp.com", department="Engineering"),
            background_tasks=None,
            current_user=admin_user,
            db=db
        )
        assert "invite_link" in res
        token = res["invite_link"].split("token=")[-1]
        raw_decrypted = decrypt_data(token)
        # raw_decrypted format: customer_id|department|email|expiry
        parts = raw_decrypted.split("|")
        assert parts[0] == str(cust.id), f"Token customer_id mismatch: {parts[0]} != {cust.id}"
        assert parts[2] == "engineer@acmecorp.com"
        print(f"  [PASS] Authorized invite minted strictly for customer_id={parts[0]} (Acme Corp)")

        # 4. Also test generate_portal_link in requests.py (domain lockdown)
        from app.api.v1.endpoints.issuance.requests import generate_portal_link
        try:
            generate_portal_link(
                recipient_email="intruder@externaldomain.com",
                department="Legal",
                hours_valid=24,
                db=db,
                current_user=admin_user
            )
            assert False, "Should have blocked unauthorized domain on generate_portal_link"
        except HTTPException as e:
            assert e.status_code == 400
            assert "not authorized for your organization" in e.detail
            print("  [PASS] requests.generate_portal_link blocked unauthorized domain (HTTP 400)")

        portal_res = generate_portal_link(
            recipient_email="legal@acmecorp.com",
            department="Legal",
            hours_valid=24,
            db=db,
            current_user=admin_user
        )
        assert "token" in portal_res
        p_raw = decrypt_data(portal_res["token"])
        p_parts = p_raw.split("|")
        assert p_parts[0] == str(cust.id)
        assert p_parts[2] == "legal@acmecorp.com"
        print(f"  [PASS] requests.generate_portal_link minted strictly for customer_id={p_parts[0]}")
    finally:
        db.close()
    print("ALL ISSUANCE INVITE SECURITY CHECKS PASSED.")


if __name__ == "__main__":
    print("=" * 70)
    print("CRITICAL RELEASE CANDIDATE VERIFICATION SUITE")
    print("=" * 70)
    test_log_privacy_filter()
    test_public_issuance_ai_and_logger()
    test_quotation_history_winner_preservation()
    test_generate_invite_security()
    print("\n" + "=" * 70)
    print("ALL 4 CRITICAL ITEMS VERIFIED 100% SUCCESFULLY!")
    print("=" * 70)
