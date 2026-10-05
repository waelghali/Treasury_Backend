# tests/test_password_recovery_and_dr.py
"""
Test Suite for Sub-Phase 8.5: Self-Contained Password Recovery & Disaster Recovery.
Verifies:
1. HMAC-SHA256 time-bounded recovery token generation & validation.
2. Anti-tampering and expiration handling on tokens.
3. Strict cooldown rate-limiting (abuse prevention).
4. Re-enveloping during credential rotation with zero data loss.
5. Full disaster recovery health check audit over database tenant keys.
"""

import time
from app.database import SessionLocal
from app.models.models import Customer
from app.models.models_quotation import QuotationTenantKey, QuotationOffer
from app.services.tenant_key_service import tenant_key_service
from app.core.security_crypto import (
    generate_recovery_token,
    verify_recovery_token,
    check_recovery_rate_limit,
    clear_recovery_rate_limit
)


def test_hmac_recovery_token_roundtrip_and_tampering():
    test_email = "treasury_admin@corp.com"
    secret_salt = "super-secret-system-salt-key-999"

    # 1. Normal valid token
    token = generate_recovery_token(test_email, secret_salt, expiry_minutes=15)
    assert isinstance(token, str)
    assert len(token) > 40

    verified_email = verify_recovery_token(token, secret_salt)
    assert verified_email == test_email

    # 2. Tampered token payload
    tampered = token[:-4] + "AAAA"
    assert verify_recovery_token(tampered, secret_salt) is None

    # 3. Wrong secret salt
    assert verify_recovery_token(token, "wrong-salt") is None

    # 4. Expired token (negative expiry)
    expired_token = generate_recovery_token(test_email, secret_salt, expiry_minutes=-1)
    assert verify_recovery_token(expired_token, secret_salt) is None


def test_cooldown_rate_limiter_abuse_prevention():
    test_email = "rate_limit_target@enterprise.com"
    clear_recovery_rate_limit(test_email)

    # 3 attempts permitted within 1-hour window
    allowed1, rem1 = check_recovery_rate_limit(test_email, max_attempts=3, window_seconds=3600)
    assert allowed1 is True
    assert rem1 == 0

    allowed2, rem2 = check_recovery_rate_limit(test_email, max_attempts=3, window_seconds=3600)
    assert allowed2 is True

    allowed3, rem3 = check_recovery_rate_limit(test_email, max_attempts=3, window_seconds=3600)
    assert allowed3 is True

    # 4th attempt must be rejected by cooldown timer
    allowed4, rem4 = check_recovery_rate_limit(test_email, max_attempts=3, window_seconds=3600)
    assert allowed4 is False
    assert rem4 > 0
    print(f"[Pass] 4th recovery attempt rejected by cooldown timer ({rem4}s remaining)")

    # Reset clears cooldown
    clear_recovery_rate_limit(test_email)
    allowed5, _ = check_recovery_rate_limit(test_email, max_attempts=3, window_seconds=3600)
    assert allowed5 is True


def test_re_enveloping_and_zero_data_loss():
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for test"
        cust_id = cust.id

        # 1. Get initial DEK and encrypt a quote
        initial_dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        raw_price = 48.9150
        offer = QuotationOffer(
            assignment_id="test-re-envelope-asgn",
            price=0.0
        )
        tenant_key_service.apply_encrypted_offer_price(offer, raw_price, initial_dek)
        cipher_before = offer.encrypted_price

        # 2. Re-envelope tenant key (simulating credential change / master rotation)
        res = tenant_key_service.re_envelope_tenant_dek(db, cust_id)
        assert res["status"] == "RE_ENVELOPED_HEALTHY"
        assert res["key_version"] >= 2

        # 3. Retrieve new active DEK from database after cache invalidation
        new_dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        # The unsealed 32-byte raw DEK must remain identical so existing historical data is NOT lost
        assert new_dek == initial_dek

        # 4. Decrypt original quote with active DEK -> exactly identical
        resolved = tenant_key_service.resolve_offer_price(offer, new_dek)
        assert abs(resolved - raw_price) < 1e-4
        print(f"[Pass] Re-enveloping confirmed: Version {res['key_version']}, Historical quote resolved: {resolved}")

    finally:
        db.close()


def test_disaster_recovery_health_check_audit():
    db = SessionLocal()
    try:
        health = tenant_key_service.verify_tenant_keys_health(db)
        assert health is not None
        assert health["status"] == "HEALTHY"
        assert health["corrupted_keys"] == 0
        assert health["healthy_keys"] >= 1
        print(f"[Pass] DR Health Audit: {health['healthy_keys']} keys verified healthy in {health['audit_duration_seconds']}s (avg: {health['avg_unseal_latency_ms']}ms / key)")

    finally:
        db.close()


if __name__ == "__main__":
    tests = [
        test_hmac_recovery_token_roundtrip_and_tampering,
        test_cooldown_rate_limiter_abuse_prevention,
        test_re_enveloping_and_zero_data_loss,
        test_disaster_recovery_health_check_audit,
    ]

    print("=" * 60)
    print("Grow Treasury — Sub-Phase 8.5 Password Recovery & DR Tests")
    print("=" * 60)
    passed = 0
    for t in tests:
        t_name = t.__name__
        try:
            t()
            print(f"  [PASS] {t_name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {t_name}: {e}")
            raise
    print("-" * 60)
    print(f"ALL {passed} TESTS PASSED SUCCESSFULLY! (100% SUCCESS)")
    print("=" * 60)
