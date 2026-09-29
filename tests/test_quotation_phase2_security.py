import time
from app.core.otp_security import hash_otp_code, verify_otp_code, MAX_OTP_FAILED_ATTEMPTS
from app.core.rate_limiter import SlidingWindowRateLimiter
from app.models.models_quotation import QuotationAccessOTP

def test_otp_hashing_and_verification():
    raw_code = "749281"
    hashed = hash_otp_code(raw_code)
    
    # Assert hash is 64 hex characters (SHA-256)
    assert len(hashed) == 64
    assert hashed != raw_code
    
    # Assert verification succeeds with correct code
    assert verify_otp_code(raw_code, hashed) is True
    
    # Assert verification fails with incorrect code
    assert verify_otp_code("123456", hashed) is False
    assert verify_otp_code("", hashed) is False
    assert verify_otp_code("749282", hashed) is False

def test_otp_legacy_plaintext_backward_compatibility():
    legacy_code = "839102"
    # In case an unexpired legacy row exists in DB before migration:
    assert verify_otp_code("839102", legacy_code) is True
    assert verify_otp_code("000000", legacy_code) is False

def test_sliding_window_rate_limiter():
    limiter = SlidingWindowRateLimiter()
    key = "test_user_ip_1"
    
    # Limit: max 3 requests per 2 seconds
    for _ in range(3):
        is_limited, _ = limiter.is_rate_limited(key, max_requests=3, window_seconds=2)
        assert is_limited is False
        
    # 4th request must be rate limited
    is_limited, retry_after = limiter.is_rate_limited(key, max_requests=3, window_seconds=2)
    assert is_limited is True
    assert retry_after > 0
    
    # Reset key
    limiter.reset_key(key)
    is_limited, _ = limiter.is_rate_limited(key, max_requests=3, window_seconds=2)
    assert is_limited is False

def test_quotation_access_otp_model_attributes():
    # Verify the model has the failed_attempts column
    assert hasattr(QuotationAccessOTP, "failed_attempts")
    assert MAX_OTP_FAILED_ATTEMPTS == 3

from app.core.otp_security import generate_scoped_deal_receipt

def test_scoped_deal_receipt():
    legs = [
        {"pair": "EUR/EGP", "direction": "BUY", "amount": 1000000.0, "rate": 53.4500, "value_date": "2026-10-05"},
        {"pair": "USD/EGP", "direction": "BUY", "amount": 2500000.0, "rate": 48.7200, "value_date": "2026-10-05"}
    ]
    receipt = generate_scoped_deal_receipt(
        rfq_id="test-rfq-123",
        ref_no="RFQ-2026-0042",
        customer_name="Acme Corp",
        bank_id=4,
        bank_name="Test Commercial Bank",
        executed_legs=legs,
        executed_at="2026-09-29 14:30:00 UTC"
    )
    
    assert receipt["receipt_id"].startswith("RCP-RFQ-2026-0042-4-")
    assert receipt["signature_hash"].startswith("SHA256:")
    assert len(receipt["signature_hash"]) == 71  # "SHA256:" (7) + 64 hex chars
    assert receipt["scoped_legs_count"] == 2
    
    # Tampering test: different bank id or rate produces different signature
    receipt_tampered = generate_scoped_deal_receipt(
        rfq_id="test-rfq-123",
        ref_no="RFQ-2026-0042",
        customer_name="Acme Corp",
        bank_id=5,  # different bank
        bank_name="Test Commercial Bank",
        executed_legs=legs,
        executed_at="2026-09-29 14:30:00 UTC"
    )
    assert receipt["signature_hash"] != receipt_tampered["signature_hash"]

if __name__ == "__main__":
    test_otp_hashing_and_verification()
    test_otp_legacy_plaintext_backward_compatibility()
    test_sliding_window_rate_limiter()
    test_quotation_access_otp_model_attributes()
    test_scoped_deal_receipt()
    print("All Phase 2 security tests PASSED successfully!")
