# tests/test_security_crypto.py
"""
Test Suite for Phase 8.1 Zero-Knowledge Cryptographic Engine.
Verifies:
1. KeyProvider wrap/unwrap round-trip & tenant binding.
2. Field encryption/decryption across native types (float, int, str, dict).
3. Authenticated encryption integrity (bit-flip tampering & AAD context mismatches).
4. Cross-tenant isolation (wrong DEK rejection).
5. Dual-Read backwards compatibility with legacy unencrypted fields.
6. Execution throughput / latency benchmarking.
"""

import time
from contextlib import contextmanager

from app.core.security_crypto import (
    LocalServerKeyProvider,
    generate_tenant_dek,
    encrypt_field,
    decrypt_field,
    encrypt_json,
    decrypt_json,
    is_encrypted,
    TamperDetectedError,
    DecryptionError,
    InvalidKeyError
)


@contextmanager
def assert_raises(exc_type):
    try:
        yield
    except exc_type:
        return
    except Exception as e:
        raise AssertionError(f"Expected {exc_type.__name__}, got {type(e).__name__}: {e}")
    raise AssertionError(f"Expected {exc_type.__name__}, but no exception was raised.")


def test_key_provider_wrap_unwrap_roundtrip():
    provider = LocalServerKeyProvider(master_secret="test-master-secret-key-12345")
    tenant_dek = generate_tenant_dek()
    tenant_id = "customer_101"

    wrapped = provider.wrap_dek(tenant_dek, tenant_id)
    assert isinstance(wrapped, str)
    assert len(wrapped) > 30

    unwrapped = provider.unwrap_dek(wrapped, tenant_id)
    assert unwrapped == tenant_dek


def test_key_provider_tenant_binding_isolation():
    provider = LocalServerKeyProvider(master_secret="test-master-secret-key-12345")
    tenant_dek = generate_tenant_dek()
    
    # Wrapped under customer_101
    wrapped = provider.wrap_dek(tenant_dek, "customer_101")

    # Attempting to unwrap under customer_202 must fail immediately due to AAD mismatch
    with assert_raises(TamperDetectedError):
        provider.unwrap_dek(wrapped, "customer_202")


def test_field_encryption_types_roundtrip():
    dek = generate_tenant_dek()

    # Float test (FX Rates)
    rate = 48.5525
    enc_rate = encrypt_field(rate, dek, field_context="price")
    assert is_encrypted(enc_rate)
    assert decrypt_field(enc_rate, dek, field_context="price") == rate

    # Int test (T-Bill ticket or nominal)
    amount = 50000000
    enc_amount = encrypt_field(amount, dek, field_context="amount")
    assert decrypt_field(enc_amount, dek, field_context="amount") == amount

    # String test (Dealer confidential notes)
    notes = "Confidential quote valid for 30s only"
    enc_notes = encrypt_field(notes, dek, field_context="notes")
    assert decrypt_field(enc_notes, dek, field_context="notes") == notes

    # Dict test (Complex breakdown)
    data = {"spread_bps": 14.5, "markup": 0.0025, "quote_id": 992}
    enc_json = encrypt_json(data, dek, context="breakdown")
    assert decrypt_json(enc_json, dek, context="breakdown") == data

    # None test
    assert encrypt_field(None, dek) is None
    assert decrypt_field(None, dek) is None


def test_context_binding_anti_swapping():
    """Verifies that ciphertext encrypted for 'price' cannot be read as 'spread'."""
    dek = generate_tenant_dek()
    rate = 50.1234
    enc_rate = encrypt_field(rate, dek, field_context="price")

    with assert_raises(TamperDetectedError):
        decrypt_field(enc_rate, dek, field_context="spread")


def test_tamper_detection_on_ciphertext_bitflip():
    dek = generate_tenant_dek()
    rate = 49.8000
    enc_rate = encrypt_field(rate, dek, field_context="price")

    # Payload structure is enc:v1:{nonce_b64}:{ct_b64}
    prefix, v, nonce_b64, ct_b64 = enc_rate.split(":")
    import base64
    ct_bytes = bytearray(base64.b64decode(ct_b64))
    ct_bytes[0] ^= 0x01  # Flip 1 bit cleanly in ciphertext
    tampered_ct = base64.b64encode(ct_bytes).decode("ascii")
    tampered_payload = f"enc:v1:{nonce_b64}:{tampered_ct}"

    with assert_raises(TamperDetectedError):
        decrypt_field(tampered_payload, dek, field_context="price")


def test_cross_tenant_wrong_dek_rejection():
    dek_tenant_a = generate_tenant_dek()
    dek_tenant_b = generate_tenant_dek()

    secret_deal = 48.9500
    enc_deal = encrypt_field(secret_deal, dek_tenant_a, field_context="price")

    with assert_raises(TamperDetectedError):
        decrypt_field(enc_deal, dek_tenant_b, field_context="price")


def test_dual_read_backwards_compatibility():
    dek = generate_tenant_dek()

    # Legacy raw float/int/string stored before Phase 8
    legacy_price = 48.5000
    legacy_text = "Legacy plain notes"

    assert not is_encrypted(legacy_price)
    assert not is_encrypted(legacy_text)

    # Calling decrypt_field on legacy data safely returns it unchanged
    assert decrypt_field(legacy_price, dek, field_context="price") == legacy_price
    assert decrypt_field(legacy_text, dek, field_context="notes") == legacy_text


def test_performance_benchmark():
    dek = generate_tenant_dek()
    sample_rate = 48.6543
    iterations = 1000

    start_t = time.perf_counter()
    for _ in range(iterations):
        enc = encrypt_field(sample_rate, dek, field_context="price")
        _ = decrypt_field(enc, dek, field_context="price")
    duration = time.perf_counter() - start_t

    avg_ms = (duration / iterations) * 1000.0
    print(f"\n[Bench] 1,000 AES-256-GCM Encrypt+Decrypt cycles: {duration:.4f}s ({avg_ms:.4f}ms / op)")
    # Must be sub-millisecond (typically ~0.015ms per operation on modern CPUs)
    assert avg_ms < 1.0, f"Cryptographic overhead too high: {avg_ms}ms"


if __name__ == "__main__":
    tests = [
        test_key_provider_wrap_unwrap_roundtrip,
        test_key_provider_tenant_binding_isolation,
        test_field_encryption_types_roundtrip,
        test_context_binding_anti_swapping,
        test_tamper_detection_on_ciphertext_bitflip,
        test_cross_tenant_wrong_dek_rejection,
        test_dual_read_backwards_compatibility,
        test_performance_benchmark,
    ]

    print("=" * 60)
    print("Grow Treasury — Phase 8.1 Zero-Knowledge Crypto Test Suite")
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
