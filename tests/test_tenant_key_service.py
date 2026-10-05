# tests/test_tenant_key_service.py
"""
Test Suite for Sub-Phase 8.2: Tenant Key Store & Dual-Read Database Layer.
Verifies:
1. Automated on-demand provisioning and persistence of QuotationTenantKey.
2. Unwrapping and caching of Tenant DEK.
3. Dual-write and dual-read for FX Spot Offers (QuotationOffer).
4. Dual-write and dual-read for T-Bill Offers (QuotationTBillOffer).
5. Safe fallback to legacy plaintext columns when encrypted columns are null.
"""

from app.database import SessionLocal
from app.models.models_quotation import QuotationTenantKey, QuotationOffer, QuotationTBillOffer
from app.models.models import Customer
from app.services.tenant_key_service import tenant_key_service
from app.core.security_crypto import is_encrypted


def test_tenant_key_provisioning_and_caching():
    db = SessionLocal()
    try:
        # Find an existing customer or create dummy
        cust = db.query(Customer).first()
        assert cust is not None, "At least one customer must exist in database for test"
        cust_id = cust.id

        # Clean existing key if any for a clean test run
        db.query(QuotationTenantKey).filter(QuotationTenantKey.customer_id == cust_id).delete()
        db.commit()
        tenant_key_service.clear_cache(cust_id)

        # 1. Provision new Tenant DEK
        dek1 = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        assert isinstance(dek1, bytes)
        assert len(dek1) == 32

        # Verify record in database
        rec = db.query(QuotationTenantKey).filter(QuotationTenantKey.customer_id == cust_id).first()
        assert rec is not None
        assert rec.status == "ACTIVE"
        assert len(rec.wrapped_dek) > 30

        # 2. Retrieve again (hits memory cache)
        dek2 = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        assert dek1 == dek2

        # 3. Retrieve after clearing cache (unwraps from DB)
        tenant_key_service.clear_cache(cust_id)
        dek3 = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        assert dek1 == dek3

    finally:
        db.close()


def test_offer_price_dual_read_and_dual_write():
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        cust_id = cust.id
        dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)

        # Test Case A: New encrypted offer (dual-write)
        offer = QuotationOffer(
            assignment_id="test-assign-1",
            price=0.0
        )
        test_price = 49.6250
        tenant_key_service.apply_encrypted_offer_price(offer, test_price, dek)

        assert is_encrypted(offer.encrypted_price)
        assert offer.price == test_price
        resolved = tenant_key_service.resolve_offer_price(offer, dek)
        assert abs(resolved - test_price) < 0.00001

        # Test Case B: Legacy unencrypted offer (dual-read fallback)
        legacy_offer = QuotationOffer(
            assignment_id="test-assign-legacy",
            price=48.2500,
            encrypted_price=None
        )
        resolved_legacy = tenant_key_service.resolve_offer_price(legacy_offer, dek)
        assert resolved_legacy == 48.2500

    finally:
        db.close()


def test_tbill_offer_dual_read_and_dual_write():
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        cust_id = cust.id
        dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)

        # Test Case A: New encrypted T-Bill offer
        tbill = QuotationTBillOffer(
            assignment_id="test-tbill-1",
            settlement_date="2026-10-10",
            maturity_date="2027-01-10",
            discount_rate=0.0,
            max_amount=0.0
        )
        test_rate = 26.50
        test_amt = 10000000.0
        tenant_key_service.apply_encrypted_tbill_offer(tbill, test_rate, test_amt, dek)

        assert is_encrypted(tbill.encrypted_discount_rate)
        assert is_encrypted(tbill.encrypted_max_amount)
        assert tbill.discount_rate == test_rate
        assert tbill.max_amount == test_amt

        resolved_rate = tenant_key_service.resolve_tbill_discount_rate(tbill, dek)
        assert abs(resolved_rate - test_rate) < 0.0001

        # Test Case B: Legacy T-Bill offer (dual-read fallback)
        legacy_tbill = QuotationTBillOffer(
            assignment_id="test-tbill-legacy",
            settlement_date="2026-10-10",
            maturity_date="2027-01-10",
            discount_rate=25.80,
            max_amount=5000000.0,
            encrypted_discount_rate=None,
            encrypted_max_amount=None
        )
        resolved_legacy_rate = tenant_key_service.resolve_tbill_discount_rate(legacy_tbill, dek)
        assert resolved_legacy_rate == 25.80

    finally:
        db.close()


if __name__ == "__main__":
    tests = [
        test_tenant_key_provisioning_and_caching,
        test_offer_price_dual_read_and_dual_write,
        test_tbill_offer_dual_read_and_dual_write,
    ]

    print("=" * 60)
    print("Grow Treasury — Sub-Phase 8.2 Tenant Key Service Test Suite")
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
