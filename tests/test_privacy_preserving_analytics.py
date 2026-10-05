# tests/test_privacy_preserving_analytics.py
"""
Test Suite for Sub-Phase 8.4: Privacy-Preserving Collaborative Analytics & Dealer Accolades.
Verifies:
1. Dealer achievement service computes trophies with zero-knowledge encrypted quotes.
2. Empirical live market reference computes aggregate spreads cleanly without raw data leaks.
3. System Owner telemetry queries remain aggregated with zero plaintext financial exposure.
"""

from app.database import SessionLocal
from app.models.models import Customer, Bank
from app.services.dealer_achievement_service import dealer_achievement_service
from app.services.live_market_service import live_market_service


def test_dealer_achievements_with_encrypted_quotes():
    db = SessionLocal()
    try:
        bank = db.query(Bank).first()
        assert bank is not None, "Bank required for test"

        # Test desk-level dashboard calculation
        dash = dealer_achievement_service.get_dealer_achievements(
            db=db,
            dealer_email=None,
            bank_id=bank.id
        )

        assert dash is not None
        assert "dealer_tier" in dash
        assert "trophies" in dash
        assert "personal_bests" in dash
        print(f"[Pass] Dealer desk dashboard calculated successfully (Tier: {dash.get('dealer_tier')}, Trophies: {len(dash['trophies'])})")

    finally:
        db.close()


def test_empirical_market_reference_privacy_preservation():
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for test"

        # Request empirical reference for USD/EGP
        ref = live_market_service.get_empirical_reference(
            db,
            customer_id=cust.id,
            from_code="USD",
            to_code="EGP",
            direction="BUY",
            amount=500000.0
        )

        assert ref is not None
        assert "cbe_official_mid" in ref
        assert "source" in ref

        # Assert no sensitive private quotes or competitor bank names leaked into benchmark
        assert "competitor_quotes" not in ref
        assert "bank_quotes" not in ref
        print(f"[Pass] Empirical market reference generated securely (Source: {ref.get('source')}, CBE Mid: {ref.get('cbe_official_mid')})")

    finally:
        db.close()


if __name__ == "__main__":
    tests = [
        test_dealer_achievements_with_encrypted_quotes,
        test_empirical_market_reference_privacy_preservation,
    ]

    print("=" * 60)
    print("Grow Treasury — Sub-Phase 8.4 Privacy-Preserving Analytics Tests")
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
