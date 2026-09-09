from app.services.collaborative_learning_service import collaborative_service

def test_privacy_sanitizer_shield():
    # Test 1: Full raw banking narrative with IBAN, amounts, dates, and reference codes
    raw_desc = "TRF TO VODAFONE CASH EG3800020001000000123456789 DATE 2026-08-15 INV #99482 EGP 1,250.00"
    signature = collaborative_service.sanitize_signature(raw_desc)
    assert signature is not None
    assert "EG3800020001000000123456789" not in signature
    assert "99482" not in signature
    assert "2026" not in signature
    assert "1,250" not in signature
    assert "VODAFONE CASH" in signature

def test_privacy_sanitizer_instapay():
    # Test 2: InstaPay transfer fee narrative
    raw_desc = "COMMISSION / CHG ON INSTAPAY TRANSFER REF/987654321 VAL 2026/08/01 USD 5.00"
    signature = collaborative_service.sanitize_signature(raw_desc)
    assert signature is not None
    assert "987654321" not in signature
    assert "INSTAPAY" in signature

def test_privacy_sanitizer_aws():
    # Test 3: International SaaS provider
    raw_desc = "POS W/D AMAZON WEB SERVICES AWS.AMAZON.CO WA US DATE 12/08/2026 CARD 4111-XXXX-XXXX-1111 $350.00"
    signature = collaborative_service.sanitize_signature(raw_desc)
    assert signature is not None
    assert "4111" not in signature
    assert "350" not in signature
    assert "AMAZON WEB SERVICES" in signature

def test_privacy_sanitizer_arabic_utility():
    # Test 4: Arabic utility bill
    raw_desc = "سداد فاتورة شركة جنوب القاهرة لتوزيع الكهرباء رقم 48201948 بمبلغ 4500 جنيه"
    signature = collaborative_service.sanitize_signature(raw_desc)
    assert signature is not None
    assert "48201948" not in signature
    assert "4500" not in signature
    assert "جنوب القاهرة" in signature

def test_privacy_sanitizer_discards_pure_noise():
    # Test 5: Meaningless generic stop words should return None
    raw_desc = "TRF NO 99482 12345 500 EGP"
    signature = collaborative_service.sanitize_signature(raw_desc)
    assert signature is None

def test_collaborative_consensus_and_promotion():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.database import Base
    from app.models.models_reconciliation_v2 import CollaborativePattern, CollaborativeTenantVote

    # In-memory SQLite for isolated test
    test_engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(test_engine, tables=[CollaborativePattern.__table__, CollaborativeTenantVote.__table__])
    TestSession = sessionmaker(bind=test_engine)
    db = TestSession()

    desc = "COMMISSION INSTAPAY TRANSFER REF 99214 DATE 2026-08-01 5.00 EGP"

    # 1. Company 1 votes -> 1 tenant, not promoted yet
    p1 = collaborative_service.submit_confirmation(
        db=db, company_id=101, bank_id=5, raw_description=desc,
        direction="DEBIT", category="BANK_CHARGES", gl_account="BANK_CHARGES"
    )
    assert p1 is not None
    assert p1.distinct_tenants_count == 1
    assert p1.is_promoted is False
    assert p1.total_confirmations == 1

    # 2. Company 1 votes 5 more times -> distinct_tenants_count stays 1 (anti-gaming)
    for _ in range(5):
        p1 = collaborative_service.submit_confirmation(
            db=db, company_id=101, bank_id=5, raw_description=desc,
            direction="DEBIT", category="BANK_CHARGES", gl_account="BANK_CHARGES"
        )
    assert p1.distinct_tenants_count == 1
    assert p1.total_confirmations == 6
    assert p1.is_promoted is False

    # 3. Company 2 with same bank (bank_id=5) votes -> K=2 reached -> PROMOTED for this bank!
    p2 = collaborative_service.submit_confirmation(
        db=db, company_id=102, bank_id=5, raw_description=desc,
        direction="DEBIT", category="BANK_CHARGES", gl_account="BANK_CHARGES"
    )
    assert p2.distinct_tenants_count == 2
    assert p2.is_promoted is True
    assert p2.confidence_score >= 70

    # 4. Now a live transaction from Company 3 banking with bank_id=5 should match!
    live_match = collaborative_service.match_collaborative(
        db=db, bank_id=5, raw_description="COMMISSION ON INSTAPAY TRANSFER REF 1111 10 EGP", direction="DEBIT"
    )
    assert live_match is not None
    assert live_match["category"] == "BANK_CHARGES"
    assert live_match["distinct_tenants"] == 2
    assert live_match["is_bank_specific"] is True

    db.close()

def test_dynamic_decay_of_stale_patterns():
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.database import Base
    from app.models.models_reconciliation_v2 import CollaborativePattern, CollaborativeTenantVote

    test_engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(test_engine, tables=[CollaborativePattern.__table__, CollaborativeTenantVote.__table__])
    TestSession = sessionmaker(bind=test_engine)
    db = TestSession()

    # Create an old promoted pattern last confirmed 200 days ago
    old_date = datetime.now(timezone.utc) - timedelta(days=200)
    stale_pattern = CollaborativePattern(
        bank_id=None,
        signature_token="OBSOLETE BANK CODE",
        direction="DEBIT",
        suggested_category="OLD_EXPENSE",
        distinct_tenants_count=3,
        total_confirmations=10,
        agreement_score=100.0,
        confidence_score=52,
        is_promoted=True,
        last_confirmed_at=old_date
    )
    db.add(stale_pattern)
    db.commit()

    # Run decay with 180 days cutoff
    result = collaborative_service.decay_stale_patterns(db, inactive_days=180, decay_factor=0.8, min_confidence=50)
    assert result["evaluated_stale_patterns"] == 1
    assert result["decayed_count"] == 1
    assert result["demoted_count"] == 1

    # Check that the pattern was demoted
    db.refresh(stale_pattern)
    assert stale_pattern.confidence_score < 50
    assert stale_pattern.is_promoted is False

    db.close()

if __name__ == "__main__":
    test_privacy_sanitizer_shield()
    test_privacy_sanitizer_instapay()
    test_privacy_sanitizer_aws()
    test_privacy_sanitizer_arabic_utility()
    test_privacy_sanitizer_discards_pure_noise()
    test_collaborative_consensus_and_promotion()
    test_dynamic_decay_of_stale_patterns()
    print("ALL_COLLABORATIVE_LEARNING_TESTS_PASSED")

