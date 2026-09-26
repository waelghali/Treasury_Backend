import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))
import uuid
from datetime import datetime, timezone, timedelta
from app.database import SessionLocal
from app.models.models_quotation import (
    QuotationRequest, QuotationLeg, QuotationBankAssignment,
    QuotationBankLegConfig, QuotationOffer, QuotationBank
)
from app.models import Customer, CustomerEntity, User, Bank

def run_stage1_verification():
    db = SessionLocal()
    print("=== STAGE 1: DATA MODEL & SCHEMA VERIFICATION ===")
    
    # 1. Backfill legacy RFQs missing legs (ensure 100% DB consistency)
    rfqs_missing_legs = db.query(QuotationRequest).filter(~QuotationRequest.legs.any()).all()
    print(f"[1/4] Checking legacy RFQs missing legs: found {len(rfqs_missing_legs)}")
    
    backfilled_count = 0
    for r in rfqs_missing_legs:
        leg = QuotationLeg(
            id=f"{r.id}-leg-1",
            rfq_id=r.id,
            leg_index=1,
            type=r.type or "FX_SPOT",
            direction=r.direction or "Buy",
            buy_currency=r.buy_currency or "USD",
            sell_currency=r.sell_currency or "EGP",
            amount=r.amount or 0.0,
            min_ticket_amount=r.min_ticket_amount,
            value_date=r.value_date,
            allow_alternative_value_date=r.allow_alternative_value_date or False,
            quotation_base=r.quotation_base or "Execution",
            max_tolerance_percent=r.max_tolerance_percent,
            status=r.status or "PENDING",
            entity_id=r.entity_id
        )
        db.add(leg)
        backfilled_count += 1
    
    if backfilled_count > 0:
        db.commit()
        print(f"      Successfully backfilled {backfilled_count} legacy RFQs with 1-leg baseline!")

    # 2. Backfill any legacy offers where leg_id is NULL
    null_leg_offers = db.query(QuotationOffer).filter(QuotationOffer.leg_id.is_(None)).all()
    print(f"[2/4] Checking legacy offers missing leg_id: found {len(null_leg_offers)}")
    offers_fixed = 0
    for off in null_leg_offers:
        assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == off.assignment_id).first()
        if assignment:
            default_leg = db.query(QuotationLeg).filter(QuotationLeg.rfq_id == assignment.rfq_id).order_by(QuotationLeg.leg_index.asc()).first()
            if default_leg:
                off.leg_id = default_leg.id
                offers_fixed += 1
    if offers_fixed > 0:
        db.commit()
        print(f"      Successfully linked {offers_fixed} legacy offers to their primary leg!")

    # 3. Create a test Multi-Pair RFQ session with 1 entity, 2 currency pairs, 2 banks, and per-pair tariffs
    print("[3/4] Creating Test Multi-Pair RFQ Session...")
    customer = db.query(Customer).filter(Customer.id == 1).first()
    if not customer:
        customer = db.query(Customer).first()
    entity = db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer.id).first()
    user = db.query(User).filter(User.customer_id == customer.id).first()
    q_banks = db.query(QuotationBank).filter(QuotationBank.customer_id == customer.id).limit(2).all()
    
    assert len(q_banks) >= 2, "Need at least 2 QuotationBanks for testing"

    test_rfq_id = f"test-stage1-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    
    test_rfq = QuotationRequest(
        id=test_rfq_id,
        ref_no=f"TEST-STAGE1-{uuid.uuid4().hex[:6].upper()}",
        customer_id=customer.id,
        created_by_user_id=user.id,
        type="FX_SPOT",
        status="OPEN",
        entity_id=entity.id if entity else None,
        window_start=now,
        window_end=now + timedelta(minutes=30),
        token_validity_hours=24
    )
    db.add(test_rfq)
    db.flush()

    # Pair 1: USD/EGP Buy $1,000,000
    leg1_id = f"{test_rfq_id}-leg-1"
    leg1 = QuotationLeg(
        id=leg1_id,
        rfq_id=test_rfq_id,
        leg_index=1,
        type="FX_SPOT",
        direction="Buy",
        buy_currency="USD",
        sell_currency="EGP",
        amount=1000000.0,
        value_date="2026-09-30",
        quotation_base="Execution",
        max_tolerance_percent=0.05,
        status="OPEN"
    )
    db.add(leg1)

    # Pair 2: EUR/EGP Buy €500,000
    leg2_id = f"{test_rfq_id}-leg-2"
    leg2 = QuotationLeg(
        id=leg2_id,
        rfq_id=test_rfq_id,
        leg_index=2,
        type="FX_SPOT",
        direction="Buy",
        buy_currency="EUR",
        sell_currency="EGP",
        amount=500000.0,
        value_date="2026-10-02",
        quotation_base="Execution",
        max_tolerance_percent=0.08,
        status="OPEN"
    )
    db.add(leg2)
    db.flush()

    # Bank Assignments: Bank A and Bank B (Each gets 1 single token for the RFQ session)
    token_a = f"token-bank-a-{uuid.uuid4().hex[:12]}"
    assign_a = QuotationBankAssignment(
        id=str(uuid.uuid4()),
        rfq_id=test_rfq_id,
        quotation_bank_id=q_banks[0].id,
        token=token_a
    )
    db.add(assign_a)

    token_b = f"token-bank-b-{uuid.uuid4().hex[:12]}"
    assign_b = QuotationBankAssignment(
        id=str(uuid.uuid4()),
        rfq_id=test_rfq_id,
        quotation_bank_id=q_banks[1].id,
        token=token_b
    )
    db.add(assign_b)
    db.flush()

    # Per-Bank Per-Pair Configurations:
    # Bank A: Leg 1 tariff 0.05%, min $50; Leg 2 tariff 0.10%, min $100
    cfg_a_leg1 = QuotationBankLegConfig(
        id=str(uuid.uuid4()),
        assignment_id=assign_a.id,
        leg_id=leg1_id,
        cost_percent=0.05,
        cost_min=50.0,
        cost_flat=10.0,
        quotation_base="Execution"
    )
    cfg_a_leg2 = QuotationBankLegConfig(
        id=str(uuid.uuid4()),
        assignment_id=assign_a.id,
        leg_id=leg2_id,
        cost_percent=0.10,
        cost_min=100.0,
        cost_flat=20.0,
        quotation_base="Execution"
    )
    db.add_all([cfg_a_leg1, cfg_a_leg2])

    # Bank B: Leg 1 tariff 0.04%, min $40; Leg 2 tariff 0.08%, min $80
    cfg_b_leg1 = QuotationBankLegConfig(
        id=str(uuid.uuid4()),
        assignment_id=assign_b.id,
        leg_id=leg1_id,
        cost_percent=0.04,
        cost_min=40.0,
        cost_flat=5.0,
        quotation_base="Execution"
    )
    cfg_b_leg2 = QuotationBankLegConfig(
        id=str(uuid.uuid4()),
        assignment_id=assign_b.id,
        leg_id=leg2_id,
        cost_percent=0.08,
        cost_min=80.0,
        cost_flat=15.0,
        quotation_base="Execution"
    )
    db.add_all([cfg_b_leg1, cfg_b_leg2])

    # Quotes: Bank A quotes both pairs; Bank B quotes Pair 1
    offer_a_leg1 = QuotationOffer(
        assignment_id=assign_a.id,
        leg_id=leg1_id,
        price=49.45,
        submitted_by_email="traderA@bank.com",
        notes="Best firm execution rate for USD"
    )
    offer_a_leg2 = QuotationOffer(
        assignment_id=assign_a.id,
        leg_id=leg2_id,
        price=54.10,
        submitted_by_email="traderA@bank.com",
        notes="Firm rate for EUR"
    )
    offer_b_leg1 = QuotationOffer(
        assignment_id=assign_b.id,
        leg_id=leg1_id,
        price=49.42,
        submitted_by_email="traderB@bank.com",
        notes="Tight spread"
    )
    db.add_all([offer_a_leg1, offer_a_leg2, offer_b_leg1])
    db.commit()

    # 4. Verify loaded relationships
    print("[4/4] Verifying loaded multi-pair object tree...")
    reloaded_rfq = db.query(QuotationRequest).filter(QuotationRequest.id == test_rfq_id).first()
    assert reloaded_rfq is not None
    assert len(reloaded_rfq.legs) == 2, f"Expected 2 legs, got {len(reloaded_rfq.legs)}"
    assert len(reloaded_rfq.assignments) == 2, f"Expected 2 assignments, got {len(reloaded_rfq.assignments)}"

    reloaded_assign_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == assign_a.id).first()
    cfg1 = reloaded_assign_a.get_config_for_leg(leg1_id)
    cfg2 = reloaded_assign_a.get_config_for_leg(leg2_id)
    assert cfg1.cost_percent == 0.05, f"Expected 0.05, got {cfg1.cost_percent}"
    assert cfg2.cost_percent == 0.10, f"Expected 0.10, got {cfg2.cost_percent}"

    reloaded_leg1 = db.query(QuotationLeg).filter(QuotationLeg.id == leg1_id).first()
    assert len(reloaded_leg1.offers) == 2, f"Expected 2 offers for leg 1, got {len(reloaded_leg1.offers)}"
    reloaded_leg2 = db.query(QuotationLeg).filter(QuotationLeg.id == leg2_id).first()
    assert len(reloaded_leg2.offers) == 1, f"Expected 1 offer for leg 2, got {len(reloaded_leg2.offers)}"

    print("      Verification successful: All assertions passed!")
    print(f"      - RFQ ID: {reloaded_rfq.id}")
    print(f"      - Pair 1 ({reloaded_leg1.buy_currency}/{reloaded_leg1.sell_currency}): Amount={reloaded_leg1.amount}, Quotes={len(reloaded_leg1.offers)}")
    print(f"      - Pair 2 ({reloaded_leg2.buy_currency}/{reloaded_leg2.sell_currency}): Amount={reloaded_leg2.amount}, Quotes={len(reloaded_leg2.offers)}")
    print(f"      - Bank A Token: {reloaded_assign_a.token} (Single token for session)")
    print(f"      - Bank A Tariffs: Pair 1 = {cfg1.cost_percent}%, Pair 2 = {cfg2.cost_percent}%")

    # Clean up test RFQ
    db.delete(reloaded_rfq)
    db.commit()
    print("      Cleaned up test RFQ cleanly.")

    # 5. Check all existing RFQs now have at least 1 leg
    rfqs_still_missing = db.query(QuotationRequest).filter(~QuotationRequest.legs.any()).count()
    total_rfqs = db.query(QuotationRequest).count()
    print(f"      Total RFQs in DB: {total_rfqs}, Missing legs: {rfqs_still_missing}")
    assert rfqs_still_missing == 0, "All RFQs must have at least 1 leg for backward compatibility!"

    db.close()
    print("=== STAGE 1 VERIFICATION COMPLETED WITH 100% SUCCESS! ===")

if __name__ == "__main__":
    run_stage1_verification()
