import sys
import os
from datetime import datetime, timezone, timedelta, date

# Ensure root app directory is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.models.models_quotation import QuotationRequest, QuotationLeg, QuotationBankAssignment, QuotationBankLegConfig, QuotationBank, QuotationOffer
from app.schemas.schemas_quotation import QuotationRequestCreate, QuotationLegCreate, FXSpotOfferCreate, FXSpotMultiOfferCreate, FXSpotOfferItem
from app.crud.crud_quotation import crud_quotation
from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings
from app.api.v1.endpoints.public_quotations import get_rfq_by_token, submit_fx_offer, submit_fx_offers_batch, get_live_rank

def run_stage2_tests():
    db = SessionLocal()
    print("==================================================")
    print("STAGE 2 TEST: Multi-Pair Calculation Engine & API")
    print("==================================================")

    try:
        # 1. Fetch available quotation banks for customer 1
        banks = db.query(QuotationBank).filter(QuotationBank.customer_id == 1).limit(2).all()
        if len(banks) < 2:
            print("ERROR: Need at least 2 active quotation banks for customer 1.")
            return

        bank_a = banks[0]
        bank_b = banks[1]
        print(f"Selected Bank A: ID {bank_a.id} ({bank_a.bank.name if bank_a.bank else 'Bank 1'})")
        print(f"Selected Bank B: ID {bank_b.id} ({bank_b.bank.name if bank_b.bank else 'Bank 2'})")

        from app.models.models import User
        user = db.query(User).filter(User.customer_id == 1).first()
        test_user_id = user.id if user else 1
        print(f"Using Test User ID: {test_user_id}")

        # 2. Build Multi-Pair QuotationRequestCreate
        now = datetime.now(timezone.utc)
        val_date = (now + timedelta(days=2)).date()

        leg1_in = QuotationLegCreate(
            direction="Buy",
            buyCurrency="USD",
            sellCurrency="EGP",
            amount=100000.0,
            valueDate=val_date,
            allowAlternativeValueDate=True,
            quotationBase="Execution",
            maxTolerancePercent=0.05,
            selectedBanks=[
                {"id": bank_a.id, "costPercent": 0.1, "quotationBase": "Execution", "valueDate": str(val_date)},
                {"id": bank_b.id, "costPercent": 0.2, "quotationBase": "Execution", "valueDate": str(val_date)}
            ]
        )
        leg2_in = QuotationLegCreate(
            direction="Buy",
            buyCurrency="EUR",
            sellCurrency="EGP",
            amount=50000.0,
            valueDate=val_date,
            allowAlternativeValueDate=True,
            quotationBase="Execution",
            maxTolerancePercent=0.05,
            selectedBanks=[
                {"id": bank_a.id, "costPercent": 0.2, "quotationBase": "Execution", "valueDate": str(val_date)},
                {"id": bank_b.id, "costPercent": 0.05, "quotationBase": "Execution", "valueDate": str(val_date)}
            ]
        )

        req_in = QuotationRequestCreate(
            type="FX_SPOT",
            windowStart=now,
            windowEnd=now + timedelta(hours=1),
            pairs=[leg1_in, leg2_in],
            legalDisclaimerAccepted=True
        )

        print("\n--- Step 1: Creating Multi-Pair RFQ via crud.create_request ---")
        rfq, created_assignments = crud_quotation.create_request(db, customer_id=1, user_id=test_user_id, requires_approval=False, obj_in=req_in)
        print(f"Created RFQ Ref: {rfq.ref_no}, ID: {rfq.id}")
        assert len(rfq.legs) == 2, f"Expected 2 legs, got {len(rfq.legs)}"
        assert len(rfq.assignments) == 2, f"Expected 2 assignments, got {len(rfq.assignments)}"
        
        # Verify leg configs
        for a in rfq.assignments:
            print(f"Assignment ID {a.id}, Token: {a.token[:8]}..., Leg Configs count: {len(a.leg_configs)}")
            assert len(a.leg_configs) == 2, f"Expected 2 leg_configs for assignment {a.id}, got {len(a.leg_configs)}"

        leg_usd = [l for l in rfq.legs if l.currency_pair == "USD/EGP"][0]
        leg_eur = [l for l in rfq.legs if l.currency_pair == "EUR/EGP"][0]
        print(f"Leg USD ID: {leg_usd.id}, Leg EUR ID: {leg_eur.id}")

        assignment_a = [a for a in rfq.assignments if a.quotation_bank_id == bank_a.id][0]
        assignment_b = [a for a in rfq.assignments if a.quotation_bank_id == bank_b.id][0]

        # Ensure rfq window is active for testing
        rfq.window_start = now - timedelta(minutes=5)
        rfq.window_end = now + timedelta(minutes=55)
        assignment_a.approval_status = 'APPROVED'
        assignment_b.approval_status = 'APPROVED'
        db.commit()

        # 3. Test Bank Portal get_rfq_by_token
        print("\n--- Step 2: Testing get_rfq_by_token for Bank A & B ---")
        import asyncio
        portal_data_a = asyncio.run(get_rfq_by_token(assignment_a.token, db))
        assert "legs" in portal_data_a, "Missing 'legs' key in portal response"
        assert len(portal_data_a["legs"]) == 2, f"Expected 2 portal legs, got {len(portal_data_a['legs'])}"
        print(f"Bank A Portal Legs: {[l['currency_pair'] for l in portal_data_a['legs']]}")
        print(f"Bank A Leg USD Tariff: cost_percent={portal_data_a['legs'][0]['cost_percent']}")

        # 4. Bank Quoting (Simulate Quotes)
        print("\n--- Step 3: Bank Quoting ---")
        # Bank A quotes USD/EGP @ 49.10 (lower is better for corporate BUY), EUR/EGP @ 53.60
        res_a = submit_fx_offers_batch(
            FXSpotMultiOfferCreate(
                token=assignment_a.token,
                quotes=[
                    FXSpotOfferItem(leg_id=leg_usd.id, price=49.10, offered_value_date=str(val_date)),
                    FXSpotOfferItem(leg_id=leg_eur.id, price=53.60, offered_value_date=str(val_date))
                ],
                email="dealer.a@bank.com"
            ),
            db
        )
        print(f"Bank A Submitted: {res_a['quotes_count']} quote(s)")

        # Bank B quotes USD/EGP @ 49.30 (higher/worse for BUY), EUR/EGP @ 53.10 (lower/better for BUY)
        res_b = submit_fx_offers_batch(
            FXSpotMultiOfferCreate(
                token=assignment_b.token,
                quotes=[
                    FXSpotOfferItem(leg_id=leg_usd.id, price=49.30, offered_value_date=str(val_date)),
                    FXSpotOfferItem(leg_id=leg_eur.id, price=53.10, offered_value_date=str(val_date))
                ],
                email="dealer.b@bank.com"
            ),
            db
        )
        print(f"Bank B Submitted: {res_b['quotes_count']} quote(s)")

        # 5. Live Rank Verification
        print("\n--- Step 4: Testing Live Ranking per Leg ---")
        from app.services.live_ranking_service import live_ranking_service
        # Direct calculation test:
        rank_a_usd = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment_a.id, leg_id=leg_usd.id)
        rank_a_eur = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment_a.id, leg_id=leg_eur.id)
        rank_b_usd = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment_b.id, leg_id=leg_usd.id)
        rank_b_eur = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment_b.id, leg_id=leg_eur.id)

        print(f"Bank A Live Rank (USD/EGP): Rank {rank_a_usd}")
        print(f"Bank A Live Rank (EUR/EGP): Rank {rank_a_eur}")
        print(f"Bank B Live Rank (USD/EGP): Rank {rank_b_usd}")
        print(f"Bank B Live Rank (EUR/EGP): Rank {rank_b_eur}")

        assert rank_a_usd == 1, f"Expected Bank A rank 1 on USD leg, got {rank_a_usd}"
        assert rank_a_eur == 2, f"Expected Bank A rank 2 on EUR leg, got {rank_a_eur}"
        assert rank_b_usd == 2, f"Expected Bank B rank 2 on USD leg, got {rank_b_usd}"
        assert rank_b_eur == 1, f"Expected Bank B rank 1 on EUR leg, got {rank_b_eur}"

        ranks_by_leg_a = live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment_a.id)
        print(f"Bank A Ranks by Leg: {ranks_by_leg_a}")
        assert ranks_by_leg_a[leg_usd.id] == 1
        assert ranks_by_leg_a[leg_eur.id] == 2

        # Endpoint structure check
        endpoint_res = get_live_rank(token=assignment_a.token, leg_id=leg_usd.id, db=db)
        assert "is_live_ranking_enabled" in endpoint_res
        assert "total_quotes" in endpoint_res
        print(f"get_live_rank endpoint response: {endpoint_res}")

        # 6. Standings & Independent Multi-Pair Winner Evaluation
        print("\n--- Step 5: Testing compute_rfq_standings ---")
        standings = compute_rfq_standings(rfq, db)
        assert "legs" in standings, "Missing 'legs' key in standings!"
        assert len(standings["legs"]) == 2, f"Expected 2 leg standings, got {len(standings['legs'])}"

        usd_standing = [s for s in standings["legs"] if s["currency_pair"] == "USD/EGP"][0]
        eur_standing = [s for s in standings["legs"] if s["currency_pair"] == "EUR/EGP"][0]

        print(f"\n[Leg 1: USD/EGP]")
        print(f"Winner Bank ID: {usd_standing.get('winner_bank_id')}")
        print(f"Winner Rate: {usd_standing.get('winner_rate')}")
        print(f"Savings: {usd_standing.get('savings_amount')} EGP ({usd_standing.get('savings_percent')}%)")
        print(f"Ladder count: {len(usd_standing.get('ladder', []))}")
        assert usd_standing.get('winner_bank_id') == bank_a.id, f"Expected Bank A ({bank_a.id}) to win USD/EGP, got {usd_standing.get('winner_bank_id')}"

        print(f"\n[Leg 2: EUR/EGP]")
        print(f"Winner Bank ID: {eur_standing.get('winner_bank_id')}")
        print(f"Winner Rate: {eur_standing.get('winner_rate')}")
        print(f"Savings: {eur_standing.get('savings_amount')} EGP ({eur_standing.get('savings_percent')}%)")
        print(f"Ladder count: {len(eur_standing.get('ladder', []))}")
        assert eur_standing.get('winner_bank_id') == bank_b.id, f"Expected Bank B ({bank_b.id}) to win EUR/EGP, got {eur_standing.get('winner_bank_id')}"

        # 7. Backward Compatibility Check on Legacy RFQ
        print("\n--- Step 6: Testing Backward Compatibility on Legacy RFQ ---")
        legacy_rfq = db.query(QuotationRequest).filter(QuotationRequest.id != rfq.id).order_by(QuotationRequest.id.asc()).first()
        if legacy_rfq:
            print(f"Testing Legacy RFQ #{legacy_rfq.id} ({legacy_rfq.ref_no})...")
            legacy_standings = compute_rfq_standings(legacy_rfq, db)
            assert "results" in legacy_standings, "Legacy results missing in standings"
            assert "legs" in legacy_standings, "Legs missing in legacy standings"
            print(f"Legacy RFQ processed successfully with {len(legacy_standings['results'])} bank results and {len(legacy_standings['legs'])} leg.")

        print("\n==================================================")
        print("ALL STAGE 2 TESTS PASSED PERFECTLY (100% SUCCESS)!")
        print("==================================================")

    finally:
        # Clean up test RFQ
        try:
            if 'rfq' in locals() and rfq:
                # Delete test quotes, leg configs, legs, assignments, rfq
                db.query(QuotationOffer).filter(QuotationOffer.assignment_id.in_([a.id for a in rfq.assignments])).delete(synchronize_session=False)
                for a in rfq.assignments:
                    db.query(QuotationBankLegConfig).filter(QuotationBankLegConfig.assignment_id == a.id).delete(synchronize_session=False)
                db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).delete(synchronize_session=False)
                db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq.id).delete(synchronize_session=False)
                db.query(QuotationRequest).filter(QuotationRequest.id == rfq.id).delete(synchronize_session=False)
                db.commit()
                print("Test data cleaned up successfully.")
        except Exception as e:
            print(f"Cleanup error (ignored): {e}")
        db.close()

if __name__ == "__main__":
    run_stage2_tests()
