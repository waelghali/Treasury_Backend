import sys
import os
from datetime import datetime, timezone, timedelta

# Ensure root app directory is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.models.models_quotation import QuotationRequest, QuotationLeg, QuotationBankAssignment, QuotationBankLegConfig, QuotationBank, QuotationOffer
from app.models.models import AuditLog, User
from app.schemas.schemas_quotation import QuotationRequestCreate, QuotationLegCreate, FXSpotMultiOfferCreate, FXSpotOfferItem
from app.crud.crud_quotation import crud_quotation
from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings, dispatch_rfq_result_emails
from app.api.v1.endpoints.public_quotations import get_rfq_by_token, submit_fx_offers_batch, get_live_rank

import asyncio

async def run_e2e_test():
    db = SessionLocal()
    print("================================================================")
    print("STAGE 7 E2E INTEGRATION TEST: Full Multi-Currency Quotation Lifecycle")
    print("================================================================")

    created_rfq = None
    try:
        # 1. Fetch 2 quotation banks
        banks = db.query(QuotationBank).filter(QuotationBank.customer_id == 1).limit(2).all()
        assert len(banks) >= 2, "Need at least 2 active quotation banks for customer 1"
        bank_a = banks[0]
        bank_b = banks[1]
        print(f"[OK] Found Bank A (ID: {bank_a.id}) and Bank B (ID: {bank_b.id})")

        user = db.query(User).filter(User.customer_id == 1).first()
        test_user_id = user.id if user else 1

        now = datetime.now(timezone.utc)
        val_date = (now + timedelta(days=2)).date()

        # 2. Create Multi-Leg RFQ
        leg1_in = QuotationLegCreate(
            direction="Buy",
            buyCurrency="USD",
            sellCurrency="EGP",
            amount=1000000.0,
            valueDate=val_date,
            allowAlternativeValueDate=False,
            quotationBase="Execution",
            maxTolerancePercent=0.05,
            selectedBanks=[
                {"id": bank_a.id, "costPercent": 0.0, "quotationBase": "Execution", "valueDate": str(val_date)},
                {"id": bank_b.id, "costPercent": 0.0, "quotationBase": "Execution", "valueDate": str(val_date)}
            ]
        )
        leg2_in = QuotationLegCreate(
            direction="Buy",
            buyCurrency="EUR",
            sellCurrency="EGP",
            amount=500000.0,
            valueDate=val_date,
            allowAlternativeValueDate=False,
            quotationBase="Execution",
            maxTolerancePercent=0.05,
            selectedBanks=[
                {"id": bank_a.id, "costPercent": 0.0, "quotationBase": "Execution", "valueDate": str(val_date)},
                {"id": bank_b.id, "costPercent": 0.0, "quotationBase": "Execution", "valueDate": str(val_date)}
            ]
        )

        rfq_create = QuotationRequestCreate(
            type="FX_SPOT",
            windowStart=now,
            windowEnd=now + timedelta(hours=1),
            pairs=[leg1_in, leg2_in],
            legalDisclaimerAccepted=True
        )

        created_rfq, created_assignments = crud_quotation.create_request(
            db, customer_id=1, user_id=test_user_id, requires_approval=False, obj_in=rfq_create
        )
        rfq_id = str(created_rfq.id)
        print(f"[OK] Created RFQ #{created_rfq.ref_no} (ID: {rfq_id}) with {len(created_rfq.legs)} legs")
        assert len(created_rfq.legs) == 2, "Expected 2 legs created"

        leg_usd = next(l for l in created_rfq.legs if l.buy_currency == "USD")
        leg_eur = next(l for l in created_rfq.legs if l.buy_currency == "EUR")

        # 3. Test Bank Portal Access via Tokens
        assign_a = [a for a in created_rfq.assignments if a.quotation_bank_id == bank_a.id][0]
        assign_b = [a for a in created_rfq.assignments if a.quotation_bank_id == bank_b.id][0]
        # Set active window and approved status
        created_rfq.window_start = now - timedelta(minutes=5)
        created_rfq.window_end = now + timedelta(minutes=55)
        assign_a.approval_status = 'APPROVED'
        assign_b.approval_status = 'APPROVED'
        db.commit()

        portal_a = await get_rfq_by_token(assign_a.token, db)
        assert "legs" in portal_a, "Missing 'legs' key in portal response"
        assert len(portal_a["legs"]) == 2, "Bank portal must receive both legs"
        print(f"[OK] Bank A opened Bank Quoting Portal with {len(portal_a['legs'])} legs")

        # 4. Batch Quote Submission:
        # Bank A quotes: USD @ 48.50, EUR @ 52.80
        res_a = submit_fx_offers_batch(
            FXSpotMultiOfferCreate(
                token=assign_a.token,
                quotes=[
                    FXSpotOfferItem(leg_id=leg_usd.id, price=48.50, offered_value_date=str(val_date)),
                    FXSpotOfferItem(leg_id=leg_eur.id, price=52.80, offered_value_date=str(val_date)),
                ],
                email="dealer.a@bank.com"
            ),
            db
        )
        assert res_a["quotes_count"] == 2, "Bank A batch quote submission failed"
        print(f"[OK] Bank A submitted batch quotes for both legs: USD @ 48.50, EUR @ 52.80")

        # Bank B quotes: USD @ 48.40 (better), EUR @ 52.90 (worse)
        res_b = submit_fx_offers_batch(
            FXSpotMultiOfferCreate(
                token=assign_b.token,
                quotes=[
                    FXSpotOfferItem(leg_id=leg_usd.id, price=48.40, offered_value_date=str(val_date)),
                    FXSpotOfferItem(leg_id=leg_eur.id, price=52.90, offered_value_date=str(val_date)),
                ],
                email="dealer.b@bank.com"
            ),
            db
        )
        assert res_b["quotes_count"] == 2, "Bank B batch quote submission failed"
        print(f"[OK] Bank B submitted batch quotes for both legs: USD @ 48.40, EUR @ 52.90")

        # 5. Live Rank Telemetry Polling
        from app.services.live_ranking_service import live_ranking_service
        ranks_by_leg_a = live_ranking_service.calculate_bank_live_ranks_by_leg(db, created_rfq.id, assign_a.id)
        ranks_by_leg_b = live_ranking_service.calculate_bank_live_ranks_by_leg(db, created_rfq.id, assign_b.id)
        print(f"[OK] Bank A Live Ranks per leg (direct): {ranks_by_leg_a}")
        print(f"[OK] Bank B Live Ranks per leg (direct): {ranks_by_leg_b}")
        assert ranks_by_leg_a[leg_usd.id] == 2, f"Bank A should be Rank 2 for USD/EGP Buy, got {ranks_by_leg_a.get(leg_usd.id)}"
        assert ranks_by_leg_a[leg_eur.id] == 1, f"Bank A should be Rank 1 for EUR/EGP Buy, got {ranks_by_leg_a.get(leg_eur.id)}"
        assert ranks_by_leg_b[leg_usd.id] == 1, f"Bank B should be Rank 1 for USD/EGP Buy, got {ranks_by_leg_b.get(leg_usd.id)}"
        assert ranks_by_leg_b[leg_eur.id] == 2, f"Bank B should be Rank 2 for EUR/EGP Buy, got {ranks_by_leg_b.get(leg_eur.id)}"

        rank_endpoint_a = get_live_rank(token=assign_a.token, db=db)
        print(f"[OK] Bank A Live Rank endpoint response: is_enabled={rank_endpoint_a.get('is_live_ranking_enabled')}")

        # 6. Corporate Standings & Independent Multi-Winner Evaluation
        standings = compute_rfq_standings(created_rfq, db, dispatch_emails=False)
        legs_result = standings["legs"]
        assert len(legs_result) == 2, "Standings must calculate results for both legs"

        usd_result = next(l for l in legs_result if l["currency_pair"] == "USD/EGP")
        eur_result = next(l for l in legs_result if l["currency_pair"] == "EUR/EGP")

        print(f"[OK] Standings USD/EGP Winner: Bank ID {usd_result['winner_bank_id']} (Bank B: {bank_b.id}) @ {usd_result['winner_rate']}")
        assert usd_result["winner_bank_id"] == bank_b.id, "Expected Bank B to win USD/EGP"
        assert usd_result["winner_rate"] == 48.40, "Expected winning rate 48.40"

        print(f"[OK] Standings EUR/EGP Winner: Bank ID {eur_result['winner_bank_id']} (Bank A: {bank_a.id}) @ {eur_result['winner_rate']}")
        assert eur_result["winner_bank_id"] == bank_a.id, "Expected Bank A to win EUR/EGP"
        assert eur_result["winner_rate"] == 52.80, "Expected winning rate 52.80"

        # 7. Post-Trade Auto-Dispatch for Multi-Leg
        created_rfq.status = "COMPLETED"
        created_rfq.window_end = datetime.now(timezone.utc) - timedelta(minutes=5)
        db.commit()

        dispatch_res = await dispatch_rfq_result_emails(rfq_id, db, force=True)
        print(f"[OK] Post-Trade Result Dispatch: {dispatch_res}")
        assert dispatch_res["status"] == "sent", "Result emails should be sent"
        assert dispatch_res["dispatched_count"] >= 1, "Should dispatch emails"

        audit_entry = db.query(AuditLog).filter(
            AuditLog.action_type == "QUOTATION_RESULTS_SENT",
            AuditLog.entity_type == "QuotationRequest"
        ).filter(
            AuditLog.details["rfq_id"].astext == rfq_id
        ).first()

        assert audit_entry is not None, "Audit log must be created for result dispatch"
        assert audit_entry.details.get("is_multi_leg") is True, "Audit log must indicate is_multi_leg=True"
        print(f"[OK] AuditLog verified: is_multi_leg={audit_entry.details.get('is_multi_leg')}, emails_count={audit_entry.details.get('emails_count')}")

        print("\n================================================================")
        print("ALL STAGE 7 MULTI-PAIR LIFECYCLE TESTS PASSED PERFECTLY!")
        print("================================================================")

    finally:
        # Cleanup
        if created_rfq:
            try:
                db.query(AuditLog).filter(AuditLog.details["rfq_id"].astext == str(created_rfq.id)).delete(synchronize_session=False)
                db.query(QuotationOffer).filter(QuotationOffer.assignment_id.in_([a.id for a in created_rfq.assignments])).delete(synchronize_session=False)
                for a in created_rfq.assignments:
                    db.query(QuotationBankLegConfig).filter(QuotationBankLegConfig.assignment_id == a.id).delete(synchronize_session=False)
                db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == created_rfq.id).delete(synchronize_session=False)
                db.query(QuotationLeg).filter(QuotationLeg.rfq_id == created_rfq.id).delete(synchronize_session=False)
                db.query(QuotationRequest).filter(QuotationRequest.id == created_rfq.id).delete(synchronize_session=False)
                db.commit()
                print("[OK] Cleaned up test database records.")
            except Exception as e:
                print(f"Cleanup warning: {e}")
        db.close()

if __name__ == "__main__":
    asyncio.run(run_e2e_test())
