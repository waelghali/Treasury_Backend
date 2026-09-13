import sys
sys.path.insert(0, r"c:\Grow")

from app.database import engine, SessionLocal
from app.models.models_quotation import (
    BankLiveRankingConfig, QuotationRequest, QuotationBankAssignment,
    QuotationBank, QuotationOffer, QuotationTBillOffer
)
from app.models import Bank, Customer, CustomerEntity
from app.services.live_ranking_service import live_ranking_service

def test_live_ranking():
    db = SessionLocal()
    try:
        # Check banks
        bank = db.query(Bank).first()
        customer = db.query(Customer).first()
        print(f"Sample Bank: {bank.name if bank else 'None'} (id={bank.id if bank else 'N/A'})")
        print(f"Sample Customer: {customer.name if customer else 'None'} (id={customer.id if customer else 'N/A'})")

        if bank and customer:
            # Test eligibility with no rule
            res1 = live_ranking_service.evaluate_live_ranking_eligibility(
                db, bank_id=bank.id, customer_id=customer.id, entity_id=None, trade_type="FX_SPOT"
            )
            print(f"Eligibility without config rule: {res1} (Expected: False)")

            # Create temporary global config rule
            cfg = BankLiveRankingConfig(
                bank_id=bank.id,
                trade_type="BOTH",
                scope_type="ALL_CUSTOMERS",
                customer_id=None,
                entity_scope_type="ALL_ENTITIES",
                entity_id=None,
                is_enabled=True
            )
            db.add(cfg)
            db.commit()
            db.refresh(cfg)
            print(f"Created temp config rule #{cfg.id}")

            # Test eligibility with global rule
            res2 = live_ranking_service.evaluate_live_ranking_eligibility(
                db, bank_id=bank.id, customer_id=customer.id, entity_id=None, trade_type="FX_SPOT"
            )
            print(f"Eligibility with global config rule: {res2} (Expected: True)")

            # Clean up temp rule
            db.delete(cfg)
            db.commit()
            print("Cleaned up temp rule successfully.")

        print("\nAll live ranking service checks passed successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    test_live_ranking()
