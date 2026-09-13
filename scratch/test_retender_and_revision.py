import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, r"c:\Grow")

from app.database import SessionLocal
from app.models.models_quotation import QuotationRequest, QuotationBankAssignment, QuotationBank
from app.models import User, Customer

def test_retender_and_revision():
    print("Testing Re-tender and Admin Revision workflows...")
    db = SessionLocal()
    try:
        user = db.query(User).first()
        customer = db.query(Customer).first()
        if not user or not customer:
            print("No user or customer found in database.")
            return

        now = datetime.now(timezone.utc)
        test_rfq = QuotationRequest(
            id="test-rfq-rev-101",
            ref_no="TEST-REV-001",
            customer_id=customer.id,
            created_by_user_id=user.id,
            type="FX_SPOT",
            direction="Buy",
            amount=1000000.0,
            buy_currency="USD",
            sell_currency="EGP",
            window_start=now,
            window_end=now + timedelta(minutes=15),
            status="PENDING_APPROVAL",
            quotation_base="Execution"
        )
        test_rfq = db.merge(test_rfq)
        db.commit()

        # 1. Admin returns for revision
        test_rfq.status = "NEEDS_REVISION"
        test_rfq.admin_revision_notes = "Please extend window to 30m and invite CIB."
        test_rfq.admin_reviewed_at = datetime.now(timezone.utc)
        db.commit()

        reloaded = db.query(QuotationRequest).filter(QuotationRequest.id == "test-rfq-rev-101").first()
        assert reloaded.status == "NEEDS_REVISION", f"Expected NEEDS_REVISION, got {reloaded.status}"
        assert "extend window" in reloaded.admin_revision_notes
        print("[PASS] Admin revision workflow verified.")

        # 2. Maker resubmits
        reloaded.window_end = now + timedelta(minutes=30)
        reloaded.status = "PENDING_APPROVAL"
        db.commit()

        reloaded2 = db.query(QuotationRequest).filter(QuotationRequest.id == "test-rfq-rev-101").first()
        assert reloaded2.status == "PENDING_APPROVAL"
        print("[PASS] Maker resubmission verified.")

        # 3. Re-tender simulation
        retender_rfq = QuotationRequest(
            id="test-rfq-retender-102",
            ref_no="TEST-REV-001-R1",
            customer_id=customer.id,
            created_by_user_id=user.id,
            type=reloaded2.type,
            direction=reloaded2.direction,
            amount=reloaded2.amount,
            buy_currency=reloaded2.buy_currency,
            sell_currency=reloaded2.sell_currency,
            window_start=now,
            window_end=now + timedelta(minutes=20),
            status="PENDING",
            quotation_base=reloaded2.quotation_base,
            parent_rfq_id=reloaded2.id
        )
        db.merge(retender_rfq)
        db.commit()

        retender_loaded = db.query(QuotationRequest).filter(QuotationRequest.id == "test-rfq-retender-102").first()
        assert retender_loaded.parent_rfq_id == "test-rfq-rev-101"
        assert retender_loaded.ref_no == "TEST-REV-001-R1"
        assert retender_loaded.parent_rfq.ref_no == "TEST-REV-001"
        print("[PASS] Re-tender parent-child relationship verified.")

        # Clean up
        db.delete(retender_loaded)
        db.delete(reloaded2)
        db.commit()
        print("[PASS] Test cleanup completed.")
        print("ALL TESTS PASSED SUCCESSFULLY.")

    finally:
        db.close()

if __name__ == "__main__":
    test_retender_and_revision()
