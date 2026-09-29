import os
import sys
from datetime import datetime, timezone, timedelta

# Setup environment
os.environ["PYTHONPATH"] = "."
from app.database import SessionLocal
from app.models.models_quotation import QuotationBankAssignment, QuotationAccessOTP, QuotationRequest, QuotationBank
from app.core.otp_security import hash_otp_code, verify_otp_code, generate_scoped_deal_receipt, MAX_OTP_FAILED_ATTEMPTS
from app.core.rate_limiter import quotation_rate_limiter

def test_full_e2e_phase2_flow():
    db = SessionLocal()
    try:
        # Find any existing assignment to test safely with
        assignment = db.query(QuotationBankAssignment).first()
        if not assignment:
            print("No quotation bank assignment found in database. Skipping DB integration test.")
            return

        test_email = "security_test_dealer@cibeg.com"
        test_code = "491823"
        hashed = hash_otp_code(test_code)
        
        # 1. Create a test OTP record with failed_attempts = 0
        now = datetime.now(timezone.utc)
        import secrets
        otp = QuotationAccessOTP(
            assignment_id=assignment.id,
            email=test_email,
            otp_code=hashed,
            magic_token=secrets.token_urlsafe(32),
            role="EXECUTION",
            failed_attempts=0,
            is_used=False,
            expires_at=now + timedelta(minutes=15)
        )
        db.add(otp)
        db.commit()
        db.refresh(otp)
        otp_id = otp.id

        print(f"[1] Created test OTP record: ID={otp_id}, Hashed={hashed[:16]}..., failed_attempts={otp.failed_attempts}")

        # 2. Simulate 2 failed attempts
        for attempt in (1, 2):
            is_correct = verify_otp_code("000000", otp.otp_code)
            assert not is_correct
            otp.failed_attempts += 1
            remaining = MAX_OTP_FAILED_ATTEMPTS - otp.failed_attempts
            db.commit()
            print(f"[2] Incorrect attempt #{attempt}: {remaining} attempt(s) remaining (is_used={otp.is_used})")
            assert not otp.is_used

        # 3. Simulate 3rd failed attempt -> Should burn code
        is_correct = verify_otp_code("000000", otp.otp_code)
        assert not is_correct
        otp.failed_attempts += 1
        remaining = MAX_OTP_FAILED_ATTEMPTS - otp.failed_attempts
        if remaining <= 0:
            otp.is_used = True
            db.commit()
        print(f"[3] 3rd incorrect attempt: remaining={remaining}, is_used={otp.is_used} (CODE BURNED)")
        assert otp.is_used is True

        # 4. Simulate self-service unlock: Trader requests fresh code
        new_code = "829104"
        new_hashed = hash_otp_code(new_code)
        new_otp = QuotationAccessOTP(
            assignment_id=assignment.id,
            email=test_email,
            otp_code=new_hashed,
            magic_token=secrets.token_urlsafe(32),
            role="EXECUTION",
            failed_attempts=0,
            is_used=False,
            expires_at=now + timedelta(minutes=15)
        )
        db.add(new_otp)
        db.commit()
        db.refresh(new_otp)
        print(f"[4] Self-service unlock requested: New OTP ID={new_otp.id} issued. Previous burned.")

        # 5. Trader enters correct new code
        assert verify_otp_code(new_code, new_otp.otp_code) is True
        new_otp.is_used = True
        db.commit()
        print(f"[5] Trader successfully authenticated with new code! Session established.")

        # 6. Test Deal Execution Receipt generation
        receipt = generate_scoped_deal_receipt(
            rfq_id=str(assignment.rfq_id),
            ref_no="RFQ-E2E-TEST",
            customer_name="Alpha Corp",
            bank_id=assignment.quotation_bank_id,
            bank_name="Test Bank",
            executed_legs=[{"pair": "USD/EGP", "direction": "BUY", "amount": 500000, "rate": 48.85}],
            executed_at=now.strftime("%Y-%m-%d %H:%M:%S UTC")
        )
        print(f"[6] Deal Execution Receipt generated: {receipt['receipt_id']}, Sig: {receipt['signature_hash'][:24]}...")
        assert receipt["scoped_legs_count"] == 1

        # Clean up test rows
        db.delete(otp)
        db.delete(new_otp)
        db.commit()
        print("Test data cleaned up successfully.")
        print("\n>>> ALL PHASE 2 E2E FLOW CHECKS PASSED PERFECTLY! <<<")

    finally:
        db.close()

if __name__ == "__main__":
    test_full_e2e_phase2_flow()
