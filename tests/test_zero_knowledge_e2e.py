# tests/test_zero_knowledge_e2e.py
"""
End-to-End Integration Test for Sub-Phase 8.3: Live Quoting & Results Zero-Knowledge Pipeline.
Verifies:
1. Bank quote submission encrypts rates into PostgreSQL.
2. Direct Raw SQL Database Inspection: Confirms ciphertext contains zero plaintext terms.
3. Standings & Results computation: Resolves decrypted values in-memory seamlessly.
4. Tamper & Cross-Tenant rejection on live DB rows.
"""

import uuid
from sqlalchemy import text
from app.database import SessionLocal
from app.models.models import Customer, Bank
from app.models.models_quotation import (
    QuotationRequest,
    QuotationLeg,
    QuotationBank,
    QuotationBankAssignment,
    QuotationBankLegConfig,
    QuotationOffer,
    QuotationTenantKey
)
from app.services.tenant_key_service import tenant_key_service
from app.core.security_crypto import (
    is_encrypted,
    generate_tenant_dek,
    decrypt_field,
    TamperDetectedError
)
from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings


def test_zero_knowledge_live_quoting_e2e():
    db = SessionLocal()
    test_rfq_id = None
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for e2e test"
        cust_id = cust.id

        from app.models.models import User
        user = db.query(User).filter(User.customer_id == cust_id).first()
        user_id = user.id if user else 1

        bank = db.query(Bank).first()
        assert bank is not None, "Bank required for e2e test"

        # 1. Ensure Tenant DEK exists
        tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        assert len(tenant_dek) == 32

        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)

        rfq_id_str = str(uuid.uuid4())
        rfq = QuotationRequest(
            id=rfq_id_str,
            customer_id=cust_id,
            created_by_user_id=user_id,
            ref_no=f"TEST-ZK-{uuid.uuid4().hex[:6].upper()}",
            type="FX_SPOT",
            status="OPEN",
            quotation_base="Execution",
            window_start=now,
            window_end=now + timedelta(minutes=15),
            acceptance_timeout_seconds=30,
            acceptance_timeout_action="AUTO_ACCEPT",
            acceptance_status="PENDING"
        )
        db.add(rfq)
        db.flush()
        test_rfq_id = rfq.id

        leg_id_str = str(uuid.uuid4())
        leg = QuotationLeg(
            id=leg_id_str,
            rfq_id=rfq.id,
            leg_index=1,
            buy_currency="USD",
            sell_currency="EGP",
            amount=500000.0,
            direction="Buy",
            quotation_base="Execution"
        )
        db.add(leg)
        db.flush()

        # Link bank
        q_bank = db.query(QuotationBank).filter(
            QuotationBank.customer_id == cust_id,
            QuotationBank.bank_id == bank.id
        ).first()
        if not q_bank:
            q_bank = QuotationBank(
                customer_id=cust_id,
                bank_id=bank.id,
                emails="dealer@banktest.com"
            )
            db.add(q_bank)
            db.flush()

        assignment = QuotationBankAssignment(
            id=str(uuid.uuid4()),
            rfq_id=rfq.id,
            quotation_bank_id=q_bank.id,
            token=f"token-{uuid.uuid4().hex[:12]}",
            approval_status="APPROVED"
        )
        db.add(assignment)
        db.flush()

        leg_cfg = QuotationBankLegConfig(
            id=f"{assignment.id}_{leg.id}",
            assignment_id=assignment.id,
            leg_id=leg.id,
            is_invited=True,
            is_passed=False
        )
        db.add(leg_cfg)
        db.flush()

        # 3. Simulate Dealer Submitting a Firm Quote of 48.8750
        raw_quote_price = 48.8750
        offer = QuotationOffer(
            assignment_id=assignment.id,
            leg_id=leg.id,
            price=0.0,
            submitted_by_email="trader@testbank.com"
        )
        # Apply Zero-Knowledge encryption
        tenant_key_service.apply_encrypted_offer_price(offer, raw_quote_price, tenant_dek)
        db.add(offer)
        db.commit()

        # 4. DIRECT DATABASE AUDIT: Inspect raw row on PostgreSQL disk
        raw_row = db.execute(text("SELECT encrypted_price, price FROM quotation_offers WHERE id = :oid"), {"oid": offer.id}).fetchone()
        assert raw_row is not None
        encrypted_price_val = raw_row[0]
        raw_db_price = raw_row[1]

        print(f"\n[Raw PostgreSQL Row] encrypted_price: {encrypted_price_val}")
        print(f"[Raw PostgreSQL Row] price column: {raw_db_price} (physically NULL)")

        # Assert ciphertext format: enc:v1:{nonce}:{ct}
        assert is_encrypted(encrypted_price_val)
        # Assert legacy price column is strictly NULL (pure ciphertext storage)
        assert raw_db_price is None, f"Expected price column to be NULL, found {raw_db_price}"
        # Assert raw numbers are NOT readable in plaintext ciphertext
        assert "48.875" not in encrypted_price_val

        # 5. Cross-Tenant Security Audit: Attempting to decrypt with another tenant's key MUST fail
        foreign_dek = generate_tenant_dek()
        try:
            decrypt_field(encrypted_price_val, foreign_dek, field_context="price")
            raise AssertionError("Security breach: Foreign DEK was able to decrypt customer quote!")
        except TamperDetectedError:
            print("[Pass] Foreign tenant DEK failed to decrypt as expected (Zero-Knowledge Isolation)")

        # 6. Corporate Evaluation & Standings Computation
        # Refresh RFQ and compute standings
        db.refresh(rfq)
        standings = compute_rfq_standings(rfq, db)

        assert standings is not None
        legs_data = standings.get("legs", [])
        assert len(legs_data) == 1
        comp_bank = legs_data[0]["results"][0]

        # Verify that the decrypted price matches the dealer's original quote to 4 decimal places
        resolved_price = comp_bank["price"]
        print(f"[Standings Output] Decrypted Bank Quoted Price: {resolved_price}")
        assert abs(resolved_price - raw_quote_price) < 0.0001
        assert comp_bank["finalPrice"] is not None

        print("[Pass] Corporate Results View standings computed and decrypted cleanly in memory!")

    finally:
        # Clean up test records
        if test_rfq_id:
            try:
                db.query(QuotationOffer).filter(QuotationOffer.assignment_id.in_(
                    db.query(QuotationBankAssignment.id).filter(QuotationBankAssignment.rfq_id == test_rfq_id)
                )).delete(synchronize_session=False)
                db.query(QuotationBankLegConfig).filter(QuotationBankLegConfig.leg_id.in_(
                    db.query(QuotationLeg.id).filter(QuotationLeg.rfq_id == test_rfq_id)
                )).delete(synchronize_session=False)
                db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == test_rfq_id).delete(synchronize_session=False)
                db.query(QuotationLeg).filter(QuotationLeg.rfq_id == test_rfq_id).delete(synchronize_session=False)
                db.query(QuotationRequest).filter(QuotationRequest.id == test_rfq_id).delete(synchronize_session=False)
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"Cleanup error: {e}")
        db.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Grow Treasury — Sub-Phase 8.3 Zero-Knowledge E2E Pipeline Test")
    print("=" * 60)
    test_zero_knowledge_live_quoting_e2e()
    print("-" * 60)
    print("SUB-PHASE 8.3 E2E INTEGRATION TEST PASSED! (100% SUCCESS)")
    print("=" * 60)
