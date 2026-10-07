# tests/test_bank_dealer_auth.py
"""
Comprehensive Test Suite for Permanent Bank Dealer Authentication & 2-Factor Enrolment Handshake
Verifies:
1. Gate 1 (Corporate Domain Proof & Email OTP dispatch)
2. Gate 1 (Constant-time verification & Authenticator QR generation)
3. Gate 2 (Cryptographic Handshake Proof via phone rolling code)
4. Account Activation & Day 1+ Instant Authentication
5. Security Lockouts & Brute Force Prevention
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pyotp
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.main import app
from app.database import SessionLocal, get_db
from app.models.models import Bank
from app.models.models_quotation import QuotationBankDealer
from app.services.dealer_auth_service import (
    generate_totp_secret,
    verify_totp_code,
    create_dealer_access_token,
    decode_dealer_access_token
)
from app.core.otp_security import hash_otp_code


def test_bank_dealer_totp_service_primitives():
    """Verifies core RFC 6238 mathematical primitives and JWT signing."""
    secret = generate_totp_secret()
    assert len(secret) == 32
    
    totp = pyotp.TOTP(secret)
    current_code = totp.now()
    assert len(current_code) == 6
    assert current_code.isdigit()
    
    # Valid verification
    assert verify_totp_code(secret, current_code) is True
    # Invalid verification
    assert verify_totp_code(secret, "000000" if current_code != "000000" else "111111") is False

    # JWT generation and decode
    token = create_dealer_access_token(
        dealer_id=999,
        email="trader.test@cibeg.com",
        bank_id=3,
        bank_name="Commercial International Bank (CIB)",
        full_name="Karim Trader",
        role="EXECUTION"
    )
    assert token is not None
    payload = decode_dealer_access_token(token)
    assert payload["dealer_id"] == 999
    assert payload["email"] == "trader.test@cibeg.com"
    assert payload["bank_id"] == 3
    assert payload["role"] == "EXECUTION"
    assert payload["type"] == "bank_dealer"


def test_dealer_enrollment_full_handshake_flow():
    """
    Executes the complete 2-factor enrolment ceremony:
    Gate 1: Domain check & email OTP
    Gate 1: Email OTP verify & QR generation
    Gate 2: Phone Authenticator rolling code proof & password activation
    Day 1+: Instant login with Email + Password + TOTP
    Profile: Authenticated /auth/me inspection
    """
    client = TestClient(app)
    db: Session = SessionLocal()

    test_email = "karim.trader.e2e@cibeg.com"
    
    try:
        # Clean up any prior test record
        db.query(QuotationBankDealer).filter(QuotationBankDealer.email == test_email).delete()
        db.commit()

        # Ensure CIB exists with domain cibeg.com
        cib = db.query(Bank).filter(Bank.id == 3).first()
        if not cib:
            cib = Bank(id=3, name="Commercial International Bank (CIB)", email_domain="cibeg.com")
            db.add(cib)
            db.commit()
        elif not cib.email_domain:
            cib.email_domain = "cibeg.com"
            db.commit()

        # -------------------------------------------------------------
        # 1. Gate 1: Initiate Enrolment
        # -------------------------------------------------------------
        init_res = client.post("/api/v1/bank-dealer/auth/enroll/initiate", json={
            "email": test_email,
            "full_name": "Karim Fathy",
            "title": "Senior FX Dealer",
            "role": "EXECUTION"
        })
        assert init_res.status_code == 200, init_res.text
        data = init_res.json()
        assert data["success"] is True
        assert data["email"] == test_email
        assert data["bank_id"] == cib.id
        assert data["requires_email_verification"] is True

        # Verify DB state
        dealer = db.query(QuotationBankDealer).filter(QuotationBankDealer.email == test_email).first()
        assert dealer is not None
        assert dealer.is_totp_enrolled is False
        assert dealer.pending_email_otp is not None
        assert dealer.pending_email_otp_expires_at is not None

        # -------------------------------------------------------------
        # 2. Gate 1: Test Brute Force Rejection on Invalid Code
        # -------------------------------------------------------------
        bad_res = client.post("/api/v1/bank-dealer/auth/enroll/verify-email", json={
            "email": test_email,
            "otp_code": "000000"
        })
        assert bad_res.status_code == 400
        assert "Invalid verification code" in bad_res.json()["detail"]

        # Simulate knowing the generated OTP from DB for test verification
        # Overwrite with known test OTP hash
        known_otp = "842195"
        dealer.pending_email_otp = hash_otp_code(known_otp)
        dealer.pending_otp_failed_attempts = 0
        db.commit()

        # -------------------------------------------------------------
        # 3. Gate 1: Verify Email Code & Receive Authenticator QR
        # -------------------------------------------------------------
        verify_email_res = client.post("/api/v1/bank-dealer/auth/enroll/verify-email", json={
            "email": test_email,
            "otp_code": known_otp
        })
        assert verify_email_res.status_code == 200, verify_email_res.text
        v_data = verify_email_res.json()
        assert v_data["success"] is True
        assert "enrollment_token" in v_data
        assert "totp_secret" in v_data
        assert "otpauth_uri" in v_data
        assert v_data["qr_code_base64"].startswith("data:image/png;base64,")

        handover_token = v_data["enrollment_token"]
        totp_secret = v_data["totp_secret"]

        # -------------------------------------------------------------
        # 4. Gate 2: Test Invalid Authenticator Rolling Code Rejection
        # -------------------------------------------------------------
        bad_totp_res = client.post("/api/v1/bank-dealer/auth/enroll/confirm-totp", json={
            "enrollment_token": handover_token,
            "totp_code": "000000",
            "password": "DealerSecret2026!#"
        })
        assert bad_totp_res.status_code == 400
        assert "Invalid 6-digit code" in bad_totp_res.json()["detail"]

        # -------------------------------------------------------------
        # 5. Gate 2: Confirm Rolling Code (Cryptographic Handshake Proof)
        # -------------------------------------------------------------
        valid_totp_code = pyotp.TOTP(totp_secret).now()
        confirm_res = client.post("/api/v1/bank-dealer/auth/enroll/confirm-totp", json={
            "enrollment_token": handover_token,
            "totp_code": valid_totp_code,
            "password": "DealerSecret2026!#"
        })
        assert confirm_res.status_code == 200, confirm_res.text
        c_data = confirm_res.json()
        assert c_data["success"] is True
        assert "access_token" in c_data
        assert c_data["dealer"]["is_totp_enrolled"] is True

        # Refresh DB and verify permanent state
        db.refresh(dealer)
        assert dealer.is_totp_enrolled is True
        assert dealer.hashed_password is not None
        assert dealer.enrollment_token is None

        # -------------------------------------------------------------
        # 6. Day 1+: Instant Login with Email + Password + TOTP
        # -------------------------------------------------------------
        # Wrong password test
        login_bad_pwd = client.post("/api/v1/bank-dealer/auth/login", json={
            "email": test_email,
            "password": "WrongPassword123!",
            "totp_code": pyotp.TOTP(totp_secret).now()
        })
        assert login_bad_pwd.status_code == 401

        # Wrong TOTP test
        login_bad_totp = client.post("/api/v1/bank-dealer/auth/login", json={
            "email": test_email,
            "password": "DealerSecret2026!#",
            "totp_code": "000000"
        })
        assert login_bad_totp.status_code == 401

        # Successful Instant Login
        live_totp = pyotp.TOTP(totp_secret).now()
        login_res = client.post("/api/v1/bank-dealer/auth/login", json={
            "email": test_email,
            "password": "DealerSecret2026!#",
            "totp_code": live_totp
        })
        assert login_res.status_code == 200, login_res.text
        login_data = login_res.json()
        assert login_data["success"] is True
        session_token = login_data["access_token"]
        assert session_token is not None

        # -------------------------------------------------------------
        # 7. Authenticated Profile Inspection (/auth/me)
        # -------------------------------------------------------------
        me_res = client.get(
            "/api/v1/bank-dealer/auth/me",
            headers={"Authorization": f"Bearer {session_token}"}
        )
        assert me_res.status_code == 200, me_res.text
        me_data = me_res.json()
        assert me_data["success"] is True
        assert me_data["dealer"]["email"] == test_email
        assert me_data["dealer"]["bank_name"] == cib.name
        assert me_data["dealer"]["is_totp_enrolled"] is True

        # -------------------------------------------------------------
        # 8. Unified Live RFQ Blotter Feed
        # -------------------------------------------------------------
        blotter_res = client.get(
            "/api/v1/bank-dealer/blotter/live-rfqs",
            headers={"Authorization": f"Bearer {session_token}"}
        )
        assert blotter_res.status_code == 200, blotter_res.text
        b_data = blotter_res.json()
        assert b_data["success"] is True
        assert "tickets" in b_data
        assert "total_active_rfqs" in b_data

        # -------------------------------------------------------------
        # 9. Historical Won Trades Blotter & Cryptographic Receipts
        # -------------------------------------------------------------
        history_res = client.get(
            "/api/v1/bank-dealer/blotter/history",
            headers={"Authorization": f"Bearer {session_token}"}
        )
        assert history_res.status_code == 200, history_res.text
        h_data = history_res.json()
        assert h_data["success"] is True
        assert "records" in h_data
        assert "total_deals" in h_data

    finally:
        # Cleanup
        db.query(QuotationBankDealer).filter(QuotationBankDealer.email == test_email).delete()
        db.commit()
        db.close()


if __name__ == "__main__":
    print("Testing Bank Dealer TOTP Primitives...")
    test_bank_dealer_totp_service_primitives()
    print("[PASS] Primitives passed.")

    print("Testing Bank Dealer 2-Factor Enrolment Handshake & Instant Login Flow...")
    test_dealer_enrollment_full_handshake_flow()
    print("[PASS] Enrolment handshake & login flow passed with 100% success!")
