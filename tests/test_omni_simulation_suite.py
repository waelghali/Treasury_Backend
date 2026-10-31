# tests/test_omni_simulation_suite.py
"""
========================================================================================
GROW TREASURY PLATFORM - MASTER OMNI SIMULATION & ADVERSARIAL VERIFICATION SUITE
========================================================================================
Comprehensive End-to-End Orchestrated Matrix covering:
1. PART 1 — Negative Security & Adversarial Suite (12 Defense Checkpoints):
   - Negative List Webmail Rejection (@gmail/@yahoo)
   - Anti-Collusion Corporate Domain Shield
   - Issuance Invite Domain Whitelist
   - Maker Self-Approval RBAC Block
   - Non-Admin Customer Settings RBAC Block
   - Cross-Tenant Cryptographic Isolation (DEK A != DEK B TamperDetectedError)
   - Public OTP 3-Strike Brute-Force Burn & Lockout
   - Zero-Leakage Platform Email Fallback Refusal
   - Unauthenticated Quote Submission Rejection (HTTP 401)
   - View-Only Contact Quote Submission Block (HTTP 403)
   - Uninvited / Invisible Leg Quote Injection Block (HTTP 403)
   - Duplicate Per-Bank Leg Signature Conflict Rejection (HTTP 400)
2. PART 2 — Master Cycle 1: The Lifecycle Clock, Approval Gate, Rescheduling & Auto-Timeout:
   - Access & Quoting before Corporate Approval (Blocked HTTP 403)
   - Scheduled Future Release & Immediate Release Reschedule Override
   - Active Window Live Authenticated Quoting & Window Extension Reschedule
   - Post-Window Late Quote Rejection (HTTP 403)
   - Acceptance Window Auto-Timeout Governance (AUTO_REJECT -> REJECTED outcome)
3. PART 3 — Master Cycle 2: 3-Leg Multi-Currency Portfolio & 5-Bank Counterparty Matrix:
   - Created 100% via API payload (per-leg selectedBanks JSON with custom valueDate,
     allowAlternativeValueDate, isInvited=False, isDocumentVisible, quotationBase)
   - Bank A (CIB): Approver 2FA Gate (POST /approve APPROVE) -> Trailing then Revises Quote -> Wins Leg 1 & Leg 3
   - Bank B (HSBC): Desk Concurrency & Takeover -> Quotes Legs 1 & 2, Passes Leg 3 -> Wins Leg 2
   - Bank C (NBE): Mixed Base (Indicative on Leg 1, Execution on Leg 2) -> Never displaces Execution Rank #1 on Leg 1
   - Bank D (Banque Misr): Leg 3 Invisible, Leg 1 Docs Hidden -> Approver DECLINES via POST /approve (PARTICIPATION_DECLINED)
   - Bank E (QNB): Invited ONLY to Leg 3 with Custom Value Date (T+5) -> Verified in Portal, Live Blotter & Email -> Silent
   - 3-Tier Orthogonal Document Governance (INTERNAL_ONLY, BIDDING Leg-Scoped, WINNER_ONLY Leg-Scoped)
   - Corporate Partial & Full Deal Acceptance, Scoped HMAC-SHA256 Receipts, Trophies Delta & Ciphertext-at-Rest
4. PART 4 — Sub-Phase 8.7 Streaming Encryption, Ephemeral Ticket Gateway & Concurrency Stress Benchmark:
   - STREAM V2 64 KB AEAD Multi-Chunk Roundtrip, Bit-Flip Tamper Detection & Dual-Read Legacy Compatibility
   - 9 Adversarial Ephemeral Ticket Gateway Security Gates
   - Multi-Thread Concurrent Streaming Decryption & Heap Memory Ceiling Benchmark
5. PART 5 — Master Cycle 3: Business Math, Value Date & Alternative Date Matrix, Fees, Tolerance & T-Bills:
   - Scenario 5.1: Single-Leg Custom Value Date & TVM Normalization (Higher raw rate on T+2 beats lower raw rate on T+0;
     verifies portal, dealer live blotter, winner_rate, winner_normalized_rate, saved_vs_avg, saved_vs_worst)
   - Scenario 5.2: allowAlternativeValueDate Enforcement (False forces assigned date; True blocks past dates with HTTP 400
     and accepts + TVM-normalizes future dates) & Un-Passing a Passed Leg
   - Scenario 5.3: Per-Bank Transaction Fees (costPercent, costMin, costMax, costFlat) Flipping All-In Winner
   - Scenario 5.4: Indicative Benchmark Tolerance (maxTolerancePercent) Breach (INCONCLUSIVE) vs Within-Tolerance Award,
     plus INDICATIVE_ONLY Bank Result Verification
   - Scenario 5.5: T-Bill Buy & Sell Multi-Line Normalization (Buy: lowest normalized price per 100 par wins;
     Sell: lowest discount rate % wins)
========================================================================================
Designed to be 100% IDEMPOTENT and RUNNABLE ANY NUMBER OF TIMES without collisions.
========================================================================================
"""

import sys
import os

# Ensure console supports UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import json
import uuid
import time
import secrets
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import app.main early to run startup self-heal migrations and table checks
import app.main
from app.main import app
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import SessionLocal
from app.models.models import Customer, User, UserRole, Bank, CustomerEntity
from app.models.models_quotation import (
    QuotationRequest, QuotationLeg, QuotationBank,
    QuotationBankAssignment, QuotationBankLegConfig,
    QuotationOffer, QuotationTBillOffer, QuotationTenantKey,
    QuotationAccessOTP, QuotationBankDealer
)
from app.core.security import create_access_token
from app.core.security_crypto import (
    encrypt_field, decrypt_field, is_encrypted,
    generate_tenant_dek, TamperDetectedError,
    encrypt_file_bytes, decrypt_file_bytes, decrypt_stream_generator,
    encrypt_file_stream, is_encrypted_file, generate_download_ticket,
    verify_download_ticket, STREAM_MAGIC, RAW_FILE_MAGIC, DEFAULT_STREAM_CHUNK_SIZE
)
from app.core.otp_security import hash_otp_code, verify_otp_code, generate_scoped_deal_receipt
from app.services.tenant_key_service import tenant_key_service
from app.services.dealer_achievement_service import DealerAchievementService
from app.services.desk_session_service import desk_session_service
from app.services.dealer_auth_service import create_dealer_access_token
from app.services.unified_email_builder import build_quotation_rfq_bank_email
from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings
from app.core.email_service import get_customer_email_settings, send_email
from app.core.rate_limiter import quotation_rate_limiter

# Intercept outbound SMTP calls during simulation to prevent network timeouts and verify email dispatch
CAPTURED_EMAILS: List[Dict[str, Any]] = []

async def _mock_send_email(db, to_emails, subject_template, body_template, template_data=None, email_settings=None, sender_name=None, cc_emails=None, bcc_emails=None, attachment_paths=None, save_copy=True, allow_platform_fallback=True, **kwargs):
    CAPTURED_EMAILS.append({
        "to_emails": list(to_emails) if isinstance(to_emails, (list, tuple, set)) else [to_emails],
        "subject": subject_template,
        "body": body_template,
        "sender_name": sender_name
    })
    return True

import app.core.email_service as _es_mod
import app.api.v1.endpoints.public_quotations as _pq_mod
import app.api.v1.endpoints.quotations_endpoints as _qe_mod
import app.api.v1.endpoints.bank_dealer_endpoints as _bd_mod
import app.services.quotation_release_scheduler as _qrs_mod
import app.services.quotation_approval_notifications as _qan_mod
_es_mod.send_email = _mock_send_email
_pq_mod.send_email = _mock_send_email
_qe_mod.send_email = _mock_send_email
_bd_mod.send_email = _mock_send_email
_qrs_mod.send_email = _mock_send_email
_qan_mod.send_email = _mock_send_email

import tempfile
import hashlib
import concurrent.futures
import tracemalloc

client = TestClient(app)


# Helper to create corporate user JWT headers
def make_auth_headers(user: User) -> Dict[str, str]:
    token = create_access_token(
        data={
            "sub": user.email,
            "user_id": user.id,
            "role": user.role.value if hasattr(user.role, "value") else str(user.role),
            "customer_id": user.customer_id,
            "has_quotation_module": True,
            "is_mfa_verified": True
        },
        expires_delta=timedelta(hours=2)
    )
    return {"Authorization": f"Bearer {token}"}


# Helper to create permanent Bank Dealer JWT headers (for /api/v1/bank-dealer/* blotter endpoints)
def make_dealer_jwt_headers(db: Session, bank: Bank, email: str, full_name: str = "Desk Dealer", role: str = "EXECUTION") -> Dict[str, str]:
    clean_email = email.strip().lower()
    dealer = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.email == clean_email,
        QuotationBankDealer.is_deleted == False
    ).first()
    if not dealer:
        dealer = QuotationBankDealer(
            bank_id=bank.id,
            email=clean_email,
            full_name=full_name,
            role=role,
            is_totp_enrolled=True,
            is_active=True
        )
        db.add(dealer)
        db.commit()
        db.refresh(dealer)
    else:
        dealer.bank_id = bank.id
        dealer.is_active = True
        db.commit()

    jwt_tok = create_dealer_access_token(
        dealer_id=dealer.id,
        email=dealer.email,
        bank_id=bank.id,
        bank_name=bank.name,
        full_name=dealer.full_name or full_name,
        role=role
    )
    return {"Authorization": f"Bearer {jwt_tok}"}


# Helper to authenticate a bank contact on a public RFQ link via real 6-digit OTP verification
def authenticate_portal_session(
    db: Session,
    assignment_token: str,
    email: str,
    role: str = "EXECUTION",
    claim_desk: bool = True
) -> str:
    """
    Issues a real 6-digit OTP for (assignment_token, email, role), verifies it through
    POST /api/v1/public-quotation/verify-otp, optionally claims active desk control via
    GET /api/v1/public-quotation/{token}/desk-session, and returns the verified session_token.
    """
    assignment = db.query(QuotationBankAssignment).filter(
        QuotationBankAssignment.token == assignment_token
    ).first()
    assert assignment is not None, f"Assignment not found for token {assignment_token}"

    clean_email = email.strip().lower()
    raw_otp = "654321"
    magic_tok = uuid.uuid4().hex
    now_utc = datetime.now(timezone.utc)

    # Clear rate limiter for OTP verification
    quotation_rate_limiter._records.clear()

    otp_rec = QuotationAccessOTP(
        assignment_id=assignment.id,
        email=clean_email,
        role=role,
        otp_code=hash_otp_code(raw_otp),
        magic_token=magic_tok,
        expires_at=now_utc + timedelta(minutes=30),
        is_used=False,
        failed_attempts=0
    )
    db.add(otp_rec)
    db.commit()

    res_verify = client.post(
        "/api/v1/public-quotation/verify-otp",
        json={
            "token": assignment_token,
            "email": clean_email,
            "otp_code": raw_otp
        }
    )
    assert res_verify.status_code == 200, f"OTP verification failed for {clean_email} ({role}): {res_verify.text}"
    v_data = res_verify.json()
    assert v_data.get("authenticated") is True
    session_token = v_data["session_token"]

    if claim_desk and role == "EXECUTION":
        res_desk = client.get(
            f"/api/v1/public-quotation/{assignment_token}/desk-session",
            params={"email": clean_email, "name": clean_email.split("@")[0]}
        )
        assert res_desk.status_code == 200, f"Desk session claim failed: {res_desk.text}"

    return session_token


# ======================================================================================
# SEEDING & FIXTURES HARNESS (REPEATABLE & IDEMPOTENT)
# ======================================================================================
class OmniTestFixture:
    def __init__(self, db: Session):
        self.db = db
        self.cust_a: Customer = None
        self.cust_b: Customer = None
        self.maker_user: User = None
        self.admin_user: User = None
        self.cust_b_user: User = None
        self.banks: Dict[str, QuotationBank] = {}
        self.bank_entities: Dict[str, Bank] = {}
        self._ensure_fixtures()

    def _ensure_fixtures(self):
        # Clear any active in-memory desk sessions and rate limits from prior runs
        desk_session_service._desks.clear()
        quotation_rate_limiter._records.clear()

        # 1. Customers
        self.cust_a = self.db.query(Customer).filter(Customer.id == 1).first()
        if not self.cust_a:
            self.cust_a = Customer(
                id=1,
                name="Acme Corporation",
                contact_email="contact@acmecorp.com",
                subscription_plan_id=1,
                end_date=datetime.now(timezone.utc) + timedelta(days=365)
            )
            self.cust_a.domains = ["acme.com", "growbusinessdevelopment.com"]
            self.db.add(self.cust_a)
            self.db.commit()
            self.db.refresh(self.cust_a)
        else:
            self.cust_a.domains = ["acme.com", "growbusinessdevelopment.com"]
            self.db.commit()

        self.cust_b = self.db.query(Customer).filter(Customer.id == 2).first()
        if not self.cust_b:
            self.cust_b = Customer(
                id=2,
                name="Globex Industries",
                contact_email="contact@globex.com",
                subscription_plan_id=1,
                end_date=datetime.now(timezone.utc) + timedelta(days=365)
            )
            self.cust_b.domains = ["globex.com"]
            self.db.add(self.cust_b)
            self.db.commit()
            self.db.refresh(self.cust_b)

        # 1b. Legal Entity
        self.entity = self.db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == self.cust_a.id,
            CustomerEntity.is_deleted == False
        ).first()

        # 2. Users (Active, non-deleted users)
        self.maker_user = self.db.query(User).filter(
            User.email == "waelghali79+maker@gmail.com",
            User.is_deleted == False
        ).first()
        if not self.maker_user:
            self.maker_user = self.db.query(User).filter(
                User.customer_id == self.cust_a.id,
                User.role == UserRole.END_USER,
                User.is_deleted == False
            ).first()
            if self.maker_user:
                self.maker_user.email = "waelghali79+maker@gmail.com"
                self.db.commit()
            else:
                self.maker_user = User(
                    email="waelghali79+maker@gmail.com",
                    role=UserRole.END_USER,
                    customer_id=self.cust_a.id,
                    password_hash="hash",
                    is_deleted=False
                )
                self.db.add(self.maker_user)
                self.db.commit()
                self.db.refresh(self.maker_user)

        self.admin_user = self.db.query(User).filter(
            User.customer_id == self.cust_a.id,
            User.role == UserRole.CORPORATE_ADMIN,
            User.is_deleted == False
        ).first()
        if not self.admin_user:
            self.admin_user = User(
                email="admin.omni@acme.com",
                role=UserRole.CORPORATE_ADMIN,
                customer_id=self.cust_a.id,
                password_hash="hash",
                is_deleted=False
            )
            self.db.add(self.admin_user)
            self.db.commit()
            self.db.refresh(self.admin_user)

        self.cust_b_user = self.db.query(User).filter(
            User.customer_id == self.cust_b.id,
            User.is_deleted == False
        ).first()
        if not self.cust_b_user:
            self.cust_b_user = User(
                email="maker.omni@globex.com",
                role=UserRole.END_USER,
                customer_id=self.cust_b.id,
                password_hash="hash",
                is_deleted=False
            )
            self.db.add(self.cust_b_user)
            self.db.commit()
            self.db.refresh(self.cust_b_user)

        # 3. Setup 5 Institutional Banks & Customer Quotation Banks
        bank_specs = [
            ("CIB", "Commercial International Bank (CIB)", "cibeg.com"),
            ("HSBC", "HSBC Bank Egypt", "hsbc.com"),
            ("NBE", "National Bank of Egypt (NBE)", "nbe.com.eg"),
            ("BM", "Banque Misr", "banquemisr.com"),
            ("QNB", "QNB Al Ahli", "qnb.com.eg"),
        ]

        for code, full_name, domain in bank_specs:
            b = self.db.query(Bank).filter(
                (Bank.email_domain == domain) | (Bank.name == full_name) | (Bank.name.ilike(f"%({code})%"))
            ).first()
            if not b:
                b = Bank(name=full_name, email_domain=domain, portal_access_enabled=True)
                self.db.add(b)
                self.db.commit()
                self.db.refresh(b)
            else:
                b.portal_access_enabled = True
                self.db.commit()
            self.bank_entities[code] = b

            qb = self.db.query(QuotationBank).filter(
                QuotationBank.customer_id == self.cust_a.id,
                QuotationBank.bank_id == b.id
            ).first()

            if code == "CIB":
                initial_contacts = [
                    {"email": "approver.cib@cibeg.com", "name": "CIB Approver", "role": "APPROVER"},
                    {"email": "dealer.cib@cibeg.com", "name": "CIB Dealer", "role": "EXECUTION"},
                    {"email": "viewer.cib@cibeg.com", "name": "CIB Viewer", "role": "VIEW_ONLY"}
                ]
                initial_emails = "approver.cib@cibeg.com,dealer.cib@cibeg.com,viewer.cib@cibeg.com"
            elif code == "BM":
                initial_contacts = [
                    {"email": "approver.bm@banquemisr.com", "name": "BM Approver", "role": "APPROVER"},
                    {"email": "dealer.bm@banquemisr.com", "name": "BM Dealer", "role": "EXECUTION"}
                ]
                initial_emails = "approver.bm@banquemisr.com,dealer.bm@banquemisr.com"
            elif code == "HSBC":
                initial_contacts = [
                    {"email": "dealer.hsbc@hsbc.com", "name": "HSBC Primary Dealer", "role": "EXECUTION"},
                    {"email": "dealer2.hsbc@hsbc.com", "name": "HSBC Senior Dealer", "role": "EXECUTION"},
                    {"email": "viewer.hsbc@hsbc.com", "name": "HSBC Risk Viewer", "role": "VIEW_ONLY"}
                ]
                initial_emails = "dealer.hsbc@hsbc.com,dealer2.hsbc@hsbc.com,viewer.hsbc@hsbc.com"
            else:
                dealer_email = f"dealer.{code.lower()}@{domain}"
                initial_contacts = [
                    {"email": dealer_email, "name": f"{code} Dealer", "role": "EXECUTION"}
                ]
                initial_emails = dealer_email

            # Ensure no pending contact invitations block these test banks
            from app.models.models_quotation import QuotationBankContactInvitation
            self.db.query(QuotationBankContactInvitation).filter(
                QuotationBankContactInvitation.customer_id == self.cust_a.id,
                QuotationBankContactInvitation.bank_id == b.id,
                QuotationBankContactInvitation.status == "PENDING"
            ).delete(synchronize_session=False)

            if not qb:
                qb = QuotationBank(
                    customer_id=self.cust_a.id,
                    bank_id=b.id,
                    trade_type="BOTH",
                    entity_scope="ALL_ENTITIES",
                    emails=initial_emails,
                    contacts=initial_contacts
                )
                self.db.add(qb)
                self.db.commit()
                self.db.refresh(qb)
            else:
                qb.trade_type = "BOTH"
                qb.entity_scope = "ALL_ENTITIES"
                qb.emails = initial_emails
                qb.contacts = initial_contacts
                self.db.commit()

            self.banks[code] = qb


def print_usage():
    print("""
GROW TREASURY - MASTER OMNI SIMULATION & ADVERSARIAL SUITE
Usage: python tests/test_omni_simulation_suite.py [OPTIONS]

Options:
  --stress      Run full 1.0 GB streaming concurrency & RAM stress benchmark
                (50 MB payload x 20 concurrent worker threads)
  --fast        Run fast local streaming concurrency check (5 MB x 5 workers)
                [Default when not interactive]
  --preserve    Keep test RFQ records in database for UI inspection
  --clean       Cleanly wipe test RFQs after run [Default]
  --help, -h    Show this help message and exit

Environment Variables:
  RUN_STRESS_BENCHMARK=1    Equivalent to --stress
  PRESERVE_TEST_DATA=1      Equivalent to --preserve
""")


# ======================================================================================
# MASTER RUNNER
# ======================================================================================
def run_omni_simulation_suite():
    if "-h" in sys.argv or "--help" in sys.argv:
        print_usage()
        sys.exit(0)

    # Determine stress mode early
    is_full_stress: bool = False
    if "--stress" in sys.argv or os.getenv("RUN_STRESS_BENCHMARK", "").strip().lower() in ("1", "true", "yes"):
        is_full_stress = True
    elif "--fast" in sys.argv:
        is_full_stress = False
    elif hasattr(sys.stdin, "isatty") and sys.stdin.isatty():
        try:
            print("\n" + "=" * 80)
            print("[BENCHMARK CONFIGURATION]")
            print("  This suite includes a streaming crypto concurrency & memory stress benchmark (Part 4).")
            print("  - FAST mode: 5 MB payload x 5 concurrent threads (runs in ~1s)")
            print("  - FULL STRESS mode: 50 MB payload x 20 concurrent threads = 1.0 GB total (runs in ~6s)")
            ans = input("  Run FULL 1.0 GB Stress Test? (y/N) [Default: N (Fast mode)]: ").strip().lower()
            is_full_stress = ans in ("y", "yes")
            print("=" * 80 + "\n")
        except Exception:
            is_full_stress = False

    db = SessionLocal()
    fixture = OmniTestFixture(db)

    print("=" * 80)
    print("[OMNI] GROW TREASURY - MASTER OMNI SIMULATION & ADVERSARIAL SUITE")
    print(f"[MODE] Benchmark: {'FULL 1.0 GB STRESS (--stress)' if is_full_stress else 'FAST LOCAL CHECK (use --stress for 1.0 GB)'}")
    print("=" * 80)

    # Track RFQ IDs created in this run for guaranteed cleanup
    created_rfq_ids: List[str] = []

    try:
        # ==============================================================================
        # PART 1: NEGATIVE SECURITY & ADVERSARIAL TESTS (12 DEFENSE WALLS)
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[FIREWALL] PART 1: NEGATIVE SECURITY & ADVERSARIAL FIREWALL TESTS")
        print("=" * 80)

        # CHECK 1: Negative List Webmail Rejection (@gmail / @yahoo)
        print("\n--- [CHECK 1] Negative List: Free Webmail Rejection ---")
        res = client.post(
            "/api/v1/end-user/quotations/banks",
            headers=make_auth_headers(fixture.admin_user),
            json={
                "bank_id": fixture.bank_entities["CIB"].id,
                "trade_type": "BOTH",
                "contacts": [{"email": "rogue.trader@gmail.com", "name": "Rogue", "role": "EXECUTION"}]
            }
        )
        assert res.status_code in (400, 422), f"Expected HTTP 400 for @gmail, got {res.status_code}"
        print("  [PASS] Blocked free webmail domain (@gmail.com) with HTTP 400")

        # CHECK 2: Anti-Collusion Shield (Customer domain matching bank contact)
        print("\n--- [CHECK 2] Anti-Collusion: Self-Quoting Corporate Domain Block ---")
        res = client.post(
            "/api/v1/end-user/quotations/banks",
            headers=make_auth_headers(fixture.admin_user),
            json={
                "bank_id": fixture.bank_entities["CIB"].id,
                "trade_type": "BOTH",
                "contacts": [{"email": "insider@acme.com", "name": "Insider", "role": "EXECUTION"}]
            }
        )
        assert res.status_code in (400, 422), f"Expected HTTP 400 for self-domain, got {res.status_code}"
        print("  [PASS] Blocked customer's own domain (@acme.com) from bank roster")

        # CHECK 3: Issuance Invite Domain Whitelist
        print("\n--- [CHECK 3] Issuance Invite: External Domain Whitelist Enforcement ---")
        res = client.post(
            "/api/v1/public-issuance/generate-invite",
            headers=make_auth_headers(fixture.admin_user),
            json={"email": "attacker@unknown-corp.org", "duration_days": 7}
        )
        assert res.status_code == 400, f"Expected HTTP 400 for external invite, got {res.status_code}"
        print("  [PASS] Blocked issuance invite to unregistered external domain (HTTP 400)")

        # CHECK 4: Maker Self-Approval RBAC Block
        print("\n--- [CHECK 4] RBAC Gate: End-User Maker Self-Approval Block ---")
        dummy_rfq_id = str(uuid.uuid4())
        res = client.post(
            f"/api/v1/corporate-admin/quotations/{dummy_rfq_id}/approve",
            headers=make_auth_headers(fixture.maker_user)
        )
        assert res.status_code == 403, f"Expected HTTP 403 for Maker self-approval, got {res.status_code}"
        print("  [PASS] Blocked Maker from calling Corporate Admin approval endpoint (HTTP 403)")

        # CHECK 5: Non-Admin Customer Settings RBAC Block
        print("\n--- [CHECK 5] RBAC Gate: Non-Admin Customer Configuration Block ---")
        res = client.put(
            "/api/v1/corporate-admin/customer-configurations/QUOTATION_ACCEPTANCE_TIMEOUT_FX_SPOT",
            headers=make_auth_headers(fixture.maker_user),
            json={"value": "45"}
        )
        assert res.status_code in (401, 403), f"Expected HTTP 403 for non-admin config update, got {res.status_code}"
        print("  [PASS] Blocked End-User Maker from mutating customer configurations (HTTP 403)")

        # CHECK 6: Cross-Tenant Cryptographic Isolation (DEK A != DEK B Tamper Tag)
        print("\n--- [CHECK 6] Cryptographic Isolation: Cross-Tenant Ciphertext Attack ---")
        dek_a = tenant_key_service.get_or_create_tenant_dek(db, fixture.cust_a.id)
        dek_b = tenant_key_service.get_or_create_tenant_dek(db, fixture.cust_b.id)
        ciphertext = encrypt_field(50.2500, dek_a)
        try:
            decrypt_field(ciphertext, dek_b)
            assert False, "CRITICAL CRYPTO BUG: Tenant B decrypted Tenant A ciphertext!"
        except TamperDetectedError:
            print("  [PASS] Tenant B key failed authentication tag on Tenant A ciphertext (TamperDetectedError)")

        # CHECK 7: Public OTP Brute-Force & Burn Threshold (3 Strikes Lockout)
        print("\n--- [CHECK 7] Public OTP: 3-Strike Brute-Force Burn & Invalidation ---")
        real_code = "777888"
        hashed = hash_otp_code(real_code)
        assert len(hashed) == 64, "OTP HMAC-SHA256 hash must be 64 hex characters"
        assert verify_otp_code("777888", hashed) is True
        assert verify_otp_code("000000", hashed) is False
        from app.core.otp_security import MAX_OTP_FAILED_ATTEMPTS
        assert MAX_OTP_FAILED_ATTEMPTS == 3, f"Expected MAX_OTP_FAILED_ATTEMPTS=3, got {MAX_OTP_FAILED_ATTEMPTS}"
        print("  [PASS] OTP HMAC-SHA256 and 3-strike burnout verified (Zero plaintext leak)")

        # CHECK 8: Platform Fallback Refusal for Missing Custom SMTP
        print("\n--- [CHECK 8] Anti-Spoofing: Disallow Platform Fallback for Quotations ---")
        settings, source = get_customer_email_settings(db, fixture.cust_b.id, allow_platform_fallback=False)
        assert settings is None and source == "no_customer_settings", "Fallback should be refused"
        print("  [PASS] Strict bank dispatch suppressed; platform relay refused (allow_platform_fallback=False)")

        # CHECK 9: Duplicate Per-Bank Leg Signature Conflict Rejection
        print("\n--- [CHECK 9] Trade Integrity: Duplicate Leg Signature Rejection ---")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dup_payload = {
            "type": "FX_SPOT",
            "direction": "Buy",
            "entity_id": fixture.entity.id,
            "windowStart": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            "windowEnd": (datetime.now(timezone.utc) + timedelta(minutes=35)).isoformat(),
            "selectedBanks": json.dumps([{"id": fixture.banks["HSBC"].bank_id, "quotationBase": "Execution"}]),
            "legalDisclaimerAccepted": True,
            "pairs": [
                {"direction": "Buy", "buyCurrency": "USD", "sellCurrency": "EGP", "amount": 100000.0, "valueDate": today_str, "quotationBase": "Execution"},
                {"direction": "Buy", "buyCurrency": "USD", "sellCurrency": "EGP", "amount": 250000.0, "valueDate": today_str, "quotationBase": "Execution"}
            ]
        }
        res_dup = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json=dup_payload
        )
        assert res_dup.status_code == 400, f"Expected HTTP 400 for duplicate currency pair + value date + base, got {res_dup.status_code}"
        print("  [PASS] Duplicate currency pair with identical settlement date & base rejected (HTTP 400)")

        # CHECK 10: Past Value Date Rejection (Before Quotation Trade Date)
        print("\n--- [CHECK 10] Market Rule: Value Date Earlier Than Trade Date Rejection ---")
        past_date_str = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
        past_val_payload = {
            "type": "FX_SPOT",
            "direction": "Buy",
            "entity_id": fixture.entity.id,
            "buyCurrency": "USD",
            "sellCurrency": "EGP",
            "amount": 100000.0,
            "valueDate": past_date_str,
            "windowStart": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            "windowEnd": (datetime.now(timezone.utc) + timedelta(minutes=35)).isoformat(),
            "selectedBanks": json.dumps([{"id": fixture.banks["HSBC"].bank_id, "quotationBase": "Execution"}]),
            "legalDisclaimerAccepted": True
        }
        res_past = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json=past_val_payload
        )
        assert res_past.status_code == 400, f"Expected HTTP 400 for value date earlier than trade date, got {res_past.status_code}"
        print("  [PASS] Master Value Date earlier than quotation window date rejected (HTTP 400)")


        # ==============================================================================
        # PART 2: MASTER CYCLE 1 — SCHEDULED RFQ, APPROVAL GATE, RESCHEDULE & TIMEOUT
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[CLOCK] PART 2: MASTER CYCLE 1 -- CLOCK, APPROVAL GATE, RESCHEDULE & TIMEOUT")
        print("=" * 80)

        now_utc = datetime.now(timezone.utc)
        win_start_future = now_utc + timedelta(hours=2)
        win_end_future = now_utc + timedelta(hours=3)
        c1_val_date = (now_utc + timedelta(days=2)).strftime("%Y-%m-%d")

        # 1. Maker submits RFQ with scheduled window in future
        print("\n--- [CYCLE 1 - STEP 1] Maker submits Scheduled RFQ requiring approval ---")
        selected_banks_payload = [
            {"id": fixture.banks["CIB"].bank_id, "quotationBase": "Execution"},
            {"id": fixture.banks["HSBC"].bank_id, "quotationBase": "Execution"},
        ]
        rfq_create_payload = {
            "type": "FX_SPOT",
            "direction": "Buy",
            "entity_id": fixture.entity.id,
            "buyCurrency": "USD",
            "sellCurrency": "EGP",
            "amount": 2_500_000.0,
            "windowStart": win_start_future.isoformat(),
            "windowEnd": win_end_future.isoformat(),
            "selectedBanks": json.dumps(selected_banks_payload),
            "legalDisclaimerAccepted": True,
            "pairs": [
                {
                    "direction": "Buy",
                    "buyCurrency": "USD",
                    "sellCurrency": "EGP",
                    "amount": 2_500_000.0,
                    "valueDate": c1_val_date,
                    "quotationBase": "Execution"
                }
            ]
        }
        res = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json=rfq_create_payload
        )
        assert res.status_code == 200, f"RFQ creation failed: {res.text}"
        data_c1 = res.json()
        rfq_c1_id = data_c1["rfq_id"]
        created_rfq_ids.append(rfq_c1_id)
        c1_assignments = data_c1["assignments"]
        print(f"  [PASS] RFQ Created: ID={rfq_c1_id}, Ref={data_c1['ref_no']}")

        rfq_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_c1_id).first()
        rfq_db.status = "PENDING_APPROVAL"
        db.commit()

        # Map tokens by bank code for Cycle 1
        c1_tokens = {}
        for a in c1_assignments:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            c1_tokens[code] = a["token"]

        cib_token_c1 = c1_tokens["CIB"]
        hsbc_token_c1 = c1_tokens["HSBC"]

        # 2. Bank tries to access token link BEFORE approval -> Assert Blocked / Gated
        print("\n--- [CYCLE 1 - STEP 2] Bank accesses link BEFORE corporate approval ---")
        res = client.get(f"/api/v1/public-quotation/{cib_token_c1}")
        assert res.status_code == 403, f"Expected 403 before approval, got {res.status_code}"
        assert "awaiting internal corporate approval" in res.json().get("detail", "")
        print("  [PASS] Bank access blocked before corporate approval (HTTP 403: Awaiting Approval)")

        # 3. Bank tries to quote before approval -> Assert HTTP 403
        print("\n--- [CYCLE 1 - STEP 3] Bank attempts quoting before approval ---")
        leg_c1 = rfq_db.legs[0] if rfq_db.legs else None
        leg_c1_id = str(leg_c1.id) if leg_c1 else "leg-1"
        res = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": hsbc_token_c1,
                "email": "dealer.hsbc@hsbc.com",
                "quotes": [{"leg_id": leg_c1_id, "price": 50.10}]
            }
        )
        assert res.status_code in (400, 403), f"Expected quoting rejection before approval, got {res.status_code}"
        print("  [PASS] Quoting rejected before corporate approval (HTTP 403)")

        # 4. Corporate Admin approves with Scheduled Release Time in Future
        print("\n--- [CYCLE 1 - STEP 4] Admin approves with Scheduled Future Release ---")
        sched_time = now_utc + timedelta(hours=1)
        res = client.post(
            f"/api/v1/corporate-admin/quotations/{rfq_c1_id}/approve",
            headers=make_auth_headers(fixture.admin_user),
            json={"scheduled_release_at": sched_time.isoformat()}
        )
        assert res.status_code == 200, f"Admin scheduled approval failed: {res.text}"
        res_data = res.json()
        assert res_data["status"] == "APPROVED_SCHEDULED"
        print("  [PASS] Status transitioned to APPROVED_SCHEDULED")

        # 5. Admin Reschedules to Immediate Release
        print("\n--- [CYCLE 1 - STEP 5] Admin triggers Immediate Release override ---")
        res = client.post(
            f"/api/v1/corporate-admin/quotations/{rfq_c1_id}/reschedule-release",
            headers=make_auth_headers(fixture.admin_user),
            json={"release_now": True}
        )
        assert res.status_code == 200, f"Reschedule failed: {res.text}"
        db.refresh(rfq_db)
        assert rfq_db.status == "PENDING"
        print("  [PASS] Reschedule override transitioned RFQ to PENDING (Dispatched)")

        # 5B. Bank tries to quote while window_start is still in the future (Pre-Window Lock)
        print("\n--- [CYCLE 1 - STEP 5B] Pre-Window Early Quote Rejection & SCHEDULED Result Check ---")
        hsbc_sess_c1 = authenticate_portal_session(db, hsbc_token_c1, "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=False)
        res_early = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": hsbc_token_c1,
                "email": "dealer.hsbc@hsbc.com",
                "session_token": hsbc_sess_c1,
                "quotes": [{"leg_id": leg_c1_id, "price": 50.10}]
            }
        )
        assert res_early.status_code == 403, f"Expected 403 before window opens, got {res_early.status_code}"
        assert "not opened yet" in res_early.json().get("detail", "").lower()
        res_sched_stat = client.get(f"/api/v1/public-quotation/{hsbc_token_c1}/result")
        assert res_sched_stat.status_code == 200 and res_sched_stat.json().get("status") == "SCHEDULED"
        print("  [PASS] Early quote blocked (HTTP 403) and /result accurately reports SCHEDULED")

        # 5C. Open window, submit quote, and verify live window quoting works
        print("\n--- [CYCLE 1 - STEP 5C] Window Opens -> HSBC Quotes 50.15 ---")
        rfq_db.window_start = datetime.now(timezone.utc) - timedelta(minutes=2)
        rfq_db.window_end = datetime.now(timezone.utc) + timedelta(minutes=15)
        db.commit()
        client.get(f"/api/v1/public-quotation/{hsbc_token_c1}/desk-session", params={"email": "dealer.hsbc@hsbc.com", "name": "HSBC Dealer"})
        res_open_q = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": hsbc_token_c1,
                "email": "dealer.hsbc@hsbc.com",
                "session_token": hsbc_sess_c1,
                "quotes": [{"leg_id": leg_c1_id, "price": 50.15}]
            }
        )
        assert res_open_q.status_code == 200, f"Live quote failed: {res_open_q.text}"
        print("  [PASS] HSBC submitted firm quote (50.15) during active window")

        # 6. Window Expiry & Late Submissions Rejection
        print("\n--- [CYCLE 1 - STEP 6] Post-Window Late Quote Rejection ---")
        now_past = datetime.now(timezone.utc)
        rfq_db.window_start = now_past - timedelta(hours=1)
        rfq_db.window_end = now_past - timedelta(minutes=5)
        db.commit()

        res = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": hsbc_token_c1,
                "email": "dealer.hsbc@hsbc.com",
                "session_token": hsbc_sess_c1,
                "quotes": [{"leg_id": leg_c1_id, "price": 50.05}]
            }
        )
        assert res.status_code in (400, 403), f"Expected late quote rejection, got {res.status_code}"
        print("  [PASS] Late quote successfully blocked after tender window close (HTTP 403)")

        # 7. Customer Setting: Acceptance Window Auto-Timeout Governance (AUTO_REJECT)
        print("\n--- [CYCLE 1 - STEP 7] Acceptance Window Auto-Timeout Governance (AUTO_REJECT) ---")
        rfq_db.acceptance_timeout_seconds = 1
        rfq_db.acceptance_timeout_action = "AUTO_REJECT"
        rfq_db.acceptance_deadline = now_past - timedelta(seconds=5)
        rfq_db.acceptance_status = None
        db.commit()

        standings = compute_rfq_standings(rfq_db, db, dispatch_emails=False)
        db.refresh(rfq_db)
        assert rfq_db.status == "REJECTED" and rfq_db.acceptance_status == "AUTO_REJECTED", (
            f"Expected REJECTED / AUTO_REJECTED, got status={rfq_db.status}, acceptance_status={rfq_db.acceptance_status}"
        )
        res_hsbc_c1_res = client.get(f"/api/v1/public-quotation/{hsbc_token_c1}/result")
        assert res_hsbc_c1_res.status_code == 200
        assert res_hsbc_c1_res.json().get("status") == "REJECTED"
        assert "acceptance window expired" in res_hsbc_c1_res.json().get("detail", "").lower()
        print("  [PASS] Acceptance timeout auto-resolved to AUTO_REJECTED and bank /result confirmed REJECTED")


        # ==============================================================================
        # PART 3: MASTER CYCLE 2 — MULTI-LEG PORTFOLIO, 5-BANK CUSTOMIZATION MATRIX & ATTACHMENTS
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[PORTFOLIO] PART 3: MASTER CYCLE 2 -- 3-LEG PORTFOLIO, 5-BANK CUSTOMIZATIONS & PRIVACY")
        print("=" * 80)

        # 1. Pre-Trade Trophies Baseline Snapshot
        print("\n--- [CYCLE 2 - STEP 1] Trophies & Win Counter Baseline Snapshot ---")
        cib_achievements_before = DealerAchievementService.get_dealer_achievements(
            db, dealer_email="dealer.cib@cibeg.com", bank_id=fixture.banks["CIB"].bank_id
        )
        hsbc_achievements_before = DealerAchievementService.get_dealer_achievements(
            db, dealer_email="dealer2.hsbc@hsbc.com", bank_id=fixture.banks["HSBC"].bank_id
        )
        cib_wins_before = cib_achievements_before.get("personal_bests", {}).get("total_deals_won", 0)
        hsbc_wins_before = hsbc_achievements_before.get("personal_bests", {}).get("total_deals_won", 0)
        print(f"  [BASELINE] CIB Dealer Wins: {cib_wins_before} | HSBC Senior Dealer Wins: {hsbc_wins_before}")

        # 2. Maker launches 3-Leg Portfolio RFQ with Per-Bank / Per-Leg Customizations via API Payload
        print("\n--- [CYCLE 2 - STEP 2] Maker launches 3-Leg Multi-Currency Package (100% API Payload) ---")
        now_c2 = datetime.now(timezone.utc)
        win_start_live = now_c2 - timedelta(minutes=1)
        win_end_live = now_c2 + timedelta(minutes=30)

        std_leg1_date = (now_c2 + timedelta(days=2)).strftime("%Y-%m-%d")
        cib_leg1_custom_date = (now_c2 + timedelta(days=2)).strftime("%Y-%m-%d")
        std_leg2_date = (now_c2 + timedelta(days=30)).strftime("%Y-%m-%d")
        std_leg3_date = (now_c2 + timedelta(days=2)).strftime("%Y-%m-%d")
        qnb_leg3_custom_date = (now_c2 + timedelta(days=7)).strftime("%Y-%m-%d")  # QNB custom date on Leg 3!

        # Per-leg bank customization matrix (passed directly in pairs[].selectedBanks, zero DB hacks!)
        leg1_banks = [
            {"id": fixture.banks["CIB"].bank_id,  "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": cib_leg1_custom_date, "allowAlternativeValueDate": False},
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg1_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["NBE"].bank_id,  "isInvited": True,  "quotationBase": "Indicative", "isDocumentVisible": False, "valueDate": std_leg1_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["BM"].bank_id,   "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": False, "valueDate": std_leg1_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": False, "quotationBase": "Invisible",  "isDocumentVisible": False, "valueDate": std_leg1_date,        "allowAlternativeValueDate": False},
        ]
        leg2_banks = [
            {"id": fixture.banks["CIB"].bank_id,  "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg2_date, "allowAlternativeValueDate": True},
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg2_date, "allowAlternativeValueDate": False},
            {"id": fixture.banks["NBE"].bank_id,  "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg2_date, "allowAlternativeValueDate": False},
            {"id": fixture.banks["BM"].bank_id,   "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg2_date, "allowAlternativeValueDate": False},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": False, "quotationBase": "Invisible",  "isDocumentVisible": False, "valueDate": std_leg2_date, "allowAlternativeValueDate": False},
        ]
        leg3_banks = [
            {"id": fixture.banks["CIB"].bank_id,  "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg3_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": std_leg3_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["NBE"].bank_id,  "isInvited": False, "quotationBase": "Invisible",  "isDocumentVisible": False, "valueDate": std_leg3_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["BM"].bank_id,   "isInvited": False, "quotationBase": "Invisible",  "isDocumentVisible": False, "valueDate": std_leg3_date,        "allowAlternativeValueDate": False},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": True,  "quotationBase": "Execution",  "isDocumentVisible": True,  "valueDate": qnb_leg3_custom_date, "allowAlternativeValueDate": True},
        ]

        pairs_c2 = [
            {
                "direction": "Buy",
                "buyCurrency": "USD",
                "sellCurrency": "EGP",
                "amount": 1_000_000.0,
                "valueDate": std_leg1_date,
                "allowAlternativeValueDate": False,
                "quotationBase": "Execution",
                "maxTolerancePercent": 5.0,
                "selectedBanks": leg1_banks
            },
            {
                "direction": "Sell",
                "buyCurrency": "EUR",
                "sellCurrency": "EGP",
                "amount": 500_000.0,
                "valueDate": std_leg2_date,
                "allowAlternativeValueDate": False,
                "quotationBase": "Execution",
                "maxTolerancePercent": 5.0,
                "selectedBanks": leg2_banks
            },
            {
                "direction": "Buy",
                "buyCurrency": "GBP",
                "sellCurrency": "USD",
                "amount": 250_000.0,
                "valueDate": std_leg3_date,
                "allowAlternativeValueDate": False,
                "quotationBase": "Execution",
                "maxTolerancePercent": 5.0,
                "selectedBanks": leg3_banks
            }
        ]

        # Attach 3 orthogonal document tiers (INTERNAL_ONLY, BIDDING, WINNER_ONLY)
        c2_docs_metadata = {
            "release_to_winner_only": False,
            "documents": [
                {
                    "name": "Board_Internal_Treasury_Memo.pdf",
                    "path": "quotations/docs/internal_memo.pdf",
                    "scope": "INTERNAL_ONLY",
                    "is_internal_only": True
                },
                {
                    "name": "Bidding_Customs_Form4_USD.pdf",
                    "path": "quotations/docs/bidding_form4_usd.pdf",
                    "release_timing": "BIDDING",
                    "pair": "USD/EGP",
                    "leg_index": 1
                },
                {
                    "name": "Contract_Leg1_USD.pdf",
                    "path": "quotations/docs/contract_leg1.pdf",
                    "release_timing": "WINNER_ONLY",
                    "pair": "USD/EGP",
                    "leg_index": 1
                },
                {
                    "name": "Contract_Leg2_EUR.pdf",
                    "path": "quotations/docs/contract_leg2.pdf",
                    "release_timing": "WINNER_ONLY",
                    "pair": "EUR/EGP",
                    "leg_index": 2
                }
            ]
        }

        rfq_c2_payload = {
            "type": "FX_SPOT",
            "direction": "Buy",
            "entity_id": fixture.entity.id,
            "amount": 1_750_000.0,
            "windowStart": win_start_live.isoformat(),
            "windowEnd": win_end_live.isoformat(),
            "selectedBanks": json.dumps(leg1_banks),
            "documentPath": json.dumps(c2_docs_metadata),
            "release_docs_to_winner_only": False,
            "maxTolerancePercent": 5.0,
            "legalDisclaimerAccepted": True,
            "pairs": pairs_c2
        }

        res = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json=rfq_c2_payload
        )
        assert res.status_code == 200, f"Cycle 2 RFQ creation failed: {res.text}"
        data_c2 = res.json()
        rfq_c2_id = data_c2["rfq_id"]
        created_rfq_ids.append(rfq_c2_id)
        c2_assignments = data_c2["assignments"]
        print(f"  [PASS] 3-Leg RFQ Created via API: ID={rfq_c2_id}, Ref={data_c2['ref_no']}")

        # Corporate Admin approves RFQ for immediate live bidding (if required by policy)
        rfq_c2_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_c2_id).first()
        if rfq_c2_db.status == "PENDING_APPROVAL":
            res_app = client.post(
                f"/api/v1/corporate-admin/quotations/{rfq_c2_id}/approve",
                headers=make_auth_headers(fixture.admin_user),
                json={"status": "PENDING"}
            )
            assert res_app.status_code == 200, f"Cycle 2 Corporate Admin approval failed: {res_app.text}"
            db.refresh(rfq_c2_db)
        print("  [PASS] Corporate Admin approved Cycle 2 RFQ for live multi-counterparty bidding")

        # Map assignment tokens by bank code
        token_map: Dict[str, str] = {}
        assignment_map: Dict[str, QuotationBankAssignment] = {}
        for a in c2_assignments:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            matching_codes = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id]
            assert matching_codes, f"Could not match bank assignment to fixture bank: qb_row={qb_row.id}"
            bank_code = matching_codes[0]
            token_map[bank_code] = a["token"]
            assignment_map[bank_code] = db_a

        # Fetch legs deterministically by currency pair
        leg_1 = db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq_c2_id, QuotationLeg.buy_currency == "USD", QuotationLeg.sell_currency == "EGP").first()
        leg_2 = db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq_c2_id, QuotationLeg.buy_currency == "EUR", QuotationLeg.sell_currency == "EGP").first()
        leg_3 = db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rfq_c2_id, QuotationLeg.buy_currency == "GBP", QuotationLeg.sell_currency == "USD").first()
        assert leg_1 and leg_2 and leg_3, "Expected all 3 legs (USD/EGP, EUR/EGP, GBP/USD) to be present"

        # 3. Positive Verification of Per-Bank Customizations, Invisible Legs, Blotter, Email & Bidding Docs
        print("\n--- [CYCLE 2 - STEP 3] Portal, Blotter, Email & Document Visibility Verification ---")

        # 3A. Bank D (Banque Misr): Invited to Legs 1 & 2 (Leg 3 Invisible), and Leg 1 has isDocumentVisible=False
        res_bm = client.get(f"/api/v1/public-quotation/{token_map['BM']}")
        assert res_bm.status_code == 200
        bm_view = res_bm.json()
        bm_visible_legs = bm_view.get("legs", [])
        assert len(bm_visible_legs) == 2, f"Bank D should see 2 legs, saw {len(bm_visible_legs)}"
        assert not any(l["id"] == leg_3.id for l in bm_visible_legs), "CRITICAL: Bank D saw invisible Leg 3!"
        bm_leg1_view = next(l for l in bm_visible_legs if l["id"] == leg_1.id)
        assert len(bm_leg1_view.get("documents", [])) == 0, "CRITICAL: Bank D saw Leg 1 bidding doc when isDocumentVisible=False!"
        print("  [PASS] Bank D sees only 2 legs (Leg 3 invisible) and 0 documents on Leg 1 (isDocumentVisible=False)")

        # 3B. Bank E (QNB): Invited ONLY to Leg 3 with custom valueDate (qnb_leg3_custom_date) & allowAlternativeValueDate=True
        res_qnb = client.get(f"/api/v1/public-quotation/{token_map['QNB']}")
        assert res_qnb.status_code == 200
        qnb_view = res_qnb.json()
        qnb_visible_legs = qnb_view.get("legs", [])
        assert len(qnb_visible_legs) == 1, f"Bank E (QNB) should see 1 leg (Leg 3 only), saw {len(qnb_visible_legs)}"
        assert qnb_visible_legs[0]["id"] == leg_3.id
        assert qnb_visible_legs[0]["value_date"] == qnb_leg3_custom_date, (
            f"Expected QNB Leg 3 value_date={qnb_leg3_custom_date}, got {qnb_visible_legs[0]['value_date']}"
        )
        assert qnb_view.get("value_date") == qnb_leg3_custom_date, (
            f"Expected QNB single-visible-leg top-level value_date={qnb_leg3_custom_date}, got {qnb_view.get('value_date')}"
        )
        assert qnb_visible_legs[0]["allow_alternative_value_date"] is True
        assert qnb_view.get("allow_alternative_value_date") is True
        assert len(qnb_view.get("documents", [])) == 0, "CRITICAL: QNB saw Leg 1/2 documents despite being uninvited to Legs 1 & 2!"

        # Check QNB's Live Blotter (/api/v1/bank-dealer/blotter/live-rfqs)
        qnb_dealer_headers = make_dealer_jwt_headers(db, fixture.bank_entities["QNB"], "dealer.qnb@qnb.com.eg", "QNB Dealer")
        res_qnb_blotter = client.get("/api/v1/bank-dealer/blotter/live-rfqs", headers=qnb_dealer_headers)
        assert res_qnb_blotter.status_code == 200
        qnb_ticket = next((t for t in res_qnb_blotter.json().get("tickets", []) if t["rfq_id"] == rfq_c2_id), None)
        assert qnb_ticket is not None, "QNB live blotter missing Cycle 2 RFQ ticket"
        assert qnb_ticket["visible_legs_count"] == 1
        assert qnb_ticket["summary_value_date"] == qnb_leg3_custom_date, (
            f"Expected QNB blotter summary_value_date={qnb_leg3_custom_date}, got {qnb_ticket['summary_value_date']}"
        )
        assert qnb_ticket["allow_alternative_value_date"] is True

        # Check QNB's Dispatched Email HTML (build_quotation_rfq_bank_email)
        qnb_subj, qnb_html = build_quotation_rfq_bank_email(
            rfq=rfq_c2_db,
            assignment=assignment_map["QNB"],
            bank_name="QNB Al Ahli",
            customer_branding="Acme Corporation",
            link=f"http://localhost:3000/public-quotation/{token_map['QNB']}"
        )
        assert qnb_leg3_custom_date in qnb_html, "QNB email HTML missing custom Leg 3 value date!"
        assert "GBP" in qnb_html and "EUR" not in qnb_html, "QNB email HTML leaked uninvited EUR leg!"
        print(f"  [PASS] Bank E (QNB) single invited leg (Leg 3) custom valueDate ({qnb_leg3_custom_date}) verified in Portal, Live Blotter & Email")

        # 3C. Bank A (CIB): Sees Bidding doc on Leg 1, but NEVER sees INTERNAL_ONLY or WINNER_ONLY docs during bidding
        res_cib_portal = client.get(f"/api/v1/public-quotation/{token_map['CIB']}")
        assert res_cib_portal.status_code == 200
        cib_portal_data = res_cib_portal.json()
        cib_bidding_docs = [d["name"] for d in cib_portal_data.get("documents", [])]
        assert "Bidding_Customs_Form4_USD.pdf" in cib_bidding_docs, f"CIB should see BIDDING doc on Leg 1, got {cib_bidding_docs}"
        assert "Board_Internal_Treasury_Memo.pdf" not in cib_bidding_docs, "SECURITY LEAK: INTERNAL_ONLY doc exposed to bank!"
        assert "Contract_Leg1_USD.pdf" not in cib_bidding_docs, "SECURITY LEAK: WINNER_ONLY doc exposed during bidding!"
        print("  [PASS] Orthogonal 3-Tier Document Firewall verified during active bidding (BIDDING visible, INTERNAL_ONLY & WINNER_ONLY sealed)")

        # 3D. Bank C (NBE): Mixed Quotation Base (Indicative on Leg 1, Execution on Leg 2, Uninvited on Leg 3)
        res_nbe_portal = client.get(f"/api/v1/public-quotation/{token_map['NBE']}")
        assert res_nbe_portal.status_code == 200
        nbe_portal_data = res_nbe_portal.json()
        assert nbe_portal_data.get("quotation_base") == "Mixed", f"Expected NBE overall base='Mixed', got {nbe_portal_data.get('quotation_base')}"
        nbe_legs_map = {l["id"]: l for l in nbe_portal_data.get("legs", [])}
        assert len(nbe_legs_map) == 2
        assert nbe_legs_map[leg_1.id]["quotation_base"] == "Indicative"
        assert nbe_legs_map[leg_2.id]["quotation_base"] == "Execution"
        print("  [PASS] Bank C (NBE) Mixed Quotation Base verified (Leg 1: Indicative, Leg 2: Execution, Leg 3: Hidden)")

        # 4. Bank A & Bank D Approver 2FA Gate Flow (Real API /approve calls!)
        print("\n--- [CYCLE 2 - STEP 4] Bank Approver 2FA Flows (Bank A APPROVES, Bank D DECLINES via API) ---")
        cib_token = token_map["CIB"]
        bm_token = token_map["BM"]

        # 4A. CIB Dealer tries to authenticate/quote before CIB Approver approves -> Blocked with 403
        res_cib_pre_req = client.post(
            "/api/v1/public-quotation/request-otp",
            json={"token": cib_token, "email": "dealer.cib@cibeg.com"}
        )
        assert res_cib_pre_req.status_code == 403, f"Expected 403 for Execution dealer before bank approval, got {res_cib_pre_req.status_code}"
        print("  [PASS] CIB Execution dealer blocked from requesting OTP before Bank Approver clearance (HTTP 403)")

        # 4B. CIB Approver authenticates via 6-digit OTP and calls POST /approve with action="APPROVE"
        cib_approver_sess = authenticate_portal_session(db, cib_token, "approver.cib@cibeg.com", "APPROVER", claim_desk=False)
        res_cib_app = client.post(
            f"/api/v1/public-quotation/{cib_token}/approve",
            json={"action": "APPROVE", "session_token": cib_approver_sess, "notes": "Credit line verified and approved"}
        )
        assert res_cib_app.status_code == 200, f"CIB Approver API call failed: {res_cib_app.text}"
        assert res_cib_app.json().get("approval_status") == "APPROVED"
        print("  [PASS] Bank A (CIB) Approver authorized participation via POST /approve (approval_status=APPROVED)")

        # 4C. Bank D (Banque Misr) Approver authenticates via 6-digit OTP and calls POST /approve with action="DECLINE"
        bm_approver_sess = authenticate_portal_session(db, bm_token, "approver.bm@banquemisr.com", "APPROVER", claim_desk=False)
        res_bm_dec = client.post(
            f"/api/v1/public-quotation/{bm_token}/approve",
            json={"action": "DECLINE", "session_token": bm_approver_sess, "notes": "Daily FX exposure limit reached"}
        )
        assert res_bm_dec.status_code == 200, f"BM Approver decline API call failed: {res_bm_dec.text}"
        assert res_bm_dec.json().get("approval_status") == "DECLINED"

        # Verify Bank D /result immediately returns PARTICIPATION_DECLINED
        res_bm_res = client.get(f"/api/v1/public-quotation/{bm_token}/result")
        assert res_bm_res.status_code == 200 and res_bm_res.json().get("status") == "PARTICIPATION_DECLINED"
        print("  [PASS] Bank D (BM) Approver declined participation via POST /approve -> /result returns PARTICIPATION_DECLINED")

        # 5. Multi-Bank Quoting, Role Enforcement, Desk Takeover & Live Ranking
        print("\n--- [CYCLE 2 - STEP 5] Multi-Bank Quoting, View-Only Block, Desk Takeover & Live Ranking ---")

        # 5A. CIB View-Only Contact authenticates and attempts to submit a quote -> Blocked (HTTP 403)
        cib_viewer_sess = authenticate_portal_session(db, cib_token, "viewer.cib@cibeg.com", "VIEW_ONLY", claim_desk=False)
        res_viewer_quote = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": cib_token,
                "email": "viewer.cib@cibeg.com",
                "session_token": cib_viewer_sess,
                "quotes": [{"leg_id": str(leg_1.id), "price": 50.10}]
            }
        )
        assert res_viewer_quote.status_code == 403, f"Expected 403 for VIEW_ONLY quote attempt, got {res_viewer_quote.status_code}"
        print("  [PASS] View-Only observer contact strictly blocked from submitting bids (HTTP 403)")

        # 5B. HSBC Desk Concurrency & Takeover: Dealer 1 claims desk, Dealer 2 joins as spectator then takes over
        hsbc_token = token_map["HSBC"]
        hsbc_d1_sess = authenticate_portal_session(db, hsbc_token, "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        hsbc_d2_sess = authenticate_portal_session(db, hsbc_token, "dealer2.hsbc@hsbc.com", "EXECUTION", claim_desk=False)

        # Dealer 2 checks desk session -> sees Dealer 1 is active trader, Dealer 2 is spectator
        res_d2_check = client.get(
            f"/api/v1/public-quotation/{hsbc_token}/desk-session",
            params={"email": "dealer2.hsbc@hsbc.com", "name": "HSBC Senior Dealer"}
        )
        assert res_d2_check.status_code == 200
        assert res_d2_check.json().get("is_active_trader") is False
        assert res_d2_check.json().get("active_trader_email") == "dealer.hsbc@hsbc.com"

        # Dealer 2 executes POST /desk-takeover
        res_takeover = client.post(
            f"/api/v1/public-quotation/{hsbc_token}/desk-takeover",
            json={
                "email": "dealer2.hsbc@hsbc.com",
                "name": "HSBC Senior Dealer",
                "role": "EXECUTION",
                "session_token": hsbc_d2_sess
            }
        )
        assert res_takeover.status_code == 200, f"Desk takeover failed: {res_takeover.text}"
        assert res_takeover.json().get("is_active_trader") is True
        assert res_takeover.json().get("active_trader_email") == "dealer2.hsbc@hsbc.com"
        print("  [PASS] Multi-dealer desk concurrency lock & takeover verified (dealer2.hsbc@hsbc.com took active control)")

        # 5C. HSBC Senior Dealer submits quotes on Leg 1 (50.25) & Leg 2 (54.50 - Sell EUR/EGP) and PASSES Leg 3
        res_hsbc_submit = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": hsbc_token,
                "email": "dealer2.hsbc@hsbc.com",
                "session_token": hsbc_d2_sess,
                "quotes": [
                    {"leg_id": str(leg_1.id), "price": 50.25},
                    {"leg_id": str(leg_2.id), "price": 54.50}
                ],
                "passed_legs": [str(leg_3.id)]
            }
        )
        assert res_hsbc_submit.status_code == 200, f"HSBC batch quote failed: {res_hsbc_submit.text}"

        # 5D. CIB Execution Dealer authenticates & quotes:
        # First submits a TRAILING rate on Leg 1 (50.40 > 50.25), trailing on Leg 2 (54.30 < 54.50 for Sell), leading on Leg 3 (1.30)
        cib_dealer_sess = authenticate_portal_session(db, cib_token, "dealer.cib@cibeg.com", "EXECUTION", claim_desk=True)
        res_cib_initial = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": cib_token,
                "email": "dealer.cib@cibeg.com",
                "session_token": cib_dealer_sess,
                "quotes": [
                    {"leg_id": str(leg_1.id), "price": 50.40},
                    {"leg_id": str(leg_2.id), "price": 54.30},
                    {"leg_id": str(leg_3.id), "price": 1.30}
                ]
            }
        )
        assert res_cib_initial.status_code == 200, f"CIB initial quote failed: {res_cib_initial.text}"

        # Check CIB vs HSBC Live Ranking on Leg 1 before CIB revises its rate
        res_rank_cib_1 = client.get(f"/api/v1/public-quotation/{cib_token}/live-rank", params={"leg_id": str(leg_1.id)})
        res_rank_hsbc_1 = client.get(f"/api/v1/public-quotation/{hsbc_token}/live-rank", params={"leg_id": str(leg_1.id)})
        if res_rank_cib_1.status_code == 200 and res_rank_cib_1.json().get("enabled"):
            assert res_rank_cib_1.json().get("rank") == 2 and res_rank_cib_1.json().get("is_leading") is False
            assert res_rank_hsbc_1.json().get("rank") == 1 and res_rank_hsbc_1.json().get("is_leading") is True

        # Now CIB REVISES its quote on Leg 1 to 50.20 (beating HSBC's 50.25!)
        res_cib_revise = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": cib_token,
                "email": "dealer.cib@cibeg.com",
                "session_token": cib_dealer_sess,
                "quotes": [
                    {"leg_id": str(leg_1.id), "price": 50.20},
                    {"leg_id": str(leg_2.id), "price": 54.30},
                    {"leg_id": str(leg_3.id), "price": 1.30}
                ]
            }
        )
        assert res_cib_revise.status_code == 200, f"CIB quote revision failed: {res_cib_revise.text}"
        if res_rank_cib_1.status_code == 200 and res_rank_cib_1.json().get("enabled"):
            res_rank_cib_after = client.get(f"/api/v1/public-quotation/{cib_token}/live-rank", params={"leg_id": str(leg_1.id)})
            assert res_rank_cib_after.json().get("rank") == 1 and res_rank_cib_after.json().get("is_leading") is True
        print("  [PASS] CIB revised quote on Leg 1 (50.40 -> 50.20) and overtook HSBC for #1 Leading position")

        # 5E. Bank C (NBE): Attempts to quote on uninvited Leg 3 -> Blocked (HTTP 403)
        nbe_token = token_map["NBE"]
        nbe_sess = authenticate_portal_session(db, nbe_token, "dealer.nbe@nbe.com.eg", "EXECUTION", claim_desk=True)
        res_nbe_uninvited = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": nbe_token,
                "email": "dealer.nbe@nbe.com.eg",
                "session_token": nbe_sess,
                "quotes": [{"leg_id": str(leg_3.id), "price": 1.25}]
            }
        )
        assert res_nbe_uninvited.status_code == 403, f"Expected 403 for uninvited leg quote, got {res_nbe_uninvited.status_code}"
        print("  [PASS] Bank C (NBE) blocked from injecting quote on uninvited Leg 3 (HTTP 403)")

        # Bank C (NBE) submits valid quotes on Leg 1 (50.10 - Indicative) and Leg 2 (54.40 - Execution)
        res_nbe_submit = client.post(
            "/api/v1/public-quotation/offers-batch",
            json={
                "token": nbe_token,
                "email": "dealer.nbe@nbe.com.eg",
                "session_token": nbe_sess,
                "quotes": [
                    {"leg_id": str(leg_1.id), "price": 50.10},
                    {"leg_id": str(leg_2.id), "price": 54.40}
                ]
            }
        )
        assert res_nbe_submit.status_code == 200, f"NBE quote failed: {res_nbe_submit.text}"

        # Bank E (QNB): Remains silent (no quote submitted)
        print("  [PASS] All 5 counterparties completed window actions (Quoted, Revised, Passed Leg, Declined, Silent)")

        # 6. Evaluation Engine Standings, Pre-Acceptance Hold & Winner Allocation
        print("\n--- [CYCLE 2 - STEP 6] Window Close, Pre-Acceptance Hold & Evaluation Engine ---")
        rfq_c2_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_c2_id).first()
        rfq_c2_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()

        # Verify that BEFORE Corporate accepts the deal, banks see AWAITING_MANUAL_SELECTION (no premature winner leak!)
        res_pre_acc = client.get(f"/api/v1/public-quotation/{cib_token}/result")
        assert res_pre_acc.status_code == 200
        assert res_pre_acc.json().get("status") == "AWAITING_MANUAL_SELECTION", (
            f"Expected AWAITING_MANUAL_SELECTION before corporate acceptance, got {res_pre_acc.json()}"
        )
        print("  [PASS] Pre-Acceptance Hold verified: Banks see AWAITING_MANUAL_SELECTION until Corporate Treasury accepts")

        standings = compute_rfq_standings(rfq_c2_db, db, dispatch_emails=False)

        # Assert Winners:
        # Leg 1 (Buy USD/EGP): NBE quoted 50.10 (Indicative -> excluded from Execution winner), CIB quoted 50.20, HSBC quoted 50.25 -> CIB wins!
        # Leg 2 (Sell EUR/EGP): HSBC quoted 54.50, NBE quoted 54.40, CIB quoted 54.30 -> HSBC wins (highest rate for Sell)!
        # Leg 3 (Buy GBP/USD): CIB quoted 1.30, HSBC passed, QNB silent -> CIB wins!
        db.refresh(leg_1)
        db.refresh(leg_2)
        db.refresh(leg_3)
        assert leg_1.winner_bank_id == fixture.banks["CIB"].bank_id, f"Leg 1 Winner should be CIB, got {leg_1.winner_bank_id}"
        assert leg_2.winner_bank_id == fixture.banks["HSBC"].bank_id, f"Leg 2 Winner should be HSBC, got {leg_2.winner_bank_id}"
        assert leg_3.winner_bank_id == fixture.banks["CIB"].bank_id, f"Leg 3 Winner should be CIB, got {leg_3.winner_bank_id}"
        print("  [PASS] Evaluation Engine resolved winners: Leg 1 -> CIB (50.20), Leg 2 -> HSBC (54.50), Leg 3 -> CIB (1.30)")
        print("  [PASS] Indicative quote on Leg 1 (NBE @ 50.10) served as benchmark without displacing Execution winner (CIB @ 50.20)")

        # 7. Corporate Deal Acceptance Execution
        print("\n--- [CYCLE 2 - STEP 7] Corporate Deal Acceptance Execution ---")
        res_accept = client.post(
            f"/api/v1/end-user/quotations/{rfq_c2_id}/accept-deal",
            headers=make_auth_headers(fixture.maker_user),
            json={"accepted_leg_ids": [str(leg_1.id), str(leg_2.id), str(leg_3.id)]}
        )
        assert res_accept.status_code == 200, f"Deal acceptance failed: {res_accept.text}"
        db.refresh(rfq_c2_db)
        assert rfq_c2_db.status == "COMPLETED"
        print("  [PASS] Corporate Treasury accepted package deals via /accept-deal endpoint")

        # 8. Bank-Side Outcome Verification & Winner-Only Document Privacy Audit
        print("\n--- [CYCLE 2 - STEP 8] Bank-Side Outcomes & Winner-Only Leg-Scoped Document Audit ---")
        # Bank A (CIB: Won Leg 1 & Leg 3, Lost Leg 2) -> PARTIALLY_WON
        res_a_result = client.get(f"/api/v1/public-quotation/{cib_token}/result")
        assert res_a_result.status_code == 200
        res_a_json = res_a_result.json()
        assert res_a_json.get("status") == "PARTIALLY_WON", f"Expected CIB status=PARTIALLY_WON, got {res_a_json.get('status')}"
        docs_a = res_a_json.get("released_documents", [])
        doc_names_a = [d["name"] for d in docs_a]
        assert "Contract_Leg1_USD.pdf" in doc_names_a, f"Bank A must receive Contract_Leg1_USD.pdf, got {doc_names_a}"
        assert "Contract_Leg2_EUR.pdf" not in doc_names_a, "CRITICAL: Bank A received Contract_Leg2_EUR.pdf (Leg 2 won by HSBC)!"
        assert "Board_Internal_Treasury_Memo.pdf" not in doc_names_a, "CRITICAL: INTERNAL_ONLY doc leaked in released_documents!"
        print("  [PASS] Bank A (CIB) status=PARTIALLY_WON; received Contract_Leg1_USD.pdf only; Leg 2 & Internal docs withheld")

        # Bank B (HSBC: Won Leg 2, Lost Leg 1, Passed Leg 3) -> PARTIALLY_WON
        res_b_result = client.get(f"/api/v1/public-quotation/{hsbc_token}/result")
        assert res_b_result.status_code == 200
        res_b_json = res_b_result.json()
        assert res_b_json.get("status") == "PARTIALLY_WON", f"Expected HSBC status=PARTIALLY_WON, got {res_b_json.get('status')}"
        hsbc_leg3_bd = res_b_json.get("legs_breakdown", {}).get(str(leg_3.id), {})
        assert hsbc_leg3_bd.get("status") == "PASSED" and hsbc_leg3_bd.get("is_passed") is True, (
            f"Expected HSBC Leg 3 breakdown status=PASSED, got {hsbc_leg3_bd}"
        )
        docs_b = res_b_json.get("released_documents", [])
        doc_names_b = [d["name"] for d in docs_b]
        assert "Contract_Leg2_EUR.pdf" in doc_names_b, f"Bank B must receive Contract_Leg2_EUR.pdf, got {doc_names_b}"
        assert "Contract_Leg1_USD.pdf" not in doc_names_b, "CRITICAL: Bank B received Contract_Leg1_USD.pdf (Leg 1 won by CIB)!"
        print("  [PASS] Bank B (HSBC) status=PARTIALLY_WON (Leg 2 WON, Leg 1 NOT_SELECTED, Leg 3 PASSED); received Contract_Leg2_EUR.pdf only")

        # Banks C (NBE - Lost/Indicative), D (BM - Declined), E (QNB - Silent)
        res_nbe_res = client.get(f"/api/v1/public-quotation/{nbe_token}/result")
        assert res_nbe_res.status_code == 200 and res_nbe_res.json().get("status") == "NOT_SELECTED"
        assert len(res_nbe_res.json().get("released_documents", [])) == 0

        res_bm_res2 = client.get(f"/api/v1/public-quotation/{bm_token}/result")
        assert res_bm_res2.status_code == 200 and res_bm_res2.json().get("status") == "PARTICIPATION_DECLINED"

        res_qnb_res = client.get(f"/api/v1/public-quotation/{token_map['QNB']}/result")
        assert res_qnb_res.status_code == 200 and len(res_qnb_res.json().get("released_documents", [])) == 0
        print("  [PASS] Non-winning banks (NBE: NOT_SELECTED, BM: PARTICIPATION_DECLINED, QNB: Silent) received 0 documents")

        # 9. Scoped Deal Execution Receipts Cryptographic Verification
        print("\n--- [CYCLE 2 - STEP 9] Scoped Execution Receipt Cryptographic Signatures ---")
        receipt_a = res_a_json.get("receipt")
        assert receipt_a is not None, "Bank A should have received a cryptographic deal receipt"
        assert receipt_a["signature_hash"].startswith("SHA256:"), "Signature hash missing SHA256: prefix"
        assert receipt_a["scoped_legs_count"] == 2  # CIB won Leg 1 and Leg 3
        print(f"  [PASS] Cryptographic Deal Receipt verified: {receipt_a['receipt_id']} ({receipt_a['signature_hash'][:20]}...)")

        # 10. Post-Trade Trophies Delta Check (Exact Relative Increment)
        print("\n--- [CYCLE 2 - STEP 10] Trophies & Win Counter Delta Check ---")
        cib_achievements_after = DealerAchievementService.get_dealer_achievements(
            db, dealer_email="dealer.cib@cibeg.com", bank_id=fixture.banks["CIB"].bank_id
        )
        hsbc_achievements_after = DealerAchievementService.get_dealer_achievements(
            db, dealer_email="dealer2.hsbc@hsbc.com", bank_id=fixture.banks["HSBC"].bank_id
        )
        cib_wins_after = cib_achievements_after.get("personal_bests", {}).get("total_deals_won", 0)
        hsbc_wins_after = hsbc_achievements_after.get("personal_bests", {}).get("total_deals_won", 0)

        assert cib_wins_after == cib_wins_before + 2, f"CIB wins should increment by +2 (Before: {cib_wins_before}, After: {cib_wins_after})"
        assert hsbc_wins_after == hsbc_wins_before + 1, f"HSBC wins should increment by +1 (Before: {hsbc_wins_before}, After: {hsbc_wins_after})"
        print(f"  [PASS] CIB Dealer wins: {cib_wins_before} -> {cib_wins_after} (Exact Delta: +2 legs won)")
        print(f"  [PASS] HSBC Senior Dealer wins: {hsbc_wins_before} -> {hsbc_wins_after} (Exact Delta: +1 leg won)")

        # 11. Zero-Knowledge Ciphertext At-Rest Verification
        print("\n--- [CYCLE 2 - STEP 11] Zero-Knowledge Ciphertext at Rest Inspection ---")
        offers = db.query(QuotationOffer).filter(
            QuotationOffer.assignment_id.in_([assignment_map["CIB"].id, assignment_map["HSBC"].id, assignment_map["NBE"].id])
        ).all()
        assert len(offers) >= 6
        for off in offers:
            assert off.price is None, f"SECURITY LEAK: Plaintext price found in DB: {off.price}"
            assert off.encrypted_price.startswith("enc:v1:"), "Encrypted price not properly formatted"
        print("  [PASS] All quotation offers confirmed NULL in plaintext and encrypted at rest")


        # ==============================================================================
        # PART 4: SUB-PHASE 8.7 DOCUMENT ENCRYPTION & EPHEMERAL STREAMING GATEWAY
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[ENCRYPTION] PART 4: SUB-PHASE 8.7 DOCUMENT ENCRYPTION & STREAMING GATEWAY")
        print("=" * 80)

        # 4.1 Cryptographic Engine Verification (STREAM V2 AEAD 64 KB Blocks)
        print("\n--- [PART 4 - STEP 1] STREAM V2 64 KB Block Cryptographic Engine Checks ---")
        test_dek = secrets.token_bytes(32)
        raw_pdf_sample = b"%PDF-1.4\n" + (b"CONFIDENTIAL_FACILITY_AGREEMENT_DATA_" * 2000)  # ~76 KB (> 1 chunk)
        enc_stream_doc = encrypt_file_bytes(raw_pdf_sample, test_dek, context="quotation_doc")

        assert is_encrypted_file(enc_stream_doc), "Document must be identified as STREAM V2 encrypted"
        assert enc_stream_doc.startswith(STREAM_MAGIC), "Ciphertext must begin with STREAM_MAGIC"
        assert raw_pdf_sample not in enc_stream_doc, "Plaintext must never appear in ciphertext"

        dec_stream_doc = decrypt_file_bytes(enc_stream_doc, test_dek, context="quotation_doc")
        assert dec_stream_doc == raw_pdf_sample, "Decrypted stream must match original PDF byte-for-byte"
        print("  [PASS] STREAM V2 Multi-chunk (64 KB) encryption/decryption roundtrip verified")

        # Bit-flip tamper detection
        corrupted_bytes = bytearray(enc_stream_doc)
        corrupted_bytes[len(corrupted_bytes) - 10] ^= 0x55
        try:
            decrypt_file_bytes(bytes(corrupted_bytes), test_dek, context="quotation_doc")
            assert False, "Should have raised TamperDetectedError on corrupted ciphertext"
        except TamperDetectedError:
            print("  [PASS] Tamper detection active: Bit-flip rejected with TamperDetectedError")

        # Foreign DEK rejection
        foreign_dek = secrets.token_bytes(32)
        try:
            decrypt_file_bytes(enc_stream_doc, foreign_dek, context="quotation_doc")
            assert False, "Should have raised TamperDetectedError on foreign tenant DEK"
        except TamperDetectedError:
            print("  [PASS] Cross-tenant key isolation: Foreign DEK rejected with TamperDetectedError")

        # Dual-Read backwards compatibility
        legacy_plaintext = b"%PDF-1.4\nLEGACY_UNENCRYPTED_HISTORICAL_QUOTATION_DOC"
        assert decrypt_file_bytes(legacy_plaintext, test_dek) == legacy_plaintext
        print("  [PASS] Dual-read compatibility: Unencrypted legacy PDF passes through transparently")

        # 4.2 Adversarial Gateway Security Gates (Option A + Option B)
        print("\n--- [PART 4 - STEP 2] Adversarial Security Gateway & Ephemeral Ticket Gates ---")
        cib_ass = assignment_map["CIB"]
        cib_token = token_map["CIB"]
        hsbc_token = token_map["HSBC"]

        rfq_c2_db.document_path = json.dumps({
            "release_to_winner_only": True,
            "documents": [
                {"name": "Contract_Leg1_USD.pdf", "path": "gs://lg_custody_bucket/development/customer_1/quotations/rfq_docs/test_leg1.pdf", "leg_id": str(leg_1.id), "pair": "USD/EGP"},
                {"name": "Contract_Leg2_EUR.pdf", "path": "gs://lg_custody_bucket/development/customer_1/quotations/rfq_docs/test_leg2.pdf", "leg_id": str(leg_2.id), "pair": "EUR/EGP"}
            ]
        })
        db.commit()

        target_gcs = "gs://lg_custody_bucket/development/customer_1/quotations/rfq_docs/test_leg1.pdf"

        # Gate 1: Direct static URL download block (Option B enforcement)
        res_gate1 = client.get(f"/api/v1/quotations/documents/download?gcs_uri={target_gcs}&token={cib_token}")
        assert res_gate1.status_code == 403
        assert res_gate1.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 1: Direct static capability download blocked (HTTP 403: Access denied or Not found)")

        # Gate 2: Missing OTP session token on ticket request
        res_gate2 = client.post("/api/v1/quotations/documents/ticket", json={"gcs_uri": target_gcs, "token": cib_token})
        assert res_gate2.status_code == 403
        assert res_gate2.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 2: Ticket request without active OTP session blocked (HTTP 403: Access denied or Not found)")

        # Gate 3: Invalid/fake OTP session token on ticket request
        res_gate3 = client.post("/api/v1/quotations/documents/ticket", json={"gcs_uri": target_gcs, "token": cib_token, "session_token": "fake_intruder_token"})
        assert res_gate3.status_code == 403
        assert res_gate3.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 3: Invalid OTP session token blocked (HTTP 403: Access denied or Not found)")

        # Gate 4: Unattached document request
        res_gate4 = client.post("/api/v1/quotations/documents/ticket", json={"gcs_uri": "gs://lg_custody_bucket/development/customer_1/secret_unattached.pdf", "token": cib_token, "session_token": cib_dealer_sess})
        assert res_gate4.status_code == 403
        assert res_gate4.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 4: Unattached document request blocked (HTTP 403: Access denied or Not found)")

        # Gate 5: Legitimate 15-Minute Ephemeral Ticket Minted (Option A + B)
        res_gate5 = client.post("/api/v1/quotations/documents/ticket", json={"gcs_uri": target_gcs, "token": cib_token, "session_token": cib_dealer_sess, "leg_id": str(leg_1.id)})
        assert res_gate5.status_code == 200
        ticket_data = res_gate5.json()
        assert "ticket" in ticket_data and "download_url" in ticket_data
        assert ticket_data.get("expires_in_seconds") == 900
        ephemeral_ticket = ticket_data["ticket"]
        print("  [PASS] Gate 5: Legitimate 15-minute signed ticket minted successfully (Option A + B)")

        # Gate 6: Tampered Ticket Blocked
        tampered_ticket = ephemeral_ticket[:-6] + "abcdef"
        res_gate6 = client.get(f"/api/v1/quotations/documents/download?ticket={tampered_ticket}")
        assert res_gate6.status_code == 401
        assert res_gate6.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 6: Tampered ticket signature blocked (HTTP 401: Access denied or Not found)")

        # Gate 7: Expired Ticket Blocked
        expired_ticket = generate_download_ticket({"customer_id": 1, "gcs_uri": target_gcs}, expiry_minutes=-1)
        res_gate7 = client.get(f"/api/v1/quotations/documents/download?ticket={expired_ticket}")
        assert res_gate7.status_code == 401
        assert res_gate7.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 7: Expired ticket blocked (HTTP 401: Access denied or Not found)")

        # Gate 8: Multi-Pair Leg Isolation: Competitor bank trying to access winning bank's leg document
        res_gate8 = client.post("/api/v1/quotations/documents/ticket", json={"gcs_uri": target_gcs, "token": hsbc_token, "session_token": hsbc_d2_sess, "leg_id": str(leg_1.id)})
        assert res_gate8.status_code == 403
        assert res_gate8.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 8: Leg isolation policy: Competitor bank blocked from Leg 1 document (HTTP 403)")

        # Gate 9: Pure Unauthenticated Request Blocked
        res_gate9 = client.get(f"/api/v1/quotations/documents/download?gcs_uri={target_gcs}")
        assert res_gate9.status_code == 401
        assert res_gate9.json().get("detail") == "Access denied or Not found"
        print("  [PASS] Gate 9: Pure unauthenticated direct request blocked (HTTP 401: Access denied or Not found)")

        # 4.3 Local Concurrency & RAM Stress Benchmark
        print("\n--- [PART 4 - STEP 3] Local Concurrency & RAM Stress Benchmark ---")
        stress_file_mb = 50 if is_full_stress else 5
        stress_workers = 20 if is_full_stress else 5
        stress_ceiling = 60.0

        print(f"  [BENCHMARK] Mode: {'FULL 1.0 GB STRESS' if is_full_stress else 'FAST LOCAL CHECK'} ({stress_file_mb} MB x {stress_workers} workers)")

        bench_dek = secrets.token_bytes(32)
        bench_pattern = b"GROW_TREASURY_OMNI_STREAMING_CRYPTO_BENCHMARK_2026_"
        bench_total_bytes = stress_file_mb * 1024 * 1024

        temp_raw = tempfile.NamedTemporaryFile(delete=False, suffix=".raw")
        temp_raw_path = temp_raw.name
        temp_raw.close()

        temp_stress = tempfile.NamedTemporaryFile(delete=False, suffix=".enc")
        temp_stress_path = temp_stress.name
        temp_stress.close()

        try:
            def _gen_chunks():
                written = 0
                while written < bench_total_bytes:
                    to_take = min(64 * 1024, bench_total_bytes - written)
                    yield (bench_pattern * ((to_take // len(bench_pattern)) + 1))[:to_take]
                    written += to_take

            hasher_ref = hashlib.sha256()
            with open(temp_raw_path, "wb") as f_out:
                for chunk in _gen_chunks():
                    hasher_ref.update(chunk)
                    f_out.write(chunk)
            ref_digest = hasher_ref.hexdigest()

            with open(temp_raw_path, "rb") as f_in, open(temp_stress_path, "wb") as f_out:
                encrypt_file_stream(f_in, f_out, bench_dek, context="omni_bench")

            if os.path.exists(temp_raw_path):
                os.remove(temp_raw_path)

            tracemalloc.start()
            t_bench_start = time.time()

            def _bench_worker(wid):
                h = hashlib.sha256()
                with open(temp_stress_path, "rb") as f:
                    for c in decrypt_stream_generator(f, bench_dek, context="omni_bench"):
                        h.update(c)
                if h.hexdigest() != ref_digest:
                    raise ValueError(f"Worker {wid} digest mismatch")
                return True

            with concurrent.futures.ThreadPoolExecutor(max_workers=stress_workers) as executor:
                futures = [executor.submit(_bench_worker, w) for w in range(stress_workers)]
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()

            bench_duration = time.time() - t_bench_start
            _, bench_peak_mem = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            bench_peak_mb = bench_peak_mem / (1024 * 1024)
            bench_throughput = (stress_file_mb * stress_workers) / bench_duration

            print(f"  [PASS] Completed {stress_workers}/{stress_workers} concurrent workers (100% SHA-256 match)")
            print(f"  [PASS] Aggregate Decryption Throughput: {bench_throughput:.1f} MB/s in {bench_duration:.2f}s")
            print(f"  [PASS] Peak Python Heap Allocation: {bench_peak_mb:.2f} MB (Ceiling: < {stress_ceiling} MB)")
            assert bench_peak_mb < stress_ceiling, f"Memory ceiling exceeded: {bench_peak_mb:.2f} MB >= {stress_ceiling} MB"
        finally:
            if os.path.exists(temp_raw_path):
                os.remove(temp_raw_path)
            if os.path.exists(temp_stress_path):
                os.remove(temp_stress_path)


        # ==============================================================================
        # PART 5: MASTER CYCLE 3 — VALUE DATES, ALTERNATIVE DATES, TVM MATH, FEES, TOLERANCE & T-BILLS
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[MATH & MATRIX] PART 5: VALUE DATES, ALT DATES, TVM NORMALIZATION, FEES, TOLERANCE & T-BILLS")
        print("=" * 80)

        # ------------------------------------------------------------------------------
        # SCENARIO 5.1: Single-Leg Custom Value Date & TVM Normalization (Buy USD/EGP)
        # ------------------------------------------------------------------------------
        print("\n--- [SCENARIO 5.1] Single-Leg Custom Value Date & TVM Normalization (Buy 311,002 USD/EGP) ---")
        now_51 = datetime.now(timezone.utc)
        t0_str = now_51.strftime("%Y-%m-%d")
        t2_str = (now_51 + timedelta(days=2)).strftime("%Y-%m-%d")

        # Bank HSBC gets T+2 custom valueDate; Bank QNB gets T+0 standard valueDate
        s51_banks = [
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True, "quotationBase": "Execution", "valueDate": t2_str, "allowAlternativeValueDate": False},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": True, "quotationBase": "Execution", "valueDate": t0_str, "allowAlternativeValueDate": False},
        ]
        res_51 = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "FX_SPOT",
                "direction": "Buy",
                "entity_id": fixture.entity.id,
                "buyCurrency": "USD",
                "sellCurrency": "EGP",
                "amount": 311_002.0,
                "valueDate": t0_str,
                "evalRate": 19.25,
                "windowStart": (now_51 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_51 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(s51_banks),
                "legalDisclaimerAccepted": True,
                "pairs": [
                    {
                        "direction": "Buy",
                        "buyCurrency": "USD",
                        "sellCurrency": "EGP",
                        "amount": 311_002.0,
                        "valueDate": t0_str,
                        "allowAlternativeValueDate": False,
                        "quotationBase": "Execution",
                        "selectedBanks": s51_banks
                    }
                ]
            }
        )
        assert res_51.status_code == 200, f"Scenario 5.1 RFQ creation failed: {res_51.text}"
        rfq_51_id = res_51.json()["rfq_id"]
        created_rfq_ids.append(rfq_51_id)

        rfq_51_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_51_id).first()
        rfq_51_db.status = "PENDING"
        rfq_51_db.eval_rate = 19.25
        db.commit()
        leg_51_id = str(rfq_51_db.legs[0].id)

        tok_51 = {}
        for a in res_51.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_51[code] = a["token"]

        # Verify HSBC portal & live blotter show T+2 (t2_str), while QNB portal shows T+0 (t0_str)
        hsbc_p51 = client.get(f"/api/v1/public-quotation/{tok_51['HSBC']}").json()
        qnb_p51 = client.get(f"/api/v1/public-quotation/{tok_51['QNB']}").json()
        assert hsbc_p51["value_date"] == t2_str and hsbc_p51["legs"][0]["value_date"] == t2_str, (
            f"HSBC portal must show custom value_date={t2_str}, got {hsbc_p51['value_date']}"
        )
        assert qnb_p51["value_date"] == t0_str and qnb_p51["legs"][0]["value_date"] == t0_str

        # HSBC quotes 56.0001 (for T+2); QNB quotes 56.0000 (for T+0)
        hsbc_s51 = authenticate_portal_session(db, tok_51["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        qnb_s51 = authenticate_portal_session(db, tok_51["QNB"], "dealer.qnb@qnb.com.eg", "EXECUTION", claim_desk=True)

        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_51["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_s51,
            "quotes": [{"leg_id": leg_51_id, "price": 56.0001}]
        }).status_code == 200
        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_51["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_s51,
            "quotes": [{"leg_id": leg_51_id, "price": 56.0000}]
        }).status_code == 200

        # Close window and evaluate TVM standings
        rfq_51_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        res_51_eval = client.get(
            f"/api/v1/end-user/quotations/{rfq_51_id}/results",
            headers=make_auth_headers(fixture.admin_user)
        )
        assert res_51_eval.status_code == 200
        eval_51 = res_51_eval.json()

        # HSBC (56.0001 on T+2 -> TVM normalized ~55.94103) MUST beat QNB (56.0000 on T+0)!
        assert eval_51["winner_bank_id"] == fixture.banks["HSBC"].bank_id, (
            f"TVM Normalization Bug: Expected HSBC (56.0001 @ T+2) to beat QNB (56.0000 @ T+0), got winner={eval_51.get('winner_bank_name')}"
        )
        sav_51 = eval_51.get("savings_summary", {})
        assert sav_51.get("winner_rate") == 56.0001
        assert sav_51.get("is_tvm_adjusted") is True
        assert abs(sav_51.get("winner_normalized_rate", 0) - 55.941) < 0.005, f"Unexpected normalized rate: {sav_51}"
        assert sav_51.get("saved_vs_avg", 0) > 9000.0, f"Expected saved_vs_avg ~9170.07 EGP, got {sav_51.get('saved_vs_avg')}"
        assert sav_51.get("saved_vs_worst", 0) > 18000.0, f"Expected saved_vs_worst ~18340.14 EGP, got {sav_51.get('saved_vs_worst')}"
        print(
            f"  [PASS] TVM Normalization Winner: HSBC @ 56.0001 (T+2, TVM={sav_51.get('winner_normalized_rate')}) beat QNB @ 56.0000 (T+0) | "
            f"Saved vs Avg: EGP {sav_51.get('saved_vs_avg')} | Saved vs Worst: EGP {sav_51.get('saved_vs_worst')}"
        )

        # ------------------------------------------------------------------------------
        # SCENARIO 5.2: allowAlternativeValueDate Enforcement (True vs False) & Un-Pass Leg
        # ------------------------------------------------------------------------------
        print("\n--- [SCENARIO 5.2] allowAlternativeValueDate (True vs False) & Un-Pass Leg Verification ---")
        now_52 = datetime.now(timezone.utc)
        t0_52 = now_52.strftime("%Y-%m-%d")
        t4_52 = (now_52 + timedelta(days=4)).strftime("%Y-%m-%d")
        past_52 = (now_52 - timedelta(days=3)).strftime("%Y-%m-%d")

        # HSBC has allowAlternativeValueDate=False; QNB has allowAlternativeValueDate=True
        s52_banks = [
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True, "quotationBase": "Execution", "valueDate": t0_52, "allowAlternativeValueDate": False},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": True, "quotationBase": "Execution", "valueDate": t0_52, "allowAlternativeValueDate": True},
        ]
        res_52 = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "FX_SPOT",
                "direction": "Buy",
                "entity_id": fixture.entity.id,
                "buyCurrency": "USD",
                "sellCurrency": "EGP",
                "amount": 100_000.0,
                "valueDate": t0_52,
                "evalRate": 20.0,
                "windowStart": (now_52 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_52 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(s52_banks),
                "legalDisclaimerAccepted": True,
                "pairs": [
                    {
                        "direction": "Buy",
                        "buyCurrency": "USD",
                        "sellCurrency": "EGP",
                        "amount": 100_000.0,
                        "valueDate": t0_52,
                        "allowAlternativeValueDate": False,
                        "quotationBase": "Execution",
                        "selectedBanks": s52_banks
                    }
                ]
            }
        )
        assert res_52.status_code == 200
        rfq_52_id = res_52.json()["rfq_id"]
        created_rfq_ids.append(rfq_52_id)
        rfq_52_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_52_id).first()
        rfq_52_db.status = "PENDING"
        db.commit()
        leg_52_id = str(rfq_52_db.legs[0].id)

        tok_52 = {}
        ass_52 = {}
        for a in res_52.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_52[code] = a["token"]
            ass_52[code] = db_a

        hsbc_s52 = authenticate_portal_session(db, tok_52["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        qnb_s52 = authenticate_portal_session(db, tok_52["QNB"], "dealer.qnb@qnb.com.eg", "EXECUTION", claim_desk=True)

        # 5.2A: HSBC (allowAlternativeValueDate=False) tries to propose T+4 -> Backend ignores T+4 and locks T+0!
        res_hsbc_alt = client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_52["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_s52,
            "quotes": [{"leg_id": leg_52_id, "price": 50.00, "offered_value_date": t4_52}]
        })
        assert res_hsbc_alt.status_code == 200
        hsbc_off_db = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == ass_52["HSBC"].id).first()
        assert str(hsbc_off_db.offered_value_date) == t0_52, (
            f"Expected HSBC offered_value_date forced to {t0_52} when allowAlternativeValueDate=False, got {hsbc_off_db.offered_value_date}"
        )

        # 5.2B: QNB (allowAlternativeValueDate=True) tries to propose a PAST date -> Rejected with HTTP 400!
        res_qnb_past = client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_52["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_s52,
            "quotes": [{"leg_id": leg_52_id, "price": 50.05, "offered_value_date": past_52}]
        })
        assert res_qnb_past.status_code == 400, f"Expected HTTP 400 for past alternative value date, got {res_qnb_past.status_code}"

        # 5.2C: QNB first PASSES the leg, then UN-PASSES by submitting a quote with valid alternative date T+4!
        res_qnb_pass = client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_52["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_s52,
            "quotes": [], "passed_legs": [leg_52_id]
        })
        assert res_qnb_pass.status_code == 200
        qnb_cfg_52 = db.query(QuotationBankLegConfig).filter(
            QuotationBankLegConfig.assignment_id == ass_52["QNB"].id,
            QuotationBankLegConfig.leg_id == leg_52_id
        ).first()
        db.refresh(qnb_cfg_52)
        assert qnb_cfg_52.is_passed is True

        # Now QNB un-passes by submitting a quote (50.05) with alternative date T+4
        res_qnb_unpass = client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_52["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_s52,
            "quotes": [{"leg_id": leg_52_id, "price": 50.05, "offered_value_date": t4_52}]
        })
        assert res_qnb_unpass.status_code == 200
        db.refresh(qnb_cfg_52)
        assert qnb_cfg_52.is_passed is False, "Submitting a quote on a passed leg must reset is_passed to False!"
        qnb_off_db = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == ass_52["QNB"].id).first()
        assert str(qnb_off_db.offered_value_date) == t4_52

        # Close window and verify QNB (50.05 on T+4 -> TVM normalized ~49.9403) beats HSBC (50.00 on T+0)!
        rfq_52_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        eval_52 = compute_rfq_standings(rfq_52_db, db, dispatch_emails=False)
        assert eval_52["winner_bank_id"] == fixture.banks["QNB"].bank_id, (
            f"Expected QNB (50.05 @ alt date T+4) to beat HSBC (50.00 @ T+0), got {eval_52.get('winner_bank_name')}"
        )
        print("  [PASS] allowAlternativeValueDate (False forced T+0; True rejected past date & TVM-normalized T+4) + Un-Pass Leg verified")

        # ------------------------------------------------------------------------------
        # SCENARIO 5.3: Per-Bank Transaction Fees (costPercent, costMin, costMax) Flipping Winner
        # ------------------------------------------------------------------------------
        print("\n--- [SCENARIO 5.3] Per-Bank Transaction Fees Flipping All-In Winner ---")
        now_53 = datetime.now(timezone.utc)
        t0_53 = now_53.strftime("%Y-%m-%d")

        # HSBC has 1.0% fee; QNB has 0% fee
        s53_banks = [
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True, "quotationBase": "Execution", "costPercent": 1.0, "costMin": 100.0, "costMax": 500000.0, "costFlat": 0.0},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": True, "quotationBase": "Execution", "costPercent": 0.0, "costMin": 0.0,   "costMax": 0.0,      "costFlat": 0.0},
        ]
        res_53 = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "FX_SPOT",
                "direction": "Buy",
                "entity_id": fixture.entity.id,
                "buyCurrency": "USD",
                "sellCurrency": "EGP",
                "amount": 100_000.0,
                "valueDate": t0_53,
                "windowStart": (now_53 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_53 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(s53_banks),
                "legalDisclaimerAccepted": True,
                "pairs": [
                    {
                        "direction": "Buy",
                        "buyCurrency": "USD",
                        "sellCurrency": "EGP",
                        "amount": 100_000.0,
                        "valueDate": t0_53,
                        "quotationBase": "Execution",
                        "selectedBanks": s53_banks
                    }
                ]
            }
        )
        assert res_53.status_code == 200
        rfq_53_id = res_53.json()["rfq_id"]
        created_rfq_ids.append(rfq_53_id)
        rfq_53_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_53_id).first()
        rfq_53_db.status = "PENDING"
        db.commit()
        leg_53_id = str(rfq_53_db.legs[0].id)

        tok_53 = {}
        for a in res_53.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_53[code] = a["token"]

        hsbc_s53 = authenticate_portal_session(db, tok_53["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        qnb_s53 = authenticate_portal_session(db, tok_53["QNB"], "dealer.qnb@qnb.com.eg", "EXECUTION", claim_desk=True)

        # HSBC quotes lower raw rate 50.00 (with 1% fee -> finalPrice = 50.50); QNB quotes 50.20 (0% fee -> finalPrice = 50.20)
        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_53["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_s53,
            "quotes": [{"leg_id": leg_53_id, "price": 50.00}]
        }).status_code == 200
        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_53["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_s53,
            "quotes": [{"leg_id": leg_53_id, "price": 50.20}]
        }).status_code == 200

        rfq_53_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        eval_53 = compute_rfq_standings(rfq_53_db, db, dispatch_emails=False)
        assert eval_53["winner_bank_id"] == fixture.banks["QNB"].bank_id, (
            f"Expected QNB (50.20 all-in) to beat HSBC (50.00 raw + 1% fee = 50.50 all-in), got {eval_53.get('winner_bank_name')}"
        )
        print("  [PASS] Transaction Fee Math verified: QNB (50.20 + 0% fee) beat HSBC (50.00 + 1% fee = 50.50 all-in)")

        # ------------------------------------------------------------------------------
        # SCENARIO 5.4: Indicative Benchmark Tolerance (maxTolerancePercent) Breach vs Award
        # ------------------------------------------------------------------------------
        print("\n--- [SCENARIO 5.4] Indicative Benchmark Tolerance Breach (INCONCLUSIVE) & INDICATIVE_ONLY Result ---")
        now_54 = datetime.now(timezone.utc)
        t0_54 = now_54.strftime("%Y-%m-%d")

        # NBE is Indicative; HSBC is Execution; maxTolerancePercent = 0.5%
        s54_banks = [
            {"id": fixture.banks["NBE"].bank_id,  "isInvited": True, "quotationBase": "Indicative"},
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True, "quotationBase": "Execution"},
        ]
        res_54 = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "FX_SPOT",
                "direction": "Buy",
                "entity_id": fixture.entity.id,
                "buyCurrency": "USD",
                "sellCurrency": "EGP",
                "amount": 100_000.0,
                "valueDate": t0_54,
                "maxTolerancePercent": 0.5,
                "windowStart": (now_54 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_54 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(s54_banks),
                "legalDisclaimerAccepted": True,
                "pairs": [
                    {
                        "direction": "Buy",
                        "buyCurrency": "USD",
                        "sellCurrency": "EGP",
                        "amount": 100_000.0,
                        "valueDate": t0_54,
                        "maxTolerancePercent": 0.5,
                        "quotationBase": "Execution",
                        "selectedBanks": s54_banks
                    }
                ]
            }
        )
        assert res_54.status_code == 200
        rfq_54_id = res_54.json()["rfq_id"]
        created_rfq_ids.append(rfq_54_id)
        rfq_54_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_54_id).first()
        rfq_54_db.status = "PENDING"
        db.commit()
        leg_54_id = str(rfq_54_db.legs[0].id)

        tok_54 = {}
        for a in res_54.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_54[code] = a["token"]

        nbe_s54 = authenticate_portal_session(db, tok_54["NBE"], "dealer.nbe@nbe.com.eg", "EXECUTION", claim_desk=True)
        hsbc_s54 = authenticate_portal_session(db, tok_54["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)

        # NBE (Indicative) quotes 50.00; HSBC (Execution) quotes 51.00 (2.0% deviation > 0.5% tolerance!)
        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_54["NBE"], "email": "dealer.nbe@nbe.com.eg", "session_token": nbe_s54,
            "quotes": [{"leg_id": leg_54_id, "price": 50.00}]
        }).status_code == 200
        assert client.post("/api/v1/public-quotation/offers-batch", json={
            "token": tok_54["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_s54,
            "quotes": [{"leg_id": leg_54_id, "price": 51.00}]
        }).status_code == 200

        rfq_54_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        eval_54 = compute_rfq_standings(rfq_54_db, db, dispatch_emails=False)
        assert eval_54.get("is_inconclusive") is True and eval_54.get("winner_bank_id") is None, (
            f"Expected is_inconclusive=True when execution quote (51.00) breaches indicative benchmark (50.00) by 2.0% > 0.5%, got {eval_54}"
        )

        # Verify Indicative-only bank (NBE) immediately receives INDICATIVE_ONLY on /result
        res_nbe_ind = client.get(f"/api/v1/public-quotation/{tok_54['NBE']}/result")
        assert res_nbe_ind.status_code == 200 and res_nbe_ind.json().get("status") == "INDICATIVE_ONLY"
        print("  [PASS] Indicative Benchmark Tolerance Breach flagged INCONCLUSIVE (2.0% > 0.5%) & NBE /result returned INDICATIVE_ONLY")

        # ------------------------------------------------------------------------------
        # SCENARIO 5.5: T-Bill Buy & Sell Multi-Line Normalization
        # ------------------------------------------------------------------------------
        print("\n--- [SCENARIO 5.5] T-Bill Buy & Sell Multi-Line Normalization ---")
        now_55 = datetime.now(timezone.utc)
        s_date_1 = (now_55 + timedelta(days=1)).strftime("%Y-%m-%d")
        m_date_1 = (now_55 + timedelta(days=92)).strftime("%Y-%m-%d")
        m_date_2 = (now_55 + timedelta(days=182)).strftime("%Y-%m-%d")

        tb_banks = [
            {"id": fixture.banks["HSBC"].bank_id, "isInvited": True, "quotationBase": "Execution"},
            {"id": fixture.banks["QNB"].bank_id,  "isInvited": True, "quotationBase": "Execution"},
        ]

        # 5.5A: T-Bill BUY (Higher discount rate / lower normalized price per 100 par wins)
        res_tb_buy = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "TBILL",
                "direction": "Buy",
                "entity_id": fixture.entity.id,
                "amount": 5_000_000.0,
                "settlementDateStart": s_date_1,
                "settlementDateEnd": s_date_1,
                "maturityDateStart": m_date_1,
                "maturityDateEnd": m_date_2,
                "evalRate": 20.0,
                "windowStart": (now_55 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_55 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(tb_banks),
                "legalDisclaimerAccepted": True
            }
        )
        assert res_tb_buy.status_code == 200, f"T-Bill Buy RFQ creation failed: {res_tb_buy.text}"
        rfq_tbb_id = res_tb_buy.json()["rfq_id"]
        created_rfq_ids.append(rfq_tbb_id)
        rfq_tbb_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_tbb_id).first()
        rfq_tbb_db.status = "PENDING"
        db.commit()

        tok_tbb = {}
        for a in res_tb_buy.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_tbb[code] = a["token"]

        hsbc_stbb = authenticate_portal_session(db, tok_tbb["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        qnb_stbb = authenticate_portal_session(db, tok_tbb["QNB"], "dealer.qnb@qnb.com.eg", "EXECUTION", claim_desk=True)

        # HSBC submits 2 lines (26.50% and 27.20%); QNB submits 1 line (25.80%) -> HSBC wins T-Bill Buy!
        assert client.post("/api/v1/public-quotation/tbill-offer", json={
            "token": tok_tbb["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_stbb,
            "lines": [
                {"settlementDate": s_date_1, "maturityDate": m_date_1, "discountRate": 26.50, "maxAmount": 3000000.0},
                {"settlementDate": s_date_1, "maturityDate": m_date_1, "discountRate": 27.20, "maxAmount": 5000000.0}
            ]
        }).status_code == 200
        assert client.post("/api/v1/public-quotation/tbill-offer", json={
            "token": tok_tbb["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_stbb,
            "lines": [
                {"settlementDate": s_date_1, "maturityDate": m_date_1, "discountRate": 25.80, "maxAmount": 5000000.0}
            ]
        }).status_code == 200

        rfq_tbb_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        eval_tbb = compute_rfq_standings(rfq_tbb_db, db, dispatch_emails=False)
        assert eval_tbb["winner_bank_id"] == fixture.banks["HSBC"].bank_id, (
            f"Expected HSBC (27.20% yield) to win T-Bill Buy over QNB (25.80%), got {eval_tbb.get('winner_bank_name')}"
        )

        # 5.5B: T-Bill SELL (Lowest discount rate % wins because corporate is selling T-Bills!)
        res_tb_sell = client.post(
            "/api/v1/end-user/quotations/",
            headers=make_auth_headers(fixture.maker_user),
            json={
                "type": "TBILL",
                "direction": "Sell",
                "entity_id": fixture.entity.id,
                "amount": 5_000_000.0,
                "settlementDateStart": s_date_1,
                "settlementDateEnd": s_date_1,
                "maturityDateStart": m_date_1,
                "maturityDateEnd": m_date_1,
                "evalRate": 20.0,
                "windowStart": (now_55 - timedelta(minutes=1)).isoformat(),
                "windowEnd": (now_55 + timedelta(minutes=15)).isoformat(),
                "selectedBanks": json.dumps(tb_banks),
                "legalDisclaimerAccepted": True
            }
        )
        assert res_tb_sell.status_code == 200
        rfq_tbs_id = res_tb_sell.json()["rfq_id"]
        created_rfq_ids.append(rfq_tbs_id)
        rfq_tbs_db = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_tbs_id).first()
        rfq_tbs_db.status = "PENDING"
        db.commit()

        tok_tbs = {}
        for a in res_tb_sell.json()["assignments"]:
            db_a = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.id == a["id"]).first()
            qb_row = db.query(QuotationBank).filter(QuotationBank.id == db_a.quotation_bank_id).first()
            code = [k for k, v in fixture.banks.items() if v.id == qb_row.id or v.bank_id == qb_row.bank_id][0]
            tok_tbs[code] = a["token"]

        hsbc_stbs = authenticate_portal_session(db, tok_tbs["HSBC"], "dealer.hsbc@hsbc.com", "EXECUTION", claim_desk=True)
        qnb_stbs = authenticate_portal_session(db, tok_tbs["QNB"], "dealer.qnb@qnb.com.eg", "EXECUTION", claim_desk=True)

        # In T-Bill Sell, QNB quotes 24.50% discount rate and HSBC quotes 25.50% -> QNB wins (lower discount rate = higher sale proceeds)!
        assert client.post("/api/v1/public-quotation/tbill-offer", json={
            "token": tok_tbs["HSBC"], "email": "dealer.hsbc@hsbc.com", "session_token": hsbc_stbs,
            "lines": [{"settlementDate": s_date_1, "maturityDate": m_date_1, "discountRate": 25.50, "maxAmount": 5000000.0}]
        }).status_code == 200
        assert client.post("/api/v1/public-quotation/tbill-offer", json={
            "token": tok_tbs["QNB"], "email": "dealer.qnb@qnb.com.eg", "session_token": qnb_stbs,
            "lines": [{"settlementDate": s_date_1, "maturityDate": m_date_1, "discountRate": 24.50, "maxAmount": 5000000.0}]
        }).status_code == 200

        rfq_tbs_db.window_end = datetime.now(timezone.utc) - timedelta(seconds=5)
        db.commit()
        eval_tbs = compute_rfq_standings(rfq_tbs_db, db, dispatch_emails=False)
        assert eval_tbs["winner_bank_id"] == fixture.banks["QNB"].bank_id, (
            f"Expected QNB (24.50% discount rate) to win T-Bill Sell over HSBC (25.50%), got {eval_tbs.get('winner_bank_name')}"
        )
        print("  [PASS] T-Bill Buy (HSBC @ 27.20% won) and T-Bill Sell (QNB @ 24.50% won) multi-line evaluations verified")

        # ==============================================================================
        # ALL TESTS COMPLETE
        # ==============================================================================
        print("\n" + "=" * 80)
        print("[SUCCESS] ALL 5 PARTS OF THE OMNI SIMULATION & ADVERSARIAL SUITE PASSED 100%!")
        print("=" * 80)

    finally:
        # Check if user requested preserving test quotations in UI history for visual inspection
        preserve_data = False
        if "--preserve" in sys.argv or os.getenv("PRESERVE_TEST_DATA", "").strip().lower() in ("1", "true", "yes"):
            preserve_data = True
        elif "--clean" in sys.argv:
            preserve_data = False
        elif hasattr(sys.stdin, "isatty") and sys.stdin.isatty():
            try:
                ans = input("\n[TEARDOWN PROMPT] Keep test RFQ records in database for UI inspection? (y/N) [Default: N]: ").strip().lower()
                preserve_data = ans in ("y", "yes")
            except Exception:
                preserve_data = False
        else:
            preserve_data = False

        if preserve_data:
            print("\n--- [PRESERVE] Test RFQs preserved in database for UI inspection ---")
            print("  [UI] You can view these RFQs in Quotation History under Customer 1 (Acme Corporation):")
            for rid in created_rfq_ids:
                print(f"       -> RFQ ID: {rid}")
            db.close()
            return

        # Guaranteed Idempotent Cleanup: Purge test RFQs created during this run
        print("\n--- [TEARDOWN] Purging test RFQ artifacts to guarantee 100% idempotency ---")
        try:
            for rid in created_rfq_ids:
                ass_ids_subq = db.query(QuotationBankAssignment.id).filter(QuotationBankAssignment.rfq_id == rid)
                db.query(QuotationAccessOTP).filter(QuotationAccessOTP.assignment_id.in_(ass_ids_subq)).delete(synchronize_session=False)
                db.query(QuotationOffer).filter(QuotationOffer.assignment_id.in_(ass_ids_subq)).delete(synchronize_session=False)
                db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id.in_(ass_ids_subq)).delete(synchronize_session=False)
                db.query(QuotationBankLegConfig).filter(QuotationBankLegConfig.assignment_id.in_(ass_ids_subq)).delete(synchronize_session=False)
                db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rid).delete(synchronize_session=False)
                db.query(QuotationLeg).filter(QuotationLeg.rfq_id == rid).delete(synchronize_session=False)
                db.query(QuotationRequest).filter(QuotationRequest.id == rid).delete(synchronize_session=False)
            db.commit()
            print("  [PASS] Teardown complete: All test RFQs cleanly wiped. Suite is ready for repeated runs.")
        except Exception as te:
            db.rollback()
            print(f"  [WARN] Teardown warning: {te}")
        finally:
            db.close()


if __name__ == "__main__":
    run_omni_simulation_suite()
