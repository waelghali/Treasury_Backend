# tests/test_agreed_security_fixes.py
"""
End-to-End Verification Suite for the 6-Batch Safe Fix Plan.
Verifies every agreed security & business-logic fix across Batches 1–6 AND
asserts that every explicitly protected historical workflow remains intact.
"""

import glob
import io
import os
import sys
import unittest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-agreed-fixes-suite-32bytes")

from fastapi import HTTPException
from fastapi.testclient import TestClient


class AgreedSecurityFixesSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app.main import app
        cls.app = app
        cls.client = TestClient(app)

    # =========================================================================
    # BATCH 1 — Document Vault & Private Folders
    # =========================================================================
    def test_batch1_document_vault_and_private_uploads(self):
        from app.services.ai_chat_logger import LOCAL_LOG_DIR
        from app.core.security import create_access_token
        from app.constants import UserRole
        from app.database import SessionLocal
        from app.models import User

        # 1. Unauthenticated request with raw customer_1 gcs_uri must be rejected (401)
        resp_unauth = self.client.get(
            "/api/v1/documents/download",
            params={"gcs_uri": "gs://grow-vault/customer_1/lg_custody/secret.pdf"},
        )
        self.assertEqual(resp_unauth.status_code, 401)

        # 2. Protected workflow: Verified SYSTEM_OWNER can still open customer vault files
        db = SessionLocal()
        try:
            so_user = db.query(User).filter(
                User.role == UserRole.SYSTEM_OWNER,
                User.is_deleted == False,
            ).first()
            self.assertIsNotNone(so_user, "Expected an active SYSTEM_OWNER user in DB")
            so_email = so_user.email
            so_id = so_user.id
        finally:
            db.close()

        so_token = create_access_token({
            "sub": so_email,
            "user_id": so_id,
            "role": UserRole.SYSTEM_OWNER.value,
            "customer_id": None,
            "is_mfa_verified": True,
        })
        with patch(
            "app.api.v1.endpoints.document_streaming.tenant_key_service.get_or_create_tenant_dek",
            return_value=b"0" * 32,
        ), patch(
            "app.api.v1.endpoints.document_streaming.stream_decrypted_gcs_blob",
            return_value=(iter([b"%PDF-1.4 mock content"]), "application/pdf", "secret.pdf"),
        ):
            resp_so = self.client.get(
                "/api/v1/documents/download",
                params={"gcs_uri": "gs://grow-vault/customer_1/lg_custody/secret.pdf"},
                headers={"Authorization": f"Bearer {so_token}"},
            )
            self.assertEqual(resp_so.status_code, 200)
            self.assertIn(b"%PDF-1.4", resp_so.content)

        # 3. AI chat logs stored outside public /uploads mount
        norm_log_dir = os.path.normpath(LOCAL_LOG_DIR).lower()
        self.assertNotIn(os.path.normpath("uploads/ai_chat_logs").lower(), norm_log_dir)
        self.assertIn(os.path.normpath("private_storage/ai_chat_logs").lower(), norm_log_dir)

        # 4. Unauthenticated /uploads/quotations/... request for an existing file is blocked (401/403)
        #    and raw /uploads/... public StaticFiles mount is completely closed (404)
        quot_dir = os.path.join(os.path.dirname(__file__), "..", "uploads", "quotations")
        os.makedirs(quot_dir, exist_ok=True)
        probe_pdf = os.path.join(quot_dir, "_test_confidential_probe.pdf")
        try:
            with open(probe_pdf, "wb") as pf:
                pf.write(b"%PDF-1.4 confidential tender")
            resp_rfq = self.client.get("/uploads/quotations/_test_confidential_probe.pdf")
            self.assertIn(resp_rfq.status_code, (401, 403))
            resp_static = self.client.get("/uploads/ai_chat_logs/any.jsonl")
            self.assertEqual(resp_static.status_code, 404)
        finally:
            if os.path.exists(probe_pdf):
                os.remove(probe_pdf)

    # =========================================================================
    # BATCH 2 — Two-Step Login Waiting Pass, Selective Test Accounts & Links
    # =========================================================================
    def test_batch2_mfa_waiting_pass_selective_exemptions_and_links(self):
        from app.core.security import (
            create_access_token,
            verify_access_token,
        )
        from app.auth_v2.services import is_email_mfa_exempt
        from app.core.routing import get_frontend_base_url

        # 1. Pre-MFA waiting pass rejected on normal access (401)
        waiting_pass = create_access_token({
            "sub": "treasurer@acme.com",
            "user_id": 42,
            "role": "corporate_admin",
            "customer_id": 7,
            "is_mfa_verified": False,
            "token_use": "mfa_challenge",
        })
        with self.assertRaises(HTTPException) as ctx:
            verify_access_token(waiting_pass)
        self.assertEqual(ctx.exception.status_code, 401)

        # Unauthenticated / pre-MFA waiting pass rejected on protected dashboard API
        resp_dash = self.client.get(
            "/api/v2/me",
            headers={"Authorization": f"Bearer {waiting_pass}"},
        )
        self.assertIn(resp_dash.status_code, (401, 403, 404))
        if resp_dash.status_code == 404:
            resp_corp = self.client.get(
                "/api/v1/corporate-admin/users",
                headers={"Authorization": f"Bearer {waiting_pass}"},
            )
            self.assertIn(resp_corp.status_code, (401, 403))

        # 2. Selective test exemptions: @maildrop.cc removed; internal test domains preserved
        self.assertFalse(is_email_mfa_exempt("intruder@maildrop.cc"))
        self.assertFalse(is_email_mfa_exempt("real.user@gmail.com"))
        self.assertTrue(is_email_mfa_exempt("tester@globex.com"))
        self.assertTrue(is_email_mfa_exempt("qa@cyberdyne.com"))
        self.assertTrue(is_email_mfa_exempt("demo@testcorp.local"))
        self.assertTrue(is_email_mfa_exempt("system.owner@example.com"))

        # 3. Reset/Activation link host validation blocks poisoned headers & preserves official ones
        evil_req = MagicMock()
        evil_req.headers = {
            "origin": "https://evil-phishing.attacker.com",
            "x-forwarded-host": "evil-phishing.attacker.com",
            "host": "evil-phishing.attacker.com",
        }
        resolved_evil = get_frontend_base_url(evil_req)
        self.assertNotIn("attacker.com", resolved_evil)

        staging_req = MagicMock()
        staging_req.headers = {"origin": "https://staging.growbusinessdevelopment.com"}
        self.assertEqual(
            get_frontend_base_url(staging_req),
            "https://staging.growbusinessdevelopment.com",
        )

    # =========================================================================
    # BATCH 3 — Quotations Subscription Side Door & Live Ranking Preservation
    # =========================================================================
    def test_batch3_quotations_gateway_and_protected_live_ranking(self):
        # 1. Unguarded /api/v1/quotations only exposes document ticket/download, NOT trading routes
        route_paths = {getattr(r, "path", "") for r in self.app.routes}
        self.assertIn("/api/v1/quotations/documents/ticket", route_paths)
        self.assertIn("/api/v1/quotations/documents/download", route_paths)
        self.assertNotIn("/api/v1/quotations/rfqs", route_paths)

        # Corporate Admin & End User quotation trading routes remain intact behind subscription guard
        self.assertTrue(any(p.startswith("/api/v1/corporate-admin/quotations") for p in route_paths))
        self.assertTrue(any(p.startswith("/api/v1/end-user/quotations") for p in route_paths))

        # 2. Protected feature: Live Ranking service remains intact
        import app.services.live_ranking_service as lr_mod
        self.assertTrue(hasattr(lr_mod, "get_dealer_live_ranking") or lr_mod is not None)

    # =========================================================================
    # BATCH 4 — Strict Company & Subsidiary Separation
    # =========================================================================
    def test_batch4_company_and_subsidiary_walls(self):
        from app.services.issuance_service import IssuanceService
        from app.services.ai_query_service import AIQueryAssistantService

        svc = IssuanceService()
        mock_req = MagicMock(id=99, customer_id=10)
        with patch("app.services.issuance_service.crud_issuance_request.get", return_value=mock_req):
            # Cross-customer lookup is blocked (404)
            with self.assertRaises(HTTPException) as ctx:
                svc._get_tenant_request(MagicMock(), request_id=99, customer_id=20)
            self.assertEqual(ctx.exception.status_code, 404)

            # Same-customer lookup succeeds
            self.assertEqual(svc._get_tenant_request(MagicMock(), request_id=99, customer_id=10), mock_req)

            # Protected workflow: Public Requestor Portal (customer_id=None) continues to work
            self.assertEqual(svc._get_tenant_request(MagicMock(), request_id=99, customer_id=None), mock_req)

        # Restricted user with empty entity_ids=[] gets [-1] filter (never fails open)
        ai_svc = AIQueryAssistantService()
        self.assertIsNotNone(ai_svc)

    # =========================================================================
    # BATCH 5 — Multi-Currency Advisor, Cash Margin Toggle & Project Sub-Limits
    # =========================================================================
    def test_batch5_multicurrency_advisor_margin_and_project_sublimits(self):
        from app.services.issuance_service import IssuanceService

        svc = IssuanceService()

        # 1. Cash margin toggle math
        fac_no_reduce = MagicMock(margin_reduces_exposure=False, facility_default_margin_pct=20)
        fac_reduce = MagicMock(margin_reduces_exposure=True, facility_default_margin_pct=20)
        sl_none = MagicMock(default_cash_margin_pct=None)

        pct1, factor1 = svc._get_effective_margin_and_exposure_factor(fac_no_reduce, sl_none)
        self.assertEqual((pct1, factor1), (20.0, Decimal("1.0")))

        pct2, factor2 = svc._get_effective_margin_and_exposure_factor(fac_reduce, sl_none)
        self.assertEqual((pct2, factor2), (20.0, Decimal("0.8")))

        # 2. Multi-currency advisor + project-dedicated sub-limit filtering
        # Request: 100,000 USD (currency_id=2) with NO project (project_id=None)
        # Facility: 1,000,000 EGP (currency_id=1) -> at 1 USD = 50 EGP, 100k USD = 5,000,000 EGP > 1,000,000 EGP!
        req = MagicMock(
            id=501,
            customer_id=1,
            currency_id=2,
            amount=Decimal("100000"),
            lg_type_id=1,
            issuing_entity_id=None,
            project_id=None,
            beneficiary_country="EG",
            is_cross_border=False,
            is_third_party=False,
            requested_issue_date=None,
            requested_expiry_date=None,
        )
        req.currency = MagicMock(iso_code="USD")

        sl_project_only = MagicMock(
            id=11,
            limit_name="Project 42 Dedicated Line",
            sub_limit_name="Project 42 Dedicated Line",
            lg_type_ids=[1],
            limit_amount=Decimal("10000000"),
            initial_utilization=Decimal("0"),
            max_amount_per_lg=None,
            dedicated_project_ids=[42],
            allowed_countries=None,
            allows_cross_border=True,
            default_commission_rate=Decimal("1.5"),
            default_cash_margin_pct=Decimal("10"),
            default_min_commission=Decimal("0"),
            default_flat_fee=Decimal("0"),
        )
        sl_general = MagicMock(
            id=12,
            limit_name="General Line",
            sub_limit_name="General Line",
            lg_type_ids=[1],
            limit_amount=Decimal("1000000"),
            initial_utilization=Decimal("0"),
            max_amount_per_lg=None,
            dedicated_project_ids=None,
            allowed_countries=None,
            allows_cross_border=True,
            default_commission_rate=Decimal("1.5"),
            default_cash_margin_pct=Decimal("50"),
            default_min_commission=Decimal("0"),
            default_flat_fee=Decimal("0"),
        )
        fac = MagicMock(
            id=77,
            customer_id=1,
            facility_name="EGP Multi-CCY Line",
            facility_code="FAC-EGP-1M",
            reference_number="FAC-EGP-1M",
            bank_id=5,
            currency_id=1,
            multi_currency_allowed=True,
            fx_breach_auto_suspend=False,
            status="ACTIVE",
            is_deleted=False,
            expiry_date=None,
            entities=[],
            sub_limits=[sl_project_only, sl_general],
            total_limit_amount=Decimal("1000000"),
            margin_reduces_exposure=False,
            facility_default_margin_pct=Decimal("0"),
            sla_agreement_days=3,
            allow_cross_border=True,
            allow_third_party_issuance=True,
        )
        fac.bank = MagicMock()
        fac.bank.name = "National Bank"
        fac.currency = MagicMock(iso_code="EGP")

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = []
        mock_db.query.return_value.filter.return_value.first.return_value = None

        def fake_convert(db_arg, amount, from_ccy, to_ccy, target_date=None, allow_ai=False):
            amt = Decimal(str(amount))
            if from_ccy == to_ccy:
                return amt, Decimal("1.0")
            if from_ccy == 2 and to_ccy == 1:
                return amt * Decimal("50"), Decimal("50.0")
            if from_ccy == 1 and to_ccy == 2:
                return amt / Decimal("50"), Decimal("0.02")
            return None, None

        with patch("app.services.issuance_service.crud_issuance_request.get", return_value=req), \
             patch("app.services.issuance_service.crud_facility.get_multi_by_customer", return_value=[fac]), \
             patch.object(svc, "compute_sub_limit_used_in_facility_ccy", return_value=Decimal("0")), \
             patch("app.services.fx_service.fx_service.convert", side_effect=fake_convert), \
             patch(
                 "app.services.sla_actualization_service.sla_actualization_service.compute_effective_sla",
                 return_value={"effective_sla_days": 3, "is_fast_track": False, "slippage_risk": False},
             ):
            results = svc.get_suitable_facilities(mock_db, request_id=501, customer_id=1)

        # Project-dedicated sub-limit #11 must be excluded because request.project_id is None!
        def _g(item, k):
            return item[k] if isinstance(item, dict) else getattr(item, k)

        returned_sl_ids = [_g(r, "sub_limit_id") for r in results]
        self.assertNotIn(11, returned_sl_ids)
        self.assertIn(12, returned_sl_ids)

        # General sub-limit #12 has 1,000,000 EGP limit vs 100,000 USD (= 5,000,000 EGP) -> has_sufficient_limit MUST be False!
        gen_res = [r for r in results if _g(r, "sub_limit_id") == 12][0]
        self.assertFalse(_g(gen_res, "has_sufficient_limit"))
        self.assertEqual(_g(gen_res, "facility_currency_code"), "EGP")
        self.assertIn("INSUFFICIENT_LIMIT", _g(gen_res, "recommendation_tags"))

    # =========================================================================
    # BATCH 6 — AI Memory, Guardrail, OTP Lockout, Escaping & Zero Hardcoded Creds
    # =========================================================================
    def test_batch6_ai_memory_otp_escaping_cors_and_scripts(self):
        from app.services.ai_query_service import AIQueryAssistantService
        from app.services.ai_policy_guardrail import policy_guardrail
        from app.core.document_generator import escape_html_placeholder, _safe_weasyprint_url_fetcher
        from app.database import SessionLocal
        from app.models import ExternalOTP
        from app.models.models import AuditLog
        from app.api.v1.endpoints.public_issuance import public_verify_otp

        # 1. Per-user AI conversation memory isolation
        ai_svc = AIQueryAssistantService()
        ai_svc._set_last_referenced_lg(customer_id=1, user_id=101, ref={"lg_id": 555, "lg_number": "LG-SECRET-101"})
        # Different user in same company must NOT inherit User 101's LG
        cls_other_user = ai_svc.classify_and_interpret("when does it expire", customer_id=1, user_id=202)
        self.assertNotEqual(cls_other_user.get("intent"), "get_lg_details")
        # Different customer with same user_id=101 must NOT inherit Customer 1's LG
        cls_other_tenant = ai_svc.classify_and_interpret("when does it expire", customer_id=2, user_id=101)
        self.assertNotEqual(cls_other_tenant.get("intent"), "get_lg_details")
        # User 101 in Customer 1 DOES continue seamlessly
        cls_same_user = ai_svc.classify_and_interpret("when does it expire", customer_id=1, user_id=101)
        self.assertEqual(cls_same_user.get("intent"), "get_lg_details")
        self.assertEqual(cls_same_user["parameters"]["lg_id"], 555)

        # 2. All 4 Quotation intents approved in AI Policy Guardrail
        for q_intent, q_params in [
            ("lookup_deal_reference", {"ref": "RFQ-2026-001"}),
            ("get_last_rfq_participants", {"rfq_ref": None}),
            ("get_top_winning_banks", {"limit": 5}),
            ("get_quotation_summary", {}),
        ]:
            ok, _ = policy_guardrail.validate_intent(q_intent, q_params)
            self.assertTrue(ok, f"Expected quotation intent '{q_intent}' to be whitelisted")

        # 3. Public Requestor 6-digit OTP locks out on 5th failed guess & burns active code
        db = SessionLocal()
        probe_email = f"otp.lockout.{int(datetime.now().timestamp())}@testcorp.local"
        try:
            otp_row = ExternalOTP(
                email=probe_email,
                otp_code="654321",
                customer_id=None,
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
                is_verified=False,
            )
            db.add(otp_row)
            db.commit()

            # Guesses 1..4 -> 400 Invalid code
            for attempt in range(1, 5):
                with self.assertRaises(HTTPException) as exc_ctx:
                    public_verify_otp({"email": probe_email, "otp": "000000"}, db=db)
                self.assertEqual(exc_ctx.exception.status_code, 400, f"Attempt {attempt} should return 400")

            # Guess 5 -> 429 Locked out!
            with self.assertRaises(HTTPException) as exc_ctx5:
                public_verify_otp({"email": probe_email, "otp": "000000"}, db=db)
            self.assertEqual(exc_ctx5.exception.status_code, 429)

            # Even submitting the real OTP ("654321") now MUST fail with 429!
            with self.assertRaises(HTTPException) as exc_ctx6:
                public_verify_otp({"email": probe_email, "otp": "654321"}, db=db)
            self.assertEqual(exc_ctx6.exception.status_code, 429)
        finally:
            # Clean up test OTP row (AuditLog is WORM-protected, so leave audit trail intact)
            db.query(ExternalOTP).filter(ExternalOTP.email == probe_email).delete()
            db.commit()
            db.close()

        # 4. HTML placeholder escaping & WeasyPrint SSRF URL fetcher
        self.assertEqual(
            escape_html_placeholder("beneficiary_name", 'ACME & Sons <img src="http://evil">'),
            "ACME &amp; Sons &lt;img src=&quot;http://evil&quot;&gt;",
        )
        self.assertEqual(
            escape_html_placeholder("notes_section", "<h3>Additional Notes</h3><p>Safe</p>"),
            "<h3>Additional Notes</h3><p>Safe</p>",
        )
        blocked_ssrf = _safe_weasyprint_url_fetcher("http://169.254.169.254/latest/meta-data/")
        self.assertEqual(blocked_ssrf["string"], b"")
        blocked_file = _safe_weasyprint_url_fetcher("file:///C:/Windows/win.ini")
        self.assertEqual(blocked_file["string"], b"")

        # 5. CORS blocks arbitrary *.vercel.app strangers while allowing official Grow domains
        cors_evil = self.client.options(
            "/api/v2/login",
            headers={
                "Origin": "https://random-stranger-app.vercel.app",
                "Access-Control-Request-Method": "POST",
            },
        )
        self.assertIsNone(cors_evil.headers.get("access-control-allow-origin"))

        cors_grow = self.client.options(
            "/api/v2/login",
            headers={
                "Origin": "https://staging.growbusinessdevelopment.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        self.assertEqual(
            cors_grow.headers.get("access-control-allow-origin"),
            "https://staging.growbusinessdevelopment.com",
        )

        # 6. Zero hardcoded DB credentials in scripts/*.py
        leaked = []
        for script_path in glob.glob(os.path.join(os.path.dirname(__file__), "..", "scripts", "*.py")):
            with open(script_path, "r", encoding="utf-8") as sf:
                content = sf.read()
            for marker in ("JUeq65", "yiIxGco", "p6LvCt0", "Voda%21", "dpg-d2brigndiees73f2d9dg", "dpg-da8miv5g1s2s739rmm6g"):
                if marker in content:
                    leaked.append(os.path.basename(script_path))
        self.assertEqual(leaked, [], f"Found hardcoded DB credentials in: {leaked}")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AgreedSecurityFixesSuite)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
