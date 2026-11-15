# tests/test_rarity_and_wording_trap_detector.py
"""
Automated Test Suite for Additive Enhancements:
1. Idea A: Smart Facility Advisor — Portfolio Rarity & Capability Preservation Weight
2. Idea D (Part 1): Bilingual (Arabic & English) Guarantee Special-Wording Trap Detector
"""

import asyncio
import os
import sys
import unittest
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-agreed-fixes-suite-32bytes")

from fastapi import HTTPException

from app.constants import GlobalConfigKey
from app.services.issuance_service import IssuanceService
from app.services.wording_risk_service import (
    analyze_special_wording_traps,
    run_deterministic_beneficiary_scan,
    run_deterministic_wording_scan,
)


def _make_sub_limit(
    sub_id: int,
    name: str,
    limit_amount: float = 1_000_000.0,
    lg_type_ids=None,
    allows_open_ended: bool = False,
    allows_confirmation: bool = False,
    max_tenor_days=365,
    dedicated_project_ids=None,
    comm_rate: float = 1.5,
    margin_pct: float = 0.0,
):
    return SimpleNamespace(
        id=sub_id,
        limit_name=name,
        limit_amount=Decimal(str(limit_amount)),
        lg_type_ids=lg_type_ids if lg_type_ids is not None else [1],
        max_amount_per_lg=None,
        max_tenor_days=max_tenor_days,
        allows_open_ended=allows_open_ended,
        allowed_countries=None,
        allows_confirmation=allows_confirmation,
        default_commission_rate=Decimal(str(comm_rate)),
        default_cash_margin_pct=Decimal(str(margin_pct)),
        default_min_commission=Decimal("0"),
        default_flat_fee=Decimal("0"),
        dedicated_project_ids=dedicated_project_ids,
        initial_utilization=Decimal("0"),
    )


def _make_facility(
    fac_id: int,
    bank_id: int,
    bank_name: str,
    sub_limits,
    currency_id: int = 1,
    total_limit: float = 2_000_000.0,
    allow_cross_border: bool = False,
    allow_third_party_issuance: bool = False,
    multi_currency_allowed: bool = False,
    tenor_months: int = 12,
):
    return SimpleNamespace(
        id=fac_id,
        facility_name=f"{bank_name} Facility",
        bank_id=bank_id,
        bank=SimpleNamespace(id=bank_id, name=bank_name),
        currency_id=currency_id,
        currency=SimpleNamespace(iso_code="EGP", code="EGP"),
        total_limit_amount=Decimal(str(total_limit)),
        status="ACTIVE",
        is_deleted=False,
        expiry_date=date.today() + timedelta(days=365),
        allow_cross_border=allow_cross_border,
        allow_third_party_issuance=allow_third_party_issuance,
        multi_currency_allowed=multi_currency_allowed,
        tenor_months=tenor_months,
        margin_reduces_exposure=False,
        facility_default_margin_pct=Decimal("0"),
        sla_agreement_days=2,
        actual_avg_sla_days=Decimal("2.0"),
        sla_commitment_pct=Decimal("95.0"),
        entities=[],
        sub_limits=sub_limits,
    )


def _make_request(
    req_id: int = 100,
    customer_id: int = 1,
    lg_type_id: int = 1,
    currency_id: int = 1,
    amount: float = 100_000.0,
    is_cross_border: bool = False,
    is_third_party: bool = False,
    is_open_ended: bool = False,
    requested_expiry_date=None,
    project_id=None,
    is_urgent: bool = False,
):
    return SimpleNamespace(
        id=req_id,
        customer_id=customer_id,
        lg_type_id=lg_type_id,
        lg_type=SimpleNamespace(id=lg_type_id, name="Bid Bond"),
        currency_id=currency_id,
        currency=SimpleNamespace(id=currency_id, iso_code="EGP"),
        amount=Decimal(str(amount)),
        is_cross_border=is_cross_border,
        is_third_party=is_third_party,
        is_open_ended=is_open_ended,
        requested_issue_date=date.today(),
        requested_expiry_date=requested_expiry_date,
        project_id=project_id,
        issuing_entity_id=10,
        issuance_country="Egypt",
        beneficiary_country="Egypt",
        cross_border_details=None,
        treasury_enrichment=None,
        is_urgent=is_urgent,
    )


def _mock_db_for_suitable():
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = []
    return db


class RarityAndWordingTrapDetectorSuite(unittest.TestCase):
    def test_idea_a_saves_rare_facility_for_routine_request_while_keeping_selectable(self):
        """
        Routine domestic fixed-expiry Bid Bond:
        - Bank Common MUST outrank Bank Rare (the company's ONLY Cross-Border + Open-Ended + Multi-Currency line).
        - Bank Rare MUST still appear in results with has_sufficient_limit=True so user can freely override!
        """
        svc = IssuanceService()
        common_sub = _make_sub_limit(101, "Standard Domestic Bid Bonds", lg_type_ids=[1])
        rare_sub = _make_sub_limit(
            202,
            "Specialized Global Umbrella",
            lg_type_ids=[1, 2],
            allows_open_ended=True,
            allows_confirmation=True,
        )
        fac_common = _make_facility(1, 11, "Bank Common", [common_sub])
        fac_rare = _make_facility(
            2,
            22,
            "Bank Rare",
            [rare_sub],
            allow_cross_border=True,
            multi_currency_allowed=True,
        )
        req = _make_request(requested_expiry_date=date.today() + timedelta(days=120))
        db = _mock_db_for_suitable()

        with patch.object(svc, "_get_tenant_request", return_value=req), patch(
            "app.services.issuance_service.crud_facility.get_multi_by_customer",
            return_value=[fac_rare, fac_common],
        ), patch.object(
            svc, "compute_sub_limit_used_in_facility_ccy", return_value=Decimal("0")
        ), patch(
            "app.crud.crud_config.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value=None,
        ), patch(
            "app.services.sla_actualization_service.sla_actualization_service.compute_effective_sla",
            return_value={"effective_sla_days": 2.0, "is_fast_track": False, "slippage_risk": False},
        ):
            results = svc.get_suitable_facilities(db, request_id=100, customer_id=1)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].facility_bank, "Bank Common")
        self.assertEqual(results[1].facility_bank, "Bank Rare")
        self.assertIn("BEST_OVERALL", results[0].recommendation_tags)
        self.assertIn("COMMON_FIT", results[0].recommendation_tags)
        self.assertIn("RARE_LINE_SAVE", results[1].recommendation_tags)
        self.assertEqual(results[0].rarity_score, 100.0)
        self.assertLess(results[1].rarity_score, 40.0)
        self.assertIn("Cross-Border", results[1].saved_capabilities or [])
        self.assertIn("Open-Ended", results[1].saved_capabilities or [])
        self.assertTrue(results[1].has_sufficient_limit)

    def test_idea_a_does_not_penalize_rare_capabilities_when_request_needs_them(self):
        """When a request genuinely requires Cross-Border + Open-Ended, those perks are NOT treated as wasted."""
        svc = IssuanceService()
        cb_oe_sub_1 = _make_sub_limit(201, "Global Line A", lg_type_ids=[1], allows_open_ended=True)
        cb_oe_sub_2 = _make_sub_limit(202, "Global Line B (+ Third Party)", lg_type_ids=[1], allows_open_ended=True)
        fac_a = _make_facility(1, 11, "Global Bank A", [cb_oe_sub_1], allow_cross_border=True)
        fac_b = _make_facility(
            2, 22, "Global Bank B", [cb_oe_sub_2], allow_cross_border=True, allow_third_party_issuance=True
        )
        req = _make_request(
            is_cross_border=True,
            is_open_ended=True,
            requested_expiry_date=None,
        )
        db = _mock_db_for_suitable()

        with patch.object(svc, "_get_tenant_request", return_value=req), patch(
            "app.services.issuance_service.crud_facility.get_multi_by_customer",
            return_value=[fac_b, fac_a],
        ), patch.object(
            svc, "compute_sub_limit_used_in_facility_ccy", return_value=Decimal("0")
        ), patch(
            "app.crud.crud_config.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value=None,
        ), patch(
            "app.services.sla_actualization_service.sla_actualization_service.compute_effective_sla",
            return_value={"effective_sla_days": 2.0, "is_fast_track": False, "slippage_risk": False},
        ):
            results = svc.get_suitable_facilities(db, request_id=100, customer_id=1)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].facility_bank, "Global Bank A")
        self.assertEqual(results[0].rarity_score, 100.0)
        self.assertNotIn("Cross-Border", results[0].saved_capabilities or [])
        self.assertNotIn("Open-Ended", results[0].saved_capabilities or [])
        self.assertIn("Third-Party", results[1].saved_capabilities or [])

    def test_idea_a_prioritizes_project_dedicated_line_to_save_general_lines(self):
        """Project-earmarked sub-limit ranks ahead of general unrestricted sub-limits for that project."""
        svc = IssuanceService()
        general_sub = _make_sub_limit(101, "General Corporate Sub-Limit", lg_type_ids=[1])
        project_sub = _make_sub_limit(102, "Project 77 Dedicated Sub-Limit", lg_type_ids=[1], dedicated_project_ids=[77])
        fac_general = _make_facility(1, 11, "General Bank", [general_sub])
        fac_project = _make_facility(2, 22, "Project Bank", [project_sub])
        req = _make_request(project_id=77, requested_expiry_date=date.today() + timedelta(days=180))
        db = _mock_db_for_suitable()

        with patch.object(svc, "_get_tenant_request", return_value=req), patch(
            "app.services.issuance_service.crud_facility.get_multi_by_customer",
            return_value=[fac_general, fac_project],
        ), patch.object(
            svc, "compute_sub_limit_used_in_facility_ccy", return_value=Decimal("0")
        ), patch(
            "app.crud.crud_config.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value=None,
        ), patch(
            "app.services.sla_actualization_service.sla_actualization_service.compute_effective_sla",
            return_value={"effective_sla_days": 2.0, "is_fast_track": False, "slippage_risk": False},
        ):
            results = svc.get_suitable_facilities(db, request_id=100, customer_id=1)

        self.assertEqual(results[0].facility_bank, "Project Bank")
        self.assertIn("DEDICATED", results[0].recommendation_tags)
        self.assertEqual(results[0].rarity_score, 100.0)
        self.assertIn("General-Purpose Line", results[1].saved_capabilities or [])

    def test_idea_a_legacy_customer_custom_5_weights_remain_100_percent_balanced(self):
        """
        Additive guarantee: if a customer previously customized any of the original 5 weights (summing to 100%),
        unconfigured FACILITY_SCORE_WEIGHT_RARITY automatically resolves to '0' instead of '15' so total stays 100%!
        """
        from app.crud.crud_config import CRUDCustomerConfiguration
        from app.models import CustomerConfiguration

        fake_global_crud = MagicMock()
        rarity_gc = SimpleNamespace(
            id=6,
            key=GlobalConfigKey.FACILITY_SCORE_WEIGHT_RARITY,
            value_default="15",
            value_min="0",
            value_max="100",
            unit="percentage",
            description="Rarity weight",
            module_tags=["issuance"],
            created_at=None,
            updated_at=None,
            is_deleted=False,
            deleted_at=None,
        )
        fake_global_crud.get_by_key.return_value = rarity_gc

        cc_crud = CRUDCustomerConfiguration(CustomerConfiguration, fake_global_crud)
        with patch.object(cc_crud, "get_by_customer_and_global_config_id", return_value=None), patch.object(
            cc_crud, "_customer_has_legacy_weight_override", return_value=True
        ):
            resolved_legacy = cc_crud.get_customer_config_or_global_fallback(
                MagicMock(), customer_id=42, config_key=GlobalConfigKey.FACILITY_SCORE_WEIGHT_RARITY
            )
        self.assertEqual(resolved_legacy["effective_value"], "0")

        with patch.object(cc_crud, "get_by_customer_and_global_config_id", return_value=None), patch.object(
            cc_crud, "_customer_has_legacy_weight_override", return_value=False
        ):
            resolved_default = cc_crud.get_customer_config_or_global_fallback(
                MagicMock(), customer_id=99, config_key=GlobalConfigKey.FACILITY_SCORE_WEIGHT_RARITY
            )
        self.assertEqual(resolved_default["effective_value"], "15")

    def test_idea_d_arabic_advance_payment_and_open_ended_traps_flagged_red(self):
        """Arabic special wording with Advance Payment (no non-operative clause) + open-ended + pure first demand -> RED."""
        arabic_trap_text = """
        خطاب ضمان دفعة مقدمة
        نتعهد نحن البنك بأن ندفع لكم بمجرد أول طلب دون إبداء أسباب ودون الالتفات إلى أي معارضة من العميل
        مبلغ وقدره 5,000,000 جنيه مصري بالإضافة إلى الفوائد والمصاريف.
        ويظل هذا الخطاب مفتوح المدة ويجدد تلقائياً لحين استرداد الأصل من طرفكم.
        """
        with patch("app.core.ai_integration._get_genai_client", return_value=None):
            res = asyncio.run(
                analyze_special_wording_traps(
                    wording_text=arabic_trap_text,
                    request_context={"lg_type": "Advance Payment Guarantee", "issuance_country": "Egypt"},
                )
            )
        self.assertEqual(res["status"], "OK")
        self.assertTrue(res["advisory_only"])
        self.assertEqual(res["detected_language"], "AR")
        self.assertEqual(res["risk_level"], "RED")
        clause_types = {f["clause_type"] for f in res["findings"]}
        self.assertIn("OPEN_ENDED_OR_EVERGREEN", clause_types)
        self.assertIn("UNCONDITIONAL_ADVANCE_PAYMENT", clause_types)
        self.assertIn("PURE_FIRST_DEMAND_WAIVER", clause_types)
        self.assertIn("UNCAPPED_OR_COMPOUNDING_LIABILITY", clause_types)
        for f in res["findings"]:
            self.assertTrue(f["plain_explanation"])
            self.assertTrue(f["suggested_protective_rewording"])

    def test_idea_d_clean_urdg758_wording_scores_green(self):
        """Clean URDG 758 wording with fixed expiry, breach statement, and non-operative clause -> GREEN."""
        clean_en_text = """
        Advance Payment Guarantee subject to ICC Uniform Rules for Demand Guarantees (URDG 758).
        Our aggregate liability shall not exceed USD 250,000.
        This Guarantee shall only become operative upon receipt of the advance payment credited to the Applicant's account.
        Any demand must be accompanied by a written statement stating in what respect the Applicant is in breach of its obligations.
        The Guarantee amount shall be automatically reduced pro-rata against interim payment certificates.
        This Guarantee expires on 31-Dec-2027 and shall become null and void automatically upon expiry whether returned or not.
        """
        det = run_deterministic_wording_scan(
            clean_en_text,
            request_context={"lg_type": "Advance Payment LG", "is_auto_reducing": True},
        )
        self.assertEqual(det["risk_level"], "GREEN")
        self.assertEqual(det["findings"], [])
        self.assertGreaterEqual(len(det["positive_protections"]), 3)

    def test_idea_d_endpoint_enforces_tenant_isolation_and_saves_additively(self):
        """Endpoint blocks cross-tenant access (404) and saves advisory analysis additively without changing status."""
        from app.api.v1.endpoints.issuance.post_issuance import analyze_request_special_wording_endpoint

        cross_tenant_db = MagicMock()
        cross_tenant_db.query.return_value.filter.return_value.first.return_value = None
        user_tenant_2 = SimpleNamespace(customer_id=2, user_id=202, role="corporate_admin")

        with self.assertRaises(HTTPException) as ctx_err:
            asyncio.run(
                analyze_request_special_wording_endpoint(
                    request_id=555,
                    document_id=None,
                    wording_text="extend or pay",
                    file=None,
                    db=cross_tenant_db,
                    current_user=user_tenant_2,
                )
            )
        self.assertEqual(ctx_err.exception.status_code, 404)

        req_obj = SimpleNamespace(
            id=555,
            customer_id=1,
            is_deleted=False,
            status="PENDING_APPROVAL",
            currency=SimpleNamespace(iso_code="USD"),
            beneficiary_name="ACME Corp",
            beneficiary_country="Germany",
            lg_type=SimpleNamespace(name="Performance Bond"),
            amount=Decimal("500000"),
            requested_expiry_date=date(2027, 6, 30),
            lg_purpose="Supply Contract",
            is_cross_border=True,
            issuance_country="Egypt",
            is_auto_reducing=False,
            applicable_rules="LOCAL_LAW",
            other_conditions=None,
            metadata_json={"existing_key": "untouched"},
        )
        auth_db = MagicMock()
        auth_db.query.return_value.filter.return_value.first.return_value = req_obj
        user_tenant_1 = SimpleNamespace(customer_id=1, user_id=101, role="corporate_admin")

        with patch("sqlalchemy.orm.attributes.flag_modified"), patch(
            "app.core.ai_integration._get_genai_client", return_value=None
        ):
            result = asyncio.run(
                analyze_request_special_wording_endpoint(
                    request_id=555,
                    document_id=None,
                    wording_text="Payable upon your first written demand without stating reasons. Freely transferable to any third party. Extend or pay.",
                    file=None,
                    db=auth_db,
                    current_user=user_tenant_1,
                )
            )

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["perspective"], "APPLICANT")
        self.assertIn(result["risk_level"], ("RED", "AMBER"))
        self.assertEqual(req_obj.status, "PENDING_APPROVAL")
        self.assertEqual(req_obj.metadata_json["existing_key"], "untouched")
        self.assertEqual(req_obj.metadata_json["special_wording_analysis"]["risk_level"], result["risk_level"])

    def test_custody_opposite_perspective_unconditional_first_demand_is_green_strength(self):
        """
        Proof of exact opposite perspectives:
        An irrevocable, unconditional first-demand + evergreen guarantee waiving court judgments & vendor consent:
        - Scores RED when WE are the Applicant (LG Issuance — locks up our credit line & waives breach proof).
        - Scores GREEN with positive protections when WE are the Beneficiary (LG Custody — strongest possible vendor LG!).
        """
        bilingual_strong_beneficiary_lg = """
        خطاب ضمان نهائي غير قابل للإلغاء وغير مشروط خاضع لقواعد URDG 758
        نتعهد تعهداً باتاً لا رجوع فيه بأن ندفع لكم بمجرد أول طلب كتابي منكم دون إبداء أسباب
        ودون حاجة إلى إنذار أو حكم قضائي أو إثبات الضرر ودون التوقف على موافقة العميل أو الالتفات لأي معارضة من العميل،
        وذلك كاملاً دون أي خصم أو استقطاع، ويجدد تلقائياً لحين إلغائه من المستفيد.
        We irrevocably and unconditionally undertake to pay you upon your first written demand without proof or justification,
        without requiring any court judgment, arbitral award, or proof of loss, and without the consent of the Applicant,
        free and clear of any deductions. This guarantee is automatically renewed until released by the Beneficiary.
        """
        with patch("app.core.ai_integration._get_genai_client", return_value=None):
            issuance_applicant_res = asyncio.run(
                analyze_special_wording_traps(
                    wording_text=bilingual_strong_beneficiary_lg,
                    request_context={"lg_type": "Performance Bond", "issuance_country": "Egypt"},
                    perspective="APPLICANT",
                )
            )
            custody_beneficiary_res = asyncio.run(
                analyze_special_wording_traps(
                    wording_text=bilingual_strong_beneficiary_lg,
                    request_context={"lg_type": "Performance Bond", "beneficiary_country": "Egypt"},
                    perspective="BENEFICIARY",
                )
            )

        self.assertEqual(issuance_applicant_res["perspective"], "APPLICANT")
        self.assertEqual(issuance_applicant_res["risk_level"], "RED")
        self.assertIn("OPEN_ENDED_OR_EVERGREEN", {f["clause_type"] for f in issuance_applicant_res["findings"]})

        self.assertEqual(custody_beneficiary_res["perspective"], "BENEFICIARY")
        self.assertEqual(custody_beneficiary_res["risk_level"], "GREEN")
        self.assertEqual(custody_beneficiary_res["findings"], [])
        self.assertGreaterEqual(len(custody_beneficiary_res["positive_protections"]), 3)

    def test_custody_arabic_vendor_claim_blocking_traps_flagged_red(self):
        """
        Incoming Arabic vendor LG containing claim-blocking hurdles (non-operative + court judgment +
        vendor objection gate + revocable) must score RED under Beneficiary Protection Check.
        """
        arabic_weak_vendor_lg = """
        خطاب ضمان ابتدائي قابل للإلغاء ويحق للبنك إلغاء هذا الخطاب في أي وقت.
        لا يصبح هذا الخطاب سارياً إلا بعد استلام المقاول لمحضر تسليم الموقع.
        ولا يصرف مبلغ الضمان إلا بموجب حكم قضائي نهائي وبعد إثبات الضرر الفعلي،
        وبشرط عدم اعتراض العميل وبعد الرجوع للعميل.
        """
        with patch("app.core.ai_integration._get_genai_client", return_value=None):
            res = asyncio.run(
                analyze_special_wording_traps(
                    wording_text=arabic_weak_vendor_lg,
                    request_context={"lg_type": "Bid Bond"},
                    perspective="BENEFICIARY",
                )
            )
        self.assertEqual(res["status"], "OK")
        self.assertEqual(res["perspective"], "BENEFICIARY")
        self.assertEqual(res["detected_language"], "AR")
        self.assertEqual(res["risk_level"], "RED")
        codes = {f["clause_type"] for f in res["findings"]}
        self.assertIn("NON_OPERATIVE_OR_SUSPENDED_VALIDITY", codes)
        self.assertIn("CONDITIONAL_OR_SURETYSHIP_WORDING", codes)
        self.assertIn("APPLICANT_OBJECTION_OR_CONSENT_GATE", codes)
        self.assertIn("REVOCABLE_OR_BANK_CANCELLATION_CLAUSE", codes)

    def test_custody_english_unilateral_reduction_and_claim_hurdles_detected(self):
        """
        Detects unilateral vendor reduction (RED), third-party engineer certificate (AMBER),
        early 15-day claim cutoff before expiry (AMBER), and unconfirmed foreign bank advice (AMBER).
        """
        en_hurdle_lg = """
        This Guarantee shall be reduced automatically upon presentation by the supplier of copies of commercial invoices.
        Any demand must be accompanied by an independent engineer certificate and must be received by us at least 15 business days prior to the expiry date.
        This guarantee is advised without any engagement or responsibility on our part and governed by and construed in accordance with the laws of England.
        """
        det = run_deterministic_beneficiary_scan(
            en_hurdle_lg,
            request_context={
                "beneficiary_country": "Egypt",
                "foreign_bank_name": "Overseas Trade Bank",
                "advising_status": "Advised",
            },
        )
        self.assertEqual(det["risk_level"], "RED")
        codes = {f["clause_type"] for f in det["findings"]}
        self.assertIn("UNILATERAL_APPLICANT_REDUCTION_CLAUSE", codes)
        self.assertIn("ONEROUS_THIRD_PARTY_CLAIM_DOCUMENTS", codes)
        self.assertIn("EARLY_CLAIM_CUTOFF_OR_COUNTER_HURDLE", codes)
        self.assertIn("UNCONFIRMED_FOREIGN_BANK_OR_LAW", codes)

    def test_custody_endpoint_enforces_tenant_isolation_and_uses_beneficiary_perspective(self):
        """
        Custody endpoint (/end-user/lg-records/{id}/analyze-special-wording):
        - Rejects cross-tenant access with 404.
        - Uses perspective='BENEFICIARY' and supports stored payment_conditions fallback when no file/text is posted.
        """
        from app.api.v1.endpoints.end_user import analyze_custody_lg_special_wording

        cross_db = MagicMock()
        cross_db.query.return_value.filter.return_value.first.return_value = None
        other_user = SimpleNamespace(customer_id=2, user_id=202, role="end_user")

        with self.assertRaises(HTTPException) as err_ctx:
            asyncio.run(
                analyze_custody_lg_special_wording(
                    lg_record_id=888,
                    document_id=None,
                    wording_text="upon your first written demand",
                    file=None,
                    db=cross_db,
                    current_user=other_user,
                )
            )
        self.assertEqual(err_ctx.exception.status_code, 404)

        custody_lg = SimpleNamespace(
            id=888,
            customer_id=1,
            is_deleted=False,
            issuer_name="Delta Contractors Co.",
            beneficiary_corporate=SimpleNamespace(entity_name="Grow Holdings Egypt"),
            lg_currency=SimpleNamespace(iso_code="EGP"),
            lg_type=SimpleNamespace(name="Performance Guarantee"),
            lg_amount=Decimal("1200000"),
            expiry_date=date(2027, 12, 31),
            description_purpose="Main civil works",
            lg_operational_status=SimpleNamespace(name="Non-Operative"),
            advising_status="Advised",
            foreign_bank_name=None,
            foreign_bank_country=None,
            auto_renewal=False,
            applicable_rule=SimpleNamespace(name="URDG 758"),
            applicable_rules_text=None,
            payment_conditions="Payable only upon presentation of a final court judgment and subject to written consent of the contractor.",
            other_conditions=None,
            mandatory_claim_statement=None,
        )
        custody_db = MagicMock()
        custody_db.query.return_value.filter.return_value.first.return_value = custody_lg
        tenant_user = SimpleNamespace(customer_id=1, user_id=101, role="end_user")

        with patch("app.core.ai_integration._get_genai_client", return_value=None):
            res = asyncio.run(
                analyze_custody_lg_special_wording(
                    lg_record_id=888,
                    document_id=None,
                    wording_text=None,  # Exercises stored payment_conditions fallback
                    file=None,
                    db=custody_db,
                    current_user=tenant_user,
                )
            )

        self.assertEqual(res["status"], "OK")
        self.assertEqual(res["perspective"], "BENEFICIARY")
        self.assertEqual(res["risk_level"], "RED")
        codes = {f["clause_type"] for f in res["findings"]}
        self.assertIn("NON_OPERATIVE_OR_SUSPENDED_VALIDITY", codes)
        self.assertIn("CONDITIONAL_OR_SURETYSHIP_WORDING", codes)
        self.assertIn("APPLICANT_OBJECTION_OR_CONSENT_GATE", codes)


if __name__ == "__main__":
    unittest.main()
