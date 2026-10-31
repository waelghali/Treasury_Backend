# tests/test_smart_document_ai_roadmap.py
import asyncio
import datetime
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.document_verification_service import (
    DEFAULT_DATE_TOLERANCE_DAYS,
    DocumentVerificationService,
)


def _make_lg_record(**overrides):
    defaults = dict(
        id=101,
        lg_number="LG-2026-9001",
        amount=500000.0,
        lg_amount=500000.0,
        issuance_date=datetime.datetime(2026, 1, 1, 0, 0),
        expiry_date=datetime.datetime(2026, 10, 30, 0, 0),
        lg_period_months=10,
        beneficiary_corporate=SimpleNamespace(entity_name="Ministry of Transport"),
        issuing_bank=SimpleNamespace(name="Commercial International Bank", short_name="CIB"),
        lg_currency=SimpleNamespace(iso_code="EGP"),
        customer_id=1,
        instructions=[],
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_instruction(**overrides):
    defaults = dict(
        id=501,
        lg_record_id=101,
        serial_number="1001-0101-0001-01-EXT-0002",
        instruction_type="LG_EXTENSION",
        global_seq_per_lg=2,
        type_seq_per_lg=1,
        maker_user_id=10,
        instruction_date=datetime.datetime(2026, 7, 10, 0, 0),
        delivery_date=datetime.datetime(2026, 7, 15, 0, 0),
        bank_reply_date=None,
        status="Instruction Delivered",
        is_deleted=False,
        details={
            "old_expiry_date": "2026-07-30",
            "new_expiry_date": "2026-10-30",
        },
        lg_record=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ==============================================================================
# ITEM 1: Bank Expiry Date Tolerance Alignment (Calendar vs. 90-Day Convention)
# ==============================================================================

class TestBankExpiryDateToleranceAlignment(unittest.TestCase):
    """Tests for Item 1: DATE_CONVENTION_VARIANCE (<= 3 calendar days threshold)."""

    def test_exact_date_match_returns_verified(self):
        text = (
            "Commercial International Bank\n"
            "Extension Amendment Advice\n"
            "Ref: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: EGP 500,000.00\n"
            "We hereby confirm that the validity is extended until 2026-10-30."
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
            )

        self.assertEqual(result["status"], "verified")
        self.assertIsNone(result["date_convention_variance"])
        self.assertEqual(result["mismatches"], [])
        self.assertTrue(any("2026-10-30" in m for m in result["matches"]))

    def test_plus_two_days_within_tolerance_returns_date_variance(self):
        # Bank uses calendar convention (+2 days -> 2026-11-01)
        text = (
            "Commercial International Bank\n"
            "Amendment / Extension Confirmation\n"
            "Guarantee No: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00 EGP\n"
            "New Expiry Date: 2026-11-01\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
            )

        self.assertEqual(result["status"], "date_variance")
        self.assertEqual(result["mismatches"], [])
        variance = result["date_convention_variance"]
        self.assertIsNotNone(variance)
        self.assertEqual(variance["classification"], "DATE_CONVENTION_VARIANCE")
        self.assertEqual(variance["requested_expiry_date"], "2026-10-30")
        self.assertEqual(variance["bank_confirmed_expiry_date"], "2026-11-01")
        self.assertEqual(variance["variance_days"], 2)
        self.assertEqual(variance["tolerance_days"], DEFAULT_DATE_TOLERANCE_DAYS)
        expected_prompt = (
            "Bank confirmed expiry date as 2026-11-01 (+2 days from requested date 2026-10-30 "
            "due to bank calendar convention). Do you accept aligning your record to the bank's confirmed date?"
        )
        self.assertEqual(variance["prompt_message"], expected_prompt)

    def test_minus_three_days_at_tolerance_boundary_returns_date_variance(self):
        # Bank uses 90-day convention (-3 days -> 2026-10-27)
        text = (
            "Commercial International Bank\n"
            "Extension Advice\n"
            "LG Reference: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00\n"
            "Extended up to: 2026-10-27\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
            )

        self.assertEqual(result["status"], "date_variance")
        variance = result["date_convention_variance"]
        self.assertIsNotNone(variance)
        self.assertEqual(variance["classification"], "DATE_CONVENTION_VARIANCE")
        self.assertEqual(variance["variance_days"], -3)
        self.assertIn("-3 days from requested date 2026-10-30", variance["prompt_message"])

    def test_beyond_tolerance_returns_hard_mismatch(self):
        # +10 days difference (2026-11-09) exceeds <= 3 day threshold
        text = (
            "Commercial International Bank\n"
            "Extension Advice\n"
            "LG Reference: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00\n"
            "New Expiry Date: 2026-11-09\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
            )

        self.assertEqual(result["status"], "mismatch")
        self.assertIsNone(result["date_convention_variance"])
        self.assertTrue(any("2026-11-09" in m and "2026-10-30" in m for m in result["mismatches"]))


# ==============================================================================
# ITEM 2: Multi-Instruction Bank Reply Disambiguation (Preventing Misfiled Replies)
# ==============================================================================

class TestMultiInstructionBankReplyDisambiguation(unittest.TestCase):
    """Tests for Item 2: WRONG_INSTRUCTION_TARGET across pending sibling instructions."""

    def test_extension_advice_uploaded_to_release_instruction_disambiguates(self):
        # Instruction #1: Release (Target where user accidentally uploads)
        inst_release = {
            "id": 501,
            "global_seq_per_lg": 1,
            "serial_number": "1001-0101-0001-01-REL-0001",
            "instruction_type": "LG_RELEASE",
            "details": {},
        }
        # Instruction #2: Extension (Sibling that actually matches the uploaded document)
        inst_extension = {
            "id": 502,
            "global_seq_per_lg": 2,
            "serial_number": "1001-0101-0001-01-EXT-0002",
            "instruction_type": "LG_EXTENSION",
            "details": {"old_expiry_date": "2026-07-30", "new_expiry_date": "2026-10-30"},
        }

        text = (
            "Commercial International Bank\n"
            "Extension Amendment Advice\n"
            "Guarantee No: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00\n"
            "Validity extended until 2026-10-30\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_RELEASE",
                expected_new_expiry=None,
                expected_new_amount=None,
                lg_number="LG-2026-9001",
                target_instruction=inst_release,
                pending_instructions=[inst_release, inst_extension],
            )

        self.assertEqual(result["status"], "mismatch")
        disambig = result["instruction_disambiguation"]
        self.assertIsNotNone(disambig)
        self.assertEqual(disambig["classification"], "WRONG_INSTRUCTION_TARGET")
        self.assertEqual(disambig["target_instruction_id"], 501)
        self.assertEqual(disambig["target_seq"], 1)
        self.assertEqual(disambig["target_type_label"], "Release")
        self.assertEqual(disambig["matched_instruction_id"], 502)
        self.assertEqual(disambig["matched_seq"], 2)
        self.assertEqual(disambig["matched_type_label"], "Extension")
        expected_msg = (
            "Document Mismatch: You are uploading to Instruction #1 (Release), "
            "but this bank document appears to be an advice for Instruction #2 (Extension)."
        )
        self.assertEqual(disambig["prompt_message"], expected_msg)
        self.assertEqual(disambig["action_label"], "Switch & Record to Instruction #2")
        self.assertEqual(result["mismatches"][0], expected_msg)

    def test_release_notice_uploaded_to_extension_instruction_disambiguates(self):
        inst_release = {
            "id": 501,
            "global_seq_per_lg": 1,
            "serial_number": "1001-0101-0001-01-REL-0001",
            "instruction_type": "LG_RELEASE",
            "details": {},
        }
        inst_extension = {
            "id": 502,
            "global_seq_per_lg": 2,
            "serial_number": "1001-0101-0001-01-EXT-0002",
            "instruction_type": "LG_EXTENSION",
            "details": {"new_expiry_date": "2026-10-30"},
        }

        text = (
            "Commercial International Bank\n"
            "Cancellation and Discharge Confirmation\n"
            "Guarantee No: LG-2026-9001\n"
            "ختم الفرع\n"
            "We confirm that the above guarantee has been released and cancelled in full."
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
                target_instruction=inst_extension,
                pending_instructions=[inst_release, inst_extension],
            )

        self.assertEqual(result["status"], "mismatch")
        disambig = result["instruction_disambiguation"]
        self.assertIsNotNone(disambig)
        self.assertEqual(disambig["target_instruction_id"], 502)
        self.assertEqual(disambig["matched_instruction_id"], 501)
        self.assertEqual(disambig["action_label"], "Switch & Record to Instruction #1")
        self.assertEqual(
            disambig["prompt_message"],
            "Document Mismatch: You are uploading to Instruction #2 (Extension), "
            "but this bank document appears to be an advice for Instruction #1 (Release).",
        )

    def test_sequential_extensions_disambiguated_by_expiry_date_and_serial(self):
        # Instruction #1: Earlier extension to 2026-10-30
        inst_ext_1 = {
            "id": 501,
            "global_seq_per_lg": 1,
            "serial_number": "1001-0101-0001-01-EXT-0001",
            "instruction_type": "LG_EXTENSION",
            "details": {"old_expiry_date": "2026-07-30", "new_expiry_date": "2026-10-30"},
        }
        # Instruction #2: Newer extension to 2027-01-30
        inst_ext_2 = {
            "id": 502,
            "global_seq_per_lg": 2,
            "serial_number": "1001-0101-0001-01-EXT-0002",
            "instruction_type": "LG_EXTENSION",
            "details": {"old_expiry_date": "2026-10-30", "new_expiry_date": "2027-01-30"},
        }

        # User uploads bank advice for Instruction #1 (2026-10-30) onto Instruction #2
        text = (
            "Commercial International Bank\n"
            "Extension Advice for Instruction 1001-0101-0001-01-EXT-0001\n"
            "Guarantee No: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00\n"
            "Extended until: 2026-10-30\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            result = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2027-01-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
                target_instruction=inst_ext_2,
                pending_instructions=[inst_ext_1, inst_ext_2],
            )

        self.assertEqual(result["status"], "mismatch")
        disambig = result["instruction_disambiguation"]
        self.assertIsNotNone(disambig)
        self.assertEqual(disambig["matched_instruction_id"], 501)
        self.assertEqual(disambig["matched_seq"], 1)
        self.assertEqual(disambig["action_label"], "Switch & Record to Instruction #1")


# ==============================================================================
# ENDPOINT INTEGRATION TESTS: record_bank_reply_api
# ==============================================================================

class TestRecordBankReplyEndpointIntegration(unittest.TestCase):
    """Tests the end_user.record_bank_reply_api endpoint flows for Item 1 and Item 2."""

    def test_record_bank_reply_api_returns_date_variance_and_then_aligns_expiry(self):
        from app.api.v1.endpoints.end_user import record_bank_reply_api

        lg_record = _make_lg_record(
            id=101,
            expiry_date=datetime.datetime(2026, 10, 30, 0, 0),
            issuance_date=datetime.datetime(2026, 1, 1, 0, 0),
            lg_period_months=3,
        )
        instruction = _make_instruction(
            id=501,
            lg_record_id=101,
            lg_record=lg_record,
            details={"old_expiry_date": "2026-07-30", "new_expiry_date": "2026-10-30"},
        )
        lg_record.instructions = [instruction]

        mock_db = MagicMock()
        mock_db.query.return_value.options.return_value.filter.return_value.first.return_value = instruction
        end_user_context = SimpleNamespace(user_id=10, customer_id=1, role="END_USER")

        upload_file = MagicMock()
        upload_file.read = AsyncMock(return_value=b"%PDF-1.4 test")
        upload_file.content_type = "application/pdf"
        upload_file.filename = "bank_reply.pdf"

        variance_payload = {
            "status": "date_variance",
            "document_type": "bank_reply",
            "matches": ["LG Number 'LG-2026-9001' verified on bank reply."],
            "mismatches": [],
            "warnings": [],
            "date_convention_variance": {
                "classification": "DATE_CONVENTION_VARIANCE",
                "requested_expiry_date": "2026-10-30",
                "bank_confirmed_expiry_date": "2026-11-01",
                "variance_days": 2,
                "tolerance_days": 3,
                "prompt_message": (
                    "Bank confirmed expiry date as 2026-11-01 (+2 days from requested date 2026-10-30 "
                    "due to bank calendar convention). Do you accept aligning your record to the bank's confirmed date?"
                ),
            },
            "instruction_disambiguation": None,
        }

        # Step 1: Initial upload returns 200 JSONResponse with status="date_variance"
        with patch(
            "app.api.v1.endpoints.end_user.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value={"effective_value": "False"},
        ), patch(
            "app.api.v1.endpoints.end_user.document_verification_service.extract_text",
            return_value="Sample bank reply text",
        ), patch(
            "app.api.v1.endpoints.end_user.document_verification_service.verify_bank_reply",
            return_value=variance_payload,
        ):
            response = asyncio.run(
                record_bank_reply_api(
                    instruction_id=501,
                    bank_reply_date=datetime.date(2026, 7, 20),
                    reply_details="Bank confirmed extension",
                    bank_reply_document_file=upload_file,
                    bank_reply_document_metadata=json.dumps({"file_name": "bank_reply.pdf"}),
                    force_override=False,
                    override_reason=None,
                    ai_verification_json=None,
                    accept_date_alignment=False,
                    aligned_expiry_date=None,
                    switched_from_instruction_id=None,
                    request_admin_terms_approval=False,
                    db=mock_db,
                    end_user_context=end_user_context,
                    request=None,
                )
            )

        body = json.loads(response.body.decode("utf-8"))
        self.assertEqual(body["status"], "date_variance")
        self.assertEqual(
            body["ai_verification"]["date_convention_variance"]["bank_confirmed_expiry_date"],
            "2026-11-01",
        )

        # Step 2: User accepts alignment -> updates lg_record.expiry_date and logs audit event
        with patch(
            "app.api.v1.endpoints.end_user.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value={"effective_value": "False"},
        ), patch(
            "app.api.v1.endpoints.end_user.crud_lg_instruction.record_bank_reply",
            new=AsyncMock(return_value=instruction),
        ), patch(
            "app.api.v1.endpoints.end_user.LGInstructionOut.model_validate",
            side_effect=lambda x: x,
        ), patch("app.api.v1.endpoints.end_user.log_action") as mock_log_action:
            asyncio.run(
                record_bank_reply_api(
                    instruction_id=501,
                    bank_reply_date=datetime.date(2026, 7, 20),
                    reply_details="Bank confirmed extension",
                    bank_reply_document_file=upload_file,
                    bank_reply_document_metadata=json.dumps({"file_name": "bank_reply.pdf"}),
                    force_override=False,
                    override_reason=None,
                    ai_verification_json=json.dumps(variance_payload),
                    accept_date_alignment=True,
                    aligned_expiry_date=datetime.date(2026, 11, 1),
                    switched_from_instruction_id=None,
                    request_admin_terms_approval=False,
                    db=mock_db,
                    end_user_context=end_user_context,
                    request=None,
                )
            )

        self.assertEqual(lg_record.expiry_date, datetime.datetime(2026, 11, 1, 0, 0))
        self.assertEqual(lg_record.lg_period_months, 3)  # Initial period months must remain unchanged
        self.assertEqual(instruction.details["new_expiry_date"], "2026-11-01")
        self.assertEqual(
            instruction.details["date_convention_alignment"]["bank_confirmed_expiry_date"], "2026-11-01"
        )
        self.assertEqual(
            instruction.details["date_convention_alignment"]["aligned_expiry_date"], "2026-11-01"
        )
        self.assertEqual(
            instruction.details["date_convention_alignment"]["requested_expiry_date"], "2026-10-30"
        )
        self.assertEqual(instruction.details["bank_reply_ai_verification"]["status"], "verified")

        # Verify AuditLog was written with LG_EXPIRY_ALIGNED_TO_BANK_CONVENTION
        mock_log_action.assert_called_once()
        call_kwargs = mock_log_action.call_args.kwargs
        self.assertEqual(call_kwargs["action_type"], "LG_EXPIRY_ALIGNED_TO_BANK_CONVENTION")
        self.assertEqual(call_kwargs["lg_record_id"], 101)
        self.assertEqual(call_kwargs["details"]["bank_confirmed_expiry_date"], "2026-11-01")

    def test_record_bank_reply_api_logs_ai_redirection_when_switched(self):
        from app.api.v1.endpoints.end_user import record_bank_reply_api

        lg_record = _make_lg_record(id=101)
        matched_instruction = _make_instruction(
            id=502,
            lg_record_id=101,
            lg_record=lg_record,
            serial_number="1001-0101-0001-01-EXT-0002",
            instruction_type="LG_EXTENSION",
            details={"new_expiry_date": "2026-10-30"},
        )
        lg_record.instructions = [matched_instruction]

        mock_db = MagicMock()
        mock_db.query.return_value.options.return_value.filter.return_value.first.return_value = matched_instruction
        end_user_context = SimpleNamespace(user_id=10, customer_id=1, role="END_USER")

        upload_file = MagicMock()
        upload_file.read = AsyncMock(return_value=b"%PDF-1.4 test")
        upload_file.content_type = "application/pdf"
        upload_file.filename = "extension_advice.pdf"

        verified_payload = {
            "status": "verified",
            "document_type": "bank_reply",
            "matches": ["LG Number 'LG-2026-9001' verified on bank reply."],
            "mismatches": [],
            "warnings": [],
            "date_convention_variance": None,
            "instruction_disambiguation": None,
        }

        with patch(
            "app.api.v1.endpoints.end_user.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value={"effective_value": "False"},
        ), patch(
            "app.api.v1.endpoints.end_user.document_verification_service.extract_text",
            return_value="Sample extension advice text",
        ), patch(
            "app.api.v1.endpoints.end_user.document_verification_service.verify_bank_reply",
            return_value=verified_payload,
        ), patch(
            "app.api.v1.endpoints.end_user.crud_lg_instruction.record_bank_reply",
            new=AsyncMock(return_value=matched_instruction),
        ), patch(
            "app.api.v1.endpoints.end_user.LGInstructionOut.model_validate",
            side_effect=lambda x: x,
        ), patch("app.api.v1.endpoints.end_user.log_action") as mock_log_action:
            asyncio.run(
                record_bank_reply_api(
                    instruction_id=502,
                    bank_reply_date=datetime.date(2026, 7, 20),
                    reply_details="Redirected bank reply",
                    bank_reply_document_file=upload_file,
                    bank_reply_document_metadata=json.dumps({"file_name": "extension_advice.pdf"}),
                    force_override=False,
                    override_reason=None,
                    ai_verification_json=None,
                    accept_date_alignment=False,
                    aligned_expiry_date=None,
                    switched_from_instruction_id=501,
                    request_admin_terms_approval=False,
                    db=mock_db,
                    end_user_context=end_user_context,
                    request=None,
                )
            )

        self.assertEqual(
            matched_instruction.details["disambiguation_redirect"]["switched_from_instruction_id"],
            501,
        )
        self.assertEqual(
            matched_instruction.details["disambiguation_redirect"]["matched_instruction_id"],
            502,
        )

        mock_log_action.assert_called_once()
        call_kwargs = mock_log_action.call_args.kwargs
        self.assertEqual(call_kwargs["action_type"], "LG_BANK_REPLY_REDIRECTED_BY_AI")
        self.assertEqual(call_kwargs["entity_id"], 502)
        self.assertEqual(call_kwargs["details"]["switched_from_instruction_id"], 501)


# ==============================================================================
# CUSTODY AUDIT FIXES & SMART ENHANCEMENTS TESTS
# ==============================================================================

class TestCustodyAuditFixesAndEnhancements(unittest.TestCase):
    """Tests for Custody Audit Fixes (#1-#7) and Enhancements (A, B, C)."""

    def test_fix2_rollback_lg_state_handles_lg_liquidate_and_case_insensitive_type(self):
        from app.crud.crud_lg_cancellation import CRUDLGCancellation
        from app.models import LgStatusEnum
        from app.constants import ACTION_TYPE_LG_LIQUIDATE

        crud_cancel = CRUDLGCancellation(MagicMock(), MagicMock(), MagicMock(), MagicMock())
        mock_db = MagicMock()

        # Case 1: Full liquidation with action_type="LG_LIQUIDATE" and liquidation_type="full"
        lg_full = _make_lg_record(lg_status_id=LgStatusEnum.LIQUIDATED.value, lg_amount=0.0)
        inst_full = _make_instruction(
            instruction_type=ACTION_TYPE_LG_LIQUIDATE,
            details={"liquidation_type": "full", "old_amount": 500000.0},
        )
        with patch("app.crud.crud_lg_cancellation.log_action"):
            asyncio.run(crud_cancel._rollback_lg_state(mock_db, lg_full, inst_full))
        self.assertEqual(lg_full.lg_status_id, LgStatusEnum.VALID.value)

        # Case 2: Partial liquidation with action_type="LG_LIQUIDATE" and liquidation_type="partial"
        lg_partial = _make_lg_record(lg_status_id=LgStatusEnum.VALID.value, lg_amount=300000.0)
        inst_partial = _make_instruction(
            instruction_type="LG_LIQUIDATE",
            details={"liquidation_type": "partial", "old_amount": 500000.0},
        )
        with patch("app.crud.crud_lg_cancellation.log_action"):
            asyncio.run(crud_cancel._rollback_lg_state(mock_db, lg_partial, inst_partial))
        self.assertEqual(float(lg_partial.lg_amount), 500000.0)

    def test_fix5_alternate_bank_names_matched_for_foreign_or_advising_bank(self):
        text = (
            "HSBC Bank Egypt\n"
            "Advising Bank Confirmation\n"
            "Guarantee Ref: LG-2026-9001\n"
            "خاتم وارد\n"
            "Date: 2026-07-15\n"
            "Serial: 1001-0101-0001-01-EXT-0002\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_delivery_prompt", return_value=None
        ):
            res = DocumentVerificationService.verify_delivery_evidence(
                raw_text=text,
                instruction_serial="1001-0101-0001-01-EXT-0002",
                expected_bank_name="Foreign Bank",
                expected_bank_short_name="FB",
                alternate_bank_names=["Deutsche Bank AG", "HSBC Bank Egypt", "HSBC"],
                lg_number="LG-2026-9001",
                instruction_issue_date=datetime.date(2026, 7, 10),
                user_delivery_date="2026-07-15",
            )
        self.assertEqual(res["status"], "verified")
        self.assertTrue(any("HSBC" in m for m in res["matches"]))

    def test_enhancement_a_bank_terms_modification_populates_and_creates_approval_request(self):
        from app.api.v1.endpoints.end_user import record_bank_reply_api

        # 1. Verify AI service populates bank_terms_modification when expiry variance > 3 days
        text = (
            "Commercial International Bank\n"
            "Extension Confirmation\n"
            "Guarantee No: LG-2026-9001\n"
            "ختم الفرع\n"
            "Amount: 500,000.00\n"
            "New Expiry Date: 2026-11-15\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            res = DocumentVerificationService.verify_bank_reply(
                raw_text=text,
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
            )

        self.assertEqual(res["status"], "mismatch")
        mod = res["bank_terms_modification"]
        self.assertIsNotNone(mod)
        self.assertEqual(mod["classification"], "BANK_MODIFIED_TERMS_APPROVAL_REQUIRED")
        self.assertEqual(mod["bank_confirmed_expiry_date"], "2026-11-15")
        self.assertEqual(mod["amendment_details"]["expiry_date"], "2026-11-15")

        # 2. Verify record_bank_reply_api with request_admin_terms_approval=True creates ApprovalRequest
        lg_record = _make_lg_record(id=101, expiry_date=datetime.datetime(2026, 7, 30, 0, 0))
        instruction = _make_instruction(
            id=501,
            lg_record_id=101,
            lg_record=lg_record,
            details={"old_expiry_date": "2026-07-30", "new_expiry_date": "2026-10-30"},
        )
        lg_record.instructions = [instruction]

        mock_db = MagicMock()
        mock_db.query.return_value.options.return_value.filter.return_value.first.return_value = instruction
        end_user_context = SimpleNamespace(
            user_id=10, customer_id=1, role="END_USER", email="maker@corp.com"
        )

        upload_file = MagicMock()
        upload_file.read = AsyncMock(return_value=b"%PDF-1.4 test")
        upload_file.content_type = "application/pdf"
        upload_file.filename = "bank_reply.pdf"

        fake_approval_req = SimpleNamespace(
            id=999,
            status=SimpleNamespace(value="PENDING"),
        )

        with patch(
            "app.api.v1.endpoints.end_user.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value={"effective_value": "False"},
        ), patch(
            "app.api.v1.endpoints.end_user.crud_lg_instruction.record_bank_reply",
            new=AsyncMock(return_value=instruction),
        ), patch(
            "app.api.v1.endpoints.end_user.crud_approval_request.create_approval_request",
            new=AsyncMock(return_value=fake_approval_req),
        ), patch(
            "app.api.v1.endpoints.end_user.LGInstructionOut.model_validate",
            side_effect=lambda x: x,
        ), patch("app.api.v1.endpoints.end_user.log_action") as mock_log_action:
            asyncio.run(
                record_bank_reply_api(
                    instruction_id=501,
                    bank_reply_date=datetime.date(2026, 7, 20),
                    reply_details="Bank extended to Nov 15 instead of Oct 30",
                    bank_reply_document_file=upload_file,
                    bank_reply_document_metadata=json.dumps({"file_name": "bank_reply.pdf"}),
                    force_override=False,
                    override_reason="Submitting bank-modified terms for Corporate Admin approval",
                    ai_verification_json=json.dumps(res),
                    accept_date_alignment=False,
                    aligned_expiry_date=None,
                    switched_from_instruction_id=None,
                    request_admin_terms_approval=True,
                    db=mock_db,
                    end_user_context=end_user_context,
                    request=None,
                )
            )

        # Ensure LG expiry was NOT directly altered by the end user
        self.assertEqual(lg_record.expiry_date, datetime.datetime(2026, 7, 30, 0, 0))
        # Ensure ApprovalRequest was created and linked on instruction.details
        self.assertEqual(
            instruction.details["bank_terms_admin_approval"]["approval_request_id"],
            999,
        )
        self.assertEqual(
            instruction.details["bank_terms_admin_approval"]["amendment_details"]["expiry_date"],
            "2026-11-15",
        )

    def test_enhancement_b_delivery_stamp_date_alignment_and_bank_reply_letter_date(self):
        # 1. Delivery stamp date differs from user-entered delivery date -> delivery_date_variance
        delivery_text = (
            "Commercial International Bank\n"
            "خاتم وارد\n"
            "Date: 2026-07-16\n"
            "Guarantee Ref: LG-2026-9001\n"
            "Serial: 1001-0101-0001-01-EXT-0002\n"
        )
        with patch.object(
            DocumentVerificationService, "_call_gemini_delivery_prompt", return_value=None
        ):
            deliv_res = DocumentVerificationService.verify_delivery_evidence(
                raw_text=delivery_text,
                instruction_serial="1001-0101-0001-01-EXT-0002",
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                lg_number="LG-2026-9001",
                instruction_issue_date=datetime.date(2026, 7, 10),
                user_delivery_date="2026-07-18",
            )
        self.assertEqual(deliv_res["status"], "mismatch")
        self.assertIsNotNone(deliv_res["delivery_date_variance"])
        self.assertEqual(
            deliv_res["delivery_date_variance"]["detected_stamp_date"], "2026-07-16"
        )
        self.assertEqual(
            deliv_res["delivery_date_variance"]["user_delivery_date"], "2026-07-18"
        )

        # 2. Bank reply letter date is earlier than received date (normal!) -> verified + letter date captured
        ai_mock = {
            "detected_bank_name": "Commercial International Bank",
            "is_bank_seal_detected": True,
            "detected_lg_numbers": ["LG-2026-9001"],
            "detected_reply_purpose": "EXTENSION",
            "amended_expiry_date": "2026-10-30",
            "amended_amount": 500000.0,
            "cited_effective_date": "2026-07-12",
            "summary": "Bank confirmed extension on 2026-07-12",
        }
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=ai_mock
        ):
            reply_res = DocumentVerificationService.verify_bank_reply(
                raw_text="Sample text",
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
                user_reply_date="2026-07-22",  # Received 10 days after bank letter date
            )
        self.assertEqual(reply_res["status"], "verified")
        self.assertEqual(reply_res["bank_reply_letter_date"], "2026-07-12")

    def test_enhancement_c_duplicate_scan_sha256_fingerprint_guard(self):
        existing_hashes = [
            {
                "file_sha256": "abc123deadbeef",
                "file_name": "delivery_proof_ext1.pdf",
                "document_type": "DELIVERY_PROOF",
                "instruction_id": 501,
            }
        ]
        with patch.object(
            DocumentVerificationService, "_call_gemini_bank_reply_prompt", return_value=None
        ):
            res = DocumentVerificationService.verify_bank_reply(
                raw_text=(
                    "Commercial International Bank\n"
                    "Ref: LG-2026-9001\n"
                    "ختم الفرع\n"
                    "New Expiry Date: 2026-10-30\n"
                ),
                expected_bank_name="Commercial International Bank",
                expected_bank_short_name="CIB",
                action_type="LG_EXTENSION",
                expected_new_expiry="2026-10-30",
                expected_new_amount=None,
                lg_number="LG-2026-9001",
                file_sha256="abc123deadbeef",
                existing_doc_hashes=existing_hashes,
            )
        self.assertEqual(res["status"], "mismatch")
        self.assertIsNotNone(res["duplicate_scan_detected"])
        self.assertTrue(any("Duplicate Scan Detected" in m for m in res["mismatches"]))


if __name__ == "__main__":
    unittest.main()
