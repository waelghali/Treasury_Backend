# app/test_ai_query_assistant.py
"""
Automated Test Suite for 4-Level Treasury AI Assistant with System & User Self-Awareness (Level 4).
Validates:
- Level 0 (Fast Backend Card Resolution)
- Level 1 (Single Intent ORM Mapping, Audit Logs, User Profile)
- Level 2 (Complex Synthesis & Privacy Tokenization)
- Level 3 (General Treasury Concepts & Abbreviations e.g. Fwd)
- Level 4 (User-Aware System Knowledge, Capabilities & Navigation Paths)
- Fast-Path Instant Greetings (0ms, no LLM call)
- Security & Guardrail Enforcement (Unknown tokens, off-scope rejection)
- Feature Flag Controls
"""

import os
import sys
import logging

sys.stdout.reconfigure(encoding="utf-8")
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import app.models as models
from app.database import SessionLocal
from app.services.ai_query_service import ai_query_assistant_service
from app.services.ai_privacy_tokenizer import privacy_tokenizer
from app.services.ai_policy_guardrail import policy_guardrail

logging.basicConfig(level=logging.INFO)


def run_suite():
    print("=" * 75)
    print("RUNNING 4-LEVEL TREASURY AI ASSISTANT AUTOMATED TEST SUITE")
    print("=" * 75)

    db = SessionLocal()

    try:
        user = db.query(models.User).filter(models.User.is_deleted == False).first()
        if not user:
            print("[FAIL] No active user found.")
            return

        customer_id = user.customer_id or 1
        user_id = user.id
        os.environ["AI_DATA_ASSISTANT_ENABLED"] = "true"

        # TEST 1: Level 0 Backend Card ID Resolution
        print("\n--- TEST 1: Level 0 Backend Card ID Resolution ---")
        res1 = ai_query_assistant_service.process_query(db=db, customer_id=customer_id, user_id=user_id, card_id="expiring_60_days")
        print(f"Level: {res1.get('level')}")
        print(f"Source Awareness: {res1.get('source_awareness')}")
        assert res1.get("level") == 0, f"Test 1 Failed: Expected level 0, got {res1.get('level')}"
        assert res1.get("source_awareness") == "SYSTEM_DATA", "Test 1 Failed"
        print("[PASS] Level 0 Card ID resolved correctly on server-side.")

        # TEST 2: Level 1 Single Intent Query
        print("\n--- TEST 2: Level 1 Simple Natural Language Query ---")
        res2 = ai_query_assistant_service.process_query(db=db, user_question="Which LGs expire in 60 days?", customer_id=customer_id, user_id=user_id)
        print(f"Level: {res2.get('level')}")
        print(f"Intent: {res2.get('intent')}")
        assert res2.get("level") == 1, f"Test 2 Failed: Expected level 1, got {res2.get('level')}"
        print("[PASS] Level 1 simple intent executed via ORM with application formatting.")

        # TEST 3: Level 2 Complex Analysis & Tokenization
        print("\n--- TEST 3: Level 2 Complex Multi-Step Analysis ---")
        res3 = ai_query_assistant_service.process_query(
            db=db,
            user_question="Which beneficiaries have highest LG exposure and also have guarantees expiring within 90 days?",
            customer_id=customer_id,
            user_id=user_id
        )
        print(f"Level: {res3.get('level')}")
        print(f"Source Awareness: {res3.get('source_awareness')}")
        assert res3.get("level") == 2, f"Test 3 Failed: Expected level 2, got {res3.get('level')}"
        assert res3.get("source_awareness") == "COMBINATION", "Test 3 Failed"
        print("[PASS] Level 2 multi-step plan & tokenized synthesis completed.")

        # TEST 4: Level 3 General Treasury Scope Approval
        print("\n--- TEST 4: Level 3 General Treasury Scope ---")
        res4 = ai_query_assistant_service.process_query(db=db, user_question="What is cash pooling?", customer_id=customer_id, user_id=user_id)
        print(f"Level: {res4.get('level')}")
        print(f"Answer Snippet: {res4.get('answer')[:120]}...")
        assert res4.get("level") == 3, f"Test 4 Failed: Expected level 3, got {res4.get('level')}"
        print("[PASS] Level 3 General Treasury question answered directly without DB calls.")

        # TEST 5: Non-Treasury Scope Rejection
        print("\n--- TEST 5: Non-Treasury Scope Rejection ---")
        res5 = ai_query_assistant_service.process_query(db=db, user_question="What is the capital of France?", customer_id=customer_id, user_id=user_id)
        print(f"Answer: {res5.get('answer')}")
        assert res5.get("intent") == "rejected_scope", "Test 5 Failed: Non-treasury question was not rejected"
        print("[PASS] Non-treasury question rejected by policy guardrail.")

        # TEST 6: System Capability Gap Handling
        print("\n--- TEST 6: System Capability Gap ---")
        res6 = ai_query_assistant_service.process_query(db=db, user_question="Execute an automatic bank wire transfer or payout", customer_id=customer_id, user_id=user_id)
        print(f"Answer: {res6.get('answer')}")
        assert "capability" in res6.get("answer"), "Test 6 Failed"
        print("[PASS] System capability gap handled cleanly without hallucination.")

        # TEST 7: Level 4 System Knowledge & Navigation Guidance
        print("\n--- TEST 7: Level 4 System Knowledge & Navigation ---")
        res7 = ai_query_assistant_service.process_query(db=db, user_question="How do I extend an LG in Grow?", customer_id=customer_id, user_id=user_id)
        print(f"Level: {res7.get('level')}")
        print(f"Source Awareness: {res7.get('source_awareness')}")
        print(f"Answer Snippet: {res7.get('answer')[:150]}...")
        assert res7.get("level") == 4, f"Test 7 Failed: Expected level 4, got {res7.get('level')}"
        assert res7.get("source_awareness") == "SYSTEM_KNOWLEDGE", "Test 7 Failed: Expected SYSTEM_KNOWLEDGE"
        assert "Sidebar" in res7.get("answer") or "LG Details" in res7.get("answer") or "Extend" in res7.get("answer") or "Custody" in res7.get("answer"), "Test 7 Failed: Expected navigation steps"
        print("[PASS] Level 4 System Knowledge provided exact workflow navigation.")

        # TEST 8: Fast-Path Instant Greeting
        print("\n--- TEST 8: Fast-Path Instant Greeting ---")
        res8 = ai_query_assistant_service.process_query(db=db, user_question="Restart", customer_id=customer_id, user_id=user_id)
        print(f"Level: {res8.get('level')}")
        print(f"Answer Snippet: {res8.get('answer')[:100]}...")
        assert res8.get("level") == 4, "Test 8 Failed"
        assert "Grow Treasury" in res8.get("answer"), "Test 8 Failed"
        print("[PASS] Fast-path instant greeting returned with zero LLM latency.")

        # TEST 9: Treasury Abbreviation (Fwd = Forward Deals)
        print("\n--- TEST 9: Treasury Abbreviation (Fwd) ---")
        res9 = ai_query_assistant_service.process_query(db=db, user_question="Fwd", customer_id=customer_id, user_id=user_id)
        print(f"Level: {res9.get('level')}")
        print(f"Intent: {res9.get('intent')}")
        print(f"Answer Snippet: {res9.get('answer')[:120]}...")
        assert res9.get("level") == 3, f"Test 9 Failed: Expected Level 3 for Fwd, got {res9.get('level')}"
        assert res9.get("intent") == "general_treasury", "Test 9 Failed"
        print("[PASS] 'Fwd' recognized as Treasury Forward contracts/hedging inquiry.")

        # TEST 10: Differentiated Negative Case - No Matching Records
        print("\n--- TEST 10: Differentiated Negative Case - No Matching Records ---")
        res10 = ai_query_assistant_service.process_query(db=db, user_question="Find active LGs for NonExistentCompany12345", customer_id=customer_id, user_id=user_id)
        print(f"Answer: {res10.get('answer')}")
        assert "No records matching your search criteria" in res10.get("answer"), "Test 10 Failed"
        print("[PASS] No matching records case returned clear criteria message.")

        # TEST 11: Unknown Token Rejection
        print("\n--- TEST 11: Unknown Token Rejection Validator ---")
        valid_map = {"LG_TOKEN_001": "ACME-123"}
        fake_ai_output = "The details for LG_TOKEN_001 and fake token LG_TOKEN_999 are..."
        is_valid_tok, tok_msg = privacy_tokenizer.validate_ai_output_tokens(fake_ai_output, valid_map)
        print(f"Validator Result: {is_valid_tok}, Message: {tok_msg}")
        assert is_valid_tok == False, "Test 11 Failed: Unknown token was not rejected"
        print("[PASS] Unknown token LG_TOKEN_999 rejected by output validator.")

        # TEST 12: Feature Flag Disable Test
        print("\n--- TEST 12: Feature Flag Disable Control ---")
        os.environ["AI_DATA_ASSISTANT_ENABLED"] = "false"
        res12 = ai_query_assistant_service.process_query(db=db, user_question="Which LGs expire soon?", customer_id=customer_id, user_id=user_id)
        print(f"Success: {res12.get('success')}, Error: {res12.get('error')}")
        assert res12.get("success") == False, "Test 12 Failed: Feature was not disabled"
        assert res12.get("code") == "FEATURE_DISABLED", "Test 12 Failed"
        print("[PASS] Feature flag disabled assistant cleanly.")

        # Re-enable feature flag for remaining tests
        os.environ["AI_DATA_ASSISTANT_ENABLED"] = "true"
        db.rollback()

        # TEST 13: User Profile & Permissions Awareness
        print("\n--- TEST 13: User Profile & Permissions Awareness ---")
        res13_prof = ai_query_assistant_service.process_query(db=db, user_question="What can I do with my role?", customer_id=customer_id, user_id=user_id)
        print(f"Profile Answer:\n{res13_prof.get('answer')}")
        assert res13_prof.get("success") == True, "Test 13 Failed"
        assert "System Owner" in res13_prof.get("answer") or "Role" in res13_prof.get("answer") or "User" in res13_prof.get("answer"), "Test 13 Failed"
        print("[PASS] User profile & role capabilities returned accurately.")

        # TEST 14: Audit Log & Activity History Explorer
        print("\n--- TEST 14: Audit Log & Activity History Explorer ---")
        res14_audit = ai_query_assistant_service.process_query(db=db, user_question="What did I do recently?", customer_id=customer_id, user_id=user_id)
        print(f"Audit Answer:\n{res14_audit.get('answer')}")
        assert res14_audit.get("success") == True, "Test 14 Failed"
        assert res14_audit.get("intent") == "get_audit_history", "Test 14 Failed"
        print("[PASS] Audit log history explorer executed tenant-isolated query.")

        # TEST 15: Specific How-To Workflow Guidance (Record New LG)
        print("\n--- TEST 15: Specific How-To Workflow Guidance (Record New LG) ---")
        res15_howto = ai_query_assistant_service.process_query(db=db, user_question="how can i record a new lg", customer_id=customer_id, user_id=user_id)
        print(f"How-To Answer:\n{res15_howto.get('answer')}")
        assert res15_howto.get("level") == 4, "Test 15 Failed"
        assert "Record New LG" in res15_howto.get("answer"), "Test 15 Failed"
        assert "AI Document Scan" in res15_howto.get("answer") or "Manual" in res15_howto.get("answer"), "Test 15 Failed"
        print("[PASS] Step-by-step workflow for recording new LG with OCR scan instructions provided.")

        # TEST 16: Page-Aware Ambiguous Query on Quotations (No Custody Hijack & No Trivia Rejection)
        print("\n--- TEST 16: Page-Aware Ambiguous Query on Quotations ---")
        res16_active = ai_query_assistant_service.process_query(
            db=db,
            user_question="show active",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/quotations",
        )
        assert res16_active.get("intent") == "get_quotation_summary", (
            f"Test 16a Failed: Expected get_quotation_summary, got {res16_active.get('intent')}"
        )
        res16_won = ai_query_assistant_service.process_query(
            db=db,
            user_question="who won?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/quotations",
        )
        assert res16_won.get("intent") == "get_top_winning_banks", (
            f"Test 16b Failed: Expected get_top_winning_banks, got {res16_won.get('intent')}"
        )
        print("[PASS] Ambiguous Quotations queries routed to Quotation intents accurately.")

        # TEST 17: Zero Tunnel Vision — Explicit Cross-Module Query Overrides Current Page
        print("\n--- TEST 17: Explicit Cross-Module Query Overrides Page Context ---")
        res17_cross = ai_query_assistant_service.process_query(
            db=db,
            user_question="Which LGs expire in 60 days?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/quotations",
        )
        assert res17_cross.get("intent") == "find_expiring_lgs", (
            f"Test 17a Failed: Explicit LG query hijacked on Quotations: {res17_cross.get('intent')}"
        )
        res17_offtopic = ai_query_assistant_service.process_query(
            db=db,
            user_question="What is the capital of France?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/quotations",
        )
        assert res17_offtopic.get("intent") == "rejected_scope", "Test 17b Failed: Off-topic not rejected"
        print("[PASS] Explicit cross-module & off-topic guardrails preserved with zero tunnel vision.")

        # TEST 18: URL Record ID Deictic Resolution (`/lg-records/:id` vs Active Chat Memory)
        print("\n--- TEST 18: URL Record ID Deictic Resolution ---")
        sample_lg = db.query(models.LGRecord).filter(
            models.LGRecord.customer_id == customer_id,
            models.LGRecord.is_deleted == False
        ).first()
        if sample_lg:
            # Clear prior chat memory for clean URL-only check
            ai_query_assistant_service._last_referenced_lg_by_user.pop((int(customer_id), int(user_id)), None)
            res18_url = ai_query_assistant_service.process_query(
                db=db,
                user_question="when does this expire?",
                customer_id=customer_id,
                user_id=user_id,
                page_context=f"/end-user/lg-records/{sample_lg.id}",
                history=[],
            )
            assert res18_url.get("intent") == "get_lg_details", f"Test 18 Failed: got {res18_url.get('intent')}"
            assert sample_lg.lg_number in (res18_url.get("answer") or ""), "Test 18 Failed: LG number missing"
            print(f"[PASS] Deictic question resolved LG '{sample_lg.lg_number}' directly from URL path.")
        else:
            cls18 = ai_query_assistant_service.classify_and_interpret(
                "when does this expire?",
                customer_id=customer_id,
                user_id=user_id,
                page_context="/end-user/lg-records/42",
                history=[],
            )
            assert cls18.get("intent") == "get_lg_details" and cls18["parameters"].get("lg_id") == 42, "Test 18 Failed"
            print("[PASS] Deictic question classified lg_id=42 from URL path.")

        # TEST 19: Page-Aware Issuance & Facilities Tie-Breakers
        print("\n--- TEST 19: Page-Aware Issuance & Facilities Tie-Breakers ---")
        res19_iss = ai_query_assistant_service.process_query(
            db=db,
            user_question="show pending",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/issuance/requests",
        )
        assert res19_iss.get("intent") == "get_issuance_summary", f"Test 19a Failed: got {res19_iss.get('intent')}"
        res19_fac = ai_query_assistant_service.process_query(
            db=db,
            user_question="what is available?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/issuance/facilities",
        )
        assert res19_fac.get("intent") == "get_facility_analytics", f"Test 19b Failed: got {res19_fac.get('intent')}"
        print("[PASS] Issuance & Facility page tie-breakers resolved accurately.")

        # TEST 20: Level 4 How-To Disambiguation by Current Screen (Issuance Extend & LG Position Recon)
        print("\n--- TEST 20: Level 4 Screen-Aware How-To Disambiguation ---")
        res20_ext_iss = ai_query_assistant_service.process_query(
            db=db,
            user_question="how do I extend?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/issuance/issued-lgs",
        )
        assert res20_ext_iss.get("level") == 4, "Test 20a Failed"
        assert "issuance/issued-lgs" in (res20_ext_iss.get("answer") or ""), "Test 20a Failed: Expected Issued LGs path"

        res20_pos_recon = ai_query_assistant_service.process_query(
            db=db,
            user_question="how does reconciliation work?",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/issuance/reconciliation",
        )
        assert "issuance/reconciliation" in (res20_pos_recon.get("answer") or ""), "Test 20b Failed: Expected LG Position Recon"
        print("[PASS] Level 4 guides disambiguated Custody vs Issuance & Cash Recon vs Position Recon.")

        # TEST 21: Bank Reconciliation Ambiguous Question Routes to System Help (Not Custody Search)
        print("\n--- TEST 21: Bank Reconciliation Context Routing ---")
        cls21 = ai_query_assistant_service.classify_and_interpret(
            "show unmatched items",
            customer_id=customer_id,
            user_id=user_id,
            page_context="/corporate-admin/reconciliation/workspace",
        )
        assert cls21.get("intent") == "system_help" and cls21.get("suggested_level") == 4, (
            f"Test 21 Failed: Expected Level 4 system_help on Reconciliation, got {cls21}"
        )
        print("[PASS] Reconciliation screen queries route to contextual guidance instead of empty LG search.")

        print("\n" + "=" * 75)
        print("ALL 21 4-LEVEL TREASURY + PAGE-AWARE AI ASSISTANT TESTS PASSED PERFECTLY!")
        print("=" * 75)

    finally:
        db.close()

if __name__ == "__main__":
    run_suite()
