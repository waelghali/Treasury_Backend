# tests/test_rfq_document_governance.py
"""
Phase 12: RFQ Document Governance Automated Verification Suite

Validates:
1. 2-Dimensional Orthogonal Document Matrix Normalization & Backwards Compatibility
2. 100% Bank Air-Gap: INTERNAL_ONLY Customer Vault Documents are strictly excluded from counterparties
3. Decoupled Timing: BIDDING vs WINNER_ONLY independent of leg association
4. Strict Multi-Leg Exclusion Firewall: Excluded bank counterparties never receive leg-scoped documents
"""

import json
from app.models.models_quotation import QuotationRequest, QuotationLeg


def test_document_normalization_and_backwards_compatibility():
    print("\n--- [TEST 1] Document Normalization & Backwards Compatibility ---")
    
    # 1. Legacy comma-separated string format
    rfq_legacy = QuotationRequest(
        id="rfq_test_legacy",
        document_path="gs://bucket/doc1.pdf, gs://bucket/doc2.pdf"
    )
    docs = rfq_legacy.get_parsed_documents()
    assert len(docs) == 2, f"Expected 2 documents, got {len(docs)}"
    assert docs[0]["scope"] == "ALL_PAIRS"
    assert docs[0]["release_timing"] == "BIDDING"
    assert docs[0]["is_internal_only"] is False
    assert docs[1]["name"] == "doc2.pdf"
    print("  [PASS] Legacy comma-separated paths normalized to ALL_PAIRS, BIDDING, is_internal_only=False")

    # 2. Legacy JSON with top-level release_to_winner_only: true
    legacy_json = json.dumps({
        "release_to_winner_only": True,
        "documents": [
            {"name": "Contract.pdf", "path": "gs://bucket/Contract.pdf", "leg_index": 0, "pair": "USD/EGP"}
        ]
    })
    rfq_legacy_winner = QuotationRequest(
        id="rfq_test_legacy_winner",
        document_path=legacy_json
    )
    docs_winner = rfq_legacy_winner.get_parsed_documents()
    assert len(docs_winner) == 1
    assert docs_winner[0]["scope"] == "SPECIFIC_LEG"
    assert docs_winner[0]["release_timing"] == "WINNER_ONLY"
    assert docs_winner[0]["pair"] == "USD/EGP"
    assert docs_winner[0]["is_internal_only"] is False
    print("  [PASS] Legacy JSON with release_to_winner_only correctly inherited as WINNER_ONLY")


def test_internal_only_vault_bank_air_gap():
    print("\n--- [TEST 2] 100% Bank Air-Gap for Internal Use Only Vault ---")

    phase12_payload = json.dumps({
        "release_to_winner_only": False,
        "documents": [
            {
                "id": "doc_pub_1",
                "name": "Commercial_Invoice_402.pdf",
                "path": "gs://bucket/Commercial_Invoice_402.pdf",
                "scope": "ALL_PAIRS",
                "release_timing": "BIDDING",
                "is_internal_only": False
            },
            {
                "id": "doc_internal_1",
                "name": "Treasury_Committee_Board_Approval.pdf",
                "path": "gs://bucket/Treasury_Committee_Board_Approval.pdf",
                "scope": "INTERNAL_ONLY",
                "release_timing": None,
                "is_internal_only": True
            },
            {
                "id": "doc_internal_2",
                "name": "Hedge_Ratio_Calculation_Sheet.xlsx",
                "path": "gs://bucket/Hedge_Ratio_Calculation_Sheet.xlsx",
                "scope": "INTERNAL_ONLY",
                "release_timing": None,
                "is_internal_only": True
            }
        ]
    })

    rfq = QuotationRequest(id="rfq_airgap_test", document_path=phase12_payload)

    # 1. Total parsed docs include all 3
    all_docs = rfq.get_parsed_documents()
    assert len(all_docs) == 3, f"Expected 3 docs, got {len(all_docs)}"
    print("  [PASS] All 3 documents parsed for corporate administration view")

    # 2. Internal docs extractor
    internal_docs = rfq.get_internal_documents()
    assert len(internal_docs) == 2, f"Expected 2 internal docs, got {len(internal_docs)}"
    assert all(d["is_internal_only"] for d in internal_docs)
    print("  [PASS] get_internal_documents() returns strictly 2 internal vault documents")

    # 3. Counterparty extractor: MUST AIR-GAP ALL INTERNAL DOCS
    counterparty_docs = rfq.get_counterparty_documents(is_bidding=True)
    assert len(counterparty_docs) == 1, f"Expected 1 counterparty doc, got {len(counterparty_docs)}"
    assert counterparty_docs[0]["name"] == "Commercial_Invoice_402.pdf"
    assert not any(d["is_internal_only"] for d in counterparty_docs)
    print("  [PASS] get_counterparty_documents() completely filters out internal vault docs (100% Air-Gap)")

    # 4. Leg extractor defaults to include_internal=False
    leg_docs_default = rfq.get_documents_for_leg()
    assert not any(d["is_internal_only"] for d in leg_docs_default)
    print("  [PASS] get_documents_for_leg(include_internal=False) strictly suppresses internal documents")


def test_decoupled_release_timing():
    print("\n--- [TEST 3] Decoupled Release Timing (Bidding vs Winner Only) ---")

    decoupled_payload = json.dumps({
        "release_to_winner_only": False,
        "documents": [
            {
                "id": "doc_bid",
                "name": "General_Specification.pdf",
                "path": "gs://bucket/General_Specification.pdf",
                "scope": "ALL_PAIRS",
                "release_timing": "BIDDING",
                "is_internal_only": False
            },
            {
                "id": "doc_winner",
                "name": "Post_Trade_Settlement_Letter.pdf",
                "path": "gs://bucket/Post_Trade_Settlement_Letter.pdf",
                "scope": "ALL_PAIRS",
                "release_timing": "WINNER_ONLY",
                "is_internal_only": False
            }
        ]
    })

    rfq = QuotationRequest(id="rfq_timing_test", document_path=decoupled_payload)

    # During bidding window
    bidding_docs = rfq.get_counterparty_documents(is_bidding=True)
    assert len(bidding_docs) == 1
    assert bidding_docs[0]["name"] == "General_Specification.pdf"
    print("  [PASS] During bidding window, WINNER_ONLY document is withheld")

    # Upon winner award
    winner_docs = rfq.get_counterparty_documents(is_winner=True, is_bidding=False)
    assert len(winner_docs) == 2
    doc_names = [d["name"] for d in winner_docs]
    assert "General_Specification.pdf" in doc_names
    assert "Post_Trade_Settlement_Letter.pdf" in doc_names
    print("  [PASS] Upon winner deal award, WINNER_ONLY document is unsealed alongside BIDDING document")


def test_strict_multi_leg_exclusion_firewall():
    print("\n--- [TEST 4] Strict Multi-Leg Counterparty Exclusion Firewall ---")

    multi_leg_payload = json.dumps({
        "release_to_winner_only": False,
        "documents": [
            {
                "id": "doc_leg1",
                "name": "US_Import_Contract.pdf",
                "path": "gs://bucket/US_Import_Contract.pdf",
                "scope": "SPECIFIC_LEG",
                "leg_index": 0,
                "leg_id": "leg_usd",
                "pair": "USD/EGP",
                "release_timing": "BIDDING",
                "is_internal_only": False
            },
            {
                "id": "doc_leg2",
                "name": "EU_Customs_Declaration.pdf",
                "path": "gs://bucket/EU_Customs_Declaration.pdf",
                "scope": "SPECIFIC_LEG",
                "leg_index": 1,
                "leg_id": "leg_eur",
                "pair": "EUR/EGP",
                "release_timing": "BIDDING",
                "is_internal_only": False
            },
            {
                "id": "doc_global",
                "name": "Master_Corporate_Profile.pdf",
                "path": "gs://bucket/Master_Corporate_Profile.pdf",
                "scope": "ALL_PAIRS",
                "release_timing": "BIDDING",
                "is_internal_only": False
            }
        ]
    })

    rfq = QuotationRequest(id="rfq_multi_leg", document_path=multi_leg_payload)

    # Counterparty on Leg 1 (USD/EGP) only
    leg1_docs = rfq.get_counterparty_documents(leg_id="leg_usd", pair="USD/EGP", leg_index=0, is_bidding=True)
    leg1_doc_names = [d["name"] for d in leg1_docs]
    assert "US_Import_Contract.pdf" in leg1_doc_names
    assert "Master_Corporate_Profile.pdf" in leg1_doc_names
    assert "EU_Customs_Declaration.pdf" not in leg1_doc_names, "Cross-leg leakage: Leg 2 doc found on Leg 1!"
    print("  [PASS] Bank participating in Leg 1 strictly receives Leg 1 and Global docs; Leg 2 is blocked")

    # Counterparty on Leg 2 (EUR/EGP) only
    leg2_docs = rfq.get_counterparty_documents(leg_id="leg_eur", pair="EUR/EGP", leg_index=1, is_bidding=True)
    leg2_doc_names = [d["name"] for d in leg2_docs]
    assert "EU_Customs_Declaration.pdf" in leg2_doc_names
    assert "Master_Corporate_Profile.pdf" in leg2_doc_names
    assert "US_Import_Contract.pdf" not in leg2_doc_names, "Cross-leg leakage: Leg 1 doc found on Leg 2!"
    print("  [PASS] Bank participating in Leg 2 strictly receives Leg 2 and Global docs; Leg 1 is blocked")


if __name__ == "__main__":
    print("=" * 70)
    print("PHASE 12 RFQ DOCUMENT GOVERNANCE VERIFICATION SUITE")
    print("=" * 70)
    test_document_normalization_and_backwards_compatibility()
    test_internal_only_vault_bank_air_gap()
    test_decoupled_release_timing()
    test_strict_multi_leg_exclusion_firewall()
    print("\n" + "=" * 70)
    print("ALL PHASE 12 RFQ DOCUMENT GOVERNANCE CHECKS PASSED (100% SUCCESS)!")
    print("=" * 70)
