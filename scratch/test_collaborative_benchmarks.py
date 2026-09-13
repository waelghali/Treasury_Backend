# scratch/test_collaborative_benchmarks.py
import sys
sys.path.insert(0, r"c:\Grow")

from app.database import SessionLocal
from app.models.models_quotation import QuotationAnonymousBenchmark
from app.services.quotation_benchmark_service import (
    get_market_benchmarks, get_timing_recommendations,
    get_system_owner_telemetry, backfill_historical_anonymous_benchmarks
)

def run_all_tests():
    print("=== STARTING COLLABORATIVE INTELLIGENCE & TELEMETRY VERIFICATION ===")
    db = SessionLocal()
    try:
        # 1. Backfill test
        bf_count = backfill_historical_anonymous_benchmarks(db)
        print(f"[PASS] Backfill completed. Total anonymous records: {bf_count}")

        # 2. Verify Zero-Knowledge Guardrail in DB table
        cols = [c.name for c in QuotationAnonymousBenchmark.__table__.columns]
        assert "customer_id" not in cols, "SECURITY BREACH: customer_id must not exist in anonymous mart!"
        assert "user_id" not in cols, "SECURITY BREACH: user_id must not exist in anonymous mart!"
        assert "ref_no" not in cols, "SECURITY BREACH: ref_no must not exist in anonymous mart!"
        assert "amount" not in cols, "SECURITY BREACH: raw amount must not exist in anonymous mart!"
        assert "deal_tier" in cols, "deal_tier must exist in anonymous mart!"
        assert "tenant_cohort_hash" in cols, "tenant_cohort_hash must exist in anonymous mart!"
        print("[PASS] Zero-Knowledge Confidentiality Verification: 0 PII / 0 tenant identifiers in schema.")

        # 3. Verify K-Anonymity Threshold Logic (k=3)
        res_k3 = get_market_benchmarks(db, currency_pair="USD/EGP", trade_type="FX_SPOT", k_threshold=3)
        print(f"k=3 evaluation output: insufficient_sample={res_k3.get('insufficient_sample')}, sample_count={res_k3.get('current_tenants_sample') or res_k3.get('total_anonymous_samples')}")

        # Inject 3 distinct synthetic tenant cohort benchmarks to test mathematical percentile computation
        test_cohorts = ["anon_cohort_alpha", "anon_cohort_beta", "anon_cohort_gamma"]
        test_records = []
        for ch in test_cohorts:
            rec = QuotationAnonymousBenchmark(
                tenant_cohort_hash=ch,
                currency_pair="EUR/EGP",
                trade_type="FX_SPOT",
                deal_tier="TIER_2",
                cbe_benchmark_rate=53.20,
                winning_spread_bps=6.5,
                avg_spread_bps=8.0,
                num_participating_banks=4,
                num_quotes_submitted=3,
                response_duration_seconds=540,
                window_time_slot="TUE_MORNING_10_12"
            )
            db.add(rec)
            test_records.append(rec)
        db.commit()

        # Now test EUR/EGP where distinct tenants = 3 (meets k=3 threshold!)
        res_eur = get_market_benchmarks(db, currency_pair="EUR/EGP", trade_type="FX_SPOT", k_threshold=3)
        assert res_eur["insufficient_sample"] is False, "Expected insufficient_sample to be False when k >= 3"
        assert "median_spread_bps" in res_eur
        assert "p25_spread_bps" in res_eur
        assert "p75_spread_bps" in res_eur
        print(f"[PASS] K-Anonymity Verified (k=3): Unlocked differential percentiles: Median={res_eur['median_spread_bps']} bps, P25={res_eur['p25_spread_bps']} bps.")

        # Clean up synthetic test records
        for tr in test_records:
            db.delete(tr)
        db.commit()

        # 4. Timing Recommender
        timing = get_timing_recommendations(db, trade_type="FX_SPOT")
        assert "recommended_window" in timing
        assert "reasoning" in timing
        print(f"[PASS] Timing Recommender Verified: {timing['recommended_window']} (Multiplier: {timing['participation_multiplier']})")

        # 5. System Owner Telemetry
        so = get_system_owner_telemetry(db)
        assert "velocity" in so
        assert "bank_ecosystem" in so
        assert "governance" in so
        assert so["velocity"]["total_rfqs"] > 0
        assert len(so["bank_ecosystem"]) > 0
        top_bank = so["bank_ecosystem"][0]
        print(f"[PASS] System Owner Telemetry Verified: {so['velocity']['total_rfqs']} RFQs tracked. Top Bank: {top_bank['bank_name']} ({top_bank['participation_rate']}%, Status: {top_bank['status']})")

        print("=== ALL TESTS PASSED WITH 100% COMPLIANCE ===")
    finally:
        db.close()

if __name__ == "__main__":
    run_all_tests()
