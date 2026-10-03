"""
Grow Treasury - Dealer Accolade & Motivation Verification Script
Cross-verifies raw database records with the DealerAchievementService engine.
"""
import sys
import os
from sqlalchemy import text
from app.database import SessionLocal
from app.services.dealer_achievement_service import DealerAchievementService

def verify_dealer(dealer_email: str = "waelghali79+cibe@gmail.com"):
    db = SessionLocal()
    clean_email = dealer_email.strip().lower()

    print("=" * 80)
    print(f"TWO-WAY INDEPENDENT DB & ENGINE VERIFICATION")
    print(f"Target Dealer Email: {clean_email}")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # METHOD 1: Direct Raw SQL Queries executed against PostgreSQL
    # -------------------------------------------------------------------------

    # 1. Total Quotes Breakdown (Execution vs Indicative)
    q_quotes = text("""
        SELECT 
            COUNT(CASE WHEN LOWER(COALESCE(ql.quotation_base, qr.quotation_base, 'execution')) = 'indicative' THEN 1 END) as indicative_quotes,
            COUNT(CASE WHEN LOWER(COALESCE(ql.quotation_base, qr.quotation_base, 'execution')) != 'indicative' THEN 1 END) as execution_quotes,
            COUNT(*) as total_quotes
        FROM quotation_offers qo
        LEFT JOIN quotation_legs ql ON qo.leg_id = ql.id
        LEFT JOIN quotation_bank_assignments qba ON qo.assignment_id = qba.id
        LEFT JOIN quotation_rfqs qr ON qba.rfq_id = qr.id
        WHERE LOWER(qo.submitted_by_email) = :email;
    """)
    raw_quotes = db.execute(q_quotes, {"email": clean_email}).fetchone()
    raw_ind_quotes = raw_quotes[0] or 0
    raw_exec_quotes = raw_quotes[1] or 0

    # 2. Total Tenders Participated
    q_tenders = text("""
        SELECT COUNT(DISTINCT qba.rfq_id)
        FROM quotation_bank_assignments qba
        WHERE qba.id IN (
            SELECT assignment_id FROM quotation_offers WHERE LOWER(submitted_by_email) = :email
            UNION
            SELECT assignment_id FROM quotation_tbill_offers WHERE LOWER(submitted_by_email) = :email
            UNION
            SELECT assignment_id FROM quotation_access_otps WHERE LOWER(email) = :email AND is_used = true
        );
    """)
    raw_tenders = db.execute(q_tenders, {"email": clean_email}).scalar() or 0

    # 3. Total Deals Won (Firm Execution Legs Only)
    q_won_legs = text("""
        SELECT COUNT(DISTINCT ql.id)
        FROM quotation_legs ql
        JOIN quotation_offers qo ON qo.leg_id = ql.id
        JOIN quotation_bank_assignments qba ON qo.assignment_id = qba.id
        JOIN quotation_banks qb ON qba.quotation_bank_id = qb.id
        WHERE LOWER(qo.submitted_by_email) = :email
          AND ql.winner_bank_id = qb.bank_id
          AND ql.status NOT IN ('REJECTED', 'CANCELLED', 'DECLINED')
          AND LOWER(COALESCE(ql.quotation_base, 'execution')) = 'execution'
          AND ABS(CAST(qo.price AS FLOAT) - CAST(ql.winner_rate AS FLOAT)) < 0.0001;
    """)
    raw_won_legs = db.execute(q_won_legs, {"email": clean_email}).scalar() or 0

    # 4. Currency Pairs Won (Execution Only)
    q_pairs = text("""
        SELECT DISTINCT 
            CASE 
                WHEN UPPER(ql.buy_currency) < UPPER(ql.sell_currency) 
                THEN UPPER(ql.buy_currency) || '/' || UPPER(ql.sell_currency)
                ELSE UPPER(ql.sell_currency) || '/' || UPPER(ql.buy_currency)
            END as pair
        FROM quotation_legs ql
        JOIN quotation_offers qo ON qo.leg_id = ql.id
        JOIN quotation_bank_assignments qba ON qo.assignment_id = qba.id
        JOIN quotation_banks qb ON qba.quotation_bank_id = qb.id
        WHERE LOWER(qo.submitted_by_email) = :email
          AND ql.winner_bank_id = qb.bank_id
          AND ql.status NOT IN ('REJECTED', 'CANCELLED', 'DECLINED')
          AND LOWER(COALESCE(ql.quotation_base, 'execution')) = 'execution'
          AND ABS(CAST(qo.price AS FLOAT) - CAST(ql.winner_rate AS FLOAT)) < 0.0001;
    """)
    raw_pairs_rows = db.execute(q_pairs, {"email": clean_email}).fetchall()
    raw_pairs = sorted([r[0] for r in raw_pairs_rows])

    # 5. Ready at the Bell (Opening bell terminal arrival)
    q_bell = text("""
        SELECT COUNT(DISTINCT qba.rfq_id)
        FROM quotation_access_otps otp
        JOIN quotation_bank_assignments qba ON otp.assignment_id = qba.id
        JOIN quotation_rfqs qr ON qba.rfq_id = qr.id
        WHERE LOWER(otp.email) = :email
          AND otp.is_used = true
          AND qr.status NOT IN ('CANCELLED', 'DRAFT')
          AND qr.window_start IS NOT NULL
          AND otp.created_at >= (qr.window_start - INTERVAL '15 minutes')
          AND otp.created_at <= (qr.window_start + INTERVAL '60 seconds');
    """)
    raw_bell = db.execute(q_bell, {"email": clean_email}).scalar() or 0

    # -------------------------------------------------------------------------
    # METHOD 2: DealerAchievementService Engine Output
    # -------------------------------------------------------------------------
    service_res = DealerAchievementService.get_dealer_achievements(db, dealer_email=clean_email)
    pb = service_res["personal_bests"]
    trophies = {t["id"]: t for t in service_res["trophies"]}

    # -------------------------------------------------------------------------
    # SIDE-BY-SIDE MATCH COMPARISON
    # -------------------------------------------------------------------------
    print(f"\n{'METRIC':<30} | {'RAW SQL (DIRECT)':<18} | {'SERVICE ENGINE':<18} | {'MATCH STATUS'}")
    print("-" * 85)

    def check(name, raw_val, srv_val):
        status = "[OK] 100% MATCH" if raw_val == srv_val else f"[FAIL] MISMATCH ({raw_val} != {srv_val})"
        print(f"{name:<30} | {str(raw_val):<18} | {str(srv_val):<18} | {status}")

    check("Execution Quotes Submitted", raw_exec_quotes, pb["execution_quotes_submitted"])
    check("Indicative Quotes Submitted", raw_ind_quotes, pb["indicative_quotes_submitted"])
    check("Total Tenders Participated", raw_tenders, pb["total_tenders_participated"])
    check("Firm Deals Won", raw_won_legs, pb["total_deals_won"])
    check("Currency Pairs Won Count", len(raw_pairs), pb["currency_pairs_count"])
    check("Ready at Bell Sessions", raw_bell, trophies["READY_AT_THE_BELL"]["current_value"])

    print(f"\nCurrency Pairs Won (Raw SQL): {raw_pairs}")
    print(f"Overall Tier Awarded: {service_res['dealer_tier']} ({service_res['dealer_perk']})")

    print("\n--- ACTIVE TROPHIES STATUS & CALIBRATED HARD MILESTONES ---")
    for tid, t in trophies.items():
        print(f"  * {t['title']:<25} [{t['current_tier']:<8}] : {t['current_value']}/{t['target_value']} {t['unit']} ({t['progress_percent']}%)")

    db.close()

if __name__ == "__main__":
    email_arg = sys.argv[1] if len(sys.argv) > 1 else "waelghali79+cibe@gmail.com"
    verify_dealer(email_arg)
