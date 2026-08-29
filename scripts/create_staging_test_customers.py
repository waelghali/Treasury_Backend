# scripts/create_staging_test_customers.py
import sys, os
sys.path.insert(0, os.path.abspath("."))
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from app.core.hashing import get_password_hash

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

def seed():
    print("=" * 60)
    print("SEEDING 2 FRESH TEST CORPORATIONS ON STAGING DATABASE")
    print("=" * 60)
    
    eng = create_engine(STAGING_DB_URL)
    now_date = datetime.now(timezone.utc).date()
    one_year_date = now_date + timedelta(days=365)
    default_pw_hash = get_password_hash("DemoPass2026!")

    with eng.connect() as conn:
        # 1. Update Subscription Plan 6 (MAX All Plan) to ensure all modules are enabled
        conn.execute(text("""
            UPDATE subscription_plans 
            SET has_issuance_module = TRUE,
                has_custody_module = TRUE,
                has_quotation_module = TRUE,
                has_reconciliation_module = TRUE,
                max_users = 25,
                max_issuance_records = 500,
                updated_at = NOW()
            WHERE id = 6;
        """))
        conn.commit()
        print("[OK] Subscription Plan 6 updated (All modules active, 25 users).")

        # Get core IDs
        egp_curr_id = conn.execute(text("SELECT id FROM currencies WHERE iso_code = 'EGP'")).scalar() or 1
        usd_curr_id = conn.execute(text("SELECT id FROM currencies WHERE iso_code = 'USD'")).scalar() or 2
        
        banks_map = {}
        for b_name in ["CIB", "NBE", "QNB", "HSBC", "BM", "FAB"]:
            bid = conn.execute(text(f"SELECT id FROM banks WHERE name ILIKE '%{b_name}%' LIMIT 1")).scalar()
            if bid:
                banks_map[b_name] = bid
        print(f"[OK] Found reference banks: {banks_map}")

        # ==============================================================================
        # CUSTOMER 1: Apex Global Engineering & Contracting SAE (Friend 1)
        # ==============================================================================
        print("\n--- Creating Customer 1: Apex Global Engineering SAE ---")
        c1_id = conn.execute(text("SELECT id FROM customers WHERE name = 'Apex Global Engineering & Contracting SAE'")).scalar()
        if not c1_id:
            c1_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted
                ) VALUES (
                    'Apex Global Engineering & Contracting SAE', 'Smart Village, Building B12, Giza, Egypt',
                    'contact@apex-global.com', '+20235371000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE
                ) RETURNING id;
            """), {"start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"   Customer 1 ID: {c1_id}")

        # Primary Entity
        e1_id = conn.execute(text("SELECT id FROM customer_entities WHERE customer_id = :cid LIMIT 1"), {"cid": c1_id}).scalar()
        if not e1_id:
            e1_id = conn.execute(text("""
                INSERT INTO customer_entities (
                    customer_id, entity_name, code, commercial_register_number, tax_id, address,
                    is_active, is_deleted, created_at, updated_at
                ) VALUES (
                    :cid, 'Apex Global Engineering SAE (HQ)', 'APEX', 'CR-992810', 'TAX-443-991-002',
                    'Smart Village, Building B12, Giza', TRUE, FALSE, NOW(), NOW()
                ) RETURNING id;
            """), {"cid": c1_id}).scalar()
            conn.commit()
        print(f"   Entity 1 ID: {e1_id}")

        # Dedicated Projects
        p1_id = conn.execute(text("SELECT id FROM corporate_projects WHERE customer_id = :cid LIMIT 1"), {"cid": c1_id}).scalar()
        if not p1_id:
            p1_id = conn.execute(text("""
                INSERT INTO corporate_projects (
                    customer_id, name, project_type, reference_number, status, created_at
                ) VALUES (
                    :cid, 'New Capital Monorail - Package B', 'INFRASTRUCTURE', 'PRJ-2026-MONO-01', 'ACTIVE', NOW()
                ) RETURNING id;
            """), {"cid": c1_id}).scalar()
            conn.commit()

        # Users for Customer 1
        c1_users = [
            ("apex.admin@globex.com", "CORPORATE_ADMIN"),
            ("apex.approver@globex.com", "CHECKER"),
            ("apex.officer@globex.com", "END_USER"),
        ]
        for email, role in c1_users:
            uid = conn.execute(text("SELECT id FROM users WHERE email = :email"), {"email": email}).scalar()
            if not uid:
                conn.execute(text("""
                    INSERT INTO users (
                        email, password_hash, role, customer_id, 
                        has_all_entity_access, must_change_password, is_deleted, 
                        created_at, updated_at
                    ) VALUES (
                        :email, :pw, :role, :cid,
                        TRUE, FALSE, FALSE,
                        NOW(), NOW()
                    );
                """), {"email": email, "pw": default_pw_hash, "role": role, "cid": c1_id})
                conn.commit()
            print(f"   [USER] {email:28s} | Role: {role}")

        # Facilities for Customer 1
        if "CIB" in banks_map:
            cib_fac = conn.execute(text("SELECT id FROM facilities WHERE customer_id = :cid AND bank_id = :bid"), {"cid": c1_id, "bid": banks_map["CIB"]}).scalar()
            if not cib_fac:
                cib_fac = conn.execute(text("""
                    INSERT INTO facilities (
                        customer_id, bank_id, currency_id, facility_name, reference_number,
                        facility_type, total_limit_amount, tenor_months, start_date, expiry_date,
                        status, is_deleted, sla_agreement_days, facility_default_margin_pct,
                        created_at, updated_at
                    ) VALUES (
                        :cid, :bid, :cur, 'CIB Main Multi-Purpose Facility', 'FAC-CIB-2026-01',
                        'LG', 50000000.00, 12, :start_d, :end_d,
                        'ACTIVE', FALSE, 2, 10.0,
                        NOW(), NOW()
                    ) RETURNING id;
                """), {"cid": c1_id, "bid": banks_map["CIB"], "cur": egp_curr_id, "start_d": now_date, "end_d": one_year_date}).scalar()
                
                # Sub-limits for CIB
                conn.execute(text("""
                    INSERT INTO issuance_facility_sub_limits (
                        facility_id, lg_type_ids, limit_name, limit_amount, max_amount_per_lg, max_tenor_days,
                        allows_confirmation, default_commission_rate, default_cash_margin_pct
                    ) VALUES 
                    (:fid, '[2]'::jsonb, 'Bid Bonds (Tender Guarantees)', 15000000.00, 5000000.00, 180, TRUE, 0.75, 5.0),
                    (:fid, '[1]'::jsonb, 'Performance Guarantees', 25000000.00, 15000000.00, 730, TRUE, 1.25, 10.0),
                    (:fid, '[3]'::jsonb, 'Advance Payment Guarantees', 10000000.00, 8000000.00, 365, TRUE, 1.50, 15.0);
                """), {"fid": cib_fac})
                conn.commit()
                print("   [FACILITY] CIB 50M EGP Credit Line & 3 Sub-limits configured.")

        if "QNB" in banks_map:
            qnb_fac = conn.execute(text("SELECT id FROM facilities WHERE customer_id = :cid AND bank_id = :bid"), {"cid": c1_id, "bid": banks_map["QNB"]}).scalar()
            if not qnb_fac:
                qnb_fac = conn.execute(text("""
                    INSERT INTO facilities (
                        customer_id, bank_id, currency_id, facility_name, reference_number,
                        facility_type, total_limit_amount, tenor_months, start_date, expiry_date,
                        status, is_deleted, sla_agreement_days, facility_default_margin_pct,
                        created_at, updated_at
                    ) VALUES (
                        :cid, :bid, :cur, 'QNB Fast-Track Facility', 'FAC-QNB-2026-01',
                        'LG', 30000000.00, 12, :start_d, :end_d,
                        'ACTIVE', FALSE, 1, 5.0,
                        NOW(), NOW()
                    ) RETURNING id;
                """), {"cid": c1_id, "bid": banks_map["QNB"], "cur": egp_curr_id, "start_d": now_date, "end_d": one_year_date}).scalar()
                
                # Sub-limit for QNB
                conn.execute(text("""
                    INSERT INTO issuance_facility_sub_limits (
                        facility_id, lg_type_ids, limit_name, limit_amount, max_amount_per_lg, max_tenor_days,
                        allows_confirmation, default_commission_rate, default_cash_margin_pct
                    ) VALUES 
                    (:fid, '[1,2,3,4]'::jsonb, 'General LG Issuance Pool (Fast-Track 24h)', 30000000.00, 10000000.00, 365, TRUE, 1.10, 5.0);
                """), {"fid": qnb_fac})
                conn.commit()
                print("   [FACILITY] QNB 30M EGP Fast-Track Credit Line configured.")

        # ==============================================================================
        # CUSTOMER 2: Horizon Infrastructure & Power SAE (Friend 2)
        # ==============================================================================
        print("\n--- Creating Customer 2: Horizon Infrastructure SAE ---")
        c2_id = conn.execute(text("SELECT id FROM customers WHERE name = 'Horizon Infrastructure & Power SAE'")).scalar()
        if not c2_id:
            c2_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted
                ) VALUES (
                    'Horizon Infrastructure & Power SAE', 'New Cairo, Sector 1, Building 44, Cairo, Egypt',
                    'treasury@horizon-power.com', '+20228190000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE
                ) RETURNING id;
            """), {"start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"   Customer 2 ID: {c2_id}")

        # Primary Entity
        e2_id = conn.execute(text("SELECT id FROM customer_entities WHERE customer_id = :cid LIMIT 1"), {"cid": c2_id}).scalar()
        if not e2_id:
            e2_id = conn.execute(text("""
                INSERT INTO customer_entities (
                    customer_id, entity_name, code, commercial_register_number, tax_id, address,
                    is_active, is_deleted, created_at, updated_at
                ) VALUES (
                    :cid, 'Horizon Power & Renewables SAE', 'HORZ', 'CR-881290', 'TAX-612-881-001',
                    'New Cairo, Sector 1, Cairo', TRUE, FALSE, NOW(), NOW()
                ) RETURNING id;
            """), {"cid": c2_id}).scalar()
            conn.commit()
        print(f"   Entity 2 ID: {e2_id}")

        # Users for Customer 2
        c2_users = [
            ("horizon.admin@globex.com", "CORPORATE_ADMIN"),
            ("horizon.approver@globex.com", "CHECKER"),
            ("horizon.officer@globex.com", "END_USER"),
        ]
        for email, role in c2_users:
            uid = conn.execute(text("SELECT id FROM users WHERE email = :email"), {"email": email}).scalar()
            if not uid:
                conn.execute(text("""
                    INSERT INTO users (
                        email, password_hash, role, customer_id, 
                        has_all_entity_access, must_change_password, is_deleted, 
                        created_at, updated_at
                    ) VALUES (
                        :email, :pw, :role, :cid,
                        TRUE, FALSE, FALSE,
                        NOW(), NOW()
                    );
                """), {"email": email, "pw": default_pw_hash, "role": role, "cid": c2_id})
                conn.commit()
            print(f"   [USER] {email:28s} | Role: {role}")

        # Facilities for Customer 2
        if "CIB" in banks_map:
            cib_fac2 = conn.execute(text("SELECT id FROM facilities WHERE customer_id = :cid AND bank_id = :bid"), {"cid": c2_id, "bid": banks_map["CIB"]}).scalar()
            if not cib_fac2:
                cib_fac2 = conn.execute(text("""
                    INSERT INTO facilities (
                        customer_id, bank_id, currency_id, facility_name, reference_number,
                        facility_type, total_limit_amount, tenor_months, start_date, expiry_date,
                        status, is_deleted, sla_agreement_days, facility_default_margin_pct,
                        created_at, updated_at
                    ) VALUES (
                        :cid, :bid, :cur, 'CIB Energy & Contracting Line', 'FAC-CIB-HORZ-01',
                        'LG', 45000000.00, 12, :start_d, :end_d,
                        'ACTIVE', FALSE, 2, 10.0,
                        NOW(), NOW()
                    ) RETURNING id;
                """), {"cid": c2_id, "bid": banks_map["CIB"], "cur": egp_curr_id, "start_d": now_date, "end_d": one_year_date}).scalar()
                
                conn.execute(text("""
                    INSERT INTO issuance_facility_sub_limits (
                        facility_id, lg_type_ids, limit_name, limit_amount, max_amount_per_lg, max_tenor_days,
                        allows_confirmation, default_commission_rate, default_cash_margin_pct
                    ) VALUES 
                    (:fid, '[1,4]'::jsonb, 'Solar & Energy LG Sub-Limit', 30000000.00, 15000000.00, 730, TRUE, 1.20, 10.0),
                    (:fid, '[2]'::jsonb, 'Tender Guarantees', 15000000.00, 5000000.00, 180, TRUE, 0.70, 5.0);
                """), {"fid": cib_fac2})
                conn.commit()
                print("   [FACILITY] CIB 45M EGP Credit Line & Sub-limits configured.")

        if "NBE" in banks_map:
            nbe_fac2 = conn.execute(text("SELECT id FROM facilities WHERE customer_id = :cid AND bank_id = :bid"), {"cid": c2_id, "bid": banks_map["NBE"]}).scalar()
            if not nbe_fac2:
                nbe_fac2 = conn.execute(text("""
                    INSERT INTO facilities (
                        customer_id, bank_id, currency_id, facility_name, reference_number,
                        facility_type, total_limit_amount, tenor_months, start_date, expiry_date,
                        status, is_deleted, sla_agreement_days, facility_default_margin_pct,
                        created_at, updated_at
                    ) VALUES (
                        :cid, :bid, :cur, 'NBE General Corporate Facility', 'FAC-NBE-HORZ-01',
                        'LG', 25000000.00, 12, :start_d, :end_d,
                        'ACTIVE', FALSE, 3, 15.0,
                        NOW(), NOW()
                    ) RETURNING id;
                """), {"cid": c2_id, "bid": banks_map["NBE"], "cur": egp_curr_id, "start_d": now_date, "end_d": one_year_date}).scalar()
                
                conn.execute(text("""
                    INSERT INTO issuance_facility_sub_limits (
                        facility_id, lg_type_ids, limit_name, limit_amount, max_amount_per_lg, max_tenor_days,
                        allows_confirmation, default_commission_rate, default_cash_margin_pct
                    ) VALUES 
                    (:fid, '[1,2,3,4]'::jsonb, 'NBE General Performance Pool', 25000000.00, 10000000.00, 365, TRUE, 1.35, 15.0);
                """), {"fid": nbe_fac2})
                conn.commit()
                print("   [FACILITY] NBE 25M EGP Credit Line configured.")

    print("\n" + "=" * 60)
    print("SUCCESS: 2 NEW TEST CORPORATIONS & PERSONAS PROVISIONED ON STAGING!")
    print("=" * 60)

if __name__ == "__main__":
    seed()
