import os, sys
sys.path.insert(0, os.path.abspath("."))
from dotenv import load_dotenv
load_dotenv('c:/Grow/.env')
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from app.core.hashing import get_password_hash

LOCAL_DB_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:Voda%21%4012@localhost:5432/grow')

def seed_local():
    eng = create_engine(LOCAL_DB_URL)
    now_date = datetime.now(timezone.utc).date()
    one_year_date = now_date + timedelta(days=365)
    default_pw_hash = get_password_hash("DemoPass2026!")

    with eng.connect() as conn:
        # Update Plan 6
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

        egp_curr_id = conn.execute(text("SELECT id FROM currencies WHERE iso_code = 'EGP'")).scalar() or 1
        usd_curr_id = conn.execute(text("SELECT id FROM currencies WHERE iso_code = 'USD'")).scalar() or 2

        # 1. Apex
        c1_id = conn.execute(text("SELECT id FROM customers WHERE name = 'Apex Global Engineering & Contracting SAE'")).scalar()
        if not c1_id:
            c1_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted, domains
                ) VALUES (
                    'Apex Global Engineering & Contracting SAE', 'Smart Village, Building B12, Giza, Egypt',
                    'contact@apex-global.com', '+20235371000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE, '["apex.com", "apex-global.com"]'::jsonb
                ) RETURNING id;
            """), {"start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"[OK] Apex Customer ID: {c1_id}")

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
        print(f"[OK] Apex Entity ID: {e1_id}")

        # 2. Horizon
        c2_id = conn.execute(text("SELECT id FROM customers WHERE name = 'Horizon Infrastructure & Power SAE'")).scalar()
        if not c2_id:
            c2_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted, domains
                ) VALUES (
                    'Horizon Infrastructure & Power SAE', 'Arkan Plaza, Building 4, Sheikh Zayed, Giza, Egypt',
                    'treasury@horizon-infra.com', '+20238510000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE, '["horizon.com", "horizon-infra.com"]'::jsonb
                ) RETURNING id;
            """), {"start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"[OK] Horizon Customer ID: {c2_id}")

        e2_id = conn.execute(text("SELECT id FROM customer_entities WHERE customer_id = :cid LIMIT 1"), {"cid": c2_id}).scalar()
        if not e2_id:
            e2_id = conn.execute(text("""
                INSERT INTO customer_entities (
                    customer_id, entity_name, code, commercial_register_number, tax_id, address,
                    is_active, is_deleted, created_at, updated_at
                ) VALUES (
                    :cid, 'Horizon Power & Renewables SAE', 'HRZN', 'CR-883912', 'TAX-331-882-003',
                    'Arkan Plaza, Building 4, Sheikh Zayed', TRUE, FALSE, NOW(), NOW()
                ) RETURNING id;
            """), {"cid": c2_id}).scalar()
            conn.commit()
        print(f"[OK] Horizon Entity ID: {e2_id}")

        # 3. Delta
        c3_id = conn.execute(text("SELECT id FROM customers WHERE name = 'Delta Modern Contracting & Infrastructure SAE'")).scalar()
        if not c3_id:
            c3_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted, domains
                ) VALUES (
                    'Delta Modern Contracting & Infrastructure SAE', '5th Settlement, North 90th Street, Building 10, New Cairo, Egypt',
                    'treasury@delta-contracting.com', '+20229810000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE, '["delta.com", "delta-contracting.com"]'::jsonb
                ) RETURNING id;
            """), {"start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"[OK] Delta Customer ID: {c3_id}")

        e3_id = conn.execute(text("SELECT id FROM customer_entities WHERE customer_id = :cid LIMIT 1"), {"cid": c3_id}).scalar()
        if not e3_id:
            e3_id = conn.execute(text("""
                INSERT INTO customer_entities (
                    customer_id, entity_name, code, commercial_register_number, tax_id, address,
                    is_active, is_deleted, created_at, updated_at
                ) VALUES (
                    :cid, 'Delta Modern Contracting SAE (HQ)', 'DLTA', 'CR-774921', 'TAX-519-774-001',
                    '5th Settlement, North 90th Street, New Cairo', TRUE, FALSE, NOW(), NOW()
                ) RETURNING id;
            """), {"cid": c3_id}).scalar()
            conn.commit()
        print(f"[OK] Delta Entity ID: {e3_id}")

        # Ensure departments for Delta, Horizon, Apex
        for cid in [c1_id, c2_id, c3_id]:
            for dept in ["General", "Procurement", "Projects & Tenders", "Finance"]:
                d_exists = conn.execute(text("SELECT id FROM departments WHERE customer_id = :cid AND name = :d"), {"cid": cid, "d": dept}).scalar()
                if not d_exists:
                    conn.execute(text("""
                        INSERT INTO departments (customer_id, name, is_deleted, created_at, updated_at)
                        VALUES (:cid, :d, FALSE, NOW(), NOW())
                    """), {"cid": cid, "d": dept})
            conn.commit()
        print("[OK] Departments verified for all test customers.")

if __name__ == "__main__":
    seed_local()
