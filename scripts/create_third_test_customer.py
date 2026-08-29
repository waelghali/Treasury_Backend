# scripts/create_third_test_customer.py
import sys, os
sys.path.insert(0, os.path.abspath("."))
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from app.core.hashing import get_password_hash

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

def seed():
    print("=" * 60)
    print("PROVISIONING 3RD TEST CORPORATION FOR OWNER PILOT (STAGING)")
    print("=" * 60)
    
    eng = create_engine(STAGING_DB_URL)
    now_date = datetime.now(timezone.utc).date()
    one_year_date = now_date + timedelta(days=365)
    default_pw_hash = get_password_hash("DemoPass2026!")

    with eng.connect() as conn:
        # Check if Customer 3 already exists
        cust_name = "Delta Modern Contracting & Infrastructure SAE"
        c3_id = conn.execute(text("SELECT id FROM customers WHERE name = :name"), {"name": cust_name}).scalar()
        if not c3_id:
            c3_id = conn.execute(text("""
                INSERT INTO customers (
                    name, address, contact_email, contact_phone, 
                    subscription_plan_id, start_date, end_date, status, 
                    active_user_count, active_lg_count, created_at, updated_at, is_deleted
                ) VALUES (
                    :name, '5th Settlement, North 90th Street, Building 10, New Cairo, Egypt',
                    'treasury@delta-contracting.com', '+20229810000',
                    6, :start_d, :end_d, 'ACTIVE',
                    3, 0, NOW(), NOW(), FALSE
                ) RETURNING id;
            """), {"name": cust_name, "start_d": now_date, "end_d": one_year_date}).scalar()
            conn.commit()
        print(f"[OK] Customer 3 ID: {c3_id} ('{cust_name}')")

        # Primary Entity
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
        print(f"[OK] Entity 3 ID: {e3_id}")

        # Users for Customer 3
        c3_users = [
            ("delta.admin@globex.com", "CORPORATE_ADMIN"),
            ("delta.approver@globex.com", "CHECKER"),
            ("delta.officer@globex.com", "END_USER"),
        ]
        for email, role in c3_users:
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
                """), {"email": email, "pw": default_pw_hash, "role": role, "cid": c3_id})
                conn.commit()
            print(f"   [USER] {email:28s} | Role: {role}")

    print("\n" + "=" * 60)
    print("SUCCESS: 3RD PILOT CUSTOMER READY ON STAGING (BLANK SLATE)!")
    print("=" * 60)

if __name__ == "__main__":
    seed()
