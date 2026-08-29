# scripts/seed_new_test_customers.py
import sys, os
sys.path.insert(0, os.path.abspath("."))
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from app.core.hashing import get_password_hash

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

def inspect_plans():
    eng = create_engine(STAGING_DB_URL)
    with eng.connect() as conn:
        res = conn.execute(text("SELECT id, name, max_users, has_issuance_module, has_custody_module, has_quotation_module, has_reconciliation_module FROM subscription_plans ORDER BY id")).mappings().all()
        print("Existing Subscription Plans:")
        for r in res:
            print(f"  Plan ID {r['id']}: {r['name']} | MaxUsers: {r['max_users']} | Issuance: {r['has_issuance_module']} | Custody: {r['has_custody_module']}")

if __name__ == "__main__":
    inspect_plans()
