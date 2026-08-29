# scripts/clean_test_facilities.py
import sys, os
sys.path.insert(0, os.path.abspath("."))
from sqlalchemy import create_engine, text

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

def clean():
    eng = create_engine(STAGING_DB_URL)
    with eng.connect() as conn:
        # Get IDs for the 2 test customers
        cust_ids = conn.execute(text("""
            SELECT id FROM customers 
            WHERE name IN ('Apex Global Engineering & Contracting SAE', 'Horizon Infrastructure & Power SAE')
        """)).scalars().all()
        
        if cust_ids:
            # Delete sub-limits linked to their facilities
            conn.execute(text("""
                DELETE FROM issuance_facility_sub_limits 
                WHERE facility_id IN (SELECT id FROM facilities WHERE customer_id = ANY(:cids))
            """), {"cids": cust_ids})
            
            # Delete facilities
            conn.execute(text("""
                DELETE FROM facilities WHERE customer_id = ANY(:cids)
            """), {"cids": cust_ids})
            
            # Delete projects so they can create their own
            conn.execute(text("""
                DELETE FROM corporate_projects WHERE customer_id = ANY(:cids)
            """), {"cids": cust_ids})
            
            conn.commit()
            print(f"[OK] Cleaned all facilities, sub-limits, and projects for customers: {cust_ids}")
        else:
            print("No test customers found.")

if __name__ == "__main__":
    clean()
