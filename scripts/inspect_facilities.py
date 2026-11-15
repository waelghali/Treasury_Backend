import os
from sqlalchemy import create_engine, text

STAGING_DB_URL = os.environ["STAGING_DB_URL"]
LOCAL_DB_URL = os.environ.get("LOCAL_DB_URL") or os.environ["DATABASE_URL"]

for name, url in [("STAGING", STAGING_DB_URL), ("LOCAL", LOCAL_DB_URL)]:
    try:
        eng = create_engine(url)
        with eng.connect() as conn:
            facs = conn.execute(text("""
                SELECT f.id, f.customer_id, f.bank_id, f.facility_name, f.currency_id, f.total_limit_amount, f.is_deleted,
                       b.name as bank_name, c.name as customer_name
                FROM facilities f
                LEFT JOIN banks b ON f.bank_id = b.id
                LEFT JOIN customers c ON f.customer_id = c.id
                ORDER BY f.id DESC LIMIT 15
            """)).mappings().all()
            print(f"=== {name} FACILITIES ({len(facs)}) ===")
            for f in facs:
                print(f"  Fac #{f['id']}: {f['facility_name']} | Cust: {f['customer_name']} (ID {f['customer_id']}) | Bank ID: {f['bank_id']} ({f['bank_name']}) | Limit: {f['total_limit_amount']} | is_deleted={f['is_deleted']}")
                subs = conn.execute(text("SELECT id, limit_name, limit_amount, lg_type_ids, is_deleted FROM issuance_facility_sub_limits WHERE facility_id = :fid"), {"fid": f["id"]}).mappings().all()
                for s in subs:
                    print(f"     Sub #{s['id']}: {s['limit_name']} | Limit: {s['limit_amount']} | lg_types: {s['lg_type_ids']} | is_deleted: {s['is_deleted']}")
    except Exception as e:
        print(f"{name} Error:", e)
