from sqlalchemy import create_engine, text

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    facs = conn.execute(text("""
        SELECT f.id, f.customer_id, f.bank_id, f.facility_name, f.currency_id, f.total_limit_amount, f.status, f.is_deleted,
               b.name as bank_name, c.name as customer_name
        FROM facilities f
        LEFT JOIN banks b ON f.bank_id = b.id
        LEFT JOIN customers c ON f.customer_id = c.id
        WHERE f.customer_id = 25
    """)).mappings().all()
    print("DELTA FACILITIES ON STAGING:")
    for f in facs:
        print(f"  ID {f['id']}: '{f['facility_name']}' | Bank: '{f['bank_name']}' (ID {f['bank_id']}) | Limit: {f['total_limit_amount']} | Status: {f['status']} | is_deleted: {f['is_deleted']}")
        subs = conn.execute(text("SELECT id, limit_name, limit_amount, lg_type_ids FROM issuance_facility_sub_limits WHERE facility_id = :fid"), {"fid": f["id"]}).mappings().all()
        for s in subs:
            print(f"     Sub #{s['id']}: '{s['limit_name']}' | Amount: {s['limit_amount']} | LG Types: {s['lg_type_ids']}")
