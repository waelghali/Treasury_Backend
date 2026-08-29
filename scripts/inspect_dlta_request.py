from sqlalchemy import create_engine, text

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    req = conn.execute(text("SELECT id, serial_number, status, customer_id FROM issuance_requests WHERE serial_number = :sn"), {"sn": "DLTA-2026-0001"}).mappings().first()
    if req:
        print(f"STAGING: Found request {req['serial_number']} (ID {req['id']}) status={req['status']}")
        docs = conn.execute(text("SELECT id, document_type, file_name, is_deleted, created_at, ai_verification_result FROM issuance_request_documents WHERE request_id = :rid ORDER BY id"), {"rid": req["id"]}).mappings().all()
        print(f"STAGING Documents ({len(docs)}):")
        for d in docs:
            ai_status = d["ai_verification_result"].get("mismatches") if d["ai_verification_result"] else "None"
            print(f"   Doc ID {d['id']}: {d['file_name']} | type={d['document_type']} | is_deleted={d['is_deleted']} | mismatches={ai_status}")
