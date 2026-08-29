from sqlalchemy import create_engine, text

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    cols = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'bank_form_templates'")).mappings().all()
    print("COLUMNS:")
    for c in cols:
        print(" ", c['column_name'], c['data_type'])
        
    rows = conn.execute(text("SELECT id, bank_id, form_type, is_active FROM bank_form_templates WHERE bank_id = 8")).mappings().all()
    print("ENBD TEMPLATES:")
    for r in rows:
        print(dict(r))
