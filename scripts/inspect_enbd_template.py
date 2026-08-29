from sqlalchemy import create_engine, text
import json

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    templates = conn.execute(text("""
        SELECT *
        FROM bank_form_templates
        WHERE bank_id = 8
    """)).mappings().all()
    print(f"Found {len(templates)} templates for ENBD:")
    for t in templates:
        print(f"--- Template #{t['id']}: {t.get('form_name') or t.get('name')} (form_type: {t['form_type']}) ---")
        fm = t['field_mapping']
        if isinstance(fm, str):
            fm = json.loads(fm)
        print(json.dumps(fm, indent=2))
