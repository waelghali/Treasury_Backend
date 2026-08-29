from sqlalchemy import create_engine, text
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    templates = conn.execute(text("""
        SELECT id, bank_id, form_type, field_mapping
        FROM bank_form_templates
        WHERE bank_id = 8
    """)).mappings().all()
    for t in templates:
        fm = t['field_mapping']
        if isinstance(fm, str):
            fm = json.loads(fm)
        print(f"--- Template #{t['id']} Checkbox fields: ---")
        for f in fm:
            if f.get('field_type') == 'checkbox' or 'Check' in f.get('pdf_field_name', ''):
                print(f"  PDF Field: '{f.get('pdf_field_name')}' | Label: '{f.get('label')}' | mapped_to: '{f.get('mapped_to')}'")
