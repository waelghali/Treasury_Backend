import os
from sqlalchemy import create_engine, text
import json

STAGING_DB_URL = os.environ["STAGING_DB_URL"]
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
