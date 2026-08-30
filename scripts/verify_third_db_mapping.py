from sqlalchemy import create_engine, text
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

PROD_DB_URL = "postgresql://grow_xl8z_user:JUeq65Xm7OZQorlc32SyeysYbuemRdBD@dpg-d2brigndiees73f2d9dg-a.frankfurt-postgres.render.com/grow_xl8z"
eng = create_engine(PROD_DB_URL)

with eng.connect() as conn:
    tmpl = conn.execute(text("SELECT id, name, field_mapping FROM bank_form_templates WHERE bank_id = 8")).mappings().first()
    fm = tmpl['field_mapping']
    if isinstance(fm, str):
        fm = json.loads(fm)
    print(f"Verification on grow_xl8z Template #{tmpl['id']} ({tmpl['name']}):")
    for f in fm:
        if "Check" in f.get("pdf_field_name", ""):
            print(f"  {f['pdf_field_name']:8} -> {f['mapped_to']:30} ({f['label']})")
