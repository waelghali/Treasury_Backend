import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine, text
import pypdf
import io
import json

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    req = conn.execute(text("SELECT id, serial_number, requires_special_wording, is_cross_border FROM issuance_requests WHERE id = 155")).mappings().first()
    print("Request 155:", dict(req))
    
    tmpl = conn.execute(text("SELECT id, file_path, field_mapping FROM bank_form_templates WHERE bank_id = 8")).mappings().first()
    fm = tmpl['field_mapping']
    if isinstance(fm, str):
        fm = json.loads(fm)
    print("\nFormat & Category Checkboxes in field_mapping:")
    for f in fm:
        if any(w in f.get('mapped_to', '') for w in ['format', 'cross', 'local', 'special']):
            print(" ", f.get('pdf_field_name'), "->", f.get('mapped_to'), f.get('label'))
