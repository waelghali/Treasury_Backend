import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.models_issuance import IssuanceRequest, BankFormTemplate
from app.core.pdf_form_filler import build_request_data_dict, fill_pdf_form
import pypdf
import io

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)
SessionLocal = sessionmaker(bind=eng)
db = SessionLocal()

req = db.query(IssuanceRequest).filter(IssuanceRequest.id == 155).first()
tmpl = db.query(BankFormTemplate).filter(BankFormTemplate.bank_id == 8, BankFormTemplate.is_active == True, BankFormTemplate.is_deleted == False).first()

pdf_bytes = tmpl.template_bytes or tmpl.pdf_content or tmpl.file_data or None
if not pdf_bytes:
    # check columns
    for attr in ['template_pdf', 'template_file', 'pdf_bytes', 'template_data']:
        if hasattr(tmpl, attr):
            pdf_bytes = getattr(tmpl, attr)
            break

data = build_request_data_dict(req, db, bank_id=8)
filled_bytes = fill_pdf_form(pdf_bytes, tmpl.field_mapping, data)
reader = pypdf.PdfReader(io.BytesIO(filled_bytes))
fields = reader.get_fields()
print("Filled PDF Checkbox states:")
for k in ["Check1", "Check2", "Check3", "Check4", "Check5", "Check6", "Check7", "Check8", "Check9"]:
    if k in fields:
        print(f"  {k}: {fields[k].get('/V')}")
