import sys
sys.path.insert(0, '.')
import io
import pypdf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.models_issuance import IssuanceRequest, BankFormTemplate
from app.core.pdf_form_filler import build_request_data_dict, fill_pdf_form

sys.stdout.reconfigure(encoding='utf-8')

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)
SessionLocal = sessionmaker(bind=eng)
db = SessionLocal()

req = db.query(IssuanceRequest).filter(IssuanceRequest.id == 155).first()
tmpl = db.query(BankFormTemplate).filter(BankFormTemplate.bank_id == 8, BankFormTemplate.is_active == True, BankFormTemplate.is_deleted == False).first()

with open("_legacy_archive/Banks_Forms/ENBD.pdf", "rb") as f:
    pdf_bytes = f.read()

data = build_request_data_dict(req, db, bank_id=8)
print("Request Flags:")
print("  lg_type:", data.get("lg_type"))
print("  lg_type_is_performance:", data.get("lg_type_is_performance"))
print("  lg_format_is_bank_standard:", data.get("lg_format_is_bank_standard"))
print("  lg_format_is_special:", data.get("lg_format_is_special"))
print("  is_local_lg:", data.get("is_local_lg"))
print("  is_cross_border:", data.get("is_cross_border"))

filled_bytes = fill_pdf_form(pdf_bytes, tmpl.field_mapping, data)
reader = pypdf.PdfReader(io.BytesIO(filled_bytes))
fields = reader.get_fields()

print("\n--- Physical Checkbox Values in Generated PDF ---")
names = {
    "Check1": "Bid Bond",
    "Check2": "Performance",
    "Check3": "Conditioned Advance Payment",
    "Check4": "Unconditioned Advance Payment",
    "Check5": "Payment Guarantee",
    "Check6": "Bank's Standard Format",
    "Check7": "As Per Attached Special Format",
    "Check8": "Local LG",
    "Check9": "Cross Border LG"
}
for k, label in names.items():
    val = fields[k].get('/V') if k in fields else 'N/A'
    status = " [CHECKED]" if val not in [None, '/Off', 'Off', ''] else " [EMPTY]"
    print(f"  {k:7} ({label:32}): {str(val):8}{status}")
