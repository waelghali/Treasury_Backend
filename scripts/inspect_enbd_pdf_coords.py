import sys
sys.path.insert(0, '.')
import pypdf
from sqlalchemy import create_engine, text
import json

sys.stdout.reconfigure(encoding='utf-8')

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
eng = create_engine(STAGING_DB_URL)

with eng.connect() as conn:
    tmpl = conn.execute(text("SELECT id, file_path, field_mapping FROM bank_form_templates WHERE bank_id = 8")).mappings().first()

# Let's inspect the local uploaded template or download from GCS/local if available
import glob
files = glob.glob("**/*enbd*.pdf", recursive=True) + glob.glob("**/*ENBD*.pdf", recursive=True)
print("Found PDF files:", files)

if files:
    pdf_path = files[0]
    reader = pypdf.PdfReader(pdf_path)
    print(f"\nAnalyzing PDF: {pdf_path}")
    for page_idx, page in enumerate(reader.pages):
        print(f"\n--- Page {page_idx + 1} Form Fields ---")
        if "/Annots" in page:
            for annot in page["/Annots"]:
                obj = annot.get_object()
                name = obj.get("/T")
                rect = obj.get("/Rect")
                ft = obj.get("/FT")
                if name:
                    print(f"  Field: {name:15} | Type: {ft} | Rect: {rect}")
