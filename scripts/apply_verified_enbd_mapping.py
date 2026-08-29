import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine, text
import json

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
LOCAL_DB_URL = "postgresql://postgres:Voda%21%4012@localhost:5432/grow"

CHECKBOX_MAP = {
    "Check1": {"mapped_to": "lg_type_is_bid_bond", "label": "Bid Bond / ابتدائي"},
    "Check2": {"mapped_to": "lg_type_is_performance", "label": "Performance / نهائي"},
    "Check3": {"mapped_to": "lg_type_is_advance_conditioned", "label": "Conditioned Advance Payment / دفعة مقدمه مشروطة"},
    "Check4": {"mapped_to": "lg_type_is_advance_unconditioned", "label": "Unconditioned Advance Payment / دفعة مقدمه غير مشروطة"},
    "Check5": {"mapped_to": "lg_type_is_payment_guarantee", "label": "Payment Guarantee / ضمان دفع"},
    "Check6": {"mapped_to": "lg_format_is_bank_standard", "label": "Bank's Standard Format / نموذج البنك القياسي"},
    "Check7": {"mapped_to": "lg_format_is_special", "label": "As Per Attached Special Format / حسب النموذج المرفق"},
    "Check8": {"mapped_to": "is_local_lg", "label": "Local LG / خطاب ضمان محلي"},
    "Check9": {"mapped_to": "is_cross_border", "label": "Cross Border LG / خطاب ضمان خارجي"},
}

for name, url in [("STAGING", STAGING_DB_URL), ("LOCAL", LOCAL_DB_URL)]:
    try:
        eng = create_engine(url)
        with eng.connect() as conn:
            templates = conn.execute(text("SELECT id, field_mapping FROM bank_form_templates WHERE bank_id = 8")).mappings().all()
            for t in templates:
                fm = t['field_mapping']
                if isinstance(fm, str):
                    fm = json.loads(fm)
                
                for field in fm:
                    pname = field.get('pdf_field_name')
                    if pname in CHECKBOX_MAP:
                        field['mapped_to'] = CHECKBOX_MAP[pname]['mapped_to']
                        field['label'] = CHECKBOX_MAP[pname]['label']
                        field['field_type'] = 'checkbox'
                        field['fill_strategy'] = 'boolean_match'

                conn.execute(
                    text("UPDATE bank_form_templates SET field_mapping = :fm WHERE id = :tid"),
                    {"fm": json.dumps(fm), "tid": t['id']}
                )
                conn.commit()
                print(f"[{name}] Successfully applied verified geometric checkbox mapping to Template #{t['id']}!")
    except Exception as e:
        print(f"[{name}] Error:", e)
