import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine, text
import json

PROD_DB_URL = "postgresql://grow_xl8z_user:JUeq65Xm7OZQorlc32SyeysYbuemRdBD@dpg-d2brigndiees73f2d9dg-a.frankfurt-postgres.render.com/grow_xl8z"

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

def apply_to_prod():
    print(f"Connecting to: grow_xl8z ...")
    eng = create_engine(PROD_DB_URL, pool_pre_ping=True)
    
    with eng.connect() as conn:
        bank_id = conn.execute(text("SELECT id FROM banks WHERE name ILIKE '%emirates%' OR name ILIKE '%enbd%' LIMIT 1")).scalar()
        print(f"Found ENBD Bank ID: {bank_id}")
        
        if not bank_id:
            print("No bank found for ENBD, checking templates directly...")
            templates = conn.execute(text("SELECT id, bank_id, field_mapping FROM bank_form_templates")).mappings().all()
        else:
            templates = conn.execute(text("SELECT id, bank_id, field_mapping FROM bank_form_templates WHERE bank_id = :bid"), {"bid": bank_id}).mappings().all()
            
        print(f"Found {len(templates)} templates to update.")
        for t in templates:
            fm = t['field_mapping']
            if isinstance(fm, str):
                fm = json.loads(fm)
            elif fm is None:
                fm = []
            
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
            print(f"[OK] Template #{t['id']} updated successfully on grow_xl8z!")
            
        # Verify the updated mapping on grow_xl8z
        print("\n--- Verifying Updated Template Field Mapping ---")
        v_templates = conn.execute(text("SELECT id, field_mapping FROM bank_form_templates WHERE bank_id = :bid"), {"bid": bank_id}).mappings().all()
        for vt in v_templates:
            vfm = vt['field_mapping']
            if isinstance(vfm, str):
                vfm = json.loads(vfm)
            for f in vfm:
                if f.get('pdf_field_name') in CHECKBOX_MAP:
                    print(f"  {f.get('pdf_field_name'):8} -> {f.get('mapped_to'):30} ({f.get('label')})")

if __name__ == "__main__":
    apply_to_prod()
