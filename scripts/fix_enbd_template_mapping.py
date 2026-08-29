from sqlalchemy import create_engine, text
import json

STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
LOCAL_DB_URL = "postgresql://postgres:Voda%21%4012@localhost:5432/grow"

for name, url in [("STAGING", STAGING_DB_URL), ("LOCAL", LOCAL_DB_URL)]:
    try:
        eng = create_engine(url)
        with eng.connect() as conn:
            templates = conn.execute(text("SELECT id, field_mapping FROM bank_form_templates WHERE bank_id = 8")).mappings().all()
            for t in templates:
                fm = t['field_mapping']
                if isinstance(fm, str):
                    fm = json.loads(fm)
                
                updated = False
                for field in fm:
                    pname = field.get('pdf_field_name')
                    # Fix Check2 and Check3 mapping for ENBD
                    if pname == 'Check2':
                        if field.get('mapped_to') != 'lg_type_is_advance_conditioned':
                            field['mapped_to'] = 'lg_type_is_advance_conditioned'
                            field['label'] = 'Conditioned Advance Payment / دفعة مقدمه مشروطة'
                            updated = True
                    elif pname == 'Check3':
                        if field.get('mapped_to') != 'lg_type_is_performance':
                            field['mapped_to'] = 'lg_type_is_performance'
                            field['label'] = 'Performance / نهائي'
                            updated = True
                    elif pname == 'Check4':
                        if field.get('mapped_to') != 'lg_type_is_advance_unconditioned':
                            field['mapped_to'] = 'lg_type_is_advance_unconditioned'
                            field['label'] = 'Unconditioned Advance Payment / دفعة مقدمه غير مشروطة'
                            updated = True

                if updated:
                    conn.execute(
                        text("UPDATE bank_form_templates SET field_mapping = :fm WHERE id = :tid"),
                        {"fm": json.dumps(fm), "tid": t['id']}
                    )
                    conn.commit()
                    print(f"[{name}] Successfully updated Template #{t['id']} field_mapping!")
                else:
                    print(f"[{name}] Template #{t['id']} already has correct mapping.")
    except Exception as e:
        print(f"[{name}] Error:", e)
