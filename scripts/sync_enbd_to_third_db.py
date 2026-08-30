import sys
sys.path.insert(0, '.')
from sqlalchemy import create_engine, text
import json

PROD_DB_URL = "postgresql://grow_xl8z_user:JUeq65Xm7OZQorlc32SyeysYbuemRdBD@dpg-d2brigndiees73f2d9dg-a.frankfurt-postgres.render.com/grow_xl8z"
STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

eng_prod = create_engine(PROD_DB_URL, pool_pre_ping=True)
eng_stag = create_engine(STAGING_DB_URL, pool_pre_ping=True)

with eng_stag.connect() as conn_s:
    st_templates = conn_s.execute(text("SELECT * FROM bank_form_templates WHERE bank_id = 8 AND is_deleted = FALSE")).mappings().all()
    print(f"Found {len(st_templates)} active ENBD templates in Staging:")
    
    with eng_prod.connect() as conn_p:
        for st in st_templates:
            existing = conn_p.execute(text("SELECT id FROM bank_form_templates WHERE bank_id = 8 AND name = :name"), {"name": st['name']}).scalar()
            if not existing:
                conn_p.execute(text("""
                    INSERT INTO bank_form_templates (
                        bank_id, name, version, form_type, form_role, form_language, priority,
                        file_path, original_filename, field_mapping,
                        is_active, is_deleted, created_at, updated_at
                    ) VALUES (
                        :bank_id, :name, :version, :form_type, :form_role, :form_language, :priority,
                        :file_path, :original_filename, :field_mapping,
                        :is_active, :is_deleted, NOW(), NOW()
                    )
                """), {
                    "bank_id": st['bank_id'],
                    "name": st['name'],
                    "version": st['version'],
                    "form_type": st['form_type'],
                    "form_role": st['form_role'] or 'PRIMARY_ISSUER',
                    "form_language": st.get('form_language') or 'BILINGUAL',
                    "priority": st.get('priority') or 10,
                    "file_path": st['file_path'],
                    "original_filename": st['original_filename'],
                    "field_mapping": json.dumps(st['field_mapping']) if isinstance(st['field_mapping'], (dict, list)) else st['field_mapping'],
                    "is_active": st['is_active'],
                    "is_deleted": st['is_deleted'],
                })
                conn_p.commit()
                print(f" [CREATED] Inserted verified template '{st['name']}' into grow_xl8z database!")
            else:
                conn_p.execute(text("""
                    UPDATE bank_form_templates SET
                        field_mapping = :field_mapping,
                        form_language = :form_language,
                        priority = :priority,
                        is_active = :is_active,
                        is_deleted = :is_deleted,
                        updated_at = NOW()
                    WHERE id = :id
                """), {
                    "field_mapping": json.dumps(st['field_mapping']) if isinstance(st['field_mapping'], (dict, list)) else st['field_mapping'],
                    "form_language": st.get('form_language') or 'BILINGUAL',
                    "priority": st.get('priority') or 10,
                    "is_active": st['is_active'],
                    "is_deleted": st['is_deleted'],
                    "id": existing
                })
                conn_p.commit()
                print(f" [UPDATED] Synced verified template #{existing} on grow_xl8z database!")

with eng_prod.connect() as conn_p:
    v = conn_p.execute(text("SELECT id, bank_id, name, form_type, is_active, priority, form_language FROM bank_form_templates WHERE bank_id = 8")).mappings().all()
    print(f"\nFinal ENBD templates in grow_xl8z: {len(v)}")
    for r in v:
        print("  ", dict(r))
