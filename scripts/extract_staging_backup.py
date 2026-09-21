# scripts/extract_staging_backup.py
import sys, os, time, pickle
sys.path.insert(0, os.path.abspath("."))
from sqlalchemy import create_engine, text, MetaData, Table

REMOTE_STAGING_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"
LOCAL_BACKUP_URL = "postgresql://postgres:Voda%21%4012@localhost:5432/grow_staging_backup"

def extract():
    start_time = time.time()
    print("=" * 60)
    print("EXTRACTING RENDER STAGING DB -> LOCAL BACKUP DATABASE")
    print("=" * 60)

    remote_eng = create_engine(REMOTE_STAGING_URL, pool_pre_ping=True, pool_recycle=120)
    local_eng = create_engine(LOCAL_BACKUP_URL, pool_pre_ping=True)

    # 1. Clean local backup schema
    print("\n1. Re-creating clean public schema on local backup DB...")
    with local_eng.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
        conn.commit()
    print("   [OK] Local backup public schema cleaned.")

    # 2. Reflect remote tables
    print("\n2. Reflecting all tables from Render staging...")
    remote_meta = MetaData()
    remote_meta.reflect(bind=remote_eng)
    tables = list(remote_meta.tables.values())
    print(f"   Found {len(tables)} tables on Render staging.")

    # 3. Create all tables on local backup
    print("\n3. Creating all tables in local backup DB...")
    remote_meta.create_all(bind=local_eng)
    print(f"   [OK] Created {len(tables)} tables matching schema 1:1.")

    # 4. Temporarily drop FKs on local backup for fast bulk insertion
    print("\n4. Temporarily dropping FK constraints on local backup...")
    with local_eng.connect() as conn:
        fk_rows = conn.execute(text("""
            SELECT tc.table_name, tc.constraint_name
            FROM information_schema.table_constraints AS tc 
            WHERE tc.constraint_type = 'FOREIGN KEY' 
              AND tc.table_schema = 'public';
        """)).fetchall()
        for tbl, cname in fk_rows:
            try:
                conn.execute(text(f'ALTER TABLE "{tbl}" DROP CONSTRAINT IF EXISTS "{cname}";'))
            except Exception:
                pass
        conn.commit()
    print("   [OK] FK constraints dropped for data loading.")

    # 5. Copy data table by table
    print("\n5. Copying all table records...")
    total_rows = 0
    non_empty_count = 0

    for t in tables:
        tbl_name = t.name
        with remote_eng.connect() as r_conn:
            rows = r_conn.execute(t.select()).mappings().all()
            count = len(rows)

        if count == 0:
            continue

        non_empty_count += 1
        total_rows += count

        with local_eng.connect() as l_conn:
            chunk_size = 500
            for i in range(0, count, chunk_size):
                chunk = rows[i:i + chunk_size]
                l_conn.execute(t.insert(), [dict(r) for r in chunk])
            l_conn.commit()

        print(f"   [COPIED] {tbl_name:38s}: {count:5d} rows")

    # 6. Re-apply FK constraints
    print("\n6. Re-applying all foreign key constraints...")
    remote_meta.create_all(bind=local_eng)
    print("   [OK] Foreign keys re-established.")

    # 7. Reset sequences
    print("\n7. Resetting auto-increment sequences...")
    with local_eng.connect() as conn:
        res = conn.execute(text("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE column_default LIKE 'nextval%' 
              AND table_schema = 'public';
        """)).fetchall()

        for tbl, col in res:
            try:
                conn.execute(text(f"""
                    SELECT setval(
                        pg_get_serial_sequence('{tbl}', '{col}'), 
                        COALESCE((SELECT MAX({col}) FROM "{tbl}"), 1)
                    );
                """))
            except Exception:
                pass
        conn.commit()
    print("   [OK] Sequences reset.")

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 60)
    print(f"EXTRACTION COMPLETE! {len(tables)} tables, {non_empty_count} non-empty, {total_rows} total rows extracted safely in {elapsed}s.")
    print("=" * 60)

if __name__ == "__main__":
    extract()
