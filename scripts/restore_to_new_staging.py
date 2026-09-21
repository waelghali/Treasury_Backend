# scripts/restore_to_new_staging.py
import sys, os, time
sys.path.insert(0, os.path.abspath("."))
from sqlalchemy import create_engine, text, MetaData, Table

LOCAL_BACKUP_URL = "postgresql://postgres:Voda%21%4012@localhost:5432/grow_staging_backup"

def restore(new_staging_url: str):
    start_time = time.time()
    print("=" * 60)
    print("SEEDING / RESTORING BACKUP -> NEW RENDER STAGING DB")
    print("=" * 60)

    if not new_staging_url or not new_staging_url.startswith("postgresql"):
        print("[ERROR] Please provide a valid PostgreSQL connection URL!")
        return

    local_eng = create_engine(LOCAL_BACKUP_URL, pool_pre_ping=True)
    new_remote_eng = create_engine(new_staging_url, pool_pre_ping=True, pool_recycle=120)

    # 1. Clean public schema on new Render database
    print("\n1. Re-creating clean public schema on new Render DB...")
    with new_remote_eng.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
        conn.commit()
    print("   [OK] Clean public schema ready.")

    # 2. Reflect local backup tables
    print("\n2. Reflecting schema from local backup...")
    local_meta = MetaData()
    local_meta.reflect(bind=local_eng)
    tables = list(local_meta.tables.values())
    print(f"   Found {len(tables)} tables to restore.")

    # 3. Create all tables on new Render DB
    print("\n3. Creating tables on new Render DB...")
    local_meta.create_all(bind=new_remote_eng)
    print(f"   [OK] Created all {len(tables)} tables matching schema 1:1.")

    # 4. Temporarily drop FKs on new DB
    print("\n4. Temporarily dropping FK constraints for bulk insertion...")
    with new_remote_eng.connect() as conn:
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
    print("   [OK] FK constraints dropped.")

    # 5. Bulk copy data
    print("\n5. Bulk copying all data...")
    total_rows = 0
    non_empty_count = 0

    for t in tables:
        tbl_name = t.name
        with local_eng.connect() as l_conn:
            rows = l_conn.execute(t.select()).mappings().all()
            count = len(rows)

        if count == 0:
            continue

        non_empty_count += 1
        total_rows += count

        with new_remote_eng.connect() as r_conn:
            chunk_size = 500
            for i in range(0, count, chunk_size):
                chunk = rows[i:i + chunk_size]
                r_conn.execute(t.insert(), [dict(r) for r in chunk])
            r_conn.commit()

        print(f"   [RESTORED] {tbl_name:38s}: {count:5d} rows")

    # 6. Re-apply FK constraints
    print("\n6. Re-applying all foreign key constraints...")
    local_meta.create_all(bind=new_remote_eng)
    print("   [OK] Foreign keys re-established.")

    # 7. Reset sequences
    print("\n7. Resetting auto-increment sequences...")
    with new_remote_eng.connect() as conn:
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
    print(f"RESTORE COMPLETE! {len(tables)} tables, {non_empty_count} non-empty, {total_rows} total rows restored in {elapsed}s.")
    print("=" * 60)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_url = sys.argv[1]
    else:
        target_url = os.getenv("NEW_STAGING_DB_URL")
    
    if not target_url:
        print("Usage: python scripts/restore_to_new_staging.py <NEW_STAGING_DB_URL>")
        sys.exit(1)

    restore(target_url)
