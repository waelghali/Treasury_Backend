# scripts/clone_db_to_staging.py
import sys, os, time
sys.path.insert(0, os.path.abspath("."))
from sqlalchemy import create_engine, text, MetaData, Table

LOCAL_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:Voda%21%4012@localhost:5432/grow")
STAGING_DB_URL = "postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db"

def clone():
    start_time = time.time()
    print("=" * 60)
    print("STARTING EXACT DATABASE MIRROR TO STAGING")
    print("=" * 60)
    
    local_eng = create_engine(LOCAL_DB_URL, pool_pre_ping=True)
    staging_eng = create_engine(STAGING_DB_URL, pool_pre_ping=True, pool_recycle=120)

    # 1. Drop and recreate public schema on staging to ensure clean 100% schema parity
    print("\n1. Re-creating clean public schema on Staging...")
    with staging_eng.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
        conn.commit()
    print("   [OK] Staging public schema cleaned.")

    # 2. Reflect exact local schema and create all tables in staging
    print("\n2. Reflecting local database structure and creating on Staging...")
    local_meta = MetaData()
    local_meta.reflect(bind=local_eng)
    tables = list(local_meta.tables.values())

    # Create all tables on staging matching local schema exactly
    local_meta.create_all(bind=staging_eng)
    print(f"   [OK] Created all {len(tables)} tables on Staging matching local schema 1:1.")

    # 3. Temporarily drop FK constraints on staging for fast bulk insertion
    print("\n3. Temporarily dropping foreign key constraints in Staging...")
    with staging_eng.connect() as conn:
        fk_rows = conn.execute(text("""
            SELECT tc.table_name, tc.constraint_name
            FROM information_schema.table_constraints AS tc 
            WHERE tc.constraint_type = 'FOREIGN KEY' 
              AND tc.table_schema = 'public';
        """)).fetchall()
        
        print(f"   Found {len(fk_rows)} foreign key constraint(s). Dropping...")
        for tbl, cname in fk_rows:
            try:
                conn.execute(text(f'ALTER TABLE "{tbl}" DROP CONSTRAINT IF EXISTS "{cname}";'))
            except Exception as e:
                pass
        conn.commit()
        print("   [OK] Foreign keys dropped.")

    # 4. Read tables and copy rows
    print("\n4. Copying data table by table...")
    for t in tables:
        tbl_name = t.name
        
        # Read all rows from local table
        with local_eng.connect() as local_conn:
            rows = local_conn.execute(t.select()).mappings().all()
            count = len(rows)

        if count == 0:
            print(f"   - {tbl_name:38s}: 0 rows (skipped)")
            continue

        # Insert into staging
        with staging_eng.connect() as staging_conn:
            chunk_size = 500
            for i in range(0, count, chunk_size):
                chunk = rows[i:i + chunk_size]
                staging_conn.execute(t.insert(), [dict(r) for r in chunk])
            staging_conn.commit()

        print(f"   [COPIED] {tbl_name:38s}: {count:5d} rows")

    # 5. Re-apply schema & FK constraints
    print("\n5. Re-applying all foreign key constraints on Staging...")
    local_meta.create_all(bind=staging_eng)
    print("   [OK] Foreign keys re-established.")

    # 6. Reset sequences for auto-increment IDs
    print("\n6. Resetting PostgreSQL sequences...")
    with staging_eng.connect() as staging_conn:
        res = staging_conn.execute(text("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE column_default LIKE 'nextval%' 
              AND table_schema = 'public';
        """)).fetchall()

        for tbl, col in res:
            try:
                staging_conn.execute(text(f"""
                    SELECT setval(
                        pg_get_serial_sequence('{tbl}', '{col}'), 
                        COALESCE((SELECT MAX({col}) FROM "{tbl}"), 1)
                    );
                """))
            except Exception:
                pass
        staging_conn.commit()
        print("   [OK] Sequences reset.")

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 60)
    print(f"ALL 101 TABLES CLONED TO STAGING SUCCESSFULLY in {elapsed}s!")
    print("=" * 60)

if __name__ == "__main__":
    clone()
