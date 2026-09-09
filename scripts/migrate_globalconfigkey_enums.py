import sys
sys.path.insert(0, '.')
import psycopg2
from app.constants import GlobalConfigKey

LOCAL_DB = 'postgresql://postgres:Voda%21%4012@localhost:5432/grow'
STAGING_DB = 'postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db'
PROD_DB = 'postgresql://grow_xl8z_user:JUeq65Xm7OZQorlc32SyeysYbuemRdBD@dpg-d2brigndiees73f2d9dg-a.frankfurt-postgres.render.com/grow_xl8z'

dbs = [
    ('Local DB (grow)', LOCAL_DB),
    ('Staging DB (treasury_staging_db)', STAGING_DB),
    ('Production DB (grow_xl8z)', PROD_DB)
]

for db_name, db_url in dbs:
    print(f"\n--- Migrating {db_name} ---")
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Get existing enum values
        cur.execute("SELECT enumlabel FROM pg_enum JOIN pg_type ON pg_enum.enumtypid = pg_type.oid WHERE pg_type.typname = 'globalconfigkey';")
        existing_labels = set(r[0] for r in cur.fetchall())
        
        added_count = 0
        for item in GlobalConfigKey:
            val = item.value
            if val not in existing_labels:
                cur.execute(f"ALTER TYPE globalconfigkey ADD VALUE '{val}';")
                print(f"  + Added enum value: {val}")
                added_count += 1
                
        print(f"Done for {db_name}: Added {added_count} new values.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error on {db_name}: {e}")
