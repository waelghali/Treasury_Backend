import os
import sys
sys.path.insert(0, '.')
import psycopg2
from app.constants import GlobalConfigKey

LOCAL_DB = os.environ.get('LOCAL_DB_URL') or os.environ.get('DATABASE_URL')
STAGING_DB = os.environ.get('STAGING_DB_URL')
PROD_DB = os.environ.get('PROD_DB_URL')

dbs = [
    (name, url)
    for name, url in [
        ('Local DB (grow)', LOCAL_DB),
        ('Staging DB (treasury_staging_db)', STAGING_DB),
        ('Production DB (grow_xl8z)', PROD_DB),
    ]
    if url
]
if not dbs:
    raise RuntimeError("Set at least one of LOCAL_DB_URL, STAGING_DB_URL, or PROD_DB_URL in environment.")

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
