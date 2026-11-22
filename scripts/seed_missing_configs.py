import os
import sys
sys.path.insert(0, '.')
import psycopg2

PROD_DB = os.environ.get('PROD_DB_URL')
STAGING_DB = os.environ.get('STAGING_DB_URL')
LOCAL_DB = os.environ.get('LOCAL_DB_URL') or os.environ.get('DATABASE_URL')

target_dbs = [
    (name, url)
    for name, url in [
        ('Local DB (grow)', LOCAL_DB),
        ('Staging DB (treasury_staging_db)', STAGING_DB),
        ('Prod DB (grow_xl8z)', PROD_DB),
    ]
    if url
]
if not target_dbs:
    raise RuntimeError("Set at least one of LOCAL_DB_URL, STAGING_DB_URL, or PROD_DB_URL in environment.")

configs_to_seed = [
    ('LOGIN_MAX_FAILED_ATTEMPTS', '5', '1', '20', 'Maximum failed login attempts before lockout', 'attempts', '["AUTH"]'),
    ('LOGIN_LOCKOUT_DURATION_MINUTES', '15', '1', '1440', 'Duration of account lockout after exceeding failed attempts', 'minutes', '["AUTH"]'),
    ('OTP_MAX_FAILED_ATTEMPTS', '5', '1', '10', 'Maximum failed OTP verification attempts', 'attempts', '["AUTH", "PUBLIC_ISSUANCE"]'),
    ('OTP_LOCKOUT_DURATION_MINUTES', '15', '1', '1440', 'Lockout duration after exceeding OTP attempts', 'minutes', '["AUTH", "PUBLIC_ISSUANCE"]'),
    ('LG_COPY_VERIFICATION_REQUIRED', 'true', 'False', 'True', 'Whether LG physical copy must be verified before handover', 'boolean', '["ISSUANCE"]'),
    ('INBOX_POLL_INTERVAL_MINUTES', '15', '5', '1440', 'Smart Inbox polling interval', 'minutes', '["SMART_INBOX"]'),
    ('INBOX_DEFAULT_POSITION_REQUEST_DAY', '25', '1', '28', 'Default day of month to request position confirmation', 'days', '["SMART_INBOX"]'),
    ('INBOX_RETENTION_MONTHS', '24', '1', '120', 'Retention period for parsed inbox emails and attachments', 'months', '["SMART_INBOX"]'),
    ('INBOX_GENERIC_DOMAIN_BLACKLIST', 'gmail.com,yahoo.com,hotmail.com,outlook.com', None, None, 'Blacklisted generic domains for Smart Inbox banking matching', 'string', '["SMART_INBOX"]'),
]

for name, url in target_dbs:
    print(f"\n--- Checking {name} ---")
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()
        
        for key, val_def, val_min, val_max, desc, unit, tags in configs_to_seed:
            cur.execute("SELECT id FROM global_configurations WHERE key = %s;", (key,))
            row = cur.fetchone()
            if not row:
                cur.execute("""
                    INSERT INTO global_configurations (key, value_default, value_min, value_max, description, unit, module_tags, is_deleted, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::json, false, NOW(), NOW());
                """, (key, val_def, val_min, val_max, desc, unit, tags))
                print(f"  + Inserted config: {key} = {val_def}")
            else:
                print(f"  [OK] {key} exists")
                
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error on {name}: {e}")
