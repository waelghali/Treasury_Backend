import sys
sys.path.insert(0, '.')
import psycopg2

PROD_DB = 'postgresql://grow_xl8z_user:JUeq65Xm7OZQorlc32SyeysYbuemRdBD@dpg-d2brigndiees73f2d9dg-a.frankfurt-postgres.render.com/grow_xl8z'
STAGING_DB = 'postgresql://treasury_staging_db_user:yiIxGco2LgNhZ2EWcdP4G9617dJ8hPwK@dpg-da8miv5g1s2s739rmm6g-a.frankfurt-postgres.render.com/treasury_staging_db'
LOCAL_DB = 'postgresql://postgres:Voda%21%4012@localhost:5432/grow'

configs_to_seed = [
    ('LOGIN_MAX_FAILED_ATTEMPTS', '5', '1', '20', 'Maximum failed login attempts before lockout', 'attempts', '["AUTH"]'),
    ('LOGIN_LOCKOUT_DURATION_MINUTES', '15', '1', '1440', 'Duration of account lockout after exceeding failed attempts', 'minutes', '["AUTH"]'),
    ('OTP_MAX_FAILED_ATTEMPTS', '5', '1', '10', 'Maximum failed OTP verification attempts', 'attempts', '["AUTH", "PUBLIC_ISSUANCE"]'),
    ('OTP_LOCKOUT_DURATION_MINUTES', '15', '1', '1440', 'Lockout duration after exceeding OTP attempts', 'minutes', '["AUTH", "PUBLIC_ISSUANCE"]'),
    ('LG_COPY_VERIFICATION_REQUIRED', 'true', None, None, 'Whether LG physical copy must be verified before handover', None, '["ISSUANCE"]'),
    ('INBOX_POLL_INTERVAL_MINUTES', '15', '5', '1440', 'Smart Inbox polling interval', 'minutes', '["SMART_INBOX"]'),
    ('INBOX_DEFAULT_POSITION_REQUEST_DAY', '25', '1', '28', 'Default day of month to request position confirmation', 'day', '["SMART_INBOX"]'),
    ('INBOX_RETENTION_MONTHS', '24', '1', '120', 'Retention period for parsed inbox emails and attachments', 'months', '["SMART_INBOX"]'),
    ('INBOX_GENERIC_DOMAIN_BLACKLIST', 'gmail.com,yahoo.com,hotmail.com,outlook.com', None, None, 'Blacklisted generic domains for Smart Inbox banking matching', None, '["SMART_INBOX"]'),
]

for name, url in [('Local DB (grow)', LOCAL_DB), ('Staging DB (treasury_staging_db)', STAGING_DB), ('Prod DB (grow_xl8z)', PROD_DB)]:
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
