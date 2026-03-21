import sys
sys.path.insert(0, r'c:\Grow')
from app.database import engine
from sqlalchemy import text
conn = engine.connect()
conn.execute(text("ALTER TABLE issued_lg_records ADD COLUMN IF NOT EXISTS bank_inquiry_log JSONB DEFAULT '[]'"))
conn.commit()
conn.close()
print('Migration OK')
