import sys
sys.path.insert(0, r'c:\Grow')
from app.database import engine
from sqlalchemy import text
conn = engine.connect()
conn.execute(text("ALTER TABLE issuance_requests ADD COLUMN IF NOT EXISTS lg_language VARCHAR DEFAULT 'AR' NOT NULL"))
conn.execute(text("ALTER TABLE bank_form_templates ADD COLUMN IF NOT EXISTS form_language VARCHAR DEFAULT 'BILINGUAL' NOT NULL"))
conn.commit()
conn.close()
print('Migration OK: lg_language + form_language added')
