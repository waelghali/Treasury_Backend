import sys
sys.path.insert(0, 'c:/Grow')
from app.database import engine
from sqlalchemy import text

conn = engine.connect()

# First, add the enum value to the PostgreSQL globalconfigkey enum type
conn.execute(text(
    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'DOC_MANDATORY_LG_HANDOVER'"
))
conn.commit()

# Now insert the config row
conn = engine.connect()
conn.execute(text(
    "INSERT INTO global_configurations (key, value_default, unit, description) "
    "VALUES ('DOC_MANDATORY_LG_HANDOVER', 'false', 'boolean', "
    "'Require a signed receiving copy document when recording LG handover to recipient') "
    "ON CONFLICT (key) DO NOTHING"
))
conn.commit()
print('Inserted DOC_MANDATORY_LG_HANDOVER config key')
