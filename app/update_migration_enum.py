import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# Add 'ISSUANCE_RECORD' to the migration_typeenum enum type in PostgreSQL
# PostgreSQL enums require ALTER TYPE ADD VALUE
sql = text("ALTER TYPE migration_typeenum ADD VALUE IF NOT EXISTS 'ISSUANCE_RECORD';")

try:
    # ALTER TYPE ... ADD VALUE cannot run in a transaction block (autocommit must be on)
    with db.get_bind().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(sql)
    print("SUCCESS: Added 'ISSUANCE_RECORD' to migration_typeenum.")
except Exception as e:
    # If it already exists or other error
    print(f"Error updating enum: {e}")
finally:
    db.close()
