import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# Check the enum values for migration_typeenum in PostgreSQL
sql = text("""
    SELECT enumlabel 
    FROM pg_enum 
    JOIN pg_type ON pg_enum.enumtypid = pg_type.oid 
    WHERE pg_type.typname = 'migration_typeenum';
""")
try:
    res = db.execute(sql).fetchall()
    print("Migration Enum values in PostgreSQL:")
    for row in res:
        print(f"- {row[0]}")
except Exception as e:
    print(f"Error checking enum: {e}")
