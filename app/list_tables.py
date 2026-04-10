import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# List all tables
sql = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
res = db.execute(sql).fetchall()
print("Tables in the database:")
for row in res:
    print(f"- {row[0]}")
