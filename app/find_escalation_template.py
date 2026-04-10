import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# Search both for name and action_type containing 'Escalation' or 'Print'
sql = text("SELECT id, name, action_type, subject, content FROM templates WHERE name ILIKE '%Escalation%' OR action_type ILIKE '%Escalation%' OR subject ILIKE '%Urgrnt%' OR content ILIKE '%Escalation notification content%';")
res = db.execute(sql).fetchall()

if not res:
    print("No templates found matching the criteria.")
else:
    for row in res:
        print(f"ID: {row[0]}")
        print(f"Name: {row[1]}")
        print(f"Action Type: {row[2]}")
        print(f"Subject: {row[3]}")
        print(f"Content: {row[4][:100]}...") # Print first 100 chars
        print("-" * 40)
