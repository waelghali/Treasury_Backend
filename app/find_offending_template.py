import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# Search for templates with placeholders or typo in subject
sql = text("""
    SELECT id, name, action_type, subject, content 
    FROM templates 
    WHERE subject ILIKE '%Urgrnt%' 
       OR content ILIKE '%Escalation notification content%';
""")
try:
    res = db.execute(sql).fetchall()
    if not res:
        print("No templates found matching the criteria.")
    else:
        print("Matching templates found:")
        for row in res:
            print(f"ID: {row[0]}")
            print(f"Name: {row[1]}")
            print(f"Action Type: {row[2]}")
            print(f"Subject: {row[3]}")
            print(f"Content: {row[4]}")
            print("-" * 40)
except Exception as e:
    print(f"Error searching templates: {e}")
