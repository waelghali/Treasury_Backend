import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# Search for templates with placeholders or typo in subject in system_notifications
sql = text("""
    SELECT id, title, content 
    FROM system_notifications 
    WHERE title ILIKE '%Urgrnt%' 
       OR content ILIKE '%Escalation notification content%';
""")
try:
    res = db.execute(sql).fetchall()
    if not res:
        print("No system notifications found matching the criteria.")
    else:
        print("Matching system notifications found:")
        for row in res:
            print(f"ID: {row[0]}")
            print(f"Title: {row[1]}")
            print(f"Content: {row[2]}")
            print("-" * 40)
except Exception as e:
    print(f"Error searching system_notifications: {e}")
