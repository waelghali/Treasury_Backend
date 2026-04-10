import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
res = db.execute(text("SELECT id, customer_id, name, action_type, subject, content FROM templates WHERE subject ILIKE '%urgrnt%';")).fetchall()
for r in res:
    print("TYPO FOUND!", r)

res2 = db.execute(text("SELECT id, customer_id, name, action_type, subject, content FROM templates WHERE content ILIKE '%Escalation notification content%';")).fetchall()
for r in res2:
    print("CONTENT FOUND!", r)
