
import sys
import os

# Add the parent directory to sys.path so we can import 'app'
sys.path.append("c:\\Grow")

from app.database import SessionLocal
from app.models import Template

db = SessionLocal()
try:
    templates = db.query(Template).filter(Template.action_type == "PRINT_ESCALATION").all()
    print(f"Found {len(templates)} templates for PRINT_ESCALATION")
    for t in templates:
        print(f"ID: {t.id}")
        print(f"Name: {t.name}")
        print(f"Subject: {t.subject}")
        print(f"Customer ID: {t.customer_id}")
        print(f"Is Notification: {t.is_notification_template}")
        print("--- Content ---")
        print(t.content)
        print("---------------")
finally:
    db.close()
