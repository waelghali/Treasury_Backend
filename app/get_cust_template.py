import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.models.models import CustomerTemplate

db = SessionLocal()
templates = db.query(CustomerTemplate).filter(CustomerTemplate.action_type == 'PRINT_ESCALATION').all()
for t in templates:
    print(f"Customer {t.customer_id} Template - Subject: {t.subject}, Content: {t.content}")
