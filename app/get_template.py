import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.models.models import Template

db = SessionLocal()
template = db.query(Template).filter(Template.action_type == 'PRINT_ESCALATION').first()
if template:
    print("Subject:", template.subject)
    print("Content:", template.content)
else:
    print("Template not found")
