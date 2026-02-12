import os
import sys
import re  # Added for filename parsing
from dotenv import load_dotenv
from typing import Iterator

# Calculate the project root directory
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if os.path.basename(script_dir) not in ['app', 'Other']:
    project_root = script_dir

sys.path.insert(0, project_root)

from app.database import get_db
from app.models import Template
from sqlalchemy.orm import Session
import traceback

load_dotenv()

TEMPLATES_FOLDER = os.path.join(project_root, "app", "templates")

def get_template_files() -> Iterator[os.DirEntry]:
    """Iterates over all HTML files in the templates folder."""
    if not os.path.exists(TEMPLATES_FOLDER):
        print(f"Error: Template folder not found at {TEMPLATES_FOLDER}")
        return
    
    with os.scandir(TEMPLATES_FOLDER) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.endswith("_template.html"):
                yield entry

def update_all_templates_sync():
    db_session: Session = None
    try:
        db_session = next(get_db())
        print(f"Starting to update templates from folder: {TEMPLATES_FOLDER}")

        for template_file in get_template_files():
            try:
                filename = template_file.name
                customer_id = None
                action_type = None
                is_global = True

                # Check if file follows Customer naming: CustomerID_##_ACTION_template.html
                customer_match = re.match(r"CustomerID_(\d+)_(.*)_template\.html", filename)
                
                if customer_match:
                    customer_id = int(customer_match.group(1))
                    action_type = customer_match.group(2)
                    is_global = False
                    print(f"-> Detected CUSTOMER template (ID: {customer_id}, Action: {action_type})")
                else:
                    # Fallback to Global naming: ACTION_template.html
                    action_type = filename.replace("_template.html", "")
                    is_global = True
                    print(f"-> Detected GLOBAL template (Action: {action_type})")

                with open(template_file.path, "r", encoding="utf-8") as f:
                    new_html_content = f.read()

                # Build the query dynamically based on global vs customer
                query = db_session.query(Template).filter(
                    Template.action_type == action_type,
                    Template.is_notification_template == False,
                    Template.is_global == is_global,
                    Template.is_deleted == False
                )

                if not is_global:
                    query = query.filter(Template.customer_id == customer_id)

                template_to_update = query.first()

                if template_to_update:
                    if template_to_update.content != new_html_content:
                        template_to_update.content = new_html_content
                        db_session.add(template_to_update)
                        owner_info = f"Customer {customer_id}" if not is_global else "Global"
                        print(f"   - Updated content for {owner_info} template: {template_to_update.name}")
                    else:
                        print(f"   - Content identical, skipping.")
                else:
                    print(f"   - Error: No matching template found in DB for {filename}")

            except Exception as e:
                print(f"An error occurred while processing '{template_file.name}': {e}")

        db_session.commit()
        print("\nSuccessfully processed all template updates.")

    except Exception as e:
        if db_session: db_session.rollback()
        print(f"Critical error: {e}")
        traceback.print_exc()
    finally:
        if db_session: db_session.close()

if __name__ == "__main__":
    update_all_templates_sync()