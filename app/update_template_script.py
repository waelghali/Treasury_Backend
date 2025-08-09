# update_template_script.py
import os
import sys
from dotenv import load_dotenv
from typing import Iterator

# Calculate the project root directory
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if os.path.basename(script_dir) not in ['app', 'Other']:
    project_root = script_dir

sys.path.insert(0, project_root)

# Import necessary components from your project using absolute imports
from app.database import get_db
from app.models import Template
from sqlalchemy.orm import Session
import traceback

# Load environment variables from .env file
load_dotenv()

# --- Define the path to your HTML template files ---
# This folder will contain all your HTML templates for letters
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
    """
    Synchronously updates the content of all letter templates in the database
    from HTML files found in the templates folder.
    """
    db_session: Session = None
    try:
        db_session = next(get_db())
        print(f"Starting to update templates from folder: {TEMPLATES_FOLDER}")

        for template_file in get_template_files():
            try:
                # Use a consistent naming convention to derive action_type
                # e.g., "LG_EXTENSION_template.html" -> "LG_EXTENSION"
                action_type = template_file.name.replace("_template.html", "")
                
                with open(template_file.path, "r", encoding="utf-8") as f:
                    new_html_content = f.read()

                print(f"-> Processing template file: {template_file.name} (Action: {action_type})")

                template_to_update = db_session.query(Template).filter(
                    Template.action_type == action_type,
                    Template.is_notification_template == False,
                    Template.is_global == True
                ).first()

                if template_to_update:
                    if template_to_update.content != new_html_content:
                        template_to_update.content = new_html_content
                        db_session.add(template_to_update)
                        print(f"   - Updated content for template ID: {template_to_update.id} ({template_to_update.name})")
                    else:
                        print(f"   - Content is identical for template ID: {template_to_update.id} ({template_to_update.name}), skipping update.")
                else:
                    print(f"   - Error: Template for action_type '{action_type}' not found in the database. Skipping.")

            except Exception as e:
                print(f"An error occurred while processing file '{template_file.name}': {e}")
                traceback.print_exc()

        # Commit all changes at once after the loop
        db_session.commit()
        print("\nSuccessfully processed and committed all template updates.")

    except Exception as e:
        if db_session:
            db_session.rollback()
        print(f"A critical error occurred during the batch template update: {e}")
        traceback.print_exc()
    finally:
        if db_session:
            db_session.close()

if __name__ == "__main__":
    print("Starting batch template update script...")
    update_all_templates_sync()
    print("Script finished.")
