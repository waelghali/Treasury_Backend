import sys
import os

sys.path.insert(0, os.path.abspath('.'))

from sqlalchemy import text
from app.database import engine

def add_columns():
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE reconciliation_sessions ADD COLUMN completeness_status VARCHAR;"))
            print("Successfully added completeness_status")
        except Exception as e:
            print("Failed to add completeness_status:", e)
            
        try:
            conn.execute(text("ALTER TABLE reconciliation_sessions ADD COLUMN completeness_note TEXT;"))
            print("Successfully added completeness_note")
        except Exception as e:
            print("Failed to add completeness_note:", e)

if __name__ == "__main__":
    add_columns()
