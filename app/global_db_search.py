import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# List of strings to search for
search_strings = ["Urgrnt", "Escalation notification content"]

# Get all tables and their columns
sql_tables = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = db.execute(sql_tables).fetchall()

print(f"Searching for {search_strings} in all public tables...")

for table_row in tables:
    table_name = table_row[0]
    
    # Get all character columns for this table
    sql_cols = text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
          AND (data_type LIKE 'char%' OR data_type LIKE 'text%' OR data_type = 'jsonb');
    """)
    cols = db.execute(sql_cols).fetchall()
    
    if not cols:
        continue
        
    for col_row in cols:
        col_name = col_row[0]
        
        for search_str in search_strings:
            # For JSONB columns, we convert to text
            search_query = f"SELECT id FROM {table_name} WHERE CAST({col_name} AS TEXT) ILIKE '%{search_str}%';"
            try:
                # We assume most tables have an 'id' column for identification
                res = db.execute(text(search_query)).fetchall()
                if res:
                    print(f"FOUND MATCH in Table: {table_name}, Column: {col_name}")
                    for match in res:
                        print(f"  - Record ID: {match[0]}")
                        # Fetch the full record to confirm
                        record = db.execute(text(f"SELECT * FROM {table_name} WHERE id = {match[0]}")).fetchone()
                        print(f"  - Full Data: {record}")
            except Exception as e:
                # Some tables might not have an 'id' column or might have other issues
                # print(f"  Skipping {table_name}.{col_name}: {e}")
                pass
