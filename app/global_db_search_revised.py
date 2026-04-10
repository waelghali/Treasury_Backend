import os
import sys

# Add the parent directory of 'app' to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
# Search for templates with placeholders or typo in subject in ALL tables with any column name
sql_tables = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = db.execute(sql_tables).fetchall()

for table_row in tables:
    table_name = table_row[0]
    sql_cols = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    cols = db.execute(sql_cols).fetchall()
    
    for col_row in cols:
        col_name = col_row[0]
        search_query = text(f"SELECT * FROM {table_name} WHERE CAST({col_name} AS TEXT) ILIKE '%Urgrnt%' OR CAST({col_name} AS TEXT) ILIKE '%Escalation notification content%';")
        try:
            res = db.execute(search_query).fetchall()
            if res:
                print(f"FOUND MATCH in Table: {table_name}, Column: {col_name}")
                print(f"  Result: {res}")
        except:
            pass
