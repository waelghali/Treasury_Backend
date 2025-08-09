# c:\Grow\app\scripts\reset_db.py

import os
import sys
from sqlalchemy import text # Import text for raw SQL execution

# --- Path Configuration ---
# This block ensures Python can find your 'app' package.
# It calculates the project root directory (e.g., 'c:\Grow').
# If your project structure is 'Grow/app/scripts/reset_db.py',
# then 'current_dir' is '.../app/scripts'.
# 'os.path.join(current_dir, "..")' goes to '.../app'.
# 'os.path.join(current_dir, "..", "..")' goes to '.../Grow'.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))

# Add the project root to sys.path if it's not already there.
# This makes 'app' directly importable as a top-level package.
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Application Imports ---
# Assuming 'Base' and 'engine' are defined in 'c:\Grow\app\database.py'
from app.database import Base, engine 

# Assuming ALL your SQLAlchemy models (User, Currency, Product, etc.)
# are defined within a single file: 'c:\Grow\app\models.py'.
# Importing this module will ensure all those model classes are
# registered with SQLAlchemy's Base.metadata, making them discoverable
# for table creation. You do NOT import individual model names like 'user' or 'currency'
# from this single models.py file, just the module itself.
import app.models 
print(f"--- DEBUG: Tables registered with SQLAlchemy: {Base.metadata.tables.keys()} ---")

# --- Database Reset Function ---
def reset_db_clean_and_create():
    """
    Ensures the 'public' schema exists, drops all tables within it,
    and then creates all defined tables based on SQLAlchemy models.
    WARNING: This will DELETE ALL DATA in your configured database.
    Use ONLY in development or testing environments when a full reset is desired.
    """
    print("WARNING: Ensuring clean database state...")
    
    # Use a connection within a 'with' block for proper resource management
    with engine.connect() as connection:
        # Ensure the public schema exists (important for PostgreSQL)
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS public;"))
        # Commit the schema creation. DDL (Data Definition Language) commands
        # implicitly commit in PostgreSQL, but explicitly committing here ensures
        # this step is finalized before subsequent operations, especially when using transactions.
        connection.commit() 

        # Set search_path for the current session to ensure operations target 'public'.
        # This is good practice to avoid ambiguity, especially in shared databases.
        connection.execute(text("SET search_path TO public;"))
        connection.commit() # Commit the search_path change

        # Drop all tables that are currently registered with Base.metadata.
        # This is why 'import app.models' (or individual model files) above is crucial.
        print("Dropping all existing database tables...")
        try:
            Base.metadata.drop_all(bind=connection)
            connection.commit() # Commit the table drop
            print("All existing tables dropped and public schema ensured! [SUCCESS]")
        except Exception as e:
            print(f"Error dropping tables: {e}")
            connection.rollback()

        print("Creating all defined database tables...")
        try:
            Base.metadata.create_all(bind=connection)
            connection.commit() # Commit the table creation
            print("Database tables created successfully! [SUCCESS]")
        except Exception as e:
            print(f"Error creating tables: {e}")
            connection.rollback()
        Base.metadata.drop_all(bind=connection)
        connection.commit() # Commit the table drop
        print("All existing tables dropped and public schema ensured!")

    print("Creating all defined database tables...")
    # Open a new connection for table creation to ensure a clean state
    with engine.connect() as connection:
        # Re-set search_path for this new connection context just to be safe.
        connection.execute(text('SET search_path TO public;'))
        connection.commit()
        
        # Create all tables defined in your models that inherit from Base.
        Base.metadata.create_all(bind=connection) 
        connection.commit() # Commit the table creation
        print("Database tables created successfully!")

# --- Script Entry Point ---
if __name__ == "__main__":
    reset_db_clean_and_create()
    print("Database reset complete. Now you should run seed_db.py to populate data.")