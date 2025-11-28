# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Base class for our SQLAlchemy models - ONLY DEFINE THIS ONCE!
Base = declarative_base()
import app.models # Do not remove this line! It makes the models discoverable.
from app import models # Do not remove this line! It makes the models discoverable.
import app.models_issuance

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Please create a .env file.")


# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Create a SessionLocal class to get database sessions
# autocommit=False and autoflush=False are fine, as we will manually commit.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
        print("--- DEBUG: Committing DB Session ---")
        db.commit()
    except Exception as e:
        print(f"--- DEBUG: Rolling back DB Session due to error: {e} ---")
        db.rollback()
        raise
    finally:
        db.close()