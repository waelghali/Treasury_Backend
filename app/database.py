# database.py
import os
import json
import logging
from decimal import Decimal
from datetime import date, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Base class for our SQLAlchemy models
Base = declarative_base()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Please create a .env file.")

# Custom JSON encoder to handle Decimal and Date objects in JSONB columns
def sqlalchemy_json_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()  # Converts dates to "YYYY-MM-DD" strings
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# Create the SQLAlchemy engine with a custom JSON serializer
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    # This now handles Decimals AND Dates for the audit log JSONB column
    json_serializer=lambda obj: json.dumps(obj, default=sqlalchemy_json_serializer)
)

# Create a SessionLocal class to get database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
        logger.debug("Committing DB Session")
        db.commit()
    except Exception as e:
        logger.error(f"Rolling back DB Session due to error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

# Imports for model discovery
from app.models import models
import app.models.models_issuance