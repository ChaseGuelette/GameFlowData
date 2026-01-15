# File: src/db/client.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Load env variables ONCE
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment.")

# 2. Create the engine ONCE
# This 'engine' object is now a "Singleton" - it lives here and everyone borrows it.
engine = create_engine(DATABASE_URL)

def get_engine():
    """Returns the main database engine."""
    return engine

def verify_connection():
    """Optional: Use this if you want to explicitly test the connection."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection verified ✓")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False