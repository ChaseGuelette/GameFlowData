# File: src/db/client.py
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Load env variables ONCE
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# 2. Create the engine ONCE (deferred to avoid crash during import in CI/test)
_engine = None

if DATABASE_URL:
    _engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,  # detect stale pgBouncer connections
        pool_size=10,  # max persistent connections (increased for parallel feature building)
        max_overflow=6,  # extra connections under load
        pool_recycle=300,  # recycle every 5 min (pgBouncer compat)
        connect_args={
            "options": "-c statement_timeout=300000"  # 5 min per statement
        },
    )

# Keep module-level 'engine' for backward compatibility with direct imports
engine = _engine


def get_engine():
    """Returns the main database engine."""
    if _engine is None:
        raise ValueError(
            "DATABASE_URL not found in environment. "
            "Set it in your .env file (local) or Railway service variables (production). "
            "See docs/railway_deployment.md for setup instructions."
        )
    return _engine


def verify_connection():
    """Optional: Use this if you want to explicitly test the connection."""
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection verified ✓")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False
