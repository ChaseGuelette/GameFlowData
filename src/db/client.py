# File: src/db/client.py
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import numpy as np
import psycopg2.extensions

psycopg2.extensions.register_adapter(
    np.int64, lambda val: psycopg2.extensions.AsIs(int(val))
)

# 1. Load env variables ONCE
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# 2. Create the engine ONCE (deferred to avoid crash during import in CI/test)
_engine = None
_local_engine = None

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


def get_engine(local: bool = False):
    """Returns a database engine.

    Args:
        local: If True, return an engine connected to LOCAL_DATABASE_URL
               (for offline training/backtesting). No statement timeout,
               no pgBouncer compat needed.
    """
    if local:
        global _local_engine
        if _local_engine is None:
            local_url = os.getenv(
                "LOCAL_DATABASE_URL",
                "postgresql://postgres:postgres@localhost:5432/gameflow_local",
            )
            _local_engine = create_engine(
                local_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=5,
            )
        return _local_engine

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
