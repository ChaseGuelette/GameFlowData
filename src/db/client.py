# File: src/db/client.py
import os
import sys

import numpy as np
import psycopg2.extensions
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

psycopg2.extensions.register_adapter(
    np.int64, lambda val: psycopg2.extensions.AsIs(int(val))
)

# 1. Load env variables ONCE
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _application_name() -> str:
    configured = os.getenv("GAMEFLOW_DB_APP_NAME")
    if configured:
        base = configured
    else:
        script = os.path.basename(sys.argv[0] or "python")
        base = f"gameflow:{script}"
    safe = "".join(ch if ch.isalnum() or ch in "._:-" else "_" for ch in base)
    return safe[:63] or "gameflow"


def _use_null_pool(database_url: str | None) -> bool:
    mode = os.getenv("DB_POOL_MODE", "").strip().lower()
    if mode:
        return mode in {"null", "nullpool", "none", "off"}
    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_SERVICE_ID"):
        return True
    return bool(database_url and "pooler.supabase.com" in database_url)


def _engine_kwargs(database_url: str) -> dict:
    connect_args = {
        "application_name": _application_name(),
        "options": "-c statement_timeout=300000 -c idle_in_transaction_session_timeout=60000",
    }
    kwargs = {
        "pool_pre_ping": True,
        "pool_recycle": _env_int("DB_POOL_RECYCLE_SECONDS", 300),
        "connect_args": connect_args,
    }
    if _use_null_pool(database_url):
        kwargs["poolclass"] = NullPool
    else:
        kwargs["pool_size"] = _env_int("DB_POOL_SIZE", 5)
        kwargs["max_overflow"] = _env_int("DB_MAX_OVERFLOW", 2)
        kwargs["pool_timeout"] = _env_int("DB_POOL_TIMEOUT_SECONDS", 30)
    return kwargs

# 2. Create the engine ONCE (deferred to avoid crash during import in CI/test)
_engine = None
_local_engine = None

if DATABASE_URL:
    _engine = create_engine(DATABASE_URL, **_engine_kwargs(DATABASE_URL))

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
                connect_args={"application_name": _application_name()},
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
