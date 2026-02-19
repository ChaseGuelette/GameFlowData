# Database Client Documentation

## Overview
Provides a shared SQLAlchemy engine and small helpers for database connectivity.

## Inputs and Dependencies
- Environment variable: `DATABASE_URL`
- Loads `.env` at import time via `dotenv.load_dotenv`
- Uses SQLAlchemy `create_engine`

## API
- `get_engine()`: returns the singleton engine instance.
- `verify_connection()`: runs `SELECT 1` and returns True/False.

## Connection Pool Configuration

The engine uses SQLAlchemy connection pooling optimized for concurrent queries:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `pool_size` | 10 | Base number of persistent connections |
| `max_overflow` | 6 | Additional connections allowed beyond pool_size |
| `pool_recycle` | 300 | Recycle connections after 5 minutes |
| `pool_pre_ping` | True | Validate connections before use |
| `statement_timeout` | 300000ms | 5-minute query timeout |

The pool was sized to support parallel feature building in `daily_runner.py`, which uses up to 8 concurrent workers for feature store queries.

## Usage
```python
from src.db.client import get_engine, verify_connection

engine = get_engine()
verify_connection()
```

## Notes
- The module uses **lazy initialization** — it is safely importable without `DATABASE_URL` (e.g., in CI/test environments where env vars are empty). The engine is only created if `DATABASE_URL` is set.
- `get_engine()` raises `ValueError` at call time if `DATABASE_URL` is missing, providing a clear error message with setup instructions.
- Module-level `engine` variable is `None` when `DATABASE_URL` is not set. Code that imports `engine` directly should use `get_engine()` instead for proper error handling.

## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
- [Populate Average Stats](populate_average_stats_documentation.md)
