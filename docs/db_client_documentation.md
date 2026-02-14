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
- Importing the module raises `ValueError` if `DATABASE_URL` is missing.

## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
- [Populate Average Stats](populate_average_stats_documentation.md)
