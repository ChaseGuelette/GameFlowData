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
