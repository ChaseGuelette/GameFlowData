# Database client

Status: active source-backed reference. Reconciled 2026-08-24.

The shared backend database adapter is `src/db/client.py`. It creates SQLAlchemy engines lazily enough for imports to remain safe in CI, registers NumPy `int64` support for psycopg2, and provides remote and local database routes.

## Configuration

Required for the normal backend route:

- `DATABASE_URL`

Optional controls:

- `LOCAL_DATABASE_URL`: local training/backtest database used by `get_engine(local=True)`.
- `GAMEFLOW_DB_APP_NAME`: PostgreSQL `application_name`; otherwise the script name is used.
- `DB_POOL_MODE`: `null`, `nullpool`, `none`, or `off` selects `NullPool`.
- `DB_POOL_SIZE`: local queue-pool size; default `5`.
- `DB_MAX_OVERFLOW`: queue-pool overflow; default `2`.
- `DB_POOL_TIMEOUT_SECONDS`: queue-pool checkout timeout; default `30`.
- `DB_POOL_RECYCLE_SECONDS`: connection recycle interval; default `300`.

`NullPool` is selected automatically on Railway and for Supabase pooler URLs. This prevents application-side persistent pools from multiplying Supavisor connections. Other routes use a small queue pool.

All normal backend connections set:

- `statement_timeout=300000` (five minutes)
- `idle_in_transaction_session_timeout=60000` (one minute)
- `pool_pre_ping=True`

The local route intentionally omits those remote/pooler timeouts and uses a five-connection queue pool with five overflow connections.

## API

```python
from src.db.client import get_engine, verify_connection

remote_engine = get_engine()
local_engine = get_engine(local=True)
assert verify_connection()
```

- `get_engine(local=False)` returns the singleton remote engine and raises `ValueError` if `DATABASE_URL` was absent when the module loaded.
- `get_engine(local=True)` lazily creates the local engine.
- `verify_connection()` executes `SELECT 1`, prints a redacted success/failure message, and returns a Boolean.
- The module-level `engine` name remains only for backward compatibility; new code should call `get_engine()`.

## Role boundary

Backend Python normally connects as the `postgres` role. Browser/dashboard code uses the Supabase authenticated client and RLS. Never expose the backend connection string or service-role key to client code.

## Operational cautions

- A changed `DATABASE_URL` in the same Python process does not replace an already-created singleton engine; restart the process.
- Railway must use the writable Supabase session-pooler route expected by production.
- Main-context agents do not query production through Supabase MCP. Use the isolated GameFlow SQL-runner workflow.
- Never print connection URLs in logs or documentation.

## Related documentation

- [Documentation index](README.md)
- [Railway deployment](railway_deployment.md)
- [NBA feature store](feature_store_documentation.md)
