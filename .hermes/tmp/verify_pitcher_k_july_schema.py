from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
TABLE = "mlb_player_props_clv_snapshots"
SQL = {
    "columns": """
SELECT column_name, data_type, udt_name, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = %(table)s
ORDER BY ordinal_position
""".strip(),
    "indexes": """
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = %(table)s
ORDER BY indexname
""".strip(),
}

out = {}
for label, env_name in (("remote", "DATABASE_URL"), ("local", "LOCAL_DATABASE_URL")):
    url = os.environ.get(env_name)
    if not url:
        raise RuntimeError(f"Missing {env_name}")
    conn = psycopg2.connect(url, connect_timeout=10, options="-c statement_timeout=30000")
    conn.set_session(readonly=True, autocommit=True)
    try:
        db = {}
        with conn.cursor() as cur:
            for name, sql in SQL.items():
                cur.execute(sql, {"table": TABLE})
                db[name] = cur.fetchall()
        out[label] = db
    finally:
        conn.close()
print(json.dumps(out, indent=2, default=str))
