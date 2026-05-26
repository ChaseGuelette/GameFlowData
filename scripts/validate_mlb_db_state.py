#!/usr/bin/env python3
"""Validate MLB remote/local DB catch-up state without printing secrets.

This is intentionally read-only. It exists so agents/users can verify remote and
local table freshness from the repo without needing to paste DB passwords into
chat.

Env vars loaded from .env:
  DATABASE_URL              remote/Supabase Postgres
  LOCAL_DATABASE_URL        local Postgres used by GameFlow scripts
  LOCAL_DATABASE_URL_AGENT  optional override for agent/WSL validation only

Usage:
  python scripts/validate_mlb_db_state.py --remote
  python scripts/validate_mlb_db_state.py --local
  python scripts/validate_mlb_db_state.py --both
"""

from __future__ import annotations

import argparse
import os
import sys
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg2
from dotenv import load_dotenv

TABLE_SPECS = [
    ("mlb_game_schedule", "game_date", False),
    ("mlb_teams", None, False),
    ("mlb_player_game_stats_pitching", "game_date", False),
    ("mlb_player_game_stats_batting", "game_date", False),
    ("mlb_player_average_batting", "game_date", False),
    ("mlb_player_average_pitching", "game_date", True),
    ("mlb_bullpen_daily_status", "game_date", False),
    ("mlb_game_lineups", "game_date", False),
    ("mlb_game_umpires", "game_date", False),
    ("mlb_active_roster", "roster_date", False),
]

SUMMARY_SQL = """
SELECT
    COUNT(*)::bigint AS row_count,
    MIN(game_date)::date AS min_date,
    MAX(game_date)::date AS max_date,
    COUNT(DISTINCT game_date)::bigint AS distinct_dates
FROM {table}
"""

COUNT_SQL = "SELECT COUNT(*)::bigint AS row_count FROM {table}"

MIN_IP_SQL = """
SELECT COUNT(min_ip_l5)::bigint
FROM mlb_player_average_pitching
"""


def with_connect_timeout(url: str, seconds: int = 10) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("connect_timeout", str(seconds))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query), parsed.fragment))


def mask_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown-host"
    db = parsed.path.lstrip("/") or "unknown-db"
    return f"{parsed.scheme}://***@{host}/{db}"


def get_url(target: str) -> tuple[str, str]:
    if target == "remote":
        key = "DATABASE_URL"
        url = os.getenv(key)
    else:
        key = "LOCAL_DATABASE_URL_AGENT" if os.getenv("LOCAL_DATABASE_URL_AGENT") else "LOCAL_DATABASE_URL"
        url = os.getenv(key)

    if not url:
        raise RuntimeError(f"Missing {key} in environment/.env")
    return key, with_connect_timeout(url)


def validate_target(target: str) -> bool:
    key, url = get_url(target)
    print(f"\n== {target.upper()} via {key}: {mask_url(url)}")

    try:
        conn = psycopg2.connect(url)
    except Exception as exc:
        print(f"ERROR: connection failed: {type(exc).__name__}: {str(exc).splitlines()[0]}")
        return False

    try:
        with conn, conn.cursor() as cur:
            print(f"{'table':35} {'rows':>12} {'min_date':>12} {'max_date':>12} {'dates':>8} {'min_ip_l5':>12}")
            for table, date_col, has_min_ip in TABLE_SPECS:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
                if not cur.fetchone()[0]:
                    print(f"{table:35} {'MISSING':>12} {'':>12} {'':>12} {'':>8} {'':>12}")
                    continue
                if date_col:
                    cur.execute(SUMMARY_SQL.replace("game_date", date_col).format(table=table))
                    row_count, min_date, max_date, distinct_dates = cur.fetchone()
                else:
                    cur.execute(COUNT_SQL.format(table=table))
                    row_count = cur.fetchone()[0]
                    min_date = max_date = distinct_dates = ""
                min_ip = ""
                if has_min_ip:
                    cur.execute(MIN_IP_SQL)
                    min_ip = str(cur.fetchone()[0])
                print(f"{table:35} {row_count:12} {str(min_date):>12} {str(max_date):>12} {distinct_dates:8} {min_ip:>12}")
    finally:
        conn.close()

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only MLB DB freshness validator")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--remote", action="store_true", help="Validate DATABASE_URL only")
    group.add_argument("--local", action="store_true", help="Validate LOCAL_DATABASE_URL only")
    group.add_argument("--both", action="store_true", help="Validate remote and local")
    args = parser.parse_args()

    load_dotenv()

    targets = ["remote", "local"] if args.both or not (args.remote or args.local) else ["remote" if args.remote else "local"]
    ok = True
    for target in targets:
        ok = validate_target(target) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
