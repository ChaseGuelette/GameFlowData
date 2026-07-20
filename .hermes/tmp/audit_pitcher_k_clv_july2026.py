#!/usr/bin/env python3
"""SELECT-only local/remote audit for pitcher_strikeouts dense CLV snapshots."""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

TABLE = "public.mlb_player_props_clv_snapshots"
PARAMS = {"market": "pitcher_strikeouts", "recent_start": "2026-06-22", "july_start": "2026-07-01", "aug_start": "2026-08-01"}

QUERIES = {
    "schema": """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'mlb_player_props_clv_snapshots'
  AND column_name IN (
    'id', 'market_key', 'outcome_label', 'line', 'game_id', 'player_id',
    'requested_snapshot_time', 'snapshot_time', 'commence_time',
    'scrape_reason', 'target_offset_minutes', 'inserted_at',
    'api_game_id', 'bookmaker', 'api_player_name'
  )
ORDER BY ordinal_position;
""",
    "overview": """
SELECT
  COUNT(*)::bigint AS total_rows,
  MIN(requested_snapshot_time) AS min_requested_snapshot_time,
  MAX(requested_snapshot_time) AS max_requested_snapshot_time,
  MIN(commence_time)::date AS min_commence_date,
  MAX(commence_time)::date AS max_commence_date,
  MAX(inserted_at) AS latest_inserted_at,
  COUNT(game_id)::bigint AS game_id_nonnull,
  ROUND(100.0 * COUNT(game_id) / NULLIF(COUNT(*), 0), 2) AS game_id_nonnull_pct,
  COUNT(player_id)::bigint AS player_id_nonnull,
  ROUND(100.0 * COUNT(player_id) / NULLIF(COUNT(*), 0), 2) AS player_id_nonnull_pct,
  COUNT(*) FILTER (WHERE game_id IS NOT NULL AND player_id IS NOT NULL)::bigint AS both_ids_nonnull,
  ROUND(100.0 * COUNT(*) FILTER (WHERE game_id IS NOT NULL AND player_id IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS both_ids_nonnull_pct
FROM public.mlb_player_props_clv_snapshots
WHERE market_key = %(market)s;
""",
    "recent_by_game_date": """
SELECT
  commence_time::date AS game_date,
  COUNT(*)::bigint AS rows,
  COUNT(DISTINCT api_game_id)::bigint AS api_games,
  COUNT(DISTINCT requested_snapshot_time)::bigint AS requested_times,
  COUNT(game_id)::bigint AS game_id_nonnull,
  COUNT(player_id)::bigint AS player_id_nonnull,
  MAX(inserted_at) AS latest_inserted_at
FROM public.mlb_player_props_clv_snapshots
WHERE market_key = %(market)s
  AND requested_snapshot_time >= %(recent_start)s::timestamptz
GROUP BY commence_time::date
ORDER BY game_date;
""",
    "july_reason_offsets": """
SELECT
  scrape_reason,
  target_offset_minutes,
  COUNT(*)::bigint AS rows,
  COUNT(DISTINCT commence_time::date)::bigint AS game_dates,
  MIN(requested_snapshot_time) AS min_requested_snapshot_time,
  MAX(requested_snapshot_time) AS max_requested_snapshot_time
FROM public.mlb_player_props_clv_snapshots
WHERE market_key = %(market)s
  AND requested_snapshot_time >= %(july_start)s::timestamptz
  AND requested_snapshot_time < %(aug_start)s::timestamptz
GROUP BY scrape_reason, target_offset_minutes
ORDER BY scrape_reason, target_offset_minutes NULLS FIRST;
""",
    "recent_pairing_by_game_date": """
WITH quote_groups AS (
  SELECT
    commence_time::date AS game_date,
    api_game_id,
    bookmaker,
    api_player_name,
    line,
    snapshot_time,
    BOOL_OR(LOWER(outcome_label) = 'over') AS has_over,
    BOOL_OR(LOWER(outcome_label) = 'under') AS has_under
  FROM public.mlb_player_props_clv_snapshots
  WHERE market_key = %(market)s
    AND requested_snapshot_time >= %(recent_start)s::timestamptz
    AND LOWER(outcome_label) IN ('over', 'under')
  GROUP BY commence_time::date, api_game_id, bookmaker, api_player_name, line, snapshot_time
)
SELECT
  game_date,
  COUNT(*)::bigint AS quote_groups,
  COUNT(*) FILTER (WHERE has_over AND has_under)::bigint AS paired_over_under_groups,
  ROUND(100.0 * COUNT(*) FILTER (WHERE has_over AND has_under) / NULLIF(COUNT(*), 0), 2) AS paired_pct
FROM quote_groups
GROUP BY game_date
ORDER BY game_date;
""",
    "recent_post_commence": """
SELECT
  COUNT(*)::bigint AS recent_rows_with_commence,
  COUNT(*) FILTER (WHERE requested_snapshot_time >= commence_time)::bigint AS requested_at_or_post_commence,
  COUNT(*) FILTER (WHERE snapshot_time >= commence_time)::bigint AS snapshot_at_or_post_commence,
  COUNT(*) FILTER (
    WHERE requested_snapshot_time >= commence_time OR snapshot_time >= commence_time
  )::bigint AS either_at_or_post_commence,
  MIN(requested_snapshot_time) FILTER (
    WHERE requested_snapshot_time >= commence_time OR snapshot_time >= commence_time
  ) AS first_violation_requested_time,
  MAX(requested_snapshot_time) FILTER (
    WHERE requested_snapshot_time >= commence_time OR snapshot_time >= commence_time
  ) AS last_violation_requested_time
FROM public.mlb_player_props_clv_snapshots
WHERE market_key = %(market)s
  AND requested_snapshot_time >= %(recent_start)s::timestamptz
  AND commence_time IS NOT NULL;
""",
}


def serial(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def with_timeout(url: str) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.setdefault("connect_timeout", "10")
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), p.fragment))


def run_target(label: str, env_key: str):
    url = os.getenv(env_key)
    if not url:
        print(json.dumps({"target": label, "error": f"missing {env_key}"}))
        return
    result = {"target": label, "env_key": env_key, "queries": {}}
    try:
        conn = psycopg2.connect(with_timeout(url), application_name="gameflow:pitcher_k_clv_readonly_audit")
        conn.set_session(readonly=True, autocommit=False)
    except Exception as exc:
        print(json.dumps({"target": label, "error": f"connect failed: {type(exc).__name__}: {str(exc).splitlines()[0]}"}))
        return
    try:
        for name, sql in QUERIES.items():
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SET LOCAL statement_timeout = '120s'")
                    cur.execute("SET LOCAL TIME ZONE 'America/New_York'")
                    cur.execute(sql, PARAMS)
                    result["queries"][name] = list(cur.fetchall())
                conn.rollback()
            except Exception as exc:
                conn.rollback()
                result["queries"][name] = {"error": f"{type(exc).__name__}: {str(exc).splitlines()[0]}"}
    finally:
        conn.close()
    print(json.dumps(result, default=serial, separators=(",", ":")))


def main():
    load_dotenv()
    local_key = "LOCAL_DATABASE_URL_AGENT" if os.getenv("LOCAL_DATABASE_URL_AGENT") else "LOCAL_DATABASE_URL"
    run_target("LOCAL", local_key)
    run_target("REMOTE", "DATABASE_URL")
    print("SQL_USED_BEGIN")
    print("-- Per-query read-only session setup:\nSET LOCAL statement_timeout = '120s';\nSET LOCAL TIME ZONE 'America/New_York';\n")
    for name, sql in QUERIES.items():
        print(f"-- {name}\n{sql.strip()}\n")
    print("-- bind parameters (non-secret): " + json.dumps(PARAMS, sort_keys=True))
    print("SQL_USED_END")


if __name__ == "__main__":
    main()
