#!/usr/bin/env python3
"""SELECT-only, ID-windowed remote/local pitcher_strikeouts dense CLV verifier."""
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

import psycopg2
from dotenv import load_dotenv

TABLE = "public.mlb_player_props_clv_snapshots"
MARKET = "pitcher_strikeouts"
START_UTC = datetime(2026, 7, 1, 4, 0, tzinfo=UTC)  # 2026-07-01 00:00 ET (EDT)
END_UTC = datetime(2026, 7, 19, 4, 0, tzinfo=UTC)   # 2026-07-19 00:00 ET (EDT), exclusive
BATCH_SIZE = 50_000

BOUND_SQL = f"""
SELECT id
FROM {TABLE}
WHERE market_key = %s
  AND commence_time >= %s
  AND commence_time < %s
ORDER BY id {{direction}}
LIMIT 1
""".strip()

SCAN_SQL = f"""
SELECT id,
       (commence_time AT TIME ZONE 'America/New_York')::date AS game_date_et,
       game_id,
       player_id,
       api_player_name,
       snapshot_time,
       requested_snapshot_time,
       commence_time
FROM {TABLE}
WHERE id > %s
  AND id <= %s
  AND market_key = %s
  AND commence_time >= %s
  AND commence_time < %s
ORDER BY id
LIMIT %s
""".strip()

ACTIVE_SQL = """
SELECT pid,
       application_name,
       usename,
       state,
       wait_event_type,
       wait_event,
       now() - query_start AS query_age,
       left(regexp_replace(query, E'[\\n\\r\\t]+', ' ', 'g'), 240) AS query_excerpt
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state = 'active'
  AND query ILIKE '%mlb_player_props_clv_snapshots%'
ORDER BY query_start
LIMIT 20
""".strip()


def clean(value):
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def connect(url: str, label: str):
    return psycopg2.connect(
        url,
        connect_timeout=15,
        application_name=f"gameflow:select_only_pitcher_k_verify:{label}",
        options="-c statement_timeout=120000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000 -c default_transaction_read_only=on",
    )


def run(url: str, label: str) -> dict:
    conn = connect(url, label)
    conn.set_session(readonly=True, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(BOND := BOUND_SQL.format(direction="ASC"), (MARKET, START_UTC, END_UTC))
            row = cur.fetchone()
            first_id = row[0] if row else None
            conn.rollback()

            cur.execute(BOUND_SQL.format(direction="DESC"), (MARKET, START_UTC, END_UTC))
            row = cur.fetchone()
            last_id = row[0] if row else None
            conn.rollback()

        counts_by_date = Counter()
        unlinked_names = Counter()
        total = game_null = player_null = post_commence = 0
        scanned_batches = 0
        if first_id is not None:
            last_seen = first_id - 1
            while last_seen < last_id:
                with conn.cursor() as cur:
                    cur.execute(
                        SCAN_SQL,
                        (last_seen, last_id, MARKET, START_UTC, END_UTC, BATCH_SIZE),
                    )
                    rows = cur.fetchall()
                conn.rollback()
                scanned_batches += 1
                if not rows:
                    break
                for rid, game_date, game_id, player_id, api_name, snapshot_time, requested_time, commence_time in rows:
                    total += 1
                    counts_by_date[str(game_date)] += 1
                    if game_id is None:
                        game_null += 1
                    if player_id is None:
                        player_null += 1
                        unlinked_names[api_name if api_name is not None else "<NULL>"] += 1
                    if commence_time is not None and (
                        (snapshot_time is not None and snapshot_time >= commence_time)
                        or (requested_time is not None and requested_time >= commence_time)
                    ):
                        post_commence += 1
                last_seen = rows[-1][0]

        with conn.cursor() as cur:
            cur.execute(ACTIVE_SQL)
            active = [dict(zip([d.name for d in cur.description], map(clean, row))) for row in cur.fetchall()]
        conn.rollback()

        return {
            "label": label,
            "first_relevant_id": first_id,
            "last_relevant_id": last_id,
            "max_relevant_id": last_id,
            "rows": total,
            "counts_by_game_date_et": dict(sorted(counts_by_date.items())),
            "game_id_null": game_null,
            "player_id_null": player_null,
            "top_unlinked_api_player_name": [
                {"api_player_name": name, "rows": count}
                for name, count in unlinked_names.most_common(20)
            ],
            "distinct_unlinked_names": len(unlinked_names),
            "post_commence_violations": post_commence,
            "scan_batches": scanned_batches,
            "active_sessions": active,
        }
    finally:
        conn.close()


def main() -> int:
    load_dotenv()
    remote_url = os.environ.get("DATABASE_URL")
    local_url = os.environ.get("LOCAL_DATABASE_URL_AGENT") or os.environ.get("LOCAL_DATABASE_URL")
    if not remote_url or not local_url:
        missing = [name for name, value in (("DATABASE_URL", remote_url), ("LOCAL_DATABASE_URL_AGENT/LOCAL_DATABASE_URL", local_url)) if not value]
        raise SystemExit("Missing required database URL(s): " + ", ".join(missing))
    output = {
        "market": MARKET,
        "window_et": "2026-07-01 through 2026-07-18 inclusive",
        "window_utc": [START_UTC.isoformat(), END_UTC.isoformat()],
        "remote": run(remote_url, "remote"),
        "local": run(local_url, "local"),
    }
    print(json.dumps(output, indent=2, default=clean))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
