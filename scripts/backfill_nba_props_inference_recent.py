#!/usr/bin/env python3
"""Bounded, resumable legacy NBA props backfill into inference storage."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_START_ID = 290_239_015
DEFAULT_FLOOR_ID = 268_000_000
DEFAULT_WINDOW_START = "2026-10-23T04:00:00Z"
DEFAULT_WINDOW_END = "2026-11-13T05:00:00Z"
DEFAULT_BATCH_SIZE = 5_000
MAX_BATCH_SIZE = 20_000
DEFAULT_MAX_BATCHES = 100
MAX_MAX_BATCHES = 10_000
DEFAULT_MAX_RUNTIME_SECONDS = 1_800
MAX_RUNTIME_SECONDS = 86_400
ADVISORY_LOCK_KEY = 7_114_220_261_112

BATCH_SQL = """
WITH selected AS MATERIALIZED (
    SELECT
        staging_id, api_game_id, api_player_name, bookmaker, market_key,
        outcome_label, line, odds_american, commence_time, home_team,
        away_team, inserted_at, market_last_update, bookmaker_last_update,
        bookmaker_name, snapshot_time, game_id, player_id, team_id
    FROM public.raw_player_props_combined
    WHERE staging_id <= %(cursor)s
      AND staging_id >= %(floor_id)s
    ORDER BY staging_id DESC
    LIMIT %(batch_size)s
), eligible AS MATERIALIZED (
    SELECT *
    FROM selected
    WHERE commence_time >= %(window_start)s
      AND commence_time < %(window_end)s
      AND COALESCE(snapshot_time, inserted_at) IS NOT NULL
      AND COALESCE(snapshot_time, inserted_at) < commence_time
), inserted AS (
    INSERT INTO public.nba_player_props_inference (
        api_game_id, api_player_name, bookmaker, market_key, outcome_label,
        line, odds_american, commence_time, home_team, away_team, inserted_at,
        market_last_update, bookmaker_last_update, bookmaker_name, snapshot_time,
        game_id, player_id, team_id, requested_snapshot_time,
        source_snapshot_time, source_kind, observed_at
    )
    SELECT
        api_game_id, api_player_name, bookmaker, market_key, outcome_label,
        line, odds_american, commence_time, home_team, away_team,
        COALESCE(inserted_at, snapshot_time), market_last_update, bookmaker_last_update,
        bookmaker_name, COALESCE(snapshot_time, inserted_at),
        CASE WHEN game_id ~ '^[0-9]{10}$' THEN game_id ELSE NULL END,
        player_id, team_id, NULL, COALESCE(snapshot_time, inserted_at),
        'historical', COALESCE(snapshot_time, inserted_at)
    FROM eligible
    ON CONFLICT (
        api_game_id, api_player_name, bookmaker, market_key, outcome_label,
        (COALESCE(line, -999999::numeric)), source_snapshot_time
    ) DO NOTHING
    RETURNING 1
)
SELECT
    count(*)::bigint AS scanned,
    count(*) FILTER (
        WHERE commence_time >= %(window_start)s AND commence_time < %(window_end)s
    )::bigint AS in_window,
    count(*) FILTER (
        WHERE commence_time >= %(window_start)s AND commence_time < %(window_end)s
          AND COALESCE(snapshot_time, inserted_at) IS NULL
    )::bigint AS rejected_null_clock,
    count(*) FILTER (
        WHERE commence_time >= %(window_start)s AND commence_time < %(window_end)s
          AND COALESCE(snapshot_time, inserted_at) IS NOT NULL
          AND COALESCE(snapshot_time, inserted_at) >= commence_time
    )::bigint AS rejected_at_after_commence,
    (SELECT count(*) FROM inserted)::bigint AS inserted,
    min(staging_id)::bigint AS next_cursor
FROM selected
"""


def utc_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include an explicit UTC offset")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="connect and commit bounded batches")
    parser.add_argument("--start-id", type=int, default=DEFAULT_START_ID)
    parser.add_argument("--floor-id", type=int, default=DEFAULT_FLOOR_ID)
    parser.add_argument("--window-start", type=utc_datetime, default=utc_datetime(DEFAULT_WINDOW_START))
    parser.add_argument("--window-end", type=utc_datetime, default=utc_datetime(DEFAULT_WINDOW_END))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-batches", type=int, default=DEFAULT_MAX_BATCHES)
    parser.add_argument("--max-runtime-seconds", type=int, default=DEFAULT_MAX_RUNTIME_SECONDS)
    parser.add_argument("--checkpoint", type=Path, default=Path.home() / "GameFlowArchives" / "nba-props-backfill-checkpoint.json")
    parser.add_argument("--statement-timeout-ms", type=int, default=120_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.start_id <= args.floor_id:
        parser.error("--start-id must be greater than --floor-id")
    if args.window_start >= args.window_end:
        parser.error("--window-start must be before --window-end")
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if not 1 <= args.max_batches <= MAX_MAX_BATCHES:
        parser.error(f"--max-batches must be between 1 and {MAX_MAX_BATCHES}")
    if not 1 <= args.max_runtime_seconds <= MAX_RUNTIME_SECONDS:
        parser.error(f"--max-runtime-seconds must be between 1 and {MAX_RUNTIME_SECONDS}")
    if args.statement_timeout_ms <= 0 or args.lock_timeout_ms <= 0:
        parser.error("timeouts must be positive")
    if args.execute:
        supplied = sys.argv[1:]
        has_start = any(value == "--start-id" or value.startswith("--start-id=") for value in supplied)
        has_floor = any(value == "--floor-id" or value.startswith("--floor-id=") for value in supplied)
        if not has_start or not has_floor:
            parser.error("execution requires explicit --start-id and --floor-id")


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def plan(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "mode": "execute" if args.execute else "dry-run",
        "id_envelope": {"start_id_inclusive": args.start_id, "floor_id_inclusive": args.floor_id},
        "utc_window": {"start_inclusive": iso(args.window_start), "end_exclusive": iso(args.window_end)},
        "batch_size": args.batch_size,
        "max_batches": args.max_batches,
        "max_runtime_seconds": args.max_runtime_seconds,
        "checkpoint": str(args.checkpoint),
    }


def load_checkpoint(args: argparse.Namespace) -> tuple[int, dict[str, int], int]:
    if not args.checkpoint.exists():
        return args.start_id, {}, 0
    data = json.loads(args.checkpoint.read_text(encoding="utf-8"))
    expected = plan(args)
    for key in ("id_envelope", "utc_window"):
        if data.get(key) != expected[key]:
            raise RuntimeError(f"checkpoint {key} does not match this invocation")
    cursor = int(data["next_cursor"])
    if cursor > args.start_id or cursor < args.floor_id - 1:
        raise RuntimeError("checkpoint cursor is outside the requested ID envelope")
    totals = {str(key): int(value) for key, value in data.get("totals", {}).items()}
    return cursor, totals, int(data.get("completed_batches", 0))


def save_checkpoint(args: argparse.Namespace, cursor: int, totals: dict[str, int], batches: int) -> None:
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    payload = plan(args) | {
        "next_cursor": cursor,
        "completed_batches": batches,
        "totals": totals,
        "updated_at": datetime.now().astimezone().isoformat(),
    }
    temporary = args.checkpoint.with_suffix(args.checkpoint.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(args.checkpoint)


def connect_database():
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(database_url, application_name="gameflow:nba-props-recent-backfill")


def execute(args: argparse.Namespace) -> int:
    empty_totals = {
        name: 0
        for name in (
            "scanned",
            "in_window",
            "rejected_null_clock",
            "rejected_at_after_commence",
            "inserted_after_dedupe",
        )
    }
    cursor, saved_totals, completed_batches = load_checkpoint(args)
    totals = empty_totals | {key: value for key, value in saved_totals.items() if key in empty_totals}
    batches_this_run = 0
    started = time.monotonic()
    conn = connect_database()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
            if not cur.fetchone()[0]:
                raise RuntimeError("another recent-props backfill holds the advisory lock")
        conn.autocommit = False
        while batches_this_run < args.max_batches and cursor >= args.floor_id:
            if time.monotonic() - started >= args.max_runtime_seconds:
                print("Maximum runtime reached before starting another batch.")
                break
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{args.statement_timeout_ms}ms",))
                    cur.execute("SELECT set_config('lock_timeout', %s, true)", (f"{args.lock_timeout_ms}ms",))
                    cur.execute("SHOW statement_timeout")
                    statement_timeout = cur.fetchone()[0]
                    cur.execute("SHOW lock_timeout")
                    lock_timeout = cur.fetchone()[0]
                    cur.execute(
                        BATCH_SQL,
                        {
                            "cursor": cursor,
                            "floor_id": args.floor_id,
                            "batch_size": args.batch_size,
                            "window_start": args.window_start,
                            "window_end": args.window_end,
                        },
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("backfill batch query returned no metrics row")
            scanned, in_window, null_clock, after_commence, inserted, next_cursor = row
            counts = {
                "scanned": int(scanned),
                "in_window": int(in_window),
                "rejected_null_clock": int(null_clock),
                "rejected_at_after_commence": int(after_commence),
                "inserted_after_dedupe": int(inserted),
            }
            batches_this_run += 1
            completed_batches += 1
            for key, value in counts.items():
                totals[key] += value
            if next_cursor is None:
                next_value = args.floor_id - 1
            else:
                next_value = int(next_cursor) - 1
            print(json.dumps({"batch": completed_batches, "batch_this_run": batches_this_run, **counts, "next_cursor": next_value, "statement_timeout": statement_timeout, "lock_timeout": lock_timeout}, sort_keys=True))
            cursor = next_value
            save_checkpoint(args, cursor, totals, completed_batches)
            if counts["scanned"] < args.batch_size:
                break
        print(json.dumps({"batches_this_run": batches_this_run, "completed_batches": completed_batches, "totals": totals, "next_cursor": cursor}, sort_keys=True))
        return 0
    finally:
        try:
            if not conn.closed:
                conn.rollback()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
        finally:
            conn.close()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    print(json.dumps(plan(args), indent=2, sort_keys=True))
    if not args.execute:
        print("Dry-run only: no environment file was loaded and no database connection was attempted.")
        return 0
    return execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
