#!/usr/bin/env python3
"""Materialize linked short-retention NBA inference quotes into compact T-60 lines."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.processing.nba_prop_snapshots import (  # noqa: E402
    POLICY_VERSION,
    quote_from_record,
    select_t60_features,
    upsert_feature_lines,
)

ET = ZoneInfo("America/New_York")


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def default_dates(now: datetime | None = None) -> tuple[date, date]:
    today = (now or datetime.now(ET)).astimezone(ET).date()
    return today - timedelta(days=1), today


def build_parser() -> argparse.ArgumentParser:
    start_default, end_default = default_dates()
    parser = argparse.ArgumentParser(
        description="Select t60_v1 compact features from linked nba_player_props_inference rows. Default is DB-free dry-run."
    )
    parser.add_argument("--start-date", type=parse_date, default=start_default)
    parser.add_argument("--end-date", type=parse_date, default=end_default)
    parser.add_argument("--max-events", type=int, default=50)
    parser.add_argument("--after-event", default=None, help="Resume after the printed opaque event cursor; keep the same date bounds")
    parser.add_argument("--max-rows-per-event", type=int, default=10000)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="Connect and write; otherwise print the bounded plan only")
    mode.add_argument("--dry-run", action="store_true", help="Explicit alias for the safe default")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.end_date < args.start_date:
        raise SystemExit("--end-date must be on or after --start-date")
    if args.max_events <= 0 or args.max_events > 500:
        raise SystemExit("--max-events must be between 1 and 500")
    if args.max_rows_per_event <= 0 or args.max_rows_per_event > 50000:
        raise SystemExit("--max-rows-per-event must be between 1 and 50000")
    if (args.end_date - args.start_date).days > 31:
        raise SystemExit("date range may not exceed 32 inclusive ET dates")


def utc_bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(start_date, dtime.min, tzinfo=ET).astimezone(UTC),
        datetime.combine(end_date + timedelta(days=1), dtime.min, tzinfo=ET).astimezone(UTC),
    )


def load_target_events(conn, *, start_date: date, end_date: date, max_events: int, after_event: str | None = None) -> tuple[list[str], bool]:
    start_utc, end_utc = utc_bounds(start_date, end_date)
    query = """
        SELECT api_game_id
        FROM public.nba_player_props_inference
        WHERE commence_time >= %s AND commence_time < %s
          AND commence_time - interval '60 minutes' <= now()
          AND game_id IS NOT NULL AND player_id IS NOT NULL
          AND (%s IS NULL OR api_game_id > %s)
        GROUP BY api_game_id
        ORDER BY api_game_id
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_utc, end_utc, after_event, after_event, max_events + 1))
        event_ids = [str(row[0]) for row in cur.fetchall()]
    return event_ids[:max_events], len(event_ids) > max_events


def load_event_rows(conn, *, event_id: str, max_rows: int) -> tuple[list[dict], bool]:
    from psycopg2.extras import RealDictCursor

    query = """
        SELECT p.*
        FROM public.nba_player_props_inference p
        WHERE p.api_game_id = %s
          AND p.game_id IS NOT NULL AND p.player_id IS NOT NULL
          AND p.bookmaker IN ('draftkings', 'pinnacle')
          AND p.market_key IN ('player_points', 'player_rebounds', 'player_assists', 'player_threes')
          AND p.observed_at <= p.commence_time - interval '60 minutes'
          AND p.source_snapshot_time <= p.commence_time - interval '60 minutes'
        ORDER BY p.api_game_id, p.player_id, p.market_key, p.source_snapshot_time DESC, p.staging_id
        LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (event_id, max_rows + 1))
        rows = [dict(row) for row in cur.fetchall()]
    return rows[:max_rows], len(rows) > max_rows


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    validate_args(args)
    print(
        f"policy={POLICY_VERSION} dates={args.start_date}..{args.end_date} "
        f"max_events={args.max_events} mode={'execute' if args.execute else 'dry-run'}"
    )
    if not args.execute:
        print("dry-run: no environment file, database connection, or writes were attempted")
        return 0

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required with --execute")

    conn = psycopg2.connect(
        database_url, connect_timeout=10,
        application_name="gameflow:nba_props_t60_materialize",
        options="-c statement_timeout=20000 -c lock_timeout=2000",
    )
    totals = {"events": 0, "raw_rows": 0, "selected": 0, "affected": 0, "unresolved": 0}
    partial = False
    try:
        event_ids, capped = load_target_events(
            conn,
            start_date=args.start_date,
            end_date=args.end_date,
            max_events=args.max_events,
            after_event=args.after_event,
        )
        conn.rollback()
        partial = capped
        for event_id in event_ids:
            try:
                rows, row_capped = load_event_rows(conn, event_id=event_id, max_rows=args.max_rows_per_event)
                if row_capped:
                    raise RuntimeError("per-event row cap exceeded")
                quotes = [quote_from_record(row) for row in rows]
                features, conflicts = select_t60_features(quotes)
                if not rows or conflicts or not features:
                    raise RuntimeError((conflicts or ["no eligible t60 rows/features"])[0])
                written = upsert_feature_lines(conn, features)
                conn.commit()
                totals["events"] += 1
                totals["raw_rows"] += len(rows)
                totals["selected"] += len(features)
                totals["affected"] += written
            except Exception as exc:
                conn.rollback()
                partial = True
                totals["unresolved"] += 1
                print(f"event={event_id} status=unresolved error={type(exc).__name__}: {exc}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        f"complete: events={totals['events']} raw_rows={totals['raw_rows']} selected={totals['selected']} "
        f"affected_rows={totals['affected']} unresolved={totals['unresolved']} event_cap_partial={capped}"
    )
    if capped and event_ids and not totals["unresolved"]:
        print(f"resume with the same date bounds and --after-event {event_ids[-1]}")
    return 2 if partial else 0


if __name__ == "__main__":
    raise SystemExit(main())
