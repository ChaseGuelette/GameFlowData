#!/usr/bin/env python3
"""Bounded, fail-safe retention cleanup for nba_player_props_inference only."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prune only materialized rows from nba_player_props_inference. Default is DB-free dry-run."
    )
    cutoff = parser.add_mutually_exclusive_group(required=True)
    cutoff.add_argument("--before-date", type=parse_date, help="ET commence date strictly before this date")
    cutoff.add_argument("--retention-days", type=int, help="Compute cutoff from current ET date")
    parser.add_argument("--max-batch-rows", type=int, default=5000)
    parser.add_argument("--max-batches", type=int, default=1)
    parser.add_argument("--scan-days", type=int, default=90, help="Finite old-row window ending at the cutoff")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="Explicit alias for the safe default")
    return parser


def resolve_cutoff(args: argparse.Namespace, now: datetime | None = None) -> date:
    if args.before_date:
        return args.before_date
    if args.retention_days is None or args.retention_days < 1:
        raise SystemExit("--retention-days must be positive")
    return (now or datetime.now(ET)).astimezone(ET).date() - timedelta(days=args.retention_days)


def validate_args(args: argparse.Namespace) -> None:
    if args.max_batch_rows <= 0 or args.max_batch_rows > 100_000:
        raise SystemExit("--max-batch-rows must be between 1 and 100000")
    if args.max_batches <= 0 or args.max_batches > 100:
        raise SystemExit("--max-batches must be between 1 and 100")
    if args.scan_days <= 0 or args.scan_days > 366:
        raise SystemExit("--scan-days must be between 1 and 366")


def utc_window(cutoff: date, scan_days: int) -> tuple[datetime, datetime]:
    start = datetime.combine(cutoff - timedelta(days=scan_days), dtime.min, tzinfo=ET).astimezone(UTC)
    end = datetime.combine(cutoff, dtime.min, tzinfo=ET).astimezone(UTC)
    return start, end


def count_candidates(conn, cutoff: date, scan_days: int) -> tuple[int, int]:
    start_utc, end_utc = utc_window(cutoff, scan_days)
    query = """
        SELECT
          count(*) FILTER (WHERE p.market_key NOT IN ('player_points','player_rebounds','player_assists','player_threes') OR
            (p.game_id IS NOT NULL AND p.player_id IS NOT NULL AND EXISTS (
              SELECT 1 FROM public.nba_player_prop_feature_lines f
              WHERE f.game_id=p.game_id AND f.player_id=p.player_id
                AND f.market_key=p.market_key AND f.selection_policy_version='t60_v1'
          ))) AS safe_rows,
          count(*) FILTER (WHERE p.market_key IN ('player_points','player_rebounds','player_assists','player_threes')
            AND (p.game_id IS NULL OR p.player_id IS NULL OR NOT EXISTS (
              SELECT 1 FROM public.nba_player_prop_feature_lines f
              WHERE f.game_id=p.game_id AND f.player_id=p.player_id
                AND f.market_key=p.market_key AND f.selection_policy_version='t60_v1'
          ))) AS retained_gaps
        FROM public.nba_player_props_inference p
        WHERE p.commence_time >= %s AND p.commence_time < %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_utc, end_utc))
        safe, gaps = cur.fetchone()
    return int(safe or 0), int(gaps or 0)


def delete_batch(conn, cutoff: date, scan_days: int, limit: int) -> int:
    start_utc, end_utc = utc_window(cutoff, scan_days)
    query = """
        WITH doomed AS (
            SELECT p.staging_id
            FROM public.nba_player_props_inference p
            WHERE p.commence_time >= %s AND p.commence_time < %s
              AND (p.market_key NOT IN ('player_points','player_rebounds','player_assists','player_threes') OR (
                p.game_id IS NOT NULL AND p.player_id IS NOT NULL AND EXISTS (
                  SELECT 1 FROM public.nba_player_prop_feature_lines f
                  WHERE f.game_id=p.game_id AND f.player_id=p.player_id
                    AND f.market_key=p.market_key AND f.selection_policy_version='t60_v1'
              )))
            ORDER BY p.commence_time, p.staging_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM public.nba_player_props_inference p
        USING doomed d
        WHERE p.staging_id=d.staging_id
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_utc, end_utc, limit))
        return max(cur.rowcount, 0)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    validate_args(args)
    cutoff = resolve_cutoff(args)
    print(
        f"table=nba_player_props_inference cutoff_et_before={cutoff} "
        f"scan_days={args.scan_days} max_batch_rows={args.max_batch_rows} max_batches={args.max_batches} "
        f"mode={'execute' if args.execute else 'dry-run'}"
    )
    if not args.execute:
        print("dry-run: no environment file, database connection, or DELETE was attempted")
        return 0

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required with --execute")
    conn = psycopg2.connect(
        database_url, connect_timeout=10,
        application_name="gameflow:nba_props_t60_prune",
        options="-c statement_timeout=20000 -c lock_timeout=2000",
    )
    deleted = 0
    try:
        safe_before, retained_gaps = count_candidates(conn, cutoff, args.scan_days)
        for _ in range(args.max_batches):
            batch = delete_batch(conn, cutoff, args.scan_days, args.max_batch_rows)
            conn.commit()
            deleted += batch
            if batch < args.max_batch_rows:
                break
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"complete: safe_before={safe_before} deleted={deleted} retained_gaps={retained_gaps}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
