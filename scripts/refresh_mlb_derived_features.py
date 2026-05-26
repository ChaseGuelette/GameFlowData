#!/usr/bin/env python3
# ruff: noqa: E402, I001
"""Refresh MLB derived feature tables from existing source stats.

This is the one-off catch-up/repair entrypoint for stale model-input tables.
It is safe-by-default: without --execute it prints the planned commands only.

Tables refreshed by --execute:
  - mlb_player_average_batting via mlb_populate_averages_incremental.py
  - mlb_player_average_pitching via mlb_populate_averages_incremental.py
  - mlb_bullpen_daily_status via mlb_bullpen_workload_scraper.py

Optional:
  - --refresh-roster runs mlb_roster_scraper_job.py for today's active roster

Use remote production DATABASE_URL by default. Local mirrors should be synced only
after remote is fixed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.client import get_engine  # noqa: E402


TABLE_MAX_DATE_SQL = {
    "source_batting": "SELECT MAX(game_date)::date FROM mlb_player_game_stats_batting",
    "source_pitching": "SELECT MAX(game_date)::date FROM mlb_player_game_stats_pitching",
    "avg_batting": "SELECT MAX(game_date)::date FROM mlb_player_average_batting",
    "avg_pitching": "SELECT MAX(game_date)::date FROM mlb_player_average_pitching",
    "bullpen": "SELECT MAX(game_date)::date FROM mlb_bullpen_daily_status",
}


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def get_max_dates() -> dict[str, date | None]:
    engine = get_engine()
    with engine.connect() as conn:
        return {name: conn.execute(text(sql)).scalar() for name, sql in TABLE_MAX_DATE_SQL.items()}


def plan_dates(args: argparse.Namespace, max_dates: dict[str, date | None]) -> tuple[date, date]:
    latest_source = min(
        d for d in (max_dates["source_batting"], max_dates["source_pitching"]) if d is not None
    )
    output_dates = [max_dates["avg_batting"], max_dates["avg_pitching"], max_dates["bullpen"]]
    existing_outputs = [d for d in output_dates if d is not None]

    if args.start_date:
        start = parse_date(args.start_date)
    elif existing_outputs:
        start = min(existing_outputs) - timedelta(days=args.overlap_days)
    else:
        start = latest_source

    if args.end_date:
        end = min(parse_date(args.end_date), latest_source)
    else:
        end = latest_source

    if start > end:
        raise SystemExit(f"Nothing to refresh: planned start {start} is after end {end}")

    return start, end


def command_for(kind: str, target_date: date) -> list[str]:
    date_arg = target_date.isoformat()
    if kind == "batting":
        return [
            sys.executable,
            "src/processing/mlb/mlb_populate_averages_incremental.py",
            "--type",
            "batting",
            "--date",
            date_arg,
        ]
    if kind == "pitching":
        return [
            sys.executable,
            "src/processing/mlb/mlb_populate_averages_incremental.py",
            "--type",
            "pitching",
            "--date",
            date_arg,
        ]
    if kind == "bullpen":
        return [
            sys.executable,
            "-m",
            "src.scrapers.mlb.mlb_bullpen_workload_scraper",
            "--date",
            date_arg,
        ]
    raise ValueError(f"unknown refresh kind: {kind}")


def run(cmd: list[str], execute: bool) -> None:
    printable = " ".join(cmd)
    if not execute:
        print(f"DRY-RUN: {printable}")
        return
    print(f"RUN: {printable}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def print_max_dates(label: str) -> dict[str, date | None]:
    max_dates = get_max_dates()
    print(f"\n== {label}")
    for key in TABLE_MAX_DATE_SQL:
        print(f"{key:16} {max_dates[key]}")
    return max_dates


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh stale MLB derived feature tables")
    parser.add_argument("--execute", action="store_true", help="Actually run refresh commands; default is dry-run")
    parser.add_argument("--start-date", help="Inclusive start date YYYY-MM-DD; default uses stalest output max date")
    parser.add_argument("--end-date", help="Inclusive end date YYYY-MM-DD; capped to latest source-stat date")
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=1,
        help="Recompute this many days before the stalest output max date (default: 1)",
    )
    parser.add_argument(
        "--refresh-roster",
        action="store_true",
        help="Also run the active-roster scraper job for today",
    )
    parser.add_argument(
        "--skip-derived",
        action="store_true",
        help="Only run optional tasks such as --refresh-roster",
    )
    args = parser.parse_args()

    before = print_max_dates("Before refresh")

    if not args.skip_derived:
        start, end = plan_dates(args, before)
        dates = list(daterange(start, end))
        print(f"\nPlanned derived refresh window: {start} through {end} ({len(dates)} dates)")
        for target_date in dates:
            for kind in ("batting", "pitching", "bullpen"):
                run(command_for(kind, target_date), execute=args.execute)

    if args.refresh_roster:
        roster_cmd = [sys.executable, "src/orchestration/mlb_roster_scraper_job.py"]
        run(roster_cmd, execute=args.execute)

    if args.execute:
        print_max_dates("After refresh")
    else:
        print("\nDry run only. Re-run with --execute to write remote derived rows.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
