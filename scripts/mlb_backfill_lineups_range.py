#!/usr/bin/env python3
"""Backfill MLB game lineups over an inclusive date range.

This is intentionally a thin wrapper around the existing one-date
MLBLineupScraper. It does not introduce new scraping logic; it just runs the
same date scraper repeatedly with per-date logging and a modest delay.

Examples:
    python scripts/mlb_backfill_lineups_range.py --start-date 2024-03-20 --end-date 2024-09-30
    python scripts/mlb_backfill_lineups_range.py --start-date 2026-03-25 --end-date 2026-04-14 --dry-run
    python scripts/mlb_backfill_lineups_range.py --start-date 2025-03-18 --end-date 2025-09-28 --local
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# Allow running as `python scripts/mlb_backfill_lineups_range.py` from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.client import get_engine
from src.scrapers.mlb.mlb_lineup_scraper import MLBLineupScraper

logger = logging.getLogger("mlb_backfill_lineups_range")


@dataclass(frozen=True)
class DateResult:
    target_date: date
    stored_count: int
    status: str
    error: str | None = None


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def backfill_lineups_range(
    *,
    start_date: date,
    end_date: date,
    local: bool,
    dry_run: bool,
    sleep_seconds: float,
    stop_on_error: bool,
) -> list[DateResult]:
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")
    if sleep_seconds < 0:
        raise ValueError("sleep-seconds must be non-negative")

    engine = get_engine(local=local)
    scraper = MLBLineupScraper(engine, dry_run=dry_run)
    results: list[DateResult] = []
    dates = list(iter_dates(start_date, end_date))

    target = "LOCAL" if local else "REMOTE"
    mode = "DRY RUN" if dry_run else "WRITE"
    logger.info(
        "Starting MLB lineup range backfill: %s to %s inclusive (%d dates, %s, %s)",
        start_date,
        end_date,
        len(dates),
        target,
        mode,
    )

    total_stored = 0
    for idx, target_date in enumerate(dates, start=1):
        logger.info("[%d/%d] Backfilling %s", idx, len(dates), target_date)
        try:
            stored_count = scraper.scrape_date(target_date)
            total_stored += stored_count
            results.append(DateResult(target_date, stored_count, "ok"))
            logger.info(
                "[%d/%d] %s complete — %d lineup entries",
                idx,
                len(dates),
                target_date,
                stored_count,
            )
        except Exception as exc:  # pragma: no cover - defensive operational logging
            logger.exception("[%d/%d] %s failed", idx, len(dates), target_date)
            results.append(DateResult(target_date, 0, "error", str(exc)))
            if stop_on_error:
                raise

        if idx < len(dates) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    failures = [r for r in results if r.status != "ok"]
    zero_dates = [r for r in results if r.status == "ok" and r.stored_count == 0]
    logger.info(
        "Finished MLB lineup range backfill: %d dates, %d total entries, %d zero-entry dates, %d failures",
        len(results),
        total_stored,
        len(zero_dates),
        len(failures),
    )

    if zero_dates:
        logger.info(
            "Zero-entry dates: %s",
            ", ".join(str(r.target_date) for r in zero_dates[:30])
            + (" ..." if len(zero_dates) > 30 else ""),
        )
    if failures:
        logger.error(
            "Failed dates: %s",
            ", ".join(f"{r.target_date} ({r.error})" for r in failures[:30])
            + (" ..." if len(failures) > 30 else ""),
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill MLB game lineups over an inclusive date range. Remote-first by default."
    )
    parser.add_argument("--start-date", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, type=parse_date, help="End date YYYY-MM-DD")
    parser.add_argument("--local", action="store_true", help="Use local Postgres instead of remote DATABASE_URL")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and log without writing to DB")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Delay between dates in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue after an unexpected per-date exception instead of stopping",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        results = backfill_lineups_range(
            start_date=args.start_date,
            end_date=args.end_date,
            local=args.local,
            dry_run=args.dry_run,
            sleep_seconds=args.sleep_seconds,
            stop_on_error=not args.continue_on_error,
        )
    except Exception as exc:
        logger.error("Backfill aborted: %s", exc)
        sys.exit(1)

    failures = [r for r in results if r.status != "ok"]
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
