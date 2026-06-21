#!/usr/bin/env python3
"""Railway-safe recurring MLB dense CLV snapshot job.

This orchestration job is intentionally separate from mlb_lines_job.py.  The
regular lines job captures live production lines; this job performs a bounded,
resume-aware dense historical CLV grid capture for validation coverage.

Defaults are conservative for Railway:
- disabled unless MLB_DENSE_CLV_ENABLED is truthy
- market-scoped to batter_hits
- date-scoped to completed ET game dates only
- modest parallelism
- link newly inserted rows after scraping
"""

from __future__ import annotations

import argparse
import logging
import os
import shlex
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBDenseCLVJob")


TRUTHY = {"1", "true", "yes", "on"}


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUTHY


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return float(value)


def env_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return [part.strip() for part in value.replace(",", " ").split() if part.strip()]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range_from_offsets(*, end_offset_days: int, lookback_days: int) -> tuple[date, date]:
    """Return completed ET date window ending N days before today.

    With defaults end_offset_days=1 and lookback_days=2, a 2:20 AM ET Railway
    run on Jun 8 covers Jun 6 through Jun 7. Resume filtering skips rows already
    captured from prior runs while catching late schedule/provider shifts.
    """
    if end_offset_days < 0:
        raise ValueError("end_offset_days must be >= 0")
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    today_et = datetime.now(ET).date()
    end = today_et - timedelta(days=end_offset_days)
    start = end - timedelta(days=lookback_days - 1)
    return start, end


def run_command(command: list[str], *, dry_run_only: bool = False) -> None:
    display = " ".join(shlex.quote(part) for part in command)
    logger.info("Running: %s", display)
    if dry_run_only:
        return
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        timeout=None,
    )
    if result.stdout:
        logger.info("stdout tail:\n%s", "\n".join(result.stdout.splitlines()[-80:]))
    if result.stderr:
        logger.info("stderr tail:\n%s", "\n".join(result.stderr.splitlines()[-120:]))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {display}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded recurring MLB dense CLV snapshot capture")
    parser.add_argument("--start-date", default=None, help="Override ET start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Override ET end date YYYY-MM-DD")
    parser.add_argument("--markets", nargs="+", default=None, help="Override markets; defaults to MLB_DENSE_CLV_MARKETS or batter_hits")
    parser.add_argument("--lookback-days", type=int, default=None, help="Completed ET dates to cover when dates are omitted")
    parser.add_argument("--end-offset-days", type=int, default=None, help="0=today, 1=yesterday when dates are omitted")
    parser.add_argument("--dry-run", action="store_true", help="Pass --dry-run to scraper and skip linker")
    parser.add_argument("--force", action="store_true", help="Run even when MLB_DENSE_CLV_ENABLED is not truthy")
    parser.add_argument("--skip-linker", action="store_true", help="Scrape only; do not run link_mlb_clv_snapshots.py")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if not args.force and not env_flag("MLB_DENSE_CLV_ENABLED", default=False):
        logger.info("MLB dense CLV job disabled; set MLB_DENSE_CLV_ENABLED=true to run.")
        return 0

    if bool(args.start_date) != bool(args.end_date):
        logger.error("Provide both --start-date and --end-date, or neither.")
        return 2

    if args.start_date and args.end_date:
        start = parse_date(args.start_date)
        end = parse_date(args.end_date)
    else:
        start, end = date_range_from_offsets(
            end_offset_days=args.end_offset_days if args.end_offset_days is not None else env_int("MLB_DENSE_CLV_END_OFFSET_DAYS", 1),
            lookback_days=args.lookback_days if args.lookback_days is not None else env_int("MLB_DENSE_CLV_LOOKBACK_DAYS", 2),
        )

    if end < start:
        logger.error("End date %s is before start date %s", end, start)
        return 2

    markets = args.markets or env_list("MLB_DENSE_CLV_MARKETS", ["batter_hits"])
    max_workers = env_int("MLB_DENSE_CLV_MAX_WORKERS", 2)
    request_sleep_seconds = env_float("MLB_DENSE_CLV_REQUEST_SLEEP_SECONDS", 0.2)
    linker_batch_size = env_int("MLB_DENSE_CLV_LINK_BATCH_SIZE", 5000)
    linker_max_batches = env_int("MLB_DENSE_CLV_LINK_MAX_BATCHES", 100)

    logger.info(
        "Dense CLV window: %s..%s | markets=%s | max_workers=%d | sleep=%.3fs",
        start,
        end,
        ",".join(markets),
        max_workers,
        request_sleep_seconds,
    )

    scraper_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "scrape_mlb_clv_snapshots.py"),
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--markets",
        *markets,
        "--request-sleep-seconds",
        str(request_sleep_seconds),
        "--max-workers",
        str(max_workers),
    ]
    if args.dry_run or env_flag("MLB_DENSE_CLV_DRY_RUN", default=False):
        scraper_cmd.append("--dry-run")

    run_command(scraper_cmd)

    if args.dry_run or env_flag("MLB_DENSE_CLV_DRY_RUN", default=False):
        logger.info("Dry run complete; skipping linker.")
        return 0

    if args.skip_linker or env_flag("MLB_DENSE_CLV_SKIP_LINKER", default=False):
        logger.info("Skipping linker by request.")
        return 0

    linker_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "link_mlb_clv_snapshots.py"),
        "--execute",
        "--batch-size",
        str(linker_batch_size),
        "--max-batches",
        str(linker_max_batches),
        "--skip-report",
    ]
    run_command(linker_cmd)
    logger.info("MLB dense CLV job completed for %s..%s", start, end)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
