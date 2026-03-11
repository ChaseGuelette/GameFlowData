#!/usr/bin/env python3
"""
NCAAB Daily Stats Job
======================
Run daily after previous night's games are final.
Active only during NCAAB season (November through April).

Steps:
1. Scrape new game results via CBBpy
2. Run ncaab_populate_averages --incremental
3. Download fresh Barttorvik snapshot
4. Run ncaab_barttorvik_linker

Usage:
    python src/orchestration/ncaab_daily_stats_job.py [--dry-run] [--season 2025]
"""

import argparse
import logging
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ncaab_daily_stats.log"),
    ],
)
logger = logging.getLogger("NCAABDailyStats")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

NCAAB_SEASON_MONTHS = [11, 12, 1, 2, 3, 4]


def is_ncaab_season() -> bool:
    """Return True if current month is within NCAAB season."""
    return datetime.utcnow().month in NCAAB_SEASON_MONTHS


def _current_season() -> int:
    """Get the current NCAAB season end year.

    Season 2025 = Nov 2024 - Apr 2025.
    If we're in Nov-Dec, the season ends next year.
    If we're in Jan-Apr, the season ends this year.
    """
    now = datetime.utcnow()
    if now.month >= 9:  # Sep-Dec: season ends next year
        return now.year + 1
    else:  # Jan-Aug: season ends this year
        return now.year


def run_step(name: str, cmd: str, timeout: int = 600, dry_run: bool = False) -> bool:
    """Run a subprocess step with timeout."""
    logger.info(f"Step: {name}")
    logger.info(f"  Command: {cmd}")

    if dry_run:
        logger.info("  [DRY RUN] Skipped.")
        return True

    try:
        result = subprocess.run(
            shlex.split(cmd),
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode == 0:
            logger.info(f"  OK ({name})")
            if result.stdout.strip():
                for line in result.stdout.strip().split("\n")[-5:]:
                    logger.info(f"    {line}")
            return True
        else:
            logger.error(f"  FAILED ({name}) exit code {result.returncode}")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n")[-5:]:
                    logger.error(f"    {line}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"  TIMEOUT ({name}) after {timeout}s")
        return False
    except Exception as e:
        logger.error(f"  ERROR ({name}): {e}")
        return False


def run_ncaab_daily_stats(dry_run: bool = False, season: int | None = None) -> bool:
    """Execute the NCAAB daily stats pipeline.

    Returns True if all critical steps succeeded.
    """
    if not is_ncaab_season():
        logger.info("NCAAB off-season — skipping daily stats.")
        return True

    season = season or _current_season()
    logger.info(f"=== NCAAB Daily Stats Job (season {season}) ===")

    steps = [
        (
            "CBBpy Schedule + Box Scores",
            f"python -m src.scrapers.ncaab.ncaab_cbbpy_scraper --season {season} --backfill",
            1200,  # 20 min timeout for box score scraping
        ),
        (
            "Rolling Averages (incremental)",
            "python -m src.processing.ncaab.ncaab_populate_averages --incremental",
            600,
        ),
        (
            "Barttorvik Ratings Snapshot",
            f"python -m src.scrapers.ncaab.ncaab_barttorvik_scraper --season {season}",
            120,
        ),
        (
            "Barttorvik Team Linker",
            "python -m src.processing.ncaab.ncaab_barttorvik_linker",
            120,
        ),
    ]

    all_ok = True
    for name, cmd, timeout in steps:
        ok = run_step(name, cmd, timeout=timeout, dry_run=dry_run)
        if not ok:
            all_ok = False
            logger.warning(f"Step '{name}' failed — continuing with remaining steps.")

    logger.info(f"=== NCAAB Daily Stats {'COMPLETE' if all_ok else 'PARTIAL FAILURE'} ===")
    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NCAAB Daily Stats Job")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be executed")
    parser.add_argument("--season", type=int, help="Override season (e.g., 2025)")

    args = parser.parse_args()
    success = run_ncaab_daily_stats(dry_run=args.dry_run, season=args.season)
    sys.exit(0 if success else 1)
