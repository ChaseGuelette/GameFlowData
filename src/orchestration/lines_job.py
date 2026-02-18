#!/usr/bin/env python3
"""
Lines Job - Player Props & Injuries Scraping
=============================================
Run multiple times daily (recommended: 12 PM, 4 PM, 6 PM ET) before games start.

This job:
1. Scrapes player prop lines from Odds API
2. Scrapes injury updates from RapidAPI
3. Runs incremental linker to match new props
4. Links injury data to player IDs

Usage:
    python src/orchestration/lines_job.py [--date YYYY-MM-DD] [--dry-run] [--skip-injuries]

Examples:
    # Normal run for today
    python src/orchestration/lines_job.py

    # Run for specific date
    python src/orchestration/lines_job.py --date 2026-02-05

    # Skip injury scraping (faster)
    python src/orchestration/lines_job.py --skip-injuries

    # Dry run
    python src/orchestration/lines_job.py --dry-run
"""

import argparse
import logging
import shlex
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configure logging
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "lines.log"),
    ],
)
logger = logging.getLogger("LinesJob")


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_command(command: str, description: str, dry_run: bool = False) -> bool:
    """Run a shell command and return success status.

    Uses shlex.split() for proper parsing of arguments with spaces/quotes.
    Runs from project root to ensure relative paths work correctly.
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: {description}")

    if dry_run:
        logger.info(f"  Command: {command}")
        return True

    start_time = time.time()
    try:
        # Use shlex.split for proper command parsing (handles args with spaces/quotes)
        cmd_args = shlex.split(command)
        result = subprocess.run(
            cmd_args,
            check=True,
            capture_output=True,
            text=True,
            shell=False,
            cwd=PROJECT_ROOT,  # Run from project root for proper path resolution
        )
        elapsed = time.time() - start_time
        logger.info(f"COMPLETED: {description} ({elapsed:.1f}s)")
        if result.stdout:
            # Log last 200 chars of stdout for debugging
            logger.debug(f"  Output: ...{result.stdout[-200:]}")
        return True
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: {description} ({elapsed:.1f}s)")
        logger.error(f"  Exit code: {e.returncode}")
        logger.error(f"  Stderr: {e.stderr[:500] if e.stderr else 'No error output'}")
        if e.stdout:
            logger.error(f"  Stdout: {e.stdout[:500]}")
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: {description} ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Lines Job - Player Props & Injuries Scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    parser.add_argument(
        "--skip-injuries",
        action="store_true",
        help="Skip injury scraping (faster)",
    )
    parser.add_argument(
        "--skip-linker",
        action="store_true",
        help="Skip incremental linker (if already run today)",
    )
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"LINES JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {args.date}")
    logger.info("=" * 60)

    success = True
    steps = []

    # Step 1: Scrape daily game lines
    steps.append((
        f"{sys.executable} src/scrapers/daily_game_lines_scraper.py --date {args.date}",
        "Scraping Daily Game Lines (Odds)",
    ))

    # Step 2: Scrape player props
    steps.append((
        f"{sys.executable} src/scrapers/daily_player_props_scraper.py --date {args.date}",
        "Scraping Player Props",
    ))

    # Step 3: Scrape injuries (optional)
    if not args.skip_injuries:
        steps.append((
            f"{sys.executable} src/scrapers/rapidapi_injury_backfill.py --start {args.date} --end {args.date}",
            "Scraping Injuries (RapidAPI)",
        ))
        steps.append((
            f"{sys.executable} src/processing/link_injury_data.py",
            "Linking Injury Player IDs",
        ))

    # Step 4: Run incremental linker (optional)
    if not args.skip_linker:
        steps.append((
            f"{sys.executable} src/processing/nba_linker_local.py incremental",
            "Linking Props (Incremental)",
        ))

    for command, description in steps:
        if not run_command(command, description, args.dry_run):
            success = False
            logger.error(f"Job failed at step: {description}")
            # Continue with other steps even if one fails
            # (injuries failing shouldn't block props)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    if success:
        logger.info(f"LINES JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
    else:
        logger.warning(f"LINES JOB COMPLETED WITH ERRORS ({elapsed:.1f}s)")
    logger.info("=" * 60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
