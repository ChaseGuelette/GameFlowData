#!/usr/bin/env python3
"""
Daily Stats Job - NBA Game Results & Processing
================================================
Run once daily (recommended: 6 AM ET) after previous night's games are final.

This job:
1. Scrapes latest NBA game results from NBA API
2. Runs full processing pipeline (linker, averages, opponent stats)
3. Does NOT scrape odds or run inference (separate jobs)

Usage:
    python src/orchestration/daily_stats_job.py [--dry-run]

Examples:
    # Normal run
    python src/orchestration/daily_stats_job.py

    # Dry run (show what would be executed)
    python src/orchestration/daily_stats_job.py --dry-run
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
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
        logging.FileHandler(LOG_DIR / "daily_stats.log"),
    ],
)
logger = logging.getLogger("DailyStatsJob")


def run_command(command: str, description: str, dry_run: bool = False) -> bool:
    """Run a shell command and return success status."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: {description}")

    if dry_run:
        logger.info(f"  Command: {command}")
        return True

    start_time = time.time()
    try:
        result = subprocess.run(
            command.split(),
            check=True,
            capture_output=True,
            text=True,
            shell=False,
        )
        elapsed = time.time() - start_time
        logger.info(f"COMPLETED: {description} ({elapsed:.1f}s)")
        return True
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: {description} ({elapsed:.1f}s)")
        logger.error(f"Error: {e.stderr[:500] if e.stderr else 'No error output'}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Daily Stats Job - NBA Game Results & Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"DAILY STATS JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    success = True
    steps = [
        # Step 1: Scrape NBA game results
        (
            "python src/scrapers/nba_unified_scraper.py",
            "Scraping NBA Game Results",
        ),
        # Step 2: Run incremental linker
        (
            "python src/processing/nba_linker_local.py incremental",
            "Linking Players (Incremental)",
        ),
        # Step 3: Backfill team IDs
        (
            "python src/processing/backfill_team_ids.py",
            "Backfilling Team IDs",
        ),
        # Step 4: Update player positions
        (
            "python src/scrapers/update_player_position_history.py",
            "Updating Player Position History",
        ),
        # Step 5: Update league averages
        (
            "python src/scrapers/update_league_position_averages.py",
            "Updating League Position Averages",
        ),
        # Step 6: Populate rolling averages
        (
            "python src/processing/populate_average_stats.py",
            "Populating Rolling Average Stats",
        ),
        # Step 7: Update opponent allowed stats
        (
            "python src/processing/backfill_opponent_allowed.py",
            "Updating Opponent Allowed Stats",
        ),
    ]

    for command, description in steps:
        if not run_command(command, description, args.dry_run):
            success = False
            logger.error(f"Job failed at step: {description}")
            break

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    if success:
        logger.info(f"DAILY STATS JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
    else:
        logger.error(f"DAILY STATS JOB FAILED ({elapsed:.1f}s)")
    logger.info("=" * 60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
