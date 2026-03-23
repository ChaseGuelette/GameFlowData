#!/usr/bin/env python3
"""
Lines Job - Player Props & Injuries Scraping
=============================================
Run multiple times daily before games start.

This job:
1. Scrapes player prop lines from Odds API (historical or live)
2. Scrapes injury updates from RapidAPI
3. Runs incremental linker to match new props
4. Links injury data to player IDs

Usage:
    python src/orchestration/lines_job.py [--date YYYY-MM-DD] [--dry-run] [--skip-injuries]
    python src/orchestration/lines_job.py --live [--props-only] [--parallel] [--dry-run]

Examples:
    # Historical scrape for today (default)
    python src/orchestration/lines_job.py

    # Live scrape — full (game lines + props + injuries + linker)
    python src/orchestration/lines_job.py --live

    # Live scrape — full with parallel (props + injuries run concurrently)
    python src/orchestration/lines_job.py --live --parallel

    # Live scrape — props only (props + linker, skip game lines/injuries)
    python src/orchestration/lines_job.py --live --props-only

    # Dry run
    python src/orchestration/lines_job.py --live --props-only --dry-run
"""

import argparse
import logging
import shlex
import subprocess
import sys
import threading
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


def run_step_group(steps: list[tuple[str, str]], dry_run: bool = False) -> bool:
    """Run a list of (command, description) steps serially. Returns True if all succeeded."""
    success = True
    for command, description in steps:
        if not run_command(command, description, dry_run):
            success = False
            logger.error(f"Job failed at step: {description}")
            # Continue with remaining steps even if one fails
    return success


def run_parallel_groups(groups: list[list[tuple[str, str]]], dry_run: bool = False) -> bool:
    """Run step groups concurrently via threads. Each group runs its steps serially."""
    results = [None] * len(groups)

    def _worker(idx, steps):
        results[idx] = run_step_group(steps, dry_run)

    threads = []
    for i, group in enumerate(groups):
        t = threading.Thread(target=_worker, args=(i, group), name=f"group-{i}")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return all(results)


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
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live API endpoints (writes props to raw_player_props_combined)",
    )
    parser.add_argument(
        "--props-only",
        action="store_true",
        help="Only scrape props + run linker (skip game lines and injuries)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run independent step groups concurrently (props path + injury path)",
    )
    args = parser.parse_args()

    start_time = time.time()
    mode_label = "LIVE" if args.live else "HISTORICAL"
    scope_label = "PROPS-ONLY" if args.props_only else "FULL"
    parallel_label = " | Parallel: ON" if args.parallel else ""
    logger.info("=" * 60)
    logger.info(f"LINES JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode: {mode_label} | Scope: {scope_label} | Date: {args.date}{parallel_label}")
    logger.info("=" * 60)

    exe = sys.executable

    # Build step groups
    # Group A: Props path (game lines → props scraper → nba linker)
    group_a = []

    if not args.props_only:
        group_a.append((
            f"{exe} src/scrapers/daily_game_lines_scraper.py --date {args.date}",
            "Scraping Daily Game Lines (Odds)",
        ))

    if args.live:
        group_a.append((
            f"{exe} src/scrapers/daily_player_props_scraper.py --live --combos --target-table raw_player_props_combined",
            "Scraping Player Props (Live)",
        ))
    else:
        group_a.append((
            f"{exe} src/scrapers/daily_player_props_scraper.py --date {args.date}",
            "Scraping Player Props (Historical)",
        ))

    if not args.skip_linker:
        group_a.append((
            f"{exe} src/processing/nba_linker_local.py incremental",
            "Linking Props (Incremental)",
        ))

    # Group B: Injury path (injury scraper → injury linker)
    group_b = []
    if not args.props_only and not args.skip_injuries:
        group_b.append((
            f"{exe} src/scrapers/rapidapi_injury_backfill.py --start {args.date} --end {args.date}",
            "Scraping Injuries (RapidAPI)",
        ))
        group_b.append((
            f"{exe} src/processing/link_injury_data.py",
            "Linking Injury Player IDs",
        ))

    # Execute
    if args.parallel and group_b:
        logger.info("Running step groups in parallel:")
        logger.info(f"  Group A (props):    {len(group_a)} steps")
        logger.info(f"  Group B (injuries): {len(group_b)} steps")
        success = run_parallel_groups([group_a, group_b], args.dry_run)
    else:
        # Sequential: all steps in one flat list (original behavior)
        all_steps = group_a + group_b
        success = run_step_group(all_steps, args.dry_run)

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
