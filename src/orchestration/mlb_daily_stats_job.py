#!/usr/bin/env python3
"""
MLB Daily Stats Job - Game Results & Processing
================================================
Run once daily (recommended: 10 AM ET on Railway) after previous night's games.

This job:
1. Scrapes MLB game results (mlb_stats_scraper.py --yesterday)
2. Scrapes Statcast data (mlb_statcast_scraper.py --yesterday)
3. Runs MLB linker (mlb_linker.py incremental)
4. Updates batting rolling averages (mlb_populate_averages_incremental.py)
5. Updates pitching rolling averages (mlb_populate_averages_incremental.py)
6. Resolves pending MLB paper bets

Usage:
    python src/orchestration/mlb_daily_stats_job.py [--dry-run] [--skip-resolution]
"""

import argparse
import logging
import shlex
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
        logging.FileHandler(LOG_DIR / "mlb_daily_stats.log"),
    ],
)
logger = logging.getLogger("MLBDailyStatsJob")

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_pending_mlb_bets(dry_run: bool = False) -> bool:
    """Resolve all pending MLB paper bets using newly available game stats."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Resolving Pending MLB Paper Bets")

    if dry_run:
        logger.info("  Would call: MLBPaperTrader().resolve_all_pending()")
        return True

    start_time = time.time()
    try:
        from src.paper_trading.mlb_paper_trader import MLBPaperTrader

        trader = MLBPaperTrader()
        result = trader.resolve_all_pending()

        elapsed = time.time() - start_time

        if result["dates_processed"] == 0 and result["dates_skipped"] == 0:
            logger.info(f"COMPLETED: No pending MLB bets to resolve ({elapsed:.1f}s)")
        else:
            logger.info(
                f"COMPLETED: MLB bet resolution - {result['total_resolved']} bets across "
                f"{result['dates_processed']} dates ({result['dates_skipped']} skipped) "
                f"[{result['total_won']}W {result['total_lost']}L {result['total_push']}P] ({elapsed:.1f}s)"
            )

        return True

    except ImportError as e:
        elapsed = time.time() - start_time
        logger.warning(f"SKIPPED: MLB bet resolution - module not found ({elapsed:.1f}s)")
        logger.warning(f"  Import error: {e}")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: MLB bet resolution ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return True  # Don't fail the job for resolution errors


def run_command(
    command: str,
    description: str,
    dry_run: bool = False,
    timeout: int = 600,
    max_retries: int = 0,
    retry_delay: int = 15,
) -> bool:
    """Run a shell command and return success status."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: {description}")

    if dry_run:
        logger.info(f"  Command: {command}")
        return True

    cmd_args = shlex.split(command)
    attempts = 1 + max_retries

    for attempt in range(1, attempts + 1):
        start_time = time.time()
        try:
            result = subprocess.run(
                cmd_args,
                check=True,
                capture_output=True,
                text=True,
                shell=False,
                cwd=PROJECT_ROOT,
                timeout=timeout,
            )
            elapsed = time.time() - start_time
            if attempt > 1:
                logger.info(f"COMPLETED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            else:
                logger.info(f"COMPLETED: {description} ({elapsed:.1f}s)")
            if result.stdout:
                logger.debug(f"  Output: ...{result.stdout[-200:]}")
            return True

        except subprocess.TimeoutExpired as e:
            elapsed = time.time() - start_time
            logger.error(f"TIMED OUT (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            if e.stderr:
                stderr = e.stderr if isinstance(e.stderr, str) else e.stderr.decode(errors="replace")
                logger.error(f"  Partial stderr: {stderr[-500:]}")

        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start_time
            logger.error(f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            logger.error(f"  Exit code: {e.returncode}")
            logger.error(f"  Stderr: {e.stderr[:500] if e.stderr else 'No error output'}")

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            logger.error(f"  Exception: {e}")

        if attempt < attempts:
            delay = retry_delay * (2 ** (attempt - 1))
            logger.info(f"  Retrying in {delay}s (attempt {attempt + 1}/{attempts})...")
            time.sleep(delay)

    return False


def main():
    parser = argparse.ArgumentParser(
        description="MLB Daily Stats Job - Game Results & Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be executed")
    parser.add_argument("--skip-resolution", action="store_true", help="Skip paper bet resolution")
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"MLB DAILY STATS JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    success = True

    # Each step: (command, description, critical, timeout_sec, max_retries)
    current_season = datetime.now().year

    steps = [
        # Step 1: Update schedule + scrape boxscores for completed games
        (
            f"{sys.executable} src/scrapers/mlb/mlb_stats_scraper.py --season {current_season}",
            "Scraping MLB Schedule & Boxscores",
            True, 600, 2,
        ),
        # Step 2: Scrape Statcast data for yesterday
        (
            f"{sys.executable} src/scrapers/mlb/mlb_statcast_scraper.py --yesterday",
            "Scraping Statcast Data",
            False, 600, 1,
        ),
        # Step 3: Run MLB linker (incremental)
        (
            f"{sys.executable} src/processing/mlb/mlb_linker.py incremental",
            "Linking MLB Players (Incremental)",
            True, 600, 2,
        ),
        # Step 4: Update batting rolling averages
        (
            f"{sys.executable} src/processing/mlb/mlb_populate_averages_incremental.py --type batting",
            "Updating Batting Rolling Averages",
            True, 900, 2,
        ),
        # Step 5: Update pitching rolling averages
        (
            f"{sys.executable} src/processing/mlb/mlb_populate_averages_incremental.py --type pitching",
            "Updating Pitching Rolling Averages",
            True, 900, 2,
        ),
    ]

    for i, (command, description, critical, timeout, retries) in enumerate(steps, 1):
        logger.info(f"Step {i}/{len(steps)}")
        if not run_command(command, description, args.dry_run, timeout=timeout, max_retries=retries):
            if critical:
                success = False
                logger.error(f"Critical step failed: {description}")
                break
            else:
                logger.warning(f"Non-critical step failed: {description} — continuing")

    # Resolve pending MLB paper bets
    if not args.skip_resolution:
        resolve_pending_mlb_bets(args.dry_run)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    if success:
        logger.info(f"MLB DAILY STATS JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
    else:
        logger.error(f"MLB DAILY STATS JOB FAILED ({elapsed:.1f}s)")
    logger.info("=" * 60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
