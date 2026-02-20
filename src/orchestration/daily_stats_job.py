#!/usr/bin/env python3
"""
Daily Stats Job - NBA Game Results & Processing
================================================
Run once daily (recommended: 9 AM ET) after previous night's games are final.

This job:
1. Scrapes latest NBA game results from NBA API
2. Runs full processing pipeline (linker, averages, opponent stats)
3. Resolves pending paper bets using newly available game results
4. Does NOT scrape odds or run inference (separate jobs)

Usage:
    python src/orchestration/daily_stats_job.py [--dry-run] [--skip-resolution]

Examples:
    # Normal run
    python src/orchestration/daily_stats_job.py

    # Dry run (show what would be executed)
    python src/orchestration/daily_stats_job.py --dry-run

    # Skip bet resolution
    python src/orchestration/daily_stats_job.py --skip-resolution
"""

import argparse
import logging
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta
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


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _send_pnl_summary(result: dict) -> None:
    """Send daily P&L summary to Discord performance channel.

    Non-fatal: failures are logged but don't affect job status.
    """
    try:
        import asyncio

        from src.discord_bot.alerts import send_pnl_summary_sync
        from src.discord_bot.services.paper_trading import get_bankroll_summary

        # Get current bankroll data
        bankroll_data = asyncio.run(get_bankroll_summary())

        if bankroll_data:
            send_pnl_summary_sync(
                resolution_result=result,
                bankroll=bankroll_data.get("balance", 1000.0),
                daily_pnl=bankroll_data.get("daily_pnl", 0.0),
                total_pnl=bankroll_data.get("total_pnl", 0.0),
            )
        else:
            logger.warning("Could not get bankroll data for P&L summary")

    except Exception as e:
        logger.warning(f"Failed to send P&L summary to Discord: {e}")


def resolve_pending_bets(dry_run: bool = False) -> bool:
    """Resolve all pending paper bets using newly available game stats.

    This is a separate function (not subprocess) for better error handling.
    Resolution failure should NOT fail the entire stats job.
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Resolving Pending Paper Bets")

    if dry_run:
        logger.info("  Would call: PaperTrader().resolve_all_pending()")
        return True

    start_time = time.time()
    try:
        from src.paper_trading.paper_trader import PaperTrader

        trader = PaperTrader()
        result = trader.resolve_all_pending()

        elapsed = time.time() - start_time

        if result["dates_processed"] == 0 and result["dates_skipped"] == 0:
            logger.info(f"COMPLETED: No pending bets to resolve ({elapsed:.1f}s)")
        else:
            logger.info(
                f"COMPLETED: Bet resolution - {result['total_resolved']} bets across "
                f"{result['dates_processed']} dates ({result['dates_skipped']} skipped) "
                f"[{result['total_won']}W {result['total_lost']}L {result['total_push']}P] ({elapsed:.1f}s)"
            )

            # Send P&L summary to Discord performance channel
            _send_pnl_summary(result)

        return True

    except ImportError as e:
        elapsed = time.time() - start_time
        logger.warning(f"SKIPPED: Bet resolution - paper trading module not found ({elapsed:.1f}s)")
        logger.warning(f"  Import error: {e}")
        return True  # Don't fail the job for missing optional module

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: Bet resolution ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return True  # Don't fail the job for resolution errors (stats are more important)


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
        description="Daily Stats Job - NBA Game Results & Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    parser.add_argument(
        "--skip-resolution",
        action="store_true",
        help="Skip paper bet resolution step",
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
            f"{sys.executable} src/scrapers/nba_unified_scraper.py",
            "Scraping NBA Game Results",
        ),
        # Step 2: Run incremental linker
        (
            f"{sys.executable} src/processing/nba_linker_local.py incremental",
            "Linking Players (Incremental)",
        ),
        # Step 3: Backfill team IDs (incremental - only recent data)
        (
            f"{sys.executable} src/processing/backfill_team_ids_incremental.py --days-back 7",
            "Backfilling Team IDs (Incremental)",
        ),
        # Step 4: Update player positions
        (
            f"{sys.executable} src/scrapers/update_player_position_history.py",
            "Updating Player Position History",
        ),
        # Step 5: Update league averages
        (
            f"{sys.executable} src/scrapers/update_league_position_averages.py",
            "Updating League Position Averages",
        ),
        # Step 6: Populate rolling averages (incremental - only yesterday's games)
        (
            f"{sys.executable} src/processing/populate_average_stats_incremental.py"
            f" --date {(datetime.now().date() - timedelta(days=1)).isoformat()}",
            "Populating Rolling Average Stats (Incremental)",
        ),
        # Step 7: Update opponent allowed stats (incremental - last 2 days)
        (
            f"{sys.executable} src/processing/backfill_opponent_allowed_incremental.py --days-back 2",
            "Updating Opponent Allowed Stats (Incremental)",
        ),
        # Step 8: Refresh play type data (Synergy)
        (
            f"{sys.executable} src/scrapers/play_type_scraper.py",
            "Refreshing Play Type Data",
        ),
    ]

    for command, description in steps:
        if not run_command(command, description, args.dry_run):
            success = False
            logger.error(f"Job failed at step: {description}")
            break

    # Step 8: Resolve pending paper bets (runs even if previous steps failed)
    # This is separate from the main loop because:
    # 1. It uses direct Python import, not subprocess
    # 2. It should not fail the job if it fails (stats are more critical)
    if not args.skip_resolution:
        resolve_pending_bets(args.dry_run)

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
