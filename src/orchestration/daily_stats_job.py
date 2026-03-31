#!/usr/bin/env python3
"""
Daily Stats Job - NBA Game Results & Processing
================================================
Run once daily (recommended: 11 AM ET on Railway) after previous night's games are final.

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


def run_calibration_check(dry_run: bool = False) -> None:
    """Run calibration drift check on resolved paper bets.

    Non-fatal: failures are logged but don't affect job status.
    Only sends Discord alert if drift is detected OR it's Monday (weekly visibility).
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Calibration Drift Check")

    if dry_run:
        logger.info("  Would call: compute_calibration_drift()")
        return

    start_time = time.time()
    try:
        from src.paper_trading.calibration_monitor import compute_calibration_drift

        metrics = compute_calibration_drift()
        elapsed = time.time() - start_time

        if metrics is None:
            logger.info(f"COMPLETED: Calibration check — insufficient data ({elapsed:.1f}s)")
            return

        logger.info(
            f"COMPLETED: Calibration check — {metrics.n_bets} bets, "
            f"severity={metrics.severity}, {len(metrics.all_alerts)} alert(s) ({elapsed:.1f}s)"
        )

        # Send Discord alert if drift detected or it's Monday (weekly report)
        is_monday = datetime.now().weekday() == 0
        should_alert = metrics.has_drift or is_monday

        if should_alert:
            try:
                from src.discord_bot.alerts import send_calibration_alert_sync

                send_calibration_alert_sync(metrics)
            except Exception as e:
                logger.warning(f"Failed to send calibration alert to Discord: {e}")
        else:
            logger.info("  No drift detected and not Monday — skipping Discord alert")

    except ImportError as e:
        elapsed = time.time() - start_time
        logger.warning(f"SKIPPED: Calibration check — module not found ({elapsed:.1f}s)")
        logger.warning(f"  Import error: {e}")

    except Exception as e:
        elapsed = time.time() - start_time
        logger.warning(f"FAILED: Calibration check ({elapsed:.1f}s)")
        logger.warning(f"  Exception: {e}")


def resolve_pending_user_bets(dry_run: bool = False) -> bool:
    """Resolve all pending user bets (from dashboard checkmark feature).

    Non-fatal: failures are logged but don't affect job status.
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Resolving Pending User Bets")

    if dry_run:
        logger.info("  Would call: UserBetResolver().resolve_all_pending()")
        return True

    start_time = time.time()
    try:
        from src.paper_trading.user_bet_resolver import UserBetResolver

        resolver = UserBetResolver()
        result = resolver.resolve_all_pending()

        elapsed = time.time() - start_time

        if result["dates_processed"] == 0 and result["dates_skipped"] == 0:
            logger.info(f"COMPLETED: No pending user bets to resolve ({elapsed:.1f}s)")
        else:
            logger.info(
                f"COMPLETED: User bet resolution - {result['total_resolved']} bets across "
                f"{result['dates_processed']} dates ({result['dates_skipped']} skipped) "
                f"[{result['total_won']}W {result['total_lost']}L {result['total_push']}P] ({elapsed:.1f}s)"
            )

        return True

    except ImportError as e:
        elapsed = time.time() - start_time
        logger.warning(f"SKIPPED: User bet resolution - module not found ({elapsed:.1f}s)")
        logger.warning(f"  Import error: {e}")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: User bet resolution ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return True  # Non-fatal


def resolve_pending_user_dfs_entries(dry_run: bool = False) -> bool:
    """Resolve all pending user DFS parlay entries.

    Non-fatal: failures are logged but don't affect job status.
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Resolving Pending User DFS Entries")

    if dry_run:
        logger.info("  Would call: UserDfsResolver().resolve_all_pending()")
        return True

    start_time = time.time()
    try:
        from src.paper_trading.user_dfs_resolver import UserDfsResolver

        resolver = UserDfsResolver()
        result = resolver.resolve_all_pending()

        elapsed = time.time() - start_time

        if result["dates_processed"] == 0 and result["dates_skipped"] == 0:
            logger.info(f"COMPLETED: No pending DFS entries to resolve ({elapsed:.1f}s)")
        else:
            logger.info(
                f"COMPLETED: DFS entry resolution - {result['total_resolved']} entries across "
                f"{result['dates_processed']} dates ({result['dates_skipped']} skipped) "
                f"[{result['total_won']}W {result['total_lost']}L {result['total_push']}P] ({elapsed:.1f}s)"
            )

        return True

    except ImportError as e:
        elapsed = time.time() - start_time
        logger.warning(f"SKIPPED: DFS entry resolution - module not found ({elapsed:.1f}s)")
        logger.warning(f"  Import error: {e}")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: DFS entry resolution ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return True  # Non-fatal


def run_command(
    command: str,
    description: str,
    dry_run: bool = False,
    timeout: int = 600,
    max_retries: int = 0,
    retry_delay: int = 15,
) -> bool:
    """Run a shell command and return success status.

    Uses shlex.split() for proper parsing of arguments with spaces/quotes.
    Runs from project root to ensure relative paths work correctly.

    Args:
        command: Shell command to run.
        description: Human-readable step description.
        dry_run: If True, log the command without executing.
        timeout: Per-attempt timeout in seconds (default 600 = 10 min).
        max_retries: Number of retry attempts after first failure (0 = no retries).
        retry_delay: Base delay between retries in seconds; doubles each attempt.
    """
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
            logger.error(
                f"TIMED OUT (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)"
            )
            logger.error(f"  Step exceeded {timeout // 60}m timeout")
            if e.stdout:
                stdout = e.stdout if isinstance(e.stdout, str) else e.stdout.decode(errors="replace")
                logger.error(f"  Partial stdout: {stdout[-500:]}")
            if e.stderr:
                stderr = e.stderr if isinstance(e.stderr, str) else e.stderr.decode(errors="replace")
                logger.error(f"  Partial stderr: {stderr[-500:]}")

        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start_time
            logger.error(
                f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)"
            )
            logger.error(f"  Exit code: {e.returncode}")
            logger.error(f"  Stderr: {e.stderr[:500] if e.stderr else 'No error output'}")
            if e.stdout:
                logger.error(f"  Stdout: {e.stdout[:500]}")

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)"
            )
            logger.error(f"  Exception: {e}")

        # Retry with exponential backoff if we have attempts remaining
        if attempt < attempts:
            delay = retry_delay * (2 ** (attempt - 1))  # 15s, 30s, 60s, ...
            logger.info(f"  Retrying in {delay}s (attempt {attempt + 1}/{attempts})...")
            time.sleep(delay)

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
    # Each step is (command, description, critical, timeout_sec, max_retries).
    # Critical steps break the pipeline on failure; non-critical log a warning and continue.
    steps = [
        # Step 1: Scrape NBA game results (CDN-only to avoid stats.nba.com IP blocks)
        (
            f"{sys.executable} src/scrapers/nba_unified_scraper.py --cdn-only",
            "Scraping NBA Game Results (CDN)",
            True, 600, 2,    # 10m timeout, 2 retries
        ),
        # Step 2: Run incremental linker
        (
            f"{sys.executable} src/processing/nba_linker_local.py incremental",
            "Linking Players (Incremental)",
            True, 600, 2,    # 10m timeout, 2 retries
        ),
        # Step 3: Backfill team IDs (incremental - only recent data)
        (
            f"{sys.executable} src/processing/backfill_team_ids_incremental.py --days-back 7",
            "Backfilling Team IDs (Incremental)",
            False, 300, 0,   # 5m timeout, no retries
        ),
        # Step 4: Update player positions
        (
            f"{sys.executable} src/scrapers/update_player_position_history.py",
            "Updating Player Position History",
            False, 300, 0,   # 5m timeout, no retries
        ),
        # Step 5: Update league averages
        (
            f"{sys.executable} src/scrapers/update_league_position_averages.py",
            "Updating League Position Averages",
            False, 300, 0,   # 5m timeout, no retries
        ),
        # Step 6: Populate rolling averages (incremental - only yesterday's games)
        (
            f"{sys.executable} src/processing/populate_average_stats_incremental.py"
            f" --date {(datetime.now().date() - timedelta(days=1)).isoformat()}",
            "Populating Rolling Average Stats (Incremental)",
            True, 1200, 2,   # 20m timeout, 2 retries (most common timeout culprit)
        ),
        # Step 7: Update opponent allowed stats (incremental - last 2 days)
        (
            f"{sys.executable} src/processing/backfill_opponent_allowed_incremental.py --days-back 2",
            "Updating Opponent Allowed Stats (Incremental)",
            True, 900, 2,    # 15m timeout, 2 retries
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

    # Resolve pending paper bets (runs even if previous steps failed)
    # This is separate from the main loop because:
    # 1. It uses direct Python import, not subprocess
    # 2. It should not fail the job if it fails (stats are more critical)
    if not args.skip_resolution:
        resolve_pending_bets(args.dry_run)

    # Resolve pending user bets (from dashboard checkmark feature)
    if not args.skip_resolution:
        resolve_pending_user_bets(args.dry_run)

    # Resolve pending user DFS entries (parlay slips from DFS page)
    if not args.skip_resolution:
        resolve_pending_user_dfs_entries(args.dry_run)

    # Run calibration drift check (after bet resolution)
    if not args.skip_resolution:
        run_calibration_check(args.dry_run)

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
