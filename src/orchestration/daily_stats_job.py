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
    python src/orchestration/daily_stats_job.py [--dry-run] [--skip-resolution] [--skip-linker] [--basic-only]

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
import os
import re
import shlex
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text

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
MAX_LOG_TEXT = 2_000
last_command_error: str | None = None


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


def _sanitize_text(value: object, limit: int = MAX_LOG_TEXT) -> str:
    """Bound log text and remove credential-bearing URLs/known secret values."""
    text_value = str(value or "")
    text_value = re.sub(
        r"(?i)\b(?:postgres(?:ql)?|https?|socks5?)://[^\s]+",
        "[REDACTED_URL]",
        text_value,
    )
    for name, secret in os.environ.items():
        upper_name = name.upper()
        if secret and len(secret) >= 8 and any(
            marker in upper_name for marker in ("SECRET", "TOKEN", "PASSWORD", "DATABASE_URL", "PROXY_URL", "API_KEY")
        ):
            text_value = text_value.replace(secret, f"[REDACTED_{upper_name}]")
    return text_value[-limit:]


def _command_argv(command: str | Sequence[object]) -> list[str]:
    """Return argv without mangling Windows paths or arguments containing spaces."""
    if not isinstance(command, str):
        return [str(part) for part in command]
    parts = shlex.split(command, posix=os.name != "nt")
    if os.name == "nt":
        parts = [part[1:-1] if len(part) >= 2 and part[0] == part[-1] == '"' else part for part in parts]
    return parts


def run_command(
    command: str | Sequence[object],
    description: str,
    dry_run: bool = False,
    timeout: int = 600,
    max_retries: int = 0,
    retry_delay: int = 15,
    retry_only_if: str | None = None,
) -> bool:
    """Run argv from the project root and retain a sanitized final failure."""
    global last_command_error
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: {description}")
    cmd_args = _command_argv(command)

    if dry_run:
        logger.info("  Command: %s", _sanitize_text(subprocess.list2cmdline(cmd_args), 1_000))
        return True

    attempts = 1 + max_retries
    final_error = "unknown command failure"
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
            suffix = f" (attempt {attempt}/{attempts})" if attempt > 1 else ""
            logger.info(f"COMPLETED{suffix}: {description} ({elapsed:.1f}s)")
            if result.stdout:
                logger.debug("  Output: %s", _sanitize_text(result.stdout, 500))
            return True
        except subprocess.TimeoutExpired as exc:
            elapsed = time.time() - start_time
            stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else exc.stderr
            stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else exc.stdout
            final_error = _sanitize_text(
                f"timeout after {timeout}s; stderr={stderr or ''}; stdout={stdout or ''}"
            )
            logger.error(f"TIMED OUT (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            logger.error("  %s", final_error)
        except subprocess.CalledProcessError as exc:
            elapsed = time.time() - start_time
            final_error = _sanitize_text(
                f"exit {exc.returncode}; stderr={exc.stderr or ''}; stdout={exc.stdout or ''}"
            )
            logger.error(f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            logger.error("  %s", final_error)
        except Exception as exc:
            elapsed = time.time() - start_time
            final_error = _sanitize_text(f"{type(exc).__name__}: {exc}")
            logger.error(f"FAILED (attempt {attempt}/{attempts}): {description} ({elapsed:.1f}s)")
            logger.error("  %s", final_error)

        if attempt < attempts:
            if retry_only_if is not None and retry_only_if not in final_error:
                break
            delay = retry_delay * (2 ** (attempt - 1))
            logger.info(f"  Retrying in {delay}s (attempt {attempt + 1}/{attempts})...")
            time.sleep(delay)

    last_command_error = f"{description}: {final_error}"
    return False


def discover_basic_catchup(engine, max_dates: int) -> tuple[str | None, list[date], int]:
    """Find oldest completed dates missing player/team basic average keys."""
    query = text("""
        WITH latest_season AS (
            SELECT season_id
            FROM player_game_stats
            WHERE game_id LIKE ANY (ARRAY['002%', '004%', '005%'])
              AND game_date::date < CURRENT_DATE
            ORDER BY game_date DESC
            LIMIT 1
        ), missing_dates AS (
            SELECT DISTINCT pgs.game_date::date AS game_date
            FROM player_game_stats pgs
            JOIN latest_season ls ON ls.season_id = pgs.season_id
            LEFT JOIN player_average_game_stats pags
              ON pags.player_id = pgs.player_id AND pags.game_id = pgs.game_id
            WHERE pgs.game_date::date < CURRENT_DATE
              AND (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
              AND pags.player_id IS NULL
            UNION
            SELECT DISTINCT tgs.game_date::date AS game_date
            FROM team_game_stats tgs
            JOIN latest_season ls ON ls.season_id = tgs.season_id
            LEFT JOIN team_average_game_stats tags
              ON tags.team_id = tgs.team_id AND tags.game_id = tgs.game_id
            WHERE tgs.game_date::date < CURRENT_DATE
              AND tags.team_id IS NULL
            UNION
            SELECT DISTINCT tgs.game_date::date AS game_date
            FROM team_game_stats tgs
            JOIN latest_season ls ON ls.season_id = tgs.season_id
            WHERE tgs.game_date::date < CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1 FROM player_game_stats pgs
                  WHERE pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
                    AND pgs.season_id = ls.season_id
              )
            UNION
            SELECT DISTINCT pgs.game_date::date AS game_date
            FROM player_game_stats pgs
            JOIN latest_season ls ON ls.season_id = pgs.season_id
            WHERE pgs.game_date::date < CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1 FROM team_game_stats tgs
                  WHERE tgs.game_id = pgs.game_id AND tgs.team_id = pgs.team_id
                    AND tgs.season_id = ls.season_id
              )
        )
        SELECT (SELECT season_id FROM latest_season), game_date,
               COUNT(*) OVER () AS total_missing_dates
        FROM missing_dates
        ORDER BY game_date ASC
        LIMIT :max_dates
    """)
    with engine.connect() as conn:
        rows = list(conn.execute(query, {"max_dates": max_dates}))
    if not rows:
        with engine.connect() as conn:
            season = conn.execute(text("""
                SELECT season_id FROM player_game_stats
                WHERE game_id LIKE ANY (ARRAY['002%', '004%', '005%'])
                  AND game_date::date < CURRENT_DATE
                ORDER BY game_date DESC LIMIT 1
            """)).scalar()
        return season, [], 0
    return rows[0][0], [row[1] for row in rows], int(rows[0][2])


def count_missing_basic_keys(engine, season_id: str, target_dates: list[date]) -> int:
    """Recheck selected source-to-derived player/team basic keys."""
    if not target_dates:
        return 0
    query = text("""
        SELECT
          (SELECT COUNT(*) FROM player_game_stats pgs
           LEFT JOIN player_average_game_stats pags
             ON pags.player_id = pgs.player_id AND pags.game_id = pgs.game_id
           WHERE pgs.season_id = :season_id AND pgs.game_date::date = ANY(:dates)
             AND (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
             AND pags.player_id IS NULL)
        + (SELECT COUNT(*) FROM team_game_stats tgs
           LEFT JOIN team_average_game_stats tags
             ON tags.team_id = tgs.team_id AND tags.game_id = tgs.game_id
           WHERE tgs.season_id = :season_id AND tgs.game_date::date = ANY(:dates)
             AND tags.team_id IS NULL)
        + (SELECT COUNT(*) FROM team_game_stats tgs
           WHERE tgs.season_id = :season_id AND tgs.game_date::date = ANY(:dates)
             AND NOT EXISTS (
                 SELECT 1 FROM player_game_stats pgs
                 WHERE pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
             ))
        + (SELECT COUNT(DISTINCT (pgs.game_id, pgs.team_id)) FROM player_game_stats pgs
           WHERE pgs.season_id = :season_id AND pgs.game_date::date = ANY(:dates)
             AND NOT EXISTS (
                 SELECT 1 FROM team_game_stats tgs
                 WHERE tgs.game_id = pgs.game_id AND tgs.team_id = pgs.team_id
             ))
    """)
    with engine.connect() as conn:
        return int(conn.execute(query, {"season_id": season_id, "dates": target_dates}).scalar_one())


def count_missing_advanced_pairs(engine, season_id: str, through_date: date) -> int:
    """Count played traditional player/game pairs lacking advanced possessions."""
    query = text("""
        SELECT (SELECT COUNT(*)
        FROM player_game_stats pgs
        LEFT JOIN player_game_advanced_stats adv
          ON adv.player_id = pgs.player_id AND adv.game_id = pgs.game_id
        WHERE pgs.season_id = :season_id
          AND pgs.game_date::date <= :through_date
          AND pgs.game_date::date < CURRENT_DATE
          AND (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
          AND pgs.min > 0
          AND (adv.player_id IS NULL OR adv.possessions IS NULL))
        + (SELECT COUNT(*) FROM team_game_stats tgs
           WHERE tgs.season_id = :season_id AND tgs.game_date::date <= :through_date
             AND tgs.game_date::date < CURRENT_DATE
             AND (tgs.possessions IS NULL OR tgs.offensive_rating IS NULL OR tgs.pace IS NULL))
    """)
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        return int(conn.execute(query, {"season_id": season_id, "through_date": through_date}).scalar_one())


def latest_completed_source_date(engine, season_id: str) -> date | None:
    """Return the latest completed competitive basic-source date in a season."""
    query = text("""
        SELECT MAX(game_date::date)
        FROM player_game_stats
        WHERE season_id = :season_id
          AND game_id LIKE ANY (ARRAY['002%', '004%', '005%'])
          AND game_date::date < CURRENT_DATE
    """)
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '10s'"))
        return conn.execute(query, {"season_id": season_id}).scalar_one()


def discover_advanced_catchup(engine, season_id: str, through_date: date, max_dates: int) -> tuple[list[date], int]:
    """Find advanced rolling outputs still missing after source coverage passes."""
    query = text("""
        WITH missing_dates AS (
            SELECT DISTINCT pgs.game_date::date AS game_date
            FROM player_game_stats pgs
            LEFT JOIN player_average_advanced_stats av
              ON av.player_id = pgs.player_id AND av.game_id = pgs.game_id
            WHERE pgs.season_id = :season_id AND pgs.game_date::date <= :through_date
              AND pgs.min > 0 AND av.player_id IS NULL
            UNION
            SELECT DISTINCT tgs.game_date::date AS game_date
            FROM team_game_stats tgs
            JOIN team_average_game_stats av
              ON av.team_id = tgs.team_id AND av.game_id = tgs.game_id
            WHERE tgs.season_id = :season_id AND tgs.game_date::date <= :through_date
              AND av.games_szn > 0 AND av.avg_poss_szn IS NULL
        )
        SELECT game_date, COUNT(*) OVER () FROM missing_dates
        ORDER BY game_date LIMIT :max_dates
    """)
    with engine.connect() as conn:
        rows = list(conn.execute(query, {
            "season_id": season_id, "through_date": through_date, "max_dates": max_dates,
        }))
    return ([row[0] for row in rows], int(rows[0][1])) if rows else ([], 0)


def main():
    parser = argparse.ArgumentParser(
        description="Daily Stats Job - NBA Game Results & Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Show commands; do not access DB/API or write")
    parser.add_argument("--skip-resolution", action="store_true", help="Skip paper/user/DFS resolution and calibration")
    parser.add_argument("--skip-linker", action="store_true", help="Skip bounded props linking; source/derived checks remain active")
    parser.add_argument(
        "--basic-only",
        action="store_true",
        help="Run CDN/basic catch-up and bounded props linking; omit advanced-dependent derived stages",
    )
    parser.add_argument(
        "--max-catchup-dates",
        type=int,
        default=14,
        help="Maximum oldest missing completed basic-stat dates to repair per run (default: 14)",
    )
    args = parser.parse_args()
    if args.max_catchup_dates <= 0:
        parser.error("--max-catchup-dates must be positive")

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"DAILY STATS JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    success = True
    first_failure: str | None = None

    def fail(message: str) -> None:
        nonlocal success, first_failure
        success = False
        sanitized = _sanitize_text(message)
        if first_failure is None:
            first_failure = sanitized
        logger.error(sanitized)

    python = sys.executable
    scrape_ok = run_command(
        [python, "src/scrapers/nba_unified_scraper.py", "--cdn-only"],
        "Scraping NBA Game Results (CDN)",
        args.dry_run,
        timeout=600,
        max_retries=2,
    )
    if not scrape_ok:
        fail(last_command_error or "Scraping NBA Game Results (CDN) failed")

    if args.dry_run:
        logger.info(
            "[DRY RUN] BASIC catch-up dates/season/coverage are runtime-discovered: UNKNOWN; "
            "no database planning query was executed"
        )
        if args.basic_only:
            logger.info(
                "[DRY RUN] BASIC-ONLY mode omits the advanced coverage gate and all "
                "advanced-dependent derived stages; this does not establish advanced readiness"
            )
        else:
            logger.info(
                "[DRY RUN] ADVANCED coverage gate is runtime-discovered: UNKNOWN; "
                "advanced-dependent stages would run only after complete possession coverage"
            )
    elif scrape_ok:
        try:
            from src.db.client import get_engine

            engine = get_engine()
            season_id, catchup_dates, total_missing_dates = discover_basic_catchup(
                engine, args.max_catchup_dates
            )
            if season_id is None:
                fail("BASIC catch-up BLOCKED: no completed competitive source season was found")
            else:
                logger.info(
                    "BASIC catch-up: season=%s selected_dates=%d total_missing_dates=%d",
                    season_id,
                    len(catchup_dates),
                    total_missing_dates,
                )
                for target_date in catchup_dates:
                    if not run_command(
                        [
                            python,
                            "src/processing/populate_average_stats_incremental.py",
                            "--date",
                            target_date.isoformat(),
                            "--basic-only",
                        ],
                        f"Populating Basic Rolling Averages for {target_date}",
                        timeout=1200,
                        max_retries=2,
                    ):
                        fail(last_command_error or f"Basic rolling averages failed for {target_date}")
                        break

                missing_keys = count_missing_basic_keys(engine, season_id, catchup_dates)
                if missing_keys:
                    fail(
                        f"BASIC catch-up INCOMPLETE: {missing_keys} selected player/team source keys "
                        "still lack derived average rows"
                    )
                elif catchup_dates:
                    logger.info("BASIC catch-up verified complete for %d selected dates", len(catchup_dates))
                else:
                    logger.info("BASIC averages already cover all completed source dates in %s", season_id)

                if total_missing_dates > len(catchup_dates):
                    fail(
                        f"BASIC catch-up INCOMPLETE: {total_missing_dates - len(catchup_dates)} "
                        f"missing dates remain beyond the {args.max_catchup_dates}-date cap"
                    )

                # The bounded durable linker below owns game-specific team fills.
                # Do not run the legacy date-scan/latest-team backfill here.

                if args.basic_only:
                    logger.info(
                        "BASIC-ONLY scope complete: advanced coverage and advanced-dependent "
                        "derived stages intentionally omitted; advanced readiness is unchanged"
                    )
                else:
                    through_date = latest_completed_source_date(engine, season_id)
                    if through_date is None:
                        fail("ADVANCED-DEPENDENT STAGES BLOCKED: no completed source target date")
                        missing_advanced = 0
                    else:
                        missing_advanced = count_missing_advanced_pairs(engine, season_id, through_date)
                    if through_date is not None and missing_advanced:
                        fail(
                            f"ADVANCED-DEPENDENT STAGES DEFERRED/BLOCKED: {missing_advanced} played "
                            f"player-game pairs through {through_date} lack advanced possessions"
                        )
                    elif success and through_date is not None:
                        advanced_dates, _ = discover_advanced_catchup(
                            engine, season_id, through_date, args.max_catchup_dates
                        )
                        for target_date in advanced_dates:
                            if not run_command(
                                [python, "src/processing/populate_average_stats_incremental.py", "--date", target_date.isoformat()],
                                f"Populating Verified Advanced Rolling Averages for {target_date}",
                                timeout=1200, max_retries=2,
                            ):
                                fail(last_command_error or f"Advanced rolling averages failed for {target_date}")
                                break
                        _, remaining_advanced = discover_advanced_catchup(
                            engine, season_id, through_date, args.max_catchup_dates
                        )
                        if remaining_advanced:
                            fail(f"ADVANCED rolling catch-up INCOMPLETE: {remaining_advanced} dates still lack outputs")
                        advanced_steps: list[tuple[list[str], str, bool, int, int]] = [
                            (
                                [python, "src/scrapers/update_player_position_history.py"],
                                "Updating Player Position History",
                                True,
                                300,
                                0,
                            ),
                            (
                                [python, "src/scrapers/update_league_position_averages.py"],
                                "Updating League Position Averages",
                                True,
                                300,
                                0,
                            ),
                            (
                                [python, "src/processing/backfill_opponent_allowed_incremental.py", "--days-back", "2"],
                                "Updating Opponent Allowed Stats (Incremental)",
                                True,
                                900,
                                2,
                            ),
                        ]
                        for command, description, critical, timeout, retries in advanced_steps:
                            if not success:
                                break
                            if not run_command(
                                command,
                                description,
                                timeout=timeout,
                                max_retries=retries,
                            ):
                                message = last_command_error or f"{description} failed"
                                if critical:
                                    fail(message)
                                    break
                                logger.warning("Non-critical step failed: %s", message)
                # Linking is independent of advanced availability and always bounded.
                if args.skip_linker:
                    logger.info("Props linking intentionally skipped by --skip-linker")
                elif not run_command(
                    [python, "src/processing/nba_linker_local.py", "incremental", "--resume", "--repair-game-conflicts",
                     "--batch-size", "500", "--max-batches", "20"],
                    "Linking Props (Bounded Durable Resume)", timeout=600,
                    max_retries=4, retry_delay=30,
                    retry_only_if="another production props linker invocation holds the durable checkpoint lock",
                ):
                    fail(last_command_error or "Bounded props linking failed or remains incomplete")
        except Exception as exc:
            fail(f"Daily stats database planning/verification failed: {type(exc).__name__}: {exc}")

    if not args.skip_resolution:
        resolve_pending_bets(args.dry_run)
        resolve_pending_user_bets(args.dry_run)
        resolve_pending_user_dfs_entries(args.dry_run)
        run_calibration_check(args.dry_run)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    if success:
        if args.basic_only:
            logger.info(
                f"DAILY STATS BASIC-ONLY JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s); "
                "advanced readiness was not established"
            )
        else:
            logger.info(f"DAILY STATS JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
    else:
        logger.error(f"DAILY STATS JOB FAILED ({elapsed:.1f}s)")
        logger.error("FIRST FAILURE (retained for scheduler stderr tail): %s", first_failure)
    logger.info("=" * 60)
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
