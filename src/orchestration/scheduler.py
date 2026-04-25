#!/usr/bin/env python3
"""
Scheduler - APScheduler-based job runner for Railway
=====================================================
Runs all daily jobs on schedule using APScheduler.
All times are in America/New_York (ET). DST transitions are handled
automatically by APScheduler + pytz.

Schedule (ET):
    9:00 AM  - daily_stats_job
    9:00 AM  - mlb_roster_scraper_job
    9:00 AM  - mlb_daily_stats_job
    9:00 AM  - nonsports_polymarket_scrape (2hr timeout)
    9:20 AM  - mlb_daily_stats_retry
    9:15 AM  - Kalshi live resolution
    9:25 AM  - mlb_weather_forecast
    9:30 AM  - daily_stats_retry (if 9 AM failed)
    9:30 AM  - mlb_lines_job --live --props-only --extended
    9:35 AM  - mlb_lineup_scraper_job
    9:50 AM  - mlb_inference_job (early MLB pass)

    10:00 AM - kalshi_daily_summary_job
    10:00 AM - lines_job --live --props-only (pre-NBA inference)
    10:15 AM - inference_job (early NBA pass)

    9 AM - 11 PM ET every 5 min:
        :00,:05,...,:55  - lines_job --live --props-only  (silent)
        :02,:07,...,:57  - edge_refresh_job               (silent)

    9 AM - 11 PM ET every 10 min:
        kalshi_refresh (NBA + MLB + non-sports)

    12:00 PM - lines_job --live (full)
    12:15 PM - inference_job (full MC)
    12:15 PM - mlb_inference_job (noon MLB pass)

    4:00 PM  - lines_job --live --parallel (full)
    4:15 PM  - inference_job (full MC)
    5:00 PM  - nonsports_polymarket_scrape (2nd run)

    "silent" = Discord alerts only on failure.

Usage:
    python src/orchestration/scheduler.py              # Start scheduler loop
    python src/orchestration/scheduler.py --run-test   # Run test job and exit
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Scheduler")

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Ensure project root is on sys.path so 'src.*' imports work
# (needed for Discord alert imports within this process)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Job display names for alerts
JOB_NAMES = {
    "daily_stats_job.py": "Daily Stats",
    "lines_job.py": "Lines Scraper",
    "inference_job.py": "Inference",
    "edge_refresh_job.py": "Edge Refresh",
    "test_job.py": "System Test",
    "mlb_daily_stats_job.py": "MLB Daily Stats",
    "mlb_lines_job.py": "MLB Lines Scraper",
    "mlb_inference_job.py": "MLB Inference",
    "mlb_edge_refresh_job.py": "MLB Edge Refresh",
    "kalshi_refresh_job.py": "Kalshi Refresh",
    "archive_old_props_job.py": "Archive Old Props",
    "kalshi_daily_summary_job.py": "Kalshi Daily Summary",
    "arb_scan_job.py": "Arb Scanner",
    "kalshi_nonsports_refresh_job.py": "Kalshi Non-Sports Refresh",
    "kalshi_execute_approved_job.py": "Kalshi Execute Approved",
    "kalshi_reprice_stale_job.py": "Kalshi Reprice Stale",
    "kalshi_pending_fills_job.py": "Kalshi Pending Fills",
    "resolve_user_paper_bets.py": "User Paper Bet Resolution",
}

# In-memory job status tracking for dependency checks.
# Updated by run_job() after every execution.
# Format: {"daily_stats_job.py": {"status": "success", "end_time": datetime, "duration": float}}
JOB_STATUS: dict[str, dict] = {}


def record_job_execution(
    job_name: str,
    started_at: datetime,
    status: str,
    duration: float,
    error_message: str | None = None,
    metrics: dict | None = None,
) -> None:
    """Record a job execution to the job_executions table in Supabase.

    Non-fatal: failures are logged but don't affect job status.
    """
    try:
        from src.db.client import get_engine

        engine = get_engine()
        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO job_executions
                        (job_name, started_at, ended_at, status, duration_seconds, error_message, metrics)
                    VALUES
                        (:job_name, :started_at, :ended_at, :status, :duration, :error_message, :metrics)
                """),
                {
                    "job_name": job_name,
                    "started_at": started_at,
                    "ended_at": datetime.now(UTC),
                    "status": status,
                    "duration": duration,
                    "error_message": error_message,
                    "metrics": json.dumps(metrics) if metrics else None,
                },
            )
        logger.debug(f"Recorded job execution: {job_name} ({status})")
    except Exception as e:
        logger.warning(f"Failed to record job execution for {job_name}: {e}")


def check_dependency(upstream_job: str, max_age_hours: float = 8) -> bool:
    """Return True if upstream job succeeded within max_age_hours.

    Checks in-memory JOB_STATUS first, then falls back to the
    job_executions table so dependency checks survive redeployments.
    """
    # 1. Fast path: in-memory check
    status = JOB_STATUS.get(upstream_job, {})
    if status.get("status") == "success":
        end_time = status.get("end_time")
        if end_time is not None:
            age_hours = (datetime.now(UTC) - end_time).total_seconds() / 3600
            if age_hours <= max_age_hours:
                return True

    # 2. Fallback: query job_executions table (survives redeployments)
    try:
        from src.db.client import get_engine

        engine = get_engine()
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT ended_at FROM job_executions
                    WHERE job_name = :job_name
                      AND status = 'success'
                      AND ended_at > now() - make_interval(hours => :max_age)
                    ORDER BY ended_at DESC
                    LIMIT 1
                """),
                {"job_name": upstream_job, "max_age": max_age_hours},
            ).fetchone()
        if row is not None:
            logger.info(
                f"Dependency '{upstream_job}' satisfied via job_executions table "
                f"(ended_at={row[0]})"
            )
            return True
    except Exception as e:
        logger.warning(f"Failed to check job_executions for {upstream_job}: {e}")

    return False


def _parse_metrics_from_output(script_name: str, stdout: str, stderr: str) -> dict | None:
    """Extract metrics from job output for display in alerts.

    Args:
        script_name: The script that was run
        stdout: Standard output from the job
        stderr: Standard error from the job

    Returns:
        Dict of metrics to display, or None if no metrics found
    """
    metrics = {}
    output = stdout + "\n" + stderr

    if script_name == "daily_stats_job.py":
        # Look for step completion info
        steps_matches = re.findall(r"Step (\d+)/(\d+)", output)
        if steps_matches:
            last_step, total = steps_matches[-1]
            metrics["steps_completed"] = f"{last_step}/{total}"

        # Look for bet resolution stats
        resolved_match = re.search(r"Resolved (\d+) bets?", output, re.IGNORECASE)
        if resolved_match:
            metrics["bets_resolved"] = resolved_match.group(1)

    elif script_name == "lines_job.py":
        # Look for props scraped info
        props_match = re.search(r"(\d+) props?", output, re.IGNORECASE)
        if props_match:
            metrics["props_scraped"] = props_match.group(1)

        # Look for games found
        games_match = re.search(r"(\d+) games?", output, re.IGNORECASE)
        if games_match:
            metrics["games_found"] = games_match.group(1)

    elif script_name == "inference_job.py":
        # Look for predictions generated
        preds_match = re.search(r"Generated (\d+) predictions?", output, re.IGNORECASE)
        if preds_match:
            metrics["predictions"] = preds_match.group(1)

        # Look for high-edge count
        edge_match = re.search(r"(\d+) with.*edge", output, re.IGNORECASE)
        if edge_match:
            metrics["high_edge"] = edge_match.group(1)

    elif script_name == "edge_refresh_job.py":
        # Look for updated predictions count
        updated_match = re.search(r"Predictions updated: (\d+)", output)
        if updated_match:
            metrics["predictions_updated"] = updated_match.group(1)

        rec_match = re.search(r"Recommended picks: (\d+)", output)
        if rec_match:
            metrics["recommended"] = rec_match.group(1)

    elif script_name == "kalshi_refresh_job.py":
        # Look for edge computation counts
        matched_match = re.search(r"(\d+) matched", output)
        if matched_match:
            metrics["markets_matched"] = matched_match.group(1)

        updated_match = re.search(r"(\d+) updated", output)
        if updated_match:
            metrics["edges_updated"] = updated_match.group(1)

        parsed_match = re.search(r"(\d+) parsed", output)
        if parsed_match:
            metrics["markets_parsed"] = parsed_match.group(1)

    elif script_name == "test_job.py":
        # Look for checks passed
        checks_match = re.search(r"(\d+)/(\d+) checks passed", output)
        if checks_match:
            metrics["checks_passed"] = f"{checks_match.group(1)}/{checks_match.group(2)}"

        # Look for env var counts
        req_match = re.search(r"Required env vars: (\d+/\d+) set", output)
        if req_match:
            metrics["required_env_vars"] = req_match.group(1)

    return metrics if metrics else None


def _send_job_alert(
    script_name: str,
    success: bool,
    duration: float,
    stdout: str,
    stderr: str,
):
    """Send Discord alert for job completion.

    Non-fatal: failures are logged but don't affect job status.
    """
    try:
        from src.discord_bot.alerts import send_job_alert_sync

        job_name = JOB_NAMES.get(script_name, script_name)
        metrics = _parse_metrics_from_output(script_name, stdout, stderr)

        # Get error message for failed jobs
        error_message = None
        if not success and stderr:
            # Get last 500 chars of stderr for error display
            error_message = stderr.strip()[-500:]

        send_job_alert_sync(
            job_name=job_name,
            success=success,
            duration_seconds=duration,
            metrics=metrics,
            error_message=error_message,
        )

    except Exception as e:
        logger.warning(f"Failed to send Discord alert for {script_name}: {e}")


def run_job(script_name: str, extra_args: str = "", silent_on_success: bool = False, timeout: int = 2700):
    """Run a job script as a subprocess and send alert on completion.

    Args:
        script_name: The script to run.
        extra_args: Additional CLI arguments.
        silent_on_success: If True, skip Discord alerts on success (still alert on failure).
        timeout: Subprocess timeout in seconds (default 2700 = 45 min).
    """
    script_path = PROJECT_ROOT / "src" / "orchestration" / script_name
    cmd = [sys.executable, str(script_path)] + (shlex.split(extra_args) if extra_args else [])
    logger.info(f"Starting job: {script_name}{' ' + extra_args if extra_args else ''}")

    start_time = time.time()
    started_at = datetime.now(UTC)
    success = False
    stdout = ""
    stderr = ""
    job_status = "failed"

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        stdout = result.stdout or ""
        stderr = result.stderr or ""
        duration = time.time() - start_time

        if result.returncode == 0:
            logger.info(f"Job completed successfully: {script_name}")
            # Surface final summary lines from both stdout and stderr so Railway
            # logs show outcomes even when silent_on_success=True suppresses Discord.
            combined = (stderr + "\n" + stdout).strip()
            if combined:
                for line in combined.splitlines()[-50:]:
                    if line.strip():
                        logger.info(f"  [out] {line.strip()}")
            success = True
            job_status = "success"
        else:
            logger.error(f"Job failed: {script_name}")
            logger.error(f"STDERR: {stderr[-2000:] if stderr else 'None'}")

        # Send alert (skip success alerts if silent_on_success)
        if not (silent_on_success and success):
            _send_job_alert(script_name, success, duration, stdout, stderr)

    except subprocess.TimeoutExpired as e:
        duration = time.time() - start_time
        job_status = "timeout"
        timeout_mins = int(timeout // 60)
        logger.error(f"Job timed out after {timeout_mins} minutes: {script_name}")
        partial_stdout = ""
        partial_stderr = f"Job timed out after {timeout_mins} minutes"
        if e.stdout:
            partial_stdout = e.stdout if isinstance(e.stdout, str) else e.stdout.decode(errors="replace")
        if e.stderr:
            partial_stderr = (e.stderr if isinstance(e.stderr, str) else e.stderr.decode(errors="replace"))
            partial_stderr += f"\n\nJob timed out after {timeout_mins} minutes"
        _send_job_alert(
            script_name,
            success=False,
            duration=duration,
            stdout=partial_stdout,
            stderr=partial_stderr,
        )

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Job error: {script_name} - {e}")
        _send_job_alert(
            script_name,
            success=False,
            duration=duration,
            stdout="",
            stderr=str(e),
        )

    # Update in-memory status for dependency checks
    JOB_STATUS[script_name] = {
        "status": job_status,
        "end_time": datetime.now(UTC),
        "duration": duration,
    }

    # Record to persistent DB (non-fatal)
    metrics = _parse_metrics_from_output(script_name, stdout, stderr)
    error_msg = stderr.strip()[-500:] if not success and stderr else None
    record_job_execution(
        job_name=script_name,
        started_at=started_at,
        status=job_status,
        duration=duration,
        error_message=error_msg,
        metrics=metrics,
    )


def _validate_environment():
    """Log status of required and optional env vars at startup."""
    required = [
        ("DATABASE_URL", "Required for all jobs"),
        ("ODDS_API_KEY", "Required for lines scraping"),
        ("RAPIDAPI_KEY", "Required for injury scraping"),
    ]
    optional = [
        ("DISCORD_BOT_TOKEN", "Discord alerts"),
        ("DISCORD_CHANNEL_ALERTS", "Job notifications"),
        ("DISCORD_CHANNEL_PREDICTIONS", "Prediction alerts"),
        ("DISCORD_CHANNEL_PERFORMANCE", "P&L summaries"),
        ("KALSHI_API_KEY", "Kalshi prediction markets"),
        ("NBA_PLAYOFF_MODE", "Use playoff model for NBA inference (set true Apr 19 - Jun 20)"),
    ]

    logger.info("Environment check:")
    missing_required = []

    for var, desc in required:
        if os.getenv(var):
            logger.info(f"  [OK]      {var} — {desc}")
        else:
            logger.warning(f"  [MISSING] {var} — {desc}")
            missing_required.append(var)

    for var, desc in optional:
        if os.getenv(var):
            logger.info(f"  [OK]      {var} — {desc}")
        else:
            logger.info(f"  [--]      {var} — {desc} (optional)")

    if missing_required:
        logger.warning(
            f"Missing required env vars: {', '.join(missing_required)}. "
            "Jobs that need these will fail. "
            "Set them in Railway service variables or local .env file."
        )


def run_daily_stats():
    run_job("daily_stats_job.py")


def run_daily_stats_retry():
    """Re-run daily stats if the 11 AM run failed or didn't run."""
    status = JOB_STATUS.get("daily_stats_job.py", {})
    if status.get("status") == "success":
        logger.info("Daily stats already succeeded today, skipping 11:30 retry.")
        return
    logger.warning("Daily stats failed or did not run at 11 AM — retrying now...")
    _send_job_alert(
        "daily_stats_job.py",
        success=False,
        duration=0,
        stdout="",
        stderr="11 AM daily stats job failed or missing — automatic retry at 11:30 AM ET",
    )
    run_job("daily_stats_job.py")


def run_lines_full():
    """Full lines scrape: game lines + props (live) + injuries + linker."""
    run_job("lines_job.py", extra_args="--live")


def run_lines_full_parallel():
    """Full lines scrape with parallel execution (props + injuries concurrently)."""
    run_job("lines_job.py", extra_args="--live --parallel")


def run_lines_props_only():
    """Props-only scrape: live props + linker (no game lines or injuries)."""
    run_job("lines_job.py", extra_args="--live --props-only")


def run_inference(skip_bets: bool = False):
    """Run inference, checking if daily stats succeeded first."""
    extra_parts = []
    if skip_bets:
        extra_parts.append("--skip-bets")
    if os.getenv("NBA_PLAYOFF_MODE", "").lower() in ("true", "1", "yes"):
        extra_parts.append("--model-dir src/models/artifacts/production_playoffs")
        logger.info("NBA playoff mode active — using production_playoffs model")
    extra = " ".join(extra_parts)
    if not check_dependency("daily_stats_job.py", max_age_hours=8):
        logger.warning(
            "Daily stats job has not succeeded in the last 8 hours — "
            "inference will run with potentially stale rolling averages."
        )
        _send_job_alert(
            "daily_stats_job.py",
            success=False,
            duration=0,
            stdout="",
            stderr=(
                "Daily stats job has not succeeded today — "
                "inference will run with stale rolling averages"
            ),
        )
        stale_extra = f"--stale-warning {extra}".strip()
        run_job("inference_job.py", extra_args=stale_extra)
    else:
        run_job("inference_job.py", extra_args=extra) if extra else run_job("inference_job.py")


def run_edge_refresh():
    """Lightweight edge recalculation using stored samples + fresh lines."""
    run_job("edge_refresh_job.py")


def run_lines_props_only_silent():
    """Props-only scrape, Discord alerts only on failure."""
    run_job("lines_job.py", extra_args="--live --props-only", silent_on_success=True)


def run_edge_refresh_silent():
    """Edge refresh, Discord alerts only on failure. Skips paper trading."""
    run_job("edge_refresh_job.py", extra_args="--skip-paper", silent_on_success=True)


# ---- MLB Jobs (April–October) ----

def run_mlb_daily_stats():
    """Run MLB daily stats job."""
    run_job("mlb_daily_stats_job.py")


def run_mlb_daily_stats_retry():
    """Re-run MLB daily stats if the 9 AM run failed."""
    status = JOB_STATUS.get("mlb_daily_stats_job.py", {})
    if status.get("status") == "success":
        logger.info("MLB daily stats already succeeded today, skipping 10:30 retry.")
        return
    logger.warning("MLB daily stats failed or did not run at 10 AM — retrying now...")
    run_job("mlb_daily_stats_job.py")


def run_mlb_lines_full():
    """Run MLB full lines scrape (game lines + props + linker)."""
    run_job("mlb_lines_job.py", extra_args="--live --parallel --extended")


def run_mlb_lines_props_only():
    """Run MLB props-only scrape (props + linker, silent on success)."""
    run_job("mlb_lines_job.py", extra_args="--live --props-only --extended", silent_on_success=True)


def run_mlb_lineup_scraper():
    """Scrape confirmed MLB batting lineups from the MLB Stats API."""
    run_job("mlb_lineup_scraper_job.py")


def run_mlb_roster_scraper():
    """Scrape 26-man active MLB rosters (IL/availability tracking)."""
    run_job("mlb_roster_scraper_job.py")


def run_mlb_weather_forecast():
    """Fetch weather forecast for today's MLB games (before inference window)."""
    run_job("mlb_weather_scraper_job.py", silent_on_success=True)


def run_mlb_inference():
    """Run MLB inference with paper betting, checking if MLB daily stats succeeded first.

    Active models: pitcher_strikeouts, batter_hits, batter_rbis.
    Each stat has per-stat Black-Litterman config in bl_config.yaml.
    """
    if not check_dependency("mlb_daily_stats_job.py", max_age_hours=8):
        logger.warning(
            "MLB daily stats job has not succeeded in the last 8 hours — "
            "MLB inference will run with potentially stale data."
        )
    run_job("mlb_inference_job.py")


def run_mlb_edge_refresh():
    """Lightweight MLB BL re-blending using stored samples + fresh lines."""
    run_job("mlb_edge_refresh_job.py", silent_on_success=True)


# ---- Kalshi Jobs ----

def run_archive_old_props():
    """Archive rows older than 30 days from raw_player_props_combined."""
    run_job("archive_old_props_job.py", silent_on_success=True)


def run_kalshi_refresh():
    """Kalshi NBA market refresh: scrape, compute edges, alert. Skips gracefully if no creds."""
    run_job("kalshi_refresh_job.py", extra_args="--sport nba", silent_on_success=True)


def run_kalshi_refresh_mlb():
    """Kalshi MLB market refresh: scrape, compute edges, alert. Skips gracefully if no creds."""
    run_job("kalshi_refresh_job.py", extra_args="--sport mlb", silent_on_success=True)


def run_kalshi_live_resolution():
    """Morning resolution of yesterday's live Kalshi bets."""
    run_job("kalshi_refresh_job.py", extra_args="--resolve-only --sport nba", silent_on_success=False)
    run_job("kalshi_refresh_job.py", extra_args="--resolve-only --sport mlb", silent_on_success=False)


def run_kalshi_daily_summary():
    """Daily Kalshi paper trading summary: resolve bets, P&L + analysis to Discord."""
    run_job("kalshi_daily_summary_job.py")


def run_kalshi_nonsports_refresh():
    """Kalshi non-sports market refresh: scrape economics/crypto markets with sport=NULL.

    Stores markets so the non-sports arb scanner can match them against Polymarket.
    Exits gracefully if KALSHI_API_KEY is not set.
    """
    run_job("kalshi_nonsports_refresh_job.py", silent_on_success=True)


def run_kalshi_execute_approved():
    """Execute Kalshi trades that were approved on the dashboard.

    Polls kalshi_trade_queue for status='approved' rows and places them via
    the Kalshi API. Exits gracefully if KALSHI_LIVE_TRADING_ENABLED != true
    or if there are no approved trades.
    """
    run_job("kalshi_execute_approved_job.py", silent_on_success=True)


def run_kalshi_reprice_stale():
    """Reprice stale resting Kalshi orders.

    Checks for resting orders where the market price has moved and reprices
    them if the edge is still retained. Exits gracefully if KALSHI_LIVE_TRADING_ENABLED != true.
    """
    run_job("kalshi_reprice_stale_job.py", silent_on_success=True)


def run_kalshi_pending_fills():
    """Poll Kalshi API every 5 min to catch pending orders that have since filled."""
    run_job("kalshi_pending_fills_job.py", silent_on_success=True)


def run_kalshi_stale_fills():
    """Detect pending orders whose game has started and enqueue for cancellation review."""
    run_job("kalshi_stale_fills_job.py", silent_on_success=True)


def run_kalshi_execute_cancellations():
    """Execute human-approved order cancellations via Kalshi API."""
    run_job("kalshi_execute_cancellations_job.py", silent_on_success=True)


# ---- Arbitrage Scanner Jobs ----

def run_arb_scan_mlb():
    """MLB Polymarket-Kalshi arb scan: scrape, match (props + game-level), detect, alert."""
    run_job("arb_scan_job.py", extra_args="--sport mlb --mode sport", silent_on_success=True)


def run_arb_scan_all_categories():
    """Non-sports Polymarket arb SCAN only (no scrape). Uses existing polymarket_markets data."""
    run_job("arb_scan_job.py", extra_args="--mode all --include-non-sports --skip-scrape --skip-paper", silent_on_success=True)


def run_nonsports_scrape():
    """Scrape ALL Polymarket categories into polymarket_markets (no scan/paper-trade).

    Slow job (~45-90 min) — runs 2x/day with extended timeout.
    Scan jobs read from this data via --skip-scrape.
    """
    run_job("arb_scan_job.py", extra_args="--mode all --scrape-only", silent_on_success=False, timeout=7200)


def run_user_paper_bet_resolution():
    """Resolve pending user paper bets against actual game stats."""
    run_job("resolve_user_paper_bets.py")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GameFlowData Scheduler")
    parser.add_argument(
        "--run-test",
        action="store_true",
        help="Run the test job immediately and exit (does not start scheduler loop)",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("GameFlowData Scheduler Starting")
    logger.info("=" * 60)

    _validate_environment()

    # --run-test: execute test_job.py via run_job() and exit
    if args.run_test:
        logger.info("Running infrastructure test job...")
        run_job("test_job.py")
        logger.info("Test job complete. Exiting.")
        return

    scheduler = BlockingScheduler(timezone="America/New_York")

    # Schedule jobs (all times in America/New_York ET)
    # ==============================================================

    ET = "America/New_York"

    # 9:00 AM ET - Daily stats
    scheduler.add_job(
        run_daily_stats,
        CronTrigger(hour=9, minute=0, timezone=ET),
        id="daily_stats",
        name="Daily Stats (9 AM ET)",
    )

    # 9:30 AM ET - Retry daily stats if 9 AM run failed
    scheduler.add_job(
        run_daily_stats_retry,
        CronTrigger(hour=9, minute=30, timezone=ET),
        id="daily_stats_retry",
        name="Daily Stats Retry (9:30 AM ET)",
    )

    # 9:30 AM ET - Resolve pending user paper bets against actual game stats
    scheduler.add_job(
        run_user_paper_bet_resolution,
        CronTrigger(hour=9, minute=30, timezone=ET),
        id="user_paper_bet_resolution",
        name="User Paper Bet Resolution (9:30 AM ET)",
    )

    # --- Early window: 11 AM NBA inference ---

    # 11:00 AM ET - Early props scrape (feed 11:15 AM NBA inference)
    scheduler.add_job(
        run_lines_props_only,
        CronTrigger(hour=11, minute=0, timezone=ET),
        id="lines_props_11am",
        name="Lines Props Only (11 AM ET, pre-inference)",
    )

    # 11:15 AM ET - Early NBA inference
    scheduler.add_job(
        run_inference,
        CronTrigger(hour=11, minute=15, timezone=ET),
        id="inference_11am",
        name="Inference (11:15 AM ET)",
    )

    # --- First window: noon full scrape + inference ---

    # 12:00 PM ET - Full lines scrape (live)
    scheduler.add_job(
        run_lines_full,
        CronTrigger(hour=12, minute=0, timezone=ET),
        id="lines_noon_full",
        name="Lines Full Parallel (12 PM ET)",
    )

    # 12:15 PM ET - Full inference
    scheduler.add_job(
        run_inference,
        CronTrigger(hour=12, minute=15, timezone=ET),
        id="inference_noon",
        name="Inference (12:15 PM ET)",
    )

    # --- Every 5 min props-only + edge refresh: 9 AM - 11 PM ET ---

    scheduler.add_job(
        run_lines_props_only_silent,
        CronTrigger(hour='9-23', minute='*/5', timezone=ET),
        id="props_every_5",
        name="Props Only (every 5 min, 9AM-11PM ET)",
    )

    scheduler.add_job(
        run_edge_refresh_silent,
        CronTrigger(hour='9-23', minute='2,7,12,17,22,27,32,37,42,47,52,57', timezone=ET),
        id="edge_refresh_every_5",
        name="Edge Refresh (every 5 min, 9AM-11PM ET)",
    )

    # --- Second window: 4 PM full scrape + inference ---

    # 4:00 PM ET - Full lines scrape (live, parallel)
    scheduler.add_job(
        run_lines_full_parallel,
        CronTrigger(hour=16, minute=0, timezone=ET),
        id="lines_4pm_full",
        name="Lines Full Parallel (4 PM ET)",
    )

    # 4:15 PM ET - Full inference (skip bets — already placed at noon)
    scheduler.add_job(
        lambda: run_inference(skip_bets=True),
        CronTrigger(hour=16, minute=15, timezone=ET),
        id="inference_4pm",
        name="Inference (4:15 PM ET, skip bets)",
    )

    # ==============================================================
    # Maintenance Jobs
    # ==============================================================

    # 3:00 AM ET - Archive old props (rows > 30 days)
    scheduler.add_job(
        run_archive_old_props,
        CronTrigger(hour=3, minute=0, timezone=ET),
        id="archive_old_props",
        name="Archive Old Props (3 AM ET)",
    )

    # ==============================================================
    # MLB Jobs
    # ==============================================================

    # 9:00 AM ET - MLB active roster (IL tracking, availability)
    scheduler.add_job(
        run_mlb_roster_scraper,
        CronTrigger(hour=9, minute=0, timezone=ET),
        id="mlb_roster_scraper",
        name="MLB Active Roster (9 AM ET)",
    )

    # 9:00 AM ET - MLB daily stats (results from last night)
    scheduler.add_job(
        run_mlb_daily_stats,
        CronTrigger(hour=9, minute=0, timezone=ET),
        id="mlb_daily_stats",
        name="MLB Daily Stats (9 AM ET)",
    )

    # 10:30 AM ET - Retry MLB daily stats if 10 AM failed
    scheduler.add_job(
        run_mlb_daily_stats_retry,
        CronTrigger(hour=10, minute=30, timezone=ET),
        id="mlb_daily_stats_retry",
        name="MLB Daily Stats Retry (10:30 AM ET)",
    )

    # --- MLB early window: 11 AM inference ---

    # 10:40 AM ET - MLB weather forecast (after daily stats retry, before props + inference)
    scheduler.add_job(
        run_mlb_weather_forecast,
        CronTrigger(hour=10, minute=40, timezone=ET),
        id="mlb_weather_forecast",
        name="MLB Weather Forecast (10:40 AM ET)",
    )

    # 10:45 AM ET - Early MLB props scrape (feed 11:00 AM inference)
    scheduler.add_job(
        run_mlb_lines_props_only,
        CronTrigger(hour=10, minute=45, timezone=ET),
        id="mlb_lines_props_1045am",
        name="MLB Props Only (10:45 AM ET, pre-inference)",
    )

    # 10:50 AM ET - Early MLB lineup scrape (best-available lineups)
    scheduler.add_job(
        run_mlb_lineup_scraper,
        CronTrigger(hour=10, minute=50, timezone=ET),
        id="mlb_lineup_scraper_1050am",
        name="MLB Lineup Scraper (10:50 AM ET)",
    )

    # 11:00 AM ET - Early MLB inference (pre-lineup-confirmation pass)
    scheduler.add_job(
        run_mlb_inference,
        CronTrigger(hour=11, minute=0, timezone=ET),
        id="mlb_inference_11am",
        name="MLB Inference (11:00 AM ET)",
    )

    # 12:00 PM ET - MLB full lines scrape (game lines + props + linker)
    scheduler.add_job(
        run_mlb_lines_full,
        CronTrigger(hour=12, minute=0, timezone=ET),
        id="mlb_lines_full_noon",
        name="MLB Full Lines (12 PM ET)",
    )

    # 12:15 PM ET - MLB noon inference (some lineups now confirmed)
    scheduler.add_job(
        run_mlb_inference,
        CronTrigger(hour=12, minute=15, timezone=ET),
        id="mlb_inference_noon",
        name="MLB Inference (12:15 PM ET)",
    )

    # 1:00 PM ET - MLB props-only refresh before afternoon games
    scheduler.add_job(
        run_mlb_lines_props_only,
        CronTrigger(hour=13, minute=0, timezone=ET),
        id="mlb_lines_props_1pm",
        name="MLB Props Only (1 PM ET)",
    )

    # 12:45 PM ET - MLB lineup confirmation (afternoon games; before 1:30 PM inference)
    scheduler.add_job(
        run_mlb_lineup_scraper,
        CronTrigger(hour=12, minute=45, timezone=ET),
        id="mlb_lineup_scraper_1pm",
        name="MLB Lineup Scraper (12:45 PM ET)",
    )

    # 1:30 PM ET - MLB inference (afternoon/evening games)
    scheduler.add_job(
        run_mlb_inference,
        CronTrigger(hour=13, minute=30, timezone=ET),
        id="mlb_inference_1pm",
        name="MLB Inference (1:30 PM ET)",
    )

    # 2:30 PM ET - MLB edge refresh (mid-afternoon, between day/evening game windows)
    scheduler.add_job(
        run_mlb_edge_refresh,
        CronTrigger(hour=14, minute=30, timezone=ET),
        id="mlb_edge_refresh_230pm",
        name="MLB Edge Refresh (2:30 PM ET)",
    )

    # 5:00 PM ET - MLB full lines scrape (catch new evening props)
    scheduler.add_job(
        run_mlb_lines_full,
        CronTrigger(hour=17, minute=0, timezone=ET),
        id="mlb_lines_full_5pm",
        name="MLB Full Lines (5 PM ET)",
    )

    # 6:00 PM ET - MLB props-only refresh before evening games
    scheduler.add_job(
        run_mlb_lines_props_only,
        CronTrigger(hour=18, minute=0, timezone=ET),
        id="mlb_lines_props_6pm",
        name="MLB Props Only (6 PM ET)",
    )

    # 6:10 PM ET - MLB lineup confirmation (evening games; before 6:30 PM inference)
    scheduler.add_job(
        run_mlb_lineup_scraper,
        CronTrigger(hour=18, minute=10, timezone=ET),
        id="mlb_lineup_scraper_6pm",
        name="MLB Lineup Scraper (6:10 PM ET)",
    )

    # 6:30 PM ET - MLB inference refresh (evening games)
    scheduler.add_job(
        run_mlb_inference,
        CronTrigger(hour=18, minute=30, timezone=ET),
        id="mlb_inference_6pm",
        name="MLB Inference (6:30 PM ET)",
    )

    # 4:30 PM ET - MLB edge refresh (after 5 PM lines, before evening games)
    scheduler.add_job(
        run_mlb_edge_refresh,
        CronTrigger(hour=16, minute=30, timezone=ET),
        id="mlb_edge_refresh_430pm",
        name="MLB Edge Refresh (4:30 PM ET)",
    )

    # ==============================================================
    # Kalshi Prediction Markets
    # ==============================================================

    # 9:15 AM ET - Kalshi live bet resolution (resolve yesterday's bets after stats pull)
    scheduler.add_job(
        run_kalshi_live_resolution,
        CronTrigger(hour=9, minute=15, timezone=ET),
        id="kalshi_live_resolution",
        name="Kalshi Live Resolution (9:15 AM ET daily)",
    )

    # 10:00 AM ET - Kalshi daily summary: resolve pending bets + P&L/analysis to Discord
    # Runs after NBA daily stats (9 AM) + first Kalshi refresh (~9:10 AM) so bets are resolved
    # and yesterday's daily log shows accurate data.
    scheduler.add_job(
        run_kalshi_daily_summary,
        CronTrigger(hour=10, minute=0, timezone=ET),
        id="kalshi_daily_summary",
        name="Kalshi Daily Summary (10 AM ET)",
    )

    # Every 10 min, 9 AM - 11 PM ET — scrape markets + compute edges
    # MLB runs first (minute=0,10,...) so its bets consume exposure cap before NBA.
    # NBA runs 2 minutes later (minute=2,12,...) so it only takes remaining cap.
    # Job exits gracefully if KALSHI_API_KEY is not set
    scheduler.add_job(
        run_kalshi_refresh_mlb,
        CronTrigger(hour='9-23', minute='0,10,20,30,40,50', timezone=ET),
        id="kalshi_refresh_mlb",
        name="Kalshi MLB Refresh (every 10 min on :00, 9AM-11PM ET — MLB first for exposure priority)",
    )

    scheduler.add_job(
        run_kalshi_refresh,
        CronTrigger(hour='9-23', minute='2,12,22,32,42,52', timezone=ET),
        id="kalshi_refresh_nba",
        name="Kalshi NBA Refresh (every 10 min on :02, 9AM-11PM ET — after MLB)",
    )

    # Every 10 min, 9 AM - 11 PM ET — scrape non-sports (economics/crypto) markets
    # Stores with sport=NULL so non-sports arb scan can match vs Polymarket.
    # Exits gracefully if KALSHI_API_KEY is not set.
    scheduler.add_job(
        run_kalshi_nonsports_refresh,
        CronTrigger(hour='9-23', minute='*/10', timezone=ET),
        id="kalshi_nonsports_refresh",
        name="Kalshi Non-Sports Refresh (every 10 min, 9AM-11PM ET)",
    )

    # Every 2 min, 9 AM - 11 PM ET — execute dashboard-approved trades
    # Picks up rows where kalshi_trade_queue.status='approved' and places them
    # via the Kalshi API. Exits gracefully when nothing is approved.
    scheduler.add_job(
        run_kalshi_execute_approved,
        CronTrigger(hour='9-23', minute='*/2', timezone=ET),
        id="kalshi_execute_approved",
        name="Kalshi Execute Approved (every 2 min, 9AM-11PM ET)",
    )

    # Every 2 min, 9 AM - 11 PM ET — reprice stale resting Kalshi orders
    # Checks if market has moved from resting order price, cancels+replaces if edge retained.
    scheduler.add_job(
        run_kalshi_reprice_stale,
        CronTrigger(hour='9-23', minute='*/2', timezone=ET),
        id="kalshi_reprice_stale",
        name="Kalshi Reprice Stale (every 2 min, 9AM-11PM ET)",
    )

    # Every 5 min, 9 AM - 11 PM ET — poll Kalshi API for pending order fills
    # Exits early (zero API calls) if no pending orders exist.
    scheduler.add_job(
        run_kalshi_pending_fills,
        CronTrigger(hour='9-23', minute='*/5', timezone=ET),
        id="kalshi_pending_fills",
        name="Kalshi Pending Fills (every 5 min, 9AM-11PM ET)",
    )

    # Stale fill detector — enqueues pending orders whose game has started for cancellation review
    scheduler.add_job(
        run_kalshi_stale_fills,
        CronTrigger(hour='9-23', minute='*/5', timezone=ET),
        id="kalshi_stale_fills",
        name="Kalshi Stale Fill Detector (every 5 min, 9AM-11PM ET)",
    )

    # Cancellation executor — executes human-approved cancellations via Kalshi API
    scheduler.add_job(
        run_kalshi_execute_cancellations,
        CronTrigger(hour='9-23', minute='*/2', timezone=ET),
        id="kalshi_execute_cancellations",
        name="Kalshi Cancel Executor (every 2 min, 9AM-11PM ET)",
    )

    # ==============================================================
    # Polymarket-Kalshi Arbitrage Scanner
    # Offset 5 min after Kalshi refresh to use fresh Kalshi data.
    # ==============================================================

    # MLB arb scan: every 10 min, 12:05 PM - 11:05 PM ET (sport-specific, game-level)
    scheduler.add_job(
        run_arb_scan_mlb,
        CronTrigger(hour='12-23', minute='5,15,25,35,45,55', timezone=ET),
        id="arb_scan_mlb",
        name="Arb Scan MLB (every 10 min, 12:05PM-11:05PM ET)",
    )

    # Non-sports scrape: 2x/day (9 AM + 5 PM ET), 2-hour timeout.
    # Fetches all 70k+ Polymarket markets and stores to polymarket_markets.
    # The scan jobs below then read from this existing data via --skip-scrape.
    scheduler.add_job(
        run_nonsports_scrape,
        CronTrigger(hour='9,17', minute='0', timezone=ET),
        id="nonsports_scrape",
        name="Non-Sports Polymarket Scrape (9AM + 5PM ET, 2hr timeout)",
    )

    # Non-sports arb SCAN: every 30 min, 9 AM - 11 PM ET.
    # Fast (<2 min) — uses existing polymarket_markets data, no re-scrape.
    # Matches Kalshi non-sports (politics, crypto, economics) vs Polymarket.
    scheduler.add_job(
        run_arb_scan_all_categories,
        CronTrigger(hour='9-23', minute='0,30', timezone=ET),
        id="arb_scan_all_categories",
        name="Arb Scan Non-Sports (every 30 min, 9AM-11PM ET, scan-only)",
    )

    # Log scheduled jobs
    logger.info("Scheduled jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")

    logger.info("Scheduler running. Press Ctrl+C to exit.")

    # Handle graceful shutdown
    def shutdown(_signum, _frame):
        logger.info("Shutdown signal received, stopping scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
