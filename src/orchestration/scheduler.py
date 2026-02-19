#!/usr/bin/env python3
"""
Scheduler - APScheduler-based job runner for Railway
=====================================================
Runs all daily jobs on schedule using APScheduler.

Schedule (ET → UTC for EST):
    9:00 AM ET  (14:00 UTC) - daily_stats_job

    12:00 PM ET (17:00 UTC) - lines_job --live (full)
    12:15 PM ET (17:15 UTC) - inference_job (full MC)

    1:00 PM ET  (18:00 UTC) - lines_job --live --props-only
    1:02 PM ET  (18:02 UTC) - edge_refresh_job

    2:00 PM ET  (19:00 UTC) - lines_job --live --props-only
    2:02 PM ET  (19:02 UTC) - edge_refresh_job

    3:00 PM ET  (20:00 UTC) - lines_job --live --props-only
    3:02 PM ET  (20:02 UTC) - edge_refresh_job

    4:00 PM ET  (21:00 UTC) - lines_job --live (full)
    4:15 PM ET  (21:15 UTC) - inference_job (full MC)

    4:30 PM ET  (21:30 UTC) - lines_job --live --props-only + edge_refresh
    5:00 PM ET  (22:00 UTC) - lines_job --live --props-only + edge_refresh
    5:30 PM ET  (22:30 UTC) - lines_job --live --props-only + edge_refresh
    6:00 PM ET  (23:00 UTC) - lines_job --live --props-only + edge_refresh
    6:30 PM ET  (23:30 UTC) - lines_job --live --props-only + edge_refresh (final)

Usage:
    python src/orchestration/scheduler.py              # Start scheduler loop
    python src/orchestration/scheduler.py --run-test   # Run test job and exit
"""

from __future__ import annotations

import logging
import re
import shlex
import signal
import subprocess
import sys
import time
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
}


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
        steps_match = re.search(r"Step (\d+)/8", output)
        if steps_match:
            metrics["steps_completed"] = f"{steps_match.group(1)}/8"

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


def run_job(script_name: str, extra_args: str = ""):
    """Run a job script as a subprocess and send alert on completion."""
    script_path = PROJECT_ROOT / "src" / "orchestration" / script_name
    cmd = [sys.executable, str(script_path)] + (shlex.split(extra_args) if extra_args else [])
    logger.info(f"Starting job: {script_name}{' ' + extra_args if extra_args else ''}")

    start_time = time.time()
    success = False
    stdout = ""
    stderr = ""

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
        )

        stdout = result.stdout or ""
        stderr = result.stderr or ""
        duration = time.time() - start_time

        if result.returncode == 0:
            logger.info(f"Job completed successfully: {script_name}")
            success = True
        else:
            logger.error(f"Job failed: {script_name}")
            logger.error(f"STDERR: {stderr[-2000:] if stderr else 'None'}")

        # Send alert
        _send_job_alert(script_name, success, duration, stdout, stderr)

    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        logger.error(f"Job timed out after 30 minutes: {script_name}")
        _send_job_alert(
            script_name,
            success=False,
            duration=duration,
            stdout="",
            stderr="Job timed out after 30 minutes",
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


def _validate_environment():
    """Log status of required and optional env vars at startup."""
    import os

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


def run_lines_full():
    """Full lines scrape: game lines + props (live) + injuries + linker."""
    run_job("lines_job.py", extra_args="--live")


def run_lines_props_only():
    """Props-only scrape: live props + linker (no game lines or injuries)."""
    run_job("lines_job.py", extra_args="--live --props-only")


def run_inference():
    run_job("inference_job.py")


def run_edge_refresh():
    """Lightweight edge recalculation using stored samples + fresh lines."""
    run_job("edge_refresh_job.py")


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

    scheduler = BlockingScheduler(timezone="UTC")

    # Schedule jobs (all times in UTC; EST = UTC-5)
    # ==============================================================

    # 9:00 AM ET (14:00 UTC) - Daily stats
    scheduler.add_job(
        run_daily_stats,
        CronTrigger(hour=14, minute=0),
        id="daily_stats",
        name="Daily Stats (9 AM ET)",
    )

    # --- First window: noon full scrape + inference ---

    # 12:00 PM ET (17:00 UTC) - Full lines scrape (live)
    scheduler.add_job(
        run_lines_full,
        CronTrigger(hour=17, minute=0),
        id="lines_noon_full",
        name="Lines Full (12 PM ET)",
    )

    # 12:15 PM ET (17:15 UTC) - Full inference
    scheduler.add_job(
        run_inference,
        CronTrigger(hour=17, minute=15),
        id="inference_noon",
        name="Inference (12:15 PM ET)",
    )

    # --- Hourly props-only + edge refresh: 1-3 PM ET ---

    for utc_hour, et_label in [(18, "1 PM"), (19, "2 PM"), (20, "3 PM")]:
        scheduler.add_job(
            run_lines_props_only,
            CronTrigger(hour=utc_hour, minute=0),
            id=f"props_{utc_hour}",
            name=f"Props Only ({et_label} ET)",
        )
        scheduler.add_job(
            run_edge_refresh,
            CronTrigger(hour=utc_hour, minute=2),
            id=f"edge_refresh_{utc_hour}",
            name=f"Edge Refresh ({et_label}:02 ET)",
        )

    # --- Second window: 4 PM full scrape + inference ---

    # 4:00 PM ET (21:00 UTC) - Full lines scrape (live)
    scheduler.add_job(
        run_lines_full,
        CronTrigger(hour=21, minute=0),
        id="lines_4pm_full",
        name="Lines Full (4 PM ET)",
    )

    # 4:15 PM ET (21:15 UTC) - Full inference
    scheduler.add_job(
        run_inference,
        CronTrigger(hour=21, minute=15),
        id="inference_4pm",
        name="Inference (4:15 PM ET)",
    )

    # --- Half-hourly props-only + edge refresh: 4:30-6:30 PM ET ---

    half_hourly = [
        (21, 30, "4:30 PM"), (22, 0, "5 PM"), (22, 30, "5:30 PM"),
        (23, 0, "6 PM"), (23, 30, "6:30 PM"),
    ]
    for utc_h, utc_m, et_label in half_hourly:
        scheduler.add_job(
            run_lines_props_only,
            CronTrigger(hour=utc_h, minute=utc_m),
            id=f"props_{utc_h}_{utc_m:02d}",
            name=f"Props Only ({et_label} ET)",
        )
        scheduler.add_job(
            run_edge_refresh,
            CronTrigger(hour=utc_h, minute=utc_m + 2),
            id=f"edge_refresh_{utc_h}_{utc_m:02d}",
            name=f"Edge Refresh ({et_label}:02 ET)",
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
