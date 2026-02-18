#!/usr/bin/env python3
"""
Scheduler - APScheduler-based job runner for Railway
=====================================================
Runs all daily jobs on schedule using APScheduler.

Schedule (ET → UTC for EST):
    9:00 AM ET  (14:00 UTC) - daily_stats_job
    12:00 PM ET (17:00 UTC) - lines_job
    4:00 PM ET  (21:00 UTC) - lines_job
    6:00 PM ET  (23:00 UTC) - lines_job
    6:30 PM ET  (23:30 UTC) - inference_job

Usage:
    python src/orchestration/scheduler.py
"""

import logging
import re
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


def run_job(script_name: str):
    """Run a job script as a subprocess and send alert on completion."""
    script_path = PROJECT_ROOT / "src" / "orchestration" / script_name
    logger.info(f"Starting job: {script_name}")

    start_time = time.time()
    success = False
    stdout = ""
    stderr = ""

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
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


def run_lines():
    run_job("lines_job.py")


def run_inference():
    run_job("inference_job.py")


def main():
    logger.info("=" * 60)
    logger.info("GameFlowData Scheduler Starting")
    logger.info("=" * 60)

    _validate_environment()

    scheduler = BlockingScheduler(timezone="UTC")

    # Schedule jobs (all times in UTC)
    # EST: UTC-5, so 9 AM ET = 14:00 UTC

    # Daily Stats - 9:00 AM ET = 14:00 UTC
    scheduler.add_job(
        run_daily_stats,
        CronTrigger(hour=14, minute=0),
        id="daily_stats",
        name="Daily Stats Job (9 AM ET)",
    )

    # Lines - 12:00 PM ET = 17:00 UTC
    scheduler.add_job(
        run_lines,
        CronTrigger(hour=17, minute=0),
        id="lines_noon",
        name="Lines Job - Noon (12 PM ET)",
    )

    # Lines - 4:00 PM ET = 21:00 UTC
    scheduler.add_job(
        run_lines,
        CronTrigger(hour=21, minute=0),
        id="lines_4pm",
        name="Lines Job - 4 PM (4 PM ET)",
    )

    # Lines - 6:00 PM ET = 23:00 UTC
    scheduler.add_job(
        run_lines,
        CronTrigger(hour=23, minute=0),
        id="lines_6pm",
        name="Lines Job - 6 PM (6 PM ET)",
    )

    # Inference - 6:30 PM ET = 23:30 UTC
    scheduler.add_job(
        run_inference,
        CronTrigger(hour=23, minute=30),
        id="inference",
        name="Inference Job (6:30 PM ET)",
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
