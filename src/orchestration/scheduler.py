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
import signal
import subprocess
import sys
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


def run_job(script_name: str):
    """Run a job script as a subprocess."""
    script_path = PROJECT_ROOT / "src" / "orchestration" / script_name
    logger.info(f"Starting job: {script_name}")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
        )

        if result.returncode == 0:
            logger.info(f"Job completed successfully: {script_name}")
        else:
            logger.error(f"Job failed: {script_name}")
            logger.error(f"STDERR: {result.stderr[-2000:] if result.stderr else 'None'}")

    except subprocess.TimeoutExpired:
        logger.error(f"Job timed out after 30 minutes: {script_name}")
    except Exception as e:
        logger.error(f"Job error: {script_name} - {e}")


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
