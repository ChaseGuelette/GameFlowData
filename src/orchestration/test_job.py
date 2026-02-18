#!/usr/bin/env python3
"""
Test Job - Infrastructure Verification
=======================================
Lightweight test job to verify Railway scheduler, DB connectivity,
Discord alerts, and metric parsing end-to-end.

No side effects — only reads from DB and optionally sends a test Discord alert.

Usage:
    python src/orchestration/test_job.py
    python src/orchestration/test_job.py --skip-discord
    python src/orchestration/test_job.py --verbose
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed; env vars must be set directly

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TestJob")


# ── ENV VAR AUDIT ───────────────────────────────────────────────────────────

REQUIRED_VARS = [
    ("DATABASE_URL", "Required for all jobs"),
    ("ODDS_API_KEY", "Required for lines scraping"),
    ("RAPIDAPI_KEY", "Required for injury scraping"),
]

OPTIONAL_VARS = [
    ("DISCORD_BOT_TOKEN", "Discord alerts"),
    ("DISCORD_CHANNEL_ALERTS", "Job notifications"),
    ("DISCORD_CHANNEL_PREDICTIONS", "Prediction alerts"),
    ("DISCORD_CHANNEL_PERFORMANCE", "P&L summaries"),
]


def audit_env_vars(verbose: bool = False) -> dict:
    """Check all required and optional env vars, return status dict."""
    results = {"required_ok": [], "required_missing": [], "optional_ok": [], "optional_missing": []}

    logger.info("Environment variable audit:")
    for var, desc in REQUIRED_VARS:
        if os.getenv(var):
            results["required_ok"].append(var)
            logger.info(f"  [OK]      {var} — {desc}")
        else:
            results["required_missing"].append(var)
            logger.warning(f"  [MISSING] {var} — {desc}")

    for var, desc in OPTIONAL_VARS:
        if os.getenv(var):
            results["optional_ok"].append(var)
            logger.info(f"  [OK]      {var} — {desc}")
        else:
            results["optional_missing"].append(var)
            logger.info(f"  [--]      {var} — {desc} (optional)")

    return results


# ── DATABASE CHECK ──────────────────────────────────────────────────────────

def check_database() -> bool:
    """Test DB connectivity with SELECT 1. Returns True on success."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set — cannot test DB connectivity")
        return False

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if row and row[0] == 1:
                logger.info("Database connectivity: OK (SELECT 1 returned 1)")
                return True
            else:
                logger.error(f"Database connectivity: UNEXPECTED (SELECT 1 returned {row})")
                return False

    except Exception as e:
        logger.error(f"Database connectivity: FAILED — {e}")
        return False


# ── DISCORD TEST ────────────────────────────────────────────────────────────

def test_discord_alert() -> bool:
    """Send a test alert via the job alert system. Returns True on success."""
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    channel_id = os.getenv("DISCORD_CHANNEL_ALERTS")

    if not bot_token:
        logger.info("Discord test: SKIPPED (DISCORD_BOT_TOKEN not set)")
        return False
    if not channel_id:
        logger.info("Discord test: SKIPPED (DISCORD_CHANNEL_ALERTS not set)")
        return False

    try:
        from src.discord_bot.alerts import send_job_alert_sync

        success = send_job_alert_sync(
            job_name="System Test",
            success=True,
            duration_seconds=0.0,
            metrics={"status": "Infrastructure test passed"},
        )

        if success:
            logger.info("Discord test: OK (test alert sent)")
        else:
            logger.warning("Discord test: FAILED (send_job_alert_sync returned False)")

        return success

    except Exception as e:
        logger.warning(f"Discord test: FAILED — {e}")
        return False


# ── METRIC OUTPUT ───────────────────────────────────────────────────────────

def print_test_metrics(db_ok: bool, discord_ok: bool, env_results: dict):
    """Print structured output that matches the scheduler's metric parsing patterns.

    The scheduler's _parse_metrics_from_output looks for patterns like:
      - "Step N/N"
      - "N props"
      - "N games"
      - "Generated N predictions"
      - "N with edge"

    We emit a test_job-specific pattern so the parser can be extended if needed,
    plus some familiar patterns to verify the regex pipeline works.
    """
    checks_passed = sum([
        db_ok,
        discord_ok,
        len(env_results["required_ok"]) == len(REQUIRED_VARS),
    ])
    checks_total = 3

    # test_job-specific metrics
    print(f"Test completed: {checks_passed}/{checks_total} checks passed")
    print(f"Required env vars: {len(env_results['required_ok'])}/{len(REQUIRED_VARS)} set")
    print(f"Optional env vars: {len(env_results['optional_ok'])}/{len(OPTIONAL_VARS)} set")

    # Emit a pattern the existing parser already recognizes (for validation)
    print(f"Step {checks_passed}/{checks_total}")


# ── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Infrastructure test job")
    parser.add_argument("--skip-discord", action="store_true", help="Skip Discord alert test")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 50)
    logger.info("GameFlowData Infrastructure Test")
    logger.info("=" * 50)

    start_time = time.time()

    # 1. Environment audit
    env_results = audit_env_vars(verbose=args.verbose)

    # 2. Database check
    db_ok = check_database()

    # 3. Discord alert test
    discord_ok = False
    if args.skip_discord:
        logger.info("Discord test: SKIPPED (--skip-discord flag)")
    else:
        discord_ok = test_discord_alert()

    # 4. Print metrics
    duration = time.time() - start_time
    print_test_metrics(db_ok, discord_ok, env_results)

    # Summary
    logger.info("-" * 50)
    logger.info(f"Test completed in {duration:.1f}s")
    logger.info(f"  Database:  {'PASS' if db_ok else 'FAIL'}")
    logger.info(f"  Discord:   {'PASS' if discord_ok else 'SKIP/FAIL'}")
    logger.info(f"  Env vars:  {len(env_results['required_ok'])}/{len(REQUIRED_VARS)} required set")
    logger.info("-" * 50)

    # Exit code: fail only if DB is unreachable (critical infrastructure)
    if not db_ok:
        logger.error("CRITICAL: Database connectivity failed — exiting 1")
        sys.exit(1)

    logger.info("All critical checks passed — exiting 0")
    sys.exit(0)


if __name__ == "__main__":
    main()
