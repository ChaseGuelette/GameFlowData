#!/usr/bin/env python3
"""
NCAAB Lines Job
================
Scrapes live NCAAB game lines and links them to schedule rows.
Active only during NCAAB season (November through April).

Steps:
1. ncaab_game_lines_scraper --live
2. ncaab_linker incremental

Usage:
    python src/orchestration/ncaab_lines_job.py --live
    python src/orchestration/ncaab_lines_job.py --date 2025-01-15
"""

import argparse
import logging
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ncaab_lines.log"),
    ],
)
logger = logging.getLogger("NCAABLinesJob")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

NCAAB_SEASON_MONTHS = [11, 12, 1, 2, 3, 4]


def is_ncaab_season() -> bool:
    """Return True if current month is within NCAAB season."""
    return datetime.utcnow().month in NCAAB_SEASON_MONTHS


def run_step(name: str, cmd: str, timeout: int = 300) -> bool:
    """Run a subprocess step with timeout."""
    logger.info(f"Step: {name}")
    try:
        result = subprocess.run(
            shlex.split(cmd),
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode == 0:
            logger.info(f"  OK ({name})")
            return True
        else:
            logger.error(f"  FAILED ({name})")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n")[-3:]:
                    logger.error(f"    {line}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"  TIMEOUT ({name}) after {timeout}s")
        return False
    except Exception as e:
        logger.error(f"  ERROR ({name}): {e}")
        return False


def run_ncaab_lines(live: bool = True, date_str: str | None = None) -> bool:
    """Execute NCAAB lines pipeline.

    Returns True if all steps succeeded.
    """
    if not is_ncaab_season():
        logger.info("NCAAB off-season — skipping lines job.")
        return True

    logger.info("=== NCAAB Lines Job ===")

    # Step 1: Scrape game lines
    if live:
        scrape_cmd = "python -m src.scrapers.ncaab.ncaab_game_lines_scraper --live"
    elif date_str:
        scrape_cmd = f"python -m src.scrapers.ncaab.ncaab_game_lines_scraper --date {date_str}"
    else:
        scrape_cmd = "python -m src.scrapers.ncaab.ncaab_game_lines_scraper --live"

    ok1 = run_step("Game Lines Scrape", scrape_cmd, timeout=120)

    # Step 2: Link to schedule
    ok2 = run_step("Game Lines Linker", "python -m src.processing.ncaab.ncaab_linker incremental", timeout=300)

    all_ok = ok1 and ok2
    logger.info(f"=== NCAAB Lines {'COMPLETE' if all_ok else 'PARTIAL FAILURE'} ===")
    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NCAAB Lines Job")
    parser.add_argument("--live", action="store_true", help="Scrape live game lines")
    parser.add_argument("--date", type=str, help="Scrape historical lines for date")

    args = parser.parse_args()
    success = run_ncaab_lines(live=args.live or not args.date, date_str=args.date)
    sys.exit(0 if success else 1)
