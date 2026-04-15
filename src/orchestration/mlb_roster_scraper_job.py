#!/usr/bin/env python3
"""
MLB Roster Scraper Job
======================
Fetches the 26-man active roster for all 30 MLB teams.
Players absent from the active roster are on the IL or otherwise unavailable.
Runs at 9:30 AM ET daily.

Usage:
    python src/orchestration/mlb_roster_scraper_job.py [--dry-run]
"""

import argparse
import logging
import sys
from datetime import date
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
        logging.FileHandler(LOG_DIR / "mlb_roster_scraper.log"),
    ],
)
logger = logging.getLogger("mlb_roster_scraper_job")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    roster_date = date.fromisoformat(args.date) if args.date else date.today()
    logger.info(f"Starting MLB roster scraper for {roster_date}")

    from src.db.client import get_engine
    from src.scrapers.mlb.mlb_roster_scraper import MLBRosterScraper

    engine = get_engine()
    scraper = MLBRosterScraper(engine, dry_run=args.dry_run)
    count = scraper.scrape_date(roster_date)
    logger.info(f"Done — {count} active roster entries stored")


if __name__ == "__main__":
    main()
