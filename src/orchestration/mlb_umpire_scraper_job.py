#!/usr/bin/env python3
"""
MLB Umpire Scraper Job
======================
Fetches umpire assignments for today's MLB games from the MLB Stats API boxscore endpoint.
Runs at 9:36 AM ET (alongside lineup scraper).

Usage:
    python src/orchestration/mlb_umpire_scraper_job.py [--dry-run]
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
        logging.FileHandler(LOG_DIR / "mlb_umpire_scraper.log"),
    ],
)
logger = logging.getLogger("mlb_umpire_scraper_job")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    logger.info(f"Starting MLB umpire scraper for {target_date}")

    from src.db.client import get_engine
    from src.scrapers.mlb.mlb_umpire_scraper import MLBUmpireScraper

    engine = get_engine()
    scraper = MLBUmpireScraper(engine, dry_run=args.dry_run)
    count = scraper.scrape_date(target_date)
    logger.info(f"Done — {count} umpire entries stored")


if __name__ == "__main__":
    main()
