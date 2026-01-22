import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from src.scrapers.game_lines_scraper import GameLineScraper, engine

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DailyGameLines")

load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")

if not API_KEY:
    logger.error("ODDS_API_KEY not found in environment.")
    sys.exit(1)


def scrape_odds_for_date(target_date_str: str):
    """
    Scrape odds for a specific date using the historical endpoint.
    Even for 'today', we use historical endpoint with a recent timestamp.
    """
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    logger.info(f"Initializing Scraper for {target_date}...")
    bot = GameLineScraper(API_KEY, engine)

    # Define snapshots: Morning and Evening (UTC)
    # NBA games usually start 7PM ET (00:00 UTC next day) to 10:30PM ET.
    # We want lines from earlier in the day.
    # 17:00 UTC is 12:00 PM ET (Noon)
    # 23:00 UTC is 6:00 PM ET (Pre-game)

    # We construct full ISO timestamps for the target date
    snapshots = []
    for hour in [17, 23]:
        # Create timestamp
        dt = datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0)

        # If target date is today, ensure we don't ask for future time
        if dt > datetime.utcnow():
            logger.warning(f"Skipping future snapshot: {dt} UTC")
            continue

        snapshots.append(dt.strftime("%Y-%m-%dT%H:%M:%SZ"))

    if not snapshots:
        logger.warning("No valid past timestamps found for this date (is it in the future?).")
        return

    total_credits = 0
    total_rows = 0

    for ts in snapshots:
        logger.info(f"Fetching odds for snapshot: {ts}...")
        data, cost = bot.get_bulk_odds(ts)
        total_credits += cost

        if data:
            rows = bot.parse_and_store(data, ts)
            total_rows += rows
            logger.info(f"  Saved {rows} rows. Cost: {cost} credits.")
        else:
            logger.warning(f"  No data returned for {ts}.")

    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"Total Rows Saved: {total_rows}")
    logger.info(f"Total Credits Used: {total_credits}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily Game Lines Scraper")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Date to scrape (YYYY-MM-DD). Defaults to UTC today.",
    )

    args = parser.parse_args()

    scrape_odds_for_date(args.date)
