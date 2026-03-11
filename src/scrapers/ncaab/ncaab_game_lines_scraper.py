"""
NCAAB Daily Game Lines Scraper
================================
Scrapes NCAAB game lines (moneyline, spreads, totals) from The Odds API.
Stores into ncaab_raw_game_lines table.

Pattern: src/scrapers/mlb/mlb_daily_game_lines_scraper.py
Change: sport key baseball_mlb -> basketball_ncaab, table -> ncaab_raw_game_lines

Supports both live and historical scraping modes.

Usage:
    python -m src.scrapers.ncaab.ncaab_game_lines_scraper --live
    python -m src.scrapers.ncaab.ncaab_game_lines_scraper --date 2025-01-15
    python -m src.scrapers.ncaab.ncaab_game_lines_scraper --backfill --start-date 2024-11-01 --end-date 2025-04-07
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[3]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NCAABGameLines")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

SPORT_KEY = "basketball_ncaab"
GAME_LINE_MARKETS = "h2h,spreads,totals"


class NCAABGameLineScraper:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()

    def get_live_odds(self):
        """Fetch live game lines for all upcoming NCAAB games.

        Returns:
            (response_json, credits_used)
        """
        url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": "us,us2,us_ex",
            "markets": GAME_LINE_MARKETS,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    return response.json(), int(response.headers.get("x-requests-last", 0))
                elif response.status_code == 429:
                    time.sleep(2 * (attempt + 1))
                elif response.status_code == 401:
                    logger.error("Unauthorized. Check API Key or credits.")
                    return None, 0
            except Exception as e:
                logger.warning(f"Connection error: {e}")
                time.sleep(1)
        return None, 0

    def get_historical_odds(self, date_str):
        """Fetch historical game lines at a specific timestamp.

        Returns:
            (response_json, credits_used)
        """
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/odds"
        params = {
            "apiKey": self.api_key,
            "date": date_str,
            "regions": "us,us2,us_ex",
            "markets": GAME_LINE_MARKETS,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    return response.json(), int(response.headers.get("x-requests-last", 0))
                elif response.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.warning(f"Connection error: {e}")
                time.sleep(1)
        return None, 0

    def parse_and_store(self, response_json, snapshot_ts):
        """Parse game lines response and insert into ncaab_raw_game_lines."""
        if isinstance(response_json, dict):
            games = response_json.get("data", [])
        elif isinstance(response_json, list):
            games = response_json
        else:
            return 0

        if not games:
            return 0

        rows = []
        for game in games:
            api_game_id = game.get("id")
            commence_time = game.get("commence_time")
            home_team = game.get("home_team")
            away_team = game.get("away_team")

            for book in game.get("bookmakers", []):
                book_key = book["key"]
                book_name = book.get("title")
                book_updated = book.get("last_update")

                for market in book.get("markets", []):
                    market_key = market["key"]
                    market_updated = market.get("last_update")

                    for outcome in market.get("outcomes", []):
                        rows.append((
                            api_game_id,
                            book_key,
                            market_key,
                            outcome.get("name"),
                            outcome.get("point"),
                            outcome.get("price"),
                            commence_time,
                            home_team,
                            away_team,
                            snapshot_ts,
                            market_updated,
                            book_updated,
                            book_name,
                        ))

        if rows:
            self._batch_insert(rows)
        return len(rows)

    def _batch_insert(self, rows):
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ncaab_raw_game_lines
                    (api_game_id, bookmaker, market_key, outcome_label,
                     line, odds_american, commence_time, home_team, away_team,
                     snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def run_live_scrape(scraper):
    """Scrape live NCAAB game lines."""
    logger.info("Running LIVE NCAAB Game Lines Scrape...")
    snapshot_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    data, cost = scraper.get_live_odds()
    if data:
        rows = scraper.parse_and_store(data, snapshot_ts)
        logger.info(f"Saved {rows} game line rows. Credits: {cost}")
    else:
        logger.warning("No data returned.")


def run_date_scrape(scraper, date_str):
    """Scrape historical NCAAB game lines for a specific date."""
    logger.info(f"Scraping NCAAB game lines for {date_str}...")

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        return

    # NCAAB games span afternoon + evening ET
    # 18:00 UTC (1pm ET), 21:00 UTC (4pm ET), 00:00 UTC+1 (7pm ET)
    total_rows = 0
    total_credits = 0

    for hour in [18, 21, 0]:
        if hour == 0:
            dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0) + timedelta(days=1)
        else:
            dt = datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0)
        if dt > datetime.utcnow():
            continue

        ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"  Snapshot: {ts}")

        data, cost = scraper.get_historical_odds(ts)
        total_credits += cost

        if data:
            rows = scraper.parse_and_store(data, ts)
            total_rows += rows
            logger.info(f"  Saved {rows} rows. Credits: {cost}")

    logger.info(f"Date scrape complete. {total_rows} rows, {total_credits} credits.")


def run_backfill(scraper, start_date: str, end_date: str):
    """Backfill NCAAB game lines over a date range."""
    logger.info(f"Backfilling NCAAB game lines from {start_date} to {end_date}...")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    timestamps = []
    current = start
    while current <= end:
        for hour in [18, 23]:
            ts = current.replace(hour=hour, minute=0, second=0)
            if ts <= datetime.utcnow():
                timestamps.append(ts.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current += timedelta(days=1)

    logger.info(f"Timestamps to scrape: {len(timestamps)}")

    total_credits = 0
    total_rows = 0

    pbar = tqdm(timestamps, desc="Game Lines", unit="snap")
    try:
        for ts in pbar:
            data, cost = scraper.get_historical_odds(ts)
            total_credits += cost

            if data:
                rows = scraper.parse_and_store(data, ts)
                total_rows += rows
                pbar.set_description(f"Rows: {total_rows} | Credits: {total_credits}")

            time.sleep(0.05)
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"Error: {e}")

    logger.info(f"Backfill complete. {total_rows} rows, {total_credits} credits.")


if __name__ == "__main__":
    if not DATABASE_URL or not API_KEY:
        logger.error("Missing DATABASE_URL or ODDS_API_KEY.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)
    scraper = NCAABGameLineScraper(API_KEY, engine)

    parser = argparse.ArgumentParser(description="NCAAB Daily Game Lines Scraper")
    parser.add_argument("--live", action="store_true", help="Scrape live game lines")
    parser.add_argument("--date", type=str, help="Scrape historical lines for date (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="Backfill game lines over date range")
    parser.add_argument("--start-date", type=str, help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Backfill end date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.live:
        run_live_scrape(scraper)
    elif args.date:
        run_date_scrape(scraper, args.date)
    elif args.backfill:
        if not args.start_date or not args.end_date:
            logger.error("--backfill requires --start-date and --end-date")
            sys.exit(1)
        run_backfill(scraper, args.start_date, args.end_date)
    else:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        run_date_scrape(scraper, today)
