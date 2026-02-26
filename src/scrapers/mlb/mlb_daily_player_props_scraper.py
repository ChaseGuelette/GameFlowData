"""
MLB Daily Player Props Scraper
===============================
Scrapes live MLB player props from The Odds API for today's games.
Stores into mlb_raw_player_props table.

Pattern: src/scrapers/daily_player_props_scraper.py (NBA equivalent)
Change: sport key basketball_nba -> baseball_mlb, MLB market keys

Usage:
    python -m src.scrapers.mlb.mlb_daily_player_props_scraper --live
    python -m src.scrapers.mlb.mlb_daily_player_props_scraper --date 2025-04-01
    python -m src.scrapers.mlb.mlb_daily_player_props_scraper --live --extended
    python -m src.scrapers.mlb.mlb_daily_player_props_scraper --live --target-table mlb_raw_player_props
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parents[3]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBDailyProps")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

SPORT_KEY = "baseball_mlb"

# Phase 1 core markets
MLB_CORE_MARKETS = [
    "pitcher_strikeouts",
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
    "pitcher_outs",
    "batter_rbis",
    "batter_runs_scored",
]

MLB_EXTENDED_MARKETS = [
    "batter_hits_runs_rbis",
    "batter_stolen_bases",
    "batter_strikeouts",
    "batter_walks",
    "batter_singles",
    "batter_doubles",
    "pitcher_earned_runs",
    "pitcher_hits_allowed",
    "pitcher_walks",
]


class MLBDailyPropsScraper:
    def __init__(self, api_key, db_engine, markets=None):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self.markets = markets or MLB_CORE_MARKETS

    def get_live_events(self):
        """Get list of current/upcoming MLB games."""
        url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events"
        params = {"apiKey": self.api_key}

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch live events: {e}")
            return []

    def get_historical_events(self, date_str):
        """Get events for a specific past date."""
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
        params = {"apiKey": self.api_key, "date": date_str}

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch historical events: {e}")
        return []

    def fetch_props(self, event_id, date_str=None, is_live=True):
        """Fetch props for a single MLB game.

        If is_live=True, uses live endpoint. Otherwise uses historical with date_str.
        """
        markets_str = ",".join(self.markets)

        if is_live:
            url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": "us,us2,us_ex,us_dfs",
                "markets": markets_str,
                "oddsFormat": "american",
                "dateFormat": "iso",
            }
        else:
            url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events/{event_id}/odds"
            params = {
                "apiKey": self.api_key,
                "date": date_str,
                "regions": "us,us2,us_ex,us_dfs",
                "markets": markets_str,
                "oddsFormat": "american",
                "dateFormat": "iso",
            }

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if is_live:
                    return data, int(response.headers.get("x-requests-last", 0))
                else:
                    return data.get("data", {}), int(response.headers.get("x-requests-last", 0))
            elif response.status_code == 422:
                return None, 0
        except Exception as e:
            logger.error(f"Error fetching props for {event_id}: {e}")
        return None, 0

    def parse_and_store(self, data, snapshot_ts, table_name="mlb_raw_player_props"):
        """Parse props and insert into specified table."""
        if not data or "bookmakers" not in data:
            return 0

        rows = []
        game_id = data.get("id")
        commence_time = data.get("commence_time")
        home_team = data.get("home_team")
        away_team = data.get("away_team")

        for book in data.get("bookmakers", []):
            book_key = book["key"]
            book_name = book.get("title")
            book_updated = book.get("last_update")

            for market in book.get("markets", []):
                market_key = market["key"]
                market_updated = market.get("last_update")

                for outcome in market.get("outcomes", []):
                    rows.append((
                        game_id,
                        outcome.get("description"),
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
            self._batch_insert(rows, table_name)
        return len(rows)

    def _batch_insert(self, rows, table_name):
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                query = f"""
                    INSERT INTO {table_name}
                    (api_game_id, api_player_name, bookmaker, market_key,
                     outcome_label, line, odds_american, commence_time,
                     home_team, away_team, snapshot_time,
                     market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """  # nosec
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def run_live_scrape(scraper, target_table="mlb_raw_player_props"):
    """Scrape live MLB props."""
    logger.info(f"Running LIVE MLB Player Props Scrape -> {target_table}...")
    events = scraper.get_live_events()
    logger.info(f"Found {len(events)} live/upcoming MLB events.")

    total_creds = 0
    total_rows = 0
    snapshot_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for event in events:
        game_id = event["id"]
        logger.info(f"Fetching props for {event.get('home_team', '?')} vs {event.get('away_team', '?')}...")

        data, cost = scraper.fetch_props(game_id, is_live=True)
        total_creds += cost

        if data:
            rows = scraper.parse_and_store(data, snapshot_ts, target_table)
            total_rows += rows
            logger.info(f"  Saved {rows} props.")

        time.sleep(0.05)

    logger.info(f"Live scrape complete. Saved {total_rows} rows. Used {total_creds} credits.")


def run_historical_scrape(scraper, date_str):
    """Scrape historical MLB props for a specific date."""
    logger.info(f"Running Historical MLB Props Scrape for {date_str}...")

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        return

    # MLB games span afternoon + evening: 17:00 UTC (12pm ET) and 23:00 UTC (6pm ET)
    snapshots = []
    for h in [17, 23]:
        snap = datetime(dt.year, dt.month, dt.day, h, 0, 0)
        if snap <= datetime.utcnow():
            snapshots.append(snap.strftime("%Y-%m-%dT%H:%M:%SZ"))

    total_creds = 0

    for snap_ts in snapshots:
        logger.info(f"Snapshot: {snap_ts}")
        events = scraper.get_historical_events(snap_ts)

        for event in events:
            data, cost = scraper.fetch_props(event["id"], date_str=snap_ts, is_live=False)
            total_creds += cost

            if data:
                rows = scraper.parse_and_store(data, snap_ts, "mlb_raw_player_props")
                logger.info(f"  Saved {rows} rows for game {event['id']}")

            time.sleep(0.05)

    logger.info(f"Historical scrape complete. Used {total_creds} credits.")


if __name__ == "__main__":
    if not DATABASE_URL or not API_KEY:
        logger.error("Missing DATABASE_URL or ODDS_API_KEY.")
        sys.exit(1)
    engine = create_engine(DATABASE_URL)

    parser = argparse.ArgumentParser(description="MLB Daily Player Props Scraper")
    parser.add_argument("--live", action="store_true", help="Scrape live props")
    parser.add_argument("--date", type=str, help="Scrape historical props for date (YYYY-MM-DD)")

    market_group = parser.add_mutually_exclusive_group()
    market_group.add_argument("--extended", action="store_true", help="Include extended markets")
    market_group.add_argument("--markets", nargs="+", default=None, help="Explicit market list")

    parser.add_argument("--target-table", type=str, default=None, help="Override target table")

    args = parser.parse_args()

    # Resolve markets
    if args.markets:
        markets = args.markets
    elif args.extended:
        markets = MLB_CORE_MARKETS + MLB_EXTENDED_MARKETS
    else:
        markets = list(MLB_CORE_MARKETS)

    scraper = MLBDailyPropsScraper(API_KEY, engine, markets=markets)
    logger.info(f"Markets ({len(markets)}): {', '.join(markets)}")

    if args.live:
        table = args.target_table or "mlb_raw_player_props"
        run_live_scrape(scraper, target_table=table)

    if args.date:
        run_historical_scrape(scraper, args.date)
