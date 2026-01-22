import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DailyPlayerProps")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

if not DATABASE_URL or not API_KEY:
    logger.error("Missing DATABASE_URL or ODDS_API_KEY.")
    sys.exit(1)

engine = create_engine(DATABASE_URL)


class DailyPlayerPropsScraper:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self._ensure_live_table_exists()

    def _ensure_live_table_exists(self):
        """Create the raw_player_props_live table if it doesn't exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS public.raw_player_props_live (
            live_id bigserial primary key,
            api_game_id text not null,
            api_player_name text not null,
            bookmaker text not null,
            market_key text not null,
            outcome_label text not null,
            line numeric null,
            odds_american integer not null,
            commence_time timestamp with time zone null,
            home_team text null,
            away_team text null,
            inserted_at timestamp with time zone null default now(),
            market_last_update timestamp with time zone null,
            bookmaker_last_update timestamp with time zone null,
            bookmaker_name text null,
            snapshot_time timestamp with time zone null,
            game_id text null,
            player_id bigint null,
            team_id bigint null
        );

        CREATE INDEX IF NOT EXISTS idx_live_props_game ON public.raw_player_props_live(api_game_id);
        CREATE INDEX IF NOT EXISTS idx_live_props_player ON public.raw_player_props_live(api_player_name);
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    def get_live_events(self):
        """Get list of current/upcoming NBA games."""
        url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
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
        url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"
        params = {"apiKey": self.api_key, "date": date_str}

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch historical events: {e}")
        return []

    def fetch_props(self, event_id, date_str=None, is_live=True):
        """
        Fetch props for a game.
        If is_live=True, use live endpoint (date_str ignored).
        If is_live=False, use historical endpoint with date_str.
        """
        if is_live:
            url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event_id}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": "us,uk",
                "markets": "player_points,player_rebounds,player_assists,player_threes,player_blocks,player_steals",
                "oddsFormat": "american",
                "dateFormat": "iso",
            }
        else:
            url = f"https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/{event_id}/odds"
            params = {
                "apiKey": self.api_key,
                "date": date_str,
                "regions": "us,uk",
                "markets": "player_points,player_rebounds,player_assists,player_threes,player_blocks,player_steals",
                "oddsFormat": "american",
                "dateFormat": "iso",
            }

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Live endpoint returns dict, Historical returns dict wrapped in 'data' usually?
                # Actually, /odds endpoint for historical is wrapped?
                # Let's check player_prop_scraper.py... "response.json().get('data', {})" for historical
                # Live endpoint returns the event object directly.

                if is_live:
                    return data, int(response.headers.get("x-requests-last", 0))
                else:
                    return data.get("data", {}), int(response.headers.get("x-requests-last", 0))

            elif response.status_code == 422:
                return None, 0  # No odds

        except Exception as e:
            logger.error(f"Error fetching props for {event_id}: {e}")

        return None, 0

    def parse_and_store(self, data, snapshot_ts, table_name):
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
                    rows.append(
                        (
                            game_id,
                            outcome.get("description"),  # Player Name
                            book_key,
                            market_key,
                            outcome.get("name"),  # Over/Under
                            outcome.get("point"),
                            outcome.get("price"),
                            commence_time,
                            home_team,
                            away_team,
                            snapshot_ts,
                            market_updated,
                            book_updated,
                            book_name,
                        )
                    )

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
                     outcome_label, line, odds_american, commence_time, home_team, away_team,
                     snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def run_live_scrape(scraper):
    """Run scrape for current time -> raw_player_props_live"""
    logger.info("Running LIVE Player Props Scrape...")
    events = scraper.get_live_events()
    logger.info(f"Found {len(events)} live/upcoming events.")

    total_creds = 0
    total_rows = 0
    snapshot_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for event in events:
        game_id = event["id"]
        logger.info(f"Fetching props for {event['home_team']} vs {event['away_team']}...")

        data, cost = scraper.fetch_props(game_id, is_live=True)
        total_creds += cost

        if data:
            rows = scraper.parse_and_store(data, snapshot_ts, "raw_player_props_live")
            total_rows += rows
            logger.info(f"  Saved {rows} props.")

        time.sleep(0.2)

    logger.info(f"Live scrape complete. Saved {total_rows} rows. Used {total_creds} credits.")


def run_historical_scrape(scraper, date_str):
    """Run scrape for 12pm/6pm snapshots on date -> raw_player_props_combined"""
    logger.info(f"Running Historical Player Props Scrape for {date_str}...")

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format.")
        return

    # Snapshots: 17:00 UTC (12pm ET) and 23:00 UTC (6pm ET)
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
                rows = scraper.parse_and_store(data, snap_ts, "raw_player_props_combined")
                logger.info(f"  Saved {rows} rows for game {event['id']}")

            time.sleep(0.2)

    logger.info(f"Historical scrape complete. Used {total_creds} credits.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Scrape live props to raw_player_props_live")
    parser.add_argument("--date", type=str, help="Scrape historical props (12pm/6pm) to raw_player_props_combined")

    args = parser.parse_args()

    scraper = DailyPlayerPropsScraper(API_KEY, engine)

    if args.live:
        run_live_scrape(scraper)

    if args.date:
        run_historical_scrape(scraper, args.date)
