import logging
import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LiveOddsScraper")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

# Initialize Engine (deferred to avoid crash during import in CI/test)
engine = create_engine(DATABASE_URL) if DATABASE_URL else None


class LiveOddsScraper:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        """Ensure the staging table exists."""
        query = """
        CREATE TABLE IF NOT EXISTS raw_game_lines_live (
            id SERIAL PRIMARY KEY,
            api_game_id TEXT,
            bookmaker TEXT,
            market_key TEXT,
            outcome_label TEXT,
            line NUMERIC,
            odds_american INTEGER,
            commence_time TIMESTAMP,
            home_team TEXT,
            away_team TEXT,
            snapshot_time TIMESTAMP,
            market_last_update TIMESTAMP,
            bookmaker_last_update TIMESTAMP,
            bookmaker_name TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def fetch_live_odds(self):
        """
        Fetch LIVE odds from the API.
        Endpoint: /v4/sports/basketball_nba/odds
        """
        url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
        params = {
            "apiKey": self.api_key,
            "regions": "us,us2,us_ex",
            "markets": "h2h,spreads,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        logger.info("Fetching LIVE odds...")
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            remaining = response.headers.get("x-requests-remaining", "?")
            logger.info(f"Successfully fetched {len(data)} live games. Credits remaining: {remaining}")
            return data

        except Exception as e:
            logger.error(f"Failed to fetch live odds: {e}")
            return []

    def parse_and_store(self, games):
        """Parse response and insert into raw_game_lines_live."""
        if not games:
            return 0

        snapshot_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        rows_to_insert = []

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
                        outcome_label = outcome.get("name")
                        odds = outcome.get("price")
                        line = outcome.get("point")

                        rows_to_insert.append(
                            (
                                api_game_id,
                                book_key,
                                market_key,
                                outcome_label,
                                line,
                                odds,
                                commence_time,
                                home_team,
                                away_team,
                                snapshot_time,
                                market_updated,
                                book_updated,
                                book_name,
                            )
                        )

        if rows_to_insert:
            self._batch_insert(rows_to_insert)

        return len(rows_to_insert)

    def _batch_insert(self, rows):
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO raw_game_lines_live
                    (api_game_id, bookmaker, market_key, outcome_label,
                     line, odds_american, commence_time, home_team, away_team,
                     snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
            logger.info(f"Inserted {len(rows)} live odds records.")
        finally:
            conn.close()


if __name__ == "__main__":
    if not DATABASE_URL or not API_KEY:
        raise ValueError("Missing DATABASE_URL or ODDS_API_KEY in .env file")
    engine = create_engine(DATABASE_URL)
    scraper = LiveOddsScraper(API_KEY, engine)
    data = scraper.fetch_live_odds()
    if data:
        scraper.parse_and_store(data)
