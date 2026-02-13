import os
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine
from tqdm import tqdm  # Changed from tqdm.notebook for terminal compatibility

# --- Configuration ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

if not DATABASE_URL or not API_KEY:
    raise ValueError("Missing credentials in .env file")

# Initialize Engine
engine = create_engine(DATABASE_URL)


class GameLineScraper:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()

    def get_bulk_odds(self, date_str):
        """
        Fetches ALL game lines (Moneyline, Spread, Total) for a specific timestamp.
        Cost: 30 credits (1 region * 3 markets * 10 cost)
        """
        url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/odds"

        params = {
            "apiKey": self.api_key,
            "date": date_str,
            "regions": "us",  # 'us' covers major US books
            "markets": "h2h,spreads,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=15)

                if response.status_code == 200:
                    # The response has a 'data' key containing the list of games
                    return response.json(), int(response.headers.get("x-requests-last", 0))

                elif response.status_code == 429:
                    time.sleep(2 * (attempt + 1))
                    continue

                elif response.status_code == 401:
                    print("❌ Error: Unauthorized. Check API Key or Credits.")
                    return None, 0

            except Exception as e:
                print(f"⚠️ Connection Error: {e}")
                time.sleep(1)

        return None, 0

    def parse_and_store(self, response_json, snapshot_ts):
        """
        Parses the bulk response and inserts into raw_game_lines_staging.
        """
        if not response_json or "data" not in response_json:
            return 0

        rows_to_insert = []
        games = response_json["data"]

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
                        line = outcome.get("point")  # Available for spreads/totals, None for h2h

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
                                snapshot_ts,
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
                    INSERT INTO raw_game_lines_staging
                    (api_game_id, bookmaker, market_key, outcome_label,
                     line, odds_american, commence_time, home_team, away_team,
                     snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


# --- Date Generation (Limited to 2020+) ---
def generate_historical_schedule():
    snapshots = []
    # Data only available from June 2020 (The "Bubble" onwards)
    seasons = [
        ("2019-20 Bubble", datetime(2020, 7, 30), datetime(2020, 10, 13)),
        ("2020-21 Season", datetime(2020, 12, 22), datetime(2021, 7, 20)),
        ("2021-22 Season", datetime(2021, 10, 19), datetime(2022, 6, 16)),
        ("2022-23 Season", datetime(2022, 10, 18), datetime(2023, 6, 12)),
        ("2023-24 Season", datetime(2023, 10, 24), datetime(2024, 6, 17)),
        ("2024-25 Season", datetime(2024, 10, 22), datetime(2025, 4, 15)),  # Current
        ("2025-26 Season", datetime(2025, 10, 20), datetime(2026, 1, 23)),  # Current Backfill
    ]

    # Strategy: 2 Snapshots per day (Afternoon & Evening) to catch movement
    hours = [17, 23]

    for season_name, start, end in seasons:
        current = start
        while current <= end:
            # Skip All-Star breaks or off days if needed, but safe to keep
            for h in hours:
                ts = current.replace(hour=h, minute=0, second=0)
                snapshots.append(ts.strftime("%Y-%m-%dT%H:%M:%SZ"))
            current += timedelta(days=1)

    return snapshots


# --- Execution ---
if __name__ == "__main__":
    bot = GameLineScraper(API_KEY, engine)
    timestamps = generate_historical_schedule()

    print("🚀 Starting Game Lines Scrape (H2H, Spreads, Totals)")
    print(f"📅 Snapshots to process: {len(timestamps)}")
    print(f"💰 Estimated Cost: {len(timestamps) * 30} credits")

    pbar = tqdm(timestamps, desc="Scraping History", unit="snap")
    total_credits = 0

    try:
        for ts in pbar:
            data, cost = bot.get_bulk_odds(ts)
            total_credits += cost

            if data:
                rows = bot.parse_and_store(data, ts)
                pbar.set_description(f"Rows: {rows} | Cost: {total_credits}")

            time.sleep(0.05)  # Odds API allows 30 req/s; 0.05s = 20 req/s max

    except KeyboardInterrupt:
        print("\n🛑 User Stopped.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

    print(f"\n✅ Done. Total Credits: {total_credits}")
