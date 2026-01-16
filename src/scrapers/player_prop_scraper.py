import json
import os
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine
from tqdm import tqdm  # Changed from tqdm.notebook for terminal compatibility

# Load credentials from .env file
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found! Check your .env file.")

API_KEY = os.getenv("ODDS_API_KEY")
if not API_KEY:
    raise ValueError("ODDS_API_KEY not found! Check your .env file.")

# Create Database Connection Pool (SQLAlchemy Engine)
engine = create_engine(DATABASE_URL)
PROGRESS_FILE = "scrape_progress.json"


class ScraperBot:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self.processed_ids = self._load_processed_ids()

    def _load_processed_ids(self):
        """
        Load history to prevent duplicate scraping.
        Checks local JSON file first, then falls back to DB check if needed.
        """
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE) as f:
                    data = json.load(f)
                    return set(tuple(x) for x in data)
            except Exception:
                pass
        return set()

    def save_progress(self):
        """Save processed IDs to local JSON file for resume capability."""
        with open(PROGRESS_FILE, "w") as f:
            json.dump(list(self.processed_ids), f)

    def get_events_for_date(self, date_str):
        """
        Step 1: Get list of Game IDs for a specific snapshot date.
        Endpoint: /v4/historical/sports/basketball_nba/events
        """
        url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"
        params = {"apiKey": self.api_key, "date": date_str}

        for _ in range(3):  # Retry loop
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    # The event list is wrapped in a 'data' key
                    return response.json().get("data", [])
                elif response.status_code == 429:
                    time.sleep(1)
            except:
                time.sleep(1)
        return []

    def scrape_event_props(self, date_str, event_id):
        """
        Step 2: Get Player Props for a single game.
        Endpoint: /v4/historical/sports/basketball_nba/events/{eventId}/odds
        """
        url = f"https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/{event_id}/odds"

        # We target 7 key player prop markets
        target_markets = [
            "player_points",
            "player_rebounds",
            "player_assists",
            "player_threes",
            "player_blocks",
            "player_steals",
            "player_turnovers",
        ]

        params = {
            "apiKey": self.api_key,
            "date": date_str,
            "regions": "us,uk",  # UK region often includes Pinnacle
            "markets": ",".join(target_markets),
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        retries = 3
        current_delay = 0.5

        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=10)
            except requests.RequestException:
                time.sleep(1)
                continue

            if response.status_code == 200:
                # Return data and credit usage header
                return response.json().get("data", {}), int(response.headers.get("x-requests-last", 0))

            elif response.status_code == 422:
                # 422 usually means no props available for this specific game
                return None, 0

            elif response.status_code == 429:
                # Rate limit hit: Exponential backoff
                time.sleep(current_delay * (2**attempt))
                continue

            elif response.status_code == 401 and "OUT_OF_USAGE_CREDITS" in response.text:
                raise Exception("CRITICAL: Out of usage credits!")

            else:
                # Other errors (500, etc.) - break to retry or fail
                break
        return None, 0

    def parse_and_store(self, data, snapshot_ts):
        """
        Parses the API response and inserts it into the staging table.
        Captures all timestamps to allow for Opening/Closing line analysis.
        """
        if not data or "bookmakers" not in data:
            return 0

        rows_to_insert = []

        game_id = data.get("id")
        commence_time = data.get("commence_time")
        home_team = data.get("home_team")
        away_team = data.get("away_team")

        if not game_id:
            return 0

        for book in data.get("bookmakers", []):
            book_key = book["key"]
            book_name = book.get("title")  # Capture readable name (e.g., "DraftKings")
            book_updated = book.get("last_update")  # Capture when bookmaker updated feed

            for market in book.get("markets", []):
                market_key = market["key"]
                market_updated = market.get("last_update")  # Capture when line actually moved

                for outcome in market.get("outcomes", []):
                    player = outcome.get("description")
                    if not player:
                        continue

                    # Note: We insert EVERY valid snapshot row.
                    # We rely on timestamp analysis later to find Opening/Closing lines.

                    rows_to_insert.append(
                        (
                            game_id,
                            player,
                            book_key,
                            market_key,
                            outcome["name"],
                            outcome.get("point", 0),
                            outcome.get("price"),
                            commence_time,
                            home_team,
                            away_team,
                            snapshot_ts,  # When we requested the data
                            market_updated,  # When the line changed
                            book_updated,  # When the bookmaker updated
                            book_name,
                        )
                    )

        if rows_to_insert:
            self._batch_insert(rows_to_insert)

        return len(rows_to_insert)

    def _batch_insert(self, rows):
        """
        High-speed bulk insert using psycopg2.extras.execute_values
        """
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                # UPDATED TABLE NAME: raw_player_props_staging_v2
                query = """
                        INSERT INTO raw_player_props_staging_v2
                        (api_game_id, api_player_name, bookmaker, market_key,
                         outcome_label, line, odds_american, commence_time, home_team, away_team,
                         snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                        VALUES %s
                    """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def generate_snapshot_timestamps():
    """Generates the list of timestamps to scrape based on NBA seasons"""
    snapshots = []

    # UPDATED HOURS: 13 (8AM EST) and 23 (6PM EST)
    # This gives you the 'Opening' liquidity and the 'Closing' line.
    target_hours = [13, 23]

    seasons = [
        ("2022-23 Playoffs", datetime(2023, 5, 3), datetime(2023, 6, 15), 1, [19]),
        ("2023-24 Regular", datetime(2023, 10, 20), datetime(2024, 4, 15), 2, target_hours),
        ("2023-24 Playoffs", datetime(2024, 4, 16), datetime(2024, 6, 20), 1, [19]),
        ("2024-25 Regular", datetime(2024, 10, 22), datetime(2025, 4, 15), 2, target_hours),
        ("2024-25 Playoffs", datetime(2025, 4, 16), datetime(2025, 6, 20), 1, [19]),
        ("2025-26 Regular", datetime(2025, 10, 20), datetime(2026, 1, 9), 2, target_hours),
    ]

    for season_name, start_date, end_date, num_snapshots, hours in seasons:
        current = start_date
        while current <= end_date:
            if current.month not in [7, 8, 9]:
                hours_to_use = hours if len(hours) > 0 else target_hours
                for hour in hours_to_use:
                    snapshot = current.replace(hour=hour, minute=0, second=0)
                    snapshots.append(
                        {
                            "timestamp": snapshot.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "season": season_name,
                        }
                    )
            current += timedelta(days=1)
    return snapshots


# --- Execution ---
if __name__ == "__main__":
    # Initialize Bot
    bot = ScraperBot(API_KEY, engine)
    snapshots = generate_snapshot_timestamps()

    print("🚀 Starting FULL DATA Scrape (7 Markets).")
    print(f"Target: {len(snapshots)} snapshots")

    total_credits = 0
    pbar_snaps = tqdm(snapshots, desc="Snapshots", unit="snap")

    try:
        for snapshot in pbar_snaps:
            ts = snapshot["timestamp"]

            # 1. Get List of Games for this Snapshot
            events_list = bot.get_events_for_date(ts)

            # 2. Loop through each game to get props
            for event in events_list:
                game_id = event["id"]

                # Scrape Props for this game
                data, credits = bot.scrape_event_props(ts, game_id)
                total_credits += credits

                if data:
                    rows = bot.parse_and_store(data, ts)
                    pbar_snaps.set_description(f"Saved: {rows} rows | Creds: {total_credits}")

                # Rate limiting sleep (Essential to avoid 429s)
                time.sleep(0.15)

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user. Progress saved.")
    except Exception as e:
        print(f"\n❌ Critical Error: {e}")

    print(f"\n🎉 Session Complete. Total Credits Used: {total_credits}")
