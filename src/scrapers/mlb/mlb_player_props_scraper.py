"""
MLB Player Props Historical Backfill Scraper
=============================================
Scrapes historical MLB player props from The Odds API.
Stores into mlb_raw_player_props table.

Pattern: src/scrapers/player_prop_scraper.py (NBA equivalent)
Change: sport key basketball_nba -> baseball_mlb, MLB market keys

Historical props available from May 2023 onward.

Usage:
    python -m src.scrapers.mlb.mlb_player_props_scraper
    python -m src.scrapers.mlb.mlb_player_props_scraper --start-date 2024-04-01 --end-date 2024-09-30
    python -m src.scrapers.mlb.mlb_player_props_scraper --dry-run
    python -m src.scrapers.mlb.mlb_player_props_scraper --markets pitcher_strikeouts batter_hits
"""

import argparse
import json
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
logger = logging.getLogger("MLBPropsBackfill")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ODDS_API_KEY")

SPORT_KEY = "baseball_mlb"
PROGRESS_FILE = "mlb_props_scrape_progress.json"

# Phase 1 priority markets
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


class MLBPropsBackfillScraper:
    def __init__(self, api_key, db_engine, markets=None):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self.markets = markets or MLB_CORE_MARKETS
        self.markets_key = ",".join(sorted(self.markets))
        self.processed_ids = self._load_processed_ids()

    def _load_processed_ids(self):
        """Load progress from JSON file. Only resumes if markets match."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE) as f:
                    data = json.load(f)
                if isinstance(data, dict) and data.get("markets") == self.markets_key:
                    ids = set(tuple(x) for x in data["processed"])
                    logger.info(f"Resumed progress: {len(ids)} events already scraped.")
                    return ids
                logger.info("Progress file markets differ. Starting fresh.")
            except Exception as e:
                logger.warning(f"Failed to load progress: {e}")
        return set()

    def save_progress(self):
        """Save processed IDs to local JSON file for resume capability."""
        data = {
            "markets": self.markets_key,
            "processed": [list(x) for x in self.processed_ids],
        }
        with open(PROGRESS_FILE, "w") as f:
            json.dump(data, f)

    def get_events_for_date(self, date_str):
        """Get list of MLB events for a specific snapshot date."""
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
        params = {"apiKey": self.api_key, "date": date_str}

        for _ in range(3):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json().get("data", [])
                elif response.status_code == 429:
                    time.sleep(1)
            except Exception:
                time.sleep(1)
        return []

    def scrape_event_props(self, date_str, event_id):
        """Fetch player props for a single MLB game at a historical snapshot."""
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events/{event_id}/odds"
        params = {
            "apiKey": self.api_key,
            "date": date_str,
            "regions": "us,us2,us_ex,us_dfs",
            "markets": ",".join(self.markets),
            "oddsFormat": "american",
            "dateFormat": "iso",
        }

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json().get("data", {}), int(response.headers.get("x-requests-last", 0))
                elif response.status_code == 422:
                    return None, 0
                elif response.status_code == 429:
                    time.sleep(0.5 * (2 ** attempt))
                elif response.status_code == 401 and "OUT_OF_USAGE_CREDITS" in response.text:
                    raise RuntimeError("CRITICAL: Out of usage credits!")
            except requests.RequestException:
                time.sleep(1)
        return None, 0

    def parse_and_store(self, data, snapshot_ts):
        """Parse props response and insert into mlb_raw_player_props."""
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
                    player = outcome.get("description")
                    if not player:
                        continue

                    rows.append((
                        game_id, player, book_key, market_key,
                        outcome.get("name"), outcome.get("point"),
                        outcome.get("price"), commence_time,
                        home_team, away_team, snapshot_ts,
                        market_updated, book_updated, book_name,
                    ))

        if rows:
            self._batch_insert(rows)
        return len(rows)

    def _batch_insert(self, rows):
        """Bulk insert into mlb_raw_player_props."""
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO mlb_raw_player_props
                    (api_game_id, api_player_name, bookmaker, market_key,
                     outcome_label, line, odds_american, commence_time,
                     home_team, away_team, snapshot_time,
                     market_last_update, bookmaker_last_update, bookmaker_name)
                    VALUES %s
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def generate_mlb_snapshot_timestamps(start_from=None, end_at=None):
    """Generate snapshot timestamps for MLB historical scraping.

    MLB props available from May 2023. Season runs late March through October.
    Two snapshots per day: 17:00 UTC (12pm ET) and 23:00 UTC (6pm ET).
    """
    snapshots = []
    target_hours = [17, 23]

    seasons = [
        ("2023 Regular", datetime(2023, 5, 1), datetime(2023, 10, 1)),
        ("2023 Postseason", datetime(2023, 10, 2), datetime(2023, 11, 5)),
        ("2024 Regular", datetime(2024, 3, 28), datetime(2024, 9, 29)),
        ("2024 Postseason", datetime(2024, 10, 1), datetime(2024, 11, 3)),
        ("2025 Regular", datetime(2025, 3, 25), datetime(2025, 9, 28)),
        ("2025 Postseason", datetime(2025, 10, 1), datetime(2025, 11, 5)),
    ]

    for season_name, season_start, season_end in seasons:
        current = season_start
        while current <= season_end:
            for hour in target_hours:
                snapshot = current.replace(hour=hour, minute=0, second=0)
                if start_from and snapshot < start_from:
                    continue
                if end_at and snapshot > end_at:
                    continue
                if snapshot > datetime.utcnow():
                    continue
                snapshots.append({
                    "timestamp": snapshot.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "season": season_name,
                })
            current += timedelta(days=1)

    return snapshots


def main():
    parser = argparse.ArgumentParser(description="MLB Player Props Historical Backfill")
    parser.add_argument("--start-date", type=str, default=None, help="Start from date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End at date (YYYY-MM-DD)")

    market_group = parser.add_mutually_exclusive_group()
    market_group.add_argument("--extended", action="store_true", help="Include extended markets")
    market_group.add_argument("--markets", nargs="+", default=None, help="Explicit market list")

    parser.add_argument("--dry-run", action="store_true", help="Estimate credits without scraping")
    parser.add_argument("--no-resume", action="store_true", help="Ignore progress file")

    args = parser.parse_args()

    if not DATABASE_URL or not API_KEY:
        logger.error("Missing DATABASE_URL or ODDS_API_KEY.")
        sys.exit(1)

    # Resolve markets
    if args.markets:
        markets = args.markets
    elif args.extended:
        markets = MLB_CORE_MARKETS + MLB_EXTENDED_MARKETS
    else:
        markets = list(MLB_CORE_MARKETS)

    # Parse date filters
    start_from = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_at = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59) if args.end_date else None

    snapshots = generate_mlb_snapshot_timestamps(start_from=start_from, end_at=end_at)

    if args.dry_run:
        avg_games_per_day = 8  # MLB averages ~15 games/day, but not all have props
        est_events = len(snapshots) * avg_games_per_day
        est_credits = est_events * len(markets) * 10
        print(f"Markets ({len(markets)}): {', '.join(markets)}")
        print(f"Snapshots: {len(snapshots)}")
        print(f"Est. events: ~{est_events}")
        print(f"Est. credits: ~{est_credits:,}")
        return

    engine = create_engine(DATABASE_URL)

    if args.no_resume and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    bot = MLBPropsBackfillScraper(API_KEY, engine, markets=markets)

    print(f"Starting MLB props backfill ({len(markets)} markets).")
    print(f"Markets: {', '.join(markets)}")
    print(f"Snapshots: {len(snapshots)}")

    total_credits = 0
    total_rows = 0
    skipped = 0

    pbar = tqdm(snapshots, desc="Snapshots", unit="snap")

    try:
        for snapshot in pbar:
            ts = snapshot["timestamp"]
            events_list = bot.get_events_for_date(ts)

            for event in events_list:
                game_id = event["id"]

                if (ts, game_id) in bot.processed_ids:
                    skipped += 1
                    continue

                data, credits = bot.scrape_event_props(ts, game_id)
                total_credits += credits

                if data:
                    rows = bot.parse_and_store(data, ts)
                    total_rows += rows

                bot.processed_ids.add((ts, game_id))
                pbar.set_description(f"Rows: {total_rows} | Creds: {total_credits} | Skip: {skipped}")

                time.sleep(0.15)

            bot.save_progress()

    except KeyboardInterrupt:
        bot.save_progress()
        print(f"\nStopped by user. Progress saved. Credits: {total_credits}")
    except Exception as e:
        bot.save_progress()
        print(f"\nError: {e}. Progress saved. Credits: {total_credits}")

    print(f"\nComplete. Total Credits: {total_credits} | Rows: {total_rows} | Skipped: {skipped}")


if __name__ == "__main__":
    main()
