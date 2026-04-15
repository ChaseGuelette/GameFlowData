import argparse
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

# Market presets
CORE_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_turnovers",
]

COMBO_MARKETS = [
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
    "player_double_double",
    "player_triple_double",
]

ALL_MARKETS = CORE_MARKETS + COMBO_MARKETS


class ScraperBot:
    def __init__(self, api_key, db_engine, markets=None):
        self.api_key = api_key
        self.engine = db_engine
        self.session = requests.Session()
        self.markets_key = ",".join(sorted(markets or CORE_MARKETS))
        self.processed_ids = self._load_processed_ids()

    def _load_processed_ids(self):
        """Load progress from JSON file. Only resumes if markets match."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE) as f:
                    data = json.load(f)
                # New format: {"markets": "...", "processed": [[ts, eid], ...]}
                if isinstance(data, dict) and "markets" in data:
                    if data["markets"] == self.markets_key:
                        ids = set(tuple(x) for x in data["processed"])
                        print(f"Resumed progress: {len(ids)} events already scraped.")
                        return ids
                    else:
                        print(f"Progress file markets differ (stored: {data['markets'][:60]}...). Starting fresh.")
                        return set()
                # Legacy format: [[ts, eid], ...] — ignore, start fresh
                print("Legacy progress file found. Starting fresh.")
            except Exception as e:
                print(f"Warning: Failed to load progress: {e}")
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
            except Exception:
                time.sleep(1)
        return []

    def scrape_event_props(self, date_str, event_id, markets=None):
        """
        Step 2: Get Player Props for a single game.
        Endpoint: /v4/historical/sports/basketball_nba/events/{eventId}/odds
        """
        url = f"https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/{event_id}/odds"

        target_markets = markets or CORE_MARKETS

        params = {
            "apiKey": self.api_key,
            "date": date_str,
            "regions": "us,us2,us_ex",  # All US regions for full coverage
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

        raw_game_id = data.get("id")
        game_id = raw_game_id.zfill(10) if raw_game_id else raw_game_id
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
                query = """
                        INSERT INTO raw_player_props_combined
                        (api_game_id, api_player_name, bookmaker, market_key,
                         outcome_label, line, odds_american, commence_time, home_team, away_team,
                         snapshot_time, market_last_update, bookmaker_last_update, bookmaker_name)
                        VALUES %s
                    """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()


def generate_snapshot_timestamps(start_from=None, end_at=None):
    """Generates the list of timestamps to scrape based on NBA seasons.

    Args:
        start_from: Optional datetime — skip all snapshots before this date.
        end_at: Optional datetime — skip all snapshots after this date.
    """
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
        ("2025-26 Regular", datetime(2025, 10, 20), datetime(2026, 4, 15), 2, target_hours),
        ("2025-26 Playoffs", datetime(2026, 4, 16), datetime(2026, 6, 20), 1, [19]),
    ]

    for season_name, season_start, season_end, num_snapshots, hours in seasons:
        current = season_start
        while current <= season_end:
            if current.month not in [7, 8, 9]:
                hours_to_use = hours if len(hours) > 0 else target_hours
                for hour in hours_to_use:
                    snapshot = current.replace(hour=hour, minute=0, second=0)
                    if start_from and snapshot < start_from:
                        continue
                    if end_at and snapshot > end_at:
                        continue
                    snapshots.append(
                        {
                            "timestamp": snapshot.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "season": season_name,
                        }
                    )
            current += timedelta(days=1)
    return snapshots


def resolve_markets(args) -> list[str]:
    """Resolve the final market list from CLI args."""
    if args.markets:
        # Explicit market list overrides presets
        return args.markets

    markets = list(CORE_MARKETS)
    if args.combos:
        markets.extend(COMBO_MARKETS)
    if args.combos_only:
        markets = list(COMBO_MARKETS)
    return markets


# --- Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Historical NBA player props backfill scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Market presets:
  core (default):  player_points, player_rebounds, player_assists,
                   player_threes, player_blocks, player_steals,
                   player_turnovers
  combos:          player_points_rebounds_assists, player_points_rebounds,
                   player_points_assists, player_rebounds_assists,
                   player_double_double, player_triple_double

Examples:
  # Full backfill with core markets (original behavior)
  python player_prop_scraper.py

  # Resume from Jan 1 2025 with core + combo markets
  python player_prop_scraper.py --start-date 2025-01-01 --combos

  # Only combo markets, specific date range
  python player_prop_scraper.py --combos-only --start-date 2024-10-22 --end-date 2025-04-15

  # Specific markets only
  python player_prop_scraper.py --markets player_double_double player_triple_double

  # Estimate credits without scraping
  python player_prop_scraper.py --combos --start-date 2025-01-01 --dry-run
""",
    )

    # Date range
    parser.add_argument(
        "--start-date", type=str, default=None,
        help="Resume from this date (YYYY-MM-DD). Skips all snapshots before it.",
    )
    parser.add_argument(
        "--end-date", type=str, default=None,
        help="Stop at this date (YYYY-MM-DD). Skips all snapshots after it.",
    )

    # Market selection (mutually exclusive presets)
    market_group = parser.add_mutually_exclusive_group()
    market_group.add_argument(
        "--combos", action="store_true",
        help="Add combo markets (PRA, P+R, P+A, R+A, DD, TD) to core markets",
    )
    market_group.add_argument(
        "--combos-only", action="store_true",
        help="Scrape ONLY combo markets (skip core markets)",
    )
    market_group.add_argument(
        "--markets", nargs="+", default=None,
        help="Explicit list of market keys to scrape (overrides presets)",
    )

    # Utility
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show snapshot count and estimated credits without scraping",
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Ignore progress file and scrape everything from scratch",
    )

    args = parser.parse_args()

    # Parse date filters
    start_from = None
    end_at = None
    if args.start_date:
        start_from = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_at = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

    # Resolve markets
    markets = resolve_markets(args)

    # Generate snapshots with date filter
    snapshots = generate_snapshot_timestamps(start_from=start_from, end_at=end_at)

    # Dry run: estimate credits and exit
    if args.dry_run:
        avg_games_per_day = 6  # rough NBA average
        n_markets = len(markets)
        total_snapshots = len(snapshots)
        # Each event costs 10 credits per market-with-data per region.
        # With 1 region call ("us2,us_ex" counts as 1), cost = 10 * min(n_markets, markets_with_data)
        # Conservative estimate: assume all requested markets have data
        est_events = total_snapshots * avg_games_per_day
        est_credits = est_events * n_markets * 10
        print(f"Markets ({n_markets}): {', '.join(markets)}")
        print(f"Snapshots: {total_snapshots}")
        print(f"Est. events: ~{est_events}")
        print(f"Est. credits: ~{est_credits:,} (assumes {avg_games_per_day} games/day, all markets have data)")
        if start_from:
            print(f"Start from: {args.start_date}")
        if end_at:
            print(f"End at: {args.end_date}")
        print("\nNote: Actual cost may be lower — API only charges for markets with data in the response.")
        exit(0)

    # Handle --no-resume: delete progress file before loading
    if args.no_resume and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("Progress file deleted (--no-resume).")

    # Initialize Bot (pass markets so progress file is market-aware)
    bot = ScraperBot(API_KEY, engine, markets=markets)

    print(f"Starting backfill scrape ({len(markets)} markets).")
    print(f"Markets: {', '.join(markets)}")
    print(f"Snapshots: {len(snapshots)}")
    if bot.processed_ids:
        print(f"Resuming: {len(bot.processed_ids)} events already completed.")
    if start_from:
        print(f"Resuming from: {args.start_date}")
    if end_at:
        print(f"Through: {args.end_date}")

    total_credits = 0
    pbar_snaps = tqdm(snapshots, desc="Snapshots", unit="snap")

    total_rows = 0
    skipped = 0

    try:
        for snapshot in pbar_snaps:
            ts = snapshot["timestamp"]

            # 1. Get List of Games for this Snapshot
            events_list = bot.get_events_for_date(ts)

            # 2. Loop through each game to get props
            for event in events_list:
                game_id = event["id"]

                # Skip if already processed (resume capability)
                if (ts, game_id) in bot.processed_ids:
                    skipped += 1
                    continue

                # Scrape Props for this game
                data, credits = bot.scrape_event_props(ts, game_id, markets=markets)
                total_credits += credits

                if data:
                    rows = bot.parse_and_store(data, ts)
                    total_rows += rows

                # Mark as processed regardless of whether data was returned
                # (422/empty means no props exist — no need to retry)
                bot.processed_ids.add((ts, game_id))

                pbar_snaps.set_description(
                    f"Rows: {total_rows} | Creds: {total_credits} | Skip: {skipped}"
                )

                # Rate limiting sleep (Essential to avoid 429s)
                time.sleep(0.15)

            # Save progress after each snapshot (all games for that timestamp)
            bot.save_progress()

    except KeyboardInterrupt:
        bot.save_progress()
        print(f"\nStopped by user. Progress saved. Credits used: {total_credits}")
    except Exception as e:
        bot.save_progress()
        print(f"\nCritical Error: {e}. Progress saved. Credits used: {total_credits}")

    print(f"\nSession Complete. Total Credits Used: {total_credits}")
    print(f"Total Rows Inserted: {total_rows} | Events Skipped (resume): {skipped}")
