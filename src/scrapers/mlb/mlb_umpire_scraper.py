"""
MLB Umpire Scraper
==================
Fetches umpire assignments for MLB games from the boxscore endpoint.
Reuses the same API endpoint as the lineup scraper.

Populates: mlb_game_umpires

Schedule:
    - Runs alongside lineup scraper (9:30 AM ET or combined with lineup job)

Usage:
    python src/scrapers/mlb/mlb_umpire_scraper.py
    python src/scrapers/mlb/mlb_umpire_scraper.py --date 2025-04-15
    python src/scrapers/mlb/mlb_umpire_scraper.py --backfill --start-date 2023-04-01
    python src/scrapers/mlb/mlb_umpire_scraper.py --local
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://statsapi.mlb.com/api/v1"
RATE_LIMIT_DELAY = 0.3


class MLBUmpireScraper:
    def __init__(self, engine, dry_run: bool = False):
        self.engine = engine
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GameFlowData/1.0"})

    def scrape_date(self, target_date: date) -> int:
        """Fetch and store umpire assignments for all games on target_date."""
        logger.info(f"Fetching umpire data for {target_date}")

        game_pks = self._get_game_pks_from_db(target_date)
        if not game_pks:
            logger.warning(f"No games in DB for {target_date}")
            return 0

        logger.info(f"Checking umpires for {len(game_pks)} games")

        total_stored = 0
        for game_pk in game_pks:
            count = self._scrape_game_umpires(game_pk, target_date)
            total_stored += count
            time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Stored {total_stored} umpire entries for {target_date}")
        return total_stored

    def _get_game_pks_from_db(self, target_date: date) -> list[int]:
        """Get game PKs from mlb_game_schedule for target_date."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT game_id FROM mlb_game_schedule
                    WHERE game_date = :d AND status != 'Cancelled'
                    ORDER BY game_id
                """),
                {"d": target_date},
            )
            return [row[0] for row in result]

    def _scrape_game_umpires(self, game_pk: int, game_date: date) -> int:
        """Fetch and store umpire data for a single game."""
        try:
            resp = self.session.get(
                f"{BASE_URL}/game/{game_pk}/boxscore",
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Game {game_pk}: not found")
                return 0
            logger.error(f"Game {game_pk}: HTTP {e.response.status_code}")
            return 0
        except Exception as e:
            logger.error(f"Game {game_pk}: request error — {e}")
            return 0

        data = resp.json()
        officials = data.get("officials", [])
        if not officials:
            logger.debug(f"Game {game_pk}: no officials data")
            return 0

        records: list[dict] = []
        for official in officials:
            person = official.get("official", {})
            official_type = official.get("officialType", "")
            records.append({
                "game_id": game_pk,
                "game_date": game_date,
                "umpire_id": person.get("id"),
                "umpire_name": person.get("fullName"),
                "position": official_type,
            })

        if not records:
            return 0

        if self.dry_run:
            logger.info(f"[DRY RUN] Game {game_pk}: would store {len(records)} umpires")
            for r in records:
                logger.info(f"  {r['umpire_name']} — {r['position']}")
            return len(records)

        with self.engine.begin() as conn:
            for r in records:
                conn.execute(
                    text("""
                        INSERT INTO mlb_game_umpires
                            (game_id, game_date, umpire_id, umpire_name, position)
                        VALUES
                            (:game_id, :game_date, :umpire_id, :umpire_name, :position)
                        ON CONFLICT (game_id, position) DO UPDATE SET
                            umpire_id   = EXCLUDED.umpire_id,
                            umpire_name = EXCLUDED.umpire_name
                    """),
                    r,
                )

        logger.info(f"Game {game_pk}: stored {len(records)} umpires")
        return len(records)

    def backfill(self, start_date: date, end_date: date | None = None) -> int:
        """Backfill umpire data from start_date to end_date (inclusive)."""
        end = end_date or date.today()
        current = start_date
        total = 0
        days_processed = 0
        while current <= end:
            count = self.scrape_date(current)
            total += count
            current += timedelta(days=1)
            days_processed += 1
            if days_processed % 30 == 0:
                logger.info(f"Backfill progress: {days_processed} days, {total} records")
        logger.info(f"Backfill complete: {total} umpire records over {days_processed} days")
        return total


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Scrape MLB umpire assignments")
    parser.add_argument("--date", type=str, help="Date YYYY-MM-DD (default: today)")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical data")
    parser.add_argument("--start-date", type=str, help="Start date for backfill")
    parser.add_argument("--end-date", type=str, help="End date for backfill (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    parser.add_argument("--local", action="store_true", help="Use local Postgres")
    args = parser.parse_args()

    engine = get_engine(local=args.local)
    scraper = MLBUmpireScraper(engine, dry_run=args.dry_run)

    if args.backfill:
        if not args.start_date:
            logger.error("--start-date required for backfill")
            sys.exit(1)
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date) if args.end_date else None
        total = scraper.backfill(start, end)
        logger.info(f"Done — {total} umpire entries backfilled")
    else:
        target_date = date.fromisoformat(args.date) if args.date else date.today()
        count = scraper.scrape_date(target_date)
        logger.info(f"Done — {count} umpire entries stored for {target_date}")

    sys.exit(0)


if __name__ == "__main__":
    main()
