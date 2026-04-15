"""
MLB Lineup Scraper
==================
Fetches confirmed starting lineups for today's MLB games from the MLB Stats API.
Uses the boxscore endpoint which becomes populated ~2-3 hours before first pitch.

Populates: mlb_game_lineups

Schedule:
    - 12:45 PM ET (before 1:30 PM inference — catches afternoon game lineups)
    - 6:10 PM ET (before 6:30 PM inference — catches evening game lineups)

Usage:
    python src/scrapers/mlb/mlb_lineup_scraper.py
    python src/scrapers/mlb/mlb_lineup_scraper.py --date 2025-04-15
    python src/scrapers/mlb/mlb_lineup_scraper.py --dry-run
    python src/scrapers/mlb/mlb_lineup_scraper.py --local
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://statsapi.mlb.com/api/v1"
RATE_LIMIT_DELAY = 0.3  # seconds between requests


class MLBLineupScraper:
    def __init__(self, engine, dry_run: bool = False):
        self.engine = engine
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GameFlowData/1.0"})

    def scrape_date(self, target_date: date) -> int:
        """Fetch and store lineups for all games on target_date. Returns count stored."""
        logger.info(f"Fetching MLB lineups for {target_date}")

        game_pks = self._get_game_pks_from_db(target_date)
        if not game_pks:
            logger.warning(f"No games in DB for {target_date} — run mlb_stats_scraper first")
            return 0

        logger.info(f"Checking lineups for {len(game_pks)} games")

        total_stored = 0
        for game_pk in game_pks:
            count = self._scrape_game_lineup(game_pk, target_date)
            total_stored += count
            time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Stored {total_stored} lineup entries for {target_date}")
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

    def _scrape_game_lineup(self, game_pk: int, game_date: date) -> int:
        """Fetch and store lineup for a single game. Returns number of players stored."""
        try:
            resp = self.session.get(
                f"{BASE_URL}/game/{game_pk}/boxscore",
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Game {game_pk}: not found (lineup not yet available)")
                return 0
            logger.error(f"Game {game_pk}: HTTP {e.response.status_code}")
            return 0
        except Exception as e:
            logger.error(f"Game {game_pk}: request error — {e}")
            return 0

        data = resp.json()
        teams = data.get("teams", {})
        records: list[dict] = []

        for side in ("home", "away"):
            team_data = teams.get(side, {})
            team_info = team_data.get("team", {})
            team_id = team_info.get("id")
            batting_order: list[int] = team_data.get("battingOrder", [])
            pitchers: set[int] = set(team_data.get("pitchers", []))
            players: dict = team_data.get("players", {})

            if not batting_order and not pitchers:
                logger.debug(f"Game {game_pk} {side}: lineup not yet posted")
                continue

            # Batting order (positions 1-9)
            batting_set = set(batting_order)
            for idx, player_id in enumerate(batting_order):
                player_detail = players.get(f"ID{player_id}", {}).get("person", {})
                records.append({
                    "game_pk": game_pk,
                    "game_date": game_date,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": player_detail.get("fullName"),
                    "lineup_position": idx + 1,
                    "is_pitcher": False,
                })

            # Starting pitcher (in pitchers list but not in batting order)
            for player_id in pitchers:
                if player_id not in batting_set:
                    player_detail = players.get(f"ID{player_id}", {}).get("person", {})
                    records.append({
                        "game_pk": game_pk,
                        "game_date": game_date,
                        "team_id": team_id,
                        "player_id": player_id,
                        "player_name": player_detail.get("fullName"),
                        "lineup_position": None,
                        "is_pitcher": True,
                    })

        if not records:
            logger.debug(f"Game {game_pk}: no lineup data returned yet")
            return 0

        if self.dry_run:
            logger.info(f"[DRY RUN] Game {game_pk}: would store {len(records)} entries")
            for r in records[:4]:
                logger.info(f"  {r['player_name']} pos={r['lineup_position']} pitcher={r['is_pitcher']}")
            return len(records)

        confirmed_at = datetime.now(UTC)
        with self.engine.begin() as conn:
            for r in records:
                conn.execute(
                    text("""
                        INSERT INTO mlb_game_lineups
                            (game_pk, game_date, team_id, player_id, player_name,
                             lineup_position, is_pitcher, confirmed_at)
                        VALUES
                            (:game_pk, :game_date, :team_id, :player_id, :player_name,
                             :lineup_position, :is_pitcher, :confirmed_at)
                        ON CONFLICT (game_pk, player_id) DO UPDATE SET
                            lineup_position = EXCLUDED.lineup_position,
                            is_pitcher      = EXCLUDED.is_pitcher,
                            player_name     = COALESCE(EXCLUDED.player_name, mlb_game_lineups.player_name),
                            confirmed_at    = EXCLUDED.confirmed_at
                    """),
                    {**r, "confirmed_at": confirmed_at},
                )

        logger.info(f"Game {game_pk}: stored {len(records)} lineup entries")
        return len(records)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Scrape MLB pre-game lineups")
    parser.add_argument("--date", type=str, help="Date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    parser.add_argument("--local", action="store_true", help="Use local Postgres")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    engine = get_engine(local=args.local)
    scraper = MLBLineupScraper(engine, dry_run=args.dry_run)
    count = scraper.scrape_date(target_date)
    logger.info(f"Done — {count} lineup entries stored for {target_date}")
    sys.exit(0)


if __name__ == "__main__":
    main()
