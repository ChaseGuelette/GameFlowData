"""
MLB Active Roster Scraper
=========================
Fetches the 26-man active roster for all 30 MLB teams daily.
Players absent from the active roster are on the IL, DFA, or otherwise unavailable.

Populates: mlb_active_roster

Schedule:
    - 9:30 AM ET daily (after overnight transactions processed by MLB)

Usage:
    python src/scrapers/mlb/mlb_roster_scraper.py
    python src/scrapers/mlb/mlb_roster_scraper.py --date 2025-04-15
    python src/scrapers/mlb/mlb_roster_scraper.py --dry-run
    python src/scrapers/mlb/mlb_roster_scraper.py --local
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://statsapi.mlb.com/api/v1"
RATE_LIMIT_DELAY = 0.3  # seconds between requests


class MLBRosterScraper:
    def __init__(self, engine, dry_run: bool = False):
        self.engine = engine
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GameFlowData/1.0"})

    def scrape_date(self, roster_date: date) -> int:
        """Fetch and store active rosters for all MLB teams. Returns total players stored."""
        logger.info(f"Fetching MLB active rosters for {roster_date}")

        team_ids = self._get_team_ids_from_db()
        if not team_ids:
            logger.error("No teams found in mlb_teams table")
            return 0

        logger.info(f"Fetching rosters for {len(team_ids)} teams")

        total_stored = 0
        for team_id in sorted(team_ids):
            count = self._scrape_team_roster(team_id, roster_date)
            total_stored += count
            time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Stored {total_stored} roster entries for {roster_date}")
        return total_stored

    def _get_team_ids_from_db(self) -> list[int]:
        """Get all MLB team IDs from mlb_teams."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT team_id FROM mlb_teams ORDER BY team_id"))
            return [row[0] for row in result]

    def _scrape_team_roster(self, team_id: int, roster_date: date) -> int:
        """Fetch and store the active roster for one team. Returns number of players stored."""
        try:
            resp = self.session.get(
                f"{BASE_URL}/teams/{team_id}/roster",
                params={
                    "rosterType": "active",
                    "date": roster_date.isoformat(),
                },
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"Team {team_id}: HTTP {e.response.status_code}")
            return 0
        except Exception as e:
            logger.error(f"Team {team_id}: request error — {e}")
            return 0

        data = resp.json()
        roster = data.get("roster", [])

        if not roster:
            logger.warning(f"Team {team_id}: empty roster response")
            return 0

        records = []
        for entry in roster:
            person = entry.get("person", {})
            player_id = person.get("id")
            if not player_id:
                continue
            position = entry.get("position", {})
            records.append({
                "roster_date": roster_date,
                "team_id": team_id,
                "player_id": player_id,
                "player_name": person.get("fullName"),
                "position": position.get("abbreviation"),
                "jersey_number": entry.get("jerseyNumber"),
            })

        if not records:
            return 0

        if self.dry_run:
            logger.info(f"[DRY RUN] Team {team_id}: would store {len(records)} players")
            for r in records[:3]:
                logger.info(f"  {r['player_name']} ({r['position']}) #{r['jersey_number']}")
            return len(records)

        scraped_at = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            for r in records:
                conn.execute(
                    text("""
                        INSERT INTO mlb_active_roster
                            (roster_date, team_id, player_id, player_name,
                             position, jersey_number, scraped_at)
                        VALUES
                            (:roster_date, :team_id, :player_id, :player_name,
                             :position, :jersey_number, :scraped_at)
                        ON CONFLICT (roster_date, player_id) DO UPDATE SET
                            position      = EXCLUDED.position,
                            jersey_number = EXCLUDED.jersey_number,
                            player_name   = COALESCE(EXCLUDED.player_name, mlb_active_roster.player_name),
                            scraped_at    = EXCLUDED.scraped_at
                    """),
                    {**r, "scraped_at": scraped_at},
                )

        logger.info(f"Team {team_id}: stored {len(records)} active roster entries")
        return len(records)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Scrape MLB 26-man active rosters")
    parser.add_argument("--date", type=str, help="Date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    parser.add_argument("--local", action="store_true", help="Use local Postgres")
    args = parser.parse_args()

    roster_date = date.fromisoformat(args.date) if args.date else date.today()
    engine = get_engine(local=args.local)
    scraper = MLBRosterScraper(engine, dry_run=args.dry_run)
    count = scraper.scrape_date(roster_date)
    logger.info(f"Done — {count} active roster entries stored for {roster_date}")
    sys.exit(0)


if __name__ == "__main__":
    main()
