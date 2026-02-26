"""
MLB Reference Data Seeder
==========================
Seeds mlb_teams (30 teams) and mlb_park_factors from the MLB Stats API.

Usage:
    python -m src.scrapers.mlb.mlb_reference                # Seed teams + park factors
    python -m src.scrapers.mlb.mlb_reference --teams-only   # Only seed teams
    python -m src.scrapers.mlb.mlb_reference --parks-only   # Only seed park factors
"""

import argparse
import logging
import sys
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBReference")

BASE_URL = "https://statsapi.mlb.com/api/v1"

# Pre-computed park factors from FanGraphs data (2022-2025 averages).
# Values > 1.0 = hitter-friendly, < 1.0 = pitcher-friendly.
# venue_id sourced from MLB Stats API.
PARK_FACTORS = {
    # venue_id: (venue_name, runs, hr, hits, so)
    15: ("Chase Field", 1.06, 1.10, 1.03, 0.98),              # ARI
    2: ("Fenway Park", 1.04, 0.95, 1.06, 0.97),               # BOS
    4705: ("American Family Field", 1.02, 1.05, 1.01, 0.99),   # MIL
    17: ("Wrigley Field", 1.05, 1.10, 1.03, 0.97),             # CHC
    4: ("Guaranteed Rate Field", 1.01, 1.03, 1.00, 1.00),      # CWS
    27: ("Great American Ball Park", 1.08, 1.15, 1.04, 0.97),  # CIN
    5: ("Progressive Field", 0.98, 0.95, 0.98, 1.01),          # CLE
    19: ("Coors Field", 1.20, 1.15, 1.15, 0.85),               # COL
    2394: ("Comerica Park", 0.97, 0.92, 0.98, 1.02),           # DET
    2392: ("Minute Maid Park", 1.03, 1.05, 1.02, 0.99),        # HOU
    7: ("Kauffman Stadium", 0.98, 0.95, 0.99, 1.01),           # KC
    1: ("Angel Stadium", 0.97, 0.98, 0.98, 1.01),              # LAA
    22: ("Dodger Stadium", 0.95, 0.92, 0.97, 1.02),            # LAD
    4169: ("loanDepot park", 0.96, 0.90, 0.97, 1.02),          # MIA
    3289: ("Citi Field", 0.96, 0.93, 0.97, 1.02),              # NYM
    3313: ("Yankee Stadium", 1.05, 1.12, 1.02, 0.98),          # NYY
    10: ("Oakland Coliseum", 0.93, 0.88, 0.95, 1.03),          # OAK
    2681: ("Citizens Bank Park", 1.04, 1.08, 1.02, 0.99),      # PHI
    31: ("PNC Park", 0.96, 0.93, 0.97, 1.02),                  # PIT
    2680: ("Petco Park", 0.94, 0.90, 0.96, 1.03),              # SD
    2395: ("Oracle Park", 0.92, 0.85, 0.95, 1.04),             # SF
    680: ("T-Mobile Park", 0.95, 0.92, 0.97, 1.02),            # SEA
    2889: ("Busch Stadium", 0.97, 0.95, 0.98, 1.01),           # STL
    12: ("Tropicana Field", 0.96, 0.93, 0.97, 1.02),           # TB
    13: ("Globe Life Field", 1.01, 1.03, 1.00, 1.00),          # TEX
    14: ("Rogers Centre", 1.03, 1.05, 1.02, 0.99),             # TOR
    3309: ("Nationals Park", 1.00, 1.00, 1.00, 1.00),          # WSH
    2862: ("Target Field", 1.00, 1.02, 1.00, 1.00),            # MIN
    5325: ("Truist Park", 1.01, 1.03, 1.00, 1.00),             # ATL
}


def seed_teams(engine):
    """Fetch and insert all 30 MLB teams from the Stats API."""
    logger.info("Fetching MLB teams from Stats API...")

    url = f"{BASE_URL}/teams"
    params = {"sportId": 1, "activeStatus": "Y"}

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    teams = data.get("teams", [])
    inserted = 0

    with engine.begin() as conn:
        for team in teams:
            team_id = team["id"]
            team_name = team["name"]
            abbreviation = team.get("abbreviation", "")
            league = team.get("league", {}).get("abbreviation", "")
            division = team.get("division", {}).get("name", "")

            # Extract just "East", "Central", "West" from "American League East"
            if division:
                division = division.split()[-1] if division.split() else division

            conn.execute(
                text("""
                    INSERT INTO mlb_teams (team_id, team_name, team_abbreviation, league, division)
                    VALUES (:tid, :name, :abbrev, :league, :division)
                    ON CONFLICT (team_id) DO UPDATE SET
                        team_name = EXCLUDED.team_name,
                        team_abbreviation = EXCLUDED.team_abbreviation,
                        league = EXCLUDED.league,
                        division = EXCLUDED.division
                """),
                {
                    "tid": team_id,
                    "name": team_name,
                    "abbrev": abbreviation,
                    "league": league,
                    "division": division,
                },
            )
            inserted += 1
            logger.info(f"  {abbreviation}: {team_name} (ID: {team_id}, {league} {division})")

    logger.info(f"Inserted/updated {inserted} teams.")
    return inserted


def seed_park_factors(engine, seasons: list[int] | None = None):
    """Insert park factors for all venues across specified seasons.

    Uses pre-computed averages from FanGraphs data. Each venue gets the same
    factors for all seasons (can be refined per-season later).
    """
    if seasons is None:
        seasons = [2022, 2023, 2024, 2025]

    logger.info(f"Seeding park factors for seasons {seasons}...")
    inserted = 0

    with engine.begin() as conn:
        for venue_id, (venue_name, runs, hr, hits, so) in PARK_FACTORS.items():
            for season in seasons:
                conn.execute(
                    text("""
                        INSERT INTO mlb_park_factors
                            (venue_id, season, venue_name, runs_factor, hr_factor, hits_factor, so_factor)
                        VALUES
                            (:vid, :season, :name, :runs, :hr, :hits, :so)
                        ON CONFLICT (venue_id, season) DO UPDATE SET
                            venue_name = EXCLUDED.venue_name,
                            runs_factor = EXCLUDED.runs_factor,
                            hr_factor = EXCLUDED.hr_factor,
                            hits_factor = EXCLUDED.hits_factor,
                            so_factor = EXCLUDED.so_factor
                    """),
                    {
                        "vid": venue_id,
                        "season": season,
                        "name": venue_name,
                        "runs": runs,
                        "hr": hr,
                        "hits": hits,
                        "so": so,
                    },
                )
                inserted += 1

    logger.info(f"Inserted/updated {inserted} park factor records.")
    return inserted


def main():
    parser = argparse.ArgumentParser(description="MLB Reference Data Seeder")
    parser.add_argument("--teams-only", action="store_true", help="Only seed teams")
    parser.add_argument("--parks-only", action="store_true", help="Only seed park factors")
    parser.add_argument(
        "--seasons", nargs="+", type=int, default=[2022, 2023, 2024, 2025],
        help="Seasons for park factors (default: 2022-2025)",
    )

    args = parser.parse_args()
    engine = get_engine()

    print("=" * 60)
    print("MLB REFERENCE DATA SEEDER")
    print("=" * 60)

    if not args.parks_only:
        seed_teams(engine)

    if not args.teams_only:
        seed_park_factors(engine, args.seasons)

    print("Done.")


if __name__ == "__main__":
    main()
