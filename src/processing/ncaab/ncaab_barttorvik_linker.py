"""
NCAAB Barttorvik Team Linker
================================
Populates team_id in ncaab_barttorvik_ratings by matching
Barttorvik team names to ncaab_teams.

Run after each Barttorvik scrape to link new ratings rows.

Usage:
    python -m src.processing.ncaab.ncaab_barttorvik_linker
"""

import logging
import os
import sys
from difflib import SequenceMatcher
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.processing.ncaab.ncaab_config import BARTTORVIK_TO_ESPN, FUZZY_THRESHOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NCAABBartLinker")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def link_barttorvik_teams(engine) -> tuple[int, int]:
    """Link Barttorvik team names to ncaab_teams.team_id.

    Strategy:
    1. Manual mappings (BARTTORVIK_TO_ESPN) -> exact match
    2. Direct name match against ncaab_teams.team_name
    3. Fuzzy match (SequenceMatcher >= threshold) for remaining

    Returns:
        (linked_count, unlinked_count)
    """
    # Build lookup: {normalized_name_lower: team_id}
    with engine.connect() as conn:
        result = conn.execute(text("SELECT team_id, team_name, short_name FROM ncaab_teams"))
        team_rows = result.fetchall()

    team_lookup = {}
    for team_id, team_name, short_name in team_rows:
        if team_name:
            team_lookup[team_name.lower().strip()] = team_id
        if short_name:
            team_lookup[short_name.lower().strip()] = team_id

    # Add manual mappings (Barttorvik name -> ESPN name -> team_id)
    manual_lookup = {}
    for bart_name, espn_name in BARTTORVIK_TO_ESPN.items():
        espn_lower = espn_name.lower().strip()
        if espn_lower in team_lookup:
            manual_lookup[bart_name.lower().strip()] = team_lookup[espn_lower]

    # Fetch unlinked rows
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT DISTINCT team_name FROM ncaab_barttorvik_ratings WHERE team_id IS NULL"
        ))
        unlinked_names = [row[0] for row in result]

    if not unlinked_names:
        logger.info("All Barttorvik ratings already linked.")
        return 0, 0

    logger.info(f"Found {len(unlinked_names)} unlinked Barttorvik team names")

    # Match each name
    updates = []  # (team_name, team_id)
    unmatched = []

    for bart_name in unlinked_names:
        name_lower = bart_name.lower().strip()
        team_id = None

        # Step 1: Manual mapping
        if name_lower in manual_lookup:
            team_id = manual_lookup[name_lower]

        # Step 2: Direct match
        if team_id is None and name_lower in team_lookup:
            team_id = team_lookup[name_lower]

        # Step 3: Fuzzy match
        if team_id is None:
            best_ratio = 0
            best_id = None
            for known_name, tid in team_lookup.items():
                ratio = SequenceMatcher(None, name_lower, known_name).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_id = tid

            if best_ratio >= FUZZY_THRESHOLD:
                team_id = best_id

        if team_id is not None:
            updates.append((bart_name, team_id))
        else:
            unmatched.append(bart_name)

    # Apply updates
    if updates:
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                for team_name, team_id in updates:
                    cur.execute("""
                        UPDATE ncaab_barttorvik_ratings
                        SET team_id = %s
                        WHERE team_name = %s AND team_id IS NULL
                    """, (team_id, team_name))
            conn.commit()
        finally:
            conn.close()

    logger.info(f"Linked: {len(updates)}, Unmatched: {len(unmatched)}")
    if unmatched:
        logger.warning(f"Unmatched names (add to BARTTORVIK_TO_ESPN): {unmatched[:20]}")

    return len(updates), len(unmatched)


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)
    linked, unlinked = link_barttorvik_teams(engine)
    logger.info(f"Done. Linked={linked}, Unlinked={unlinked}")
