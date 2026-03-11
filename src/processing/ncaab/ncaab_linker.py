"""
NCAAB Game Lines Linker
=========================
Links ncaab_raw_game_lines rows by populating game_id, home_team_id, away_team_id.
Game-level only — no player matching needed (unlike MLB/NBA linkers).

Matches Odds API game records to ncaab_game_schedule by (home_team, away_team, date).

Usage:
    python -m src.processing.ncaab.ncaab_linker status
    python -m src.processing.ncaab.ncaab_linker incremental
    python -m src.processing.ncaab.ncaab_linker backfill
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.processing.ncaab.ncaab_config import (
    FUZZY_THRESHOLD,
    LINKER_BATCH_SIZE,
    ODDS_API_TEAM_ALIASES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NCAABLinker")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def normalize_team(name: str, alias_map: dict | None = None) -> str | None:
    """Normalize team name using alias map and basic cleaning."""
    if not name or not name.strip():
        return None

    name = name.strip()

    # Check alias map first
    if alias_map and name in alias_map:
        return alias_map[name]

    # Try lowercase lookup
    if alias_map:
        for k, v in alias_map.items():
            if k.lower() == name.lower():
                return v

    return name


def build_schedule_lookup(engine) -> dict:
    """Build {(home_name_lower, away_name_lower): [(game_id, game_date, home_team_id, away_team_id)]}
    from ncaab_game_schedule + ncaab_teams.
    """
    query = text("""
        SELECT gs.game_id, gs.game_date, gs.home_team_id, gs.away_team_id,
               ht.team_name AS home_name, at.team_name AS away_name
        FROM ncaab_game_schedule gs
        LEFT JOIN ncaab_teams ht ON ht.team_id = gs.home_team_id
        LEFT JOIN ncaab_teams at ON at.team_id = gs.away_team_id
        WHERE ht.team_name IS NOT NULL AND at.team_name IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    lookup = {}
    for game_id, game_date, home_tid, away_tid, home_name, away_name in rows:
        key = (home_name.lower().strip(), away_name.lower().strip())
        if key not in lookup:
            lookup[key] = []
        lookup[key].append((game_id, game_date, home_tid, away_tid))

    logger.info(f"Schedule lookup: {len(lookup)} unique matchups, {len(rows)} games")
    return lookup


def build_team_name_lookup(engine) -> dict:
    """Build {normalized_name_lower: team_name} from ncaab_teams."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT team_name, short_name, abbreviation FROM ncaab_teams"
        ))
        rows = result.fetchall()

    lookup = {}
    for team_name, short_name, abbrev in rows:
        if team_name:
            lookup[team_name.lower().strip()] = team_name
        if short_name:
            lookup[short_name.lower().strip()] = team_name
        if abbrev:
            lookup[abbrev.lower().strip()] = team_name

    return lookup


def find_closest_game(candidates: list, target_date, max_days: int = 1):
    """Find game within ±max_days of target_date."""
    if not candidates:
        return None

    best = None
    best_diff = max_days + 1

    for game_id, game_date, home_tid, away_tid in candidates:
        if game_date is None:
            continue
        diff = abs((game_date - target_date).days)
        if diff <= max_days and diff < best_diff:
            best = (game_id, home_tid, away_tid)
            best_diff = diff

    return best


def fuzzy_match_team(name: str, team_lookup: dict) -> str | None:
    """Fuzzy match a team name against known teams."""
    name_lower = name.lower().strip()

    # Exact match
    if name_lower in team_lookup:
        return team_lookup[name_lower]

    # Fuzzy match
    best_match = None
    best_ratio = 0

    for known_name in team_lookup:
        ratio = SequenceMatcher(None, name_lower, known_name).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = team_lookup[known_name]

    if best_ratio >= FUZZY_THRESHOLD:
        return best_match

    return None


def match_batch(batch_df: pd.DataFrame, schedule_lookup: dict,
                team_lookup: dict, fuzzy_cache: dict) -> pd.DataFrame:
    """Match game lines to schedule entries.

    For each row: normalize home/away team from Odds API,
    look up in schedule by (home, away, date ±1 day).
    """
    results = []

    for _, row in batch_df.iterrows():
        home_raw = str(row.get("home_team", "")).strip()
        away_raw = str(row.get("away_team", "")).strip()

        # Normalize via alias map
        home_norm = normalize_team(home_raw, ODDS_API_TEAM_ALIASES)
        away_norm = normalize_team(away_raw, ODDS_API_TEAM_ALIASES)

        # Try fuzzy match against known teams
        if home_norm:
            if home_norm.lower() not in team_lookup:
                if home_norm in fuzzy_cache:
                    home_norm = fuzzy_cache[home_norm]
                else:
                    matched = fuzzy_match_team(home_norm, team_lookup)
                    fuzzy_cache[home_norm] = matched
                    home_norm = matched

        if away_norm:
            if away_norm.lower() not in team_lookup:
                if away_norm in fuzzy_cache:
                    away_norm = fuzzy_cache[away_norm]
                else:
                    matched = fuzzy_match_team(away_norm, team_lookup)
                    fuzzy_cache[away_norm] = matched
                    away_norm = matched

        game_id = None
        home_team_id = None
        away_team_id = None

        if home_norm and away_norm:
            key = (home_norm.lower(), away_norm.lower())
            candidates = schedule_lookup.get(key, [])

            # Parse commence_time to date
            target_date = None
            ct = row.get("commence_time")
            if ct:
                try:
                    target_date = datetime.fromisoformat(ct.replace("Z", "+00:00")).date()
                except (ValueError, AttributeError):
                    pass

            if target_date and candidates:
                match = find_closest_game(candidates, target_date, max_days=1)
                if match:
                    game_id, home_team_id, away_team_id = match

        results.append({
            "staging_id": row["staging_id"],
            "game_id": game_id,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        })

    return pd.DataFrame(results)


def apply_updates(engine, updates_df: pd.DataFrame) -> int:
    """Update ncaab_raw_game_lines with matched game_id, home_team_id, away_team_id."""
    matched = updates_df.dropna(subset=["game_id"])
    if matched.empty:
        return 0

    rows = [
        (int(r["staging_id"]), int(r["game_id"]),
         int(r["home_team_id"]) if r["home_team_id"] else None,
         int(r["away_team_id"]) if r["away_team_id"] else None)
        for _, r in matched.iterrows()
    ]

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE _ncaab_line_updates (
                    staging_id BIGINT,
                    game_id BIGINT,
                    home_team_id INTEGER,
                    away_team_id INTEGER
                )
            """)
            extras.execute_values(
                cur,
                "INSERT INTO _ncaab_line_updates VALUES %s",
                rows,
            )
            cur.execute("""
                UPDATE ncaab_raw_game_lines r
                SET game_id = u.game_id,
                    home_team_id = u.home_team_id,
                    away_team_id = u.away_team_id
                FROM _ncaab_line_updates u
                WHERE r.staging_id = u.staging_id
                  AND r.game_id IS NULL
            """)
            updated = cur.rowcount
            cur.execute("DROP TABLE _ncaab_line_updates")
        conn.commit()
    finally:
        conn.close()

    return updated


def run_status(engine):
    """Show linking status."""
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM ncaab_raw_game_lines")).scalar()
        linked = conn.execute(text("SELECT COUNT(*) FROM ncaab_raw_game_lines WHERE game_id IS NOT NULL")).scalar()
        unlinked = total - linked

    logger.info(f"Total: {total:,}")
    logger.info(f"Linked: {linked:,} ({100*linked/total:.1f}%)" if total > 0 else "Linked: 0")
    logger.info(f"Unlinked: {unlinked:,}")


def run_link(engine, mode: str = "incremental"):
    """Run the linker in incremental or backfill mode."""
    schedule_lookup = build_schedule_lookup(engine)
    team_lookup = build_team_name_lookup(engine)
    fuzzy_cache = {}

    where = "WHERE game_id IS NULL"
    limit = f"LIMIT {LINKER_BATCH_SIZE}" if mode == "incremental" else ""

    with engine.connect() as conn:
        batch = pd.read_sql(
            f"SELECT staging_id, home_team, away_team, commence_time FROM ncaab_raw_game_lines {where} {limit}",
            conn,
        )

    if batch.empty:
        logger.info("No unlinked rows.")
        return

    logger.info(f"Processing {len(batch):,} unlinked rows...")
    updates = match_batch(batch, schedule_lookup, team_lookup, fuzzy_cache)
    matched_count = updates["game_id"].notna().sum()
    logger.info(f"Matched: {matched_count:,} / {len(batch):,}")

    if matched_count > 0:
        updated = apply_updates(engine, updates)
        logger.info(f"Updated {updated:,} rows in DB")

    # Log unmatched teams for alias building
    if fuzzy_cache:
        unmatched = {k: v for k, v in fuzzy_cache.items() if v is None}
        if unmatched:
            logger.warning(f"Unmatched teams ({len(unmatched)}): {list(unmatched.keys())[:20]}")


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)

    parser = argparse.ArgumentParser(description="NCAAB Game Lines Linker")
    parser.add_argument("mode", choices=["status", "incremental", "backfill"],
                        help="Operating mode")

    args = parser.parse_args()

    if args.mode == "status":
        run_status(engine)
    else:
        run_link(engine, args.mode)
