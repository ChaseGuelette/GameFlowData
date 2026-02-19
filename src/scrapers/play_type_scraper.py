#!/usr/bin/env python3
"""
NBA Play Type Scraper (Synergy Data)
=====================================
Fetches team-level offensive and defensive play type data from the NBA API
SynergyPlayTypes endpoint.

Tables updated:
- team_play_types: Season-level play type frequency and efficiency data

Usage:
    python src/scrapers/play_type_scraper.py [--season YYYY-YY]

Examples:
    python src/scrapers/play_type_scraper.py
    python src/scrapers/play_type_scraper.py --season 2024-25
"""

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from nba_api.stats.endpoints import synergyplaytypes
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from sqlalchemy import create_engine, text

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env", override=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

DEFAULT_SEASON = "2025-26"
DELAY_MIN = 0.6
DELAY_MAX = 1.2
MAX_RETRIES = 3
BAN_COOLDOWN = 300  # 5 minutes

PLAY_TYPES = [
    "Isolation",
    "Transition",
    "PRBallHandler",
    "PRRollman",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
]
TYPE_GROUPINGS = ["Offensive", "Defensive"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------
def fetch_play_type(
    season: str, play_type: str, grouping: str
) -> pd.DataFrame | None:
    """Fetch a single play type + grouping combo with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            spt = synergyplaytypes.SynergyPlayTypes(
                per_mode_simple="PerGame",
                league_id="00",
                season=season,
                season_type_all_star="Regular Season",
                type_grouping_nullable=grouping,
                player_or_team_abbreviation="T",
                play_type_nullable=play_type,
            )
            df = spt.get_data_frames()[0]
            return df
        except (
            ReadTimeout,
            ConnectionError,
            ConnectionResetError,
            ChunkedEncodingError,
        ) as e:
            logger.warning(f"  Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                cooldown = BAN_COOLDOWN if "blocked" in str(e).lower() else 10
                time.sleep(cooldown)
        except Exception as e:
            logger.error(f"  Unexpected error (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5)
    return None


def scrape_all_play_types(season: str) -> pd.DataFrame:
    """Fetch all 22 combinations (11 play types x 2 groupings)."""
    all_frames = []
    total = len(PLAY_TYPES) * len(TYPE_GROUPINGS)
    i = 0

    for grouping in TYPE_GROUPINGS:
        for play_type in PLAY_TYPES:
            i += 1
            logger.info(f"[{i}/{total}] {grouping} — {play_type}")
            df = fetch_play_type(season, play_type, grouping)
            if df is not None and not df.empty:
                all_frames.append(df)
                logger.info(f"  -> {len(df)} rows")
            else:
                logger.warning("  -> No data returned")

            delay = round(random.uniform(DELAY_MIN, DELAY_MAX), 2)
            time.sleep(delay)

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_COLUMNS = [
    "season_id",
    "team_id",
    "team_abbreviation",
    "team_name",
    "play_type",
    "type_grouping",
    "percentile",
    "gp",
    "poss_pct",
    "ppp",
    "fg_pct",
    "ft_poss_pct",
    "tov_poss_pct",
    "sf_poss_pct",
    "plusone_poss_pct",
    "score_poss_pct",
    "efg_pct",
    "poss",
    "pts",
    "fgm",
    "fga",
    "fgmx",
]


def save_to_db(eng, df: pd.DataFrame) -> None:
    """Full-refresh save: delete existing season rows, insert fresh data."""
    # Normalize column names to snake_case
    df = df.copy()
    df.columns = df.columns.str.lower()

    # Keep only the columns we need
    final_df = df[[c for c in DB_COLUMNS if c in df.columns]]

    season_id = final_df["season_id"].iloc[0]
    logger.info(f"Saving {len(final_df)} rows for season {season_id}")

    with eng.begin() as conn:
        conn.execute(
            text("DELETE FROM team_play_types WHERE season_id = :sid"),
            {"sid": season_id},
        )
        final_df.to_sql("team_play_types", conn, if_exists="append", index=False)

    logger.info("Save complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA Play Type Scraper (Synergy)")
    parser.add_argument(
        "--season",
        type=str,
        default=DEFAULT_SEASON,
        help=f"Season to scrape (default: {DEFAULT_SEASON})",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("NBA PLAY TYPE SCRAPER")
    logger.info(f"Season: {args.season}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    if engine is None:
        logger.error("DATABASE_URL not set. Exiting.")
        sys.exit(1)

    df = scrape_all_play_types(args.season)

    if df.empty:
        logger.error("No data fetched!")
        sys.exit(1)

    logger.info(f"Total rows fetched: {len(df)}")
    save_to_db(engine, df)

    logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
