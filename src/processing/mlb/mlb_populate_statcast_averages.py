"""
MLB Populate Statcast Rolling Averages (Full Backfill)
======================================================
Calculates rolling averages for Statcast batting and pitching metrics,
then TRUNCATE + reload into mlb_player_average_statcast_batting and
mlb_player_average_statcast_pitching.

Pattern: mlb_populate_averages.py (boxscore equivalent).

Key design decisions:
  - shift(1) on every stat so game N's average uses only games 1..N-1
  - Batting uses L5/L10/L20/SZN windows (same as boxscore batting)
  - Pitching uses L3/L5/SZN windows (same as boxscore pitching)
  - Percentage stats (barrel_pct, xba, etc.) are already per-game rates,
    so simple rolling mean is correct (unlike boxscore rate stats)

Usage:
    python -m src.processing.mlb.mlb_populate_statcast_averages --table all
    python -m src.processing.mlb.mlb_populate_statcast_averages --table batting
    python -m src.processing.mlb.mlb_populate_statcast_averages --table pitching
    python -m src.processing.mlb.mlb_populate_statcast_averages --table all --season 2024
"""

import argparse
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.processing.mlb.mlb_config import BATCH_SIZE, BATTING_WINDOWS, PITCHING_WINDOWS
from src.processing.populate_average_stats import rolling_with_groupby

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# STAT DEFINITIONS
# ============================================================================

# Statcast batting: stats that get all 4 windows (L5/L10/L20/SZN)
STATCAST_BAT_FULL_WINDOWS = [
    "avg_exit_velocity", "max_exit_velocity", "avg_launch_angle",
    "barrel_pct", "hard_hit_pct", "sweet_spot_pct",
    "xba", "xslg", "xwoba", "woba",
]

# Statcast batting: stats with reduced windows (L10/SZN only — less volatile)
STATCAST_BAT_REDUCED_WINDOWS = [
    "gb_pct", "fb_pct", "ld_pct", "popup_pct",
    "pull_pct", "center_pct", "oppo_pct",
]

# Statcast batting: plate discipline (L5/L10/SZN)
STATCAST_BAT_DISCIPLINE = ["zone_pct", "chase_pct", "whiff_pct"]

# Statcast batting: volume (L5/L10/SZN)
STATCAST_BAT_VOLUME = ["batted_balls", "total_pitches_seen"]

# Statcast batting: std devs at L5
STATCAST_BAT_STD_STATS = ["avg_exit_velocity", "barrel_pct", "xwoba", "hard_hit_pct"]

# Statcast pitching: contact quality against (L3/L5/SZN)
STATCAST_PITCH_CONTACT = [
    "avg_exit_velocity_against", "max_exit_velocity_against",
    "barrel_pct_against", "hard_hit_pct_against",
]

# Statcast pitching: expected stats (L3/L5/SZN)
STATCAST_PITCH_EXPECTED = [
    "xba_against", "xslg_against", "xwoba_against", "xera",
]

# Statcast pitching: velocity/spin (L3/L5/SZN)
STATCAST_PITCH_VELO = [
    "avg_fastball_velo", "max_fastball_velo",
    "avg_fastball_spin", "avg_breaking_spin",
]

# Statcast pitching: pitch mix (L5/SZN only)
STATCAST_PITCH_MIX = ["fastball_pct", "breaking_pct", "offspeed_pct"]

# Statcast pitching: batted ball distribution (L5/SZN)
STATCAST_PITCH_BATTED_BALL = ["gb_pct", "fb_pct", "ld_pct", "popup_pct"]

# Statcast pitching: discipline (L3/L5/SZN)
STATCAST_PITCH_DISCIPLINE = ["zone_pct", "chase_pct", "whiff_pct", "csw_pct"]

# Statcast pitching: volume (L3/L5/SZN)
STATCAST_PITCH_VOLUME = ["total_pitches"]

# Statcast pitching: std devs at L3
STATCAST_PITCH_STD_STATS = ["avg_exit_velocity_against", "xera", "whiff_pct", "avg_fastball_velo"]


# ============================================================================
# DATA FETCHING
# ============================================================================


def _get_seasons(engine, table: str) -> list[int]:
    """Return sorted list of distinct seasons in a statcast table."""
    query = f"SELECT DISTINCT EXTRACT(YEAR FROM game_date)::int AS season FROM {table} ORDER BY season"
    df = pd.read_sql(query, engine)
    return df["season"].tolist()


def fetch_statcast_batting(engine, season: int | None = None) -> pd.DataFrame:
    """Fetch statcast batting stats, season-by-season."""
    base = """
        SELECT
            player_id, game_date::date AS game_date,
            EXTRACT(YEAR FROM game_date)::int AS season,
            avg_exit_velocity, max_exit_velocity, avg_launch_angle,
            barrel_pct, hard_hit_pct, sweet_spot_pct,
            xba, xslg, xwoba, woba,
            gb_pct, fb_pct, ld_pct, popup_pct,
            pull_pct, center_pct, oppo_pct,
            zone_pct, chase_pct, whiff_pct,
            batted_balls, total_pitches_seen
        FROM mlb_player_game_statcast_batting
        WHERE batted_balls > 0
    """
    seasons = [season] if season else _get_seasons(engine, "mlb_player_game_statcast_batting")
    logger.info(f"Fetching statcast batting for {len(seasons)} season(s): {seasons}")

    chunks = []
    for s in seasons:
        query = base + f" AND EXTRACT(YEAR FROM game_date) = {int(s)} ORDER BY player_id, game_date"
        df_s = pd.read_sql(query, engine)
        logger.info(f"  Season {s}: {len(df_s):,} rows")
        chunks.append(df_s)

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    logger.info(f"Fetched {len(df):,} statcast batting rows total")
    return df


def fetch_statcast_pitching(engine, season: int | None = None) -> pd.DataFrame:
    """Fetch statcast pitching stats, season-by-season."""
    base = """
        SELECT
            player_id, game_date::date AS game_date,
            EXTRACT(YEAR FROM game_date)::int AS season,
            avg_exit_velocity_against, max_exit_velocity_against,
            barrel_pct_against, hard_hit_pct_against,
            xba_against, xslg_against, xwoba_against, xera,
            avg_fastball_velo, max_fastball_velo,
            avg_fastball_spin, avg_breaking_spin,
            fastball_pct, breaking_pct, offspeed_pct,
            gb_pct, fb_pct, ld_pct, popup_pct,
            zone_pct, chase_pct, whiff_pct, csw_pct,
            total_pitches
        FROM mlb_player_game_statcast_pitching
        WHERE total_pitches > 0
    """
    seasons = [season] if season else _get_seasons(engine, "mlb_player_game_statcast_pitching")
    logger.info(f"Fetching statcast pitching for {len(seasons)} season(s): {seasons}")

    chunks = []
    for s in seasons:
        query = base + f" AND EXTRACT(YEAR FROM game_date) = {int(s)} ORDER BY player_id, game_date"
        df_s = pd.read_sql(query, engine)
        logger.info(f"  Season {s}: {len(df_s):,} rows")
        chunks.append(df_s)

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    logger.info(f"Fetched {len(df):,} statcast pitching rows total")
    return df


# ============================================================================
# BATTING ROLLING AVERAGES
# ============================================================================


def calculate_statcast_batting_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling statcast batting averages and std devs."""
    logger.info("Calculating statcast batting averages...")

    group_cols = ["player_id", "season"]
    df = df.sort_values(group_cols + ["game_date"]).copy()
    group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)

    df["game_number"] = df.groupby(group_cols).cumcount() + 1

    # Full windows (L5/L10/L20/SZN)
    for stat in STATCAST_BAT_FULL_WINDOWS:
        if stat not in df.columns:
            logger.warning(f"Column {stat} not found, skipping")
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in BATTING_WINDOWS.items():
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Reduced windows (L10/SZN only)
    for stat in STATCAST_BAT_REDUCED_WINDOWS:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in [("l10", 10), ("szn", None)]:
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Discipline (L5/L10/SZN)
    for stat in STATCAST_BAT_DISCIPLINE:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in [("l5", 5), ("l10", 10), ("szn", None)]:
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Volume (L5/L10/SZN)
    for stat in STATCAST_BAT_VOLUME:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in [("l5", 5), ("l10", 10), ("szn", None)]:
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Std devs at L5
    for stat in STATCAST_BAT_STD_STATS:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        df[f"std_{stat}_l5"] = rolling_with_groupby(shifted, group_key, window=5, min_periods=2, agg="std")

    logger.info("Statcast batting averages calculated")
    return df


# ============================================================================
# PITCHING ROLLING AVERAGES
# ============================================================================


def calculate_statcast_pitching_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling statcast pitching averages and std devs."""
    logger.info("Calculating statcast pitching averages...")

    group_cols = ["player_id", "season"]
    df = df.sort_values(group_cols + ["game_date"]).copy()
    group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)

    df["game_number"] = df.groupby(group_cols).cumcount() + 1

    # Contact quality, expected stats, velocity/spin (L3/L5/SZN)
    for stat in STATCAST_PITCH_CONTACT + STATCAST_PITCH_EXPECTED + STATCAST_PITCH_VELO:
        if stat not in df.columns:
            logger.warning(f"Column {stat} not found, skipping")
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in PITCHING_WINDOWS.items():
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Pitch mix (L5/SZN only)
    for stat in STATCAST_PITCH_MIX:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in [("l5", 5), ("szn", None)]:
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Batted ball distribution (L5/SZN)
    for stat in STATCAST_PITCH_BATTED_BALL:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in [("l5", 5), ("szn", None)]:
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Discipline (L3/L5/SZN)
    for stat in STATCAST_PITCH_DISCIPLINE:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in PITCHING_WINDOWS.items():
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Volume (L3/L5/SZN)
    for stat in STATCAST_PITCH_VOLUME:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in PITCHING_WINDOWS.items():
            df[f"avg_{stat}_{wname}"] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Std devs at L3
    for stat in STATCAST_PITCH_STD_STATS:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        df[f"std_{stat}_l3"] = rolling_with_groupby(shifted, group_key, window=3, min_periods=2, agg="std")

    logger.info("Statcast pitching averages calculated")
    return df


# ============================================================================
# DATABASE INSERT
# ============================================================================


def _is_rolling_col(col: str) -> bool:
    """Check if column is a computed rolling average/std (has window suffix)."""
    suffixes = ("_l3", "_l5", "_l10", "_l20", "_szn")
    return any(col.endswith(s) for s in suffixes)


def insert_statcast_batting_averages(engine, df: pd.DataFrame):
    """TRUNCATE + batch insert into mlb_player_average_statcast_batting."""
    logger.info("Inserting statcast batting averages...")

    # Only include computed rolling columns (with window suffix), not raw source columns
    base_cols = ["player_id", "game_date", "season", "game_number"]
    avg_cols = [c for c in df.columns if _is_rolling_col(c)]
    columns = base_cols + sorted(avg_cols)

    insert_df = df[[c for c in columns if c in df.columns]].copy()

    # Round numerics
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE mlb_player_average_statcast_batting"))

        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i : i + BATCH_SIZE]
            batch.to_sql("mlb_player_average_statcast_batting", conn, if_exists="append", index=False, method="multi")
            if (i // BATCH_SIZE + 1) % 50 == 0 or i + BATCH_SIZE >= len(insert_df):
                logger.info(f"Inserted {min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,} statcast batting rows")

    logger.info(f"Inserted {len(insert_df):,} statcast batting average rows total")


def insert_statcast_pitching_averages(engine, df: pd.DataFrame):
    """TRUNCATE + batch insert into mlb_player_average_statcast_pitching."""
    logger.info("Inserting statcast pitching averages...")

    # Only include computed rolling columns (with window suffix), not raw source columns
    base_cols = ["player_id", "game_date", "season", "game_number"]
    avg_cols = [c for c in df.columns if _is_rolling_col(c)]
    columns = base_cols + sorted(avg_cols)

    insert_df = df[[c for c in columns if c in df.columns]].copy()

    # Round numerics
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE mlb_player_average_statcast_pitching"))

        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i : i + BATCH_SIZE]
            batch.to_sql("mlb_player_average_statcast_pitching", conn, if_exists="append", index=False, method="multi")
            if (i // BATCH_SIZE + 1) % 50 == 0 or i + BATCH_SIZE >= len(insert_df):
                logger.info(f"Inserted {min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,} statcast pitching rows")

    logger.info(f"Inserted {len(insert_df):,} statcast pitching average rows total")


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="MLB Populate Statcast Rolling Averages (Full Backfill)")
    parser.add_argument(
        "--table",
        choices=["batting", "pitching", "all"],
        default="all",
        help="Which table(s) to populate",
    )
    parser.add_argument("--season", type=int, default=None, help="Specific season year (e.g. 2024)")
    args = parser.parse_args()

    engine = get_engine()

    logger.info("=" * 60)
    logger.info("MLB STATCAST ROLLING AVERAGE POPULATION (FULL BACKFILL)")
    logger.info(f"Table: {args.table}")
    logger.info(f"Season: {args.season or 'ALL'}")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        if args.table in ("batting", "all"):
            df = fetch_statcast_batting(engine, args.season)
            if not df.empty:
                df = calculate_statcast_batting_averages(df)
                insert_statcast_batting_averages(engine, df)
            del df

        if args.table in ("pitching", "all"):
            df = fetch_statcast_pitching(engine, args.season)
            if not df.empty:
                df = calculate_statcast_pitching_averages(df)
                insert_statcast_pitching_averages(engine, df)
            del df

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"Completed in {elapsed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
