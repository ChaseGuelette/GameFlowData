"""
NCAAB Team Rolling Averages
=============================
Computes shift(1) rolling averages for all teams using ncaab_team_box_scores.
Populates ncaab_team_rolling_averages.

Pattern: src/processing/mlb/mlb_populate_averages.py
Key difference: group by (team_id, season), not (player_id, season).

Usage:
    python -m src.processing.ncaab.ncaab_populate_averages --all
    python -m src.processing.ncaab.ncaab_populate_averages --season 2025
    python -m src.processing.ncaab.ncaab_populate_averages --incremental
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.processing.ncaab.ncaab_config import (
    BATCH_SIZE,
    ROLLING_WINDOWS,
    STD_STATS,
    TEAM_BOX_STATS,
    TEAM_OPP_STATS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def fetch_team_box_scores(engine, season: int | None = None) -> pd.DataFrame:
    """Fetch team box scores for rolling average computation."""
    cols = ", ".join(["team_id", "game_id", "game_date", "season"] + TEAM_BOX_STATS + TEAM_OPP_STATS)
    query = f"""
        SELECT {cols}
        FROM ncaab_team_box_scores
        {"WHERE season = " + str(int(season)) if season else ""}
        ORDER BY team_id, game_date
    """
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df):,} box score rows")

    # Ensure numeric
    for col in TEAM_BOX_STATS + TEAM_OPP_STATS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def compute_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Compute shift(1) rolling averages for each team within each season.

    All stats use shift(1) so game N's average only uses games 1..N-1.
    """
    if df.empty:
        return df

    df = df.sort_values(["team_id", "season", "game_date"]).reset_index(drop=True)

    all_stats = TEAM_BOX_STATS + TEAM_OPP_STATS
    result_cols = {}

    # Game number within season
    df["game_number"] = df.groupby(["team_id", "season"]).cumcount() + 1

    # Shift all stats by 1 within group (prevents leakage)
    for stat in all_stats:
        if stat in df.columns:
            df[f"_shifted_{stat}"] = df.groupby(["team_id", "season"])[stat].shift(1)

    # Rolling windows
    for window_name, window_size in ROLLING_WINDOWS.items():
        for stat in all_stats:
            shifted_col = f"_shifted_{stat}"
            if shifted_col not in df.columns:
                continue

            out_col = f"avg_{stat}_{window_name}"

            if window_size is None:
                # Expanding (season-to-date) average
                result_cols[out_col] = df.groupby(["team_id", "season"])[shifted_col].transform(
                    lambda x: x.expanding(min_periods=1).mean()
                )
            else:
                result_cols[out_col] = df.groupby(["team_id", "season"])[shifted_col].transform(
                    lambda x: x.rolling(window_size, min_periods=1).mean()
                )

    # Window game counts
    for window_name, window_size in ROLLING_WINDOWS.items():
        col_name = f"games_{window_name}"
        if window_size is None:
            result_cols[col_name] = (df["game_number"] - 1).clip(lower=0)
        else:
            result_cols[col_name] = (df["game_number"] - 1).clip(lower=0, upper=window_size)

    # Standard deviations at L5
    for stat in STD_STATS:
        shifted_col = f"_shifted_{stat}"
        if shifted_col in df.columns:
            result_cols[f"std_{stat}_l5"] = df.groupby(["team_id", "season"])[shifted_col].transform(
                lambda x: x.rolling(5, min_periods=2).std()
            )

    # Rest days
    df["_prev_date"] = df.groupby(["team_id", "season"])["game_date"].shift(1)
    result_cols["rest_days"] = (df["game_date"] - df["_prev_date"]).dt.days.clip(0, 14).fillna(4).astype(int)

    # Games in last 7 days
    def _games_last_7d(group):
        dates = group["game_date"].values
        counts = []
        for i, d in enumerate(dates):
            cutoff = d - pd.Timedelta(days=7)
            count = sum(1 for j in range(i) if dates[j] >= cutoff)
            counts.append(count)
        return pd.Series(counts, index=group.index)

    result_cols["games_last_7d"] = df.groupby(["team_id", "season"]).apply(
        _games_last_7d
    ).droplevel(0).reindex(df.index).astype(int)

    # Merge all result columns
    for col, values in result_cols.items():
        df[col] = values

    # Drop temp columns
    temp_cols = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=temp_cols)

    return df


def store_averages(engine, df: pd.DataFrame, truncate: bool = True) -> int:
    """Store rolling averages into ncaab_team_rolling_averages."""
    if df.empty:
        return 0

    # Identify which columns exist in both df and the target table
    # Use the columns that start with avg_, std_, games_, rest_, or are identity cols
    identity_cols = ["team_id", "game_id", "game_date", "season", "game_number"]
    avg_cols = [c for c in df.columns if c.startswith("avg_") or c.startswith("std_") or c.startswith("games_")]
    context_cols = ["rest_days", "games_last_7d"]

    all_cols = identity_cols + avg_cols + context_cols
    # Filter to columns that actually exist in df
    all_cols = [c for c in all_cols if c in df.columns]

    insert_df = df[all_cols].copy()
    insert_df["game_date"] = insert_df["game_date"].dt.date

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            if truncate:
                cur.execute("TRUNCATE ncaab_team_rolling_averages")
                logger.info("Truncated ncaab_team_rolling_averages")

            col_names = ", ".join(all_cols)

            rows = [tuple(row) for row in insert_df.itertuples(index=False, name=None)]

            # Batch insert
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                extras.execute_values(
                    cur,
                    f"INSERT INTO ncaab_team_rolling_averages ({col_names}) VALUES %s",
                    batch,
                )
                if (i + BATCH_SIZE) % 10000 < BATCH_SIZE:
                    logger.info(f"  Inserted {min(i + BATCH_SIZE, len(rows)):,} / {len(rows):,}")

        conn.commit()
    finally:
        conn.close()

    logger.info(f"Stored {len(rows):,} rolling average rows")
    return len(rows)


def run_incremental(engine) -> None:
    """Recompute rolling averages only for teams with new box scores."""
    with engine.connect() as conn:
        # Find teams with box scores newer than their latest rolling average
        result = conn.execute(text("""
            SELECT DISTINCT bs.team_id, bs.season
            FROM ncaab_team_box_scores bs
            WHERE NOT EXISTS (
                SELECT 1 FROM ncaab_team_rolling_averages ra
                WHERE ra.team_id = bs.team_id AND ra.game_id = bs.game_id
            )
        """))
        stale = result.fetchall()

    if not stale:
        logger.info("No stale teams — all rolling averages are current.")
        return

    team_ids = list(set(r[0] for r in stale))
    seasons = list(set(r[1] for r in stale))
    logger.info(f"Recomputing {len(team_ids)} teams across {len(seasons)} seasons")

    for season in sorted(seasons):
        df = fetch_team_box_scores(engine, season=season)
        if df.empty:
            continue

        # Filter to stale teams only
        season_team_ids = [tid for tid, s in stale if s == season]
        df = df[df["team_id"].isin(season_team_ids)]

        if df.empty:
            continue

        result = compute_rolling_averages(df)

        # Delete existing rows for these teams in this season, then insert
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ncaab_team_rolling_averages WHERE team_id = ANY(%s) AND season = %s",
                    (season_team_ids, season),
                )
            conn.commit()
        finally:
            conn.close()

        store_averages(engine, result, truncate=False)


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)

    parser = argparse.ArgumentParser(description="NCAAB Team Rolling Averages")
    parser.add_argument("--all", action="store_true", help="Full backfill (TRUNCATE + reload)")
    parser.add_argument("--season", type=int, help="Single season backfill")
    parser.add_argument("--incremental", action="store_true", help="Recompute only stale teams")

    args = parser.parse_args()

    if args.incremental:
        run_incremental(engine)
    elif args.season:
        df = fetch_team_box_scores(engine, season=args.season)
        result = compute_rolling_averages(df)
        store_averages(engine, result, truncate=True)
    elif args.all:
        df = fetch_team_box_scores(engine)
        result = compute_rolling_averages(df)
        store_averages(engine, result, truncate=True)
    else:
        logger.error("Specify --all, --season, or --incremental")
        sys.exit(1)
