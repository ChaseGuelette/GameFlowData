"""
Incremental Rolling Average Stats Update

Lightweight version of populate_average_stats.py for daily cron jobs.
Only updates rows for games that occurred on the specified date.

Key optimizations:
  - Only fetches recent games (last 20 per player) instead of full history
  - Only calculates averages for players who played on target date
  - Uses UPSERT instead of TRUNCATE + reload

Usage:
  python populate_average_stats_incremental.py                    # Today's games
  python populate_average_stats_incremental.py --date 2026-02-09  # Specific date
"""

import argparse
import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine

# ============================================================================
# CONFIGURATION
# ============================================================================

WINDOWS = {
    "l5": 5,
    "l15": 15,
    "szn": None,  # Expanding window
}

# B3 uses L3 for specific stats only
L3_STATS = ["min", "pts", "reb", "ast", "fg3m"]

# How many prior games to fetch per player (must be > max window size)
LOOKBACK_GAMES = 20

# Stats to compute
PLAYER_BASIC_STATS = [
    "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
    "ftm", "fta", "ft_pct", "oreb", "dreb", "reb", "ast",
    "stl", "blk", "tov", "pf", "pts", "plus_minus",
]

B3_B4_STATS = ["min", "pts", "reb", "ast", "fg3m"]
STARTER_MINUTES_THRESHOLD = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# DATA FETCHING
# ============================================================================

def get_players_with_games_on_date(engine, target_date: date) -> list[int]:
    """Get list of player IDs who played on the target date."""
    query = text("""
        SELECT DISTINCT player_id
        FROM player_game_stats
        WHERE game_date::date = :target_date
          AND (did_not_play = false OR did_not_play IS NULL)
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"target_date": target_date})
        player_ids = [row[0] for row in result]

    logger.info(f"Found {len(player_ids)} players with games on {target_date}")
    return player_ids


def fetch_recent_games_for_players(engine, player_ids: list[int], target_date: date) -> pd.DataFrame:
    """
    Fetch recent games for specific players.

    Gets last LOOKBACK_GAMES games up to and including target_date.
    """
    if not player_ids:
        return pd.DataFrame()

    # Convert to tuple for SQL IN clause
    player_tuple = tuple(player_ids)

    query = f"""
        WITH ranked_games AS (
            SELECT
                player_id, game_id, season_id, game_date::date as game_date, team_id,
                min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                ftm, fta, ft_pct, oreb, dreb, reb, ast,
                stl, blk, tov, pf, pts, plus_minus,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id
                    ORDER BY game_date DESC
                ) as rn
            FROM player_game_stats
            WHERE player_id IN {player_tuple}
              AND game_date::date <= :target_date
              AND (did_not_play = false OR did_not_play IS NULL)
        )
        SELECT * FROM ranked_games
        WHERE rn <= {LOOKBACK_GAMES}
        ORDER BY player_id, game_date
    """

    logger.info(f"Fetching last {LOOKBACK_GAMES} games for {len(player_ids)} players...")
    df = pd.read_sql(text(query), engine, params={"target_date": target_date})
    logger.info(f"Fetched {len(df):,} game rows")

    return df


# ============================================================================
# ROLLING CALCULATIONS
# ============================================================================

def calculate_rolling_for_player(player_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling averages for a single player's games."""
    player_df = player_df.sort_values("game_date").copy()

    # Game number within data (for window sizing)
    player_df["game_number"] = range(1, len(player_df) + 1)
    player_df["games_l5"] = (player_df["game_number"] - 1).clip(upper=5)
    player_df["games_l15"] = (player_df["game_number"] - 1).clip(upper=15)
    player_df["games_szn"] = player_df["game_number"] - 1

    # Rolling averages for each stat (L5, L15, szn)
    for stat in PLAYER_BASIC_STATS:
        if stat not in player_df.columns:
            continue

        shifted = player_df[stat].shift(1)  # Only prior games

        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{stat}_{window_name}"

            if window_size is None:
                # Expanding (season-to-date)
                player_df[col_name] = shifted.expanding(min_periods=1).mean()
            else:
                player_df[col_name] = shifted.rolling(window=window_size, min_periods=1).mean()

    # B3: L3 averages (only for specific stats)
    for stat in L3_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        player_df[f"avg_{stat}_l3"] = shifted.rolling(window=3, min_periods=1).mean()

    # B3/B4: L5 standard deviations
    for stat in B3_B4_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        player_df[f"std_{stat}_l5"] = shifted.rolling(window=5, min_periods=2).std()

    # B4: Minutes floor (L5 min)
    if "min" in player_df.columns:
        shifted_min = player_df["min"].shift(1)
        player_df["min_floor_l5"] = shifted_min.rolling(window=5, min_periods=1).min()

    # B4: Games started L5
    if "min" in player_df.columns:
        is_starter = (player_df["min"] >= STARTER_MINUTES_THRESHOLD).astype(float)
        shifted_starter = is_starter.shift(1)
        player_df["games_started_l5"] = shifted_starter.rolling(window=5, min_periods=1).sum()

    # B2: Rest days
    game_dates = pd.to_datetime(player_df["game_date"])
    prev_date = game_dates.shift(1)
    player_df["rest_days"] = (game_dates - prev_date).dt.days.clip(0, 7).fillna(3).astype(int)

    # B2: Games in last 7 days
    player_df["games_last_7d"] = 0
    dates = game_dates.values
    for i in range(len(dates)):
        count = 0
        for j in range(i - 1, -1, -1):
            diff = (dates[i] - dates[j]) / pd.Timedelta(days=1)
            if diff <= 7:
                count += 1
            else:
                break
        player_df.iloc[i, player_df.columns.get_loc("games_last_7d")] = count

    return player_df


def calculate_incremental_averages(df: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Calculate rolling averages for all players, return only target date rows."""
    logger.info("Calculating rolling averages...")

    results = []
    player_ids = df["player_id"].unique()

    for i, player_id in enumerate(player_ids):
        player_df = df[df["player_id"] == player_id].copy()
        player_df = calculate_rolling_for_player(player_df)

        # Only keep the target date row
        target_row = player_df[player_df["game_date"] == target_date]
        if not target_row.empty:
            results.append(target_row)

        if (i + 1) % 50 == 0:
            logger.info(f"Processed {i + 1}/{len(player_ids)} players")

    if results:
        result_df = pd.concat(results, ignore_index=True)
        logger.info(f"Generated {len(result_df)} rows for {target_date}")
        return result_df
    else:
        return pd.DataFrame()


# ============================================================================
# DATABASE UPDATE
# ============================================================================

def upsert_player_averages(engine, df: pd.DataFrame):
    """Upsert player average rows into database."""
    if df.empty:
        logger.info("No rows to upsert")
        return

    logger.info(f"Upserting {len(df)} rows...")

    # Build column list
    base_cols = ["player_id", "game_id", "season_id", "game_date", "team_id",
                 "game_number", "games_l5", "games_l15", "games_szn"]

    avg_cols = [c for c in df.columns if c.startswith("avg_")]
    std_cols = [c for c in df.columns if c.startswith("std_")]
    extra_cols = ["min_floor_l5", "games_started_l5", "rest_days", "games_last_7d"]
    extra_cols = [c for c in extra_cols if c in df.columns]

    all_cols = base_cols + avg_cols + std_cols + extra_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    # Round numeric columns
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)
        elif col == "min_floor_l5":
            insert_df[col] = insert_df[col].round(2)

    # Build upsert query
    cols = list(insert_df.columns)
    col_list = ", ".join(cols)
    placeholder_list = ", ".join([f":{c}" for c in cols])
    update_list = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in ["player_id", "game_id"]])

    upsert_sql = f"""
        INSERT INTO player_average_game_stats ({col_list})
        VALUES ({placeholder_list})
        ON CONFLICT (player_id, game_id)
        DO UPDATE SET {update_list}
    """

    with engine.begin() as conn:
        for _, row in insert_df.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
            conn.execute(text(upsert_sql), params)

    logger.info(f"Upserted {len(insert_df)} rows successfully")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Incremental rolling average stats update")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    engine = get_engine()

    logger.info("=" * 60)
    logger.info(f"Incremental average stats update for {target_date}")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        # Step 1: Find players who played on target date
        player_ids = get_players_with_games_on_date(engine, target_date)

        if not player_ids:
            logger.info("No players found for target date. Nothing to update.")
            return

        # Step 2: Fetch recent games for those players
        df = fetch_recent_games_for_players(engine, player_ids, target_date)

        if df.empty:
            logger.info("No game data found. Nothing to update.")
            return

        # Step 3: Calculate rolling averages
        result_df = calculate_incremental_averages(df, target_date)

        # Step 4: Upsert to database
        upsert_player_averages(engine, result_df)

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"Completed in {elapsed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
