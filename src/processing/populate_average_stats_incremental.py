"""
Incremental Rolling Average Stats Update

Lightweight version of populate_average_stats.py for daily cron jobs.
Updates all three rolling-average tables:
  - player_average_game_stats (basic box score + B2/B3/B4 features)
  - player_average_advanced_stats (advanced metrics)
  - team_average_game_stats (team basic + advanced)

Only updates rows for games that occurred on the specified date.
Fetches all games within the current season for correct SZN expanding averages.

Key optimizations vs full backfill:
  - Only processes entities (players/teams) with games on target date
  - Only keeps target-date rows for UPSERT
  - Uses UPSERT instead of TRUNCATE + reload

Usage:
  python populate_average_stats_incremental.py                    # Today's games
  python populate_average_stats_incremental.py --date 2026-02-09  # Specific date
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

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

# Stats to compute for player basic averages
PLAYER_BASIC_STATS = [
    "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
    "ftm", "fta", "ft_pct", "oreb", "dreb", "reb", "ast",
    "stl", "blk", "tov", "pf", "pts", "plus_minus",
]

B3_B4_STATS = ["min", "pts", "reb", "ast", "fg3m"]
STARTER_MINUTES_THRESHOLD = 20

# Player advanced stat mapping: source column → target name
PLAYER_ADVANCED_MAPPING = {
    "offensive_rating": "off_rtg",
    "defensive_rating": "def_rtg",
    "net_rating": "net_rtg",
    "true_shooting_percentage": "ts_pct",
    "effective_field_goal_percentage": "efg_pct",
    "usage_percentage": "usg_pct",
    "assist_ratio": "ast_ratio",
    "assist_percentage": "ast_pct",
    "assist_to_turnover": "ast_tov",
    "turnover_ratio": "tov_ratio",
    "rebound_percentage": "reb_pct",
    "offensive_rebound_percentage": "oreb_pct",
    "defensive_rebound_percentage": "dreb_pct",
    "pace": "pace",
    "possessions": "poss",
    "pie": "pie",
}

# Team stat lists
TEAM_BASIC_STATS = [
    "team_pts", "team_fgm", "team_fga", "team_fg_pct",
    "team_fg3m", "team_fg3a", "team_fg3_pct",
    "team_ftm", "team_fta", "team_ft_pct",
    "team_oreb", "team_dreb", "team_reb",
    "team_ast", "team_stl", "team_blk", "team_tov", "team_pf",
    "team_plus_minus",
]
TEAM_BASIC_MAPPING = {stat: stat.replace("team_", "") for stat in TEAM_BASIC_STATS}

TEAM_ADVANCED_MAPPING = {
    "offensive_rating": "off_rtg",
    "defensive_rating": "def_rtg",
    "net_rating": "net_rtg",
    "true_shooting_percentage": "ts_pct",
    "effective_field_goal_percentage": "efg_pct",
    "pace": "pace",
    "possessions": "poss",
    "assist_ratio": "ast_ratio",
    "turnover_ratio": "tov_ratio",
    "rebound_percentage": "reb_pct",
    "offensive_rebound_percentage": "oreb_pct",
    "defensive_rebound_percentage": "dreb_pct",
    "pie": "pie",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# DATA FETCHING
# ============================================================================

def get_season_id(engine, target_date: date) -> str | None:
    """Get the season_id for the target date."""
    query = text("""
        SELECT DISTINCT season_id
        FROM player_game_stats
        WHERE game_date::date = :target_date
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"target_date": target_date})
        row = result.fetchone()
    return row[0] if row else None


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


def get_teams_with_games_on_date(engine, target_date: date) -> list[int]:
    """Get list of team IDs that played on the target date."""
    query = text("""
        SELECT DISTINCT team_id
        FROM team_game_stats
        WHERE game_date::date = :target_date
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"target_date": target_date})
        team_ids = [row[0] for row in result]
    logger.info(f"Found {len(team_ids)} teams with games on {target_date}")
    return team_ids


def fetch_player_basic_season_games(
    engine, player_ids: list[int], season_id: str, target_date: date
) -> pd.DataFrame:
    """Fetch all season games for specific players (basic stats)."""
    if not player_ids:
        return pd.DataFrame()

    player_tuple = tuple(player_ids)
    query = f"""
        SELECT
            player_id, game_id, season_id, game_date::date as game_date, team_id,
            min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            ftm, fta, ft_pct, oreb, dreb, reb, ast,
            stl, blk, tov, pf, pts, plus_minus
        FROM player_game_stats
        WHERE player_id IN {player_tuple}
          AND season_id = :season_id
          AND game_date::date <= :target_date
          AND (did_not_play = false OR did_not_play IS NULL)
        ORDER BY player_id, game_date
    """
    logger.info(f"Fetching season {season_id} basic games for {len(player_ids)} players...")
    df = pd.read_sql(text(query), engine, params={"season_id": season_id, "target_date": target_date})
    logger.info(f"Fetched {len(df):,} player basic game rows")
    return df


def fetch_player_advanced_season_games(
    engine, player_ids: list[int], season_id: str, target_date: date
) -> pd.DataFrame:
    """Fetch all season games for specific players (advanced stats via join)."""
    if not player_ids:
        return pd.DataFrame()

    player_tuple = tuple(player_ids)
    adv_cols = ", ".join([f"adv.{col}" for col in PLAYER_ADVANCED_MAPPING.keys()])
    query = f"""
        SELECT
            pgs.player_id, pgs.game_id, pgs.season_id,
            pgs.game_date::date as game_date, pgs.team_id,
            {adv_cols}
        FROM player_game_stats pgs
        LEFT JOIN player_game_advanced_stats adv
            ON pgs.player_id = adv.player_id AND pgs.game_id = adv.game_id
        WHERE pgs.player_id IN {player_tuple}
          AND pgs.season_id = :season_id
          AND pgs.game_date::date <= :target_date
          AND (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
        ORDER BY pgs.player_id, pgs.game_date
    """
    logger.info(f"Fetching season {season_id} advanced games for {len(player_ids)} players...")
    df = pd.read_sql(text(query), engine, params={"season_id": season_id, "target_date": target_date})
    logger.info(f"Fetched {len(df):,} player advanced game rows")
    return df


def fetch_team_season_games(
    engine, team_ids: list[int], season_id: str, target_date: date
) -> pd.DataFrame:
    """Fetch all season games for specific teams."""
    if not team_ids:
        return pd.DataFrame()

    team_tuple = tuple(team_ids)
    basic_cols = ", ".join(TEAM_BASIC_STATS)
    adv_cols = ", ".join(TEAM_ADVANCED_MAPPING.keys())
    query = f"""
        SELECT
            team_id, game_id, season_id, game_date::date as game_date,
            {basic_cols}, {adv_cols}
        FROM team_game_stats
        WHERE team_id IN {team_tuple}
          AND season_id = :season_id
          AND game_date::date <= :target_date
        ORDER BY team_id, game_date
    """
    logger.info(f"Fetching season {season_id} games for {len(team_ids)} teams...")
    df = pd.read_sql(text(query), engine, params={"season_id": season_id, "target_date": target_date})
    logger.info(f"Fetched {len(df):,} team game rows")
    return df


# ============================================================================
# ROLLING CALCULATIONS — PLAYER BASIC
# ============================================================================

def calculate_basic_rolling_for_player(player_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling averages for a single player's basic stats."""
    player_df = player_df.sort_values("game_date").copy()

    # Game number within season (1-indexed)
    player_df["game_number"] = range(1, len(player_df) + 1)
    player_df["games_l5"] = (player_df["game_number"] - 1).clip(upper=5)
    player_df["games_l15"] = (player_df["game_number"] - 1).clip(upper=15)
    player_df["games_szn"] = player_df["game_number"] - 1

    # Rolling averages for each stat (L5, L15, szn)
    for stat in PLAYER_BASIC_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{stat}_{window_name}"
            if window_size is None:
                player_df[col_name] = shifted.expanding(min_periods=1).mean()
            else:
                player_df[col_name] = shifted.rolling(window=window_size, min_periods=1).mean()

    # B3: L3 averages
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


# ============================================================================
# ROLLING CALCULATIONS — PLAYER ADVANCED
# ============================================================================

def calculate_advanced_rolling_for_player(player_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling averages for a single player's advanced stats."""
    player_df = player_df.sort_values("game_date").copy()

    # Game number within season
    player_df["game_number"] = range(1, len(player_df) + 1)
    player_df["games_l5"] = (player_df["game_number"] - 1).clip(upper=5)
    player_df["games_l15"] = (player_df["game_number"] - 1).clip(upper=15)
    player_df["games_szn"] = player_df["game_number"] - 1

    for source_col, target_name in PLAYER_ADVANCED_MAPPING.items():
        if source_col not in player_df.columns:
            continue
        shifted = player_df[source_col].shift(1)
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            if window_size is None:
                player_df[col_name] = shifted.expanding(min_periods=1).mean()
            else:
                player_df[col_name] = shifted.rolling(window=window_size, min_periods=1).mean()

    return player_df


# ============================================================================
# ROLLING CALCULATIONS — TEAM
# ============================================================================

def calculate_team_rolling(team_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling averages for a single team's basic + advanced stats."""
    team_df = team_df.sort_values("game_date").copy()

    # Game number within season
    team_df["game_number"] = range(1, len(team_df) + 1)
    team_df["games_l5"] = (team_df["game_number"] - 1).clip(upper=5)
    team_df["games_l15"] = (team_df["game_number"] - 1).clip(upper=15)
    team_df["games_szn"] = team_df["game_number"] - 1

    # Basic stats
    for source_col, target_name in TEAM_BASIC_MAPPING.items():
        if source_col not in team_df.columns:
            continue
        shifted = team_df[source_col].shift(1)
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            if window_size is None:
                team_df[col_name] = shifted.expanding(min_periods=1).mean()
            else:
                team_df[col_name] = shifted.rolling(window=window_size, min_periods=1).mean()

    # Advanced stats
    for source_col, target_name in TEAM_ADVANCED_MAPPING.items():
        if source_col not in team_df.columns:
            continue
        shifted = team_df[source_col].shift(1)
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            if window_size is None:
                team_df[col_name] = shifted.expanding(min_periods=1).mean()
            else:
                team_df[col_name] = shifted.rolling(window=window_size, min_periods=1).mean()

    return team_df


# ============================================================================
# INCREMENTAL PIPELINE
# ============================================================================

def calculate_incremental(df: pd.DataFrame, target_date: date, id_col: str, calc_fn) -> pd.DataFrame:
    """Calculate rolling averages for all entities, return only target date rows."""
    results = []
    entity_ids = df[id_col].unique()

    for i, entity_id in enumerate(entity_ids):
        entity_df = df[df[id_col] == entity_id].copy()
        entity_df = calc_fn(entity_df)

        target_row = entity_df[entity_df["game_date"] == target_date]
        if not target_row.empty:
            results.append(target_row)

        if (i + 1) % 50 == 0:
            logger.info(f"Processed {i + 1}/{len(entity_ids)} entities")

    if results:
        result_df = pd.concat(results, ignore_index=True)
        logger.info(f"Generated {len(result_df)} rows for {target_date}")
        return result_df
    return pd.DataFrame()


# ============================================================================
# DATABASE UPSERT
# ============================================================================

def _build_upsert_sql(table: str, cols: list[str], conflict_cols: list[str]) -> str:
    """Build a parameterized UPSERT query."""
    col_list = ", ".join(cols)
    placeholder_list = ", ".join([f":{c}" for c in cols])
    update_cols = [c for c in cols if c not in conflict_cols]
    update_list = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholder_list})
        ON CONFLICT ({', '.join(conflict_cols)})
        DO UPDATE SET {update_list}
    """


def upsert_player_basic_averages(engine, df: pd.DataFrame):
    """Upsert player basic average rows."""
    if df.empty:
        return
    logger.info(f"Upserting {len(df)} player basic rows...")

    base_cols = ["player_id", "game_id", "season_id", "game_date", "team_id",
                 "game_number", "games_l5", "games_l15", "games_szn"]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]
    std_cols = [c for c in df.columns if c.startswith("std_")]
    extra_cols = [c for c in ["min_floor_l5", "games_started_l5", "rest_days", "games_last_7d"] if c in df.columns]

    all_cols = base_cols + avg_cols + std_cols + extra_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)
        elif col == "min_floor_l5":
            insert_df[col] = insert_df[col].round(2)

    cols = list(insert_df.columns)
    upsert_sql = _build_upsert_sql("player_average_game_stats", cols, ["player_id", "game_id"])

    with engine.begin() as conn:
        for _, row in insert_df.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
            conn.execute(text(upsert_sql), params)
    logger.info(f"Upserted {len(insert_df)} player basic rows")


def upsert_player_advanced_averages(engine, df: pd.DataFrame):
    """Upsert player advanced average rows."""
    if df.empty:
        return
    logger.info(f"Upserting {len(df)} player advanced rows...")

    base_cols = ["player_id", "game_id", "season_id", "game_date", "team_id",
                 "game_number", "games_l5", "games_l15", "games_szn"]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]

    all_cols = base_cols + avg_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    for col in insert_df.columns:
        if col.startswith("avg_"):
            insert_df[col] = insert_df[col].round(4)

    cols = list(insert_df.columns)
    upsert_sql = _build_upsert_sql("player_average_advanced_stats", cols, ["player_id", "game_id"])

    with engine.begin() as conn:
        for _, row in insert_df.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
            conn.execute(text(upsert_sql), params)
    logger.info(f"Upserted {len(insert_df)} player advanced rows")


def upsert_team_averages(engine, df: pd.DataFrame):
    """Upsert team average rows."""
    if df.empty:
        return
    logger.info(f"Upserting {len(df)} team rows...")

    base_cols = ["team_id", "game_id", "season_id", "game_date",
                 "game_number", "games_l5", "games_l15", "games_szn"]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]

    all_cols = base_cols + avg_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    for col in insert_df.columns:
        if col.startswith("avg_"):
            insert_df[col] = insert_df[col].round(4)

    cols = list(insert_df.columns)
    upsert_sql = _build_upsert_sql("team_average_game_stats", cols, ["team_id", "game_id"])

    with engine.begin() as conn:
        for _, row in insert_df.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
            conn.execute(text(upsert_sql), params)
    logger.info(f"Upserted {len(insert_df)} team rows")


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
        # Determine current season
        season_id = get_season_id(engine, target_date)
        if not season_id:
            logger.info("No season data found for target date. Nothing to update.")
            return
        logger.info(f"Season: {season_id}")

        # ── Player Basic Stats ──────────────────────────────────────
        player_ids = get_players_with_games_on_date(engine, target_date)
        if player_ids:
            df = fetch_player_basic_season_games(engine, player_ids, season_id, target_date)
            if not df.empty:
                result_df = calculate_incremental(
                    df, target_date, "player_id", calculate_basic_rolling_for_player
                )
                upsert_player_basic_averages(engine, result_df)

            # ── Player Advanced Stats ───────────────────────────────
            adv_df = fetch_player_advanced_season_games(engine, player_ids, season_id, target_date)
            if not adv_df.empty:
                adv_result = calculate_incremental(
                    adv_df, target_date, "player_id", calculate_advanced_rolling_for_player
                )
                upsert_player_advanced_averages(engine, adv_result)
        else:
            logger.info("No players found for target date.")

        # ── Team Stats ──────────────────────────────────────────────
        team_ids = get_teams_with_games_on_date(engine, target_date)
        if team_ids:
            team_df = fetch_team_season_games(engine, team_ids, season_id, target_date)
            if not team_df.empty:
                team_result = calculate_incremental(
                    team_df, target_date, "team_id", calculate_team_rolling
                )
                upsert_team_averages(engine, team_result)
        else:
            logger.info("No teams found for target date.")

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"Completed in {elapsed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
