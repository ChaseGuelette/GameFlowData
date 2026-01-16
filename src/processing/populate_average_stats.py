"""
Populate Average Stats Tables
Calculates L5, L15, and Season-to-date rolling averages for:
  - player_average_game_stats (basic box score)
  - player_average_advanced_stats (advanced metrics)
  - team_average_game_stats (basic + advanced)

Usage:
  python populate_average_stats.py                    # All tables, all seasons
  python populate_average_stats.py --season 2024-25  # Specific season
  python populate_average_stats.py --table player    # Just player basic stats
"""

import argparse
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np
from sqlalchemy import text
from src.db.client import get_engine

# ============================================================================
# CONFIGURATION
# ============================================================================

WINDOWS = {
    'l5': 5,
    'l15': 15,
    'szn': None  # None = expanding window (all prior games)
}

# Batch size for inserts
BATCH_SIZE = 1000

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# ROLLING AVERAGE HELPERS
# ============================================================================

def calculate_rolling_averages(
    df: pd.DataFrame,
    group_cols: list[str],
    stat_cols: list[str],
    sort_col: str = 'game_date'
) -> pd.DataFrame:
    """
    Calculate rolling averages for multiple windows.
    
    Uses shift(1) to ensure we only use PRIOR games (no data leakage).
    
    Args:
        df: DataFrame with game-level stats
        group_cols: Columns to group by (e.g., ['player_id', 'season_id'])
        stat_cols: Columns to calculate averages for
        sort_col: Column to sort by within groups
    
    Returns:
        DataFrame with new columns: {stat}_l5, {stat}_l15, {stat}_szn
    """
    df = df.sort_values(group_cols + [sort_col]).copy()
    
    for stat in stat_cols:
        # Shift to only use prior games
        shifted = df.groupby(group_cols)[stat].shift(1)
        
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{stat}_{window_name}"
            
            if window_size is None:
                # Expanding window (season-to-date)
                df[col_name] = shifted.groupby(df[group_cols].apply(tuple, axis=1)).expanding().mean().reset_index(level=0, drop=True)
            else:
                # Fixed rolling window
                df[col_name] = shifted.groupby(df[group_cols].apply(tuple, axis=1)).rolling(window=window_size, min_periods=1).mean().reset_index(level=0, drop=True)
    
    return df


def calculate_games_in_window(
    df: pd.DataFrame,
    group_cols: list[str],
    sort_col: str = 'game_date'
) -> pd.DataFrame:
    """
    Calculate how many games are in each window (for context).
    
    Early in season, L5 might only have 2 games, etc.
    """
    df = df.sort_values(group_cols + [sort_col]).copy()
    
    # Game number within group (1-indexed)
    df['game_number'] = df.groupby(group_cols).cumcount() + 1
    
    # Games available for each window (prior games only, hence -1)
    df['games_l5'] = (df['game_number'] - 1).clip(upper=5)
    df['games_l15'] = (df['game_number'] - 1).clip(upper=15)
    df['games_szn'] = df['game_number'] - 1
    
    return df


def rolling_with_groupby(
    series: pd.Series,
    group_keys: pd.Series,
    window: Optional[int],
    min_periods: int = 1
) -> pd.Series:
    """
    Apply rolling window respecting group boundaries.
    
    More memory-efficient than the lambda approach for large datasets.
    """
    result = pd.Series(index=series.index, dtype=float)
    
    for key, group_idx in series.groupby(group_keys).groups.items():
        group_data = series.loc[group_idx]
        
        if window is None:
            # Expanding window
            rolled = group_data.expanding(min_periods=min_periods).mean()
        else:
            rolled = group_data.rolling(window=window, min_periods=min_periods).mean()
        
        result.loc[group_idx] = rolled
    
    return result


# ============================================================================
# PLAYER BASIC STATS
# ============================================================================

PLAYER_BASIC_STATS = [
    'min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
    'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb', 'ast',
    'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus'
]


def fetch_player_game_stats(engine, season_id: Optional[str] = None) -> pd.DataFrame:
    """Fetch player game stats from database."""
    query = """
        SELECT 
            player_id,
            game_id,
            season_id,
            game_date::date as game_date,
            team_id,
            min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            ftm, fta, ft_pct, oreb, dreb, reb, ast,
            stl, blk, tov, pf, pts, plus_minus,
            did_not_play
        FROM player_game_stats
        WHERE did_not_play = false OR did_not_play IS NULL
    """
    
    if season_id:
        query += f" AND season_id = '{season_id}'"
    
    query += " ORDER BY player_id, season_id, game_date"
    
    logger.info("Fetching player game stats...")
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df):,} player game rows")
    
    return df


def calculate_player_basic_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling averages for player basic stats."""
    logger.info("Calculating player basic stat averages...")
    
    group_cols = ['player_id', 'season_id']
    df = df.sort_values(group_cols + ['game_date']).copy()
    
    # Calculate game number and games in each window
    df = calculate_games_in_window(df, group_cols)
    
    # Calculate rolling averages for each stat
    for stat in PLAYER_BASIC_STATS:
        if stat not in df.columns:
            logger.warning(f"Column {stat} not found, skipping")
            continue
            
        # Shift to only use prior games (prevent leakage)
        shifted = df.groupby(group_cols)[stat].shift(1)
        
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{stat}_{window_name}"
            
            # Create group key for efficient rolling
            group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)
            
            if window_size is None:
                # Season-to-date (expanding)
                df[col_name] = rolling_with_groupby(shifted, group_key, window=None)
            else:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=window_size)
    
    # First game of season has no prior data - these will be NULL
    # That's correct behavior (no average available yet)
    
    logger.info("Player basic averages calculated")
    return df


def insert_player_basic_averages(engine, df: pd.DataFrame):
    """Insert player basic averages into database."""
    logger.info("Inserting player basic averages...")
    
    # Select only columns we need for the table
    columns = ['player_id', 'game_id', 'season_id', 'game_date', 'team_id', 'game_number',
               'games_l5', 'games_l15', 'games_szn']
    
    for stat in PLAYER_BASIC_STATS:
        for window in ['l5', 'l15', 'szn']:
            col = f"avg_{stat}_{window}"
            if col in df.columns:
                columns.append(col)
    
    insert_df = df[columns].copy()
    
    # Round numeric columns for cleaner storage
    numeric_cols = [c for c in insert_df.columns if c.startswith('avg_')]
    insert_df[numeric_cols] = insert_df[numeric_cols].round(4)
    
    # Batch insert with upsert
    with engine.begin() as conn:
        # Truncate and reload (simpler than upsert for full refresh)
        conn.execute(text("TRUNCATE TABLE player_average_game_stats"))
        
        # Insert in batches
        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i:i + BATCH_SIZE]
            batch.to_sql(
                'player_average_game_stats',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1} ({min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,})")
    
    logger.info(f"Inserted {len(insert_df):,} player basic average rows")


# ============================================================================
# PLAYER ADVANCED STATS
# ============================================================================

PLAYER_ADVANCED_STATS = [
    'offensive_rating', 'defensive_rating', 'net_rating',
    'true_shooting_percentage', 'effective_field_goal_percentage',
    'usage_percentage', 'assist_ratio', 'assist_percentage',
    'assist_to_turnover', 'turnover_ratio', 'rebound_percentage',
    'offensive_rebound_percentage', 'defensive_rebound_percentage',
    'pace', 'possessions', 'pie'
]

# Mapping from source column to target column name
PLAYER_ADVANCED_MAPPING = {
    'offensive_rating': 'off_rtg',
    'defensive_rating': 'def_rtg',
    'net_rating': 'net_rtg',
    'true_shooting_percentage': 'ts_pct',
    'effective_field_goal_percentage': 'efg_pct',
    'usage_percentage': 'usg_pct',
    'assist_ratio': 'ast_ratio',
    'assist_percentage': 'ast_pct',
    'assist_to_turnover': 'ast_tov',
    'turnover_ratio': 'tov_ratio',
    'rebound_percentage': 'reb_pct',
    'offensive_rebound_percentage': 'oreb_pct',
    'defensive_rebound_percentage': 'dreb_pct',
    'pace': 'pace',
    'possessions': 'poss',
    'pie': 'pie'
}


def fetch_player_advanced_stats(engine, season_id: Optional[str] = None) -> pd.DataFrame:
    """Fetch player advanced stats by joining game stats with advanced stats."""
    query = """
        SELECT 
            pgs.player_id,
            pgs.game_id,
            pgs.season_id,
            pgs.game_date::date as game_date,
            pgs.team_id,
            pgs.did_not_play,
            pgs.offensive_rating,
            pgs.defensive_rating,
            pgs.net_rating,
            pgs.true_shooting_percentage,
            pgs.effective_field_goal_percentage,
            pgs.usage_percentage,
            pgs.assist_ratio,
            pgs.assist_percentage,
            pgs.assist_to_turnover,
            pgs.turnover_ratio,
            pgs.rebound_percentage,
            pgs.offensive_rebound_percentage,
            pgs.defensive_rebound_percentage,
            pgs.pace,
            pgs.possessions,
            pgs.pie
        FROM player_game_stats pgs
        WHERE (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
    """
    
    if season_id:
        query += f" AND pgs.season_id = '{season_id}'"
    
    query += " ORDER BY pgs.player_id, pgs.season_id, pgs.game_date"
    
    logger.info("Fetching player advanced stats...")
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df):,} player advanced stat rows")
    
    return df


def calculate_player_advanced_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling averages for player advanced stats."""
    logger.info("Calculating player advanced stat averages...")
    
    group_cols = ['player_id', 'season_id']
    df = df.sort_values(group_cols + ['game_date']).copy()
    
    # Calculate game number and games in each window
    df = calculate_games_in_window(df, group_cols)
    
    # Calculate rolling averages for each stat
    for source_col, target_name in PLAYER_ADVANCED_MAPPING.items():
        if source_col not in df.columns:
            logger.warning(f"Column {source_col} not found, skipping")
            continue
        
        # Shift to only use prior games
        shifted = df.groupby(group_cols)[source_col].shift(1)
        group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)
        
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            
            if window_size is None:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=None)
            else:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=window_size)
    
    logger.info("Player advanced averages calculated")
    return df


def insert_player_advanced_averages(engine, df: pd.DataFrame):
    """Insert player advanced averages into database."""
    logger.info("Inserting player advanced averages...")
    
    # Select only columns we need
    columns = ['player_id', 'game_id', 'season_id', 'game_date', 'team_id', 'game_number',
               'games_l5', 'games_l15', 'games_szn']
    
    for target_name in PLAYER_ADVANCED_MAPPING.values():
        for window in ['l5', 'l15', 'szn']:
            col = f"avg_{target_name}_{window}"
            if col in df.columns:
                columns.append(col)
    
    insert_df = df[columns].copy()
    
    # Round numeric columns
    numeric_cols = [c for c in insert_df.columns if c.startswith('avg_')]
    insert_df[numeric_cols] = insert_df[numeric_cols].round(4)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE player_average_advanced_stats"))
        
        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i:i + BATCH_SIZE]
            batch.to_sql(
                'player_average_advanced_stats',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1} ({min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,})")
    
    logger.info(f"Inserted {len(insert_df):,} player advanced average rows")


# ============================================================================
# TEAM STATS
# ============================================================================

TEAM_BASIC_STATS = [
    'team_pts', 'team_fgm', 'team_fga', 'team_fg_pct',
    'team_fg3m', 'team_fg3a', 'team_fg3_pct',
    'team_ftm', 'team_fta', 'team_ft_pct',
    'team_oreb', 'team_dreb', 'team_reb',
    'team_ast', 'team_stl', 'team_blk', 'team_tov', 'team_pf',
    'team_plus_minus'
]

TEAM_ADVANCED_STATS = [
    'offensive_rating', 'defensive_rating', 'net_rating',
    'true_shooting_percentage', 'effective_field_goal_percentage',
    'pace', 'possessions', 'assist_ratio', 'turnover_ratio',
    'rebound_percentage', 'offensive_rebound_percentage',
    'defensive_rebound_percentage', 'pie'
]

# Mapping for team stats (strip team_ prefix for output)
TEAM_BASIC_MAPPING = {stat: stat.replace('team_', '') for stat in TEAM_BASIC_STATS}

TEAM_ADVANCED_MAPPING = {
    'offensive_rating': 'off_rtg',
    'defensive_rating': 'def_rtg',
    'net_rating': 'net_rtg',
    'true_shooting_percentage': 'ts_pct',
    'effective_field_goal_percentage': 'efg_pct',
    'pace': 'pace',
    'possessions': 'poss',
    'assist_ratio': 'ast_ratio',
    'turnover_ratio': 'tov_ratio',
    'rebound_percentage': 'reb_pct',
    'offensive_rebound_percentage': 'oreb_pct',
    'defensive_rebound_percentage': 'dreb_pct',
    'pie': 'pie'
}


def fetch_team_game_stats(engine, season_id: Optional[str] = None) -> pd.DataFrame:
    """Fetch team game stats from database."""
    query = """
        SELECT 
            team_id,
            game_id,
            season_id,
            game_date::date as game_date,
            team_pts, team_fgm, team_fga, team_fg_pct,
            team_fg3m, team_fg3a, team_fg3_pct,
            team_ftm, team_fta, team_ft_pct,
            team_oreb, team_dreb, team_reb,
            team_ast, team_stl, team_blk, team_tov, team_pf,
            team_plus_minus,
            offensive_rating, defensive_rating, net_rating,
            true_shooting_percentage, effective_field_goal_percentage,
            pace, possessions, assist_ratio, turnover_ratio,
            rebound_percentage, offensive_rebound_percentage,
            defensive_rebound_percentage, pie
        FROM team_game_stats
    """
    
    if season_id:
        query += f" WHERE season_id = '{season_id}'"
    
    query += " ORDER BY team_id, season_id, game_date"
    
    logger.info("Fetching team game stats...")
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df):,} team game rows")
    
    return df


def calculate_team_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling averages for team stats (basic + advanced)."""
    logger.info("Calculating team stat averages...")
    
    group_cols = ['team_id', 'season_id']
    df = df.sort_values(group_cols + ['game_date']).copy()
    
    # Calculate game number and games in each window
    df = calculate_games_in_window(df, group_cols)
    
    # Process basic stats
    for source_col, target_name in TEAM_BASIC_MAPPING.items():
        if source_col not in df.columns:
            logger.warning(f"Column {source_col} not found, skipping")
            continue
        
        shifted = df.groupby(group_cols)[source_col].shift(1)
        group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)
        
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            
            if window_size is None:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=None)
            else:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=window_size)
    
    # Process advanced stats
    for source_col, target_name in TEAM_ADVANCED_MAPPING.items():
        if source_col not in df.columns:
            logger.warning(f"Column {source_col} not found, skipping")
            continue
        
        shifted = df.groupby(group_cols)[source_col].shift(1)
        group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)
        
        for window_name, window_size in WINDOWS.items():
            col_name = f"avg_{target_name}_{window_name}"
            
            if window_size is None:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=None)
            else:
                df[col_name] = rolling_with_groupby(shifted, group_key, window=window_size)
    
    logger.info("Team averages calculated")
    return df


def insert_team_averages(engine, df: pd.DataFrame):
    """Insert team averages into database."""
    logger.info("Inserting team averages...")
    
    # Build column list
    columns = ['team_id', 'game_id', 'season_id', 'game_date', 'game_number',
               'games_l5', 'games_l15', 'games_szn']
    
    # Add basic stat columns
    for target_name in TEAM_BASIC_MAPPING.values():
        for window in ['l5', 'l15', 'szn']:
            col = f"avg_{target_name}_{window}"
            if col in df.columns:
                columns.append(col)
    
    # Add advanced stat columns
    for target_name in TEAM_ADVANCED_MAPPING.values():
        for window in ['l5', 'l15', 'szn']:
            col = f"avg_{target_name}_{window}"
            if col in df.columns and col not in columns:  # Avoid duplicates
                columns.append(col)
    
    insert_df = df[columns].copy()
    
    # Round numeric columns
    numeric_cols = [c for c in insert_df.columns if c.startswith('avg_')]
    insert_df[numeric_cols] = insert_df[numeric_cols].round(4)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE team_average_game_stats"))
        
        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i:i + BATCH_SIZE]
            batch.to_sql(
                'team_average_game_stats',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1} ({min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,})")
    
    logger.info(f"Inserted {len(insert_df):,} team average rows")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Populate average stats tables')
    parser.add_argument('--season', type=str, help='Specific season (e.g., 2024-25)')
    parser.add_argument('--table', type=str, choices=['player', 'player_advanced', 'team', 'all'],
                        default='all', help='Which table(s) to populate')
    args = parser.parse_args()
    
    engine = get_engine()
    
    logger.info("=" * 60)
    logger.info("Starting average stats population")
    logger.info(f"Season filter: {args.season or 'ALL'}")
    logger.info(f"Tables: {args.table}")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # Player basic stats
        if args.table in ['player', 'all']:
            df = fetch_player_game_stats(engine, args.season)
            df = calculate_player_basic_averages(df)
            insert_player_basic_averages(engine, df)
            del df  # Free memory
        
        # Player advanced stats
        if args.table in ['player_advanced', 'all']:
            df = fetch_player_advanced_stats(engine, args.season)
            df = calculate_player_advanced_averages(df)
            insert_player_advanced_averages(engine, df)
            del df
        
        # Team stats
        if args.table in ['team', 'all']:
            df = fetch_team_game_stats(engine, args.season)
            df = calculate_team_averages(df)
            insert_team_averages(engine, df)
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