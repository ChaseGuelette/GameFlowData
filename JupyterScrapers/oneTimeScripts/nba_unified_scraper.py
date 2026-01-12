#!/usr/bin/env python3
"""
NBA Unified Stats Scraper
=========================
Combines team game stats, player game stats, and advanced stats into a single script.
Designed to be run periodically to fetch new games.

Tables updated:
- team_game_stats: Basic + Advanced team stats per game
- player_game_advanced_stats: Player advanced metrics per game

Usage:
    python nba_unified_scraper.py [--season YYYY-YY] [--season-type TYPE] [--skip-team] [--skip-advanced]
    
Examples:
    python nba_unified_scraper.py                              # Scrape current regular season (2024-25)
    python nba_unified_scraper.py --season 2023-24            # Scrape specific regular season
    python nba_unified_scraper.py --season-type Playoffs      # Scrape current playoffs
    python nba_unified_scraper.py --season-type PlayIn        # Scrape current play-in tournament
    python nba_unified_scraper.py --season 2023-24 --season-type Playoffs  # Scrape 2023-24 playoffs
    python nba_unified_scraper.py --skip-advanced             # Only fetch basic team stats
"""

import argparse
import os
import random
import re
import sys
import time
from datetime import datetime
from typing import Optional, Set, Tuple, List

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout, ConnectionError, ChunkedEncodingError
from sqlalchemy import create_engine, text, inspect

# NBA API imports
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoreadvancedv3,
)

# =============================================================================
# Configuration
# =============================================================================

# Rate limiting settings
SHORT_DELAY_MIN = 0.6
SHORT_DELAY_MAX = 1.5
LONG_PAUSE_EVERY = 100
LONG_PAUSE_MIN = 30
LONG_PAUSE_MAX = 60
BAN_COOLDOWN = 600  # 10 minutes if rate limited
MAX_RETRIES = 3

# Current season default
DEFAULT_SEASON = '2024-25'


# =============================================================================
# Helper Functions
# =============================================================================

def normalize_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names for PostgreSQL (lowercase + underscores)."""
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def parse_minutes(minutes_str) -> float:
    """Parse minutes from various formats to float."""
    if pd.isna(minutes_str) or minutes_str == '' or minutes_str is None:
        return 0.0
    
    if isinstance(minutes_str, (int, float)):
        return float(minutes_str)
    
    minutes_str = str(minutes_str)
    
    # MM:SS format
    if ':' in minutes_str and 'PT' not in minutes_str:
        try:
            parts = minutes_str.split(':')
            mins = int(parts[0])
            secs = int(parts[1]) if len(parts) > 1 else 0
            return round(mins + secs / 60, 3)
        except (ValueError, IndexError):
            return 0.0
    
    # ISO 8601 format (PT28M17.00S)
    if 'PT' in minutes_str:
        try:
            match = re.match(r'PT(\d+)M([\d.]+)S', minutes_str)
            if match:
                mins = int(match.group(1))
                secs = float(match.group(2))
                return round(mins + secs / 60, 3)
        except (ValueError, AttributeError):
            return 0.0
    
    return 0.0


def determine_dnp(row: pd.Series) -> bool:
    """Determine if a player did not play."""
    comment = row.get('comment', '')
    minutes = row.get('minutes', 0)
    
    if pd.notna(comment) and str(comment).strip() != '':
        return True
    
    if pd.isna(minutes) or minutes == 0:
        return True
    
    return False


def rate_limit_delay(game_num: int):
    """Apply rate limiting between API calls."""
    if game_num > 0 and game_num % LONG_PAUSE_EVERY == 0:
        pause = round(random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX), 1)
        print(f"\n  ⏸️  Long pause: {pause}s (every {LONG_PAUSE_EVERY} games)\n")
        time.sleep(pause)
    else:
        delay = round(random.uniform(SHORT_DELAY_MIN, SHORT_DELAY_MAX), 2)
        time.sleep(delay)


def extract_opponent_id(matchup: str, team_id: int, team_abbrevs: dict) -> Optional[int]:
    """Extract opponent team ID from matchup string like 'LAL vs. GSW' or 'LAL @ GSW'."""
    if not matchup or pd.isna(matchup):
        return None
    
    # Parse matchup: "TEAM vs. OPP" or "TEAM @ OPP"
    if ' vs. ' in matchup:
        parts = matchup.split(' vs. ')
    elif ' @ ' in matchup:
        parts = matchup.split(' @ ')
    else:
        return None
    
    if len(parts) != 2:
        return None
    
    # Get the opponent abbreviation (the one that's not our team)
    opp_abbrev = parts[1].strip()
    
    return team_abbrevs.get(opp_abbrev)


# =============================================================================
# Database Connection
# =============================================================================

def get_engine():
    """Create and return database engine."""
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment. Check your .env file.")
    
    engine = create_engine(DATABASE_URL)
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    
    return engine


# =============================================================================
# Team Game Stats Scraper
# =============================================================================

def get_existing_game_ids(engine, table_name: str = 'team_game_stats') -> Set[str]:
    """Get set of game IDs already in the database."""
    inspector = inspect(engine)
    
    if table_name not in inspector.get_table_names():
        return set()
    
    existing_games = pd.read_sql(f"SELECT DISTINCT game_id FROM {table_name}", engine)
    return set(existing_games['game_id'].unique())


def get_team_abbreviation_map(engine) -> dict:
    """Get mapping of team abbreviation to team_id."""
    # NBA team abbreviation mapping
    abbrev_map = {
        'ATL': 1610612737, 'BOS': 1610612738, 'BKN': 1610612751, 'CHA': 1610612766,
        'CHI': 1610612741, 'CLE': 1610612739, 'DAL': 1610612742, 'DEN': 1610612743,
        'DET': 1610612765, 'GSW': 1610612744, 'HOU': 1610612745, 'IND': 1610612754,
        'LAC': 1610612746, 'LAL': 1610612747, 'MEM': 1610612763, 'MIA': 1610612748,
        'MIL': 1610612749, 'MIN': 1610612750, 'NOP': 1610612740, 'NYK': 1610612752,
        'OKC': 1610612760, 'ORL': 1610612753, 'PHI': 1610612755, 'PHX': 1610612756,
        'POR': 1610612757, 'SAC': 1610612758, 'SAS': 1610612759, 'TOR': 1610612761,
        'UTA': 1610612762, 'WAS': 1610612764
    }
    
    return abbrev_map


def scrape_team_game_stats(engine, seasons: List[str], season_type: str = 'Regular Season') -> int:
    """
    Scrape basic team game stats from NBA API.
    
    Args:
        engine: Database engine
        seasons: List of seasons to scrape (e.g., ['2024-25'])
        season_type: 'Regular Season', 'Playoffs', or 'PlayIn' (default: 'Regular Season')
    
    Returns:
        Number of new games added.
    """
    print("\n" + "="*60)
    print(f"STEP 1: Fetching Team Game Stats ({season_type})")
    print("="*60)
    
    existing_game_ids = get_existing_game_ids(engine)
    print(f"Found {len(existing_game_ids)} games already in database.")
    
    team_abbrev_map = get_team_abbreviation_map(engine)
    
    all_seasons_data = []
    
    for season in seasons:
        try:
            print(f"\nFetching {season_type} games for {season}...")
            game_finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                league_id_nullable='00',
                season_type_nullable=season_type
            )
            
            games_df = game_finder.get_data_frames()[0]
            games_df['SEASON'] = season
            
            # Filter out games we already have
            new_games_df = games_df[~games_df['GAME_ID'].isin(existing_game_ids)]
            
            if not new_games_df.empty:
                all_seasons_data.append(new_games_df)
                print(f"✓ Found {len(new_games_df)} new game records for {season}")
            else:
                print(f"- No new games found for {season}")
            
            time.sleep(round(random.uniform(1, 3), 1))
            
        except Exception as e:
            print(f"✗ Error for {season}: {e}")
    
    if not all_seasons_data:
        print("\n🙌 No new team data to add!")
        return 0
    
    # Combine all seasons
    print("\nCombining collected seasons into single dataframe...")
    combined_df = pd.concat(all_seasons_data, ignore_index=True)
    
    # Normalize column names
    combined_df = normalize_for_postgres(combined_df)
    
    # Map to database schema
    mapping = {
        'game_date': 'team_game_date',
        'matchup': 'team_matchup',
        'wl': 'team_wl',
        'min': 'team_min',
        'pts': 'team_pts',
        'fgm': 'team_fgm',
        'fga': 'team_fga',
        'fg_pct': 'team_fg_pct',
        'fg3m': 'team_fg3m',
        'fg3a': 'team_fg3a',
        'fg3_pct': 'team_fg3_pct',
        'ftm': 'team_ftm',
        'fta': 'team_fta',
        'ft_pct': 'team_ft_pct',
        'oreb': 'team_oreb',
        'dreb': 'team_dreb',
        'reb': 'team_reb',
        'ast': 'team_ast',
        'stl': 'team_stl',
        'blk': 'team_blk',
        'tov': 'team_tov',
        'pf': 'team_pf',
        'plus_minus': 'team_plus_minus'
    }
    combined_df = combined_df.rename(columns=mapping)
    
    # Add opponent_id by parsing matchup
    combined_df['opponent_id'] = combined_df.apply(
        lambda row: extract_opponent_id(row.get('team_matchup'), row.get('team_id'), team_abbrev_map),
        axis=1
    )
    
    # Filter for columns the DB actually has (basic stats only - advanced will be added later)
    db_cols = [
        'season_id', 'team_id', 'team_abbreviation', 'team_name', 'game_id',
        'team_game_date', 'team_matchup', 'team_wl', 'team_min', 'team_pts',
        'team_fgm', 'team_fga', 'team_fg_pct', 'team_fg3m', 'team_fg3a',
        'team_fg3_pct', 'team_ftm', 'team_fta', 'team_ft_pct', 'team_oreb',
        'team_dreb', 'team_reb', 'team_ast', 'team_stl', 'team_blk',
        'team_tov', 'team_pf', 'team_plus_minus', 'season', 'opponent_id'
    ]
    
    final_df = combined_df[[c for c in db_cols if c in combined_df.columns]]
    
    # Push to SQL
    print(f"\nPushing {len(final_df)} records to 'team_game_stats'...")
    
    try:
        final_df.to_sql('team_game_stats', engine, if_exists='append', index=False)
        print("✅ Successfully updated team_game_stats!")
        return len(final_df)
    except Exception as e:
        print(f"❌ Error during SQL push: {e}")
        return 0


# =============================================================================
# Advanced Stats Scraper
# =============================================================================

def transform_player_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform player DataFrame from API format to database format."""
    if df.empty:
        return df
    
    # Rename columns: camelCase -> snake_case
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})
    
    # Rename person_id to player_id to match schema
    if 'person_id' in df.columns:
        df = df.rename(columns={'person_id': 'player_id'})
    
    # Parse minutes
    if 'minutes' in df.columns:
        df['minutes'] = df['minutes'].apply(parse_minutes)
    
    # Add DNP flag
    df['did_not_play'] = df.apply(determine_dnp, axis=1)
    
    # Add timestamp
    df['created_at'] = datetime.now()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['game_id', 'player_id'], keep='first')
    
    return df


def transform_team_adv_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform team advanced stats DataFrame for database update."""
    if df.empty:
        return df
    
    # Rename columns: camelCase -> snake_case
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})
    
    # Parse minutes
    if 'minutes' in df.columns:
        df['minutes'] = df['minutes'].apply(parse_minutes)
    
    return df


def get_games_missing_advanced_stats(engine) -> List[str]:
    """Get game IDs that have basic stats but are missing advanced stats."""
    sql = """
    SELECT DISTINCT game_id 
    FROM team_game_stats 
    WHERE offensive_rating IS NULL
    ORDER BY game_id ASC
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return [row[0] for row in result]


def scrape_single_game_advanced(game_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrape advanced stats for a single game.
    Returns: (player_df, team_df)
    """
    box_adv = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
    dfs = box_adv.get_data_frames()
    
    # dfs[0] = players, dfs[1] = teams
    player_df = transform_player_df(dfs[0].copy())
    team_df = transform_team_adv_df(dfs[1].copy())
    
    return player_df, team_df


def update_team_advanced_stats(engine, team_df: pd.DataFrame, game_id: str):
    """Update team_game_stats with advanced stats for a specific game."""
    if team_df.empty:
        return
    
    # Advanced stat columns to update in team_game_stats
    adv_columns = [
        'minutes', 'offensive_rating', 'defensive_rating', 'net_rating',
        'estimated_offensive_rating', 'estimated_defensive_rating', 'estimated_net_rating',
        'effective_field_goal_percentage', 'true_shooting_percentage', 'pie',
        'possessions', 'pace', 'estimated_pace', 'pace_per40',
        'assist_ratio', 'turnover_ratio', 'assist_to_turnover',
        'estimated_team_turnover_percentage', 'assist_percentage',
        'rebound_percentage', 'offensive_rebound_percentage', 'defensive_rebound_percentage'
    ]
    
    for _, row in team_df.iterrows():
        team_id = row.get('team_id')
        
        # Build SET clause for UPDATE
        set_parts = []
        params = {'game_id': game_id, 'team_id': team_id}
        
        for col in adv_columns:
            if col in row.index and pd.notna(row[col]):
                set_parts.append(f"{col} = :{col}")
                params[col] = row[col]
        
        if not set_parts:
            continue
        
        sql = f"""
        UPDATE team_game_stats 
        SET {', '.join(set_parts)}
        WHERE game_id = :game_id AND team_id = :team_id
        """
        
        with engine.begin() as conn:
            conn.execute(text(sql), params)


def get_player_game_advanced_stats_columns() -> List[str]:
    """Return the columns for player_game_advanced_stats table."""
    return [
        'game_id', 'player_id', 'team_id', 'first_name', 'family_name', 'name_i',
        'player_slug', 'position', 'comment', 'jersey_num', 'team_city', 'team_name',
        'team_tricode', 'team_slug', 'minutes', 'estimated_offensive_rating',
        'offensive_rating', 'estimated_defensive_rating', 'defensive_rating',
        'estimated_net_rating', 'net_rating', 'assist_percentage', 'assist_to_turnover',
        'assist_ratio', 'offensive_rebound_percentage', 'defensive_rebound_percentage',
        'rebound_percentage', 'estimated_team_turnover_percentage', 'turnover_ratio',
        'effective_field_goal_percentage', 'true_shooting_percentage', 'usage_percentage',
        'estimated_usage_percentage', 'estimated_pace', 'pace', 'possessions', 'pie',
        'did_not_play', 'created_at', 'pace_per40'
    ]


def ensure_players_exist(engine, player_df: pd.DataFrame):
    """
    Check if all players in the DataFrame exist in the players table.
    Insert any missing players.
    """
    if player_df.empty:
        return
    
    # Get unique players from this game
    players_in_game = player_df[['player_id', 'first_name', 'family_name']].drop_duplicates()
    player_ids = players_in_game['player_id'].tolist()
    
    # Check which players already exist
    placeholders = ','.join([':id' + str(i) for i in range(len(player_ids))])
    params = {f'id{i}': pid for i, pid in enumerate(player_ids)}
    
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT player_id FROM players WHERE player_id IN ({placeholders})"),
            params
        )
        existing_ids = {row[0] for row in result}
    
    # Find missing players
    missing_players = players_in_game[~players_in_game['player_id'].isin(existing_ids)]
    
    if missing_players.empty:
        return
    
    # Insert missing players
    print(f"    📝 Adding {len(missing_players)} new player(s) to players table...")
    
    with engine.begin() as conn:
        for _, player in missing_players.iterrows():
            player_name = f"{player['first_name']} {player['family_name']}"
            conn.execute(
                text("INSERT INTO players (player_id, player_name) VALUES (:id, :name) ON CONFLICT (player_id) DO NOTHING"),
                {'id': player['player_id'], 'name': player_name}
            )
    
    print(f"    ✓ Players added")


def scrape_advanced_stats(engine, limit: Optional[int] = None) -> Tuple[int, int]:
    """
    Scrape advanced stats for all games missing them.
    Returns: (games_processed, games_failed)
    """
    print("\n" + "="*60)
    print("STEP 2: Fetching Advanced Stats")
    print("="*60)
    
    games_to_scrape = get_games_missing_advanced_stats(engine)
    
    if not games_to_scrape:
        print("\n✅ All games already have advanced stats!")
        return 0, 0
    
    if limit:
        games_to_scrape = games_to_scrape[:limit]
    
    total = len(games_to_scrape)
    print(f"\nFound {total} games missing advanced stats")
    
    # Get existing player advanced stats to avoid duplicates
    existing_player_games = set()
    try:
        existing = pd.read_sql(
            "SELECT DISTINCT game_id FROM player_game_advanced_stats", 
            engine
        )
        existing_player_games = set(existing['game_id'].unique())
        print(f"Found {len(existing_player_games)} games with player advanced stats")
    except Exception:
        pass
    
    player_columns = get_player_game_advanced_stats_columns()
    games_processed = 0
    games_failed = 0
    
    for i, game_id in enumerate(games_to_scrape, 1):
        success = False
        attempt = 0
        
        while attempt < MAX_RETRIES and not success:
            attempt += 1
            try:
                print(f"[{i}/{total}] Scraping {game_id} (Try {attempt})... ", end="", flush=True)
                
                # Scrape
                player_df, team_df = scrape_single_game_advanced(game_id)
                
                # Check if data is empty
                if player_df.empty and team_df.empty:
                    print("⚠️  Empty data returned (Skipping)")
                    break
                
                # Update team advanced stats
                if not team_df.empty:
                    update_team_advanced_stats(engine, team_df, game_id)
                
                # Insert player advanced stats (if not already exists)
                if not player_df.empty and game_id not in existing_player_games:
                    # Ensure all players exist in the players table first
                    ensure_players_exist(engine, player_df)
                    
                    # Filter to only columns that exist in schema
                    available_cols = [c for c in player_columns if c in player_df.columns]
                    player_df_filtered = player_df[available_cols]
                    
                    with engine.begin() as conn:
                        player_df_filtered.to_sql(
                            'player_game_advanced_stats', 
                            conn, 
                            if_exists='append', 
                            index=False
                        )
                
                print("✓ Saved")
                success = True
                games_processed += 1
                
                # Rate limiting
                rate_limit_delay(i)
                
            except (ReadTimeout, ConnectionError, ConnectionResetError) as e:
                print(f"\n🛑 CONNECTION BLOCKED: {e}")
                print(f"💤 Sleeping for {BAN_COOLDOWN/60} minutes to reset...")
                time.sleep(BAN_COOLDOWN)
                
            except Exception as e:
                error_str = str(e)
                print(f"\n❌ Error: {e}")
                
                if "duplicate key" in error_str or "UniqueViolation" in error_str:
                    print("   (Game already exists, moving on)")
                    success = True
                    games_processed += 1
                else:
                    time.sleep(5)
        
        if not success:
            print(f"💀 Giving up on {game_id} after {MAX_RETRIES} attempts.")
            games_failed += 1
    
    return games_processed, games_failed


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='NBA Unified Stats Scraper')
    parser.add_argument(
        '--season', 
        type=str, 
        default=DEFAULT_SEASON,
        help=f'Season to scrape (default: {DEFAULT_SEASON})'
    )
    parser.add_argument(
        '--season-type',
        type=str,
        default='Regular Season',
        choices=['Regular Season', 'Playoffs', 'PlayIn'],
        help='Season type to scrape: Regular Season, Playoffs, or PlayIn (default: Regular Season)'
    )
    parser.add_argument(
        '--skip-team',
        action='store_true',
        help='Skip team game stats scraping'
    )
    parser.add_argument(
        '--skip-advanced',
        action='store_true',
        help='Skip advanced stats scraping'
    )
    parser.add_argument(
        '--advanced-limit',
        type=int,
        default=None,
        help='Limit number of games to scrape for advanced stats (useful for testing)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("NBA UNIFIED STATS SCRAPER")
    print("="*60)
    print(f"Season: {args.season}")
    print(f"Season Type: {args.season_type}")
    print(f"Skip Team Stats: {args.skip_team}")
    print(f"Skip Advanced Stats: {args.skip_advanced}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Connect to database
    print("\nConnecting to database...")
    try:
        engine = get_engine()
        print("✓ Database connection verified")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)
    
    seasons = [args.season]
    
    # Step 1: Team Game Stats
    new_games = 0
    if not args.skip_team:
        new_games = scrape_team_game_stats(engine, seasons, args.season_type)
    else:
        print("\n⏭️  Skipping team game stats (--skip-team)")
    
    # Step 2: Advanced Stats
    adv_processed = 0
    adv_failed = 0
    if not args.skip_advanced:
        adv_processed, adv_failed = scrape_advanced_stats(engine, args.advanced_limit)
    else:
        print("\n⏭️  Skipping advanced stats (--skip-advanced)")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"New team game records added: {new_games}")
    print(f"Games with advanced stats updated: {adv_processed}")
    print(f"Games failed: {adv_failed}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == '__main__':
    main()