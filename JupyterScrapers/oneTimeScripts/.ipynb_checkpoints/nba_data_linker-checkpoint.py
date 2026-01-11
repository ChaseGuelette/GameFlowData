"""
NBA Betting Data Linker
=======================
Links raw betting odds tables to official NBA stats database.

Tables processed:
- raw_game_lines_staging (1.2M rows) -> nba_game_id, nba_home_team_id, nba_away_team_id
- raw_player_props_combined (12M rows) -> game_id, player_id, team_id

Requirements:
    pip install sqlalchemy psycopg2-binary python-dotenv pytz tqdm

Usage:
    python nba_data_linker.py [--phase PHASE] [--chunk-size SIZE] [--dry-run]
    
Phases:
    1 = Build lookup tables only
    2 = Link game lines
    3 = Link player props (games)
    4 = Link player props (players)
    all = Run all phases (default)
    stats = Show current linking statistics
"""

import os
import sys
import argparse
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pytz
from tqdm import tqdm

# ============================================================================
# CONFIGURATION
# ============================================================================

CHUNK_SIZE_DEFAULT = 50000  # Rows per batch update
EASTERN = pytz.timezone('America/New_York')
UTC = pytz.UTC

# Team name variations -> canonical name (as it appears in team_game_stats)
# Add more mappings as you discover them
TEAM_NAME_ALIASES = {
    # Current teams with variations
    "LA Clippers": "Los Angeles Clippers",
    "L.A. Clippers": "Los Angeles Clippers", 
    "LAC": "Los Angeles Clippers",
    
    "LA Lakers": "Los Angeles Lakers",
    "L.A. Lakers": "Los Angeles Lakers",
    "LAL": "Los Angeles Lakers",
    
    # Historical franchise moves/renames
    "New Jersey Nets": "Brooklyn Nets",
    "NJ Nets": "Brooklyn Nets",
    
    "Charlotte Bobcats": "Charlotte Hornets",
    
    "New Orleans Hornets": "New Orleans Pelicans",
    "NO Hornets": "New Orleans Pelicans",
    "New Orleans/Oklahoma City Hornets": "New Orleans Pelicans",
    
    "Seattle SuperSonics": "Oklahoma City Thunder",
    "Seattle Supersonics": "Oklahoma City Thunder",
    
    "Vancouver Grizzlies": "Memphis Grizzlies",
    
    # Common API variations
    "Sixers": "Philadelphia 76ers",
    "Philly 76ers": "Philadelphia 76ers",
    "76ers": "Philadelphia 76ers",
    
    "Blazers": "Portland Trail Blazers",
    "Trail Blazers": "Portland Trail Blazers",
    
    "Wolves": "Minnesota Timberwolves",
    "T-Wolves": "Minnesota Timberwolves",
}

# Player name normalization patterns
PLAYER_NAME_REPLACEMENTS = {
    # Accented characters
    "ć": "c",
    "č": "c", 
    "ž": "z",
    "š": "s",
    "đ": "d",
    "ñ": "n",
    "ü": "u",
    "ö": "o",
    "é": "e",
    "á": "a",
    "í": "i",
    "ó": "o",
    "ú": "u",
    
    # Common variations
    " Jr.": "",
    " Jr": "",
    " III": "",
    " II": "",
    " IV": "",
    ".": "",
    "'": "",
    "-": " ",  # Gilgeous-Alexander -> Gilgeous Alexander
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure logging with both file and console output."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"nba_linker_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # File handler - detailed logging (utf-8 encoding)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    
    # Console handler - less verbose to work well with tqdm
    # Use sys.stdout with error handling for Windows encoding issues
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_engine():
    """Create database engine from environment."""
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment. Check your .env file.")
    
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    
    # Verify connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connection verified [OK]")
    
    return engine

# ============================================================================
# TEAM NAME UTILITIES
# ============================================================================

def normalize_team_name(name: str) -> str:
    """Normalize team name to match database format."""
    if not name:
        return name
    
    name = name.strip()
    
    # Check alias table first
    if name in TEAM_NAME_ALIASES:
        return TEAM_NAME_ALIASES[name]
    
    return name

def build_team_lookup(engine) -> dict:
    """
    Build comprehensive team lookup from team_game_stats.
    Returns: {team_name: team_id}
    """
    logger.info("Building team lookup table...")
    
    query = """
    SELECT DISTINCT team_id, team_name 
    FROM team_game_stats 
    WHERE team_name IS NOT NULL
    """
    
    team_lookup = {}
    reverse_lookup = {}  # team_id -> team_name (for logging)
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            team_id, team_name = row[0], row[1]
            team_lookup[team_name] = team_id
            reverse_lookup[team_id] = team_name
    
    # Add aliases pointing to same team_ids
    for alias, canonical in TEAM_NAME_ALIASES.items():
        if canonical in team_lookup:
            team_lookup[alias] = team_lookup[canonical]
    
    logger.info(f"Team lookup built: {len(team_lookup)} entries (including aliases)")
    return team_lookup, reverse_lookup

# ============================================================================
# PLAYER NAME UTILITIES  
# ============================================================================

def normalize_player_name(name: str) -> str:
    """Normalize player name for matching."""
    if not name:
        return name
    
    name = name.lower().strip()
    
    for old, new in PLAYER_NAME_REPLACEMENTS.items():
        name = name.replace(old.lower(), new)
    
    # Collapse multiple spaces
    name = " ".join(name.split())
    
    return name

def build_player_lookup(engine) -> tuple[dict, dict]:
    """
    Build player lookup from players table.
    Returns: 
        - exact_lookup: {normalized_name: player_id}
        - all_players: {player_id: original_name}
    """
    logger.info("Building player lookup table...")
    
    query = "SELECT player_id, player_name FROM players WHERE player_name IS NOT NULL"
    
    exact_lookup = {}
    all_players = {}
    duplicates = defaultdict(list)
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            player_id, player_name = row[0], row[1]
            normalized = normalize_player_name(player_name)
            all_players[player_id] = player_name
            
            if normalized in exact_lookup:
                duplicates[normalized].append((player_id, player_name))
            else:
                exact_lookup[normalized] = player_id
    
    if duplicates:
        logger.warning(f"Found {len(duplicates)} normalized name collisions:")
        for norm_name, players in list(duplicates.items())[:10]:
            logger.warning(f"  '{norm_name}': {players}")
    
    logger.info(f"Player lookup built: {len(exact_lookup)} unique normalized names")
    return exact_lookup, all_players

# ============================================================================
# GAME LINKING - PHASE 2 (raw_game_lines_staging)
# ============================================================================

def get_game_lines_date_range(engine) -> tuple[datetime, datetime]:
    """Get date range of unlinked game lines."""
    query = """
    SELECT 
        MIN(commence_time) as min_date,
        MAX(commence_time) as max_date
    FROM raw_game_lines_staging
    WHERE nba_game_id IS NULL
      AND commence_time IS NOT NULL
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        return result[0], result[1]

def build_game_lookup(engine) -> dict:
    """
    Build game lookup from team_game_stats.
    Key: (home_team_name, game_date_str)
    Value: (game_id, home_team_id, away_team_id)
    """
    logger.info("Building game lookup from team_game_stats...")
    
    # Get home games (matchup contains 'vs.')
    query = """
    SELECT 
        tgs_home.game_id,
        tgs_home.team_name as home_team,
        tgs_home.team_id as home_team_id,
        tgs_home.team_game_date,
        tgs_away.team_id as away_team_id,
        tgs_away.team_name as away_team
    FROM team_game_stats tgs_home
    JOIN team_game_stats tgs_away 
        ON tgs_home.game_id = tgs_away.game_id 
        AND tgs_home.team_id != tgs_away.team_id
    WHERE tgs_home.team_matchup LIKE '%vs.%'
    """
    
    game_lookup = {}
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            game_id, home_team, home_team_id, game_date, away_team_id, away_team = row
            
            # Parse date (it's stored as text like '2024-01-15')
            if game_date:
                date_str = str(game_date)[:10]  # Get just YYYY-MM-DD
                
                # Store with canonical home team name
                key = (home_team, date_str)
                game_lookup[key] = (game_id, home_team_id, away_team_id)
                
                # Also store with aliases
                for alias, canonical in TEAM_NAME_ALIASES.items():
                    if canonical == home_team:
                        alias_key = (alias, date_str)
                        game_lookup[alias_key] = (game_id, home_team_id, away_team_id)
    
    logger.info(f"Game lookup built: {len(game_lookup)} home team/date combinations")
    return game_lookup

def link_game_lines(engine, game_lookup: dict, chunk_size: int, dry_run: bool = False):
    """
    Link raw_game_lines_staging to NBA game IDs.
    Processes in monthly chunks.
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: Linking raw_game_lines_staging")
    logger.info("=" * 60)
    
    min_date, max_date = get_game_lines_date_range(engine)
    
    if not min_date or not max_date:
        logger.info("No unlinked game lines found.")
        return
    
    logger.info(f"Date range to process: {min_date} to {max_date}")
    
    # Calculate total months for progress bar
    current_start = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months = []
    while current_start <= max_date:
        months.append(current_start)
        if current_start.month == 12:
            current_start = current_start.replace(year=current_start.year + 1, month=1)
        else:
            current_start = current_start.replace(month=current_start.month + 1)
    
    total_updated = 0
    total_unmatched = 0
    unmatched_games = set()
    
    # Process month by month with progress bar
    with tqdm(months, desc="Phase 2: Game Lines", unit="month", 
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} months [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
        for current_start in pbar:
            # Calculate month end
            if current_start.month == 12:
                current_end = current_start.replace(year=current_start.year + 1, month=1)
            else:
                current_end = current_start.replace(month=current_start.month + 1)
            
            pbar.set_postfix({'month': current_start.strftime('%Y-%m'), 'updated': f'{total_updated:,}'})
            
            # Get unique games for this month
            unique_query = """
            SELECT DISTINCT 
                home_team,
                (commence_time AT TIME ZONE 'America/New_York')::date as game_date
            FROM raw_game_lines_staging
            WHERE nba_game_id IS NULL
              AND commence_time >= :start_date
              AND commence_time < :end_date
              AND home_team IS NOT NULL
            """
            
            updates = []
            month_unmatched = 0
            
            with engine.connect() as conn:
                result = conn.execute(text(unique_query), {
                    "start_date": current_start,
                    "end_date": current_end
                })
                
                for row in result:
                    home_team, game_date = row[0], row[1]
                    date_str = str(game_date)
                    
                    # Try to find match
                    normalized_home = normalize_team_name(home_team)
                    key = (normalized_home, date_str)
                    
                    if key in game_lookup:
                        game_id, home_id, away_id = game_lookup[key]
                        updates.append({
                            "home_team": home_team,
                            "game_date": game_date,
                            "game_id": game_id,
                            "home_id": home_id,
                            "away_id": away_id
                        })
                    else:
                        month_unmatched += 1
                        unmatched_games.add((home_team, date_str))
            
            # Apply updates
            if updates and not dry_run:
                update_query = """
                UPDATE raw_game_lines_staging
                SET 
                    nba_game_id = :game_id,
                    nba_home_team_id = :home_id,
                    nba_away_team_id = :away_id
                WHERE home_team = :home_team
                  AND (commence_time AT TIME ZONE 'America/New_York')::date = :game_date
                  AND nba_game_id IS NULL
                """
                
                with engine.begin() as conn:
                    for update in updates:
                        result = conn.execute(text(update_query), update)
                        total_updated += result.rowcount
            
            total_unmatched += month_unmatched
    
    # Report unmatched
    logger.info(f"\nPHASE 2 COMPLETE: {total_updated:,} rows updated, {len(unmatched_games)} unique games unmatched")
    
    if unmatched_games:
        # Write unmatched to CSV
        unmatched_file = "unmatched_game_lines.csv"
        with open(unmatched_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['home_team', 'game_date'])
            for home_team, date_str in sorted(unmatched_games):
                writer.writerow([home_team, date_str])
        logger.info(f"Unmatched games written to {unmatched_file}")

# ============================================================================
# PLAYER PROPS GAME LINKING - PHASE 3 
# ============================================================================

def get_props_date_range(engine) -> tuple[datetime, datetime]:
    """Get date range of unlinked player props."""
    query = """
    SELECT 
        MIN(commence_time) as min_date,
        MAX(commence_time) as max_date
    FROM raw_player_props_combined
    WHERE game_id IS NULL
      AND commence_time IS NOT NULL
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        return result[0], result[1]

def build_props_game_lookup(engine) -> dict:
    """
    Build game lookup for player props.
    Since props have both home and away teams, we need to match on both.
    Key: (home_team, away_team, game_date_str)
    Value: game_id
    """
    logger.info("Building game lookup for player props...")
    
    query = """
    SELECT 
        tgs_home.game_id,
        tgs_home.team_name as home_team,
        tgs_away.team_name as away_team,
        tgs_home.team_game_date
    FROM team_game_stats tgs_home
    JOIN team_game_stats tgs_away 
        ON tgs_home.game_id = tgs_away.game_id 
        AND tgs_home.team_id != tgs_away.team_id
    WHERE tgs_home.team_matchup LIKE '%vs.%'
    """
    
    game_lookup = {}
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            game_id, home_team, away_team, game_date = row
            
            if game_date:
                date_str = str(game_date)[:10]
                
                # Store with original names
                key = (home_team, away_team, date_str)
                game_lookup[key] = game_id
                
                # Store with all alias combinations
                home_variants = [home_team] + [a for a, c in TEAM_NAME_ALIASES.items() if c == home_team]
                away_variants = [away_team] + [a for a, c in TEAM_NAME_ALIASES.items() if c == away_team]
                
                for hv in home_variants:
                    for av in away_variants:
                        game_lookup[(hv, av, date_str)] = game_id
    
    logger.info(f"Props game lookup built: {len(game_lookup)} combinations")
    return game_lookup

def link_player_props_games(engine, game_lookup: dict, chunk_size: int, dry_run: bool = False):
    """
    Link raw_player_props_combined to game IDs.
    """
    logger.info("=" * 60)
    logger.info("PHASE 3: Linking player props to games")
    logger.info("=" * 60)
    
    min_date, max_date = get_props_date_range(engine)
    
    if not min_date or not max_date:
        logger.info("No unlinked player props found.")
        return
    
    logger.info(f"Date range to process: {min_date} to {max_date}")
    
    # Calculate total months for progress bar
    current_start = min_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months = []
    while current_start <= max_date:
        months.append(current_start)
        if current_start.month == 12:
            current_start = current_start.replace(year=current_start.year + 1, month=1)
        else:
            current_start = current_start.replace(month=current_start.month + 1)
    
    total_updated = 0
    unmatched_games = set()
    
    # Process month by month with progress bar
    with tqdm(months, desc="Phase 3: Props→Games", unit="month",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} months [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
        for current_start in pbar:
            if current_start.month == 12:
                current_end = current_start.replace(year=current_start.year + 1, month=1)
            else:
                current_end = current_start.replace(month=current_start.month + 1)
            
            pbar.set_postfix({'month': current_start.strftime('%Y-%m'), 'updated': f'{total_updated:,}'})
            
            # Get unique game combinations for this month
            unique_query = """
            SELECT DISTINCT 
                home_team,
                away_team,
                (commence_time AT TIME ZONE 'America/New_York')::date as game_date
            FROM raw_player_props_combined
            WHERE game_id IS NULL
              AND commence_time >= :start_date
              AND commence_time < :end_date
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
            """
            
            updates = []
            
            with engine.connect() as conn:
                result = conn.execute(text(unique_query), {
                    "start_date": current_start,
                    "end_date": current_end
                })
                
                for row in result:
                    home_team, away_team, game_date = row
                    date_str = str(game_date)
                    
                    # Normalize team names
                    norm_home = normalize_team_name(home_team)
                    norm_away = normalize_team_name(away_team)
                    
                    key = (norm_home, norm_away, date_str)
                    
                    if key in game_lookup:
                        updates.append({
                            "home_team": home_team,
                            "away_team": away_team,
                            "game_date": game_date,
                            "game_id": game_lookup[key]
                        })
                    else:
                        unmatched_games.add((home_team, away_team, date_str))
            
            # Apply updates
            if updates and not dry_run:
                update_query = """
                UPDATE raw_player_props_combined
                SET game_id = :game_id
                WHERE home_team = :home_team
                  AND away_team = :away_team
                  AND (commence_time AT TIME ZONE 'America/New_York')::date = :game_date
                  AND game_id IS NULL
                """
                
                with engine.begin() as conn:
                    for update in updates:
                        result = conn.execute(text(update_query), update)
                        total_updated += result.rowcount
    
    logger.info(f"\nPHASE 3 COMPLETE: {total_updated:,} rows updated, {len(unmatched_games)} unique games unmatched")
    
    if unmatched_games:
        unmatched_file = "unmatched_prop_games.csv"
        with open(unmatched_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['home_team', 'away_team', 'game_date'])
            for home_team, away_team, date_str in sorted(unmatched_games):
                writer.writerow([home_team, away_team, date_str])
        logger.info(f"Unmatched games written to {unmatched_file}")

# ============================================================================
# PLAYER LINKING - PHASE 4
# ============================================================================

def link_players(engine, player_lookup: dict, chunk_size: int, dry_run: bool = False):
    """
    Link raw_player_props_combined to player IDs.
    Only processes rows that already have a game_id.
    """
    logger.info("=" * 60)
    logger.info("PHASE 4: Linking players")
    logger.info("=" * 60)
    
    # Get count of rows to process
    count_query = """
    SELECT COUNT(*) FROM raw_player_props_combined
    WHERE game_id IS NOT NULL AND player_id IS NULL
    """
    
    with engine.connect() as conn:
        total_rows = conn.execute(text(count_query)).scalar()
    
    logger.info(f"Rows to process: {total_rows:,}")
    
    if total_rows == 0:
        logger.info("No rows to process.")
        return
    
    # Get unique player names that need matching
    unique_players_query = """
    SELECT DISTINCT api_player_name
    FROM raw_player_props_combined
    WHERE game_id IS NOT NULL 
      AND player_id IS NULL
      AND api_player_name IS NOT NULL
    """
    
    player_matches = {}  # api_name -> player_id
    unmatched_players = set()
    
    logger.info("Matching player names...")
    with engine.connect() as conn:
        result = conn.execute(text(unique_players_query))
        all_names = [row[0] for row in result]
        
        for api_name in tqdm(all_names, desc="Phase 4a: Name matching", unit="player"):
            normalized = normalize_player_name(api_name)
            
            if normalized in player_lookup:
                player_matches[api_name] = player_lookup[normalized]
            else:
                unmatched_players.add(api_name)
    
    logger.info(f"Player matching: {len(player_matches):,} matched, {len(unmatched_players):,} unmatched")
    
    # Apply updates in batches
    if player_matches and not dry_run:
        # First, update player_id based on api_player_name
        update_query = """
        UPDATE raw_player_props_combined
        SET player_id = :player_id
        WHERE api_player_name = :api_name
          AND game_id IS NOT NULL
          AND player_id IS NULL
        """
        
        total_updated = 0
        with engine.begin() as conn:
            for api_name, player_id in tqdm(player_matches.items(), 
                                             desc="Phase 4b: Updating player_id", 
                                             unit="player"):
                result = conn.execute(text(update_query), {
                    "api_name": api_name,
                    "player_id": player_id
                })
                total_updated += result.rowcount
        
        logger.info(f"Updated player_id for {total_updated:,} rows")
        
        # Now update team_id based on player_game_stats
        logger.info("Updating team_id from player_game_stats...")
        team_update_query = """
        UPDATE raw_player_props_combined rp
        SET team_id = pgs.team_id
        FROM player_game_stats pgs
        WHERE rp.player_id = pgs.player_id
          AND rp.game_id = pgs.game_id
          AND rp.team_id IS NULL
          AND rp.player_id IS NOT NULL
        """
        
        with engine.begin() as conn:
            result = conn.execute(text(team_update_query))
            logger.info(f"Updated team_id for {result.rowcount:,} rows")
    
    # Report unmatched
    if unmatched_players:
        unmatched_file = "unmatched_players.csv"
        with open(unmatched_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['api_player_name', 'normalized_name'])
            for name in sorted(unmatched_players):
                writer.writerow([name, normalize_player_name(name)])
        logger.info(f"Unmatched players written to {unmatched_file}")
        logger.info(f"Unmatched players written to {unmatched_file}")

# ============================================================================
# STATISTICS & VALIDATION
# ============================================================================

def print_statistics(engine):
    """Print current linking statistics."""
    logger.info("\n" + "=" * 60)
    logger.info("CURRENT STATISTICS")
    logger.info("=" * 60)
    
    # Game lines stats
    game_lines_query = """
    SELECT 
        COUNT(*) as total,
        COUNT(nba_game_id) as linked,
        COUNT(*) - COUNT(nba_game_id) as unlinked
    FROM raw_game_lines_staging
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(game_lines_query)).fetchone()
        logger.info(f"\nraw_game_lines_staging:")
        logger.info(f"  Total rows:    {result[0]:>12,}")
        logger.info(f"  Linked:        {result[1]:>12,} ({100*result[1]/result[0]:.1f}%)")
        logger.info(f"  Unlinked:      {result[2]:>12,}")
    
    # Player props stats
    props_query = """
    SELECT 
        COUNT(*) as total,
        COUNT(game_id) as games_linked,
        COUNT(player_id) as players_linked,
        COUNT(team_id) as teams_linked
    FROM raw_player_props_combined
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(props_query)).fetchone()
        total = result[0]
        logger.info(f"\nraw_player_props_combined:")
        logger.info(f"  Total rows:    {total:>12,}")
        logger.info(f"  game_id:       {result[1]:>12,} ({100*result[1]/total:.1f}%)")
        logger.info(f"  player_id:     {result[2]:>12,} ({100*result[2]/total:.1f}%)")
        logger.info(f"  team_id:       {result[3]:>12,} ({100*result[3]/total:.1f}%)")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Link NBA betting data to stats database')
    parser.add_argument('--phase', type=str, default='all',
                        choices=['1', '2', '3', '4', 'all', 'stats'],
                        help='Phase to run (1=lookups, 2=game_lines, 3=props_games, 4=props_players, all, stats)')
    parser.add_argument('--chunk-size', type=int, default=CHUNK_SIZE_DEFAULT,
                        help=f'Batch size for updates (default: {CHUNK_SIZE_DEFAULT})')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("NBA Data Linker Starting")
    logger.info("=" * 60)
    logger.info(f"Phase: {args.phase}, Chunk size: {args.chunk_size}, Dry run: {args.dry_run}")
    
    try:
        engine = get_engine()
        
        if args.phase == 'stats':
            print_statistics(engine)
            return
        
        # Phase 1: Build lookups (always needed)
        if args.phase in ['1', 'all']:
            team_lookup, team_reverse = build_team_lookup(engine)
            player_lookup, all_players = build_player_lookup(engine)
            game_lookup = build_game_lookup(engine)
            props_game_lookup = build_props_game_lookup(engine)
        else:
            # Build only what's needed for specific phase
            if args.phase == '2':
                game_lookup = build_game_lookup(engine)
            elif args.phase == '3':
                props_game_lookup = build_props_game_lookup(engine)
            elif args.phase == '4':
                player_lookup, _ = build_player_lookup(engine)
        
        # Phase 2: Link game lines
        if args.phase in ['2', 'all']:
            link_game_lines(engine, game_lookup, args.chunk_size, args.dry_run)
        
        # Phase 3: Link player props to games
        if args.phase in ['3', 'all']:
            link_player_props_games(engine, props_game_lookup, args.chunk_size, args.dry_run)
        
        # Phase 4: Link players
        if args.phase in ['4', 'all']:
            link_players(engine, player_lookup, args.chunk_size, args.dry_run)
        
        # Final statistics
        print_statistics(engine)
        
        elapsed = time.time() - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        hours, minutes = divmod(minutes, 60)
        
        logger.info("\n" + "=" * 60)
        if hours > 0:
            logger.info(f"[OK] Complete! Total time: {hours}h {minutes}m {seconds}s")
        elif minutes > 0:
            logger.info(f"[OK] Complete! Total time: {minutes}m {seconds}s")
        else:
            logger.info(f"[OK] Complete! Total time: {seconds}s")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()