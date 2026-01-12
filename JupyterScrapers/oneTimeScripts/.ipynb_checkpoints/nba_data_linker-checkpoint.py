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

CHUNK_SIZE_DEFAULT = 2000  # Small chunks for Supabase free tier
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
    
    # For Supabase Transaction Mode (port 6543):
    # - Disable prepared statements (PgBouncer doesn't support them)
    # - Use smaller pool to avoid hogging connections
    engine = create_engine(
        database_url,
        pool_size=2,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=1800,
        connect_args={
            "prepare_threshold": None,  # REQUIRED for port 6543
            "connect_timeout": 30
        }
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
    
    # Dead simple query - no WHERE, no JOIN
    query = "SELECT game_id, team_name, team_id, team_game_date, team_matchup, opponent_id FROM team_game_stats"
    
    game_lookup = {}
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            game_id, team_name, team_id, game_date, matchup, opponent_id = row
            
            # Home games have 'vs.' in matchup
            if game_date and matchup and 'vs.' in matchup:
                date_str = str(game_date)[:10]
                
                key = (team_name, date_str)
                game_lookup[key] = (game_id, team_id, opponent_id)
                
                # Add aliases
                for alias, canonical in TEAM_NAME_ALIASES.items():
                    if canonical == team_name:
                        game_lookup[(alias, date_str)] = (game_id, team_id, opponent_id)
    
    logger.info(f"Game lookup built: {len(game_lookup)} home team/date combinations")
    return game_lookup

def link_game_lines(engine, game_lookup: dict, chunk_size: int, dry_run: bool = False):
    """
    Link raw_game_lines_staging to NBA game IDs.
    Uses staging_id batching to avoid expensive WHERE clause computations.
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: Linking raw_game_lines_staging")
    logger.info("=" * 60)
    
    # Get ID range (full table - we filter NULLs in batches)
    range_query = """
    SELECT MIN(staging_id), MAX(staging_id)
    FROM raw_game_lines_staging
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(range_query)).fetchone()
        min_id, max_id = result[0], result[1]
    
    if not min_id or not max_id:
        logger.info("No game lines found.")
        return
    
    logger.info(f"Processing staging_id {min_id} to {max_id}")
    
    total_updated = 0
    unmatched_games = set()
    
    # Process in batches by staging_id
    current_min = min_id
    total_batches = (max_id - min_id) // chunk_size + 1
    
    with tqdm(total=total_batches, desc="Phase 2: Game Lines", unit="batch",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} batches [{elapsed}<{remaining}]') as pbar:
        
        while current_min <= max_id:
            # Fetch batch of unlinked rows
            fetch_query = """
            SELECT staging_id, home_team, 
                   (commence_time AT TIME ZONE 'America/New_York')::date as game_date
            FROM raw_game_lines_staging
            WHERE staging_id >= :min_id 
              AND staging_id < :max_id
              AND nba_game_id IS NULL
              AND home_team IS NOT NULL
            """
            
            batch_max = current_min + chunk_size
            updates = []
            
            with engine.connect() as conn:
                result = conn.execute(text(fetch_query), {
                    "min_id": current_min,
                    "max_id": batch_max
                })
                
                rows_in_batch = 0
                for row in result:
                    rows_in_batch += 1
                    staging_id, home_team, game_date = row
                    date_str = str(game_date)
                    
                    # Normalize and lookup
                    normalized_home = normalize_team_name(home_team)
                    key = (normalized_home, date_str)
                    
                    if key in game_lookup:
                        game_id, home_id, away_id = game_lookup[key]
                        updates.append({
                            "staging_id": staging_id,
                            "game_id": game_id,
                            "home_id": home_id,
                            "away_id": away_id
                        })
                    else:
                        unmatched_games.add((home_team, date_str))
            
            # Batch update by staging_id (fast, indexed primary key)
            if updates and not dry_run:
                update_query = """
                UPDATE raw_game_lines_staging
                SET nba_game_id = :game_id,
                    nba_home_team_id = :home_id,
                    nba_away_team_id = :away_id
                WHERE staging_id = :staging_id
                """
                
                with engine.begin() as conn:
                    conn.execute(text(update_query), updates)
                    total_updated += len(updates)
            
            pbar.update(1)
            pbar.set_postfix({'updated': f'{total_updated:,}', 'unmatched': len(unmatched_games)})
            
            current_min = batch_max
    
    logger.info(f"\nPHASE 2 COMPLETE: {total_updated:,} rows updated, {len(unmatched_games)} unique games unmatched")
    
    if unmatched_games:
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
    Key: (home_team, away_team, game_date_str)
    Value: game_id
    """
    logger.info("Building game lookup for player props...")
    
    # First get team_id -> team_name mapping
    teams_query = "SELECT team_id, team_name FROM teams"
    team_names = {}
    with engine.connect() as conn:
        result = conn.execute(text(teams_query))
        for row in result:
            team_names[row[0]] = row[1]
    
    logger.info(f"  Loaded {len(team_names)} teams")
    
    # Now get all games - no WHERE clause
    games_query = "SELECT game_id, team_name, team_game_date, team_matchup, opponent_id FROM team_game_stats"
    
    game_lookup = {}
    
    with engine.connect() as conn:
        result = conn.execute(text(games_query))
        for row in result:
            game_id, home_team, game_date, matchup, opponent_id = row
            
            # Home games have 'vs.' in matchup
            if game_date and matchup and 'vs.' in matchup and opponent_id in team_names:
                date_str = str(game_date)[:10]
                away_team = team_names[opponent_id]
                
                key = (home_team, away_team, date_str)
                game_lookup[key] = game_id
                
                # Add aliases
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
    Uses staging_id batching to avoid expensive WHERE clause computations.
    """
    logger.info("=" * 60)
    logger.info("PHASE 3: Linking player props to games")
    logger.info("=" * 60)
    
    # Get ID range (full table - we filter NULLs in batches)
    range_query = """
    SELECT MIN(staging_id), MAX(staging_id)
    FROM raw_player_props_combined
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(range_query)).fetchone()
        min_id, max_id = result[0], result[1]
    
    if not min_id or not max_id:
        logger.info("No player props found.")
        return
    
    logger.info(f"Processing staging_id {min_id} to {max_id}")
    
    total_updated = 0
    unmatched_games = set()
    
    # Process in batches by staging_id
    current_min = min_id
    total_batches = (max_id - min_id) // chunk_size + 1
    
    with tqdm(total=total_batches, desc="Phase 3: Props->Games", unit="batch",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} batches [{elapsed}<{remaining}]') as pbar:
        
        while current_min <= max_id:
            # Fetch batch of unlinked rows
            fetch_query = """
            SELECT staging_id, home_team, away_team,
                   (commence_time AT TIME ZONE 'America/New_York')::date as game_date
            FROM raw_player_props_combined
            WHERE staging_id >= :min_id 
              AND staging_id < :max_id
              AND game_id IS NULL
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
            """
            
            batch_max = current_min + chunk_size
            updates = []
            
            with engine.connect() as conn:
                result = conn.execute(text(fetch_query), {
                    "min_id": current_min,
                    "max_id": batch_max
                })
                
                rows_in_batch = 0
                for row in result:
                    rows_in_batch += 1
                    staging_id, home_team, away_team, game_date = row
                    date_str = str(game_date)
                    
                    # Normalize and lookup
                    norm_home = normalize_team_name(home_team)
                    norm_away = normalize_team_name(away_team)
                    key = (norm_home, norm_away, date_str)
                    
                    if key in game_lookup:
                        updates.append({
                            "staging_id": staging_id,
                            "game_id": game_lookup[key]
                        })
                    else:
                        unmatched_games.add((home_team, away_team, date_str))
            
            # Batch update by staging_id (fast, indexed primary key)
            if updates and not dry_run:
                update_query = """
                UPDATE raw_player_props_combined
                SET game_id = :game_id
                WHERE staging_id = :staging_id
                """
                
                with engine.begin() as conn:
                    conn.execute(text(update_query), updates)
                    total_updated += len(updates)
            
            pbar.update(1)
            pbar.set_postfix({'updated': f'{total_updated:,}', 'unmatched': len(unmatched_games)})
            
            current_min = batch_max
    
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
    Link raw_player_props_combined to player IDs AND team IDs safely.
    Uses staging_id batching to avoid massive table scans.
    """
    logger.info("=" * 60)
    logger.info("PHASE 4: Linking players & teams (Batched)")
    logger.info("=" * 60)

    # 1. Build a helper lookup for (player_id, game_id) -> team_id
    logger.info("Building player->team history lookup...")
    player_team_map = {}  # Key: (player_id, game_id), Value: team_id
    
    stats_query = "SELECT player_id, game_id, team_id FROM player_game_stats"
    
    with engine.connect() as conn:
        result = conn.execute(text(stats_query))
        for row in result:
            player_team_map[(row[0], row[1])] = row[2]
            
    logger.info(f"Loaded {len(player_team_map):,} player-game-team combinations")

    # 2. Get ID range
    range_query = "SELECT MIN(staging_id), MAX(staging_id) FROM raw_player_props_combined"
    with engine.connect() as conn:
        result = conn.execute(text(range_query)).fetchone()
        min_id, max_id = result[0], result[1]

    if not min_id or not max_id:
        logger.info("No rows to process.")
        return

    logger.info(f"Processing staging_id {min_id} to {max_id}")

    # 3. Process in batches
    current_min = min_id
    total_batches = (max_id - min_id) // chunk_size + 1
    total_updated = 0
    unmatched_players = set()

    with tqdm(total=total_batches, desc="Phase 4: Linking Players", unit="batch",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} batches [{elapsed}<{remaining}]') as pbar:
        while current_min <= max_id:
            batch_max = current_min + chunk_size
            
            # Fetch unlinked rows that have a game_id (from Phase 3)
            fetch_query = """
            SELECT staging_id, api_player_name, game_id
            FROM raw_player_props_combined
            WHERE staging_id >= :min_id 
              AND staging_id < :max_id
              AND game_id IS NOT NULL
              AND player_id IS NULL
            """
            
            updates = []
            
            with engine.connect() as conn:
                result = conn.execute(text(fetch_query), {"min_id": current_min, "max_id": batch_max})
                
                for row in result:
                    staging_id, api_name, game_id = row
                    normalized = normalize_player_name(api_name)
                    
                    if normalized in player_lookup:
                        p_id = player_lookup[normalized]
                        
                        # Look up team_id for this specific game
                        t_id = player_team_map.get((p_id, game_id))
                        
                        updates.append({
                            "staging_id": staging_id,
                            "player_id": p_id,
                            "team_id": t_id  # Might be None
                        })
                    else:
                        unmatched_players.add(api_name)

            # Bulk update by staging_id (fast, indexed)
            if updates and not dry_run:
                update_query = """
                UPDATE raw_player_props_combined
                SET player_id = :player_id,
                    team_id = :team_id
                WHERE staging_id = :staging_id
                """
                with engine.begin() as conn:
                    conn.execute(text(update_query), updates)
                    total_updated += len(updates)

            pbar.update(1)
            pbar.set_postfix({'updated': f'{total_updated:,}', 'unmatched': len(unmatched_players)})
            current_min = batch_max

    logger.info(f"\nPHASE 4 COMPLETE: {total_updated:,} rows updated, {len(unmatched_players)} unmatched players")
    
    # Export unmatched
    if unmatched_players:
        unmatched_file = "unmatched_players.csv"
        with open(unmatched_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['api_player_name', 'normalized_name'])
            for name in sorted(unmatched_players):
                writer.writerow([name, normalize_player_name(name)])
        logger.info(f"Unmatched players written to {unmatched_file}")

# ============================================================================
# STATISTICS & VALIDATION
# ============================================================================

def print_statistics(engine):
    """Print current linking statistics."""
    logger.info("\n" + "=" * 60)
    logger.info("CURRENT STATISTICS")
    logger.info("=" * 60)
    logger.info("(Run separate COUNT queries in Supabase dashboard for exact numbers)")
    logger.info("Linking complete - check unmatched_*.csv files for any issues")

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