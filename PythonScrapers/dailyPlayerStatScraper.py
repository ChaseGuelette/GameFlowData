from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

import pandas as pd
import random
import time 

from nba_api.stats.static import teams
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog

# from dotenv import load_dotenv
# import os

# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")
# engine = create_engine(DATABASE_URL)

def normalize_for_postgres(df):
    """Normalize DataFrame column names for PostgreSQL"""
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

# Filter all players for active players
active_players = [p for p in players.get_players() if p['is_active']]

# Function used in the set_team_id function
# Determines which team is home and returns the proper team ID
def parse_team_from_matchup(matchup, is_home):
    """Extract team abbreviation from MATCHUP string
    Example: 'PHI vs. HOU' or 'PHI @ HOU'"""
    teams_in_matchup = matchup.replace(' vs. ', ' @ ').split(' @ ')
    return teams_in_matchup[0] if is_home else teams_in_matchup[1]

def set_team_id(games_df):
    """Add TEAM_ID column based on MATCHUP string"""
    team_mapping = {}
    nba_teams = teams.get_teams()
    for team in nba_teams:
        team_mapping[team['abbreviation']] = team['id']
    
    # Determine if game is home or away based on @ symbol
    games_df['TEAM_ID'] = games_df.apply(
        lambda row: team_mapping[parse_team_from_matchup(
            row['MATCHUP'], 
            '@' not in row['MATCHUP']  # True if home game (vs.), False if away (@)
        )], 
        axis=1
    )
    
    return games_df

def get_recent_games(player_id, days_back=20):
    """Get recent games for a player"""
    # Calculate the date threshold
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    # Get games for the current season
    game_log = playergamelog.PlayerGameLog(
        player_id=player_id,
        season='2024-25'
    )
    
    # Convert to dataframe
    df = game_log.get_data_frames()[0]
    
    # Convert GAME_DATE to datetime
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format='%b %d, %Y')
    
    # Add TEAM_ID based on matchup
    df = set_team_id(df)
    
    # Filter for only recent games
    recent_games = df[df['GAME_DATE'] >= cutoff_date]
    
    return recent_games, cutoff_date

def update_player_stats(engine):
    """Update database with recent player game stats"""
    successful_updates = 0
    failed_updates = 0
    
    for i, player in enumerate(active_players, 1):
        player_id = player['id']
        player_name = player['full_name']
        
        try:
            print(f"[{i}/{len(active_players)}] Scraping recent games for {player_name}...")
            recent_games, cutoff_date = get_recent_games(player_id, days_back=20)

            if not recent_games.empty:
                print(f"  Found {len(recent_games)} recent games")
                
                # Normalize column names for PostgreSQL
                recent_games = normalize_for_postgres(recent_games)
                
                # Update the database 
                with engine.connect() as conn:
                    with conn.begin():
                        # Delete recent records for this player (using lowercase column names)
                        delete_query = text("""
                            DELETE FROM player_game_stats
                            WHERE player_id = :player_id
                            AND game_date::date >= :cutoff_date
                        """)
                        
                        result = conn.execute(delete_query, 
                                    {"player_id": player_id, 
                                     "cutoff_date": cutoff_date})
                        
                        deleted_count = result.rowcount
                        if deleted_count > 0:
                            print(f"  Deleted {deleted_count} old records")
                                                
                        # Append new records
                        recent_games.to_sql('player_game_stats', 
                                          conn, 
                                          if_exists='append', 
                                          index=False)
                        
                        print(f"  ✓ Successfully updated {len(recent_games)} records")
                        successful_updates += 1
            else:
                print(f"  - No recent games found")
                
            # Handle rate limiting
            if i % 40 == 0:
                sleep_time = round(random.uniform(60, 90), 1)
                print(f"\n  === Long sleep ({sleep_time}s) after 40 players ===\n")
                time.sleep(sleep_time)
            else:
                time.sleep(round(random.uniform(2, 3), 1))
            
        except Exception as e:
            print(f"  ✗ Error updating player {player_id} ({player_name}): {e}")
            failed_updates += 1
            time.sleep(round(random.uniform(2, 3), 1))
    
    # Print summary
    print("\n" + "=" * 60)
    print("UPDATE SUMMARY")
    print("=" * 60)
    print(f"Total players processed: {len(active_players)}")
    print(f"Successful updates: {successful_updates}")
    print(f"Failed updates: {failed_updates}")
    print("=" * 60)

# Run the update
# update_player_stats(engine)