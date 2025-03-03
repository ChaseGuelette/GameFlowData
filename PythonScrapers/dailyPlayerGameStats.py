from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

import pandas as pd
import random
import time 

from nba_api.stats.static import teams
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog

#start by creating an engine to add each dataframe to the sql database
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

#filter all players for active players
active_players = [p for p in players.get_players() if p['is_active']]


#function used inthe set_team_id function.
#determines which team is home and returns the proper team ID
def parse_team_from_matchup(matchup, is_home):
    """Extract team abbreviation from MATCHUP string
    Example: 'PHI vs. HOU' or 'PHI @ HOU'"""
    teams = matchup.replace(' vs. ', ' @ ').split(' @ ')
    return teams[0] if is_home else teams[1]

def set_team_id(games_df):

    team_mapping = {}
    nba_teams = teams.get_teams()
    for team in nba_teams:
        team_mapping[team['abbreviation']] = team['id']
    
    games_df['TEAM_ID'] = None

    # print(f"Processing {len(games_df)} records...")
    
    games_df['TEAM_ID'] = games_df.apply(  # Changed to TEAM_ID to match column name
        lambda row: team_mapping[parse_team_from_matchup(
            row['MATCHUP'], 
            '@' not in row['MATCHUP']
        )], 
        axis=1
    )


#lets define our functions for getting most recent data
def get_recent_games(player_id, days_back=20):
    # Calculate the date threshold
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    # Get games for the season
    game_log = playergamelog.PlayerGameLog(
        player_id=player_id,
        season='2024-25'
    )
    
    # Convert to dataframe
    df = game_log.get_data_frames()[0]
    
    # Convert GAME_DATE to datetime
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format='%b %d, %Y')

    set_team_id(df)
    
    # Filter for only recent games
    recent_games = df[df['GAME_DATE'] >= cutoff_date].copy()

    recent_games['GAME_DATE'] = recent_games['GAME_DATE'].dt.strftime('%b %d, %Y').str.upper()

    
    return recent_games, cutoff_date


def update_team_names(engine):

    nba_teams = teams.get_teams()
    team_dict = {team['id']: team['full_name'] for team in nba_teams}
    print("Got teams")
    
    query = "SELECT * FROM all_player_game_stats"
    with engine.connect() as conn:
        print("Fetching games from database...")
        games_df = pd.read_sql(query, conn)

    print(f"Processing {len(games_df)} records...")

    # Update database with team IDs
    with engine.connect() as conn:
        with conn.begin():
            for index, row in games_df.iterrows():
                update_query = text("""
                    UPDATE all_player_game_stats
                    SET "TEAM_NAME" = :team_name
                    WHERE "PLAYER_ID" = :player_id
                    AND "GAME_DATE" = :game_date
                """)
                conn.execute(update_query, {
                    'team_name': team_dict[row['TEAM_ID']],
                    'player_id': row['PLAYER_ID'],
                    'game_date': row['GAME_DATE']
                })
                # if index % 1000 == 0:
                #     print(f"Updated {index} records...")
    print("Update complete!")

def update_player_game_stats(engine):
    i = 1
    for player in active_players:
        player_id = player['id']
        try:
            print(f"Scraping recent games for {player['full_name']} - {i}")
            recent_games, cutoff_date = get_recent_games(player_id, days_back=20)

            if not recent_games.empty:

                
                # Don't add PLAYER_ID column since it already exists in the database
                # Just ensure we have the correct column mapping
                if 'Player_ID' in recent_games.columns:
                    recent_games = recent_games.rename(columns={'Player_ID': 'PLAYER_ID'})
                    
                #update the database 
                with engine.connect() as conn:
                    with conn.begin():
                        # Delete recent records for this player
                        delete_query = text("""
                            DELETE FROM "all_player_game_stats"
                            WHERE "PLAYER_ID" = :player_id
                            AND "GAME_DATE"::date >= :cutoff_date
                        """)
                        
                        # Execute with parameters
                        conn.execute(delete_query, 
                                    {"player_id": player_id, 
                                     "cutoff_date": cutoff_date})
                                                
                        # Append new records
                        recent_games.to_sql('all_player_game_stats', 
                                          conn, 
                                          if_exists='append', 
                                          index=False)
                    
            #handle rate limiting issues
            time.sleep(round(random.uniform(2, 3), 1))
            if i % 40 == 0:
                time.sleep(round(random.uniform(60, 120), 1))
                print()
                print("Long Sleep!")
                print()
                
            else:
                time.sleep(round(random.uniform(2, 3), 1))
            i += 1
            
        except Exception as e:
            print(f"Error updating player {player_id}: {e} - sleeping for 5 minutes")
            time.sleep(300)
    update_team_names(engine)

# update_player_stats(engine)