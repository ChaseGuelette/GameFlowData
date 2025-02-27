from sqlalchemy import create_engine, text
from requests.exceptions import ReadTimeout
from datetime import datetime, timedelta

import pandas as pd
import random
import time 
import json

from nba_api.stats.endpoints import leaguegamefinder

# Create engine
engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')

def update_daily_team_game_data(engine):
    
    # Calculate dates for recent games (e.g., last 3 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    season = '2024-25'
    
    try:
        # Get recent games
        game_finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=start_date.strftime('%m/%d/%Y'),
            date_to_nullable=end_date.strftime('%m/%d/%Y'),
            league_id_nullable='00',
            season_type_nullable='Regular Season'
        )
        
        # Convert to DataFrame
        games_df = game_finder.get_data_frames()[0]
        
        if not games_df.empty:
            # Sort the dataframe
            games_df = games_df.set_index('TEAM_ID')
            games_df_sorted = games_df.sort_values(['TEAM_ID', 'GAME_DATE'], ascending=[True, False])
    
             #update the database, connect to database
            with engine.connect() as conn:
                #start sql transaction
                with conn.begin():
                    # Delete recent records of games if we already added them 
                    delete_query = text("""
                    DELETE FROM "2024-25_historic_game_data"
                    WHERE "GAME_DATE" >= :start_date
                    """)
                    conn.execute(delete_query, {
                        'start_date': start_date.strftime('%Y-%m-%d')
                    })
                    
                    # Append new records
                    games_df_sorted.to_sql('2024-25_historic_game_data', 
                                      conn, 
                                      if_exists='append', 
                                      index=True)
                
            print(f"Added new games to {season} season table")
            print("Finished updating recent games")
        else:
            print("No new games found in date range")
    
    except (ReadTimeout, json.decoder.JSONDecodeError) as e:
        print(f"Error: {e} - please try again later")
        time.sleep(60)

# update_daily_team_game_data(engine)