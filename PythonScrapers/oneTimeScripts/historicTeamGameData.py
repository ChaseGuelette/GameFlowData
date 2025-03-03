#this is likely the endpoint we will use:
from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy import create_engine
from requests.exceptions import ReadTimeout
import pandas as pd
import random
import time 
import json

#create our engine for pushing to sql database
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

#define seasons to gather data for 
seasons = ['2022-23', '2023-24', '2024-25']

for season in seasons:
    try:
        # Get all games for a season
        game_finder = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,  # Format: YYYY-YY
            league_id_nullable='00',    # 00 is NBA
            season_type_nullable='Regular Season'  # or 'Playoffs'
        )
        
        # Convert to DataFrame
        games_df = game_finder.get_data_frames()[0]
        
        #make team ID the index, and sort the dataframe based on ID and gameDate
        games_df = games_df.set_index('TEAM_ID')
        games_df_sorted = games_df.sort_values(['TEAM_ID', 'GAME_DATE'], ascending=[True, False])
    
        #once we've created the dataframe, we want to push that to a SQL database
        filename = f"{season}_historic_game_data"
        games_df_sorted.to_sql(filename, engine, if_exists='replace', index=True)
        print(f"Created and stored dataframe for {season} season")
        print()
        time.sleep(round(random.uniform(3, 6), 1))

    #handle timeout/api rejection errors
    except (ReadTimeout, json.decoder.JSONDecodeError) as e:
        print(f"Error for {season}: {e} - retrying after 60 seconds")
        time.sleep(60)
        continue

print("Finished data gathering for specified seasons")