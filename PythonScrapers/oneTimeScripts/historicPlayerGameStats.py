from sqlalchemy import create_engine
from requests.exceptions import ReadTimeout
import random
import time 
import pandas as pd
import json

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog

#start by creating an engine to add each dataframe to the sql database
engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')

#filter all players for active players
active_players = [p for p in players.get_players() if p['is_active']]
print(len(active_players))

#season to gather data for 
season="2024-25"

#setup dataframe
player_stats_df = pd.DataFrame()
i = 1

for player in active_players:
    while True:
        try:
            print(f"Getting stats for {player['full_name']}")
            #get player dict and ID
            PID = player['id']
            
            #retreive player game stats
            game_log = playergamelog.PlayerGameLog(
                    player_id=PID,  # Use the player's ID from their dict
                    season='2024-25'  # Changed to current season since 2024-25 hasn't started
                )
            df = game_log.get_data_frames()[0]

            if not df.empty:
                player_stats_df = pd.concat([player_stats_df, df])
                print(f"Data retreived for {player['full_name']}")
            else:
                print(f"No games found for player")
        
            if i % 40 == 0:
                time.sleep(round(random.uniform(60, 120), 1))
                print()
                print("Long Sleep!")
                print()
                
            else:
                time.sleep(round(random.uniform(3, 4), 1))
            i += 1

            #exit loop
            break 
            
        except (ReadTimeout, json.decoder.JSONDecodeError, Exception) as e:
            print(f"Error for {player['full_name']}: {e} - retrying after 60 seconds")
            time.sleep(180)
            continue 

filename = f"all_player_game_stats"
player_stats_df.to_sql(filename, engine, if_exists='replace', index=False)
print(f"Finished gathering all data for all players for all games in {season}")