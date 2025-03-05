from sqlalchemy import create_engine
from requests.exceptions import ReadTimeout
import random
import time 
import pandas as pd
import json

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog

#start by creating an engine to add each dataframe to the sql database
from dotenv import load_dotenv
import os
from nba_api.stats.endpoints import teamyearbyyearstats, commonteamroster
from nba_api.stats.static import teams
import time
import random

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

#filter all players for active players
#active_players = [p for p in players.get_players() if p['is_active']]
# Get all teams
print("Fetching all NBA teams...")
nba_teams = teams.get_teams()
print(f"Found {len(nba_teams)} teams in total")

all_players = []
team_count = 0
active_teams = 0

for team in nba_teams:
    team_id = team['id']
    team_name = team['full_name']
    team_count += 1
    
    print(f"[{team_count}/{len(nba_teams)}] Processing team: {team_name} (ID: {team_id})")
    
    try:
        # Check if the team was active that season
        print(f"  Fetching season history for {team_name}...")
        team_seasons = teamyearbyyearstats.TeamYearByYearStats(team_id=team_id)
        seasons_df = team_seasons.get_data_frames()[0]
        
        # Add a sleep to avoid rate limiting
        sleep_time = random.uniform(1.5, 3.0)
        print(f"  Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)
        
        if '2020-21' in seasons_df['YEAR'].values:
            active_teams += 1            
            # Get team roster for that season
            print(f"  Fetching 2020-21 roster for {team_name}...")
            roster = commonteamroster.CommonTeamRoster(
                team_id=team_id,
                season='2020-21'
            )
            roster_df = roster.get_data_frames()[0]
            
            player_count = len(roster_df)
            all_players.extend(roster_df.to_dict('records'))
            print(f"  ✓ Added {player_count} players from {team_name}")
            
            # Add a longer sleep after successful data retrieval
            # sleep_time = random.uniform(2.0, 4.0)
            # print(f"  Sleeping for {sleep_time:.2f} seconds...")
            # time.sleep(sleep_time)
        else:
            print(f"  ✗ {team_name} was NOT active in 2020-21 season")
    
    except Exception as e:
        print(f"  ⚠ Error processing {team_name}: {str(e)}")
    
    # Add a separator for readability
    print("-" * 50)
    
    # Add a longer pause every 5 teams to be extra safe with rate limits
    # if team_count % 5 == 0:
    #     long_sleep = random.uniform(10.0, 15.0)
    #     print(f"Taking a longer break after processing 5 teams... ({long_sleep:.2f} seconds)")
    #     time.sleep(long_sleep)
    #     print("-" * 50)

print("\nSummary:")
print(f"Processed {team_count} total teams")
print(f"Found {active_teams} teams active in the 2020-21 season")
print(f"Collected data for {len(all_players)} players")
print(len(all_players))

#season to gather data for 
season="2020-21"

#setup dataframe
player_stats_df = pd.DataFrame()
i = 1

for player in all_players:
    while True:
        try:
            print(f"Getting stats for {player['PLAYER']}")
            #get player dict and ID
            PID = player['PLAYER_ID']
            
            #retreive player game stats
            game_log = playergamelog.PlayerGameLog(
                    player_id=PID,  # Use the player's ID from their dict
                    season='2020-21'  # Changed to current season since 2024-25 hasn't started
                )
            df = game_log.get_data_frames()[0]

            if not df.empty:
                player_stats_df = pd.concat([player_stats_df, df])
                print(f"Data retreived for {player['PLAYER']}")
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
            print(f"Error for {player['PLAYER']}: {e} - retrying after 60 seconds")
            time.sleep(180)
            continue 

filename = f"all_player_game_stats_2020-21"
player_stats_df.to_sql(filename, engine, if_exists='replace', index=False)
print(f"Finished gathering all data for all players for all games in {season}")