from sqlalchemy import create_engine
from requests.exceptions import ReadTimeout
import random
import time 
import pandas as pd

#full team stats, aquired and stored in a sql database
#note that the nba_api has rate limiting, so we cant go too fast
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import playercareerstats


#start by creating an engine to add each dataframe to the sql database

from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
#now I iterate through a list of all of the NBA teams
# get_teams returns a list of 30 dictionaries, each an NBA team.
nba_teams = teams.get_teams()

for team in nba_teams:
    #print(team['id'])
    #print(team['full_name'])
    #print(team)
    while True:
        try:
            print(f"Getting roster for {team['full_name']} ---------------")
            roster = commonteamroster.CommonTeamRoster(team_id=team['id'])
            roster_df = roster.get_data_frames()[0]

            #create an empty team roster 
            team_roster = pd.DataFrame()
            player_names = []

            #next, as we loop through that, i want to pull their career statistics
            for index, row in roster_df.iterrows():
                
                player_id = row['PLAYER_ID']
                player_name = row['PLAYER']
                age = row['AGE'] 
                player_names.append(player_name)
                
                while True:
                    try:
                        print(f"getting stats for {player_name}")
                        stats = playercareerstats.PlayerCareerStats(player_id=player_id)
                        player_stats = stats.get_data_frames()[0]
                        
                        #ok now we need to process the data we are pulling 
                        if not player_stats.empty:
                            most_recent_season = pd.DataFrame([player_stats.iloc[-1]])
                            team_roster = pd.concat([team_roster, most_recent_season])
                        else:
                            print(f"No stats found for player")
                            player_names.pop()
                                                
                        
                        time.sleep(round(random.uniform(0.5, 1.5), 1))
                        break
                    except ReadTimeout:
                        print(f"Timeout for {player_name} - retrying after 5 seconds")
                        time.sleep(60)
                        continue
            
            team_roster.insert(0, 'PLAYER_NAME', player_names)
            print(f"DataFrame created for {team['full_name']}")

            #once we've created the dataframe, we want to push that to a SQL database
            #df to sql
            #append, fail, repalce for the if_exists parameter
            filename = f"{team['full_name']}_team_roster"
            team_roster.to_sql(filename, engine, if_exists='replace', index=False)
            print()
            
            break
            
        except ReadTimeout:
            print(f"Timeout for {team['full_name']} - retrying after 5 seconds")
            time.sleep(60)
            continue
            
    
print('finished all NBA teams')