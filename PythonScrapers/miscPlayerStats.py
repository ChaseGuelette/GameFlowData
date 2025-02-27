from sqlalchemy import create_engine, text 
from requests.exceptions import ReadTimeout
import random
import time 
import pandas as pd
import json

#full team stats, aquired and stored in a sql database
#note that the nba_api has rate limiting, so we cant go too fast
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import playercareerstats


#start by creating an engine to add each dataframe to the sql database
engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')


def update_misc_player_stats(engine):

    # get_teams returns a list of 30 dictionaries, each an NBA team.
    nba_teams = teams.get_teams()

    all_players = pd.DataFrame()

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
                                                    
                            
                            time.sleep(round(random.uniform(2, 6), 1))
                            break
                        except ReadTimeout:
                            print(f"Timeout for {player_name} - retrying after 5 seconds")
                            time.sleep(60)
                            continue
                        except (ReadTimeout, json.decoder.JSONDecodeError) as e:
                            print(f"Error for {player_name}: {e} - retrying after 60 seconds")
                            time.sleep(60)
                            continue
                #attach player names, add team roster to all players dataframe
                team_roster.insert(0, 'PLAYER_NAME', player_names)
                all_players = pd.concat([all_players, team_roster])
                print(f"DataFrame created for {team['full_name']}")
                print()
                
                break
                
            except ReadTimeout:
                print(f"Timeout for {team['full_name']} - retrying after 5 seconds")
                time.sleep(300)
                continue

    with engine.connect() as conn:
        #start sql transaction
        with conn.begin():

            all_players.to_sql('temp_players_misc_stats', conn, if_exists='replace', index=True)

            #update our data table:
            upsert_query = text("""
                INSERT INTO "all_players_misc_stats" (
                    "PLAYER_ID",
                    "PLAYER_NAME",
                    "SEASON_ID",
                    "LEAGUE_ID",
                    "TEAM_ID",
                    "TEAM_ABBREVIATION",
                    "PLAYER_AGE",
                    "GP",
                    "GS",
                    "MIN",
                    "FGM",
                    "FGA",
                    "FG_PCT",
                    "FG3M",
                    "FG3A",
                    "FG3_PCT",
                    "FTM",
                    "FTA",
                    "FT_PCT",
                    "OREB",
                    "DREB",
                    "REB",
                    "AST",
                    "STL",
                    "BLK",
                    "TOV",
                    "PF",
                    "PTS"
                )
                SELECT 
                    "PLAYER_ID",
                    "PLAYER_NAME",
                    "SEASON_ID",
                    "LEAGUE_ID",
                    "TEAM_ID",
                    "TEAM_ABBREVIATION",
                    "PLAYER_AGE",
                    "GP",
                    "GS",
                    "MIN",
                    "FGM",
                    "FGA",
                    "FG_PCT",
                    "FG3M",
                    "FG3A",
                    "FG3_PCT",
                    "FTM",
                    "FTA",
                    "FT_PCT",
                    "OREB",
                    "DREB",
                    "REB",
                    "AST",
                    "STL",
                    "BLK",
                    "TOV",
                    "PF",
                    "PTS"
                FROM temp_players_misc_stats
                ON CONFLICT ("PLAYER_ID")
                DO UPDATE SET
                    "PLAYER_NAME" = EXCLUDED."PLAYER_NAME",
                    "SEASON_ID" = EXCLUDED."SEASON_ID",
                    "LEAGUE_ID" = EXCLUDED."LEAGUE_ID",
                    "TEAM_ID" = EXCLUDED."TEAM_ID",
                    "TEAM_ABBREVIATION" = EXCLUDED."TEAM_ABBREVIATION",
                    "PLAYER_AGE" = EXCLUDED."PLAYER_AGE",
                    "GP" = EXCLUDED."GP",
                    "GS" = EXCLUDED."GS",
                    "MIN" = EXCLUDED."MIN",
                    "FGM" = EXCLUDED."FGM",
                    "FGA" = EXCLUDED."FGA",
                    "FG_PCT" = EXCLUDED."FG_PCT",
                    "FG3M" = EXCLUDED."FG3M",
                    "FG3A" = EXCLUDED."FG3A",
                    "FG3_PCT" = EXCLUDED."FG3_PCT",
                    "FTM" = EXCLUDED."FTM",
                    "FTA" = EXCLUDED."FTA",
                    "FT_PCT" = EXCLUDED."FT_PCT",
                    "OREB" = EXCLUDED."OREB",
                    "DREB" = EXCLUDED."DREB",
                    "REB" = EXCLUDED."REB",
                    "AST" = EXCLUDED."AST",
                    "STL" = EXCLUDED."STL",
                    "BLK" = EXCLUDED."BLK",
                    "TOV" = EXCLUDED."TOV",
                    "PF" = EXCLUDED."PF",
                    "PTS" = EXCLUDED."PTS"
            """)
            # Delete recent records of teams if we already added them 
            # delete_query = text("""
            # DELETE FROM "all_players_misc_stats" 
            # """)
            
            conn.execute(upsert_query)

            conn.execute(text('DROP TABLE IF EXISTS temp_players_misc_stats'))

    #fix the players who had 0 ids when we grabbed them
    fix_0_player_ids(nba_teams, engine)   


    # all_players = all_players.set_index('PLAYER_ID')
    # filename = f"all_players_misc_stats"
    # all_players.to_sql(filename, engine, if_exists='append', index=True)
    print('finished all NBA teams')



def fix_0_player_ids(nba_teams, engine):

    
    #print(nba_teams)
    big_df = pd.DataFrame()

    for team in nba_teams:
        roster = commonteamroster.CommonTeamRoster(team_id=team['id'])
        roster_df = roster.get_data_frames()[0]
        #print(f"grabbing data for {team['full_name']}")
        
        big_df = pd.concat([big_df, roster_df])
        time.sleep(round(random.uniform(2, 3), 1))

    player_dict = {
        203471: "Dennis Schröder",
        20078: "P.J. Tucker",
        1627827: "Dorian Finney-Smith",
        1629003: "Shake Milton",
        1641998: "Trey Jemison III",
        1641803: "Tristen Newton",
        1626156: "D'Angelo Russell",
        1641736: "Reece Beekman",
        1641721: "Maxwell Lewis",
        202711: "Bojan Bogdanović",
        1628418: "Thomas Bryant",
        1630208: "Nick Richards",
        1631115: "Orlando Robinson",
        1629006: "Josh Okogie"
    }

    for player in player_dict:
        player_id = player
        
        player_stats = big_df[big_df['PLAYER_ID'] == player_id]
        #print(player_stats)
        
        if not player_stats.empty:
            team_id = int(player_stats['TeamID'].iloc[0])
        else:
            print(f"No player found with ID {player_id}")    
        
        with engine.connect() as conn:
            with conn.begin():
                update_query = text("""
                    UPDATE "all_players_misc_stats"
                    SET "TEAM_ID" = :team_id
                    WHERE "PLAYER_ID" = :player_id
                """)
                
                conn.execute(update_query, {"team_id" : team_id, "player_id" : player_id})