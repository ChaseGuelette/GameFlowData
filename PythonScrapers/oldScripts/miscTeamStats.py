#now I want to combine this to create a bunch of team dataframes with basic stats 
from sqlalchemy import create_engine, text
import pandas as pd

from nba_api.stats.endpoints import leaguedashteamstats

#create our engine for creating sql entries - this wont work right now 
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

def update_misc_team_stats(engine):
    #grab dataframe for teams
    stats = leaguedashteamstats.LeagueDashTeamStats()
    team_df = stats.get_data_frames()[0]
    team_df = team_df.set_index('TEAM_ID')

    with engine.connect() as conn:
        #start sql transaction
        with conn.begin():
                        # Instead of deleting, use an UPSERT pattern
            # First create a temporary table with new data
            team_df.to_sql('temp_teams_misc_stats', conn, if_exists='replace', index=True)
            
            # Then perform the UPSERT operation
            upsert_query = text("""
                INSERT INTO "all_teams_misc_stats"
                SELECT * FROM temp_teams_misc_stats
                ON CONFLICT ("TEAM_ID") 
                DO UPDATE SET
                    -- List all columns to update except TEAM_ID
                    "TEAM_NAME" = EXCLUDED."TEAM_NAME",
                    "GP" = EXCLUDED."GP",
                    "W" = EXCLUDED."W",
                    "L" = EXCLUDED."L",
                    "W_PCT" = EXCLUDED."W_PCT",
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
                    "TOV" = EXCLUDED."TOV",
                    "STL" = EXCLUDED."STL",
                    "BLK" = EXCLUDED."BLK",
                    "BLKA" = EXCLUDED."BLKA",
                    "PF" = EXCLUDED."PF",
                    "PFD" = EXCLUDED."PFD",
                    "PTS" = EXCLUDED."PTS",
                    "PLUS_MINUS" = EXCLUDED."PLUS_MINUS"
            """)
            
            # Execute the UPSERT
            conn.execute(upsert_query)
            
            # Clean up temporary table
            conn.execute(text('DROP TABLE IF EXISTS temp_teams_misc_stats'))

    #no need for the operation below because we are updating the table with sql commands:
    
    #once we've created the dataframe, we want to push that to a SQL database
    #df to sql
    #append, fail, repalce for the if_exists parameter
    # filename = f"all_teams_misc_stats"
    # team_df.to_sql(filename, engine, if_exists='append', index=True)

    print("Basic updated retrieved for all teams!")