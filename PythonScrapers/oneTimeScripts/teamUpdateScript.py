from nba_api.stats.static import teams
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


def parse_team_from_matchup(matchup, is_home):
    """Extract team abbreviation from MATCHUP string
    Example: 'PHI vs. HOU' or 'PHI @ HOU'"""
    teams = matchup.replace(' vs. ', ' @ ').split(' @ ')
    return teams[0] if is_home else teams[1]
    
def update_team_ids(engine):

    team_mapping = {}
    nba_teams = teams.get_teams()
    for team in nba_teams:
        team_mapping[team['abbreviation']] = team['id']

    print("Got team mappings...")
    
    query = "SELECT * FROM all_player_game_stats"
    with engine.connect() as conn:
        print("Fetching games from database...")
        games_df = pd.read_sql(query, conn)

    print(f"Processing {len(games_df)} records...")
    
    games_df['TEAM_ID'] = games_df.apply(  # Changed to TEAM_ID to match column name
        lambda row: team_mapping[parse_team_from_matchup(
            row['MATCHUP'], 
            '@' not in row['MATCHUP']
        )], 
        axis=1
    )

        # Update database with team IDs
    with engine.connect() as conn:
        with conn.begin():
            i = 0
            for index, row in games_df.iterrows():
                update_query = text("""
                    UPDATE all_player_game_stats
                    SET "TEAM_ID" = :team_id
                    WHERE "PLAYER_ID" = :player_id
                    AND "GAME_DATE" = :game_date
                """)
                conn.execute(update_query, {
                    'team_id': row['TEAM_ID'],
                    'player_id': row['PLAYER_ID'],
                    'game_date': row['GAME_DATE']
                })
                if i % 1000 == 0:
                    print(f"Updated {index} records...")
                i += 1

    print("Update complete!")


DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
update_team_ids(engine)