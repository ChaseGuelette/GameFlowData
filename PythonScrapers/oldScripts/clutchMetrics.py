from nba_api.stats.endpoints import leaguedashplayerclutch
from sqlalchemy import create_engine, text
import pandas as pd
import time
import random

def all_player_clutch_stats(engine):
    #do clutch api call
    clutch_stats = leaguedashplayerclutch.LeagueDashPlayerClutch(
        last_n_games=0,  # 0 for all games
        measure_type_detailed_defense='Base',
        month=0,  # 0 for all months
        opponent_team_id=0,  # 0 for all teams
        pace_adjust='N',
        per_mode_detailed='PerGame',
        period=0,  # 0 for all periods
        plus_minus='N',
        rank='N',
        season='2024-25',
        season_type_all_star='Regular Season',
        outcome_nullable='',
        location_nullable='',
        season_segment_nullable='',
        date_from_nullable='',
        date_to_nullable='',
        game_segment_nullable='',
        clutch_time='Last 5 Minutes',
        ahead_behind='Ahead or Behind',
        point_diff='5',
        team_id_nullable=''
    )
    
    clutch_df = clutch_stats.get_data_frames()[0]
    #clutch_df = clutch_df.set_index('PLAYER_ID')
    
    #lets get players net rating
    #clutch points per game
    #clutch effective feild goal %
    #win probabitliy added 
    #clutch usage rate 
    clutch_df['CLUTCH_SCORE_PCT'] = (clutch_df['FGM'] / clutch_df['FGA']).fillna(0)
    clutch_df['CLUTCH_USAGE_RATE'] = (clutch_df['FGA'] + 0.44 * clutch_df['FTA'] + clutch_df['TOV']) / clutch_df['MIN']
    clutch_df['CLUTCH_NET_RATING'] = clutch_df['PLUS_MINUS'] / clutch_df['GP']
    
    with engine.connect() as conn:
            #start sql transaction
            with conn.begin():
                #push table to database
                clutch_df.to_sql('player_clutch_stats', conn, if_exists='replace', index=True)