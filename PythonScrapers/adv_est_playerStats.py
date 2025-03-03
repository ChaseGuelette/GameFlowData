from nba_api.stats.endpoints import playercareerstats
from sqlalchemy import create_engine, text
from nba_api.stats.endpoints import leaguedashplayerstats
from nba_api.stats.endpoints import playerestimatedmetrics
from nba_api.stats.endpoints import playerdashboardbyyearoveryear
import pandas as pd


def update_estimated_player_stats(engine):
    #get metric data 
    metrics = playerestimatedmetrics.PlayerEstimatedMetrics()
    metrics_df = metrics.get_data_frames()[0]
    metrics_df.set_index('PLAYER_ID')

    with engine.connect() as conn:
        with conn.begin():
            # Create temporary table with new data
            metrics_df.to_sql('temp_players_estimated_stats', conn, if_exists='replace', index=True)
            
            # Perform UPSERT operation
            upsert_query = text("""
                INSERT INTO "all_players_estimated_stats"
                SELECT * FROM temp_players_estimated_stats
                ON CONFLICT ("PLAYER_ID") 
                DO UPDATE SET
                    "index" = EXCLUDED."index",
                    "PLAYER_NAME" = EXCLUDED."PLAYER_NAME",
                    "GP" = EXCLUDED."GP",
                    "W" = EXCLUDED."W",
                    "L" = EXCLUDED."L",
                    "W_PCT" = EXCLUDED."W_PCT",
                    "MIN" = EXCLUDED."MIN",
                    "E_OFF_RATING" = EXCLUDED."E_OFF_RATING",
                    "E_DEF_RATING" = EXCLUDED."E_DEF_RATING",
                    "E_NET_RATING" = EXCLUDED."E_NET_RATING",
                    "E_AST_RATIO" = EXCLUDED."E_AST_RATIO",
                    "E_OREB_PCT" = EXCLUDED."E_OREB_PCT",
                    "E_DREB_PCT" = EXCLUDED."E_DREB_PCT",
                    "E_REB_PCT" = EXCLUDED."E_REB_PCT",
                    "E_TOV_PCT" = EXCLUDED."E_TOV_PCT",
                    "E_USG_PCT" = EXCLUDED."E_USG_PCT",
                    "E_PACE" = EXCLUDED."E_PACE",
                    "GP_RANK" = EXCLUDED."GP_RANK",
                    "W_RANK" = EXCLUDED."W_RANK",
                    "L_RANK" = EXCLUDED."L_RANK",
                    "W_PCT_RANK" = EXCLUDED."W_PCT_RANK",
                    "MIN_RANK" = EXCLUDED."MIN_RANK",
                    "E_OFF_RATING_RANK" = EXCLUDED."E_OFF_RATING_RANK",
                    "E_DEF_RATING_RANK" = EXCLUDED."E_DEF_RATING_RANK",
                    "E_NET_RATING_RANK" = EXCLUDED."E_NET_RATING_RANK",
                    "E_AST_RATIO_RANK" = EXCLUDED."E_AST_RATIO_RANK",
                    "E_OREB_PCT_RANK" = EXCLUDED."E_OREB_PCT_RANK",
                    "E_DREB_PCT_RANK" = EXCLUDED."E_DREB_PCT_RANK",
                    "E_REB_PCT_RANK" = EXCLUDED."E_REB_PCT_RANK",
                    "E_TOV_PCT_RANK" = EXCLUDED."E_TOV_PCT_RANK",
                    "E_USG_PCT_RANK" = EXCLUDED."E_USG_PCT_RANK",
                    "E_PACE_RANK" = EXCLUDED."E_PACE_RANK"
            """)
            
            # Execute UPSERT
            conn.execute(upsert_query)
            
            # Clean up temporary table
            conn.execute(text('DROP TABLE IF EXISTS temp_players_estimated_stats'))

    print('Estimated metrics updated for all players')



def update_advanced_player_stats(engine):
    advanced_stats = leaguedashplayerstats.LeagueDashPlayerStats(
        measure_type_detailed_defense='Advanced',
        per_mode_detailed='PerGame',
        season='2024-25',
        season_type_all_star='Regular Season'
    )
    advanced_df = advanced_stats.get_data_frames()[0]
    advanced_df.set_index('PLAYER_ID')

    with engine.connect() as conn:
        with conn.begin():
            # Create temporary table with new data
            advanced_df.to_sql('temp_players_advanced_stats', conn, if_exists='replace', index=True)
            
            # Perform UPSERT operation
            upsert_query = text("""
                INSERT INTO "all_players_advanced_stats"
                SELECT * FROM temp_players_advanced_stats
                ON CONFLICT ("PLAYER_ID") 
                DO UPDATE SET
                    "index" = EXCLUDED."index",
                    "PLAYER_NAME" = EXCLUDED."PLAYER_NAME",
                    "NICKNAME" = EXCLUDED."NICKNAME",
                    "TEAM_ID" = EXCLUDED."TEAM_ID",
                    "TEAM_ABBREVIATION" = EXCLUDED."TEAM_ABBREVIATION",
                    "AGE" = EXCLUDED."AGE",
                    "GP" = EXCLUDED."GP",
                    "W" = EXCLUDED."W",
                    "L" = EXCLUDED."L",
                    "W_PCT" = EXCLUDED."W_PCT",
                    "MIN" = EXCLUDED."MIN",
                    "E_OFF_RATING" = EXCLUDED."E_OFF_RATING",
                    "OFF_RATING" = EXCLUDED."OFF_RATING",
                    "sp_work_OFF_RATING" = EXCLUDED."sp_work_OFF_RATING",
                    "E_DEF_RATING" = EXCLUDED."E_DEF_RATING",
                    "DEF_RATING" = EXCLDED."DEF_RATING",
                    "sp_work_DEF_RATING" = EXCLUDED."sp_work_DEF_RATING",
                    "E_NET_RATING" = EXCLUDED."E_NET_RATING",
                    "NET_RATING" = EXCLUDED."NET_RATING",
                    "sp_work_NET_RATING" = EXCLUDED."sp_work_NET_RATING",
                    "AST_PCT" = EXCLUDED."AST_PCT",
                    "AST_TO" = EXCLUDED."AST_TO",
                    "AST_RATIO" = EXCLUDED."AST_RATIO",
                    "OREB_PCT" = EXCLUDED."OREB_PCT",
                    "DREB_PCT" = EXCLUDED."DREB_PCT",
                    "REB_PCT" = EXCLUDED."REB_PCT",
                    "TM_TOV_PCT" = EXCLUDED."TM_TOV_PCT",
                    "E_TOV_PCT" = EXCLUDED."E_TOV_PCT",
                    "EFG_PCT" = EXCLUDED."EFG_PCT",
                    "TS_PCT" = EXCLUDED."TS_PCT",
                    "USG_PCT" = EXCLUDED."USG_PCT",
                    "E_USG_PCT" = EXCLUDED."E_USG_PCT",
                    "E_PACE" = EXCLUDED."E_PACE",
                    "PACE" = EXCLUDED."PACE",
                    "PACE_PER40" = EXCLUDED."PACE_PER40",
                    "sp_work_PACE" = EXCLUDED."sp_work_PACE",
                    "PIE" = EXCLUDED."PIE",
                    "POSS" = EXCLUDED."POSS",
                    "FGM" = EXCLUDED."FGM",
                    "FGA" = EXCLUDED."FGA",
                    "FGM_PG" = EXCLUDED."FGM_PG",
                    "FGA_PG" = EXCLUDED."FGA_PG",
                    "FG_PCT" = EXCLUDED."FG_PCT",
                    "GP_RANK" = EXCLUDED."GP_RANK",
                    "W_RANK" = EXCLUDED."W_RANK",
                    "L_RANK" = EXCLUDED."L_RANK",
                    "W_PCT_RANK" = EXCLUDED."W_PCT_RANK",
                    "MIN_RANK" = EXCLUDED."MIN_RANK",
                    "E_OFF_RATING_RANK" = EXCLUDED."E_OFF_RATING_RANK",
                    "OFF_RATING_RANK" = EXCLUDED."OFF_RATING_RANK",
                    "sp_work_OFF_RATING_RANK" = EXCLUDED."sp_work_OFF_RATING_RANK",
                    "E_DEF_RATING_RANK" = EXCLUDED."E_DEF_RATING_RANK",
                    "DEF_RATING_RANK" = EXCLUDED."DEF_RATING_RANK",
                    "sp_work_DEF_RATING_RANK" = EXCLUDED."sp_work_DEF_RATING_RANK",
                    "E_NET_RATING_RANK" = EXCLUDED."E_NET_RATING_RANK",
                    "NET_RATING_RANK" = EXCLUDED."NET_RATING_RANK",
                    "sp_work_NET_RATING_RANK" = EXCLUDED."sp_work_NET_RATING_RANK",
                    "AST_PCT_RANK" = EXCLUDED."AST_PCT_RANK",
                    "AST_TO_RANK" = EXCLUDED."AST_TO_RANK",
                    "AST_RATIO_RANK" = EXCLUDED."AST_RATIO_RANK",
                    "OREB_PCT_RANK" = EXCLUDED."OREB_PCT_RANK",
                    "DREB_PCT_RANK" = EXCLUDED."DREB_PCT_RANK",
                    "REB_PCT_RANK" = EXCLUDED."REB_PCT_RANK",
                    "TM_TOV_PCT_RANK" = EXCLUDED."TM_TOV_PCT_RANK",
                    "E_TOV_PCT_RANK" = EXCLUDED."E_TOV_PCT_RANK",
                    "EFG_PCT_RANK" = EXCLUDED."EFG_PCT_RANK",
                    "TS_PCT_RANK" = EXCLUDED."TS_PCT_RANK",
                    "USG_PCT_RANK" = EXCLUDED."USG_PCT_RANK",
                    "E_USG_PCT_RANK" = EXCLUDED."E_USG_PCT_RANK",
                    "E_PACE_RANK" = EXCLUDED."E_PACE_RANK",
                    "PACE_RANK" = EXCLUDED."PACE_RANK",
                    "sp_work_PACE_RANK" = EXCLUDED."sp_work_PACE_RANK",
                    "PIE_RANK" = EXCLUDED."PIE_RANK",
                    "FGM_RANK" = EXCLUDED."FGM_RANK",
                    "FGA_RANK" = EXCLUDED."FGA_RANK",
                    "FGM_PG_RANK" = EXCLUDED."FGM_PG_RANK",
                    "FGA_PG_RANK" = EXCLUDED."FGA_PG_RANK",
                    "FG_PCT_RANK" = EXCLUDED."FG_PCT_RANK"
            """)
            
            # Execute UPSERT
            conn.execute(upsert_query)
            
            # Clean up temporary table
            conn.execute(text('DROP TABLE IF EXISTS temp_players_advanced_stats'))

    print('Advanced stats updated for all players')