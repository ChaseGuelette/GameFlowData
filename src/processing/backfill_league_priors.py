import datetime
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm
from src.db.client import get_engine

def get_season_start_date(snapshot_date_str):
    """
    Returns the start date (Oct 1st) of the season for a given snapshot.
    """
    snap = pd.to_datetime(snapshot_date_str)
    # If Jan-Sept, season started Oct of PREVIOUS year
    if snap.month <= 9:
        start_year = snap.year - 1
    # If Oct-Dec, season started Oct of CURRENT year
    else:
        start_year = snap.year
    return f"{start_year}-10-01"

def backfill_league_priors(engine):
    """
    Generates monthly snapshots. 
    Auto-detects season_id from the database.
    """
    # 1. Define range of years to process (Adjust as needed)
    years = range(2018, 2027) 
    
    # 2. Generate "1st of Month" dates for NBA Season (Nov-Apr)
    snapshot_dates = []
    for year in years:
        # Late year: Nov (11), Dec (12)
        for m in [11, 12]: 
            snapshot_dates.append(f"{year}-{m:02d}-01")
        # Early next year: Jan (1) - Apr (4)
        for m in [1, 2, 3, 4]:
            snapshot_dates.append(f"{year+1}-{m:02d}-01")
            
    # Filter out future dates
    today = datetime.date.today().strftime('%Y-%m-%d')
    snapshot_dates = [d for d in snapshot_dates if d <= today]
            
    print(f"--- Generating League Priors for {len(snapshot_dates)} months ---")
    
    for snap_date in tqdm(snapshot_dates, desc="Processing Months"):
        season_start = get_season_start_date(snap_date)
        calculate_and_insert_snapshot(engine, season_start, snap_date)
            
    print("--- Priors Backfill Complete ---")

def calculate_and_insert_snapshot(engine, season_start_date, snapshot_date):
    """
    Calculates stats from season_start up to snapshot_date.
    Selects season_id dynamically from the data.
    """
    # FIX: Removed 'updated_at' from ON CONFLICT clause
    query = text("""
        INSERT INTO league_priors_history (
            season_id, position_group, snapshot_date,
            league_off_rtg, league_reb_per100, league_ast_per100,
            league_stl_per100, league_blk_per100, league_threes_per100, league_tov_per100,
            league_fta_per100, league_oreb_per100, league_pf_per100,
            total_possessions, total_games
        )
        SELECT 
            tgs.season_id,        
            ph.position_group,
            CAST(:snap_date AS DATE),
            
            -- Stats Calculation (Sum / Total Possessions * 100)
            ROUND((SUM(pgs.pts) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.reb) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.ast) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.stl) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.blk) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.fg3m) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.tov) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            -- Sharp Stats
            ROUND((SUM(pgs.fta) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.oreb) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            ROUND((SUM(pgs.pf) / NULLIF(SUM(adv.possessions), 0) * 100), 2),
            
            SUM(adv.possessions),
            COUNT(DISTINCT pgs.game_id)

        FROM player_game_stats pgs
        JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
        JOIN player_game_advanced_stats adv ON pgs.game_id = adv.game_id AND pgs.player_id = adv.player_id
        
        -- Correct History Join
        JOIN LATERAL (
            SELECT position_group FROM player_position_history ph
            WHERE ph.player_id = pgs.player_id 
              AND ph.snapshot_date < tgs.game_date::DATE
            ORDER BY ph.snapshot_date DESC LIMIT 1
        ) ph ON TRUE

        WHERE tgs.game_date::DATE >= CAST(:season_start AS DATE)
          AND tgs.game_date::DATE < CAST(:snap_date AS DATE)
          AND pgs.min > 0 
          AND adv.possessions > 0
          
        GROUP BY tgs.season_id, ph.position_group
        
        ON CONFLICT (season_id, position_group, snapshot_date) DO UPDATE SET
            league_off_rtg = EXCLUDED.league_off_rtg,
            league_reb_per100 = EXCLUDED.league_reb_per100,
            league_ast_per100 = EXCLUDED.league_ast_per100,
            league_stl_per100 = EXCLUDED.league_stl_per100,
            league_blk_per100 = EXCLUDED.league_blk_per100,
            league_threes_per100 = EXCLUDED.league_threes_per100,
            league_tov_per100 = EXCLUDED.league_tov_per100,
            league_fta_per100 = EXCLUDED.league_fta_per100,
            league_oreb_per100 = EXCLUDED.league_oreb_per100,
            league_pf_per100 = EXCLUDED.league_pf_per100,
            total_possessions = EXCLUDED.total_possessions,
            total_games = EXCLUDED.total_games;
    """)
    
    with engine.begin() as conn:
        conn.execute(query, {'season_start': season_start_date, 'snap_date': snapshot_date})

if __name__ == "__main__":
    engine = get_engine()
    backfill_league_priors(engine)