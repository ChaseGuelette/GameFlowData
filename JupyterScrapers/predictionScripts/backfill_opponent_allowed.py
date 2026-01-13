# scripts/backfill_opponent_allowed.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from tqdm import tqdm

def compute_shrunk_rate(
    total_stat: float, 
    total_minutes: float, 
    league_rate: float,
    prior_minutes: float = 400
) -> float:
    """
    Compute shrinkage-adjusted per-36 rate.
    """
    if total_minutes <= 0:
        return league_rate * 36
    
    raw_rate = total_stat / total_minutes
    shrunk_rate = (prior_minutes * league_rate + total_stat) / (prior_minutes + total_minutes)
    return shrunk_rate * 36


def backfill_team_allowed_by_position(engine, seasons: list[str]):
    """
    Backfill the team_allowed_by_position table for historical games.
    """
    
    # 1. Get league averages for shrinkage priors
    league_avgs = pd.read_sql("""
        SELECT season_id, position_group,
               league_pts_per_min, league_reb_per_min, 
               league_ast_per_min, league_stl_per_min, league_blk_per_min
        FROM league_position_averages
    """, engine).set_index(['season_id', 'position_group'])
    
    # 2. Get all games we need to compute for
    games_query = text("""
        SELECT DISTINCT 
            tgs.team_id,
            tgs.game_id,
            tgs.team_game_date::date as game_date,
            tgs.season_id
        FROM team_game_stats tgs
        WHERE tgs.season_id IN :seasons
        ORDER BY tgs.team_game_date
    """)
    
    with engine.connect() as conn:
        games_df = pd.read_sql(games_query, conn, params={'seasons': tuple(seasons)})
    
    print(f"Processing {len(games_df):,} team-games...")
    
    # 3. For each game, compute opponent allowed stats
    rows_to_insert = []
    
    for _, game_row in tqdm(games_df.iterrows(), total=len(games_df)):
        team_id = game_row['team_id']
        game_id = game_row['game_id']
        game_date = game_row['game_date']
        season_id = game_row['season_id']
        
        for position_group in ['G', 'W', 'B']:
            # Get league average for this season/position
            try:
                lg = league_avgs.loc[(season_id, position_group)]
            except KeyError:
                # Fallback to overall average
                lg = {'league_pts_per_min': 0.4, 'league_reb_per_min': 0.15,
                      'league_ast_per_min': 0.08, 'league_stl_per_min': 0.03,
                      'league_blk_per_min': 0.02}
            
            # Compute allowed stats for each window
            for window, n_games in [('l5', 5), ('l15', 15), ('szn', 82)]:
                stats = _compute_allowed_stats(
                    engine, team_id, position_group, 
                    game_date, n_games, season_id
                )
                
                if stats is None:
                    continue
                
                # Apply shrinkage
                prior_minutes = {'pts': 400, 'reb': 300, 'ast': 350, 
                                'stl': 500, 'blk': 500}
                
                row = {
                    'team_id': team_id,
                    'game_id': game_id,
                    'game_date': game_date,
                    'position_group': position_group,
                    f'total_pts_allowed_{window}': stats['total_pts'],
                    f'total_reb_allowed_{window}': stats['total_reb'],
                    f'total_ast_allowed_{window}': stats['total_ast'],
                    f'total_stl_allowed_{window}': stats['total_stl'],
                    f'total_blk_allowed_{window}': stats['total_blk'],
                    f'total_min_allowed_{window}': stats['total_min'],
                    f'games_{window}': stats['games'],
                }
                
                # Compute shrunk rates
                for stat in ['pts', 'reb', 'ast', 'stl', 'blk']:
                    row[f'{stat}_allowed_per36_{window}'] = compute_shrunk_rate(
                        stats[f'total_{stat}'],
                        stats['total_min'],
                        lg[f'league_{stat}_per_min'],
                        prior_minutes[stat]
                    )
                
                rows_to_insert.append(row)
        
        # Batch insert every 1000 games
        if len(rows_to_insert) >= 3000:  # 1000 games × 3 positions
            _batch_insert(engine, rows_to_insert)
            rows_to_insert = []
    
    # Final batch
    if rows_to_insert:
        _batch_insert(engine, rows_to_insert)


def _compute_allowed_stats(
    engine, 
    team_id: int, 
    position_group: str, 
    as_of_date, 
    n_games: int,
    season_id: str
) -> dict:
    """
    Compute what a team has allowed to a position group
    in their last N games before as_of_date.
    """
    query = text("""
        WITH team_games AS (
            -- Get the team's last N games before as_of_date
            SELECT game_id, team_game_date
            FROM team_game_stats
            WHERE team_id = :team_id
              AND team_game_date::date < :as_of_date
              AND season_id = :season_id
            ORDER BY team_game_date DESC
            LIMIT :n_games
        ),
        opponent_players AS (
            -- Get opponent player stats in those games
            SELECT 
                pgs.pts, pgs.reb, pgs.ast, pgs.stl, pgs.blk, pgs.min
            FROM player_game_stats pgs
            JOIN team_games tg ON pgs.game_id = tg.game_id
            JOIN player_position_groups ppg ON pgs.player_id = ppg.player_id
            WHERE pgs.team_id != :team_id  -- Opponent players only
              AND ppg.position_group = :position_group
              AND pgs.min >= 10  -- Minimum minutes threshold
        )
        SELECT 
            COALESCE(SUM(pts), 0) as total_pts,
            COALESCE(SUM(reb), 0) as total_reb,
            COALESCE(SUM(ast), 0) as total_ast,
            COALESCE(SUM(stl), 0) as total_stl,
            COALESCE(SUM(blk), 0) as total_blk,
            COALESCE(SUM(min), 0) as total_min,
            COUNT(*) as player_games,
            (SELECT COUNT(*) FROM team_games) as games
        FROM opponent_players
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            'team_id': team_id,
            'position_group': position_group,
            'as_of_date': as_of_date,
            'n_games': n_games,
            'season_id': season_id
        }).fetchone()
    
    if result is None or result.total_min == 0:
        return None
        
    return {
        'total_pts': float(result.total_pts),
        'total_reb': float(result.total_reb),
        'total_ast': float(result.total_ast),
        'total_stl': float(result.total_stl),
        'total_blk': float(result.total_blk),
        'total_min': float(result.total_min),
        'games': int(result.games),
    }


def _batch_insert(engine, rows: list[dict]):
    """Batch insert/upsert rows into team_allowed_by_position."""
    df = pd.DataFrame(rows)
    
    # Aggregate rows with same (team_id, game_id, position_group)
    # This handles the multiple windows
    key_cols = ['team_id', 'game_id', 'game_date', 'position_group']
    
    # Group and merge all window columns
    grouped = df.groupby(key_cols).first().reset_index()
    
    # Upsert to database
    with engine.begin() as conn:
        for _, row in grouped.iterrows():
            # Build upsert query
            cols = [c for c in row.index if pd.notna(row[c])]
            values = {c: row[c] for c in cols}
            
            conn.execute(text(f"""
                INSERT INTO team_allowed_by_position ({', '.join(cols)})
                VALUES ({', '.join([f':{c}' for c in cols])})
                ON CONFLICT (team_id, game_id, position_group) DO UPDATE SET
                {', '.join([f'{c} = EXCLUDED.{c}' for c in cols if c not in key_cols])}
            """), values)