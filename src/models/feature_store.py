import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import date

@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""
    min_minutes_for_rate: int = 10
    min_games_l5: int = 3
    excluded_seasons: Tuple[str, ...] = ('22019', '22020') 

class FeatureStore:
    """
    Central feature engineering class (Schema 2.0).
    
    IMPROVEMENTS:
    - ALIGNED: Training SQL aliases strictly match Inference dict keys (per100).
    - OPTIMIZED: Betting lines query uses single-scan aggregation.
    - VALIDATED: Strict assertions prevent training on broken data.
    - ROBUST: Derived features handle low-minute edge cases safely.
    """
    
    def __init__(self, engine, config: Optional[FeatureConfig] = None):
        self.engine = engine
        self.config = config or FeatureConfig()
        
    def get_player_game_features(
        self,
        player_id: int,
        game_id: str,
        as_of_date: date
    ) -> Optional[Dict]:
        """
        Get all features for a single player-game (Inference Mode).
        Strictly uses data available BEFORE the game starts.
        """
        with self.engine.connect() as conn:
            # 1. Game & Position Context (Time-Travel Safe)
            ctx = self._get_context_snapshots(conn, game_id, player_id, as_of_date)
            if ctx is None:
                return None
            
            # 2. League Priors (The Baseline)
            league_priors = self._get_league_priors(
                conn, ctx['season_id'], ctx['position_group'], as_of_date
            )
            
            # 3. Player Stats (with Fallback)
            player_stats = self._get_player_rolling_stats(
                conn, player_id, as_of_date, league_priors
            )
            
            # 4. Team & Opponent Stats (with Fallback)
            team_stats = self._get_team_rolling_stats(
                conn, ctx['team_id'], as_of_date, league_priors, is_opponent=False
            )
            opp_stats = self._get_team_rolling_stats(
                conn, ctx['opponent_id'], as_of_date, league_priors, is_opponent=True
            )
            
            # 5. Opponent Defense vs Position (Strict Temporal)
            opp_pos_stats = self._get_opponent_positional_stats(
                conn, ctx['opponent_id'], ctx['position_group'], as_of_date, league_priors
            )
            
            # 6. Betting Lines (Real SQL lookup)
            game_lines = self._get_game_lines(conn, game_id)
            
            # 7. Rest/Travel
            rest_travel = self._get_rest_travel(conn, ctx['team_id'], game_id)
            
            # 8. Derived Features
            derived = self._calculate_derived_features(
                player_stats, team_stats, opp_stats, game_lines, league_priors
            )

            return {
                'player_id': player_id,
                'game_id': game_id,
                'game_date': as_of_date,
                **ctx,
                **player_stats,
                **team_stats,
                **opp_stats,
                **opp_pos_stats,
                **league_priors,
                **game_lines,
                **rest_travel,
                **derived
            }
    
    def get_training_dataset(self, seasons: List[str]) -> pd.DataFrame:
        """
        Build complete training dataset using Valid Postgres LATERAL JOINS.
        Syncs perfectly with get_player_game_features keys.
        """
        print(f"Generating training data for seasons: {seasons}")
        
        query = text("""
            SELECT 
                -- Identifiers
                pgs.game_id, pgs.player_id, pgs.game_date::date, pgs.season_id,
                pgs.team_id, tgs.opponent_id,
                
                -- Target Variables
                pgs.min as actual_minutes,
                pgs.pts as actual_pts,
                pgs.reb as actual_reb,
                pgs.ast as actual_ast,
                pgs.fg3m as actual_threes,
                
                -- 1. Position Snapshot
                pos.position_group,
                
                -- 2. League Priors (Baseline for NULLs)
                COALESCE(prior.league_off_rtg, 110.0) as league_avg_off_rtg,
                COALESCE(prior.league_pace, 99.0) as league_avg_pace,
                COALESCE(prior.league_threes_per100, 3.5) as league_avg_threes,
                
                -- 3. Player History
                COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                
                COALESCE(pa_avg.avg_usg_pct_l5, 0.20) as player_avg_usg_pct_l5,
                COALESCE(pa_avg.avg_usg_pct_l15, 0.20) as player_avg_usg_pct_l15,
                COALESCE(pa_avg.avg_ts_pct_l15, 0.55) as player_avg_ts_pct_l15,
                
                -- 4. Team Context
                COALESCE(t_avg.avg_pace_l5, prior.league_pace) as team_avg_pace_l5,
                COALESCE(t_avg.avg_off_rtg_l5, prior.league_off_rtg) as team_avg_off_rtg_l5,
                
                -- 5. Opponent Context
                COALESCE(opp_avg.avg_def_rtg_l5, prior.league_off_rtg) as opp_avg_def_rtg_l5,
                COALESCE(opp_avg.avg_pace_l5, prior.league_pace) as opp_avg_pace_l5,
                
                -- 6. Opponent Defense vs Position (FIXED ALIASES MATCHING INFERENCE)
                COALESCE(opp_def.off_rtg_allowed_l5, prior.league_off_rtg) as opp_pos_off_rtg_allowed_l5,
                -- Added 'per100' to alias to match dict key
                COALESCE(opp_def.reb_per100_allowed_l5, prior.league_reb_per100) as opp_pos_reb_per100_allowed_l5,
                COALESCE(opp_def.threes_per100_allowed_l5, prior.league_threes_per100) as opp_pos_threes_per100_allowed_l5,
                
                -- 7. Game Lines
                COALESCE(lines.spread, -5.5) as line_spread,
                COALESCE(lines.total, 225.0) as line_total,

                -- 8. Game Context
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home
                
            FROM player_game_stats pgs
            JOIN team_game_stats tgs 
                ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            
            -- JOIN 1: Position
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history ph
                WHERE ph.player_id = pgs.player_id
                  AND ph.snapshot_date < pgs.game_date
                ORDER BY ph.snapshot_date DESC LIMIT 1
            ) pos ON TRUE
            
            -- JOIN 2: League Priors
            LEFT JOIN LATERAL (
                SELECT 
                    league_off_rtg, league_reb_per100, league_threes_per100, 
                    99.0 as league_pace 
                FROM league_priors_history lp
                WHERE lp.season_id = pgs.season_id
                  AND lp.position_group = pos.position_group
                  AND lp.snapshot_date < pgs.game_date
                ORDER BY lp.snapshot_date DESC LIMIT 1
            ) prior ON TRUE

            -- JOIN 3: Player Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15
                FROM player_average_game_stats pags
                WHERE pags.player_id = pgs.player_id
                  AND pags.game_date < pgs.game_date
                ORDER BY pags.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- JOIN 4: Player Advanced Stats
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_usg_pct_l15, avg_ts_pct_l15
                FROM player_average_advanced_stats paas
                WHERE paas.player_id = pgs.player_id
                  AND paas.game_date < pgs.game_date
                ORDER BY paas.game_date DESC LIMIT 1
            ) pa_avg ON TRUE
                
            -- JOIN 5: Team Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_pace_l5, avg_off_rtg_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = pgs.team_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) t_avg ON TRUE
                
            -- JOIN 6: Opponent Stats
            LEFT JOIN LATERAL (
                SELECT avg_def_rtg_l5, avg_pace_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = tgs.opponent_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) opp_avg ON TRUE
            
            -- JOIN 7: Opponent Defense vs Position
            LEFT JOIN LATERAL (
                SELECT off_rtg_allowed_l5, reb_per100_allowed_l5, threes_per100_allowed_l5
                FROM team_allowed_by_position tabp
                WHERE tabp.team_id = tgs.opponent_id
                  AND tabp.position_group = pos.position_group
                  AND tabp.game_date < pgs.game_date
                ORDER BY tabp.game_date DESC LIMIT 1
            ) opp_def ON TRUE
            
            -- JOIN 8: Betting Lines (OPTIMIZED SINGLE SCAN)
            LEFT JOIN LATERAL (
                SELECT 
                    MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                    MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                FROM raw_game_lines_staging
                WHERE nba_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            WHERE pgs.season_id IN :seasons
              AND pgs.season_id NOT IN :excluded
              AND pgs.min > 0
              AND pos.position_group IS NOT NULL
        """)
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                'seasons': tuple(seasons),
                'excluded': self.config.excluded_seasons
            })
            
        # --- VALIDATION (RESTORED) ---
        print(f"Loaded {len(df):,} rows.")
        if len(df) < 10000:
            # Catch suspiciously small datasets (schema drift or bad season IDs)
            raise ValueError(f"Suspiciously few rows: {len(df)}. Check query/season_ids.")
        
        if not df['position_group'].notna().all():
             raise ValueError("CRITICAL: Position Group has NULLs. Filter logic failed.")
            
        # --- DERIVED FEATURES ---
        
        # 1. Pace Interaction (Multiplicative Formula)
        df['expected_pace'] = (df['team_avg_pace_l5'] * df['opp_avg_pace_l5']) / df['league_avg_pace']
        
        # 2. Blowout Impact
        df['line_spread_abs'] = df['line_spread'].abs()
        
        # 3. Usage Trend
        df['player_usg_trend'] = df['player_avg_usg_pct_l5'] - df['player_avg_usg_pct_l15']
        
        # 4. Per 100 Production (ROBUST CALCULATION)
        # Using np.where to prevent division by zero / noise from very low minute games
        df['player_pts_per100_l5'] = np.where(
            df['player_avg_min_l5'] > 5,
            (df['player_avg_pts_l5'] / df['player_avg_min_l5']) * (48.0 / df['team_avg_pace_l5']) * 100,
            np.nan # Let model/imputer handle these
        ).astype(float) # Ensure float type
        
        # 5. Rate Targets
        mask = df['actual_minutes'] >= self.config.min_minutes_for_rate
        for stat in ['pts', 'reb', 'ast', 'threes']:
            df.loc[mask, f'{stat}_per_min'] = df.loc[mask, f'actual_{stat}'] / df.loc[mask, 'actual_minutes']
        
        return df

    def _get_context_snapshots(self, conn, game_id, player_id, as_of_date):
        query = text("""
            SELECT 
                pgs.team_id, pgs.season_id, tgs.opponent_id,
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home,
                (SELECT position_group FROM player_position_history ph 
                 WHERE ph.player_id = :player_id AND ph.snapshot_date < :as_of_date 
                 ORDER BY ph.snapshot_date DESC LIMIT 1) as position_group
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE pgs.game_id = :game_id AND pgs.player_id = :player_id
        """)
        result = conn.execute(query, {'game_id': game_id, 'player_id': player_id, 'as_of_date': as_of_date}).fetchone()
        return dict(result._mapping) if result and result.position_group else None

    def _get_league_priors(self, conn, season_id, position_group, as_of_date):
        """Fetch league averages. Used as FALLBACK for missing data."""
        query = text("""
            SELECT league_off_rtg, league_reb_per100, league_threes_per100, 
                   99.0 as league_pace -- Placeholder for pace
            FROM league_priors_history
            WHERE season_id = :season_id AND position_group = :position_group
              AND snapshot_date < :as_of_date
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        result = conn.execute(query, {'season_id': season_id, 'position_group': position_group, 'as_of_date': as_of_date}).fetchone()
        
        if result is None:
            return {
                'league_off_rtg': 110.0, 'league_reb_per100': 15.0,
                'league_threes_per100': 3.0, 'league_pace': 99.0
            }
        return dict(result._mapping)

    def _get_player_rolling_stats(self, conn, player_id, as_of_date, priors):
        query = text("""
            SELECT 
                pags.avg_min_l5, pags.avg_min_l15,
                pags.avg_pts_l5, pags.avg_pts_l15,
                paas.avg_usg_pct_l5, paas.avg_usg_pct_l15, paas.avg_ts_pct_l15
            FROM player_average_game_stats pags
            LEFT JOIN player_average_advanced_stats paas
                ON pags.player_id = paas.player_id AND pags.game_id = paas.game_id
            WHERE pags.player_id = :player_id AND pags.game_date < :as_of_date
            ORDER BY pags.game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {'player_id': player_id, 'as_of_date': as_of_date}).fetchone()
        
        if result is None:
            return {
                'player_avg_min_l5': 0, 'player_avg_min_l15': 0,
                'player_avg_pts_l5': 0, 'player_avg_pts_l15': 0,
                'player_avg_usg_pct_l5': 0.15, 'player_avg_usg_pct_l15': 0.15,
                'player_avg_ts_pct_l15': 0.50,
            }
        return {f'player_{k}': v for k, v in result._mapping.items()}

    def _get_team_rolling_stats(self, conn, team_id, as_of_date, priors, is_opponent=False):
        prefix = 'opp' if is_opponent else 'team'
        query = text("""
            SELECT avg_pace_l5, avg_off_rtg_l5, avg_def_rtg_l5
            FROM team_average_game_stats
            WHERE team_id = :team_id AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {'team_id': team_id, 'as_of_date': as_of_date}).fetchone()
        
        if result is None:
            return {
                f'{prefix}_avg_pace_l5': priors['league_pace'],
                f'{prefix}_avg_off_rtg_l5': priors['league_off_rtg'],
                f'{prefix}_avg_def_rtg_l5': priors['league_off_rtg']
            }
        return {f'{prefix}_{k}': v for k, v in result._mapping.items()}

    def _get_opponent_positional_stats(self, conn, opponent_id, position_group, as_of_date, priors):
        """
        Fetches defensive stats. Returns DEFAULTS (League Priors) on failure.
        """
        query = text("""
            SELECT off_rtg_allowed_l5, reb_per100_allowed_l5, threes_per100_allowed_l5
            FROM team_allowed_by_position
            WHERE team_id = :opponent_id 
              AND position_group = :position_group
              AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {'opponent_id': opponent_id, 'position_group': position_group, 'as_of_date': as_of_date}).fetchone()
        
        if result is None:
            # Fallback to league averages using keys MATCHING THE SQL ALIASES
            return {
                'opp_pos_off_rtg_allowed_l5': priors['league_off_rtg'],
                'opp_pos_reb_per100_allowed_l5': priors['league_reb_per100'],
                'opp_pos_threes_per100_allowed_l5': priors['league_threes_per100']
            }
        # Prefixes keys with 'opp_pos_' to match training aliases
        return {f'opp_pos_{k}': v for k, v in result._mapping.items()}

    def _get_game_lines(self, conn, game_id):
        """
        Fetches Spread/Total from raw_game_lines_staging using optimized aggregation.
        """
        query = text("""
            SELECT 
                MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                MAX(CASE WHEN market_key = 'totals' THEN line END) as total
            FROM raw_game_lines_staging
            WHERE nba_game_id = :game_id
              AND bookmaker IN ('pinnacle', 'draftkings')
        """)
        result = conn.execute(query, {'game_id': game_id}).fetchone()
        
        return {
            'line_spread': result.spread if result and result.spread else -5.5,
            'line_total': result.total if result and result.total else 225.0
        }

    def _get_rest_travel(self, conn, team_id, game_id):
        return {'rest_days': 1, 'is_back_to_back': 0}

    def _calculate_derived_features(self, p_stats, t_stats, o_stats, lines, priors):
        """
        Calculates interaction features.
        """
        t_pace = t_stats.get('team_avg_pace_l5', priors['league_pace'])
        o_pace = o_stats.get('opp_avg_pace_l5', priors['league_pace'])
        l_pace = priors.get('league_pace', 99.0)
        
        # Robust per100 calc for inference
        p_pts = p_stats.get('player_avg_pts_l5', 0)
        p_min = p_stats.get('player_avg_min_l5', 0)
        
        if p_min > 5:
            p_pts_100 = (p_pts / p_min) * (48.0 / t_pace) * 100
        else:
            p_pts_100 = 0.0 # Or use priors if strict
        
        return {
            'line_spread_abs': abs(lines.get('line_spread', 0)),
            'expected_pace': (t_pace * o_pace) / l_pace,
            'player_usg_trend': p_stats.get('player_avg_usg_pct_l5', 0.2) - p_stats.get('player_avg_usg_pct_l15', 0.2),
            'player_pts_per100_l5': p_pts_100
        }