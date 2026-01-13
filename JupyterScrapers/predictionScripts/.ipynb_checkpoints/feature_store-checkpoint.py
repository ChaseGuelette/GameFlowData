# features/feature_store.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import date

@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""
    min_minutes_for_rate: int = 10
    min_games_l5: int = 3
    min_games_l15: int = 8
    min_minutes_opp_l5: int = 100
    min_minutes_opp_l15: int = 300
    min_minutes_opp_szn: int = 600
    shrinkage_prior_minutes: Dict[str, int] = None
    excluded_seasons: Tuple[str, ...] = ('22019', '22020')
    
    def __post_init__(self):
        if self.shrinkage_prior_minutes is None:
            self.shrinkage_prior_minutes = {
                'pts': 400,
                'reb': 300,
                'ast': 350,
                'stl': 500,
                'blk': 500,
            }


class FeatureStore:
    """
    Central feature engineering class.
    Handles both historical (training) and inference (prediction) scenarios.
    """
    
    def __init__(self, engine, config: Optional[FeatureConfig] = None):
        self.engine = engine
        self.config = config or FeatureConfig()
        
    def get_player_game_features(
        self,
        player_id: int,
        game_id: str,
        as_of_date: date
    ) -> Dict:
        """
        Get all features for a single player-game.
        
        This is the PRIMARY interface for both training and inference.
        Guarantees no temporal leakage by using as_of_date strictly.
        """
        with self.engine.connect() as conn:
            # 1. Game context (teams, home/away, opponent)
            game_ctx = self._get_game_context(conn, game_id, player_id)
            if game_ctx is None:
                return None
                
            # 2. Player rolling stats
            player_stats = self._get_player_rolling_stats(
                conn, player_id, as_of_date
            )
            
            # 3. Player's team context
            team_stats = self._get_team_rolling_stats(
                conn, game_ctx['team_id'], as_of_date
            )
            
            # 4. Opponent team context (overall)
            opp_stats = self._get_team_rolling_stats(
                conn, game_ctx['opponent_id'], as_of_date
            )
            
            # 5. Opponent allowed by position (the new feature)
            opp_positional = self._get_opponent_positional_stats(
                conn, 
                game_ctx['opponent_id'], 
                game_ctx['position_group'],
                game_id
            )
            
            # 6. Game lines (spread, total)
            game_lines = self._get_game_lines(conn, game_id)
            
            # 7. Rest and travel
            rest_travel = self._get_rest_travel(
                conn, game_ctx['team_id'], game_id
            )
            
            return {
                # Identifiers (for debugging, dropped before training)
                'player_id': player_id,
                'game_id': game_id,
                'game_date': as_of_date,
                'team_id': game_ctx['team_id'],
                'opponent_id': game_ctx['opponent_id'],
                'position_group': game_ctx['position_group'],
                
                # Game context
                'is_home': game_ctx['is_home'],
                
                # Player features (prefixed)
                **{f'player_{k}': v for k, v in player_stats.items()},
                
                # Team features (prefixed)
                **{f'team_{k}': v for k, v in team_stats.items()},
                
                # Opponent features (prefixed)
                **{f'opp_{k}': v for k, v in opp_stats.items()},
                
                # Opponent positional features (prefixed)
                **{f'opp_pos_{k}': v for k, v in opp_positional.items()},
                
                # Game lines
                **{f'line_{k}': v for k, v in game_lines.items()},
                
                # Rest/travel
                **rest_travel,
            }
    
    def get_training_dataset(
        self,
        seasons: List[str],
        min_player_minutes: int = 10
    ) -> pd.DataFrame:
        """
        Build complete training dataset for specified seasons.
        Returns one row per player-game.
        """
        query = text("""
            SELECT 
                pgs.player_id,
                pgs.game_id,
                pgs.game_date::date as game_date,
                pgs.season_id,
                pgs.team_id,
                tgs.opponent_id,
                ppg.position_group,
                
                -- Targets
                pgs.min as actual_minutes,
                pgs.pts as actual_pts,
                pgs.reb as actual_reb,
                pgs.ast as actual_ast,
                pgs.stl as actual_stl,
                pgs.blk as actual_blk,
                
                -- Rate targets (only valid if minutes >= threshold)
                CASE WHEN pgs.min >= :min_minutes 
                     THEN pgs.pts::NUMERIC / pgs.min 
                     ELSE NULL END as pts_per_min,
                CASE WHEN pgs.min >= :min_minutes 
                     THEN pgs.reb::NUMERIC / pgs.min 
                     ELSE NULL END as reb_per_min,
                CASE WHEN pgs.min >= :min_minutes 
                     THEN pgs.ast::NUMERIC / pgs.min 
                     ELSE NULL END as ast_per_min,
                CASE WHEN pgs.min >= :min_minutes 
                     THEN pgs.stl::NUMERIC / pgs.min 
                     ELSE NULL END as stl_per_min,
                CASE WHEN pgs.min >= :min_minutes 
                     THEN pgs.blk::NUMERIC / pgs.min 
                     ELSE NULL END as blk_per_min,
                
                -- Home/away
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home
                
            FROM player_game_stats pgs
            JOIN team_game_stats tgs 
                ON pgs.game_id = tgs.game_id 
                AND pgs.team_id = tgs.team_id
            JOIN player_position_groups ppg 
                ON pgs.player_id = ppg.player_id
            WHERE pgs.season_id IN :seasons
              AND pgs.season_id NOT IN :excluded
              AND pgs.min > 0  -- Played at least some minutes
              AND ppg.position_group IS NOT NULL
            ORDER BY pgs.game_date, pgs.game_id, pgs.player_id
        """)
        
        with self.engine.connect() as conn:
            base_df = pd.read_sql(
                query, conn,
                params={
                    'seasons': tuple(seasons),
                    'excluded': self.config.excluded_seasons,
                    'min_minutes': self.config.min_minutes_for_rate
                }
            )
        
        print(f"Loaded {len(base_df):,} player-game rows")
        
        # Now enrich with rolling features
        # This is the expensive part - we batch it for efficiency
        enriched_df = self._enrich_with_rolling_features(base_df)
        
        return enriched_df
    
    def _get_game_context(
        self, 
        conn, 
        game_id: str, 
        player_id: int
    ) -> Optional[Dict]:
        """Get basic game context for a player."""
        query = text("""
            SELECT 
                pgs.team_id,
                tgs.opponent_id,
                ppg.position_group,
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home
            FROM player_game_stats pgs
            JOIN team_game_stats tgs 
                ON pgs.game_id = tgs.game_id 
                AND pgs.team_id = tgs.team_id
            JOIN player_position_groups ppg 
                ON pgs.player_id = ppg.player_id
            WHERE pgs.game_id = :game_id 
              AND pgs.player_id = :player_id
        """)
        
        result = conn.execute(
            query, 
            {'game_id': game_id, 'player_id': player_id}
        ).fetchone()
        
        if result is None:
            return None
            
        return {
            'team_id': result.team_id,
            'opponent_id': result.opponent_id,
            'position_group': result.position_group,
            'is_home': result.is_home,
        }
    
    def _get_player_rolling_stats(
        self, 
        conn, 
        player_id: int, 
        as_of_date: date
    ) -> Dict:
        """Get player's rolling averages as of a specific date."""
        query = text("""
            SELECT 
                -- L5 stats
                avg_min_l5, avg_pts_l5, avg_reb_l5, avg_ast_l5,
                avg_stl_l5, avg_blk_l5, avg_tov_l5,
                avg_fg_pct_l5, avg_fg3_pct_l5, avg_ft_pct_l5,
                games_l5,
                
                -- L15 stats  
                avg_min_l15, avg_pts_l15, avg_reb_l15, avg_ast_l15,
                avg_stl_l15, avg_blk_l15, avg_tov_l15,
                avg_fg_pct_l15, avg_fg3_pct_l15, avg_ft_pct_l15,
                games_l15,
                
                -- Season stats
                avg_min_szn, avg_pts_szn, avg_reb_szn, avg_ast_szn,
                avg_stl_szn, avg_blk_szn, avg_tov_szn,
                avg_fg_pct_szn, avg_fg3_pct_szn, avg_ft_pct_szn,
                games_szn,
                
                -- Advanced stats (from player_average_advanced_stats)
                avg_usg_pct_l5, avg_usg_pct_l15, avg_usg_pct_szn,
                avg_ts_pct_l5, avg_ts_pct_l15, avg_ts_pct_szn,
                avg_pace_l5, avg_pace_l15, avg_pace_szn,
                avg_off_rtg_l5, avg_off_rtg_l15, avg_off_rtg_szn,
                avg_def_rtg_l5, avg_def_rtg_l15, avg_def_rtg_szn
                
            FROM player_average_game_stats pags
            LEFT JOIN player_average_advanced_stats paas
                ON pags.player_id = paas.player_id 
                AND pags.game_id = paas.game_id
            WHERE pags.player_id = :player_id
              AND pags.game_date < :as_of_date
            ORDER BY pags.game_date DESC
            LIMIT 1
        """)
        
        result = conn.execute(
            query, 
            {'player_id': player_id, 'as_of_date': as_of_date}
        ).fetchone()
        
        if result is None:
            return self._get_default_player_stats()
            
        return dict(result._mapping)
    
    def _get_team_rolling_stats(
        self, 
        conn, 
        team_id: int, 
        as_of_date: date
    ) -> Dict:
        """Get team's rolling averages as of a specific date."""
        query = text("""
            SELECT 
                avg_pts_l5, avg_pts_l15, avg_pts_szn,
                avg_fg_pct_l5, avg_fg_pct_l15, avg_fg_pct_szn,
                avg_fg3_pct_l5, avg_fg3_pct_l15, avg_fg3_pct_szn,
                avg_reb_l5, avg_reb_l15, avg_reb_szn,
                avg_ast_l5, avg_ast_l15, avg_ast_szn,
                avg_tov_l5, avg_tov_l15, avg_tov_szn,
                avg_pace_l5, avg_pace_l15, avg_pace_szn,
                avg_off_rtg_l5, avg_off_rtg_l15, avg_off_rtg_szn,
                avg_def_rtg_l5, avg_def_rtg_l15, avg_def_rtg_szn,
                games_l5, games_l15, games_szn
            FROM team_average_game_stats
            WHERE team_id = :team_id
              AND game_date < :as_of_date
            ORDER BY game_date DESC
            LIMIT 1
        """)
        
        result = conn.execute(
            query, 
            {'team_id': team_id, 'as_of_date': as_of_date}
        ).fetchone()
        
        if result is None:
            return self._get_default_team_stats()
            
        return dict(result._mapping)
    
    def _get_opponent_positional_stats(
        self, 
        conn, 
        opponent_id: int, 
        position_group: str,
        game_id: str
    ) -> Dict:
        """Get opponent's allowed stats for player's position group."""
        query = text("""
            SELECT 
                pts_allowed_per36_l5, pts_allowed_per36_l15, pts_allowed_per36_szn,
                reb_allowed_per36_l5, reb_allowed_per36_l15, reb_allowed_per36_szn,
                ast_allowed_per36_l5, ast_allowed_per36_l15, ast_allowed_per36_szn,
                stl_allowed_per36_l5, stl_allowed_per36_l15, stl_allowed_per36_szn,
                blk_allowed_per36_l5, blk_allowed_per36_l15, blk_allowed_per36_szn,
                total_min_allowed_l5, total_min_allowed_l15, total_min_allowed_szn,
                games_l5, games_l15, games_szn
            FROM team_allowed_by_position
            WHERE team_id = :opponent_id
              AND position_group = :position_group
              AND game_id = :game_id
        """)
        
        result = conn.execute(
            query, 
            {
                'opponent_id': opponent_id, 
                'position_group': position_group,
                'game_id': game_id
            }
        ).fetchone()
        
        if result is None:
            return self._get_default_opponent_positional_stats()
            
        return dict(result._mapping)
    
    def _get_game_lines(self, conn, game_id: str) -> Dict:
        """Get betting lines for the game."""
        query = text("""
            SELECT 
                -- Get spread (home team perspective)
                MAX(CASE WHEN market_key = 'spreads' AND outcome_label = home_team 
                         THEN line END) as spread,
                -- Get total
                MAX(CASE WHEN market_key = 'totals' AND outcome_label = 'Over' 
                         THEN line END) as total,
                -- Get moneyline implied probability
                MAX(CASE WHEN market_key = 'h2h' AND outcome_label = home_team 
                         THEN odds_american END) as home_ml_odds
            FROM raw_game_lines_staging
            WHERE nba_game_id = :game_id
              AND bookmaker = 'pinnacle'  -- Prefer sharp book
            GROUP BY nba_game_id
        """)
        
        result = conn.execute(query, {'game_id': game_id}).fetchone()
        
        if result is None:
            return {'spread': 0.0, 'total': 220.0, 'home_ml_implied': 0.5}
        
        # Convert ML odds to implied probability
        ml_odds = result.home_ml_odds or -110
        if ml_odds < 0:
            implied = abs(ml_odds) / (abs(ml_odds) + 100)
        else:
            implied = 100 / (ml_odds + 100)
            
        return {
            'spread': result.spread or 0.0,
            'total': result.total or 220.0,
            'home_ml_implied': implied,
        }
    
    def _get_rest_travel(
        self, 
        conn, 
        team_id: int, 
        game_id: str
    ) -> Dict:
        """Get rest days and travel distance."""
        # This uses your existing travel calculation logic
        # Simplified version here
        query = text("""
            WITH team_schedule AS (
                SELECT 
                    game_id,
                    team_game_date::date as game_date,
                    LAG(team_game_date::date) OVER (
                        PARTITION BY team_id ORDER BY team_game_date
                    ) as prev_game_date
                FROM team_game_stats
                WHERE team_id = :team_id
            )
            SELECT 
                COALESCE(game_date - prev_game_date, 7) as rest_days
            FROM team_schedule
            WHERE game_id = :game_id
        """)
        
        result = conn.execute(
            query, 
            {'team_id': team_id, 'game_id': game_id}
        ).fetchone()
        
        rest_days = min(result.rest_days if result else 7, 7)
        
        return {
            'rest_days': rest_days,
            'is_back_to_back': 1 if rest_days <= 1 else 0,
        }
    
    def _enrich_with_rolling_features(self, base_df: pd.DataFrame) -> pd.DataFrame:
        """
        Batch enrich base dataframe with rolling features.
        More efficient than row-by-row for training.
        """
        print("Enriching with rolling features...")
        
        # This would be a more efficient batch version
        # For now, we'll use the row-by-row approach with progress bar
        from tqdm import tqdm
        
        enriched_rows = []
        for _, row in tqdm(base_df.iterrows(), total=len(base_df)):
            features = self.get_player_game_features(
                player_id=row['player_id'],
                game_id=row['game_id'],
                as_of_date=row['game_date']
            )
            if features:
                # Add targets from base_df
                features['actual_minutes'] = row['actual_minutes']
                features['actual_pts'] = row['actual_pts']
                features['actual_reb'] = row['actual_reb']
                features['actual_ast'] = row['actual_ast']
                features['actual_stl'] = row['actual_stl']
                features['actual_blk'] = row['actual_blk']
                features['pts_per_min'] = row['pts_per_min']
                features['reb_per_min'] = row['reb_per_min']
                features['ast_per_min'] = row['ast_per_min']
                enriched_rows.append(features)
        
        return pd.DataFrame(enriched_rows)
    
    def _get_default_player_stats(self) -> Dict:
        """Default values for players with no history."""
        return {f'{stat}_{window}': 0.0 
                for stat in ['avg_min', 'avg_pts', 'avg_reb', 'avg_ast']
                for window in ['l5', 'l15', 'szn']}
    
    def _get_default_team_stats(self) -> Dict:
        """Default values for teams with no history."""
        return {'avg_pts_l5': 110.0, 'avg_pace_l5': 100.0}  # League averages
    
    def _get_default_opponent_positional_stats(self) -> Dict:
        """Default values when no opponent positional data."""
        return {f'{stat}_allowed_per36_{window}': 0.0
                for stat in ['pts', 'reb', 'ast', 'stl', 'blk']
                for window in ['l5', 'l15', 'szn']}