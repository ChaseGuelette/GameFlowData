import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""

    min_minutes_for_rate: int = 10
    min_games_l5: int = 3
    excluded_seasons: tuple[str, ...] = ("22019", "22020")


# Centralized feature definitions for consistency between training and inference
MINUTES_FEATURES = [
    "player_avg_min_l5",
    "player_avg_min_l15",
    "player_avg_usg_pct_l5",
    "team_avg_pace_l5",
    "opp_avg_pace_l5",
    "line_spread",
    "line_total",
    "is_home",
    # B2: Rest/Schedule
    "rest_days",
    "is_back_to_back",
    "games_in_last_7_days",
    # B3: Short-window trend
    "player_avg_min_l3",
    # B4: Minutes stability
    "player_min_std_l5",
    "player_min_floor_l5",
    "player_games_started_l5",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
    "player_is_questionable",
    "player_is_probable",
]

RATE_FEATURES_PTS = [
    "player_avg_pts_l5",
    "player_avg_pts_l15",
    "player_avg_usg_pct_l5",
    "player_avg_ts_pct_l15",
    "team_avg_pace_l5",
    "opp_avg_def_rtg_l5",
    "opp_pos_off_rtg_allowed_l5",
    "opp_pos_off_rtg_allowed_l15",
    "is_home",
    "prop_line_pts",
    # B2: Rest/Schedule
    "rest_days",
    "is_back_to_back",
    "games_in_last_7_days",
    # B3: Trend + variability
    "player_avg_pts_l3",
    "player_pts_l3_l15_ratio",
    "player_std_pts_l5",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
]

RATE_FEATURES_REB = [
    "player_avg_reb_l5",
    "player_avg_reb_pct_l5",
    "opp_pos_reb_allowed_l5",
    "opp_pos_reb_per100_allowed_l5",
    "opp_pos_reb_allowed_l15",
    "team_avg_pace_l5",
    "opp_avg_pace_l5",
    "is_home",
    "prop_line_reb",
    # B2: Rest/Schedule
    "rest_days",
    "is_back_to_back",
    "games_in_last_7_days",
    # B3: Trend + variability
    "player_avg_reb_l3",
    "player_reb_l3_l15_ratio",
    "player_std_reb_l5",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
]

RATE_FEATURES_AST = [
    "player_avg_ast_l5",
    "player_avg_ast_pct_l5",
    "player_avg_usg_pct_l5",
    "opp_pos_ast_allowed_l5",
    "opp_pos_ast_per100_allowed_l5",
    "opp_pos_ast_allowed_l15",
    "team_avg_pace_l5",
    "is_home",
    "prop_line_ast",
    # B2: Rest/Schedule
    "rest_days",
    "is_back_to_back",
    "games_in_last_7_days",
    # B3: Trend + variability
    "player_avg_ast_l3",
    "player_ast_l3_l15_ratio",
    "player_std_ast_l5",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
]

RATE_FEATURES_THREES = [
    "player_avg_fg3m_l5",
    "player_avg_fg3a_l5",
    "opp_pos_threes_allowed_l5",
    "opp_pos_threes_per100_allowed_l5",
    "opp_pos_threes_allowed_l15",
    "team_avg_fg3a_l5",
    "team_avg_fg3_pct_l5",
    "opp_avg_fg3a_l5",
    "opp_avg_fg3_pct_l5",
    "team_avg_pace_l5",
    "is_home",
    "prop_line_threes",
    # B2: Rest/Schedule
    "rest_days",
    "is_back_to_back",
    "games_in_last_7_days",
    # B3: Trend + variability
    "player_avg_fg3m_l3",
    "player_fg3m_l3_l15_ratio",
    "player_std_fg3m_l5",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
]


class FeatureStore:
    """
    Central feature engineering class.

    Streamlined feature set:
    - No derived per100 features (use raw stats)
    - Fill missing with 0 (not fake league averages)
    - Opponent stats derived via join on opponent_id
    """

    def __init__(self, engine, config: FeatureConfig | None = None):
        self.engine = engine
        self.config = config or FeatureConfig()

    def get_player_game_features(
        self,
        player_id: int,
        game_id: str,
        as_of_date: date,
        team_id: int | None = None,
        opponent_id: int | None = None,
        is_home: bool | None = None,
    ) -> dict | None:
        """
        Get all features for a single player-game (Inference Mode).
        Strictly uses data available BEFORE the game starts.

        For scheduled games (not yet played), pass team_id, opponent_id, is_home
        to avoid querying player_game_stats which won't have the game yet.
        """
        with self.engine.connect() as conn:
            # 1. Game & Position Context
            if team_id is not None and opponent_id is not None:
                # Use provided context (for scheduled games not in player_game_stats)
                position_group = self._get_player_position(conn, player_id, as_of_date)
                if position_group is None:
                    return None
                ctx = {
                    "team_id": team_id,
                    "opponent_id": opponent_id,
                    "is_home": is_home if is_home is not None else True,
                    "position_group": position_group,
                    "season_id": "22025",  # Current season
                }
            else:
                # Query from player_game_stats (for historical games)
                ctx = self._get_context_snapshots(conn, game_id, player_id, as_of_date)
                if ctx is None:
                    return None

            # 2. Player Stats
            player_stats = self._get_player_rolling_stats(conn, player_id, as_of_date)

            # 3. Team & Opponent Stats
            team_stats = self._get_team_rolling_stats(conn, ctx["team_id"], as_of_date, is_opponent=False)
            opp_stats = self._get_team_rolling_stats(conn, ctx["opponent_id"], as_of_date, is_opponent=True)

            # 4. Opponent Defense vs Position
            opp_pos_stats = self._get_opponent_positional_stats(
                conn, ctx["opponent_id"], ctx["position_group"], as_of_date
            )

            # 5. Betting Lines
            game_lines = self._get_game_lines(conn, game_id)

            # 6. Player Prop Lines (centering features for rate models)
            prop_lines = self._get_player_prop_lines(conn, player_id, game_id)

            # 7. Injury Context
            injury_context = self._get_injury_context(
                conn, player_id, ctx["team_id"], ctx["opponent_id"], as_of_date
            )

            # Deprecated travel/opp features (not in any feature list, kept for column compat)
            dead_features = {
                "travel_dist": 0,
                "opp_rest_days": 0,
                "opp_travel_dist": 0,
                "opp_is_back_to_back": 0,
            }

            # Make spread team-directional (negative = player's team favored)
            raw_spread = game_lines.pop("line_spread_raw", 0)
            game_lines["line_spread"] = -raw_spread if ctx.get("is_home") else raw_spread

            return {
                "player_id": player_id,
                "game_id": game_id,
                "game_date": as_of_date,
                **ctx,
                **player_stats,
                **team_stats,
                **opp_stats,
                **opp_pos_stats,
                **game_lines,
                **prop_lines,
                **injury_context,
                **dead_features,
            }

    def get_features_for_date(self, game_date: date) -> pd.DataFrame:
        """
        Efficiently fetch features for ALL players on a specific date in one query.
        Used for backtesting to prevent N+1 query exhaustion.
        """
        query = text("""
            SELECT
                -- Identifiers
                pgs.game_id, pgs.player_id, pgs.game_date::date, pgs.season_id,
                pgs.team_id, tgs.opponent_id,
                p.player_name,

                -- Position
                pos.position_group,

                -- Player Box Score Averages
                COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,

                -- Player Advanced Averages
                COALESCE(pa_avg.avg_usg_pct_l5, 0.20) as player_avg_usg_pct_l5,
                COALESCE(pa_avg.avg_ts_pct_l15, 0.56) as player_avg_ts_pct_l15,
                COALESCE(pa_avg.avg_reb_pct_l5, 0.10) as player_avg_reb_pct_l5,
                COALESCE(pa_avg.avg_ast_pct_l5, 0.12) as player_avg_ast_pct_l5,

                -- Team Context
                COALESCE(t_avg.avg_pace_l5, 99.5) as team_avg_pace_l5,
                COALESCE(t_avg.avg_fg3a_l5, 34.0) as team_avg_fg3a_l5,
                COALESCE(t_avg.avg_fg3_pct_l5, 0.36) as team_avg_fg3_pct_l5,

                -- Opponent Context (via opponent_id join)
                COALESCE(opp_avg.avg_def_rtg_l5, 112.0) as opp_avg_def_rtg_l5,
                COALESCE(opp_avg.avg_pace_l5, 99.5) as opp_avg_pace_l5,
                COALESCE(opp_avg.avg_fg3a_l5, 34.0) as opp_avg_fg3a_l5,
                COALESCE(opp_avg.avg_fg3_pct_l5, 0.36) as opp_avg_fg3_pct_l5,

                -- Opponent Defense vs Position (L5 raw)
                COALESCE(opp_def.off_rtg_allowed_l5, 112.0) as opp_pos_off_rtg_allowed_l5,
                COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,

                -- Opponent Defense vs Position (L5 pace-adjusted per 100 possessions)
                COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,

                -- Opponent Defense vs Position (L15 raw)
                COALESCE(opp_def.off_rtg_allowed_l15, 112.0) as opp_pos_off_rtg_allowed_l15,
                COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,

                -- Game Lines (spread is team-directional: negative = player's team favored)
                CASE WHEN pgs.matchup LIKE '%vs.%'
                     THEN -COALESCE(lines.spread, 0)
                     ELSE COALESCE(lines.spread, 0) END as line_spread,
                COALESCE(lines.total, 0) as line_total,

                -- Player Prop Lines (centering features)
                COALESCE(prop_lines.prop_line_pts, 0) as prop_line_pts,
                COALESCE(prop_lines.prop_line_reb, 0) as prop_line_reb,
                COALESCE(prop_lines.prop_line_ast, 0) as prop_line_ast,
                COALESCE(prop_lines.prop_line_threes, 0) as prop_line_threes,

                -- B3: L3 averages
                COALESCE(p_avg.avg_min_l3, 0) as player_avg_min_l3,
                COALESCE(p_avg.avg_pts_l3, 0) as player_avg_pts_l3,
                COALESCE(p_avg.avg_reb_l3, 0) as player_avg_reb_l3,
                COALESCE(p_avg.avg_ast_l3, 0) as player_avg_ast_l3,
                COALESCE(p_avg.avg_fg3m_l3, 0) as player_avg_fg3m_l3,

                -- B3: Momentum ratios (L3/L15 for PTS, L3/L5 for others)
                CASE WHEN COALESCE(p_avg.avg_pts_l15, 0) > 0
                     THEN COALESCE(p_avg.avg_pts_l3, 0) / p_avg.avg_pts_l15
                     ELSE 1.0 END as player_pts_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_reb_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_reb_l3, 0) / p_avg.avg_reb_l5
                     ELSE 1.0 END as player_reb_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_ast_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_ast_l3, 0) / p_avg.avg_ast_l5
                     ELSE 1.0 END as player_ast_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_fg3m_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_fg3m_l3, 0) / p_avg.avg_fg3m_l5
                     ELSE 1.0 END as player_fg3m_l3_l15_ratio,

                -- B3/B4: L5 standard deviations
                COALESCE(p_avg.std_min_l5, 0) as player_min_std_l5,
                COALESCE(p_avg.std_pts_l5, 0) as player_std_pts_l5,
                COALESCE(p_avg.std_reb_l5, 0) as player_std_reb_l5,
                COALESCE(p_avg.std_ast_l5, 0) as player_std_ast_l5,
                COALESCE(p_avg.std_fg3m_l5, 0) as player_std_fg3m_l5,

                -- B4: Minutes stability
                COALESCE(p_avg.min_floor_l5, 0) as player_min_floor_l5,
                COALESCE(p_avg.games_started_l5, 0) as player_games_started_l5,

                -- B2: Rest/schedule
                LEAST(COALESCE(p_avg.rest_days, 3), 7) as rest_days,
                CASE WHEN COALESCE(p_avg.rest_days, 3) = 1 THEN 1 ELSE 0 END as is_back_to_back,
                COALESCE(p_avg.games_last_7d, 2) as games_in_last_7_days,

                -- Game Context
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home,

                -- Injury Context
                COALESCE(team_inj.out_count, 0) as team_out_count,
                COALESCE(team_inj.out_min_sum, 0) as team_out_min_sum,
                COALESCE(team_inj.out_pts_sum, 0) as team_out_pts_sum,
                COALESCE(team_inj.out_reb_sum, 0) as team_out_reb_sum,
                COALESCE(team_inj.out_ast_sum, 0) as team_out_ast_sum,
                COALESCE(team_inj_adv.out_usg_sum, 0) as team_out_usg_sum,
                COALESCE(opp_inj.out_count, 0) as opp_out_count,
                COALESCE(opp_inj.out_min_sum, 0) as opp_out_min_sum,
                CASE WHEN player_inj.inj_status = 'Questionable' THEN 1 ELSE 0 END as player_is_questionable,
                CASE WHEN player_inj.inj_status = 'Probable' THEN 1 ELSE 0 END as player_is_probable

            FROM player_game_stats pgs
            JOIN players p ON pgs.player_id = p.player_id
            JOIN team_game_stats tgs
                ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id

            -- Position
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history ph
                WHERE ph.player_id = pgs.player_id
                  AND ph.snapshot_date < pgs.game_date
                ORDER BY ph.snapshot_date DESC LIMIT 1
            ) pos ON TRUE

            -- Player Box Score Averages + B2/B3/B4 features
            -- NOTE: Using <= because the row for game_date X contains averages computed
            -- BEFORE game X (due to shift(1) in populate_average_stats). Safe, not leakage.
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                       avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5,
                       avg_min_l3, avg_pts_l3, avg_reb_l3, avg_ast_l3, avg_fg3m_l3,
                       std_min_l5, std_pts_l5, std_reb_l5, std_ast_l5, std_fg3m_l5,
                       min_floor_l5, games_started_l5,
                       rest_days, games_last_7d
                FROM player_average_game_stats pags
                WHERE pags.player_id = pgs.player_id
                  AND pags.game_date <= pgs.game_date
                ORDER BY pags.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Player Advanced Stats
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                FROM player_average_advanced_stats paas
                WHERE paas.player_id = pgs.player_id
                  AND paas.game_date <= pgs.game_date
                ORDER BY paas.game_date DESC LIMIT 1
            ) pa_avg ON TRUE

            -- Team Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = pgs.team_id
                  AND tags.game_date <= pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) t_avg ON TRUE

            -- Opponent Stats (via opponent_id)
            LEFT JOIN LATERAL (
                SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = tgs.opponent_id
                  AND tags.game_date <= pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) opp_avg ON TRUE

            -- Opponent Defense vs Position
            LEFT JOIN LATERAL (
                SELECT
                    off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                    threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                    off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                FROM team_allowed_by_position tabp
                WHERE tabp.team_id = tgs.opponent_id
                  AND tabp.position_group = pos.position_group
                  AND tabp.game_date <= pgs.game_date
                ORDER BY tabp.game_date DESC LIMIT 1
            ) opp_def ON TRUE

            -- Betting Lines
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                    MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                FROM raw_game_lines_staging
                WHERE nba_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Player Prop Lines (centering features for rate models)
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
                    MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
                    MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
                    MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM raw_player_props_combined
                    WHERE player_id = pgs.player_id
                      AND game_id = pgs.game_id
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
            ) prop_lines ON TRUE

            -- Team Injury Context: game stats (OUT players on this team)
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) as out_count,
                    COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
                    COALESCE(SUM(inj_sub.avg_pts_l5), 0) as out_pts_sum,
                    COALESCE(SUM(inj_sub.avg_reb_l5), 0) as out_reb_sum,
                    COALESCE(SUM(inj_sub.avg_ast_l5), 0) as out_ast_sum
                FROM (
                    SELECT DISTINCT ON (ri.player_id)
                        pags_inj.avg_min_l5, pags_inj.avg_pts_l5,
                        pags_inj.avg_reb_l5, pags_inj.avg_ast_l5
                    FROM rapidapi_injuries ri
                    LEFT JOIN player_average_game_stats pags_inj
                        ON pags_inj.player_id = ri.player_id
                        AND pags_inj.game_date < pgs.game_date
                    WHERE ri.nba_team_id = pgs.team_id
                      AND ri.report_date = pgs.game_date
                      AND ri.status = 'Out'
                      AND ri.player_id IS NOT NULL
                    ORDER BY ri.player_id, pags_inj.game_date DESC
                ) inj_sub
            ) team_inj ON TRUE

            -- Team Injury Context: advanced stats (OUT players on this team)
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum
                FROM (
                    SELECT DISTINCT ON (ri.player_id)
                        paas_inj.avg_usg_pct_l5
                    FROM rapidapi_injuries ri
                    LEFT JOIN player_average_advanced_stats paas_inj
                        ON paas_inj.player_id = ri.player_id
                        AND paas_inj.game_date < pgs.game_date
                    WHERE ri.nba_team_id = pgs.team_id
                      AND ri.report_date = pgs.game_date
                      AND ri.status = 'Out'
                      AND ri.player_id IS NOT NULL
                    ORDER BY ri.player_id, paas_inj.game_date DESC
                ) inj_sub
            ) team_inj_adv ON TRUE

            -- Opponent Injury Context (OUT players on opponent)
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) as out_count,
                    COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum
                FROM (
                    SELECT DISTINCT ON (ri.player_id)
                        pags_inj.avg_min_l5
                    FROM rapidapi_injuries ri
                    LEFT JOIN player_average_game_stats pags_inj
                        ON pags_inj.player_id = ri.player_id
                        AND pags_inj.game_date < pgs.game_date
                    WHERE ri.nba_team_id = tgs.opponent_id
                      AND ri.report_date = pgs.game_date
                      AND ri.status = 'Out'
                      AND ri.player_id IS NOT NULL
                    ORDER BY ri.player_id, pags_inj.game_date DESC
                ) inj_sub
            ) opp_inj ON TRUE

            -- Player Injury Status
            LEFT JOIN LATERAL (
                SELECT status as inj_status
                FROM rapidapi_injuries ri
                WHERE ri.player_id = pgs.player_id
                  AND ri.report_date = pgs.game_date
                ORDER BY ri.id DESC LIMIT 1
            ) player_inj ON TRUE

            WHERE pgs.game_date = :game_date
              AND pgs.min >= 5
              AND pos.position_group IS NOT NULL
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            return df

        # Deprecated travel/opp features (not in any feature list, kept for column compat)
        for col in ["travel_dist", "opp_rest_days", "opp_travel_dist", "opp_is_back_to_back"]:
            df[col] = 0.0

        return df

    def _get_game_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        """Get all distinct game dates in a range."""
        query = text("""
            SELECT DISTINCT game_date
            FROM player_game_stats
            WHERE game_date >= :start_date AND game_date <= :end_date
            ORDER BY game_date
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
            return [row[0] for row in result]

    def get_features_for_date_range(
        self,
        start_date: date,
        end_date: date,
        chunk_size: int = 25,
    ) -> dict[date, pd.DataFrame]:
        """
        Prefetch features for all players across a date range in chunked queries.
        Same LATERAL JOIN query as get_features_for_date() but batched by date chunks
        to avoid Supabase statement timeouts.

        Returns:
            Dict mapping game_date -> DataFrame of features for that date.
        """
        all_dates = self._get_game_dates_in_range(start_date, end_date)
        if not all_dates:
            return {}

        # Chunk dates to keep individual queries under timeout
        chunks = [all_dates[i : i + chunk_size] for i in range(0, len(all_dates), chunk_size)]
        logger.info(f"Fetching features in {len(chunks)} chunks (chunk_size={chunk_size})")

        chunk_dfs = []
        failed_chunks: list[dict] = []  # Track failed chunks for summary
        for chunk_idx, chunk_dates in enumerate(chunks):
            chunk_start = chunk_dates[0]
            chunk_end = chunk_dates[-1]

            query = text("""
                SELECT
                    pgs.game_id, pgs.player_id, pgs.game_date::date as game_date, pgs.season_id,
                    pgs.team_id, tgs.opponent_id,
                    p.player_name,
                    pos.position_group,
                    COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                    COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                    COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                    COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                    COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                    COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                    COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                    COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,
                    COALESCE(pa_avg.avg_usg_pct_l5, 0.20) as player_avg_usg_pct_l5,
                    COALESCE(pa_avg.avg_ts_pct_l15, 0.56) as player_avg_ts_pct_l15,
                    COALESCE(pa_avg.avg_reb_pct_l5, 0.10) as player_avg_reb_pct_l5,
                    COALESCE(pa_avg.avg_ast_pct_l5, 0.12) as player_avg_ast_pct_l5,
                    COALESCE(t_avg.avg_pace_l5, 0) as team_avg_pace_l5,
                    COALESCE(t_avg.avg_fg3a_l5, 0) as team_avg_fg3a_l5,
                    COALESCE(t_avg.avg_fg3_pct_l5, 0) as team_avg_fg3_pct_l5,
                    COALESCE(opp_avg.avg_def_rtg_l5, 0) as opp_avg_def_rtg_l5,
                    COALESCE(opp_avg.avg_pace_l5, 0) as opp_avg_pace_l5,
                    COALESCE(opp_avg.avg_fg3a_l5, 0) as opp_avg_fg3a_l5,
                    COALESCE(opp_avg.avg_fg3_pct_l5, 0) as opp_avg_fg3_pct_l5,
                    COALESCE(opp_def.off_rtg_allowed_l5, 112.0) as opp_pos_off_rtg_allowed_l5,
                    COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                    COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                    COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,
                    COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                    COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                    COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,
                    COALESCE(opp_def.off_rtg_allowed_l15, 112.0) as opp_pos_off_rtg_allowed_l15,
                    COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                    COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                    COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,
                    COALESCE(lines.spread, 0) as line_spread,
                    COALESCE(lines.total, 0) as line_total,
                    COALESCE(prop_lines.prop_line_pts, 0) as prop_line_pts,
                    COALESCE(prop_lines.prop_line_reb, 0) as prop_line_reb,
                    COALESCE(prop_lines.prop_line_ast, 0) as prop_line_ast,
                    COALESCE(prop_lines.prop_line_threes, 0) as prop_line_threes,
                    -- B3: L3 averages
                    COALESCE(p_avg.avg_min_l3, 0) as player_avg_min_l3,
                    COALESCE(p_avg.avg_pts_l3, 0) as player_avg_pts_l3,
                    COALESCE(p_avg.avg_reb_l3, 0) as player_avg_reb_l3,
                    COALESCE(p_avg.avg_ast_l3, 0) as player_avg_ast_l3,
                    COALESCE(p_avg.avg_fg3m_l3, 0) as player_avg_fg3m_l3,
                    -- B3: Momentum ratios
                    CASE WHEN COALESCE(p_avg.avg_pts_l15, 0) > 0
                         THEN COALESCE(p_avg.avg_pts_l3, 0) / p_avg.avg_pts_l15
                         ELSE 1.0 END as player_pts_l3_l15_ratio,
                    CASE WHEN COALESCE(p_avg.avg_reb_l5, 0) > 0
                         THEN COALESCE(p_avg.avg_reb_l3, 0) / p_avg.avg_reb_l5
                         ELSE 1.0 END as player_reb_l3_l15_ratio,
                    CASE WHEN COALESCE(p_avg.avg_ast_l5, 0) > 0
                         THEN COALESCE(p_avg.avg_ast_l3, 0) / p_avg.avg_ast_l5
                         ELSE 1.0 END as player_ast_l3_l15_ratio,
                    CASE WHEN COALESCE(p_avg.avg_fg3m_l5, 0) > 0
                         THEN COALESCE(p_avg.avg_fg3m_l3, 0) / p_avg.avg_fg3m_l5
                         ELSE 1.0 END as player_fg3m_l3_l15_ratio,
                    -- B3/B4: L5 standard deviations
                    COALESCE(p_avg.std_min_l5, 0) as player_min_std_l5,
                    COALESCE(p_avg.std_pts_l5, 0) as player_std_pts_l5,
                    COALESCE(p_avg.std_reb_l5, 0) as player_std_reb_l5,
                    COALESCE(p_avg.std_ast_l5, 0) as player_std_ast_l5,
                    COALESCE(p_avg.std_fg3m_l5, 0) as player_std_fg3m_l5,
                    -- B4: Minutes stability
                    COALESCE(p_avg.min_floor_l5, 0) as player_min_floor_l5,
                    COALESCE(p_avg.games_started_l5, 0) as player_games_started_l5,
                    -- B2: Rest/schedule
                    LEAST(COALESCE(p_avg.rest_days, 3), 7) as rest_days,
                    CASE WHEN COALESCE(p_avg.rest_days, 3) = 1 THEN 1 ELSE 0 END as is_back_to_back,
                    COALESCE(p_avg.games_last_7d, 2) as games_in_last_7_days,
                    CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home,
                    -- Injury Context
                    COALESCE(team_inj.out_count, 0) as team_out_count,
                    COALESCE(team_inj.out_min_sum, 0) as team_out_min_sum,
                    COALESCE(team_inj.out_pts_sum, 0) as team_out_pts_sum,
                    COALESCE(team_inj.out_reb_sum, 0) as team_out_reb_sum,
                    COALESCE(team_inj.out_ast_sum, 0) as team_out_ast_sum,
                    COALESCE(team_inj_adv.out_usg_sum, 0) as team_out_usg_sum,
                    COALESCE(opp_inj.out_count, 0) as opp_out_count,
                    COALESCE(opp_inj.out_min_sum, 0) as opp_out_min_sum,
                    CASE WHEN player_inj.inj_status = 'Questionable' THEN 1 ELSE 0 END as player_is_questionable,
                    CASE WHEN player_inj.inj_status = 'Probable' THEN 1 ELSE 0 END as player_is_probable
                FROM player_game_stats pgs
                JOIN players p ON pgs.player_id = p.player_id
                JOIN team_game_stats tgs
                    ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
                LEFT JOIN LATERAL (
                    SELECT position_group
                    FROM player_position_history ph
                    WHERE ph.player_id = pgs.player_id
                      AND ph.snapshot_date < pgs.game_date
                    ORDER BY ph.snapshot_date DESC LIMIT 1
                ) pos ON TRUE
                -- NOTE: Using <= because the row for game_date X contains averages computed
                -- BEFORE game X (due to shift(1) in populate_average_stats). Safe, not leakage.
                LEFT JOIN LATERAL (
                    SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                           avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5,
                           avg_min_l3, avg_pts_l3, avg_reb_l3, avg_ast_l3, avg_fg3m_l3,
                           std_min_l5, std_pts_l5, std_reb_l5, std_ast_l5, std_fg3m_l5,
                           min_floor_l5, games_started_l5,
                           rest_days, games_last_7d
                    FROM player_average_game_stats pags
                    WHERE pags.player_id = pgs.player_id
                      AND pags.game_date <= pgs.game_date
                    ORDER BY pags.game_date DESC LIMIT 1
                ) p_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                    FROM player_average_advanced_stats paas
                    WHERE paas.player_id = pgs.player_id
                      AND paas.game_date <= pgs.game_date
                    ORDER BY paas.game_date DESC LIMIT 1
                ) pa_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                    FROM team_average_game_stats tags
                    WHERE tags.team_id = pgs.team_id
                      AND tags.game_date <= pgs.game_date
                    ORDER BY tags.game_date DESC LIMIT 1
                ) t_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                    FROM team_average_game_stats tags
                    WHERE tags.team_id = tgs.opponent_id
                      AND tags.game_date <= pgs.game_date
                    ORDER BY tags.game_date DESC LIMIT 1
                ) opp_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                        threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                        off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                    FROM team_allowed_by_position tabp
                    WHERE tabp.team_id = tgs.opponent_id
                      AND tabp.position_group = pos.position_group
                      AND tabp.game_date <= pgs.game_date
                    ORDER BY tabp.game_date DESC LIMIT 1
                ) opp_def ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                        MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                    FROM raw_game_lines_staging
                    WHERE nba_game_id = pgs.game_id
                      AND bookmaker IN ('pinnacle', 'draftkings')
                ) lines ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
                        MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
                        MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
                        MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
                    FROM (
                        SELECT DISTINCT ON (market_key) market_key, line
                        FROM raw_player_props_combined
                        WHERE player_id = pgs.player_id
                          AND game_id = pgs.game_id
                          AND bookmaker IN ('pinnacle', 'draftkings')
                        ORDER BY market_key, snapshot_time DESC NULLS LAST
                    ) sub
                ) prop_lines ON TRUE
                -- Team Injury Context: game stats (OUT players on this team)
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) as out_count,
                        COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
                        COALESCE(SUM(inj_sub.avg_pts_l5), 0) as out_pts_sum,
                        COALESCE(SUM(inj_sub.avg_reb_l5), 0) as out_reb_sum,
                        COALESCE(SUM(inj_sub.avg_ast_l5), 0) as out_ast_sum
                    FROM (
                        SELECT DISTINCT ON (ri.player_id)
                            pags_inj.avg_min_l5, pags_inj.avg_pts_l5,
                            pags_inj.avg_reb_l5, pags_inj.avg_ast_l5
                        FROM rapidapi_injuries ri
                        LEFT JOIN player_average_game_stats pags_inj
                            ON pags_inj.player_id = ri.player_id
                            AND pags_inj.game_date < pgs.game_date
                        WHERE ri.nba_team_id = pgs.team_id
                          AND ri.report_date = pgs.game_date
                          AND ri.status = 'Out'
                          AND ri.player_id IS NOT NULL
                        ORDER BY ri.player_id, pags_inj.game_date DESC
                    ) inj_sub
                ) team_inj ON TRUE
                -- Team Injury Context: advanced stats (OUT players on this team)
                LEFT JOIN LATERAL (
                    SELECT
                        COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum
                    FROM (
                        SELECT DISTINCT ON (ri.player_id)
                            paas_inj.avg_usg_pct_l5
                        FROM rapidapi_injuries ri
                        LEFT JOIN player_average_advanced_stats paas_inj
                            ON paas_inj.player_id = ri.player_id
                            AND paas_inj.game_date < pgs.game_date
                        WHERE ri.nba_team_id = pgs.team_id
                          AND ri.report_date = pgs.game_date
                          AND ri.status = 'Out'
                          AND ri.player_id IS NOT NULL
                        ORDER BY ri.player_id, paas_inj.game_date DESC
                    ) inj_sub
                ) team_inj_adv ON TRUE
                -- Opponent Injury Context (OUT players on opponent)
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) as out_count,
                        COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum
                    FROM (
                        SELECT DISTINCT ON (ri.player_id)
                            pags_inj.avg_min_l5
                        FROM rapidapi_injuries ri
                        LEFT JOIN player_average_game_stats pags_inj
                            ON pags_inj.player_id = ri.player_id
                            AND pags_inj.game_date < pgs.game_date
                        WHERE ri.nba_team_id = tgs.opponent_id
                          AND ri.report_date = pgs.game_date
                          AND ri.status = 'Out'
                          AND ri.player_id IS NOT NULL
                        ORDER BY ri.player_id, pags_inj.game_date DESC
                    ) inj_sub
                ) opp_inj ON TRUE
                -- Player Injury Status
                LEFT JOIN LATERAL (
                    SELECT status as inj_status
                    FROM rapidapi_injuries ri
                    WHERE ri.player_id = pgs.player_id
                      AND ri.report_date = pgs.game_date
                    ORDER BY ri.id DESC LIMIT 1
                ) player_inj ON TRUE
                WHERE pgs.game_date >= :chunk_start
                  AND pgs.game_date <= :chunk_end
                  AND pgs.min >= 5
                  AND pos.position_group IS NOT NULL
            """)

            try:
                with self.engine.connect() as conn:
                    chunk_df = pd.read_sql(
                        query,
                        conn,
                        params={"chunk_start": chunk_start, "chunk_end": chunk_end},
                    )
                logger.info(
                    f"  Feature chunk {chunk_idx + 1}/{len(chunks)}: "
                    f"{chunk_start} to {chunk_end} ({len(chunk_dates)} dates) -> {len(chunk_df)} rows"
                )
                if not chunk_df.empty:
                    chunk_dfs.append(chunk_df)
            except Exception as e:
                logger.error(f"  Feature chunk {chunk_idx + 1}/{len(chunks)} failed: {e}")
                failed_chunks.append({
                    "chunk_idx": chunk_idx + 1,
                    "dates": chunk_dates,
                    "start": chunk_start,
                    "end": chunk_end,
                    "error": str(e),
                })
                continue

        # Log summary of chunk processing results
        if failed_chunks:
            failed_date_count = sum(len(c["dates"]) for c in failed_chunks)
            logger.warning(
                f"CHUNK FAILURES: {len(failed_chunks)}/{len(chunks)} chunks failed, "
                f"dropping {failed_date_count}/{len(all_dates)} dates from result. "
                f"Failed chunk indices: {[c['chunk_idx'] for c in failed_chunks]}"
            )
            for fc in failed_chunks:
                logger.warning(f"  Chunk {fc['chunk_idx']}: {fc['start']} to {fc['end']} - {fc['error']}")
        else:
            logger.info(f"All {len(chunks)} chunks succeeded ({len(all_dates)} dates)")

        if not chunk_dfs:
            return {}

        all_results = pd.concat(chunk_dfs, ignore_index=True)

        # Deprecated travel/opp features (not in any feature list, kept for column compat)
        for col in ["travel_dist", "opp_rest_days", "opp_travel_dist", "opp_is_back_to_back"]:
            all_results[col] = 0.0

        # Ensure game_date is a proper date type for groupby
        if hasattr(all_results["game_date"].iloc[0], "date"):
            all_results["game_date"] = all_results["game_date"].apply(lambda x: x.date() if hasattr(x, "date") else x)

        return {game_date: group_df.reset_index(drop=True) for game_date, group_df in all_results.groupby("game_date")}

    def get_training_dataset(self, seasons: list[str]) -> pd.DataFrame:
        """
        Build complete training dataset.
        Loads each season individually for progress visibility and query performance,
        then concatenates and enriches with injury features via efficient bulk queries.
        """
        import time

        print(f"Generating training data for seasons: {seasons}")
        season_dfs = []

        for i, season in enumerate(seasons, 1):
            season_start = time.time()
            print(f"  Loading season {i}/{len(seasons)}: {season} ...")

            df_season = self._load_single_season_training(season)

            elapsed = time.time() - season_start
            print(f"  Season {season}: {len(df_season):,} rows in {elapsed:.1f}s")
            season_dfs.append(df_season)

        df = pd.concat(season_dfs, ignore_index=True)
        print(f"  Combined: {len(df):,} total rows across {len(seasons)} seasons")

        # --- Injury features (loaded separately for performance) ---
        inj_start = time.time()
        print("  Loading injury context features ...")
        game_dates = df["game_date"].tolist()

        # Team/opponent injury aggregations
        team_inj = self._load_injury_features_bulk(game_dates, [])
        if not team_inj.empty:
            # Merge for teammate injuries (team_id -> team_out_*)
            team_inj_renamed = team_inj.rename(columns={
                "nba_team_id": "_inj_team_id", "report_date": "_inj_date",
                "out_count": "team_out_count", "out_min_sum": "team_out_min_sum",
                "out_pts_sum": "team_out_pts_sum", "out_reb_sum": "team_out_reb_sum",
                "out_ast_sum": "team_out_ast_sum", "out_usg_sum": "team_out_usg_sum",
            })
            df = df.merge(
                team_inj_renamed,
                left_on=["team_id", "game_date"],
                right_on=["_inj_team_id", "_inj_date"],
                how="left",
            ).drop(columns=["_inj_team_id", "_inj_date"], errors="ignore")

            # Merge for opponent injuries (opponent_id -> opp_out_*)
            opp_inj_renamed = team_inj[["nba_team_id", "report_date", "out_count", "out_min_sum"]].rename(columns={
                "nba_team_id": "_inj_team_id", "report_date": "_inj_date",
                "out_count": "opp_out_count", "out_min_sum": "opp_out_min_sum",
            })
            df = df.merge(
                opp_inj_renamed,
                left_on=["opponent_id", "game_date"],
                right_on=["_inj_team_id", "_inj_date"],
                how="left",
            ).drop(columns=["_inj_team_id", "_inj_date"], errors="ignore")
        else:
            for col in ["team_out_count", "team_out_min_sum", "team_out_pts_sum",
                         "team_out_reb_sum", "team_out_ast_sum", "team_out_usg_sum",
                         "opp_out_count", "opp_out_min_sum"]:
                df[col] = 0

        # Player injury status
        player_inj = self._load_player_injury_status_bulk(game_dates)
        if not player_inj.empty:
            df = df.merge(
                player_inj.rename(columns={"report_date": "_inj_date"}),
                left_on=["player_id", "game_date"],
                right_on=["player_id", "_inj_date"],
                how="left",
            ).drop(columns=["_inj_date"], errors="ignore")
            df["player_is_questionable"] = (df["inj_status"] == "Questionable").astype(int)
            df["player_is_probable"] = (df["inj_status"] == "Probable").astype(int)
            df.drop(columns=["inj_status"], inplace=True, errors="ignore")
        else:
            df["player_is_questionable"] = 0
            df["player_is_probable"] = 0

        # Fill any NaN injury features with 0
        injury_cols = [
            "team_out_count", "team_out_min_sum", "team_out_pts_sum",
            "team_out_reb_sum", "team_out_ast_sum", "team_out_usg_sum",
            "opp_out_count", "opp_out_min_sum",
            "player_is_questionable", "player_is_probable",
        ]
        for col in injury_cols:
            df[col] = df[col].fillna(0)

        inj_elapsed = time.time() - inj_start
        print(f"  Injury features merged in {inj_elapsed:.1f}s")

        # Deprecated travel/opp features (not in any feature list, kept for column compat)
        for col in ["travel_dist", "opp_rest_days", "opp_travel_dist", "opp_is_back_to_back"]:
            df[col] = 0.0

        # Validation
        print(f"Loaded {len(df):,} rows.")
        if len(df) < 10000:
            raise ValueError(f"Suspiciously few rows: {len(df)}. Check query/season_ids.")

        if not df["position_group"].notna().all():
            raise ValueError("CRITICAL: Position Group has NULLs. Filter logic failed.")

        # Rate targets for training
        mask = df["actual_minutes"] >= self.config.min_minutes_for_rate
        for stat in ["pts", "reb", "ast", "threes"]:
            df.loc[mask, f"{stat}_per_min"] = df.loc[mask, f"actual_{stat}"] / df.loc[mask, "actual_minutes"]

        return df

    def _load_single_season_training(self, season: str) -> pd.DataFrame:
        """Load training features for a single season."""
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

                -- Position
                pos.position_group,

                -- Player Box Score Averages
                COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,

                -- Player Advanced Averages
                COALESCE(pa_avg.avg_usg_pct_l5, 0.20) as player_avg_usg_pct_l5,
                COALESCE(pa_avg.avg_ts_pct_l15, 0.56) as player_avg_ts_pct_l15,
                COALESCE(pa_avg.avg_reb_pct_l5, 0.10) as player_avg_reb_pct_l5,
                COALESCE(pa_avg.avg_ast_pct_l5, 0.12) as player_avg_ast_pct_l5,

                -- Team Context
                COALESCE(t_avg.avg_pace_l5, 99.5) as team_avg_pace_l5,
                COALESCE(t_avg.avg_fg3a_l5, 34.0) as team_avg_fg3a_l5,
                COALESCE(t_avg.avg_fg3_pct_l5, 0.36) as team_avg_fg3_pct_l5,

                -- Opponent Context (via opponent_id join)
                COALESCE(opp_avg.avg_def_rtg_l5, 112.0) as opp_avg_def_rtg_l5,
                COALESCE(opp_avg.avg_pace_l5, 99.5) as opp_avg_pace_l5,
                COALESCE(opp_avg.avg_fg3a_l5, 34.0) as opp_avg_fg3a_l5,
                COALESCE(opp_avg.avg_fg3_pct_l5, 0.36) as opp_avg_fg3_pct_l5,

                -- Opponent Defense vs Position (L5 raw)
                COALESCE(opp_def.off_rtg_allowed_l5, 112.0) as opp_pos_off_rtg_allowed_l5,
                COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,

                -- Opponent Defense vs Position (L5 pace-adjusted per 100 possessions)
                COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,

                -- Opponent Defense vs Position (L15 raw)
                COALESCE(opp_def.off_rtg_allowed_l15, 112.0) as opp_pos_off_rtg_allowed_l15,
                COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,

                -- Game Lines (spread is team-directional: negative = player's team favored)
                CASE WHEN pgs.matchup LIKE '%vs.%'
                     THEN -COALESCE(lines.spread, 0)
                     ELSE COALESCE(lines.spread, 0) END as line_spread,
                COALESCE(lines.total, 0) as line_total,

                -- Player Prop Lines (centering features)
                COALESCE(prop_lines.prop_line_pts, 0) as prop_line_pts,
                COALESCE(prop_lines.prop_line_reb, 0) as prop_line_reb,
                COALESCE(prop_lines.prop_line_ast, 0) as prop_line_ast,
                COALESCE(prop_lines.prop_line_threes, 0) as prop_line_threes,

                -- B3: L3 averages
                COALESCE(p_avg.avg_min_l3, 0) as player_avg_min_l3,
                COALESCE(p_avg.avg_pts_l3, 0) as player_avg_pts_l3,
                COALESCE(p_avg.avg_reb_l3, 0) as player_avg_reb_l3,
                COALESCE(p_avg.avg_ast_l3, 0) as player_avg_ast_l3,
                COALESCE(p_avg.avg_fg3m_l3, 0) as player_avg_fg3m_l3,

                -- B3: Momentum ratios (L3/L15 for PTS, L3/L5 for others)
                CASE WHEN COALESCE(p_avg.avg_pts_l15, 0) > 0
                     THEN COALESCE(p_avg.avg_pts_l3, 0) / p_avg.avg_pts_l15
                     ELSE 1.0 END as player_pts_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_reb_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_reb_l3, 0) / p_avg.avg_reb_l5
                     ELSE 1.0 END as player_reb_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_ast_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_ast_l3, 0) / p_avg.avg_ast_l5
                     ELSE 1.0 END as player_ast_l3_l15_ratio,
                CASE WHEN COALESCE(p_avg.avg_fg3m_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_fg3m_l3, 0) / p_avg.avg_fg3m_l5
                     ELSE 1.0 END as player_fg3m_l3_l15_ratio,

                -- B3/B4: L5 standard deviations
                COALESCE(p_avg.std_min_l5, 0) as player_min_std_l5,
                COALESCE(p_avg.std_pts_l5, 0) as player_std_pts_l5,
                COALESCE(p_avg.std_reb_l5, 0) as player_std_reb_l5,
                COALESCE(p_avg.std_ast_l5, 0) as player_std_ast_l5,
                COALESCE(p_avg.std_fg3m_l5, 0) as player_std_fg3m_l5,

                -- B4: Minutes stability
                COALESCE(p_avg.min_floor_l5, 0) as player_min_floor_l5,
                COALESCE(p_avg.games_started_l5, 0) as player_games_started_l5,

                -- B2: Rest/schedule
                LEAST(COALESCE(p_avg.rest_days, 3), 7) as rest_days,
                CASE WHEN COALESCE(p_avg.rest_days, 3) = 1 THEN 1 ELSE 0 END as is_back_to_back,
                COALESCE(p_avg.games_last_7d, 2) as games_in_last_7_days,

                -- Game Context
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home

            FROM player_game_stats pgs
            JOIN team_game_stats tgs
                ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id

            -- Position
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history ph
                WHERE ph.player_id = pgs.player_id
                  AND ph.snapshot_date < pgs.game_date
                ORDER BY ph.snapshot_date DESC LIMIT 1
            ) pos ON TRUE

            -- Player Box Score Averages + B2/B3/B4 features
            -- NOTE: Using <= because the row for game_date X contains averages computed
            -- BEFORE game X (due to shift(1) in populate_average_stats). Safe, not leakage.
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                       avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5,
                       avg_min_l3, avg_pts_l3, avg_reb_l3, avg_ast_l3, avg_fg3m_l3,
                       std_min_l5, std_pts_l5, std_reb_l5, std_ast_l5, std_fg3m_l5,
                       min_floor_l5, games_started_l5,
                       rest_days, games_last_7d
                FROM player_average_game_stats pags
                WHERE pags.player_id = pgs.player_id
                  AND pags.game_date <= pgs.game_date
                ORDER BY pags.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Player Advanced Stats
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                FROM player_average_advanced_stats paas
                WHERE paas.player_id = pgs.player_id
                  AND paas.game_date <= pgs.game_date
                ORDER BY paas.game_date DESC LIMIT 1
            ) pa_avg ON TRUE

            -- Team Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = pgs.team_id
                  AND tags.game_date <= pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) t_avg ON TRUE

            -- Opponent Stats (via opponent_id)
            LEFT JOIN LATERAL (
                SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = tgs.opponent_id
                  AND tags.game_date <= pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) opp_avg ON TRUE

            -- Opponent Defense vs Position
            LEFT JOIN LATERAL (
                SELECT
                    off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                    threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                    off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                FROM team_allowed_by_position tabp
                WHERE tabp.team_id = tgs.opponent_id
                  AND tabp.position_group = pos.position_group
                  AND tabp.game_date <= pgs.game_date
                ORDER BY tabp.game_date DESC LIMIT 1
            ) opp_def ON TRUE

            -- Betting Lines
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                    MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                FROM raw_game_lines_staging
                WHERE nba_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Player Prop Lines (centering features for rate models)
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
                    MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
                    MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
                    MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM raw_player_props_combined
                    WHERE player_id = pgs.player_id
                      AND game_id = pgs.game_id
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
            ) prop_lines ON TRUE

            WHERE pgs.season_id = :season
              AND pgs.season_id NOT IN :excluded
              AND pgs.min >= 5
              AND pos.position_group IS NOT NULL
        """).bindparams(
            bindparam("excluded", expanding=True),
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"season": season, "excluded": list(self.config.excluded_seasons)},
            )

        return df

    def _load_injury_features_bulk(self, game_dates: list, team_ids: list) -> pd.DataFrame:
        """
        Pre-aggregate injury features for all (team, date) pairs in one pass.
        Returns DataFrame keyed by (nba_team_id, report_date) with team injury aggregations.

        Much more efficient than per-row LATERAL JOINs: iterates over injury records (~20K)
        instead of player-game rows (~26K × 3 nested subqueries).
        """
        import numpy as np

        query = text("""
            SELECT
                ri.nba_team_id,
                ri.report_date,
                ri.player_id as inj_player_id,
                pags_l.avg_min_l5,
                pags_l.avg_pts_l5,
                pags_l.avg_reb_l5,
                pags_l.avg_ast_l5,
                paas_l.avg_usg_pct_l5
            FROM (
                SELECT DISTINCT ON (nba_team_id, report_date, player_id)
                    nba_team_id, report_date, player_id
                FROM rapidapi_injuries
                WHERE status = 'Out'
                  AND player_id IS NOT NULL
                  AND report_date = ANY(:dates)
                ORDER BY nba_team_id, report_date, player_id, id DESC
            ) ri
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_pts_l5, avg_reb_l5, avg_ast_l5
                FROM player_average_game_stats
                WHERE player_id = ri.player_id
                  AND game_date < ri.report_date
                ORDER BY game_date DESC LIMIT 1
            ) pags_l ON TRUE
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5
                FROM player_average_advanced_stats
                WHERE player_id = ri.player_id
                  AND game_date < ri.report_date
                ORDER BY game_date DESC LIMIT 1
            ) paas_l ON TRUE
        """)

        with self.engine.connect() as conn:
            inj_df = pd.read_sql(query, conn, params={"dates": list(set(game_dates))})

        if inj_df.empty:
            return pd.DataFrame(columns=[
                "nba_team_id", "report_date",
                "out_count", "out_min_sum", "out_pts_sum",
                "out_reb_sum", "out_ast_sum", "out_usg_sum",
            ])

        # Aggregate per (team, date)
        agg = inj_df.groupby(["nba_team_id", "report_date"]).agg(
            out_count=("inj_player_id", "nunique"),
            out_min_sum=("avg_min_l5", lambda x: np.nansum(x)),
            out_pts_sum=("avg_pts_l5", lambda x: np.nansum(x)),
            out_reb_sum=("avg_reb_l5", lambda x: np.nansum(x)),
            out_ast_sum=("avg_ast_l5", lambda x: np.nansum(x)),
            out_usg_sum=("avg_usg_pct_l5", lambda x: np.nansum(x)),
        ).reset_index()

        return agg

    def _load_player_injury_status_bulk(self, game_dates: list) -> pd.DataFrame:
        """Load player injury statuses for all (player, date) pairs."""
        query = text("""
            SELECT DISTINCT ON (player_id, report_date)
                player_id, report_date, status as inj_status
            FROM rapidapi_injuries
            WHERE player_id IS NOT NULL
              AND report_date = ANY(:dates)
            ORDER BY player_id, report_date, id DESC
        """)

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"dates": list(set(game_dates))})

    def _get_player_position(self, conn, player_id: int, as_of_date: date) -> str | None:
        """Get player's position group from position history."""
        query = text("""
            SELECT position_group FROM player_position_history
            WHERE player_id = :player_id AND snapshot_date < :as_of_date
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        result = conn.execute(query, {"player_id": player_id, "as_of_date": as_of_date}).fetchone()
        return result[0] if result else None

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
        result = conn.execute(query, {"game_id": game_id, "player_id": player_id, "as_of_date": as_of_date}).fetchone()
        return dict(result._mapping) if result and result.position_group else None

    def _get_player_rolling_stats(self, conn, player_id, as_of_date):
        query = text("""
            SELECT
                pags.avg_min_l5, pags.avg_min_l15,
                pags.avg_pts_l5, pags.avg_pts_l15,
                pags.avg_reb_l5, pags.avg_ast_l5,
                pags.avg_fg3m_l5, pags.avg_fg3a_l5,
                paas.avg_usg_pct_l5, paas.avg_ts_pct_l15,
                paas.avg_reb_pct_l5, paas.avg_ast_pct_l5,
                -- B3: L3 averages
                pags.avg_min_l3, pags.avg_pts_l3, pags.avg_reb_l3,
                pags.avg_ast_l3, pags.avg_fg3m_l3,
                -- B3/B4: L5 standard deviations
                pags.std_min_l5, pags.std_pts_l5, pags.std_reb_l5,
                pags.std_ast_l5, pags.std_fg3m_l5,
                -- B4: Minutes stability
                pags.min_floor_l5, pags.games_started_l5,
                -- B2: Rest/schedule
                pags.rest_days AS stored_rest_days, pags.games_last_7d
            FROM player_average_game_stats pags
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                FROM player_average_advanced_stats
                WHERE player_id = pags.player_id
                  AND game_date < :as_of_date
                ORDER BY game_date DESC LIMIT 1
            ) paas ON TRUE
            WHERE pags.player_id = :player_id AND pags.game_date < :as_of_date
            ORDER BY pags.game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {"player_id": player_id, "as_of_date": as_of_date}).fetchone()

        if result is None:
            return {
                "player_avg_min_l5": 0, "player_avg_min_l15": 0,
                "player_avg_pts_l5": 0, "player_avg_pts_l15": 0,
                "player_avg_reb_l5": 0, "player_avg_ast_l5": 0,
                "player_avg_fg3m_l5": 0, "player_avg_fg3a_l5": 0,
                "player_avg_usg_pct_l5": 0.20, "player_avg_ts_pct_l15": 0.56,
                "player_avg_reb_pct_l5": 0.10, "player_avg_ast_pct_l5": 0.12,
                # B3: L3 averages
                "player_avg_min_l3": 0, "player_avg_pts_l3": 0,
                "player_avg_reb_l3": 0, "player_avg_ast_l3": 0,
                "player_avg_fg3m_l3": 0,
                # B3: Momentum ratios (default = 1.0 = no trend)
                "player_pts_l3_l15_ratio": 1.0, "player_reb_l3_l15_ratio": 1.0,
                "player_ast_l3_l15_ratio": 1.0, "player_fg3m_l3_l15_ratio": 1.0,
                # B3/B4: L5 standard deviations
                "player_min_std_l5": 0, "player_std_pts_l5": 0,
                "player_std_reb_l5": 0, "player_std_ast_l5": 0,
                "player_std_fg3m_l5": 0,
                # B4: Minutes stability
                "player_min_floor_l5": 0, "player_games_started_l5": 0,
                # B2: Rest/schedule
                "rest_days": 3, "is_back_to_back": 0, "games_in_last_7_days": 2,
            }

        row = result._mapping

        # Standard player_* prefix columns
        stats = {}
        for k in [
            "avg_min_l5", "avg_min_l15", "avg_pts_l5", "avg_pts_l15",
            "avg_reb_l5", "avg_ast_l5", "avg_fg3m_l5", "avg_fg3a_l5",
            "avg_usg_pct_l5", "avg_ts_pct_l15", "avg_reb_pct_l5", "avg_ast_pct_l5",
            "avg_min_l3", "avg_pts_l3", "avg_reb_l3", "avg_ast_l3", "avg_fg3m_l3",
        ]:
            stats[f"player_{k}"] = row.get(k) or 0

        # B3/B4: Std deviations (custom naming: std_min_l5 -> player_min_std_l5)
        stats["player_min_std_l5"] = row.get("std_min_l5") or 0
        stats["player_std_pts_l5"] = row.get("std_pts_l5") or 0
        stats["player_std_reb_l5"] = row.get("std_reb_l5") or 0
        stats["player_std_ast_l5"] = row.get("std_ast_l5") or 0
        stats["player_std_fg3m_l5"] = row.get("std_fg3m_l5") or 0

        # B4: Minutes stability
        stats["player_min_floor_l5"] = row.get("min_floor_l5") or 0
        stats["player_games_started_l5"] = row.get("games_started_l5") or 0

        # B3: Momentum ratios (computed in Python — safe division, default 1.0)
        pts_l3 = row.get("avg_pts_l3") or 0
        pts_l15 = row.get("avg_pts_l15") or 0
        stats["player_pts_l3_l15_ratio"] = (pts_l3 / pts_l15) if pts_l15 > 0 else 1.0

        reb_l3 = row.get("avg_reb_l3") or 0
        reb_l5 = row.get("avg_reb_l5") or 0
        stats["player_reb_l3_l15_ratio"] = (reb_l3 / reb_l5) if reb_l5 > 0 else 1.0

        ast_l3 = row.get("avg_ast_l3") or 0
        ast_l5 = row.get("avg_ast_l5") or 0
        stats["player_ast_l3_l15_ratio"] = (ast_l3 / ast_l5) if ast_l5 > 0 else 1.0

        fg3m_l3 = row.get("avg_fg3m_l3") or 0
        fg3m_l5 = row.get("avg_fg3m_l5") or 0
        stats["player_fg3m_l3_l15_ratio"] = (fg3m_l3 / fg3m_l5) if fg3m_l5 > 0 else 1.0

        # B2: Rest/schedule (no player_ prefix)
        stored_rest = row.get("stored_rest_days")
        rest = min(stored_rest, 7) if stored_rest is not None else 3
        stats["rest_days"] = rest
        stats["is_back_to_back"] = 1 if rest == 1 else 0
        stats["games_in_last_7_days"] = row.get("games_last_7d") or 2

        return stats

    def _get_team_rolling_stats(self, conn, team_id, as_of_date, is_opponent=False):
        prefix = "opp" if is_opponent else "team"
        query = text("""
            SELECT avg_pace_l5, avg_def_rtg_l5, avg_fg3a_l5, avg_fg3_pct_l5
            FROM team_average_game_stats
            WHERE team_id = :team_id AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {"team_id": team_id, "as_of_date": as_of_date}).fetchone()

        defaults = {
            "avg_pace_l5": 99.5,
            "avg_def_rtg_l5": 112.0,
            "avg_fg3a_l5": 34.0,
            "avg_fg3_pct_l5": 0.36,
        }
        if result is None:
            return {f"{prefix}_{k}": v for k, v in defaults.items()}
        return {f"{prefix}_{k}": v if v is not None else defaults.get(k, 0) for k, v in result._mapping.items()}

    def _get_opponent_positional_stats(self, conn, opponent_id, position_group, as_of_date):
        """Fetch opponent's positional defense stats. Returns 0 if not found."""
        query = text("""
            SELECT
                off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
            FROM team_allowed_by_position
            WHERE team_id = :opponent_id
              AND position_group = :position_group
              AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(
            query,
            {"opponent_id": opponent_id, "position_group": position_group, "as_of_date": as_of_date},
        ).fetchone()

        opp_pos_defaults = {
            "off_rtg_allowed_l5": 112.0,
            "off_rtg_allowed_l15": 112.0,
        }
        if result is None:
            return {
                "opp_pos_off_rtg_allowed_l5": 112.0,
                "opp_pos_reb_allowed_l5": 0,
                "opp_pos_ast_allowed_l5": 0,
                "opp_pos_threes_allowed_l5": 0,
                "opp_pos_threes_per100_allowed_l5": 0,
                "opp_pos_reb_per100_allowed_l5": 0,
                "opp_pos_ast_per100_allowed_l5": 0,
                "opp_pos_off_rtg_allowed_l15": 112.0,
                "opp_pos_reb_allowed_l15": 0,
                "opp_pos_ast_allowed_l15": 0,
                "opp_pos_threes_allowed_l15": 0,
            }
        return {f"opp_pos_{k}": v if v is not None else opp_pos_defaults.get(k, 0) for k, v in result._mapping.items()}

    def _get_game_lines(self, conn, game_id):
        """Fetch spread/total from betting lines. Returns 0 if not found."""
        query = text("""
            SELECT
                MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                MAX(CASE WHEN market_key = 'totals' THEN line END) as total
            FROM raw_game_lines_staging
            WHERE nba_game_id = :game_id
              AND bookmaker IN ('pinnacle', 'draftkings')
        """)
        result = conn.execute(query, {"game_id": game_id}).fetchone()

        return {
            "line_spread_raw": result.spread if result and result.spread else 0,
            "line_total": result.total if result and result.total else 0,
        }

    def _get_player_prop_lines(self, conn, player_id, game_id):
        """Fetch per-stat prop lines for a single player-game. Returns 0 if not found."""
        query = text("""
            SELECT
                MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
                MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
                MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
                MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
            FROM (
                SELECT DISTINCT ON (market_key) market_key, line
                FROM raw_player_props_combined
                WHERE player_id = :player_id
                  AND game_id = :game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
                ORDER BY market_key, snapshot_time DESC NULLS LAST
            ) sub
        """)
        result = conn.execute(query, {"player_id": player_id, "game_id": game_id}).fetchone()

        if result is None:
            return {
                "prop_line_pts": 0,
                "prop_line_reb": 0,
                "prop_line_ast": 0,
                "prop_line_threes": 0,
            }
        return {
            "prop_line_pts": result.prop_line_pts or 0,
            "prop_line_reb": result.prop_line_reb or 0,
            "prop_line_ast": result.prop_line_ast or 0,
            "prop_line_threes": result.prop_line_threes or 0,
        }

    def _get_injury_context(self, conn, player_id, team_id, opponent_id, game_date):
        """Fetch injury context: team/opponent OUT player aggregates + player injury status."""
        defaults = {
            "team_out_count": 0,
            "team_out_min_sum": 0,
            "team_out_pts_sum": 0,
            "team_out_reb_sum": 0,
            "team_out_ast_sum": 0,
            "team_out_usg_sum": 0,
            "opp_out_count": 0,
            "opp_out_min_sum": 0,
            "player_is_questionable": 0,
            "player_is_probable": 0,
        }

        # Team injury context: game stats
        team_game_query = text("""
            SELECT
                COUNT(*) as out_count,
                COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
                COALESCE(SUM(inj_sub.avg_pts_l5), 0) as out_pts_sum,
                COALESCE(SUM(inj_sub.avg_reb_l5), 0) as out_reb_sum,
                COALESCE(SUM(inj_sub.avg_ast_l5), 0) as out_ast_sum
            FROM (
                SELECT DISTINCT ON (ri.player_id)
                    pags_inj.avg_min_l5, pags_inj.avg_pts_l5,
                    pags_inj.avg_reb_l5, pags_inj.avg_ast_l5
                FROM rapidapi_injuries ri
                LEFT JOIN player_average_game_stats pags_inj
                    ON pags_inj.player_id = ri.player_id
                    AND pags_inj.game_date < :game_date
                WHERE ri.nba_team_id = :team_id
                  AND ri.report_date = :game_date
                  AND ri.status = 'Out'
                  AND ri.player_id IS NOT NULL
                ORDER BY ri.player_id, pags_inj.game_date DESC
            ) inj_sub
        """)
        team_game_result = conn.execute(
            team_game_query, {"team_id": team_id, "game_date": game_date}
        ).fetchone()
        if team_game_result:
            defaults["team_out_count"] = team_game_result.out_count or 0
            defaults["team_out_min_sum"] = float(team_game_result.out_min_sum or 0)
            defaults["team_out_pts_sum"] = float(team_game_result.out_pts_sum or 0)
            defaults["team_out_reb_sum"] = float(team_game_result.out_reb_sum or 0)
            defaults["team_out_ast_sum"] = float(team_game_result.out_ast_sum or 0)

        # Team injury context: advanced stats
        team_adv_query = text("""
            SELECT
                COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum
            FROM (
                SELECT DISTINCT ON (ri.player_id)
                    paas_inj.avg_usg_pct_l5
                FROM rapidapi_injuries ri
                LEFT JOIN player_average_advanced_stats paas_inj
                    ON paas_inj.player_id = ri.player_id
                    AND paas_inj.game_date < :game_date
                WHERE ri.nba_team_id = :team_id
                  AND ri.report_date = :game_date
                  AND ri.status = 'Out'
                  AND ri.player_id IS NOT NULL
                ORDER BY ri.player_id, paas_inj.game_date DESC
            ) inj_sub
        """)
        team_adv_result = conn.execute(
            team_adv_query, {"team_id": team_id, "game_date": game_date}
        ).fetchone()
        if team_adv_result:
            defaults["team_out_usg_sum"] = float(team_adv_result.out_usg_sum or 0)

        # Opponent injury context
        opp_query = text("""
            SELECT
                COUNT(*) as out_count,
                COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum
            FROM (
                SELECT DISTINCT ON (ri.player_id)
                    pags_inj.avg_min_l5
                FROM rapidapi_injuries ri
                LEFT JOIN player_average_game_stats pags_inj
                    ON pags_inj.player_id = ri.player_id
                    AND pags_inj.game_date < :game_date
                WHERE ri.nba_team_id = :opponent_id
                  AND ri.report_date = :game_date
                  AND ri.status = 'Out'
                  AND ri.player_id IS NOT NULL
                ORDER BY ri.player_id, pags_inj.game_date DESC
            ) inj_sub
        """)
        opp_result = conn.execute(
            opp_query, {"opponent_id": opponent_id, "game_date": game_date}
        ).fetchone()
        if opp_result:
            defaults["opp_out_count"] = opp_result.out_count or 0
            defaults["opp_out_min_sum"] = float(opp_result.out_min_sum or 0)

        # Player injury status
        player_query = text("""
            SELECT status
            FROM rapidapi_injuries
            WHERE player_id = :player_id
              AND report_date = :game_date
            ORDER BY id DESC LIMIT 1
        """)
        player_result = conn.execute(
            player_query, {"player_id": player_id, "game_date": game_date}
        ).fetchone()
        if player_result:
            defaults["player_is_questionable"] = 1 if player_result.status == "Questionable" else 0
            defaults["player_is_probable"] = 1 if player_result.status == "Probable" else 0

        return defaults
