"""NBA single-date batch feature loader."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.models.features.nba.requests import DateFeatureRequest


class DateBatchFeatureLoader:
    """Load all NBA player features for one game date."""

    def __init__(self, feature_store):
        self.feature_store = feature_store
        self.engine = feature_store.engine
        self.config = feature_store.config

    def load(self, request: DateFeatureRequest) -> pd.DataFrame:
        """Load a single-date feature dataframe."""
        game_date = request.game_date
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

                -- B3: Momentum ratios — NOTE (ISS-017): *_l3_l15_ratio names are
                -- misleading: only PTS uses L15 denominator. REB/AST/THREES use
                -- L3/L5. Names kept for saved model artifact compatibility.
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

                -- Minutes trend ratios (role-change signal)
                CASE WHEN COALESCE(p_avg.avg_min_l5, 0) > 0
                     THEN COALESCE(p_avg.avg_min_l3, 0) / p_avg.avg_min_l5
                     ELSE 1.0 END as player_min_l3_l5_ratio,
                CASE WHEN COALESCE(p_avg.avg_min_l15, 0) > 0
                     THEN COALESCE(p_avg.avg_min_l3, 0) / p_avg.avg_min_l15
                     ELSE 1.0 END as player_min_l3_l15_ratio,

                -- Season average minutes (baseline anchor)
                COALESCE(p_avg.avg_min_szn, 0) as player_avg_min_szn,

                -- B3/B4: L5 standard deviations
                COALESCE(p_avg.std_min_l5, 0) as player_min_std_l5,
                COALESCE(p_avg.std_pts_l5, 0) as player_std_pts_l5,
                COALESCE(p_avg.std_reb_l5, 0) as player_std_reb_l5,
                COALESCE(p_avg.std_ast_l5, 0) as player_std_ast_l5,
                COALESCE(p_avg.std_fg3m_l5, 0) as player_std_fg3m_l5,

                -- B4: Minutes stability
                COALESCE(p_avg.min_floor_l5, 0) as player_min_floor_l5,
                COALESCE(p_avg.games_started_l5, 0) as player_games_started_l5,
                LEAST(COALESCE(p_avg.games_started_l5, 0) / 5.0, 1.0) as player_starter_prob,

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
                       avg_min_szn,
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
                      AND COALESCE(snapshot_time, inserted_at)::date <= pgs.game_date
                      AND (commence_time IS NULL OR COALESCE(snapshot_time, inserted_at) < commence_time)
                    ORDER BY market_key, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST
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
