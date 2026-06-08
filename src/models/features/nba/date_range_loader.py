"""NBA date-range batch feature loader."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from sqlalchemy import text

from src.models.features.nba.date_batch_loader import DateBatchFeatureLoader
from src.models.features.nba.requests import DateFeatureRequest, DateRangeFeatureRequest

logger = logging.getLogger(__name__)


class DateRangeFeatureLoader:
    """Load all NBA player features for an inclusive date range."""

    def __init__(self, feature_store):
        self.feature_store = feature_store
        self.engine = feature_store.engine
        self.config = feature_store.config

    def __getattr__(self, name):
        return getattr(self.feature_store, name)

    def get_features_for_date(self, game_date: date) -> pd.DataFrame:
        return DateBatchFeatureLoader(self.feature_store).load(DateFeatureRequest(game_date=game_date))

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

    def load(self, request: DateRangeFeatureRequest, chunk_size: int = 25) -> dict[date, pd.DataFrame]:

        start_date = request.start_date
        end_date = request.end_date
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
                    -- B3: Momentum ratios — NOTE (ISS-017): *_l3_l15_ratio names
                    -- only accurate for PTS; REB/AST/THREES use L3/L5 denominator.
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
                           avg_min_szn,
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
                      AND COALESCE(snapshot_time, inserted_at)::date <= pgs.game_date
                      AND (commence_time IS NULL OR COALESCE(snapshot_time, inserted_at) < commence_time)
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
