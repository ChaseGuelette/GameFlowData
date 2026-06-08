"""NBA training feature loader."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from src.models.features.nba.requests import TrainingFeatureRequest


class TrainingFeatureLoader:
    """Load NBA training feature datasets."""

    def __init__(self, feature_store):
        self.feature_store = feature_store
        self.engine = feature_store.engine
        self.config = feature_store.config

    def __getattr__(self, name):
        return getattr(self.feature_store, name)

    def load(self, request: TrainingFeatureRequest) -> pd.DataFrame:
        """Load a full training dataframe for requested seasons."""
        seasons = request.seasons
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
        team_inj, inj_per_player = self._load_injury_features_bulk(game_dates)
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

        # Position-matched injury features: aggregate OUT teammates in same position group
        if not inj_per_player.empty and "position_group" in df.columns:
            pos_inj = inj_per_player[inj_per_player["inj_position_group"].notna()].copy()
            if not pos_inj.empty:
                # Compute starter_prob for each injured player (same formula as feature_store)
                pos_inj["_starter_prob"] = (pos_inj["games_started_l5"].fillna(0) / 5.0).clip(0, 1)
                pos_agg = pos_inj.groupby(
                    ["nba_team_id", "report_date", "inj_position_group"]
                ).agg(
                    same_pos_out_count=("inj_player_id", "nunique"),
                    same_pos_out_min_sum=("avg_min_l5", lambda x: np.nansum(x)),
                    same_pos_out_usg_sum=("avg_usg_pct_l5", lambda x: np.nansum(x)),
                    same_pos_out_starter_sum=("_starter_prob", lambda x: np.nansum(x)),
                ).reset_index()
                pos_agg_renamed = pos_agg.rename(columns={
                    "nba_team_id": "_inj_team_id",
                    "report_date": "_inj_date",
                    "inj_position_group": "_inj_pos",
                    "same_pos_out_count": "team_out_same_pos_count",
                    "same_pos_out_min_sum": "team_out_same_pos_min_sum",
                    "same_pos_out_usg_sum": "team_out_same_pos_usg_sum",
                    "same_pos_out_starter_sum": "team_out_same_pos_starter_sum",
                })
                df = df.merge(
                    pos_agg_renamed,
                    left_on=["team_id", "game_date", "position_group"],
                    right_on=["_inj_team_id", "_inj_date", "_inj_pos"],
                    how="left",
                ).drop(columns=["_inj_team_id", "_inj_date", "_inj_pos"], errors="ignore")
            else:
                df["team_out_same_pos_count"] = 0
                df["team_out_same_pos_min_sum"] = 0
                df["team_out_same_pos_usg_sum"] = 0
                df["team_out_same_pos_starter_sum"] = 0
        else:
            df["team_out_same_pos_count"] = 0
            df["team_out_same_pos_min_sum"] = 0
            df["team_out_same_pos_usg_sum"] = 0
            df["team_out_same_pos_starter_sum"] = 0

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
            "team_out_same_pos_count", "team_out_same_pos_min_sum",
            "team_out_same_pos_usg_sum", "team_out_same_pos_starter_sum",
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
        if len(df) < 1000:
            raise ValueError(f"Suspiciously few rows: {len(df)}. Check query/season_ids.")

        if not df["position_group"].notna().all():
            raise ValueError("CRITICAL: Position Group has NULLs. Filter logic failed.")

        # Rate targets for training
        mask = df["actual_minutes"] >= self.config.min_minutes_for_rate
        for stat in ["pts", "reb", "ast", "threes"]:
            df.loc[mask, f"{stat}_per_min"] = df.loc[mask, f"actual_{stat}"] / df.loc[mask, "actual_minutes"]

        return df

    def load_single_season(self, season: str) -> pd.DataFrame:
        """Load training features for one season."""
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

    def _load_single_season_training(self, season: str) -> pd.DataFrame:
        return self.load_single_season(season)
