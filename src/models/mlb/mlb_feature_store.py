"""MLB Feature Store — Central feature engineering for pitcher strikeout model.

Mirrors the pattern from src/models/feature_store.py (NBA) but adapted for MLB:
- No minutes decomposition (MLB stats predicted directly)
- Pitcher rolling averages from mlb_player_average_pitching
- Statcast features from mlb_player_average_statcast_pitching
- Park factors from mlb_park_factors
- Opposing team batting from mlb_player_game_stats_batting (aggregated)
- Prop/game lines from mlb_raw_player_props and mlb_raw_game_lines
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.models.mlb.features.contracts import (
    LINEUP_FEATURE_DEFAULTS,
    PITCHER_K_FEATURES,
)
from src.models.mlb.features.prop_line_feature_source import (
    build_lateral_prop_line_join,
    fetch_single_prop_line,
)
from src.models.mlb.features.transforms import (
    add_pitcher_derived_features,
    add_pitcher_interaction_features,
)

logger = logging.getLogger(__name__)


@dataclass
class MLBFeatureConfig:
    """Configuration for MLB feature engineering."""

    min_starts_for_stable: int = 3


class MLBFeatureStore:
    """Feature store for MLB pitcher strikeout model.

    Provides training data loading, single-player inference features,
    and batch features for backtesting — all with time-travel safety.
    """

    def __init__(self, engine: Engine, config: MLBFeatureConfig | None = None):
        self.engine = engine
        self.config = config or MLBFeatureConfig()

    def _table_exists(self, table_name: str) -> bool:
        """Return True when an optional public table exists in the target DB."""
        with self.engine.connect() as conn:
            return bool(
                conn.execute(
                    text("SELECT to_regclass(:table_name) IS NOT NULL"),
                    {"table_name": f"public.{table_name}"},
                ).scalar()
            )

    # ------------------------------------------------------------------
    # Training data
    # ------------------------------------------------------------------

    def get_training_dataset(self, seasons: list[int]) -> pd.DataFrame:
        """Load training features for one or more seasons.

        Returns DataFrame with PITCHER_K_FEATURES columns plus identifiers
        and the target column 'actual_so'.
        """
        frames = []
        for season in seasons:
            logger.info("Loading MLB training data for season %d...", season)
            df = self._load_single_season_training(season)
            logger.info("  Season %d: %d rows", season, len(df))
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)

        # Compute derived features in Python
        combined = self._add_derived_features(combined)

        logger.info("Total MLB training rows: %d", len(combined))
        return combined

    def _load_single_season_training(self, season: int) -> pd.DataFrame:
        """Load training features for a single MLB season via SQL joins."""
        query = text("""
            SELECT
                -- Identifiers
                pgs.game_id,
                pgs.player_id,
                pgs.game_date::date,
                pgs.season,
                pgs.team_id,

                -- Target
                pgs.so AS actual_so,
                pgs.ip AS actual_ip,

                -- Game context
                CASE WHEN gs.home_team_id = pgs.team_id THEN 1 ELSE 0 END AS is_home,
                CASE WHEN gs.home_team_id = pgs.team_id
                     THEN gs.away_team_id
                     ELSE gs.home_team_id END AS opp_team_id,

                -- Pitcher rolling averages
                COALESCE(p_avg.avg_so_l3, 0) AS pitcher_avg_so_l3,
                COALESCE(p_avg.avg_so_l5, 0) AS pitcher_avg_so_l5,
                COALESCE(p_avg.avg_so_szn, 0) AS pitcher_avg_so_szn,
                COALESCE(p_avg.avg_k_per_9_l5, 0) AS pitcher_avg_k_per_9_l5,
                COALESCE(p_avg.avg_ip_l3, 0) AS pitcher_avg_ip_l3,
                COALESCE(p_avg.avg_ip_l5, 0) AS pitcher_avg_ip_l5,
                COALESCE(p_avg.avg_ip_szn, 0) AS pitcher_avg_ip_szn,
                COALESCE(ip_ctx.min_ip_l5, COALESCE(p_avg.min_ip_l5, 0)) AS pitcher_min_ip_l5,
                COALESCE(ip_ctx.max_ip_l5, 0) AS pitcher_max_ip_l5,
                COALESCE(ip_ctx.median_ip_l5, COALESCE(p_avg.avg_ip_l5, 0)) AS pitcher_median_ip_l5,
                COALESCE(ip_ctx.ip_range_l5, 0) AS pitcher_ip_range_l5,
                COALESCE(ip_ctx.short_start_rate_l5, 0) AS pitcher_short_start_rate_l5,
                COALESCE(ip_ctx.pct_starts_under_5_ip_l10, 0) AS pitcher_pct_starts_under_5_ip_l10,
                COALESCE(ip_ctx.left_last_start_early_flag, 0) AS pitcher_left_last_start_early_flag,
                COALESCE(ip_ctx.start_stability_l5, 0) AS pitcher_start_stability_l5,
                COALESCE(ip_ctx.avg_batters_faced_l5, COALESCE((3 * p_avg.avg_ip_l5 + p_avg.avg_h_allowed_l5 + p_avg.avg_bb_l5), 0)) AS pitcher_avg_batters_faced_l5,
                COALESCE((3 * p_avg.avg_ip_szn + p_avg.avg_h_allowed_l5 + p_avg.avg_bb_l5), 0) AS pitcher_avg_batters_faced_szn,
                COALESCE(ip_ctx.avg_pitches_thrown_l5, COALESCE(p_avg.avg_pitches_thrown_l5, 0)) AS pitcher_avg_pitches_per_start_l5,
                COALESCE(ip_ctx.workload_spike_ratio, 1.0) AS pitcher_workload_spike_ratio,
                COALESCE(ip_ctx.recent_pitch_count_trend, 1.0) AS pitcher_recent_pitch_count_trend,
                COALESCE(ip_ctx.rest_after_high_pitch_count, (LEAST(COALESCE(p_avg.days_rest, 5), 14) / 5.0) * (COALESCE(p_avg.pitch_count_last_start, 0) / 100.0)) AS rest_after_high_pitch_count,
                COALESCE(p_avg.avg_bb_l5, 0) AS pitcher_avg_bb_l5,
                COALESCE(p_avg.std_so_l3, 0) AS pitcher_std_so_l3,
                COALESCE(p_avg.avg_h_allowed_l5, 0) AS pitcher_avg_h_allowed_l5,

                -- Pitcher context
                LEAST(COALESCE(p_avg.days_rest, 5), 14) AS pitcher_days_rest,
                COALESCE(p_avg.pitch_count_last_start, 0) AS pitcher_pitch_count_last_start,
                COALESCE(p_avg.starts_szn, 0) AS pitcher_starts_szn,

                -- Statcast pitching averages
                COALESCE(sc_avg.avg_whiff_pct_l5, 0) AS pitcher_avg_whiff_pct_l5,
                COALESCE(sc_avg.avg_csw_pct_l5, 0) AS pitcher_avg_csw_pct_l5,
                COALESCE(sc_avg.avg_chase_pct_l5, 0) AS pitcher_avg_chase_pct_l5,
                COALESCE(sc_avg.avg_zone_pct_l5, 0) AS pitcher_avg_zone_pct_l5,
                COALESCE(sc_avg.avg_avg_fastball_velo_l5, 0) AS pitcher_avg_fastball_velo_l5,
                COALESCE(sc_avg.avg_avg_fastball_velo_l3 - sc_avg.avg_avg_fastball_velo_szn, 0) AS pitcher_fastball_velo_delta_l3_vs_szn,
                COALESCE(sc_avg.std_whiff_pct_l3, 0) AS pitcher_std_whiff_pct_l3,
                COALESCE(sc_avg.avg_fastball_pct_l5, 0) AS pitcher_fastball_pct_l5,
                COALESCE(sc_avg.avg_breaking_pct_l5, 0) AS pitcher_breaking_pct_l5,
                COALESCE(sc_avg.avg_offspeed_pct_l5, 0) AS pitcher_offspeed_pct_l5,

                -- FanGraphs season-to-date (point-in-time; LATERAL strict as_of_date < game_date)
                COALESCE(fg.fip, 0) AS pitcher_fip_szn,
                COALESCE(fg.k_pct, 0) AS pitcher_k_pct_szn,

                -- Park factor
                COALESCE(pf.so_factor, 1.0) AS park_so_factor,

                -- Weather features
                COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
                COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,

                -- Own-team bullpen workload
                COALESCE(bull.bullpen_ip_last_3d, 0) AS team_bullpen_ip_last_3d,
                COALESCE(bull.bullpen_pitches_last_3d, 0) AS team_bullpen_pitches_last_3d,

                -- Team starter context (previous 10/30 starts)
                COALESCE(team_leash.avg_ip_l10, 0) AS team_starter_avg_ip_l10,
                COALESCE(team_leash.short_start_rate_l10, 0) AS team_starter_short_start_rate_l10,
                COALESCE(team_leash.avg_pitches_thrown_l10, 0) AS team_starter_avg_pitches_l10,
                COALESCE(team_leash.avg_ip_l30, 0) AS team_starter_avg_ip_l30,
                COALESCE(team_leash.short_hook_rate_l30, 0) AS team_starter_short_hook_rate_l30,
                COALESCE(team_leash.manager_short_hook_rate_l30, 0) AS manager_starter_short_hook_rate_l30,
                COALESCE(team_leash.deep_start_rate_l30, 0) AS team_starter_deep_start_rate_l30,

                -- Game lines (total)
                COALESCE(lines.game_total, 0) AS line_total,

                -- Player prop line (pitcher strikeouts)
                COALESCE(props.prop_line, 0) AS prop_line_pitcher_strikeouts,

                -- Inning-level fatigue features (L5 starts)
                COALESCE(inn_agg.velo_drop_late, 0) AS pitcher_velo_drop_late_l5,
                COALESCE(inn_agg.avg_whiff_rate_late, 0) AS pitcher_avg_whiff_rate_late_l5,
                COALESCE(inn_agg.avg_k_rate_early, 0) AS pitcher_avg_k_rate_early_l5,
                COALESCE(inn_agg.avg_pitches_per_inning, 15) AS pitcher_avg_pitches_per_inning_l5,
                COALESCE(inn_agg.avg_csw_rate, 0) AS pitcher_avg_csw_rate_l5_inning,
                COALESCE(inn_agg.deep_inning_pct, 0.5) AS pitcher_deep_inning_pct_l5,
                COALESCE(inn_agg.avg_k_first_5ip, 0) AS pitcher_avg_k_first_5ip_l5

            FROM mlb_player_game_stats_pitching pgs

            JOIN mlb_game_schedule gs
                ON pgs.game_id = gs.game_id

            -- Pitcher rolling averages
            -- NOTE: Using <= because the row for game_date X contains averages
            -- computed BEFORE game X (shift(1) in populate_averages). Safe.
            LEFT JOIN LATERAL (
                SELECT avg_so_l3, avg_so_l5, avg_so_szn,
                       avg_k_per_9_l5,
                       avg_ip_l3, avg_ip_l5, avg_ip_szn,
                       avg_pitches_thrown_l3, avg_pitches_thrown_l5,
                       min_ip_l5,
                       avg_bb_l5, avg_h_allowed_l5,
                       std_so_l3,
                       days_rest, pitch_count_last_start, starts_szn
                FROM mlb_player_average_pitching pa
                WHERE pa.player_id = pgs.player_id
                  AND pa.game_date <= pgs.game_date
                ORDER BY pa.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Additional pitcher IP-context features from raw recent starts
            LEFT JOIN LATERAL (
                WITH recent_pitcher_starts AS (
                    SELECT
                        ip,
                        outs_recorded,
                        h_allowed,
                        bb,
                        pitches_thrown,
                        game_date,
                        ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                    FROM mlb_player_game_stats_pitching
                    WHERE player_id = pgs.player_id
                      AND season = pgs.season
                      AND is_starter = TRUE
                      AND did_not_play = FALSE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 10
                ),
                previous_start AS (
                    SELECT ip, game_date
                    FROM recent_pitcher_starts
                    WHERE rn = 1
                ),
                prior_to_previous_start AS (
                    SELECT AVG(prev.ip) AS avg_ip_before_previous_start
                    FROM mlb_player_game_stats_pitching prev
                    JOIN previous_start ps ON TRUE
                    WHERE prev.player_id = pgs.player_id
                      AND prev.season = pgs.season
                      AND prev.is_starter = TRUE
                      AND prev.did_not_play = FALSE
                      AND prev.game_date < ps.game_date
                )
                SELECT
                    MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS min_ip_l5,
                    MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS max_ip_l5,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rps.ip) FILTER (WHERE rps.rn <= 5) AS median_ip_l5,
                    MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) - MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS ip_range_l5,
                    SUM(CASE WHEN rps.rn <= 5 AND rps.ip < 4.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END), 0) AS short_start_rate_l5,
                    SUM(CASE WHEN rps.ip < 5.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS pct_starts_under_5_ip_l10,
                    CASE
                        WHEN MAX(ps.ip) IS NOT NULL
                             AND MAX(prior.avg_ip_before_previous_start) IS NOT NULL
                             AND MAX(ps.ip) <= MAX(prior.avg_ip_before_previous_start) - 1.5
                        THEN 1 ELSE 0
                    END AS left_last_start_early_flag,
                    SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END)::float / 5.0 AS start_stability_l5,
                    AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.outs_recorded, 0) + COALESCE(rps.h_allowed, 0) + COALESCE(rps.bb, 0) END)::float AS avg_batters_faced_l5,
                    AVG(COALESCE(rps.outs_recorded, 0) + COALESCE(rps.h_allowed, 0) + COALESCE(rps.bb, 0))::float AS avg_batters_faced_szn,
                    AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END) AS avg_pitches_thrown_l5,
                    COALESCE(MAX(CASE WHEN rps.rn = 1 THEN COALESCE(rps.pitches_thrown, 0) END), 0)
                        / NULLIF(AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END), 0) AS workload_spike_ratio,
                    AVG(CASE WHEN rps.rn <= 3 THEN COALESCE(rps.pitches_thrown, 0) END)
                        / NULLIF(AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END), 0) AS recent_pitch_count_trend,
                    (LEAST(COALESCE(p_avg.days_rest, 5), 14) / 5.0)
                        * (COALESCE(p_avg.pitch_count_last_start, 0) / 100.0) AS rest_after_high_pitch_count
                FROM recent_pitcher_starts rps
                LEFT JOIN previous_start ps ON TRUE
                LEFT JOIN prior_to_previous_start prior ON TRUE
            ) ip_ctx ON TRUE

            -- Team starter leash context (previous 30 team starts; L10 is a prefix subset)
            LEFT JOIN LATERAL (
                WITH recent_team_starts AS (
                    SELECT
                        ip,
                        pitches_thrown,
                        ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                    FROM mlb_player_game_stats_pitching
                    WHERE team_id = pgs.team_id
                      AND season = pgs.season
                      AND is_starter = TRUE
                      AND did_not_play = FALSE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 30
                )
                SELECT
                    AVG(CASE WHEN rn <= 10 THEN ip END) AS avg_ip_l10,
                    SUM(CASE WHEN rn <= 10 AND ip < 4.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN rn <= 10 THEN 1 ELSE 0 END), 0) AS short_start_rate_l10,
                    AVG(CASE WHEN rn <= 10 THEN COALESCE(pitches_thrown, 0) END) AS avg_pitches_thrown_l10,
                    AVG(ip) AS avg_ip_l30,
                    SUM(CASE WHEN ip < 4.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS short_hook_rate_l30,
                    SUM(CASE WHEN ip < 5.0 AND COALESCE(pitches_thrown, 0) < 80 THEN 1 ELSE 0 END)::float
                        / NULLIF(COUNT(*), 0) AS manager_short_hook_rate_l30,
                    SUM(CASE WHEN ip >= 6.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS deep_start_rate_l30
                FROM recent_team_starts
            ) team_leash ON TRUE

            -- Statcast pitching averages
            LEFT JOIN LATERAL (
                SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                       avg_chase_pct_l5, avg_zone_pct_l5,
                       avg_avg_fastball_velo_l3, avg_avg_fastball_velo_l5, avg_avg_fastball_velo_szn,
                       std_whiff_pct_l3,
                       avg_fastball_pct_l5, avg_breaking_pct_l5, avg_offspeed_pct_l5
                FROM mlb_player_average_statcast_pitching sc
                WHERE sc.player_id = pgs.player_id
                  AND sc.game_date <= pgs.game_date
                ORDER BY sc.game_date DESC LIMIT 1
            ) sc_avg ON TRUE

            -- FanGraphs season-to-date snapshot (LATERAL, strict as_of_date < game_date)
            LEFT JOIN LATERAL (
                SELECT fip, k_pct
                FROM mlb_player_season_advanced_history h
                WHERE h.player_id = pgs.player_id
                  AND h.player_type = 'pitcher'
                  AND h.season = pgs.season
                  AND h.as_of_date < pgs.game_date
                ORDER BY h.as_of_date DESC
                LIMIT 1
            ) fg ON TRUE

            -- Park factors (join on venue)
            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            -- Game weather
            LEFT JOIN mlb_game_weather gw ON gw.game_pk = pgs.game_id

            -- Own-team bullpen workload
            LEFT JOIN mlb_bullpen_daily_status bull
                ON bull.team_id = pgs.team_id
               AND bull.game_date = pgs.game_date

            -- Game total line (latest snapshot from pinnacle/draftkings)
            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Pitcher strikeout prop line (latest point-in-time snapshot)
            {prop_line_lateral_join}

            -- Inning-level fatigue from L5 starts
            LEFT JOIN LATERAL (
                SELECT
                    AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END)
                        - COALESCE(AVG(CASE WHEN sub.inning >= 5 THEN sub.avg_release_speed END),
                                   AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END))
                        AS velo_drop_late,
                    CASE WHEN SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END) > 0
                         THEN SUM(CASE WHEN sub.inning >= 5 THEN sub.whiff_rate * sub.pitches_thrown END)
                              / SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END)
                         ELSE 0 END AS avg_whiff_rate_late,
                    CASE WHEN SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END) > 0
                         THEN SUM(CASE WHEN sub.inning <= 3 THEN sub.strikeouts END)::float
                              / SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END)
                         ELSE 0 END AS avg_k_rate_early,
                    CASE WHEN COUNT(*) > 0
                         THEN AVG(sub.pitches_thrown)
                         ELSE 15 END AS avg_pitches_per_inning,
                    CASE WHEN SUM(sub.pitches_thrown) > 0
                         THEN SUM(sub.csw_rate * sub.pitches_thrown) / SUM(sub.pitches_thrown)
                         ELSE 0 END AS avg_csw_rate,
                    COUNT(DISTINCT CASE WHEN sub.inning >= 6 THEN sub.game_id END)::float
                        / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct,
                    COALESCE(
                        SUM(CASE WHEN sub.inning <= 5 THEN sub.strikeouts ELSE 0 END)::FLOAT /
                        NULLIF(SUM(CASE WHEN sub.inning <= 5 THEN sub.batters_faced ELSE 0 END), 0),
                        0
                    ) AS avg_k_first_5ip
                FROM mlb_pitcher_inning_stats sub
                INNER JOIN (
                    SELECT DISTINCT game_id, game_date
                    FROM mlb_pitcher_inning_stats
                    WHERE player_id = pgs.player_id
                      AND is_starter = TRUE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 5
                ) recent ON sub.game_id = recent.game_id
                WHERE sub.player_id = pgs.player_id
            ) inn_agg ON TRUE

            WHERE pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND gs.game_type = 'R'
              AND pgs.season = :season
        """.replace(
            "{prop_line_lateral_join}",
            build_lateral_prop_line_join(row_alias="pgs", market_key_sql="'pitcher_strikeouts'"),
        ))

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"season": season, "as_of_time": None})

        return df

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Python-computed derived/default features after SQL load."""
        return add_pitcher_derived_features(df)

    def enrich_with_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge opposing team batting stats into training DataFrame.

        Uses compute_matchup_features_bulk from mlb_matchup_features for efficiency.
        """
        from src.processing.mlb.mlb_matchup_features import (
            compute_lineup_features_bulk,
            compute_matchup_features_bulk,
        )

        seasons = df["season"].unique()
        matchup_frames = []
        for season in seasons:
            mf = compute_matchup_features_bulk(self.engine, int(season))
            if not mf.empty:
                matchup_frames.append(mf)

        matchup_cols = [
            "opp_team_avg_so_l10",
            "opp_team_avg_batting_avg_l10",
            "opp_team_k_pct_l10",
            "opp_team_whiff_pct_l10",
            "opp_team_contact_rate_l10",
            "opp_team_chase_pct_l10",
            "opp_team_zone_contact_pct_l10",
        ]
        matchup_defaults = {
            "opp_team_avg_so_l10": 0,
            "opp_team_avg_batting_avg_l10": 0,
            "opp_team_k_pct_l10": 0,
            "opp_team_whiff_pct_l10": 0,
            "opp_team_contact_rate_l10": 1.0,
            "opp_team_chase_pct_l10": 0,
            "opp_team_zone_contact_pct_l10": 0,
        }

        if matchup_frames:
            matchup_df = pd.concat(matchup_frames, ignore_index=True)
            df = df.merge(
                matchup_df,
                on=["player_id", "game_id"],
                how="left",
                suffixes=("", "_matchup"),
            )

            for col in matchup_cols:
                match_col = f"{col}_matchup"
                if match_col in df.columns:
                    if col in df.columns:
                        df[col] = df[match_col].combine_first(df[col])
                    else:
                        df[col] = df[match_col]
                    df = df.drop(columns=[match_col])

        # Fill missing matchup features with defaults
        for col, default in matchup_defaults.items():
            if col not in df.columns:
                df[col] = default
            df[col] = df[col].fillna(default)

        # Lineup-based features (projected K%, contact profile, handedness composition)
        lineup_frames = []
        for season in seasons:
            lf = compute_lineup_features_bulk(self.engine, int(season))
            if not lf.empty:
                lineup_frames.append(lf)

        if lineup_frames:
            lineup_df = pd.concat(lineup_frames, ignore_index=True)
            df = df.merge(lineup_df, on=["player_id", "game_id"], how="left", suffixes=("", "_lineup"))
            for col in LINEUP_FEATURE_DEFAULTS:
                lineup_col = f"{col}_lineup"
                if lineup_col in df.columns:
                    if col in df.columns:
                        # Computed lineup values must override pre-existing neutral defaults.
                        df[col] = df[lineup_col].combine_first(df[col])
                    else:
                        df[col] = df[lineup_col]
                    df = df.drop(columns=[lineup_col])

        for col, default in LINEUP_FEATURE_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

        # Umpire tendency features
        df = self._compute_umpire_features_bulk(df)

        return df

    def _add_interaction_features(self, df):
        """Compute interaction features that depend on matchup data."""
        return add_pitcher_interaction_features(df)

    # ------------------------------------------------------------------
    # Single-player inference (game-time)
    # ------------------------------------------------------------------

    def get_player_game_features(
        self,
        player_id: int,
        game_id: int,
        game_date: str,
        team_id: int,
        opp_team_id: int,
        venue_id: int,
        season: int,
        is_home: bool,
        as_of_time: datetime | None = None,
    ) -> dict:
        """Assemble features for a single pitcher for inference.

        Returns flat dict with all PITCHER_K_FEATURES keys.
        """
        features: dict = {}

        # 1. Pitcher rolling averages
        pitching_avgs = self._get_pitcher_rolling_stats(player_id, game_date)
        features.update(pitching_avgs)

        # 2. Statcast averages
        statcast_avgs = self._get_statcast_stats(player_id, game_date)
        features.update(statcast_avgs)

        # 3. FanGraphs season-to-date snapshot (history table, date-guarded)
        features.update(self._get_fangraphs_stats(player_id, season, game_date))

        # 4. Park factor
        features["park_so_factor"] = self._get_park_factor(venue_id, season)

        # 4b. Weather features
        features.update(self._get_game_weather(game_id))

        # 5. Game context
        features["is_home"] = 1 if is_home else 0

        # 5b. Own-team bullpen workload
        features.update(self._get_team_bullpen_stats(team_id, game_date))

        # 6. Game total line
        features["line_total"] = self._get_game_total(game_id)

        # 7. Prop line
        features["prop_line_pitcher_strikeouts"] = self._get_prop_line(
            player_id,
            game_id,
            as_of_time=as_of_time,
        )

        # 8. Opposing team batting
        from src.processing.mlb.mlb_matchup_features import (
            get_lineup_k_features,
            get_opposing_team_batting_stats,
            get_pitcher_handedness,
        )

        opp_stats = get_opposing_team_batting_stats(self.engine, opp_team_id, game_date, season)
        features.update(
            {
                "opp_team_avg_so_l10": opp_stats.get("opp_team_avg_so_l10") or 0,
                "opp_team_avg_batting_avg_l10": opp_stats.get("opp_team_avg_batting_avg_l10") or 0,
                "opp_team_k_pct_l10": opp_stats.get("opp_team_k_pct_l10") or 0,
                "opp_team_whiff_pct_l10": opp_stats.get("opp_team_whiff_pct_l10") or 0,
                "opp_team_contact_rate_l10": (
                    1.0 - opp_stats["opp_team_whiff_pct_l10"]
                    if opp_stats.get("opp_team_whiff_pct_l10") is not None
                    else 1.0
                ),
                "opp_team_chase_pct_l10": opp_stats.get("opp_team_chase_pct_l10") or 0,
                "opp_team_zone_contact_pct_l10": opp_stats.get("opp_team_zone_contact_pct_l10") or 0,
            }
        )

        # 8a. Lineup-based features (K%, handedness)
        pitcher_throws = get_pitcher_handedness(self.engine, player_id)
        lineup_feats = get_lineup_k_features(
            self.engine, opp_team_id, game_id, game_date, season,
            pitcher_throws=pitcher_throws,
        )
        features.update(lineup_feats)

        # 8b. Inning-level fatigue features
        inning_fatigue = self._get_inning_fatigue_stats(player_id, game_date)
        features.update(inning_fatigue)

        # 9. Add exact IP context and derived defaults
        features.update(
            self._get_pitcher_ip_context_features(
                player_id=player_id,
                game_date=game_date,
                season=season,
                days_rest=features.get("pitcher_days_rest", 5),
                pitch_count_last_start=features.get("pitcher_pitch_count_last_start", 0),
            )
        )

        # Team-level starter leash context from recent starts
        features.update(self._get_team_starter_leash_features(team_id=team_id, game_date=game_date, season=season))

        # Bullpen fatigue pressure from joined bullpen context
        features["bullpen_fatigue_pressure"] = (
            features.get("team_bullpen_ip_last_3d", 0) / 9.0
            + features.get("team_bullpen_pitches_last_3d", 0) / 150.0
        )

        # Run batch-derived feature normalization for consistency
        derived = self._add_derived_features(pd.DataFrame([features]))
        features = derived.iloc[0].to_dict()

        # Interaction features
        features["pitcher_k_opp_k_interaction"] = (
            features.get("pitcher_avg_k_per_9_l5", 0) *
            features.get("opp_team_k_pct_l10", 0)
        )
        features["pitcher_whiff_opp_whiff_interaction"] = (
            features.get("pitcher_avg_whiff_pct_l5", 0) *
            features.get("opp_team_whiff_pct_l10", 0)
        )

        # Pitch repertoire diversity
        pitch_pcts = [
            features.get("pitcher_fastball_pct_l5", 0),
            features.get("pitcher_breaking_pct_l5", 0),
            features.get("pitcher_offspeed_pct_l5", 0),
        ]
        features["pitcher_num_pitch_types_l5"] = sum(1 for p in pitch_pcts if p > 0.05)

        # Umpire tendency
        features.update(self._get_umpire_features(game_id, game_date))

        # Keep return contract stable for callers expecting feature store schema.
        return {k: features[k] for k in PITCHER_K_FEATURES if k in features}

    # ------------------------------------------------------------------
    # Batch inference (backtesting)
    # ------------------------------------------------------------------

    def get_features_for_date(self, game_date: str, as_of_time: datetime | None = None) -> pd.DataFrame:
        """Get features for all starting pitchers on a given date.

        Uses the same SQL pattern as training but filtered to a single date.
        Returns DataFrame with PITCHER_K_FEATURES columns.
        """
        query = text("""
            SELECT
                pgs.game_id,
                pgs.player_id,
                pgs.game_date::date,
                pgs.season,
                pgs.team_id,
                p.player_name,

                -- Target (for backtesting evaluation)
                pgs.so AS actual_so,
                pgs.ip AS actual_ip,

                -- Game context
                CASE WHEN gs.home_team_id = pgs.team_id THEN 1 ELSE 0 END AS is_home,
                CASE WHEN gs.home_team_id = pgs.team_id
                     THEN gs.away_team_id
                     ELSE gs.home_team_id END AS opp_team_id,

                -- Pitcher rolling averages
                COALESCE(p_avg.avg_so_l3, 0) AS pitcher_avg_so_l3,
                COALESCE(p_avg.avg_so_l5, 0) AS pitcher_avg_so_l5,
                COALESCE(p_avg.avg_so_szn, 0) AS pitcher_avg_so_szn,
                COALESCE(p_avg.avg_k_per_9_l5, 0) AS pitcher_avg_k_per_9_l5,
                COALESCE(p_avg.avg_ip_l3, 0) AS pitcher_avg_ip_l3,
                COALESCE(p_avg.avg_ip_l5, 0) AS pitcher_avg_ip_l5,
                COALESCE(p_avg.avg_ip_szn, 0) AS pitcher_avg_ip_szn,
                COALESCE(ip_ctx.min_ip_l5, COALESCE(p_avg.min_ip_l5, 0)) AS pitcher_min_ip_l5,
                COALESCE(ip_ctx.max_ip_l5, 0) AS pitcher_max_ip_l5,
                COALESCE(ip_ctx.median_ip_l5, COALESCE(p_avg.avg_ip_l5, 0)) AS pitcher_median_ip_l5,
                COALESCE(ip_ctx.ip_range_l5, 0) AS pitcher_ip_range_l5,
                COALESCE(ip_ctx.short_start_rate_l5, 0) AS pitcher_short_start_rate_l5,
                COALESCE(ip_ctx.pct_starts_under_5_ip_l10, 0) AS pitcher_pct_starts_under_5_ip_l10,
                COALESCE(ip_ctx.left_last_start_early_flag, 0) AS pitcher_left_last_start_early_flag,
                COALESCE(ip_ctx.start_stability_l5, 0) AS pitcher_start_stability_l5,
                COALESCE(ip_ctx.avg_batters_faced_l5, COALESCE((3 * p_avg.avg_ip_l5 + p_avg.avg_h_allowed_l5 + p_avg.avg_bb_l5), 0)) AS pitcher_avg_batters_faced_l5,
                COALESCE((3 * p_avg.avg_ip_szn + p_avg.avg_h_allowed_l5 + p_avg.avg_bb_l5), 0) AS pitcher_avg_batters_faced_szn,
                COALESCE(ip_ctx.avg_pitches_thrown_l5, COALESCE(p_avg.avg_pitches_thrown_l5, 0)) AS pitcher_avg_pitches_per_start_l5,
                COALESCE(ip_ctx.workload_spike_ratio, 1.0) AS pitcher_workload_spike_ratio,
                COALESCE(ip_ctx.recent_pitch_count_trend, 1.0) AS pitcher_recent_pitch_count_trend,
                COALESCE(ip_ctx.rest_after_high_pitch_count, (LEAST(COALESCE(p_avg.days_rest, 5), 14) / 5.0) * (COALESCE(p_avg.pitch_count_last_start, 0) / 100.0)) AS rest_after_high_pitch_count,
                COALESCE(p_avg.avg_bb_l5, 0) AS pitcher_avg_bb_l5,
                COALESCE(p_avg.std_so_l3, 0) AS pitcher_std_so_l3,
                COALESCE(p_avg.avg_h_allowed_l5, 0) AS pitcher_avg_h_allowed_l5,

                -- Pitcher context
                LEAST(COALESCE(p_avg.days_rest, 5), 14) AS pitcher_days_rest,
                COALESCE(p_avg.pitch_count_last_start, 0) AS pitcher_pitch_count_last_start,
                COALESCE(p_avg.starts_szn, 0) AS pitcher_starts_szn,

                -- Statcast
                COALESCE(sc_avg.avg_whiff_pct_l5, 0) AS pitcher_avg_whiff_pct_l5,
                COALESCE(sc_avg.avg_csw_pct_l5, 0) AS pitcher_avg_csw_pct_l5,
                COALESCE(sc_avg.avg_chase_pct_l5, 0) AS pitcher_avg_chase_pct_l5,
                COALESCE(sc_avg.avg_zone_pct_l5, 0) AS pitcher_avg_zone_pct_l5,
                COALESCE(sc_avg.avg_avg_fastball_velo_l5, 0) AS pitcher_avg_fastball_velo_l5,
                COALESCE(sc_avg.avg_avg_fastball_velo_l3 - sc_avg.avg_avg_fastball_velo_szn, 0) AS pitcher_fastball_velo_delta_l3_vs_szn,
                COALESCE(sc_avg.std_whiff_pct_l3, 0) AS pitcher_std_whiff_pct_l3,
                COALESCE(sc_avg.avg_fastball_pct_l5, 0) AS pitcher_fastball_pct_l5,
                COALESCE(sc_avg.avg_breaking_pct_l5, 0) AS pitcher_breaking_pct_l5,
                COALESCE(sc_avg.avg_offspeed_pct_l5, 0) AS pitcher_offspeed_pct_l5,

                -- FanGraphs season-to-date (point-in-time; LATERAL strict as_of_date < game_date)
                COALESCE(fg.fip, 0) AS pitcher_fip_szn,
                COALESCE(fg.k_pct, 0) AS pitcher_k_pct_szn,

                -- Park factor
                COALESCE(pf.so_factor, 1.0) AS park_so_factor,

                -- Weather features
                COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
                COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,

                -- Own-team bullpen workload
                COALESCE(bull.bullpen_ip_last_3d, 0) AS team_bullpen_ip_last_3d,
                COALESCE(bull.bullpen_pitches_last_3d, 0) AS team_bullpen_pitches_last_3d,

                -- Team starter leash context (previous 10/30 starts)
                COALESCE(team_leash.avg_ip_l10, 0) AS team_starter_avg_ip_l10,
                COALESCE(team_leash.short_start_rate_l10, 0) AS team_starter_short_start_rate_l10,
                COALESCE(team_leash.avg_pitches_thrown_l10, 0) AS team_starter_avg_pitches_l10,
                COALESCE(team_leash.avg_ip_l30, 0) AS team_starter_avg_ip_l30,
                COALESCE(team_leash.short_hook_rate_l30, 0) AS team_starter_short_hook_rate_l30,
                COALESCE(team_leash.manager_short_hook_rate_l30, 0) AS manager_starter_short_hook_rate_l30,
                COALESCE(team_leash.deep_start_rate_l30, 0) AS team_starter_deep_start_rate_l30,

                -- Game lines (total)
                COALESCE(lines.game_total, 0) AS line_total,

                -- Prop line
                COALESCE(props.prop_line, 0) AS prop_line_pitcher_strikeouts,

                -- Inning-level fatigue features (L5 starts)
                COALESCE(inn_agg.velo_drop_late, 0) AS pitcher_velo_drop_late_l5,
                COALESCE(inn_agg.avg_whiff_rate_late, 0) AS pitcher_avg_whiff_rate_late_l5,
                COALESCE(inn_agg.avg_k_rate_early, 0) AS pitcher_avg_k_rate_early_l5,
                COALESCE(inn_agg.avg_pitches_per_inning, 15) AS pitcher_avg_pitches_per_inning_l5,
                COALESCE(inn_agg.avg_csw_rate, 0) AS pitcher_avg_csw_rate_l5_inning,
                COALESCE(inn_agg.deep_inning_pct, 0.5) AS pitcher_deep_inning_pct_l5,
                COALESCE(inn_agg.avg_k_first_5ip, 0) AS pitcher_avg_k_first_5ip_l5

            FROM mlb_player_game_stats_pitching pgs

            JOIN mlb_game_schedule gs ON pgs.game_id = gs.game_id
            LEFT JOIN mlb_players p ON p.player_id = pgs.player_id

            LEFT JOIN LATERAL (
                SELECT avg_so_l3, avg_so_l5, avg_so_szn,
                       avg_k_per_9_l5,
                       avg_ip_l3, avg_ip_l5, avg_ip_szn,
                       avg_pitches_thrown_l3, avg_pitches_thrown_l5,
                       min_ip_l5,
                       avg_bb_l5, avg_h_allowed_l5,
                       std_so_l3,
                       days_rest, pitch_count_last_start, starts_szn
                FROM mlb_player_average_pitching pa
                WHERE pa.player_id = pgs.player_id
                  AND pa.game_date <= pgs.game_date
                ORDER BY pa.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Additional pitcher IP-context features from raw recent starts
            LEFT JOIN LATERAL (
                WITH recent_pitcher_starts AS (
                    SELECT
                        ip,
                        outs_recorded,
                        h_allowed,
                        bb,
                        pitches_thrown,
                        game_date,
                        ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                    FROM mlb_player_game_stats_pitching
                    WHERE player_id = pgs.player_id
                      AND season = pgs.season
                      AND is_starter = TRUE
                      AND did_not_play = FALSE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 10
                ),
                previous_start AS (
                    SELECT ip, game_date
                    FROM recent_pitcher_starts
                    WHERE rn = 1
                ),
                prior_to_previous_start AS (
                    SELECT AVG(prev.ip) AS avg_ip_before_previous_start
                    FROM mlb_player_game_stats_pitching prev
                    JOIN previous_start ps ON TRUE
                    WHERE prev.player_id = pgs.player_id
                      AND prev.season = pgs.season
                      AND prev.is_starter = TRUE
                      AND prev.did_not_play = FALSE
                      AND prev.game_date < ps.game_date
                )
                SELECT
                    MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS min_ip_l5,
                    MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS max_ip_l5,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rps.ip) FILTER (WHERE rps.rn <= 5) AS median_ip_l5,
                    MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) - MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS ip_range_l5,
                    SUM(CASE WHEN rps.rn <= 5 AND rps.ip < 4.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END), 0) AS short_start_rate_l5,
                    SUM(CASE WHEN rps.ip < 5.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS pct_starts_under_5_ip_l10,
                    CASE
                        WHEN MAX(ps.ip) IS NOT NULL
                             AND MAX(prior.avg_ip_before_previous_start) IS NOT NULL
                             AND MAX(ps.ip) <= MAX(prior.avg_ip_before_previous_start) - 1.5
                        THEN 1 ELSE 0
                    END AS left_last_start_early_flag,
                    SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END)::float / 5.0 AS start_stability_l5,
                    AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.outs_recorded, 0) + COALESCE(rps.h_allowed, 0) + COALESCE(rps.bb, 0) END)::float AS avg_batters_faced_l5,
                    AVG(COALESCE(rps.outs_recorded, 0) + COALESCE(rps.h_allowed, 0) + COALESCE(rps.bb, 0))::float AS avg_batters_faced_szn,
                    AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END) AS avg_pitches_thrown_l5,
                    COALESCE(MAX(CASE WHEN rps.rn = 1 THEN COALESCE(rps.pitches_thrown, 0) END), 0)
                        / NULLIF(AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END), 0) AS workload_spike_ratio,
                    AVG(CASE WHEN rps.rn <= 3 THEN COALESCE(rps.pitches_thrown, 0) END)
                        / NULLIF(AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END), 0) AS recent_pitch_count_trend,
                    (LEAST(COALESCE(p_avg.days_rest, 5), 14) / 5.0)
                        * (COALESCE(p_avg.pitch_count_last_start, 0) / 100.0) AS rest_after_high_pitch_count
                FROM recent_pitcher_starts rps
                LEFT JOIN previous_start ps ON TRUE
                LEFT JOIN prior_to_previous_start prior ON TRUE
            ) ip_ctx ON TRUE

            -- Team starter leash context (previous 30 team starts; L10 is a prefix subset)
            LEFT JOIN LATERAL (
                WITH recent_team_starts AS (
                    SELECT
                        ip,
                        pitches_thrown,
                        ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                    FROM mlb_player_game_stats_pitching
                    WHERE team_id = pgs.team_id
                      AND season = pgs.season
                      AND is_starter = TRUE
                      AND did_not_play = FALSE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 30
                )
                SELECT
                    AVG(CASE WHEN rn <= 10 THEN ip END) AS avg_ip_l10,
                    SUM(CASE WHEN rn <= 10 AND ip < 4.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN rn <= 10 THEN 1 ELSE 0 END), 0) AS short_start_rate_l10,
                    AVG(CASE WHEN rn <= 10 THEN COALESCE(pitches_thrown, 0) END) AS avg_pitches_thrown_l10,
                    AVG(ip) AS avg_ip_l30,
                    SUM(CASE WHEN ip < 4.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS short_hook_rate_l30,
                    SUM(CASE WHEN ip < 5.0 AND COALESCE(pitches_thrown, 0) < 80 THEN 1 ELSE 0 END)::float
                        / NULLIF(COUNT(*), 0) AS manager_short_hook_rate_l30,
                    SUM(CASE WHEN ip >= 6.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS deep_start_rate_l30
                FROM recent_team_starts
            ) team_leash ON TRUE

            -- Statcast pitching averages
            LEFT JOIN LATERAL (
                SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                       avg_chase_pct_l5, avg_zone_pct_l5,
                       avg_avg_fastball_velo_l3, avg_avg_fastball_velo_l5, avg_avg_fastball_velo_szn,
                       std_whiff_pct_l3,
                       avg_fastball_pct_l5, avg_breaking_pct_l5, avg_offspeed_pct_l5
                FROM mlb_player_average_statcast_pitching sc
                WHERE sc.player_id = pgs.player_id
                  AND sc.game_date <= pgs.game_date
                ORDER BY sc.game_date DESC LIMIT 1
            ) sc_avg ON TRUE

            -- FanGraphs season-to-date snapshot (LATERAL, strict as_of_date < game_date)
            LEFT JOIN LATERAL (
                SELECT fip, k_pct
                FROM mlb_player_season_advanced_history h
                WHERE h.player_id = pgs.player_id
                  AND h.player_type = 'pitcher'
                  AND h.season = pgs.season
                  AND h.as_of_date < pgs.game_date
                ORDER BY h.as_of_date DESC
                LIMIT 1
            ) fg ON TRUE

            -- Park factors (join on venue)
            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            -- Game weather
            LEFT JOIN mlb_game_weather gw ON gw.game_pk = pgs.game_id

            -- Own-team bullpen workload
            LEFT JOIN mlb_bullpen_daily_status bull
                ON bull.team_id = pgs.team_id
               AND bull.game_date = pgs.game_date

            -- Game total line
            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Pitcher strikeout prop line (latest point-in-time snapshot)
            {prop_line_lateral_join}

            -- Inning-level fatigue from L5 starts
            LEFT JOIN LATERAL (
                SELECT
                    AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END)
                        - COALESCE(AVG(CASE WHEN sub.inning >= 5 THEN sub.avg_release_speed END),
                                   AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END))
                        AS velo_drop_late,
                    CASE WHEN SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END) > 0
                         THEN SUM(CASE WHEN sub.inning >= 5 THEN sub.whiff_rate * sub.pitches_thrown END)
                              / SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END)
                         ELSE 0 END AS avg_whiff_rate_late,
                    CASE WHEN SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END) > 0
                         THEN SUM(CASE WHEN sub.inning <= 3 THEN sub.strikeouts END)::float
                              / SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END)
                         ELSE 0 END AS avg_k_rate_early,
                    CASE WHEN COUNT(*) > 0
                         THEN AVG(sub.pitches_thrown)
                         ELSE 15 END AS avg_pitches_per_inning,
                    CASE WHEN SUM(sub.pitches_thrown) > 0
                         THEN SUM(sub.csw_rate * sub.pitches_thrown) / SUM(sub.pitches_thrown)
                         ELSE 0 END AS avg_csw_rate,
                    COUNT(DISTINCT CASE WHEN sub.inning >= 6 THEN sub.game_id END)::float
                        / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct,
                    COALESCE(
                        SUM(CASE WHEN sub.inning <= 5 THEN sub.strikeouts ELSE 0 END)::FLOAT /
                        NULLIF(SUM(CASE WHEN sub.inning <= 5 THEN sub.batters_faced ELSE 0 END), 0),
                        0
                    ) AS avg_k_first_5ip
                FROM mlb_pitcher_inning_stats sub
                INNER JOIN (
                    SELECT DISTINCT game_id, game_date
                    FROM mlb_pitcher_inning_stats
                    WHERE player_id = pgs.player_id
                      AND is_starter = TRUE
                      AND game_date < pgs.game_date
                    ORDER BY game_date DESC
                    LIMIT 5
                ) recent ON sub.game_id = recent.game_id
                WHERE sub.player_id = pgs.player_id
            ) inn_agg ON TRUE

            WHERE pgs.game_date = :game_date
              AND pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND gs.game_type = 'R'
        """.replace(
            "{prop_line_lateral_join}",
            build_lateral_prop_line_join(row_alias="pgs", market_key_sql="'pitcher_strikeouts'"),
        ))

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date, "as_of_time": as_of_time})

        if df.empty:
            return df

        # Add derived features
        df = self._add_derived_features(df)

        # Add matchup features
        df = self.enrich_with_matchup_features(df)

        # Add interaction features
        df = self._add_interaction_features(df)

        return df

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Private helpers (single-player inference queries)
    # ------------------------------------------------------------------
    # Private helpers (single-player inference queries)
    # ------------------------------------------------------------------

    def _get_pitcher_rolling_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch pitcher rolling averages for inference."""
        query = text("""
            SELECT avg_so_l3, avg_so_l5, avg_so_szn,
                   avg_k_per_9_l5,
                   avg_ip_l3, avg_ip_l5, avg_ip_szn,
                   min_ip_l5,
                   avg_pitches_thrown_l3, avg_pitches_thrown_l5,
                   avg_bb_l5, avg_h_allowed_l5,
                   starts_szn, starts_l5,
                   std_so_l3,
                   days_rest, pitch_count_last_start
            FROM mlb_player_average_pitching
            WHERE player_id = :player_id
              AND game_date <= :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        if row is None:
            return {
                "pitcher_min_ip_l5": 0,
                "pitcher_avg_pitches_thrown_l3": 0,
                "pitcher_avg_pitches_thrown_l5": 0,
                "pitcher_avg_pitches_per_start_l5": 0,
                "pitcher_avg_batters_faced_l5": 0,
                "pitcher_avg_batters_faced_szn": 0,
                "pitcher_workload_spike_ratio": 1.0,
                "pitcher_recent_pitch_count_trend": 1.0,
                "pitcher_start_stability_l5": 0,
                "pitcher_avg_bb_l5": 0,
                "pitcher_avg_h_allowed_l5": 0,
                "pitcher_std_so_l3": 0,
                "pitcher_days_rest": 5,
                "pitcher_pitch_count_last_start": 0,
                "pitcher_starts_szn": 0,
            }

        avg_ip_l5 = float(row.avg_ip_l5 or 0)
        avg_ip_szn = float(row.avg_ip_szn or 0)
        avg_h_allowed_l5 = float(row.avg_h_allowed_l5 or 0)
        avg_bb_l5 = float(row.avg_bb_l5 or 0)
        avg_bb_pitches = float(row.avg_pitches_thrown_l5 or 0)

        return {
            "pitcher_avg_so_l3": float(row.avg_so_l3 or 0),
            "pitcher_avg_so_l5": float(row.avg_so_l5 or 0),
            "pitcher_avg_so_szn": float(row.avg_so_szn or 0),
            "pitcher_avg_k_per_9_l5": float(row.avg_k_per_9_l5 or 0),
            "pitcher_avg_ip_l3": float(row.avg_ip_l3 or 0),
            "pitcher_avg_ip_l5": avg_ip_l5,
            "pitcher_avg_ip_szn": avg_ip_szn,
            "pitcher_min_ip_l5": float(row.min_ip_l5 or 0),
            "pitcher_avg_pitches_thrown_l3": float(row.avg_pitches_thrown_l3 or 0),
            "pitcher_avg_pitches_thrown_l5": avg_bb_pitches,
            "pitcher_avg_pitches_per_start_l5": avg_bb_pitches,
            "pitcher_avg_batters_faced_l5": 3 * avg_ip_l5 + avg_h_allowed_l5 + avg_bb_l5,
            "pitcher_avg_batters_faced_szn": 3 * avg_ip_szn + avg_h_allowed_l5 + avg_bb_l5,
            "pitcher_workload_spike_ratio": (
                float(row.pitch_count_last_start or 0) / avg_bb_pitches
                if avg_bb_pitches > 0 else 1.0
            ),
            "pitcher_recent_pitch_count_trend": (
                float(row.avg_pitches_thrown_l3 or 0) / avg_bb_pitches
                if avg_bb_pitches > 0 else 1.0
            ),
            "pitcher_start_stability_l5": float((row.starts_l5 or 0) / 5.0),
            "pitcher_avg_bb_l5": avg_bb_l5,
            "pitcher_avg_h_allowed_l5": avg_h_allowed_l5,
            "pitcher_std_so_l3": float(row.std_so_l3 or 0),
            "pitcher_days_rest": min(int(row.days_rest or 5), 14),
            "pitcher_pitch_count_last_start": int(row.pitch_count_last_start or 0),
            "pitcher_starts_szn": int(row.starts_szn or 0),
            "rest_after_high_pitch_count": (
                min(int(row.days_rest or 5), 14) / 5.0
                * (float(row.pitch_count_last_start or 0) / 100.0)
            ),
        }

    def _get_pitcher_ip_context_features(
        self,
        player_id: int,
        game_date: str,
        season: int,
        days_rest: int,
        pitch_count_last_start: int,
    ) -> dict:
        """Fetch pitcher IP-context features from prior starts for inference."""
        query = text("""
            WITH recent_pitcher_starts AS (
                SELECT
                    ip,
                    outs_recorded,
                    h_allowed,
                    bb,
                    pitches_thrown,
                    game_date,
                    ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                FROM mlb_player_game_stats_pitching
                WHERE player_id = :player_id
                  AND season = :season
                  AND is_starter = TRUE
                  AND did_not_play = FALSE
                  AND game_date < :game_date
                ORDER BY game_date DESC
                LIMIT 10
            ),
            previous_start AS (
                SELECT ip, game_date
                FROM recent_pitcher_starts
                WHERE rn = 1
            ),
            prior_to_previous_start AS (
                SELECT AVG(prev.ip) AS avg_ip_before_previous_start
                FROM mlb_player_game_stats_pitching prev
                JOIN previous_start ps ON TRUE
                WHERE prev.player_id = :player_id
                  AND prev.season = :season
                  AND prev.is_starter = TRUE
                  AND prev.did_not_play = FALSE
                  AND prev.game_date < ps.game_date
            )
            SELECT
                MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS min_ip_l5,
                MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS max_ip_l5,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rps.ip) FILTER (WHERE rps.rn <= 5) AS median_ip_l5,
                MAX(CASE WHEN rps.rn <= 5 THEN rps.ip END) - MIN(CASE WHEN rps.rn <= 5 THEN rps.ip END) AS ip_range_l5,
                SUM(CASE WHEN rps.rn <= 5 AND rps.ip < 4.0 THEN 1 ELSE 0 END)::float
                    / NULLIF(SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END), 0) AS short_start_rate_l5,
                SUM(CASE WHEN rps.ip < 5.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS pct_starts_under_5_ip_l10,
                CASE
                    WHEN MAX(ps.ip) IS NOT NULL
                         AND MAX(prior.avg_ip_before_previous_start) IS NOT NULL
                         AND MAX(ps.ip) <= MAX(prior.avg_ip_before_previous_start) - 1.5
                    THEN 1 ELSE 0
                END AS left_last_start_early_flag,
                SUM(CASE WHEN rps.rn <= 5 THEN 1 ELSE 0 END)::float / 5.0 AS start_stability_l5,
                AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.outs_recorded, 0) + COALESCE(rps.h_allowed, 0) + COALESCE(rps.bb, 0) END)::float AS avg_batters_faced_l5,
                AVG(CASE WHEN rps.rn <= 5 THEN COALESCE(rps.pitches_thrown, 0) END) AS avg_pitches_thrown_l5,
                MAX(CASE WHEN rps.rn = 1 THEN COALESCE(rps.pitches_thrown, 0) END) AS latest_start_pitch_count,
                AVG(CASE WHEN rps.rn <= 3 THEN COALESCE(rps.pitches_thrown, 0) END) AS recent_avg_pitch_count
            FROM recent_pitcher_starts rps
            LEFT JOIN previous_start ps ON TRUE
            LEFT JOIN prior_to_previous_start prior ON TRUE
        """)
        with self.engine.connect() as conn:
            row = conn.execute(
                query,
                {
                    "player_id": player_id,
                    "game_date": game_date,
                    "season": season,
                },
            ).fetchone()

        if row is None:
            return {
                "pitcher_max_ip_l5": 0,
                "pitcher_median_ip_l5": 0,
                "pitcher_ip_range_l5": 0,
                "pitcher_short_start_rate_l5": 0,
                "pitcher_pct_starts_under_5_ip_l10": 0,
                "pitcher_left_last_start_early_flag": 0,
                "pitcher_start_stability_l5": 0,
                "pitcher_avg_batters_faced_l5": 0,
                "pitcher_avg_pitches_per_start_l5": 0,
                "pitcher_workload_spike_ratio": 1.0,
                "pitcher_recent_pitch_count_trend": 1.0,
                "pitcher_avg_batters_faced_szn": 0,
                "rest_after_high_pitch_count": (min(days_rest, 14) / 5.0) * (pitch_count_last_start / 100.0),
            }

        workload_spike_ratio = 1.0
        recent_pitch_count_trend = 1.0

        avg_pitches_thrown_l5 = float(row.avg_pitches_thrown_l5 or 0)
        if avg_pitches_thrown_l5:
            if row.latest_start_pitch_count is not None:
                workload_spike_ratio = float(row.latest_start_pitch_count) / avg_pitches_thrown_l5
            if row.recent_avg_pitch_count is not None:
                recent_pitch_count_trend = float(row.recent_avg_pitch_count) / avg_pitches_thrown_l5
        workload_spike_ratio = min(max(float(workload_spike_ratio or 1.0), 0.0), 3.0)
        recent_pitch_count_trend = min(max(float(recent_pitch_count_trend or 1.0), 0.0), 3.0)

        return {
            "pitcher_max_ip_l5": float(row.max_ip_l5 or 0),
            "pitcher_median_ip_l5": float(row.median_ip_l5 or 0),
            "pitcher_ip_range_l5": float(row.ip_range_l5 or 0),
            "pitcher_short_start_rate_l5": float(row.short_start_rate_l5 or 0),
            "pitcher_pct_starts_under_5_ip_l10": float(row.pct_starts_under_5_ip_l10 or 0),
            "pitcher_left_last_start_early_flag": int(row.left_last_start_early_flag or 0),
            "pitcher_start_stability_l5": float(row.start_stability_l5 or 0),
            "pitcher_avg_batters_faced_l5": float(row.avg_batters_faced_l5 or 0),
            "pitcher_avg_batters_faced_szn": float(row.avg_batters_faced_l5 or 0),
            "pitcher_avg_pitches_per_start_l5": float(row.avg_pitches_thrown_l5 or 0),
            "pitcher_workload_spike_ratio": workload_spike_ratio if workload_spike_ratio else 1.0,
            "pitcher_recent_pitch_count_trend": recent_pitch_count_trend if recent_pitch_count_trend else 1.0,
            "rest_after_high_pitch_count": (min(days_rest, 14) / 5.0) * (pitch_count_last_start / 100.0),
        }

    def _get_team_starter_leash_features(self, team_id: int, game_date: str, season: int) -> dict:
        """Fetch team starter context from previous 10/30 starts for inference."""
        query = text("""
            WITH recent_team_starts AS (
                SELECT
                    ip,
                    pitches_thrown,
                    ROW_NUMBER() OVER (ORDER BY game_date DESC) AS rn
                FROM mlb_player_game_stats_pitching
                WHERE team_id = :team_id
                  AND season = :season
                  AND is_starter = TRUE
                  AND did_not_play = FALSE
                  AND game_date < :game_date
                ORDER BY game_date DESC
                LIMIT 30
            )
            SELECT
                AVG(CASE WHEN rn <= 10 THEN ip END) AS avg_ip_l10,
                SUM(CASE WHEN rn <= 10 AND ip < 4.0 THEN 1 ELSE 0 END)::float
                    / NULLIF(SUM(CASE WHEN rn <= 10 THEN 1 ELSE 0 END), 0) AS short_start_rate_l10,
                AVG(CASE WHEN rn <= 10 THEN COALESCE(pitches_thrown, 0) END) AS avg_pitches_thrown_l10,
                AVG(ip) AS avg_ip_l30,
                SUM(CASE WHEN ip < 4.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS short_hook_rate_l30,
                SUM(CASE WHEN ip < 5.0 AND COALESCE(pitches_thrown, 0) < 80 THEN 1 ELSE 0 END)::float
                    / NULLIF(COUNT(*), 0) AS manager_short_hook_rate_l30,
                SUM(CASE WHEN ip >= 6.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS deep_start_rate_l30
            FROM recent_team_starts
        """)
        with self.engine.connect() as conn:
            row = conn.execute(
                query,
                {
                    "team_id": team_id,
                    "game_date": game_date,
                    "season": season,
                },
            ).fetchone()

        if row is None:
            return {
                "team_starter_avg_ip_l10": 0.0,
                "team_starter_short_start_rate_l10": 0.0,
                "team_starter_avg_pitches_l10": 0.0,
                "team_starter_avg_ip_l30": 0.0,
                "team_starter_short_hook_rate_l30": 0.0,
                "manager_starter_short_hook_rate_l30": 0.0,
                "team_starter_deep_start_rate_l30": 0.0,
            }

        return {
            "team_starter_avg_ip_l10": float(row.avg_ip_l10 or 0),
            "team_starter_short_start_rate_l10": float(row.short_start_rate_l10 or 0),
            "team_starter_avg_pitches_l10": float(row.avg_pitches_thrown_l10 or 0),
            "team_starter_avg_ip_l30": float(row.avg_ip_l30 or 0),
            "team_starter_short_hook_rate_l30": float(row.short_hook_rate_l30 or 0),
            "manager_starter_short_hook_rate_l30": float(row.manager_short_hook_rate_l30 or 0),
            "team_starter_deep_start_rate_l30": float(row.deep_start_rate_l30 or 0),
        }

    def _get_statcast_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch Statcast rolling averages for inference."""
        query = text("""
            SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                   avg_chase_pct_l5, avg_zone_pct_l5,
                   avg_avg_fastball_velo_l3, avg_avg_fastball_velo_l5, avg_avg_fastball_velo_szn,
                   std_whiff_pct_l3,
                   avg_fastball_pct_l5, avg_breaking_pct_l5, avg_offspeed_pct_l5
            FROM mlb_player_average_statcast_pitching
            WHERE player_id = :player_id
              AND game_date <= :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        if row is None:
            return {
                "pitcher_avg_whiff_pct_l5": 0,
                "pitcher_avg_csw_pct_l5": 0,
                "pitcher_avg_chase_pct_l5": 0,
                "pitcher_avg_zone_pct_l5": 0,
                "pitcher_avg_fastball_velo_l5": 0,
                "pitcher_fastball_velo_delta_l3_vs_szn": 0,
                "pitcher_std_whiff_pct_l3": 0,
                "pitcher_fastball_pct_l5": 0,
                "pitcher_breaking_pct_l5": 0,
                "pitcher_offspeed_pct_l5": 0,
            }

        return {
            "pitcher_avg_whiff_pct_l5": float(row.avg_whiff_pct_l5 or 0),
            "pitcher_avg_csw_pct_l5": float(row.avg_csw_pct_l5 or 0),
            "pitcher_avg_chase_pct_l5": float(row.avg_chase_pct_l5 or 0),
            "pitcher_avg_zone_pct_l5": float(row.avg_zone_pct_l5 or 0),
            "pitcher_avg_fastball_velo_l5": float(row.avg_avg_fastball_velo_l5 or 0),
            "pitcher_fastball_velo_delta_l3_vs_szn": float(
                (row.avg_avg_fastball_velo_l3 or 0) - (row.avg_avg_fastball_velo_szn or 0)
            ),
            "pitcher_std_whiff_pct_l3": float(row.std_whiff_pct_l3 or 0),
            "pitcher_fastball_pct_l5": float(row.avg_fastball_pct_l5 or 0),
            "pitcher_breaking_pct_l5": float(row.avg_breaking_pct_l5 or 0),
            "pitcher_offspeed_pct_l5": float(row.avg_offspeed_pct_l5 or 0),
        }

    def _get_fangraphs_stats(self, player_id: int, season: int, game_date) -> dict:
        """Fetch most-recent FG season-to-date snapshot strictly before game_date.

        Joins mlb_player_season_advanced_history with as_of_date < game_date.
        Returns zeros if no snapshot exists yet for the player/season.
        """
        query = text("""
            SELECT fip, k_pct
            FROM mlb_player_season_advanced_history
            WHERE player_id = :player_id
              AND season = :season
              AND player_type = 'pitcher'
              AND as_of_date < :game_date
            ORDER BY as_of_date DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(
                query,
                {"player_id": player_id, "season": season, "game_date": game_date},
            ).fetchone()

        if row is None:
            return {"pitcher_fip_szn": 0, "pitcher_k_pct_szn": 0}

        return {
            "pitcher_fip_szn": float(row.fip or 0),
            "pitcher_k_pct_szn": float(row.k_pct or 0),
        }

    def _get_park_factor(self, venue_id: int, season: int) -> float:
        """Fetch park K factor for venue."""
        query = text("""
            SELECT so_factor
            FROM mlb_park_factors
            WHERE venue_id = :venue_id AND season = :season
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"venue_id": venue_id, "season": season}).fetchone()
        return float(row.so_factor) if row and row.so_factor else 1.0

    def _get_game_weather(self, game_id: int) -> dict:
        """Fetch weather features for a game. Returns neutral defaults if not yet loaded."""
        query = text("""
            SELECT air_density_idx, wind_out_mph
            FROM mlb_game_weather
            WHERE game_pk = :game_id
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"game_id": game_id}).fetchone()
        if row is None:
            return {"air_density_idx": 1.0, "wind_out_mph": 0.0}
        return {
            "air_density_idx": float(row.air_density_idx or 1.0),
            "wind_out_mph": float(row.wind_out_mph or 0.0),
        }

    def _get_team_bullpen_stats(self, team_id: int, game_date: str) -> dict[str, float]:
        """Fetch own-team bullpen workload for pitcher inference."""
        query = text("""
            SELECT bullpen_ip_last_3d, bullpen_pitches_last_3d
            FROM mlb_bullpen_daily_status
            WHERE team_id = :team_id
              AND game_date = :game_date
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"team_id": team_id, "game_date": game_date}).fetchone()

        if row is None:
            return {"team_bullpen_ip_last_3d": 0.0, "team_bullpen_pitches_last_3d": 0.0}

        return {
            "team_bullpen_ip_last_3d": float(row.bullpen_ip_last_3d or 0.0),
            "team_bullpen_pitches_last_3d": float(row.bullpen_pitches_last_3d or 0.0),
        }

    def _get_game_total(self, game_id: int) -> float:
        """Fetch game total line."""
        query = text("""
            SELECT MAX(CASE WHEN market_key = 'totals' THEN line END) AS game_total
            FROM mlb_raw_game_lines
            WHERE mlb_game_id = :game_id
              AND bookmaker IN ('pinnacle', 'draftkings')
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"game_id": game_id}).fetchone()
        return float(row.game_total) if row and row.game_total else 0

    def _get_prop_line(self, player_id: int, game_id: int, as_of_time: datetime | None = None) -> float:
        """Fetch pitcher strikeout prop line at or before ``as_of_time``."""
        return fetch_single_prop_line(
            self.engine,
            player_id=player_id,
            game_id=game_id,
            market_key="pitcher_strikeouts",
            as_of_time=as_of_time,
        )

    def _get_inning_fatigue_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch inning-level fatigue features from L5 starts."""
        query = text("""
            SELECT
                AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END)
                    - COALESCE(AVG(CASE WHEN sub.inning >= 5 THEN sub.avg_release_speed END),
                               AVG(CASE WHEN sub.inning <= 3 THEN sub.avg_release_speed END))
                    AS velo_drop_late,
                CASE WHEN SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END) > 0
                     THEN SUM(CASE WHEN sub.inning >= 5 THEN sub.whiff_rate * sub.pitches_thrown END)
                          / SUM(CASE WHEN sub.inning >= 5 THEN sub.pitches_thrown END)
                     ELSE 0 END AS avg_whiff_rate_late,
                CASE WHEN SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END) > 0
                     THEN SUM(CASE WHEN sub.inning <= 3 THEN sub.strikeouts END)::float
                          / SUM(CASE WHEN sub.inning <= 3 THEN sub.batters_faced END)
                     ELSE 0 END AS avg_k_rate_early,
                CASE WHEN COUNT(*) > 0
                     THEN AVG(sub.pitches_thrown)
                     ELSE 15 END AS avg_pitches_per_inning,
                CASE WHEN SUM(sub.pitches_thrown) > 0
                     THEN SUM(sub.csw_rate * sub.pitches_thrown) / SUM(sub.pitches_thrown)
                     ELSE 0 END AS avg_csw_rate,
                COUNT(DISTINCT CASE WHEN sub.inning >= 6 THEN sub.game_id END)::float
                    / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct,
                COALESCE(
                    SUM(CASE WHEN sub.inning <= 5 THEN sub.strikeouts ELSE 0 END)::FLOAT /
                    NULLIF(SUM(CASE WHEN sub.inning <= 5 THEN sub.batters_faced ELSE 0 END), 0),
                    0
                ) AS avg_k_first_5ip
            FROM mlb_pitcher_inning_stats sub
            INNER JOIN (
                SELECT DISTINCT game_id, game_date
                FROM mlb_pitcher_inning_stats
                WHERE player_id = :player_id
                  AND is_starter = TRUE
                  AND game_date < :game_date
                ORDER BY game_date DESC
                LIMIT 5
            ) recent ON sub.game_id = recent.game_id
            WHERE sub.player_id = :player_id
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        if row is None:
            return {
                "pitcher_velo_drop_late_l5": 0,
                "pitcher_avg_whiff_rate_late_l5": 0,
                "pitcher_avg_k_rate_early_l5": 0,
                "pitcher_avg_pitches_per_inning_l5": 15,
                "pitcher_avg_csw_rate_l5_inning": 0,
                "pitcher_deep_inning_pct_l5": 0.5,
                "pitcher_avg_k_first_5ip_l5": 0,
            }

        return {
            "pitcher_velo_drop_late_l5": float(row.velo_drop_late or 0),
            "pitcher_avg_whiff_rate_late_l5": float(row.avg_whiff_rate_late or 0),
            "pitcher_avg_k_rate_early_l5": float(row.avg_k_rate_early or 0),
            "pitcher_avg_pitches_per_inning_l5": float(row.avg_pitches_per_inning if row.avg_pitches_per_inning is not None else 15),
            "pitcher_avg_csw_rate_l5_inning": float(row.avg_csw_rate or 0),
            "pitcher_deep_inning_pct_l5": float(row.deep_inning_pct if row.deep_inning_pct is not None else 0.5),
            "pitcher_avg_k_first_5ip_l5": float(row.avg_k_first_5ip or 0),
        }

    def _get_umpire_features(self, game_id: int, game_date: str) -> dict:
        """Get umpire-based features for a single game.

        Computes rolling average total Ks in the home plate umpire's last 20 games.
        Falls back to 8.5 (league average K/game) when no umpire data exists.
        """
        if not self._table_exists("mlb_game_umpires"):
            logger.warning("mlb_game_umpires table not found; using default umpire K tendency")
            return {"umpire_avg_k_per_game_l20": 8.5}

        query = text("""
            WITH hp_umpire AS (
                SELECT umpire_id
                FROM mlb_game_umpires
                WHERE game_id = :game_id AND position = 'Home Plate'
                LIMIT 1
            ),
            recent_games AS (
                SELECT gu.game_id, gu.game_date
                FROM mlb_game_umpires gu
                JOIN hp_umpire hp ON hp.umpire_id = gu.umpire_id
                WHERE gu.position = 'Home Plate'
                    AND gu.game_date < :game_date
                ORDER BY gu.game_date DESC
                LIMIT 20
            )
            SELECT COALESCE(AVG(total_k), 8.5) AS umpire_avg_k_per_game_l20
            FROM (
                SELECT rg.game_id, SUM(pgs.so) AS total_k
                FROM recent_games rg
                JOIN mlb_player_game_stats_pitching pgs ON pgs.game_id = rg.game_id
                GROUP BY rg.game_id
            ) game_ks
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"game_id": game_id, "game_date": str(game_date)}).fetchone()

        return {"umpire_avg_k_per_game_l20": float(result.umpire_avg_k_per_game_l20) if result else 8.5}

    def _compute_umpire_features_bulk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add umpire_avg_k_per_game_l20 to a DataFrame with game_id and game_date columns.

        For each game, finds the home-plate umpire and computes the rolling avg
        total Ks in that umpire's last 20 games. Falls back to 8.5 when no data.
        """
        if df.empty:
            df["umpire_avg_k_per_game_l20"] = 8.5
            return df

        if not self._table_exists("mlb_game_umpires"):
            logger.warning("mlb_game_umpires table not found; using default umpire K tendency for %d rows", len(df))
            df["umpire_avg_k_per_game_l20"] = 8.5
            return df

        game_ids = df["game_id"].unique().tolist()
        if not game_ids:
            df["umpire_avg_k_per_game_l20"] = 8.5
            return df

        query = text("""
            WITH target_umpires AS (
                SELECT gu.game_id, gu.umpire_id, gu.game_date
                FROM mlb_game_umpires gu
                WHERE gu.game_id = ANY(:game_ids)
                  AND gu.position = 'Home Plate'
            ),
            umpire_history AS (
                SELECT
                    tu.game_id AS target_game_id,
                    hist.game_id AS hist_game_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY tu.game_id
                        ORDER BY hist.game_date DESC
                    ) AS rn
                FROM target_umpires tu
                JOIN mlb_game_umpires hist
                    ON hist.umpire_id = tu.umpire_id
                   AND hist.position = 'Home Plate'
                   AND hist.game_date < tu.game_date
            ),
            recent_20 AS (
                SELECT target_game_id, hist_game_id
                FROM umpire_history
                WHERE rn <= 20
            ),
            game_ks AS (
                SELECT r.target_game_id, r.hist_game_id, SUM(pgs.so) AS total_k
                FROM recent_20 r
                JOIN mlb_player_game_stats_pitching pgs ON pgs.game_id = r.hist_game_id
                GROUP BY r.target_game_id, r.hist_game_id
            )
            SELECT target_game_id AS game_id,
                   AVG(total_k) AS umpire_avg_k_per_game_l20
            FROM game_ks
            GROUP BY target_game_id
        """)

        with self.engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '120000'"))
            ump_df = pd.read_sql(query, conn, params={"game_ids": game_ids})

        if ump_df.empty:
            df["umpire_avg_k_per_game_l20"] = 8.5
            return df

        df = df.merge(ump_df, on="game_id", how="left")
        df["umpire_avg_k_per_game_l20"] = df["umpire_avg_k_per_game_l20"].fillna(8.5)
        return df
