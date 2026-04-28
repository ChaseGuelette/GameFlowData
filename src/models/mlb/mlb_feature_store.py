"""MLB Feature Store — Central feature engineering for pitcher strikeout model.

Mirrors the pattern from src/models/feature_store.py (NBA) but adapted for MLB:
- No minutes decomposition (MLB stats predicted directly)
- Pitcher rolling averages from mlb_player_average_pitching
- Statcast features from mlb_player_average_statcast_pitching
- FanGraphs season stats from mlb_player_season_advanced
- Park factors from mlb_park_factors
- Opposing team batting from mlb_player_game_stats_batting (aggregated)
- Prop/game lines from mlb_raw_player_props and mlb_raw_game_lines
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature lists (centralized, locked for model compatibility)
# ---------------------------------------------------------------------------

PITCHER_K_FEATURES: list[str] = [
    # Pitcher rolling averages (from mlb_player_average_pitching)
    "pitcher_avg_so_l3",
    "pitcher_avg_so_l5",
    "pitcher_avg_so_szn",
    "pitcher_avg_k_per_9_l5",
    "pitcher_avg_ip_l3",
    "pitcher_avg_ip_l5",
    "pitcher_avg_ip_szn",
    "pitcher_avg_pitches_thrown_l3",
    "pitcher_avg_bb_l5",
    "pitcher_std_so_l3",
    # Pitcher Statcast (from mlb_player_average_statcast_pitching)
    "pitcher_avg_whiff_pct_l5",
    "pitcher_avg_csw_pct_l5",
    "pitcher_avg_chase_pct_l5",
    "pitcher_avg_zone_pct_l5",
    "pitcher_avg_fastball_velo_l5",
    "pitcher_std_whiff_pct_l3",
    # FanGraphs season-level (from mlb_player_season_advanced)
    "pitcher_fip_szn",
    "pitcher_k_pct_szn",
    # Context
    "pitcher_days_rest",
    "pitcher_pitch_count_last_start",
    "pitcher_starts_szn",
    # Opposing team batting context
    "opp_team_avg_so_l10",
    "opp_team_avg_batting_avg_l10",
    "opp_team_k_pct_l10",
    "opp_team_whiff_pct_l10",
    # Game context
    "park_so_factor",
    "is_home",
    "line_total",
    # Weather (mlb_game_weather)
    "air_density_idx",
    "wind_out_mph",
    # Betting signal
    "prop_line_pitcher_strikeouts",
    # Derived
    "pitcher_est_bf_l5",
    # Trend
    "pitcher_so_l3_l5_ratio",
    # Inning-level fatigue (from mlb_pitcher_inning_stats)
    "pitcher_velo_drop_late_l5",
    "pitcher_avg_whiff_rate_late_l5",
    "pitcher_avg_k_rate_early_l5",
    "pitcher_avg_pitches_per_inning_l5",
    "pitcher_avg_csw_rate_l5_inning",
    "pitcher_deep_inning_pct_l5",
]


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
                COALESCE(p_avg.avg_pitches_thrown_l3, 0) AS pitcher_avg_pitches_thrown_l3,
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
                COALESCE(sc_avg.std_whiff_pct_l3, 0) AS pitcher_std_whiff_pct_l3,

                -- FanGraphs season-level
                COALESCE(fg.fip, 0) AS pitcher_fip_szn,
                COALESCE(fg.k_pct, 0) AS pitcher_k_pct_szn,

                -- Park factor
                COALESCE(pf.so_factor, 1.0) AS park_so_factor,

                -- Weather features
                COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
                COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,

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
                COALESCE(inn_agg.deep_inning_pct, 0.5) AS pitcher_deep_inning_pct_l5

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
                       avg_pitches_thrown_l3,
                       avg_bb_l5, avg_h_allowed_l5,
                       std_so_l3,
                       days_rest, pitch_count_last_start, starts_szn
                FROM mlb_player_average_pitching pa
                WHERE pa.player_id = pgs.player_id
                  AND pa.game_date <= pgs.game_date
                ORDER BY pa.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Statcast pitching averages
            LEFT JOIN LATERAL (
                SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                       avg_chase_pct_l5, avg_zone_pct_l5,
                       avg_avg_fastball_velo_l5,
                       std_whiff_pct_l3
                FROM mlb_player_average_statcast_pitching sc
                WHERE sc.player_id = pgs.player_id
                  AND sc.game_date <= pgs.game_date
                ORDER BY sc.game_date DESC LIMIT 1
            ) sc_avg ON TRUE

            -- FanGraphs season advanced (pitcher rows)
            LEFT JOIN mlb_player_season_advanced fg
                ON fg.player_id = pgs.player_id
               AND fg.season = pgs.season
               AND fg.player_type = 'pitcher'

            -- Park factors (join on venue)
            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            -- Game weather
            LEFT JOIN mlb_game_weather gw ON gw.game_pk = pgs.game_id

            -- Game total line (latest snapshot from pinnacle/draftkings)
            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Pitcher strikeout prop line (latest snapshot)
            LEFT JOIN LATERAL (
                SELECT sub.line AS prop_line
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM mlb_raw_player_props
                    WHERE player_id = pgs.player_id
                      AND game_id = pgs.game_id
                      AND market_key = 'pitcher_strikeouts'
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
                LIMIT 1
            ) props ON TRUE

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
                        / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct
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
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"season": season})

        return df

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Python-computed derived features after SQL load."""
        # Momentum ratio: L3/L5 strikeout trend
        df["pitcher_so_l3_l5_ratio"] = df.apply(
            lambda r: (r["pitcher_avg_so_l3"] / r["pitcher_avg_so_l5"]) if r["pitcher_avg_so_l5"] > 0 else 1.0,
            axis=1,
        )

        # Estimated batters faced per game (L5 avg): BF ≈ 3*IP + H + BB
        df["pitcher_est_bf_l5"] = 3 * df["pitcher_avg_ip_l5"] + df["pitcher_avg_h_allowed_l5"] + df["pitcher_avg_bb_l5"]

        return df

    def enrich_with_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge opposing team batting stats into training DataFrame.

        Uses compute_matchup_features_bulk from mlb_matchup_features for efficiency.
        """
        from src.processing.mlb.mlb_matchup_features import (
            compute_matchup_features_bulk,
        )

        seasons = df["season"].unique()
        matchup_frames = []
        for season in seasons:
            mf = compute_matchup_features_bulk(self.engine, int(season))
            matchup_frames.append(mf)

        if not matchup_frames:
            df["opp_team_avg_so_l10"] = 0
            df["opp_team_avg_batting_avg_l10"] = 0
            df["opp_team_k_pct_l10"] = 0
            df["opp_team_whiff_pct_l10"] = 0
            return df

        matchup_df = pd.concat(matchup_frames, ignore_index=True)
        df = df.merge(
            matchup_df,
            on=["player_id", "game_id"],
            how="left",
            suffixes=("", "_matchup"),
        )

        # Fill missing matchup features with defaults
        df["opp_team_avg_so_l10"] = df["opp_team_avg_so_l10"].fillna(0)
        df["opp_team_avg_batting_avg_l10"] = df["opp_team_avg_batting_avg_l10"].fillna(0)
        df["opp_team_k_pct_l10"] = df["opp_team_k_pct_l10"].fillna(0)
        df["opp_team_whiff_pct_l10"] = df["opp_team_whiff_pct_l10"].fillna(0)

        return df

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

        # 3. FanGraphs season stats
        fangraphs = self._get_fangraphs_stats(player_id, season)
        features.update(fangraphs)

        # 4. Park factor
        features["park_so_factor"] = self._get_park_factor(venue_id, season)

        # 4b. Weather features
        features.update(self._get_game_weather(game_id))

        # 5. Game context
        features["is_home"] = 1 if is_home else 0

        # 6. Game total line
        features["line_total"] = self._get_game_total(game_id)

        # 7. Prop line
        features["prop_line_pitcher_strikeouts"] = self._get_prop_line(player_id, game_id)

        # 8. Opposing team batting
        from src.processing.mlb.mlb_matchup_features import (
            get_opposing_team_batting_stats,
        )

        opp_stats = get_opposing_team_batting_stats(self.engine, opp_team_id, game_date, season)
        features["opp_team_avg_so_l10"] = opp_stats.get("opp_team_avg_so_l10") or 0
        features["opp_team_avg_batting_avg_l10"] = opp_stats.get("opp_team_avg_batting_avg_l10") or 0
        features["opp_team_k_pct_l10"] = opp_stats.get("opp_team_k_pct_l10") or 0
        features["opp_team_whiff_pct_l10"] = opp_stats.get("opp_team_whiff_pct_l10") or 0

        # 8b. Inning-level fatigue features
        inning_fatigue = self._get_inning_fatigue_stats(player_id, game_date)
        features.update(inning_fatigue)

        # 9. Derived features
        features["pitcher_so_l3_l5_ratio"] = (
            features["pitcher_avg_so_l3"] / features["pitcher_avg_so_l5"]
            if features.get("pitcher_avg_so_l5", 0) > 0
            else 1.0
        )
        features["pitcher_est_bf_l5"] = (
            3 * features.get("pitcher_avg_ip_l5", 0)
            + features.get("pitcher_avg_h_allowed_l5", 0)
            + features.get("pitcher_avg_bb_l5", 0)
        )

        return features

    # ------------------------------------------------------------------
    # Batch inference (backtesting)
    # ------------------------------------------------------------------

    def get_features_for_date(self, game_date: str) -> pd.DataFrame:
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
                COALESCE(p_avg.avg_pitches_thrown_l3, 0) AS pitcher_avg_pitches_thrown_l3,
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
                COALESCE(sc_avg.std_whiff_pct_l3, 0) AS pitcher_std_whiff_pct_l3,

                -- FanGraphs
                COALESCE(fg.fip, 0) AS pitcher_fip_szn,
                COALESCE(fg.k_pct, 0) AS pitcher_k_pct_szn,

                -- Park factor
                COALESCE(pf.so_factor, 1.0) AS park_so_factor,

                -- Weather features
                COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
                COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,

                -- Game lines
                COALESCE(lines.game_total, 0) AS line_total,

                -- Prop line
                COALESCE(props.prop_line, 0) AS prop_line_pitcher_strikeouts,

                -- Inning-level fatigue features (L5 starts)
                COALESCE(inn_agg.velo_drop_late, 0) AS pitcher_velo_drop_late_l5,
                COALESCE(inn_agg.avg_whiff_rate_late, 0) AS pitcher_avg_whiff_rate_late_l5,
                COALESCE(inn_agg.avg_k_rate_early, 0) AS pitcher_avg_k_rate_early_l5,
                COALESCE(inn_agg.avg_pitches_per_inning, 15) AS pitcher_avg_pitches_per_inning_l5,
                COALESCE(inn_agg.avg_csw_rate, 0) AS pitcher_avg_csw_rate_l5_inning,
                COALESCE(inn_agg.deep_inning_pct, 0.5) AS pitcher_deep_inning_pct_l5

            FROM mlb_player_game_stats_pitching pgs

            JOIN mlb_game_schedule gs ON pgs.game_id = gs.game_id
            LEFT JOIN mlb_players p ON p.player_id = pgs.player_id

            LEFT JOIN LATERAL (
                SELECT avg_so_l3, avg_so_l5, avg_so_szn,
                       avg_k_per_9_l5,
                       avg_ip_l3, avg_ip_l5, avg_ip_szn,
                       avg_pitches_thrown_l3,
                       avg_bb_l5, avg_h_allowed_l5,
                       std_so_l3,
                       days_rest, pitch_count_last_start, starts_szn
                FROM mlb_player_average_pitching pa
                WHERE pa.player_id = pgs.player_id
                  AND pa.game_date <= pgs.game_date
                ORDER BY pa.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            LEFT JOIN LATERAL (
                SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                       avg_chase_pct_l5, avg_zone_pct_l5,
                       avg_avg_fastball_velo_l5,
                       std_whiff_pct_l3
                FROM mlb_player_average_statcast_pitching sc
                WHERE sc.player_id = pgs.player_id
                  AND sc.game_date <= pgs.game_date
                ORDER BY sc.game_date DESC LIMIT 1
            ) sc_avg ON TRUE

            LEFT JOIN mlb_player_season_advanced fg
                ON fg.player_id = pgs.player_id
               AND fg.season = pgs.season
               AND fg.player_type = 'pitcher'

            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            -- Game weather
            LEFT JOIN mlb_game_weather gw ON gw.game_pk = pgs.game_id

            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            LEFT JOIN LATERAL (
                SELECT sub.line AS prop_line
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM mlb_raw_player_props
                    WHERE player_id = pgs.player_id
                      AND game_id = pgs.game_id
                      AND market_key = 'pitcher_strikeouts'
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
                LIMIT 1
            ) props ON TRUE

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
                        / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct
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
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            return df

        # Add derived features
        df = self._add_derived_features(df)

        # Add matchup features
        df = self.enrich_with_matchup_features(df)

        return df

    # ------------------------------------------------------------------
    # Private helpers (single-player inference queries)
    # ------------------------------------------------------------------

    def _get_pitcher_rolling_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch pitcher rolling averages for inference."""
        query = text("""
            SELECT avg_so_l3, avg_so_l5, avg_so_szn,
                   avg_k_per_9_l5,
                   avg_ip_l3, avg_ip_l5, avg_ip_szn,
                   avg_pitches_thrown_l3,
                   avg_bb_l5, avg_h_allowed_l5,
                   std_so_l3,
                   days_rest, pitch_count_last_start, starts_szn
            FROM mlb_player_average_pitching
            WHERE player_id = :player_id
              AND game_date <= :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        if row is None:
            return {f: 0 for f in PITCHER_K_FEATURES if f.startswith("pitcher_avg_") or f.startswith("pitcher_std_")}

        return {
            "pitcher_avg_so_l3": float(row.avg_so_l3 or 0),
            "pitcher_avg_so_l5": float(row.avg_so_l5 or 0),
            "pitcher_avg_so_szn": float(row.avg_so_szn or 0),
            "pitcher_avg_k_per_9_l5": float(row.avg_k_per_9_l5 or 0),
            "pitcher_avg_ip_l3": float(row.avg_ip_l3 or 0),
            "pitcher_avg_ip_l5": float(row.avg_ip_l5 or 0),
            "pitcher_avg_ip_szn": float(row.avg_ip_szn or 0),
            "pitcher_avg_pitches_thrown_l3": float(row.avg_pitches_thrown_l3 or 0),
            "pitcher_avg_bb_l5": float(row.avg_bb_l5 or 0),
            "pitcher_avg_h_allowed_l5": float(row.avg_h_allowed_l5 or 0),
            "pitcher_std_so_l3": float(row.std_so_l3 or 0),
            "pitcher_days_rest": min(int(row.days_rest or 5), 14),
            "pitcher_pitch_count_last_start": int(row.pitch_count_last_start or 0),
            "pitcher_starts_szn": int(row.starts_szn or 0),
        }

    def _get_statcast_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch Statcast rolling averages for inference."""
        query = text("""
            SELECT avg_whiff_pct_l5, avg_csw_pct_l5,
                   avg_chase_pct_l5, avg_zone_pct_l5,
                   avg_avg_fastball_velo_l5,
                   std_whiff_pct_l3
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
                "pitcher_std_whiff_pct_l3": 0,
            }

        return {
            "pitcher_avg_whiff_pct_l5": float(row.avg_whiff_pct_l5 or 0),
            "pitcher_avg_csw_pct_l5": float(row.avg_csw_pct_l5 or 0),
            "pitcher_avg_chase_pct_l5": float(row.avg_chase_pct_l5 or 0),
            "pitcher_avg_zone_pct_l5": float(row.avg_zone_pct_l5 or 0),
            "pitcher_avg_fastball_velo_l5": float(row.avg_avg_fastball_velo_l5 or 0),
            "pitcher_std_whiff_pct_l3": float(row.std_whiff_pct_l3 or 0),
        }

    def _get_fangraphs_stats(self, player_id: int, season: int) -> dict:
        """Fetch FanGraphs season-level pitcher stats."""
        query = text("""
            SELECT fip, k_pct
            FROM mlb_player_season_advanced
            WHERE player_id = :player_id
              AND season = :season
              AND player_type = 'pitcher'
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "season": season}).fetchone()

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

    def _get_prop_line(self, player_id: int, game_id: int) -> float:
        """Fetch pitcher strikeout prop line (latest snapshot)."""
        query = text("""
            SELECT line
            FROM mlb_raw_player_props
            WHERE player_id = :player_id
              AND game_id = :game_id
              AND market_key = 'pitcher_strikeouts'
              AND bookmaker IN ('pinnacle', 'draftkings')
            ORDER BY snapshot_time DESC NULLS LAST
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_id": game_id}).fetchone()
        return float(row.line) if row and row.line else 0

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
                    / GREATEST(COUNT(DISTINCT sub.game_id), 1) AS deep_inning_pct
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
            }

        return {
            "pitcher_velo_drop_late_l5": float(row.velo_drop_late or 0),
            "pitcher_avg_whiff_rate_late_l5": float(row.avg_whiff_rate_late or 0),
            "pitcher_avg_k_rate_early_l5": float(row.avg_k_rate_early or 0),
            "pitcher_avg_pitches_per_inning_l5": float(row.avg_pitches_per_inning if row.avg_pitches_per_inning is not None else 15),
            "pitcher_avg_csw_rate_l5_inning": float(row.avg_csw_rate or 0),
            "pitcher_deep_inning_pct_l5": float(row.deep_inning_pct if row.deep_inning_pct is not None else 0.5),
        }
