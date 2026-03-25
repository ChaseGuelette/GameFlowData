"""MLB Batter Feature Store — Central feature engineering for batter prop models.

Mirrors MLBFeatureStore (pitcher) but adapted for batter targets:
- Batter rolling averages from mlb_player_average_batting
- Batter Statcast features from mlb_player_average_statcast_batting
- FanGraphs season stats (player_type='batter')
- Opposing starting pitcher stats
- Platoon splits (batter vs pitcher handedness)
- Park factors (hits, HR, runs)
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

# Stat key -> prop market_key mapping
BATTER_STAT_MARKET_KEY: dict[str, str] = {
    "hits": "batter_hits",
    "home_runs": "batter_home_runs",
    "total_bases": "batter_total_bases",
    "rbis": "batter_rbis",
    "runs": "batter_runs_scored",
}

# Stat key -> target column in mlb_player_game_stats_batting
BATTER_STAT_TARGET: dict[str, str] = {
    "hits": "h",
    "home_runs": "hr",
    "total_bases": "tb",
    "rbis": "rbi",
    "runs": "r",
}

BATTER_BASE_FEATURES: list[str] = [
    # Batter rolling averages (mlb_player_average_batting)
    "batter_avg_h_l5", "batter_avg_h_l10", "batter_avg_h_l20", "batter_avg_h_szn",
    "batter_avg_hr_l5", "batter_avg_hr_l10", "batter_avg_hr_szn",
    "batter_avg_tb_l5", "batter_avg_tb_l10", "batter_avg_tb_szn",
    "batter_avg_ab_l5", "batter_avg_pa_l5",
    "batter_avg_r_l5", "batter_avg_r_l10", "batter_avg_r_szn",
    "batter_avg_rbi_l5", "batter_avg_rbi_l10", "batter_avg_rbi_szn",
    "batter_avg_bb_l5",
    "batter_avg_batting_avg_l10", "batter_avg_obp_l10",
    "batter_avg_slg_l10", "batter_avg_ops_l10",
    "batter_std_h_l5", "batter_std_hr_l5", "batter_std_tb_l5",
    "batter_std_rbi_l5", "batter_std_r_l5",
    # Context
    "batter_rest_days", "batter_games_last_7d", "batter_game_number",
    # Statcast (mlb_player_average_statcast_batting)
    "batter_avg_exit_velocity_l5", "batter_avg_exit_velocity_l10",
    "batter_avg_launch_angle_l5",
    "batter_barrel_pct_l5", "batter_barrel_pct_l10",
    "batter_hard_hit_pct_l5",
    "batter_xba_l5", "batter_xba_l10",
    "batter_xslg_l5", "batter_xwoba_l5",
    "batter_zone_pct_l5", "batter_chase_pct_l5", "batter_whiff_pct_l5",
    # FanGraphs season (mlb_player_season_advanced, player_type='batter')
    "batter_wrc_plus_szn", "batter_woba_szn", "batter_iso_szn",
    "batter_bb_pct_szn", "batter_k_pct_szn", "batter_hard_pct_szn", "batter_babip_szn",
    # Lineup
    "lineup_position",
    # Opposing starter pitcher
    "opp_pitcher_avg_era_l5", "opp_pitcher_avg_whip_l5",
    "opp_pitcher_avg_k_per_9_l5", "opp_pitcher_avg_bb_per_9_l5",
    "opp_pitcher_avg_h_allowed_l5", "opp_pitcher_avg_hr_allowed_l5",
    "opp_pitcher_xwoba_against_l5", "opp_pitcher_hard_hit_pct_against_l5",
    "opp_pitcher_avg_fastball_velo_l5", "opp_pitcher_days_rest",
    # Platoon
    "is_same_hand", "batter_avg_h_vs_hand_l20", "batter_avg_ops_vs_hand_l20",
    # Game context
    "is_home", "line_total",
    # Derived (Python post-SQL)
    "batter_h_l5_l10_ratio",
]

# Per-stat extensions
BATTER_HITS_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_hits_factor", "prop_line_batter_hits",
    "projected_ab",
]
BATTER_HR_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_hr_factor", "batter_avg_hr_vs_hand_l20",
    "batter_fb_pct_l10", "prop_line_batter_home_runs",
]
BATTER_TOTAL_BASES_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_hits_factor", "park_hr_factor", "prop_line_batter_total_bases",
]
BATTER_RBIS_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_runs_factor", "prop_line_batter_rbis",
]
BATTER_RUNS_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_runs_factor", "prop_line_batter_runs_scored",
]

BATTER_FEATURE_MAP: dict[str, list[str]] = {
    "hits": BATTER_HITS_FEATURES,
    "home_runs": BATTER_HR_FEATURES,
    "total_bases": BATTER_TOTAL_BASES_FEATURES,
    "rbis": BATTER_RBIS_FEATURES,
    "runs": BATTER_RUNS_FEATURES,
}


def get_features_for_stat(stat: str) -> list[str]:
    """Return the feature list for a given batter stat."""
    return BATTER_FEATURE_MAP[stat]


@dataclass
class MLBBatterFeatureConfig:
    """Configuration for MLB batter feature engineering."""

    min_games_for_stable: int = 3


class MLBBatterFeatureStore:
    """Feature store for MLB batter prop models.

    Provides training data loading, single-player inference features,
    and batch features for backtesting — all with time-travel safety.
    """

    def __init__(self, engine: Engine, config: MLBBatterFeatureConfig | None = None):
        self.engine = engine
        self.config = config or MLBBatterFeatureConfig()

    # ------------------------------------------------------------------
    # Training data
    # ------------------------------------------------------------------

    def get_training_dataset(self, seasons: list[int], stat: str = "hits") -> pd.DataFrame:
        """Load training features for one or more seasons.

        Returns DataFrame with batter feature columns plus identifiers
        and the target column 'actual_{stat}'.
        """
        target_col = BATTER_STAT_TARGET[stat]
        market_key = BATTER_STAT_MARKET_KEY[stat]
        frames = []
        for season in seasons:
            logger.info("Loading MLB batter training data for season %d, stat=%s...", season, stat)
            df = self._load_single_season_training(season, target_col, market_key)
            logger.info("  Season %d: %d rows", season, len(df))
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        combined = self._add_derived_features(combined)

        logger.info("Total MLB batter training rows: %d", len(combined))
        return combined

    def enrich_with_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge opposing starter stats and platoon splits into training DataFrame."""
        from src.processing.mlb.mlb_batter_matchup_features import (
            compute_opposing_starter_bulk,
            compute_platoon_splits_bulk,
        )

        seasons = df["season"].unique()

        # Opposing starter stats
        opp_frames = []
        platoon_frames = []
        for season in seasons:
            opp_frames.append(compute_opposing_starter_bulk(self.engine, int(season)))
            platoon_frames.append(compute_platoon_splits_bulk(self.engine, int(season)))

        # Merge opposing starter
        if opp_frames:
            opp_df = pd.concat(opp_frames, ignore_index=True)
            df = df.merge(opp_df, on=["player_id", "game_id"], how="left", suffixes=("", "_opp"))

        opp_cols = [c for c in BATTER_BASE_FEATURES if c.startswith("opp_pitcher_")]
        for col in opp_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0

        # Merge platoon splits
        if platoon_frames:
            plat_df = pd.concat(platoon_frames, ignore_index=True)
            df = df.merge(plat_df, on=["player_id", "game_id"], how="left", suffixes=("", "_plat"))

        for col in ("is_same_hand", "batter_avg_h_vs_hand_l20", "batter_avg_ops_vs_hand_l20"):
            if col in df.columns:
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0

        # HR-specific platoon feature
        if "batter_avg_hr_vs_hand_l20" in df.columns:
            df["batter_avg_hr_vs_hand_l20"] = df["batter_avg_hr_vs_hand_l20"].fillna(0)
        else:
            df["batter_avg_hr_vs_hand_l20"] = 0

        return df

    def _load_single_season_training(self, season: int, target_col: str, market_key: str) -> pd.DataFrame:
        """Load training features for a single MLB season via SQL joins."""
        query = text("""
            SELECT
                -- Identifiers
                bgs.game_id,
                bgs.player_id,
                bgs.game_date::date,
                bgs.season,
                bgs.team_id,

                -- Target
                bgs.{target_col} AS actual,

                -- At-bats (for binomial model)
                COALESCE(bgs.ab, 0) AS at_bats,

                -- Game context
                CASE WHEN gs.home_team_id = bgs.team_id THEN 1 ELSE 0 END AS is_home,
                CASE WHEN gs.home_team_id = bgs.team_id
                     THEN gs.away_team_id
                     ELSE gs.home_team_id END AS opp_team_id,

                -- Lineup position
                COALESCE(bgs.lineup_position, 0) AS lineup_position,

                -- Batter rolling averages
                COALESCE(ba.avg_h_l5, 0) AS batter_avg_h_l5,
                COALESCE(ba.avg_h_l10, 0) AS batter_avg_h_l10,
                COALESCE(ba.avg_h_l20, 0) AS batter_avg_h_l20,
                COALESCE(ba.avg_h_szn, 0) AS batter_avg_h_szn,
                COALESCE(ba.avg_hr_l5, 0) AS batter_avg_hr_l5,
                COALESCE(ba.avg_hr_l10, 0) AS batter_avg_hr_l10,
                COALESCE(ba.avg_hr_szn, 0) AS batter_avg_hr_szn,
                COALESCE(ba.avg_tb_l5, 0) AS batter_avg_tb_l5,
                COALESCE(ba.avg_tb_l10, 0) AS batter_avg_tb_l10,
                COALESCE(ba.avg_tb_szn, 0) AS batter_avg_tb_szn,
                COALESCE(ba.avg_ab_l5, 0) AS batter_avg_ab_l5,
                COALESCE(ba.avg_pa_l5, 0) AS batter_avg_pa_l5,
                COALESCE(ba.avg_r_l5, 0) AS batter_avg_r_l5,
                COALESCE(ba.avg_r_l10, 0) AS batter_avg_r_l10,
                COALESCE(ba.avg_r_szn, 0) AS batter_avg_r_szn,
                COALESCE(ba.avg_rbi_l5, 0) AS batter_avg_rbi_l5,
                COALESCE(ba.avg_rbi_l10, 0) AS batter_avg_rbi_l10,
                COALESCE(ba.avg_rbi_szn, 0) AS batter_avg_rbi_szn,
                COALESCE(ba.avg_bb_l5, 0) AS batter_avg_bb_l5,
                COALESCE(ba.avg_batting_avg_l10, 0) AS batter_avg_batting_avg_l10,
                COALESCE(ba.avg_obp_l10, 0) AS batter_avg_obp_l10,
                COALESCE(ba.avg_slg_l10, 0) AS batter_avg_slg_l10,
                COALESCE(ba.avg_ops_l10, 0) AS batter_avg_ops_l10,
                COALESCE(ba.std_h_l5, 0) AS batter_std_h_l5,
                COALESCE(ba.std_hr_l5, 0) AS batter_std_hr_l5,
                COALESCE(ba.std_tb_l5, 0) AS batter_std_tb_l5,
                COALESCE(ba.std_rbi_l5, 0) AS batter_std_rbi_l5,
                COALESCE(ba.std_r_l5, 0) AS batter_std_r_l5,

                -- Batter context
                COALESCE(ba.rest_days, 2) AS batter_rest_days,
                COALESCE(ba.games_last_7d, 0) AS batter_games_last_7d,
                COALESCE(ba.game_number, 0) AS batter_game_number,

                -- Statcast batting averages
                COALESCE(sc.avg_avg_exit_velocity_l5, 0) AS batter_avg_exit_velocity_l5,
                COALESCE(sc.avg_avg_exit_velocity_l10, 0) AS batter_avg_exit_velocity_l10,
                COALESCE(sc.avg_avg_launch_angle_l5, 0) AS batter_avg_launch_angle_l5,
                COALESCE(sc.avg_barrel_pct_l5, 0) AS batter_barrel_pct_l5,
                COALESCE(sc.avg_barrel_pct_l10, 0) AS batter_barrel_pct_l10,
                COALESCE(sc.avg_hard_hit_pct_l5, 0) AS batter_hard_hit_pct_l5,
                COALESCE(sc.avg_xba_l5, 0) AS batter_xba_l5,
                COALESCE(sc.avg_xba_l10, 0) AS batter_xba_l10,
                COALESCE(sc.avg_xslg_l5, 0) AS batter_xslg_l5,
                COALESCE(sc.avg_xwoba_l5, 0) AS batter_xwoba_l5,
                COALESCE(sc.avg_zone_pct_l5, 0) AS batter_zone_pct_l5,
                COALESCE(sc.avg_chase_pct_l5, 0) AS batter_chase_pct_l5,
                COALESCE(sc.avg_whiff_pct_l5, 0) AS batter_whiff_pct_l5,
                -- HR-specific statcast
                COALESCE(sc.avg_fb_pct_l10, 0) AS batter_fb_pct_l10,

                -- FanGraphs season-level (batter)
                COALESCE(fg.wrc_plus, 0) AS batter_wrc_plus_szn,
                COALESCE(fg.woba, 0) AS batter_woba_szn,
                COALESCE(fg.iso, 0) AS batter_iso_szn,
                COALESCE(fg.bb_pct, 0) AS batter_bb_pct_szn,
                COALESCE(fg.k_pct, 0) AS batter_k_pct_szn,
                COALESCE(fg.hard_pct, 0) AS batter_hard_pct_szn,
                COALESCE(fg.babip, 0) AS batter_babip_szn,

                -- Park factors
                COALESCE(pf.hits_factor, 1.0) AS park_hits_factor,
                COALESCE(pf.hr_factor, 1.0) AS park_hr_factor,
                COALESCE(pf.runs_factor, 1.0) AS park_runs_factor,

                -- Game total line
                COALESCE(lines.game_total, 0) AS line_total,

                -- Player prop line (dynamic per stat)
                COALESCE(props.prop_line, 0) AS prop_line

            FROM mlb_player_game_stats_batting bgs

            JOIN mlb_game_schedule gs ON bgs.game_id = gs.game_id

            -- Batter rolling averages
            LEFT JOIN LATERAL (
                SELECT avg_h_l5, avg_h_l10, avg_h_l20, avg_h_szn,
                       avg_hr_l5, avg_hr_l10, avg_hr_szn,
                       avg_tb_l5, avg_tb_l10, avg_tb_szn,
                       avg_ab_l5, avg_pa_l5,
                       avg_r_l5, avg_r_l10, avg_r_szn,
                       avg_rbi_l5, avg_rbi_l10, avg_rbi_szn,
                       avg_bb_l5,
                       avg_batting_avg_l10, avg_obp_l10,
                       avg_slg_l10, avg_ops_l10,
                       std_h_l5, std_hr_l5, std_tb_l5, std_rbi_l5, std_r_l5,
                       rest_days, games_last_7d, game_number
                FROM mlb_player_average_batting ba2
                WHERE ba2.player_id = bgs.player_id
                  AND ba2.game_date <= bgs.game_date
                ORDER BY ba2.game_date DESC LIMIT 1
            ) ba ON TRUE

            -- Statcast batting averages
            LEFT JOIN LATERAL (
                SELECT avg_avg_exit_velocity_l5, avg_avg_exit_velocity_l10,
                       avg_avg_launch_angle_l5,
                       avg_barrel_pct_l5, avg_barrel_pct_l10,
                       avg_hard_hit_pct_l5,
                       avg_xba_l5, avg_xba_l10,
                       avg_xslg_l5, avg_xwoba_l5,
                       avg_zone_pct_l5, avg_chase_pct_l5, avg_whiff_pct_l5,
                       avg_fb_pct_l10
                FROM mlb_player_average_statcast_batting sc2
                WHERE sc2.player_id = bgs.player_id
                  AND sc2.game_date <= bgs.game_date
                ORDER BY sc2.game_date DESC LIMIT 1
            ) sc ON TRUE

            -- FanGraphs season advanced (batter rows)
            LEFT JOIN mlb_player_season_advanced fg
                ON fg.player_id = bgs.player_id
               AND fg.season = bgs.season
               AND fg.player_type = 'batter'

            -- Park factors (join on venue)
            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            -- Game total line
            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = bgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            -- Player prop line (stat-specific)
            LEFT JOIN LATERAL (
                SELECT sub.line AS prop_line
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM mlb_raw_player_props
                    WHERE player_id = bgs.player_id
                      AND game_id = bgs.game_id
                      AND market_key = :market_key
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
                LIMIT 1
            ) props ON TRUE

            WHERE bgs.is_starter = TRUE
              AND bgs.did_not_play = FALSE
              AND gs.game_type = 'R'
              AND bgs.season = :season
        """.replace("{target_col}", target_col))

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"season": season, "market_key": market_key})

        # Rename prop_line to stat-specific name
        prop_col = f"prop_line_{market_key}"
        if "prop_line" in df.columns:
            df.rename(columns={"prop_line": prop_col}, inplace=True)

        return df

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Python-computed derived features after SQL load."""
        # Momentum ratio: L5/L10 hit trend
        df["batter_h_l5_l10_ratio"] = df.apply(
            lambda r: (r["batter_avg_h_l5"] / r["batter_avg_h_l10"])
            if r.get("batter_avg_h_l10", 0) > 0
            else 1.0,
            axis=1,
        )

        # Projected AB for binomial model (use rolling avg, avoid leakage)
        if "batter_avg_ab_l5" in df.columns:
            df["projected_ab"] = df["batter_avg_ab_l5"].clip(lower=1.0).fillna(3.5)
        else:
            df["projected_ab"] = 3.5

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
        opp_pitcher_id: int | None = None,
        lineup_pos: int | None = None,
        stat: str = "hits",
    ) -> dict:
        """Assemble features for a single batter for inference.

        Returns flat dict with all features for the given stat.
        """
        features: dict = {}

        # 1. Batter rolling averages
        features.update(self._get_batter_rolling_stats(player_id, game_date))

        # 2. Statcast averages
        features.update(self._get_batter_statcast_stats(player_id, game_date))

        # 3. FanGraphs season stats
        features.update(self._get_batter_fangraphs_stats(player_id, season))

        # 4. Park factors
        park = self._get_park_factors(venue_id, season)
        features["park_hits_factor"] = park["hits_factor"]
        features["park_hr_factor"] = park["hr_factor"]
        features["park_runs_factor"] = park["runs_factor"]

        # 5. Game context
        features["is_home"] = 1 if is_home else 0
        features["lineup_position"] = lineup_pos or 0

        # 6. Game total line
        features["line_total"] = self._get_game_total(game_id)

        # 7. Prop line
        market_key = BATTER_STAT_MARKET_KEY[stat]
        features[f"prop_line_{market_key}"] = self._get_prop_line(player_id, game_id, market_key)

        # 8. Opposing starter stats
        if opp_pitcher_id:
            from src.processing.mlb.mlb_batter_matchup_features import (
                get_opposing_starter_stats,
            )
            opp_stats = get_opposing_starter_stats(self.engine, opp_pitcher_id, game_date)
            features.update(opp_stats)

            # Platoon
            features.update(self._get_platoon_features(player_id, opp_pitcher_id, game_date))
        else:
            for col in [c for c in BATTER_BASE_FEATURES if c.startswith("opp_pitcher_")]:
                features.setdefault(col, 0)
            features.setdefault("is_same_hand", 0)
            features.setdefault("batter_avg_h_vs_hand_l20", 0)
            features.setdefault("batter_avg_ops_vs_hand_l20", 0)
            features.setdefault("batter_avg_hr_vs_hand_l20", 0)

        # 9. Derived features
        features["batter_h_l5_l10_ratio"] = (
            features["batter_avg_h_l5"] / features["batter_avg_h_l10"]
            if features.get("batter_avg_h_l10", 0) > 0
            else 1.0
        )

        # Projected AB for binomial model
        features["projected_ab"] = max(features.get("batter_avg_ab_l5", 3.5), 1.0)

        return features

    # ------------------------------------------------------------------
    # Batch inference (backtesting)
    # ------------------------------------------------------------------

    def get_features_for_date(self, game_date: str, stat: str = "hits") -> pd.DataFrame:
        """Get features for all starting batters on a given date.

        Returns DataFrame with batter feature columns.
        """
        target_col = BATTER_STAT_TARGET[stat]
        market_key = BATTER_STAT_MARKET_KEY[stat]

        query = text("""
            SELECT
                bgs.game_id,
                bgs.player_id,
                bgs.game_date::date,
                bgs.season,
                bgs.team_id,
                p.player_name,

                -- Target (for backtesting evaluation)
                bgs.{target_col} AS actual,

                -- At-bats (for binomial model)
                COALESCE(bgs.ab, 0) AS at_bats,

                -- Game context
                CASE WHEN gs.home_team_id = bgs.team_id THEN 1 ELSE 0 END AS is_home,
                CASE WHEN gs.home_team_id = bgs.team_id
                     THEN gs.away_team_id
                     ELSE gs.home_team_id END AS opp_team_id,

                COALESCE(bgs.lineup_position, 0) AS lineup_position,

                -- Batter rolling averages
                COALESCE(ba.avg_h_l5, 0) AS batter_avg_h_l5,
                COALESCE(ba.avg_h_l10, 0) AS batter_avg_h_l10,
                COALESCE(ba.avg_h_l20, 0) AS batter_avg_h_l20,
                COALESCE(ba.avg_h_szn, 0) AS batter_avg_h_szn,
                COALESCE(ba.avg_hr_l5, 0) AS batter_avg_hr_l5,
                COALESCE(ba.avg_hr_l10, 0) AS batter_avg_hr_l10,
                COALESCE(ba.avg_hr_szn, 0) AS batter_avg_hr_szn,
                COALESCE(ba.avg_tb_l5, 0) AS batter_avg_tb_l5,
                COALESCE(ba.avg_tb_l10, 0) AS batter_avg_tb_l10,
                COALESCE(ba.avg_tb_szn, 0) AS batter_avg_tb_szn,
                COALESCE(ba.avg_ab_l5, 0) AS batter_avg_ab_l5,
                COALESCE(ba.avg_pa_l5, 0) AS batter_avg_pa_l5,
                COALESCE(ba.avg_r_l5, 0) AS batter_avg_r_l5,
                COALESCE(ba.avg_r_l10, 0) AS batter_avg_r_l10,
                COALESCE(ba.avg_r_szn, 0) AS batter_avg_r_szn,
                COALESCE(ba.avg_rbi_l5, 0) AS batter_avg_rbi_l5,
                COALESCE(ba.avg_rbi_l10, 0) AS batter_avg_rbi_l10,
                COALESCE(ba.avg_rbi_szn, 0) AS batter_avg_rbi_szn,
                COALESCE(ba.avg_bb_l5, 0) AS batter_avg_bb_l5,
                COALESCE(ba.avg_batting_avg_l10, 0) AS batter_avg_batting_avg_l10,
                COALESCE(ba.avg_obp_l10, 0) AS batter_avg_obp_l10,
                COALESCE(ba.avg_slg_l10, 0) AS batter_avg_slg_l10,
                COALESCE(ba.avg_ops_l10, 0) AS batter_avg_ops_l10,
                COALESCE(ba.std_h_l5, 0) AS batter_std_h_l5,
                COALESCE(ba.std_hr_l5, 0) AS batter_std_hr_l5,
                COALESCE(ba.std_tb_l5, 0) AS batter_std_tb_l5,
                COALESCE(ba.std_rbi_l5, 0) AS batter_std_rbi_l5,
                COALESCE(ba.std_r_l5, 0) AS batter_std_r_l5,
                COALESCE(ba.rest_days, 2) AS batter_rest_days,
                COALESCE(ba.games_last_7d, 0) AS batter_games_last_7d,
                COALESCE(ba.game_number, 0) AS batter_game_number,

                -- Statcast
                COALESCE(sc.avg_avg_exit_velocity_l5, 0) AS batter_avg_exit_velocity_l5,
                COALESCE(sc.avg_avg_exit_velocity_l10, 0) AS batter_avg_exit_velocity_l10,
                COALESCE(sc.avg_avg_launch_angle_l5, 0) AS batter_avg_launch_angle_l5,
                COALESCE(sc.avg_barrel_pct_l5, 0) AS batter_barrel_pct_l5,
                COALESCE(sc.avg_barrel_pct_l10, 0) AS batter_barrel_pct_l10,
                COALESCE(sc.avg_hard_hit_pct_l5, 0) AS batter_hard_hit_pct_l5,
                COALESCE(sc.avg_xba_l5, 0) AS batter_xba_l5,
                COALESCE(sc.avg_xba_l10, 0) AS batter_xba_l10,
                COALESCE(sc.avg_xslg_l5, 0) AS batter_xslg_l5,
                COALESCE(sc.avg_xwoba_l5, 0) AS batter_xwoba_l5,
                COALESCE(sc.avg_zone_pct_l5, 0) AS batter_zone_pct_l5,
                COALESCE(sc.avg_chase_pct_l5, 0) AS batter_chase_pct_l5,
                COALESCE(sc.avg_whiff_pct_l5, 0) AS batter_whiff_pct_l5,
                COALESCE(sc.avg_fb_pct_l10, 0) AS batter_fb_pct_l10,

                -- FanGraphs
                COALESCE(fg.wrc_plus, 0) AS batter_wrc_plus_szn,
                COALESCE(fg.woba, 0) AS batter_woba_szn,
                COALESCE(fg.iso, 0) AS batter_iso_szn,
                COALESCE(fg.bb_pct, 0) AS batter_bb_pct_szn,
                COALESCE(fg.k_pct, 0) AS batter_k_pct_szn,
                COALESCE(fg.hard_pct, 0) AS batter_hard_pct_szn,
                COALESCE(fg.babip, 0) AS batter_babip_szn,

                -- Park factors
                COALESCE(pf.hits_factor, 1.0) AS park_hits_factor,
                COALESCE(pf.hr_factor, 1.0) AS park_hr_factor,
                COALESCE(pf.runs_factor, 1.0) AS park_runs_factor,

                -- Game lines
                COALESCE(lines.game_total, 0) AS line_total,

                -- Prop line
                COALESCE(props.prop_line, 0) AS prop_line

            FROM mlb_player_game_stats_batting bgs

            JOIN mlb_game_schedule gs ON bgs.game_id = gs.game_id
            LEFT JOIN mlb_players p ON p.player_id = bgs.player_id

            LEFT JOIN LATERAL (
                SELECT avg_h_l5, avg_h_l10, avg_h_l20, avg_h_szn,
                       avg_hr_l5, avg_hr_l10, avg_hr_szn,
                       avg_tb_l5, avg_tb_l10, avg_tb_szn,
                       avg_ab_l5, avg_pa_l5,
                       avg_r_l5, avg_r_l10, avg_r_szn,
                       avg_rbi_l5, avg_rbi_l10, avg_rbi_szn,
                       avg_bb_l5,
                       avg_batting_avg_l10, avg_obp_l10,
                       avg_slg_l10, avg_ops_l10,
                       std_h_l5, std_hr_l5, std_tb_l5, std_rbi_l5, std_r_l5,
                       rest_days, games_last_7d, game_number
                FROM mlb_player_average_batting ba2
                WHERE ba2.player_id = bgs.player_id
                  AND ba2.game_date <= bgs.game_date
                ORDER BY ba2.game_date DESC LIMIT 1
            ) ba ON TRUE

            LEFT JOIN LATERAL (
                SELECT avg_avg_exit_velocity_l5, avg_avg_exit_velocity_l10,
                       avg_avg_launch_angle_l5,
                       avg_barrel_pct_l5, avg_barrel_pct_l10,
                       avg_hard_hit_pct_l5,
                       avg_xba_l5, avg_xba_l10,
                       avg_xslg_l5, avg_xwoba_l5,
                       avg_zone_pct_l5, avg_chase_pct_l5, avg_whiff_pct_l5,
                       avg_fb_pct_l10
                FROM mlb_player_average_statcast_batting sc2
                WHERE sc2.player_id = bgs.player_id
                  AND sc2.game_date <= bgs.game_date
                ORDER BY sc2.game_date DESC LIMIT 1
            ) sc ON TRUE

            LEFT JOIN mlb_player_season_advanced fg
                ON fg.player_id = bgs.player_id
               AND fg.season = bgs.season
               AND fg.player_type = 'batter'

            LEFT JOIN mlb_park_factors pf
                ON pf.venue_id = gs.venue_id
               AND pf.season = gs.season

            LEFT JOIN LATERAL (
                SELECT MAX(CASE WHEN market_key = 'totals'
                                THEN line END) AS game_total
                FROM mlb_raw_game_lines
                WHERE mlb_game_id = bgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            LEFT JOIN LATERAL (
                SELECT sub.line AS prop_line
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM mlb_raw_player_props
                    WHERE player_id = bgs.player_id
                      AND game_id = bgs.game_id
                      AND market_key = :market_key
                      AND bookmaker IN ('pinnacle', 'draftkings')
                    ORDER BY market_key, snapshot_time DESC NULLS LAST
                ) sub
                LIMIT 1
            ) props ON TRUE

            WHERE bgs.game_date = :game_date
              AND bgs.is_starter = TRUE
              AND bgs.did_not_play = FALSE
              AND gs.game_type = 'R'
        """.replace("{target_col}", target_col))

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date, "market_key": market_key})

        if df.empty:
            return df

        # Rename prop_line to stat-specific name
        prop_col = f"prop_line_{market_key}"
        if "prop_line" in df.columns:
            df.rename(columns={"prop_line": prop_col}, inplace=True)

        # Add derived features
        df = self._add_derived_features(df)

        # Add matchup features
        df = self.enrich_with_matchup_features(df)

        return df

    # ------------------------------------------------------------------
    # Private helpers (single-player inference queries)
    # ------------------------------------------------------------------

    def _get_batter_rolling_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch batter rolling averages for inference."""
        query = text("""
            SELECT avg_h_l5, avg_h_l10, avg_h_l20, avg_h_szn,
                   avg_hr_l5, avg_hr_l10, avg_hr_szn,
                   avg_tb_l5, avg_tb_l10, avg_tb_szn,
                   avg_ab_l5, avg_pa_l5,
                   avg_r_l5, avg_r_l10, avg_r_szn,
                   avg_rbi_l5, avg_rbi_l10, avg_rbi_szn,
                   avg_bb_l5,
                   avg_batting_avg_l10, avg_obp_l10,
                   avg_slg_l10, avg_ops_l10,
                   std_h_l5, std_hr_l5, std_tb_l5, std_rbi_l5, std_r_l5,
                   rest_days, games_last_7d, game_number
            FROM mlb_player_average_batting
            WHERE player_id = :player_id
              AND game_date <= :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        if row is None:
            return {f: 0 for f in BATTER_BASE_FEATURES
                    if f.startswith("batter_avg_") or f.startswith("batter_std_")
                    or f in ("batter_rest_days", "batter_games_last_7d", "batter_game_number")}

        return {
            "batter_avg_h_l5": float(row.avg_h_l5 or 0),
            "batter_avg_h_l10": float(row.avg_h_l10 or 0),
            "batter_avg_h_l20": float(row.avg_h_l20 or 0),
            "batter_avg_h_szn": float(row.avg_h_szn or 0),
            "batter_avg_hr_l5": float(row.avg_hr_l5 or 0),
            "batter_avg_hr_l10": float(row.avg_hr_l10 or 0),
            "batter_avg_hr_szn": float(row.avg_hr_szn or 0),
            "batter_avg_tb_l5": float(row.avg_tb_l5 or 0),
            "batter_avg_tb_l10": float(row.avg_tb_l10 or 0),
            "batter_avg_tb_szn": float(row.avg_tb_szn or 0),
            "batter_avg_ab_l5": float(row.avg_ab_l5 or 0),
            "batter_avg_pa_l5": float(row.avg_pa_l5 or 0),
            "batter_avg_r_l5": float(row.avg_r_l5 or 0),
            "batter_avg_r_l10": float(row.avg_r_l10 or 0),
            "batter_avg_r_szn": float(row.avg_r_szn or 0),
            "batter_avg_rbi_l5": float(row.avg_rbi_l5 or 0),
            "batter_avg_rbi_l10": float(row.avg_rbi_l10 or 0),
            "batter_avg_rbi_szn": float(row.avg_rbi_szn or 0),
            "batter_avg_bb_l5": float(row.avg_bb_l5 or 0),
            "batter_avg_batting_avg_l10": float(row.avg_batting_avg_l10 or 0),
            "batter_avg_obp_l10": float(row.avg_obp_l10 or 0),
            "batter_avg_slg_l10": float(row.avg_slg_l10 or 0),
            "batter_avg_ops_l10": float(row.avg_ops_l10 or 0),
            "batter_std_h_l5": float(row.std_h_l5 or 0),
            "batter_std_hr_l5": float(row.std_hr_l5 or 0),
            "batter_std_tb_l5": float(row.std_tb_l5 or 0),
            "batter_std_rbi_l5": float(row.std_rbi_l5 or 0),
            "batter_std_r_l5": float(row.std_r_l5 or 0),
            "batter_rest_days": int(row.rest_days or 2),
            "batter_games_last_7d": int(row.games_last_7d or 0),
            "batter_game_number": int(row.game_number or 0),
        }

    def _get_batter_statcast_stats(self, player_id: int, game_date: str) -> dict:
        """Fetch Statcast batting rolling averages for inference."""
        query = text("""
            SELECT avg_avg_exit_velocity_l5, avg_avg_exit_velocity_l10,
                   avg_avg_launch_angle_l5,
                   avg_barrel_pct_l5, avg_barrel_pct_l10,
                   avg_hard_hit_pct_l5,
                   avg_xba_l5, avg_xba_l10,
                   avg_xslg_l5, avg_xwoba_l5,
                   avg_zone_pct_l5, avg_chase_pct_l5, avg_whiff_pct_l5,
                   avg_fb_pct_l10
            FROM mlb_player_average_statcast_batting
            WHERE player_id = :player_id
              AND game_date <= :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "game_date": game_date}).fetchone()

        defaults = {
            "batter_avg_exit_velocity_l5": 0, "batter_avg_exit_velocity_l10": 0,
            "batter_avg_launch_angle_l5": 0,
            "batter_barrel_pct_l5": 0, "batter_barrel_pct_l10": 0,
            "batter_hard_hit_pct_l5": 0,
            "batter_xba_l5": 0, "batter_xba_l10": 0,
            "batter_xslg_l5": 0, "batter_xwoba_l5": 0,
            "batter_zone_pct_l5": 0, "batter_chase_pct_l5": 0, "batter_whiff_pct_l5": 0,
            "batter_fb_pct_l10": 0,
        }

        if row is None:
            return defaults

        return {
            "batter_avg_exit_velocity_l5": float(row.avg_avg_exit_velocity_l5 or 0),
            "batter_avg_exit_velocity_l10": float(row.avg_avg_exit_velocity_l10 or 0),
            "batter_avg_launch_angle_l5": float(row.avg_avg_launch_angle_l5 or 0),
            "batter_barrel_pct_l5": float(row.avg_barrel_pct_l5 or 0),
            "batter_barrel_pct_l10": float(row.avg_barrel_pct_l10 or 0),
            "batter_hard_hit_pct_l5": float(row.avg_hard_hit_pct_l5 or 0),
            "batter_xba_l5": float(row.avg_xba_l5 or 0),
            "batter_xba_l10": float(row.avg_xba_l10 or 0),
            "batter_xslg_l5": float(row.avg_xslg_l5 or 0),
            "batter_xwoba_l5": float(row.avg_xwoba_l5 or 0),
            "batter_zone_pct_l5": float(row.avg_zone_pct_l5 or 0),
            "batter_chase_pct_l5": float(row.avg_chase_pct_l5 or 0),
            "batter_whiff_pct_l5": float(row.avg_whiff_pct_l5 or 0),
            "batter_fb_pct_l10": float(row.avg_fb_pct_l10 or 0),
        }

    def _get_batter_fangraphs_stats(self, player_id: int, season: int) -> dict:
        """Fetch FanGraphs season-level batter stats."""
        query = text("""
            SELECT wrc_plus, woba, iso, bb_pct, k_pct, hard_pct, babip
            FROM mlb_player_season_advanced
            WHERE player_id = :player_id
              AND season = :season
              AND player_type = 'batter'
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id, "season": season}).fetchone()

        if row is None:
            return {
                "batter_wrc_plus_szn": 0, "batter_woba_szn": 0, "batter_iso_szn": 0,
                "batter_bb_pct_szn": 0, "batter_k_pct_szn": 0, "batter_hard_pct_szn": 0,
                "batter_babip_szn": 0,
            }

        return {
            "batter_wrc_plus_szn": float(row.wrc_plus or 0),
            "batter_woba_szn": float(row.woba or 0),
            "batter_iso_szn": float(row.iso or 0),
            "batter_bb_pct_szn": float(row.bb_pct or 0),
            "batter_k_pct_szn": float(row.k_pct or 0),
            "batter_hard_pct_szn": float(row.hard_pct or 0),
            "batter_babip_szn": float(row.babip or 0),
        }

    def _get_park_factors(self, venue_id: int, season: int) -> dict:
        """Fetch park factors for venue."""
        query = text("""
            SELECT hits_factor, hr_factor, runs_factor
            FROM mlb_park_factors
            WHERE venue_id = :venue_id AND season = :season
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"venue_id": venue_id, "season": season}).fetchone()
        if row is None:
            return {"hits_factor": 1.0, "hr_factor": 1.0, "runs_factor": 1.0}
        return {
            "hits_factor": float(row.hits_factor or 1.0),
            "hr_factor": float(row.hr_factor or 1.0),
            "runs_factor": float(row.runs_factor or 1.0),
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

    def _get_prop_line(self, player_id: int, game_id: int, market_key: str) -> float:
        """Fetch batter prop line (latest snapshot)."""
        query = text("""
            SELECT line
            FROM mlb_raw_player_props
            WHERE player_id = :player_id
              AND game_id = :game_id
              AND market_key = :market_key
              AND bookmaker IN ('pinnacle', 'draftkings')
            ORDER BY snapshot_time DESC NULLS LAST
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(
                query, {"player_id": player_id, "game_id": game_id, "market_key": market_key}
            ).fetchone()
        return float(row.line) if row and row.line else 0

    def _get_platoon_features(self, player_id: int, opp_pitcher_id: int, game_date: str) -> dict:
        """Fetch platoon split features for a single batter."""
        # Get handedness
        query = text("""
            SELECT bp.bats, opp.throws
            FROM mlb_players bp, mlb_players opp
            WHERE bp.player_id = :batter_id AND opp.player_id = :pitcher_id
        """)
        with self.engine.connect() as conn:
            row = conn.execute(
                query, {"batter_id": player_id, "pitcher_id": opp_pitcher_id}
            ).fetchone()

        if row is None or row.throws is None:
            return {"is_same_hand": 0, "batter_avg_h_vs_hand_l20": 0,
                    "batter_avg_ops_vs_hand_l20": 0, "batter_avg_hr_vs_hand_l20": 0}

        bats = row.bats
        throws = row.throws
        is_same_hand = 0 if bats == "S" else (1 if bats == throws else 0)

        # L20 vs same hand
        platoon_query = text("""
            SELECT
                CASE WHEN COUNT(*) >= 3 THEN AVG(b2.h) END AS avg_h,
                CASE WHEN COUNT(*) >= 3 THEN AVG(b2.hr) END AS avg_hr,
                CASE WHEN SUM(b2.ab) > 0 AND COUNT(*) >= 3
                     THEN (SUM(b2.h) + SUM(b2.bb))::NUMERIC
                          / NULLIF(SUM(b2.ab) + SUM(b2.bb), 0)
                          + SUM(b2.tb)::NUMERIC / NULLIF(SUM(b2.ab), 0)
                END AS ops
            FROM (
                SELECT b2.h, b2.hr, b2.ab, b2.bb, b2.tb
                FROM mlb_player_game_stats_batting b2
                JOIN mlb_player_game_stats_pitching opp2
                    ON opp2.game_id = b2.game_id
                   AND opp2.team_id != b2.team_id
                   AND opp2.is_starter = TRUE AND opp2.did_not_play = FALSE
                JOIN mlb_players opp_p ON opp_p.player_id = opp2.player_id
                WHERE b2.player_id = :player_id
                  AND b2.game_date < :game_date
                  AND b2.did_not_play = FALSE
                  AND opp_p.throws = :throws
                ORDER BY b2.game_date DESC LIMIT 20
            ) b2
        """)
        with self.engine.connect() as conn:
            prow = conn.execute(
                platoon_query,
                {"player_id": player_id, "game_date": game_date, "throws": throws},
            ).fetchone()

        return {
            "is_same_hand": is_same_hand,
            "batter_avg_h_vs_hand_l20": float(prow.avg_h or 0) if prow else 0,
            "batter_avg_hr_vs_hand_l20": float(prow.avg_hr or 0) if prow else 0,
            "batter_avg_ops_vs_hand_l20": float(prow.ops or 0) if prow else 0,
        }
