"""MLB feature contract constants and validation helpers.

This module owns feature lists/stat mappings for the MLB feature-store boundary
migration. Legacy facade modules re-export these names for compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Feature lists (centralized, locked for model compatibility)
# ---------------------------------------------------------------------------

PITCHER_K_FEATURES: list[str] = [
    # Pitcher rolling/context features (from mlb_player_average_pitching)
    "pitcher_avg_so_l3",
    "pitcher_avg_so_l5",
    "pitcher_avg_so_szn",
    "pitcher_avg_k_per_9_l5",
    "pitcher_avg_ip_l3",
    "pitcher_avg_ip_l5",
    "pitcher_avg_ip_szn",
    "pitcher_min_ip_l5",
    "pitcher_max_ip_l5",
    "pitcher_median_ip_l5",
    "pitcher_ip_range_l5",
    "pitcher_short_start_rate_l5",
    "pitcher_start_stability_l5",
    "pitcher_avg_batters_faced_l5",
    "pitcher_avg_batters_faced_szn",
    "pitcher_avg_pitches_per_start_l5",
    "pitcher_avg_pitches_thrown_l3",
    "pitcher_avg_pitches_thrown_l5",
    "pitcher_workload_spike_ratio",
    "pitcher_recent_pitch_count_trend",
    "rest_after_high_pitch_count",
    "pitcher_avg_bb_l5",
    "pitcher_std_so_l3",

    # Derived/normalized features
    "pitcher_est_bf_l5",
    "pitcher_pitches_per_ip_l5",
    "pitcher_so_l3_l5_ratio",

    # Pitcher Statcast (from mlb_player_average_statcast_pitching)
    "pitcher_avg_whiff_pct_l5",
    "pitcher_avg_csw_pct_l5",
    "pitcher_avg_chase_pct_l5",
    "pitcher_avg_zone_pct_l5",
    "pitcher_avg_fastball_velo_l5",
    "pitcher_std_whiff_pct_l3",

    # Team context
    "team_starter_avg_ip_l10",
    "team_starter_short_start_rate_l10",
    "team_starter_avg_pitches_l10",
    "team_starter_avg_ip_l30",
    "team_starter_short_hook_rate_l30",
    "team_starter_deep_start_rate_l30",
    "bullpen_fatigue_pressure",
    "pitcher_days_rest",
    "pitcher_pitch_count_last_start",
    "pitcher_starts_szn",

    # Phase 3B: pitcher-side downside / short-outing risk only.
    "manager_starter_short_hook_rate_l30",
    "pitcher_pct_starts_under_5_ip_l10",
    "pitcher_fastball_velo_delta_l3_vs_szn",
    "team_bullpen_pitches_last_3d",
    "pitcher_left_last_start_early_flag",

    # Existing bullpen context retained for compatibility / derived fatigue pressure.
    "team_bullpen_ip_last_3d",

    # FanGraphs season-to-date snapshots from mlb_player_season_advanced_history.
    # Joined with `as_of_date < game_date` (strict) — point-in-time, no leak.
    "pitcher_fip_szn",
    "pitcher_k_pct_szn",

    # Opposing team batting context
    "opp_team_avg_so_l10",
    "opp_team_avg_batting_avg_l10",
    "opp_team_k_pct_l10",
    "opp_team_whiff_pct_l10",
    "opp_team_contact_rate_l10",
    "opp_team_chase_pct_l10",
    "opp_team_zone_contact_pct_l10",

    # Game context
    "park_so_factor",
    "is_home",
    "line_total",

    # Weather (mlb_game_weather)
    "air_density_idx",
    "wind_out_mph",

    # Betting signal
    "prop_line_pitcher_strikeouts",

    # Inning-level fatigue (from mlb_pitcher_inning_stats)
    "pitcher_velo_drop_late_l5",
    "pitcher_avg_whiff_rate_late_l5",
    "pitcher_avg_k_rate_early_l5",
    "pitcher_avg_pitches_per_inning_l5",
    "pitcher_avg_csw_rate_l5_inning",
    "pitcher_deep_inning_pct_l5",

    # First-5-IP K rate
    "pitcher_avg_k_first_5ip_l5",

    # Interaction features
    "pitcher_k_opp_k_interaction",
    "pitcher_whiff_opp_whiff_interaction",

    # Pitch repertoire diversity
    "pitcher_fastball_pct_l5",
    "pitcher_breaking_pct_l5",
    "pitcher_offspeed_pct_l5",
    "pitcher_num_pitch_types_l5",

    # Opposing lineup composition/contact profile
    "projected_lineup_k_pct",
    "projected_lineup_whiff_pct",
    "projected_lineup_chase_pct",
    "projected_lineup_contact_rate",
    "projected_lineup_same_hand_k_pct",
    "projected_lineup_opposite_hand_k_pct",
    "projected_lineup_hand_k_delta",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",

    # Umpire tendency
    "umpire_avg_k_per_game_l20",
]


# Feature 3 uses the existing mlb_player_average_statcast_pitching rolling
# population: avg_avg_fastball_velo_l3 - avg_avg_fastball_velo_szn. This keeps
# Phase 3B cheap and aligned with current pitcher-K Statcast feature sourcing.
PITCHER_K_PHASE3B_ADDED_FEATURES: list[str] = [
    "manager_starter_short_hook_rate_l30",
    "pitcher_pct_starts_under_5_ip_l10",
    "pitcher_fastball_velo_delta_l3_vs_szn",
    "team_bullpen_pitches_last_3d",
    "pitcher_left_last_start_early_flag",
]

# Phase 3A lineup/contact features were evaluated and rejected: they compressed
# the Phase 2 contrarian-under edge. Keep them computable/returnable for
# backwards compatibility with old artifacts, but do not let new training runs
# select them unless this constant is deliberately changed in a future phase.
PITCHER_K_PHASE3A_REJECTED_FEATURES: set[str] = {
    "projected_lineup_k_pct",
    "projected_lineup_whiff_pct",
    "projected_lineup_chase_pct",
    "projected_lineup_contact_rate",
    "projected_lineup_same_hand_k_pct",
    "projected_lineup_opposite_hand_k_pct",
    "projected_lineup_hand_k_delta",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",
    "umpire_avg_k_per_game_l20",
}

PITCHER_K_TRAINING_FEATURES: list[str] = [
    feature for feature in PITCHER_K_FEATURES
    if feature not in PITCHER_K_PHASE3A_REJECTED_FEATURES
]
PITCHER_K_EXCLUDED_TRAINING_FEATURES = PITCHER_K_PHASE3A_REJECTED_FEATURES


LINEUP_FEATURE_DEFAULTS: dict[str, float] = {
    "projected_lineup_k_pct": 0.22,
    "projected_lineup_whiff_pct": 0.22,
    "projected_lineup_chase_pct": 0.28,
    "projected_lineup_contact_rate": 0.78,
    "projected_lineup_same_hand_k_pct": 0.22,
    "projected_lineup_opposite_hand_k_pct": 0.22,
    "projected_lineup_hand_k_delta": 0.0,
    "projected_lineup_top3_k_pct": 0.22,
    "projected_lineup_mid3_k_pct": 0.22,
    "projected_lineup_bot3_k_pct": 0.22,
    "projected_lineup_k_concentration": 0.0,
    "pct_opp_lineup_same_hand": 0.50,
}


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
# For compound stats, the value is a SQL expression fragment: used as
# `bgs.{target_col}` in the training query, so bgs.h + bgs.r + bgs.rbi is correct.
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
    "batter_avg_hr_l5", "batter_avg_hr_l10",
    "batter_avg_tb_l5", "batter_avg_tb_l10", "batter_avg_tb_szn",
    "batter_avg_ab_l5", "batter_avg_pa_l5",
    "batter_avg_r_l10", "batter_avg_r_szn",
    "batter_avg_rbi_l5", "batter_avg_rbi_l10",
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
    # FanGraphs season-to-date snapshots (point-in-time via _history table,
    # joined with `as_of_date < game_date`). BR backfill populates k_pct,
    # bb_pct, iso, babip, avg/obp/slg/ops; FG-only columns (wrc_plus, woba,
    # hard_pct) are NULL for pre-2026 dates and FG-populated for 2026+ daily.
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
    "opp_pitcher_babip_against_l5",
    # Platoon
    "is_same_hand", "batter_avg_h_vs_hand_l20", "batter_avg_ops_vs_hand_l20",
    # Game context
    "is_home", "line_total",
    # Weather (mlb_game_weather)
    "air_density_idx",
    "wind_out_mph",
    "has_precip",
    # Derived (Python post-SQL)
    "batter_h_l5_l10_ratio",
    # Opposing bullpen workload (mlb_bullpen_daily_status)
    "opp_bullpen_ip_last_3d",
    "opp_bullpen_era_last_7d",
    "opp_relievers_available",
    "opp_bullpen_pitches_last_3d",
    # Opposing starter inning-level (mlb_pitcher_inning_stats)
    "opp_pitcher_velo_drop_late_l5",
    "opp_pitcher_avg_pitches_per_inning_l5",
    "opp_pitcher_deep_inning_pct_l5",
    # Umpire tendency
    "umpire_avg_k_per_game_l20",
]

# Per-stat extensions
BATTER_HITS_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_hits_factor", "prop_line_batter_hits",
    "projected_ab",
    "batter_gb_pct_l10", "batter_fb_pct_l10",
    "batter_babip_opp_babip_interaction", "projected_ab_x_recent_form",
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

BATTER_FORCE_FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "market": (
        "prop_line_batter_hits",
        "line_total",
    ),
    "recent_form": (
        "batter_avg_h_l5",
        "batter_avg_h_l10",
        "batter_avg_h_l20",
        "batter_avg_h_szn",
        "batter_h_l5_l10_ratio",
        "batter_std_h_l5",
    ),
    "contact_quality": (
        "batter_avg_exit_velocity_l5",
        "batter_avg_exit_velocity_l10",
        "batter_avg_launch_angle_l5",
        "batter_barrel_pct_l5",
        "batter_barrel_pct_l10",
        "batter_hard_hit_pct_l5",
        "batter_xba_l5",
        "batter_xba_l10",
        "batter_xslg_l5",
        "batter_xwoba_l5",
        "batter_zone_pct_l5",
        "batter_chase_pct_l5",
        "batter_whiff_pct_l5",
        "batter_gb_pct_l10",
        "batter_fb_pct_l10",
        "batter_babip_szn",
        "batter_hard_pct_szn",
    ),
    "matchup_pitcher": (
        "opp_pitcher_avg_era_l5",
        "opp_pitcher_avg_whip_l5",
        "opp_pitcher_avg_k_per_9_l5",
        "opp_pitcher_avg_bb_per_9_l5",
        "opp_pitcher_avg_h_allowed_l5",
        "opp_pitcher_avg_hr_allowed_l5",
        "opp_pitcher_xwoba_against_l5",
        "opp_pitcher_hard_hit_pct_against_l5",
        "opp_pitcher_avg_fastball_velo_l5",
        "opp_pitcher_days_rest",
        "opp_pitcher_babip_against_l5",
        "opp_pitcher_velo_drop_late_l5",
        "opp_pitcher_avg_pitches_per_inning_l5",
        "opp_pitcher_deep_inning_pct_l5",
    ),
    "bullpen": (
        "opp_bullpen_ip_last_3d",
        "opp_bullpen_era_last_7d",
        "opp_relievers_available",
        "opp_bullpen_pitches_last_3d",
    ),
    "platoon": (
        "is_same_hand",
        "batter_avg_h_vs_hand_l20",
        "batter_avg_ops_vs_hand_l20",
    ),
    "environment": (
        "park_hits_factor",
        "air_density_idx",
        "wind_out_mph",
        "has_precip",
        "is_home",
    ),
    "opportunity": (
        "lineup_position",
        "projected_ab",
        "batter_avg_ab_l5",
        "batter_avg_pa_l5",
        "batter_rest_days",
        "batter_games_last_7d",
        "batter_game_number",
    ),
}


def normalize_feature_family_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize CLI family names and fail on unknown names."""
    if not names:
        return []

    normalized: list[str] = []
    unknown: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip().lower().replace("-", "_")
            if not name:
                continue
            if name not in BATTER_FORCE_FEATURE_FAMILIES:
                unknown.append(name)
            elif name not in normalized:
                normalized.append(name)

    if unknown:
        valid = ", ".join(sorted(BATTER_FORCE_FEATURE_FAMILIES))
        raise ValueError(f"Unknown batter feature family/families: {unknown}. Valid: {valid}")
    return normalized


def normalize_feature_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize comma-separated CLI feature names while preserving order."""
    if not names:
        return []

    normalized: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip()
            if name and name not in normalized:
                normalized.append(name)
    return normalized


def features_for_batter_families(families: list[str] | tuple[str, ...]) -> list[str]:
    """Expand family names to a de-duped feature list, preserving registry order."""
    expanded: list[str] = []
    for family in families:
        for feature in BATTER_FORCE_FEATURE_FAMILIES[family]:
            if feature not in expanded:
                expanded.append(feature)
    return expanded


@dataclass(frozen=True)
class PitcherFeatureContract:
    stat: str = "pitcher_strikeouts"
    features: tuple[str, ...] = tuple(PITCHER_K_FEATURES)
    training_features: tuple[str, ...] = tuple(PITCHER_K_TRAINING_FEATURES)
    market_key: str = "pitcher_strikeouts"


@dataclass(frozen=True)
class BatterFeatureContract:
    stat: str
    features: tuple[str, ...]
    target: str
    market_key: str


def get_batter_feature_contract(stat: str) -> BatterFeatureContract:
    return BatterFeatureContract(
        stat=stat,
        features=tuple(BATTER_FEATURE_MAP[stat]),
        target=BATTER_STAT_TARGET[stat],
        market_key=BATTER_STAT_MARKET_KEY[stat],
    )


def get_features_for_stat(stat: str) -> list[str]:
    """Return a copy of the feature list for a given batter stat."""
    return list(BATTER_FEATURE_MAP[stat])


def find_duplicate_features(features: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    dupes: list[str] = []
    for feature in features:
        if feature in seen and feature not in dupes:
            dupes.append(feature)
        seen.add(feature)
    return dupes


FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "pitcher_contract": tuple(PITCHER_K_FEATURES),
    "pitcher_training": tuple(PITCHER_K_TRAINING_FEATURES),
    "batter_base": tuple(BATTER_BASE_FEATURES),
    "batter_hits": tuple(BATTER_HITS_FEATURES),
    "batter_home_runs": tuple(BATTER_HR_FEATURES),
    "batter_total_bases": tuple(BATTER_TOTAL_BASES_FEATURES),
    "batter_rbis": tuple(BATTER_RBIS_FEATURES),
    "batter_runs": tuple(BATTER_RUNS_FEATURES),
}


def uncovered_features() -> list[str]:
    covered: set[str] = set()
    for members in FEATURE_FAMILIES.values():
        covered.update(members)
    all_features = set(PITCHER_K_FEATURES)
    for features in BATTER_FEATURE_MAP.values():
        all_features.update(features)
    return sorted(all_features - covered)
