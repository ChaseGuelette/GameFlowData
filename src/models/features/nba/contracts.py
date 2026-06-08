"""NBA feature contract constants and validation helpers.

This module owns the stable NBA feature lists for the feature-store boundary
migration. ``src.models.feature_store`` re-exports these names for compatibility.
"""

from __future__ import annotations

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
    # Minutes trend ratios (role-change signal)
    "player_min_l3_l5_ratio",
    "player_min_l3_l15_ratio",
    # Season average minutes (baseline anchor)
    "player_avg_min_szn",
    # B4: Minutes stability
    "player_min_std_l5",
    "player_min_floor_l5",
    "player_games_started_l5",
    "player_starter_prob",
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
    # B5: Position-matched injury opportunity
    "team_out_same_pos_count",
    "team_out_same_pos_min_sum",
    "team_out_same_pos_usg_sum",
    "team_out_same_pos_starter_sum",
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
    "player_pts_l3_l15_ratio",  # PTS uses L3/L15; REB/AST/THREES use L3/L5 (column name kept for model compat)
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
    # B4: Starter signal
    "player_starter_prob",
    # Minutes trend ratios (role-change signal)
    "player_min_l3_l5_ratio",
    "player_min_l3_l15_ratio",
    # B5: Position-matched injury opportunity
    "team_out_same_pos_count",
    "team_out_same_pos_min_sum",
    "team_out_same_pos_usg_sum",
    "team_out_same_pos_starter_sum",
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
    "player_reb_l3_l15_ratio",  # Actually L3/L5 — name kept for model artifact compat (ISS-017)
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
    # B4: Starter signal
    "player_starter_prob",
    # Minutes trend ratios (role-change signal)
    "player_min_l3_l5_ratio",
    "player_min_l3_l15_ratio",
    # B5: Position-matched injury opportunity
    "team_out_same_pos_count",
    "team_out_same_pos_min_sum",
    "team_out_same_pos_usg_sum",
    "team_out_same_pos_starter_sum",
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
    "player_ast_l3_l15_ratio",  # Actually L3/L5 — name kept for model artifact compat (ISS-017)
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
    # B4: Starter signal
    "player_starter_prob",
    # Minutes trend ratios (role-change signal)
    "player_min_l3_l5_ratio",
    "player_min_l3_l15_ratio",
    # B5: Position-matched injury opportunity
    "team_out_same_pos_count",
    "team_out_same_pos_min_sum",
    "team_out_same_pos_usg_sum",
    "team_out_same_pos_starter_sum",
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
    "player_fg3m_l3_l15_ratio",  # Actually L3/L5 — name kept for model artifact compat (ISS-017)
    "player_std_fg3m_l5",
    # Minutes trend ratios (role-change signal)
    "player_min_l3_l5_ratio",
    "player_min_l3_l15_ratio",
    # Injury context
    "team_out_count",
    "team_out_min_sum",
    "team_out_pts_sum",
    "team_out_reb_sum",
    "team_out_ast_sum",
    "team_out_usg_sum",
    "opp_out_count",
    "opp_out_min_sum",
    # B5: Position-matched injury opportunity
    "team_out_same_pos_count",
    "team_out_same_pos_min_sum",
    "team_out_same_pos_usg_sum",
    "team_out_same_pos_starter_sum",
]

RATE_FEATURES_BY_STAT = {
    "pts": RATE_FEATURES_PTS,
    "reb": RATE_FEATURES_REB,
    "ast": RATE_FEATURES_AST,
    "threes": RATE_FEATURES_THREES,
}

NBA_FEATURE_LISTS = {
    "minutes": MINUTES_FEATURES,
    **RATE_FEATURES_BY_STAT,
}

NBA_FEATURE_FAMILIES = {
    "player_rolling",
    "team_context",
    "opponent_context",
    "line_context",
    "game_context",
    "schedule_context",
    "injury_context",
}


def feature_family(feature: str) -> str:
    """Return a coarse review family for a stable NBA feature name."""
    if feature.startswith("player_"):
        return "player_rolling"
    if feature.startswith("team_avg_"):
        return "team_context"
    if feature.startswith("opp_avg_") or feature.startswith("opp_pos_"):
        return "opponent_context"
    if feature.startswith("prop_line_") or feature.startswith("line_"):
        return "line_context"
    if feature in {"is_home"}:
        return "game_context"
    if feature in {"rest_days", "is_back_to_back", "games_in_last_7_days"}:
        return "schedule_context"
    if feature.startswith("team_out_") or feature.startswith("opp_out_"):
        return "injury_context"
    raise KeyError(f"Unknown NBA feature family for {feature!r}")


def validate_feature_lists() -> None:
    """Fail if stable contract lists contain duplicates or unowned features."""
    for name, features in NBA_FEATURE_LISTS.items():
        if len(features) != len(set(features)):
            raise ValueError(f"Duplicate features in {name}")
        for feature in features:
            feature_family(feature)


__all__ = [
    "MINUTES_FEATURES",
    "RATE_FEATURES_PTS",
    "RATE_FEATURES_REB",
    "RATE_FEATURES_AST",
    "RATE_FEATURES_THREES",
    "RATE_FEATURES_BY_STAT",
    "NBA_FEATURE_LISTS",
    "NBA_FEATURE_FAMILIES",
    "feature_family",
    "validate_feature_lists",
]
