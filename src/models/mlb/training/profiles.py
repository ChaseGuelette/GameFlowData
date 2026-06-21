"""Shared MLB training profile declarations for stat-suite migration slices.

This module is declarative only. It must not import training pipelines or change
model behavior; existing entrypoints continue to own execution until later slices.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping

from src.models.mlb.features.contracts import (
    BATTER_FORCE_FEATURE_FAMILIES,
    PITCHER_K_PHASE3A_REJECTED_FEATURES,
)

TrainEntrypointKind = Literal["pitcher_quantile", "batter"]
DefaultDirection = Literal["over", "under", "both"]


PITCHER_K_FORCE_FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "market": (
        "prop_line_pitcher_strikeouts",
        "line_total",
    ),
    "workload_leash": (
        "pitcher_avg_ip_l3",
        "pitcher_avg_ip_l5",
        "pitcher_avg_ip_szn",
        "pitcher_min_ip_l5",
        "pitcher_max_ip_l5",
        "pitcher_median_ip_l5",
        "pitcher_ip_range_l5",
        "pitcher_short_start_rate_l5",
        "pitcher_start_stability_l5",
        "pitcher_avg_pitches_per_start_l5",
        "pitcher_avg_pitches_thrown_l3",
        "pitcher_avg_pitches_thrown_l5",
        "pitcher_workload_spike_ratio",
        "pitcher_recent_pitch_count_trend",
        "rest_after_high_pitch_count",
    ),
    "team_hook": (
        "team_starter_avg_ip_l30",
        "team_starter_short_hook_rate_l30",
        "team_starter_deep_start_rate_l30",
        "manager_starter_short_hook_rate_l30",
    ),
    "pitcher_stuff": (
        "pitcher_avg_whiff_pct_l5",
        "pitcher_avg_csw_pct_l5",
        "pitcher_avg_chase_pct_l5",
        "pitcher_avg_zone_pct_l5",
        "pitcher_avg_fastball_velo_l5",
        "pitcher_std_whiff_pct_l3",
        "pitcher_fastball_velo_delta_l3_vs_szn",
        "pitcher_fip_szn",
        "pitcher_k_pct_szn",
    ),
    "inning_fatigue": (
        "pitcher_velo_drop_late_l5",
        "pitcher_avg_whiff_rate_late_l5",
        "pitcher_avg_k_rate_early_l5",
        "pitcher_avg_pitches_per_inning_l5",
        "pitcher_avg_csw_rate_l5_inning",
        "pitcher_deep_inning_pct_l5",
        "pitcher_avg_k_first_5ip_l5",
    ),
    "opponent_contact": (
        "opp_team_avg_so_l10",
        "opp_team_k_pct_l10",
        "opp_team_whiff_pct_l10",
        "opp_team_contact_rate_l10",
        "opp_team_chase_pct_l10",
        "opp_team_zone_contact_pct_l10",
    ),
    "environment": (
        "park_so_factor",
        "is_home",
        "line_total",
        "air_density_idx",
        "wind_out_mph",
    ),
    "phase3b_downside": (
        "manager_starter_short_hook_rate_l30",
        "pitcher_pct_starts_under_5_ip_l10",
        "pitcher_fastball_velo_delta_l3_vs_szn",
        "team_bullpen_pitches_last_3d",
        "pitcher_left_last_start_early_flag",
    ),
    "ip_feature_source": (
        "predicted_ip_q25",
        "predicted_ip_q50",
        "predicted_ip_spread",
        "predicted_ip_q25_delta",
    ),
}


@dataclass(frozen=True)
class MLBTrainingProfile:
    stat_key: str
    display_name: str
    train_entrypoint_kind: TrainEntrypointKind
    model_type: str
    target_columns: tuple[str, ...]
    prop_line_feature: str | None
    default_direction: DefaultDirection
    artifact_prefix: str
    model_artifact_names: tuple[str, ...]
    feature_families: Mapping[str, tuple[str, ...]]
    locked_out_features: tuple[str, ...]
    default_quote_policy: str
    default_line_source: str
    default_book_routing_policy: str
    min_decision_grade_bets: int
    train_short_stat: str | None = None


_PROFILES: dict[str, MLBTrainingProfile] = {
    "batter_hits": MLBTrainingProfile(
        stat_key="batter_hits",
        display_name="Batter Hits",
        train_entrypoint_kind="batter",
        model_type="binomial",
        train_short_stat="hits",
        target_columns=("actual", "actual_at_bats"),
        prop_line_feature="prop_line_batter_hits",
        default_direction="both",
        artifact_prefix="mlb_run_batter_hits",
        model_artifact_names=(
            "batter_hits_binomial_booster.json",
            "batter_hits_binomial_meta.json",
            "batter_ab_xgblss_booster.json",
            "batter_ab_negbin_meta.json",
        ),
        feature_families=MappingProxyType(dict(BATTER_FORCE_FEATURE_FAMILIES)),
        locked_out_features=(),
        default_quote_policy="slate_or_tminus",
        default_line_source="mlb_player_props_clv_snapshots",
        default_book_routing_policy="preferred_book_first",
        min_decision_grade_bets=100,
    ),
    "pitcher_strikeouts": MLBTrainingProfile(
        stat_key="pitcher_strikeouts",
        display_name="Pitcher Strikeouts",
        train_entrypoint_kind="pitcher_quantile",
        model_type="quantile",
        train_short_stat=None,
        target_columns=("actual_so", "actual_ip"),
        prop_line_feature="prop_line_pitcher_strikeouts",
        default_direction="under",
        artifact_prefix="mlb_run",
        model_artifact_names=("pitcher_k_model.joblib", "pitcher_k_feature_config.joblib"),
        feature_families=MappingProxyType(PITCHER_K_FORCE_FEATURE_FAMILIES),
        locked_out_features=tuple(sorted(PITCHER_K_PHASE3A_REJECTED_FEATURES)),
        default_quote_policy="slate_or_tminus",
        default_line_source="mlb_player_props_clv_snapshots",
        default_book_routing_policy="preferred_book_first",
        min_decision_grade_bets=100,
    ),
}

_ALIASES: dict[str, str] = {
    "hits": "batter_hits",
    "batter-hits": "batter_hits",
    "pitcher_k": "pitcher_strikeouts",
    "pitcher-k": "pitcher_strikeouts",
    "pitcher_ks": "pitcher_strikeouts",
    "pitcher-strikeouts": "pitcher_strikeouts",
}


def _normalize_profile_name(name: str) -> str:
    key = str(name).strip().lower()
    return _ALIASES.get(key, key)


def get_training_profile(name: str) -> MLBTrainingProfile:
    """Return an MLB stat training profile by canonical name or supported alias."""
    key = _normalize_profile_name(name)
    try:
        return _PROFILES[key]
    except KeyError as exc:
        valid = ", ".join(list_training_profiles())
        raise ValueError(f"Unknown MLB training profile {name!r}. Valid: {valid}") from exc


def list_training_profiles() -> list[str]:
    """Return canonical profiles in stable implementation order."""
    return ["batter_hits", "pitcher_strikeouts"]
