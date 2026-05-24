"""Pure pandas transforms for MLB feature-store outputs.

These functions intentionally preserve the legacy facade behavior while making
Python-only feature transforms testable without DB-backed feature-store classes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.models.mlb.features.contracts import LINEUP_FEATURE_DEFAULTS

PA_BY_LINEUP_POSITION = {1: 4.3, 2: 4.2, 3: 3.9, 4: 3.8, 5: 3.7, 6: 3.5, 7: 3.3, 8: 3.1, 9: 3.0}


def add_batter_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add batter derived features, preserving legacy in-place mutation."""
    df["batter_h_l5_l10_ratio"] = df.apply(
        lambda r: (r["batter_avg_h_l5"] / r["batter_avg_h_l10"])
        if r.get("batter_avg_h_l10", 0) > 0
        else 1.0,
        axis=1,
    )

    if "batter_avg_ab_l5" in df.columns:
        df["_position_pa"] = df["lineup_position"].map(PA_BY_LINEUP_POSITION)
        df["projected_ab"] = df.apply(
            lambda r: max(0.5 * (r["batter_avg_ab_l5"] if pd.notna(r["batter_avg_ab_l5"]) else 3.5) + 0.5 * r["_position_pa"], 1.0)
            if pd.notna(r["_position_pa"])
            else max(r["batter_avg_ab_l5"] if pd.notna(r["batter_avg_ab_l5"]) else 3.5, 1.0),
            axis=1,
        )
        df.drop(columns=["_position_pa"], inplace=True)
    else:
        df["projected_ab"] = 3.5
    return df


def add_batter_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute batter interaction features that depend on matchup data."""
    df["batter_babip_opp_babip_interaction"] = (
        df["batter_babip_szn"].fillna(0.300)
        * df["opp_pitcher_babip_against_l5"].fillna(0.300)
    )
    df["projected_ab_x_recent_form"] = (
        df["projected_ab"].fillna(0) * df["batter_avg_h_l10"].fillna(0)
    )
    return df


def add_pitcher_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add pitcher derived/default features, preserving legacy in-place mutation."""
    def ensure_col(name: str, default=0.0):
        if name not in df.columns:
            df[name] = default
        return df[name]

    for col in [
        "pitcher_avg_so_l3", "pitcher_avg_so_l5", "pitcher_avg_ip_l5",
        "pitcher_avg_ip_szn", "pitcher_min_ip_l5", "pitcher_avg_h_allowed_l5",
        "pitcher_avg_bb_l5", "pitcher_avg_pitches_thrown_l3",
        "pitcher_avg_pitches_thrown_l5", "pitcher_pitch_count_last_start",
        "pitcher_days_rest", "pitcher_starts_szn", "team_bullpen_ip_last_3d",
        "team_bullpen_pitches_last_3d", "opp_team_whiff_pct_l10",
        "pitcher_fastball_pct_l5", "pitcher_breaking_pct_l5", "pitcher_offspeed_pct_l5",
        "pitcher_pct_starts_under_5_ip_l10", "pitcher_fastball_velo_delta_l3_vs_szn",
        "pitcher_left_last_start_early_flag", "manager_starter_short_hook_rate_l30",
    ]:
        ensure_col(col, 0.0)

    df["pitcher_so_l3_l5_ratio"] = df["pitcher_avg_so_l3"].div(
        df["pitcher_avg_so_l5"].replace(0, np.nan)
    ).fillna(1.0)

    bf_l5 = 3 * df["pitcher_avg_ip_l5"] + df["pitcher_avg_h_allowed_l5"] + df["pitcher_avg_bb_l5"]
    df["pitcher_est_bf_l5"] = bf_l5
    if "pitcher_avg_batters_faced_l5" not in df.columns:
        df["pitcher_avg_batters_faced_l5"] = bf_l5
    else:
        df["pitcher_avg_batters_faced_l5"] = df["pitcher_avg_batters_faced_l5"].fillna(bf_l5)
    bf_szn = 3 * df["pitcher_avg_ip_szn"] + df["pitcher_avg_h_allowed_l5"] + df["pitcher_avg_bb_l5"]
    if "pitcher_avg_batters_faced_szn" not in df.columns:
        df["pitcher_avg_batters_faced_szn"] = bf_szn
    else:
        df["pitcher_avg_batters_faced_szn"] = df["pitcher_avg_batters_faced_szn"].fillna(bf_szn).fillna(0.0)

    if "pitcher_avg_pitches_per_start_l5" not in df.columns:
        df["pitcher_avg_pitches_per_start_l5"] = df["pitcher_avg_pitches_thrown_l5"].where(
            df["pitcher_avg_pitches_thrown_l5"].gt(0), df["pitcher_avg_pitches_thrown_l3"]
        )
    df["pitcher_pitches_per_ip_l5"] = df["pitcher_avg_pitches_thrown_l3"].div(
        df["pitcher_avg_ip_l5"].replace(0, np.nan)
    ).fillna(0)
    if "pitcher_workload_spike_ratio" not in df.columns:
        df["pitcher_workload_spike_ratio"] = df["pitcher_pitch_count_last_start"].div(
            df["pitcher_avg_pitches_thrown_l5"].replace(0, np.nan)
        ).fillna(1.0)
    else:
        df["pitcher_workload_spike_ratio"] = df["pitcher_workload_spike_ratio"].fillna(1.0)
    if "pitcher_recent_pitch_count_trend" not in df.columns:
        df["pitcher_recent_pitch_count_trend"] = df["pitcher_avg_pitches_thrown_l3"].div(
            df["pitcher_avg_pitches_thrown_l5"].replace(0, np.nan)
        ).fillna(1.0)
    else:
        df["pitcher_recent_pitch_count_trend"] = df["pitcher_recent_pitch_count_trend"].fillna(1.0)
    if "rest_after_high_pitch_count" not in df.columns:
        df["rest_after_high_pitch_count"] = (df["pitcher_days_rest"].clip(upper=14) / 5.0) * (df["pitcher_pitch_count_last_start"] / 100.0)
    df["pitcher_workload_spike_ratio"] = df["pitcher_workload_spike_ratio"].clip(lower=0.0, upper=3.0)
    df["pitcher_recent_pitch_count_trend"] = df["pitcher_recent_pitch_count_trend"].clip(lower=0.0, upper=3.0)

    if "pitcher_max_ip_l5" not in df.columns:
        df["pitcher_max_ip_l5"] = df["pitcher_avg_ip_l5"]
    if "pitcher_median_ip_l5" not in df.columns:
        df["pitcher_median_ip_l5"] = df["pitcher_avg_ip_l5"]
    if "pitcher_ip_range_l5" not in df.columns:
        df["pitcher_ip_range_l5"] = (df["pitcher_max_ip_l5"] - df["pitcher_min_ip_l5"]).clip(lower=0)
    if "pitcher_short_start_rate_l5" not in df.columns:
        df["pitcher_short_start_rate_l5"] = 0.0
    if "pitcher_start_stability_l5" not in df.columns:
        df["pitcher_start_stability_l5"] = (df["pitcher_starts_szn"].clip(upper=5) / 5.0).fillna(0.0)
    for col in [
        "team_starter_avg_ip_l10",
        "team_starter_short_start_rate_l10",
        "team_starter_avg_pitches_l10",
        "team_starter_avg_ip_l30",
        "team_starter_short_hook_rate_l30",
        "manager_starter_short_hook_rate_l30",
        "team_starter_deep_start_rate_l30",
    ]:
        ensure_col(col, 0.0)

    df["bullpen_fatigue_pressure"] = (
        df["team_bullpen_ip_last_3d"].fillna(0) / 9.0
        + df["team_bullpen_pitches_last_3d"].fillna(0) / 150.0
    )
    if "opp_team_contact_rate_l10" not in df.columns:
        df["opp_team_contact_rate_l10"] = (1.0 - df["opp_team_whiff_pct_l10"]).clip(lower=0, upper=1).fillna(1.0)
    if "opp_team_chase_pct_l10" not in df.columns:
        df["opp_team_chase_pct_l10"] = 0.0
    if "opp_team_zone_contact_pct_l10" not in df.columns:
        df["opp_team_zone_contact_pct_l10"] = 0.0

    for col, default in LINEUP_FEATURE_DEFAULTS.items():
        ensure_col(col, default)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

    pitch_type_cols = ["pitcher_fastball_pct_l5", "pitcher_breaking_pct_l5", "pitcher_offspeed_pct_l5"]
    df["pitcher_num_pitch_types_l5"] = df[pitch_type_cols].fillna(0).gt(0.05).sum(axis=1)
    return df


def add_pitcher_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute pitcher interaction features that depend on matchup data."""
    df["pitcher_k_opp_k_interaction"] = (
        df["pitcher_avg_k_per_9_l5"].fillna(0) *
        df["opp_team_k_pct_l10"].fillna(0)
    )
    df["pitcher_whiff_opp_whiff_interaction"] = (
        df["pitcher_avg_whiff_pct_l5"].fillna(0) *
        df["opp_team_whiff_pct_l10"].fillna(0)
    )
    return df
