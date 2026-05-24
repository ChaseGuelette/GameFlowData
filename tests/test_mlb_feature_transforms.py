"""Tests for pure MLB feature transforms."""

from __future__ import annotations

import pandas as pd

from src.models.mlb.features.transforms import (
    add_batter_derived_features,
    add_batter_interaction_features,
    add_pitcher_derived_features,
)


def test_batter_derived_features_compute_ratio_and_projected_ab_without_engine():
    df = pd.DataFrame({
        "batter_avg_h_l5": [2.0, 1.0],
        "batter_avg_h_l10": [1.0, 0.0],
        "batter_avg_ab_l5": [4.0, None],
        "lineup_position": [1, 99],
    })

    result = add_batter_derived_features(df)

    assert result["batter_h_l5_l10_ratio"].tolist() == [2.0, 1.0]
    assert result["projected_ab"].round(2).tolist() == [4.15, 3.5]
    assert "_position_pa" not in result.columns


def test_batter_interaction_features_preserve_defaults():
    df = pd.DataFrame({
        "batter_babip_szn": [None, 0.320],
        "opp_pitcher_babip_against_l5": [None, 0.250],
        "projected_ab": [4.0, None],
        "batter_avg_h_l10": [1.2, 2.0],
    })

    result = add_batter_interaction_features(df)

    assert result["batter_babip_opp_babip_interaction"].round(4).tolist() == [0.09, 0.08]
    assert result["projected_ab_x_recent_form"].tolist() == [4.8, 0.0]


def test_pitcher_derived_features_preserve_key_ratios_and_defaults():
    df = pd.DataFrame({
        "pitcher_avg_so_l3": [6.0],
        "pitcher_avg_so_l5": [3.0],
        "pitcher_avg_ip_l5": [5.0],
        "pitcher_avg_ip_szn": [5.5],
        "pitcher_min_ip_l5": [4.0],
        "pitcher_avg_h_allowed_l5": [4.0],
        "pitcher_avg_bb_l5": [2.0],
        "pitcher_avg_pitches_thrown_l3": [90.0],
        "pitcher_avg_pitches_thrown_l5": [80.0],
        "pitcher_pitch_count_last_start": [100.0],
        "pitcher_days_rest": [5.0],
        "pitcher_starts_szn": [10.0],
        "team_bullpen_ip_last_3d": [9.0],
        "team_bullpen_pitches_last_3d": [150.0],
        "opp_team_whiff_pct_l10": [0.25],
        "pitcher_fastball_pct_l5": [0.6],
        "pitcher_breaking_pct_l5": [0.3],
        "pitcher_offspeed_pct_l5": [0.1],
    })

    result = add_pitcher_derived_features(df)

    assert result.loc[0, "pitcher_so_l3_l5_ratio"] == 2.0
    assert result.loc[0, "pitcher_est_bf_l5"] == 21.0
    assert result.loc[0, "pitcher_pitches_per_ip_l5"] == 18.0
    assert result.loc[0, "bullpen_fatigue_pressure"] == 2.0
    assert result.loc[0, "pitcher_num_pitch_types_l5"] == 3
