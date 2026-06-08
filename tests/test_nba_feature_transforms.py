"""Pure transform guards for NBA feature-store boundary migration."""

from __future__ import annotations

import pytest

from src.models.features.nba.transforms import (
    build_player_rolling_features,
    default_player_rolling_features,
    rest_schedule_features,
    safe_ratio,
    starter_probability,
)


def test_safe_ratio_preserves_legacy_default_for_zero_denominator():
    assert safe_ratio(6, 3) == pytest.approx(2.0)
    assert safe_ratio(6, 0) == pytest.approx(1.0)
    assert safe_ratio(None, 3) == pytest.approx(0.0)
    assert safe_ratio(6, None) == pytest.approx(1.0)


def test_starter_probability_caps_games_started_l5():
    assert starter_probability(None) == pytest.approx(0.0)
    assert starter_probability(0) == pytest.approx(0.0)
    assert starter_probability(3) == pytest.approx(0.6)
    assert starter_probability(8) == pytest.approx(1.0)


def test_rest_schedule_features_preserve_defaults_and_cap():
    assert rest_schedule_features(None, None) == {
        "rest_days": 3,
        "is_back_to_back": 0,
        "games_in_last_7_days": 2,
    }
    assert rest_schedule_features(1, 4) == {
        "rest_days": 1,
        "is_back_to_back": 1,
        "games_in_last_7_days": 4,
    }
    assert rest_schedule_features(10, 0)["rest_days"] == 7
    assert rest_schedule_features(2, 0)["games_in_last_7_days"] == 2


def test_default_player_rolling_features_match_legacy_fallbacks():
    defaults = default_player_rolling_features()
    assert defaults["player_avg_min_l5"] == 0
    assert defaults["player_avg_usg_pct_l5"] == pytest.approx(0.20)
    assert defaults["player_avg_ts_pct_l15"] == pytest.approx(0.56)
    assert defaults["player_pts_l3_l15_ratio"] == pytest.approx(1.0)
    assert defaults["player_min_l3_l5_ratio"] == pytest.approx(1.0)
    assert defaults["player_starter_prob"] == pytest.approx(0.0)
    assert defaults["rest_days"] == 3
    assert defaults["is_back_to_back"] == 0
    assert defaults["games_in_last_7_days"] == 2


def test_build_player_rolling_features_maps_values_and_artifact_compatible_ratios():
    row = {
        "avg_min_l5": 12,
        "avg_min_l15": 18,
        "avg_pts_l5": 10,
        "avg_pts_l15": 14,
        "avg_reb_l5": 5,
        "avg_ast_l5": 3,
        "avg_fg3m_l5": 2,
        "avg_fg3a_l5": 5,
        "avg_usg_pct_l5": 0.2,
        "avg_ts_pct_l15": 0.55,
        "avg_reb_pct_l5": 0.1,
        "avg_ast_pct_l5": 0.15,
        "avg_min_l3": 15,
        "avg_pts_l3": 12,
        "avg_reb_l3": 6,
        "avg_ast_l3": 4,
        "avg_fg3m_l3": 3,
        "avg_min_szn": 21,
        "std_min_l5": 2.5,
        "std_pts_l5": 3.0,
        "std_reb_l5": 1.5,
        "std_ast_l5": 1.0,
        "std_fg3m_l5": 0.8,
        "min_floor_l5": 10,
        "games_started_l5": 4,
        "stored_rest_days": 2,
        "games_last_7d": 3,
    }
    features = build_player_rolling_features(row)
    assert features["player_avg_min_l5"] == 12
    assert features["player_avg_min_szn"] == 21
    assert features["player_min_std_l5"] == pytest.approx(2.5)
    assert features["player_starter_prob"] == pytest.approx(0.8)
    # Historical names are kept for artifact compatibility: PTS uses L15;
    # REB/AST/THREES use L5 despite the *_l3_l15_ratio names.
    assert features["player_pts_l3_l15_ratio"] == pytest.approx(12 / 14)
    assert features["player_reb_l3_l15_ratio"] == pytest.approx(6 / 5)
    assert features["player_ast_l3_l15_ratio"] == pytest.approx(4 / 3)
    assert features["player_fg3m_l3_l15_ratio"] == pytest.approx(3 / 2)
    assert features["player_min_l3_l5_ratio"] == pytest.approx(15 / 12)
    assert features["player_min_l3_l15_ratio"] == pytest.approx(15 / 18)
    assert features["rest_days"] == 2
    assert features["is_back_to_back"] == 0
    assert features["games_in_last_7_days"] == 3
