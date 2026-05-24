"""Tests for MLB feature contract ownership."""

from __future__ import annotations

from src.models.mlb import mlb_batter_feature_store as legacy_batter
from src.models.mlb import mlb_feature_store as legacy_pitcher
from src.models.mlb.features import contracts


def test_legacy_pitcher_contract_exports_are_identical_objects():
    assert legacy_pitcher.PITCHER_K_FEATURES is contracts.PITCHER_K_FEATURES
    assert legacy_pitcher.PITCHER_K_TRAINING_FEATURES is contracts.PITCHER_K_TRAINING_FEATURES
    assert legacy_pitcher.LINEUP_FEATURE_DEFAULTS is contracts.LINEUP_FEATURE_DEFAULTS


def test_legacy_batter_contract_exports_are_identical_objects():
    assert legacy_batter.BATTER_FEATURE_MAP is contracts.BATTER_FEATURE_MAP
    assert legacy_batter.BATTER_STAT_TARGET is contracts.BATTER_STAT_TARGET
    assert legacy_batter.BATTER_STAT_MARKET_KEY is contracts.BATTER_STAT_MARKET_KEY
    assert legacy_batter.get_features_for_stat("hits") == contracts.get_features_for_stat("hits")


def test_feature_lists_have_no_duplicates():
    assert contracts.find_duplicate_features(contracts.PITCHER_K_FEATURES) == []
    for stat, features in contracts.BATTER_FEATURE_MAP.items():
        assert contracts.find_duplicate_features(features) == [], stat


def test_each_batter_stat_includes_base_features():
    base = set(contracts.BATTER_BASE_FEATURES)
    for stat, features in contracts.BATTER_FEATURE_MAP.items():
        assert base.issubset(features), stat


def test_prop_line_features_have_market_key_or_explicit_hrr_exception():
    market_values = set(contracts.BATTER_STAT_MARKET_KEY.values()) | {"pitcher_strikeouts"}
    prop_features = [
        feature
        for features in contracts.BATTER_FEATURE_MAP.values()
        for feature in features
        if feature.startswith("prop_line_")
    ] + [feature for feature in contracts.PITCHER_K_FEATURES if feature.startswith("prop_line_")]
    for feature in prop_features:
        market = feature.removeprefix("prop_line_")
        assert market in market_values or market == "batter_hrr"


def test_feature_families_cover_all_contract_features():
    assert contracts.uncovered_features() == []
