"""Contract guards for NBA feature-store boundary migration."""

from __future__ import annotations

import inspect

import pytest

import src.models.feature_store as legacy_feature_store
from src.models.features.nba import contracts

LEGACY_FEATURE_LIST_NAMES = [
    "MINUTES_FEATURES",
    "RATE_FEATURES_PTS",
    "RATE_FEATURES_REB",
    "RATE_FEATURES_AST",
    "RATE_FEATURES_THREES",
]


def test_nba_contract_lists_match_legacy_re_exports():
    for name in LEGACY_FEATURE_LIST_NAMES:
        assert getattr(contracts, name) == getattr(legacy_feature_store, name)
        assert getattr(contracts, name) is getattr(legacy_feature_store, name)


def test_nba_contract_lists_have_no_duplicates():
    for name in LEGACY_FEATURE_LIST_NAMES:
        features = getattr(contracts, name)
        assert len(features) == len(set(features)), f"{name} contains duplicate features"


def test_rate_feature_mapping_preserves_current_stat_lists():
    assert contracts.RATE_FEATURES_BY_STAT == {
        "pts": contracts.RATE_FEATURES_PTS,
        "reb": contracts.RATE_FEATURES_REB,
        "ast": contracts.RATE_FEATURES_AST,
        "threes": contracts.RATE_FEATURES_THREES,
    }


@pytest.mark.parametrize("feature", sorted({f for name in LEGACY_FEATURE_LIST_NAMES for f in getattr(contracts, name)}))
def test_every_nba_feature_has_family_metadata(feature: str):
    family = contracts.feature_family(feature)
    assert family in contracts.NBA_FEATURE_FAMILIES


def test_feature_store_re_exports_contracts_instead_of_owning_feature_lists():
    source = inspect.getsource(legacy_feature_store)
    assert "from src.models.features.nba.contracts import" in source
    assert "MINUTES_FEATURES = [" not in source
    assert "RATE_FEATURES_PTS = [" not in source
    assert "RATE_FEATURES_REB = [" not in source
    assert "RATE_FEATURES_AST = [" not in source
    assert "RATE_FEATURES_THREES = [" not in source
