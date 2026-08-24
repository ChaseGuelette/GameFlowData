"""Thin compatibility facade for the MLB batter feature store.

The implementation remains in ``features.legacy_batter_feature_store`` while
callers finish migrating to explicit training/inference loaders. Keep this file
small: new source SQL and feature-family helpers belong in ``src.models.mlb.features``.
"""

from __future__ import annotations

from src.models.mlb.features.contracts import (
    BATTER_BASE_FEATURES,
    BATTER_FEATURE_MAP,
    BATTER_HITS_FEATURES,
    BATTER_HR_FEATURES,
    BATTER_RBIS_FEATURES,
    BATTER_RUNS_FEATURES,
    BATTER_STAT_MARKET_KEY,
    BATTER_STAT_TARGET,
    BATTER_TOTAL_BASES_FEATURES,
    get_features_for_stat,
)
from src.models.mlb.features.legacy_batter_feature_store import (
    MLBBatterFeatureConfig,
)
from src.models.mlb.features.legacy_batter_feature_store import (
    MLBBatterFeatureStore as _LegacyMLBBatterFeatureStore,
)

__all__ = [
    "BATTER_BASE_FEATURES",
    "BATTER_FEATURE_MAP",
    "BATTER_HITS_FEATURES",
    "BATTER_HR_FEATURES",
    "BATTER_RBIS_FEATURES",
    "BATTER_RUNS_FEATURES",
    "BATTER_STAT_MARKET_KEY",
    "BATTER_STAT_TARGET",
    "BATTER_TOTAL_BASES_FEATURES",
    "MLBBatterFeatureConfig",
    "MLBBatterFeatureStore",
    "get_features_for_stat",
]


class MLBBatterFeatureStore(_LegacyMLBBatterFeatureStore):
    """Compatibility adapter for legacy imports of the batter feature store."""
