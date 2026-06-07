"""Thin compatibility facade for the MLB pitcher feature store.

The implementation remains in ``features.legacy_pitcher_feature_store`` while
callers finish migrating to explicit training/inference loaders. Keep this file
small: new source SQL and feature-family helpers belong in ``src.models.mlb.features``.
"""

from __future__ import annotations

from src.models.mlb.features.contracts import (
    LINEUP_FEATURE_DEFAULTS,
    PITCHER_K_EXCLUDED_TRAINING_FEATURES,
    PITCHER_K_FEATURES,
    PITCHER_K_PHASE3A_REJECTED_FEATURES,
    PITCHER_K_PHASE3B_ADDED_FEATURES,
    PITCHER_K_TRAINING_FEATURES,
)
from src.models.mlb.features.legacy_pitcher_feature_store import (
    MLBFeatureConfig,
)
from src.models.mlb.features.legacy_pitcher_feature_store import (
    MLBFeatureStore as _LegacyMLBFeatureStore,
)

__all__ = [
    "LINEUP_FEATURE_DEFAULTS",
    "MLBFeatureConfig",
    "MLBFeatureStore",
    "PITCHER_K_EXCLUDED_TRAINING_FEATURES",
    "PITCHER_K_FEATURES",
    "PITCHER_K_PHASE3A_REJECTED_FEATURES",
    "PITCHER_K_PHASE3B_ADDED_FEATURES",
    "PITCHER_K_TRAINING_FEATURES",
]


class MLBFeatureStore(_LegacyMLBFeatureStore):
    """Compatibility adapter for legacy imports of the pitcher feature store."""
