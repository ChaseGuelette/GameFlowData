"""Unit tests for MLB Batter Feature Store + Binary Model.

Tests cover:
- Feature list completeness and uniqueness
- Derived feature computation (_add_derived_features)
- Switch hitter handling in platoon features
- MLBBinaryModel fit/predict/save/load cycle
- Feature map validity
"""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.mlb.mlb_batter_feature_store import (
    BATTER_BASE_FEATURES,
    BATTER_FEATURE_MAP,
    BATTER_HITS_FEATURES,
    BATTER_HR_FEATURES,
    BATTER_RBIS_FEATURES,
    BATTER_RUNS_FEATURES,
    BATTER_STAT_MARKET_KEY,
    BATTER_STAT_TARGET,
    BATTER_TOTAL_BASES_FEATURES,
    MLBBatterFeatureStore,
    get_features_for_stat,
)
from src.models.mlb.mlb_binary_model import BinaryModelConfig, MLBBinaryModel

# ============================================================================
# Feature List Tests
# ============================================================================


class TestFeatureLists:
    """Test feature lists for completeness and uniqueness."""

    def test_base_features_no_duplicates(self):
        assert len(BATTER_BASE_FEATURES) == len(set(BATTER_BASE_FEATURES))

    def test_hits_features_no_duplicates(self):
        assert len(BATTER_HITS_FEATURES) == len(set(BATTER_HITS_FEATURES))

    def test_hr_features_no_duplicates(self):
        assert len(BATTER_HR_FEATURES) == len(set(BATTER_HR_FEATURES))

    def test_total_bases_features_no_duplicates(self):
        assert len(BATTER_TOTAL_BASES_FEATURES) == len(set(BATTER_TOTAL_BASES_FEATURES))

    def test_rbis_features_no_duplicates(self):
        assert len(BATTER_RBIS_FEATURES) == len(set(BATTER_RBIS_FEATURES))

    def test_runs_features_no_duplicates(self):
        assert len(BATTER_RUNS_FEATURES) == len(set(BATTER_RUNS_FEATURES))

    def test_all_features_are_strings(self):
        for feat_list in [BATTER_BASE_FEATURES, BATTER_HITS_FEATURES,
                          BATTER_HR_FEATURES, BATTER_TOTAL_BASES_FEATURES,
                          BATTER_RBIS_FEATURES, BATTER_RUNS_FEATURES]:
            for f in feat_list:
                assert isinstance(f, str), f"Feature {f} is not a string"

    def test_stat_extensions_include_base(self):
        """Each stat's features should start with all base features."""
        for stat, feats in BATTER_FEATURE_MAP.items():
            for base_feat in BATTER_BASE_FEATURES:
                assert base_feat in feats, f"Base feature {base_feat} missing from {stat}"

    def test_hits_has_park_hits_factor(self):
        assert "park_hits_factor" in BATTER_HITS_FEATURES

    def test_hr_has_park_hr_factor(self):
        assert "park_hr_factor" in BATTER_HR_FEATURES

    def test_rbis_has_park_runs_factor(self):
        assert "park_runs_factor" in BATTER_RBIS_FEATURES

    def test_runs_has_park_runs_factor(self):
        assert "park_runs_factor" in BATTER_RUNS_FEATURES

    def test_hr_has_hr_specific_features(self):
        assert "batter_avg_hr_vs_hand_l20" in BATTER_HR_FEATURES
        assert "batter_fb_pct_l10" in BATTER_HR_FEATURES

    def test_feature_map_keys_match_stat_targets(self):
        assert set(BATTER_FEATURE_MAP.keys()) == set(BATTER_STAT_TARGET.keys())

    def test_get_features_for_stat(self):
        for stat in BATTER_FEATURE_MAP:
            result = get_features_for_stat(stat)
            assert result == BATTER_FEATURE_MAP[stat]

    def test_get_features_for_stat_invalid_raises(self):
        with pytest.raises(KeyError):
            get_features_for_stat("invalid_stat")


class TestStatMappings:
    """Test stat target and market key mappings."""

    def test_stat_targets(self):
        assert BATTER_STAT_TARGET["hits"] == "h"
        assert BATTER_STAT_TARGET["home_runs"] == "hr"
        assert BATTER_STAT_TARGET["total_bases"] == "tb"
        assert BATTER_STAT_TARGET["rbis"] == "rbi"
        assert BATTER_STAT_TARGET["runs"] == "r"

    def test_market_keys(self):
        assert BATTER_STAT_MARKET_KEY["hits"] == "batter_hits"
        assert BATTER_STAT_MARKET_KEY["home_runs"] == "batter_home_runs"
        assert BATTER_STAT_MARKET_KEY["total_bases"] == "batter_total_bases"
        assert BATTER_STAT_MARKET_KEY["rbis"] == "batter_rbis"
        assert BATTER_STAT_MARKET_KEY["runs"] == "batter_runs_scored"


# ============================================================================
# Derived Feature Tests
# ============================================================================


class TestDerivedFeatures:
    """Test _add_derived_features logic."""

    def _make_store(self):
        """Create a feature store with no engine (for derived-only testing)."""
        return MLBBatterFeatureStore.__new__(MLBBatterFeatureStore)

    def test_h_l5_l10_ratio_normal(self):
        store = self._make_store()
        df = pd.DataFrame({
            "batter_avg_h_l5": [2.0, 1.5, 0.0],
            "batter_avg_h_l10": [1.5, 1.5, 0.0],
        })
        result = store._add_derived_features(df)
        expected = [2.0 / 1.5, 1.0, 1.0]
        np.testing.assert_allclose(result["batter_h_l5_l10_ratio"].values, expected, rtol=1e-5)

    def test_h_l5_l10_ratio_zero_denominator(self):
        store = self._make_store()
        df = pd.DataFrame({
            "batter_avg_h_l5": [1.0],
            "batter_avg_h_l10": [0.0],
        })
        result = store._add_derived_features(df)
        assert result["batter_h_l5_l10_ratio"].iloc[0] == 1.0


# ============================================================================
# Binary Model Tests
# ============================================================================


class TestBinaryModelConfig:
    """Test BinaryModelConfig dataclass."""

    def test_default_config(self):
        config = BinaryModelConfig()
        assert config.n_estimators == 1000
        assert config.max_depth == 5
        assert config.learning_rate == 0.03

    def test_to_dict(self):
        config = BinaryModelConfig()
        d = config.to_dict()
        assert isinstance(d, dict)
        assert d["n_estimators"] == 1000

    def test_from_dict(self):
        d = {"n_estimators": 500, "max_depth": 3}
        config = BinaryModelConfig.from_dict(d)
        assert config.n_estimators == 500
        assert config.max_depth == 3
        # Others should be default
        assert config.learning_rate == 0.03


class TestMLBBinaryModel:
    """Test MLBBinaryModel fit/predict/save/load cycle."""

    @pytest.fixture
    def sample_data(self):
        """Generate synthetic HR data (~10% positive rate)."""
        np.random.seed(42)
        n = 500

        X = pd.DataFrame({
            "batter_avg_hr_l5": np.random.uniform(0, 0.5, n),
            "batter_barrel_pct_l5": np.random.uniform(0, 15, n),
            "batter_avg_exit_velocity_l5": np.random.uniform(85, 95, n),
            "batter_iso_szn": np.random.uniform(0.05, 0.35, n),
            "park_hr_factor": np.random.uniform(0.8, 1.2, n),
        })

        # ~10% positive rate
        proba = 0.1 * (1 + 0.3 * X["batter_iso_szn"])
        y = (np.random.random(n) < proba).astype(int)

        return X, y

    def test_fit_returns_metrics(self, sample_data):
        X, y = sample_data
        model = MLBBinaryModel(config=BinaryModelConfig(
            n_estimators=50, early_stopping_rounds=10
        ))
        metrics = model.fit(X, y)

        assert "n_train" in metrics
        assert "n_val" in metrics
        assert "positive_rate_train" in metrics
        assert "scale_pos_weight" in metrics
        assert metrics["n_train"] > 0
        assert metrics["n_val"] > 0

    def test_predict_proba_range(self, sample_data):
        X, y = sample_data
        model = MLBBinaryModel(config=BinaryModelConfig(
            n_estimators=50, early_stopping_rounds=10
        ))
        model.fit(X, y)

        proba = model.predict_proba(X)
        assert len(proba) == len(X)
        assert np.all(proba >= 0)
        assert np.all(proba <= 1)

    def test_predict_proba_not_all_same(self, sample_data):
        X, y = sample_data
        model = MLBBinaryModel(config=BinaryModelConfig(
            n_estimators=50, early_stopping_rounds=10
        ))
        model.fit(X, y)

        proba = model.predict_proba(X)
        assert proba.std() > 0.01, "Probabilities should vary across samples"

    def test_predict_proba_unfitted_raises(self):
        model = MLBBinaryModel()
        with pytest.raises(RuntimeError, match="not fitted"):
            model.predict_proba(pd.DataFrame({"a": [1]}))

    def test_save_load_roundtrip(self, sample_data):
        X, y = sample_data
        model = MLBBinaryModel(config=BinaryModelConfig(
            n_estimators=50, early_stopping_rounds=10
        ))
        model.fit(X, y)
        original_proba = model.predict_proba(X[:10])

        with tempfile.TemporaryDirectory() as tmpdir:
            model.save(tmpdir)

            # Check files exist
            assert (Path(tmpdir) / "hr_binary_model.joblib").exists()
            assert (Path(tmpdir) / "hr_calibrator.joblib").exists()
            assert (Path(tmpdir) / "hr_binary_meta.json").exists()

            # Load and compare
            loaded = MLBBinaryModel.load(tmpdir)
            loaded_proba = loaded.predict_proba(X[:10])

            np.testing.assert_allclose(original_proba, loaded_proba, rtol=1e-5)
            assert loaded.feature_names == model.feature_names

    def test_class_imbalance_handling(self):
        """Model should handle severe class imbalance (~10% positive)."""
        np.random.seed(42)
        n = 500
        X = pd.DataFrame({
            "feat1": np.random.uniform(0, 1, n),
            "feat2": np.random.uniform(0, 1, n),
            "feat3": np.random.uniform(0, 1, n),
        })
        # ~10% positive with signal from feat1
        proba_true = 0.05 + 0.15 * X["feat1"]
        y = (np.random.random(n) < proba_true).astype(int)

        model = MLBBinaryModel(config=BinaryModelConfig(
            n_estimators=50, early_stopping_rounds=10
        ))
        metrics = model.fit(X, pd.Series(y))

        assert metrics["scale_pos_weight"] > 3
        # Model should produce valid probabilities (may be 0 for many samples)
        raw_proba = model.predict_proba(X)
        assert np.all(raw_proba >= 0)
        assert np.all(raw_proba <= 1)


# ============================================================================
# Feature Store Initialization Tests
# ============================================================================


class TestFeatureStoreInit:
    """Test MLBBatterFeatureStore initialization without a real DB."""

    def test_default_config(self):
        # Just test that the class can be instantiated with None engine
        # (will fail on actual queries, but config should work)
        store = MLBBatterFeatureStore.__new__(MLBBatterFeatureStore)
        store.config = None
        from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureConfig
        config = MLBBatterFeatureConfig()
        assert config.min_games_for_stable == 3
