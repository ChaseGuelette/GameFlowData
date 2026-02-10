"""Tests for ThreesMulticlassModel."""

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
import tempfile

from src.models.threes_multiclass import (
    ThreesMulticlassModel,
    ThreesMulticlassConfig,
)


@pytest.fixture
def sample_data():
    """Generate sample training data with realistic threes distribution."""
    np.random.seed(42)
    n_samples = 1000

    # Features
    X = pd.DataFrame({
        'player_avg_fg3m_l5': np.random.uniform(0, 5, n_samples),
        'player_avg_min_l5': np.random.uniform(15, 35, n_samples),
        'prop_line_threes': np.random.uniform(1, 4, n_samples),
        'opp_pos_threes_allowed_l5': np.random.uniform(8, 14, n_samples),
    })

    # Generate threes counts with realistic distribution
    # ~35% zeros, declining frequency for higher counts
    probs = np.array([0.35, 0.25, 0.18, 0.12, 0.06, 0.025, 0.01, 0.004, 0.001])
    probs = probs / probs.sum()
    y = np.random.choice(9, size=n_samples, p=probs)

    # Ensure all 9 classes are present (XGBoost requires this for multiclass)
    # Add at least 5 samples of each class to guarantee they appear in training
    for cls in range(9):
        if (y == cls).sum() < 5:
            # Replace some random samples with this class
            indices = np.random.choice(n_samples, size=5, replace=False)
            y[indices] = cls

    return X, y


@pytest.fixture
def trained_model(sample_data):
    """Return a trained model for testing."""
    X, y = sample_data
    config = ThreesMulticlassConfig(
        n_estimators=50,  # Fewer trees for faster tests
        max_depth=3,
        early_stopping_rounds=10,
    )
    model = ThreesMulticlassModel(config)
    model.fit(X, y)
    return model


class TestThreesMulticlassConfig:
    """Tests for configuration dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ThreesMulticlassConfig()
        assert config.n_estimators == 1000
        assert config.max_depth == 5
        assert config.learning_rate == 0.03
        assert config.num_classes == 9
        assert config.val_size == 0.15

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ThreesMulticlassConfig(
            n_estimators=500,
            max_depth=4,
            num_classes=10,
        )
        assert config.n_estimators == 500
        assert config.max_depth == 4
        assert config.num_classes == 10


class TestThreesMulticlassModel:
    """Tests for the multiclass model."""

    def test_fit_returns_info(self, sample_data):
        """Test that fit() returns training info dict."""
        X, y = sample_data
        config = ThreesMulticlassConfig(n_estimators=20, max_depth=2)
        model = ThreesMulticlassModel(config)

        info = model.fit(X, y)

        assert 'n_samples' in info
        assert 'n_features' in info
        assert 'val_accuracy' in info
        assert 'class_distribution' in info
        assert info['n_samples'] == len(X)
        assert info['n_features'] == len(X.columns)

    def test_fit_stores_feature_names(self, sample_data):
        """Test that feature names are stored after fitting."""
        X, y = sample_data
        model = ThreesMulticlassModel(ThreesMulticlassConfig(n_estimators=20))
        model.fit(X, y)

        assert model.feature_names == list(X.columns)

    def test_predict_proba_shape(self, trained_model, sample_data):
        """Test that predict_proba returns correct shape."""
        X, _ = sample_data
        probs = trained_model.predict_proba(X[:10])

        assert probs.shape == (10, 9)

    def test_predict_proba_sums_to_one(self, trained_model, sample_data):
        """Test that probabilities sum to 1 for each sample."""
        X, _ = sample_data
        probs = trained_model.predict_proba(X[:10])

        np.testing.assert_array_almost_equal(probs.sum(axis=1), np.ones(10))

    def test_predict_proba_non_negative(self, trained_model, sample_data):
        """Test that all probabilities are non-negative."""
        X, _ = sample_data
        probs = trained_model.predict_proba(X[:10])

        assert (probs >= 0).all()

    def test_sample_returns_integers(self, trained_model, sample_data):
        """Test that sample() returns integer counts."""
        X, _ = sample_data
        samples = trained_model.sample(X[:1], n_samples=100)

        assert samples.dtype in [np.int32, np.int64, int]
        assert (samples >= 0).all()
        assert (samples <= 8).all()  # Max class is 8

    def test_sample_single_row_shape(self, trained_model, sample_data):
        """Test sample shape for single row input."""
        X, _ = sample_data
        samples = trained_model.sample(X[:1], n_samples=100)

        assert samples.shape == (100,)

    def test_sample_batch_shape(self, trained_model, sample_data):
        """Test sample shape for batch input."""
        X, _ = sample_data
        samples = trained_model.sample(X[:5], n_samples=100)

        assert samples.shape == (5, 100)

    def test_sample_single_from_dict(self, trained_model):
        """Test sample_single with feature dict."""
        features = {
            'player_avg_fg3m_l5': 2.5,
            'player_avg_min_l5': 30.0,
            'prop_line_threes': 2.5,
            'opp_pos_threes_allowed_l5': 11.0,
        }
        samples = trained_model.sample_single(features, n_samples=100)

        assert samples.shape == (100,)
        assert (samples >= 0).all()
        assert (samples <= 8).all()

    def test_prob_over_range(self, trained_model, sample_data):
        """Test that prob_over returns values in [0, 1]."""
        X, _ = sample_data
        prob = trained_model.prob_over(X[:10], line=2.5)

        assert prob.shape == (10,)
        assert (prob >= 0).all()
        assert (prob <= 1).all()

    def test_prob_over_threshold_logic(self, trained_model, sample_data):
        """Test prob_over threshold: over 2.5 means >= 3."""
        X, _ = sample_data
        probs = trained_model.predict_proba(X[:1])
        prob_over_2_5 = trained_model.prob_over(X[:1], line=2.5)

        # Over 2.5 means classes 3, 4, 5, 6, 7, 8
        expected = probs[0, 3:].sum()
        np.testing.assert_almost_equal(prob_over_2_5[0], expected)

    def test_prob_under_range(self, trained_model, sample_data):
        """Test that prob_under returns values in [0, 1]."""
        X, _ = sample_data
        prob = trained_model.prob_under(X[:10], line=2.5)

        assert prob.shape == (10,)
        assert (prob >= 0).all()
        assert (prob <= 1).all()

    def test_prob_under_threshold_logic(self, trained_model, sample_data):
        """Test prob_under threshold: under 2.5 means <= 2."""
        X, _ = sample_data
        probs = trained_model.predict_proba(X[:1])
        prob_under_2_5 = trained_model.prob_under(X[:1], line=2.5)

        # Under 2.5 means classes 0, 1, 2
        expected = probs[0, :3].sum()
        np.testing.assert_almost_equal(prob_under_2_5[0], expected)

    def test_prob_over_plus_under_equals_one(self, trained_model, sample_data):
        """Test that P(over) + P(under) = 1 for half-point lines."""
        X, _ = sample_data
        prob_over = trained_model.prob_over(X[:10], line=2.5)
        prob_under = trained_model.prob_under(X[:10], line=2.5)

        # For line 2.5: over means >=3, under means <=2
        # These are mutually exclusive and exhaustive
        np.testing.assert_array_almost_equal(prob_over + prob_under, np.ones(10))

    def test_get_quantiles_shape(self, trained_model, sample_data):
        """Test get_quantiles returns correct shape."""
        X, _ = sample_data
        quantiles = trained_model.get_quantiles(X[:10])

        assert quantiles.shape == (10, 5)  # 5 default quantiles

    def test_get_quantiles_monotonic(self, trained_model, sample_data):
        """Test that quantiles are monotonically increasing."""
        X, _ = sample_data
        quantiles = trained_model.get_quantiles(X[:10])

        # Each row should be monotonically non-decreasing
        for row in quantiles:
            assert all(row[i] <= row[i + 1] for i in range(len(row) - 1))

    def test_get_quantiles_custom(self, trained_model, sample_data):
        """Test get_quantiles with custom quantile levels."""
        X, _ = sample_data
        quantiles = trained_model.get_quantiles(X[:10], quantiles=[0.5])

        assert quantiles.shape == (10, 1)

    def test_high_line_prob_over_zero(self, trained_model, sample_data):
        """Test that prob_over for very high line is near zero."""
        X, _ = sample_data
        prob = trained_model.prob_over(X[:10], line=8.5)

        # Over 8.5 is impossible in our model (max class is 8)
        np.testing.assert_array_equal(prob, np.zeros(10))


class TestThreesMulticlassModelPersistence:
    """Tests for model save/load functionality."""

    def test_save_creates_files(self, trained_model):
        """Test that save creates expected files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            trained_model.save(path)

            assert (path / "threes_multiclass_model.joblib").exists()
            assert (path / "threes_multiclass_meta.json").exists()

    def test_load_restores_model(self, trained_model, sample_data):
        """Test that load restores a working model."""
        X, _ = sample_data

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            trained_model.save(path)

            loaded_model = ThreesMulticlassModel.load(path)

            # Compare predictions
            orig_probs = trained_model.predict_proba(X[:5])
            loaded_probs = loaded_model.predict_proba(X[:5])

            np.testing.assert_array_almost_equal(orig_probs, loaded_probs)

    def test_load_restores_feature_names(self, trained_model):
        """Test that load restores feature names."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            trained_model.save(path)

            loaded_model = ThreesMulticlassModel.load(path)

            assert loaded_model.feature_names == trained_model.feature_names

    def test_exists_returns_true(self, trained_model):
        """Test exists() returns True when model is saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            trained_model.save(path)

            assert ThreesMulticlassModel.exists(path)

    def test_exists_returns_false(self):
        """Test exists() returns False for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            assert not ThreesMulticlassModel.exists(path)


class TestThreesMulticlassModelEdgeCases:
    """Tests for edge cases and error handling."""

    def test_unfitted_model_raises_error(self, sample_data):
        """Test that predict_proba raises error for unfitted model."""
        X, _ = sample_data
        model = ThreesMulticlassModel()

        with pytest.raises(ValueError, match="Model not fitted"):
            model.predict_proba(X[:1])

    def test_missing_features_handled(self, trained_model):
        """Test that missing features are filled with zeros."""
        # Create data with missing column
        X_partial = pd.DataFrame({
            'player_avg_fg3m_l5': [2.5],
            'player_avg_min_l5': [30.0],
            # Missing: prop_line_threes, opp_pos_threes_allowed_l5
        })

        # Should not raise, should fill missing with 0
        probs = trained_model.predict_proba(X_partial)
        assert probs.shape == (1, 9)

    def test_binning_high_values(self, sample_data):
        """Test that values >= 8 are binned to class 8."""
        X, _ = sample_data
        # Create target with all classes 0-7 present, plus high values that bin to 8
        # XGBoost requires seeing all classes during training
        # Classes 0-7: one each = 8 samples
        # High values (10, 15, 20, 8, 9, 12, 11): 7 samples that all bin to class 8
        y_mixed = np.array([0, 1, 2, 3, 4, 5, 6, 7, 10, 15, 20, 8, 9, 12, 11])

        config = ThreesMulticlassConfig(n_estimators=20, early_stopping_rounds=5)
        model = ThreesMulticlassModel(config)

        # Use subset of features matching target length
        X_small = X.iloc[:15].copy()
        info = model.fit(X_small, y_mixed)

        # 7 values (10, 15, 20, 8, 9, 12, 11) all bin to class 8 = 7/15 ≈ 0.47
        assert info['class_distribution'][8] >= 0.4

    def test_reproducibility_with_seed(self, sample_data):
        """Test that results are reproducible with same seed."""
        X, y = sample_data
        config = ThreesMulticlassConfig(n_estimators=20, random_state=42)

        model1 = ThreesMulticlassModel(config)
        model1.fit(X, y)
        probs1 = model1.predict_proba(X[:5])

        model2 = ThreesMulticlassModel(config)
        model2.fit(X, y)
        probs2 = model2.predict_proba(X[:5])

        np.testing.assert_array_almost_equal(probs1, probs2)

    def test_sample_reproducibility(self, trained_model, sample_data):
        """Test that sampling is reproducible with same rng."""
        X, _ = sample_data

        rng1 = np.random.default_rng(42)
        samples1 = trained_model.sample(X[:1], n_samples=100, rng=rng1)

        rng2 = np.random.default_rng(42)
        samples2 = trained_model.sample(X[:1], n_samples=100, rng=rng2)

        np.testing.assert_array_equal(samples1, samples2)
