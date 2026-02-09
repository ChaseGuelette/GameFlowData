"""
Unit tests for TruncatedNegBinModel.

Tests cover:
- Model fitting and parameter estimation
- Prediction in valid ranges
- Sampling produces integers >= 1
- Save/load functionality
- Edge case handling
"""

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from scipy.stats import nbinom
import tempfile

from src.models.truncated_negbin import TruncatedNegBinModel, TruncatedNegBinConfig


class TestTruncatedNegBinConfig:
    """Test configuration dataclass."""

    def test_default_config(self):
        config = TruncatedNegBinConfig()
        assert config.mu_n_estimators == 1000
        assert config.mu_max_depth == 5
        assert config.alpha_n_estimators == 500
        assert config.log_mu_min == -2.0
        assert config.log_mu_max == 3.0

    def test_custom_config(self):
        config = TruncatedNegBinConfig(
            mu_n_estimators=500,
            mu_max_depth=3,
        )
        assert config.mu_n_estimators == 500
        assert config.mu_max_depth == 3


class TestTruncatedNegBinModel:
    """Test TruncatedNegBinModel class."""

    @pytest.fixture
    def sample_data(self):
        """Generate simulated positive-only threes data."""
        np.random.seed(42)
        n_samples = 1000

        # Features
        X = pd.DataFrame({
            'player_avg_fg3a_l5': np.random.uniform(2, 8, n_samples),
            'player_avg_fg3_pct_l5': np.random.uniform(0.3, 0.45, n_samples),
            'player_avg_min_l5': np.random.uniform(20, 38, n_samples),
            'opp_avg_fg3_pct_allowed_l5': np.random.uniform(0.32, 0.40, n_samples),
        })

        # Generate target from truncated NegBin
        mu_true = 0.3 * X['player_avg_fg3a_l5'] + 0.1 * X['player_avg_min_l5'] / 30
        alpha_true = 0.3

        # Sample from NegBin and truncate zeros
        n = 1.0 / alpha_true
        p = n / (n + mu_true)
        y_full = np.array([nbinom.rvs(n, pp) for pp in p])

        # Keep only positive samples
        positive_mask = y_full > 0
        X_positive = X[positive_mask].reset_index(drop=True)
        y_positive = pd.Series(y_full[positive_mask])

        return X_positive, y_positive

    def test_fit_converges(self, sample_data):
        """Test that model fitting converges."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        result = model.fit(X, y)

        assert 'global_mu' in result
        assert 'global_alpha' in result
        assert result['global_mu'] > 0
        assert result['global_alpha'] > 0
        assert model.mu_model is not None
        assert model.alpha_model is not None

    def test_predict_params_in_valid_range(self, sample_data):
        """Test that predicted params are in valid ranges."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        mu, alpha = model.predict_params(X)

        # mu should be > 0 (exp of clamped log)
        assert (mu > 0).all()
        assert (mu <= 20).all()  # exp(3) ~ 20

        # alpha should be > 0
        assert (alpha > 0).all()
        assert (alpha <= 8).all()  # exp(2) ~ 7.4

    def test_sample_produces_integers(self, sample_data):
        """Test that sampling produces integer counts >= 1."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Sample for first row
        samples = model.sample(X.iloc[[0]], n_samples=1000)

        # Check all samples are integers >= 1
        assert samples.dtype in [np.int32, np.int64, int]
        assert (samples >= 1).all()

        # Check reasonable range
        assert samples.max() <= 20  # Very unlikely to exceed 20 threes

    def test_sample_batch(self, sample_data):
        """Test batch sampling."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Sample for multiple rows
        samples = model.sample(X.iloc[:10], n_samples=500)

        assert samples.shape == (10, 500)
        assert (samples >= 1).all()

    def test_sample_distribution_reasonable(self, sample_data):
        """Test that sampled distribution has reasonable statistics."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Sample many times for first high-volume row
        high_vol_idx = X['player_avg_fg3a_l5'].idxmax()
        samples = model.sample(X.iloc[[high_vol_idx]], n_samples=10000).flatten()

        # Mean should be reasonable (1-5 for typical 3PT shooter)
        assert 0.5 < samples.mean() < 10

        # Variance should show overdispersion
        assert samples.var() > 0

    def test_save_load_roundtrip(self, sample_data):
        """Test save and load functionality."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Get predictions before save
        mu_before, alpha_before = model.predict_params(X.iloc[:5])

        with tempfile.TemporaryDirectory() as tmpdir:
            # Save
            model.save(Path(tmpdir))

            # Load
            loaded = TruncatedNegBinModel.load(Path(tmpdir))

            # Get predictions after load
            mu_after, alpha_after = loaded.predict_params(X.iloc[:5])

            # Should be identical
            np.testing.assert_array_almost_equal(mu_before, mu_after)
            np.testing.assert_array_almost_equal(alpha_before, alpha_after)

            # Metadata preserved
            assert loaded.feature_names == model.feature_names
            assert loaded._global_mu == model._global_mu
            assert loaded._global_alpha == model._global_alpha

    def test_exists_check(self, sample_data):
        """Test exists() static method."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Before save
            assert not TruncatedNegBinModel.exists(tmppath)

            # After save
            model.save(tmppath)
            assert TruncatedNegBinModel.exists(tmppath)

    def test_missing_columns_handled(self, sample_data):
        """Test that missing columns are filled with zeros."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Predict with subset of features
        X_partial = X[['player_avg_fg3a_l5', 'player_avg_min_l5']]
        mu, alpha = model.predict_params(X_partial)

        # Should not raise, and produce valid outputs
        assert (mu > 0).all()
        assert (alpha > 0).all()

    def test_sample_single_convenience(self, sample_data):
        """Test sample_single convenience method."""
        X, y = sample_data

        model = TruncatedNegBinModel()
        model.fit(X, y)

        # Sample using dict interface
        features = X.iloc[0].to_dict()
        samples = model.sample_single(features, n_samples=100)

        assert samples.shape == (100,)
        assert (samples >= 1).all()


class TestTruncatedNegBinInvCDF:
    """Test inverse CDF sampling logic."""

    def test_invcdf_boundary_values(self):
        """Test inverse CDF at boundary u values."""
        model = TruncatedNegBinModel()
        model._global_mu = 2.0
        model._global_alpha = 0.3

        # Very low u should give 1
        low_u = np.array([0.001])
        samples = model._truncated_negbin_invcdf(low_u, 2.0, 0.3)
        assert samples[0] >= 1

        # High u should give larger values
        high_u = np.array([0.99])
        samples = model._truncated_negbin_invcdf(high_u, 2.0, 0.3)
        assert samples[0] >= 1

    def test_invcdf_uniform_coverage(self):
        """Test that uniform samples cover the distribution well."""
        model = TruncatedNegBinModel()

        u = np.linspace(0.01, 0.99, 100)
        samples = model._truncated_negbin_invcdf(u, 2.0, 0.3)

        # Should have multiple unique values
        unique_vals = np.unique(samples)
        assert len(unique_vals) >= 5

        # All should be positive
        assert (samples >= 1).all()


class TestTruncatedNegBinEdgeCases:
    """Test edge cases and error handling."""

    def test_zero_values_rejected(self):
        """Test that y values < 1 raise an error."""
        X = pd.DataFrame({'feat1': [1, 2, 3, 4, 5]})
        y = pd.Series([0, 1, 2, 1, 3])  # Contains a zero

        model = TruncatedNegBinModel()
        with pytest.raises(ValueError, match="y >= 1"):
            model.fit(X, y)

    def test_unfitted_model_raises(self):
        """Test that predict_params raises on unfitted model."""
        model = TruncatedNegBinModel()
        X = pd.DataFrame({'feat1': [1, 2, 3]})

        with pytest.raises(ValueError, match="not fitted"):
            model.predict_params(X)

    def test_very_small_alpha(self):
        """Test with very small alpha (near-Poisson)."""
        np.random.seed(42)

        # Generate near-Poisson data (low overdispersion)
        X = pd.DataFrame({
            'feat1': np.random.uniform(0, 1, 200),
            'feat2': np.random.uniform(0, 1, 200),
        })

        # Poisson-like with mean 2
        y = pd.Series(np.random.poisson(2, 200))
        y = y[y > 0].reset_index(drop=True)
        X = X.iloc[:len(y)].reset_index(drop=True)

        model = TruncatedNegBinModel()
        result = model.fit(X, y)

        # Alpha should still be > 0 (clamped)
        assert result['global_alpha'] > 0

    def test_very_large_values(self):
        """Test with some large count values."""
        np.random.seed(42)

        X = pd.DataFrame({
            'feat1': np.random.uniform(0, 1, 100),
        })

        # Mix of typical and large values
        y = pd.Series([1, 2, 3, 2, 1, 4, 3, 2, 10, 12] * 10)

        model = TruncatedNegBinModel()
        model.fit(X, y)

        samples = model.sample(X.iloc[[0]], n_samples=100)
        assert (samples >= 1).all()
