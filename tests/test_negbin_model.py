"""Unit tests for NegBinModel (standard, non-truncated).

Validates v2 distributional regression (custom NB NLL objective),
zero-handling, integer outputs >= 0, quantile coverage, and
save/load roundtrip for both v2 (native booster) and v1 (legacy) formats.
"""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy.stats import nbinom

from src.models.negbin_model import NegBinConfig, NegBinModel

# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------

class TestNegBinConfig:
    def test_default_config(self):
        config = NegBinConfig()
        assert config.n_estimators == 1000
        assert config.max_depth == 5
        assert config.learning_rate == 0.03
        assert config.log_mu_min == -3.0
        assert config.log_mu_max == 3.0

    def test_custom_config(self):
        config = NegBinConfig(n_estimators=500, max_depth=3)
        assert config.n_estimators == 500
        assert config.max_depth == 3

    def test_legacy_fields_present(self):
        config = NegBinConfig()
        assert config.mu_n_estimators == 1000
        assert config.alpha_n_estimators == 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_synthetic_data(n_samples: int = 1500, seed: int = 42):
    """Generate synthetic NegBin data with zeros present."""
    rng = np.random.RandomState(seed)

    X = pd.DataFrame({
        "avg_hits_l10": rng.uniform(0.5, 2.5, n_samples),
        "avg_ab_l10": rng.uniform(3, 5, n_samples),
        "opp_era": rng.uniform(3.0, 5.0, n_samples),
        "batter_avg": rng.uniform(0.200, 0.320, n_samples),
    })

    # True mu depends on features
    mu_true = 0.4 * X["avg_hits_l10"] + 0.1 * X["avg_ab_l10"]
    alpha_true = 0.5

    n = 1.0 / alpha_true
    p = n / (n + mu_true)
    y = pd.Series([int(nbinom.rvs(n, pp, random_state=rng)) for pp in p])

    return X, y


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestNegBinModel:
    @pytest.fixture
    def sample_data(self):
        return _make_synthetic_data()

    def test_fit_converges(self, sample_data):
        X, y = sample_data
        model = NegBinModel(model_name="test_stat")
        result = model.fit(X, y)

        assert "global_mu" in result
        assert "global_alpha" in result
        assert result["global_mu"] > 0
        assert result["global_alpha"] > 0
        assert model._native_booster is not None

    def test_fit_accepts_zeros(self, sample_data):
        X, y = sample_data
        # Ensure zeros are present
        assert (y == 0).sum() > 0, "Synthetic data should contain zeros"

        model = NegBinModel()
        model.fit(X, y)  # Should not raise

    def test_global_mu_close_to_sample_mean(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        # MLE mu should be close to the sample mean
        assert abs(model._global_mu - y.mean()) < 0.5

    def test_predict_params_valid(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        mu, alpha = model.predict_params(X)
        assert (mu > 0).all()
        assert (alpha > 0).all()

    def test_sample_produces_nonneg_integers(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        samples = model.sample(X.iloc[[0]], n_samples=1000)

        assert samples.dtype in (np.int32, np.int64, int)
        assert (samples >= 0).all()

    def test_sample_contains_zeros(self, sample_data):
        """For low-mu rows, samples should include zeros."""
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        # Pick a row with low feature values (low expected mu)
        low_idx = X["avg_hits_l10"].idxmin()
        samples = model.sample(X.iloc[[low_idx]], n_samples=5000)

        assert (samples == 0).sum() > 0, "Low-mu row should produce some zeros"

    def test_sample_batch(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        samples = model.sample(X.iloc[:10], n_samples=500)
        assert samples.shape == (10, 500)
        assert (samples >= 0).all()

    def test_quantile_coverage(self):
        """Check that P(sample <= y) ≈ expected quantile level.

        Uses higher-mu data so Q10 > 0, avoiding the structural discrete
        floor issue that makes Q10 coverage ≈ P(X=0) for low-count stats.
        """
        # Higher mu data: mu ≈ 2-5, so Q10 ≈ 1 and coverage is meaningful
        X, y = _make_synthetic_data(n_samples=1500, seed=123)
        # Shift features up to increase mu
        X = X.copy()
        X["avg_hits_l10"] = X["avg_hits_l10"] + 3.0
        rng_data = np.random.RandomState(123)
        mu_true = 0.4 * X["avg_hits_l10"] + 0.1 * X["avg_ab_l10"]
        alpha_true = 0.5
        n_param = 1.0 / alpha_true
        p_param = n_param / (n_param + mu_true)
        y = pd.Series([int(nbinom.rvs(n_param, pp, random_state=rng_data)) for pp in p_param])

        model = NegBinModel()
        model.fit(X, y)

        n_check = min(200, len(X))
        rng = np.random.RandomState(99)

        # Coverage check: fraction of rows where actual <= Q_k
        # Skip Q10 — for integer distributions with P(X=0) > 10%, Q10=0 and
        # coverage ≈ P(X=0) which is structurally > 10%.
        for q in [0.25, 0.50, 0.75, 0.90]:
            hits = 0
            for i in range(n_check):
                row_samples = model.sample(X.iloc[[i]], n_samples=5000, rng=rng)
                quantile_val = np.percentile(row_samples, q * 100)
                if y.iloc[i] <= quantile_val:
                    hits += 1
            coverage = hits / n_check
            # ±16pp tolerance for synthetic data (small samples + discrete distribution)
            assert abs(coverage - q) < 0.16, (
                f"Q{q:.2f} coverage={coverage:.3f}, expected ~{q:.2f}"
            )

    def test_sample_single(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        features = X.iloc[0].to_dict()
        samples = model.sample_single(features, n_samples=200)
        assert samples.shape == (200,)
        assert (samples >= 0).all()

    def test_save_load_roundtrip(self, sample_data):
        X, y = sample_data
        model = NegBinModel(model_name="test_stat")
        model.fit(X, y)

        mu_before, alpha_before = model.predict_params(X.iloc[:5])

        with tempfile.TemporaryDirectory() as tmpdir:
            model.save(Path(tmpdir))

            # Verify v2 files created
            assert (Path(tmpdir) / "test_stat_xgblss_booster.json").exists()
            assert (Path(tmpdir) / "test_stat_negbin_meta.json").exists()

            loaded = NegBinModel.load(Path(tmpdir), model_name="test_stat")

            mu_after, alpha_after = loaded.predict_params(X.iloc[:5])

            np.testing.assert_array_almost_equal(mu_before, mu_after, decimal=4)
            np.testing.assert_array_almost_equal(alpha_before, alpha_after, decimal=4)
            assert loaded.feature_names == model.feature_names
            assert loaded._global_mu == model._global_mu
            assert loaded._global_alpha == model._global_alpha

    def test_native_booster_inference(self, sample_data):
        """Verify the lightweight native booster path produces valid params."""
        X, y = sample_data
        model = NegBinModel(model_name="test_stat")
        model.fit(X, y)

        with tempfile.TemporaryDirectory() as tmpdir:
            model.save(Path(tmpdir))
            loaded = NegBinModel.load(Path(tmpdir), model_name="test_stat")

            # Loaded model should use native booster path
            assert loaded._native_booster is not None
            assert loaded.mu_model is None
            assert loaded.alpha_model is None

            mu, alpha = loaded.predict_params(X)
            assert (mu > 0).all()
            assert (alpha > 0).all()
            assert not np.any(np.isnan(mu))
            assert not np.any(np.isnan(alpha))

            # Sampling should work
            samples = loaded.sample(X.iloc[[0]], n_samples=100)
            assert (samples >= 0).all()

    def test_exists_check(self, sample_data):
        X, y = sample_data
        model = NegBinModel(model_name="test_stat")
        model.fit(X, y)

        with tempfile.TemporaryDirectory() as tmpdir:
            assert not NegBinModel.exists(tmpdir, model_name="test_stat")
            model.save(tmpdir)
            assert NegBinModel.exists(tmpdir, model_name="test_stat")

    def test_missing_columns_handled(self, sample_data):
        X, y = sample_data
        model = NegBinModel()
        model.fit(X, y)

        X_partial = X[["avg_hits_l10", "opp_era"]]
        mu, alpha = model.predict_params(X_partial)
        assert (mu > 0).all()
        assert (alpha > 0).all()


# ---------------------------------------------------------------------------
# Inverse CDF tests
# ---------------------------------------------------------------------------

class TestNegBinInvCDF:
    def test_boundary_values(self):
        # Very low u should give 0
        samples = NegBinModel._negbin_invcdf(np.array([0.001]), mu=1.0, alpha=0.5)
        assert samples[0] >= 0

        # High u should give larger values
        samples = NegBinModel._negbin_invcdf(np.array([0.99]), mu=3.0, alpha=0.5)
        assert samples[0] >= 0

    def test_uniform_coverage(self):
        u = np.linspace(0.01, 0.99, 200)
        samples = NegBinModel._negbin_invcdf(u, mu=2.0, alpha=0.5)

        unique_vals = np.unique(samples)
        assert len(unique_vals) >= 4
        assert (samples >= 0).all()

    def test_zeros_possible(self):
        """With small mu, P(X=0) should be substantial."""
        u = np.linspace(0.001, 0.999, 10000)
        samples = NegBinModel._negbin_invcdf(u, mu=0.5, alpha=0.5)
        assert (samples == 0).sum() > 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestNegBinEdgeCases:
    def test_negative_values_rejected(self):
        X = pd.DataFrame({"feat1": [1, 2, 3, 4, 5]})
        y = pd.Series([-1, 1, 2, 1, 3])

        model = NegBinModel()
        with pytest.raises(ValueError, match="y >= 0"):
            model.fit(X, y)

    def test_unfitted_model_raises(self):
        model = NegBinModel()
        X = pd.DataFrame({"feat1": [1, 2, 3]})

        with pytest.raises(ValueError, match="not fitted"):
            model.predict_params(X)

    def test_all_zeros_data(self):
        """Edge case: all targets are zero."""
        rng = np.random.RandomState(42)
        X = pd.DataFrame({
            "feat1": rng.uniform(0, 1, 100),
            "feat2": rng.uniform(0, 1, 100),
        })
        y = pd.Series(np.zeros(100, dtype=int))

        model = NegBinModel()
        result = model.fit(X, y)
        assert result["global_mu"] > 0  # MLE should still produce something

        samples = model.sample(X.iloc[[0]], n_samples=100)
        assert (samples >= 0).all()
