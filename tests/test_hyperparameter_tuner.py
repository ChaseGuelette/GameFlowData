"""Tests for hyperparameter_tuner module."""

import json

import numpy as np
import pandas as pd
import pytest

from src.models.hyperparameter_tuner import (
    SEARCH_SPACE,
    QuantileHyperparameterTuner,
    TuningResult,
)
from src.models.quantile_trainer import QuantileModelConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tuner():
    """Create a tuner with minimal trials for fast tests."""
    return QuantileHyperparameterTuner(
        n_trials=2,
        timeout=30,
        per_quantile=False,
        pruning=False,
        val_fraction=0.3,
        random_state=42,
    )


@pytest.fixture
def sample_data():
    """Generate small synthetic dataset for tuning tests."""
    rng = np.random.RandomState(42)
    n = 200
    X = pd.DataFrame(
        {
            "feat_a": rng.normal(0, 1, n),
            "feat_b": rng.normal(5, 2, n),
            "feat_c": rng.uniform(0, 10, n),
        }
    )
    y = pd.Series(2 * X["feat_a"] + 0.5 * X["feat_b"] + rng.normal(0, 0.5, n), name="target")
    return X, y


# ---------------------------------------------------------------------------
# TuningResult dataclass
# ---------------------------------------------------------------------------


class TestTuningResult:
    """Tests for TuningResult dataclass."""

    def test_basic_creation(self):
        result = TuningResult(
            best_params={"max_depth": 5},
            best_calibration_gap=0.03,
            n_trials=50,
            study_name="test_study",
            timestamp="2024-01-01T00:00:00",
            quantiles=(0.10, 0.50, 0.90),
        )
        assert result.best_calibration_gap == 0.03
        assert result.per_quantile is False
        assert result.per_quantile_params is None

    def test_per_quantile_creation(self):
        result = TuningResult(
            best_params={},
            best_calibration_gap=0.05,
            n_trials=100,
            study_name="pq_study",
            timestamp="2024-01-01",
            quantiles=(0.10, 0.50, 0.90),
            per_quantile=True,
            per_quantile_params={0.10: {"max_depth": 4}, 0.50: {"max_depth": 6}},
        )
        assert result.per_quantile is True
        assert 0.10 in result.per_quantile_params


# ---------------------------------------------------------------------------
# SEARCH_SPACE
# ---------------------------------------------------------------------------


class TestSearchSpace:
    """Tests for default search space."""

    def test_has_all_expected_keys(self):
        expected = [
            "max_depth",
            "min_child_weight",
            "learning_rate",
            "n_estimators",
            "subsample",
            "colsample_bytree",
            "reg_alpha",
            "reg_lambda",
            "early_stopping_rounds",
        ]
        for key in expected:
            assert key in SEARCH_SPACE

    def test_ranges_are_valid(self):
        for key, (lo, hi) in SEARCH_SPACE.items():
            assert lo < hi, f"{key}: {lo} >= {hi}"


# ---------------------------------------------------------------------------
# QuantileHyperparameterTuner
# ---------------------------------------------------------------------------


class TestTunerInit:
    """Tests for tuner initialization."""

    def test_default_values(self):
        t = QuantileHyperparameterTuner()
        assert t.n_trials == 100
        assert t.per_quantile is False
        assert t.pruning is True
        assert t.val_fraction == 0.15
        assert t.random_state == 42
        assert t.study is None
        assert t.result is None

    def test_custom_values(self):
        t = QuantileHyperparameterTuner(
            n_trials=10,
            timeout=60,
            per_quantile=True,
            pruning=False,
            val_fraction=0.2,
            random_state=99,
        )
        assert t.n_trials == 10
        assert t.timeout == 60
        assert t.per_quantile is True
        assert t.pruning is False
        assert t.val_fraction == 0.2
        assert t.random_state == 99

    def test_custom_search_space(self):
        custom = {"max_depth": (2, 4)}
        t = QuantileHyperparameterTuner(search_space=custom)
        assert t.search_space == custom

    def test_study_name_auto_generated(self):
        t = QuantileHyperparameterTuner()
        assert t.study_name.startswith("quantile_tuning_")

    def test_study_name_custom(self):
        t = QuantileHyperparameterTuner(study_name="my_study")
        assert t.study_name == "my_study"


class TestCalibrationGap:
    """Tests for _calculate_calibration_gap."""

    def test_perfect_calibration(self):
        tuner = QuantileHyperparameterTuner()
        y_true = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
        # Predict the 50th percentile value as 5.5 -> exactly 5/10 = 50% below
        y_pred = np.full(10, 5.5)
        gap = tuner._calculate_calibration_gap(y_true, y_pred, 0.50)
        assert gap == pytest.approx(0.0, abs=0.01)

    def test_undercoverage(self):
        tuner = QuantileHyperparameterTuner()
        y_true = np.array([10, 20, 30, 40, 50], dtype=float)
        y_pred = np.full(5, 5.0)  # All actuals above pred -> coverage = 0%
        gap = tuner._calculate_calibration_gap(y_true, y_pred, 0.50)
        assert gap == pytest.approx(0.50, abs=0.01)

    def test_overcoverage(self):
        tuner = QuantileHyperparameterTuner()
        y_true = np.array([1, 2, 3, 4, 5], dtype=float)
        y_pred = np.full(5, 100.0)  # All actuals below pred -> coverage = 100%
        gap = tuner._calculate_calibration_gap(y_true, y_pred, 0.50)
        assert gap == pytest.approx(0.50, abs=0.01)

    def test_gap_is_absolute(self):
        tuner = QuantileHyperparameterTuner()
        y_true = np.arange(100, dtype=float)
        y_pred = np.full(100, 20.0)  # coverage ~ 21%
        gap = tuner._calculate_calibration_gap(y_true, y_pred, 0.50)
        assert gap >= 0


class TestParamsToConfig:
    """Tests for _params_to_config."""

    def test_converts_params(self):
        tuner = QuantileHyperparameterTuner()
        params = {
            "n_estimators": 500,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.85,
            "colsample_bytree": 0.7,
            "min_child_weight": 5,
            "early_stopping_rounds": 40,
        }
        config = tuner._params_to_config(params, (0.10, 0.50, 0.90))

        assert isinstance(config, QuantileModelConfig)
        assert config.n_estimators == 500
        assert config.max_depth == 6
        assert config.learning_rate == 0.05
        assert config.quantiles == (0.10, 0.50, 0.90)

    def test_uses_defaults_for_missing(self):
        tuner = QuantileHyperparameterTuner()
        config = tuner._params_to_config({}, (0.50,))

        assert config.n_estimators == 1000
        assert config.max_depth == 5
        assert config.learning_rate == 0.03


class TestTune:
    """Tests for tune() method (runs actual Optuna optimization)."""

    def test_tune_returns_config(self, tuner, sample_data):
        """Test tune returns a QuantileModelConfig."""
        X, y = sample_data
        config = tuner.tune(X, y, quantiles=(0.25, 0.50, 0.75))

        assert isinstance(config, QuantileModelConfig)
        assert config.quantiles == (0.25, 0.50, 0.75)

    def test_tune_populates_study(self, tuner, sample_data):
        """Test tune populates the study attribute."""
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.50,))

        assert tuner.study is not None
        assert len(tuner.study.trials) == 2  # n_trials=2

    def test_tune_populates_result(self, tuner, sample_data):
        """Test tune populates the result attribute."""
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.50,))

        assert tuner.result is not None
        assert tuner.result.n_trials == 2
        assert tuner.result.per_quantile is False
        assert tuner.result.best_calibration_gap >= 0

    def test_tune_with_feature_names_subset(self, tuner, sample_data):
        """Test tune with explicit feature_names subset."""
        X, y = sample_data
        config = tuner.tune(X, y, quantiles=(0.50,), feature_names=["feat_a", "feat_b"])

        assert isinstance(config, QuantileModelConfig)


class TestTunePerQuantile:
    """Tests for tune_per_quantile() method."""

    def test_returns_dict_of_configs(self, sample_data):
        tuner = QuantileHyperparameterTuner(n_trials=2, pruning=False, timeout=30)
        X, y = sample_data
        configs = tuner.tune_per_quantile(X, y, quantiles=(0.25, 0.75))

        assert isinstance(configs, dict)
        assert 0.25 in configs
        assert 0.75 in configs
        for q, cfg in configs.items():
            assert isinstance(cfg, QuantileModelConfig)

    def test_populates_studies_per_quantile(self, sample_data):
        tuner = QuantileHyperparameterTuner(n_trials=2, pruning=False, timeout=30)
        X, y = sample_data
        tuner.tune_per_quantile(X, y, quantiles=(0.25, 0.75))

        assert len(tuner.studies_per_quantile) == 2
        assert 0.25 in tuner.studies_per_quantile
        assert 0.75 in tuner.studies_per_quantile

    def test_result_is_per_quantile(self, sample_data):
        tuner = QuantileHyperparameterTuner(n_trials=2, pruning=False, timeout=30)
        X, y = sample_data
        tuner.tune_per_quantile(X, y, quantiles=(0.50,))

        assert tuner.result is not None
        assert tuner.result.per_quantile is True


class TestGetTrialsDataframe:
    """Tests for get_trials_dataframe."""

    def test_raises_without_study(self):
        tuner = QuantileHyperparameterTuner()
        with pytest.raises(ValueError, match="No study available"):
            tuner.get_trials_dataframe()

    def test_returns_dataframe_after_tune(self, tuner, sample_data):
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.50,))

        df = tuner.get_trials_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2


class TestSaveAndLoad:
    """Tests for save_study, save_best_config, load_best_config."""

    def test_save_study_raises_without_study(self, tmp_path):
        tuner = QuantileHyperparameterTuner()
        with pytest.raises(ValueError, match="No study available"):
            tuner.save_study(str(tmp_path / "study.pkl"))

    def test_save_best_config_raises_without_result(self, tmp_path):
        tuner = QuantileHyperparameterTuner()
        with pytest.raises(ValueError, match="No tuning result"):
            tuner.save_best_config(str(tmp_path / "config.json"))

    def test_save_and_load_shared_config(self, tuner, sample_data, tmp_path):
        """Test round-trip: tune -> save -> load for shared config."""
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.25, 0.50, 0.75))

        config_path = str(tmp_path / "config.json")
        tuner.save_best_config(config_path)

        loaded = QuantileHyperparameterTuner.load_best_config(config_path)
        assert isinstance(loaded, QuantileModelConfig)
        assert loaded.quantiles == (0.25, 0.50, 0.75)

    def test_save_and_load_per_quantile_config(self, sample_data, tmp_path):
        """Test round-trip for per-quantile config."""
        tuner = QuantileHyperparameterTuner(n_trials=2, pruning=False, timeout=30)
        X, y = sample_data
        tuner.tune_per_quantile(X, y, quantiles=(0.25, 0.75))

        config_path = str(tmp_path / "pq_config.json")
        tuner.save_best_config(config_path)

        loaded = QuantileHyperparameterTuner.load_best_config(config_path)
        assert isinstance(loaded, dict)
        assert 0.25 in loaded
        assert 0.75 in loaded

    def test_save_study_creates_file(self, tuner, sample_data, tmp_path):
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.50,))

        study_path = str(tmp_path / "study.pkl")
        tuner.save_study(study_path)
        assert (tmp_path / "study.pkl").exists()

    def test_load_config_json_structure(self, tuner, sample_data, tmp_path):
        """Test the JSON file has expected keys."""
        X, y = sample_data
        tuner.tune(X, y, quantiles=(0.50,))

        config_path = tmp_path / "config.json"
        tuner.save_best_config(str(config_path))

        with open(config_path) as f:
            data = json.load(f)

        assert "best_params" in data
        assert "best_calibration_gap" in data
        assert "n_trials" in data
        assert "quantiles" in data
        assert "per_quantile" in data
