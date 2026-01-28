"""Tests for train_pipeline module (TrainingOrchestrator)."""

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# Patch get_engine before importing TrainingOrchestrator so __init__ doesn't hit the DB
with patch("src.models.train_pipeline.get_engine", return_value=MagicMock()):
    with patch("src.models.train_pipeline.FeatureStore"):
        from src.models.train_pipeline import TrainingOrchestrator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_feature_store():
    return MagicMock()


@pytest.fixture
def orchestrator(tmp_path, mock_engine, mock_feature_store):
    """Create a TrainingOrchestrator with mocked DB, writing to tmp_path."""
    with patch("src.models.train_pipeline.get_engine", return_value=mock_engine):
        with patch("src.models.train_pipeline.FeatureStore", return_value=mock_feature_store):
            orch = TrainingOrchestrator(base_artifacts_dir=str(tmp_path / "artifacts"))
    return orch


@pytest.fixture
def sample_df():
    """Realistic training DataFrame with features and actuals."""
    rng = np.random.RandomState(42)
    n = 200

    minutes = rng.normal(28, 6, n).clip(5, 48)
    pts_per_min = rng.normal(0.7, 0.15, n).clip(0, 2)
    reb_per_min = rng.normal(0.2, 0.08, n).clip(0, 1)
    ast_per_min = rng.normal(0.15, 0.06, n).clip(0, 1)

    return pd.DataFrame(
        {
            "player_id": rng.choice([1, 2, 3, 4, 5], n),
            "game_id": [f"00224{i:05d}" for i in range(n)],
            "game_date": pd.date_range("2024-10-22", periods=n, freq="D"),
            "actual_minutes": minutes,
            "pts_per_min": pts_per_min,
            "reb_per_min": reb_per_min,
            "ast_per_min": ast_per_min,
            "threes_per_min": rng.normal(0.1, 0.05, n).clip(0, 0.5),
            "pts": minutes * pts_per_min,
            "reb": minutes * reb_per_min,
            "ast": minutes * ast_per_min,
            "line_spread": rng.choice([-5.5, -2.5, 0, 3.5, 8.0, 12.0], n),
            # Features the models expect
            "avg_minutes_10": rng.normal(28, 3, n),
            "avg_minutes_season": rng.normal(28, 2, n),
            "home_flag": rng.choice([0, 1], n),
            "rest_days": rng.choice([1, 2, 3], n),
        }
    )


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestTrainingOrchestratorInit:
    """Tests for TrainingOrchestrator.__init__."""

    def test_creates_run_directory(self, orchestrator):
        assert orchestrator.run_dir.exists()

    def test_run_dir_is_timestamped(self, orchestrator):
        assert orchestrator.run_dir.name.startswith("run_")

    def test_default_tuning_disabled(self, orchestrator):
        assert orchestrator.tune_hyperparams is False

    def test_tuning_enabled(self, tmp_path):
        with patch("src.models.train_pipeline.get_engine"):
            with patch("src.models.train_pipeline.FeatureStore"):
                orch = TrainingOrchestrator(
                    base_artifacts_dir=str(tmp_path),
                    tune_hyperparams=True,
                    tuning_trials=20,
                    tuning_timeout=300,
                    tuning_per_quantile=True,
                )
        assert orch.tune_hyperparams is True
        assert orch.tuning_trials == 20
        assert orch.tuning_timeout == 300
        assert orch.tuning_per_quantile is True

    def test_constants(self, orchestrator):
        assert orchestrator.CALIBRATION_TOLERANCE == 0.05
        assert orchestrator.CALIBRATION_HARD_FAIL == 0.10


# ---------------------------------------------------------------------------
# _save_run_config
# ---------------------------------------------------------------------------


class TestSaveRunConfig:
    """Tests for _save_run_config."""

    def test_creates_config_file(self, orchestrator):
        orchestrator._save_run_config(["22023", "22024"], "22025")
        config_path = orchestrator.run_dir / "run_config.json"
        assert config_path.exists()

    def test_config_contents(self, orchestrator):
        orchestrator._save_run_config(["22023"], "22024")
        with open(orchestrator.run_dir / "run_config.json") as f:
            data = json.load(f)

        assert data["train_seasons"] == ["22023"]
        assert data["calibration_season"] == "22024"
        assert "timestamp" in data
        assert data["calibration_tolerance"] == 0.05
        assert data["calibration_hard_fail"] == 0.10


# ---------------------------------------------------------------------------
# _run_hyperparameter_tuning
# ---------------------------------------------------------------------------


class TestRunHyperparameterTuning:
    """Tests for _run_hyperparameter_tuning."""

    def test_returns_none_when_disabled(self, orchestrator, sample_df):
        orchestrator.tune_hyperparams = False
        orchestrator.hyperparams_path = None
        result = orchestrator._run_hyperparameter_tuning(sample_df, {"minutes_features": []})
        assert result is None

    def test_loads_from_file(self, orchestrator, tmp_path):
        hyperparams = {"minutes": {"max_depth": 6, "n_estimators": 800}}
        hp_path = tmp_path / "hp.json"
        with open(hp_path, "w") as f:
            json.dump(hyperparams, f)

        orchestrator.hyperparams_path = str(hp_path)
        result = orchestrator._run_hyperparameter_tuning(pd.DataFrame(), {})

        assert result == hyperparams
        # Should also save a copy to run dir
        assert orchestrator.hyperparams_config_path.exists()

    def test_loaded_copy_matches_original(self, orchestrator, tmp_path):
        hyperparams = {"minutes": {"learning_rate": 0.05}}
        hp_path = tmp_path / "hp.json"
        with open(hp_path, "w") as f:
            json.dump(hyperparams, f)

        orchestrator.hyperparams_path = str(hp_path)
        orchestrator._run_hyperparameter_tuning(pd.DataFrame(), {})

        with open(orchestrator.hyperparams_config_path) as f:
            saved = json.load(f)
        assert saved == hyperparams


# ---------------------------------------------------------------------------
# _calibrate_model
# ---------------------------------------------------------------------------


class TestCalibrateModel:
    """Tests for _calibrate_model."""

    def _make_mock_model(self, feature_names, predictions):
        """Create a mock model that returns given predictions."""
        model = MagicMock()
        model.all_feature_names = feature_names

        pred_df = pd.DataFrame(predictions)
        model.predict_quantiles.return_value = pred_df
        return model

    def test_returns_list_of_reports(self, orchestrator):
        n = 100
        rng = np.random.RandomState(42)
        actuals = rng.normal(28, 5, n)
        df = pd.DataFrame(
            {
                "actual_minutes": actuals,
                "feat_a": rng.normal(0, 1, n),
            }
        )

        # Predictions that roughly match the quantiles
        preds = {
            "q10": np.percentile(actuals, 10) * np.ones(n),
            "q25": np.percentile(actuals, 25) * np.ones(n),
            "q50": np.percentile(actuals, 50) * np.ones(n),
            "q75": np.percentile(actuals, 75) * np.ones(n),
            "q90": np.percentile(actuals, 90) * np.ones(n),
        }
        model = self._make_mock_model(["feat_a"], preds)

        reports = orchestrator._calibrate_model(
            model=model,
            df=df,
            actual_col="actual_minutes",
            filter_mask=df["actual_minutes"] > 0,
            name="minutes",
        )

        assert isinstance(reports, list)
        assert len(reports) == 5  # 5 quantiles
        for r in reports:
            assert "quantile" in r
            assert "coverage" in r
            assert "gap" in r

    def test_empty_data_returns_empty(self, orchestrator):
        df = pd.DataFrame({"actual_minutes": [], "feat_a": []})
        model = self._make_mock_model(["feat_a"], {})

        reports = orchestrator._calibrate_model(
            model=model,
            df=df,
            actual_col="actual_minutes",
            filter_mask=pd.Series(dtype=bool),
            name="minutes",
        )
        assert reports == []

    def test_report_quantile_values(self, orchestrator):
        n = 50
        df = pd.DataFrame(
            {
                "actual_minutes": np.linspace(10, 40, n),
                "feat_a": np.ones(n),
            }
        )
        preds = {f"q{int(q * 100):02d}": np.full(n, 25.0) for q in [0.10, 0.25, 0.50, 0.75, 0.90]}
        model = self._make_mock_model(["feat_a"], preds)

        reports = orchestrator._calibrate_model(
            model=model,
            df=df,
            actual_col="actual_minutes",
            filter_mask=pd.Series([True] * n),
            name="test",
        )

        quantiles_reported = [r["quantile"] for r in reports]
        assert quantiles_reported == [0.10, 0.25, 0.50, 0.75, 0.90]


# ---------------------------------------------------------------------------
# _save_calibration_report
# ---------------------------------------------------------------------------


class TestSaveCalibrationReport:
    """Tests for _save_calibration_report."""

    def test_saves_json_default_name(self, orchestrator):
        reports = {"minutes": [{"quantile": 0.5, "coverage": 0.48, "gap": -0.02}]}
        orchestrator._save_calibration_report(reports)

        path = orchestrator.run_dir / "calibration_report.json"
        assert path.exists()
        with open(path) as f:
            data = json.load(f)
        assert data == reports

    def test_saves_with_suffix(self, orchestrator):
        reports = {"pts": [{"quantile": 0.5}]}
        orchestrator._save_calibration_report(reports, suffix="_combined")

        path = orchestrator.run_dir / "calibration_report_combined.json"
        assert path.exists()


# ---------------------------------------------------------------------------
# _analyze_minutes_rate_correlation
# ---------------------------------------------------------------------------


class TestAnalyzeMinutesRateCorrelation:
    """Tests for _analyze_minutes_rate_correlation."""

    def test_returns_correlations(self, orchestrator):
        rng = np.random.RandomState(42)
        n = 200
        minutes = rng.normal(28, 5, n).clip(10, 48)

        df = pd.DataFrame(
            {
                "actual_minutes": minutes,
                "pts_per_min": 0.7 + rng.normal(0, 0.1, n),
                "reb_per_min": 0.2 + rng.normal(0, 0.05, n),
                "ast_per_min": 0.15 + rng.normal(0, 0.04, n),
            }
        )

        result = orchestrator._analyze_minutes_rate_correlation(df)

        assert "pts" in result
        assert "reb" in result
        assert "ast" in result
        for stat in result:
            assert "overall" in result[stat]

    def test_saves_json(self, orchestrator):
        rng = np.random.RandomState(42)
        n = 100
        df = pd.DataFrame(
            {
                "actual_minutes": rng.normal(28, 5, n).clip(10, 48),
                "pts_per_min": rng.normal(0.7, 0.1, n),
            }
        )
        orchestrator._analyze_minutes_rate_correlation(df)

        path = orchestrator.run_dir / "correlation_analysis.json"
        assert path.exists()

    def test_skips_missing_rate_columns(self, orchestrator):
        n = 100
        df = pd.DataFrame(
            {
                "actual_minutes": np.random.normal(28, 5, n).clip(10, 48),
            }
        )
        result = orchestrator._analyze_minutes_rate_correlation(df)
        assert result == {}

    def test_filters_low_minutes(self, orchestrator):
        df = pd.DataFrame(
            {
                "actual_minutes": [5.0, 7.0, 25.0, 30.0] * 25,
                "pts_per_min": [0.5, 0.6, 0.7, 0.8] * 25,
            }
        )
        result = orchestrator._analyze_minutes_rate_correlation(df)
        # Should still work, just with fewer rows
        assert isinstance(result, dict)

    def test_bucket_analysis(self, orchestrator):
        rng = np.random.RandomState(42)
        n = 400
        minutes = rng.uniform(12, 45, n)
        df = pd.DataFrame(
            {
                "actual_minutes": minutes,
                "pts_per_min": rng.normal(0.7, 0.1, n),
            }
        )
        result = orchestrator._analyze_minutes_rate_correlation(df)

        if "pts" in result and "by_bucket" in result["pts"]:
            for bucket in result["pts"]["by_bucket"]:
                assert "bucket" in bucket
                assert "n" in bucket
                assert "correlation" in bucket
                assert "mean_rate" in bucket


# ---------------------------------------------------------------------------
# _evaluate_calibration
# ---------------------------------------------------------------------------


class TestEvaluateCalibration:
    """Tests for _evaluate_calibration."""

    def test_returns_reports_dict(self, orchestrator):
        rng = np.random.RandomState(42)
        n = 100
        minutes = rng.normal(28, 5, n).clip(5, 48)

        df = pd.DataFrame(
            {
                "actual_minutes": minutes,
                "pts_per_min": rng.normal(0.7, 0.1, n),
                "reb_per_min": rng.normal(0.2, 0.05, n),
                "ast_per_min": rng.normal(0.15, 0.04, n),
                "feat_a": rng.normal(0, 1, n),
            }
        )

        # Mock pipeline with minutes_model and rate_models
        mock_pipeline = MagicMock()

        def make_model(feat_names):
            m = MagicMock()
            m.all_feature_names = feat_names
            pred_df = pd.DataFrame(
                {
                    f"q{int(q * 100):02d}": np.percentile(minutes, q * 100) * np.ones(n)
                    for q in [0.10, 0.25, 0.50, 0.75, 0.90]
                }
            )
            m.predict_quantiles.return_value = pred_df
            return m

        mock_pipeline.minutes_model = make_model(["feat_a"])
        mock_pipeline.rate_models = {
            "pts": make_model(["feat_a"]),
            "reb": make_model(["feat_a"]),
            "ast": make_model(["feat_a"]),
        }

        reports = orchestrator._evaluate_calibration(mock_pipeline, df)

        assert "minutes" in reports
        assert "pts" in reports
        assert "reb" in reports
        assert "ast" in reports

    def test_writes_calibration_report_file(self, orchestrator):
        n = 50
        rng = np.random.RandomState(42)
        df = pd.DataFrame(
            {
                "actual_minutes": rng.normal(28, 5, n).clip(5, 48),
                "feat_a": rng.normal(0, 1, n),
            }
        )

        mock_pipeline = MagicMock()
        m = MagicMock()
        m.all_feature_names = ["feat_a"]
        m.predict_quantiles.return_value = pd.DataFrame(
            {f"q{int(q * 100):02d}": np.full(n, 25.0) for q in [0.10, 0.25, 0.50, 0.75, 0.90]}
        )
        mock_pipeline.minutes_model = m
        mock_pipeline.rate_models = {}

        orchestrator._evaluate_calibration(mock_pipeline, df)
        assert (orchestrator.run_dir / "calibration_report.json").exists()

    def test_hard_fail_creates_file(self, orchestrator):
        """Calibration gap > 10% should create CALIBRATION_FAILED.txt."""
        n = 100
        df = pd.DataFrame(
            {
                "actual_minutes": np.full(n, 30.0),
                "feat_a": np.ones(n),
            }
        )

        # Predict 0 for all quantiles -> coverage = 0% for all -> huge gap
        m = MagicMock()
        m.all_feature_names = ["feat_a"]
        m.predict_quantiles.return_value = pd.DataFrame(
            {f"q{int(q * 100):02d}": np.full(n, 0.0) for q in [0.10, 0.25, 0.50, 0.75, 0.90]}
        )

        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model = m
        mock_pipeline.rate_models = {}

        orchestrator._evaluate_calibration(mock_pipeline, df)
        assert (orchestrator.run_dir / "CALIBRATION_FAILED.txt").exists()

    def test_warning_creates_file(self, orchestrator):
        """Calibration gap between 5-10% should create CALIBRATION_WARNING.txt."""
        n = 100
        actuals = np.linspace(10, 40, n)
        df = pd.DataFrame(
            {
                "actual_minutes": actuals,
                "feat_a": np.ones(n),
            }
        )

        # Construct predictions where worst gap is between 5-10%
        # Q10 target = 0.10, set pred so that coverage = ~0.16 (gap = 0.06)
        q10_val = np.percentile(actuals, 16)
        # Other quantiles: roughly calibrated
        preds = {
            "q10": np.full(n, q10_val),
            "q25": np.full(n, np.percentile(actuals, 25)),
            "q50": np.full(n, np.percentile(actuals, 50)),
            "q75": np.full(n, np.percentile(actuals, 75)),
            "q90": np.full(n, np.percentile(actuals, 90)),
        }

        m = MagicMock()
        m.all_feature_names = ["feat_a"]
        m.predict_quantiles.return_value = pd.DataFrame(preds)

        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model = m
        mock_pipeline.rate_models = {}

        orchestrator._evaluate_calibration(mock_pipeline, df)
        assert (orchestrator.run_dir / "CALIBRATION_WARNING.txt").exists()


# ---------------------------------------------------------------------------
# _run_sanity_check
# ---------------------------------------------------------------------------


class TestRunSanityCheck:
    """Tests for _run_sanity_check."""

    def test_skips_on_empty_df(self, orchestrator):
        """Should not raise on empty DataFrame."""
        mock_pipeline = MagicMock()
        df = pd.DataFrame()
        # Should return gracefully, no exception
        orchestrator._run_sanity_check(mock_pipeline, df)

    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_runs_inference(self, mock_mc_cls, orchestrator, sample_df):
        """Test sanity check calls predict."""
        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model.all_feature_names = ["avg_minutes_10"]
        mock_pipeline.rate_models = {
            "pts": MagicMock(all_feature_names=["avg_minutes_10"]),
            "reb": MagicMock(all_feature_names=["avg_minutes_10"]),
            "ast": MagicMock(all_feature_names=["avg_minutes_10"]),
        }

        # Mock prediction output
        mock_pred = MagicMock()
        mock_pred.median = 25.0
        mock_pred.mean = 24.5
        mock_pred.q10 = 15.0
        mock_pred.q90 = 35.0

        mock_mc_cls.return_value.predict.return_value = {
            "pts": mock_pred,
            "reb": mock_pred,
            "ast": mock_pred,
        }

        orchestrator.feature_store.get_player_game_features.return_value = {
            "avg_minutes_10": 28.0,
        }

        orchestrator._run_sanity_check(mock_pipeline, sample_df)
        mock_mc_cls.return_value.predict.assert_called_once()

    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_fails_on_negative_mean(self, mock_mc_cls, orchestrator, sample_df):
        """Test sanity check raises on negative mean prediction."""
        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model.all_feature_names = ["avg_minutes_10"]
        mock_pipeline.rate_models = {}

        mock_pred = MagicMock()
        mock_pred.median = -5.0
        mock_pred.mean = -5.0
        mock_pred.q10 = -10.0
        mock_pred.q90 = 0.0

        mock_mc_cls.return_value.predict.return_value = {"pts": mock_pred, "reb": mock_pred, "ast": mock_pred}
        orchestrator.feature_store.get_player_game_features.return_value = {"avg_minutes_10": 28.0}

        with pytest.raises(ValueError, match="Negative mean"):
            orchestrator._run_sanity_check(mock_pipeline, sample_df)

    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_fails_on_quantile_inversion(self, mock_mc_cls, orchestrator, sample_df):
        """Test sanity check raises when Q10 > Q90."""
        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model.all_feature_names = ["avg_minutes_10"]
        mock_pipeline.rate_models = {}

        mock_pred = MagicMock()
        mock_pred.median = 25.0
        mock_pred.mean = 25.0
        mock_pred.q10 = 40.0  # inverted
        mock_pred.q90 = 10.0

        mock_mc_cls.return_value.predict.return_value = {"pts": mock_pred, "reb": mock_pred, "ast": mock_pred}
        orchestrator.feature_store.get_player_game_features.return_value = {"avg_minutes_10": 28.0}

        with pytest.raises(ValueError, match="Quantile inversion"):
            orchestrator._run_sanity_check(mock_pipeline, sample_df)

    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_handles_null_features(self, mock_mc_cls, orchestrator, sample_df):
        """Test sanity check when feature store returns None."""
        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model.all_feature_names = []
        mock_pipeline.rate_models = {}

        orchestrator.feature_store.get_player_game_features.return_value = None

        # Should not raise, just log and return
        orchestrator._run_sanity_check(mock_pipeline, sample_df)
        mock_mc_cls.return_value.predict.assert_not_called()


# ---------------------------------------------------------------------------
# Full run() integration (mocked)
# ---------------------------------------------------------------------------


class TestRunIntegration:
    """Integration tests for the full run() method with all components mocked."""

    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_full_run_completes(self, mock_mc_cls, orchestrator, sample_df):
        """Test that run() executes all steps without error."""
        # Mock feature_store
        orchestrator.feature_store.get_training_dataset.return_value = sample_df
        orchestrator.feature_store.get_player_game_features.return_value = {
            "avg_minutes_10": 28.0,
        }

        # Mock feature selection
        feature_config = {
            "minutes_features": {
                "0.1": ["avg_minutes_10"],
                "0.25": ["avg_minutes_10"],
                "0.5": ["avg_minutes_10"],
                "0.75": ["avg_minutes_10"],
                "0.9": ["avg_minutes_10"],
            },
            "pts_rate_features": {"0.5": ["avg_minutes_10"]},
            "reb_rate_features": {"0.5": ["avg_minutes_10"]},
            "ast_rate_features": {"0.5": ["avg_minutes_10"]},
            "threes_rate_features": {"0.5": ["avg_minutes_10"]},
        }

        # Mock pipeline returned by _train_models
        mock_model = MagicMock()
        mock_model.all_feature_names = ["avg_minutes_10"]
        n = len(sample_df)
        mock_model.predict_quantiles.return_value = pd.DataFrame(
            {f"q{int(q * 100):02d}": np.full(n, 25.0) for q in [0.10, 0.25, 0.50, 0.75, 0.90]}
        )

        mock_pipeline = MagicMock()
        mock_pipeline.minutes_model = mock_model
        mock_pipeline.rate_models = {"pts": mock_model, "reb": mock_model, "ast": mock_model}

        # Mock MC predictions — must set ALL quantile attrs as floats
        # because _evaluate_combined_calibration accesses q10/q25/q50/q75/q90/mean
        mock_pred = MagicMock()
        mock_pred.median = 25.0
        mock_pred.mean = 24.5
        mock_pred.q10 = 15.0
        mock_pred.q25 = 20.0
        mock_pred.q50 = 25.0
        mock_pred.q75 = 30.0
        mock_pred.q90 = 35.0
        mock_mc_cls.return_value.predict.return_value = {
            "pts": mock_pred,
            "reb": mock_pred,
            "ast": mock_pred,
        }

        with patch.object(orchestrator, "_run_feature_selection", return_value=feature_config):
            with patch.object(orchestrator, "_train_models", return_value=mock_pipeline):
                orchestrator.run(train_seasons=["22023"], calibration_season="22024")

        # Verify artifacts were saved
        assert (orchestrator.run_dir / "run_config.json").exists()
        assert (orchestrator.run_dir / "calibration_report.json").exists()
        assert (orchestrator.run_dir / "correlation_analysis.json").exists()
        mock_pipeline.save_all.assert_called_once()
