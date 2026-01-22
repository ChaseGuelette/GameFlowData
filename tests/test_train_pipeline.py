import sys
from unittest.mock import MagicMock, patch

# MOCK DEPENDENCIES
mock_xgb = MagicMock()
sys.modules["xgboost"] = mock_xgb
mock_sklearn = MagicMock()
sys.modules["sklearn"] = mock_sklearn
sys.modules["sklearn.model_selection"] = MagicMock()
sys.modules["sklearn.preprocessing"] = MagicMock()
sys.modules["sklearn.metrics"] = MagicMock()
sys.modules["sklearn.isotonic"] = MagicMock()

import unittest
from pathlib import Path

import numpy as np
import pandas as pd

# Import source
from src.models.train_pipeline import TrainingOrchestrator


class TestTrainingOrchestrator(unittest.TestCase):
    def setUp(self):
        self.test_artifacts_dir = Path("tests/artifacts")
        self.test_artifacts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil

        if self.test_artifacts_dir.exists():
            shutil.rmtree(self.test_artifacts_dir)

    def _create_calibrated_preds(self, n_rows, actual_val):
        """
        Create predictions that perfectly calibrate to the target quantiles
        against a constant actual value.
        """
        # Targets: 0.10, 0.25, 0.50, 0.75, 0.90
        # For a constant actual_val, we set pred >= actual_val for the top N% rows
        # Q10 (0.10 coverage): pred >= actual in 10% of rows

        preds = {}
        for q in [10, 25, 50, 75, 90]:
            target_cov = q / 100.0
            n_covered = int(n_rows * target_cov)

            # Covered means actual <= pred
            # So set pred = actual + 1 for covered rows
            # Set pred = actual - 1 for uncovered rows
            col_vals = [actual_val - 1] * n_rows
            for i in range(n_covered):
                col_vals[i] = actual_val + 1

            preds[f"q{q:02d}"] = col_vals

        return pd.DataFrame(preds)

    @patch("src.models.train_pipeline.get_engine")
    @patch("src.models.train_pipeline.FeatureStore")
    @patch("src.models.train_pipeline.PlayerPropsModelPipeline")
    @patch("src.models.train_pipeline.FeatureSelector")
    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_happy_path_execution(self, MockMC, MockSelector, MockPipeline, MockFeatureStore, MockGetEngine):
        orchestrator = TrainingOrchestrator(base_artifacts_dir=str(self.test_artifacts_dir))

        # 100 rows for easy percentiles
        n_rows = 100
        mock_train_df = pd.DataFrame(
            {
                "player_id": [1] * n_rows,
                "game_id": [f"g{i}" for i in range(n_rows)],
                "game_date": ["2022-01-01"] * n_rows,
                "actual_minutes": [30.0] * n_rows,
                "actual_pts": [20.0] * n_rows,
                "actual_reb": [5.0] * n_rows,
                "actual_ast": [5.0] * n_rows,
                "actual_threes": [2.0] * n_rows,
                "pts_per_min": [0.6] * n_rows,
                "reb_per_min": [0.1] * n_rows,
                "ast_per_min": [0.1] * n_rows,
                "threes_per_min": [0.05] * n_rows,
                "feature_1": np.random.rand(n_rows),
            }
        )
        MockFeatureStore.return_value.get_training_dataset.side_effect = [mock_train_df, mock_train_df]

        MockSelector.return_value.select_features.return_value = ["feature_1"]

        mock_pipeline_instance = MockPipeline.return_value

        # Mock Calibrated Predictions for Minutes
        mock_pipeline_instance.minutes_model.predict_quantiles.return_value = self._create_calibrated_preds(
            n_rows, 30.0
        )

        # Mock Calibrated Predictions for Rates
        mock_pipeline_instance.rate_models = {}
        for stat, actual_val in [("pts", 0.6), ("reb", 0.1), ("ast", 0.1), ("threes", 0.05)]:
            model = MagicMock()
            model.predict_quantiles.return_value = self._create_calibrated_preds(n_rows, actual_val)
            mock_pipeline_instance.rate_models[stat] = model

        # Sanity Check Mock
        MockFeatureStore.return_value.get_player_game_features.return_value = {"f": 1}
        MockMC.return_value.predict.return_value = {
            "pts": MagicMock(mean=20, median=20, q10=10, q90=30),
            "reb": MagicMock(mean=5, median=5, q10=2, q90=8),
            "ast": MagicMock(mean=5, median=5, q10=2, q90=8),
        }

        orchestrator.run(["2022"], "2023")

        self.assertEqual(MockFeatureStore.return_value.get_training_dataset.call_count, 2)
        mock_pipeline_instance.save_all.assert_called_once()

    @patch("src.models.train_pipeline.get_engine")
    @patch("src.models.train_pipeline.FeatureStore")
    @patch("src.models.train_pipeline.PlayerPropsModelPipeline")
    @patch("src.models.train_pipeline.FeatureSelector")
    def test_calibration_hard_fail(self, MockSelector, MockPipeline, MockFeatureStore, MockGetEngine):
        orchestrator = TrainingOrchestrator(base_artifacts_dir=str(self.test_artifacts_dir))

        n_rows = 100
        # Add ALL required columns to avoid KeyError
        df = pd.DataFrame(
            {
                "actual_minutes": [30.0] * n_rows,
                "player_id": [1] * n_rows,
                "game_id": ["g"] * n_rows,
                "pts_per_min": [0.6] * n_rows,
                "reb_per_min": [0.1] * n_rows,
                "ast_per_min": [0.1] * n_rows,
                "threes_per_min": [0.05] * n_rows,
                "f1": [0.5] * n_rows,
            }
        )
        MockFeatureStore.return_value.get_training_dataset.return_value = df
        MockSelector.return_value.select_features.return_value = ["f1"]

        # Bad predictions (0.0 vs 30.0 actual)
        # All actuals (30.0) > pred (0.0). Coverage = 0.
        # Q10 (target 0.10) - Coverage 0 = Gap -0.10 (Borderline)
        # Q90 (target 0.90) - Coverage 0 = Gap -0.90 (Hard Fail)
        bad_preds = pd.DataFrame({f"q{q:02d}": [0.0] * n_rows for q in [10, 25, 50, 75, 90]})

        mock_pipeline_instance = MockPipeline.return_value
        mock_pipeline_instance.minutes_model.predict_quantiles.return_value = bad_preds
        mock_pipeline_instance.rate_models = {}

        with self.assertRaises(ValueError) as context:
            orchestrator.run(["2022"], "2023")
        self.assertIn("Calibration failed", str(context.exception))

    @patch("src.models.train_pipeline.get_engine")
    @patch("src.models.train_pipeline.FeatureStore")
    @patch("src.models.train_pipeline.PlayerPropsModelPipeline")
    @patch("src.models.train_pipeline.FeatureSelector")
    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_calibration_warning(self, MockMC, MockSelector, MockPipeline, MockFeatureStore, MockGetEngine):
        orchestrator = TrainingOrchestrator(base_artifacts_dir=str(self.test_artifacts_dir))

        n_rows = 100
        df = pd.DataFrame(
            {
                "actual_minutes": [10.0] * n_rows,
                "player_id": [1] * n_rows,
                "game_id": ["g"] * n_rows,
                "game_date": ["d"] * n_rows,
                "pts_per_min": [0.6] * n_rows,
                "reb_per_min": [0.1] * n_rows,
                "ast_per_min": [0.1] * n_rows,
                "threes_per_min": [0.05] * n_rows,
                "f1": [0.5] * n_rows,
            }
        )
        MockFeatureStore.return_value.get_training_dataset.return_value = df
        MockSelector.return_value.select_features.return_value = ["f1"]

        # Construct Warning Case (Gap ~0.06)
        # Target Q10 (0.10). Make coverage 0.16.
        # preds >= actual in 16 cases.
        # actual=10.0. preds=11.0 (16 cases), 0.0 (84 cases).
        q_cols = {f"q{q:02d}": [0.0] * n_rows for q in [10, 25, 50, 75, 90]}
        q_cols["q10"] = [11.0] * 16 + [0.0] * 84

        # For other quantiles, just make them perfect to isolate Q10 failure
        # Q90 (0.90) -> 90 covered
        q_cols["q90"] = [11.0] * 90 + [0.0] * 10
        # Q50 -> 50 covered
        q_cols["q50"] = [11.0] * 50 + [0.0] * 50
        # Q25 -> 25 covered
        q_cols["q25"] = [11.0] * 25 + [0.0] * 75
        # Q75 -> 75 covered
        q_cols["q75"] = [11.0] * 75 + [0.0] * 25

        mock_pipeline_instance = MockPipeline.return_value
        mock_pipeline_instance.minutes_model.predict_quantiles.return_value = pd.DataFrame(q_cols)
        mock_pipeline_instance.rate_models = {}

        # Sanity pass
        MockFeatureStore.return_value.get_player_game_features.return_value = {"f": 1}
        MockMC.return_value.predict.return_value = {}

        orchestrator.run(["2022"], "2023")

        run_dir = list(self.test_artifacts_dir.glob("run_*"))[0]
        self.assertTrue((run_dir / "CALIBRATION_WARNING.txt").exists())

    @patch("src.models.train_pipeline.get_engine")
    @patch("src.models.train_pipeline.FeatureStore")
    @patch("src.models.train_pipeline.PlayerPropsModelPipeline")
    @patch("src.models.train_pipeline.FeatureSelector")
    @patch("src.models.train_pipeline.MonteCarloPredictor")
    def test_feature_injection(self, MockMC, MockSelector, MockPipeline, MockFeatureStore, MockGetEngine):
        orchestrator = TrainingOrchestrator(base_artifacts_dir=str(self.test_artifacts_dir))

        # Add all columns
        n_rows = 10
        df = pd.DataFrame(
            {
                "actual_minutes": [1] * n_rows,
                "pts_per_min": [1] * n_rows,
                "reb_per_min": [1] * n_rows,
                "ast_per_min": [1] * n_rows,
                "threes_per_min": [1] * n_rows,
                "player_id": [1] * n_rows,
                "game_id": ["g"] * n_rows,
                "game_date": ["d"] * n_rows,
                "selected_feat_1": [0.5] * n_rows,
            }
        )
        MockFeatureStore.return_value.get_training_dataset.return_value = df
        mock_features = ["selected_feat_1"]
        MockSelector.return_value.select_features.return_value = mock_features

        # Mock predictions for calibration step
        # Use calibrated preds to pass the hard fail check
        mock_preds = self._create_calibrated_preds(n_rows, 1.0)

        pipeline = MockPipeline.return_value
        pipeline.minutes_model.predict_quantiles.return_value = mock_preds
        pipeline.rate_models = {"pts": MagicMock(), "reb": MagicMock(), "ast": MagicMock(), "threes": MagicMock()}
        for m in pipeline.rate_models.values():
            m.predict_quantiles.return_value = mock_preds

        # Sanity Check Mocks
        MockFeatureStore.return_value.get_player_game_features.return_value = {"f": 1}
        MockMC.return_value.predict.return_value = {
            "pts": MagicMock(mean=20, median=20, q10=10, q90=30),
            "reb": MagicMock(mean=5, median=5, q10=2, q90=8),
            "ast": MagicMock(mean=5, median=5, q10=2, q90=8),
        }

        orchestrator.run(["2022"], "2023")

        self.assertEqual(pipeline.minutes_features, mock_features)


if __name__ == "__main__":
    unittest.main()
