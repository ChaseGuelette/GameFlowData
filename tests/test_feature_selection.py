import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from src.processing.feature_selection import ImprovedFeatureSelector, get_candidate_columns


class TestImprovedFeatureSelector(unittest.TestCase):
    def setUp(self):
        # Create synthetic time-series data
        dates = pd.date_range(start="2023-01-01", periods=100)
        self.df = pd.DataFrame(
            {
                "game_date": dates,
                "target": np.random.randn(100),
                "feature_good": np.random.randn(100) + np.random.randn(100),
                "feature_noise": np.random.randn(100),
                "feature_constant": [1] * 100,
                "id_col": range(100),
            }
        )
        self.selector = ImprovedFeatureSelector(n_splits=2, quantiles=[0.5])

    @patch("src.processing.feature_selection.xgb.XGBRegressor")
    @patch("src.processing.feature_selection.permutation_importance")
    @patch("src.processing.feature_selection.TimeSeriesSplit")
    def test_feature_ranking_logic(self, mock_tscv, mock_perm_imp, mock_xgb_cls):
        """Test that ranking uses the permutation importance output."""
        # Setup TimeSeriesSplit mock to actually yield indices
        mock_tscv.return_value.split.return_value = [
            (list(range(0, 50)), list(range(50, 100)))  # One split
        ]

        # Setup Mock Permutation Importance
        mock_perm_imp.return_value = MagicMock(
            importances_mean=np.array([0.5, 0.0, 0.0])  # good, noise, constant
        )

        cols = ["feature_good", "feature_noise", "feature_constant"]

        ranked = self.selector._rank_features_for_quantile(self.df[cols], self.df["target"], 0.5)

        self.assertEqual(ranked[0], "feature_good")
        self.assertIn("feature_noise", ranked[1:])

    @patch("src.processing.feature_selection.xgb.XGBRegressor")
    @patch("src.processing.feature_selection.mean_pinball_loss")
    @patch("src.processing.feature_selection.TimeSeriesSplit")
    def test_optimization_k_selection(self, mock_tscv, mock_loss, mock_xgb_cls):
        """Test that optimization loop runs and selects K."""
        mock_tscv.return_value.split.return_value = [(list(range(0, 50)), list(range(50, 100)))]

        # Mock mean_pinball_loss to decrease then increase
        mock_loss.side_effect = [0.5, 0.4, 0.6] + [0.6] * 100

        candidates = ["feature_good", "feature_noise", "feature_constant"]

        selected = self.selector._optimize_count_for_quantile(self.df, self.df["target"], candidates, 0.5)

        self.assertEqual(selected, candidates)  # Should return all if max < 5

    @patch("src.processing.feature_selection.xgb.XGBRegressor")
    @patch("src.processing.feature_selection.permutation_importance")
    @patch("src.processing.feature_selection.TimeSeriesSplit")
    @patch("src.processing.feature_selection.mean_pinball_loss")
    def test_empty_data_handling(self, mock_loss, mock_tscv, mock_perm_imp, mock_xgb_cls):
        """Test handling of empty or NaN-only data."""
        mock_tscv.return_value.split.return_value = []

        df_empty = pd.DataFrame({"target": [np.nan] * 10, "feat": [1] * 10})
        selected = self.selector.select_features_per_quantile(df_empty, "target", ["feat"])

        # Should return dict with empty lists for each quantile
        for q in self.selector.quantiles:
            self.assertEqual(selected[q], [])

    def test_candidate_generation(self):
        """Test get_candidate_columns exclusion logic."""
        df = pd.DataFrame(
            {
                "player_id": [1],
                "game_date": [1],
                "actual_pts": [1],
                "pts_per_min": [1],
                "valid_stat": [1],
                "is_home": [1],
            }
        )
        candidates = get_candidate_columns(df, "target")

        self.assertNotIn("player_id", candidates)
        self.assertNotIn("actual_pts", candidates)
        self.assertIn("valid_stat", candidates)
        self.assertIn("is_home", candidates)


if __name__ == "__main__":
    unittest.main()
