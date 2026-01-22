import sys
from unittest.mock import MagicMock

# MOCK DEPENDENCIES BEFORE IMPORTING SOURCE
# This allows tests to run in environments without xgboost/sklearn installed
mock_xgb = MagicMock()
sys.modules["xgboost"] = mock_xgb
mock_sklearn = MagicMock()
sys.modules["sklearn"] = mock_sklearn
sys.modules["sklearn.model_selection"] = MagicMock()
sys.modules["sklearn.inspection"] = MagicMock()
sys.modules["sklearn.metrics"] = MagicMock()
sys.modules["sklearn.preprocessing"] = MagicMock()

import unittest
import pandas as pd
import numpy as np

# Now import the module under test
from src.processing.feature_selection import ImprovedFeatureSelector, get_candidate_columns

class TestImprovedFeatureSelector(unittest.TestCase):
    
    def setUp(self):
        # Create synthetic time-series data
        dates = pd.date_range(start="2023-01-01", periods=100)
        self.df = pd.DataFrame({
            "game_date": dates,
            "target": np.random.randn(100),
            "feature_good": np.random.randn(100) + np.random.randn(100), 
            "feature_noise": np.random.randn(100),
            "feature_constant": [1]*100,
            "id_col": range(100)
        })
        self.selector = ImprovedFeatureSelector(n_splits=2, quantiles=[0.5])

        # Setup Mock XGBRegressor behavior for ranking
        # We need to ensure when it's instantiated and fit, we can inspect it
        self.mock_xgb_cls = mock_xgb.XGBRegressor
        
        # Setup Mock Permutation Importance
        # It needs to return an object with 'importances_mean'
        self.mock_perm_imp = sys.modules["sklearn.inspection"].permutation_importance
        self.mock_perm_imp.return_value = MagicMock(
            importances_mean=np.array([0.5, 0.0, 0.0]) # good, noise, constant
        )
        
        # Setup TimeSeriesSplit mock to actually yield indices
        self.mock_tscv = sys.modules["sklearn.model_selection"].TimeSeriesSplit
        self.mock_tscv.return_value.split.return_value = [
            (list(range(0, 50)), list(range(50, 100))) # One split
        ]

    def test_feature_ranking_logic(self):
        """Test that ranking uses the permutation importance output."""
        # We control the permutation importance output in setUp
        # The selector sorts by this importance
        
        # We need to make sure the columns match our mock importance order
        # Our mock return value was [0.5, 0.0, 0.0]
        # So we should pass 3 feature columns
        cols = ["feature_good", "feature_noise", "feature_constant"]
        
        ranked = self.selector._rank_features(self.df[cols], self.df["target"])
        
        self.assertEqual(ranked[0], "feature_good")
        self.assertIn("feature_noise", ranked[1:])

    def test_optimization_k_selection(self):
        """Test that optimization loop runs and selects K."""
        # Mock mean_pinball_loss to decrease then increase
        # K=1 -> 0.5, K=2 -> 0.4, K=3 -> 0.6
        mock_loss = sys.modules["sklearn.metrics"].mean_pinball_loss
        mock_loss.side_effect = [0.5, 0.4, 0.6] + [0.6]*100 # Sequence of return values
        
        candidates = ["feature_good", "feature_noise", "feature_constant"]
        
        # We need to force count_to_test to be granular or just verify flow
        # The code calculates counts_to_test = range(5, max, 5).
        # With 3 features, it will just test [3].
        # So optimization loop runs once.
        
        selected = self.selector._optimize_feature_count(
            self.df, self.df["target"], candidates
        )
        
        self.assertEqual(selected, candidates) # Should return all if max < 5

    def test_empty_data_handling(self):
        """Test handling of empty or NaN-only data."""
        df_empty = pd.DataFrame({"target": [np.nan]*10, "feat": [1]*10})
        selected = self.selector.select_features(
            df_empty, "target", ["feat"]
        )
        self.assertEqual(selected, [])

    def test_candidate_generation(self):
        """Test get_candidate_columns exclusion logic."""
        df = pd.DataFrame({
            "player_id": [1],
            "game_date": [1],
            "actual_pts": [1],
            "pts_per_min": [1],
            "valid_stat": [1],
            "is_home": [1] 
        })
        candidates = get_candidate_columns(df, "target")
        
        self.assertNotIn("player_id", candidates)
        self.assertNotIn("actual_pts", candidates)
        self.assertIn("valid_stat", candidates)
        self.assertIn("is_home", candidates)

if __name__ == "__main__":
    unittest.main()