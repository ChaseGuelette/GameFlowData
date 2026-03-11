"""Tests for NCAAB Feature Store."""

import pandas as pd
import pytest

from src.models.ncaab_feature_store import (
    SPREAD_FEATURES,
    TOTAL_FEATURES,
    NCAABFeatureStore,
)


class TestComputeDifferentials:
    def setup_method(self):
        from unittest.mock import MagicMock
        self.fs = NCAABFeatureStore(MagicMock())

    def test_rolling_stat_differentials(self):
        """Verify diff_* = home_* - away_*."""
        df = pd.DataFrame([{
            "home_avg_pts_l5": 80.0,
            "away_avg_pts_l5": 70.0,
            "home_avg_pts_l10": 78.0,
            "away_avg_pts_l10": 72.0,
            "home_avg_poss_l5": 70.0,
            "away_avg_poss_l5": 68.0,
            "home_avg_efg_pct_l5": 0.52,
            "away_avg_efg_pct_l5": 0.48,
            "home_avg_tov_pct_l5": 0.15,
            "away_avg_tov_pct_l5": 0.18,
            "home_avg_orb_pct_l5": 0.30,
            "away_avg_orb_pct_l5": 0.28,
            "home_avg_ft_rate_l5": 0.35,
            "away_avg_ft_rate_l5": 0.32,
            "home_avg_opp_efg_pct_l5": 0.45,
            "away_avg_opp_efg_pct_l5": 0.50,
            "home_avg_opp_tov_pct_l5": 0.20,
            "away_avg_opp_tov_pct_l5": 0.17,
        }])

        result = self.fs._compute_differentials(df)

        assert result["diff_avg_pts_l5"].iloc[0] == pytest.approx(10.0)
        assert result["diff_avg_pts_l10"].iloc[0] == pytest.approx(6.0)
        assert result["diff_avg_efg_pct_l5"].iloc[0] == pytest.approx(0.04)
        assert result["diff_avg_tov_pct_l5"].iloc[0] == pytest.approx(-0.03)

    def test_barttorvik_differentials(self):
        """Verify Barttorvik efficiency differentials."""
        df = pd.DataFrame([{
            "home_bart_adj_em": 20.0,
            "away_bart_adj_em": 8.0,
            "home_bart_adj_oe": 115.0,
            "away_bart_adj_oe": 105.0,
            "home_bart_adj_de": 95.0,
            "away_bart_adj_de": 97.0,
            "home_bart_barthag": 0.95,
            "away_bart_barthag": 0.80,
            "home_bart_adj_tempo": 70.0,
            "away_bart_adj_tempo": 66.0,
        }])

        result = self.fs._compute_differentials(df)

        assert result["diff_adj_em"].iloc[0] == pytest.approx(12.0)
        assert result["diff_adj_oe"].iloc[0] == pytest.approx(10.0)
        assert result["diff_adj_de"].iloc[0] == pytest.approx(-2.0)
        assert result["diff_barthag"].iloc[0] == pytest.approx(0.15)
        assert result["avg_tempo_combined"].iloc[0] == pytest.approx(68.0)

    def test_rest_differential(self):
        """Verify rest day differential computation."""
        df = pd.DataFrame([{
            "home_rest_days": 3,
            "away_rest_days": 1,
        }])

        result = self.fs._compute_differentials(df)
        assert result["rest_differential"].iloc[0] == 2

    def test_missing_columns_handled(self):
        """Missing columns should not raise errors."""
        df = pd.DataFrame([{"home_avg_pts_l5": 80.0}])
        result = self.fs._compute_differentials(df)
        assert "avg_tempo_combined" in result.columns  # Should exist but be NaN


class TestFeatureLists:
    def test_spread_features_not_empty(self):
        assert len(SPREAD_FEATURES) > 10

    def test_total_features_not_empty(self):
        assert len(TOTAL_FEATURES) > 10

    def test_spread_features_include_market(self):
        """Spread features should include market signals."""
        assert "line_spread" in SPREAD_FEATURES
        assert "line_total" in SPREAD_FEATURES

    def test_total_features_include_pace(self):
        """Total features should include pace/tempo."""
        assert "avg_tempo_combined" in TOTAL_FEATURES

    def test_spread_features_include_context(self):
        """Spread features should include game context."""
        assert "is_neutral_site" in SPREAD_FEATURES
        assert "is_tournament" in SPREAD_FEATURES

    def test_no_duplicate_features(self):
        """No duplicates in feature lists."""
        assert len(SPREAD_FEATURES) == len(set(SPREAD_FEATURES))
        assert len(TOTAL_FEATURES) == len(set(TOTAL_FEATURES))
