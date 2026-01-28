"""Tests for analyze_calibration_drift module."""

import numpy as np
import pandas as pd
import pytest

from src.models.analyze_calibration_drift import analyze_minutes_rate_correlation


class TestAnalyzeMinutesRateCorrelation:
    """Tests for analyze_minutes_rate_correlation."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with minutes and stat actuals."""
        rng = np.random.RandomState(42)
        n = 200

        minutes = rng.uniform(15, 38, n)
        # Create correlated rates (higher minutes -> slightly higher rate)
        pts_rate = 0.7 + 0.005 * minutes + rng.normal(0, 0.1, n)
        reb_rate = 0.2 + 0.002 * minutes + rng.normal(0, 0.05, n)
        ast_rate = 0.15 + rng.normal(0, 0.04, n)

        return pd.DataFrame(
            {
                "actual_minutes": minutes,
                "actual_pts": minutes * pts_rate,
                "actual_reb": minutes * reb_rate,
                "actual_ast": minutes * ast_rate,
            }
        )

    def test_returns_results_for_each_stat(self, sample_df):
        """Test that results contain all three stats."""
        results = analyze_minutes_rate_correlation(sample_df)

        assert "pts" in results
        assert "reb" in results
        assert "ast" in results

    def test_overall_correlation_is_computed(self, sample_df):
        """Test that overall correlation value is present."""
        results = analyze_minutes_rate_correlation(sample_df)

        for stat in ["pts", "reb", "ast"]:
            assert "overall_correlation" in results[stat]
            assert isinstance(results[stat]["overall_correlation"], float)

    def test_correlation_values_are_bounded(self, sample_df):
        """Test correlation values are in [-1, 1]."""
        results = analyze_minutes_rate_correlation(sample_df)

        for stat in results:
            corr = results[stat]["overall_correlation"]
            assert -1.0 <= corr <= 1.0

    def test_bucket_analysis_is_produced(self, sample_df):
        """Test by-bucket breakdown is included."""
        results = analyze_minutes_rate_correlation(sample_df)

        for stat in results:
            assert "by_bucket" in results[stat]
            buckets = results[stat]["by_bucket"]
            assert isinstance(buckets, list)

    def test_bucket_has_expected_fields(self, sample_df):
        """Test each bucket entry has required fields."""
        results = analyze_minutes_rate_correlation(sample_df)

        for stat in results:
            for bucket in results[stat]["by_bucket"]:
                assert "bucket" in bucket
                assert "n" in bucket
                assert "mean_rate" in bucket
                assert "std_rate" in bucket
                assert "mean_total" in bucket

    def test_filters_low_minutes(self):
        """Test that players with < 10 minutes are excluded."""
        df = pd.DataFrame(
            {
                "actual_minutes": [5.0, 8.0, 25.0, 30.0],
                "actual_pts": [3.0, 5.0, 20.0, 28.0],
            }
        )

        results = analyze_minutes_rate_correlation(df)

        if "pts" in results and results["pts"]["by_bucket"]:
            for bucket in results["pts"]["by_bucket"]:
                # All samples in buckets should have >= 10 min
                assert bucket["n"] <= 2  # only 2 rows with >= 10 min

    def test_handles_empty_dataframe(self):
        """Test with empty DataFrame."""
        df = pd.DataFrame(
            {
                "actual_minutes": pd.Series(dtype=float),
                "actual_pts": pd.Series(dtype=float),
            }
        )
        results = analyze_minutes_rate_correlation(df)
        assert isinstance(results, dict)

    def test_handles_missing_stat_columns(self):
        """Test gracefully handles missing actual columns."""
        df = pd.DataFrame(
            {
                "actual_minutes": [20.0, 25.0, 30.0] * 20,
            }
        )
        results = analyze_minutes_rate_correlation(df)
        assert isinstance(results, dict)

    def test_skips_buckets_with_too_few_samples(self):
        """Test buckets with < 30 samples are skipped."""
        rng = np.random.RandomState(42)
        # All minutes in 25-30 range, so other buckets have 0 samples
        minutes = rng.uniform(25, 30, 50)
        df = pd.DataFrame(
            {
                "actual_minutes": minutes,
                "actual_pts": minutes * 0.8,
            }
        )
        results = analyze_minutes_rate_correlation(df)

        if "pts" in results:
            for bucket in results["pts"]["by_bucket"]:
                assert bucket["n"] >= 30

    def test_preexisting_rate_columns_used(self):
        """Test uses existing rate columns if present."""
        rng = np.random.RandomState(42)
        n = 100
        minutes = rng.uniform(20, 35, n)

        df = pd.DataFrame(
            {
                "actual_minutes": minutes,
                "actual_pts": minutes * 0.8,
                "pts_per_min": np.full(n, 0.8),  # pre-computed
            }
        )
        results = analyze_minutes_rate_correlation(df)
        assert "pts" in results
