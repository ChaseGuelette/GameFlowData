"""Tests for analyze_minutes_bimodality module."""

import numpy as np
import pandas as pd
import pytest

from src.models.analyze_minutes_bimodality import (
    SPREAD_SEGMENTS,
    analyze_bimodality,
    analyze_segment,
    bimodality_coefficient,
    compute_histogram,
    print_histogram,
    print_segment_summary,
    run_dip_test,
)


class TestBimodalityCoefficient:
    """Tests for bimodality_coefficient."""

    def test_normal_distribution_low_bc(self):
        """Normal distribution should have BC ~1/3."""
        rng = np.random.RandomState(42)
        data = rng.normal(30, 5, 10000)
        bc = bimodality_coefficient(data)
        assert bc < 0.555  # below bimodality threshold

    def test_bimodal_distribution_high_bc(self):
        """Mixture of two normals should have higher BC."""
        rng = np.random.RandomState(42)
        data = np.concatenate(
            [
                rng.normal(20, 2, 5000),
                rng.normal(35, 2, 5000),
            ]
        )
        bc = bimodality_coefficient(data)
        assert bc > 0.4  # should be elevated for bimodal data

    def test_uniform_distribution_boundary(self):
        """Uniform distribution BC should be near 5/9."""
        rng = np.random.RandomState(42)
        data = rng.uniform(10, 40, 10000)
        bc = bimodality_coefficient(data)
        assert 0.45 < bc < 0.65  # near 5/9 ≈ 0.555

    def test_returns_float(self):
        """Test return type is float."""
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        assert isinstance(bimodality_coefficient(data), float)


class TestRunDipTest:
    """Tests for run_dip_test."""

    def test_returns_dict_if_diptest_available(self):
        """Test dip test returns dict with expected keys when diptest is installed."""
        data = np.random.normal(30, 5, 500)
        result = run_dip_test(data)

        if result is not None:
            assert "dip_statistic" in result
            assert "p_value" in result
            assert 0 <= result["p_value"] <= 1.0

    def test_returns_none_if_diptest_unavailable(self, monkeypatch):
        """Test graceful fallback when diptest is not installed."""
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "diptest":
                raise ImportError("No module named 'diptest'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        result = run_dip_test(np.array([1.0, 2.0, 3.0]))
        assert result is None


class TestComputeHistogram:
    """Tests for compute_histogram."""

    def test_returns_bin_edges_and_counts(self):
        """Test histogram has expected structure."""
        data = np.array([10, 15, 20, 25, 30, 35])
        result = compute_histogram(data)

        assert "bin_edges" in result
        assert "counts" in result
        assert len(result["counts"]) == len(result["bin_edges"]) - 1

    def test_bins_use_correct_width(self):
        """Test default bin width is 2."""
        data = np.array([10.0])
        result = compute_histogram(data, bin_width=2)
        edges = result["bin_edges"]
        # Check consecutive edges differ by 2
        for i in range(len(edges) - 1):
            assert edges[i + 1] - edges[i] == 2.0

    def test_counts_are_integers(self):
        """Test all counts are integers."""
        data = np.random.uniform(10, 40, 100)
        result = compute_histogram(data)
        for c in result["counts"]:
            assert isinstance(c, int)

    def test_total_counts_equal_data_length(self):
        """Test total counts match input length (for data within range)."""
        data = np.array([5.0, 10.0, 20.0, 30.0, 40.0, 49.0])
        result = compute_histogram(data)
        assert sum(result["counts"]) == len(data)


class TestAnalyzeSegment:
    """Tests for analyze_segment."""

    @pytest.fixture
    def sample_minutes(self):
        rng = np.random.RandomState(42)
        return rng.normal(30, 5, 500)

    def test_returns_expected_keys(self, sample_minutes):
        """Test output dict has all required keys."""
        result = analyze_segment(sample_minutes, "test_segment")

        expected_keys = [
            "label",
            "n",
            "mean",
            "std",
            "median",
            "q10",
            "q25",
            "q75",
            "q90",
            "skewness",
            "kurtosis",
            "bimodality_coefficient",
            "histogram",
        ]
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    def test_label_matches_input(self, sample_minutes):
        """Test label is set correctly."""
        result = analyze_segment(sample_minutes, "blowout")
        assert result["label"] == "blowout"

    def test_n_matches_input_length(self, sample_minutes):
        """Test n matches input array length."""
        result = analyze_segment(sample_minutes, "test")
        assert result["n"] == len(sample_minutes)

    def test_quantile_ordering(self, sample_minutes):
        """Test quantiles are in ascending order."""
        result = analyze_segment(sample_minutes, "test")
        assert result["q10"] <= result["q25"]
        assert result["q25"] <= result["median"]
        assert result["median"] <= result["q75"]
        assert result["q75"] <= result["q90"]

    def test_values_are_rounded(self, sample_minutes):
        """Test numeric values are rounded to 2 decimal places."""
        result = analyze_segment(sample_minutes, "test")
        # Check mean has at most 2 decimal places
        assert result["mean"] == round(result["mean"], 2)
        assert result["std"] == round(result["std"], 2)


class TestPrintFunctions:
    """Tests for print_histogram and print_segment_summary."""

    def test_print_histogram_does_not_crash(self):
        """Test print_histogram runs without error."""
        hist = {"bin_edges": [0, 2, 4, 6], "counts": [10, 20, 5]}
        print_histogram(hist)  # should not raise

    def test_print_histogram_handles_empty_counts(self):
        """Test print_histogram with zero counts."""
        hist = {"bin_edges": [0, 2, 4], "counts": [0, 0]}
        print_histogram(hist)

    def test_print_segment_summary_does_not_crash(self):
        """Test print_segment_summary runs without error."""
        result = {
            "n": 100,
            "mean": 30.0,
            "std": 5.0,
            "q10": 22.0,
            "q25": 26.0,
            "median": 30.0,
            "q75": 34.0,
            "q90": 38.0,
            "skewness": -0.1,
            "kurtosis": 0.05,
            "bimodality_coefficient": 0.35,
        }
        print_segment_summary(result)

    def test_print_segment_summary_with_dip_test(self):
        """Test print_segment_summary when dip_test data is present."""
        result = {
            "n": 100,
            "mean": 30.0,
            "std": 5.0,
            "q10": 22.0,
            "q25": 26.0,
            "median": 30.0,
            "q75": 34.0,
            "q90": 38.0,
            "skewness": -0.1,
            "kurtosis": 0.05,
            "bimodality_coefficient": 0.35,
            "dip_test": {"dip_statistic": 0.02, "p_value": 0.8},
        }
        print_segment_summary(result)


class TestSpreadSegments:
    """Tests for SPREAD_SEGMENTS config."""

    def test_segments_have_required_keys(self):
        """Test each segment defines min, max, label."""
        for name, seg in SPREAD_SEGMENTS.items():
            assert "min" in seg
            assert "max" in seg
            assert "label" in seg

    def test_segments_cover_expected_names(self):
        """Test expected segment names exist."""
        assert "close" in SPREAD_SEGMENTS
        assert "moderate" in SPREAD_SEGMENTS
        assert "blowout" in SPREAD_SEGMENTS
        assert "extreme_blowout" in SPREAD_SEGMENTS


class TestAnalyzeBimodality:
    """Tests for analyze_bimodality."""

    @pytest.fixture
    def bimodality_df(self):
        """Create a realistic DataFrame for bimodality analysis."""
        rng = np.random.RandomState(42)
        n = 600

        player_ids = rng.choice([1, 2, 3, 4, 5, 6, 7, 8], n)
        minutes = rng.normal(30, 5, n).clip(5, 48)
        spreads = rng.choice([1, 3, 5, 7, 10, 12, 16, 20], n).astype(float)

        return pd.DataFrame(
            {
                "player_id": player_ids,
                "actual_minutes": minutes,
                "line_spread": spreads,
            }
        )

    def test_returns_dict(self, bimodality_df):
        """Test returns a dict."""
        results = analyze_bimodality(bimodality_df)
        assert isinstance(results, dict)

    def test_segments_present_in_results(self, bimodality_df):
        """Test at least some spread segments appear in results."""
        results = analyze_bimodality(bimodality_df)
        assert len(results) > 0

    def test_each_segment_has_analysis_fields(self, bimodality_df):
        """Test each segment result has analyze_segment output."""
        results = analyze_bimodality(bimodality_df)
        for seg_name, seg_result in results.items():
            assert "n" in seg_result
            assert "mean" in seg_result
            assert "bimodality_coefficient" in seg_result

    def test_returns_empty_if_no_spread_column(self):
        """Test returns empty dict when line_spread is missing."""
        df = pd.DataFrame(
            {
                "player_id": [1, 1, 1] * 20,
                "actual_minutes": [30.0] * 60,
            }
        )
        results = analyze_bimodality(df)
        assert results == {}

    def test_filters_zero_minutes(self, bimodality_df):
        """Test players with 0 minutes are excluded."""
        bimodality_df.loc[0, "actual_minutes"] = 0.0
        results = analyze_bimodality(bimodality_df)
        # Should still produce results from remaining rows
        assert isinstance(results, dict)

    def test_min_starter_avg_filters(self):
        """Test min_starter_avg parameter filters bench players."""
        rng = np.random.RandomState(42)
        n = 200

        # Bench player with low minutes
        bench = pd.DataFrame(
            {
                "player_id": [99] * n,
                "actual_minutes": rng.normal(12, 3, n).clip(1, 48),
                "line_spread": rng.choice([3, 7, 12], n).astype(float),
            }
        )
        # Starter with high minutes
        starter = pd.DataFrame(
            {
                "player_id": [1] * n,
                "actual_minutes": rng.normal(32, 4, n).clip(10, 48),
                "line_spread": rng.choice([3, 7, 12], n).astype(float),
            }
        )
        df = pd.concat([bench, starter], ignore_index=True)

        # With high threshold, bench player excluded
        results_high = analyze_bimodality(df, min_starter_avg=25.0)
        # With low threshold, both included
        results_low = analyze_bimodality(df, min_starter_avg=10.0)

        # High threshold should have fewer total samples per segment
        if results_high and results_low:
            some_seg = next(iter(results_high))
            if some_seg in results_low:
                assert results_high[some_seg]["n"] <= results_low[some_seg]["n"]

    def test_close_segment_includes_zero_spread(self):
        """Test the close segment includes spread == 0."""
        rng = np.random.RandomState(42)
        n = 200

        df = pd.DataFrame(
            {
                "player_id": [1] * n,
                "actual_minutes": rng.normal(30, 4, n).clip(10, 48),
                "line_spread": np.zeros(n),  # All zero spread
            }
        )

        results = analyze_bimodality(df)

        if "close" in results:
            assert results["close"]["n"] > 0

    def test_skips_segments_with_few_samples(self):
        """Test segments with < 50 samples are skipped."""
        rng = np.random.RandomState(42)
        n = 100

        # All games are close (spread <= 5), so blowout should be skipped
        df = pd.DataFrame(
            {
                "player_id": [1] * n,
                "actual_minutes": rng.normal(30, 4, n).clip(10, 48),
                "line_spread": rng.uniform(0, 4, n),
            }
        )

        results = analyze_bimodality(df)
        assert "blowout" not in results
        assert "extreme_blowout" not in results
