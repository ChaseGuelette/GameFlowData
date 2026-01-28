"""Tests for PerformanceMetrics and MetricsCalculator."""

import numpy as np
import pandas as pd
import pytest

from src.backtesting.performance_metrics import (
    CalibrationResult,
    MetricsCalculator,
    PerformanceMetrics,
)


class TestCalibrationResult:
    """Tests for CalibrationResult dataclass."""

    def test_creation(self):
        """Test creating a calibration result."""
        result = CalibrationResult(
            quantile=0.50,
            target_coverage=0.50,
            actual_coverage=0.48,
            gap=-0.02,
            n_samples=1000,
        )

        assert result.quantile == 0.50
        assert result.gap == -0.02


class TestMetricsCalculator:
    """Tests for MetricsCalculator class."""

    @pytest.fixture
    def calculator(self):
        return MetricsCalculator()

    @pytest.fixture
    def sample_bets_df(self):
        """Create sample bets DataFrame."""
        return pd.DataFrame(
            [
                {"stat": "pts", "edge": 0.08, "outcome": "win", "profit": 90.0, "stake": 100.0},
                {"stat": "pts", "edge": 0.06, "outcome": "loss", "profit": -100.0, "stake": 100.0},
                {"stat": "reb", "edge": 0.10, "outcome": "win", "profit": 100.0, "stake": 100.0},
                {"stat": "reb", "edge": 0.07, "outcome": "win", "profit": 90.0, "stake": 100.0},
                {"stat": "ast", "edge": 0.12, "outcome": "loss", "profit": -100.0, "stake": 100.0},
                {"stat": "ast", "edge": 0.05, "outcome": "push", "profit": 0.0, "stake": 100.0},
            ]
        )

    @pytest.fixture
    def sample_predictions_df(self):
        """Create sample predictions DataFrame with actuals."""
        np.random.seed(42)
        n = 100

        # Generate predictions and actuals
        pred_q50 = np.random.uniform(15, 25, n)
        actuals = pred_q50 + np.random.normal(0, 3, n)

        return pd.DataFrame(
            {
                "player_id": range(n),
                "game_id": [f"game_{i}" for i in range(n)],
                "stat": ["pts"] * n,
                "pred_q10": pred_q50 - 5,
                "pred_q25": pred_q50 - 2,
                "pred_q50": pred_q50,
                "pred_q75": pred_q50 + 2,
                "pred_q90": pred_q50 + 5,
                "actual": actuals,
            }
        )

    def test_calculate_betting_metrics(self, calculator, sample_bets_df):
        """Test betting metrics calculation."""
        metrics = calculator._calculate_betting_metrics(sample_bets_df)

        assert metrics["total_bets"] == 6
        assert metrics["wins"] == 3
        assert metrics["losses"] == 2
        assert metrics["pushes"] == 1
        assert metrics["total_staked"] == 500.0  # 5 bets (excluding push)
        assert metrics["total_profit"] == 80.0  # 90 - 100 + 100 + 90 - 100
        assert metrics["hit_rate"] == 0.6  # 3 / (3 + 2)

    def test_calculate_betting_metrics_empty(self, calculator):
        """Test betting metrics with empty DataFrame."""
        metrics = calculator._calculate_betting_metrics(pd.DataFrame())

        assert metrics["total_bets"] == 0
        assert metrics["roi"] == 0.0

    def test_calculate_risk_metrics(self, calculator, sample_bets_df):
        """Test risk metrics calculation."""
        metrics = calculator._calculate_risk_metrics(sample_bets_df)

        assert "sharpe_ratio" in metrics
        assert "max_drawdown" in metrics
        assert "win_streak" in metrics
        assert "loss_streak" in metrics

    def test_calculate_streaks(self, calculator):
        """Test win/loss streak calculation."""
        outcomes = np.array(["win", "win", "win", "loss", "loss", "win"])
        win_streak, loss_streak = calculator._calculate_streaks(outcomes)

        assert win_streak == 3
        assert loss_streak == 2

    def test_calculate_streaks_empty(self, calculator):
        """Test streaks with empty array."""
        win_streak, loss_streak = calculator._calculate_streaks(np.array([]))

        assert win_streak == 0
        assert loss_streak == 0

    def test_calculate_calibration(self, calculator, sample_predictions_df):
        """Test calibration calculation."""
        results = calculator._calculate_calibration(sample_predictions_df)

        assert len(results) == 5  # 5 quantiles

        # Q50 should have coverage close to 0.50
        q50_result = next(r for r in results if r.quantile == 0.50)
        assert abs(q50_result.actual_coverage - 0.50) < 0.15  # Within 15%

    def test_calculate_calibration_empty(self, calculator):
        """Test calibration with empty DataFrame."""
        results = calculator._calculate_calibration(pd.DataFrame())

        assert len(results) == 0

    def test_calculate_by_stat(self, calculator, sample_bets_df):
        """Test by-stat breakdown."""
        by_stat = calculator._calculate_by_stat(sample_bets_df)

        assert "pts" in by_stat
        assert "reb" in by_stat
        assert "ast" in by_stat

        assert by_stat["pts"]["bets"] == 2
        assert by_stat["reb"]["bets"] == 2
        assert by_stat["ast"]["bets"] == 2

    def test_calculate_by_edge_bucket(self, calculator, sample_bets_df):
        """Test by-edge breakdown."""
        by_edge = calculator._calculate_by_edge_bucket(sample_bets_df)

        assert "0.05-0.10" in by_edge
        assert "0.10-0.15" in by_edge
        assert "0.15-0.20" in by_edge
        assert "0.20+" in by_edge

    def test_full_calculate(self, calculator, sample_predictions_df, sample_bets_df):
        """Test full metrics calculation."""
        metrics = calculator.calculate(sample_predictions_df, sample_bets_df)

        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.total_bets == 6
        assert metrics.wins == 3
        assert len(metrics.calibration_results) == 5


class TestPerformanceMetrics:
    """Tests for PerformanceMetrics dataclass."""

    @pytest.fixture
    def sample_metrics(self):
        return PerformanceMetrics(
            total_bets=100,
            wins=55,
            losses=42,
            pushes=3,
            total_staked=9700.0,
            total_profit=500.0,
            roi=0.0515,
            return_on_capital=0.05,
            hit_rate=0.567,
            sharpe_ratio=1.2,
            max_drawdown=0.15,
            win_streak=8,
            loss_streak=5,
            calibration_results=[
                CalibrationResult(0.10, 0.10, 0.09, -0.01, 100),
                CalibrationResult(0.50, 0.50, 0.48, -0.02, 100),
                CalibrationResult(0.90, 0.90, 0.88, -0.02, 100),
            ],
            overall_calibration_gap=0.017,
            by_stat={"pts": {"bets": 50, "roi": 0.06}},
            by_edge_bucket={"0.05-0.10": {"bets": 60, "roi": 0.04}},
        )

    def test_to_dict(self, sample_metrics):
        """Test converting to dictionary."""
        d = sample_metrics.to_dict()

        assert "betting" in d
        assert "risk" in d
        assert "calibration" in d
        assert d["betting"]["total_bets"] == 100
        assert d["risk"]["sharpe_ratio"] == 1.2

    def test_str(self, sample_metrics):
        """Test string representation."""
        s = str(sample_metrics)

        assert "BACKTEST PERFORMANCE SUMMARY" in s
        assert "Total Bets: 100" in s
        assert "ROI:" in s
        assert "Sharpe Ratio:" in s
