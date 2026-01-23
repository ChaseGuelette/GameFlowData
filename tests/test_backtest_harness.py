"""Tests for BacktestHarness class."""

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.backtesting.backtest_harness import BacktestHarness, BacktestResult


class MockPropPrediction:
    """Mock PropPrediction for testing."""

    def __init__(self, stat: str, mean: float = 20.0):
        self.stat = stat
        self.mean = mean
        self.median = mean
        self.q10 = mean - 5
        self.q25 = mean - 2
        self.q50 = mean
        self.q75 = mean + 2
        self.q90 = mean + 5
        self.samples = np.random.normal(mean, 3, 1000)


class TestBacktestHarness:
    """Tests for BacktestHarness class."""

    @pytest.fixture
    def mock_engine(self):
        """Create mock SQLAlchemy engine."""
        engine = MagicMock()
        return engine

    @pytest.fixture
    def mock_feature_store(self):
        """Create mock FeatureStore."""
        store = MagicMock()
        store.get_player_game_features.return_value = {
            "player_avg_pts_l5": 20.0,
            "player_avg_min_l5": 30.0,
            "team_avg_pace_l5": 100.0,
        }
        return store

    @pytest.fixture
    def mock_pipeline(self):
        """Create mock model pipeline."""
        pipeline = MagicMock()
        pipeline.minutes_features = ["player_avg_min_l5"]
        pipeline.rate_features = ["player_avg_pts_l5"]
        return pipeline

    @pytest.fixture
    def mock_predictor(self):
        """Create mock MonteCarloPredictor."""
        predictor = MagicMock()

        def mock_predict(player_id, game_id, features, stats=None):
            stats = stats or ["pts", "reb", "ast"]
            return {stat: MockPropPrediction(stat) for stat in stats}

        predictor.predict.side_effect = mock_predict
        return predictor

    @pytest.fixture
    def harness(self, mock_engine, mock_feature_store, mock_pipeline, mock_predictor):
        """Create BacktestHarness with mocks."""
        return BacktestHarness(
            engine=mock_engine,
            feature_store=mock_feature_store,
            model_pipeline=mock_pipeline,
            predictor=mock_predictor,
            edge_threshold=0.05,
            starting_bankroll=10000.0,
        )

    def test_initialization(self, harness):
        """Test harness initialization."""
        assert harness.edge_threshold == 0.05
        assert harness.starting_bankroll == 10000.0
        assert harness._simulator is not None
        assert harness._metrics_calc is not None

    def test_get_game_dates(self, harness, mock_engine):
        """Test getting game dates from database."""
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([(date(2024, 1, 15),), (date(2024, 1, 16),), (date(2024, 1, 17),)])

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        dates = harness._get_game_dates(date(2024, 1, 15), date(2024, 1, 17))

        assert len(dates) == 3
        assert dates[0] == date(2024, 1, 15)

    def test_get_games_for_date(self, harness, mock_engine):
        """Test getting games for a specific date."""
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([("0022400001",), ("0022400002",)])

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        games = harness._get_games_for_date(date(2024, 1, 15))

        assert len(games) == 2
        assert games[0]["game_id"] == "0022400001"

    def test_get_players_for_date(self, harness, mock_engine):
        """Test getting players for a specific date."""
        mock_row1 = MagicMock()
        mock_row1._mapping = {
            "player_id": 1,
            "game_id": "0022400001",
            "team_id": 100,
            "player_name": "Player One",
        }

        mock_row2 = MagicMock()
        mock_row2._mapping = {
            "player_id": 2,
            "game_id": "0022400001",
            "team_id": 101,
            "player_name": "Player Two",
        }

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row1, mock_row2])

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        players = harness._get_players_for_date(date(2024, 1, 15))

        assert len(players) == 2
        assert players[0]["player_id"] == 1

    def test_calculate_edges(self, harness):
        """Test edge calculation."""
        predictions_df = pd.DataFrame(
            [
                {
                    "player_id": 1,
                    "game_id": "001",
                    "stat": "pts",
                    "pred_q10": 15.0,
                    "pred_q25": 18.0,
                    "pred_q50": 20.0,
                    "pred_q75": 22.0,
                    "pred_q90": 25.0,
                }
            ]
        )

        lines_df = pd.DataFrame(
            [
                {
                    "player_id": 1,
                    "game_id": "001",
                    "market_key": "player_points",
                    "line": 19.5,
                    "over_odds": -110,
                    "under_odds": -110,
                }
            ]
        )

        result = harness._calculate_edges(predictions_df, lines_df)

        assert "over_prob" in result.columns
        assert "under_prob" in result.columns
        assert "over_edge" in result.columns
        assert "implied_over" in result.columns

        # Line 19.5 is between q25 (18) and q50 (20), so over_prob should be > 0.5
        assert result.iloc[0]["over_prob"] > 0.5

    def test_get_actuals(self, harness, mock_engine):
        """Test getting actual outcomes."""
        mock_df = pd.DataFrame(
            [
                {"player_id": 1, "game_id": "001", "pts": 25, "reb": 8, "ast": 5, "threes": 3},
                {"player_id": 2, "game_id": "001", "pts": 18, "reb": 10, "ast": 3, "threes": 1},
            ]
        )

        with patch("pandas.read_sql", return_value=mock_df):
            actuals = harness._get_actuals(date(2024, 1, 15), date(2024, 1, 15))

        # Should be melted to long format
        assert "stat" in actuals.columns
        assert "actual_value" in actuals.columns

    def test_merge_actuals(self, harness):
        """Test merging actuals with predictions."""
        predictions_df = pd.DataFrame(
            [
                {"player_id": 1, "game_id": "001", "stat": "pts", "pred_mean": 20.0},
                {"player_id": 1, "game_id": "001", "stat": "reb", "pred_mean": 8.0},
            ]
        )

        actuals_df = pd.DataFrame(
            [
                {"player_id": 1, "game_id": "001", "stat": "pts", "actual_value": 25},
                {"player_id": 1, "game_id": "001", "stat": "reb", "actual_value": 10},
            ]
        )

        merged = harness._merge_actuals(predictions_df, actuals_df)

        assert "actual" in merged.columns
        assert merged.iloc[0]["actual"] == 25


class TestBacktestResult:
    """Tests for BacktestResult class."""

    @pytest.fixture
    def sample_result(self, tmp_path):
        """Create sample BacktestResult."""
        from src.backtesting.performance_metrics import CalibrationResult, PerformanceMetrics

        metrics = PerformanceMetrics(
            total_bets=10,
            wins=6,
            losses=4,
            pushes=0,
            total_staked=1000.0,
            total_profit=100.0,
            roi=0.10,
            hit_rate=0.60,
            sharpe_ratio=1.5,
            max_drawdown=0.05,
            win_streak=3,
            loss_streak=2,
            calibration_results=[
                CalibrationResult(0.50, 0.50, 0.48, -0.02, 100),
            ],
            overall_calibration_gap=0.02,
            by_stat={},
            by_edge_bucket={},
        )

        return BacktestResult(
            predictions_df=pd.DataFrame({"player_id": [1], "pred_mean": [20.0]}),
            bets_df=pd.DataFrame({"player_id": [1], "profit": [100.0]}),
            metrics=metrics,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            config={"edge_threshold": 0.05},
        )

    def test_to_csv(self, sample_result, tmp_path):
        """Test exporting results to CSV."""
        output_dir = tmp_path / "results"

        sample_result.to_csv(str(output_dir))

        assert (output_dir / "predictions.csv").exists()
        assert (output_dir / "bets.csv").exists()
        assert (output_dir / "metrics.json").exists()
