"""Tests for run_backtest module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.backtesting.run_backtest import find_latest_model_dir


class TestFindLatestModelDir:
    """Tests for find_latest_model_dir."""

    def test_raises_if_base_dir_missing(self, tmp_path):
        """Test error when artifacts directory doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Artifacts directory not found"):
            find_latest_model_dir(str(tmp_path / "nonexistent"))

    def test_returns_base_if_contains_model_files(self, tmp_path):
        """Test returns base dir when it directly contains model artifacts."""
        (tmp_path / "minutes_model.joblib").touch()
        result = find_latest_model_dir(str(tmp_path))
        assert result == tmp_path

    def test_finds_latest_run_directory(self, tmp_path):
        """Test finds the most recent nba_run_* subdirectory."""
        (tmp_path / "nba_run_20240101").mkdir()
        (tmp_path / "nba_run_20240201").mkdir()
        (tmp_path / "nba_run_20240301").mkdir()

        result = find_latest_model_dir(str(tmp_path))
        assert result.name == "nba_run_20240301"

    def test_raises_if_no_run_dirs(self, tmp_path):
        """Test error when no nba_run_* directories exist."""
        (tmp_path / "other_dir").mkdir()
        with pytest.raises(FileNotFoundError, match="No nba_run_\\* directories found"):
            find_latest_model_dir(str(tmp_path))

    def test_ignores_non_run_directories(self, tmp_path):
        """Test ignores directories not starting with nba_run_."""
        (tmp_path / "output_123").mkdir()
        (tmp_path / "nba_run_20240101").mkdir()
        (tmp_path / "run_20240101").mkdir()
        (tmp_path / "logs").mkdir()

        result = find_latest_model_dir(str(tmp_path))
        assert result.name == "nba_run_20240101"

    def test_model_files_take_priority_over_run_dirs(self, tmp_path):
        """Test base dir with model files is preferred over nba_run_* subdirs."""
        (tmp_path / "minutes_model.joblib").touch()
        (tmp_path / "nba_run_20240101").mkdir()

        result = find_latest_model_dir(str(tmp_path))
        assert result == tmp_path


class TestMainArgParsing:
    """Tests for main() argument parsing and flow."""

    @patch("src.backtesting.run_backtest.argparse.ArgumentParser.parse_args")
    @patch("src.backtesting.run_backtest.find_latest_model_dir")
    @patch("src.backtesting.run_backtest.get_engine")
    @patch("src.backtesting.run_backtest.FeatureStore")
    @patch("src.backtesting.run_backtest.PlayerPropsModelPipeline")
    @patch("src.backtesting.run_backtest.MonteCarloPredictor")
    @patch("src.backtesting.run_backtest.BacktestHarness")
    def test_main_runs_backtest_with_defaults(
        self,
        mock_harness_cls,
        mock_mc_cls,
        mock_pipeline_cls,
        mock_fs_cls,
        mock_engine,
        mock_find_model,
        mock_parse_args,
        tmp_path,
    ):
        """Test main() orchestrates components correctly."""
        from src.backtesting.run_backtest import main

        output_dir = tmp_path / "output"
        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            model_dir="src/models/artifacts",
            output_dir=str(output_dir),
            n_samples=100,
            stats=["pts"],
            edge_threshold=["0.05"],
            starting_bankroll=10000.0,
            kelly_fraction=0.125,
            max_bet_pct=None,
            bookmakers=["draftkings"],
            allowed_bets=None,

            bl_tau=None,
            bl_sizing_tau=None,
        )
        mock_find_model.return_value = Path("fake/path")

        mock_result = MagicMock()
        mock_result.metrics = MagicMock(
            total_bets=10,
            hit_rate=0.6,
            roi=0.05,
            total_profit=500.0,
            sharpe_ratio=1.2,
            max_drawdown=0.1,
        )
        mock_harness_cls.return_value.run.return_value = mock_result

        main()

        mock_harness_cls.return_value.run.assert_called_once_with(
            start_date="2024-01-01", end_date="2024-01-31"
        )
        mock_result.to_csv.assert_called_once_with(str(output_dir))

    @patch("src.backtesting.run_backtest.argparse.ArgumentParser.parse_args")
    @patch("src.backtesting.run_backtest.find_latest_model_dir")
    @patch("src.backtesting.run_backtest.get_engine")
    @patch("src.backtesting.run_backtest.FeatureStore")
    @patch("src.backtesting.run_backtest.PlayerPropsModelPipeline")
    @patch("src.backtesting.run_backtest.MonteCarloPredictor")
    @patch("src.backtesting.run_backtest.BacktestHarness")
    def test_main_parses_allowed_bets(
        self,
        mock_harness_cls,
        mock_mc_cls,
        mock_pipeline_cls,
        mock_fs_cls,
        mock_engine,
        mock_find_model,
        mock_parse_args,
        tmp_path,
    ):
        """Test main() correctly parses allowed_bets from CLI args."""
        from src.backtesting.run_backtest import main

        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            model_dir="src/models/artifacts",
            output_dir=str(tmp_path),
            n_samples=100,
            stats=["pts", "reb"],
            edge_threshold=["0.05"],
            starting_bankroll=10000.0,
            kelly_fraction=0.125,
            max_bet_pct=None,
            bookmakers=["draftkings"],
            allowed_bets=["pts:under", "reb:over"],

            bl_tau=None,
            bl_sizing_tau=None,
        )
        mock_find_model.return_value = Path("fake/path")
        mock_result = MagicMock()
        mock_result.metrics = MagicMock(
            total_bets=5,
            hit_rate=0.5,
            roi=0.02,
            total_profit=100.0,
            sharpe_ratio=0.8,
            max_drawdown=0.05,
        )
        mock_harness_cls.return_value.run.return_value = mock_result

        main()

        # Verify allowed_bets was parsed and passed to BacktestHarness
        call_kwargs = mock_harness_cls.call_args[1]
        assert call_kwargs["allowed_bets"] == [("pts", "under"), ("reb", "over")]

    @patch("src.backtesting.run_backtest.argparse.ArgumentParser.parse_args")
    @patch("src.backtesting.run_backtest.find_latest_model_dir")
    @patch("src.backtesting.run_backtest.get_engine")
    @patch("src.backtesting.run_backtest.FeatureStore")
    @patch("src.backtesting.run_backtest.PlayerPropsModelPipeline")
    @patch("src.backtesting.run_backtest.MonteCarloPredictor")
    @patch("src.backtesting.run_backtest.BacktestHarness")
    def test_main_creates_timestamped_output_dir(
        self,
        mock_harness_cls,
        mock_mc_cls,
        mock_pipeline_cls,
        mock_fs_cls,
        mock_engine,
        mock_find_model,
        mock_parse_args,
        tmp_path,
        monkeypatch,
    ):
        """Test main() creates timestamped output dir when none specified."""
        from src.backtesting.run_backtest import main

        monkeypatch.chdir(tmp_path)
        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            model_dir="src/models/artifacts",
            output_dir=None,
            n_samples=100,
            stats=["pts"],
            edge_threshold=["0.05"],
            starting_bankroll=10000.0,
            kelly_fraction=0.125,
            max_bet_pct=None,
            bookmakers=["draftkings"],
            allowed_bets=None,

            bl_tau=None,
            bl_sizing_tau=None,
        )
        mock_find_model.return_value = Path("fake/path")
        mock_result = MagicMock()
        mock_result.metrics = MagicMock(
            total_bets=0,
            hit_rate=0.0,
            roi=0.0,
            total_profit=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
        )
        mock_harness_cls.return_value.run.return_value = mock_result

        main()

        # Output dir should have been created under backtest_results/
        assert (tmp_path / "backtest_results").exists()
