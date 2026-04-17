"""Tests for the backtest parameter sweep script."""

import json
from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from src.backtesting.performance_metrics import PerformanceMetrics
from src.backtesting.run_sweep import (
    SweepConfig,
    SweepResult,
    _config_dir_name,
    build_sweep_grid,
    print_comparison_table,
    run_shared_phases,
    run_single_config,
    save_results,
)

# ---------------------------------------------------------------------------
# SweepConfig tests
# ---------------------------------------------------------------------------

class TestSweepConfig:
    def test_label_with_tau(self):
        config = SweepConfig(tau=0.05, edge_threshold=0.05, kelly_fraction=0.125)
        assert config.label == "tau=0.05 z_max=1.0 | edge=0.05 | kelly=0.125"

    def test_label_with_tau_custom_zmax(self):
        config = SweepConfig(tau=0.05, edge_threshold=0.05, kelly_fraction=0.125, z_max=0.5)
        assert config.label == "tau=0.05 z_max=0.5 | edge=0.05 | kelly=0.125"

    def test_label_no_bl(self):
        config = SweepConfig(tau=None, edge_threshold=0.03, kelly_fraction=0.25)
        assert config.label == "no_BL | edge=0.03 | kelly=0.25"

    def test_to_dict(self):
        config = SweepConfig(tau=0.10, edge_threshold=0.05, kelly_fraction=0.125)
        d = config.to_dict()
        assert d == {"tau": 0.10, "z_max": 1.0, "max_weight": 0.50, "edge_threshold": 0.05, "kelly_fraction": 0.125}

    def test_to_dict_none_tau(self):
        config = SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.125)
        d = config.to_dict()
        assert d["tau"] is None
        assert d["z_max"] == 1.0

    def test_default_z_max(self):
        config = SweepConfig(tau=0.10, edge_threshold=0.05, kelly_fraction=0.125)
        assert config.z_max == 1.0

    def test_custom_z_max(self):
        config = SweepConfig(tau=0.10, edge_threshold=0.05, kelly_fraction=0.125, z_max=2.0)
        assert config.z_max == 2.0


# ---------------------------------------------------------------------------
# build_sweep_grid tests
# ---------------------------------------------------------------------------

class TestBuildSweepGrid:
    def test_single_values(self):
        configs = build_sweep_grid([None], [0.05], [0.125])
        assert len(configs) == 1
        assert configs[0].tau is None
        assert configs[0].edge_threshold == 0.05
        assert configs[0].kelly_fraction == 0.125
        assert configs[0].z_max == 1.0  # default

    def test_cartesian_product(self):
        configs = build_sweep_grid([None, 0.05], [0.03, 0.05], [0.125])
        assert len(configs) == 4  # 2 x 2 x 1 (z_max defaults to [1.0])

    def test_full_grid(self):
        taus = [None, 0.03, 0.05]
        edges = [0.03, 0.05, 0.08]
        kellys = [0.10, 0.125]
        configs = build_sweep_grid(taus, edges, kellys)
        assert len(configs) == 3 * 3 * 2  # 18

    def test_none_tau_in_grid(self):
        configs = build_sweep_grid([None, 0.05], [0.05], [0.125])
        assert configs[0].tau is None
        assert configs[1].tau == 0.05

    def test_preserves_order(self):
        configs = build_sweep_grid([None, 0.03, 0.10], [0.05], [0.125])
        assert [c.tau for c in configs] == [None, 0.03, 0.10]

    def test_z_max_sweep(self):
        """z_max should only multiply configs for non-None tau."""
        configs = build_sweep_grid([0.10], [0.05], [0.125], [0.5, 1.0, 2.0])
        assert len(configs) == 3  # 1 tau x 1 edge x 1 kelly x 3 z_max
        assert [c.z_max for c in configs] == [0.5, 1.0, 2.0]

    def test_z_max_ignored_for_no_bl(self):
        """no-BL configs should only appear once regardless of z_max values."""
        configs = build_sweep_grid([None, 0.10], [0.05], [0.125], [0.5, 1.0])
        # None: 1 config (z_max ignored), 0.10: 2 configs (one per z_max)
        assert len(configs) == 3
        no_bl_configs = [c for c in configs if c.tau is None]
        assert len(no_bl_configs) == 1

    def test_z_max_defaults_to_one(self):
        """When z_max_values not provided, default to [1.0]."""
        configs = build_sweep_grid([0.10], [0.05], [0.125])
        assert len(configs) == 1
        assert configs[0].z_max == 1.0


# ---------------------------------------------------------------------------
# Fixtures for run_shared_phases and run_single_config
# ---------------------------------------------------------------------------

def _make_mock_harness():
    """Create a mock BacktestHarness with stubbed data-loading methods."""
    harness = MagicMock()
    harness._get_game_dates.return_value = [date(2026, 1, 1), date(2026, 1, 2)]
    harness._get_actuals.return_value = pd.DataFrame({
        "player_id": [1, 1, 2, 2],
        "game_id": ["g1", "g1", "g1", "g1"],
        "stat": ["pts", "reb", "pts", "reb"],
        "actual_value": [22.0, 8.0, 15.0, 5.0],
    })
    harness._get_voids.return_value = pd.DataFrame(columns=["player_id", "game_id"])
    harness._prefetch_all_lines.return_value = {}
    return harness


def _make_mock_features():
    """Create a mock feature DataFrame for one date."""
    return pd.DataFrame({
        "player_id": [1, 2],
        "game_id": ["g1", "g1"],
        "player_name": ["Player A", "Player B"],
        "team_id": [100, 200],
        "player_avg_min_l5": [32.0, 28.0],
        "player_avg_pts_l5": [20.0, 15.0],
    })


def _make_mock_predictor():
    """Create a mock predictor that returns deterministic predictions."""
    predictor = MagicMock()

    def mock_predict_batch(features_df, stats=None):
        stats = stats or ["pts", "reb", "ast"]
        predictions = []
        samples_dict = {}
        for _, row in features_df.iterrows():
            for stat in stats:
                mean = 20.0 if stat == "pts" else 8.0
                pred = {
                    "player_id": row["player_id"],
                    "player_name": row.get("player_name", "Unknown"),
                    "game_id": row["game_id"],
                    "team_id": row.get("team_id", 0),
                    "stat": stat,
                    "pred_mean": mean,
                    "pred_std": 3.0,
                    "pred_median": mean,
                    "pred_q10": mean - 5,
                    "pred_q25": mean - 2,
                    "pred_q50": mean,
                    "pred_q75": mean + 2,
                    "pred_q90": mean + 5,
                }
                predictions.append(pred)
                samples_dict[(row["player_id"], row["game_id"], stat)] = np.random.normal(mean, 3, 100)
        return predictions, samples_dict

    predictor.predict_batch_for_date.side_effect = mock_predict_batch
    return predictor


# ---------------------------------------------------------------------------
# run_shared_phases tests
# ---------------------------------------------------------------------------

class TestRunSharedPhases:
    def test_returns_expected_structure(self):
        harness = _make_mock_harness()
        feature_store = MagicMock()
        feature_store.get_features_for_date_range.return_value = {
            date(2026, 1, 1): _make_mock_features(),
        }
        predictor = _make_mock_predictor()

        game_dates, lines, actuals, voids, preds, samples = run_shared_phases(
            harness=harness,
            feature_store=feature_store,
            predictor=predictor,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            stats=["pts", "reb"],
        )

        assert len(game_dates) == 2
        assert isinstance(actuals, pd.DataFrame)
        assert isinstance(voids, pd.DataFrame)
        assert date(2026, 1, 1) in preds
        assert date(2026, 1, 1) in samples

    def test_skips_empty_feature_dates(self):
        harness = _make_mock_harness()
        feature_store = MagicMock()
        feature_store.get_features_for_date_range.return_value = {
            date(2026, 1, 1): pd.DataFrame(),  # Empty
        }
        predictor = _make_mock_predictor()

        _, _, _, _, preds, samples = run_shared_phases(
            harness=harness,
            feature_store=feature_store,
            predictor=predictor,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            stats=["pts"],
        )

        assert len(preds) == 0
        assert len(samples) == 0

    def test_filters_low_minutes(self):
        harness = _make_mock_harness()
        feature_store = MagicMock()

        features = _make_mock_features()
        features.loc[1, "player_avg_min_l5"] = 5.0  # Below threshold
        feature_store.get_features_for_date_range.return_value = {
            date(2026, 1, 1): features,
        }
        predictor = _make_mock_predictor()

        _, _, _, _, preds, _ = run_shared_phases(
            harness=harness,
            feature_store=feature_store,
            predictor=predictor,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            stats=["pts"],
            min_minutes_avg=10,
        )

        # Only 1 player should pass the filter, producing 1 prediction per stat
        if date(2026, 1, 1) in preds:
            pred_df = preds[date(2026, 1, 1)]
            assert len(pred_df) == 1  # 1 player * 1 stat


# ---------------------------------------------------------------------------
# run_single_config tests
# ---------------------------------------------------------------------------

class TestRunSingleConfig:
    def _make_cached_data(self):
        """Build the shared data that run_single_config expects."""
        game_dates = [date(2026, 1, 1)]
        preds_df = pd.DataFrame([
            {
                "player_id": 1, "player_name": "Player A", "game_id": "g1",
                "team_id": 100, "stat": "pts", "pred_mean": 20.0, "pred_std": 3.0,
                "pred_median": 20.0, "pred_q10": 15.0, "pred_q25": 18.0,
                "pred_q50": 20.0, "pred_q75": 22.0, "pred_q90": 25.0,
                "game_date": date(2026, 1, 1),
            },
        ])
        samples = {(1, "g1", "pts"): np.random.normal(20, 3, 100)}

        lines_df = pd.DataFrame({
            "player_id": [1],
            "game_id": ["g1"],
            "market_key": ["player_points"],
            "line": [19.5],
            "bookmaker": ["draftkings"],
            "over_odds": [-110],
            "under_odds": [-110],
        })

        actuals_df = pd.DataFrame({
            "player_id": [1],
            "game_id": ["g1"],
            "stat": ["pts"],
            "actual_value": [22.0],
        })
        voids_df = pd.DataFrame(columns=["player_id", "game_id"])

        return game_dates, preds_df, samples, lines_df, actuals_df, voids_df

    def test_returns_sweep_result(self):
        game_dates, preds_df, samples, lines_df, actuals_df, voids_df = self._make_cached_data()

        config = SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.125)

        result = run_single_config(
            config=config,
            engine=MagicMock(),
            feature_store=MagicMock(),
            model_pipeline=MagicMock(),
            predictor=MagicMock(),
            game_dates=game_dates,
            prefetched_lines={date(2026, 1, 1): lines_df},
            actuals_df=actuals_df,
            voids_df=voids_df,
            date_predictions={date(2026, 1, 1): preds_df},
            date_samples={date(2026, 1, 1): samples},
            stats=["pts"],
            starting_bankroll=10000.0,
            bookmakers=["draftkings"],
            allowed_bets=None,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 1),
        )

        assert isinstance(result, SweepResult)
        assert result.config == config
        assert isinstance(result.metrics, PerformanceMetrics)
        assert result.elapsed_seconds >= 0

    def test_bl_config_creates_blender(self):
        game_dates, preds_df, samples, lines_df, actuals_df, voids_df = self._make_cached_data()
        config = SweepConfig(tau=0.05, edge_threshold=0.05, kelly_fraction=0.125)

        result = run_single_config(
            config=config,
            engine=MagicMock(),
            feature_store=MagicMock(),
            model_pipeline=MagicMock(),
            predictor=MagicMock(),
            game_dates=game_dates,
            prefetched_lines={date(2026, 1, 1): lines_df},
            actuals_df=actuals_df,
            voids_df=voids_df,
            date_predictions={date(2026, 1, 1): preds_df},
            date_samples={date(2026, 1, 1): samples},
            stats=["pts"],
            starting_bankroll=10000.0,
            bookmakers=["draftkings"],
            allowed_bets=None,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 1),
        )

        assert isinstance(result, SweepResult)

    def test_does_not_mutate_cached_predictions(self):
        game_dates, preds_df, samples, lines_df, actuals_df, voids_df = self._make_cached_data()

        original_cols = set(preds_df.columns)
        original_len = len(preds_df)

        config = SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.125)

        run_single_config(
            config=config,
            engine=MagicMock(),
            feature_store=MagicMock(),
            model_pipeline=MagicMock(),
            predictor=MagicMock(),
            game_dates=game_dates,
            prefetched_lines={date(2026, 1, 1): lines_df},
            actuals_df=actuals_df,
            voids_df=voids_df,
            date_predictions={date(2026, 1, 1): preds_df},
            date_samples={date(2026, 1, 1): samples},
            stats=["pts"],
            starting_bankroll=10000.0,
            bookmakers=["draftkings"],
            allowed_bets=None,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 1),
        )

        # Verify cached data was not mutated
        assert set(preds_df.columns) == original_cols
        assert len(preds_df) == original_len


# ---------------------------------------------------------------------------
# print_comparison_table tests
# ---------------------------------------------------------------------------

class TestPrintComparisonTable:
    def _make_dummy_metrics(self, roi=0.05, bets=100) -> PerformanceMetrics:
        return PerformanceMetrics(
            total_bets=bets,
            wins=55,
            losses=45,
            pushes=0,
            total_staked=5000.0,
            total_profit=roi * 5000.0,
            roi=roi,
            return_on_capital=roi * 5000.0 / 10000.0,
            hit_rate=0.55,
            sharpe_ratio=1.5,
            max_drawdown=0.08,
            win_streak=5,
            loss_streak=3,
            calibration_results=[],
            overall_calibration_gap=0.02,
            by_stat={"pts": {"bets": 50, "roi": 0.03, "hit_rate": 0.54}},
            by_edge_bucket={},
        )

    def test_prints_without_error(self, capsys):
        results = [
            SweepResult(
                config=SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.125),
                metrics=self._make_dummy_metrics(roi=-0.05, bets=200),
                bets_df=pd.DataFrame(),
                predictions_df=pd.DataFrame(),
                all_edges_df=pd.DataFrame(),
                elapsed_seconds=1.5,
            ),
            SweepResult(
                config=SweepConfig(tau=0.05, edge_threshold=0.05, kelly_fraction=0.125),
                metrics=self._make_dummy_metrics(roi=0.03, bets=150),
                bets_df=pd.DataFrame(),
                predictions_df=pd.DataFrame(),
                all_edges_df=pd.DataFrame(),
                elapsed_seconds=1.2,
            ),
        ]

        print_comparison_table(
            results,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=120.0,
            total_predictions=5000,
            total_dates=29,
        )

        captured = capsys.readouterr()
        assert "BACKTEST SWEEP" in captured.out
        assert "no_BL" in captured.out
        assert "tau=0.05" in captured.out
        assert "Best ROI" in captured.out

    def test_handles_empty_results(self, capsys):
        print_comparison_table(
            [],
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=10.0,
            total_predictions=0,
            total_dates=0,
        )
        captured = capsys.readouterr()
        assert "BACKTEST SWEEP" in captured.out


# ---------------------------------------------------------------------------
# save_results tests
# ---------------------------------------------------------------------------

class TestSaveResults:
    def _make_dummy_result(self, tau=None) -> SweepResult:
        bets_df = pd.DataFrame({
            "player_id": [1], "game_id": ["g1"], "game_date": ["2026-01-01"],
            "stat": ["pts"], "side": ["over"], "line": [19.5], "odds": [-110],
            "stake": [100.0], "model_prob": [0.55], "implied_prob": [0.50],
            "edge": [0.05], "actual": [22.0], "outcome": ["win"], "profit": [90.91],
        })
        predictions_df = pd.DataFrame({
            "player_id": [1], "game_id": ["g1"], "stat": ["pts"],
            "pred_mean": [20.0], "player_name": ["Player A"],
        })
        edges_df = pd.DataFrame({
            "player_id": [1, 1], "game_id": ["g1", "g1"], "stat": ["pts", "pts"],
            "bookmaker": ["draftkings", "fanduel"], "line": [19.5, 20.5],
            "over_odds": [-110, -105], "under_odds": [-110, -115],
            "over_edge": [0.05, 0.03], "under_edge": [-0.02, 0.01],
            "game_date": ["2026-01-01", "2026-01-01"],
        })
        return SweepResult(
            config=SweepConfig(tau=tau, edge_threshold=0.05, kelly_fraction=0.125),
            metrics=PerformanceMetrics(
                total_bets=100, wins=55, losses=45, pushes=0,
                total_staked=5000.0, total_profit=250.0,
                roi=0.05, return_on_capital=0.025, hit_rate=0.55,
                sharpe_ratio=1.5, max_drawdown=0.08,
                win_streak=5, loss_streak=3,
                calibration_results=[], overall_calibration_gap=0.02,
                by_stat={"pts": {"bets": 50, "roi": 0.03, "hit_rate": 0.54}},
                by_edge_bucket={},
            ),
            bets_df=bets_df,
            predictions_df=predictions_df,
            all_edges_df=edges_df,
            elapsed_seconds=2.0,
        )

    def test_creates_json_and_csv(self, tmp_path):
        results = [self._make_dummy_result(None), self._make_dummy_result(0.05)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=120.0,
            total_predictions=5000,
            total_dates=29,
            sweep_grid_size=2,
        )

        json_path = tmp_path / "sweep_results.json"
        csv_path = tmp_path / "sweep_summary.csv"

        assert json_path.exists()
        assert csv_path.exists()

    def test_json_structure(self, tmp_path):
        results = [self._make_dummy_result(0.05)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=1,
        )

        with open(tmp_path / "sweep_results.json") as f:
            data = json.load(f)

        assert "sweep_metadata" in data
        assert "results" in data
        assert data["sweep_metadata"]["total_configs"] == 1
        assert data["results"][0]["config"]["tau"] == 0.05

    def test_csv_columns(self, tmp_path):
        results = [self._make_dummy_result(None)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=1,
        )

        df = pd.read_csv(tmp_path / "sweep_summary.csv")
        assert "tau" in df.columns
        assert "edge_threshold" in df.columns
        assert "kelly_fraction" in df.columns
        assert "roi" in df.columns
        assert "sharpe_ratio" in df.columns
        assert "total_bets" in df.columns
        assert "pts_roi" in df.columns

    def test_creates_per_config_subdirectories(self, tmp_path):
        results = [self._make_dummy_result(None), self._make_dummy_result(0.05)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=2,
            starting_bankroll=10000.0,
            bookmakers=["draftkings"],
            stats=["pts"],
        )

        # Check config subdirectories exist
        config1_dir = tmp_path / "config_01_no_BL_edge0.05_kelly0.125"
        config2_dir = tmp_path / "config_02_tau0.05_zmax1.0_edge0.05_kelly0.125"

        assert config1_dir.is_dir()
        assert config2_dir.is_dir()

        # Check all expected files exist in each config dir
        for config_dir in [config1_dir, config2_dir]:
            assert (config_dir / "bets.csv").exists()
            assert (config_dir / "predictions.csv").exists()
            assert (config_dir / "all_bookmaker_edges.csv").exists()
            assert (config_dir / "metrics.json").exists()

    def test_per_config_metrics_json_has_config_key(self, tmp_path):
        results = [self._make_dummy_result(0.10)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=1,
            starting_bankroll=15000.0,
            bookmakers=["draftkings", "fanduel"],
            stats=["pts", "reb"],
        )

        config_dir = tmp_path / "config_01_tau0.1_zmax1.0_edge0.05_kelly0.125"
        with open(config_dir / "metrics.json") as f:
            data = json.load(f)

        # visualize_results.py expects config.starting_bankroll
        assert "config" in data
        assert data["config"]["starting_bankroll"] == 15000.0
        assert data["config"]["bl_tau"] == 0.10
        assert data["config"]["bl_z_max"] == 1.0
        assert data["config"]["edge_threshold"] == 0.05
        assert data["config"]["kelly_fraction"] == 0.125
        assert data["config"]["bookmakers"] == ["draftkings", "fanduel"]
        assert data["config"]["stats"] == ["pts", "reb"]

        # Also has standard metrics sections
        assert "betting" in data
        assert "risk" in data
        assert "calibration" in data

    def test_per_config_bets_csv_content(self, tmp_path):
        results = [self._make_dummy_result(None)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=1,
        )

        config_dir = tmp_path / "config_01_no_BL_edge0.05_kelly0.125"
        bets = pd.read_csv(config_dir / "bets.csv")
        assert len(bets) == 1
        assert "player_id" in bets.columns
        assert "outcome" in bets.columns

    def test_per_config_edges_csv_content(self, tmp_path):
        results = [self._make_dummy_result(None)]

        save_results(
            results,
            output_dir=tmp_path,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 29),
            phase01_time=100.0,
            total_predictions=3000,
            total_dates=20,
            sweep_grid_size=1,
        )

        config_dir = tmp_path / "config_01_no_BL_edge0.05_kelly0.125"
        edges = pd.read_csv(config_dir / "all_bookmaker_edges.csv")
        assert len(edges) == 2  # two bookmakers
        assert "bookmaker" in edges.columns
        assert "over_edge" in edges.columns


# ---------------------------------------------------------------------------
# _config_dir_name tests
# ---------------------------------------------------------------------------

class TestConfigDirName:
    def test_no_bl(self):
        config = SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.125)
        assert _config_dir_name(1, config) == "config_01_no_BL_edge0.05_kelly0.125"

    def test_with_tau(self):
        config = SweepConfig(tau=0.10, edge_threshold=0.03, kelly_fraction=0.15)
        assert _config_dir_name(3, config) == "config_03_tau0.1_zmax1.0_edge0.03_kelly0.15"

    def test_with_tau_custom_zmax(self):
        config = SweepConfig(tau=0.10, edge_threshold=0.03, kelly_fraction=0.15, z_max=0.5)
        assert _config_dir_name(3, config) == "config_03_tau0.1_zmax0.5_edge0.03_kelly0.15"

    def test_zero_padded_index(self):
        config = SweepConfig(tau=0.05, edge_threshold=0.05, kelly_fraction=0.125)
        assert _config_dir_name(9, config).startswith("config_09_")
