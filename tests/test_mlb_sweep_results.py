"""Behavior tests for MLB sweep result serialization."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd

from src.backtesting.mlb.sweep_config import SweepConfig
from src.backtesting.performance_metrics import PerformanceMetrics


def _metrics() -> PerformanceMetrics:
    return PerformanceMetrics(
        total_bets=12,
        wins=7,
        losses=4,
        pushes=1,
        total_staked=1200.456,
        total_profit=123.456,
        roi=0.10288,
        return_on_capital=0.012345,
        hit_rate=0.63636,
        sharpe_ratio=1.23456,
        max_drawdown=0.07891,
        win_streak=4,
        loss_streak=2,
        calibration_results=[],
        overall_calibration_gap=0.0,
        by_stat={
            "pitcher_strikeouts": {"bets": 8, "roi": 0.11119, "hit_rate": 0.625},
            "batter_hits": {"bets": 4, "roi": -0.05001, "hit_rate": 0.5},
        },
        by_edge_bucket={},
    )


def test_save_results_writes_summary_json_and_per_config_artifacts(tmp_path):
    from src.backtesting.mlb.sweep_results import SweepResult, save_results

    result = SweepResult(
        config=SweepConfig(
            tau=None,
            edge_threshold=0.08,
            kelly_fraction=0.125,
            flat_bet_size=25.0,
            z_max=2.5,
            max_weight=0.35,
        ),
        metrics=_metrics(),
        bets_df=pd.DataFrame([{"player_id": 1, "stake": 25.0, "profit": 22.7}]),
        predictions_df=pd.DataFrame([{"player_id": 1, "pred_q50": 6.0, "actual": 7.0}]),
        elapsed_seconds=3.456,
    )

    save_results(
        [result],
        output_dir=tmp_path,
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 2),
        phase01_time=12.34,
        total_predictions=99,
        total_dates=2,
        starting_bankroll=10000.0,
    )

    sweep_json = json.loads((tmp_path / "sweep_results.json").read_text())
    assert sweep_json["sweep_metadata"] == {
        "start_date": "2025-07-01",
        "end_date": "2025-07-02",
        "game_dates": 2,
        "total_predictions": 99,
        "total_configs": 1,
        "phase01_time_seconds": 12.3,
    }
    assert sweep_json["results"][0]["config"]["tau"] is None
    assert sweep_json["results"][0]["elapsed_seconds"] == 3.46

    summary = pd.read_csv(tmp_path / "sweep_summary.csv")
    row = summary.iloc[0].to_dict()
    assert row["tau"] != row["tau"]  # pandas reads None/blank as NaN, preserving old CSV behavior.
    assert row["edge_threshold"] == 0.08
    assert row["kelly_fraction"] == 0.125
    assert row["flat_bet_size"] == 25.0
    assert row["hit_rate"] == 0.6364
    assert row["roi"] == 0.1029
    assert row["return_on_capital"] == 0.0123
    assert row["total_profit"] == 123.46
    assert row["total_staked"] == 1200.46
    assert row["sharpe_ratio"] == 1.235
    assert row["max_drawdown"] == 0.0789
    assert row["pitcher_strikeouts_bets"] == 8
    assert row["pitcher_strikeouts_roi"] == 0.1112
    assert row["pitcher_strikeouts_hit_rate"] == 0.625

    config_dir = tmp_path / "config_01_no_BL_edge0.08_kelly0.125"
    assert (config_dir / "bets.csv").exists()
    assert (config_dir / "predictions.csv").exists()
    metrics_json = json.loads((config_dir / "metrics.json").read_text())
    assert metrics_json["config"]["edge_threshold"] == 0.08
    assert metrics_json["betting"]["total_bets"] == 12


def test_save_results_records_promotion_contract_metadata_when_provided(tmp_path):
    from src.backtesting.mlb.sweep_results import SweepResult, save_results

    promotion_metadata = {
        "promotion_grade": True,
        "evidence_label": "promotion_grade_quote_clean",
        "quote_clean": {
            "enabled": True,
            "cutoff_time_et": "13:30",
            "decision_policy": "slate_or_tminus",
            "relative_minutes": 60,
            "line_source": "mlb_raw_player_props",
        },
        "line_source": "mlb_raw_player_props",
        "quote_decision_policy": "slate_or_tminus",
        "dense_clv_linked_coverage_audit_required": False,
        "dense_clv_linked_coverage_audit_note": None,
        "warnings": [],
    }
    result = SweepResult(
        config=SweepConfig(tau=None, edge_threshold=0.08, kelly_fraction=0.125),
        metrics=_metrics(),
        bets_df=pd.DataFrame(),
        predictions_df=pd.DataFrame(),
        elapsed_seconds=1.0,
    )

    save_results(
        [result],
        output_dir=tmp_path,
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 2),
        phase01_time=12.34,
        total_predictions=99,
        total_dates=2,
        promotion_metadata=promotion_metadata,
    )

    sweep_json = json.loads((tmp_path / "sweep_results.json").read_text())
    assert sweep_json["sweep_metadata"]["promotion_contract"] == promotion_metadata


def test_save_results_writes_bets_and_predictions_verbatim_for_cli_audits(tmp_path):
    from src.backtesting.mlb.sweep_results import SweepResult, save_results

    result = SweepResult(
        config=SweepConfig(tau=None, edge_threshold=0.08, kelly_fraction=0.125),
        metrics=_metrics(),
        bets_df=pd.DataFrame([{
            "player_id": 1,
            "bookmaker": "LowVigBook",
            "selected_snapshot_time": "2025-07-01T16:04:00Z",
            "selected_decision_time": "2025-07-01T16:03:00Z",
            "quote_decision_policy": "slate_or_tminus:20:00ET:-120m",
            "stake": 25.0,
            "profit": 22.7,
        }]),
        predictions_df=pd.DataFrame([{
            "player_id": 1,
            "bookmaker": "LowVigBook",
            "selected_snapshot_time": "2025-07-01T16:04:00Z",
            "selected_decision_time": "2025-07-01T16:03:00Z",
            "quote_decision_policy": "slate_or_tminus:20:00ET:-120m",
            "pred_q50": 6.0,
            "actual": 7.0,
        }]),
        elapsed_seconds=3.456,
    )

    save_results(
        [result],
        output_dir=tmp_path,
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 2),
        phase01_time=12.34,
        total_predictions=99,
        total_dates=2,
    )

    config_dir = tmp_path / "config_01_no_BL_edge0.08_kelly0.125"
    bets = pd.read_csv(config_dir / "bets.csv")
    predictions = pd.read_csv(config_dir / "predictions.csv")

    for output in [bets, predictions]:
        assert output.loc[0, "bookmaker"] == "LowVigBook"
        assert output.loc[0, "selected_snapshot_time"] == "2025-07-01T16:04:00Z"
        assert output.loc[0, "selected_decision_time"] == "2025-07-01T16:03:00Z"
        assert output.loc[0, "quote_decision_policy"] == "slate_or_tminus:20:00ET:-120m"


def test_save_results_skips_empty_bets_and_predictions_files_but_writes_metrics(tmp_path):
    from src.backtesting.mlb.sweep_results import SweepResult, save_results

    result = SweepResult(
        config=SweepConfig(tau=0.05, edge_threshold=0.1, kelly_fraction=0.2),
        metrics=_metrics(),
        bets_df=pd.DataFrame(),
        predictions_df=pd.DataFrame(),
        elapsed_seconds=1.0,
    )

    save_results(
        [result],
        output_dir=tmp_path,
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 1),
        phase01_time=1.0,
        total_predictions=0,
        total_dates=0,
    )

    config_dir = tmp_path / "config_01_tau0.05_edge0.1_kelly0.2"
    assert config_dir.exists()
    assert not (config_dir / "bets.csv").exists()
    assert not (config_dir / "predictions.csv").exists()
    assert (config_dir / "metrics.json").exists()
    assert (tmp_path / "sweep_results.json").exists()
    assert (tmp_path / "sweep_summary.csv").exists()
