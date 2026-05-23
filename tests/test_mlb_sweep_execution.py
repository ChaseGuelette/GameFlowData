"""Tests for MLB sweep per-config execution seam."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.backtesting.mlb.sweep_config import SweepConfig
from src.backtesting.mlb.sweep_execution import run_single_config, run_single_config_fast_mlb


def test_run_single_config_fast_mlb_returns_empty_result_for_empty_precompute():
    config = SweepConfig(tau=None, edge_threshold=0.08, kelly_fraction=0.125)

    result = run_single_config_fast_mlb(
        config=config,
        precomputed_df=pd.DataFrame(),
        game_dates=[date(2025, 7, 1)],
        starting_bankroll=1000.0,
    )

    assert result.config is config
    assert result.bets_df.empty
    assert result.predictions_df.empty
    assert result.elapsed_seconds >= 0


def test_run_single_config_orchestrates_edges_resolution_and_metrics(monkeypatch):
    import src.backtesting.mlb.sweep_execution as sweep_execution

    config = SweepConfig(tau=None, edge_threshold=0.08, kelly_fraction=0.125)
    game_day = date(2025, 7, 1)
    edge_calls = []
    simulator_events = []

    def fake_compute_edges_for_config(preds, lines, bl_blender, actuals):
        edge_calls.append({"preds": preds, "lines": lines, "bl_blender": bl_blender, "actuals": actuals})
        return [
            {
                "game_date": game_day,
                "player_id": 101,
                "game_id": 9001,
                "stat": "pitcher_strikeouts",
                "actual": 7.0,
                "line": 5.5,
                "over_prob": 0.60,
                "under_prob": 0.40,
                "implied_over": 0.50,
                "implied_under": 0.50,
                "over_edge": 0.10,
                "under_edge": -0.10,
            }
        ]

    class FakeSimulator:
        def __init__(self, **kwargs):
            simulator_events.append(("init", kwargs))

        def resolve_bets(self, actuals_df):
            simulator_events.append(("resolve", actuals_df.to_dict("records")))

        def evaluate_predictions(self, day_df, game_date):
            simulator_events.append(("evaluate", game_date, day_df.to_dict("records")))

        def to_dataframe(self):
            return pd.DataFrame([{"player_id": 101, "profit": 10.0}])

    class FakeMetricsCalculator:
        def calculate(self, predictions_df, bets_df, *, starting_bankroll):
            return {
                "prediction_rows": len(predictions_df),
                "bet_rows": len(bets_df),
                "starting_bankroll": starting_bankroll,
            }

    monkeypatch.setattr(sweep_execution, "compute_edges_for_config", fake_compute_edges_for_config)
    monkeypatch.setattr(sweep_execution, "BetSimulator", FakeSimulator)
    monkeypatch.setattr(sweep_execution, "MetricsCalculator", FakeMetricsCalculator)

    result = run_single_config(
        config=config,
        game_dates=[game_day],
        date_predictions={game_day: [object()]},
        date_lines={game_day: pd.DataFrame([{"line": 5.5}])},
        date_actuals={game_day: {(101, "pitcher_strikeouts"): 7.0}},
        starting_bankroll=1000.0,
        max_bet_pct=0.02,
        flat_bet_size=25.0,
        allowed_bets={("pitcher_strikeouts", "over")},
    )

    assert edge_calls == [
        {
            "preds": [edge_calls[0]["preds"][0]],
            "lines": edge_calls[0]["lines"],
            "bl_blender": None,
            "actuals": {(101, "pitcher_strikeouts"): 7.0},
        }
    ]
    assert simulator_events[0] == (
        "init",
        {
            "edge_threshold": 0.08,
            "starting_bankroll": 1000.0,
            "kelly_fraction": 0.125,
            "max_bet_pct": 0.02,
            "flat_bet_size": 25.0,
            "allowed_bets": {("pitcher_strikeouts", "over")},
        },
    )
    assert simulator_events[1][0] == "resolve"
    assert simulator_events[2][0] == "evaluate"
    assert simulator_events[3][0] == "resolve"
    assert len(result.predictions_df) == 1
    assert len(result.bets_df) == 1
    assert result.metrics == {"prediction_rows": 1, "bet_rows": 1, "starting_bankroll": 1000.0}
