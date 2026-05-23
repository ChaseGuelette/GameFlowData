"""Regression tests for CLI audit/CLV metadata emitted by decomposed MLB sweep modules."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from src.backtesting.mlb.edge_engine import (
    apply_book_routing_policy,
    build_config_edge_frame,
    compute_edges_for_config,
    precompute_mlb_base_probs,
    select_sharpest_line,
)
from src.backtesting.mlb.prediction_cache import DatePrediction
from src.backtesting.mlb.sweep_config import SweepConfig


def _prediction() -> DatePrediction:
    return DatePrediction(
        game_date=date(2025, 7, 1),
        player_id=42,
        game_id=9001,
        team_id=10,
        opponent_id=20,
        stat="pitcher_strikeouts",
        model_type="quantile",
        pred_mean=6.2,
        pred_median=6.0,
        pred_q10=3.0,
        pred_q25=5.0,
        pred_q50=6.0,
        pred_q75=8.0,
        pred_q90=10.0,
        samples=np.array([4.0, 5.5, 6.5, 7.5, 8.0]),
    )


def _line_rows() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "player_id": 42,
            "game_id": 9001,
            "market_key": "pitcher_strikeouts",
            "line": 5.5,
            "over_odds": -130,
            "under_odds": +110,
            "bookmaker": "WideVigBook",
            "selected_snapshot_time": "2025-07-01T16:01:00Z",
            "over_snapshot_time": "2025-07-01T16:01:00Z",
            "under_snapshot_time": "2025-07-01T16:01:02Z",
            "selected_decision_time": "2025-07-01T16:00:00Z",
            "quote_decision_policy": "relative_to_commence:-120m",
        },
        {
            "player_id": 42,
            "game_id": 9001,
            "market_key": "pitcher_strikeouts",
            "line": 6.5,
            "over_odds": -105,
            "under_odds": -105,
            "bookmaker": "LowVigBook",
            "selected_snapshot_time": "2025-07-01T16:04:00Z",
            "over_snapshot_time": "2025-07-01T16:04:00Z",
            "under_snapshot_time": "2025-07-01T16:04:03Z",
            "selected_decision_time": "2025-07-01T16:03:00Z",
            "quote_decision_policy": "slate_or_tminus:20:00ET:-120m",
        },
    ])


def test_sharpest_line_selection_preserves_quote_audit_metadata() -> None:
    selected = select_sharpest_line(
        _line_rows(),
        player_id=42,
        game_id=9001,
        market_key="pitcher_strikeouts",
    )

    assert selected is not None
    assert selected["bookmaker"] == "LowVigBook"
    assert selected["selected_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert selected["over_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert selected["under_snapshot_time"] == "2025-07-01T16:04:03Z"
    assert selected["selected_decision_time"] == "2025-07-01T16:03:00Z"
    assert selected["quote_decision_policy"] == "slate_or_tminus:20:00ET:-120m"


def test_edge_rows_include_quote_audit_metadata_for_bets_outputs() -> None:
    rows = compute_edges_for_config(
        predictions=[_prediction()],
        lines_df=_line_rows(),
        bl_blender=None,
        actuals={(42, "pitcher_strikeouts"): 7.0},
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["bookmaker"] == "LowVigBook"
    assert row["selected_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert row["over_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert row["under_snapshot_time"] == "2025-07-01T16:04:03Z"
    assert row["selected_decision_time"] == "2025-07-01T16:03:00Z"
    assert row["quote_decision_policy"] == "slate_or_tminus:20:00ET:-120m"
    assert row["actual"] == 7.0


def test_fast_sweep_base_probs_and_config_edges_preserve_quote_audit_metadata() -> None:
    base = precompute_mlb_base_probs(
        game_dates=[date(2025, 7, 1)],
        date_predictions={date(2025, 7, 1): [_prediction()]},
        date_lines={date(2025, 7, 1): _line_rows()},
        date_actuals={date(2025, 7, 1): {(42, "pitcher_strikeouts"): 7.0}},
    )

    assert len(base) == 2
    assert set(base["bookmaker"]) == {"WideVigBook", "LowVigBook"}
    base_row = base[base["bookmaker"] == "LowVigBook"].iloc[0].to_dict()
    assert base_row["candidate_booksum"] < base[base["bookmaker"] == "WideVigBook"].iloc[0]["candidate_booksum"]
    assert base_row["selected_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert base_row["over_snapshot_time"] == "2025-07-01T16:04:00Z"
    assert base_row["under_snapshot_time"] == "2025-07-01T16:04:03Z"
    assert base_row["selected_decision_time"] == "2025-07-01T16:03:00Z"
    assert base_row["quote_decision_policy"] == "slate_or_tminus:20:00ET:-120m"

    edged = build_config_edge_frame(
        SweepConfig(tau=None, edge_threshold=0.05, kelly_fraction=0.1),
        base,
    )
    routed, candidates = apply_book_routing_policy(
        edged,
        edge_threshold=0.05,
        book_routing_policy="lowest_vig",
    )
    assert len(candidates) == 2
    assert candidates["selected_by_policy"].sum() == 1
    edged_row = routed.iloc[0].to_dict()
    assert edged_row["selected_decision_time"] == "2025-07-01T16:03:00Z"
    assert edged_row["quote_decision_policy"] == "slate_or_tminus:20:00ET:-120m"
