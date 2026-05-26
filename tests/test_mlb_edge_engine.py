"""Tests for MLB sweep edge/base-probability computation seam."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.backtesting.mlb.edge_engine import (
    build_config_edge_frame,
    compute_edges_for_config,
    odds_to_prob,
    precompute_mlb_base_probs,
    select_sharpest_line,
)
from src.backtesting.mlb.prediction_cache import DatePrediction


def _prediction(*, samples: list[float] | None = None, stat: str = "pitcher_strikeouts") -> DatePrediction:
    sample_array = np.array(samples if samples is not None else [0.0, 1.0, 2.0, 3.0], dtype=float)
    return DatePrediction(
        game_date=date(2025, 7, 1),
        player_id=101,
        game_id=9001,
        team_id=10,
        opponent_id=20,
        stat=stat,
        model_type="quantile",
        pred_mean=float(sample_array.mean()),
        pred_median=float(np.median(sample_array)),
        pred_q10=float(np.quantile(sample_array, 0.10)),
        pred_q25=float(np.quantile(sample_array, 0.25)),
        pred_q50=float(np.quantile(sample_array, 0.50)),
        pred_q75=float(np.quantile(sample_array, 0.75)),
        pred_q90=float(np.quantile(sample_array, 0.90)),
        samples=sample_array,
    )


def _lines() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player_id": 101,
                "game_id": 9001,
                "market_key": "pitcher_strikeouts",
                "line": 1.5,
                "over_odds": -130,
                "under_odds": +100,
                "bookmaker": "wide_book",
                "selected_snapshot_time": "2025-07-01T16:00:00Z",
                "over_snapshot_time": "2025-07-01T16:00:00Z",
                "under_snapshot_time": "2025-07-01T16:00:00Z",
                "selected_decision_time": "2025-07-01T17:00:00Z",
                "quote_decision_policy": "fixed_et",
            },
            {
                "player_id": 101,
                "game_id": 9001,
                "market_key": "pitcher_strikeouts",
                "line": 1.5,
                "over_odds": +100,
                "under_odds": +100,
                "bookmaker": "low_vig_book",
                "selected_snapshot_time": "2025-07-01T16:05:00Z",
                "over_snapshot_time": "2025-07-01T16:05:00Z",
                "under_snapshot_time": "2025-07-01T16:05:00Z",
                "selected_decision_time": "2025-07-01T17:00:00Z",
                "quote_decision_policy": "fixed_et",
            },
            {
                "player_id": 202,
                "game_id": 9001,
                "market_key": "pitcher_strikeouts",
                "line": 5.5,
                "over_odds": None,
                "under_odds": -110,
                "bookmaker": "missing_side_book",
            },
        ]
    )


class FakeBlender:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def blend_prediction(self, *, samples, line, over_odds, under_odds):
        self.calls.append(
            {
                "samples": samples,
                "line": line,
                "over_odds": over_odds,
                "under_odds": under_odds,
            }
        )
        return {"posterior_over": 0.70, "posterior_under": 0.30}


def test_odds_to_prob_and_lowest_vig_line_selection_match_runner_behavior():
    assert odds_to_prob(+150) == 100 / 250
    assert odds_to_prob(-120) == 120 / 220

    selected = select_sharpest_line(_lines(), 101, 9001, "pitcher_strikeouts")

    assert selected is not None
    assert selected["bookmaker"] == "low_vig_book"
    assert selected["line"] == 1.5
    assert selected["selected_snapshot_time"] == "2025-07-01T16:05:00Z"


def test_compute_edges_for_config_uses_empirical_cdf_and_preserves_row_metadata():
    pred = _prediction(samples=[0, 1, 2, 3])

    rows = compute_edges_for_config(
        [pred],
        _lines(),
        bl_blender=None,
        actuals={(101, "pitcher_strikeouts"): 2.0},
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["over_prob"] == 0.5  # (samples > 1.5).mean(), not Gaussian CDF
    assert row["under_prob"] == 0.5
    assert row["implied_over"] == 0.5
    assert row["implied_under"] == 0.5
    assert row["over_edge"] == 0.0
    assert row["under_edge"] == 0.0
    assert row["bookmaker"] == "low_vig_book"
    assert row["actual"] == 2.0
    assert row["model_type"] == "quantile"
    assert row["selected_decision_time"] == "2025-07-01T17:00:00Z"


def test_compute_edges_for_config_applies_per_stat_blender_without_changing_market_probs():
    blender = FakeBlender()
    pred = _prediction(samples=[0, 1, 2, 3])

    rows = compute_edges_for_config(
        [pred],
        _lines(),
        bl_blender={"pitcher_strikeouts": blender},
        actuals=None,
    )

    assert len(blender.calls) == 1
    assert blender.calls[0]["line"] == 1.5
    assert blender.calls[0]["over_odds"] == 100
    assert rows[0]["over_prob"] == 0.70
    assert rows[0]["under_prob"] == 0.30
    assert rows[0]["implied_over"] == 0.5
    assert rows[0]["bl_over_prob"] == 0.70
    assert rows[0]["bl_under_edge"] == -0.20


def test_build_config_edge_frame_applies_vectorized_bl_math_for_one_config():
    precomputed = pd.DataFrame(
        [
            {
                "model_over": 0.75,
                "market_over": 0.50,
                "market_under": 0.50,
                "z_raw": 1.0,
                "model_logit": np.log(0.75 / 0.25),
                "market_logit": 0.0,
            }
        ]
    )
    config = SimpleNamespace(tau=0.10, z_max=2.0, max_weight=0.20)

    df = build_config_edge_frame(config, precomputed)

    expected_weight = 0.10 * min(1.0 / 2.0, 1.0)
    expected_over = 1.0 / (1.0 + np.exp(-(expected_weight * np.log(0.75 / 0.25))))
    assert df.loc[0, "over_prob"] == expected_over
    assert df.loc[0, "under_prob"] == 1.0 - expected_over
    assert df.loc[0, "over_edge"] == expected_over - 0.50
    assert df.loc[0, "under_edge"] == (1.0 - expected_over) - 0.50


def test_precompute_mlb_base_probs_builds_vectorized_cache_for_all_candidate_books_with_empirical_probabilities():
    pred = _prediction(samples=[0, 1, 2, 3])
    game_date = date(2025, 7, 1)

    df = precompute_mlb_base_probs(
        [game_date],
        {game_date: [pred]},
        {game_date: _lines()},
        {game_date: {(101, "pitcher_strikeouts"): 2.0}},
    )

    assert set(df["bookmaker"]) == {"wide_book", "low_vig_book"}
    low_vig_row = df[df["bookmaker"] == "low_vig_book"].iloc[0]
    assert low_vig_row["model_over"] == 0.5
    assert low_vig_row["market_over"] == 0.5
    assert low_vig_row["market_under"] == 0.5
    assert low_vig_row["model_logit"] == 0.0
    assert low_vig_row["market_logit"] == 0.0
    assert low_vig_row["actual"] == 2.0
    assert low_vig_row["selected_snapshot_time"] == "2025-07-01T16:05:00Z"
    assert df.loc[df["bookmaker"] == "wide_book", "candidate_booksum"].iloc[0] > low_vig_row["candidate_booksum"]
