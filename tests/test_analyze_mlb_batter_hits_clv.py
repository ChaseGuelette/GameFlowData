from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "analyze_mlb_batter_hits_clv.py"


def load_module():
    spec = importlib.util.spec_from_file_location("analyze_mlb_batter_hits_clv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_plus_odds_band_boundaries():
    m = load_module()
    assert m.plus_odds_band(-110) == "-110_to_+99"
    assert m.plus_odds_band(99) == "-110_to_+99"
    assert m.plus_odds_band(100) == "+100_to_+149"
    assert m.plus_odds_band(149) == "+100_to_+149"
    assert m.plus_odds_band(150) == "+150_plus"


def test_same_book_close_primary_and_consensus_fallback():
    m = load_module()
    bets = pd.DataFrame(
        {
            "bet_id": [0, 1],
            "game_date": ["2026-04-13", "2026-04-13"],
            "player_id": [10, 11],
            "game_id": [100, 100],
            "stat": ["batter_hits", "batter_hits"],
            "side": ["under", "under"],
            "line": [0.5, 0.5],
            "odds": [150, 130],
            "bookmaker": ["draftkings", "fanduel"],
            "edge": [0.20, 0.16],
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [10, 10, 11, 11],
            "game_id": [100, 100, 100, 100],
            "market_key": ["batter_hits"] * 4,
            "bookmaker": ["draftkings", "fanduel", "draftkings", "betmgm"],
            "line": [0.5, 0.5, 0.5, 0.5],
            "outcome_label": ["Under"] * 4,
            "odds_american": [120, 118, 105, 115],
            "snapshot_time": pd.to_datetime(["2026-04-13T22:55:00Z"] * 4),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"] * 4),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    by_bet = out.set_index("bet_id")

    assert by_bet.loc[0, "clv_source"] == "same_book_close"
    assert by_bet.loc[0, "bookmaker_at_close"] == "draftkings"
    assert by_bet.loc[0, "odds_at_close"] == 120
    assert by_bet.loc[0, "same_book_clv_cents"] == 30
    assert by_bet.loc[0, "clv_implied_prob"] > 0

    assert by_bet.loc[1, "clv_source"] == "consensus_close_fallback"
    assert by_bet.loc[1, "bookmaker_at_close"] == "consensus"
    assert by_bet.loc[1, "odds_at_close"] == 110


def test_changed_line_is_classified_and_odds_clv_not_scored():
    m = load_module()
    bets = pd.DataFrame(
        {
            "bet_id": [0],
            "game_date": ["2026-04-13"],
            "player_id": [10],
            "game_id": [100],
            "stat": ["batter_hits"],
            "side": ["under"],
            "line": [0.5],
            "odds": [150],
            "bookmaker": ["draftkings"],
            "edge": [0.20],
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [10],
            "game_id": [100],
            "market_key": ["batter_hits"],
            "bookmaker": ["draftkings"],
            "line": [1.5],
            "outcome_label": ["Under"],
            "odds_american": [-125],
            "snapshot_time": pd.to_datetime(["2026-04-13T22:55:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    row = out.iloc[0]
    assert row["line_movement_class"] == "favorable_line_move"
    assert pd.isna(row["same_book_clv_cents"])
    assert pd.isna(row["clv_implied_prob"])


def test_plus_15_requires_bet_timestamp_and_scores_when_available():
    m = load_module()
    bets = pd.DataFrame(
        {
            "bet_id": [0],
            "game_date": ["2026-04-13"],
            "player_id": [10],
            "game_id": [100],
            "stat": ["batter_hits"],
            "side": ["under"],
            "line": [0.5],
            "odds": [150],
            "bookmaker": ["draftkings"],
            "edge": [0.20],
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T20:00:00Z"]),
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [10, 10],
            "game_id": [100, 100],
            "market_key": ["batter_hits", "batter_hits"],
            "bookmaker": ["draftkings", "draftkings"],
            "line": [0.5, 0.5],
            "outcome_label": ["Under", "Under"],
            "odds_american": [140, 120],
            "snapshot_time": pd.to_datetime(["2026-04-13T20:14:00Z", "2026-04-13T22:55:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z", "2026-04-13T23:05:00Z"]),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    row = out.iloc[0]
    assert row["plus15_odds"] == 140
    assert row["plus15_clv_implied_prob"] > 0


def test_phase1b_decision_rules_stop_restrict_confirm():
    m = load_module()
    stop = m.decide_phase1b(
        mean_clv_ci_low=-0.01,
        mean_clv=0.001,
        edge_corr_ci_low=-0.05,
        edge_corr=0.02,
        failing_bands=[],
    )
    assert stop["decision"] == "stop_feature_expansion"

    restrict = m.decide_phase1b(
        mean_clv_ci_low=0.01,
        mean_clv=0.02,
        edge_corr_ci_low=0.01,
        edge_corr=0.20,
        failing_bands=["+150_plus"],
    )
    assert restrict["decision"] == "restrict_plus_odds_band"

    confirmed = m.decide_phase1b(
        mean_clv_ci_low=0.01,
        mean_clv=0.02,
        edge_corr_ci_low=0.01,
        edge_corr=0.20,
        failing_bands=[],
    )
    assert confirmed["decision"] == "phase2_allowed"
