from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "analyze_mlb_clv.py"


def load_module():
    spec = importlib.util.spec_from_file_location("analyze_mlb_clv", SCRIPT_PATH)
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
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T22:00:00Z", "2026-04-13T22:00:00Z"]),
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
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T22:00:00Z"]),
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


def test_close_before_bet_time_is_dropped_not_scored():
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
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T22:00:00Z"]),
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [10],
            "game_id": [100],
            "market_key": ["batter_hits"],
            "bookmaker": ["draftkings"],
            "line": [0.5],
            "outcome_label": ["Under"],
            "odds_american": [120],
            "snapshot_time": pd.to_datetime(["2026-04-13T21:55:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    row = out.iloc[0]

    assert row["clv_source"] == "unmatched"
    assert row["unmatched_reason"] == "no_close_match"
    assert pd.isna(row["clv_implied_prob"])


def test_bet_at_or_after_commence_is_dropped():
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
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [10],
            "game_id": [100],
            "market_key": ["batter_hits"],
            "bookmaker": ["draftkings"],
            "line": [0.5],
            "outcome_label": ["Under"],
            "odds_american": [120],
            "snapshot_time": pd.to_datetime(["2026-04-13T23:04:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    row = out.iloc[0]

    assert row["clv_source"] == "unmatched"
    assert row["unmatched_reason"] == "bet_time_at_or_after_commence"
    assert pd.isna(row["clv_implied_prob"])


def test_normalize_snapshots_uses_effective_snapshot_time_fallback():
    m = load_module()
    snapshots = pd.DataFrame(
        {
            "player_id": [10],
            "game_id": [100],
            "market_key": ["batter_hits"],
            "bookmaker": ["draftkings"],
            "line": [0.5],
            "outcome_label": ["Under"],
            "odds_american": [120],
            "snapshot_time": [pd.NaT],
            "inserted_at": pd.to_datetime(["2026-04-13T22:55:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )

    out = m.normalize_snapshots(snapshots)

    assert out.loc[0, "snapshot_time"] == pd.Timestamp("2026-04-13T22:55:00Z")


def test_fetch_snapshots_query_selects_effective_snapshot_time(monkeypatch):
    m = load_module()
    captured = {}

    class FakeConn:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def execute(self, *args, **kwargs):
            return None

    class FakeEngine:
        def connect(self):
            return FakeConn()

    def fake_get_engine(local=False):
        return FakeEngine()

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setitem(sys.modules, "src.db.client", type("M", (), {"get_engine": fake_get_engine}))
    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    bets = pd.DataFrame({
        "game_id": [100],
        "player_id": [10],
        "stat": ["batter_hits"],
        "side": ["under"],
        "line": [0.5],
        "odds": [150],
        "bookmaker": ["draftkings"],
    })
    m.fetch_snapshots_for_bets(bets)

    assert "COALESCE(p.snapshot_time, p.inserted_at) AS snapshot_time" in captured["sql"]
    assert "COALESCE(p.snapshot_time, p.inserted_at) IS NOT NULL" in captured["sql"]


def test_fetch_snapshots_can_use_dense_clv_snapshot_table(monkeypatch):
    m = load_module()
    captured = {}

    class FakeConn:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def execute(self, *args, **kwargs):
            return None

    class FakeEngine:
        def connect(self):
            return FakeConn()

    def fake_get_engine(local=False):
        return FakeEngine()

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setitem(sys.modules, "src.db.client", type("M", (), {"get_engine": fake_get_engine}))
    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    bets = pd.DataFrame({
        "game_id": [100],
        "player_id": [10],
        "stat": ["batter_hits"],
        "side": ["under"],
        "line": [0.5],
        "odds": [150],
        "bookmaker": ["draftkings"],
    })
    m.fetch_snapshots_for_bets(bets, source_table="mlb_player_props_clv_snapshots")

    assert "FROM mlb_player_props_clv_snapshots p" in captured["sql"]
    assert "p.snapshot_time AS snapshot_time" in captured["sql"]
    assert "p.game_id IS NOT NULL AND p.player_id IS NOT NULL" in captured["sql"]


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



def test_assumed_bet_time_fills_missing_only():
    m = load_module()
    bets = pd.DataFrame(
        {
            "game_date": ["2026-04-13", "2026-04-13"],
            "bet_snapshot_time": ["2026-04-13T20:00:00Z", pd.NaT],
        }
    )
    out = m.apply_assumed_bet_time_et(bets, "13:30")
    assert out.loc[0, "bet_snapshot_time"] == pd.Timestamp("2026-04-13T20:00:00Z")
    assert out.loc[0, "bet_time_source"] == "artifact"
    assert out.loc[1, "bet_snapshot_time"] == pd.Timestamp("2026-04-13T17:30:00Z")
    assert out.loc[1, "bet_time_source"] == "assumed"


def test_plus_15_30_60_horizon_columns_are_populated():
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
            "player_id": [10, 10, 10, 10],
            "game_id": [100, 100, 100, 100],
            "market_key": ["batter_hits"] * 4,
            "bookmaker": ["draftkings"] * 4,
            "line": [0.5] * 4,
            "outcome_label": ["Under"] * 4,
            "odds_american": [145, 140, 135, 120],
            "snapshot_time": pd.to_datetime([
                "2026-04-13T20:15:00Z",
                "2026-04-13T20:30:00Z",
                "2026-04-13T21:00:00Z",
                "2026-04-13T22:55:00Z",
            ]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"] * 4),
        }
    )
    row = m.build_clv_matches(bets, snapshots).iloc[0]
    assert row["plus15_odds"] == 145
    assert row["plus30_odds"] == 140
    assert row["plus60_odds"] == 135
    assert row["plus15_match_source"] == "same_book_same_line"
    assert row["plus30_match_source"] == "same_book_same_line"
    assert row["plus60_match_source"] == "same_book_same_line"


def test_assumption_caused_early_game_invalid_reason():
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
    bets = m.apply_assumed_bet_time_et(m.normalize_bets(bets), "19:30")
    snapshots = pd.DataFrame(
        {
            "player_id": [10],
            "game_id": [100],
            "market_key": ["batter_hits"],
            "bookmaker": ["draftkings"],
            "line": [0.5],
            "outcome_label": ["Under"],
            "odds_american": [120],
            "snapshot_time": pd.to_datetime(["2026-04-13T23:00:00Z"]),
            "commence_time": pd.to_datetime(["2026-04-13T23:05:00Z"]),
        }
    )
    row = m.build_clv_matches(bets, snapshots).iloc[0]
    assert row["unmatched_reason"] == "invalid_assumed_time_early_game"


def test_timing_stability_long_rows_include_all_horizons():
    m = load_module()
    matches = pd.DataFrame([
        {
            "bet_id": 0,
            "game_date": "2026-04-13",
            "player_id": 10,
            "game_id": 100,
            "bookmaker_at_bet": "draftkings",
            "line_at_bet": 0.5,
            "odds_at_bet": 150,
            "plus15_odds": 145,
            "plus15_snapshot_time": "2026-04-13T20:15:00Z",
            "plus15_clv_implied_prob": 0.01,
            "plus15_match_source": "same_book_same_line",
            "plus30_odds": 140,
            "plus30_snapshot_time": "2026-04-13T20:30:00Z",
            "plus30_clv_implied_prob": 0.02,
            "plus30_match_source": "same_book_same_line",
            "plus60_odds": 135,
            "plus60_snapshot_time": "2026-04-13T21:00:00Z",
            "plus60_clv_implied_prob": 0.03,
            "plus60_match_source": "same_book_same_line",
            "clv_implied_prob": 0.04,
        }
    ])
    timing = m.build_timing_stability(matches)
    assert set(timing["horizon"]) == {"+15m", "+30m", "+60m"}
    assert "horizon_clv_implied_prob" in timing.columns



def test_pitcher_strikeouts_same_book_and_consensus_fallback_generic_path():
    m = load_module()
    bets = pd.DataFrame(
        {
            "bet_id": [1, 2],
            "game_date": ["2026-04-13", "2026-04-13"],
            "player_id": [99, 99],
            "game_id": [200, 200],
            "stat": ["pitcher_strikeouts", "pitcher_strikeouts"],
            "side": ["under", "under"],
            "line": [5.5, 6.5],
            "odds": [110, 115],
            "bookmaker": ["draftkings", "fanduel"],
            "edge": [0.18, 0.19],
            "bet_snapshot_time": pd.to_datetime(["2026-04-13T21:00:00Z", "2026-04-13T21:00:00Z"]),
        }
    )
    snapshots = pd.DataFrame(
        {
            "player_id": [99, 99, 99, 99],
            "game_id": [200, 200, 200, 200],
            "market_key": ["pitcher_strikeouts"] * 4,
            "bookmaker": ["draftkings", "betmgm", "draftkings", "betmgm"],
            "line": [5.5, 5.5, 6.5, 6.5],
            "outcome_label": ["Under"] * 4,
            "odds_american": [105, 100, 112, 109],
            "snapshot_time": pd.to_datetime(["2026-04-13T22:00:00Z"] * 4),
            "commence_time": pd.to_datetime(["2026-04-14T00:00:00Z"] * 4),
        }
    )

    out = m.build_clv_matches(bets, snapshots)
    by_bet = out.set_index("bet_id")
    assert by_bet.loc[1, "clv_source"] == "same_book_close"
    assert by_bet.loc[1, "bookmaker_at_close"] == "draftkings"
    assert by_bet.loc[2, "clv_source"] == "consensus_close_fallback"
    assert by_bet.loc[2, "bookmaker_at_close"] == "consensus"
