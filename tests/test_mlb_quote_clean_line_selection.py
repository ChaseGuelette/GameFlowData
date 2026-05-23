"""Regression tests for shared MLB quote-clean line selection."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.backtesting.mlb.line_selection import fetch_lines_at_decision_time
from src.backtesting.mlb.mlb_backtest_harness import MLBBacktestHarness
from src.backtesting.mlb.quote_decision_policy import decision_time_for_game


class _FakeConnect:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None


class _FakeEngine:
    def connect(self):
        return _FakeConnect()


def test_shared_line_selection_query_enforces_decision_and_commence_cutoffs(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=UTC)

    out = fetch_lines_at_decision_time(
        _FakeEngine(),
        game_ids=[1001, 1002],
        market_keys=["batter_hits"],
        as_of_time=as_of,
    )

    assert out.empty
    sql = captured["sql"]
    assert "market_last_update <= :as_of_time" in sql
    assert "COALESCE(snapshot_time, inserted_at) <= :as_of_time" in sql
    assert "market_last_update < commence_time" in sql
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in sql
    assert "PARTITION BY player_id, game_id, market_key, bookmaker, line, outcome_label" in sql
    assert "over_snapshot_time" in sql
    assert "under_snapshot_time" in sql
    assert captured["params"]["as_of_time"] == as_of


def test_shared_line_selection_can_use_dense_clv_snapshot_table(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=UTC)

    fetch_lines_at_decision_time(
        _FakeEngine(),
        game_ids=[1001],
        market_keys=["batter_hits"],
        as_of_time=as_of,
        source_table="mlb_player_props_clv_snapshots",
    )

    sql = captured["sql"]
    assert "FROM mlb_player_props_clv_snapshots" in sql
    assert "snapshot_time AS effective_snapshot_time" in sql
    assert "AND game_id IS NOT NULL AND player_id IS NOT NULL" in sql


def test_legacy_latest_line_selection_is_explicit_and_still_excludes_post_commence(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    fetch_lines_at_decision_time(
        _FakeEngine(),
        game_ids=[1001],
        market_keys=["batter_hits"],
        as_of_time=None,
        allow_latest_without_as_of=True,
    )

    sql = captured["sql"]
    assert "latest-without-as-of is legacy/backfill only" in sql
    assert "market_last_update < commence_time" in sql
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in sql
    assert "as_of_time" not in captured["params"]


def test_core_mlb_backtest_harness_uses_shared_quote_clean_line_selection(monkeypatch):
    calls = []

    def fake_fetch(engine, *, game_ids, market_keys, as_of_time=None, allow_latest_without_as_of=False, bookmakers=None):
        calls.append(
            {
                "engine": engine,
                "game_ids": game_ids,
                "market_keys": market_keys,
                "as_of_time": as_of_time,
                "allow_latest_without_as_of": allow_latest_without_as_of,
                "bookmakers": bookmakers,
            }
        )
        return pd.DataFrame(
            [
                {
                    "player_id": 10,
                    "game_id": 1001,
                    "bookmaker": "draftkings",
                    "market_key": "batter_hits",
                    "line": 0.5,
                    "over_odds": -120,
                    "under_odds": 100,
                    "selected_snapshot_time": pd.Timestamp("2026-05-10T17:25:00Z"),
                }
            ]
        )

    monkeypatch.setattr("src.backtesting.mlb.mlb_backtest_harness.fetch_lines_at_decision_time", fake_fetch)

    engine = object()
    harness = MLBBacktestHarness(engine=engine, stats=["batter_hits"])
    rows = harness._get_lines_for_player(10, 1001, "batter_hits")

    assert len(rows) == 1
    assert rows[0]["under_odds"] == 100
    assert calls == [
        {
            "engine": engine,
            "game_ids": [1001],
            "market_keys": ["batter_hits"],
            "as_of_time": None,
            "allow_latest_without_as_of": True,
            "bookmakers": harness.bookmakers,
        }
    ]


def test_slate_or_tminus_policy_uses_slate_and_fallback_for_early_games():
    main_slate_game = {"game_id": 1, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")}
    early_game = {"game_id": 2, "game_time_utc": pd.Timestamp("2026-04-13T13:05:00Z")}

    main_decision = decision_time_for_game(
        main_slate_game,
        policy="slate_or_tminus",
        fixed_cutoff_ts=None,
        relative_minutes=60,
    )
    early_decision = decision_time_for_game(
        early_game,
        policy="slate_or_tminus",
        fixed_cutoff_ts=None,
        relative_minutes=60,
    )

    assert main_decision == pd.Timestamp("2026-04-13T17:30:00-04:00").to_pydatetime()
    assert early_decision == pd.Timestamp("2026-04-13T08:05:00-04:00").to_pydatetime()
