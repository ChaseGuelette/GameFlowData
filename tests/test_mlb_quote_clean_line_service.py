"""Tests for MLB quote-clean line service orchestration."""

from __future__ import annotations

import pandas as pd

from src.backtesting.mlb.quote_clean_line_service import fetch_lines_for_date


class _FakeEngine:
    pass


def _line_row(game_id: int) -> dict:
    return {
        "player_id": game_id + 100,
        "game_id": game_id,
        "bookmaker": "draftkings",
        "market_key": "batter_hits",
        "line": 0.5,
        "over_odds": -120,
        "under_odds": 100,
    }


def test_fixed_policy_fetches_all_games_once_and_adds_decision_metadata(monkeypatch):
    calls = []

    def fake_fetch(engine, *, game_ids, market_keys, as_of_time, allow_latest_without_as_of, bookmakers, source_table):
        calls.append(
            {
                "engine": engine,
                "game_ids": game_ids,
                "market_keys": market_keys,
                "as_of_time": as_of_time,
                "allow_latest_without_as_of": allow_latest_without_as_of,
                "bookmakers": bookmakers,
                "source_table": source_table,
            }
        )
        return pd.DataFrame([_line_row(game_ids[0])])

    monkeypatch.setattr("src.backtesting.mlb.quote_clean_line_service.fetch_lines_at_decision_time", fake_fetch)
    engine = _FakeEngine()
    cutoff = pd.Timestamp("2026-04-13T17:30:00-04:00").to_pydatetime()

    out = fetch_lines_for_date(
        engine,
        games=[{"game_id": 1}, {"game_id": 2}],
        market_keys=["batter_hits"],
        quote_clean_cutoff_ts=cutoff,
        quote_decision_policy="fixed_et",
        line_source="mlb_player_props_clv_snapshots",
    )

    assert calls == [
        {
            "engine": engine,
            "game_ids": [1, 2],
            "market_keys": ["batter_hits"],
            "as_of_time": cutoff,
            "allow_latest_without_as_of": False,
            "bookmakers": None,
            "source_table": "mlb_player_props_clv_snapshots",
        }
    ]
    assert out["selected_decision_time"].tolist() == [cutoff]
    assert out["quote_decision_policy"].tolist() == ["fixed_et"]


def test_per_game_policy_fetches_each_game_at_its_decision_time(monkeypatch):
    calls = []

    def fake_fetch(engine, *, game_ids, market_keys, as_of_time, allow_latest_without_as_of, bookmakers, source_table):
        calls.append((game_ids, as_of_time, allow_latest_without_as_of, source_table))
        return pd.DataFrame([_line_row(game_ids[0])])

    monkeypatch.setattr("src.backtesting.mlb.quote_clean_line_service.fetch_lines_at_decision_time", fake_fetch)
    cutoff = pd.Timestamp("2026-04-13T13:30:00-04:00").to_pydatetime()
    games = [
        {"game_id": 1, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")},
        {"game_id": 2, "game_time_utc": pd.Timestamp("2026-04-14T02:05:00Z")},
    ]

    out = fetch_lines_for_date(
        _FakeEngine(),
        games=games,
        market_keys=["batter_hits"],
        quote_clean_cutoff_ts=cutoff,
        quote_decision_policy="relative_to_commence",
        quote_relative_minutes=60,
    )

    assert calls == [
        ([1], pd.Timestamp("2026-04-13T22:05:00Z").to_pydatetime(), False, "mlb_raw_player_props"),
        ([2], pd.Timestamp("2026-04-14T01:05:00Z").to_pydatetime(), False, "mlb_raw_player_props"),
    ]
    assert out["game_id"].tolist() == [1, 2]
    assert out["quote_decision_policy"].tolist() == ["relative_to_commence", "relative_to_commence"]


def test_skip_early_fixed_policy_omits_games_started_before_cutoff(monkeypatch):
    calls = []

    def fake_fetch(engine, *, game_ids, market_keys, as_of_time, allow_latest_without_as_of, bookmakers, source_table):
        calls.append(game_ids)
        return pd.DataFrame([_line_row(game_ids[0])])

    monkeypatch.setattr("src.backtesting.mlb.quote_clean_line_service.fetch_lines_at_decision_time", fake_fetch)
    cutoff = pd.Timestamp("2026-04-13T13:30:00-04:00").to_pydatetime()

    out = fetch_lines_for_date(
        _FakeEngine(),
        games=[
            {"game_id": 1, "game_time_utc": pd.Timestamp("2026-04-13T17:00:00Z")},
            {"game_id": 2, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")},
        ],
        market_keys=["batter_hits"],
        quote_clean_cutoff_ts=cutoff,
        quote_decision_policy="skip_early_fixed_et",
    )

    assert calls == [[2]]
    assert out["game_id"].tolist() == [2]


def test_legacy_game_ids_path_allows_latest_without_as_of(monkeypatch):
    calls = []

    def fake_fetch(engine, *, game_ids, market_keys, as_of_time, allow_latest_without_as_of, bookmakers, source_table):
        calls.append((game_ids, as_of_time, allow_latest_without_as_of))
        return pd.DataFrame([_line_row(game_ids[0])])

    monkeypatch.setattr("src.backtesting.mlb.quote_clean_line_service.fetch_lines_at_decision_time", fake_fetch)

    out = fetch_lines_for_date(
        _FakeEngine(),
        market_keys=["batter_hits"],
        quote_clean_cutoff_ts=None,
        game_ids=[10, 11],
    )

    assert calls == [([10, 11], None, True)]
    assert "selected_decision_time" not in out.columns
    assert "quote_decision_policy" not in out.columns


def test_empty_games_or_markets_returns_empty_without_fetch(monkeypatch):
    def fake_fetch(*args, **kwargs):
        raise AssertionError("fetch should not be called")

    monkeypatch.setattr("src.backtesting.mlb.quote_clean_line_service.fetch_lines_at_decision_time", fake_fetch)

    assert fetch_lines_for_date(_FakeEngine(), games=[], market_keys=["batter_hits"]).empty
    assert fetch_lines_for_date(_FakeEngine(), games=[{"game_id": 1}], market_keys=[]).empty
