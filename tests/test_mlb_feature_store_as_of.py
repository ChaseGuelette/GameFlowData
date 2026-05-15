"""Regression tests for point-in-time MLB prop-line feature selection."""

from datetime import datetime, timezone

import pandas as pd

from src.backtesting.mlb.run_mlb_sweep import _fetch_lines_for_date
from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
from src.models.mlb.mlb_feature_store import MLBFeatureStore


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None


class _FakeEngine:
    def connect(self):
        return _FakeConnection()


def test_pitcher_feature_store_prop_line_query_applies_as_of_cutoff(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame({"line": [5.5]})

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    store = MLBFeatureStore(_FakeEngine())
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)

    line = store._get_prop_line(player_id=123, game_id=456, as_of_time=as_of)

    assert line == 5.5
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in captured["sql"]
    assert "ORDER BY market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of


def test_batter_feature_store_prop_line_query_applies_as_of_cutoff(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame({"line": [1.5]})

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    store = MLBBatterFeatureStore(_FakeEngine())
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)

    line = store._get_prop_line(
        player_id=123,
        game_id=456,
        market_key="batter_hits",
        as_of_time=as_of,
    )

    assert line == 1.5
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in captured["sql"]
    assert "ORDER BY market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of


def test_pitcher_batch_feature_query_applies_as_of_cutoff(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    store = MLBFeatureStore(_FakeEngine())
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)

    result = store.get_features_for_date("2026-05-10", as_of_time=as_of)

    assert result.empty
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in captured["sql"]
    assert "ORDER BY market_key, market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of


def test_batter_batch_feature_query_applies_as_of_cutoff(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    store = MLBBatterFeatureStore(_FakeEngine())
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)

    result = store.get_features_for_date("2026-05-10", stat="hits", as_of_time=as_of)

    assert result.empty
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in captured["sql"]
    assert "ORDER BY market_key, market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of


def test_batter_training_feature_query_binds_as_of_cutoff_and_post_commence_guard(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    store = MLBBatterFeatureStore(_FakeEngine())
    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)

    result = store.get_training_dataset([2026], stat="hits", as_of_time=as_of)

    assert result.empty
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in captured["sql"]
    assert "ORDER BY market_key, market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of


def test_quote_clean_line_fetch_uses_effective_timestamp_fallback(monkeypatch):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["sql"] = str(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    as_of = datetime(2026, 5, 10, 17, 30, tzinfo=timezone.utc)
    result = _fetch_lines_for_date(
        _FakeEngine(),
        game_ids=[456],
        market_keys=["pitcher_strikeouts"],
        quote_clean_cutoff_ts=as_of,
    )

    assert result.empty
    assert "market_last_update <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) <= :as_of_time" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) AS effective_snapshot_time" in captured["sql"]
    assert "ORDER BY market_last_update DESC NULLS LAST" in captured["sql"]
    assert "COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in captured["sql"]
    assert "selected_snapshot_time" in captured["sql"]
    assert captured["params"]["as_of_time"] == as_of
