"""Tests for NBA mode-specific date/range/training loaders."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

import pandas as pd

from src.models.feature_store import FeatureStore
from src.models.features.nba.requests import (
    DateFeatureRequest,
    DateRangeFeatureRequest,
    TrainingFeatureRequest,
)


def _store_with_connection():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    return FeatureStore(engine=engine), conn


def test_date_batch_feature_loader_module_loads_one_date_via_request(monkeypatch):
    from src.models.features.nba.date_batch_loader import DateBatchFeatureLoader

    store, _conn = _store_with_connection()
    observed = {}

    def fake_read_sql(query, conn, params):
        observed["params"] = params
        return pd.DataFrame([{"game_id": "g1", "game_date": date(2025, 1, 2)}])

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = DateBatchFeatureLoader(store).load(DateFeatureRequest(game_date=date(2025, 1, 2)))

    assert observed["params"] == {"game_date": date(2025, 1, 2)}
    assert result.loc[0, "travel_dist"] == 0.0
    assert result.loc[0, "opp_is_back_to_back"] == 0.0


def test_feature_store_date_facade_delegates_to_date_batch_loader(monkeypatch):
    store, _conn = _store_with_connection()
    observed = {}

    class FakeLoader:
        def __init__(self, feature_store):
            observed["feature_store"] = feature_store

        def load(self, request):
            observed["request"] = request
            return pd.DataFrame([{"ok": True}])

    monkeypatch.setattr("src.models.feature_store.DateBatchFeatureLoader", FakeLoader)

    result = store.get_features_for_date(date(2025, 1, 2))

    assert result.loc[0, "ok"] == True
    assert observed["feature_store"] is store
    assert observed["request"] == DateFeatureRequest(game_date=date(2025, 1, 2))


def test_feature_store_date_range_facade_delegates_to_date_range_loader(monkeypatch):
    store, _conn = _store_with_connection()
    observed = {}

    class FakeLoader:
        def __init__(self, feature_store):
            observed["feature_store"] = feature_store

        def load(self, request, chunk_size=25):
            observed["request"] = request
            observed["chunk_size"] = chunk_size
            return {date(2025, 1, 2): pd.DataFrame([{"ok": True}])}

    monkeypatch.setattr("src.models.feature_store.DateRangeFeatureLoader", FakeLoader)

    result = store.get_features_for_date_range(date(2025, 1, 1), date(2025, 1, 3))

    assert result[date(2025, 1, 2)].loc[0, "ok"] == True
    assert observed["feature_store"] is store
    assert observed["request"] == DateRangeFeatureRequest(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
    )
    assert observed["chunk_size"] == 25


def test_feature_store_training_facade_delegates_to_training_loader(monkeypatch):
    store, _conn = _store_with_connection()
    observed = {}

    class FakeLoader:
        def __init__(self, feature_store):
            observed["feature_store"] = feature_store

        def load(self, request):
            observed["request"] = request
            return pd.DataFrame([{"ok": True}])

        def load_single_season(self, season):
            observed["single_season"] = season
            return pd.DataFrame([{"season": season}])

    monkeypatch.setattr("src.models.feature_store.TrainingFeatureLoader", FakeLoader)

    result = store.get_training_dataset(["22024"])
    single = store._load_single_season_training("22024")

    assert result.loc[0, "ok"] == True
    assert single.loc[0, "season"] == "22024"
    assert observed["feature_store"] is store
    assert observed["request"] == TrainingFeatureRequest(seasons=["22024"])
    assert observed["single_season"] == "22024"
