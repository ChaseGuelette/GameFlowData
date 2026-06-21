"""Tests for NBA feature request objects and inference loader."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

from src.models.feature_store import FeatureStore
from src.models.features.nba.inference_loader import InferenceFeatureLoader
from src.models.features.nba.requests import (
    DateFeatureRequest,
    DateRangeFeatureRequest,
    PlayerGameFeatureRequest,
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


def _stub_sources(store):
    store._get_context_snapshots = Mock(
        return_value={"season_id": "22024", "position_group": "G", "team_id": 1, "opponent_id": 2, "is_home": 1}
    )
    store._get_player_rolling_stats = Mock(return_value={
        "player_avg_min_l5": 20,
        "rest_days": 2,
        "is_back_to_back": 0,
        "games_in_last_7_days": 3,
    })
    store._get_team_rolling_stats = Mock(side_effect=[{"team_avg_pace_l5": 100}, {"opp_avg_pace_l5": 98}])
    store._get_opponent_positional_stats = Mock(return_value={"opp_pos_off_rtg_allowed_l5": 110})
    store._get_game_lines = Mock(return_value={"line_spread_raw": 6, "line_total": 225})
    store._get_player_prop_lines = Mock(return_value={"prop_line_pts": 20.5})
    store._get_injury_context = Mock(return_value={})


def test_player_game_feature_request_preserves_legacy_arguments_and_scheduled_flag():
    request = PlayerGameFeatureRequest(
        player_id=10,
        game_id="g1",
        as_of_date=date(2025, 1, 1),
        team_id=1,
        opponent_id=2,
    )

    assert request.is_scheduled_context is True
    assert request.is_home is None

    historical = PlayerGameFeatureRequest(player_id=10, game_id="g1", as_of_date=date(2025, 1, 1))
    assert historical.is_scheduled_context is False


def test_phase6_request_objects_exist_for_mode_specific_loaders():
    assert DateFeatureRequest(game_date=date(2025, 1, 1)).game_date == date(2025, 1, 1)
    assert DateRangeFeatureRequest(start_date=date(2025, 1, 1), end_date=date(2025, 1, 2)).end_date == date(2025, 1, 2)
    assert TrainingFeatureRequest(seasons=["22024"]).seasons == ["22024"]


def test_inference_loader_combines_historical_context_like_legacy_feature_store():
    store, _conn = _store_with_connection()
    _stub_sources(store)
    loader = InferenceFeatureLoader(store)

    result = loader.load(PlayerGameFeatureRequest(player_id=1, game_id="g1", as_of_date=date(2025, 1, 1)))

    assert result["player_id"] == 1
    assert result["game_id"] == "g1"
    assert result["season_id"] == "22024"
    assert result["player_avg_min_l5"] == 20
    assert result["line_spread"] == -6
    assert result["rest_days"] == 2
    assert result["travel_dist"] == 0
    assert result["opp_rest_days"] == 0


def test_inference_loader_scheduled_context_uses_position_lookup_and_default_home():
    store, _conn = _store_with_connection()
    _stub_sources(store)
    store._get_context_snapshots = Mock()
    store._get_player_position = Mock(return_value="W")
    loader = InferenceFeatureLoader(store)

    result = loader.load(
        PlayerGameFeatureRequest(
            player_id=1,
            game_id="g1",
            as_of_date=date(2025, 1, 1),
            team_id=11,
            opponent_id=22,
        )
    )

    assert result["team_id"] == 11
    assert result["opponent_id"] == 22
    assert result["is_home"] is True
    assert result["position_group"] == "W"
    assert result["season_id"] == "22025"
    store._get_context_snapshots.assert_not_called()


def test_feature_store_player_game_facade_delegates_to_inference_loader(monkeypatch):
    store, _conn = _store_with_connection()
    observed = {}

    class FakeLoader:
        def __init__(self, feature_store):
            observed["feature_store"] = feature_store

        def load(self, request):
            observed["request"] = request
            return {"ok": True}

    monkeypatch.setattr("src.models.feature_store.InferenceFeatureLoader", FakeLoader)

    result = store.get_player_game_features(
        1,
        "g1",
        date(2025, 1, 1),
        team_id=11,
        opponent_id=22,
        is_home=False,
    )

    assert result == {"ok": True}
    assert observed["feature_store"] is store
    assert observed["request"] == PlayerGameFeatureRequest(
        player_id=1,
        game_id="g1",
        as_of_date=date(2025, 1, 1),
        team_id=11,
        opponent_id=22,
        is_home=False,
    )
