from datetime import UTC, datetime

import pytest

from src.models.mlb.features.requests import (
    DateFeatureRequest,
    FeatureMode,
    PlayerGameFeatureRequest,
    TrainingFeatureRequest,
)


def test_training_feature_request_requires_seasons():
    with pytest.raises(ValueError, match="requires at least one season"):
        TrainingFeatureRequest(seasons=())

    req = TrainingFeatureRequest(seasons=(2025,), stat="hits")
    assert req.seasons == (2025,)
    assert req.stat == "hits"


def test_date_feature_request_requires_date_or_backtest_mode():
    as_of = datetime(2026, 5, 23, tzinfo=UTC)
    req = DateFeatureRequest(game_date="2026-05-23", mode=FeatureMode.BACKTEST, as_of_time=as_of)
    assert req.as_of_time == as_of

    with pytest.raises(ValueError, match="date_batch or backtest"):
        DateFeatureRequest(game_date="2026-05-23", mode=FeatureMode.TRAINING)


def test_player_game_feature_request_carries_context_fields():
    req = PlayerGameFeatureRequest(
        player_id=1,
        game_id=2,
        game_date="2026-05-23",
        team_id=10,
        opp_team_id=20,
        venue_id=30,
        season=2026,
        is_home=True,
    )
    assert req.player_id == 1
    assert req.venue_id == 30
    assert req.is_home is True
