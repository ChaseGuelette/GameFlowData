"""Behavior tests for the MLB sweep prediction-cache seam."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd


@dataclass
class FakePrediction:
    player_id: int
    game_id: int
    stat: str
    mean: float = 6.0
    median: float = 5.5
    q10: float = 2.0
    q25: float = 4.0
    q50: float = 5.5
    q75: float = 7.0
    q90: float = 9.0
    samples: np.ndarray | None = None

    def __post_init__(self):
        if self.samples is None:
            self.samples = np.array([4.0, 6.0, 8.0])


class FakePitcherFeatureStore:
    def __init__(self):
        self.calls = []

    def get_player_game_features(self, **kwargs):
        self.calls.append(kwargs)
        return {"pitcher_feature": kwargs["player_id"]}


class FakeBatterFeatureStore:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_features_for_date(self, game_date, *, stat, matchup_cache, as_of_time):
        self.calls.append({
            "game_date": game_date,
            "stat": stat,
            "matchup_cache": matchup_cache,
            "as_of_time": as_of_time,
        })
        return pd.DataFrame(self.rows)


class FakeSuite:
    def __init__(self, supported_stats):
        self.supported_stats = set(supported_stats)
        self.predict_calls = []

    def has_stat(self, stat):
        return stat in self.supported_stats

    def predict(self, stat, player_id, game_id, features):
        self.predict_calls.append({
            "stat": stat,
            "player_id": player_id,
            "game_id": game_id,
            "features": features,
        })
        return FakePrediction(player_id=int(player_id), game_id=int(game_id), stat=stat)

    def get_model_type(self, stat):
        return f"model-type:{stat}"


def test_extract_probable_pitchers_preserves_home_away_context_and_skips_missing_pitchers():
    from src.backtesting.mlb.prediction_cache import extract_probable_pitchers

    game_date = date(2025, 7, 4)
    games = [
        {
            "game_id": "10",
            "home_team_id": 1,
            "away_team_id": 2,
            "probable_pitcher_home_id": "101",
            "probable_pitcher_away_id": None,
            "venue_id": 55,
            "season": 2025,
        },
        {
            "game_id": 11,
            "home_team_id": 3,
            "away_team_id": 4,
            "probable_pitcher_home_id": None,
            "probable_pitcher_away_id": 202,
        },
    ]

    assert extract_probable_pitchers(games, game_date) == [
        {
            "player_id": 101,
            "game_id": 10,
            "team_id": 1,
            "opponent_id": 2,
            "is_home": True,
            "venue_id": 55,
            "season": 2025,
        },
        {
            "player_id": 202,
            "game_id": 11,
            "team_id": 4,
            "opponent_id": 3,
            "is_home": False,
            "venue_id": 0,
            "season": 2025,
        },
    ]


def test_build_predictions_for_date_routes_pitcher_and_batter_features_with_as_of_time():
    from src.backtesting.mlb.prediction_cache import DatePrediction, build_predictions_for_date

    game_date = date(2025, 7, 4)
    as_of_time = datetime(2025, 7, 4, 17, 30)
    matchup_cache = {2025: (pd.DataFrame(), pd.DataFrame())}
    games = [
        {
            "game_id": 10,
            "home_team_id": 1,
            "away_team_id": 2,
            "probable_pitcher_home_id": 101,
            "probable_pitcher_away_id": None,
            "venue_id": 55,
            "season": 2025,
        }
    ]
    pitcher_store = FakePitcherFeatureStore()
    batter_store = FakeBatterFeatureStore([
        {"player_id": 301, "game_id": 10, "team_id": 1, "opp_team_id": 2, "bat_feature": 99}
    ])
    suite = FakeSuite({"pitcher_strikeouts", "batter_hits"})

    predictions = build_predictions_for_date(
        pitcher_feature_store=pitcher_store,
        batter_feature_store=batter_store,
        suite=suite,
        game_date=game_date,
        games=games,
        stats=["pitcher_strikeouts", "batter_hits", "batter_total_bases"],
        matchup_cache=matchup_cache,
        as_of_time=as_of_time,
    )

    assert [p.stat for p in predictions] == ["pitcher_strikeouts", "batter_hits"]
    assert all(isinstance(p, DatePrediction) for p in predictions)
    assert predictions[0].model_type == "quantile"
    assert predictions[0].team_id == 1
    assert predictions[0].opponent_id == 2
    assert predictions[1].model_type == "model-type:batter_hits"
    assert predictions[1].team_id == 1
    assert predictions[1].opponent_id == 2
    assert np.array_equal(predictions[1].samples, np.array([4.0, 6.0, 8.0]))

    assert pitcher_store.calls == [
        {
            "player_id": 101,
            "game_id": 10,
            "game_date": "2025-07-04",
            "team_id": 1,
            "opp_team_id": 2,
            "venue_id": 55,
            "season": 2025,
            "is_home": True,
            "as_of_time": as_of_time,
        }
    ]
    assert batter_store.calls == [
        {
            "game_date": "2025-07-04",
            "stat": "hits",
            "matchup_cache": matchup_cache,
            "as_of_time": as_of_time,
        }
    ]
    assert suite.predict_calls[0]["features"] == {"pitcher_feature": 101}
    assert suite.predict_calls[1]["features"]["bat_feature"] == 99


def test_build_predictions_for_date_skips_pitchers_with_missing_features_and_absent_batter_store():
    from src.backtesting.mlb.prediction_cache import build_predictions_for_date

    class MissingPitcherFeatureStore(FakePitcherFeatureStore):
        def get_player_game_features(self, **kwargs):
            self.calls.append(kwargs)
            return None

    games = [
        {
            "game_id": 10,
            "home_team_id": 1,
            "away_team_id": 2,
            "probable_pitcher_home_id": 101,
            "probable_pitcher_away_id": None,
        }
    ]
    suite = FakeSuite({"pitcher_strikeouts", "batter_hits"})

    predictions = build_predictions_for_date(
        pitcher_feature_store=MissingPitcherFeatureStore(),
        batter_feature_store=None,
        suite=suite,
        game_date=date(2025, 7, 4),
        games=games,
        stats=["pitcher_strikeouts", "batter_hits"],
    )

    assert predictions == []
    assert suite.predict_calls == []
