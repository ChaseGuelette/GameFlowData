from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np

from src.models.mlb.mlb_daily_runner import MLBDailyPredictionRunner


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


class FakePitcherPredictor:
    def predict_batch(self, player_games):
        return [
            FakePrediction(player_id=int(player_id), game_id=int(game_id), stat="pitcher_strikeouts")
            for player_id, game_id, _features in player_games
        ]


class FakeSuite:
    batter_stats = ["batter_hits"]

    def has_stat(self, stat):
        return stat == "batter_hits"

    def get_model_type(self, stat):
        return "negbin"

    def get_predictor(self, stat):
        return None

    def predict_batch(self, stat, player_games):
        return [
            FakePrediction(player_id=int(player_id), game_id=int(game_id), stat=stat)
            for player_id, game_id, _features in player_games
        ]


class FakeNameResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeNameConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        return FakeNameResult([(pid, f"Pitcher {pid}") for pid in params["pids"]])


class FakeEngine:
    def connect(self):
        return FakeNameConn()


def test_daily_runner_pitcher_predictions_use_explicit_inference_loader(monkeypatch):
    from src.models.mlb.features.pitcher_inference_loader import PitcherInferenceLoader

    class DirectPitcherFeatureStore:
        def get_player_game_features(self, **kwargs):
            raise AssertionError("daily runner should use PitcherInferenceLoader")

    loader_calls = []

    def fake_load_player_game(self, request):
        loader_calls.append(request)
        return {"pitcher_feature": request.player_id}

    monkeypatch.setattr(PitcherInferenceLoader, "load_player_game", fake_load_player_game)

    runner = MLBDailyPredictionRunner(
        FakeEngine(),
        pitcher_feature_store=DirectPitcherFeatureStore(),
        pitcher_k_predictor=FakePitcherPredictor(),
    )

    predictions, samples, features = runner._run_pitcher_predictions(
        [
            {
                "player_id": 101,
                "game_id": 10,
                "team_id": 1,
                "opponent_id": 2,
                "venue_id": 55,
                "season": 2025,
                "is_home": True,
            }
        ],
        date(2025, 7, 4),
        ["pitcher_strikeouts"],
    )

    assert [p["stat"] for p in predictions] == ["pitcher_strikeouts"]
    assert loader_calls[0].player_id == 101
    assert loader_calls[0].game_date == "2025-07-04"
    assert (101, 10, "pitcher_strikeouts") in samples
    assert features[(101, 10)] == {"pitcher_feature": 101}


def test_daily_runner_batter_predictions_use_explicit_inference_loader(monkeypatch):
    from src.models.mlb.features.batter_inference_loader import BatterInferenceLoader

    class DirectBatterFeatureStore:
        def get_player_game_features(self, **kwargs):
            raise AssertionError("daily runner should use BatterInferenceLoader")

    loader_calls = []

    def fake_load_player_game(self, request, *, stat="hits", opp_pitcher_id=None, lineup_pos=None):
        loader_calls.append((request, stat, opp_pitcher_id, lineup_pos))
        return {"batter_feature": request.player_id, "batter_avg_ab_l5": 4.0}

    monkeypatch.setattr(BatterInferenceLoader, "load_player_game", fake_load_player_game)

    runner = MLBDailyPredictionRunner(
        FakeEngine(),
        suite=FakeSuite(),
        batter_feature_store=DirectBatterFeatureStore(),
    )
    monkeypatch.setattr(
        runner,
        "_bulk_fetch_batter_prop_lines",
        lambda batter_features, available_stats: {(301, 10, "batter_hits"): 1.5},
    )

    predictions, samples, features = runner._run_batter_predictions(
        [
            {
                "player_id": 301,
                "player_name": "Batter 301",
                "game_id": 10,
                "team_id": 1,
                "opponent_id": 2,
                "venue_id": 55,
                "season": 2025,
                "is_home": True,
                "opp_pitcher_id": 101,
                "confirmed_lineup_pos": 3,
            }
        ],
        date(2025, 7, 4),
        ["batter_hits"],
    )

    assert [p["stat"] for p in predictions] == ["batter_hits"]
    assert loader_calls[0][0].player_id == 301
    assert loader_calls[0][0].game_date == "2025-07-04"
    assert loader_calls[0][1] == "hits"
    assert loader_calls[0][2] == 101
    assert loader_calls[0][3] == 3
    assert (301, 10, "batter_hits") in samples
    assert features[(301, 10)]["batter_feature"] == 301
