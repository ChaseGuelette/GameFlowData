"""NBA daily discovery accepts only competitive game IDs across rollover."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from src.models.daily_runner import DailyPredictionRunner


def _runner():
    return DailyPredictionRunner(engine=MagicMock(), feature_store=Mock(), model_pipeline=Mock(), predictor=Mock())


def test_cdn_discovery_excludes_preseason_and_keeps_play_in():
    schedule = {"leagueSchedule": {"gameDates": [{
        "gameDate": "10/20/2026 00:00:00",
        "games": [
            {
                "gameId": game_id,
                "weekNumber": 1,
                "homeTeam": {"teamTricode": "BOS"},
                "awayTeam": {"teamTricode": "NYK"},
            }
            for game_id in ("0012600001", "0022600001", "0032600001", "0042600001", "0052600001")
        ],
    }]}}

    with patch("requests.get") as get:
        get.return_value.json.return_value = schedule
        games = _runner()._get_games_from_cdn(date(2026, 10, 20))

    assert [game["game_id"] for game in games] == ["0022600001", "0042600001", "0052600001"]


def test_nba_api_discovery_excludes_preseason_and_all_star():
    frame = pd.DataFrame({
        "GAME_ID": ["0012600001", "0022600001", "0032600001", "0042600001", "0052600001"],
        "HOME_TEAM_ID": [1] * 5,
        "VISITOR_TEAM_ID": [2] * 5,
    })
    with patch("nba_api.stats.endpoints.scoreboardv2.ScoreboardV2") as scoreboard, patch(
        "src.models.daily_runner.time.sleep"
    ):
        scoreboard.return_value.get_data_frames.return_value = [frame]
        games = _runner()._get_games_from_nba_api(date(2026, 10, 20))

    assert [game["game_id"] for game in games] == ["0022600001", "0042600001", "0052600001"]


def test_db_fallback_excludes_noncompetitive_games():
    runner = _runner()
    conn = runner.engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = [
        SimpleNamespace(_mapping={"game_id": game_id, "home_team_id": 1, "away_team_id": 2})
        for game_id in ("0012500001", "0022500001", "0032500001", "0042500001", "0052500001")
    ]

    games = runner._get_games_from_db(date(2025, 10, 21))

    assert [game["game_id"] for game in games] == ["0022500001", "0042500001", "0052500001"]
