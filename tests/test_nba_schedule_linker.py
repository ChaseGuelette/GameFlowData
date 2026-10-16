"""Railway props linkage uses competitive schedule identity, never stats.nba.com."""

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from src.processing import nba_linker_local as linker


def _row(*, game_id="0022600001", home=1610612738, away=1610612752, fresh=True):
    observed = datetime(2026, 9, 24, 17, 45, tzinfo=UTC) if fresh else datetime(2026, 9, 20, tzinfo=UTC)
    return SimpleNamespace(_mapping={
        "game_id": game_id,
        "game_date": date(2026, 10, 20),
        "home_team_id": home,
        "away_team_id": away,
        "game_status": 1,
        "status_text": "7:30 pm ET",
        "postponed_status": "N",
        "commence_time_utc": datetime(2026, 10, 20, 23, 30, tzinfo=UTC),
        "observed_at": observed,
        "source_updated_at": observed,
    })


def test_schedule_db_supplies_future_game_before_boxscore():
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = [_row()]
    with patch.object(linker, "_fetch_upcoming_games_from_cdn") as cdn:
        games = linker._fetch_upcoming_games_from_schedule(
            engine, date(2026, 10, 20), observed_at=datetime(2026, 9, 24, 18, tzinfo=UTC)
        )
    assert games == [{
        "game_id": "0022600001", "game_date": "2026-10-20", "home_team": "BOS", "away_team": "NYK"
    }]
    cdn.assert_not_called()
    query, params = engine.connect.return_value.__enter__.return_value.execute.call_args.args
    assert "public.nba_game_schedule" in str(query)
    assert "game_type IN ('002', '004', '005')" in str(query)
    assert params["target_date"] == date(2026, 10, 20)


def test_incomplete_or_stale_schedule_uses_cdn_instead_of_partial_db():
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = [_row(), _row(game_id="0022600002", fresh=False)]
    fallback = [{"game_id": "0022600003", "game_date": "2026-10-20", "home_team": "BOS", "away_team": "NYK"}]
    with patch.object(linker, "_fetch_upcoming_games_from_cdn", return_value=fallback) as cdn:
        games = linker._fetch_upcoming_games_from_schedule(
            engine, date(2026, 10, 20), observed_at=datetime(2026, 9, 24, 18, tzinfo=UTC)
        )
    assert games == fallback
    cdn.assert_called_once_with(date(2026, 10, 20))


def test_cdn_linker_excludes_cup_and_postponed_games():
    def game(game_id, *, postponed="N"):
        return {
            "gameId": game_id, "gameStatus": 1, "gameStatusText": "7:30 pm ET",
            "postponedStatus": postponed, "gameDateTimeUTC": "2026-10-20T23:30:00Z",
            "homeTeam": {"teamId": 1610612738}, "awayTeam": {"teamId": 1610612752},
        }
    now = datetime.now(ZoneInfo("America/New_York"))
    payload = {
        "meta": {"time": now.strftime("%Y-%m-%dT%H:%M:%S") + "Z"},
        "leagueSchedule": {"gameDates": [{"gameDate": "10/20/2026 00:00:00", "games": [
            game("0022600001"), game("0062600001"), game("0022600002", postponed="Y")
        ]}]},
    }
    with patch.object(linker, "fetch_schedule", return_value=payload):
        games = linker._fetch_upcoming_games_from_cdn(date(2026, 10, 20))
    assert [game["game_id"] for game in games] == ["0022600001"]


def test_incremental_live_path_does_not_call_nba_stats():
    source = (Path(__file__).resolve().parents[1] / "src" / "processing" / "nba_linker_local.py").read_text(encoding="utf-8")
    incremental = source.split("def link_incremental(", 1)[1]
    assert "upcoming_games = _fetch_upcoming_games_from_schedule(engine)" in incremental
    assert "_fetch_upcoming_games_from_nba_api(" not in incremental
