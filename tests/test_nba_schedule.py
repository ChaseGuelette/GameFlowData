"""NBA CDN schedule parsing, canary, and explicit ingest behavior."""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.scrapers.nba_schedule import (
    canary_summary,
    ingest_schedule,
    parse_schedule,
    upsert_schedule,
)


def _game(
    game_id: str = "0022600001",
    *,
    status: int = 1,
    status_text: str = "7:30 pm ET",
    start: str | None = "2026-10-20T23:30:00Z",
    home_id: int | None = 1610612738,
    away_id: int | None = 1610612752,
    arena: str | None = "TD Garden",
    postponed: str = "N",
) -> dict:
    game = {
        "gameId": game_id,
        "gameStatus": status,
        "gameStatusText": status_text,
        "postponedStatus": postponed,
        "homeTeam": {"teamId": home_id},
        "awayTeam": {"teamId": away_id},
        "arenaName": arena,
    }
    if start is not None:
        game["gameDateTimeUTC"] = start
    return game


def _payload(*games: dict, game_date: str = "10/20/2026 00:00:00") -> dict:
    return {
        "meta": {"time": "2026-09-24T13:19:02.192Z"},
        "leagueSchedule": {
            "gameDates": [{"gameDate": game_date, "games": list(games)}],
        },
    }


def test_parse_schedule_uses_utc_start_and_preserves_game_identity():
    observed_at = datetime(2026, 9, 24, 14, 0, tzinfo=UTC)

    rows = parse_schedule(_payload(_game()), observed_at=observed_at)

    assert rows == [
        {
            "game_id": "0022600001",
            "season_id": "22026",
            "season": "2026-27",
            "game_type": "002",
            "game_date": date(2026, 10, 20),
            "commence_time_utc": datetime(2026, 10, 20, 23, 30, tzinfo=UTC),
            "home_team_id": 1610612738,
            "away_team_id": 1610612752,
            "game_status": 1,
            "status_text": "7:30 pm ET",
            "postponed_status": "N",
            "venue_name": "TD Garden",
            "source": "nba_cdn",
            "source_updated_at": datetime(2026, 9, 24, 17, 19, 2, 192000, tzinfo=UTC),
            "observed_at": observed_at,
        }
    ]


def test_parse_schedule_handles_tbd_and_excludes_every_006_game():
    rows = parse_schedule(
        _payload(
            _game(home_id=0, away_id=None, start=None, status_text="TBD"),
            _game("0062600001"),
            _game("0062600099", status=3, status_text="Final"),
            _game("0012600001"),
            _game("0032600001"),
            _game("0042600001"),
            _game("0052600001"),
        )
    )

    assert [row["game_id"] for row in rows] == ["0022600001", "0042600001", "0052600001"]
    assert rows[0]["home_team_id"] is None
    assert rows[0]["away_team_id"] is None
    assert rows[0]["commence_time_utc"] is None


def test_parse_schedule_rejects_missing_dates_and_ambiguous_duplicates():
    with pytest.raises(ValueError, match="gameDates"):
        parse_schedule({"meta": {}, "leagueSchedule": {}})
    with pytest.raises(ValueError, match="gameDates"):
        parse_schedule({"meta": {}, "leagueSchedule": {"gameDates": []}})

    duplicate = _payload(
        _game(),
        _game(start="2026-10-21T00:00:00Z", status=2),
    )
    with pytest.raises(ValueError, match="Duplicate game ID"):
        parse_schedule(duplicate)


@pytest.mark.parametrize(
    "bad_game",
    [
        _game(game_id="226000001"),
        {**_game(), "homeTeam": "BOS"},
        {**_game(), "gameStatus": "scheduled"},
        {**_game(), "gameDateTimeUTC": "not-a-time"},
    ],
)
def test_parse_schedule_rejects_malformed_competitive_games(bad_game):
    with pytest.raises(ValueError):
        parse_schedule(_payload(bad_game))


def test_upsert_schedule_is_parameterized_and_updates_mutable_fields():
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    first = parse_schedule(_payload(_game()))
    changed = parse_schedule(
        _payload(
            _game(status=2, status_text="Q1", start="2026-10-21T00:00:00Z"),
            game_date="10/21/2026 00:00:00",
        )
    )

    assert upsert_schedule(engine, first) == 1
    assert upsert_schedule(engine, changed) == 1

    first_sql, first_params = conn.execute.call_args_list[0].args
    second_sql, second_params = conn.execute.call_args_list[1].args
    sql = str(first_sql)
    assert "ON CONFLICT (game_id) DO UPDATE" in sql
    assert "commence_time_utc = EXCLUDED.commence_time_utc" in sql
    assert ":game_id" in sql
    assert first_params[0]["game_id"] == second_params[0]["game_id"] == "0022600001"
    assert second_params[0]["game_date"] == date(2026, 10, 21)
    assert second_params[0]["game_status"] == 2


def test_malformed_or_failed_source_never_opens_write_transaction():
    engine = MagicMock()

    with pytest.raises(ValueError):
        ingest_schedule(engine=engine, schedule_data={"leagueSchedule": {}})
    engine.begin.assert_not_called()

    with patch("src.scrapers.nba_schedule.fetch_schedule", side_effect=RuntimeError("CDN down")):
        with pytest.raises(RuntimeError, match="CDN down"):
            ingest_schedule(engine=engine)
    engine.begin.assert_not_called()

    with pytest.raises(ValueError, match="stale"):
        ingest_schedule(engine=engine, schedule_data=_payload(_game()), observed_at=datetime(2026, 10, 1, tzinfo=UTC))
    engine.begin.assert_not_called()


def test_canary_is_read_only_and_reports_exclusions_and_tbd():
    schedule = _payload(
        _game(),
        _game("0062600001"),
        _game("0022600002", home_id=0, away_id=0, start=None, status_text="TBD"),
    )

    with patch("src.scrapers.nba_schedule.fetch_schedule", return_value=schedule) as fetch, patch(
        "src.db.client.get_engine"
    ) as get_engine:
        summary = canary_summary(observed_at=datetime(2026, 9, 24, 18, tzinfo=UTC))

    fetch.assert_called_once_with()
    get_engine.assert_not_called()
    assert summary["included_games"] == 2
    assert summary["excluded_006"] == 1
    assert summary["tbd_games"] == 1
    assert summary["missing_utc"] == 1
    assert summary["date_start"] == date(2026, 10, 20)
    assert summary["date_end"] == date(2026, 10, 20)
    assert summary["source_updated_at"] == datetime(2026, 9, 24, 17, 19, 2, 192000, tzinfo=UTC)


def test_canary_cli_writes_read_only_json_report(tmp_path, monkeypatch):
    from src.scrapers.nba_schedule import main

    report = tmp_path / "canary.json"
    monkeypatch.setattr(sys, "argv", ["nba_schedule.py", "--canary", "--output", str(report)])
    payload = _payload(_game(), _game("0062600001"))
    payload["meta"]["time"] = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    with patch("src.scrapers.nba_schedule.fetch_schedule", return_value=payload):
        with patch("src.db.client.get_engine") as get_engine:
            main()

    assert json.loads(report.read_text(encoding="utf-8"))["included_games"] == 1
    assert json.loads(report.read_text(encoding="utf-8"))["excluded_006"] == 1
    get_engine.assert_not_called()



def test_canary_rejects_stale_metadata():
    with pytest.raises(ValueError, match="stale"):
        canary_summary(_payload(_game()), observed_at=datetime(2026, 10, 1, tzinfo=UTC))


def test_upsert_does_not_regress_newer_source_snapshot():
    from src.scrapers.nba_schedule import UPSERT_SQL

    assert "EXCLUDED.source_updated_at >= public.nba_game_schedule.source_updated_at" in str(UPSERT_SQL)


def test_parser_retains_postponement_status():
    row = parse_schedule(_payload(_game(postponed="Y")), observed_at=datetime(2026, 9, 24, tzinfo=UTC))[0]
    assert row["postponed_status"] == "Y"


def test_cdn_meta_time_is_eastern_wall_clock_even_with_z_suffix():
    payload = _payload(_game())
    payload["meta"]["time"] = "2026-12-01T10:30:00Z"
    row = parse_schedule(payload)[0]
    assert row["source_updated_at"] == datetime(2026, 12, 1, 15, 30, tzinfo=UTC)
