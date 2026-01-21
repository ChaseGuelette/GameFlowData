"""
Tests for player_prop_scraper module.

Covers progress tracking, API retry logic, parsing, and batch insert behavior.
"""

import importlib
import json
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

# Add src/scrapers to path for imports
SCRAPERS_PATH = Path(__file__).parent.parent / "src" / "scrapers"
sys.path.insert(0, str(SCRAPERS_PATH))


class FakeResponse:
    def __init__(self, status_code, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json_data


class DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConnection:
    def __init__(self):
        self.committed = False
        self.closed = False

    def cursor(self):
        return DummyCursor()

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture
def scraper_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("ODDS_API_KEY", "test-key")
    if "player_prop_scraper" in sys.modules:
        del sys.modules["player_prop_scraper"]
    return importlib.import_module("player_prop_scraper")


@pytest.fixture
def bot(scraper_module, tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    monkeypatch.setattr(scraper_module, "PROGRESS_FILE", str(progress_path))
    return scraper_module.ScraperBot("test-key", Mock())


def test_load_processed_ids_from_file(scraper_module, tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    progress_path.write_text(json.dumps([["2024-01-01", "game1"], ["2024-01-02", "game2"]]))
    monkeypatch.setattr(scraper_module, "PROGRESS_FILE", str(progress_path))

    scraper = scraper_module.ScraperBot("test-key", Mock())

    assert ("2024-01-01", "game1") in scraper.processed_ids
    assert ("2024-01-02", "game2") in scraper.processed_ids


def test_load_processed_ids_invalid_json_returns_empty(scraper_module, tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    progress_path.write_text("{bad json")
    monkeypatch.setattr(scraper_module, "PROGRESS_FILE", str(progress_path))

    scraper = scraper_module.ScraperBot("test-key", Mock())

    assert scraper.processed_ids == set()


def test_save_progress_writes_json(scraper_module, tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    monkeypatch.setattr(scraper_module, "PROGRESS_FILE", str(progress_path))

    scraper = scraper_module.ScraperBot("test-key", Mock())
    scraper.processed_ids = {("2024-01-01", "game1"), ("2024-01-02", "game2")}
    scraper.save_progress()

    data = json.loads(progress_path.read_text())
    assert set(tuple(x) for x in data) == scraper.processed_ids


def test_get_events_for_date_returns_data(bot):
    bot.session.get = Mock(return_value=FakeResponse(200, {"data": [{"id": "1"}]}))

    result = bot.get_events_for_date("2025-01-01T00:00:00Z")

    assert result == [{"id": "1"}]


def test_get_events_for_date_retries_on_rate_limit(bot, scraper_module, monkeypatch):
    bot.session.get = Mock(side_effect=[FakeResponse(429), FakeResponse(200, {"data": [{"id": "2"}]})])
    monkeypatch.setattr(scraper_module.time, "sleep", lambda *_: None)

    result = bot.get_events_for_date("2025-01-01T00:00:00Z")

    assert result == [{"id": "2"}]
    assert bot.session.get.call_count == 2


def test_get_events_for_date_handles_exception(bot, scraper_module, monkeypatch):
    bot.session.get = Mock(side_effect=Exception("boom"))
    monkeypatch.setattr(scraper_module.time, "sleep", lambda *_: None)

    result = bot.get_events_for_date("2025-01-01T00:00:00Z")

    assert result == []
    assert bot.session.get.call_count == 3


def test_scrape_event_props_success(bot):
    response = FakeResponse(
        200,
        {"data": {"id": "evt"}},
        headers={"x-requests-last": "5"},
    )
    bot.session.get = Mock(return_value=response)

    data, credits = bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")

    assert data == {"id": "evt"}
    assert credits == 5

    call_args, call_kwargs = bot.session.get.call_args
    params = call_kwargs["params"]
    assert "player_points" in params["markets"]
    assert params["oddsFormat"] == "american"


def test_scrape_event_props_handles_422(bot):
    bot.session.get = Mock(return_value=FakeResponse(422))

    data, credits = bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")

    assert data is None
    assert credits == 0


def test_scrape_event_props_rate_limit_then_success(bot, scraper_module, monkeypatch):
    bot.session.get = Mock(
        side_effect=[
            FakeResponse(429),
            FakeResponse(200, {"data": {"id": "evt"}}, headers={"x-requests-last": "2"}),
        ]
    )
    monkeypatch.setattr(scraper_module.time, "sleep", lambda *_: None)

    data, credits = bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")

    assert data == {"id": "evt"}
    assert credits == 2
    assert bot.session.get.call_count == 2


def test_scrape_event_props_out_of_credits_raises(bot):
    bot.session.get = Mock(return_value=FakeResponse(401, text="OUT_OF_USAGE_CREDITS"))

    with pytest.raises(Exception, match="Out of usage credits"):
        bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")


def test_scrape_event_props_other_error_returns_none(bot):
    bot.session.get = Mock(return_value=FakeResponse(500))

    data, credits = bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")

    assert data is None
    assert credits == 0


def test_scrape_event_props_exception_returns_none(bot, scraper_module, monkeypatch):
    bot.session.get = Mock(side_effect=requests.RequestException("boom"))
    monkeypatch.setattr(scraper_module.time, "sleep", lambda *_: None)

    data, credits = bot.scrape_event_props("2025-01-01T00:00:00Z", "evt123")

    assert data is None
    assert credits == 0
    assert bot.session.get.call_count == 3


def test_parse_and_store_returns_zero_on_missing_data(bot):
    assert bot.parse_and_store(None, "2025-01-01T00:00:00Z") == 0
    assert bot.parse_and_store({"id": "game"}, "2025-01-01T00:00:00Z") == 0
    assert bot.parse_and_store({"bookmakers": []}, "2025-01-01T00:00:00Z") == 0


def test_parse_and_store_inserts_expected_rows(bot, monkeypatch):
    inserted = []

    def fake_batch(rows):
        inserted.extend(rows)

    monkeypatch.setattr(bot, "_batch_insert", fake_batch)

    data = {
        "id": "game1",
        "commence_time": "2025-01-01T01:00:00Z",
        "home_team": "Home",
        "away_team": "Away",
        "bookmakers": [
            {
                "key": "book1",
                "title": "Book One",
                "last_update": "2025-01-01T00:30:00Z",
                "markets": [
                    {
                        "key": "player_points",
                        "last_update": "2025-01-01T00:25:00Z",
                        "outcomes": [
                            {
                                "description": "Player A",
                                "name": "Over",
                                "point": 20.5,
                                "price": -110,
                            },
                            {
                                "name": "Under",
                                "point": 20.5,
                                "price": -110,
                            },
                        ],
                    }
                ],
            }
        ],
    }

    count = bot.parse_and_store(data, "2025-01-01T00:00:00Z")

    assert count == 1
    assert len(inserted) == 1

    row = inserted[0]
    assert row[0] == "game1"
    assert row[1] == "Player A"
    assert row[2] == "book1"
    assert row[3] == "player_points"
    assert row[4] == "Over"
    assert row[5] == 20.5
    assert row[6] == -110
    assert row[7] == "2025-01-01T01:00:00Z"
    assert row[8] == "Home"
    assert row[9] == "Away"
    assert row[10] == "2025-01-01T00:00:00Z"
    assert row[11] == "2025-01-01T00:25:00Z"
    assert row[12] == "2025-01-01T00:30:00Z"
    assert row[13] == "Book One"


def test_batch_insert_executes_values(scraper_module, tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    monkeypatch.setattr(scraper_module, "PROGRESS_FILE", str(progress_path))

    engine = Mock()
    conn = DummyConnection()
    engine.raw_connection.return_value = conn

    execute_values = Mock()
    monkeypatch.setattr(scraper_module.extras, "execute_values", execute_values)

    scraper = scraper_module.ScraperBot("test-key", engine)
    rows = [
        (
            "game1",
            "Player A",
            "book1",
            "player_points",
            "Over",
            20.5,
            -110,
            "2025-01-01T01:00:00Z",
            "Home",
            "Away",
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:25:00Z",
            "2025-01-01T00:30:00Z",
            "Book One",
        )
    ]

    scraper._batch_insert(rows)

    execute_values.assert_called_once()
    args = execute_values.call_args[0]
    assert args[2] == rows
    assert "raw_player_props_staging_v2" in args[1]
    assert conn.committed is True
    assert conn.closed is True


def test_generate_snapshot_timestamps_filters_summer_and_sets_hours(scraper_module):
    snapshots = scraper_module.generate_snapshot_timestamps()

    assert snapshots
    assert all("timestamp" in snap and "season" in snap for snap in snapshots)

    months = {int(snap["timestamp"][5:7]) for snap in snapshots}
    assert months.isdisjoint({7, 8, 9})

    hours = {int(snap["timestamp"][11:13]) for snap in snapshots}
    assert hours.issubset({13, 19, 23})

    for snap in snapshots:
        if "Playoffs" in snap["season"]:
            dt = datetime.strptime(snap["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            assert dt.hour == 19
