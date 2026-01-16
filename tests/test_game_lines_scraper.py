"""
Tests for game_lines_scraper module.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

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
def module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("ODDS_API_KEY", "test-key")
    if "game_lines_scraper" in sys.modules:
        del sys.modules["game_lines_scraper"]
    return importlib.import_module("game_lines_scraper")


@pytest.fixture
def bot(module):
    return module.GameLineScraper("test-key", Mock())


def test_get_bulk_odds_success(bot):
    response = FakeResponse(
        200,
        {"data": [{"id": "game1"}]},
        headers={"x-requests-last": "30"},
    )
    bot.session.get = Mock(return_value=response)

    data, credits = bot.get_bulk_odds("2025-01-01T00:00:00Z")

    assert data == {"data": [{"id": "game1"}]}
    assert credits == 30


def test_get_bulk_odds_retries_on_rate_limit(bot, module, monkeypatch):
    bot.session.get = Mock(
        side_effect=[FakeResponse(429), FakeResponse(200, {"data": []}, {"x-requests-last": "1"})]
    )
    monkeypatch.setattr(module.time, "sleep", lambda *_: None)

    data, credits = bot.get_bulk_odds("2025-01-01T00:00:00Z")

    assert data == {"data": []}
    assert credits == 1
    assert bot.session.get.call_count == 2


def test_get_bulk_odds_unauthorized_returns_none(bot):
    bot.session.get = Mock(return_value=FakeResponse(401))

    data, credits = bot.get_bulk_odds("2025-01-01T00:00:00Z")

    assert data is None
    assert credits == 0


def test_get_bulk_odds_exception_returns_none(bot, module, monkeypatch):
    bot.session.get = Mock(side_effect=Exception("boom"))
    monkeypatch.setattr(module.time, "sleep", lambda *_: None)

    data, credits = bot.get_bulk_odds("2025-01-01T00:00:00Z")

    assert data is None
    assert credits == 0
    assert bot.session.get.call_count == 3


def test_parse_and_store_returns_zero_on_missing_data(bot):
    assert bot.parse_and_store(None, "2025-01-01T00:00:00Z") == 0
    assert bot.parse_and_store({"meta": {}}, "2025-01-01T00:00:00Z") == 0


def test_parse_and_store_inserts_rows(bot, monkeypatch):
    inserted = []

    def fake_batch(rows):
        inserted.extend(rows)

    monkeypatch.setattr(bot, "_batch_insert", fake_batch)

    response_json = {
        "data": [
            {
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
                                "key": "h2h",
                                "last_update": "2025-01-01T00:25:00Z",
                                "outcomes": [
                                    {"name": "Home", "price": -110},
                                    {"name": "Away", "price": 100, "point": None},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    count = bot.parse_and_store(response_json, "2025-01-01T00:00:00Z")

    assert count == 2
    assert len(inserted) == 2

    row = inserted[0]
    assert row[0] == "game1"
    assert row[1] == "book1"
    assert row[2] == "h2h"
    assert row[3] == "Home"
    assert row[4] is None
    assert row[5] == -110
    assert row[6] == "2025-01-01T01:00:00Z"
    assert row[7] == "Home"
    assert row[8] == "Away"
    assert row[9] == "2025-01-01T00:00:00Z"
    assert row[10] == "2025-01-01T00:25:00Z"
    assert row[11] == "2025-01-01T00:30:00Z"
    assert row[12] == "Book One"


def test_batch_insert_executes_values(module):
    engine = Mock()
    conn = DummyConnection()
    engine.raw_connection.return_value = conn

    execute_values = Mock()
    module.extras.execute_values = execute_values

    scraper = module.GameLineScraper("test-key", engine)
    rows = [
        (
            "game1",
            "book1",
            "h2h",
            "Home",
            None,
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
    assert "raw_game_lines_staging" in args[1]
    assert conn.committed is True
    assert conn.closed is True


def test_generate_historical_schedule_has_expected_hours(module):
    snapshots = module.generate_historical_schedule()

    assert snapshots
    assert all(ts.endswith("Z") for ts in snapshots)

    hours = {int(ts[11:13]) for ts in snapshots}
    assert hours == {17, 23}
