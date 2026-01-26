import logging
from unittest.mock import MagicMock, patch

import pytest

from src.scrapers.daily_player_props_scraper import DailyPlayerPropsScraper, run_historical_scrape, run_live_scrape


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def scraper(mock_engine):
    return DailyPlayerPropsScraper(api_key="test_key", db_engine=mock_engine)


def test_init(scraper, mock_engine):
    assert scraper.api_key == "test_key"
    assert scraper.engine == mock_engine
    assert scraper.session is not None


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_get_live_events_success(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": "game1"}, {"id": "game2"}]
    mock_get.return_value = mock_response

    events = scraper.get_live_events()

    assert len(events) == 2
    assert events[0]["id"] == "game1"
    mock_get.assert_called_once_with(
        "https://api.the-odds-api.com/v4/sports/basketball_nba/events",
        params={"apiKey": "test_key"},
        timeout=10,
    )


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_get_live_events_failure(mock_get, scraper, caplog):
    mock_get.side_effect = Exception("API Error")

    with caplog.at_level(logging.ERROR):
        events = scraper.get_live_events()

    assert events == []
    assert "Failed to fetch live events" in caplog.text


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_get_historical_events_success(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [{"id": "game1"}]}
    mock_get.return_value = mock_response

    events = scraper.get_historical_events("2023-10-25T12:00:00Z")

    assert len(events) == 1
    assert events[0]["id"] == "game1"
    mock_get.assert_called_once()


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_fetch_props_live_success(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "game1", "bookmakers": []}
    mock_response.headers = {"x-requests-last": "10"}
    mock_get.return_value = mock_response

    data, cost = scraper.fetch_props("game1", is_live=True)

    assert data["id"] == "game1"
    assert cost == 10
    mock_get.assert_called_once()
    assert "/odds" in mock_get.call_args[0][0]
    assert "historical" not in mock_get.call_args[0][0]


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_fetch_props_historical_success(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"id": "game1", "bookmakers": []}}
    mock_response.headers = {"x-requests-last": "5"}
    mock_get.return_value = mock_response

    data, cost = scraper.fetch_props("game1", date_str="2023-10-25T12:00:00Z", is_live=False)

    assert data["id"] == "game1"
    assert cost == 5
    mock_get.assert_called_once()
    assert "historical" in mock_get.call_args[0][0]


@patch("src.scrapers.daily_player_props_scraper.requests.Session.get")
def test_fetch_props_422(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.status_code = 422
    mock_get.return_value = mock_response

    data, cost = scraper.fetch_props("game1")

    assert data is None
    assert cost == 0


def test_parse_and_store_success(scraper):
    data = {
        "id": "game1",
        "commence_time": "2023-10-25T23:00:00Z",
        "home_team": "Team A",
        "away_team": "Team B",
        "bookmakers": [
            {
                "key": "book1",
                "title": "Book One",
                "last_update": "2023-10-25T22:00:00Z",
                "markets": [
                    {
                        "key": "player_points",
                        "last_update": "2023-10-25T22:00:00Z",
                        "outcomes": [{"description": "Player 1", "name": "Over", "point": 20.5, "price": -110}],
                    }
                ],
            }
        ],
    }

    scraper._batch_insert = MagicMock()

    count = scraper.parse_and_store(data, "2023-10-25T12:00:00Z", "test_table")

    assert count == 1
    scraper._batch_insert.assert_called_once()
    rows = scraper._batch_insert.call_args[0][0]
    assert rows[0][1] == "Player 1"
    assert rows[0][3] == "player_points"


def test_parse_and_store_empty(scraper):
    count = scraper.parse_and_store({}, "2023-10-25T12:00:00Z", "test_table")
    assert count == 0


@patch("src.scrapers.daily_player_props_scraper.extras.execute_values")
def test_batch_insert(mock_execute, scraper, mock_engine):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_engine.raw_connection.return_value = mock_conn

    rows = [
        (
            1,
            "Player",
            "Book",
            "Market",
            "Over",
            10.5,
            -110,
            "Time",
            "Home",
            "Away",
            "Snap",
            "Update",
            "BookUp",
            "BookName",
        )
    ]
    scraper._batch_insert(rows, "test_table")

    mock_execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("src.scrapers.daily_player_props_scraper.time.sleep")
def test_run_live_scrape(mock_sleep, scraper):
    scraper.get_live_events = MagicMock(return_value=[{"id": "game1", "home_team": "H", "away_team": "A"}])
    scraper.fetch_props = MagicMock(return_value=({"id": "game1", "bookmakers": []}, 10))
    scraper.parse_and_store = MagicMock(return_value=5)

    run_live_scrape(scraper)

    scraper.get_live_events.assert_called_once()
    scraper.fetch_props.assert_called_once_with("game1", is_live=True)
    scraper.parse_and_store.assert_called_once()


# Redo test_run_historical_scrape without complex datetime mocking
@patch("src.scrapers.daily_player_props_scraper.time.sleep")
def test_run_historical_scrape_simple(mock_sleep, scraper):
    # Use a date far in the past so snapshots are valid
    date_str = "2020-01-01"

    scraper.get_historical_events = MagicMock(return_value=[{"id": "game1"}])
    scraper.fetch_props = MagicMock(return_value=({"data": {}}, 5))
    scraper.parse_and_store = MagicMock(return_value=2)

    run_historical_scrape(scraper, date_str)

    # Should call for 17:00 and 23:00 UTC
    assert scraper.get_historical_events.call_count == 2
    assert scraper.fetch_props.call_count == 2
    assert scraper.parse_and_store.call_count == 2
