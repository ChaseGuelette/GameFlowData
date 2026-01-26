import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test
# We need to patch sys.path before import if we were running this standalone,
# but pytest handles python path usually.
# However, the module itself modifies sys.path and imports from src.
# We will use patch to mock GameLineScraper.
from src.scrapers.daily_game_lines_scraper import scrape_odds_for_date


@patch("src.scrapers.daily_game_lines_scraper.GameLineScraper")
@patch("src.scrapers.daily_game_lines_scraper.engine")
def test_scrape_odds_for_date_success(mock_engine, mock_scraper_cls, caplog):
    # Setup mock scraper instance
    mock_bot = MagicMock()
    mock_scraper_cls.return_value = mock_bot

    # Mock data return
    # First snapshot: data, cost
    # Second snapshot: data, cost
    mock_bot.get_bulk_odds.side_effect = [([{"id": "game1"}], 5), ([{"id": "game2"}], 5)]
    mock_bot.parse_and_store.side_effect = [10, 10]

    # Use a past date to ensure snapshots are valid
    # 2023-01-01 is definitely in the past relative to now
    date_str = "2023-01-01"

    with caplog.at_level(logging.INFO):
        scrape_odds_for_date(date_str)

    assert mock_scraper_cls.call_count == 1
    # Check that get_bulk_odds was called twice (17:00 and 23:00)
    assert mock_bot.get_bulk_odds.call_count == 2

    # Verify calls args
    calls = mock_bot.get_bulk_odds.call_args_list
    assert "2023-01-01T17:00:00Z" in calls[0][0][0]
    assert "2023-01-01T23:00:00Z" in calls[1][0][0]

    assert mock_bot.parse_and_store.call_count == 2

    assert "Total Rows Saved: 20" in caplog.text
    assert "Total Credits Used: 10" in caplog.text


@patch("src.scrapers.daily_game_lines_scraper.GameLineScraper")
@patch("src.scrapers.daily_game_lines_scraper.engine")
@patch("src.scrapers.daily_game_lines_scraper.datetime")
def test_scrape_odds_for_date_future_skip(mock_datetime, mock_engine, mock_scraper_cls, caplog):
    # Mock datetime to control "now"
    # We want to simulate a time where 17:00 is past but 23:00 is future
    # Let's say "now" is 2023-01-01 20:00:00
    mock_now = datetime(2023, 1, 1, 20, 0, 0)
    mock_datetime.utcnow.return_value = mock_now

    # We also need strptime and constructor to work
    # Side effect for constructor to return real datetime objects
    def datetime_side_effect(*args, **kwargs):
        return datetime(*args, **kwargs)

    mock_datetime.side_effect = datetime_side_effect
    mock_datetime.strptime.side_effect = lambda d, f: datetime.strptime(d, f)

    # Setup mock scraper
    mock_bot = MagicMock()
    mock_scraper_cls.return_value = mock_bot
    mock_bot.get_bulk_odds.return_value = ([], 0)

    date_str = "2023-01-01"

    scrape_odds_for_date(date_str)

    # Should only call for 17:00, skip 23:00
    assert mock_bot.get_bulk_odds.call_count == 1
    args = mock_bot.get_bulk_odds.call_args[0][0]
    assert "17:00:00Z" in args


@patch("src.scrapers.daily_game_lines_scraper.sys.exit")
def test_scrape_odds_for_date_invalid_format(mock_exit, caplog):
    mock_exit.side_effect = SystemExit
    with pytest.raises(SystemExit):
        scrape_odds_for_date("invalid-date")
    mock_exit.assert_called_once_with(1)
    assert "Invalid date format" in caplog.text


@patch("src.scrapers.daily_game_lines_scraper.GameLineScraper")
@patch("src.scrapers.daily_game_lines_scraper.engine")
def test_scrape_odds_for_date_no_data(mock_engine, mock_scraper_cls, caplog):
    mock_bot = MagicMock()
    mock_scraper_cls.return_value = mock_bot
    mock_bot.get_bulk_odds.return_value = (None, 0)

    date_str = "2023-01-01"

    scrape_odds_for_date(date_str)

    assert mock_bot.get_bulk_odds.call_count == 2
    assert mock_bot.parse_and_store.call_count == 0
    assert "No data returned" in caplog.text
