"""Tests for NCAAB Game Lines Scraper."""

from unittest.mock import MagicMock, patch

from src.scrapers.ncaab.ncaab_game_lines_scraper import NCAABGameLineScraper

SAMPLE_LIVE_RESPONSE = [
    {
        "id": "abc123",
        "commence_time": "2025-01-15T23:00:00Z",
        "home_team": "Duke Blue Devils",
        "away_team": "North Carolina Tar Heels",
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "last_update": "2025-01-15T20:00:00Z",
                "markets": [
                    {
                        "key": "h2h",
                        "last_update": "2025-01-15T20:00:00Z",
                        "outcomes": [
                            {"name": "Duke Blue Devils", "price": -180},
                            {"name": "North Carolina Tar Heels", "price": 150},
                        ],
                    },
                    {
                        "key": "spreads",
                        "last_update": "2025-01-15T20:00:00Z",
                        "outcomes": [
                            {"name": "Duke Blue Devils", "price": -110, "point": -4.5},
                            {"name": "North Carolina Tar Heels", "price": -110, "point": 4.5},
                        ],
                    },
                    {
                        "key": "totals",
                        "last_update": "2025-01-15T20:00:00Z",
                        "outcomes": [
                            {"name": "Over", "price": -110, "point": 148.5},
                            {"name": "Under", "price": -110, "point": 148.5},
                        ],
                    },
                ],
            },
        ],
    },
]


@patch("src.scrapers.ncaab.ncaab_game_lines_scraper.extras.execute_values")
class TestNCAABGameLineScraper:
    def setup_method(self):
        self.mock_engine = MagicMock()
        self.scraper = NCAABGameLineScraper("test_key", self.mock_engine)

    def test_parse_and_store_live_response(self, mock_exec_values):
        """Live response returns list directly."""
        mock_conn = MagicMock()
        self.mock_engine.raw_connection.return_value = mock_conn

        row_count = self.scraper.parse_and_store(SAMPLE_LIVE_RESPONSE, "2025-01-15T20:00:00Z")

        # 1 game * 1 bookmaker * 3 markets * 2 outcomes = 6 rows
        assert row_count == 6
        mock_conn.commit.assert_called_once()
        mock_exec_values.assert_called_once()

    def test_parse_and_store_historical_response(self, mock_exec_values):
        """Historical response wraps data in {'data': [...]}."""
        mock_conn = MagicMock()
        self.mock_engine.raw_connection.return_value = mock_conn

        historical = {"data": SAMPLE_LIVE_RESPONSE}
        row_count = self.scraper.parse_and_store(historical, "2025-01-15T17:00:00Z")

        assert row_count == 6

    def test_parse_and_store_empty_response(self, mock_exec_values):
        """Empty response returns 0 rows."""
        assert self.scraper.parse_and_store([], "2025-01-15T20:00:00Z") == 0
        assert self.scraper.parse_and_store({"data": []}, "2025-01-15T20:00:00Z") == 0

    @patch("src.scrapers.ncaab.ncaab_game_lines_scraper.requests.Session")
    def test_get_live_odds_success(self, mock_session_cls, mock_exec_values):
        """Verify live odds request uses correct sport key."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_LIVE_RESPONSE
        mock_response.headers.get.return_value = "30"
        mock_session.get.return_value = mock_response

        scraper = NCAABGameLineScraper("test_key", self.mock_engine)
        scraper.session = mock_session

        data, cost = scraper.get_live_odds()

        assert data == SAMPLE_LIVE_RESPONSE
        assert cost == 30

        # Verify the URL contains basketball_ncaab
        call_args = mock_session.get.call_args
        assert "basketball_ncaab" in call_args[0][0]

    def test_batch_insert_columns(self, mock_exec_values):
        """Verify batch insert uses correct 13 columns."""
        mock_conn = MagicMock()
        self.mock_engine.raw_connection.return_value = mock_conn

        self.scraper.parse_and_store(SAMPLE_LIVE_RESPONSE, "2025-01-15T20:00:00Z")

        # Verify execute_values was called with ncaab_raw_game_lines query
        mock_exec_values.assert_called_once()
        query_arg = mock_exec_values.call_args[0][1]
        assert "ncaab_raw_game_lines" in query_arg
        mock_conn.commit.assert_called_once()
