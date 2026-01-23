import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import Timeout

from src.scrapers.live_odds_scraper import LiveOddsScraper


class TestLiveOddsScraper(unittest.TestCase):
    def setUp(self):
        self.mock_engine = MagicMock()
        self.api_key = "test_key"
        self.scraper = LiveOddsScraper(self.api_key, self.mock_engine)

    @patch("src.scrapers.live_odds_scraper.extras.execute_values")
    @patch("src.scrapers.live_odds_scraper.requests.Session.get")
    def test_successful_fetch_and_parse(self, mock_get, mock_execute_values):
        """Test successful API response parsing and DB insertion."""
        # Mock API Response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "game1",
                "commence_time": "2024-01-01T00:00:00Z",
                "home_team": "Team A",
                "away_team": "Team B",
                "bookmakers": [
                    {
                        "key": "book1",
                        "title": "Book One",
                        "last_update": "2024-01-01T00:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "last_update": "2024-01-01T00:00:00Z",
                                "outcomes": [{"name": "Team A", "price": 150}, {"name": "Team B", "price": -180}],
                            }
                        ],
                    }
                ],
            }
        ]
        mock_get.return_value = mock_response

        # Run
        data = self.scraper.fetch_live_odds()
        count = self.scraper.parse_and_store(data)

        # Assertions
        self.assertEqual(count, 2)  # 2 outcomes
        mock_execute_values.assert_called()

    @patch("src.scrapers.live_odds_scraper.extras.execute_values")
    @patch("src.scrapers.live_odds_scraper.requests.Session.get")
    def test_data_integrity(self, mock_get, mock_execute_values):
        """Verify integer/float values are preserved."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "g1",
                "commence_time": "2024-01-01",
                "home_team": "H",
                "away_team": "A",
                "bookmakers": [
                    {
                        "key": "b1",
                        "title": "B1",
                        "last_update": "2024-01-01",
                        "markets": [
                            {
                                "key": "spreads",
                                "last_update": "2024-01-01",
                                "outcomes": [{"name": "H", "price": -110, "point": -5.5}],
                            }
                        ],
                    }
                ],
            }
        ]
        mock_get.return_value = mock_response

        data = self.scraper.fetch_live_odds()
        self.scraper.parse_and_store(data)

        # Check args passed to execute_values
        args, _ = mock_execute_values.call_args
        rows = args[2]  # 3rd arg is rows
        row = rows[0]

        # Check Line (-5.5) and Odds (-110)
        self.assertEqual(row[4], -5.5)  # line is index 4
        self.assertEqual(row[5], -110)  # odds is index 5

    @patch("src.scrapers.live_odds_scraper.requests.Session.get")
    def test_api_failure(self, mock_get):
        """Test non-200 response handling."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("500 Error")
        mock_get.return_value = mock_response

        data = self.scraper.fetch_live_odds()
        self.assertEqual(data, [])  # Should return empty list

    @patch("src.scrapers.live_odds_scraper.requests.Session.get")
    def test_api_timeout(self, mock_get):
        """Test timeout handling."""
        mock_get.side_effect = Timeout("Timed out")

        data = self.scraper.fetch_live_odds()
        self.assertEqual(data, [])

    @patch("src.scrapers.live_odds_scraper.requests.Session.get")
    def test_partial_data(self, mock_get):
        """Test robustness against missing keys."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "game1",
                # Missing 'bookmakers'
            }
        ]
        mock_get.return_value = mock_response

        data = self.scraper.fetch_live_odds()
        count = self.scraper.parse_and_store(data)
        self.assertEqual(count, 0)  # Should be safe

    def test_db_table_creation(self):
        """Verify DDL execution on init."""
        # SetUp already called init, so check mock_engine calls
        # We expect a begin() -> execute(text(...))
        self.mock_engine.begin.assert_called()

    def test_empty_api_response(self):
        """Test empty list from API."""
        count = self.scraper.parse_and_store([])
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
