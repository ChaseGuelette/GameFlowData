import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.scrapers.injury_scraper_job import InjuryScraperJob


class TestInjuryScraperJob(unittest.TestCase):

    @patch("src.scrapers.injury_scraper_job.InjuryDatabase")
    @patch("src.scrapers.injury_scraper_job.ESPNInjuryScraper")
    def test_run_success(self, MockScraper, MockDB):
        """Test successful job execution."""

        # Setup Scraper Mock
        mock_scraper_instance = MockScraper.return_value
        mock_scraper_instance.scrape.return_value = ["inj1", "inj2"] # Mock list of injuries

        # Setup DB Mock
        mock_db_instance = MockDB.return_value
        mock_db_instance.get_latest_injuries.return_value = [] # No previous
        mock_db_instance.store_injuries.return_value = (2, 0) # 2 inserted

        job = InjuryScraperJob(log_dir="tests/logs")
        results = job.run()

        self.assertTrue(results["success"])
        self.assertEqual(results["stats"]["total_injuries"], 2)
        self.assertEqual(results["stats"]["inserted"], 2)

        # Verify interactions
        mock_db_instance.get_latest_injuries.assert_called_once()
        mock_scraper_instance.scrape.assert_called_once()
        mock_db_instance.store_injuries.assert_called_once()

    @patch("src.scrapers.injury_scraper_job.InjuryDatabase")
    @patch("src.scrapers.injury_scraper_job.ESPNInjuryScraper")
    def test_run_no_data(self, MockScraper, MockDB):
        """Test behavior when scraper returns nothing."""

        mock_scraper_instance = MockScraper.return_value
        mock_scraper_instance.scrape.return_value = []

        job = InjuryScraperJob(log_dir="tests/logs")
        results = job.run()

        self.assertFalse(results["success"])
        self.assertEqual(results.get("stats"), {})

        # Verify DB store was NOT called
        MockDB.return_value.store_injuries.assert_not_called()

    @patch("src.scrapers.injury_scraper_job.InjuryDatabase")
    @patch("src.scrapers.injury_scraper_job.ESPNInjuryScraper")
    def test_run_exception_handling(self, MockScraper, MockDB):
        """Test that job catches exceptions and logs failure."""

        mock_scraper_instance = MockScraper.return_value
        mock_scraper_instance.scrape.side_effect = Exception("API Error")

        job = InjuryScraperJob(log_dir="tests/logs")
        results = job.run()

        self.assertFalse(results["success"])
        self.assertTrue(len(results["errors"]) > 0, "No errors recorded in results")
        self.assertIn("API Error", results["errors"][0])

if __name__ == "__main__":
    unittest.main()
