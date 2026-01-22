# Adjust path to find the src module
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.scrapers.espn_injury_scraper import InjuryRecord
from src.scrapers.injury_database import InjuryDatabase


class TestInjuryDatabase(unittest.TestCase):

    def setUp(self):
        # Mock engine and connection
        self.mock_engine = MagicMock()
        self.mock_connection = self.mock_engine.connect.return_value.__enter__.return_value
        self.mock_begin = self.mock_engine.begin.return_value.__enter__.return_value

    @patch("src.scrapers.injury_database.get_engine")
    def test_init_creates_tables(self, mock_get_engine):
        """Verify initialization creates tables using DDL."""
        mock_get_engine.return_value = self.mock_engine

        InjuryDatabase()

        # Verify engine was requested
        mock_get_engine.assert_called_once()
        # Verify DDL execution
        self.mock_begin.execute.assert_called()
        # Ensure DDL contains create table statement
        args = self.mock_begin.execute.call_args_list[0]
        ddl_text = args[0][0].text
        self.assertIn("CREATE TABLE IF NOT EXISTS public.espn_injuries", ddl_text)

    @patch("src.scrapers.injury_database.get_engine")
    def test_store_injuries_empty(self, mock_get_engine):
        """Store empty list returns (0, 0)."""
        mock_get_engine.return_value = self.mock_engine
        db = InjuryDatabase()

        inserted, updated = db.store_injuries([])
        self.assertEqual(inserted, 0)
        self.assertEqual(updated, 0)

    @patch("src.scrapers.injury_database.get_engine")
    def test_store_injuries_execution(self, mock_get_engine):
        """Verify SQL execution for storing injuries."""
        mock_get_engine.return_value = self.mock_engine
        db = InjuryDatabase()

        record = InjuryRecord(
            espn_injury_id="1", espn_player_id="p1", player_name="Test Player",
            team_id="t1", team_name="Team", status="Out",
            injury_type="Knee", injury_location="Knee", injury_side="Left", injury_detail="Sprain",
            return_date="Soon", date_reported="2024-01-01", short_comment="Hurt", long_comment="Really hurt",
            fantasy_status="Out", scrape_timestamp="2024-01-01T12:00:00Z"
        )

        # Mock execution result
        mock_result = MagicMock()
        mock_result.rowcount = 1
        self.mock_begin.execute.return_value = mock_result

        inserted, _ = db.store_injuries([record])

        self.assertEqual(inserted, 1)
        # Verify execute was called twice (1 for table create, 1 for insert)
        # Check the insert call
        insert_call = self.mock_begin.execute.call_args
        sql_text = insert_call[0][0].text
        self.assertIn("INSERT INTO espn_injuries", sql_text)
        self.assertIn("ON CONFLICT (espn_injury_id, scrape_timestamp)", sql_text)

    @patch("src.scrapers.injury_database.get_engine")
    def test_get_latest_injuries(self, mock_get_engine):
        """Verify SQL select for latest injuries."""
        mock_get_engine.return_value = self.mock_engine
        db = InjuryDatabase()

        # Mock first query (MAX timestamp)
        self.mock_connection.execute.return_value.scalar.return_value = "2024-01-01T12:00:00Z"

        # Mock second query (Select rows)
        mock_rows = [
            {"player_name": "Player 1", "status": "Out"},
            {"player_name": "Player 2", "status": "Day-to-Day"}
        ]
        mock_result_proxy = MagicMock()
        mock_result_proxy.mappings.return_value.all.return_value = mock_rows

        def execute_side_effect(*args, **kwargs):
            sql = args[0].text
            if "SELECT MAX(scrape_timestamp)" in sql:
                m = MagicMock()
                m.scalar.return_value = "2024-01-01T12:00:00Z"
                return m
            elif "SELECT * FROM espn_injuries" in sql:
                return mock_result_proxy
            return MagicMock()

        self.mock_connection.execute.side_effect = execute_side_effect

        latest = db.get_latest_injuries()
        self.assertEqual(len(latest), 2)
        self.assertEqual(latest[0]["player_name"], "Player 1")

    @patch("src.scrapers.injury_database.get_engine")
    def test_get_injuries_for_date(self, mock_get_engine):
        """Verify SQL select for specific date."""
        mock_get_engine.return_value = self.mock_engine
        db = InjuryDatabase()

        target_date = datetime(2024, 1, 1)

        def execute_side_effect(*args, **kwargs):
            sql = args[0].text
            if "SELECT MAX(scrape_timestamp)" in sql:
                # Should be bounded by start/end
                self.assertIn("scrape_timestamp >=", sql)
                m = MagicMock()
                m.scalar.return_value = "2024-01-01T18:00:00Z"
                return m
            elif "SELECT * FROM espn_injuries" in sql:
                m = MagicMock()
                m.mappings.return_value.all.return_value = [{"id": 1}]
                return m
            return MagicMock()

        self.mock_connection.execute.side_effect = execute_side_effect

        results = db.get_injuries_for_date(target_date)
        self.assertEqual(len(results), 1)

if __name__ == "__main__":
    unittest.main()
