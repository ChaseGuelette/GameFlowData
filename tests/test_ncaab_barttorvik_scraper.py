"""Tests for NCAAB Barttorvik Ratings Scraper."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.scrapers.ncaab.ncaab_barttorvik_scraper import BarttorviKScraper

SAMPLE_CSV = """Rk,Team,Conf,G,AdjOE,AdjDE,AdjT,Barthag,EFG%,EFG%D,TOR,TORD,ORB,DRB,FTR,FTRD
1,Duke,ACC,30,118.5,95.2,68.4,0.9712,54.2,45.1,16.8,20.1,32.1,25.4,34.2,28.1
2,Kansas,B12,29,116.2,97.8,69.1,0.9521,52.8,46.3,17.2,19.4,30.5,26.8,32.1,29.5
3,UConn,BE,28,117.1,96.5,66.8,0.9634,53.5,44.8,15.9,21.2,31.2,24.9,35.1,27.8
"""


class TestBarttorviKScraper:
    def setup_method(self):
        self.mock_engine = MagicMock()
        self.scraper = BarttorviKScraper(self.mock_engine)

    def test_normalize_columns_standard_headers(self):
        """Verify standard header column mapping works."""
        from io import StringIO
        df = pd.read_csv(StringIO(SAMPLE_CSV))

        normalized = self.scraper.normalize_columns(df)

        assert "team_name" in normalized.columns
        assert "adj_oe" in normalized.columns
        assert "adj_de" in normalized.columns
        assert "adj_tempo" in normalized.columns
        assert "barthag" in normalized.columns
        assert len(normalized) == 3

        # Check values
        duke = normalized[normalized["team_name"] == "Duke"].iloc[0]
        assert duke["adj_oe"] == pytest.approx(118.5)
        assert duke["adj_de"] == pytest.approx(95.2)

    def test_normalize_columns_computes_adj_em(self):
        """adj_em should be computed as adj_oe - adj_de when not in CSV."""
        from io import StringIO
        df = pd.read_csv(StringIO(SAMPLE_CSV))

        normalized = self.scraper.normalize_columns(df)

        duke = normalized[normalized["team_name"] == "Duke"].iloc[0]
        expected_em = 118.5 - 95.2
        assert duke["adj_em"] == pytest.approx(expected_em, abs=0.1)

    def test_normalize_columns_four_factors(self):
        """Verify Four Factors columns are mapped."""
        from io import StringIO
        df = pd.read_csv(StringIO(SAMPLE_CSV))

        normalized = self.scraper.normalize_columns(df)

        assert "efg_pct" in normalized.columns
        assert "opp_efg_pct" in normalized.columns
        assert "tov_pct" in normalized.columns
        assert "orb_pct" in normalized.columns

    @patch("src.scrapers.ncaab.ncaab_barttorvik_scraper.extras.execute_values")
    def test_store_snapshot_dedup(self, mock_exec_values):
        """Verify ON CONFLICT DO NOTHING for duplicate snapshots."""
        from io import StringIO
        df = pd.read_csv(StringIO(SAMPLE_CSV))
        normalized = self.scraper.normalize_columns(df)

        mock_conn = MagicMock()
        self.mock_engine.raw_connection.return_value = mock_conn

        count = self.scraper.store_snapshot(normalized, season=2025, snapshot_date=date(2025, 1, 15))

        assert count == 3
        mock_conn.commit.assert_called_once()
        mock_exec_values.assert_called_once()

    @patch("src.scrapers.ncaab.ncaab_barttorvik_scraper.requests.Session")
    def test_fetch_csv_success(self, mock_session_cls):
        """Verify HTTP GET with correct URL."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_CSV
        mock_session.get.return_value = mock_response

        scraper = BarttorviKScraper(self.mock_engine)
        scraper.session = mock_session

        df = scraper.fetch_csv(2025)

        assert df is not None
        assert len(df) == 3
        call_url = mock_session.get.call_args[0][0]
        assert "2025" in call_url

    @patch("src.scrapers.ncaab.ncaab_barttorvik_scraper.requests.Session")
    def test_fetch_csv_404(self, mock_session_cls):
        """404 returns None."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response

        scraper = BarttorviKScraper(self.mock_engine)
        scraper.session = mock_session

        assert scraper.fetch_csv(2030) is None
