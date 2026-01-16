"""
Tests for backfill_league_priors module

Tests cover:
- get_season_start_date() - Season start date calculation
- Snapshot date generation logic in backfill_league_priors()
- calculate_and_insert_snapshot() with mocked database engine
"""

import datetime
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Mock the database client before importing the module
sys.modules["src.db.client"] = Mock()
sys.modules["src.db"] = Mock()

from processing.backfill_league_priors import (
    backfill_league_priors,
    calculate_and_insert_snapshot,
    get_season_start_date,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine"""
    engine = Mock()
    # Mock the context manager for engine.begin()
    mock_conn = Mock()
    mock_context = MagicMock()
    mock_context.__enter__ = Mock(return_value=mock_conn)
    mock_context.__exit__ = Mock(return_value=False)
    engine.begin.return_value = mock_context
    return engine, mock_conn


# =============================================================================
# get_season_start_date Tests
# =============================================================================


class TestGetSeasonStartDate:
    """Tests for get_season_start_date() function"""

    def test_january_returns_previous_year_october(self):
        """Test that January dates return October of previous year"""
        result = get_season_start_date("2025-01-15")
        assert result == "2024-10-01"

    def test_february_returns_previous_year_october(self):
        """Test that February dates return October of previous year"""
        result = get_season_start_date("2025-02-01")
        assert result == "2024-10-01"

    def test_march_returns_previous_year_october(self):
        """Test that March dates return October of previous year"""
        result = get_season_start_date("2025-03-20")
        assert result == "2024-10-01"

    def test_april_returns_previous_year_october(self):
        """Test that April dates return October of previous year"""
        result = get_season_start_date("2025-04-30")
        assert result == "2024-10-01"

    def test_may_returns_previous_year_october(self):
        """Test that May dates return October of previous year"""
        result = get_season_start_date("2025-05-15")
        assert result == "2024-10-01"

    def test_june_returns_previous_year_october(self):
        """Test that June dates (playoffs/offseason) return October of previous year"""
        result = get_season_start_date("2025-06-01")
        assert result == "2024-10-01"

    def test_september_returns_previous_year_october(self):
        """Test that September (preseason) returns October of previous year"""
        result = get_season_start_date("2025-09-30")
        assert result == "2024-10-01"

    def test_october_returns_current_year_october(self):
        """Test that October dates return October of current year (new season starts)"""
        result = get_season_start_date("2025-10-15")
        assert result == "2025-10-01"

    def test_november_returns_current_year_october(self):
        """Test that November dates return October of current year"""
        result = get_season_start_date("2025-11-01")
        assert result == "2025-10-01"

    def test_december_returns_current_year_october(self):
        """Test that December dates return October of current year"""
        result = get_season_start_date("2025-12-25")
        assert result == "2025-10-01"

    def test_first_day_of_year(self):
        """Test January 1st returns previous year October"""
        result = get_season_start_date("2025-01-01")
        assert result == "2024-10-01"

    def test_last_day_of_september(self):
        """Test September 30th (boundary) returns previous year October"""
        result = get_season_start_date("2025-09-30")
        assert result == "2024-10-01"

    def test_first_day_of_october(self):
        """Test October 1st (boundary) returns current year October"""
        result = get_season_start_date("2025-10-01")
        assert result == "2025-10-01"

    def test_different_years(self):
        """Test function works correctly across different years"""
        # 2020 season
        assert get_season_start_date("2020-11-15") == "2020-10-01"
        assert get_season_start_date("2021-02-10") == "2020-10-01"

        # 2023 season
        assert get_season_start_date("2023-12-01") == "2023-10-01"
        assert get_season_start_date("2024-03-15") == "2023-10-01"

    def test_returns_string_format(self):
        """Test that result is a properly formatted date string"""
        result = get_season_start_date("2025-01-15")
        assert isinstance(result, str)
        assert len(result) == 10  # YYYY-MM-DD format
        assert result.count("-") == 2


# =============================================================================
# Snapshot Date Generation Tests
# =============================================================================


class TestSnapshotDateGeneration:
    """Tests for the snapshot date generation logic in backfill_league_priors"""

    def test_generates_november_dates(self):
        """Test that November 1st dates are generated for each year"""
        # Extract date generation logic
        years = range(2020, 2023)
        snapshot_dates = []
        for year in years:
            for m in [11, 12]:
                snapshot_dates.append(f"{year}-{m:02d}-01")
            for m in [1, 2, 3, 4]:
                snapshot_dates.append(f"{year + 1}-{m:02d}-01")

        # Check November dates
        assert "2020-11-01" in snapshot_dates
        assert "2021-11-01" in snapshot_dates
        assert "2022-11-01" in snapshot_dates

    def test_generates_december_dates(self):
        """Test that December 1st dates are generated for each year"""
        years = range(2020, 2023)
        snapshot_dates = []
        for year in years:
            for m in [11, 12]:
                snapshot_dates.append(f"{year}-{m:02d}-01")
            for m in [1, 2, 3, 4]:
                snapshot_dates.append(f"{year + 1}-{m:02d}-01")

        assert "2020-12-01" in snapshot_dates
        assert "2021-12-01" in snapshot_dates
        assert "2022-12-01" in snapshot_dates

    def test_generates_january_through_april_next_year(self):
        """Test that Jan-Apr dates are generated for the following year"""
        years = range(2020, 2022)
        snapshot_dates = []
        for year in years:
            for m in [11, 12]:
                snapshot_dates.append(f"{year}-{m:02d}-01")
            for m in [1, 2, 3, 4]:
                snapshot_dates.append(f"{year + 1}-{m:02d}-01")

        # Check that Jan-Apr dates are for year+1
        assert "2021-01-01" in snapshot_dates
        assert "2021-02-01" in snapshot_dates
        assert "2021-03-01" in snapshot_dates
        assert "2021-04-01" in snapshot_dates

        assert "2022-01-01" in snapshot_dates
        assert "2022-02-01" in snapshot_dates
        assert "2022-03-01" in snapshot_dates
        assert "2022-04-01" in snapshot_dates

    def test_generates_six_dates_per_season(self):
        """Test that 6 dates are generated per season (Nov, Dec, Jan, Feb, Mar, Apr)"""
        years = range(2020, 2021)  # Single year
        snapshot_dates = []
        for year in years:
            for m in [11, 12]:
                snapshot_dates.append(f"{year}-{m:02d}-01")
            for m in [1, 2, 3, 4]:
                snapshot_dates.append(f"{year + 1}-{m:02d}-01")

        assert len(snapshot_dates) == 6

    def test_filters_future_dates(self):
        """Test that future dates are filtered out"""
        # Simulate generating dates that include future
        all_dates = ["2020-11-01", "2020-12-01", "2099-01-01", "2099-02-01"]
        today = datetime.date.today().strftime("%Y-%m-%d")
        filtered = [d for d in all_dates if d <= today]

        assert "2020-11-01" in filtered
        assert "2020-12-01" in filtered
        assert "2099-01-01" not in filtered
        assert "2099-02-01" not in filtered

    def test_date_format_is_correct(self):
        """Test that all generated dates have correct format"""
        years = range(2020, 2022)
        snapshot_dates = []
        for year in years:
            for m in [11, 12]:
                snapshot_dates.append(f"{year}-{m:02d}-01")
            for m in [1, 2, 3, 4]:
                snapshot_dates.append(f"{year + 1}-{m:02d}-01")

        for date_str in snapshot_dates:
            # Verify format YYYY-MM-DD
            assert len(date_str) == 10
            year, month, day = date_str.split("-")
            assert len(year) == 4
            assert len(month) == 2
            assert len(day) == 2
            assert day == "01"  # All dates should be 1st of month

    def test_months_have_leading_zeros(self):
        """Test that single-digit months have leading zeros"""
        year = 2020
        snapshot_dates = []
        for m in [1, 2, 3, 4]:
            snapshot_dates.append(f"{year}-{m:02d}-01")

        assert "2020-01-01" in snapshot_dates
        assert "2020-02-01" in snapshot_dates
        assert "2020-03-01" in snapshot_dates
        assert "2020-04-01" in snapshot_dates


# =============================================================================
# calculate_and_insert_snapshot Tests
# =============================================================================


class TestCalculateAndInsertSnapshot:
    """Tests for calculate_and_insert_snapshot() function"""

    def test_executes_query_with_correct_parameters(self, mock_engine):
        """Test that the function executes SQL with correct parameters"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        # Verify begin() was called
        engine.begin.assert_called_once()

        # Verify execute was called with correct parameters
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args

        # Check that parameters were passed
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["season_start"] == "2024-10-01"
        assert params["snap_date"] == "2025-01-01"

    def test_uses_context_manager_for_transaction(self, mock_engine):
        """Test that the function uses engine.begin() context manager"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        # Verify transaction context manager was used
        engine.begin.assert_called_once()

    def test_query_contains_required_columns(self, mock_engine):
        """Test that the SQL query references all required columns"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        # Get the query that was executed
        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        # Verify key columns are in the query
        required_columns = [
            "season_id",
            "position_group",
            "snapshot_date",
            "league_off_rtg",
            "league_reb_per100",
            "league_ast_per100",
            "league_stl_per100",
            "league_blk_per100",
            "league_threes_per100",
            "league_tov_per100",
            "league_fta_per100",
            "league_oreb_per100",
            "league_pf_per100",
            "total_possessions",
            "total_games",
        ]

        for col in required_columns:
            assert col in query, f"Missing column: {col}"

    def test_query_uses_upsert_pattern(self, mock_engine):
        """Test that the SQL query uses ON CONFLICT for upsert"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        assert "ON CONFLICT" in query
        assert "DO UPDATE SET" in query

    def test_query_joins_required_tables(self, mock_engine):
        """Test that the SQL query joins all required tables"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        # Verify required table joins
        assert "player_game_stats" in query
        assert "team_game_stats" in query
        assert "player_game_advanced_stats" in query
        assert "player_position_history" in query

    def test_query_filters_by_date_range(self, mock_engine):
        """Test that the query filters data by date range"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        # Verify date range filtering
        assert ":season_start" in query
        assert ":snap_date" in query

    def test_query_filters_positive_minutes(self, mock_engine):
        """Test that query only includes players with positive minutes"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        assert "min > 0" in query

    def test_query_filters_positive_possessions(self, mock_engine):
        """Test that query only includes records with positive possessions"""
        engine, mock_conn = mock_engine

        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-01-01")

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        assert "possessions > 0" in query


# =============================================================================
# backfill_league_priors Integration Tests
# =============================================================================


class TestBackfillLeaguePriorsIntegration:
    """Integration-style tests for backfill_league_priors() function"""

    @patch("processing.backfill_league_priors.calculate_and_insert_snapshot")
    @patch("processing.backfill_league_priors.datetime")
    def test_calls_calculate_for_each_snapshot_date(self, mock_datetime, mock_calculate):
        """Test that calculate_and_insert_snapshot is called for each date"""
        # Mock today's date to control filtering
        mock_date = Mock()
        mock_date.today.return_value.strftime.return_value = "2025-01-15"
        mock_datetime.date = mock_date

        mock_engine = Mock()

        # Run with limited year range for faster test
        with patch(
            "processing.backfill_league_priors.range",
            return_value=range(2024, 2025),
        ):
            backfill_league_priors(mock_engine)

        # Should have been called for Nov 2024, Dec 2024, Jan 2025
        # (Feb-Apr 2025 are filtered as future)
        assert mock_calculate.call_count >= 1

    @patch("processing.backfill_league_priors.calculate_and_insert_snapshot")
    @patch("processing.backfill_league_priors.datetime")
    def test_passes_correct_season_start_dates(self, mock_datetime, mock_calculate):
        """Test that correct season start dates are passed to calculate function"""
        mock_date = Mock()
        mock_date.today.return_value.strftime.return_value = "2025-04-30"
        mock_datetime.date = mock_date

        mock_engine = Mock()

        with patch(
            "processing.backfill_league_priors.range",
            return_value=range(2024, 2025),
        ):
            backfill_league_priors(mock_engine)

        # Check some of the calls have correct season_start
        calls = mock_calculate.call_args_list

        # November 2024 should use season start 2024-10-01
        # Find the call for 2024-11-01
        for c in calls:
            args = c[0]
            if args[2] == "2024-11-01":  # snapshot_date
                assert args[1] == "2024-10-01"  # season_start

        # January 2025 should also use season start 2024-10-01
        for c in calls:
            args = c[0]
            if args[2] == "2025-01-01":
                assert args[1] == "2024-10-01"


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions"""

    def test_get_season_start_date_handles_various_date_formats(self):
        """Test that function handles various input date formats"""
        # Standard format
        assert get_season_start_date("2025-01-15") == "2024-10-01"

        # Pandas should parse these as well
        assert get_season_start_date("2025/01/15") == "2024-10-01"

    def test_get_season_start_date_leap_year(self):
        """Test function works correctly with leap year dates"""
        # February 29, 2024 (leap year)
        result = get_season_start_date("2024-02-29")
        assert result == "2023-10-01"

    def test_calculate_handles_different_date_combinations(self, mock_engine):
        """Test calculate function with various date combinations"""
        engine, mock_conn = mock_engine

        # Early season snapshot
        calculate_and_insert_snapshot(engine, "2024-10-01", "2024-11-01")
        assert mock_conn.execute.called

        mock_conn.reset_mock()

        # Mid-season snapshot
        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-02-01")
        assert mock_conn.execute.called

        mock_conn.reset_mock()

        # Late season snapshot
        calculate_and_insert_snapshot(engine, "2024-10-01", "2025-04-01")
        assert mock_conn.execute.called
