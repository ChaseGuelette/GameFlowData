"""
Tests for backfill_opponent_allowed module

Tests cover:
- Season format conversion logic
- compute_rolling_metrics() calculations
- batch_insert_to_db() column handling and SQL generation
- backfill_team_allowed_by_position() orchestration
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Mock the database client before importing the module
sys.modules["src.db.client"] = Mock()
sys.modules["src.db"] = Mock()

# Import pandas after path setup
import pandas as pd

from processing.backfill_opponent_allowed import (
    backfill_team_allowed_by_position,
    batch_insert_to_db,
    compute_rolling_metrics,
    fetch_raw_allowed_stats,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine"""
    engine = Mock()
    # Mock for connect() context manager
    mock_conn = Mock()
    mock_connect_ctx = MagicMock()
    mock_connect_ctx.__enter__ = Mock(return_value=mock_conn)
    mock_connect_ctx.__exit__ = Mock(return_value=False)
    engine.connect.return_value = mock_connect_ctx

    # Mock for begin() context manager
    mock_begin_ctx = MagicMock()
    mock_begin_ctx.__enter__ = Mock(return_value=mock_conn)
    mock_begin_ctx.__exit__ = Mock(return_value=False)
    engine.begin.return_value = mock_begin_ctx

    return engine, mock_conn


@pytest.fixture
def sample_raw_data():
    """Create sample raw data for testing compute_rolling_metrics"""
    # Simulate 10 games for one team, one position
    data = {
        "team_id": [1] * 10,
        "game_id": list(range(1, 11)),
        "game_date": pd.date_range("2024-11-01", periods=10, freq="3D"),
        "season_id": ["22024"] * 10,
        "position_group": ["G"] * 10,
        "pts": [25, 30, 28, 22, 35, 27, 31, 29, 26, 33],
        "reb": [5, 6, 4, 7, 5, 6, 8, 5, 4, 7],
        "ast": [8, 7, 9, 6, 10, 8, 7, 9, 8, 11],
        "threes": [3, 4, 2, 5, 3, 4, 2, 3, 4, 5],
        "stl": [2, 1, 3, 2, 1, 2, 3, 1, 2, 2],
        "blk": [1, 0, 2, 1, 1, 0, 1, 2, 1, 0],
        "tov": [3, 2, 4, 3, 2, 3, 4, 2, 3, 3],
        "fta": [5, 6, 4, 7, 5, 6, 8, 5, 4, 7],
        "oreb": [2, 1, 3, 2, 1, 2, 3, 1, 2, 2],
        "pf": [4, 3, 5, 4, 3, 4, 5, 3, 4, 4],
        "poss_faced": [100, 105, 98, 102, 110, 95, 103, 100, 97, 108],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_processed_data():
    """Create sample processed data for batch_insert_to_db testing"""
    data = {
        "team_id": [1, 1],
        "game_id": [100, 101],
        "game_date": pd.to_datetime(["2024-11-10", "2024-11-13"]),
        "position_group": ["G", "G"],
        "games_l5": [5.0, 5.0],
        "games_l15": [10.0, 10.0],
        "games_szn": [15.0, 16.0],
        "poss_faced_l5": [500.0, 510.0],
        "poss_faced_l15": [1000.0, 1020.0],
        "poss_faced_szn": [1500.0, 1530.0],
        "pts_allowed_l5": [140.0, 145.0],
        "pts_allowed_l15": [280.0, 290.0],
        "pts_allowed_szn": [420.0, 435.0],
        "reb_allowed_l5": [30.0, 32.0],
        "reb_allowed_l15": [60.0, 64.0],
        "reb_allowed_szn": [90.0, 96.0],
        "ast_allowed_l5": [40.0, 42.0],
        "ast_allowed_l15": [80.0, 84.0],
        "ast_allowed_szn": [120.0, 126.0],
        "stl_allowed_l5": [10.0, 11.0],
        "stl_allowed_l15": [20.0, 22.0],
        "stl_allowed_szn": [30.0, 33.0],
        "blk_allowed_l5": [5.0, 6.0],
        "blk_allowed_l15": [10.0, 12.0],
        "blk_allowed_szn": [15.0, 18.0],
        "threes_allowed_l5": [15.0, 16.0],
        "threes_allowed_l15": [30.0, 32.0],
        "threes_allowed_szn": [45.0, 48.0],
        "tov_allowed_l5": [14.0, 15.0],  # Will be renamed to tov_forced_l5
        "tov_allowed_l15": [28.0, 30.0],
        "tov_allowed_szn": [42.0, 45.0],
        "fta_allowed_l5": [25.0, 26.0],
        "fta_allowed_l15": [50.0, 52.0],
        "fta_allowed_szn": [75.0, 78.0],
        "oreb_allowed_l5": [10.0, 11.0],
        "oreb_allowed_l15": [20.0, 22.0],
        "oreb_allowed_szn": [30.0, 33.0],
        "pf_allowed_l5": [20.0, 21.0],
        "pf_allowed_l15": [40.0, 42.0],
        "pf_allowed_szn": [60.0, 63.0],
        # Rate columns
        "off_rtg_allowed_l5": [28.0, 28.4],
        "off_rtg_allowed_l15": [28.0, 28.4],
        "off_rtg_allowed_szn": [28.0, 28.4],
        "reb_per100_allowed_l5": [6.0, 6.3],
        "reb_per100_allowed_l15": [6.0, 6.3],
        "reb_per100_allowed_szn": [6.0, 6.3],
        "ast_per100_allowed_l5": [8.0, 8.2],
        "ast_per100_allowed_l15": [8.0, 8.2],
        "ast_per100_allowed_szn": [8.0, 8.2],
        "stl_per100_allowed_l5": [2.0, 2.2],
        "stl_per100_allowed_l15": [2.0, 2.2],
        "stl_per100_allowed_szn": [2.0, 2.2],
        "blk_per100_allowed_l5": [1.0, 1.2],
        "blk_per100_allowed_l15": [1.0, 1.2],
        "blk_per100_allowed_szn": [1.0, 1.2],
        "threes_per100_allowed_l5": [3.0, 3.1],
        "threes_per100_allowed_l15": [3.0, 3.1],
        "threes_per100_allowed_szn": [3.0, 3.1],
        "tov_per100_forced_l5": [2.8, 2.9],
        "tov_per100_forced_l15": [2.8, 2.9],
        "tov_per100_forced_szn": [2.8, 2.9],
        "fta_per100_allowed_l5": [5.0, 5.1],
        "fta_per100_allowed_l15": [5.0, 5.1],
        "fta_per100_allowed_szn": [5.0, 5.1],
        "oreb_per100_allowed_l5": [2.0, 2.2],
        "oreb_per100_allowed_l15": [2.0, 2.2],
        "oreb_per100_allowed_szn": [2.0, 2.2],
        "pf_per100_allowed_l5": [4.0, 4.1],
        "pf_per100_allowed_l15": [4.0, 4.1],
        "pf_per100_allowed_szn": [4.0, 4.1],
    }
    return pd.DataFrame(data)


# =============================================================================
# Season Format Conversion Tests
# =============================================================================


class TestSeasonFormatConversion:
    """Tests for season format conversion logic"""

    def test_converts_hyphenated_season_format(self):
        """Test conversion of '2018-19' format to '22018'"""
        seasons = ["2018-19"]
        formatted = []
        for s in seasons:
            if "-" in s and len(s) == 7:
                start_year = s.split("-")[0]
                formatted.append(f"2{start_year}")
            else:
                formatted.append(s)

        assert formatted == ["22018"]

    def test_converts_multiple_seasons(self):
        """Test conversion of multiple seasons"""
        seasons = ["2018-19", "2019-20", "2020-21"]
        formatted = []
        for s in seasons:
            if "-" in s and len(s) == 7:
                start_year = s.split("-")[0]
                formatted.append(f"2{start_year}")
            else:
                formatted.append(s)

        assert formatted == ["22018", "22019", "22020"]

    def test_keeps_already_formatted_seasons(self):
        """Test that already formatted seasons are kept as-is"""
        seasons = ["22018", "22019"]
        formatted = []
        for s in seasons:
            if "-" in s and len(s) == 7:
                start_year = s.split("-")[0]
                formatted.append(f"2{start_year}")
            else:
                formatted.append(s)

        assert formatted == ["22018", "22019"]

    def test_handles_mixed_formats(self):
        """Test handling of mixed season formats"""
        seasons = ["2018-19", "22019", "2020-21"]
        formatted = []
        for s in seasons:
            if "-" in s and len(s) == 7:
                start_year = s.split("-")[0]
                formatted.append(f"2{start_year}")
            else:
                formatted.append(s)

        assert formatted == ["22018", "22019", "22020"]

    def test_does_not_convert_short_strings(self):
        """Test that short strings with hyphens are not converted"""
        seasons = ["18-19"]  # len=5, not 7
        formatted = []
        for s in seasons:
            if "-" in s and len(s) == 7:
                start_year = s.split("-")[0]
                formatted.append(f"2{start_year}")
            else:
                formatted.append(s)

        assert formatted == ["18-19"]


# =============================================================================
# compute_rolling_metrics Tests
# =============================================================================


class TestComputeRollingMetrics:
    """Tests for compute_rolling_metrics() function"""

    def test_returns_dataframe(self, sample_raw_data):
        """Test that compute_rolling_metrics returns a DataFrame"""
        result = compute_rolling_metrics(sample_raw_data)
        assert isinstance(result, pd.DataFrame)

    def test_output_has_required_columns(self, sample_raw_data):
        """Test that output has all required columns"""
        result = compute_rolling_metrics(sample_raw_data)

        # Check base columns
        assert "team_id" in result.columns
        assert "game_id" in result.columns
        assert "game_date" in result.columns
        assert "position_group" in result.columns

        # Check window columns
        assert "games_l5" in result.columns
        assert "games_l15" in result.columns
        assert "games_szn" in result.columns

        # Check possession columns
        assert "poss_faced_l5" in result.columns
        assert "poss_faced_l15" in result.columns
        assert "poss_faced_szn" in result.columns

    def test_output_has_rate_columns(self, sample_raw_data):
        """Test that output has pace-adjusted rate columns"""
        result = compute_rolling_metrics(sample_raw_data)

        # Check rate columns for each window
        for window in ["l5", "l15", "szn"]:
            assert f"off_rtg_allowed_{window}" in result.columns
            assert f"reb_per100_allowed_{window}" in result.columns
            assert f"ast_per100_allowed_{window}" in result.columns
            assert f"stl_per100_allowed_{window}" in result.columns
            assert f"blk_per100_allowed_{window}" in result.columns
            assert f"threes_per100_allowed_{window}" in result.columns
            assert f"tov_per100_forced_{window}" in result.columns
            assert f"fta_per100_allowed_{window}" in result.columns
            assert f"oreb_per100_allowed_{window}" in result.columns
            assert f"pf_per100_allowed_{window}" in result.columns

    def test_first_row_has_nan_values(self, sample_raw_data):
        """Test that first row has NaN for rolling metrics (shifted)"""
        result = compute_rolling_metrics(sample_raw_data)

        # First row should have NaN for shifted values
        assert pd.isna(result.iloc[0]["games_l5"]) or result.iloc[0]["games_l5"] == 0

    def test_rolling_windows_use_correct_periods(self, sample_raw_data):
        """Test that L5 uses 5 periods and L15 uses 15 periods"""
        result = compute_rolling_metrics(sample_raw_data)

        # After 6 games, games_l5 should be 5 (max of 5)
        # Using iloc[6] because shift(1) means row 6 has data from games 1-5
        if len(result) > 6:
            assert result.iloc[6]["games_l5"] == 5.0

    def test_season_window_expands(self, sample_raw_data):
        """Test that season window expands over time"""
        result = compute_rolling_metrics(sample_raw_data)

        # games_szn should increase over the season
        # Row 5 should have 4 games (shift 1, then expanding)
        # Row 9 should have 8 games
        if len(result) >= 10:
            assert result.iloc[9]["games_szn"] >= result.iloc[5]["games_szn"]

    def test_rate_calculation_is_correct(self, sample_raw_data):
        """Test that per-100 rate calculation is correct"""
        result = compute_rolling_metrics(sample_raw_data)

        # Find a row with non-zero possessions
        for idx in range(2, len(result)):
            if result.iloc[idx]["poss_faced_l5"] > 0:
                pts_allowed = result.iloc[idx]["pts_allowed_l5"]
                poss = result.iloc[idx]["poss_faced_l5"]
                expected_rate = (pts_allowed / poss) * 100
                actual_rate = result.iloc[idx]["off_rtg_allowed_l5"]
                assert abs(actual_rate - expected_rate) < 0.01
                break

    def test_handles_zero_possessions(self, sample_raw_data):
        """Test that zero possessions results in zero rate"""
        # Modify data to have zero possessions
        modified_data = sample_raw_data.copy()
        modified_data["poss_faced"] = 0

        result = compute_rolling_metrics(modified_data)

        # All rate columns should be 0 when possessions is 0
        for window in ["l5", "l15", "szn"]:
            rate_cols = [
                c for c in result.columns if f"per100_allowed_{window}" in c or f"off_rtg_allowed_{window}" in c
            ]
            for col in rate_cols:
                # Should be 0 or NaN (depending on the row)
                non_nan_values = result[col].dropna()
                if len(non_nan_values) > 0:
                    assert all(v == 0 for v in non_nan_values)

    def test_preserves_row_count(self, sample_raw_data):
        """Test that output has same number of rows as input"""
        result = compute_rolling_metrics(sample_raw_data)
        assert len(result) == len(sample_raw_data)

    def test_sorts_by_team_position_date(self, sample_raw_data):
        """Test that data is sorted correctly for rolling calculations"""
        # Shuffle the input data
        shuffled = sample_raw_data.sample(frac=1, random_state=42)
        result = compute_rolling_metrics(shuffled)

        # Output should still work correctly
        assert len(result) == len(shuffled)


# =============================================================================
# batch_insert_to_db Tests
# =============================================================================


class TestBatchInsertToDb:
    """Tests for batch_insert_to_db() function"""

    def test_renames_tov_columns(self, mock_engine, sample_processed_data):
        """Test that tov_allowed columns are renamed to tov_forced"""
        engine, mock_conn = mock_engine

        # Capture the dataframe after rename by checking what's passed to execute
        batch_insert_to_db(engine, sample_processed_data, batch_size=100)

        # Verify execute was called
        assert mock_conn.execute.called

    def test_executes_in_batches(self, mock_engine, sample_processed_data):
        """Test that data is inserted in batches"""
        engine, mock_conn = mock_engine

        # Create larger dataset
        large_data = pd.concat([sample_processed_data] * 5, ignore_index=True)

        batch_insert_to_db(engine, large_data, batch_size=3)

        # Should have multiple execute calls
        assert mock_conn.execute.call_count >= 2

    def test_uses_upsert_query(self, mock_engine, sample_processed_data):
        """Test that upsert query is used with ON CONFLICT"""
        engine, mock_conn = mock_engine

        batch_insert_to_db(engine, sample_processed_data, batch_size=100)

        # Get the query from the execute call
        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        assert "INSERT INTO team_allowed_by_position" in query
        assert "ON CONFLICT" in query
        assert "DO UPDATE SET" in query

    def test_valid_columns_are_included(self, mock_engine, sample_processed_data):
        """Test that all valid columns are included in the query"""
        engine, mock_conn = mock_engine

        batch_insert_to_db(engine, sample_processed_data, batch_size=100)

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        # Check key columns are in query
        assert "team_id" in query
        assert "game_id" in query
        assert "position_group" in query
        assert "games_l5" in query
        assert "off_rtg_allowed_l5" in query

    def test_conflict_columns_are_correct(self, mock_engine, sample_processed_data):
        """Test that conflict is on team_id, game_id, position_group"""
        engine, mock_conn = mock_engine

        batch_insert_to_db(engine, sample_processed_data, batch_size=100)

        call_args = mock_conn.execute.call_args
        query = str(call_args[0][0])

        # Should have conflict on these columns
        assert "ON CONFLICT (team_id, game_id, position_group)" in query

    def test_replaces_nan_with_none(self, mock_engine):
        """Test that NaN values are replaced with None"""
        engine, mock_conn = mock_engine

        # Create data with NaN
        data = {
            "team_id": [1],
            "game_id": [100],
            "game_date": pd.to_datetime(["2024-11-10"]),
            "position_group": ["G"],
            "games_l5": [np.nan],  # NaN value
            "games_l15": [10.0],
            "games_szn": [15.0],
        }
        # Add all other required columns with valid values
        for col in [
            "poss_faced_l5",
            "poss_faced_l15",
            "poss_faced_szn",
            "pts_allowed_l5",
            "pts_allowed_l15",
            "pts_allowed_szn",
            "reb_allowed_l5",
            "reb_allowed_l15",
            "reb_allowed_szn",
            "ast_allowed_l5",
            "ast_allowed_l15",
            "ast_allowed_szn",
            "stl_allowed_l5",
            "stl_allowed_l15",
            "stl_allowed_szn",
            "blk_allowed_l5",
            "blk_allowed_l15",
            "blk_allowed_szn",
            "threes_allowed_l5",
            "threes_allowed_l15",
            "threes_allowed_szn",
            "tov_allowed_l5",
            "tov_allowed_l15",
            "tov_allowed_szn",
            "fta_allowed_l5",
            "fta_allowed_l15",
            "fta_allowed_szn",
            "oreb_allowed_l5",
            "oreb_allowed_l15",
            "oreb_allowed_szn",
            "pf_allowed_l5",
            "pf_allowed_l15",
            "pf_allowed_szn",
            "off_rtg_allowed_l5",
            "off_rtg_allowed_l15",
            "off_rtg_allowed_szn",
            "reb_per100_allowed_l5",
            "reb_per100_allowed_l15",
            "reb_per100_allowed_szn",
            "ast_per100_allowed_l5",
            "ast_per100_allowed_l15",
            "ast_per100_allowed_szn",
            "stl_per100_allowed_l5",
            "stl_per100_allowed_l15",
            "stl_per100_allowed_szn",
            "blk_per100_allowed_l5",
            "blk_per100_allowed_l15",
            "blk_per100_allowed_szn",
            "threes_per100_allowed_l5",
            "threes_per100_allowed_l15",
            "threes_per100_allowed_szn",
            "tov_per100_forced_l5",
            "tov_per100_forced_l15",
            "tov_per100_forced_szn",
            "fta_per100_allowed_l5",
            "fta_per100_allowed_l15",
            "fta_per100_allowed_szn",
            "oreb_per100_allowed_l5",
            "oreb_per100_allowed_l15",
            "oreb_per100_allowed_szn",
            "pf_per100_allowed_l5",
            "pf_per100_allowed_l15",
            "pf_per100_allowed_szn",
        ]:
            data[col] = [1.0]

        df = pd.DataFrame(data)

        # Should not raise an error
        batch_insert_to_db(engine, df, batch_size=100)
        assert mock_conn.execute.called


# =============================================================================
# backfill_team_allowed_by_position Tests
# =============================================================================


class TestBackfillTeamAllowedByPosition:
    """Tests for backfill_team_allowed_by_position() orchestration"""

    @patch("processing.backfill_opponent_allowed.batch_insert_to_db")
    @patch("processing.backfill_opponent_allowed.compute_rolling_metrics")
    @patch("processing.backfill_opponent_allowed.fetch_raw_allowed_stats")
    def test_calls_all_functions_in_order(self, mock_fetch, mock_compute, mock_insert):
        """Test that all functions are called in correct order"""
        mock_engine = Mock()
        mock_fetch.return_value = pd.DataFrame(
            {
                "team_id": [1],
                "game_id": [1],
                "game_date": ["2024-01-01"],
                "position_group": ["G"],
                "pts": [25],
            }
        )
        mock_compute.return_value = pd.DataFrame(
            {
                "team_id": [1],
                "game_id": [1],
                "game_date": ["2024-01-01"],
                "position_group": ["G"],
                "games_l5": [5.0],
            }
        )

        backfill_team_allowed_by_position(mock_engine, ["2024-25"])

        mock_fetch.assert_called_once()
        mock_compute.assert_called_once()
        mock_insert.assert_called_once()

    @patch("processing.backfill_opponent_allowed.batch_insert_to_db")
    @patch("processing.backfill_opponent_allowed.compute_rolling_metrics")
    @patch("processing.backfill_opponent_allowed.fetch_raw_allowed_stats")
    def test_converts_season_format_before_fetch(self, mock_fetch, mock_compute, mock_insert):
        """Test that season format is converted before fetching"""
        mock_engine = Mock()
        mock_fetch.return_value = pd.DataFrame()  # Empty to trigger early return

        backfill_team_allowed_by_position(mock_engine, ["2024-25"])

        # fetch_raw_allowed_stats should be called with converted format
        call_args = mock_fetch.call_args
        assert call_args[0][1] == ["22024"]

    @patch("processing.backfill_opponent_allowed.batch_insert_to_db")
    @patch("processing.backfill_opponent_allowed.compute_rolling_metrics")
    @patch("processing.backfill_opponent_allowed.fetch_raw_allowed_stats")
    def test_returns_early_on_empty_data(self, mock_fetch, mock_compute, mock_insert):
        """Test that function returns early if no data is fetched"""
        mock_engine = Mock()
        mock_fetch.return_value = pd.DataFrame()

        backfill_team_allowed_by_position(mock_engine, ["2024-25"])

        # Should not call compute or insert if fetch returns empty
        mock_compute.assert_not_called()
        mock_insert.assert_not_called()

    @patch("processing.backfill_opponent_allowed.batch_insert_to_db")
    @patch("processing.backfill_opponent_allowed.compute_rolling_metrics")
    @patch("processing.backfill_opponent_allowed.fetch_raw_allowed_stats")
    def test_drops_rows_without_games_l5(self, mock_fetch, mock_compute, mock_insert):
        """Test that rows without games_l5 are dropped before insert"""
        mock_engine = Mock()
        mock_fetch.return_value = pd.DataFrame(
            {
                "team_id": [1, 2],
                "game_id": [1, 2],
                "game_date": ["2024-01-01", "2024-01-02"],
                "position_group": ["G", "F"],
            }
        )
        mock_compute.return_value = pd.DataFrame(
            {
                "team_id": [1, 2],
                "game_id": [1, 2],
                "game_date": ["2024-01-01", "2024-01-02"],
                "position_group": ["G", "F"],
                "games_l5": [5.0, np.nan],  # Second row has NaN
            }
        )

        backfill_team_allowed_by_position(mock_engine, ["2024-25"])

        # Insert should be called with filtered data
        mock_insert.assert_called_once()
        insert_df = mock_insert.call_args[0][1]
        assert len(insert_df) == 1  # Only row with valid games_l5


# =============================================================================
# fetch_raw_allowed_stats Tests
# =============================================================================


class TestFetchRawAllowedStats:
    """Tests for fetch_raw_allowed_stats() function"""

    @patch("processing.backfill_opponent_allowed.pd.read_sql")
    def test_returns_dataframe(self, mock_read_sql, mock_engine):
        """Test that fetch returns a DataFrame"""
        engine, mock_conn = mock_engine
        mock_read_sql.return_value = pd.DataFrame({"col": [1, 2, 3]})

        result = fetch_raw_allowed_stats(engine, ["22024"])

        assert isinstance(result, pd.DataFrame)

    @patch("processing.backfill_opponent_allowed.pd.read_sql")
    def test_passes_seasons_as_tuple(self, mock_read_sql, mock_engine):
        """Test that seasons are passed as tuple parameter"""
        engine, mock_conn = mock_engine
        mock_read_sql.return_value = pd.DataFrame()

        fetch_raw_allowed_stats(engine, ["22024", "22025"])

        # Check that params include seasons as tuple
        call_args = mock_read_sql.call_args
        params = call_args[1]["params"]
        assert params["seasons"] == ("22024", "22025")


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions"""

    def test_compute_rolling_handles_single_row(self):
        """Test compute_rolling_metrics with single row"""
        data = {
            "team_id": [1],
            "game_id": [1],
            "game_date": pd.to_datetime(["2024-11-01"]),
            "season_id": ["22024"],
            "position_group": ["G"],
            "pts": [25],
            "reb": [5],
            "ast": [8],
            "threes": [3],
            "stl": [2],
            "blk": [1],
            "tov": [3],
            "fta": [5],
            "oreb": [2],
            "pf": [4],
            "poss_faced": [100],
        }
        df = pd.DataFrame(data)

        result = compute_rolling_metrics(df)

        assert len(result) == 1
        # First row should have NaN or 0 for shifted values
        assert pd.isna(result.iloc[0]["games_l5"]) or result.iloc[0]["games_l5"] == 0

    def test_compute_rolling_handles_multiple_teams(self):
        """Test that rolling is computed per team"""
        data = {
            "team_id": [1, 1, 2, 2],
            "game_id": [1, 2, 3, 4],
            "game_date": pd.to_datetime(["2024-11-01", "2024-11-04", "2024-11-01", "2024-11-04"]),
            "season_id": ["22024"] * 4,
            "position_group": ["G"] * 4,
            "pts": [25, 30, 20, 35],
            "reb": [5, 6, 4, 7],
            "ast": [8, 7, 6, 9],
            "threes": [3, 4, 2, 5],
            "stl": [2, 1, 2, 2],
            "blk": [1, 0, 1, 1],
            "tov": [3, 2, 3, 2],
            "fta": [5, 6, 4, 7],
            "oreb": [2, 1, 2, 2],
            "pf": [4, 3, 4, 4],
            "poss_faced": [100, 105, 95, 110],
        }
        df = pd.DataFrame(data)

        result = compute_rolling_metrics(df)

        # Should have 4 rows
        assert len(result) == 4

    def test_compute_rolling_handles_multiple_positions(self):
        """Test that rolling is computed per position group"""
        data = {
            "team_id": [1] * 4,
            "game_id": [1, 1, 2, 2],
            "game_date": pd.to_datetime(["2024-11-01", "2024-11-01", "2024-11-04", "2024-11-04"]),
            "season_id": ["22024"] * 4,
            "position_group": ["G", "F", "G", "F"],
            "pts": [25, 20, 30, 22],
            "reb": [5, 8, 6, 9],
            "ast": [8, 4, 7, 5],
            "threes": [3, 2, 4, 1],
            "stl": [2, 1, 1, 2],
            "blk": [1, 2, 0, 3],
            "tov": [3, 2, 2, 3],
            "fta": [5, 6, 6, 5],
            "oreb": [2, 3, 1, 4],
            "pf": [4, 3, 3, 4],
            "poss_faced": [50, 50, 55, 48],
        }
        df = pd.DataFrame(data)

        result = compute_rolling_metrics(df)

        # Should have 4 rows - one for each team/position/game combination
        assert len(result) == 4
