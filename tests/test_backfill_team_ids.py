"""
Tests for backfill_team_ids module

Tests cover:
- BATCH_SIZE configuration
- main() function flow with mocked database
- Early return when no rows to backfill
- Batch processing loop behavior
- SQL query structure validation
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Mock the database client before importing the module
sys.modules["src.db.client"] = Mock()
sys.modules["src.db"] = Mock()

from processing.backfill_team_ids import (
    BATCH_SIZE,
    main,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine with connect and begin context managers"""
    engine = Mock()

    # Mock for connect() context manager
    mock_conn = Mock()
    mock_connect_ctx = MagicMock()
    mock_connect_ctx.__enter__ = Mock(return_value=mock_conn)
    mock_connect_ctx.__exit__ = Mock(return_value=False)
    engine.connect.return_value = mock_connect_ctx

    # Mock for begin() context manager
    mock_begin_conn = Mock()
    mock_begin_ctx = MagicMock()
    mock_begin_ctx.__enter__ = Mock(return_value=mock_begin_conn)
    mock_begin_ctx.__exit__ = Mock(return_value=False)
    engine.begin.return_value = mock_begin_ctx

    return engine, mock_conn, mock_begin_conn


# =============================================================================
# BATCH_SIZE Configuration Tests
# =============================================================================


class TestBatchSizeConfiguration:
    """Tests for BATCH_SIZE constant"""

    def test_batch_size_is_positive_integer(self):
        """Test that BATCH_SIZE is a positive integer"""
        assert isinstance(BATCH_SIZE, int)
        assert BATCH_SIZE > 0

    def test_batch_size_is_reasonable(self):
        """Test that BATCH_SIZE is within reasonable range for DB operations"""
        # Should be at least 100 for efficiency
        assert BATCH_SIZE >= 100
        # Should not be too large to avoid memory issues
        assert BATCH_SIZE <= 100000

    def test_batch_size_default_value(self):
        """Test that BATCH_SIZE has expected default value"""
        assert BATCH_SIZE == 5000


# =============================================================================
# main() Function Flow Tests
# =============================================================================


class TestMainFunctionFlow:
    """Tests for main() function execution flow"""

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_main_counts_rows_first(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that main() counts rows to backfill first"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query to return 0 (nothing to backfill)
        mock_result = Mock()
        mock_result.fetchone.return_value = (0,)
        mock_conn.execute.return_value = mock_result

        main()

        # Verify connect was called for counting
        engine.connect.assert_called()
        # Verify SQL was executed
        mock_conn.execute.assert_called()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_main_returns_early_when_nothing_to_backfill(
        self, mock_tqdm, mock_get_engine, mock_engine
    ):
        """Test that main() returns early when no rows need backfilling"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query to return 0
        mock_result = Mock()
        mock_result.fetchone.return_value = (0,)
        mock_conn.execute.return_value = mock_result

        main()

        # tqdm should not be called if nothing to backfill
        # (or called with total=0)
        # begin() should not be called for update if nothing to backfill
        engine.begin.assert_not_called()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_main_creates_progress_bar_with_total(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that progress bar is created with correct total"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query to return 1000
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (1000,)

        # Mock update query to return 0 rows (simulate completion)
        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # Mock remaining count to return 0
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (1000, 1000, 0)

        # Setup execute to return different results based on call order
        mock_conn.execute.side_effect = [
            mock_count_result,  # Initial count
            mock_remaining_result,  # Remaining check
            mock_verify_result,  # Verification query
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        # Mock progress bar
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Verify tqdm was called with total=1000
        mock_tqdm.assert_called_once()
        call_kwargs = mock_tqdm.call_args[1]
        assert call_kwargs["total"] == 1000

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_main_uses_batch_size_in_query(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that BATCH_SIZE is passed to the SQL query"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        # Mock update query to return 0 (complete after first batch)
        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # Mock remaining count
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        # Mock progress bar
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Verify that execute was called with batch_size parameter
        update_call = mock_begin_conn.execute.call_args
        params = update_call[0][1] if len(update_call[0]) > 1 else update_call[1]
        assert params["batch_size"] == BATCH_SIZE


# =============================================================================
# Batch Processing Loop Tests
# =============================================================================


class TestBatchProcessingLoop:
    """Tests for the batch processing loop in main()"""

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_loop_continues_while_rows_updated(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that loop continues processing batches while rows are updated"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (200,)

        # Mock update queries - return rows for first two batches, then 0
        mock_update_result1 = Mock()
        mock_update_result1.fetchall.return_value = [("id1",), ("id2",)]  # 2 rows

        mock_update_result2 = Mock()
        mock_update_result2.fetchall.return_value = [("id3",)]  # 1 row

        mock_update_result3 = Mock()
        mock_update_result3.fetchall.return_value = []  # 0 rows, exit loop

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (200, 200, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.side_effect = [
            mock_update_result1,
            mock_update_result2,
            mock_update_result3,
        ]

        # Mock progress bar
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Verify begin was called 3 times (3 batches)
        assert engine.begin.call_count == 3

        # Verify progress bar was updated
        assert mock_pbar.update.call_count >= 2

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_loop_exits_when_no_rows_updated(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that loop exits when update returns 0 rows"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        # Mock update to return 0 rows immediately
        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # Mock remaining count
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (50,)  # 50 couldn't be backfilled

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 50, 50)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        # Mock progress bar
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Should only have 1 batch attempt before exiting
        assert engine.begin.call_count == 1

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_progress_bar_closes_on_completion(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that progress bar is closed when processing completes"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        # Mock update to return 0 rows
        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # Mock remaining count
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        # Mock progress bar
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Verify progress bar was closed
        mock_pbar.close.assert_called_once()


# =============================================================================
# SQL Query Structure Tests
# =============================================================================


class TestSQLQueryStructure:
    """Tests for SQL query structure validation"""

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_uses_cte(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that update query uses Common Table Expressions (WITH clause)"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Setup mocks
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification query
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Get the update query
        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        # Verify CTE structure
        assert "WITH" in query.upper()
        assert "to_update" in query.lower()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_targets_null_team_id(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that query only targets rows where team_id IS NULL"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        assert "team_id IS NULL" in query

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_joins_player_game_stats(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that query joins with player_game_stats for recent team"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        assert "player_game_stats" in query

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_joins_team_game_stats(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that query joins with team_game_stats for home/away teams"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        assert "team_game_stats" in query

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_uses_returning(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that query uses RETURNING to count updated rows"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        assert "RETURNING" in query.upper()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_update_query_checks_home_away_pattern(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that query checks for home (vs.) and away (@) team patterns"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (100, 100, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        update_call = mock_begin_conn.execute.call_args
        query = str(update_call[0][0])

        # Check for home team pattern (vs.)
        assert "vs." in query
        # Check for away team pattern (@)
        assert "@" in query


# =============================================================================
# Verification Query Tests
# =============================================================================


class TestVerificationQuery:
    """Tests for the final verification query"""

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_runs_verification_after_backfill(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that verification query runs after backfill completes"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        # Mock count query
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        # Mock update result
        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # Mock remaining count
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        # Mock verification result
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (1000, 950, 50)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Should have 3 connect calls: count, remaining check, verification
        assert engine.connect.call_count == 3


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions"""

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_handles_remaining_rows_that_cannot_be_backfilled(
        self, mock_tqdm, mock_get_engine, mock_engine
    ):
        """Test handling of rows that can't be backfilled (player's team not in game)"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (100,)

        mock_update_result = Mock()
        mock_update_result.fetchall.return_value = []

        # 50 rows remaining that couldn't be backfilled
        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (50,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (1000, 950, 50)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.return_value = mock_update_result

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        # Should not raise an error
        main()

        # Verify the function completed
        mock_pbar.close.assert_called_once()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_handles_exact_batch_size_rows(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test when total rows equals exactly one batch size"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (BATCH_SIZE,)

        # First batch updates all rows
        mock_update_result1 = Mock()
        mock_update_result1.fetchall.return_value = [("id",)] * BATCH_SIZE

        # Second batch updates 0 rows
        mock_update_result2 = Mock()
        mock_update_result2.fetchall.return_value = []

        mock_remaining_result = Mock()
        mock_remaining_result.fetchone.return_value = (0,)

        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = (BATCH_SIZE, BATCH_SIZE, 0)

        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_remaining_result,
            mock_verify_result,
        ]
        mock_begin_conn.execute.side_effect = [
            mock_update_result1,
            mock_update_result2,
        ]

        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        main()

        # Should complete successfully
        mock_pbar.close.assert_called_once()

    @patch("processing.backfill_team_ids.get_engine")
    @patch("processing.backfill_team_ids.tqdm")
    def test_requires_player_id_not_null(self, mock_tqdm, mock_get_engine, mock_engine):
        """Test that queries filter for player_id IS NOT NULL"""
        engine, mock_conn, mock_begin_conn = mock_engine
        mock_get_engine.return_value = engine

        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (0,)
        mock_conn.execute.return_value = mock_count_result

        main()

        # Get the count query
        count_call = mock_conn.execute.call_args
        query = str(count_call[0][0])

        assert "player_id IS NOT NULL" in query
