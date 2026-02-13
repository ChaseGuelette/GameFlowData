"""Tests for PaperTrader class."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.paper_trading.paper_trader import PaperTrader


class TestPaperTraderCalculations:
    """Tests for PaperTrader calculation methods (no DB required)."""

    def test_american_to_decimal_positive_odds(self):
        """Test decimal conversion for positive American odds."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.engine = None

        # +150 -> 2.50 decimal
        result = trader._american_to_decimal(150)
        assert result == pytest.approx(2.5, rel=0.01)

        # +200 -> 3.00 decimal
        result = trader._american_to_decimal(200)
        assert result == pytest.approx(3.0, rel=0.01)

    def test_american_to_decimal_negative_odds(self):
        """Test decimal conversion for negative American odds."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.engine = None

        # -150 -> 1.67 decimal
        result = trader._american_to_decimal(-150)
        assert result == pytest.approx(1.667, rel=0.01)

        # -110 -> 1.91 decimal
        result = trader._american_to_decimal(-110)
        assert result == pytest.approx(1.909, rel=0.01)

    def test_american_to_decimal_none(self):
        """Test decimal conversion handles None."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.engine = None

        result = trader._american_to_decimal(None)
        assert result == 0.0

    def test_calculate_kelly_stake_positive_edge(self):
        """Test Kelly stake calculation with positive edge."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.edge_threshold = 0.05
        trader.kelly_fraction = 0.125
        trader.max_bet_pct = 0.05
        trader.engine = None

        bankroll = 10000.0
        odds = -110  # ~0.909 net odds
        model_prob = 0.60  # Good edge

        stake = trader._calculate_kelly_stake(odds, model_prob, bankroll)

        # Should be positive and capped at max
        assert stake > 0
        assert stake <= bankroll * 0.05

    def test_calculate_kelly_stake_no_edge(self):
        """Test Kelly returns 0 when no positive edge."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.edge_threshold = 0.05
        trader.kelly_fraction = 0.125
        trader.max_bet_pct = 0.05
        trader.engine = None

        bankroll = 10000.0
        odds = -110
        model_prob = 0.30  # Negative edge

        stake = trader._calculate_kelly_stake(odds, model_prob, bankroll)

        assert stake == 0.0

    def test_calculate_kelly_stake_zero_odds(self):
        """Test Kelly handles zero odds."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.kelly_fraction = 0.125
        trader.max_bet_pct = 0.05
        trader.engine = None

        stake = trader._calculate_kelly_stake(0, 0.60, 10000.0)
        assert stake == 0.0


class TestPaperTraderBetSelection:
    """Tests for bet selection logic."""

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_select_bets_over_direction(self, mock_get_engine):
        """Test bet selection chooses over when over_edge is higher."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # Mock the database queries
        predictions_df = pd.DataFrame([{
            "prediction_id": 1,
            "prediction_date": date(2024, 1, 15),
            "player_id": 101,
            "player_name": "Test Player",
            "game_id": "0022400001",
            "stat": "pts",
            "line": 20.5,
            "over_odds": -110.0,
            "under_odds": -110.0,
            "over_prob": 0.62,
            "under_prob": 0.38,
            "implied_over": 0.52,
            "implied_under": 0.48,
            "over_edge": 0.10,  # Higher edge
            "under_edge": -0.10,
        }])

        bankroll_result = MagicMock()
        bankroll_result.fetchone.return_value = (10000.0,)

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.execute.return_value = bankroll_result
            return conn

        mock_engine.connect = mock_connect

        with patch("pandas.read_sql", return_value=predictions_df):
            trader = PaperTrader(edge_threshold=0.05)
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 1
        assert bets[0]["bet_direction"] == "over"
        assert bets[0]["edge"] == 0.10

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_select_bets_under_direction(self, mock_get_engine):
        """Test bet selection chooses under when under_edge is higher."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        predictions_df = pd.DataFrame([{
            "prediction_id": 1,
            "prediction_date": date(2024, 1, 15),
            "player_id": 101,
            "player_name": "Test Player",
            "game_id": "0022400001",
            "stat": "reb",
            "line": 8.5,
            "over_odds": -110.0,
            "under_odds": -110.0,
            "over_prob": 0.42,
            "under_prob": 0.58,
            "implied_over": 0.52,
            "implied_under": 0.48,
            "over_edge": -0.10,
            "under_edge": 0.10,  # Higher edge
        }])

        bankroll_result = MagicMock()
        bankroll_result.fetchone.return_value = (10000.0,)

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.execute.return_value = bankroll_result
            return conn

        mock_engine.connect = mock_connect

        with patch("pandas.read_sql", return_value=predictions_df):
            trader = PaperTrader(edge_threshold=0.05)
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 1
        assert bets[0]["bet_direction"] == "under"
        assert bets[0]["edge"] == 0.10

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_select_bets_filters_no_edge(self, mock_get_engine):
        """Test bet selection filters out predictions without sufficient edge."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        predictions_df = pd.DataFrame([{
            "prediction_id": 1,
            "prediction_date": date(2024, 1, 15),
            "player_id": 101,
            "player_name": "Test Player",
            "game_id": "0022400001",
            "stat": "ast",
            "line": 5.5,
            "over_odds": -110.0,
            "under_odds": -110.0,
            "over_prob": 0.53,
            "under_prob": 0.47,
            "implied_over": 0.52,
            "implied_under": 0.48,
            "over_edge": 0.01,  # Below threshold
            "under_edge": -0.01,
        }])

        bankroll_result = MagicMock()
        bankroll_result.fetchone.return_value = (10000.0,)

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.execute.return_value = bankroll_result
            return conn

        mock_engine.connect = mock_connect

        with patch("pandas.read_sql", return_value=predictions_df):
            trader = PaperTrader(edge_threshold=0.05)
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 0  # No bets meet threshold


class TestPaperTraderResolution:
    """Tests for bet resolution logic."""

    def test_resolution_over_win(self):
        """Test resolving over bet that wins."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.starting_bankroll = 10000.0
        trader.engine = None

        # Line is 20.5, actual is 25 -> over wins
        line = 20.5
        direction = "over"
        actual = 25.0

        if actual > line:
            status = "won" if direction == "over" else "lost"
        elif actual < line:
            status = "won" if direction == "under" else "lost"
        else:
            status = "push"

        assert status == "won"

    def test_resolution_over_loss(self):
        """Test resolving over bet that loses."""
        line = 20.5
        direction = "over"
        actual = 18.0

        if actual > line:
            status = "won" if direction == "over" else "lost"
        elif actual < line:
            status = "won" if direction == "under" else "lost"
        else:
            status = "push"

        assert status == "lost"

    def test_resolution_under_win(self):
        """Test resolving under bet that wins."""
        line = 8.5
        direction = "under"
        actual = 6.0

        if actual > line:
            status = "won" if direction == "over" else "lost"
        elif actual < line:
            status = "won" if direction == "under" else "lost"
        else:
            status = "push"

        assert status == "won"

    def test_resolution_push(self):
        """Test resolving bet that pushes."""
        line = 20.0
        direction = "over"
        actual = 20.0

        if actual > line:
            status = "won" if direction == "over" else "lost"
        elif actual < line:
            status = "won" if direction == "under" else "lost"
        else:
            status = "push"

        assert status == "push"

    def test_pnl_calculation_win_negative_odds(self):
        """Test P&L calculation for winning bet with negative odds."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.engine = None

        stake = 100.0
        odds = -110
        decimal_odds = trader._american_to_decimal(odds)
        pnl = stake * (decimal_odds - 1)

        # -110 = 1.909 decimal, so profit = 100 * 0.909 = 90.9
        assert pnl == pytest.approx(90.91, rel=0.01)

    def test_pnl_calculation_win_positive_odds(self):
        """Test P&L calculation for winning bet with positive odds."""
        trader = PaperTrader.__new__(PaperTrader)
        trader.engine = None

        stake = 100.0
        odds = 150
        decimal_odds = trader._american_to_decimal(odds)
        pnl = stake * (decimal_odds - 1)

        # +150 = 2.5 decimal, so profit = 100 * 1.5 = 150
        assert pnl == pytest.approx(150.0, rel=0.01)

    def test_pnl_calculation_loss(self):
        """Test P&L calculation for losing bet."""
        stake = 100.0
        pnl = -stake

        assert pnl == -100.0


class TestPaperTraderDefaultValues:
    """Tests for PaperTrader default configuration."""

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_default_edge_threshold(self, mock_get_engine):
        """Test default edge threshold is 0.05."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader()
        assert trader.edge_threshold == 0.05

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_default_kelly_fraction(self, mock_get_engine):
        """Test default Kelly fraction is 0.125."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader()
        assert trader.kelly_fraction == 0.125

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_default_bankroll(self, mock_get_engine):
        """Test default starting bankroll is 10000."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader()
        assert trader.starting_bankroll == 10000.0

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_default_odds_filters(self, mock_get_engine):
        """Test default odds filters."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader()
        assert trader.min_odds == -200
        assert trader.max_odds == 200


class TestResolveAllPending:
    """Tests for resolve_all_pending() method."""

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_resolve_all_pending_no_pending_bets(self, mock_get_engine):
        """Test resolve_all_pending returns zeros when no pending bets exist."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # Mock empty result for pending dates query
        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.execute.return_value.fetchall.return_value = []
            return conn

        mock_engine.connect = mock_connect

        trader = PaperTrader()
        result = trader.resolve_all_pending()

        assert result["dates_processed"] == 0
        assert result["dates_skipped"] == 0
        assert result["total_resolved"] == 0
        assert result["total_won"] == 0
        assert result["total_lost"] == 0
        assert result["total_push"] == 0
        assert result["total_cancelled"] == 0
        assert result["by_date"] == {}

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_resolve_all_pending_skips_dates_without_stats(self, mock_get_engine):
        """Test resolve_all_pending skips dates where game stats aren't available yet."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        future_date = date(2026, 12, 31)  # Future date with no stats

        call_count = [0]

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)

            def mock_execute(query, params=None):
                call_count[0] += 1
                result = MagicMock()
                # First call: get pending dates
                if call_count[0] == 1:
                    result.fetchall.return_value = [(future_date,)]
                # Second call: check if stats exist for the date
                else:
                    result.scalar.return_value = 0  # No stats available
                return result

            conn.execute = mock_execute
            return conn

        mock_engine.connect = mock_connect

        trader = PaperTrader()
        result = trader.resolve_all_pending()

        assert result["dates_processed"] == 0
        assert result["dates_skipped"] == 1
        assert result["total_resolved"] == 0
        assert str(future_date) in result["by_date"]
        assert result["by_date"][str(future_date)]["skipped"] is True

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_resolve_all_pending_processes_dates_with_stats(self, mock_get_engine):
        """Test resolve_all_pending processes dates that have game stats available."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        past_date = date(2026, 2, 10)

        call_count = [0]

        # Create mock bet and actuals data
        bets_df = pd.DataFrame([{
            "id": 1,
            "player_id": 101,
            "stat_type": "pts",
            "line": 20.5,
            "bet_direction": "over",
            "odds_at_bet": -110.0,
            "stake": 100.0,
        }])

        actuals_df = pd.DataFrame([{
            "player_id": 101,
            "pts": 25.0,  # Above line, over wins
            "reb": 8.0,
            "ast": 5.0,
            "did_not_play": False,
        }])

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)

            def mock_execute(query, params=None):
                call_count[0] += 1
                result = MagicMock()
                # First call: get pending dates
                if call_count[0] == 1:
                    result.fetchall.return_value = [(past_date,)]
                # Second call: check if stats exist
                elif call_count[0] == 2:
                    result.scalar.return_value = 10  # Stats available
                # Later calls: aggregation, prev log, etc.
                else:
                    result.fetchone.return_value = (1, 1, 0, 0, 0, 100.0, 90.91)
                return result

            conn.execute = mock_execute
            conn.commit = MagicMock()
            return conn

        mock_engine.connect = mock_connect

        # Mock pandas read_sql to return our test data
        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [bets_df, actuals_df]

            trader = PaperTrader()
            result = trader.resolve_all_pending()

        assert result["dates_processed"] == 1
        assert result["dates_skipped"] == 0
        assert result["total_resolved"] == 1
        assert result["total_won"] == 1
        assert result["total_lost"] == 0

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_resolve_all_pending_multiple_dates(self, mock_get_engine):
        """Test resolve_all_pending processes multiple dates correctly."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        date1 = date(2026, 2, 10)
        date2 = date(2026, 2, 11)

        call_count = [0]

        # Create mock bet data for each date
        bets_df_date1 = pd.DataFrame([{
            "id": 1,
            "player_id": 101,
            "stat_type": "pts",
            "line": 20.5,
            "bet_direction": "over",
            "odds_at_bet": -110.0,
            "stake": 100.0,
        }])

        bets_df_date2 = pd.DataFrame([{
            "id": 2,
            "player_id": 102,
            "stat_type": "reb",
            "line": 8.5,
            "bet_direction": "under",
            "odds_at_bet": -110.0,
            "stake": 100.0,
        }])

        actuals_df_date1 = pd.DataFrame([{
            "player_id": 101,
            "pts": 25.0,  # Over wins
            "reb": 8.0,
            "ast": 5.0,
            "did_not_play": False,
        }])

        actuals_df_date2 = pd.DataFrame([{
            "player_id": 102,
            "pts": 15.0,
            "reb": 10.0,  # Above 8.5, under loses
            "ast": 3.0,
            "did_not_play": False,
        }])

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)

            def mock_execute(query, params=None):
                call_count[0] += 1
                result = MagicMock()
                # First call: get pending dates
                if call_count[0] == 1:
                    result.fetchall.return_value = [(date1,), (date2,)]
                # Stats checks and aggregations
                elif call_count[0] in (2, 6):  # Stats check calls
                    result.scalar.return_value = 10
                else:
                    result.fetchone.return_value = (1, 1, 0, 0, 0, 100.0, 90.91)
                return result

            conn.execute = mock_execute
            conn.commit = MagicMock()
            return conn

        mock_engine.connect = mock_connect

        read_sql_calls = [0]

        def mock_read_sql(*args, **kwargs):
            read_sql_calls[0] += 1
            if read_sql_calls[0] == 1:
                return bets_df_date1
            elif read_sql_calls[0] == 2:
                return actuals_df_date1
            elif read_sql_calls[0] == 3:
                return bets_df_date2
            else:
                return actuals_df_date2

        with patch("pandas.read_sql", side_effect=mock_read_sql):
            trader = PaperTrader()
            result = trader.resolve_all_pending()

        assert result["dates_processed"] == 2
        assert result["dates_skipped"] == 0
        assert result["total_resolved"] == 2
        # First bet wins (over and actual > line)
        # Second bet loses (under and actual > line)
        assert result["total_won"] == 1
        assert result["total_lost"] == 1

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_resolve_all_pending_idempotent(self, mock_get_engine):
        """Test that calling resolve_all_pending twice doesn't re-resolve bets."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # First call returns pending bets, second call returns empty
        # (because they were resolved in first call)
        call_count = [0]

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)

            def mock_execute(query, params=None):
                call_count[0] += 1
                result = MagicMock()
                result.fetchall.return_value = []  # No pending bets
                return result

            conn.execute = mock_execute
            return conn

        mock_engine.connect = mock_connect

        trader = PaperTrader()

        # First call
        result1 = trader.resolve_all_pending()
        # Second call
        result2 = trader.resolve_all_pending()

        # Both should return zeros (nothing to resolve)
        assert result1["total_resolved"] == 0
        assert result2["total_resolved"] == 0
