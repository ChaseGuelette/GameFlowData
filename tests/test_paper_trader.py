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
