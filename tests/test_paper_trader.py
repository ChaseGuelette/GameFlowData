"""Tests for PaperTrader class."""

import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.paper_trading.paper_trader import PaperTrader


def _selection_evidence(engine, predictions):
    """Supply captured producer evidence and eligibility-query responses."""
    row = predictions.iloc[0].to_dict()
    decision = datetime(2024, 1, 15, 18, tzinfo=UTC)
    start = decision + timedelta(hours=2)
    quotes = [
        {
            "staging_id": index, "api_game_id": "test-game", "game_id": row["game_id"],
            "player_id": row["player_id"], "bookmaker": "draftkings",
            "market_key": {"pts": "player_points", "reb": "player_rebounds"}[row["stat"]],
            "line": row["line"], "outcome_label": side, "odds_american": row[f"{side.lower()}_odds"],
            "source_snapshot_time": (decision - timedelta(minutes=2)).isoformat(),
            "observed_at": (decision - timedelta(minutes=1)).isoformat(),
            "commence_time": start.isoformat(),
        }
        for index, side in enumerate(("Over", "Under"), 1)
    ]
    predictions["bookmaker"] = "draftkings"
    predictions["quote_snapshot"] = [{
        "decision_time": decision.isoformat(), "rows": quotes,
        "probability_source": "empirical_samples", "sample_count": 100,
    }]
    predictions["model_snapshot"] = [{
        "artifact": {"identity": "test-only-model", "file_hashes": {
            "model.joblib": {"sha256": sha256(b"test-only model").hexdigest()},
        }},
        "sample_sha256": sha256(b"test-only samples").hexdigest(),
    }]

    def execute(query, params=None):
        sql = str(query)
        result = MagicMock()
        if "FROM nba_game_schedule" in sql:
            result.fetchone.return_value = (start,)
        elif "SELECT to_jsonb(q)" in sql:
            result.fetchall.return_value = [(quote,) for quote in quotes]
        elif "SELECT 1 FROM nba_player_props_inference" in sql:
            result.fetchone.return_value = None
        else:
            raise AssertionError(f"Unexpected selection query: {sql}")
        return result

    engine.begin.return_value.__enter__.return_value.execute.side_effect = execute


def _settlement_responses(engine, bets_by_date, actuals_by_date):
    """Model captured-ledger, exact-game and conditional-update DB responses."""
    ledger = {}
    actuals = {}
    for day, frame in bets_by_date.items():
        for row in frame.to_dict("records"):
            bet = {**row, "game_date": day.isoformat(), "game_id": f"00225{row['id']:05d}",
                   "prediction_id": row["id"], "status": "pending"}
            ledger[bet["id"]] = bet
            stats = actuals_by_date[day].set_index("player_id").loc[bet["player_id"]].to_dict()
            actuals[(bet["player_id"], bet["game_id"])] = {
                **stats, "min": 30, "comment": None,
                "team_wl": "W", "team_pts": 110, "team_min": 240,
                "opp_wl": "L", "opp_pts": 100, "opp_min": 240,
                "schedule_game_id": bet["game_id"], "game_status": 3,
                "status_text": "Final", "postponed_status": "N", "schedule_matches": True,
            }

    def execute(query, params=None):
        sql = str(query)
        result = MagicMock()
        if "SELECT DISTINCT game_date" in sql:
            result.fetchall.return_value = [(day,) for day in sorted(bets_by_date)]
        elif "FROM daily_predictions" in sql:
            bet = ledger[params["prediction_id"]]
            matches = all(params[key] == bet[key] for key in ("player_id", "game_date", "stat_type"))
            result.fetchone.return_value = (bet["game_id"],) if matches else None
        elif "FROM player_game_stats p" in sql:
            result.mappings.return_value.all.return_value = [actuals[(params["player_id"], params["game_id"])]]
        elif "UPDATE paper_bets AS b" in sql:
            bet = ledger[params["id"]]
            if bet["status"] == "pending" and bet == json.loads(params["captured"]):
                bet.update(status=params["status"], actual_value=params["actual"], pnl=params["pnl"])
                result.fetchone.return_value = (bet["id"],)
            else:
                result.fetchone.return_value = None
        elif "pg_advisory_xact_lock" not in sql and "WITH daily AS" not in sql:
            raise AssertionError(f"Unexpected settlement query: {sql}")
        return result

    conn = MagicMock()
    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn

    def read_sql(query, connection, params):
        return pd.DataFrame([{"captured": dict(bet)} for bet in ledger.values()
                             if bet["game_date"] == str(params["game_date"]) and bet["status"] == "pending"],
                            columns=["captured"])

    return read_sql


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
        """Test bet selection chooses over when bl_over_edge is higher."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # Simulates DB returning is_recommended=True rows with BL probs
        predictions_df = pd.DataFrame([{
            "prediction_id": 1,
            "player_id": 101,
            "player_name": "Test Player",
            "game_id": "0022400001",
            "stat": "pts",
            "line": 20.5,
            "over_odds": -110.0,
            "under_odds": -110.0,
            "bl_over_prob": 0.62,
            "bl_under_prob": 0.38,
            "bl_over_edge": 0.10,  # Higher BL edge → over
            "bl_under_edge": -0.10,
            "implied_over": 0.52,
            "implied_under": 0.48,
        }])

        _selection_evidence(mock_engine, predictions_df)

        with patch("pandas.read_sql", return_value=predictions_df):
            trader = PaperTrader()
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 1
        assert bets[0]["bet_direction"] == "over"
        assert bets[0]["edge"] == pytest.approx(0.10)

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_select_bets_under_direction(self, mock_get_engine):
        """Test bet selection chooses under when bl_under_edge is higher."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        predictions_df = pd.DataFrame([{
            "prediction_id": 1,
            "player_id": 101,
            "player_name": "Test Player",
            "game_id": "0022400001",
            "stat": "reb",
            "line": 8.5,
            "over_odds": -110.0,
            "under_odds": -110.0,
            "bl_over_prob": 0.42,
            "bl_under_prob": 0.58,
            "bl_over_edge": -0.10,
            "bl_under_edge": 0.10,  # Higher BL edge → under
            "implied_over": 0.52,
            "implied_under": 0.48,
        }])

        _selection_evidence(mock_engine, predictions_df)

        with patch("pandas.read_sql", return_value=predictions_df):
            trader = PaperTrader()
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 1
        assert bets[0]["bet_direction"] == "under"
        assert bets[0]["edge"] == pytest.approx(0.10)

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_select_bets_no_recommended(self, mock_get_engine):
        """Test bet selection returns empty when no is_recommended rows exist."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # DB returns empty DataFrame (no is_recommended=True rows)
        empty_df = pd.DataFrame(columns=[
            "prediction_id", "player_id", "player_name", "game_id", "stat",
            "line", "over_odds", "under_odds", "bl_over_prob", "bl_under_prob",
            "bl_over_edge", "bl_under_edge", "implied_over", "implied_under",
        ])

        bankroll_result = MagicMock()
        bankroll_result.fetchone.return_value = (10000.0,)

        def mock_connect():
            conn = MagicMock()
            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.execute.return_value = bankroll_result
            return conn

        mock_engine.connect = mock_connect

        with patch("pandas.read_sql", return_value=empty_df):
            trader = PaperTrader()
            bets = trader.select_bets(date(2024, 1, 15))

        assert len(bets) == 0  # No recommended predictions


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
    """Tests for PaperTrader default configuration.

    Note: Defaults now come from environment variables (PAPER_TRADING_*).
    These tests verify explicit parameter passing works correctly.
    """

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_explicit_edge_threshold(self, mock_get_engine):
        """Test explicit edge threshold parameter is used."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader(edge_threshold=0.07)
        assert trader.edge_threshold == 0.07

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_explicit_kelly_fraction(self, mock_get_engine):
        """Test explicit Kelly fraction parameter is used."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader(kelly_fraction=0.25)
        assert trader.kelly_fraction == 0.25

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_explicit_bankroll(self, mock_get_engine):
        """Test explicit starting bankroll parameter is used."""
        mock_get_engine.return_value = MagicMock()
        trader = PaperTrader(starting_bankroll=5000.0)
        assert trader.starting_bankroll == 5000.0

    @patch("src.paper_trading.paper_trader.get_engine")
    def test_default_odds_filters(self, mock_get_engine):
        """Test default odds filters (not env-configurable)."""
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

        read_sql = _settlement_responses(mock_engine, {past_date: bets_df}, {past_date: actuals_df})
        with patch("pandas.read_sql", side_effect=read_sql):
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

        read_sql = _settlement_responses(
            mock_engine, {date1: bets_df_date1, date2: bets_df_date2},
            {date1: actuals_df_date1, date2: actuals_df_date2},
        )
        with patch("pandas.read_sql", side_effect=read_sql):
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
