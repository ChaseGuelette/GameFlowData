"""Tests for BetSimulator class."""

from datetime import date

import pandas as pd
import pytest

from src.backtesting.bet_simulator import (
    Bet,
    BetOutcome,
    BetSide,
    BetSimulator,
)


class TestBet:
    """Tests for the Bet dataclass."""

    def test_resolve_over_win(self):
        """Test resolving an over bet that wins."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            stake=100.0,
            model_prob=0.60,
            implied_prob=0.52,
            edge=0.08,
        )

        bet.resolve(25.0)  # Actual > line = win

        assert bet.outcome == BetOutcome.WIN
        assert bet.actual == 25.0
        assert bet.profit == pytest.approx(90.91, rel=0.01)  # 100 * (100/110)

    def test_resolve_over_loss(self):
        """Test resolving an over bet that loses."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            stake=100.0,
            model_prob=0.60,
            implied_prob=0.52,
            edge=0.08,
        )

        bet.resolve(18.0)  # Actual < line = loss

        assert bet.outcome == BetOutcome.LOSS
        assert bet.actual == 18.0
        assert bet.profit == -100.0

    def test_resolve_over_push(self):
        """Test resolving an over bet that pushes."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.0,
            odds=-110,
            stake=100.0,
            model_prob=0.60,
            implied_prob=0.52,
            edge=0.08,
        )

        bet.resolve(20.0)  # Actual == line = push

        assert bet.outcome == BetOutcome.PUSH
        assert bet.profit == 0.0

    def test_resolve_under_win(self):
        """Test resolving an under bet that wins."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.UNDER,
            line=20.5,
            odds=-110,
            stake=100.0,
            model_prob=0.55,
            implied_prob=0.48,
            edge=0.07,
        )

        bet.resolve(18.0)  # Actual < line = win for under

        assert bet.outcome == BetOutcome.WIN
        assert bet.profit == pytest.approx(90.91, rel=0.01)

    def test_resolve_under_loss(self):
        """Test resolving an under bet that loses."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.UNDER,
            line=20.5,
            odds=-110,
            stake=100.0,
            model_prob=0.55,
            implied_prob=0.48,
            edge=0.07,
        )

        bet.resolve(25.0)  # Actual > line = loss for under

        assert bet.outcome == BetOutcome.LOSS
        assert bet.profit == -100.0

    def test_positive_odds_profit(self):
        """Test profit calculation with positive odds."""
        bet = Bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=150,  # Positive odds
            stake=100.0,
            model_prob=0.50,
            implied_prob=0.40,
            edge=0.10,
        )

        bet.resolve(25.0)  # Win

        assert bet.outcome == BetOutcome.WIN
        assert bet.profit == 150.0  # 100 * (150/100)


class TestBetSimulator:
    """Tests for BetSimulator class."""

    def test_should_bet_with_edge(self):
        """Test should_bet returns True when edge exceeds threshold."""
        simulator = BetSimulator(edge_threshold=0.05)

        result = simulator.should_bet(
            model_prob=0.60,
            implied_prob=0.52,
            odds=-110,
            side=BetSide.OVER,
        )

        assert result is True

    def test_should_not_bet_without_edge(self):
        """Test should_bet returns False when edge below threshold."""
        simulator = BetSimulator(edge_threshold=0.05)

        result = simulator.should_bet(
            model_prob=0.53,
            implied_prob=0.52,
            odds=-110,
            side=BetSide.OVER,
        )

        assert result is False

    def test_should_not_bet_extreme_odds(self):
        """Test should_bet filters extreme odds."""
        simulator = BetSimulator(edge_threshold=0.05, min_odds=-200, max_odds=200)

        # Too heavy favorite
        result = simulator.should_bet(
            model_prob=0.85,
            implied_prob=0.75,
            odds=-250,
            side=BetSide.OVER,
        )
        assert result is False

        # Too long shot
        result = simulator.should_bet(
            model_prob=0.45,
            implied_prob=0.30,
            odds=250,
            side=BetSide.OVER,
        )
        assert result is False

    def test_should_not_bet_with_none_values(self):
        """Test should_bet handles None values."""
        simulator = BetSimulator()

        assert simulator.should_bet(None, 0.52, -110, BetSide.OVER) is False
        assert simulator.should_bet(0.60, None, -110, BetSide.OVER) is False
        assert simulator.should_bet(0.60, 0.52, None, BetSide.OVER) is False

    def test_place_bet(self):
        """Test placing a bet."""
        simulator = BetSimulator()

        bet = simulator.place_bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
            stake=100.0,
        )

        assert len(simulator.bets) == 1
        assert bet.stake == 100.0
        assert bet.edge == pytest.approx(0.08, rel=0.01)

    def test_evaluate_predictions(self):
        """Test evaluating predictions DataFrame."""
        simulator = BetSimulator(edge_threshold=0.05)

        predictions_df = pd.DataFrame(
            [
                {
                    "player_id": 1,
                    "game_id": "001",
                    "stat": "pts",
                    "line": 20.5,
                    "over_odds": -110,
                    "under_odds": -110,
                    "over_prob": 0.60,
                    "under_prob": 0.40,
                    "implied_over": 0.52,
                    "implied_under": 0.52,
                },
                {
                    "player_id": 2,
                    "game_id": "001",
                    "stat": "pts",
                    "line": 15.5,
                    "over_odds": -110,
                    "under_odds": -110,
                    "over_prob": 0.52,  # No edge
                    "under_prob": 0.48,
                    "implied_over": 0.52,
                    "implied_under": 0.52,
                },
            ]
        )

        new_bets = simulator.evaluate_predictions(predictions_df, date(2024, 1, 15))

        # Only player 1 has edge on over
        assert len(new_bets) == 1
        assert new_bets[0].player_id == 1
        assert new_bets[0].side == BetSide.OVER

    def test_resolve_bets(self):
        """Test resolving bets with actuals."""
        simulator = BetSimulator()

        simulator.place_bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
        )

        actuals_df = pd.DataFrame([{"player_id": 1, "game_id": "001", "stat": "pts", "actual_value": 25.0}])

        resolved = simulator.resolve_bets(actuals_df)

        assert resolved == 1
        assert simulator.bets[0].outcome == BetOutcome.WIN

    def test_to_dataframe(self):
        """Test converting bets to DataFrame."""
        simulator = BetSimulator()

        simulator.place_bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
        )

        df = simulator.to_dataframe()

        assert len(df) == 1
        assert "player_id" in df.columns
        assert "side" in df.columns
        assert df.iloc[0]["side"] == "over"

    def test_get_summary(self):
        """Test getting summary statistics."""
        simulator = BetSimulator()

        # Place and resolve some bets
        bet1 = simulator.place_bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
        )
        bet1.resolve(25.0)  # Win

        bet2 = simulator.place_bet(
            player_id=2,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=15.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
        )
        bet2.resolve(10.0)  # Loss

        summary = simulator.get_summary()

        assert summary["total_bets"] == 2
        assert summary["resolved_bets"] == 2
        assert summary["wins"] == 1
        assert summary["losses"] == 1
        assert summary["hit_rate"] == 0.5

    def test_reset(self):
        """Test resetting the simulator."""
        simulator = BetSimulator()

        simulator.place_bet(
            player_id=1,
            game_id="001",
            game_date=date(2024, 1, 15),
            stat="pts",
            side=BetSide.OVER,
            line=20.5,
            odds=-110,
            model_prob=0.60,
            implied_prob=0.52,
        )

        assert len(simulator.bets) == 1

        simulator.reset()

        assert len(simulator.bets) == 0
