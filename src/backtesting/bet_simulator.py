"""
Bet simulation and P&L tracking for backtesting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from src.config.stat_config import StatConfigSet


class BetSide(Enum):
    """Bet direction."""

    OVER = "over"
    UNDER = "under"


class BetOutcome(Enum):
    """Bet result."""

    WIN = "win"
    LOSS = "loss"
    PUSH = "push"


@dataclass
class Bet:
    """Represents a single simulated bet."""

    # Identifiers
    player_id: int
    game_id: str
    game_date: date
    stat: str

    # Bet details
    side: BetSide
    line: float
    odds: int
    stake: float

    # Model info
    model_prob: float
    implied_prob: float
    edge: float

    # Line shopping info
    bookmaker: str | None = None  # Bookmaker offering the best line

    # BL diagnostic (optional)
    posterior_prob: float | None = None  # BL-blended probability (when BL is enabled)

    # Outcome (filled after result)
    actual: float | None = None
    outcome: BetOutcome | None = None
    profit: float | None = None

    def resolve(self, actual_value: float) -> None:
        """Resolve the bet with the actual outcome."""
        self.actual = actual_value

        if self.side == BetSide.OVER:
            if actual_value > self.line:
                self.outcome = BetOutcome.WIN
            elif actual_value < self.line:
                self.outcome = BetOutcome.LOSS
            else:
                self.outcome = BetOutcome.PUSH
        else:  # UNDER
            if actual_value < self.line:
                self.outcome = BetOutcome.WIN
            elif actual_value > self.line:
                self.outcome = BetOutcome.LOSS
            else:
                self.outcome = BetOutcome.PUSH

        self.profit = self._calculate_profit()

    def _calculate_profit(self) -> float:
        """Calculate profit/loss based on outcome."""
        if self.outcome == BetOutcome.PUSH:
            return 0.0

        if self.outcome == BetOutcome.WIN:
            if self.odds > 0:
                return self.stake * (self.odds / 100)
            else:
                return self.stake * (100 / abs(self.odds))
        else:  # LOSS
            return -self.stake


@dataclass
class BetSimulator:
    """
    Simulates betting based on model predictions and tracks P&L.

    Supports per-stat edge thresholds via stat_config. When stat_config is
    provided, edge_threshold serves as the global fallback.
    """

    edge_threshold: float = 0.05
    starting_bankroll: float = 10000.0
    kelly_fraction: float = 0.125
    max_bet_pct: float | None = None  # Cap bet size as % of bankroll (e.g., 0.025 = 2.5%)
    min_odds: int = -200  # Don't bet on heavy favorites
    max_odds: int = 200  # Don't bet on long shots
    allowed_bets: set[tuple[str, str]] | None = None  # e.g., {("pts", "under"), ("reb", "over")}
    use_bl_for_sizing: bool = False  # Use BL-blended probs for Kelly sizing (sizing_prob_* columns)
    stat_config: StatConfigSet | None = None  # Per-stat edge thresholds

    bets: list[Bet] = field(default_factory=list)
    current_bankroll: float = field(init=False)

    def __post_init__(self):
        self.current_bankroll = self.starting_bankroll

    def _get_edge_threshold(self, stat: str) -> float:
        """Get edge threshold for a stat, using per-stat config if available."""
        if self.stat_config is not None:
            return self.stat_config.get_edge_threshold(stat)
        return self.edge_threshold

    def _calculate_kelly_stake(self, odds: int, model_prob: float) -> float:
        """Calculate stake using fractional Kelly Criterion with optional cap."""
        if odds == 0:
            return 0.0

        # Convert American odds to decimal odds (b = net fractional odds)
        # Decimal Odds = (odds / 100) + 1 for positive
        # Decimal Odds = (100 / abs(odds)) + 1 for negative
        # Kelly 'b' is the net odds (Decimal - 1)
        if odds > 0:
            b = odds / 100.0
        else:
            b = 100.0 / abs(odds)

        # Kelly formula: f = (p(b + 1) - 1) / b
        # Simplified: f = (p * b - (1 - p)) / b
        f = (model_prob * (b + 1) - 1) / b

        # Apply fraction
        f_fractional = f * self.kelly_fraction

        if f_fractional <= 0:
            return 0.0

        # Apply max bet cap if configured
        if self.max_bet_pct is not None:
            f_fractional = min(f_fractional, self.max_bet_pct)

        # Calculate stake based on current bankroll
        stake = f_fractional * self.current_bankroll

        # Safety: Ensure we don't bet more than we have
        return min(stake, self.current_bankroll)

    def should_bet(
        self,
        model_prob: float,
        implied_prob: float,
        odds: int,
        side: BetSide,
        stat: str | None = None,
    ) -> bool:
        """Determine if a bet should be placed based on edge and filters.

        Args:
            stat: Stat type for per-stat edge threshold lookup. If None,
                  uses global edge_threshold.
        """
        if model_prob is None or implied_prob is None or odds is None:
            return False

        if pd.isna(model_prob) or pd.isna(implied_prob) or pd.isna(odds):
            return False

        edge = model_prob - implied_prob

        # Must have minimum edge (per-stat or global)
        threshold = self._get_edge_threshold(stat) if stat else self.edge_threshold
        if edge < threshold:
            return False

        # Filter extreme odds
        if odds < self.min_odds or odds > self.max_odds:
            return False

        return True

    def place_bet(
        self,
        player_id: int,
        game_id: str,
        game_date: date,
        stat: str,
        side: BetSide,
        line: float,
        odds: int,
        model_prob: float,
        implied_prob: float,
        stake: float | None = None,
        sizing_prob: float | None = None,
    ) -> Bet:
        """Create and record a bet.

        Args:
            sizing_prob: Optional BL-blended probability for Kelly sizing.
                         When provided, used instead of model_prob for stake calculation.
        """
        # Calculate stake if not provided
        # Use sizing_prob for Kelly when available, else use model_prob
        if stake is None:
            kelly_prob = sizing_prob if sizing_prob is not None else model_prob
            stake = self._calculate_kelly_stake(odds, kelly_prob)

        # Ensure we have funds (double check)
        if stake > self.current_bankroll:
            stake = self.current_bankroll

        # Deduct stake from bankroll immediately
        self.current_bankroll -= stake

        bet = Bet(
            player_id=player_id,
            game_id=game_id,
            game_date=game_date,
            stat=stat,
            side=side,
            line=line,
            odds=odds,
            stake=stake,
            model_prob=model_prob,
            implied_prob=implied_prob,
            edge=model_prob - implied_prob,
        )
        self.bets.append(bet)
        return bet

    def evaluate_predictions(
        self,
        predictions_df: pd.DataFrame,
        game_date: date,
    ) -> list[Bet]:
        """
        Evaluate predictions DataFrame and place appropriate bets.

        Expected columns:
        - player_id, game_id, stat
        - line, over_odds, under_odds
        - over_prob, under_prob
        - implied_over, implied_under
        """
        new_bets = []

        for _, row in predictions_df.iterrows():
            if pd.isna(row.get("line")):
                continue

            # Stop if bankroll is depleted
            if self.current_bankroll <= 0:
                break

            stat = row["stat"]

            # Check over bet (skip if not in allowlist)
            if self.allowed_bets is None or (stat, "over") in self.allowed_bets:
                if self.should_bet(
                    model_prob=row.get("over_prob"),
                    implied_prob=row.get("implied_over"),
                    odds=row.get("over_odds"),
                    side=BetSide.OVER,
                    stat=stat,
                ):
                    # Get sizing prob for Kelly when BL sizing is enabled
                    sizing_prob = None
                    if self.use_bl_for_sizing and "sizing_prob_over" in row.index:
                        sizing_prob = row.get("sizing_prob_over")
                        if pd.isna(sizing_prob):
                            sizing_prob = None

                    bet = self.place_bet(
                        player_id=row["player_id"],
                        game_id=row["game_id"],
                        game_date=game_date,
                        stat=stat,
                        side=BetSide.OVER,
                        line=row["line"],
                        odds=int(row["over_odds"]),
                        model_prob=row["over_prob"],
                        implied_prob=row["implied_over"],
                        sizing_prob=sizing_prob,
                    )
                    # Store bookmaker if available (line shopping)
                    if "bookmaker" in row.index and not pd.isna(row.get("bookmaker")):
                        bet.bookmaker = row["bookmaker"]
                    # Store BL posterior if available
                    if "posterior_over" in row.index and not pd.isna(row.get("posterior_over")):
                        bet.posterior_prob = row["posterior_over"]
                    # Also store sizing_prob as posterior when using BL sizing
                    elif sizing_prob is not None:
                        bet.posterior_prob = sizing_prob
                    new_bets.append(bet)

            # Check under bet (skip if not in allowlist)
            if self.allowed_bets is None or (stat, "under") in self.allowed_bets:
                if self.should_bet(
                    model_prob=row.get("under_prob"),
                    implied_prob=row.get("implied_under"),
                    odds=row.get("under_odds"),
                    side=BetSide.UNDER,
                    stat=stat,
                ):
                    # Get sizing prob for Kelly when BL sizing is enabled
                    sizing_prob = None
                    if self.use_bl_for_sizing and "sizing_prob_under" in row.index:
                        sizing_prob = row.get("sizing_prob_under")
                        if pd.isna(sizing_prob):
                            sizing_prob = None

                    bet = self.place_bet(
                        player_id=row["player_id"],
                        game_id=row["game_id"],
                        game_date=game_date,
                        stat=stat,
                        side=BetSide.UNDER,
                        line=row["line"],
                        odds=int(row["under_odds"]),
                        model_prob=row["under_prob"],
                        implied_prob=row["implied_under"],
                        sizing_prob=sizing_prob,
                    )
                    # Store bookmaker if available (line shopping)
                    if "bookmaker" in row.index and not pd.isna(row.get("bookmaker")):
                        bet.bookmaker = row["bookmaker"]
                    # Store BL posterior if available
                    if "posterior_under" in row.index and not pd.isna(row.get("posterior_under")):
                        bet.posterior_prob = row["posterior_under"]
                    # Also store sizing_prob as posterior when using BL sizing
                    elif sizing_prob is not None:
                        bet.posterior_prob = sizing_prob
                    new_bets.append(bet)

        return new_bets

    def resolve_bets(self, actuals_df: pd.DataFrame) -> int:
        """
        Resolve unresolved bets with actual outcomes.

        Expected columns: player_id, game_id, stat, actual_value
        """
        resolved_count = 0

        # Build lookup for actuals
        actuals_lookup = {}
        for _, row in actuals_df.iterrows():
            key = (row["player_id"], row["game_id"], row["stat"])
            actuals_lookup[key] = row["actual_value"]

        # Resolve each unresolved bet
        for bet in self.bets:
            if bet.outcome is not None:
                continue

            key = (bet.player_id, bet.game_id, bet.stat)
            if key in actuals_lookup:
                bet.resolve(actuals_lookup[key])
                resolved_count += 1

                # Update bankroll based on outcome
                if bet.outcome == BetOutcome.WIN:
                    # Return stake + profit
                    self.current_bankroll += bet.stake + bet.profit
                elif bet.outcome == BetOutcome.PUSH:
                    # Return stake only
                    self.current_bankroll += bet.stake
                # On LOSS, money is already gone (deducted at placement)

        return resolved_count

    def resolve_voids(self, voids_df: pd.DataFrame) -> int:
        """
        Resolve bets as PUSH (void) for players who did not play.

        Expected columns: player_id, game_id
        """
        if voids_df.empty:
            return 0

        voided_count = 0

        # Build set of (player_id, game_id) for fast lookup
        # We don't need 'stat' because if a player didn't play, ALL their props are void
        void_keys = set()
        for _, row in voids_df.iterrows():
            void_keys.add((row["player_id"], row["game_id"]))

        for bet in self.bets:
            if bet.outcome is not None:
                continue

            if (bet.player_id, bet.game_id) in void_keys:
                bet.outcome = BetOutcome.PUSH
                bet.actual = 0  # No stats recorded
                bet.profit = 0.0

                # Refund stake
                self.current_bankroll += bet.stake
                voided_count += 1

        return voided_count

    def to_dataframe(self) -> pd.DataFrame:
        """Convert bets to DataFrame for analysis."""
        records = []
        for bet in self.bets:
            records.append(
                {
                    "player_id": bet.player_id,
                    "game_id": bet.game_id,
                    "game_date": bet.game_date,
                    "stat": bet.stat,
                    "side": bet.side.value,
                    "line": bet.line,
                    "odds": bet.odds,
                    "stake": bet.stake,
                    "bookmaker": bet.bookmaker,
                    "model_prob": bet.model_prob,
                    "implied_prob": bet.implied_prob,
                    "edge": bet.edge,
                    "posterior_prob": bet.posterior_prob,
                    "actual": bet.actual,
                    "outcome": bet.outcome.value if bet.outcome else None,
                    "profit": bet.profit,
                }
            )
        return pd.DataFrame(records)

    def get_summary(self) -> dict:
        """Get summary statistics for all bets."""
        resolved = [b for b in self.bets if b.outcome is not None]

        if not resolved:
            return {
                "total_bets": len(self.bets),
                "resolved_bets": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "total_staked": 0.0,
                "total_profit": 0.0,
                "roi": 0.0,
                "hit_rate": 0.0,
            }

        wins = sum(1 for b in resolved if b.outcome == BetOutcome.WIN)
        losses = sum(1 for b in resolved if b.outcome == BetOutcome.LOSS)
        pushes = sum(1 for b in resolved if b.outcome == BetOutcome.PUSH)
        total_staked = sum(b.stake for b in resolved if b.outcome != BetOutcome.PUSH)
        total_profit = sum(b.profit for b in resolved)

        return {
            "total_bets": len(self.bets),
            "resolved_bets": len(resolved),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "total_staked": total_staked,
            "total_profit": total_profit,
            "roi": total_profit / total_staked if total_staked > 0 else 0.0,
            "hit_rate": wins / (wins + losses) if (wins + losses) > 0 else 0.0,
        }

    def reset(self) -> None:
        """Clear all bets."""
        self.bets = []
