"""
Paper Trading Module for NBA Player Props.

Converts daily predictions into paper bets with Kelly-based stake sizing,
and resolves bets against actual game results.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine

if TYPE_CHECKING:
    from src.config.stat_config import StatConfigSet

logger = logging.getLogger(__name__)

# Supported stat types
SUPPORTED_STATS = {"pts", "reb", "ast"}


@dataclass
class PaperTrader:
    """
    Converts daily predictions into paper bets and tracks P&L.

    Supports standalone operation for dashboard integration.
    Supports per-stat edge thresholds via stat_config.
    """

    edge_threshold: float = 0.05
    kelly_fraction: float = 0.125
    max_bet_pct: float = 0.05  # Max 5% of bankroll per bet
    starting_bankroll: float = 10000.0
    min_odds: int = -200  # Don't bet heavy favorites
    max_odds: int = 200  # Don't bet long shots
    stat_config: StatConfigSet | None = None  # Per-stat edge thresholds

    def __post_init__(self):
        self.engine = get_engine()

    def _get_edge_threshold(self, stat: str) -> float:
        """Get edge threshold for a stat, using per-stat config if available."""
        if self.stat_config is not None:
            return self.stat_config.get_edge_threshold(stat)
        return self.edge_threshold

    def _american_to_decimal(self, odds: float) -> float:
        """Convert American odds to decimal odds."""
        if odds is None or pd.isna(odds):
            return 0.0
        if odds > 0:
            return (odds / 100.0) + 1.0
        else:
            return (100.0 / abs(odds)) + 1.0

    def _calculate_kelly_stake(
        self, odds: float, model_prob: float, bankroll: float
    ) -> float:
        """Calculate stake using fractional Kelly Criterion."""
        if odds is None or pd.isna(odds) or odds == 0:
            return 0.0

        # Convert to net fractional odds (b in Kelly formula)
        if odds > 0:
            b = odds / 100.0
        else:
            b = 100.0 / abs(odds)

        # Kelly: f = (p(b + 1) - 1) / b
        f = (model_prob * (b + 1) - 1) / b

        # Apply fraction
        f_fractional = f * self.kelly_fraction

        if f_fractional <= 0:
            return 0.0

        stake = f_fractional * bankroll

        # Cap at max_bet_pct
        max_stake = self.max_bet_pct * bankroll
        return min(stake, max_stake, bankroll)

    def _get_current_bankroll(self) -> float:
        """Get current bankroll from most recent daily log entry."""
        query = text("""
            SELECT bankroll_after
            FROM paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()

        if result is None:
            return self.starting_bankroll
        return float(result[0])

    def select_bets(self, game_date: date) -> list[dict[str, Any]]:
        """
        Query daily_predictions for game_date, filter by edge threshold,
        and calculate Kelly stakes.

        Returns list of bet dictionaries ready for placement.
        """
        bankroll = self._get_current_bankroll()

        # Query predictions with edge
        query = text("""
            SELECT
                id as prediction_id,
                prediction_date,
                player_id,
                player_name,
                game_id,
                stat,
                line,
                over_odds,
                under_odds,
                over_prob,
                under_prob,
                implied_over,
                implied_under,
                over_edge,
                under_edge
            FROM daily_predictions
            WHERE prediction_date = :game_date
              AND stat IN ('pts', 'reb', 'ast')
              AND line IS NOT NULL
            ORDER BY player_name, stat
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            logger.warning(f"No predictions found for {game_date}")
            return []

        bets = []
        for _, row in df.iterrows():
            stat = row["stat"]
            threshold = self._get_edge_threshold(stat)

            # Determine which direction has better edge
            over_edge = row["over_edge"] if pd.notna(row["over_edge"]) else 0
            under_edge = row["under_edge"] if pd.notna(row["under_edge"]) else 0

            # Select the direction with higher edge that meets threshold
            direction = None
            edge = 0
            odds = 0
            model_prob = 0
            implied_prob = 0

            if over_edge > under_edge and over_edge >= threshold:
                direction = "over"
                edge = over_edge
                odds = row["over_odds"]
                model_prob = row["over_prob"]
                implied_prob = row["implied_over"]
            elif under_edge >= threshold:
                direction = "under"
                edge = under_edge
                odds = row["under_odds"]
                model_prob = row["under_prob"]
                implied_prob = row["implied_under"]

            if direction is None:
                continue

            # Filter extreme odds
            if pd.isna(odds) or odds < self.min_odds or odds > self.max_odds:
                continue

            # Calculate stake
            stake = self._calculate_kelly_stake(odds, model_prob, bankroll)
            if stake <= 0:
                continue

            bet = {
                "prediction_id": int(row["prediction_id"]),
                "game_date": game_date,
                "player_id": int(row["player_id"]),
                "player_name": row["player_name"],
                "stat_type": row["stat"],
                "line": float(row["line"]),
                "bet_direction": direction,
                "odds_at_bet": float(odds),
                "implied_prob": float(implied_prob),
                "model_prob": float(model_prob),
                "edge": float(edge),
                "stake": round(stake, 2),
                "kelly_fraction": self.kelly_fraction,
            }
            bets.append(bet)

        logger.info(f"Selected {len(bets)} bets for {game_date}")
        return bets

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """
        Insert bets into paper_bets table (UPSERT for idempotency).

        Returns count of bets placed.
        """
        if not bets:
            return 0

        # UPSERT query
        query = text("""
            INSERT INTO paper_bets (
                prediction_id, game_date, player_id, player_name, stat_type,
                line, bet_direction, odds_at_bet, implied_prob, model_prob,
                edge, stake, kelly_fraction, status
            ) VALUES (
                :prediction_id, :game_date, :player_id, :player_name, :stat_type,
                :line, :bet_direction, :odds_at_bet, :implied_prob, :model_prob,
                :edge, :stake, :kelly_fraction, 'pending'
            )
            ON CONFLICT (game_date, player_id, stat_type, bet_direction)
            DO UPDATE SET
                prediction_id = EXCLUDED.prediction_id,
                line = EXCLUDED.line,
                odds_at_bet = EXCLUDED.odds_at_bet,
                implied_prob = EXCLUDED.implied_prob,
                model_prob = EXCLUDED.model_prob,
                edge = EXCLUDED.edge,
                stake = EXCLUDED.stake,
                kelly_fraction = EXCLUDED.kelly_fraction,
                placed_at = NOW()
            WHERE paper_bets.status = 'pending'
        """)

        with self.engine.connect() as conn:
            for bet in bets:
                conn.execute(query, bet)
            conn.commit()

        logger.info(f"Placed {len(bets)} bets")
        return len(bets)

    def resolve_bets(self, game_date: date) -> dict[str, Any]:
        """
        Resolve pending bets for game_date using actual stats from player_game_stats.

        Updates paper_bets with status/pnl and paper_trading_daily_log with summary.

        Returns resolution summary.
        """
        # Get pending bets for date
        bets_query = text("""
            SELECT id, player_id, stat_type, line, bet_direction, odds_at_bet, stake
            FROM paper_bets
            WHERE game_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": game_date})

        if bets_df.empty:
            logger.info(f"No pending bets to resolve for {game_date}")
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Get actual stats - join with team_game_stats to get game_date
        actuals_query = text("""
            SELECT
                pgs.player_id,
                pgs.pts,
                pgs.reb,
                pgs.ast,
                pgs.did_not_play
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE tgs.game_date = :game_date
        """)

        with self.engine.connect() as conn:
            actuals_df = pd.read_sql(
                actuals_query, conn, params={"game_date": game_date}
            )

        # Build actuals lookup: player_id -> {stat -> value}
        actuals_lookup: dict[int, dict[str, float | None]] = {}
        for _, row in actuals_df.iterrows():
            player_id = int(row["player_id"])
            did_not_play = row.get("did_not_play", False) or False

            if did_not_play:
                actuals_lookup[player_id] = {"pts": None, "reb": None, "ast": None}
            else:
                actuals_lookup[player_id] = {
                    "pts": float(row["pts"]) if pd.notna(row["pts"]) else None,
                    "reb": float(row["reb"]) if pd.notna(row["reb"]) else None,
                    "ast": float(row["ast"]) if pd.notna(row["ast"]) else None,
                }

        # Resolve each bet
        results = {"won": 0, "lost": 0, "push": 0, "cancelled": 0}
        updates = []

        for _, bet in bets_df.iterrows():
            player_id = int(bet["player_id"])
            stat_type = bet["stat_type"]
            line = float(bet["line"])
            direction = bet["bet_direction"]
            odds = float(bet["odds_at_bet"])
            stake = float(bet["stake"])
            bet_id = int(bet["id"])

            # Get actual value
            player_actuals = actuals_lookup.get(player_id, {})
            actual = player_actuals.get(stat_type)

            # Determine outcome
            if actual is None:
                status = "cancelled"
                pnl = 0.0
                results["cancelled"] += 1
            elif actual > line:
                if direction == "over":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1)
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake
                    results["lost"] += 1
            elif actual < line:
                if direction == "under":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1)
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake
                    results["lost"] += 1
            else:  # actual == line
                status = "push"
                pnl = 0.0
                results["push"] += 1

            updates.append(
                {
                    "bet_id": bet_id,
                    "status": status,
                    "actual_value": actual,
                    "pnl": round(pnl, 2),
                }
            )

        # Update bets in database
        update_query = text("""
            UPDATE paper_bets
            SET status = :status,
                actual_value = :actual_value,
                pnl = :pnl,
                resolved_at = NOW()
            WHERE id = :bet_id
        """)

        with self.engine.connect() as conn:
            for update in updates:
                conn.execute(update_query, update)
            conn.commit()

        # Update daily log
        self._update_daily_log(game_date)

        results["resolved"] = len(updates)
        logger.info(
            f"Resolved {results['resolved']} bets: "
            f"{results['won']}W {results['lost']}L {results['push']}P {results['cancelled']}C"
        )
        return results

    def _update_daily_log(self, game_date: date) -> None:
        """Update or create daily log entry with aggregated bet stats."""
        # Aggregate stats for the date
        agg_query = text("""
            SELECT
                COUNT(*) as total_bets,
                COUNT(*) FILTER (WHERE status = 'won') as bets_won,
                COUNT(*) FILTER (WHERE status = 'lost') as bets_lost,
                COUNT(*) FILTER (WHERE status = 'push') as bets_push,
                COUNT(*) FILTER (WHERE status = 'pending') as bets_pending,
                COALESCE(SUM(stake) FILTER (WHERE status IN ('won', 'lost')), 0) as total_staked,
                COALESCE(SUM(pnl), 0) as total_pnl
            FROM paper_bets
            WHERE game_date = :game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(agg_query, {"game_date": game_date}).fetchone()

        if result is None or result[0] == 0:
            return

        total_bets = result[0]
        bets_won = result[1]
        bets_lost = result[2]
        bets_push = result[3]
        bets_pending = result[4]
        total_staked = float(result[5])
        total_pnl = float(result[6])

        roi_pct = (total_pnl / total_staked * 100) if total_staked > 0 else 0

        # Get previous cumulative P&L
        prev_query = text("""
            SELECT cumulative_pnl, bankroll_after
            FROM paper_trading_daily_log
            WHERE game_date < :game_date
            ORDER BY game_date DESC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            prev_result = conn.execute(prev_query, {"game_date": game_date}).fetchone()

        if prev_result is None:
            prev_cumulative = 0.0
            prev_bankroll = self.starting_bankroll
        else:
            prev_cumulative = float(prev_result[0])
            prev_bankroll = float(prev_result[1])

        cumulative_pnl = prev_cumulative + total_pnl
        bankroll_after = prev_bankroll + total_pnl

        # UPSERT daily log
        upsert_query = text("""
            INSERT INTO paper_trading_daily_log (
                game_date, total_bets, bets_won, bets_lost, bets_push, bets_pending,
                total_staked, total_pnl, roi_pct, cumulative_pnl, bankroll_after
            ) VALUES (
                :game_date, :total_bets, :bets_won, :bets_lost, :bets_push, :bets_pending,
                :total_staked, :total_pnl, :roi_pct, :cumulative_pnl, :bankroll_after
            )
            ON CONFLICT (game_date) DO UPDATE SET
                total_bets = EXCLUDED.total_bets,
                bets_won = EXCLUDED.bets_won,
                bets_lost = EXCLUDED.bets_lost,
                bets_push = EXCLUDED.bets_push,
                bets_pending = EXCLUDED.bets_pending,
                total_staked = EXCLUDED.total_staked,
                total_pnl = EXCLUDED.total_pnl,
                roi_pct = EXCLUDED.roi_pct,
                cumulative_pnl = EXCLUDED.cumulative_pnl,
                bankroll_after = EXCLUDED.bankroll_after,
                updated_at = NOW()
        """)

        with self.engine.connect() as conn:
            conn.execute(
                upsert_query,
                {
                    "game_date": game_date,
                    "total_bets": total_bets,
                    "bets_won": bets_won,
                    "bets_lost": bets_lost,
                    "bets_push": bets_push,
                    "bets_pending": bets_pending,
                    "total_staked": total_staked,
                    "total_pnl": round(total_pnl, 2),
                    "roi_pct": round(roi_pct, 2),
                    "cumulative_pnl": round(cumulative_pnl, 2),
                    "bankroll_after": round(bankroll_after, 2),
                },
            )
            conn.commit()

        logger.info(f"Updated daily log for {game_date}: P&L={total_pnl:.2f}")

    def get_pending_bets(self, game_date: date | None = None) -> pd.DataFrame:
        """Query paper_bets with status='pending'."""
        if game_date:
            query = text("""
                SELECT * FROM paper_bets
                WHERE status = 'pending' AND game_date = :game_date
                ORDER BY player_name, stat_type
            """)
            params = {"game_date": game_date}
        else:
            query = text("""
                SELECT * FROM paper_bets
                WHERE status = 'pending'
                ORDER BY game_date, player_name, stat_type
            """)
            params = {}

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_daily_summary(self, game_date: date) -> dict[str, Any] | None:
        """Query paper_trading_daily_log for date."""
        query = text("""
            SELECT * FROM paper_trading_daily_log
            WHERE game_date = :game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"game_date": game_date}).fetchone()

        if result is None:
            return None

        return {
            "game_date": result[1],
            "total_bets": result[2],
            "bets_won": result[3],
            "bets_lost": result[4],
            "bets_push": result[5],
            "bets_pending": result[6],
            "total_staked": float(result[7]),
            "total_pnl": float(result[8]),
            "roi_pct": float(result[9]) if result[9] else 0,
            "cumulative_pnl": float(result[10]),
            "bankroll_after": float(result[11]),
        }

    def get_bets_for_date(self, game_date: date) -> pd.DataFrame:
        """Get all bets for a specific date."""
        query = text("""
            SELECT * FROM paper_bets
            WHERE game_date = :game_date
            ORDER BY player_name, stat_type
        """)

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"game_date": game_date})

    def get_recent_performance(self, days: int = 7) -> pd.DataFrame:
        """Get P&L summary for last N days."""
        query = text("""
            SELECT * FROM paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT :days
        """)

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"days": days})
