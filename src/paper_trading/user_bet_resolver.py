#!/usr/bin/env python3
"""
User Bet Resolver
=================
Resolves user-placed bets (from the dashboard checkmark) against actual game results.
Mirrors the resolution logic in PaperTrader.resolve_bets() but operates on the
user_bets table instead of paper_bets.

Called from daily_stats_job.py after existing paper bet resolution.
"""

import logging
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.paper_trading.paper_trader import PaperTrader

logger = logging.getLogger(__name__)


class UserBetResolver:
    """Resolve user bets from user_bets table against actual game stats."""

    def __init__(self):
        # Reuse PaperTrader's engine (same DB connection)
        trader = PaperTrader()
        self.engine = trader.engine

    @staticmethod
    def _american_to_decimal(american_odds: float) -> float:
        """Convert American odds to decimal odds."""
        if american_odds > 0:
            return 1 + american_odds / 100
        else:
            return 1 + 100 / abs(american_odds)

    def resolve_bets_for_date(self, game_date: date) -> dict[str, Any]:
        """
        Resolve pending user bets for a single game_date using actual stats.

        Returns resolution summary dict.
        """
        # Get pending user bets for date
        bets_query = text("""
            SELECT id, player_id, stat_type, line, bet_direction, odds_at_bet, stake
            FROM user_bets
            WHERE game_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": game_date})

        if bets_df.empty:
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Get actual stats via player_game_stats + team_game_stats
        actuals_query = text("""
            SELECT
                pgs.player_id,
                pgs.pts,
                pgs.reb,
                pgs.ast,
                pgs.min,
                pgs.did_not_play
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE tgs.game_date = :game_date
        """)

        with self.engine.connect() as conn:
            actuals_df = pd.read_sql(actuals_query, conn, params={"game_date": game_date})

        if actuals_df.empty:
            logger.info(f"No game stats available yet for {game_date} — skipping user bet resolution")
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Build actuals lookup
        actuals_lookup: dict[int, dict[str, float | None]] = {}
        for _, row in actuals_df.iterrows():
            player_id = int(row["player_id"])
            did_not_play = row.get("did_not_play", False) or False
            minutes = row.get("min", None)
            zero_minutes = minutes is not None and (
                minutes == 0 or (pd.notna(minutes) and float(minutes) == 0)
            )

            if did_not_play or zero_minutes:
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
            odds = float(bet["odds_at_bet"]) if pd.notna(bet["odds_at_bet"]) else -110.0
            stake = float(bet["stake"]) if pd.notna(bet["stake"]) else 0.0
            bet_id = int(bet["id"])

            player_actuals = actuals_lookup.get(player_id, {})
            actual = player_actuals.get(stat_type)

            if actual is None:
                # DNP or no stats → cancelled
                status = "cancelled"
                pnl = 0.0
                results["cancelled"] += 1
            elif actual > line:
                if direction == "over":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1) if stake > 0 else 0.0
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake if stake > 0 else 0.0
                    results["lost"] += 1
            elif actual < line:
                if direction == "under":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1) if stake > 0 else 0.0
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake if stake > 0 else 0.0
                    results["lost"] += 1
            else:
                status = "push"
                pnl = 0.0
                results["push"] += 1

            updates.append({
                "bet_id": bet_id,
                "status": status,
                "actual_value": actual,
                "pnl": round(pnl, 2),
            })

        # Batch update
        update_query = text("""
            UPDATE user_bets
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

        results["resolved"] = len(updates)
        logger.info(
            f"User bets resolved for {game_date}: "
            f"{results['won']}W {results['lost']}L {results['push']}P {results['cancelled']}C"
        )
        return results

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        """
        Resolve ALL pending user bets where game results are available.
        Handles multi-day catchup.

        Args:
            exclude_today: Skip today's bets (games may not be finished).
        """
        today = date.today()

        # Get distinct pending dates
        pending_query = text("""
            SELECT DISTINCT game_date
            FROM user_bets
            WHERE status = 'pending'
            ORDER BY game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(pending_query)
            pending_dates = [row[0] for row in result]

        if not pending_dates:
            logger.info("No pending user bets to resolve")
            return {
                "dates_processed": 0,
                "dates_skipped": 0,
                "total_resolved": 0,
                "total_won": 0,
                "total_lost": 0,
                "total_push": 0,
                "total_cancelled": 0,
            }

        totals = {
            "dates_processed": 0,
            "dates_skipped": 0,
            "total_resolved": 0,
            "total_won": 0,
            "total_lost": 0,
            "total_push": 0,
            "total_cancelled": 0,
        }

        for game_date in pending_dates:
            if exclude_today and game_date >= today:
                totals["dates_skipped"] += 1
                continue

            result = self.resolve_bets_for_date(game_date)
            if result["resolved"] > 0:
                totals["dates_processed"] += 1
                totals["total_resolved"] += result["resolved"]
                totals["total_won"] += result["won"]
                totals["total_lost"] += result["lost"]
                totals["total_push"] += result["push"]
                totals["total_cancelled"] += result["cancelled"]

        logger.info(
            f"User bet resolution complete: {totals['total_resolved']} bets across "
            f"{totals['dates_processed']} dates "
            f"[{totals['total_won']}W {totals['total_lost']}L {totals['total_push']}P]"
        )
        return totals
