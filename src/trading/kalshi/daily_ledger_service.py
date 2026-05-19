"""Daily ledger rollup service for Kalshi live trading.

This service owns the live daily aggregation/upsert. It preserves the migrated
P&L, ROI, cumulative P&L, and balance-after formulas.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


class KalshiDailyLedgerService:
    """Aggregate Kalshi live orders into the daily trading ledger."""

    def __init__(self, *, engine: Any, starting_bankroll: float):
        self.engine = engine
        self.starting_bankroll = starting_bankroll

    def update_daily_log(self, game_date: date) -> None:
        """Aggregate live order results and upsert a daily log entry."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'won') as won,
                    COUNT(*) FILTER (WHERE status = 'lost') as lost,
                    COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
                    COUNT(*) FILTER (WHERE status IN ('pending', 'filled')) as pending,
                    COALESCE(SUM(total_cost) FILTER (WHERE status IN ('won', 'lost')), 0) as total_cost,
                    COALESCE(SUM(pnl), 0) as total_pnl
                FROM kalshi_live_orders
                WHERE game_date = :d
            """), {"d": game_date}).fetchone()

        if result is None or result[0] == 0:
            return

        total, won, lost, cancelled, pending, total_cost, total_pnl = result
        total_cost = float(total_cost)
        total_pnl = float(total_pnl)
        roi_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        with self.engine.connect() as conn:
            prev = conn.execute(text("""
                SELECT cumulative_pnl, balance_after
                FROM kalshi_live_trading_daily_log
                WHERE game_date < :d
                ORDER BY game_date DESC LIMIT 1
            """), {"d": game_date}).fetchone()

        prev_cum = float(prev[0]) if prev else 0.0
        prev_bal = float(prev[1]) if prev else self.starting_bankroll
        cumulative_pnl = prev_cum + total_pnl
        balance_after = prev_bal + total_pnl

        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO kalshi_live_trading_daily_log (
                    game_date, total_trades, trades_won, trades_lost,
                    trades_cancelled, trades_pending, total_cost, total_pnl,
                    roi_pct, cumulative_pnl, balance_after
                ) VALUES (
                    :d, :total, :won, :lost,
                    :cancelled, :pending, :cost, :pnl,
                    :roi, :cum_pnl, :bal
                )
                ON CONFLICT (game_date) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    trades_won = EXCLUDED.trades_won,
                    trades_lost = EXCLUDED.trades_lost,
                    trades_cancelled = EXCLUDED.trades_cancelled,
                    trades_pending = EXCLUDED.trades_pending,
                    total_cost = EXCLUDED.total_cost,
                    total_pnl = EXCLUDED.total_pnl,
                    roi_pct = EXCLUDED.roi_pct,
                    cumulative_pnl = EXCLUDED.cumulative_pnl,
                    balance_after = EXCLUDED.balance_after,
                    updated_at = now()
            """), {
                "d": game_date,
                "total": total,
                "won": won,
                "lost": lost,
                "cancelled": cancelled,
                "pending": pending,
                "cost": round(total_cost, 2),
                "pnl": round(total_pnl, 2),
                "roi": round(roi_pct, 2),
                "cum_pnl": round(cumulative_pnl, 2),
                "bal": round(balance_after, 2),
            })
            conn.commit()

        logger.info(
            f"Updated Kalshi live daily log for {game_date}: "
            f"P&L=${total_pnl:.2f}, balance=${balance_after:.2f}"
        )
