"""Kalshi live order settlement service.

This service owns the filled-order settlement seam around ``kalshi_live_orders``.
It preserves the migrated PnL formulas, actual-stat callback contract,
daily-log update callback, streak update callback, and resolution-alert
behavior.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


class KalshiSettlementService:
    """Resolve filled Kalshi live orders after actual player stats are available."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any,
        fetch_actuals: Callable[[date, pd.DataFrame, str], dict[tuple[int, str], float | None]],
        send_resolution_alert: Callable[[pd.Series, str, float | None, float, float], None],
        update_daily_log: Callable[[date], None],
        get_consecutive_losses: Callable[[], int],
        update_streak: Callable[[int], None],
    ):
        self.engine = engine
        self.client = client
        self.fetch_actuals = fetch_actuals
        self.send_resolution_alert = send_resolution_alert
        self.update_daily_log = update_daily_log
        self.get_consecutive_losses = get_consecutive_losses
        self.update_streak = update_streak

    def resolve_settled(self) -> dict[str, Any]:
        """Check filled live orders and resolve them to won/lost/cancelled."""
        orders = self._fetch_filled_orders()
        if orders.empty:
            logger.info("No filled Kalshi live orders to resolve")
            return {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}

        totals = {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}

        for game_date in orders["game_date"].unique():
            if game_date >= date.today():
                continue

            date_orders = orders[orders["game_date"] == game_date]
            sport = date_orders.iloc[0]["sport"]
            actuals = self.fetch_actuals(game_date, date_orders, sport)

            for _, order in date_orders.iterrows():
                resolved = self._resolve_order(order, actuals)
                if resolved is None:
                    continue

                status, actual, pnl = resolved
                self._update_order(int(order["id"]), status, actual, pnl)
                totals["resolved"] += 1

                if status == "won":
                    totals["won"] += 1
                elif status == "lost":
                    totals["lost"] += 1
                elif status == "cancelled":
                    totals["cancelled"] += 1

                if status in ("won", "lost"):
                    balance_data = self.client.get_balance()
                    balance = (balance_data.get("balance", 0) / 100.0) if balance_data else 0
                    self.send_resolution_alert(order, status, actual, pnl, balance)

            self.update_daily_log(game_date)

        streak = self.get_consecutive_losses()
        self.update_streak(streak)

        logger.info(
            f"Resolved {totals['resolved']} Kalshi live orders: "
            f"{totals['won']}W {totals['lost']}L {totals['cancelled']}C"
        )
        return totals

    def _fetch_filled_orders(self) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(text("""
                SELECT id, game_date, ticker, player_id, player_name,
                       stat_type, line, side, fill_price, fill_count,
                       total_cost, fee_paid, sport
                FROM kalshi_live_orders
                WHERE status = 'filled'
                ORDER BY game_date ASC
            """), conn)

    def _resolve_order(
        self,
        order: pd.Series,
        actuals: dict[tuple[int, str], float | None],
    ) -> tuple[str, float | None, float] | None:
        player_id = int(order["player_id"]) if pd.notna(order["player_id"]) else None
        stat_type = order["stat_type"]
        line = float(order["line"])
        side = order["side"]

        if pd.isna(order["fill_price"]) or pd.isna(order["fill_count"]):
            logger.warning(
                f"SKIP RESOLUTION: order {int(order['id'])} ({order.get('ticker', '?')}) "
                f"has null fill_price={order['fill_price']} or "
                f"fill_count={order['fill_count']} — run reconcile_fills() first"
            )
            return None

        fill_price = int(order["fill_price"])
        fill_count = int(order["fill_count"])
        fee = float(order["fee_paid"]) if pd.notna(order["fee_paid"]) else 0.0
        actual = actuals.get((player_id, stat_type)) if player_id else None

        if actual is None:
            return "cancelled", actual, 0.0

        yes_wins = actual >= line

        if side == "yes":
            if yes_wins:
                return "won", actual, fill_count * (100 - fill_price) / 100.0 - fee
            return "lost", actual, -(fill_count * fill_price / 100.0)

        if not yes_wins:
            return "won", actual, fill_count * fill_price / 100.0 - fee
        return "lost", actual, -(fill_count * (100 - fill_price) / 100.0)

    def _update_order(self, order_id: int, status: str, actual: float | None, pnl: float) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = :status,
                    actual_value = :actual,
                    pnl = :pnl,
                    resolved_at = now()
                WHERE id = :id
            """), {
                "status": status,
                "actual": actual,
                "pnl": round(pnl, 2),
                "id": order_id,
            })
            conn.commit()
