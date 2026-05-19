"""Kalshi live order repricing service.

This service owns the stale resting-order reprice seam. It preserves the
migrated cancel-and-replace behavior, sweep limits, edge-retention checks,
replacement order price buffer, and DB row shapes.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Callable

from sqlalchemy import text

from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee

logger = logging.getLogger(__name__)


class KalshiRepricingService:
    """Cancel and replace resting Kalshi orders when the orderbook price moved."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any,
        get_best_available_price: Callable[[str, str, int], int | None],
        sweep_max_cents: int = 10,
        sweep_edge_retention: float = 0.50,
    ):
        self.engine = engine
        self.client = client
        self.get_best_available_price = get_best_available_price
        self.sweep_max_cents = sweep_max_cents
        self.sweep_edge_retention = sweep_edge_retention

    def reprice_stale_orders(self) -> int:
        """Detect and reprice resting orders whose market price has moved."""
        resting = self.client.list_orders(status="resting")
        if not resting:
            logger.info("REPRICE: No resting orders found")
            return 0

        logger.info(f"REPRICE: Found {len(resting)} resting orders to evaluate")
        order_ids = [order.get("order_id", order.get("id", "")) for order in resting]
        if not order_ids:
            return 0

        db_orders = self._fetch_pending_db_orders(order_ids)
        if not db_orders:
            logger.info("REPRICE: No resting orders matched pending DB records")
            return 0

        repriced = 0
        for api_order in resting:
            api_order_id = api_order.get("order_id", api_order.get("id", ""))
            db_row = db_orders.get(api_order_id)
            if db_row is None:
                continue

            result = self._maybe_reprice_one(api_order_id, db_row)
            if result:
                repriced += 1

        logger.info(f"REPRICE: Done — {repriced}/{len(resting)} orders repriced")
        return repriced

    def _fetch_pending_db_orders(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT kalshi_order_id, ticker, side, model_prob, fee_adjusted_edge,
                       yes_price, game_date, sport, player_id, player_name,
                       stat_type, line, edge, kalshi_implied, contracts,
                       game_start_time
                FROM kalshi_live_orders
                WHERE kalshi_order_id = ANY(:ids)
                  AND status = 'pending'
            """), {"ids": order_ids}).fetchall()

        db_orders: dict[str, dict[str, Any]] = {}
        for row in rows:
            row_dict = dict(row._mapping)
            db_orders[row_dict["kalshi_order_id"]] = row_dict
        return db_orders

    def _maybe_reprice_one(self, api_order_id: str, db_row: dict[str, Any]) -> bool:
        ticker = db_row["ticker"]
        side = db_row["side"]
        resting_price = db_row["yes_price"]
        model_prob = float(db_row["model_prob"])
        original_edge = float(db_row["fee_adjusted_edge"])

        actual_price = self.get_best_available_price(ticker, side, resting_price)
        if actual_price is None:
            logger.debug(f"REPRICE: Orderbook unavailable for {ticker} — skipping")
            return False

        delta = abs(actual_price - resting_price)
        if delta == 0:
            return False

        if delta > self.sweep_max_cents:
            logger.info(
                f"REPRICE SKIP [{ticker}]: price moved {delta}c "
                f"({resting_price}c -> {actual_price}c) exceeds max {self.sweep_max_cents}c"
            )
            return False

        recalc_edge = fee_adjusted_edge(
            model_prob,
            actual_price,
            is_yes=(side == "yes"),
            is_maker=False,
        )
        edge_floor = original_edge * self.sweep_edge_retention
        if recalc_edge < edge_floor:
            logger.info(
                f"REPRICE SKIP [{ticker}]: edge at {actual_price}c = {recalc_edge:.1%} "
                f"below {self.sweep_edge_retention:.0%} floor {edge_floor:.1%}"
            )
            return False

        cancel_result = self.client.cancel_order(api_order_id)
        if cancel_result is None:
            logger.warning(f"REPRICE: Failed to cancel {api_order_id} for {ticker}")
            return False

        new_order = self._place_replacement_order(db_row, actual_price)
        if new_order is None:
            logger.error(f"REPRICE FAILED [{ticker}]: cancelled {api_order_id} but replacement failed!")
            self._mark_old_order_cancelled(api_order_id)
            return False

        new_order_data = new_order.get("order", new_order)
        new_order_id = new_order_data.get("order_id", new_order_data.get("id", ""))
        record = self._replacement_record(db_row, new_order_data, new_order_id, actual_price, recalc_edge)
        self._replace_db_order(api_order_id, record)

        logger.info(
            f"REPRICE OK [{ticker}]: {resting_price}c -> {actual_price}c, "
            f"edge {original_edge:.1%} -> {recalc_edge:.1%}, "
            f"old={api_order_id} new={new_order_id} status={record['status']}"
        )
        return True

    def _place_replacement_order(self, db_row: dict[str, Any], actual_price: int) -> dict[str, Any] | None:
        sweep_buffer = 3
        side = db_row["side"]
        if side == "yes":
            return self.client.create_order(
                ticker=db_row["ticker"],
                action="buy",
                side="yes",
                order_type="market",
                count=int(db_row["contracts"]),
                yes_price=min(actual_price + sweep_buffer, 99),
            )
        return self.client.create_order(
            ticker=db_row["ticker"],
            action="buy",
            side="no",
            order_type="market",
            count=int(db_row["contracts"]),
            no_price=min(100 - actual_price + sweep_buffer, 99),
        )

    def _replacement_record(
        self,
        db_row: dict[str, Any],
        new_order_data: dict[str, Any],
        new_order_id: str,
        actual_price: int,
        recalc_edge: float,
    ) -> dict[str, Any]:
        side = db_row["side"]
        new_status = new_order_data.get("status", "unknown")
        new_fill_price = None
        new_fill_count = 0
        new_total_cost = 0.0
        new_fee_paid = 0.0

        if new_status in ("executed", "filled"):
            new_fill_price = new_order_data.get("yes_price") or new_order_data.get("avg_price") or actual_price
            new_fill_count = new_order_data.get("count", int(db_row["contracts"]))
            price_per = actual_price / 100.0 if side == "yes" else (100 - actual_price) / 100.0
            new_total_cost = new_fill_count * price_per
            new_fee_paid = kalshi_taker_fee(actual_price if side == "yes" else 100 - actual_price) * new_fill_count
            record_status = "filled"
        elif new_status == "resting":
            record_status = "pending"
        else:
            record_status = "pending"

        return {
            "game_date": db_row["game_date"],
            "ticker": db_row["ticker"],
            "sport": db_row["sport"],
            "player_id": db_row.get("player_id"),
            "player_name": db_row.get("player_name"),
            "stat_type": db_row["stat_type"],
            "line": db_row["line"],
            "side": side,
            "contracts": int(db_row["contracts"]),
            "order_id": new_order_id,
            "fill_price": new_fill_price,
            "fill_count": new_fill_count,
            "total_cost": round(new_total_cost, 2),
            "fee_paid": round(new_fee_paid, 4),
            "model_prob": float(db_row["model_prob"]),
            "kalshi_implied": db_row["kalshi_implied"],
            "edge": db_row["edge"],
            "fee_adjusted_edge": recalc_edge,
            "status": record_status,
            "filled_at": datetime.now(UTC).replace(tzinfo=None) if record_status == "filled" else None,
            "game_start_time": db_row.get("game_start_time"),
        }

    def _mark_old_order_cancelled(self, api_order_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'cancelled'
                WHERE kalshi_order_id = :oid
            """), {"oid": api_order_id})

    def _replace_db_order(self, api_order_id: str, record: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'cancelled'
                WHERE kalshi_order_id = :oid
            """), {"oid": api_order_id})

            conn.execute(text("""
                INSERT INTO kalshi_live_orders (
                    game_date, ticker, sport, player_id, player_name,
                    stat_type, line, side, order_type, contracts,
                    kalshi_order_id, fill_price, fill_count, total_cost, fee_paid,
                    model_prob, kalshi_implied, edge, fee_adjusted_edge,
                    status, filled_at, game_start_time
                ) VALUES (
                    :game_date, :ticker, :sport, :player_id, :player_name,
                    :stat_type, :line, :side, 'market', :contracts,
                    :order_id, :fill_price, :fill_count, :total_cost, :fee_paid,
                    :model_prob, :kalshi_implied, :edge, :fee_adjusted_edge,
                    :status, :filled_at, :game_start_time
                )
            """), record)
