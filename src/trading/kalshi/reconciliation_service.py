"""Kalshi live order fill reconciliation service.

This service owns the ``kalshi_live_orders`` fill reconciliation seam. It
preserves the migrated SQL, status transitions, and safety behavior: never
cancel an order that already has fill data, even if the Kalshi fills API is
empty.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import text

from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee

logger = logging.getLogger(__name__)


class KalshiReconciliationService:
    """Reconcile Kalshi API fill state with ``kalshi_live_orders`` rows."""

    def __init__(self, *, engine: Any, client: Any):
        self.engine = engine
        self.client = client

    def reconcile_fills(self, target_date: date | None = None) -> dict[str, Any]:
        """Fetch fills from API and reconcile with existing live order records."""
        pending = self._fetch_reconciliation_candidates(target_date)
        if not pending:
            return {"reconciled": 0}

        resting_orders = self.client.list_orders(status="resting", limit=200)
        resting_ids = {order["order_id"] for order in resting_orders}
        logger.info(f"reconcile_fills: {len(resting_ids)} orders currently resting on Kalshi")

        reconciled = 0
        cancelled = 0
        derived = 0
        promoted = 0

        for row in pending:
            db_id = row[0]
            kalshi_order_id = row[1]
            ticker = row[2]
            row_side = row[3]
            row_total_cost = row[5]
            row_fill_count = row[6]
            row_status = row[7]
            row_fill_price = row[8]

            if not kalshi_order_id:
                continue

            if self._should_promote_without_fill_lookup(
                kalshi_order_id, row_status, row_fill_price, row_fill_count, resting_ids
            ):
                self._promote_to_filled(db_id)
                promoted += 1
                logger.info(
                    f"Promoted pending order {db_id} ({ticker}) to filled "
                    f"(fill_price={row_fill_price}, count={row_fill_count})"
                )
                continue

            fills = self.client.get_fills(order_id=kalshi_order_id)
            if not fills:
                if self._can_derive_missing_fill_price(row_status, row_total_cost, row_fill_count):
                    derived_fill_price = self._derive_fill_price(row_side, row_total_cost, row_fill_count)
                    self._update_fill_price(db_id, derived_fill_price)
                    derived += 1
                    logger.info(
                        f"Derived fill_price={derived_fill_price} for order {db_id} "
                        f"from total_cost={row_total_cost}/fill_count={row_fill_count}"
                    )
                    continue

                if row_fill_price is not None and row_fill_count and row_fill_count > 0:
                    self._promote_to_filled(db_id)
                    promoted += 1
                    logger.info(
                        f"Promoted order {db_id} ({ticker}) to filled — has fill data "
                        f"(price={row_fill_price}, count={row_fill_count}) but API returned no fills"
                    )
                elif kalshi_order_id not in resting_ids:
                    self._mark_cancelled(db_id)
                    cancelled += 1
                    logger.info(
                        f"Marked order {kalshi_order_id} as cancelled (not resting, no fills, no fill data)"
                    )
                continue

            total_filled = sum(fill.get("count", 0) for fill in fills)
            avg_price = None
            if total_filled > 0:
                weighted = sum(fill.get("yes_price", 0) * fill.get("count", 0) for fill in fills)
                avg_price = int(weighted / total_filled)

            if total_filled > 0:
                if row_side == "yes":
                    total_cost = total_filled * avg_price / 100.0
                else:
                    total_cost = total_filled * (100 - avg_price) / 100.0
                fee_paid = kalshi_taker_fee(avg_price if row_side == "yes" else 100 - avg_price) * total_filled
                self._update_from_fills(db_id, avg_price, total_filled, total_cost, fee_paid)
                reconciled += 1

        logger.info(
            f"Reconciled {reconciled} fills, {promoted} promoted, {derived} derived, {cancelled} cancelled"
            + (f" for {target_date}" if target_date else "")
        )
        return {"reconciled": reconciled, "promoted": promoted, "derived": derived, "cancelled": cancelled}

    def _fetch_reconciliation_candidates(self, target_date: date | None) -> list[Any]:
        if target_date is not None:
            where_clause = (
                "WHERE game_date = :d AND "
                "(status = 'pending' OR (status = 'filled' AND fill_price IS NULL))"
            )
            params: dict[str, Any] = {"d": target_date}
        else:
            where_clause = "WHERE status = 'pending' OR (status = 'filled' AND fill_price IS NULL)"
            params = {}

        with self.engine.connect() as conn:
            return conn.execute(text(f"""
                SELECT id, kalshi_order_id, ticker, side, contracts,
                       total_cost, fill_count, status, fill_price
                FROM kalshi_live_orders
                {where_clause}
            """), params).fetchall()

    @staticmethod
    def _should_promote_without_fill_lookup(
        kalshi_order_id: str,
        row_status: str,
        row_fill_price: int | None,
        row_fill_count: int | None,
        resting_ids: set[str],
    ) -> bool:
        return (
            row_status == "pending"
            and row_fill_price is not None
            and row_fill_count
            and row_fill_count > 0
            and kalshi_order_id not in resting_ids
        )

    @staticmethod
    def _can_derive_missing_fill_price(
        row_status: str, row_total_cost: Any, row_fill_count: int | None
    ) -> bool:
        return (
            row_status == "filled"
            and row_fill_count
            and row_fill_count > 0
            and row_total_cost is not None
            and float(row_total_cost) > 0
        )

    @staticmethod
    def _derive_fill_price(row_side: str, row_total_cost: Any, row_fill_count: int) -> int:
        no_price_cents = round(float(row_total_cost) / row_fill_count * 100)
        return 100 - no_price_cents if row_side == "no" else no_price_cents

    def _promote_to_filled(self, db_id: int) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'filled', filled_at = COALESCE(filled_at, now())
                WHERE id = :id
            """), {"id": db_id})
            conn.commit()

    def _update_fill_price(self, db_id: int, fill_price: int) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET fill_price = :price
                WHERE id = :id
            """), {"price": fill_price, "id": db_id})
            conn.commit()

    def _mark_cancelled(self, db_id: int) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'cancelled', pnl = 0.0, resolved_at = now()
                WHERE id = :id
            """), {"id": db_id})
            conn.commit()

    def _update_from_fills(
        self,
        db_id: int,
        avg_price: int,
        total_filled: int,
        total_cost: float,
        fee_paid: float,
    ) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'filled',
                    fill_price = :price,
                    fill_count = :count,
                    total_cost = :cost,
                    fee_paid = :fee,
                    filled_at = now()
                WHERE id = :id
            """), {
                "price": avg_price,
                "count": total_filled,
                "cost": round(total_cost, 2),
                "fee": round(fee_paid, 4),
                "id": db_id,
            })
            conn.commit()
