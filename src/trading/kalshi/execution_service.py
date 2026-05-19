"""Kalshi live order execution service.

This service owns live order placement and live-order recording. It intentionally
preserves the migrated SQL shapes, result dicts, sweep-buffer behavior,
exposure-cap checks, and alert callback contract.
"""

from __future__ import annotations

import logging
import math
import os
from datetime import UTC, date, datetime
from typing import Any, Callable

from sqlalchemy import text

from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


class KalshiExecutionService:
    """Execute approved Kalshi trade intents and record live orders."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any,
        get_best_available_price: Callable[[str, str, int], int | None],
        calculate_kelly_contracts: Callable[[float, int, str, float], int],
        send_trade_placed_alert: Callable[..., None],
        daily_exposure_pct: float | None = None,
        min_daily_exposure: float | None = None,
        max_daily_exposure: float | None = None,
        sweep_max_cents: int = 10,
        sweep_edge_retention: float = 0.50,
    ):
        self.engine = engine
        self.client = client
        self.get_best_available_price = get_best_available_price
        self.calculate_kelly_contracts = calculate_kelly_contracts
        self.send_trade_placed_alert = send_trade_placed_alert
        self.daily_exposure_pct = (
            daily_exposure_pct
            if daily_exposure_pct is not None
            else _env_float("KALSHI_DAILY_EXPOSURE_PCT", 0.60)
        )
        self.min_daily_exposure = (
            min_daily_exposure
            if min_daily_exposure is not None
            else _env_float("KALSHI_MIN_DAILY_EXPOSURE", 80.0)
        )
        self.max_daily_exposure = (
            max_daily_exposure
            if max_daily_exposure is not None
            else _env_float("KALSHI_MAX_DAILY_EXPOSURE", 500.0)
        )
        self.sweep_max_cents = sweep_max_cents
        self.sweep_edge_retention = sweep_edge_retention

    def execute_trades(self, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Place real taker orders through the Kalshi API."""
        if not trades:
            return []

        results: list[dict[str, Any]] = []
        effective_cap, running_exposure = self._execution_cap(trades)

        for trade in trades:
            balance_data = self.client.get_balance()
            if balance_data is None:
                logger.error("Balance check failed — aborting remaining trades")
                break
            balance = balance_data.get("balance", 0) / 100.0

            if balance < trade["expected_cost"]:
                logger.warning(
                    f"Insufficient balance (${balance:.2f}) for {trade['ticker']} "
                    f"(cost: ${trade['expected_cost']:.2f}) — skipping"
                )
                continue

            trade = self._clamp_trade_to_cap(trade, running_exposure, effective_cap)
            if trade is None:
                if running_exposure >= effective_cap:
                    break
                continue

            adjusted = self._apply_sweep_check(trade, balance, running_exposure, effective_cap)
            if adjusted is None:
                continue
            trade, swept_from, swept_to, recalc_edge_val = adjusted

            order_result = self._place_market_order(trade)
            if order_result is None:
                logger.error(f"Order failed for {trade['ticker']} — no API response")
                continue

            order = order_result.get("order", order_result)
            order_id = order.get("order_id", order.get("id", ""))
            status = order.get("status", "unknown")
            fill_price, fill_count, total_cost, fee_paid = self._fill_details(trade, order, status)
            db_status = "filled" if status in ("executed", "filled") else "pending"

            self.record_order(trade, order_id, db_status, fill_price, fill_count, total_cost, fee_paid)
            running_exposure += total_cost

            new_balance_data = self.client.get_balance()
            new_balance = (new_balance_data.get("balance", 0) / 100.0) if new_balance_data else balance - total_cost
            self.send_trade_placed_alert(
                trade,
                fill_price,
                fill_count,
                total_cost,
                new_balance,
                swept_from=swept_from,
                swept_to=swept_to,
                recalc_edge=recalc_edge_val,
            )

            results.append({
                "ticker": trade["ticker"],
                "side": trade["side"],
                "contracts": fill_count,
                "fill_price": fill_price,
                "total_cost": total_cost,
                "order_id": order_id,
                "status": db_status,
            })

            logger.info(
                f"LIVE TRADE: {trade['player_name']} {trade['stat_type']} "
                f"{trade['side'].upper()} @ {fill_price}c x{fill_count} "
                f"= ${total_cost:.2f} (edge: {trade['fee_adjusted_edge']:.1%})"
            )

        logger.info(f"Executed {len(results)}/{len(trades)} live trades")
        return results

    def record_order(
        self,
        trade: dict[str, Any],
        order_id: str,
        status: str,
        fill_price: int | None,
        fill_count: int,
        total_cost: float,
        fee_paid: float,
    ) -> None:
        """Insert a live order record into the database."""
        with self.engine.connect() as conn:
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
            """), {
                "game_date": trade["game_date"],
                "ticker": trade["ticker"],
                "sport": trade["sport"],
                "player_id": trade.get("player_id"),
                "player_name": trade.get("player_name"),
                "stat_type": trade["stat_type"],
                "line": trade["line"],
                "side": trade["side"],
                "contracts": trade["contracts"],
                "order_id": order_id,
                "fill_price": fill_price,
                "fill_count": fill_count,
                "total_cost": round(total_cost, 2),
                "fee_paid": round(fee_paid, 4),
                "model_prob": trade["model_prob"],
                "kalshi_implied": trade["kalshi_implied"],
                "edge": trade["edge"],
                "fee_adjusted_edge": trade["fee_adjusted_edge"],
                "status": status,
                "filled_at": datetime.now(UTC).replace(tzinfo=None) if status == "filled" else None,
                "game_start_time": trade.get("game_start_time"),
            })
            conn.commit()

    def _execution_cap(self, trades: list[dict[str, Any]]) -> tuple[float, float]:
        balance_data_for_cap = self.client.get_balance()
        bankroll_for_cap = (balance_data_for_cap.get("balance", 0) / 100.0) if balance_data_for_cap else 0
        dynamic_cap = bankroll_for_cap * self.daily_exposure_pct
        effective_cap = max(self.min_daily_exposure, min(dynamic_cap, self.max_daily_exposure))

        target_date = trades[0]["game_date"] if trades else date.today()
        with self.engine.connect() as conn:
            existing_exposure = conn.execute(text("""
                SELECT COALESCE(SUM(total_cost), 0)
                FROM kalshi_live_orders
                WHERE game_date = :d AND status != 'cancelled'
            """), {"d": target_date}).scalar()
        running_exposure = float(existing_exposure or 0)
        logger.info(
            f"Execute exposure cap: ${effective_cap:.2f} | already committed: ${running_exposure:.2f} | "
            f"headroom: ${effective_cap - running_exposure:.2f}"
        )
        return effective_cap, running_exposure

    def _clamp_trade_to_cap(
        self, trade: dict[str, Any], running_exposure: float, effective_cap: float
    ) -> dict[str, Any] | None:
        cap_remaining = effective_cap - running_exposure
        if trade["expected_cost"] <= cap_remaining:
            return trade
        if cap_remaining <= 0:
            logger.warning(
                f"Daily cap exhausted (${running_exposure:.2f}/${effective_cap:.2f}) "
                f"— skipping {trade['ticker']} and remaining trades"
            )
            return None
        cost_per = trade["expected_cost"] / trade["contracts"]
        new_contracts = int(math.floor(cap_remaining / cost_per))
        if new_contracts <= 0:
            logger.warning(f"Cannot fit {trade['ticker']} in remaining cap — skipping")
            return None
        clamped = {**trade, "contracts": new_contracts, "expected_cost": round(new_contracts * cost_per, 2)}
        logger.info(
            f"CAP CLAMP [{clamped['ticker']}]: reduced to {new_contracts} contracts "
            f"(remaining cap: ${cap_remaining:.2f})"
        )
        return clamped

    def _apply_sweep_check(
        self,
        trade: dict[str, Any],
        balance: float,
        running_exposure: float,
        effective_cap: float,
    ) -> tuple[dict[str, Any], int | None, int | None, float | None] | None:
        snapshot_price = trade["yes_price"]
        actual_price = self.get_best_available_price(trade["ticker"], trade["side"], snapshot_price)
        swept_from: int | None = None
        swept_to: int | None = None
        recalc_edge_val: float | None = None

        if actual_price is None:
            logger.warning(f"Orderbook unavailable for {trade['ticker']} — using snapshot+buffer fallback")
            return trade, swept_from, swept_to, recalc_edge_val

        if actual_price == snapshot_price:
            return trade, swept_from, swept_to, recalc_edge_val

        price_delta = abs(actual_price - snapshot_price)
        if price_delta > self.sweep_max_cents:
            logger.warning(
                f"SWEEP REJECTED [{trade['ticker']}]: price moved {price_delta}c "
                f"({snapshot_price}c -> {actual_price}c) exceeds max {self.sweep_max_cents}c — skipping"
            )
            return None

        recalc_edge_val = fee_adjusted_edge(
            trade["model_prob"], actual_price,
            is_yes=(trade["side"] == "yes"), is_maker=False,
        )
        original_edge = trade["fee_adjusted_edge"]
        edge_floor = original_edge * self.sweep_edge_retention

        if recalc_edge_val < edge_floor:
            logger.warning(
                f"SWEEP REJECTED [{trade['ticker']}]: edge at {actual_price}c = "
                f"{recalc_edge_val:.1%} below {self.sweep_edge_retention:.0%} retention "
                f"floor {edge_floor:.1%} (original: {original_edge:.1%}) — skipping"
            )
            return None

        logger.info(
            f"SWEEP ACCEPTED [{trade['ticker']}]: {snapshot_price}c -> {actual_price}c, "
            f"recalc edge {recalc_edge_val:.1%} (was {original_edge:.1%})"
        )
        swept_from = snapshot_price
        swept_to = actual_price
        new_contracts = self.calculate_kelly_contracts(
            trade["model_prob"], actual_price, trade["side"], balance
        )
        price_per = actual_price / 100.0 if trade["side"] == "yes" else (100 - actual_price) / 100.0
        new_expected_cost = new_contracts * price_per
        new_expected_fee = kalshi_taker_fee(
            actual_price if trade["side"] == "yes" else 100 - actual_price
        ) * new_contracts

        if new_expected_cost > balance:
            new_contracts = max(0, int(balance / price_per))
            new_expected_cost = new_contracts * price_per
            new_expected_fee = kalshi_taker_fee(
                actual_price if trade["side"] == "yes" else 100 - actual_price
            ) * new_contracts

        cap_remaining = effective_cap - running_exposure
        if new_expected_cost > cap_remaining:
            if cap_remaining <= price_per:
                logger.warning(
                    f"SWEEP RESIZE: daily cap exhausted (${running_exposure:.2f}/${effective_cap:.2f}) "
                    f"for {trade['ticker']} — skipping"
                )
                return None
            new_contracts = int(math.floor(cap_remaining / price_per))
            new_expected_cost = new_contracts * price_per
            new_expected_fee = kalshi_taker_fee(
                actual_price if trade["side"] == "yes" else 100 - actual_price
            ) * new_contracts
            logger.info(
                f"SWEEP CAP CLAMP [{trade['ticker']}]: capped to {new_contracts} contracts "
                f"(remaining cap: ${cap_remaining:.2f})"
            )

        if new_contracts == 0:
            logger.warning(
                f"SWEEP RESIZE: 0 contracts after resize for {trade['ticker']} — skipping"
            )
            return None

        old_contracts = trade["contracts"]
        logger.info(
            f"SWEEP RESIZE [{trade['ticker']}]: {old_contracts} -> {new_contracts} contracts "
            f"(price: {snapshot_price}c -> {actual_price}c, edge: {recalc_edge_val:.1%})"
        )
        return {
            **trade,
            "yes_price": actual_price,
            "fee_adjusted_edge": recalc_edge_val,
            "contracts": new_contracts,
            "expected_cost": new_expected_cost,
            "expected_fee": new_expected_fee,
        }, swept_from, swept_to, recalc_edge_val

    def _place_market_order(self, trade: dict[str, Any]) -> dict[str, Any] | None:
        sweep_buffer = 3
        yes_px = trade["yes_price"]
        if trade["side"] == "yes":
            return self.client.create_order(
                ticker=trade["ticker"],
                action="buy",
                side="yes",
                order_type="market",
                count=trade["contracts"],
                yes_price=min(yes_px + sweep_buffer, 99),
            )
        return self.client.create_order(
            ticker=trade["ticker"],
            action="buy",
            side="no",
            order_type="market",
            count=trade["contracts"],
            no_price=min(100 - yes_px + sweep_buffer, 99),
        )

    def _fill_details(
        self, trade: dict[str, Any], order: dict[str, Any], status: str
    ) -> tuple[int | None, int, float, float]:
        if status in ("executed", "filled"):
            fill_price = order.get("yes_price") or order.get("avg_price")
            if not fill_price:
                fill_price = trade["yes_price"]
            fill_count = order.get("count", trade["contracts"])
            if fill_price:
                if trade["side"] == "yes":
                    total_cost = fill_count * fill_price / 100.0
                else:
                    total_cost = fill_count * (100 - fill_price) / 100.0
                fee_paid = kalshi_taker_fee(fill_price if trade["side"] == "yes" else 100 - fill_price) * fill_count
            else:
                total_cost = trade["expected_cost"]
                fee_paid = trade["expected_fee"]
            return fill_price, fill_count, total_cost, fee_paid

        return trade["yes_price"], trade["contracts"], trade["expected_cost"], trade["expected_fee"]
