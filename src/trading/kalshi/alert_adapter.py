"""Kalshi alert/event adapter.

Keeps Discord-specific payload formatting and alert routing out of live-money
services. Core services/facades pass domain events here; this adapter converts
those events to the existing Discord alert functions.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.trading.kalshi.events import (
    CircuitBreakerTripped,
    HighEdgeMarketsFound,
    KalshiEvent,
    OrderFilled,
    OrderResolved,
    StaleOrdersReviewNeeded,
    TradeApprovalNeeded,
    TradeApprovalReminder,
    TradeExecutionFailed,
    TradePlaced,
)

logger = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")

TradeAlertSender = Callable[..., bool]
TradeFailureSender = Callable[..., bool]
MarketAlertSender = Callable[..., bool]


def _default_trade_alert_sender(*args: Any, **kwargs: Any) -> bool:
    from src.discord_bot.alerts import send_kalshi_trade_alert_sync

    return send_kalshi_trade_alert_sync(*args, **kwargs)


def _default_trade_failure_sender(*args: Any, **kwargs: Any) -> bool:
    from src.discord_bot.alerts import send_kalshi_trade_failure_alert_sync

    return send_kalshi_trade_failure_alert_sync(*args, **kwargs)


def _default_market_alert_sender(*args: Any, **kwargs: Any) -> bool:
    from src.discord_bot.alerts import send_kalshi_alert_sync

    return send_kalshi_alert_sync(*args, **kwargs)


class KalshiAlertAdapter:
    """Convert Kalshi domain events into existing Discord alert calls."""

    def __init__(
        self,
        *,
        engine: Any | None = None,
        trade_alert_sender: TradeAlertSender | None = None,
        trade_failure_sender: TradeFailureSender | None = None,
        market_alert_sender: MarketAlertSender | None = None,
    ) -> None:
        self.engine = engine
        self.trade_alert_sender = trade_alert_sender or _default_trade_alert_sender
        self.trade_failure_sender = trade_failure_sender or _default_trade_failure_sender
        self.market_alert_sender = market_alert_sender or _default_market_alert_sender

    def send(self, event: KalshiEvent) -> bool:
        """Dispatch one Kalshi event. Alert failures are logged and non-fatal."""
        try:
            if isinstance(event, TradePlaced):
                return self._send_trade_placed(event)
            if isinstance(event, OrderResolved):
                return self._send_order_resolved(event)
            if isinstance(event, OrderFilled):
                return self._send_order_filled(event)
            if isinstance(event, CircuitBreakerTripped):
                return self._send_circuit_breaker(event)
            if isinstance(event, TradeApprovalNeeded):
                return self._send_trade_approval_needed(event)
            if isinstance(event, TradeApprovalReminder):
                return self._send_trade_approval_reminder(event)
            if isinstance(event, TradeExecutionFailed):
                return self._send_trade_execution_failed(event)
            if isinstance(event, StaleOrdersReviewNeeded):
                return self._send_stale_orders_review_needed(event)
            if isinstance(event, HighEdgeMarketsFound):
                return self._send_high_edge_markets(event)
        except Exception as exc:
            logger.warning("Kalshi alert dispatch failed for %s: %s", type(event).__name__, exc)
            return False

        logger.warning("Unhandled Kalshi alert event: %s", type(event).__name__)
        return False

    def _send_trade_placed(self, event: TradePlaced) -> bool:
        trade = event.trade
        payload: dict[str, Any] = {
            "player_name": trade.get("player_name", "Unknown"),
            "stat_type": trade["stat_type"],
            "line": trade["line"],
            "side": trade["side"],
            "fill_price": event.fill_price or trade["yes_price"],
            "contracts": event.contracts,
            "total_cost": event.total_cost,
            "fee_adjusted_edge": trade["fee_adjusted_edge"],
            "balance_after": event.balance,
        }
        if event.swept_from is not None and event.swept_to is not None and event.swept_from != event.swept_to:
            edge = event.recalc_edge if event.recalc_edge is not None else 0
            payload["swept"] = f"{event.swept_from}c -> {event.swept_to}c (edge: {edge:.1%})"
        return self.trade_alert_sender("placed", payload, **self._channel_kwargs(event.channel_id))

    def _send_order_resolved(self, event: OrderResolved) -> bool:
        order = event.order
        payload = {
            "player_name": _get(order, "player_name", "Unknown"),
            "stat_type": _get(order, "stat_type"),
            "line": float(_get(order, "line")),
            "side": _get(order, "side"),
            "actual_value": event.actual,
            "pnl": event.pnl,
            "status": event.status,
            "balance_after": event.balance,
        }
        return self.trade_alert_sender("resolved", payload, **self._channel_kwargs(event.channel_id))

    def _send_order_filled(self, event: OrderFilled) -> bool:
        payload = {
            "player_name": event.player_name or "Unknown",
            "stat_type": event.stat_type or "",
            "side": event.side or "",
            "fill_price": event.fill_price,
            "contracts": event.contracts,
        }
        return self.trade_alert_sender("filled", payload, **self._channel_kwargs(event.channel_id))

    def _send_circuit_breaker(self, event: CircuitBreakerTripped) -> bool:
        if event.dedupe and self._already_sent_circuit_alert_today():
            logger.info("Circuit breaker alert suppressed (already sent today): %s", event.reason)
            return False
        if event.dedupe:
            self._mark_circuit_alert_sent()
        return self.trade_alert_sender(
            "circuit_breaker",
            {"reason": event.reason, "balance": event.balance, "action": event.action},
            **self._channel_kwargs(event.channel_id),
        )

    def _send_trade_approval_needed(self, event: TradeApprovalNeeded) -> bool:
        payload = _queue_payload(event.trades, event.sport, already_pending=event.already_pending)
        return self.trade_alert_sender("approval_needed", payload, **self._channel_kwargs(event.channel_id))

    def _send_trade_approval_reminder(self, event: TradeApprovalReminder) -> bool:
        payload = _queue_payload(event.pending_trades, event.sport)
        return self.trade_alert_sender("approval_reminder", payload, **self._channel_kwargs(event.channel_id))

    def _send_trade_execution_failed(self, event: TradeExecutionFailed) -> bool:
        return self.trade_failure_sender(event.trade, event.error_msg, **self._channel_kwargs(event.channel_id))

    def _send_stale_orders_review_needed(self, event: StaleOrdersReviewNeeded) -> bool:
        lines = []
        for order in event.orders:
            cost = order.get("total_cost")
            cost_str = f"${float(cost):.2f}" if cost is not None else "?"
            lines.append(
                f"  {order.get('player_name') or order.get('ticker')} | {order.get('stat_type') or ''} "
                f"{order.get('side') or ''} | {order.get('contracts') or '?'} contracts | {cost_str}"
            )
        msg = "\n".join(lines)
        return self.trade_alert_sender(
            "circuit_breaker",
            {
                "reason": f"{len(event.orders)} stale Kalshi order(s) need review — game has started.\n"
                f"Approve cancellation on the dashboard (Bot Tracker -> Stale Orders).\n\n{msg}",
                "action": "Review stale orders",
                "balance": 0,
            },
            **self._channel_kwargs(event.channel_id),
        )

    def _send_high_edge_markets(self, event: HighEdgeMarketsFound) -> bool:
        return self.market_alert_sender(
            event.markets,
            event.target_date,
            **self._channel_kwargs(event.channel_id),
            sport=event.sport,
        )

    def _already_sent_circuit_alert_today(self) -> bool:
        if self.engine is None:
            return False
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT last_circuit_alert_at
                    FROM kalshi_live_trading_config
                    WHERE id = 1
                """)).fetchone()
            last_alert = row[0] if row else None
            if last_alert is None:
                return False
            if isinstance(last_alert, str):
                last_alert = datetime.fromisoformat(last_alert)
            if last_alert.tzinfo is None:
                last_alert = last_alert.replace(tzinfo=UTC)
            return last_alert.astimezone(_ET).date() >= datetime.now(_ET).date()
        except Exception as exc:
            logger.warning("Circuit alert dedup check failed, sending alert anyway: %s", exc)
            return False

    def _mark_circuit_alert_sent(self) -> None:
        if self.engine is None:
            return
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE kalshi_live_trading_config
                    SET last_circuit_alert_at = now(), last_updated = now()
                    WHERE id = 1
                """))
                conn.commit()
        except Exception as exc:
            logger.warning("Failed to update last_circuit_alert_at: %s", exc)

    @staticmethod
    def _channel_kwargs(channel_id: str | None) -> dict[str, str]:
        return {"channel_id": channel_id} if channel_id else {}


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if hasattr(obj, "get"):
        return obj.get(key, default)
    try:
        return obj[key]
    except Exception:
        return getattr(obj, key, default)


def _queue_payload(trades: list[dict[str, Any]], sport: str, already_pending: int = 0) -> dict[str, Any]:
    total_exposure = sum(t.get("expected_cost", 0) for t in trades)
    edges = [t.get("fee_adjusted_edge", 0) for t in trades if t.get("fee_adjusted_edge")]
    edge_range = f"{min(edges):.0%}-{max(edges):.0%}" if edges else "N/A"
    payload = {
        "sport": sport.upper(),
        "count": len(trades),
        "total_exposure": total_exposure,
        "edge_range": edge_range,
        "trades": [
            {
                "player_name": t.get("player_name", "Unknown"),
                "stat_type": t.get("stat_type", ""),
                "side": t.get("side", "yes"),
                "contracts": t.get("contracts", 0),
                "expected_cost": t.get("expected_cost", 0),
                "fee_adjusted_edge": t.get("fee_adjusted_edge", 0),
            }
            for t in trades[:10]
        ],
    }
    if already_pending:
        payload["already_pending"] = already_pending
    return payload
