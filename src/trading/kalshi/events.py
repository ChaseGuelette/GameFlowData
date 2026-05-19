"""Domain events for Kalshi live-trading notifications.

These dataclasses describe what happened in the trading lifecycle without
knowing how Discord formats or sends the alert.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class TradePlaced:
    trade: dict[str, Any]
    fill_price: int | None
    contracts: int
    total_cost: float
    balance: float
    swept_from: int | None = None
    swept_to: int | None = None
    recalc_edge: float | None = None
    channel_id: str | None = None


@dataclass(frozen=True)
class OrderResolved:
    order: Any
    status: str
    actual: float | None
    pnl: float
    balance: float
    channel_id: str | None = None


@dataclass(frozen=True)
class OrderFilled:
    player_name: str
    stat_type: str
    side: str
    fill_price: int | None
    contracts: int
    channel_id: str | None = None


@dataclass(frozen=True)
class CircuitBreakerTripped:
    reason: str
    balance: float
    action: str
    channel_id: str | None = None
    dedupe: bool = False


@dataclass(frozen=True)
class TradeApprovalNeeded:
    trades: list[dict[str, Any]]
    sport: str
    already_pending: int = 0
    channel_id: str | None = None


@dataclass(frozen=True)
class TradeApprovalReminder:
    pending_trades: list[dict[str, Any]]
    sport: str
    channel_id: str | None = None


@dataclass(frozen=True)
class TradeExecutionFailed:
    trade: dict[str, Any]
    error_msg: str
    channel_id: str | None = None


@dataclass(frozen=True)
class StaleOrdersReviewNeeded:
    orders: list[dict[str, Any]]
    channel_id: str | None = None


@dataclass(frozen=True)
class HighEdgeMarketsFound:
    markets: list[dict[str, Any]]
    target_date: date
    sport: str
    channel_id: str | None = None


KalshiEvent = (
    TradePlaced
    | OrderResolved
    | OrderFilled
    | CircuitBreakerTripped
    | TradeApprovalNeeded
    | TradeApprovalReminder
    | TradeExecutionFailed
    | StaleOrdersReviewNeeded
    | HighEdgeMarketsFound
)
