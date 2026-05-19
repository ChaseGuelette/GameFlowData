"""Central Kalshi live-trading status constants.

These values intentionally match existing database strings.  Keep them as
``str`` enums so they can be passed directly into SQL parameters and compared
with raw DB values during the migration away from scattered string literals.
"""

from __future__ import annotations

from enum import StrEnum


class TradeQueueStatus(StrEnum):
    """Statuses for rows in ``kalshi_trade_queue``."""

    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    EXECUTED = "executed"
    FAILED = "failed"


class LiveOrderStatus(StrEnum):
    """Statuses for rows in ``kalshi_live_orders``."""

    PENDING = "pending"
    FILLED = "filled"
    WON = "won"
    LOST = "lost"
    CANCELLED = "cancelled"


class CancelQueueStatus(StrEnum):
    """Statuses for rows in ``kalshi_cancel_queue``."""

    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    FAILED = "failed"


ALL_TRADE_QUEUE_STATUSES = {status.value for status in TradeQueueStatus}
ALL_LIVE_ORDER_STATUSES = {status.value for status in LiveOrderStatus}
ALL_CANCEL_QUEUE_STATUSES = {status.value for status in CancelQueueStatus}
