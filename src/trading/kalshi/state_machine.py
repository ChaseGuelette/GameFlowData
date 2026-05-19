"""Pure Kalshi live-trading lifecycle transition rules."""

from __future__ import annotations

from enum import StrEnum

from src.trading.kalshi.statuses import (
    CancelQueueStatus,
    LiveOrderStatus,
    TradeQueueStatus,
)


class EntityType(StrEnum):
    """State-machine entity types for the live Kalshi lifecycle."""

    TRADE_QUEUE = "trade_queue"
    LIVE_ORDER = "live_order"
    CANCEL_QUEUE = "cancel_queue"


class InvalidTransitionError(ValueError):
    """Raised when a lifecycle transition is not allowed."""


TransitionKey = tuple[EntityType, str, str]

_ALLOWED_TRANSITIONS: set[TransitionKey] = {
    # Human approval queue for proposed trades.
    (EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.APPROVED),
    (EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.REJECTED),
    (EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.EXPIRED),
    (EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.EXECUTED),
    (EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.FAILED),
    (EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.EXPIRED),
    # Live order lifecycle.
    (EntityType.LIVE_ORDER, LiveOrderStatus.PENDING, LiveOrderStatus.FILLED),
    (EntityType.LIVE_ORDER, LiveOrderStatus.PENDING, LiveOrderStatus.CANCELLED),
    (EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.WON),
    (EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.LOST),
    (EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.CANCELLED),
    # Human approval queue for cancellation requests.
    (EntityType.CANCEL_QUEUE, CancelQueueStatus.PENDING_REVIEW, CancelQueueStatus.APPROVED),
    (EntityType.CANCEL_QUEUE, CancelQueueStatus.PENDING_REVIEW, CancelQueueStatus.REJECTED),
    (EntityType.CANCEL_QUEUE, CancelQueueStatus.APPROVED, CancelQueueStatus.EXECUTED),
    (EntityType.CANCEL_QUEUE, CancelQueueStatus.APPROVED, CancelQueueStatus.FAILED),
}

_TERMINAL_ORDER_STATUSES = {
    LiveOrderStatus.WON,
    LiveOrderStatus.LOST,
    LiveOrderStatus.CANCELLED,
}


def _value(status: StrEnum | str) -> str:
    return status.value if isinstance(status, StrEnum) else str(status)


def can_transition(entity_type: EntityType | str, old_status: StrEnum | str, new_status: StrEnum | str) -> bool:
    """Return whether ``old_status -> new_status`` is valid for ``entity_type``."""
    entity = entity_type if isinstance(entity_type, EntityType) else EntityType(str(entity_type))
    return (entity, _value(old_status), _value(new_status)) in _ALLOWED_TRANSITIONS


def assert_transition(entity_type: EntityType | str, old_status: StrEnum | str, new_status: StrEnum | str) -> None:
    """Raise ``InvalidTransitionError`` unless the lifecycle transition is valid."""
    entity = entity_type if isinstance(entity_type, EntityType) else EntityType(str(entity_type))
    old_value = _value(old_status)
    new_value = _value(new_status)
    if not can_transition(entity, old_value, new_value):
        raise InvalidTransitionError(
            f"Invalid {entity.value} transition: {old_value} -> {new_value}"
        )


def is_terminal_order_status(status: LiveOrderStatus | str) -> bool:
    """Return whether a live order status should not transition further."""
    return _value(status) in {s.value for s in _TERMINAL_ORDER_STATUSES}
