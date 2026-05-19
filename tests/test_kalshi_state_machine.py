import pytest

from src.trading.kalshi.state_machine import (
    EntityType,
    InvalidTransitionError,
    assert_transition,
    can_transition,
    is_terminal_order_status,
)
from src.trading.kalshi.statuses import (
    CancelQueueStatus,
    LiveOrderStatus,
    TradeQueueStatus,
)


def test_trade_queue_allows_expected_approval_lifecycle_transitions():
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.APPROVED)
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.REJECTED)
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.PENDING_APPROVAL, TradeQueueStatus.EXPIRED)
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.EXECUTED)
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.FAILED)
    assert can_transition(EntityType.TRADE_QUEUE, TradeQueueStatus.APPROVED, TradeQueueStatus.EXPIRED)


def test_live_order_allows_expected_fill_and_resolution_transitions():
    assert can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.PENDING, LiveOrderStatus.FILLED)
    assert can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.PENDING, LiveOrderStatus.CANCELLED)
    assert can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.WON)
    assert can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.LOST)
    assert can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.FILLED, LiveOrderStatus.CANCELLED)


def test_cancel_queue_allows_review_approval_and_execution_transitions():
    assert can_transition(EntityType.CANCEL_QUEUE, CancelQueueStatus.PENDING_REVIEW, CancelQueueStatus.APPROVED)
    assert can_transition(EntityType.CANCEL_QUEUE, CancelQueueStatus.PENDING_REVIEW, CancelQueueStatus.REJECTED)
    assert can_transition(EntityType.CANCEL_QUEUE, CancelQueueStatus.APPROVED, CancelQueueStatus.EXECUTED)
    assert can_transition(EntityType.CANCEL_QUEUE, CancelQueueStatus.APPROVED, CancelQueueStatus.FAILED)


def test_invalid_transition_fails_loudly():
    assert not can_transition(EntityType.LIVE_ORDER, LiveOrderStatus.WON, LiveOrderStatus.PENDING)
    with pytest.raises(InvalidTransitionError, match="live_order.*won.*pending"):
        assert_transition(EntityType.LIVE_ORDER, LiveOrderStatus.WON, LiveOrderStatus.PENDING)


def test_terminal_order_statuses_are_explicit():
    assert is_terminal_order_status(LiveOrderStatus.WON)
    assert is_terminal_order_status(LiveOrderStatus.LOST)
    assert is_terminal_order_status(LiveOrderStatus.CANCELLED)
    assert not is_terminal_order_status(LiveOrderStatus.PENDING)
    assert not is_terminal_order_status(LiveOrderStatus.FILLED)
