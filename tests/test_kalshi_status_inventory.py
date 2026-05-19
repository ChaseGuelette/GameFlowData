from src.trading.kalshi.statuses import (
    ALL_CANCEL_QUEUE_STATUSES,
    ALL_LIVE_ORDER_STATUSES,
    ALL_TRADE_QUEUE_STATUSES,
    CancelQueueStatus,
    LiveOrderStatus,
    TradeQueueStatus,
)


def test_trade_queue_status_values_match_existing_database_strings():
    assert TradeQueueStatus.PENDING_APPROVAL == "pending_approval"
    assert TradeQueueStatus.APPROVED == "approved"
    assert TradeQueueStatus.REJECTED == "rejected"
    assert TradeQueueStatus.EXPIRED == "expired"
    assert TradeQueueStatus.EXECUTED == "executed"
    assert TradeQueueStatus.FAILED == "failed"


def test_live_order_status_values_match_existing_database_strings():
    assert LiveOrderStatus.PENDING == "pending"
    assert LiveOrderStatus.FILLED == "filled"
    assert LiveOrderStatus.WON == "won"
    assert LiveOrderStatus.LOST == "lost"
    assert LiveOrderStatus.CANCELLED == "cancelled"


def test_cancel_queue_status_values_match_existing_database_strings():
    assert CancelQueueStatus.PENDING_REVIEW == "pending_review"
    assert CancelQueueStatus.APPROVED == "approved"
    assert CancelQueueStatus.REJECTED == "rejected"
    assert CancelQueueStatus.EXECUTED == "executed"
    assert CancelQueueStatus.FAILED == "failed"


def test_status_collections_are_plain_strings_for_sql_compatibility():
    assert ALL_TRADE_QUEUE_STATUSES == {
        "pending_approval",
        "approved",
        "rejected",
        "expired",
        "executed",
        "failed",
    }
    assert ALL_LIVE_ORDER_STATUSES == {"pending", "filled", "won", "lost", "cancelled"}
    assert ALL_CANCEL_QUEUE_STATUSES == {"pending_review", "approved", "rejected", "executed", "failed"}
