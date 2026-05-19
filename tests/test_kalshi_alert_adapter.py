from __future__ import annotations

from datetime import date, datetime, UTC

from src.trading.kalshi.alert_adapter import KalshiAlertAdapter
from src.trading.kalshi.events import (
    CircuitBreakerTripped,
    HighEdgeMarketsFound,
    OrderFilled,
    OrderResolved,
    StaleOrdersReviewNeeded,
    TradeApprovalNeeded,
    TradeApprovalReminder,
    TradeExecutionFailed,
    TradePlaced,
)


def test_adapter_sends_trade_placed_payload_through_existing_trade_alert_sender():
    calls = []
    adapter = KalshiAlertAdapter(trade_alert_sender=lambda *args, **kwargs: calls.append((args, kwargs)) or True)

    adapter.send(TradePlaced(
        trade={"player_name": "A Player", "stat_type": "pts", "line": 22.5, "side": "no", "yes_price": 61, "fee_adjusted_edge": 0.18},
        fill_price=57,
        contracts=3,
        total_cost=12.34,
        balance=87.66,
        swept_from=60,
        swept_to=57,
        recalc_edge=0.16,
    ))

    assert calls == [(('placed', {
        "player_name": "A Player",
        "stat_type": "pts",
        "line": 22.5,
        "side": "no",
        "fill_price": 57,
        "contracts": 3,
        "total_cost": 12.34,
        "fee_adjusted_edge": 0.18,
        "balance_after": 87.66,
        "swept": "60c -> 57c (edge: 16.0%)",
    }), {})]


def test_adapter_sends_resolution_and_fill_events():
    calls = []
    adapter = KalshiAlertAdapter(trade_alert_sender=lambda *args, **kwargs: calls.append((args, kwargs)) or True)

    adapter.send(OrderResolved(
        order={"player_name": "B Player", "stat_type": "reb", "line": 8.5, "side": "yes"},
        status="won",
        actual=10,
        pnl=4.25,
        balance=104.25,
    ))
    adapter.send(OrderFilled(player_name="C Player", stat_type="ast", side="no", fill_price=44, contracts=5, channel_id="chan"))

    assert calls[0] == (('resolved', {
        "player_name": "B Player",
        "stat_type": "reb",
        "line": 8.5,
        "side": "yes",
        "actual_value": 10,
        "pnl": 4.25,
        "status": "won",
        "balance_after": 104.25,
    }), {})
    assert calls[1] == (('filled', {
        "player_name": "C Player",
        "stat_type": "ast",
        "side": "no",
        "fill_price": 44,
        "contracts": 5,
    }), {"channel_id": "chan"})


def test_adapter_sends_queue_approval_and_reminder_payloads():
    calls = []
    adapter = KalshiAlertAdapter(trade_alert_sender=lambda *args, **kwargs: calls.append((args, kwargs)) or True)
    trades = [{"player_name": "D Player", "stat_type": "hits", "side": "no", "contracts": 2, "expected_cost": 7.5, "fee_adjusted_edge": 0.21}]

    adapter.send(TradeApprovalNeeded(trades=trades, sport="mlb", already_pending=2))
    adapter.send(TradeApprovalReminder(pending_trades=trades, sport="mlb"))

    assert calls[0][0][0] == "approval_needed"
    assert calls[0][0][1]["sport"] == "MLB"
    assert calls[0][0][1]["count"] == 1
    assert calls[0][0][1]["total_exposure"] == 7.5
    assert calls[0][0][1]["edge_range"] == "21%-21%"
    assert calls[0][0][1]["already_pending"] == 2
    assert calls[1][0][0] == "approval_reminder"
    assert calls[1][0][1]["trades"] == trades


def test_adapter_sends_circuit_stale_failure_and_high_edge_events():
    trade_calls = []
    failure_calls = []
    market_calls = []
    adapter = KalshiAlertAdapter(
        trade_alert_sender=lambda *args, **kwargs: trade_calls.append((args, kwargs)) or True,
        trade_failure_sender=lambda *args, **kwargs: failure_calls.append((args, kwargs)) or True,
        market_alert_sender=lambda *args, **kwargs: market_calls.append((args, kwargs)) or True,
    )

    adapter.send(CircuitBreakerTripped(reason="daily loss", balance=80, action="Pause"))
    adapter.send(StaleOrdersReviewNeeded(orders=[{"player_name": "E Player", "stat_type": "pts", "side": "no", "contracts": 1, "total_cost": 4.5}], channel_id="chan"))
    adapter.send(TradeExecutionFailed(trade={"ticker": "T", "player_name": "F Player"}, error_msg="no fill", channel_id="chan"))
    adapter.send(HighEdgeMarketsFound(markets=[{"ticker": "M"}], target_date=date(2026, 5, 18), sport="nba"))

    assert trade_calls[0] == (('circuit_breaker', {"reason": "daily loss", "balance": 80, "action": "Pause"}), {})
    assert trade_calls[1][0][0] == "circuit_breaker"
    assert "stale Kalshi order" in trade_calls[1][0][1]["reason"]
    assert trade_calls[1][1] == {"channel_id": "chan"}
    assert failure_calls == [(({"ticker": "T", "player_name": "F Player"}, "no fill"), {"channel_id": "chan"})]
    assert market_calls == [(([{"ticker": "M"}], datetime(2026, 5, 18, tzinfo=UTC).date()), {"sport": "nba"})]
