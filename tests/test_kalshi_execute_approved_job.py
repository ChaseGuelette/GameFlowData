
from __future__ import annotations

import sys
import types

import src.orchestration.kalshi_execute_approved_job as execute_job
from src.trading.kalshi.events import TradeExecutionFailed


class FakeFetchAllResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params or {}))
        return FakeFetchAllResult(self.engine.preview_rows)


class FakeEngine:
    def __init__(self):
        self.preview_rows = [
            (101, "KXMLBHIT-26MAY181900PLAYER", "Test Batter", "batter_hits", 1.0, "yes", 2, 1.40, 0.18),
        ]
        self.calls = []

    def connect(self):
        return FakeConnection(self)


class FakeClient:
    is_authenticated = True

    def get_balance(self):
        return {"balance": 10000, "portfolio_value": 0}

    def get_orderbook(self, ticker, depth=10):
        return {"orderbook": {"no": [[30, 10]], "yes": [[70, 10]]}}


def test_execute_approved_job_uses_queue_risk_and_execution_services_directly(monkeypatch):
    engine = FakeEngine()
    calls = {}

    class ForbiddenLiveTrader:
        def __init__(self, *args, **kwargs):
            raise AssertionError("execute-approved job must not instantiate KalshiLiveTrader")

    live_trader_module = types.ModuleType("src.paper_trading.kalshi_live_trader")
    live_trader_module.KalshiLiveTrader = ForbiddenLiveTrader
    monkeypatch.setitem(sys.modules, "src.paper_trading.kalshi_live_trader", live_trader_module)

    class FakeQueueService:
        def __init__(self, engine):
            calls["queue_engine"] = engine

        def fetch_approved_rows(self, trade_ids):
            calls["trade_ids"] = trade_ids
            return ["approved-row"]

        def mark_expired_trade_ids(self, trade_ids):
            calls["expired_ids"] = trade_ids

        def mark_execution_results(self, trades, results):
            calls["mark_execution"] = (trades, results)

    class FakeRiskService:
        def __init__(self, **kwargs):
            calls["risk_kwargs"] = kwargs

        def ensure_config(self):
            calls["ensure_config"] = True

        def check_circuit_breakers(self):
            calls["checked_circuit_breakers"] = True
            return True, ""

    class FakeExecutionService:
        def __init__(self, **kwargs):
            calls["execution_kwargs"] = kwargs

        def execute_trades(self, trades):
            calls["executed_trades"] = trades
            return [{"ticker": "KXMLBHIT-26MAY181900PLAYER", "order_id": "ord-1", "fill_price": 70, "total_cost": 1.4}]

    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")
    monkeypatch.delenv("DISCORD_CHANNEL_KALSHI", raising=False)
    monkeypatch.delenv("DISCORD_CHANNEL_PREDICTIONS", raising=False)
    monkeypatch.setattr(sys, "argv", ["kalshi_execute_approved_job.py"])
    monkeypatch.setattr(execute_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(execute_job, "KalshiClient", FakeClient, raising=False)
    monkeypatch.setattr(execute_job, "KalshiQueueService", FakeQueueService, raising=False)
    monkeypatch.setattr(execute_job, "KalshiRiskService", FakeRiskService, raising=False)
    monkeypatch.setattr(execute_job, "KalshiExecutionService", FakeExecutionService, raising=False)
    monkeypatch.setattr(execute_job, "split_executable_approved_rows", lambda rows: ([{"ticker": "KXMLBHIT-26MAY181900PLAYER", "side": "yes", "yes_price": 70, "model_prob": 0.8, "expected_cost": 1.4}], []), raising=False)
    monkeypatch.setattr(execute_job, "_send_trade_placed_alert", lambda *args, **kwargs: calls.setdefault("placed_alert", True), raising=False)

    execute_job.main()

    assert calls["queue_engine"] is engine
    assert calls["trade_ids"] == [101]
    assert calls["ensure_config"] is True
    assert calls["checked_circuit_breakers"] is True
    assert calls["executed_trades"] == [{"ticker": "KXMLBHIT-26MAY181900PLAYER", "side": "yes", "yes_price": 70, "model_prob": 0.8, "expected_cost": 1.4}]
    assert calls["mark_execution"][1][0]["order_id"] == "ord-1"
    assert calls["execution_kwargs"]["engine"] is engine
    assert isinstance(calls["execution_kwargs"]["client"], FakeClient)
    assert callable(calls["execution_kwargs"]["get_best_available_price"])
    assert callable(calls["execution_kwargs"]["calculate_kelly_contracts"])
    assert callable(calls["execution_kwargs"]["send_trade_placed_alert"])


def test_execute_approved_job_routes_failed_executions_through_alert_adapter(monkeypatch):
    engine = FakeEngine()
    calls = {"events": []}

    class FakeQueueService:
        def __init__(self, engine):
            pass

        def fetch_approved_rows(self, trade_ids):
            return ["approved-row"]

        def mark_execution_results(self, trades, results):
            calls["mark_execution"] = (trades, results)

    class FakeRiskService:
        def __init__(self, **kwargs):
            pass

        def ensure_config(self):
            pass

        def check_circuit_breakers(self):
            return True, ""

    class FakeExecutionService:
        def __init__(self, **kwargs):
            pass

        def execute_trades(self, trades):
            return []

    class FakeAlertAdapter:
        def send(self, event):
            calls["events"].append(event)
            return True

    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("DISCORD_CHANNEL_KALSHI", "kalshi-channel")
    monkeypatch.setattr(sys, "argv", ["kalshi_execute_approved_job.py"])
    monkeypatch.setattr(execute_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(execute_job, "KalshiClient", FakeClient, raising=False)
    monkeypatch.setattr(execute_job, "KalshiQueueService", FakeQueueService, raising=False)
    monkeypatch.setattr(execute_job, "KalshiRiskService", FakeRiskService, raising=False)
    monkeypatch.setattr(execute_job, "KalshiExecutionService", FakeExecutionService, raising=False)
    monkeypatch.setattr(
        execute_job,
        "split_executable_approved_rows",
        lambda rows: ([{"ticker": "KXMLBHIT-26MAY181900PLAYER", "side": "yes", "yes_price": 70, "model_prob": 0.8, "expected_cost": 1.4}], []),
        raising=False,
    )
    monkeypatch.setattr(execute_job, "KalshiAlertAdapter", FakeAlertAdapter, raising=False)

    execute_job.main()

    assert len(calls["events"]) == 1
    event = calls["events"][0]
    assert isinstance(event, TradeExecutionFailed)
    assert event.channel_id == "kalshi-channel"
    assert event.error_msg == "Order returned no fill — check orderbook liquidity"
    assert event.trade["ticker"] == "KXMLBHIT-26MAY181900PLAYER"
