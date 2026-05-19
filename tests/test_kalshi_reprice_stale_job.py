from __future__ import annotations

import sys
import types

import src.orchestration.kalshi_reprice_stale_job as reprice_job


class FakeClient:
    is_authenticated = True

    def __init__(self):
        self.list_calls = []

    def list_orders(self, **kwargs):
        self.list_calls.append(kwargs)
        return [{"ticker": "TICKER", "side": "yes", "yes_price": 70}]

    def get_orderbook(self, ticker, depth=10):
        return {"orderbook": {"no": [[30, 10]], "yes": [[70, 10]]}}


class FakeEngine:
    pass


def test_reprice_stale_job_uses_repricing_service_directly(monkeypatch):
    engine = FakeEngine()
    calls = {}

    class ForbiddenLiveTrader:
        def __init__(self, *args, **kwargs):
            raise AssertionError("reprice-stale job must not instantiate KalshiLiveTrader")

    live_trader_module = types.ModuleType("src.paper_trading.kalshi_live_trader")
    live_trader_module.KalshiLiveTrader = ForbiddenLiveTrader
    monkeypatch.setitem(sys.modules, "src.paper_trading.kalshi_live_trader", live_trader_module)

    class FakeRepricingService:
        def __init__(self, **kwargs):
            calls["repricing_kwargs"] = kwargs

        def reprice_stale_orders(self):
            calls["reprice_called"] = True
            return 3

    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setattr(sys, "argv", ["kalshi_reprice_stale_job.py"])
    monkeypatch.setattr(reprice_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(reprice_job, "KalshiClient", FakeClient, raising=False)
    monkeypatch.setattr(reprice_job, "KalshiRepricingService", FakeRepricingService, raising=False)

    reprice_job.main()

    assert calls["reprice_called"] is True
    assert calls["repricing_kwargs"]["engine"] is engine
    assert isinstance(calls["repricing_kwargs"]["client"], FakeClient)
    assert callable(calls["repricing_kwargs"]["get_best_available_price"])
    assert calls["repricing_kwargs"]["sweep_max_cents"] == 10
    assert calls["repricing_kwargs"]["sweep_edge_retention"] == 0.50


def test_reprice_stale_job_dry_run_uses_client_without_facade(monkeypatch):
    engine = FakeEngine()
    client = FakeClient()

    class ForbiddenLiveTrader:
        def __init__(self, *args, **kwargs):
            raise AssertionError("dry-run must not instantiate KalshiLiveTrader")

    live_trader_module = types.ModuleType("src.paper_trading.kalshi_live_trader")
    live_trader_module.KalshiLiveTrader = ForbiddenLiveTrader
    monkeypatch.setitem(sys.modules, "src.paper_trading.kalshi_live_trader", live_trader_module)

    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setattr(sys, "argv", ["kalshi_reprice_stale_job.py", "--dry-run"])
    monkeypatch.setattr(reprice_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(reprice_job, "KalshiClient", lambda: client, raising=False)

    reprice_job.main()

    assert client.list_calls == [{"status": "resting"}]
