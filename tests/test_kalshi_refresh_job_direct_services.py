from __future__ import annotations

import sys
import types
from datetime import date

import src.orchestration.kalshi_refresh_job as refresh_job


class FakeEngine:
    pass


class FakeClient:
    pass


def install_forbidden_live_trader(monkeypatch):
    class ForbiddenLiveTrader:
        def __init__(self, *args, **kwargs):
            raise AssertionError("kalshi_refresh_job must not instantiate KalshiLiveTrader")

    live_trader_module = types.ModuleType("src.paper_trading.kalshi_live_trader")
    setattr(live_trader_module, "KalshiLiveTrader", ForbiddenLiveTrader)
    monkeypatch.setitem(sys.modules, "src.paper_trading.kalshi_live_trader", live_trader_module)


def install_resolution_fakes(monkeypatch, calls, resolve_result=None):
    engine = FakeEngine()
    client = FakeClient()
    resolve_result = resolve_result or {"resolved": 2, "won": 1, "lost": 1, "cancelled": 0}

    class FakeReconciliationService:
        def __init__(self, *, engine, client):
            calls["reconciliation_kwargs"] = {"engine": engine, "client": client}

        def reconcile_fills(self, target_date=None):
            calls["reconcile_target_date"] = target_date
            return {"reconciled": 1}

    class FakeActualsAdapter:
        def __init__(self, engine):
            calls["actuals_engine"] = engine
            self.fetch_actuals = object()

    class FakeDailyLedgerService:
        def __init__(self, *, engine, starting_bankroll):
            calls["ledger_kwargs"] = {"engine": engine, "starting_bankroll": starting_bankroll}
            self.update_daily_log = object()

    class FakeRiskService:
        def __init__(self, **kwargs):
            calls["risk_kwargs"] = kwargs
            calls["risk_instance"] = self

        def get_consecutive_losses(self):
            calls["get_consecutive_losses"] = True
            return 0

        def update_streak(self, streak):
            calls["updated_streak"] = streak

    class FakeSettlementService:
        def __init__(self, **kwargs):
            calls["settlement_kwargs"] = kwargs

        def resolve_settled(self):
            calls["resolve_called"] = True
            return resolve_result

    monkeypatch.setattr(refresh_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiClient", lambda: client, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiReconciliationService", FakeReconciliationService, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiActualsAdapter", FakeActualsAdapter, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiDailyLedgerService", FakeDailyLedgerService, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiRiskService", FakeRiskService, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiSettlementService", FakeSettlementService, raising=False)
    return engine, client, resolve_result


def test_refresh_resolve_only_uses_direct_resolution_services(monkeypatch):
    calls = {}
    install_forbidden_live_trader(monkeypatch)
    engine, client, resolve_result = install_resolution_fakes(monkeypatch, calls)

    summary = refresh_job.run(date(2026, 5, 18), resolve_only=True)

    assert summary["live_resolution"] == resolve_result
    assert calls["reconciliation_kwargs"] == {"engine": engine, "client": client}
    assert calls["reconcile_target_date"] is None
    assert calls["actuals_engine"] is engine
    assert calls["ledger_kwargs"] == {"engine": engine, "starting_bankroll": 100.0}
    assert calls["settlement_kwargs"]["engine"] is engine
    assert calls["settlement_kwargs"]["client"] is client
    assert calls["settlement_kwargs"]["fetch_actuals"] is not None
    assert calls["settlement_kwargs"]["update_daily_log"] is not None
    assert calls["settlement_kwargs"]["get_consecutive_losses"].__self__ is calls["risk_instance"]
    assert calls["settlement_kwargs"]["update_streak"].__self__ is calls["risk_instance"]
    assert calls["resolve_called"] is True


def install_pipeline_fakes(monkeypatch):
    scraper_module = types.ModuleType("src.scrapers.kalshi.kalshi_market_scraper")
    scraper_module.scrape_and_store = lambda **kwargs: {"parsed": 0, "stored": 0}
    monkeypatch.setitem(sys.modules, "src.scrapers.kalshi.kalshi_market_scraper", scraper_module)

    edge_module = types.ModuleType("src.models.kalshi_edge")

    class FakeEdgeCalculator:
        def compute_edges(self, target_date, sport="nba"):
            return {"matched": 0, "updated": 0}

    setattr(edge_module, "KalshiEdgeCalculator", FakeEdgeCalculator)
    monkeypatch.setitem(sys.modules, "src.models.kalshi_edge", edge_module)
    monkeypatch.setattr(refresh_job, "_fetch_orderbooks", lambda target_date, sport: 0)


def test_refresh_live_resolution_step_uses_direct_services_with_target_date(monkeypatch):
    calls = {}
    target_date = date(2026, 5, 18)
    install_forbidden_live_trader(monkeypatch)
    _, _, resolve_result = install_resolution_fakes(monkeypatch, calls, {"resolved": 1, "won": 1, "lost": 0, "cancelled": 0})
    install_pipeline_fakes(monkeypatch)

    summary = refresh_job.run(
        target_date,
        sport="mlb",
        skip_paper=True,
        skip_live=True,
        skip_discord=True,
    )

    assert summary["live_resolution"] == resolve_result
    assert calls["reconcile_target_date"] == target_date
    assert calls["resolve_called"] is True


def test_refresh_missing_samples_skips_new_paper_live_and_alert_steps(monkeypatch):
    calls = {"paper_trader_init": 0, "live_trading": 0, "alerts": 0}
    target_date = date(2026, 5, 27)

    install_forbidden_live_trader(monkeypatch)
    install_resolution_fakes(monkeypatch, calls, {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0})

    scraper_module = types.ModuleType("src.scrapers.kalshi.kalshi_market_scraper")
    setattr(scraper_module, "scrape_and_store", lambda **kwargs: {"parsed": 2, "stored": 2})
    monkeypatch.setitem(sys.modules, "src.scrapers.kalshi.kalshi_market_scraper", scraper_module)

    edge_module = types.ModuleType("src.models.kalshi_edge")

    class FakeEdgeCalculator:
        def compute_edges(self, target_date, sport="nba"):
            return {
                "markets": 2,
                "matched": 0,
                "updated": 0,
                "sample_output_gap": True,
                "blocking_output_gap": False,
                "warning": "No MC samples found — Kalshi edges cannot be computed",
            }

    setattr(edge_module, "KalshiEdgeCalculator", FakeEdgeCalculator)
    monkeypatch.setitem(sys.modules, "src.models.kalshi_edge", edge_module)
    monkeypatch.setattr(refresh_job, "_fetch_orderbooks", lambda target_date, sport: 2)
    monkeypatch.setattr(refresh_job, "_run_live_trading", lambda target_date, sport: calls.__setitem__("live_trading", 1))
    monkeypatch.setattr(refresh_job, "_send_high_edge_alerts", lambda target_date, sport: calls.__setitem__("alerts", 1))
    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")

    paper_module = types.ModuleType("src.paper_trading.kalshi_paper_trader")

    class FakePaperTrader:
        def __init__(self):
            calls["paper_trader_init"] += 1

    setattr(paper_module, "KalshiPaperTrader", FakePaperTrader)
    monkeypatch.setitem(sys.modules, "src.paper_trading.kalshi_paper_trader", paper_module)

    summary = refresh_job.run(target_date, sport="nba")

    assert summary["paper_trading"] == {"skipped": "missing_mc_samples"}
    assert summary["live_trading"] == {"skipped": "missing_mc_samples"}
    assert summary["alerts_sent"] is False
    assert summary["live_resolution"] == {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}
    assert calls["resolve_called"] is True
    assert calls["paper_trader_init"] == 0
    assert calls["live_trading"] == 0
    assert calls["alerts"] == 0


def test_refresh_live_trading_step_uses_direct_risk_selection_and_queue_services(monkeypatch):
    calls = {"alerts": []}
    target_date = date(2026, 5, 18)
    engine = FakeEngine()

    class FakeClient:
        is_authenticated = True

    client = FakeClient()
    trades = [{
        "game_date": target_date,
        "ticker": "KXNBA-TEST",
        "sport": "nba",
        "player_id": 42,
        "player_name": "Test Player",
        "stat_type": "pts",
        "line": 20.5,
        "side": "no",
        "yes_price": 60,
        "contracts": 3,
        "expected_cost": 1.2,
        "expected_fee": 0.03,
        "model_prob": 0.55,
        "kalshi_implied": 0.60,
        "edge": 0.05,
        "fee_adjusted_edge": 0.18,
    }]
    pending = [{"player_name": "Already Pending", "stat_type": "reb", "side": "no", "contracts": 1}]

    install_forbidden_live_trader(monkeypatch)
    install_pipeline_fakes(monkeypatch)
    install_resolution_fakes(monkeypatch, calls, {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0})
    monkeypatch.setenv("KALSHI_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("NBA_TRADING_ENABLED", "true")
    monkeypatch.setattr(refresh_job, "get_engine", lambda: engine, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiClient", lambda: client, raising=False)
    monkeypatch.setattr(refresh_job, "_send_trade_approval_alert", lambda trades, sport, already_pending=0: calls["alerts"].append((trades, sport, already_pending)))

    class FakeRiskService:
        def __init__(self, **kwargs):
            calls.setdefault("risk_kwargs", []).append(kwargs)

        def ensure_config(self):
            calls["ensure_config"] = True

        def check_circuit_breakers(self):
            calls["check_circuit_breakers"] = True
            return True, ""

        def get_consecutive_losses(self):
            return 0

        def update_streak(self, streak):
            calls["updated_streak"] = streak

    class FakeQueueService:
        def __init__(self, engine_arg):
            calls["queue_engine"] = engine_arg

        def renew_expired_pending_trades(self, date_arg, sport):
            calls["renew_args"] = (date_arg, sport)
            return 2

        def fetch_pending_approval_trades(self, date_arg, sport):
            calls["pending_args"] = (date_arg, sport)
            return pending

        def propose_trades(self, trades_arg):
            calls["proposed_trades"] = trades_arg
            return len(trades_arg)

    class FakeInputs:
        candidates = [object()]
        config = types.SimpleNamespace(prior_exposure=0.0)
        existing_player_stats = {(1, "pts")}
        queued_player_stats = {(2, "reb")}
        held_positions = {"KXNBA-TEST": object()}
        mode_str = "NO-only"
        effective_daily_exposure_cap = 80.0

    class FakeSelectionInputLoader:
        def __init__(self, **kwargs):
            calls["loader_kwargs"] = kwargs

        def load_inputs(self, date_arg, *, sport, prior_exposure, strategy_knobs):
            calls["load_inputs_args"] = (date_arg, sport, prior_exposure, strategy_knobs)
            return FakeInputs()

    monkeypatch.setattr(refresh_job, "KalshiRiskService", FakeRiskService, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiQueueService", FakeQueueService, raising=False)
    monkeypatch.setattr(refresh_job, "KalshiSelectionInputLoader", FakeSelectionInputLoader, raising=False)
    monkeypatch.setattr(refresh_job, "select_trade_intents", lambda *args, **kwargs: [types.SimpleNamespace(as_legacy_dict=lambda: trades[0])], raising=False)

    summary = refresh_job.run(
        target_date,
        sport="nba",
        skip_paper=True,
        skip_discord=True,
    )

    assert summary["live_trading"] == {"selected": 1, "proposed": 1, "renewed": 2}
    assert calls["ensure_config"] is True
    assert calls["check_circuit_breakers"] is True
    assert calls["renew_args"] == (target_date, "nba")
    assert calls["pending_args"] == (target_date, "nba")
    assert calls["proposed_trades"] == trades
    assert calls["alerts"] == [(trades, "nba", 1)]
    assert calls["loader_kwargs"]["engine"] is engine
    assert calls["loader_kwargs"]["client"] is client
    assert calls["load_inputs_args"] == (
        target_date,
        "nba",
        0.0,
        {"min_edge": 0.15, "min_price": 5, "max_contracts": 50, "kelly_fraction": 0.125},
    )
