from __future__ import annotations

import src.orchestration.kalshi_pending_fills_job as pending_fills_job


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params or {}))
        return FakeScalarResult(self.engine.pending_count)


class FakeEngine:
    def __init__(self, pending_count=2):
        self.pending_count = pending_count
        self.calls = []

    def connect(self):
        return FakeConnection(self)


class FakeClient:
    pass


def test_pending_fills_job_uses_reconciliation_service_directly(monkeypatch):
    engine = FakeEngine(pending_count=2)
    calls = {}

    class FakeReconciliationService:
        def __init__(self, *, engine, client):
            calls["engine"] = engine
            calls["client"] = client

        def reconcile_fills(self):
            calls["reconcile_called"] = True
            return {"reconciled": 0, "promoted": 1, "derived": 0, "cancelled": 0}

    monkeypatch.setenv("KALSHI_API_KEY", "test-key")
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(pending_fills_job, "create_engine", lambda url: engine)
    monkeypatch.setattr(pending_fills_job, "KalshiClient", FakeClient)
    monkeypatch.setattr(pending_fills_job, "KalshiReconciliationService", FakeReconciliationService)

    pending_fills_job.main()

    assert "FROM kalshi_live_orders WHERE status = 'pending'" in engine.calls[0][0]
    assert calls["engine"] is engine
    assert isinstance(calls["client"], FakeClient)
    assert calls["reconcile_called"] is True
