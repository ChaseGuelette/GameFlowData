import src.orchestration.kalshi_execute_cancellations_job as execute_job
import src.orchestration.kalshi_stale_fills_job as stale_job


def test_stale_fills_job_delegates_detection_and_review_enqueue_to_service(monkeypatch):
    calls = {}

    class FakeService:
        def __init__(self, **kwargs):
            calls["kwargs"] = kwargs

        def enqueue_stale_orders_for_review(self):
            calls["enqueue"] = True
            return 2

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(stale_job, "get_engine", lambda: "engine")
    monkeypatch.setattr(stale_job, "KalshiCancellationService", FakeService)

    stale_job.main()

    assert calls["enqueue"] is True
    assert calls["kwargs"]["engine"] == "engine"
    assert callable(calls["kwargs"]["alert_stale_orders"])


def test_execute_cancellations_job_delegates_approved_execution_to_service(monkeypatch):
    calls = {}

    class FakeService:
        def __init__(self, **kwargs):
            calls["kwargs"] = kwargs

        def execute_approved_cancellations(self):
            calls["execute"] = True
            return {"cancelled": 1, "failed": 0, "skipped_auth": 0}

    monkeypatch.setattr(execute_job, "get_engine", lambda: "engine")
    monkeypatch.setattr(execute_job, "KalshiClient", lambda: "client")
    monkeypatch.setattr(execute_job, "KalshiCancellationService", FakeService)

    execute_job.main()

    assert calls["execute"] is True
    assert calls["kwargs"] == {"engine": "engine", "client": "client"}
