from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from gameflow_engineering_os.models import HealthCheckResult, HealthStatus
from gameflow_engineering_os.render import generate_brief
from gameflow_engineering_os.runner import collect_all
from gameflow_engineering_os.state import StateStore


def make_result(check_id: str, status: HealthStatus, summary: str = "summary") -> HealthCheckResult:
    return HealthCheckResult(check_id=check_id, status=status, summary=summary, observed_at=datetime.now(UTC), source="fixture")


def test_runner_isolates_collector_failure(cfg):
    def bad(_cfg):
        raise RuntimeError("boom")

    def good(_cfg):
        return make_result("good", HealthStatus.HEALTHY, "ok")

    results = collect_all(cfg, [bad, good])
    assert [r.status for r in results] == [HealthStatus.UNKNOWN, HealthStatus.HEALTHY]


def test_brief_deterministic_failed_unknown_and_truncation(cfg):
    results = [make_result("a", HealthStatus.FAILED, "x" * 400), make_result("b", HealthStatus.UNKNOWN, "unknown")]
    fixed = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
    first = generate_brief(results, cfg, fixed)
    second = generate_brief(results, cfg, fixed)
    assert first.text == second.text
    assert first.overall_status == HealthStatus.FAILED
    assert "[truncated]" in first.text
    assert "unknown" in first.text


def test_state_events_dedup_repeat_recovery_unknown(cfg, tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    failure = make_result("check", HealthStatus.FAILED, "down")
    assert len(store.persist_results([failure], cfg)) == 1
    assert store.persist_results([failure], cfg) == []
    with store._connect() as conn:
        conn.execute("update check_state set last_event_at=?", ((datetime.now(UTC) - timedelta(hours=25)).isoformat(),))
    assert len(store.persist_results([failure], cfg)) == 1
    assert len(store.persist_results([make_result("check", HealthStatus.HEALTHY, "ok")], cfg)) == 1
    assert store.persist_results([make_result("check", HealthStatus.UNKNOWN, "unknown")], cfg) == []



def test_brief_replaces_same_date_and_concurrent_access(cfg, tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    result = make_result("check", HealthStatus.HEALTHY, "ok")
    brief = generate_brief([result], cfg, datetime(2026, 7, 27, 12, 0, tzinfo=UTC))
    store.save_brief(brief)
    store.save_brief(brief)
    assert len(store.brief_history()) == 1

    def write_once():
        return len(store.persist_results([result], cfg))

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: write_once(), range(8)))
    assert store.latest_results()[0].check_id == "check"


def test_brief_retention_prunes_older_dates(cfg, tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    result = make_result("check", HealthStatus.HEALTHY, "ok")
    old = generate_brief([result], cfg, datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    current = generate_brief([result], cfg, datetime(2026, 7, 27, 12, 0, tzinfo=UTC))
    store.save_brief(old)
    store.save_brief(current, retain_days=30)
    assert [brief.brief_date for brief in store.brief_history()] == [current.brief_date]

