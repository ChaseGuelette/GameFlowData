from __future__ import annotations

# ruff: noqa: E402,I001

from datetime import UTC, datetime, timedelta

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from gameflow_engineering_os import cli
from gameflow_engineering_os.models import HealthCheckResult, HealthStatus
from gameflow_engineering_os.render import generate_brief
from gameflow_engineering_os.state import store_for_config
from gameflow_engineering_os.web.app import create_app


def result(status: HealthStatus = HealthStatus.HEALTHY, summary: str = "ok") -> HealthCheckResult:
    return HealthCheckResult(check_id="fixture", status=status, summary=summary, observed_at=datetime.now(UTC), source="fixture", evidence=["Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789"])


def test_cli_brief_stdout_and_status(cfg, config_file):
    store = store_for_config(cfg)
    item = result()
    store.persist_results([item], cfg)
    store.save_brief(generate_brief([item], cfg))
    assert cli.main(["--config", str(config_file), "brief", "--stdout"]) == 0
    assert cli.main(["--config", str(config_file), "status"]) == 0
    assert cli.main(["--config", str(config_file), "events"]) == 0


def test_cli_check_json_uses_injected_collectors(monkeypatch, config_file):
    monkeypatch.setattr(cli, "collect_all", lambda cfg: [result(HealthStatus.WARNING, "warn")])
    assert cli.main(["--config", str(config_file), "check", "--json"]) == 0


def test_cli_brief_generate_uses_configured_retention(monkeypatch, config_file):
    monkeypatch.setattr(cli, "collect_all", lambda cfg: [result()])
    assert cli.main(["--config", str(config_file), "brief", "--generate"]) == 0



def test_web_empty_and_healthz(cfg):
    client = TestClient(create_app(cfg))
    assert client.get("/healthz").status_code == 200
    text = client.get("/").text
    assert "No collector data" in text


def test_web_health_states_stale_and_secret_leakage(cfg):
    store = store_for_config(cfg)
    stale_result = HealthCheckResult(check_id="fixture", status=HealthStatus.FAILED, summary="down", observed_at=datetime.now(UTC) - timedelta(hours=1), source="fixture", evidence=["Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789"])
    store.persist_results([stale_result], cfg)
    store.save_brief(generate_brief([stale_result], cfg))
    client = TestClient(create_app(cfg))
    page = client.get("/")
    assert page.status_code == 200
    assert "Collector data is stale" in page.text
    assert "failed" in page.text
    assert "-04:00" in page.text
    assert "s old" in page.text
    detail = client.get("/health/fixture")
    assert detail.status_code == 200
    assert "abcdefghijklmnopqrstuvwxyz0123456789" not in detail.text
    assert client.get("/briefs").status_code == 200


def test_web_refresh_disabled(cfg):
    assert TestClient(create_app(cfg)).post("/refresh").status_code == 404
