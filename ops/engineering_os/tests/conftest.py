from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def pytest_configure(config):
    if config.option.basetemp is None:
        config.option.basetemp = str(ROOT / ".tmp_pytest")


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    data = {
        "timezone": "America/New_York",
        "daily_brief": {"enabled": True, "schedule": "08:00", "retain_days": 90},
        "events": {"enabled": True, "record_recovery": True, "repeat_after_hours": 24},
        "web": {"bind_host": "127.0.0.1", "bind_port": 8765, "public_base_url": None, "stale_after_minutes": 20, "manual_refresh_enabled": False, "csrf_token": None},
        "paths": {
            "kanban_db": str(tmp_path / "kanban.db"),
            "kanban_backups": str(tmp_path / "backups"),
            "gameflow_repo": str(tmp_path / "repo"),
            "state_dir": str(tmp_path / "state"),
            "log_dir": str(tmp_path / "state" / "logs"),
        },
        "commands": {"hermes": "/bin/hermes", "systemctl": "/bin/systemctl", "tailscale": "/bin/tailscale", "git": "/bin/git", "df": "/bin/df", "du": "/bin/du"},
        "collector": {"timeout_seconds": 0.1},
        "thresholds": {"backup_max_age_hours": 36, "stuck_task_minutes": 30, "disk_warning_percent": None, "disk_critical_percent": None, "artifact_growth_warning_bytes": None},
        "systemd": {"gateway_service": "hermes-gateway.service", "gbrain_service": "gbrain-gameflow.service", "expected_timers": ["good.timer"], "timer_services": {"good.timer": "bad.service"}},
        "disk": {"path": "/"},
        "artifacts": {"directories": [str(tmp_path / "artifacts")]},
    }
    path = tmp_path / "engineering_os.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


@pytest.fixture
def cfg(config_file: Path):
    from gameflow_engineering_os.config import load_config

    return load_config(config_file)


def create_kanban_db(path: Path, stale_time: str | None = None) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "create table tasks(id text, title text, status text, started_at text, completed_at text, claim_expires text, worker_pid integer, last_heartbeat_at text, current_run_id text, consecutive_failures integer, last_failure_error text)"
        )
        conn.executemany(
            "insert into tasks(id,title,status,last_heartbeat_at,claim_expires) values(?,?,?,?,?)",
            [("done-1", "Completed fixture", "done", None, None), ("blocked-1", "Blocked fixture", "blocked", None, None)],
        )
        if stale_time:
            conn.execute("insert into tasks(status,last_heartbeat_at,claim_expires) values('running',?,?)", (stale_time, stale_time))


@pytest.fixture(autouse=True)
def no_env_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GFOS_CONFIG", raising=False)
