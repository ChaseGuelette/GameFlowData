from __future__ import annotations

import pytest
from gameflow_engineering_os.collectors import artifacts, disk, gateway, gbrain, git_repo, scheduler
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import CommandResult


def test_gateway_healthy(monkeypatch: pytest.MonkeyPatch, cfg):
    monkeypatch.setattr(gateway, "run_command", lambda *a, **k: CommandResult([], 0, "ActiveState=active\nSubState=running\nResult=success\n", ""))
    assert gateway.collect(cfg).status == HealthStatus.HEALTHY


def test_gateway_failed(monkeypatch: pytest.MonkeyPatch, cfg):
    monkeypatch.setattr(gateway, "run_command", lambda *a, **k: CommandResult([], 0, "ActiveState=failed\nSubState=dead\nResult=failed\n", ""))
    assert gateway.collect(cfg).status == HealthStatus.FAILED


def test_gateway_nonzero_command_is_unknown(monkeypatch: pytest.MonkeyPatch, cfg):
    monkeypatch.setattr(gateway, "run_command", lambda *a, **k: CommandResult([], 1, "ActiveState=active\nSubState=running\n", "permission denied"))
    assert gateway.collect(cfg).status == HealthStatus.UNKNOWN


def test_gbrain_redacts_and_requires_transport(monkeypatch: pytest.MonkeyPatch, cfg):
    calls = []

    def fake(args, timeout):
        calls.append(args)
        if "systemctl" in args[0]:
            return CommandResult(args, 0, "ActiveState=active\nSubState=running\n", "")
        return CommandResult(args, 0, "connected 11 tools Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789", "")

    monkeypatch.setattr(gbrain, "run_command", fake)
    result = gbrain.collect(cfg)
    assert result.status == HealthStatus.HEALTHY
    assert "abcdefghijklmnopqrstuvwxyz0123456789" not in "\n".join(result.evidence)


def test_gbrain_zero_tools_is_unknown(monkeypatch: pytest.MonkeyPatch, cfg):
    def fake(args, timeout):
        if "systemctl" in args[0]:
            return CommandResult(args, 0, "ActiveState=active\nSubState=running\n", "")
        return CommandResult(args, 0, "connection failed; 0 tools available", "")

    monkeypatch.setattr(gbrain, "run_command", fake)
    assert gbrain.collect(cfg).status == HealthStatus.UNKNOWN


def test_git_dirty_warning(monkeypatch: pytest.MonkeyPatch, cfg):
    def fake(args, timeout):
        if "branch" in args:
            return CommandResult(args, 0, "main\n", "")
        if "rev-list" in args:
            return CommandResult(args, 0, "0 0\n", "")
        return CommandResult(args, 0, "## main...origin/main\n M file.py\n", "")

    monkeypatch.setattr(git_repo, "run_command", fake)
    assert git_repo.collect(cfg).status == HealthStatus.WARNING


def test_scheduler_surfaces_failed_service(monkeypatch: pytest.MonkeyPatch, cfg):
    def fake(args, timeout):
        if args[-1] == "good.timer":
            return CommandResult(args, 0, "ActiveState=active\nSubState=waiting\n", "")
        return CommandResult(args, 0, "ActiveState=failed\nResult=failed\n", "")

    monkeypatch.setattr(scheduler, "run_command", fake)
    result = scheduler.collect(cfg)
    assert result.status == HealthStatus.WARNING
    assert "bad.service" in result.evidence


def test_scheduler_nonzero_service_probe_is_unknown(monkeypatch: pytest.MonkeyPatch, cfg):
    def fake(args, timeout):
        if args[-1] == "good.timer":
            return CommandResult(args, 0, "ActiveState=active\nSubState=waiting\n", "")
        return CommandResult(args, 1, "", "unit lookup failed")

    monkeypatch.setattr(scheduler, "run_command", fake)
    assert scheduler.collect(cfg).status == HealthStatus.UNKNOWN


def test_disk_healthy_when_thresholds_unset(monkeypatch: pytest.MonkeyPatch, cfg):
    monkeypatch.setattr(disk, "run_command", lambda *a, **k: CommandResult([], 0, "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/root 100 22 78 22% /\n", ""))
    result = disk.collect(cfg)
    assert result.status == HealthStatus.HEALTHY
    assert result.metrics["warning_percent"] is None


@pytest.mark.parametrize(
    ("used", "expected"),
    [(85, HealthStatus.WARNING), (95, HealthStatus.FAILED)],
)
def test_disk_configured_thresholds(monkeypatch: pytest.MonkeyPatch, cfg, used, expected):
    cfg.thresholds.disk_warning_percent = 80
    cfg.thresholds.disk_critical_percent = 90
    monkeypatch.setattr(
        disk,
        "run_command",
        lambda *a, **k: CommandResult([], 0, f"Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/root 100 {used} 1 {used}% /\n", ""),
    )
    assert disk.collect(cfg).status == expected


def test_artifacts_bounded_measurement(monkeypatch: pytest.MonkeyPatch, cfg):
    cfg.artifacts.directories[0].mkdir(parents=True)
    monkeypatch.setattr(artifacts, "run_command", lambda *a, **k: CommandResult([], 0, "12\t/path\n", ""))
    result = artifacts.collect(cfg)
    assert result.status == HealthStatus.HEALTHY
    assert result.metrics["total_kib"] == 12
