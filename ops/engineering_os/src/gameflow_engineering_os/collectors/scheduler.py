from __future__ import annotations

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def _props(stdout: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in stdout.splitlines() if "=" in line)


def collect(config: EngineeringOSConfig):
    source = "systemctl --user show expected timers and services"
    missing: list[str] = []
    failed_services: list[str] = []
    timer_states: dict[str, str | None] = {}
    for timer in config.systemd.expected_timers:
        cmd = run_command([str(config.commands.systemctl), "--user", "show", timer, "--property=ActiveState,SubState"], config.collector.timeout_seconds)
        problem = command_problem("scheduler.timers", cmd, source)
        if problem:
            return problem
        props = _props(cmd.stdout)
        timer_states[timer] = props.get("ActiveState")
        if props.get("ActiveState") != "active":
            missing.append(timer)
        service = config.systemd.timer_services.get(timer, timer.replace(".timer", ".service"))
        svc = run_command([str(config.commands.systemctl), "--user", "show", service, "--property=Result,ActiveState"], config.collector.timeout_seconds)
        problem = command_problem("scheduler.timers", svc, source)
        if problem:
            return problem
        svc_props = _props(svc.stdout)
        if svc_props.get("Result") == "failed" or svc_props.get("ActiveState") == "failed":
            failed_services.append(service)
    if failed_services:
        status = HealthStatus.WARNING
        summary = f"{len(failed_services)} timer service(s) failed"
    elif missing:
        status = HealthStatus.WARNING
        summary = f"{len(missing)} expected timer(s) inactive"
    else:
        status = HealthStatus.HEALTHY
        summary = f"{len(config.systemd.expected_timers)} expected timers active"
    return result("scheduler.timers", status, summary, source, {"timer_states": timer_states, "failed_services": failed_services}, missing + failed_services)
