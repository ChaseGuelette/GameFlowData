from __future__ import annotations

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def collect(config: EngineeringOSConfig):
    source = f"systemctl --user show {config.systemd.gateway_service}"
    cmd = run_command([str(config.commands.systemctl), "--user", "show", config.systemd.gateway_service, "--property=ActiveState,SubState,Result"], config.collector.timeout_seconds)
    problem = command_problem("gateway.service", cmd, source)
    if problem:
        return problem
    props = dict(line.split("=", 1) for line in cmd.stdout.splitlines() if "=" in line)
    active, sub, service_result = props.get("ActiveState"), props.get("SubState"), props.get("Result")
    if active == "active" and sub == "running":
        status = HealthStatus.HEALTHY
    elif active in {"inactive", "failed"} or service_result == "failed":
        status = HealthStatus.FAILED
    else:
        status = HealthStatus.UNKNOWN
    return result("gateway.service", status, f"gateway {active or 'unknown'}/{sub or 'unknown'}", source, {"active_state": active, "sub_state": sub, "result": service_result}, [cmd.stdout])
