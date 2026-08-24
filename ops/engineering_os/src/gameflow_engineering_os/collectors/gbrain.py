from __future__ import annotations

import re

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def collect(config: EngineeringOSConfig):
    source = "gbrain systemd service plus hermes mcp test gbrain"
    svc = run_command([str(config.commands.systemctl), "--user", "show", config.systemd.gbrain_service, "--property=ActiveState,SubState,Result"], config.collector.timeout_seconds)
    problem = command_problem("gbrain.mcp", svc, source)
    if problem:
        return problem
    props = dict(line.split("=", 1) for line in svc.stdout.splitlines() if "=" in line)
    mcp = run_command([str(config.commands.hermes), "mcp", "test", "gbrain"], config.collector.timeout_seconds)
    problem = command_problem("gbrain.mcp", mcp, source)
    if problem:
        return problem
    active = props.get("ActiveState") == "active"
    discovered = bool(re.search(r"\b[1-9]\d*\s+tools?\b", mcp.stdout.lower()))
    if active and discovered:
        status = HealthStatus.HEALTHY
    elif not active:
        status = HealthStatus.FAILED
    else:
        status = HealthStatus.UNKNOWN
    return result("gbrain.mcp", status, "gbrain service active; MCP transport checked" if status == HealthStatus.HEALTHY else "gbrain MCP degraded", source, {"service_active": active, "mcp_returncode": mcp.returncode}, [svc.stdout, mcp.stdout, mcp.stderr])
