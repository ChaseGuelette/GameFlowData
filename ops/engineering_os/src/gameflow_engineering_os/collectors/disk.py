from __future__ import annotations

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def collect(config: EngineeringOSConfig):
    source = f"df -P {config.disk.path}"
    cmd = run_command([str(config.commands.df), "-P", str(config.disk.path)], config.collector.timeout_seconds)
    problem = command_problem("disk.filesystem", cmd, source)
    if problem:
        return problem
    lines = [line.split() for line in cmd.stdout.splitlines() if line.strip()]
    if len(lines) < 2 or len(lines[-1]) < 5:
        return result("disk.filesystem", HealthStatus.UNKNOWN, "df output malformed", source, evidence=[cmd.stdout])
    used_percent = int(lines[-1][4].rstrip("%"))
    warn, crit = config.thresholds.disk_warning_percent, config.thresholds.disk_critical_percent
    if crit is not None and used_percent >= crit:
        status = HealthStatus.FAILED
    elif warn is not None and used_percent >= warn:
        status = HealthStatus.WARNING
    else:
        status = HealthStatus.HEALTHY
    threshold_text = "thresholds unset" if warn is None and crit is None else f"warning {warn}, critical {crit}"
    return result("disk.filesystem", status, f"disk usage {used_percent}%; {threshold_text}", source, {"used_percent": used_percent, "warning_percent": warn, "critical_percent": crit}, [cmd.stdout])
