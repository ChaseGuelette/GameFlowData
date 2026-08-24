from __future__ import annotations

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def collect(config: EngineeringOSConfig):
    source = "bounded du -sk on configured artifact directories"
    total_kb = 0
    evidence: list[str] = []
    missing: list[str] = []
    for directory in config.artifacts.directories:
        if not directory.exists():
            missing.append(str(directory))
            continue
        cmd = run_command([str(config.commands.du), "-sk", str(directory)], config.collector.timeout_seconds)
        problem = command_problem("artifacts.bounded", cmd, source)
        if problem:
            return problem
        try:
            kb = int(cmd.stdout.split()[0])
        except (IndexError, ValueError):
            return result("artifacts.bounded", HealthStatus.UNKNOWN, "du output malformed", source, evidence=[cmd.stdout])
        total_kb += kb
        evidence.append(cmd.stdout.strip())
    threshold = config.thresholds.artifact_growth_warning_bytes
    summary = f"bounded artifact footprint {total_kb} KiB"
    if threshold is None:
        summary += "; growth threshold unset"
    return result("artifacts.bounded", HealthStatus.HEALTHY, summary, source, {"total_kib": total_kb, "missing": missing, "growth_warning_bytes": threshold}, evidence + [f"missing optional: {p}" for p in missing])
