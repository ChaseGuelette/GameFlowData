from __future__ import annotations

from gameflow_engineering_os.collectors.common import command_problem, result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus
from gameflow_engineering_os.subprocesses import run_command


def _git(config: EngineeringOSConfig, *args: str):
    return run_command([str(config.commands.git), "-C", str(config.paths.gameflow_repo), *args], config.collector.timeout_seconds)


def collect(config: EngineeringOSConfig):
    source = f"git readonly status {config.paths.gameflow_repo}"
    branch = _git(config, "branch", "--show-current")
    problem = command_problem("git.checkout", branch, source)
    if problem:
        return problem
    status_cmd = _git(config, "status", "--porcelain=v1", "--branch")
    problem = command_problem("git.checkout", status_cmd, source)
    if problem:
        return problem
    rev = _git(config, "rev-list", "--left-right", "--count", "@{upstream}...HEAD")
    ahead = behind = None
    if rev.returncode == 0:
        parts = rev.stdout.strip().split()
        if len(parts) == 2:
            behind, ahead = int(parts[0]), int(parts[1])
    dirty_paths = [line for line in status_cmd.stdout.splitlines() if line and not line.startswith("##")]
    health = HealthStatus.WARNING if dirty_paths or ahead or behind else HealthStatus.HEALTHY
    summary = f"branch {branch.stdout.strip() or 'unknown'}; {len(dirty_paths)} dirty paths"
    if ahead or behind:
        summary += f"; {ahead} ahead/{behind} behind"
    return result("git.checkout", health, summary, source, {"branch": branch.stdout.strip(), "dirty_paths": len(dirty_paths), "ahead": ahead, "behind": behind}, dirty_paths[:20])
