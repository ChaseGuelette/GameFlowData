from __future__ import annotations

from collections.abc import Callable

from gameflow_engineering_os.collectors import (
    artifacts,
    disk,
    gateway,
    gbrain,
    git_repo,
    kanban,
    kanban_backup,
    scheduler,
)
from gameflow_engineering_os.collectors.common import result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthCheckResult, HealthStatus

COLLECTORS: list[Callable[[EngineeringOSConfig], HealthCheckResult]] = [
    gateway.collect,
    kanban.collect,
    kanban_backup.collect,
    gbrain.collect,
    git_repo.collect,
    scheduler.collect,
    disk.collect,
    artifacts.collect,
]


def collect_all(config: EngineeringOSConfig, collectors: list[Callable[[EngineeringOSConfig], HealthCheckResult]] | None = None) -> list[HealthCheckResult]:
    results: list[HealthCheckResult] = []
    for collector in collectors or COLLECTORS:
        try:
            results.append(collector(config))
        except Exception as exc:
            check_id = getattr(collector, "__module__", "collector").rsplit(".", 1)[-1]
            results.append(result(f"{check_id}.collector", HealthStatus.UNKNOWN, "collector raised exception", "collector isolation", evidence=[str(exc)]))
    return results
