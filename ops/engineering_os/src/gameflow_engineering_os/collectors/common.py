from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from gameflow_engineering_os.models import HealthCheckResult, HealthStatus
from gameflow_engineering_os.subprocesses import CommandResult, redact


def now_utc() -> datetime:
    return datetime.now(UTC)


def result(
    check_id: str,
    status: HealthStatus,
    summary: str,
    source: str,
    metrics: dict[str, Any] | None = None,
    evidence: list[str] | None = None,
    recommended_action: str | None = None,
    observed_at: datetime | None = None,
) -> HealthCheckResult:
    return HealthCheckResult(
        check_id=check_id,
        status=status,
        summary=summary,
        observed_at=observed_at or now_utc(),
        source=source,
        freshness_seconds=0,
        metrics=metrics or {},
        evidence=[redact(item) for item in (evidence or [])],
        recommended_action=recommended_action,
    )


def command_problem(check_id: str, command: CommandResult, source: str) -> HealthCheckResult | None:
    if command.timed_out:
        return result(check_id, HealthStatus.UNKNOWN, "collector command timed out", source, evidence=[command.error or "timeout"])
    if command.error:
        return result(check_id, HealthStatus.UNKNOWN, "collector command could not run", source, evidence=[command.error])
    if command.returncode != 0:
        return result(
            check_id,
            HealthStatus.UNKNOWN,
            f"collector command exited {command.returncode}",
            source,
            evidence=[command.stderr or command.stdout or "non-zero exit"],
        )
    return None
