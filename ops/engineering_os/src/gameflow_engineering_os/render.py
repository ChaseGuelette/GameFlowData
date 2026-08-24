from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import DailyBrief, HealthCheckResult, HealthStatus
from gameflow_engineering_os.state import worst_status


def truncate(value: str, limit: int = 240) -> str:
    return value if len(value) <= limit else value[: limit - 14] + "...[truncated]"


def generate_brief(results: list[HealthCheckResult], config: EngineeringOSConfig, generated_at: datetime | None = None) -> DailyBrief:
    generated = (generated_at or datetime.now(UTC)).astimezone(UTC)
    local_date = generated.astimezone(ZoneInfo(config.timezone)).date().isoformat()
    overall = worst_status([r.status for r in results])
    blockers = [f"{r.check_id}: {r.summary}" for r in results if r.status == HealthStatus.FAILED]
    warnings = [f"{r.check_id}: {r.summary}" for r in results if r.status in {HealthStatus.WARNING, HealthStatus.UNKNOWN}]
    active_work: list[str] = []
    for item in results:
        if item.check_id == "kanban.board":
            blocked = item.metrics.get("blocked", 0)
            running = item.metrics.get("running", 0)
            stale = item.metrics.get("stale_claims", 0)
            active_work.append(f"{running} running, {blocked} blocked, {stale} stale")
            for task in item.metrics.get("active_tasks", [])[:20]:
                task_id = truncate(str(task.get("id") or "unknown"), 80)
                title = truncate(str(task.get("title") or "untitled"), 120)
                active_work.append(f"[{task.get('status', 'unknown')}] {task_id}: {title}")
    decisions = blockers[:3]
    risks = (blockers + warnings)[:5]
    actions = [r.recommended_action or f"Inspect {r.check_id}" for r in results if r.status in {HealthStatus.FAILED, HealthStatus.WARNING, HealthStatus.UNKNOWN}][:3]
    if not actions and any(r.status == HealthStatus.NOT_CONFIGURED for r in results):
        actions = ["Review unset MVP 0 thresholds"]
    lines = [
        f"GameFlow Engineering OS Brief - {local_date}",
        f"Generated: {generated.isoformat()}",
        f"Overall health: {overall.value}",
        "",
        "Decisions",
        *(f"- {truncate(x)}" for x in (decisions or ["None"])),
        "",
        "Health",
        *(f"- {r.check_id}: {r.status.value} - {truncate(r.summary)} ({r.source}; observed {r.observed_at.isoformat()})" for r in results),
        "",
        "Active Work",
        *(f"- {truncate(x)}" for x in (active_work or ["None"])),
        "",
        "Risks / Changes",
        *(f"- {truncate(x)}" for x in (risks or ["None"])),
        "",
        "Suggested Actions",
        *(f"- {truncate(x)}" for x in (actions or ["None"])),
    ]
    return DailyBrief(
        brief_date=local_date,
        generated_at=generated,
        overall_status=overall,
        health=results,
        decisions=decisions,
        active_work=active_work,
        risks=risks,
        suggested_actions=actions,
        text="\n".join(lines),
    )
