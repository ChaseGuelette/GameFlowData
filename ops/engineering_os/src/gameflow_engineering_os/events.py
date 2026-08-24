from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthCheckResult, HealthEvent, HealthStatus

DEGRADED = {HealthStatus.WARNING, HealthStatus.FAILED}


def summary_hash(summary: str) -> str:
    return hashlib.sha256(summary.encode("utf-8")).hexdigest()


def transition_event(
    previous_status: HealthStatus | None,
    previous_hash: str | None,
    last_event_at: datetime | None,
    result: HealthCheckResult,
    config: EngineeringOSConfig,
) -> HealthEvent | None:
    if not config.events.enabled:
        return None
    now = datetime.now(UTC)
    current_hash = summary_hash(result.summary)
    if result.status in DEGRADED:
        if previous_status not in DEGRADED:
            transition = "failure"
        elif previous_hash != current_hash:
            transition = "failure"
        elif last_event_at and now - last_event_at < timedelta(hours=config.events.repeat_after_hours):
            return None
        else:
            transition = "repeat"
        return HealthEvent(check_id=result.check_id, status=result.status, transition_type=transition, summary=result.summary, observed_at=result.observed_at, created_at=now)
    if result.status == HealthStatus.HEALTHY and previous_status in DEGRADED and config.events.record_recovery:
        return HealthEvent(check_id=result.check_id, status=result.status, transition_type="recovery", summary=result.summary, observed_at=result.observed_at, created_at=now)
    return None
