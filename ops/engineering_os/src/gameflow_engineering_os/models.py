from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    WARNING = "warning"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NOT_CONFIGURED = "not_configured"


class HealthCheckResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    check_id: str
    status: HealthStatus
    summary: str
    observed_at: datetime
    source: str
    freshness_seconds: int | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list, max_length=20)
    recommended_action: str | None = None

    @field_validator("observed_at")
    @classmethod
    def observed_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value.astimezone(UTC)


class DailyBrief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    brief_date: str
    generated_at: datetime
    overall_status: HealthStatus
    health: list[HealthCheckResult]
    decisions: list[str] = Field(default_factory=list)
    active_work: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    suggested_actions: list[str] = Field(default_factory=list, max_length=3)
    text: str

    @field_validator("generated_at")
    @classmethod
    def generated_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        return value.astimezone(UTC)


class HealthEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    check_id: str
    status: HealthStatus
    transition_type: Literal["failure", "repeat", "recovery"]
    summary: str
    observed_at: datetime
    created_at: datetime

    @field_validator("observed_at", "created_at")
    @classmethod
    def event_times_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event timestamps must be timezone-aware")
        return value.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)
