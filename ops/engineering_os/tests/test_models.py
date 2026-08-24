from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gameflow_engineering_os.models import HealthCheckResult, HealthStatus


def test_status_validation_and_serialization():
    result = HealthCheckResult(check_id="x", status="healthy", summary="ok", observed_at=datetime.now(UTC), source="fixture")
    assert result.status == HealthStatus.HEALTHY
    assert result.model_dump(mode="json")["status"] == "healthy"


def test_unknown_status_rejected():
    with pytest.raises(ValueError):
        HealthCheckResult(check_id="x", status="fine", summary="bad", observed_at=datetime.now(UTC), source="fixture")


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError):
        HealthCheckResult(check_id="x", status="healthy", summary="bad", observed_at=datetime(2026, 1, 1), source="fixture")
