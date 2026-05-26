"""Timezone window helpers for sargable timestamp queries."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = UTC


def et_day_utc_bounds(target_date: date) -> tuple[datetime, datetime]:
    """Return the UTC half-open bounds for a calendar day in Eastern Time.

    Postgres can use regular indexes on ``timestamptz`` columns when callers use
    ``timestamp >= :start_utc AND timestamp < :end_utc``. Avoid casting the
    table column to an ET date in SQL, which forces large scans on wide tables.
    """
    start_et = datetime.combine(target_date, time.min, tzinfo=ET)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(UTC), end_et.astimezone(UTC)
