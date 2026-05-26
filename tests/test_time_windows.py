from __future__ import annotations

from datetime import date, timezone

from src.utils.time_windows import et_day_utc_bounds


def test_et_day_utc_bounds_uses_half_open_normal_day():
    start_utc, end_utc = et_day_utc_bounds(date(2026, 5, 26))

    assert start_utc.tzinfo is timezone.utc
    assert end_utc.tzinfo is timezone.utc
    assert start_utc.isoformat() == "2026-05-26T04:00:00+00:00"
    assert end_utc.isoformat() == "2026-05-27T04:00:00+00:00"


def test_et_day_utc_bounds_handles_spring_dst_transition():
    start_utc, end_utc = et_day_utc_bounds(date(2026, 3, 8))

    assert start_utc.isoformat() == "2026-03-08T05:00:00+00:00"
    assert end_utc.isoformat() == "2026-03-09T04:00:00+00:00"
    assert (end_utc - start_utc).total_seconds() == 23 * 60 * 60


def test_et_day_utc_bounds_handles_fall_dst_transition():
    start_utc, end_utc = et_day_utc_bounds(date(2026, 11, 1))

    assert start_utc.isoformat() == "2026-11-01T04:00:00+00:00"
    assert end_utc.isoformat() == "2026-11-02T05:00:00+00:00"
    assert (end_utc - start_utc).total_seconds() == 25 * 60 * 60
