"""Tests for MLB quote-clean decision-time policy helpers."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.backtesting.mlb.quote_decision_policy import (
    build_fixed_cutoff_ts,
    build_slate_decision_ts,
    decision_time_for_game,
)


def test_fixed_cutoff_ts_builds_timezone_aware_et_datetime():
    cutoff = build_fixed_cutoff_ts(date(2026, 4, 13), "17:30")

    assert cutoff == datetime(2026, 4, 13, 17, 30, tzinfo=ZoneInfo("America/New_York"))
    assert cutoff.tzinfo == ZoneInfo("America/New_York")


def test_fixed_cutoff_ts_rejects_invalid_time_string():
    with pytest.raises(ValueError, match="Invalid --quote-cutoff-time-et='bad'; expected HH:MM"):
        build_fixed_cutoff_ts(date(2026, 4, 13), "bad")


def test_fixed_policy_returns_fixed_cutoff():
    fixed_cutoff = build_fixed_cutoff_ts(date(2026, 4, 13), "13:30")
    game = {"game_id": 1, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")}

    decision = decision_time_for_game(
        game,
        policy="fixed_et",
        fixed_cutoff_ts=fixed_cutoff,
        relative_minutes=60,
    )

    assert decision == fixed_cutoff


def test_skip_early_fixed_policy_skips_games_after_fixed_cutoff():
    fixed_cutoff = build_fixed_cutoff_ts(date(2026, 4, 13), "13:30")
    early_game = {"game_id": 2, "game_time_utc": pd.Timestamp("2026-04-13T17:00:00Z")}

    decision = decision_time_for_game(
        early_game,
        policy="skip_early_fixed_et",
        fixed_cutoff_ts=fixed_cutoff,
        relative_minutes=60,
    )

    assert decision is None


def test_relative_to_commence_policy_uses_tminus_minutes():
    game = {"game_id": 3, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")}

    decision = decision_time_for_game(
        game,
        policy="relative_to_commence",
        fixed_cutoff_ts=None,
        relative_minutes=45,
    )

    assert decision == pd.Timestamp("2026-04-13T22:20:00Z").to_pydatetime()


def test_slate_policy_uses_main_slate_time_for_evening_games():
    decision = build_slate_decision_ts(
        pd.Timestamp("2026-04-13T23:05:00Z").to_pydatetime(),
        fallback_relative_minutes=60,
    )

    assert decision == pd.Timestamp("2026-04-13T17:30:00-04:00").to_pydatetime()


def test_slate_policy_falls_back_to_tminus_for_early_games():
    decision = build_slate_decision_ts(
        pd.Timestamp("2026-04-13T13:05:00Z").to_pydatetime(),
        fallback_relative_minutes=60,
    )

    assert decision == pd.Timestamp("2026-04-13T08:05:00-04:00").to_pydatetime()


def test_missing_commence_falls_back_to_fixed_cutoff():
    fixed_cutoff = build_fixed_cutoff_ts(date(2026, 4, 13), "17:30")
    game = {"game_id": 4, "game_time_utc": None}

    decision = decision_time_for_game(
        game,
        policy="relative_to_commence",
        fixed_cutoff_ts=fixed_cutoff,
        relative_minutes=60,
    )

    assert decision == fixed_cutoff


def test_unknown_policy_preserves_existing_fallback_to_fixed_cutoff():
    fixed_cutoff = build_fixed_cutoff_ts(date(2026, 4, 13), "17:30")
    game = {"game_id": 5, "game_time_utc": pd.Timestamp("2026-04-13T23:05:00Z")}

    decision = decision_time_for_game(
        game,
        policy="future_policy",
        fixed_cutoff_ts=fixed_cutoff,
        relative_minutes=60,
    )

    assert decision == fixed_cutoff
