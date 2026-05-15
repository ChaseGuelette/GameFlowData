"""Tests for MLB daily props scraper timing filters."""

from datetime import datetime, timedelta, timezone

from src.scrapers.mlb.mlb_daily_player_props_scraper import _is_in_pregame_window


def _event(minutes_until: int):
    commence = datetime.now(timezone.utc) + timedelta(minutes=minutes_until)
    return {"commence_time": commence.isoformat().replace("+00:00", "Z")}


def test_pregame_window_keeps_games_near_commence_minus_30_minutes():
    assert _is_in_pregame_window(_event(30), minutes_before=30, tolerance_minutes=5)
    assert _is_in_pregame_window(_event(34), minutes_before=30, tolerance_minutes=5)
    assert not _is_in_pregame_window(_event(45), minutes_before=30, tolerance_minutes=5)


def test_pregame_window_disabled_keeps_all_events():
    assert _is_in_pregame_window({"commence_time": None}, minutes_before=None, tolerance_minutes=5)
