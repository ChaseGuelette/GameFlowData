"""Shared Kalshi live-trading configuration and ticker-time helpers."""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

SUPPORTED_STATS: dict[str, set[str]] = {
    "nba": {"pts", "reb", "ast", "pra", "pr", "pa", "ra", "stl", "blk", "3pm"},
    "mlb": {"pitcher_strikeouts", "batter_hits", "batter_hrr"},
}

SPORTSBOOK_LINE_FALLBACK_GAP = 0.08

_TICKER_DT_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})(\d{4})[A-Z]")
_MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_ET = ZoneInfo("America/New_York")


def parse_game_time_from_ticker(ticker: str) -> datetime | None:
    """Extract game start time in Eastern Time from a Kalshi sports ticker."""
    match = _TICKER_DT_RE.search(ticker)
    if not match:
        return None
    try:
        year = 2000 + int(match.group(1))
        month = _MONTH_MAP.get(match.group(2))
        if month is None:
            return None
        day = int(match.group(3))
        hhmm = match.group(4)
        hour, minute = int(hhmm[:2]), int(hhmm[2:])
        return datetime(year, month, day, hour, minute, tzinfo=_ET)
    except (ValueError, IndexError):
        return None


def get_game_start_time(ticker: str, start_times: dict[str, datetime | None]) -> datetime | None:
    """Return DB close_time for ticker, falling back to parsing the ticker."""
    db_time = start_times.get(ticker)
    if db_time is not None:
        return db_time
    return parse_game_time_from_ticker(ticker)
