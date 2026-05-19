from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.trading.kalshi.live_trading_config import (
    SPORTSBOOK_LINE_FALLBACK_GAP,
    SUPPORTED_STATS,
    get_game_start_time,
    parse_game_time_from_ticker,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_live_trading_shared_config_owns_supported_stats_and_ticker_time_parsing():
    assert SUPPORTED_STATS["nba"] == {"pts", "reb", "ast", "pra", "pr", "pa", "ra", "stl", "blk", "3pm"}
    assert SUPPORTED_STATS["mlb"] == {"pitcher_strikeouts", "batter_hits", "batter_hrr"}
    assert SPORTSBOOK_LINE_FALLBACK_GAP == 0.08

    parsed = parse_game_time_from_ticker("KXMLBHIT-26APR251415SEASTL")

    assert parsed == datetime(2026, 4, 25, 14, 15, tzinfo=ZoneInfo("America/New_York"))
    assert parse_game_time_from_ticker("NOT-A-KALSHI-TICKER") is None
    db_time = datetime(2026, 4, 25, 13, 0, tzinfo=ZoneInfo("America/New_York"))
    assert get_game_start_time("KXMLBHIT-26APR251415SEASTL", {"KXMLBHIT-26APR251415SEASTL": db_time}) is db_time


def test_live_trading_config_is_not_redefined_by_orchestration_or_policy_modules():
    forbidden_snippets = {
        "SUPPORTED_STATS:",
        "SUPPORTED_STATS =",
        "SPORTSBOOK_LINE_FALLBACK_GAP =",
        "_SPORTSBOOK_LINE_FALLBACK_GAP =",
        "_TICKER_DT_RE =",
        "_MONTH_MAP =",
        "def _parse_game_time_from_ticker",
        "def _get_game_start_time",
    }
    checked_paths = [
        PROJECT_ROOT / "src" / "orchestration" / "kalshi_refresh_job.py",
        PROJECT_ROOT / "src" / "trading" / "kalshi" / "selection_loader.py",
        PROJECT_ROOT / "src" / "trading" / "kalshi" / "strategy.py",
    ]
    offenders: list[str] = []
    for path in checked_paths:
        text = path.read_text(encoding="utf-8")
        for snippet in forbidden_snippets:
            if snippet in text:
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}: {snippet}")

    assert offenders == []
