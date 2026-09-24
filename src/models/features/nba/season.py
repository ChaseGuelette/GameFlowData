"""Resolve NBA competitive-game season identity from the game's own ID and date."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

COMPETITIVE_GAME_PREFIXES = ("002", "004", "005")


@dataclass(frozen=True)
class NBASeason:
    start_year: int
    season_id: str
    label: str


def is_competitive_nba_game_id(game_id: str) -> bool:
    """Accept regular-season, playoff and play-in NBA schedule game IDs."""
    return isinstance(game_id, str) and len(game_id) == 10 and game_id.isascii() and game_id.isdigit() and game_id.startswith(
        COMPETITIVE_GAME_PREFIXES
    )


def resolve_nba_season(game_id: str, game_date: date) -> NBASeason:
    """Resolve the season from a competitive game ID, rejecting date/ID mismatches."""
    if not is_competitive_nba_game_id(game_id):
        raise ValueError(f"Invalid competitive NBA game ID: {game_id!r}")

    year_suffix = int(game_id[3:5])
    start_year = game_date.year // 100 * 100 + year_suffix
    if start_year > game_date.year:
        start_year -= 100
    # The 2019-20 bubble playoffs extended into October 2020 (Finals Game 6).
    bubble_playoff = game_id.startswith("00419") and game_date <= date(2020, 10, 11)
    if (
        game_date.year not in (start_year, start_year + 1)
        or (game_date.year == start_year and game_date.month < 9)
        or (game_date.year == start_year + 1 and game_date.month >= 9 and not bubble_playoff)
    ):
        raise ValueError(f"NBA game ID {game_id} season does not match game date {game_date}")

    return NBASeason(start_year, f"2{start_year}", f"{start_year}-{(start_year + 1) % 100:02d}")
