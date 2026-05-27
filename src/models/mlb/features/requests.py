"""Explicit request objects for MLB feature generation modes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class FeatureMode(StrEnum):
    TRAINING = "training"
    DATE_BATCH = "date_batch"
    PLAYER_GAME = "player_game"
    BACKTEST = "backtest"


@dataclass(frozen=True)
class TrainingFeatureRequest:
    seasons: tuple[int, ...]
    stat: str = "pitcher_strikeouts"
    as_of_time: datetime | None = None

    def __post_init__(self) -> None:
        if not self.seasons:
            raise ValueError("TrainingFeatureRequest requires at least one season")


@dataclass(frozen=True)
class DateFeatureRequest:
    game_date: str
    mode: FeatureMode
    as_of_time: datetime | None = None

    def __post_init__(self) -> None:
        if self.mode not in {FeatureMode.DATE_BATCH, FeatureMode.BACKTEST}:
            raise ValueError("DateFeatureRequest mode must be date_batch or backtest")


@dataclass(frozen=True)
class PlayerGameFeatureRequest:
    player_id: int
    game_id: int
    game_date: str
    team_id: int
    opp_team_id: int
    venue_id: int | None = None
    season: int | None = None
    is_home: bool | None = None
    as_of_time: datetime | None = None
