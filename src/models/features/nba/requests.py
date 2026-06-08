"""NBA feature-loader request objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class PlayerGameFeatureRequest:
    """Request for single player-game feature inference."""

    player_id: int
    game_id: str
    as_of_date: date
    team_id: int | None = None
    opponent_id: int | None = None
    is_home: bool | None = None

    @property
    def is_scheduled_context(self) -> bool:
        """Whether caller supplied scheduled-game team/opponent context."""
        return self.team_id is not None and self.opponent_id is not None


@dataclass(frozen=True)
class DateFeatureRequest:
    """Request for all NBA features on one game date."""

    game_date: date


@dataclass(frozen=True)
class DateRangeFeatureRequest:
    """Request for NBA features across an inclusive date range."""

    start_date: date
    end_date: date


@dataclass(frozen=True)
class TrainingFeatureRequest:
    """Request for NBA training features across seasons."""

    seasons: list[str]
