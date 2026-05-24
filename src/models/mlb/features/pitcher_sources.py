"""Pitcher-specific source query boundaries for MLB feature stores."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


@dataclass(frozen=True)
class SourceQuery:
    sql: TextClause
    params: dict[str, object]


def build_pitcher_rolling_stats_query(*, player_id: int, target_game_date: str) -> SourceQuery:
    """Build a point-in-time pitcher rolling-stats query boundary.

    The strict previous-game predicate is intentionally visible here so it can
    be tested without constructing the legacy facade.
    """
    return SourceQuery(
        sql=text("""
            SELECT *
            FROM mlb_player_average_pitching
            WHERE player_id = :player_id
              AND game_date < :target_game_date
            ORDER BY game_date DESC
            LIMIT 1
        """),
        params={"player_id": player_id, "target_game_date": target_game_date},
    )


def default_pitcher_source_features() -> dict[str, float]:
    return {
        "pitcher_avg_so_l5": 0.0,
        "pitcher_avg_ip_l5": 0.0,
        "pitcher_avg_k_per_9_l5": 0.0,
    }
