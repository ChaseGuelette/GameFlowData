"""Batter-specific source query boundaries for MLB feature stores."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from src.models.mlb.features.contracts import BATTER_STAT_MARKET_KEY, BATTER_STAT_TARGET


@dataclass(frozen=True)
class SourceQuery:
    sql: TextClause
    params: dict[str, object]


def build_batter_rolling_stats_query(*, player_id: int, target_game_date: str) -> SourceQuery:
    """Build a point-in-time batter rolling-stats query boundary."""
    return SourceQuery(
        sql=text("""
            SELECT *
            FROM mlb_player_average_batting
            WHERE player_id = :player_id
              AND game_date < :target_game_date
            ORDER BY game_date DESC
            LIMIT 1
        """),
        params={"player_id": player_id, "target_game_date": target_game_date},
    )


def target_for_stat(stat: str) -> str:
    return BATTER_STAT_TARGET[stat]


def market_key_for_stat(stat: str) -> str:
    return BATTER_STAT_MARKET_KEY[stat]


def default_batter_source_features() -> dict[str, float]:
    return {
        "batter_avg_h_l5": 0.0,
        "batter_avg_ab_l5": 3.5,
        "is_same_hand": 0.0,
        "opp_bullpen_ip_last_3d": 0.0,
    }
