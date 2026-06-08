"""NBA opponent positional-defense source helpers."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

OPPONENT_POSITIONAL_DEFAULTS = {
    "off_rtg_allowed_l5": 112.0,
    "reb_allowed_l5": 0,
    "ast_allowed_l5": 0,
    "threes_allowed_l5": 0,
    "threes_per100_allowed_l5": 0,
    "reb_per100_allowed_l5": 0,
    "ast_per100_allowed_l5": 0,
    "off_rtg_allowed_l15": 112.0,
    "reb_allowed_l15": 0,
    "ast_allowed_l15": 0,
    "threes_allowed_l15": 0,
}


def opponent_positional_stats_query() -> TextClause:
    """Build query for latest opponent defense allowed by position before target game."""
    return text("""
        SELECT
            off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
            threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
            off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
        FROM team_allowed_by_position
        WHERE team_id = :opponent_id
          AND position_group = :position_group
          AND game_date < :as_of_date
        ORDER BY game_date DESC LIMIT 1
    """)


def default_opponent_positional_stats() -> dict[str, float | int]:
    """Return legacy opponent positional-defense defaults."""
    return {f"opp_pos_{key}": value for key, value in OPPONENT_POSITIONAL_DEFAULTS.items()}


def _row_mapping(row) -> dict:
    if hasattr(row, "_mapping"):
        return row._mapping
    return vars(row)


def row_to_opponent_positional_stats(row) -> dict[str, float | int]:
    """Map an opponent positional-defense row into FeatureStore keys."""
    if row is None:
        return default_opponent_positional_stats()
    return {
        f"opp_pos_{key}": value if value is not None else OPPONENT_POSITIONAL_DEFAULTS.get(key, 0)
        for key, value in _row_mapping(row).items()
    }


def get_opponent_positional_stats(conn, opponent_id, position_group, as_of_date) -> dict[str, float | int]:
    """Fetch opponent's positional defense stats. Returns defaults if not found."""
    result = conn.execute(
        opponent_positional_stats_query(),
        {"opponent_id": opponent_id, "position_group": position_group, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_opponent_positional_stats(result)
