"""NBA team rolling source helpers."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

TEAM_ROLLING_DEFAULTS = {
    "avg_pace_l5": 99.5,
    "avg_def_rtg_l5": 112.0,
    "avg_fg3a_l5": 34.0,
    "avg_fg3_pct_l5": 0.36,
}


def team_rolling_stats_query() -> TextClause:
    """Build query for latest team rolling stats before an as-of date."""
    return text("""
        SELECT avg_pace_l5, avg_def_rtg_l5, avg_fg3a_l5, avg_fg3_pct_l5
        FROM team_average_game_stats
        WHERE team_id = :team_id AND game_date < :as_of_date
        ORDER BY game_date DESC LIMIT 1
    """)


def default_team_rolling_stats(prefix: str) -> dict[str, float]:
    """Return prefixed default team/opponent rolling stats."""
    return {f"{prefix}_{key}": value for key, value in TEAM_ROLLING_DEFAULTS.items()}


def _row_mapping(row) -> dict:
    if hasattr(row, "_mapping"):
        return row._mapping
    return vars(row)


def row_to_team_rolling_stats(row, prefix: str) -> dict[str, float]:
    """Map a team rolling query row into prefixed FeatureStore keys."""
    if row is None:
        return default_team_rolling_stats(prefix)
    return {
        f"{prefix}_{key}": value if value is not None else TEAM_ROLLING_DEFAULTS.get(key, 0)
        for key, value in _row_mapping(row).items()
    }


def get_team_rolling_stats(conn, team_id, as_of_date, *, is_opponent: bool = False) -> dict[str, float]:
    """Fetch latest team/opponent rolling stats before an as-of date."""
    prefix = "opp" if is_opponent else "team"
    result = conn.execute(
        team_rolling_stats_query(),
        {"team_id": team_id, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_team_rolling_stats(result, prefix=prefix)
