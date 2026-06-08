"""NBA game/player context source helpers."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def player_position_query() -> TextClause:
    """Build query for a player's latest position group before an as-of date."""
    return text("""
        SELECT position_group FROM player_position_history
        WHERE player_id = :player_id AND snapshot_date < :as_of_date
        ORDER BY snapshot_date DESC LIMIT 1
    """)


def context_snapshots_query() -> TextClause:
    """Build historical game/team/opponent/home/position context query."""
    return text("""
        SELECT
            pgs.team_id, pgs.season_id, tgs.opponent_id,
            CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home,
            (SELECT position_group FROM player_position_history ph
             WHERE ph.player_id = :player_id AND ph.snapshot_date < :as_of_date
             ORDER BY ph.snapshot_date DESC LIMIT 1) as position_group
        FROM player_game_stats pgs
        JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
        WHERE pgs.game_id = :game_id AND pgs.player_id = :player_id
    """)


def player_position_from_row(row) -> str | None:
    """Map a position query row into the legacy return value."""
    if row is None:
        return None
    return row[0]


def _row_mapping(row) -> dict:
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return vars(row)


def get_context_snapshots_from_row(row) -> dict | None:
    """Map a context snapshot row into the legacy context dict or None."""
    if row is None:
        return None
    mapping = _row_mapping(row)
    return mapping if mapping.get("position_group") else None


def get_player_position(conn, player_id: int, as_of_date) -> str | None:
    """Get player's position group from position history."""
    result = conn.execute(
        player_position_query(),
        {"player_id": player_id, "as_of_date": as_of_date},
    ).fetchone()
    return player_position_from_row(result)


def get_context_snapshots(conn, game_id, player_id, as_of_date) -> dict | None:
    """Get historical team/opponent/home/position context for a player-game."""
    result = conn.execute(
        context_snapshots_query(),
        {"game_id": game_id, "player_id": player_id, "as_of_date": as_of_date},
    ).fetchone()
    return get_context_snapshots_from_row(result)
