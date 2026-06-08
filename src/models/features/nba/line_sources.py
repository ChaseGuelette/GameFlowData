"""NBA betting line source helpers.

This module owns the SQL/query defaults for NBA game lines and player prop-line
features used by the FeatureStore compatibility facade. These feature lines are
model-input centering features and intentionally remain separate from downstream
edge-calculation line selection.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def default_game_lines() -> dict[str, float | int]:
    """Default game-line feature values when no quote row is available."""
    return {"line_spread_raw": 0, "line_total": 0}


def default_player_prop_lines() -> dict[str, float | int]:
    """Default player prop-line feature values when no quote row is available."""
    return {
        "prop_line_pts": 0,
        "prop_line_reb": 0,
        "prop_line_ast": 0,
        "prop_line_threes": 0,
    }


def game_lines_query() -> TextClause:
    """Build the current as-of/pre-game NBA game-line feature query."""
    return text("""
        SELECT
            MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
            MAX(CASE WHEN market_key = 'totals' THEN line END) as total
        FROM raw_game_lines_staging
        WHERE nba_game_id = :game_id
          AND bookmaker IN ('pinnacle', 'draftkings')
          AND (
              :as_of_date IS NULL
              OR COALESCE(snapshot_time, inserted_at)::date <= :as_of_date
          )
          AND (commence_time IS NULL OR COALESCE(snapshot_time, inserted_at) < commence_time)
    """)


def player_prop_lines_query() -> TextClause:
    """Build the current as-of/pre-game NBA player prop-line feature query."""
    return text("""
        SELECT
            MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
            MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
            MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
            MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
        FROM (
            SELECT DISTINCT ON (market_key) market_key, line
            FROM raw_player_props_combined
            WHERE player_id = :player_id
              AND game_id = :game_id
              AND bookmaker IN ('pinnacle', 'draftkings')
              AND (
                  :as_of_date IS NULL
                  OR COALESCE(snapshot_time, inserted_at)::date <= :as_of_date
              )
              AND (commence_time IS NULL OR COALESCE(snapshot_time, inserted_at) < commence_time)
            ORDER BY market_key, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST
        ) sub
    """)


def row_to_game_lines(row) -> dict[str, float | int]:
    """Map a raw SQL row into FeatureStore game-line feature keys."""
    if row is None:
        return default_game_lines()
    return {
        "line_spread_raw": row.spread if row.spread else 0,
        "line_total": row.total if row.total else 0,
    }


def row_to_player_prop_lines(row) -> dict[str, float | int]:
    """Map a raw SQL row into FeatureStore prop-line feature keys."""
    if row is None:
        return default_player_prop_lines()
    return {
        "prop_line_pts": row.prop_line_pts or 0,
        "prop_line_reb": row.prop_line_reb or 0,
        "prop_line_ast": row.prop_line_ast or 0,
        "prop_line_threes": row.prop_line_threes or 0,
    }


def get_game_lines(conn, game_id, as_of_date=None) -> dict[str, float | int]:
    """Fetch NBA spread/total feature lines using only as-of, pregame quotes."""
    result = conn.execute(
        game_lines_query(),
        {"game_id": game_id, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_game_lines(result)


def get_player_prop_lines(conn, player_id, game_id, as_of_date=None) -> dict[str, float | int]:
    """Fetch per-stat NBA prop-line features using only as-of, pregame quotes."""
    result = conn.execute(
        player_prop_lines_query(),
        {"player_id": player_id, "game_id": game_id, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_player_prop_lines(result)
