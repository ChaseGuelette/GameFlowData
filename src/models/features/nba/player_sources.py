"""NBA player rolling source helpers."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from src.models.features.nba.transforms import (
    build_player_rolling_features,
    default_player_rolling_features,
)


def player_rolling_stats_query() -> TextClause:
    """Build single-player rolling average query for data before the target game."""
    return text("""
        SELECT
            pags.avg_min_l5, pags.avg_min_l15,
            pags.avg_pts_l5, pags.avg_pts_l15,
            pags.avg_reb_l5, pags.avg_ast_l5,
            pags.avg_fg3m_l5, pags.avg_fg3a_l5,
            paas.avg_usg_pct_l5, paas.avg_ts_pct_l15,
            paas.avg_reb_pct_l5, paas.avg_ast_pct_l5,
            -- B3: L3 averages
            pags.avg_min_l3, pags.avg_pts_l3, pags.avg_reb_l3,
            pags.avg_ast_l3, pags.avg_fg3m_l3,
            pags.avg_min_szn,
            -- B3/B4: L5 standard deviations
            pags.std_min_l5, pags.std_pts_l5, pags.std_reb_l5,
            pags.std_ast_l5, pags.std_fg3m_l5,
            -- B4: Minutes stability
            pags.min_floor_l5, pags.games_started_l5,
            -- B2: Rest/schedule
            pags.rest_days AS stored_rest_days, pags.games_last_7d
        FROM player_average_game_stats pags
        LEFT JOIN LATERAL (
            SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
            FROM player_average_advanced_stats
            WHERE player_id = pags.player_id
              AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        ) paas ON TRUE
        WHERE pags.player_id = :player_id AND pags.game_date < :as_of_date
        ORDER BY pags.game_date DESC LIMIT 1
    """)


def row_to_player_rolling_stats(row) -> dict[str, float | int]:
    """Map a player rolling query row into FeatureStore player feature keys."""
    if row is None:
        return default_player_rolling_features()
    return build_player_rolling_features(row._mapping)


def get_player_rolling_stats(conn, player_id, as_of_date) -> dict[str, float | int]:
    """Fetch and map single-player rolling stats before the target game."""
    result = conn.execute(
        player_rolling_stats_query(),
        {"player_id": player_id, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_player_rolling_stats(result)
