"""NBA injury-context source helpers.

This module owns injury-context defaults, SQL query builders, and row mapping for
NBA FeatureStore compatibility paths. It preserves the current production
semantics while moving injury source ownership out of the god-class facade.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def default_injury_context() -> dict[str, float | int]:
    """Default injury feature values when injury source rows are unavailable."""
    return {
        "team_out_count": 0,
        "team_out_min_sum": 0,
        "team_out_pts_sum": 0,
        "team_out_reb_sum": 0,
        "team_out_ast_sum": 0,
        "team_out_usg_sum": 0,
        "opp_out_count": 0,
        "opp_out_min_sum": 0,
        "player_is_questionable": 0,
        "player_is_probable": 0,
        "team_out_same_pos_count": 0,
        "team_out_same_pos_min_sum": 0,
        "team_out_same_pos_usg_sum": 0,
        "team_out_same_pos_starter_sum": 0,
    }


def status_flags(status: str | None) -> dict[str, int]:
    """Map injury status text into FeatureStore questionable/probable flags."""
    return {
        "player_is_questionable": 1 if status == "Questionable" else 0,
        "player_is_probable": 1 if status == "Probable" else 0,
    }


def _as_float(value) -> float:
    return float(value or 0)


def build_injury_context(
    *,
    team_game_result=None,
    team_advanced_result=None,
    opponent_result=None,
    player_status_result=None,
    same_position_result=None,
) -> dict[str, float | int]:
    """Build FeatureStore injury-context features from fetched query rows."""
    context = default_injury_context()

    if team_game_result:
        context["team_out_count"] = team_game_result.out_count or 0
        context["team_out_min_sum"] = _as_float(team_game_result.out_min_sum)
        context["team_out_pts_sum"] = _as_float(team_game_result.out_pts_sum)
        context["team_out_reb_sum"] = _as_float(team_game_result.out_reb_sum)
        context["team_out_ast_sum"] = _as_float(team_game_result.out_ast_sum)

    if team_advanced_result:
        context["team_out_usg_sum"] = _as_float(team_advanced_result.out_usg_sum)

    if opponent_result:
        context["opp_out_count"] = opponent_result.out_count or 0
        context["opp_out_min_sum"] = _as_float(opponent_result.out_min_sum)

    if player_status_result:
        context.update(status_flags(player_status_result.status))

    if same_position_result:
        context["team_out_same_pos_count"] = same_position_result.out_count or 0
        context["team_out_same_pos_min_sum"] = _as_float(same_position_result.out_min_sum)
        context["team_out_same_pos_usg_sum"] = _as_float(same_position_result.out_usg_sum)
        context["team_out_same_pos_starter_sum"] = _as_float(same_position_result.out_starter_sum)

    return context


def team_injury_game_query() -> TextClause:
    """Build team OUT-player game-stat injury aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
            COALESCE(SUM(inj_sub.avg_pts_l5), 0) as out_pts_sum,
            COALESCE(SUM(inj_sub.avg_reb_l5), 0) as out_reb_sum,
            COALESCE(SUM(inj_sub.avg_ast_l5), 0) as out_ast_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5, pags_inj.avg_pts_l5,
                pags_inj.avg_reb_l5, pags_inj.avg_ast_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def team_injury_advanced_query() -> TextClause:
    """Build team OUT-player advanced-stat injury aggregate query."""
    return text("""
        SELECT
            COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                paas_inj.avg_usg_pct_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_advanced_stats paas_inj
                ON paas_inj.player_id = ri.player_id
                AND paas_inj.game_date < :game_date
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, paas_inj.game_date DESC
        ) inj_sub
    """)


def opponent_injury_query() -> TextClause:
    """Build opponent OUT-player injury aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            WHERE ri.nba_team_id = :opponent_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def player_injury_status_query() -> TextClause:
    """Build latest player injury-status query for one player/date."""
    return text("""
        SELECT status
        FROM rapidapi_injuries
        WHERE player_id = :player_id
          AND report_date = :game_date
        ORDER BY id DESC LIMIT 1
    """)


def same_position_injury_query() -> TextClause:
    """Build same-position teammate OUT-player opportunity aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
            COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum,
            COALESCE(SUM(LEAST(inj_sub.games_started_l5 / 5.0, 1.0)), 0) as out_starter_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5,
                pags_inj.games_started_l5,
                paas_inj.avg_usg_pct_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            LEFT JOIN player_average_advanced_stats paas_inj
                ON paas_inj.player_id = ri.player_id
                AND paas_inj.game_date < :game_date
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history
                WHERE player_id = ri.player_id
                  AND snapshot_date < :game_date
                ORDER BY snapshot_date DESC LIMIT 1
            ) pph ON TRUE
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
              AND ri.player_id != :player_id
              AND pph.position_group = :position_group
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def bulk_injury_features_query() -> TextClause:
    """Build bulk OUT-player injury feature source query for training enrichment."""
    return text("""
        SELECT
            ri.nba_team_id,
            ri.report_date,
            ri.player_id as inj_player_id,
            pags_l.avg_min_l5,
            pags_l.avg_pts_l5,
            pags_l.avg_reb_l5,
            pags_l.avg_ast_l5,
            pags_l.games_started_l5,
            paas_l.avg_usg_pct_l5,
            pph.position_group as inj_position_group
        FROM (
            SELECT DISTINCT ON (nba_team_id, report_date, player_id)
                nba_team_id, report_date, player_id
            FROM rapidapi_injuries
            WHERE status = 'Out'
              AND player_id IS NOT NULL
              AND report_date = ANY(:dates)
            ORDER BY nba_team_id, report_date, player_id, id DESC
        ) ri
        LEFT JOIN LATERAL (
            SELECT avg_min_l5, avg_pts_l5, avg_reb_l5, avg_ast_l5, games_started_l5
            FROM player_average_game_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) pags_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT avg_usg_pct_l5
            FROM player_average_advanced_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) paas_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT position_group
            FROM player_position_history
            WHERE player_id = ri.player_id
              AND snapshot_date < ri.report_date
            ORDER BY snapshot_date DESC LIMIT 1
        ) pph ON TRUE
    """)


def player_injury_status_bulk_query() -> TextClause:
    """Build bulk latest player injury-status query for training enrichment."""
    return text("""
        SELECT DISTINCT ON (player_id, report_date)
            player_id, report_date, status as inj_status
        FROM rapidapi_injuries
        WHERE player_id IS NOT NULL
          AND report_date = ANY(:dates)
        ORDER BY player_id, report_date, id DESC
    """)


TEAM_INJURY_AGG_COLUMNS = [
    "nba_team_id",
    "report_date",
    "out_count",
    "out_min_sum",
    "out_pts_sum",
    "out_reb_sum",
    "out_ast_sum",
    "out_usg_sum",
]


def empty_team_injury_aggregation() -> pd.DataFrame:
    """Return the current empty bulk team-injury aggregation schema."""
    return pd.DataFrame(columns=TEAM_INJURY_AGG_COLUMNS)


def aggregate_team_injury_features(inj_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bulk per-injured-player rows by NBA team and report date."""
    if inj_df.empty:
        return empty_team_injury_aggregation()

    return inj_df.groupby(["nba_team_id", "report_date"]).agg(
        out_count=("inj_player_id", "nunique"),
        out_min_sum=("avg_min_l5", lambda x: np.nansum(x)),
        out_pts_sum=("avg_pts_l5", lambda x: np.nansum(x)),
        out_reb_sum=("avg_reb_l5", lambda x: np.nansum(x)),
        out_ast_sum=("avg_ast_l5", lambda x: np.nansum(x)),
        out_usg_sum=("avg_usg_pct_l5", lambda x: np.nansum(x)),
    ).reset_index()


def load_injury_features_bulk(engine, game_dates: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pre-aggregate injury features for all (team, date) pairs in one pass."""
    with engine.connect() as conn:
        inj_df = pd.read_sql(
            bulk_injury_features_query(),
            conn,
            params={"dates": list(set(game_dates))},
        )

    return aggregate_team_injury_features(inj_df), inj_df


def load_player_injury_status_bulk(engine, game_dates: list) -> pd.DataFrame:
    """Load latest player injury statuses for all (player, date) pairs."""
    with engine.connect() as conn:
        return pd.read_sql(
            player_injury_status_bulk_query(),
            conn,
            params={"dates": list(set(game_dates))},
        )


def get_injury_context(
    conn,
    player_id,
    team_id,
    opponent_id,
    game_date,
    player_position_group: str | None = None,
) -> dict[str, float | int]:
    """Fetch injury context for one player/game feature row."""
    team_game_result = conn.execute(
        team_injury_game_query(),
        {"team_id": team_id, "game_date": game_date},
    ).fetchone()
    team_advanced_result = conn.execute(
        team_injury_advanced_query(),
        {"team_id": team_id, "game_date": game_date},
    ).fetchone()
    opponent_result = conn.execute(
        opponent_injury_query(),
        {"opponent_id": opponent_id, "game_date": game_date},
    ).fetchone()
    player_status_result = conn.execute(
        player_injury_status_query(),
        {"player_id": player_id, "game_date": game_date},
    ).fetchone()

    same_position_result = None
    if player_position_group:
        same_position_result = conn.execute(
            same_position_injury_query(),
            {
                "team_id": team_id,
                "game_date": game_date,
                "player_id": player_id,
                "position_group": player_position_group,
            },
        ).fetchone()

    return build_injury_context(
        team_game_result=team_game_result,
        team_advanced_result=team_advanced_result,
        opponent_result=opponent_result,
        player_status_result=player_status_result,
        same_position_result=same_position_result,
    )
