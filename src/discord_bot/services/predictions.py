"""Prediction query services for Discord bot."""

import asyncio
import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine

logger = logging.getLogger(__name__)


async def get_predictions_for_date(
    prediction_date: date | None = None,
    min_edge: float = 0.05,
    stat_filter: str | None = None,
    limit: int = 10,
) -> list[dict]:
    """Get predictions for a date with optional filters.

    Args:
        prediction_date: Date to query (defaults to today)
        min_edge: Minimum edge threshold (over OR under)
        stat_filter: Filter by stat type ('pts', 'reb', 'ast', or None for all)
        limit: Max number of results

    Returns:
        List of prediction dicts sorted by edge magnitude
    """
    if prediction_date is None:
        prediction_date = date.today()

    def _query():
        engine = get_engine()

        conditions = ["dp.prediction_date = :prediction_date"]
        conditions.append("(dp.over_edge >= :min_edge OR dp.under_edge >= :min_edge)")

        params = {
            "prediction_date": prediction_date,
            "min_edge": min_edge,
        }

        if stat_filter and stat_filter.lower() != "all":
            conditions.append("dp.stat = :stat")
            params["stat"] = stat_filter.lower()

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                dp.player_id,
                dp.player_name,
                dp.stat,
                dp.line,
                dp.over_edge,
                dp.under_edge,
                dp.over_prob,
                dp.under_prob,
                dp.pred_q50,
                dp.game_time,
                t.team_name as team,
                dp.feat_opp_abbrev as opponent
            FROM daily_predictions dp
            LEFT JOIN teams t ON dp.team_id = t.team_id
            WHERE {where_clause}
            ORDER BY GREATEST(COALESCE(dp.over_edge, 0), COALESCE(dp.under_edge, 0)) DESC
            LIMIT :limit
        """
        params["limit"] = limit

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            columns = result.keys()

        return [dict(zip(columns, row)) for row in rows]

    return await asyncio.to_thread(_query)


async def get_player_predictions(
    player_name: str,
    prediction_date: date | None = None,
) -> list[dict]:
    """Get all predictions for a specific player.

    Args:
        player_name: Player name (fuzzy matched)
        prediction_date: Date to query (defaults to today)

    Returns:
        List of prediction dicts for the player
    """
    if prediction_date is None:
        prediction_date = date.today()

    def _query():
        engine = get_engine()

        # First, fuzzy match the player
        player_query = """
            SELECT player_id, player_name
            FROM players
            WHERE LOWER(player_name) LIKE :pattern
            ORDER BY
                CASE WHEN LOWER(player_name) = :exact THEN 0 ELSE 1 END,
                player_name
            LIMIT 1
        """
        pattern = f"%{player_name.lower()}%"

        with engine.connect() as conn:
            result = conn.execute(text(player_query), {
                "pattern": pattern,
                "exact": player_name.lower(),
            })
            player_row = result.fetchone()

        if not player_row:
            return []

        player_id = player_row[0]
        matched_name = player_row[1]

        # Now get predictions for this player
        pred_query = """
            SELECT
                dp.player_id,
                dp.player_name,
                dp.stat,
                dp.line,
                dp.over_edge,
                dp.under_edge,
                dp.over_prob,
                dp.under_prob,
                dp.pred_q50,
                dp.pred_q10,
                dp.pred_q90,
                dp.game_time,
                t.team_name as team,
                dp.feat_opp_abbrev as opponent
            FROM daily_predictions dp
            LEFT JOIN teams t ON dp.team_id = t.team_id
            WHERE dp.prediction_date = :prediction_date
              AND dp.player_id = :player_id
            ORDER BY dp.stat
        """

        with engine.connect() as conn:
            result = conn.execute(text(pred_query), {
                "prediction_date": prediction_date,
                "player_id": player_id,
            })
            rows = result.fetchall()
            columns = result.keys()

        predictions = [dict(zip(columns, row)) for row in rows]

        # Add the matched name to help with display
        for pred in predictions:
            pred["matched_name"] = matched_name

        return predictions

    return await asyncio.to_thread(_query)


async def get_top_picks(
    prediction_date: date | None = None,
    n: int = 5,
    min_edge: float = 0.09,
) -> list[dict]:
    """Get top N picks by edge for alerts.

    Args:
        prediction_date: Date to query (defaults to today)
        n: Number of top picks to return
        min_edge: Minimum edge threshold

    Returns:
        List of top picks sorted by edge
    """
    return await get_predictions_for_date(
        prediction_date=prediction_date,
        min_edge=min_edge,
        stat_filter=None,
        limit=n,
    )


async def search_players(query: str, limit: int = 5) -> list[dict]:
    """Search for players by name.

    Args:
        query: Search string
        limit: Max results

    Returns:
        List of matching players with id and name
    """
    def _query():
        engine = get_engine()

        search_query = """
            SELECT player_id, player_name
            FROM players
            WHERE LOWER(player_name) LIKE :pattern
            ORDER BY player_name
            LIMIT :limit
        """

        with engine.connect() as conn:
            result = conn.execute(text(search_query), {
                "pattern": f"%{query.lower()}%",
                "limit": limit,
            })
            rows = result.fetchall()

        return [{"player_id": row[0], "player_name": row[1]} for row in rows]

    return await asyncio.to_thread(_query)
