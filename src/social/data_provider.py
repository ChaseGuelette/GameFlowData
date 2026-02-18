"""Synchronous database queries for social media image data.

Uses src/db/client.get_engine() directly with sync SQLAlchemy —
intentionally does NOT reuse the async Discord services.
"""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import text

from src.db.client import get_engine

logger = logging.getLogger(__name__)


def get_top_picks_sync(
    prediction_date: date,
    n: int = 5,
    min_edge: float = 0.05,
) -> list[dict]:
    """Fetch top N picks for a date, ordered by best edge descending.

    Returns list of dicts with keys: player_id, player_name, stat, line,
    over_edge, under_edge, pred_q50, pred_q10, pred_q90, game_time,
    feat_opp_abbrev.
    """
    query = text("""
        SELECT
            dp.player_id,
            dp.player_name,
            dp.stat,
            dp.line,
            dp.over_edge,
            dp.under_edge,
            dp.pred_q50,
            dp.pred_q10,
            dp.pred_q90,
            dp.game_time,
            dp.feat_opp_abbrev
        FROM daily_predictions dp
        WHERE dp.prediction_date = :prediction_date
          AND (COALESCE(dp.over_edge, 0) >= :min_edge
               OR COALESCE(dp.under_edge, 0) >= :min_edge)
        ORDER BY GREATEST(COALESCE(dp.over_edge, 0), COALESCE(dp.under_edge, 0)) DESC
        LIMIT :n
    """)

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            query,
            {"prediction_date": prediction_date, "min_edge": min_edge, "n": n},
        )
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]


def get_resolved_bets(game_date: date) -> list[dict]:
    """Fetch resolved paper bets for a game date.

    Returns list of dicts with keys: player_id, player_name, stat_type,
    line, bet_direction, status, pnl, actual_value.
    """
    query = text("""
        SELECT
            player_id,
            player_name,
            stat_type,
            line,
            bet_direction,
            status,
            pnl,
            actual_value
        FROM paper_bets
        WHERE game_date = :game_date
          AND resolved_at IS NOT NULL
        ORDER BY player_name, stat_type
    """)

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(query, {"game_date": game_date})
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]


def get_daily_summary(game_date: date) -> dict | None:
    """Fetch the paper_trading_daily_log row for a game date.

    Returns dict with keys: bets_won, bets_lost, bets_push, total_pnl,
    total_bets — or None if no log exists.
    """
    query = text("""
        SELECT
            total_bets,
            bets_won,
            bets_lost,
            bets_push,
            total_pnl
        FROM paper_trading_daily_log
        WHERE game_date = :game_date
    """)

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(query, {"game_date": game_date}).fetchone()

    if row is None:
        return None

    return {
        "total_bets": row[0],
        "bets_won": row[1],
        "bets_lost": row[2],
        "bets_push": row[3],
        "total_pnl": float(row[4]) if row[4] else 0.0,
    }


def get_performance_stats_sync(days: int = 30) -> dict:
    """Aggregate season performance from paper_bets (last N days).

    Returns dict with keys: win_rate, roi, total_bets.
    """
    query = text("""
        SELECT
            COUNT(*) AS total_bets,
            SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN status = 'push' THEN 1 ELSE 0 END) AS pushes,
            COALESCE(SUM(pnl), 0) AS total_pnl,
            COALESCE(SUM(stake), 0) AS total_staked
        FROM paper_bets
        WHERE resolved_at IS NOT NULL
          AND game_date >= CURRENT_DATE - :days
    """)

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(query, {"days": days}).fetchone()

    total = row[0] or 0
    wins = row[1] or 0
    losses = row[2] or 0
    total_pnl = float(row[4]) if row[4] else 0.0
    total_staked = float(row[5]) if row[5] else 0.0

    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    roi = total_pnl / total_staked if total_staked > 0 else 0.0

    return {
        "win_rate": win_rate,
        "roi": roi,
        "total_bets": total,
    }
