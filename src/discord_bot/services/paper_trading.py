"""Paper trading query services for Discord bot."""

import asyncio
import logging
from datetime import date, timedelta

from sqlalchemy import text

from src.db.client import get_engine

logger = logging.getLogger(__name__)


async def get_bankroll_summary() -> dict | None:
    """Get current bankroll summary.

    Returns:
        Dict with balance, daily_pnl, total_pnl, or None if no data
    """
    def _query():
        engine = get_engine()

        query = """
            SELECT
                game_date,
                bankroll_after as balance,
                total_pnl as daily_pnl,
                cumulative_pnl as total_pnl
            FROM paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT 1
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()

        if not row:
            # Return default values if no paper trading history
            return {
                "date": date.today(),
                "balance": 1000.0,
                "daily_pnl": 0.0,
                "total_pnl": 0.0,
            }

        return {
            "date": row[0],
            "balance": float(row[1]) if row[1] else 1000.0,
            "daily_pnl": float(row[2]) if row[2] else 0.0,
            "total_pnl": float(row[3]) if row[3] else 0.0,
        }

    return await asyncio.to_thread(_query)


async def get_performance_stats(days: int = 30) -> dict:
    """Get performance statistics over a period.

    Args:
        days: Number of days to look back

    Returns:
        Dict with win_rate, roi, total_bets, wins, losses, best_stat
    """
    def _query():
        engine = get_engine()

        cutoff_date = date.today() - timedelta(days=days)

        # Overall stats
        overall_query = """
            SELECT
                COUNT(*) as total_bets,
                SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN status = 'push' THEN 1 ELSE 0 END) as pushes,
                SUM(pnl) as total_pnl,
                SUM(stake) as total_staked
            FROM paper_bets
            WHERE resolved_at IS NOT NULL
              AND game_date >= :cutoff_date
        """

        with engine.connect() as conn:
            result = conn.execute(text(overall_query), {"cutoff_date": cutoff_date})
            overall = result.fetchone()

        if not overall or overall[0] == 0:
            return {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "win_rate": 0.0,
                "roi": 0.0,
                "total_pnl": 0.0,
                "best_stat": None,
                "best_stat_roi": 0.0,
            }

        total_bets = overall[0]
        wins = overall[1] or 0
        losses = overall[2] or 0
        pushes = overall[3] or 0
        total_pnl = float(overall[4]) if overall[4] else 0.0
        total_staked = float(overall[5]) if overall[5] else 0.0

        # Calculate win rate (excluding pushes)
        bets_with_result = wins + losses
        win_rate = wins / bets_with_result if bets_with_result > 0 else 0.0

        # Calculate ROI
        roi = total_pnl / total_staked if total_staked > 0 else 0.0

        # Per-stat breakdown to find best performer
        stat_query = """
            SELECT
                stat_type as stat,
                SUM(pnl) as stat_pnl,
                SUM(stake) as stat_staked,
                COUNT(*) as stat_bets
            FROM paper_bets
            WHERE resolved_at IS NOT NULL
              AND game_date >= :cutoff_date
            GROUP BY stat_type
            ORDER BY SUM(pnl) / NULLIF(SUM(stake), 0) DESC
            LIMIT 1
        """

        with engine.connect() as conn:
            result = conn.execute(text(stat_query), {"cutoff_date": cutoff_date})
            best_stat_row = result.fetchone()

        best_stat = None
        best_stat_roi = 0.0
        if best_stat_row and best_stat_row[2]:
            best_stat = best_stat_row[0]
            best_stat_roi = float(best_stat_row[1]) / float(best_stat_row[2])

        return {
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": win_rate,
            "roi": roi,
            "total_pnl": total_pnl,
            "best_stat": best_stat,
            "best_stat_roi": best_stat_roi,
        }

    return await asyncio.to_thread(_query)


async def get_recent_bets(limit: int = 10) -> list[dict]:
    """Get recent resolved bets.

    Args:
        limit: Max number of bets to return

    Returns:
        List of recent bet dicts
    """
    def _query():
        engine = get_engine()

        query = """
            SELECT
                pb.game_date,
                pb.player_name,
                pb.stat_type as stat,
                pb.line,
                pb.bet_direction as side,
                pb.odds_at_bet as odds,
                pb.stake,
                pb.status as result,
                pb.pnl,
                pb.actual_value
            FROM paper_bets pb
            WHERE pb.resolved_at IS NOT NULL
            ORDER BY pb.resolved_at DESC
            LIMIT :limit
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            rows = result.fetchall()
            columns = result.keys()

        return [dict(zip(columns, row)) for row in rows]

    return await asyncio.to_thread(_query)


async def get_pending_bets() -> list[dict]:
    """Get all pending (unresolved) bets.

    Returns:
        List of pending bet dicts
    """
    def _query():
        engine = get_engine()

        query = """
            SELECT
                pb.game_date,
                pb.player_name,
                pb.stat_type as stat,
                pb.line,
                pb.bet_direction as side,
                pb.odds_at_bet as odds,
                pb.stake,
                pb.edge
            FROM paper_bets pb
            WHERE pb.resolved_at IS NULL
            ORDER BY pb.game_date DESC, pb.player_name
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()

        return [dict(zip(columns, row)) for row in rows]

    return await asyncio.to_thread(_query)
