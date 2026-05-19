"""Kalshi trade approval queue service.

This service owns the ``kalshi_trade_queue`` lifecycle seam. It mirrors the
migrated SQL/return shapes so orchestration callers can use it directly without
changing live behavior.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text


def _row_mapping(row: Any) -> dict[str, Any]:
    return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)


def split_executable_approved_rows(
    rows: list[Any], *, now: datetime | None = None
) -> tuple[list[dict[str, Any]], list[int]]:
    """Convert approved queue rows into legacy trade dicts and expired IDs."""
    now = now or datetime.now(UTC)
    if now.tzinfo is not None:
        now = now.astimezone(UTC).replace(tzinfo=None)

    trades: list[dict[str, Any]] = []
    expired_ids: list[int] = []

    for row in rows:
        row_dict = _row_mapping(row)
        expires_at = row_dict.get("expires_at")
        if expires_at is not None:
            comparable_expires_at = expires_at
            if comparable_expires_at.tzinfo is not None:
                comparable_expires_at = comparable_expires_at.astimezone(UTC).replace(tzinfo=None)
            if comparable_expires_at < now:
                expired_ids.append(row_dict["id"])
                continue

        trades.append({
            "game_date": row_dict["game_date"],
            "ticker": row_dict["ticker"],
            "sport": row_dict["sport"],
            "player_id": row_dict["player_id"],
            "player_name": row_dict["player_name"],
            "stat_type": row_dict["stat_type"],
            "line": float(row_dict["line"]),
            "side": row_dict["side"],
            "yes_price": row_dict["yes_price"],
            "contracts": row_dict["contracts"],
            "expected_cost": float(row_dict["expected_cost"]),
            "expected_fee": float(row_dict["expected_fee"]) if row_dict.get("expected_fee") else 0,
            "model_prob": float(row_dict["model_prob"]) if row_dict.get("model_prob") else 0,
            "kalshi_implied": float(row_dict["kalshi_implied"]) if row_dict.get("kalshi_implied") else 0,
            "edge": float(row_dict["edge"]) if row_dict.get("edge") else 0,
            "fee_adjusted_edge": float(row_dict["fee_adjusted_edge"]) if row_dict.get("fee_adjusted_edge") else 0,
            "_queue_id": row_dict["id"],
        })

    return trades, expired_ids


class KalshiQueueService:
    """Persistence service for the human approval trade queue."""

    def __init__(self, engine: Any):
        self.engine = engine

    def propose_trades(self, trades: list[dict[str, Any]]) -> int:
        """Insert selected trades into ``kalshi_trade_queue`` for approval."""
        if not trades:
            return 0

        with self.engine.begin() as conn:
            for trade in trades:
                conn.execute(text("""
                    INSERT INTO kalshi_trade_queue (
                        game_date, ticker, sport, player_id, player_name,
                        stat_type, line, side, yes_price, contracts,
                        expected_cost, expected_fee, model_prob, kalshi_implied,
                        edge, fee_adjusted_edge, sportsbook_consensus_line,
                        status, expires_at
                    ) VALUES (
                        :game_date, :ticker, :sport, :player_id, :player_name,
                        :stat_type, :line, :side, :yes_price, :contracts,
                        :expected_cost, :expected_fee, :model_prob, :kalshi_implied,
                        :edge, :fee_adjusted_edge, :sportsbook_consensus_line,
                        'pending_approval',
                        now() + interval '30 minutes'
                    )
                """), {
                    "game_date": trade["game_date"],
                    "ticker": trade["ticker"],
                    "sport": trade["sport"],
                    "player_id": trade.get("player_id"),
                    "player_name": trade.get("player_name"),
                    "stat_type": trade["stat_type"],
                    "line": trade["line"],
                    "side": trade["side"],
                    "yes_price": trade["yes_price"],
                    "contracts": trade["contracts"],
                    "expected_cost": trade["expected_cost"],
                    "expected_fee": trade.get("expected_fee"),
                    "model_prob": trade["model_prob"],
                    "kalshi_implied": trade["kalshi_implied"],
                    "edge": trade["edge"],
                    "fee_adjusted_edge": trade["fee_adjusted_edge"],
                    "sportsbook_consensus_line": trade.get("sportsbook_consensus_line"),
                })
        return len(trades)

    def renew_expired_pending_trades(self, target_date, sport: str) -> int:
        """Extend recent expired pending-approval trades while markets are open."""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                UPDATE kalshi_trade_queue q
                SET expires_at = now() + interval '30 minutes'
                WHERE q.game_date = :d
                  AND q.sport = :sport
                  AND q.status = 'pending_approval'
                  AND q.expires_at BETWEEN now() - interval '60 minutes' AND now()
                  AND EXISTS (
                      SELECT 1 FROM kalshi_markets m
                      WHERE m.ticker = q.ticker
                        AND m.market_status = 'open'
                        AND m.snapshot_time >= now() - interval '15 minutes'
                  )
            """), {"d": target_date, "sport": sport})
        return result.rowcount

    def fetch_pending_approval_trades(self, target_date, sport: str) -> list[dict[str, Any]]:
        """Fetch visible pending-approval trades for approval/reminder alerts."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT player_name, stat_type, side, contracts,
                       expected_cost, fee_adjusted_edge
                FROM kalshi_trade_queue
                WHERE sport = :sport
                  AND game_date = :d
                  AND status = 'pending_approval'
                  AND expires_at > now()
                ORDER BY proposed_at ASC
            """), {"sport": sport, "d": target_date}).fetchall()
        return [_row_mapping(row) for row in rows]

    def fetch_approved_rows(self, trade_ids: list[int]) -> list[Any]:
        """Fetch approved queue rows by ID for execution."""
        if not trade_ids:
            return []
        with self.engine.connect() as conn:
            return conn.execute(text("""
                SELECT id, game_date, ticker, sport, player_id, player_name,
                       stat_type, line, side, yes_price, contracts,
                       expected_cost, expected_fee, model_prob, kalshi_implied,
                       edge, fee_adjusted_edge, expires_at
                FROM kalshi_trade_queue
                WHERE id = ANY(:ids) AND status = 'approved'
            """), {"ids": trade_ids}).fetchall()

    def mark_expired_trade_ids(self, trade_ids: list[int]) -> None:
        """Mark approved/pending queue rows expired."""
        if not trade_ids:
            return
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE kalshi_trade_queue SET status = 'expired'
                WHERE id = ANY(:ids)
            """), {"ids": trade_ids})

    def mark_execution_results(self, trades: list[dict[str, Any]], results: list[dict[str, Any]]) -> None:
        """Mark queue rows executed when an order was placed, otherwise failed."""
        if not trades:
            return
        executed_tickers = {result["ticker"] for result in results}
        with self.engine.begin() as conn:
            for trade in trades:
                queue_id = trade["_queue_id"]
                if trade["ticker"] in executed_tickers:
                    conn.execute(text("""
                        UPDATE kalshi_trade_queue
                        SET status = 'executed', executed_at = now()
                        WHERE id = :id
                    """), {"id": queue_id})
                else:
                    conn.execute(text("""
                        UPDATE kalshi_trade_queue
                        SET status = 'failed'
                        WHERE id = :id
                    """), {"id": queue_id})
