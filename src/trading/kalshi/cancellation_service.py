"""Kalshi stale-order detection and cancellation execution service.

This service owns the ``kalshi_cancel_queue`` seam.  Detection/enqueue is the
human-review side of the flow; execution is only for rows already approved by a
human in the dashboard.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import text

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
_TICKER_DT_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})(\d{4})[A-Z]")
_MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def parse_game_time_from_ticker(ticker: str) -> datetime | None:
    """Extract game start time (ET) from a Kalshi ticker when encoded."""
    match = _TICKER_DT_RE.search(ticker or "")
    if not match:
        return None
    try:
        year = 2000 + int(match.group(1))
        month = _MONTH_MAP.get(match.group(2))
        if month is None:
            return None
        day = int(match.group(3))
        hhmm = match.group(4)
        hour = int(hhmm[:2])
        minute = int(hhmm[2:])
        return datetime(year, month, day, hour, minute, tzinfo=ET)
    except (ValueError, IndexError):
        return None


def _row_mapping(row: Any) -> dict[str, Any]:
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


def _queued_order_id(row: Any) -> str:
    if hasattr(row, "_mapping"):
        return row._mapping["kalshi_order_id"]
    return row[0]


class KalshiCancellationService:
    """Manage stale-order review queueing and approved cancellation execution."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any | None = None,
        alert_stale_orders: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        self.engine = engine
        self.client = client
        self.alert_stale_orders = alert_stale_orders

    def enqueue_stale_orders_for_review(self, *, now: datetime | None = None) -> int:
        """Detect stale pending live orders and enqueue new ones for human review."""
        all_pending = self._fetch_recent_pending_orders()
        if not all_pending:
            logger.info("No pending orders found.")
            return 0

        now = now or datetime.now(ET)
        if now.tzinfo is None:
            now = now.replace(tzinfo=ET)

        stale_rows = [row for row in all_pending if self._is_stale_pending_order(row, now)]
        if not stale_rows:
            logger.info(f"No stale orders among {len(all_pending)} pending orders.")
            return 0

        logger.info(f"Found {len(stale_rows)} stale pending order(s)")
        queued_ids = self._fetch_non_rejected_cancel_queue_order_ids()
        new_orders = [row for row in stale_rows if _row_mapping(row)["kalshi_order_id"] not in queued_ids]
        if not new_orders:
            logger.info("All stale orders already in cancel queue.")
            return 0

        inserted: list[dict[str, Any]] = []
        for row in new_orders:
            row_dict = _row_mapping(row)
            try:
                self._insert_cancel_review_row(row_dict)
                inserted.append(row_dict)
            except Exception as exc:  # pragma: no cover - defensive logging path
                logger.error(f"Failed to enqueue order {row_dict.get('kalshi_order_id')}: {exc}")

        if inserted and self.alert_stale_orders is not None:
            try:
                self.alert_stale_orders(inserted)
            except Exception as exc:  # pragma: no cover - alert failures are non-fatal
                logger.warning(f"Stale-order alert failed (non-fatal): {exc}")

        logger.info(f"Enqueued {len(inserted)} stale orders for cancellation review.")
        return len(inserted)

    def execute_approved_cancellations(self) -> dict[str, int]:
        """Cancel orders that were already approved by human review."""
        approved_rows = self._fetch_approved_cancel_rows()
        if not approved_rows:
            logger.info("No approved cancellations to execute.")
            return {"cancelled": 0, "failed": 0, "skipped_auth": 0}

        logger.info(f"Found {len(approved_rows)} approved cancellation(s)")
        if self.client is None or not getattr(self.client, "is_authenticated", False):
            logger.warning("KalshiClient not authenticated — skipping cancellations")
            return {"cancelled": 0, "failed": 0, "skipped_auth": len(approved_rows)}

        cancelled = 0
        failed = 0
        for row in approved_rows:
            queue_id = row[0]
            order_id = row[1]
            try:
                self.client.cancel_order(order_id)
                self._mark_cancellation_executed(queue_id, order_id)
                logger.info(
                    f"Cancelled order {order_id} | "
                    f"{row[2] or '?'} {row[3] or ''} {row[4] or ''}"
                )
                cancelled += 1
            except Exception as exc:
                self._mark_cancellation_failed(queue_id, str(exc))
                logger.error(f"Failed to cancel order {order_id}: {exc}")
                failed += 1

        logger.info(f"Cancellation run complete: {cancelled} cancelled, {failed} failed.")
        return {"cancelled": cancelled, "failed": failed, "skipped_auth": 0}

    def _fetch_recent_pending_orders(self) -> list[Any]:
        with self.engine.connect() as conn:
            return conn.execute(text("""
                SELECT id, kalshi_order_id, game_date, ticker, sport, player_id,
                       player_name, stat_type, line, side, contracts, total_cost,
                       game_start_time
                FROM kalshi_live_orders
                WHERE status = 'pending'
                  AND game_date >= CURRENT_DATE - INTERVAL '3 days'
            """)).fetchall()

    def _fetch_non_rejected_cancel_queue_order_ids(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT kalshi_order_id
                FROM kalshi_cancel_queue
                WHERE status != 'rejected'
            """)).fetchall()
        return {_queued_order_id(row) for row in rows}

    def _is_stale_pending_order(self, row: Any, now: datetime) -> bool:
        row_dict = _row_mapping(row)
        game_start = row_dict.get("game_start_time")
        ticker = row_dict.get("ticker")
        game_date = row_dict.get("game_date")

        if game_start is not None:
            if game_start.tzinfo is None:
                game_start = game_start.replace(tzinfo=ET)
            if game_start <= now:
                return True

        parsed_time = parse_game_time_from_ticker(ticker)
        if parsed_time is not None and parsed_time <= now:
            logger.info(
                f"Detected stale via ticker parse: {row_dict.get('player_name') or ticker} "
                f"(game started {parsed_time.strftime('%I:%M %p ET')})"
            )
            return True

        return game_date is not None and game_date < now.date()

    def _insert_cancel_review_row(self, row: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO kalshi_cancel_queue
                    (kalshi_order_id, game_date, ticker, sport, player_id,
                     player_name, stat_type, line, side, contracts,
                     expected_cost, game_start_time, status)
                VALUES
                    (:kalshi_order_id, :game_date, :ticker, :sport, :player_id,
                     :player_name, :stat_type, :line, :side, :contracts,
                     :expected_cost, :game_start_time, 'pending_review')
                ON CONFLICT (kalshi_order_id) DO NOTHING
            """), {
                "kalshi_order_id": row["kalshi_order_id"],
                "game_date": str(row["game_date"]),
                "ticker": row["ticker"],
                "sport": row["sport"],
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "stat_type": row.get("stat_type"),
                "line": float(row["line"]) if row.get("line") is not None else None,
                "side": row.get("side"),
                "contracts": row.get("contracts"),
                "expected_cost": float(row["total_cost"]) if row.get("total_cost") is not None else None,
                "game_start_time": row.get("game_start_time"),
            })

    def _fetch_approved_cancel_rows(self) -> list[Any]:
        with self.engine.connect() as conn:
            return conn.execute(text("""
                SELECT id, kalshi_order_id, player_name, stat_type, side, contracts
                FROM kalshi_cancel_queue
                WHERE status = 'approved'
                ORDER BY approved_at ASC NULLS LAST
            """)).fetchall()

    def _mark_cancellation_executed(self, queue_id: int, order_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE kalshi_cancel_queue
                SET status = 'executed', executed_at = now()
                WHERE id = :id
            """), {"id": queue_id})
            conn.execute(text("""
                UPDATE kalshi_live_orders
                SET status = 'cancelled'
                WHERE kalshi_order_id = :oid AND status = 'pending'
            """), {"oid": order_id})

    def _mark_cancellation_failed(self, queue_id: int, error: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE kalshi_cancel_queue
                SET status = 'failed', cancel_error = :error
                WHERE id = :id
            """), {"id": queue_id, "error": error})
