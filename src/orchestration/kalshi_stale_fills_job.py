#!/usr/bin/env python3
"""
Kalshi Stale Fills Job — runs every 5 minutes (9 AM - 11 PM ET).
Detects pending Kalshi orders whose game has already started and enqueues
them in kalshi_cancel_queue for human review.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

from src.db.client import get_engine  # noqa: E402
from src.trading.kalshi.cancellation_service import KalshiCancellationService  # noqa: E402

log_dir = Path(__file__).resolve().parents[2] / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / "kalshi_stale_fills.log"),
    ],
)
logger = logging.getLogger("kalshi_stale_fills_job")


def send_stale_cancel_alert(orders: list[dict[str, Any]]) -> None:
    """Send the legacy Discord review alert for newly queued stale orders."""
    try:
        from src.discord_bot.alerts import send_kalshi_trade_alert_sync

        channel = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        if not channel:
            return

        lines = []
        for order in orders:
            cost = order.get("total_cost")
            cost_str = f"${float(cost):.2f}" if cost is not None else "?"
            lines.append(
                f"  {order.get('player_name') or order.get('ticker')} | {order.get('stat_type') or ''} "
                f"{order.get('side') or ''} | {order.get('contracts') or '?'} contracts | {cost_str}"
            )

        msg = "\n".join(lines)
        send_kalshi_trade_alert_sync("circuit_breaker", {
            "reason": f"{len(orders)} stale Kalshi order(s) need review — game has started.\n"
                      f"Approve cancellation on the dashboard (Bot Tracker -> Stale Orders).\n\n{msg}",
            "action": "Review stale orders",
            "balance": 0,
        }, channel_id=channel)
    except Exception as exc:
        logger.warning(f"Discord alert failed (non-fatal): {exc}")


def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        return

    engine = get_engine()
    KalshiCancellationService(
        engine=engine,
        alert_stale_orders=send_stale_cancel_alert,
    ).enqueue_stale_orders_for_review()


if __name__ == "__main__":
    main()
