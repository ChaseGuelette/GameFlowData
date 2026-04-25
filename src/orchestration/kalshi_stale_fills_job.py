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

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

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


def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        return

    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()

    with engine.connect() as conn:
        stale_rows = conn.execute(text("""
            SELECT id, kalshi_order_id, game_date, ticker, sport, player_id,
                   player_name, stat_type, line, side, contracts, total_cost,
                   game_start_time
            FROM kalshi_live_orders
            WHERE status = 'pending'
              AND game_date >= CURRENT_DATE - INTERVAL '3 days'
              AND (
                (game_start_time IS NOT NULL AND game_start_time <= now())
                OR (game_start_time IS NULL AND game_date < CURRENT_DATE)
              )
        """)).fetchall()

    if not stale_rows:
        logger.info("No stale pending orders found.")
        return

    logger.info(f"Found {len(stale_rows)} stale pending order(s)")

    with engine.connect() as conn:
        already_queued = conn.execute(text("""
            SELECT kalshi_order_id
            FROM kalshi_cancel_queue
            WHERE status != 'rejected'
        """)).fetchall()
    queued_ids = {row[0] for row in already_queued}

    new_orders = [r for r in stale_rows if r[1] not in queued_ids]
    if not new_orders:
        logger.info("All stale orders already in cancel queue.")
        return

    for row in new_orders:
        try:
            with engine.begin() as conn:
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
                    "kalshi_order_id": row[1],
                    "game_date": str(row[2]),
                    "ticker": row[3],
                    "sport": row[4],
                    "player_id": row[5],
                    "player_name": row[6],
                    "stat_type": row[7],
                    "line": float(row[8]) if row[8] is not None else None,
                    "side": row[9],
                    "contracts": row[10],
                    "expected_cost": float(row[11]) if row[11] is not None else None,
                    "game_start_time": row[12],
                })
        except Exception as e:
            logger.error(f"Failed to enqueue order {row[1]}: {e}")

    logger.info(f"Enqueued {len(new_orders)} stale orders for cancellation review.")

    try:
        from src.discord_bot.alerts import send_kalshi_trade_alert_sync

        channel = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        if channel:
            lines = []
            for row in new_orders:
                cost_str = f"${float(row[11]):.2f}" if row[11] is not None else "?"
                lines.append(
                    f"  {row[6] or row[3]} | {row[7] or ''} "
                    f"{row[9] or ''} | {row[10] or '?'} contracts | {cost_str}"
                )

            msg = "\n".join(lines)
            send_kalshi_trade_alert_sync("circuit_breaker", {
                "reason": f"{len(new_orders)} stale Kalshi order(s) need review — game has started.\n"
                          f"Approve cancellation on the dashboard (Bot Tracker -> Stale Orders).\n\n{msg}",
                "action": "Review stale orders",
                "balance": 0,
            }, channel_id=channel)
    except Exception as e:
        logger.warning(f"Discord alert failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
