#!/usr/bin/env python3
"""
Kalshi Execute Cancellations Job
================================
Polls kalshi_cancel_queue for approved records and calls cancel_order()
via the Kalshi API to release unfilled contracts.
Designed to run on a short interval (every 2 min) during trading hours.

Usage:
    python src/orchestration/kalshi_execute_cancellations_job.py
"""

import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "kalshi_execute_cancellations.log"),
    ],
)
logger = logging.getLogger("KalshiExecuteCancellations")


def main():
    # Cancellations always run — they execute human-approved actions regardless
    # of whether new trading is enabled.
    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()

    with engine.connect() as conn:
        approved_rows = conn.execute(text("""
            SELECT id, kalshi_order_id, player_name, stat_type, side, contracts
            FROM kalshi_cancel_queue
            WHERE status = 'approved'
            ORDER BY approved_at ASC NULLS LAST
        """)).fetchall()

    if not approved_rows:
        logger.info("No approved cancellations to execute.")
        return

    logger.info(f"Found {len(approved_rows)} approved cancellation(s)")

    from src.scrapers.kalshi.kalshi_client import KalshiClient

    client = KalshiClient()
    if not client.is_authenticated:
        logger.warning("KalshiClient not authenticated — skipping cancellations")
        return

    cancelled = 0
    failed = 0

    for row in approved_rows:
        queue_id = row[0]
        order_id = row[1]
        try:
            client.cancel_order(order_id)
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE kalshi_cancel_queue
                    SET status = 'cancelled', executed_at = now()
                    WHERE id = :id
                """), {"id": queue_id})
            logger.info(
                f"Cancelled order {order_id} | "
                f"{row[2] or '?'} {row[3] or ''} {row[4] or ''}"
            )
            cancelled += 1
        except Exception as e:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE kalshi_cancel_queue
                    SET status = 'failed', cancel_error = :error
                    WHERE id = :id
                """), {"id": queue_id, "error": str(e)})
            logger.error(f"Failed to cancel order {order_id}: {e}")
            failed += 1

    logger.info(f"Cancellation run complete: {cancelled} cancelled, {failed} failed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
