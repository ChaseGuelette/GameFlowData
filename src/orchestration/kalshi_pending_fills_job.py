#!/usr/bin/env python3
"""
Kalshi Pending Fills Job — runs every 5 minutes (9 AM - 11 PM ET).
Polls the Kalshi API to reconcile any pending orders that have since filled.
Exits early (zero API calls) if no pending orders exist.
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from datetime import UTC

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
        logging.FileHandler(log_dir / "kalshi_pending_fills.log"),
    ],
)
logger = logging.getLogger("kalshi_pending_fills_job")


def main():
    if not os.getenv("KALSHI_API_KEY"):
        logger.info("KALSHI_API_KEY not set — skipping")
        return

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        return

    from sqlalchemy import create_engine, text

    engine = create_engine(database_url)

    with engine.connect() as conn:
        pending_count = conn.execute(
            text("SELECT COUNT(*) FROM kalshi_live_orders WHERE status = 'pending'")
        ).scalar()

    if not pending_count:
        logger.info("No pending orders — skipping")
        return

    logger.info(f"Found {pending_count} pending order(s) — polling Kalshi API")

    from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

    trader = KalshiLiveTrader(resolve_only=True)
    result = trader.reconcile_fills()
    reconciled = result.get("reconciled", 0) if isinstance(result, dict) else 0
    logger.info(f"Reconciled {reconciled} pending order(s) to filled")

    if reconciled > 0:
        try:
            from datetime import datetime, timedelta

            from src.discord_bot.alerts import send_kalshi_trade_alert_sync

            channel = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
            if channel:
                cutoff = datetime.now(UTC) - timedelta(seconds=30)
                with engine.connect() as conn:
                    filled_rows = conn.execute(text("""
                        SELECT player_name, stat_type, side, fill_price, contracts
                        FROM kalshi_live_orders
                        WHERE status = 'filled' AND filled_at >= :cutoff
                    """), {"cutoff": cutoff}).fetchall()
                for row in filled_rows:
                    send_kalshi_trade_alert_sync("filled", {
                        "player_name": row.player_name or "Unknown",
                        "stat_type": row.stat_type or "",
                        "side": row.side or "",
                        "fill_price": row.fill_price,
                        "contracts": row.contracts,
                    }, channel_id=channel)
        except Exception as e:
            logger.warning(f"Discord alert failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
