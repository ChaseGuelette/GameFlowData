#!/usr/bin/env python3
"""
Kalshi Execute Approved Trades Job
===================================
Polls `kalshi_trade_queue` for trades with status='approved' and executes
them via the Kalshi API. Designed to run on a short interval (every 2 min)
during trading hours so human approvals on the dashboard get placed quickly.

Usage:
    python src/orchestration/kalshi_execute_approved_job.py
    python src/orchestration/kalshi_execute_approved_job.py --dry-run
"""

import argparse
import logging
import os
import sys
import time
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
        logging.FileHandler(LOG_DIR / "kalshi_execute_approved.log"),
    ],
)
logger = logging.getLogger("KalshiExecuteApproved")


def main():
    parser = argparse.ArgumentParser(description="Execute approved Kalshi trades from the queue")
    parser.add_argument("--dry-run", action="store_true", help="Log approved trades but don't execute")
    args = parser.parse_args()

    live_enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower() == "true"
    if not live_enabled:
        logger.info("KALSHI_LIVE_TRADING_ENABLED != true — skipping")
        return

    start = time.time()

    from sqlalchemy import text

    from src.db.client import get_engine
    from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

    engine = get_engine()

    # Find approved trades that haven't expired yet
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, ticker, player_name, stat_type, line, side, contracts, expected_cost
            FROM kalshi_trade_queue
            WHERE status = 'approved'
              AND expires_at > now()
            ORDER BY approved_at ASC NULLS LAST
        """)).fetchall()

    if not rows:
        logger.info("No approved trades in queue")
        return

    trade_ids = [row[0] for row in rows]
    logger.info(f"Found {len(trade_ids)} approved trades ready to execute")
    for row in rows:
        logger.info(
            f"  id={row[0]} {row[1]} {row[2]} {row[3]} {row[4]} "
            f"{row[5]} x{row[6]} cost=${float(row[7]):.2f}"
        )

    if args.dry_run:
        logger.info("[DRY RUN] Would execute these trades")
        return

    try:
        trader = KalshiLiveTrader()
    except RuntimeError as e:
        logger.warning(f"KalshiLiveTrader unavailable: {e}")
        return

    # Circuit breakers apply here too — don't place if halted
    can_trade, reason = trader.check_circuit_breakers()
    if not can_trade:
        logger.warning(f"Circuit breaker triggered — not executing: {reason}")
        return

    results = trader.execute_approved_trades(trade_ids)

    elapsed = time.time() - start
    placed = sum(1 for r in results if r.get("order_id"))
    logger.info(
        f"Executed {placed}/{len(trade_ids)} trades in {elapsed:.1f}s "
        f"(results={len(results)})"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
