#!/usr/bin/env python3
"""
Kalshi Reprice Stale Orders Job
================================
Checks for resting Kalshi orders where the market price has moved and
reprices them if the edge is still retained. Designed to run every 2 min
during trading hours alongside the execute-approved job.

Usage:
    python src/orchestration/kalshi_reprice_stale_job.py
    python src/orchestration/kalshi_reprice_stale_job.py --dry-run
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
        logging.FileHandler(LOG_DIR / "kalshi_reprice_stale.log"),
    ],
)
logger = logging.getLogger("KalshiRepriceStale")


def main():
    parser = argparse.ArgumentParser(description="Reprice stale resting Kalshi orders")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be repriced but don't act")
    args = parser.parse_args()

    live_enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower() == "true"
    if not live_enabled:
        logger.info("KALSHI_LIVE_TRADING_ENABLED != true — skipping")
        return

    start = time.time()

    from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

    try:
        trader = KalshiLiveTrader()
    except RuntimeError as e:
        logger.warning(f"KalshiLiveTrader unavailable: {e}")
        return

    if args.dry_run:
        resting = trader.client.list_orders(status="resting")
        logger.info(f"[DRY RUN] Found {len(resting)} resting orders")
        for order in resting:
            logger.info(f"  {order.get('ticker')} {order.get('side')} @ {order.get('yes_price', '?')}c")
        return

    repriced = trader.reprice_stale_orders()

    elapsed = time.time() - start
    logger.info(f"Reprice job complete: {repriced} orders repriced in {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
