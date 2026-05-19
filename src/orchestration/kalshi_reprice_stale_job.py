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

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.trading.kalshi.repricing_service import KalshiRepricingService

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


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _get_best_available_price(client: KalshiClient, ticker: str, side: str, target_cents: int) -> int | None:
    """Get the taker fill price in YES-equivalent cents from the live orderbook."""
    try:
        ob = client.get_orderbook(ticker, depth=10)
        if ob is None:
            return None
        orderbook = ob.get("orderbook", {})

        if side == "yes":
            no_bids = orderbook.get("no", [])
            for no_bid, qty in no_bids:
                if qty > 0:
                    return 100 - no_bid
            return None

        yes_bids = orderbook.get("yes", [])
        for yes_bid, qty in yes_bids:
            if qty > 0:
                return yes_bid
        return None
    except Exception as e:
        logger.warning(f"Orderbook query failed for {ticker}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Reprice stale resting Kalshi orders")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be repriced but don't act")
    args = parser.parse_args()

    live_enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower() == "true"
    if not live_enabled:
        logger.info("KALSHI_LIVE_TRADING_ENABLED != true — skipping")
        return

    start = time.time()

    client = KalshiClient()
    if not getattr(client, "is_authenticated", False):
        logger.warning("KalshiClient unavailable: Kalshi API credentials not configured for live trading")
        return

    if args.dry_run:
        resting = client.list_orders(status="resting")
        logger.info(f"[DRY RUN] Found {len(resting)} resting orders")
        for order in resting:
            logger.info(f"  {order.get('ticker')} {order.get('side')} @ {order.get('yes_price', '?')}c")
        return

    engine = get_engine()
    repricing_service = KalshiRepricingService(
        engine=engine,
        client=client,
        get_best_available_price=lambda ticker, side, target_cents: _get_best_available_price(
            client, ticker, side, target_cents,
        ),
        sweep_max_cents=_env_int("KALSHI_SWEEP_MAX_CENTS", 10),
        sweep_edge_retention=_env_float("KALSHI_SWEEP_EDGE_RETENTION", 0.50),
    )
    repriced = repricing_service.reprice_stale_orders()

    elapsed = time.time() - start
    logger.info(f"Reprice job complete: {repriced} orders repriced in {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
