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

from src.db.client import get_engine  # noqa: E402
from src.scrapers.kalshi.kalshi_client import KalshiClient  # noqa: E402
from src.trading.kalshi.cancellation_service import KalshiCancellationService  # noqa: E402

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
    engine = get_engine()
    client = KalshiClient()
    KalshiCancellationService(engine=engine, client=client).execute_approved_cancellations()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
