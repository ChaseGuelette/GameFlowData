"""
Kalshi Non-Sports Refresh Job
==============================
Scrapes non-sports Kalshi markets (economics, crypto) and stores them
with sport=NULL so the non-sports arb scanner can match them against
Polymarket equivalents.

No edge computation or paper trading — just scrape + store.

Runs every 10 minutes alongside the sports Kalshi refresh.

Usage:
    python src/orchestration/kalshi_nonsports_refresh_job.py
    python src/orchestration/kalshi_nonsports_refresh_job.py --dry-run
"""

import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiNonSportsRefreshJob")


def run(dry_run: bool = False) -> dict:
    """Scrape and store Kalshi non-sports markets.

    Args:
        dry_run: Print parsed markets without DB writes.

    Returns:
        Summary dict with counts.
    """
    summary: dict = {"parsed": 0, "stored": 0, "by_series": {}}

    logger.info("Scraping Kalshi non-sports markets (economics, crypto)...")
    try:
        from src.scrapers.kalshi.kalshi_market_scraper import scrape_non_sports_and_store

        stats = scrape_non_sports_and_store(dry_run=dry_run)
        summary["parsed"] = stats.get("parsed", 0)
        summary["stored"] = stats.get("stored", 0)
        summary["by_series"] = stats.get("by_series", {})
        logger.info(
            f"Non-sports scrape: {stats.get('parsed', 0)} parsed, "
            f"{stats.get('stored', 0)} stored"
        )
    except Exception as e:
        logger.error(f"Non-sports scrape failed: {e}", exc_info=True)
        summary["error"] = str(e)

    return summary


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Kalshi non-sports market refresh")
    parser.add_argument("--dry-run", action="store_true", help="Print markets without DB writes")
    args = parser.parse_args()

    # Check credentials early
    has_creds = bool(os.getenv("KALSHI_API_KEY"))
    if not has_creds:
        logger.info("KALSHI_API_KEY not set — exiting gracefully")
        sys.exit(0)

    logger.info("=" * 60)
    logger.info("Kalshi Non-Sports Refresh Job")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 60)

    summary = run(dry_run=args.dry_run)

    logger.info("=" * 60)
    logger.info("NON-SPORTS REFRESH COMPLETE")
    logger.info(f"  Parsed:  {summary.get('parsed', 0)}")
    logger.info(f"  Stored:  {summary.get('stored', 0)}")
    logger.info(f"  By series: {summary.get('by_series', {})}")
    if summary.get("error"):
        logger.error(f"  Error: {summary['error']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
