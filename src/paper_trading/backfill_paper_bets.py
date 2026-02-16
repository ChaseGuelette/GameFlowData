#!/usr/bin/env python
"""
Backfill Paper Bets from Historical Model Picks.

Replays historical predictions marked as `is_recommended=true` in daily_predictions,
places paper bets using current config, and resolves them against actual results.

Usage:
    # Backfill last 30 days
    python src/paper_trading/backfill_paper_bets.py --days 30

    # Backfill specific date range
    python src/paper_trading/backfill_paper_bets.py --start 2026-01-15 --end 2026-02-14

    # Dry run (show what would be placed)
    python src/paper_trading/backfill_paper_bets.py --days 30 --dry-run
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.paper_trading.paper_trader import PaperTrader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_dates_with_predictions(engine, start_date: date, end_date: date) -> list[date]:
    """Get all dates that have is_recommended predictions."""
    query = text("""
        SELECT DISTINCT prediction_date
        FROM daily_predictions
        WHERE prediction_date >= :start_date
          AND prediction_date <= :end_date
          AND is_recommended = true
        ORDER BY prediction_date
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
        return [row[0] for row in result]


def backfill_date(trader: PaperTrader, game_date: date, dry_run: bool = False) -> dict:
    """Backfill paper bets for a single date."""
    # Select bets using current config (BL blending, Kelly sizing)
    bets = trader.select_bets(game_date)

    if not bets:
        return {"date": game_date, "placed": 0, "skipped": True}

    if dry_run:
        total_stake = sum(b["stake"] for b in bets)
        logger.info(f"  [DRY RUN] {game_date}: Would place {len(bets)} bets, ${total_stake:.2f} total stake")
        for bet in bets[:5]:  # Show first 5
            logger.info(
                f"    {bet['player_name'][:20]:<20} {bet['stat_type']:<4} "
                f"{bet['bet_direction']:<5} {bet['line']:>5.1f} "
                f"edge={bet['edge']*100:>5.1f}% stake=${bet['stake']:>7.2f}"
            )
        if len(bets) > 5:
            logger.info(f"    ... and {len(bets) - 5} more")
        return {"date": game_date, "placed": len(bets), "stake": total_stake, "dry_run": True}

    # Place bets
    placed = trader.place_bets(bets)

    # Resolve bets (we have actuals for historical dates)
    resolution = trader.resolve_bets(game_date)

    total_stake = sum(b["stake"] for b in bets)
    logger.info(
        f"  {game_date}: {placed} bets placed, ${total_stake:.2f} staked, "
        f"{resolution['won']}W-{resolution['lost']}L-{resolution['push']}P"
    )

    return {
        "date": game_date,
        "placed": placed,
        "stake": total_stake,
        "won": resolution["won"],
        "lost": resolution["lost"],
        "push": resolution["push"],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Backfill paper bets from historical Model Picks"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    parser.add_argument(
        "--start",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date (YYYY-MM-DD). Overrides --days.",
    )
    parser.add_argument(
        "--end",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be placed without actually placing bets",
    )

    args = parser.parse_args()

    # Determine date range
    end_date = args.end or (date.today() - timedelta(days=1))  # Yesterday by default
    if args.start:
        start_date = args.start
    else:
        start_date = end_date - timedelta(days=args.days - 1)

    logger.info("=" * 60)
    logger.info("PAPER TRADING BACKFILL")
    logger.info("=" * 60)
    logger.info(f"Date range: {start_date} to {end_date}")

    # Initialize trader with current config (from .env)
    trader = PaperTrader()
    engine = get_engine()

    # Get dates with Model Picks
    dates = get_dates_with_predictions(engine, start_date, end_date)
    logger.info(f"Found {len(dates)} dates with Model Picks")

    if not dates:
        logger.warning("No dates with is_recommended predictions found in range")
        return 0

    # Backfill each date
    results = []
    for game_date in dates:
        try:
            result = backfill_date(trader, game_date, args.dry_run)
            results.append(result)
        except Exception as e:
            logger.error(f"  {game_date}: FAILED - {e}")
            results.append({"date": game_date, "error": str(e)})

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    total_bets = sum(r.get("placed", 0) for r in results)
    total_stake = sum(r.get("stake", 0) for r in results)
    total_won = sum(r.get("won", 0) for r in results)
    total_lost = sum(r.get("lost", 0) for r in results)
    total_push = sum(r.get("push", 0) for r in results)

    logger.info(f"Dates processed: {len(dates)}")
    logger.info(f"Total bets: {total_bets}")
    logger.info(f"Total stake: ${total_stake:,.2f}")

    if not args.dry_run:
        logger.info(f"Record: {total_won}W - {total_lost}L - {total_push}P")
        if total_won + total_lost > 0:
            hit_rate = total_won / (total_won + total_lost) * 100
            logger.info(f"Hit rate: {hit_rate:.1f}%")

        # Get final bankroll
        summary = trader.get_daily_summary(dates[-1])
        if summary:
            logger.info(f"Final bankroll: ${summary['bankroll_after']:,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
