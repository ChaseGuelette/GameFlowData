#!/usr/bin/env python
"""
CLI script to resolve paper bets using actual game results.

Usage:
    python src/paper_trading/resolve_bets.py --date 2026-02-04
    python src/paper_trading/resolve_bets.py --date 2026-02-04 --dry-run
"""

import argparse
import logging
import sys
from datetime import date, datetime

import pandas as pd

from src.paper_trading.paper_trader import PaperTrader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def format_resolution_table(bets_df: pd.DataFrame) -> str:
    """Format resolved bets as a readable table."""
    if bets_df.empty:
        return "No bets to display."

    lines = []
    lines.append(
        f"{'Player':<25} {'Stat':<5} {'Dir':<6} {'Line':>6} "
        f"{'Actual':>7} {'Status':<10} {'P&L':>9}"
    )
    lines.append("-" * 80)

    total_pnl = 0
    for _, bet in bets_df.iterrows():
        name = str(bet["player_name"])[:24]
        actual = bet.get("actual_value")
        actual_str = f"{actual:>7.1f}" if pd.notna(actual) else "    N/A"
        pnl = bet.get("pnl", 0) or 0
        status = bet.get("status", "pending")

        # Color-code status (using text markers)
        status_marker = {
            "won": "[WIN]",
            "lost": "[LOSS]",
            "push": "[PUSH]",
            "cancelled": "[VOID]",
            "pending": "[PEND]",
        }.get(status, status)

        pnl_str = f"${pnl:>+8.2f}" if status not in ("pending", "cancelled") else "       -"

        lines.append(
            f"{name:<25} {bet['stat_type']:<5} {bet['bet_direction']:<6} "
            f"{bet['line']:>6.1f} {actual_str} {status_marker:<10} {pnl_str}"
        )
        total_pnl += pnl

    lines.append("-" * 80)
    lines.append(f"{'TOTAL P&L':<69} ${total_pnl:>+8.2f}")

    return "\n".join(lines)


def format_summary(summary: dict | None) -> str:
    """Format daily summary."""
    if summary is None:
        return "No daily log entry found."

    lines = []
    lines.append(f"Date:          {summary['game_date']}")
    lines.append(f"Total Bets:    {summary['total_bets']}")
    lines.append(
        f"Record:        {summary['bets_won']}W - {summary['bets_lost']}L - "
        f"{summary['bets_push']}P ({summary['bets_pending']} pending)"
    )
    lines.append(f"Total Staked:  ${summary['total_staked']:.2f}")
    lines.append(f"Day P&L:       ${summary['total_pnl']:+.2f}")
    lines.append(f"Day ROI:       {summary['roi_pct']:+.1f}%")
    lines.append("-" * 40)
    lines.append(f"Cumulative P&L: ${summary['cumulative_pnl']:+.2f}")
    lines.append(f"Current Bankroll: ${summary['bankroll_after']:.2f}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Resolve paper bets with actual results")
    parser.add_argument(
        "--date",
        required=True,
        type=parse_date,
        help="Game date to resolve (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show results without updating database",
    )

    args = parser.parse_args()

    logger.info(f"Resolving bets for {args.date}")

    # Initialize trader
    trader = PaperTrader()

    # Get pending bets for preview
    pending_bets = trader.get_pending_bets(args.date)

    if pending_bets.empty:
        print(f"\nNo pending bets found for {args.date}")
        # Show existing bets if any
        all_bets = trader.get_bets_for_date(args.date)
        if not all_bets.empty:
            print(f"\nExisting bets for {args.date}:")
            print(format_resolution_table(all_bets))
        return 0

    print(f"\n{'='*80}")
    print(f"RESOLVING BETS FOR {args.date}")
    print(f"{'='*80}")
    print(f"\nPending bets to resolve: {len(pending_bets)}")

    if args.dry_run:
        print("\n[DRY RUN] Would resolve the following bets:\n")
        print(format_resolution_table(pending_bets))
        print("\n[DRY RUN] Bets were NOT updated in database.")
        logger.info(f"[DRY RUN] Would resolve {len(pending_bets)} bets")
        return 0

    # Resolve bets
    results = trader.resolve_bets(args.date)

    # Display results
    print(f"\nResolution Complete:")
    print(f"  - Won:       {results['won']}")
    print(f"  - Lost:      {results['lost']}")
    print(f"  - Push:      {results['push']}")
    print(f"  - Cancelled: {results['cancelled']}")

    # Show resolved bets
    resolved_bets = trader.get_bets_for_date(args.date)
    print(f"\n{'='*80}")
    print("RESOLVED BETS")
    print(f"{'='*80}\n")
    print(format_resolution_table(resolved_bets))

    # Show daily summary
    summary = trader.get_daily_summary(args.date)
    print(f"\n{'='*80}")
    print("DAILY SUMMARY")
    print(f"{'='*80}\n")
    print(format_summary(summary))

    return 0


if __name__ == "__main__":
    sys.exit(main())
