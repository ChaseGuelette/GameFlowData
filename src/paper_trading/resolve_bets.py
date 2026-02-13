#!/usr/bin/env python
"""
CLI script to resolve paper bets using actual game results.

Usage:
    # Resolve bets for a specific date:
    python src/paper_trading/resolve_bets.py --date 2026-02-04
    python src/paper_trading/resolve_bets.py --date 2026-02-04 --dry-run

    # Resolve ALL pending bets (multi-day catchup):
    python src/paper_trading/resolve_bets.py --all-pending
    python src/paper_trading/resolve_bets.py --all-pending --dry-run
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


def format_all_pending_summary(result: dict) -> str:
    """Format summary for --all-pending resolution."""
    lines = []
    lines.append(f"Dates processed: {result['dates_processed']}")
    lines.append(f"Dates skipped:   {result['dates_skipped']} (no game stats yet)")
    lines.append(f"Total resolved:  {result['total_resolved']}")
    lines.append(
        f"Record:          {result['total_won']}W - {result['total_lost']}L - "
        f"{result['total_push']}P - {result['total_cancelled']}C"
    )

    if result["by_date"]:
        lines.append("\nBy Date:")
        for date_str, date_result in result["by_date"].items():
            if date_result.get("skipped"):
                lines.append(f"  {date_str}: SKIPPED ({date_result.get('reason', 'unknown')})")
            else:
                lines.append(
                    f"  {date_str}: {date_result['resolved']} bets "
                    f"({date_result['won']}W {date_result['lost']}L "
                    f"{date_result['push']}P {date_result['cancelled']}C)"
                )

    return "\n".join(lines)


def run_single_date_resolution(trader: PaperTrader, game_date: date, dry_run: bool) -> int:
    """Resolve bets for a single date."""
    logger.info(f"Resolving bets for {game_date}")

    # Get pending bets for preview
    pending_bets = trader.get_pending_bets(game_date)

    if pending_bets.empty:
        print(f"\nNo pending bets found for {game_date}")
        # Show existing bets if any
        all_bets = trader.get_bets_for_date(game_date)
        if not all_bets.empty:
            print(f"\nExisting bets for {game_date}:")
            print(format_resolution_table(all_bets))
        return 0

    print(f"\n{'='*80}")
    print(f"RESOLVING BETS FOR {game_date}")
    print(f"{'='*80}")
    print(f"\nPending bets to resolve: {len(pending_bets)}")

    if dry_run:
        print("\n[DRY RUN] Would resolve the following bets:\n")
        print(format_resolution_table(pending_bets))
        print("\n[DRY RUN] Bets were NOT updated in database.")
        logger.info(f"[DRY RUN] Would resolve {len(pending_bets)} bets")
        return 0

    # Resolve bets
    results = trader.resolve_bets(game_date)

    # Display results
    print(f"\nResolution Complete:")
    print(f"  - Won:       {results['won']}")
    print(f"  - Lost:      {results['lost']}")
    print(f"  - Push:      {results['push']}")
    print(f"  - Cancelled: {results['cancelled']}")

    # Show resolved bets
    resolved_bets = trader.get_bets_for_date(game_date)
    print(f"\n{'='*80}")
    print("RESOLVED BETS")
    print(f"{'='*80}\n")
    print(format_resolution_table(resolved_bets))

    # Show daily summary
    summary = trader.get_daily_summary(game_date)
    print(f"\n{'='*80}")
    print("DAILY SUMMARY")
    print(f"{'='*80}\n")
    print(format_summary(summary))

    return 0


def run_all_pending_resolution(trader: PaperTrader, dry_run: bool) -> int:
    """Resolve all pending bets across all dates."""
    logger.info("Resolving ALL pending bets")

    # Get all pending bets for preview
    all_pending = trader.get_pending_bets()

    if all_pending.empty:
        print("\nNo pending bets found.")
        return 0

    # Group by date for display
    dates_with_pending = all_pending["game_date"].unique()
    print(f"\n{'='*80}")
    print("RESOLVING ALL PENDING BETS")
    print(f"{'='*80}")
    print(f"\nFound {len(all_pending)} pending bets across {len(dates_with_pending)} dates:")
    for d in sorted(dates_with_pending):
        count = len(all_pending[all_pending["game_date"] == d])
        print(f"  - {d}: {count} bets")

    if dry_run:
        print("\n[DRY RUN] Would resolve the following bets:\n")
        print(format_resolution_table(all_pending))
        print("\n[DRY RUN] Bets were NOT updated in database.")
        logger.info(f"[DRY RUN] Would resolve {len(all_pending)} bets")
        return 0

    # Resolve all pending bets
    result = trader.resolve_all_pending()

    # Display results
    print(f"\n{'='*80}")
    print("RESOLUTION SUMMARY")
    print(f"{'='*80}\n")
    print(format_all_pending_summary(result))

    # Show final bankroll
    bankroll = trader._get_current_bankroll()
    print(f"\n{'='*80}")
    print(f"CURRENT BANKROLL: ${bankroll:,.2f}")
    print(f"{'='*80}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Resolve paper bets with actual results")
    parser.add_argument(
        "--date",
        type=parse_date,
        help="Game date to resolve (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--all-pending",
        action="store_true",
        help="Resolve ALL pending bets across all dates (for multi-day catchup)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show results without updating database",
    )

    args = parser.parse_args()

    # Validate: require either --date or --all-pending
    if not args.date and not args.all_pending:
        parser.error("Must specify either --date YYYY-MM-DD or --all-pending")

    if args.date and args.all_pending:
        parser.error("Cannot use both --date and --all-pending together")

    # Initialize trader
    trader = PaperTrader()

    if args.all_pending:
        return run_all_pending_resolution(trader, args.dry_run)
    else:
        return run_single_date_resolution(trader, args.date, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
