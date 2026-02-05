#!/usr/bin/env python
"""
CLI script to place paper bets from daily predictions.

Usage:
    python src/paper_trading/place_bets.py --date 2026-02-04
    python src/paper_trading/place_bets.py --date 2026-02-04 --dry-run
    python src/paper_trading/place_bets.py --date 2026-02-04 --edge-threshold 0.08
"""

import argparse
import logging
import sys
from datetime import date, datetime

from src.paper_trading.paper_trader import PaperTrader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def format_bet_table(bets: list[dict]) -> str:
    """Format bets as a readable table."""
    if not bets:
        return "No bets to display."

    lines = []
    lines.append(
        f"{'Player':<25} {'Stat':<5} {'Dir':<6} {'Line':>6} "
        f"{'Odds':>6} {'Edge':>6} {'Prob':>6} {'Stake':>8}"
    )
    lines.append("-" * 80)

    total_stake = 0
    for bet in bets:
        name = bet["player_name"][:24]
        lines.append(
            f"{name:<25} {bet['stat_type']:<5} {bet['bet_direction']:<6} "
            f"{bet['line']:>6.1f} {int(bet['odds_at_bet']):>+6} "
            f"{bet['edge']*100:>5.1f}% {bet['model_prob']*100:>5.1f}% "
            f"${bet['stake']:>7.2f}"
        )
        total_stake += bet["stake"]

    lines.append("-" * 80)
    lines.append(f"{'Total':<68} ${total_stake:>7.2f}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Place paper bets from daily predictions"
    )
    parser.add_argument(
        "--date",
        required=True,
        type=parse_date,
        help="Game date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show bets without placing them",
    )
    parser.add_argument(
        "--edge-threshold",
        type=float,
        default=0.05,
        help="Minimum edge to place bet (default: 0.05)",
    )
    parser.add_argument(
        "--kelly-fraction",
        type=float,
        default=0.125,
        help="Kelly fraction for stake sizing (default: 0.125)",
    )
    parser.add_argument(
        "--bankroll",
        type=float,
        default=10000.0,
        help="Starting bankroll if no previous bets (default: 10000)",
    )

    args = parser.parse_args()

    logger.info(f"Paper Bet Placement for {args.date}")
    logger.info(f"Edge threshold: {args.edge_threshold:.1%}")
    logger.info(f"Kelly fraction: {args.kelly_fraction}")

    # Initialize trader
    trader = PaperTrader(
        edge_threshold=args.edge_threshold,
        kelly_fraction=args.kelly_fraction,
        starting_bankroll=args.bankroll,
    )

    # Select bets
    bets = trader.select_bets(args.date)

    if not bets:
        logger.warning("No bets qualify for placement")
        print("\nNo predictions meet the edge threshold criteria.")
        return 0

    # Display bet summary
    print(f"\n{'='*80}")
    print(f"PAPER BETS FOR {args.date}")
    print(f"{'='*80}\n")
    print(format_bet_table(bets))
    print()

    if args.dry_run:
        print("[DRY RUN] Bets were NOT placed to database.")
        logger.info(f"[DRY RUN] Would place {len(bets)} bets")
    else:
        # Place bets
        count = trader.place_bets(bets)
        print(f"Successfully placed {count} bets.")
        logger.info(f"Placed {count} bets for {args.date}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
