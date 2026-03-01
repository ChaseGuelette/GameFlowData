#!/usr/bin/env python
"""
Audit & Resolve Paper Bets - Diagnostic + Fix Script
=====================================================
Shows the state of all paper bets, finds unresolved bets where game stats
exist, resolves them, and re-places any missed bets from historical predictions
that were never bet on (e.g., edges discovered after old schedule cutoff).

Usage:
    # Audit only (no changes)
    python src/paper_trading/audit_and_resolve.py --audit

    # Resolve all pending bets where stats exist
    python src/paper_trading/audit_and_resolve.py --resolve

    # Backfill + resolve: re-place bets from historical predictions, then resolve
    python src/paper_trading/audit_and_resolve.py --backfill --days 30

    # Full fix: backfill + resolve
    python src/paper_trading/audit_and_resolve.py --backfill --resolve --days 30
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.paper_trading.paper_trader import PaperTrader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def audit_bets(engine) -> pd.DataFrame:
    """Show bet counts by date and status."""
    query = text("""
        SELECT
            game_date,
            status,
            COUNT(*) as count,
            ROUND(SUM(stake)::numeric, 2) as total_stake,
            ROUND(SUM(pnl)::numeric, 2) as total_pnl
        FROM paper_bets
        GROUP BY game_date, status
        ORDER BY game_date DESC, status
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def find_unresolved_with_stats(engine) -> list[date]:
    """Find dates that have pending bets AND game stats available."""
    query = text("""
        SELECT DISTINCT pb.game_date
        FROM paper_bets pb
        JOIN team_game_stats tgs ON tgs.game_date = pb.game_date
        WHERE pb.status = 'pending'
        ORDER BY pb.game_date
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]


def find_missed_bet_dates(engine, start_date: date, end_date: date) -> list[dict]:
    """Find dates where predictions exist but fewer bets were placed than recommended.

    Compares daily_predictions.is_recommended count vs paper_bets count per date.
    """
    query = text("""
        WITH recommended AS (
            SELECT
                prediction_date,
                COUNT(*) as rec_count
            FROM daily_predictions
            WHERE prediction_date >= :start_date
              AND prediction_date <= :end_date
              AND is_recommended = true
              AND line IS NOT NULL
            GROUP BY prediction_date
        ),
        placed AS (
            SELECT
                game_date,
                COUNT(*) as bet_count
            FROM paper_bets
            WHERE game_date >= :start_date
              AND game_date <= :end_date
            GROUP BY game_date
        )
        SELECT
            r.prediction_date as game_date,
            r.rec_count as recommended,
            COALESCE(p.bet_count, 0) as bets_placed,
            r.rec_count - COALESCE(p.bet_count, 0) as gap
        FROM recommended r
        LEFT JOIN placed p ON p.game_date = r.prediction_date
        WHERE r.rec_count > COALESCE(p.bet_count, 0)
        ORDER BY r.prediction_date
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
        return [dict(row._mapping) for row in result]


def main():
    parser = argparse.ArgumentParser(description="Audit & Resolve Paper Bets")
    parser.add_argument("--audit", action="store_true", help="Show bet status breakdown")
    parser.add_argument("--resolve", action="store_true", help="Resolve all pending bets with available stats")
    parser.add_argument("--backfill", action="store_true", help="Re-place bets from historical predictions that were missed")
    parser.add_argument("--days", type=int, default=30, help="Days to look back for backfill (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")

    args = parser.parse_args()

    if not any([args.audit, args.resolve, args.backfill]):
        args.audit = True  # Default to audit

    engine = get_engine()
    trader = PaperTrader()

    # ---- AUDIT ----
    if args.audit or args.resolve or args.backfill:
        print("\n" + "=" * 80)
        print("PAPER BETS AUDIT")
        print("=" * 80)

        bets_df = audit_bets(engine)
        if bets_df.empty:
            print("\nNo paper bets found in database.")
        else:
            # Pivot for readability
            pivot = bets_df.pivot_table(
                index="game_date", columns="status",
                values="count", fill_value=0, aggfunc="sum",
            )
            # Add P&L column
            pnl_by_date = bets_df.groupby("game_date")["total_pnl"].sum()
            pivot["total_pnl"] = pnl_by_date

            print("\nBets by date and status:")
            print(pivot.to_string())

            # Summary
            total_pending = int(bets_df[bets_df["status"] == "pending"]["count"].sum())
            total_resolved = int(bets_df[bets_df["status"].isin(["won", "lost", "push", "cancelled"])]["count"].sum())
            total_pnl = float(bets_df["total_pnl"].sum())
            print(f"\nTotal pending: {total_pending}")
            print(f"Total resolved: {total_resolved}")
            print(f"Total P&L: ${total_pnl:+,.2f}")

        # Check for pending bets that CAN be resolved
        resolvable_dates = find_unresolved_with_stats(engine)
        if resolvable_dates:
            print(f"\nWARNING: Found {len(resolvable_dates)} dates with PENDING bets that have game stats available:")
            for d in resolvable_dates:
                print(f"  - {d}")
        else:
            print("\nNo pending bets with available stats to resolve.")

        # Check for missed bets
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
        missed = find_missed_bet_dates(engine, start_date, end_date)
        if missed:
            total_gap = sum(m["gap"] for m in missed)
            print(f"\nWARNING: Found {len(missed)} dates with MORE recommended predictions than placed bets ({total_gap} total gap):")
            for m in missed:
                print(f"  - {m['game_date']}: {m['recommended']} recommended, {m['bets_placed']} placed (gap: {m['gap']})")
        else:
            print(f"\nNo missed bets in last {args.days} days.")

    # ---- BACKFILL ----
    if args.backfill:
        print("\n" + "=" * 80)
        print("BACKFILLING MISSED BETS")
        print("=" * 80)

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)

        missed = find_missed_bet_dates(engine, start_date, end_date)
        if not missed:
            print("No missed bets to backfill.")
        else:
            total_placed = 0
            total_resolved_count = 0
            for m in missed:
                game_date = m["game_date"]
                if args.dry_run:
                    bets = trader.select_bets(game_date)
                    print(f"  [DRY RUN] {game_date}: would place {len(bets)} bets")
                    continue

                # Place bets (upsert - won't duplicate)
                bets = trader.select_bets(game_date)
                if bets:
                    count = trader.place_bets(bets)
                    total_placed += count
                    print(f"  {game_date}: placed {count} bets")

                    # Immediately resolve if stats exist
                    result = trader.resolve_bets(game_date)
                    total_resolved_count += result["resolved"]
                    print(
                        f"    -> resolved {result['resolved']}: "
                        f"{result['won']}W {result['lost']}L {result['push']}P {result['cancelled']}C"
                    )

            if not args.dry_run:
                print(f"\nBackfill complete: {total_placed} bets placed, {total_resolved_count} resolved")

    # ---- RESOLVE ----
    if args.resolve:
        print("\n" + "=" * 80)
        print("RESOLVING ALL PENDING BETS")
        print("=" * 80)

        if args.dry_run:
            pending = trader.get_pending_bets()
            print(f"[DRY RUN] Would resolve {len(pending)} pending bets")
        else:
            result = trader.resolve_all_pending()
            print(f"Dates processed: {result['dates_processed']}")
            print(f"Dates skipped (no stats): {result['dates_skipped']}")
            print(f"Total resolved: {result['total_resolved']}")
            print(f"Record: {result['total_won']}W - {result['total_lost']}L - {result['total_push']}P - {result['total_cancelled']}C")

            if result["by_date"]:
                print("\nBy date:")
                for date_str, dr in result["by_date"].items():
                    if dr.get("skipped"):
                        print(f"  {date_str}: SKIPPED (no stats)")
                    else:
                        print(f"  {date_str}: {dr['resolved']} resolved ({dr['won']}W {dr['lost']}L {dr['push']}P)")

            bankroll = trader._get_current_bankroll()
            print(f"\nCurrent bankroll: ${bankroll:,.2f}")


if __name__ == "__main__":
    main()
