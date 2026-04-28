#!/usr/bin/env python3
"""Sync actual realized P&L from Kalshi API into kalshi_live_orders."""

import argparse
import os
import sys
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient


def fetch_settled_positions() -> dict[str, float]:
    """Fetch realized P&L per ticker from /portfolio/settlements.

    P&L = (revenue cents / 100) - cost_paid - fees
    """
    client = KalshiClient()
    if not client.is_authenticated:
        print("ERROR: Kalshi client not authenticated. Check KALSHI_API_KEY and private key.")
        sys.exit(1)

    positions: dict[str, float] = {}
    cursor = None

    while True:
        params: dict = {"limit": 200}
        if cursor:
            params["cursor"] = cursor

        result = client._request("GET", "/portfolio/settlements", params=params)
        if result is None:
            break

        batch = result.get("settlements", [])
        for s in batch:
            ticker = s.get("ticker")
            if not ticker:
                continue
            revenue = float(s.get("revenue", 0)) / 100.0  # cents → dollars
            cost = float(s.get("no_total_cost_dollars", 0)) + float(s.get("yes_total_cost_dollars", 0))
            fee = float(s.get("fee_cost", 0))
            positions[ticker] = revenue - cost - fee

        cursor = result.get("cursor")
        if not cursor or not batch:
            break

    print(f"Fetched {len(positions)} settled positions from Kalshi API")
    return positions


def fetch_db_orders(engine, sport: str | None = None, days: int | None = None) -> list[dict]:
    query = """
        SELECT id, ticker, COALESCE(pnl, 0.0) AS pnl, sport, placed_at
        FROM kalshi_live_orders
        WHERE status IN ('won', 'lost') AND ticker IS NOT NULL
    """
    params = {}

    if sport:
        query += " AND sport = :sport"
        params["sport"] = sport

    if days:
        cutoff = datetime.now(UTC) - timedelta(days=days)
        query += " AND placed_at >= :cutoff"
        params["cutoff"] = cutoff

    query += " ORDER BY id"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return [{**dict(r), "pnl": float(r["pnl"])} for r in rows]


def fmt_pnl(val: float) -> str:
    return f"+${val:.2f}" if val >= 0 else f"-${abs(val):.2f}"


def main():
    parser = argparse.ArgumentParser(description="Sync actual realized P&L from Kalshi API")
    parser.add_argument("--apply", action="store_true", help="Apply updates to DB (default: dry-run)")
    parser.add_argument("--sport", type=str, default=None, help="Filter by sport")
    parser.add_argument("--days", type=int, default=None, help="Only orders placed within N days")
    args = parser.parse_args()

    api_positions = fetch_settled_positions()
    if not api_positions:
        print("WARNING: No settled positions returned from API. Aborting to prevent data loss.")
        sys.exit(1)

    engine = get_engine()
    db_orders = fetch_db_orders(engine, sport=args.sport, days=args.days)

    if not db_orders:
        print("No settled orders found in DB matching filters")
        return

    matched_rows = []
    unmatched_count = 0

    for order in db_orders:
        ticker = order["ticker"]
        if ticker in api_positions:
            api_pnl = api_positions[ticker]
            matched_rows.append({
                "id": order["id"],
                "ticker": ticker,
                "db_pnl": order["pnl"],
                "api_pnl": api_pnl,
                # api_pnl=0 + db_pnl!=0 means bet was never placed (Kalshi has no position)
                "unplaced": api_pnl == 0.0 and order["pnl"] != 0.0,
            })
        else:
            unmatched_count += 1

    unplaced_count = sum(1 for r in matched_rows if r["unplaced"])
    print(f"Matched {len(matched_rows)} / {len(db_orders)} live orders in DB")
    if unplaced_count:
        print(f"  ({unplaced_count} show API=$0.00 — bets never placed; zeroing DB P&L is correct)")
    print()

    header = f"  {'Order':>5}  {'Ticker':<30}  {'DB P&L':>8}  {'API P&L':>8}  {'Delta':>8}  Note"
    print(header)
    print("  " + "─" * (len(header)))

    matched_rows.sort(key=lambda r: r["id"])
    total_db = 0.0
    total_api = 0.0

    for row in matched_rows:
        delta = row["api_pnl"] - row["db_pnl"]
        total_db += row["db_pnl"]
        total_api += row["api_pnl"]
        note = "unplaced" if row["unplaced"] else ""
        print(
            f"  {row['id']:>5}  {row['ticker']:<30}  "
            f"{fmt_pnl(row['db_pnl']):>8}  {fmt_pnl(row['api_pnl']):>8}  "
            f"{fmt_pnl(delta):>8}  {note}"
        )

    if unmatched_count:
        print(f"\n  {unmatched_count} orders had no matching ticker in API (too old / still resting)")

    discrepancy = total_api - total_db
    pct = (abs(discrepancy) / abs(total_db) * 100) if total_db != 0 else 0.0
    print(f"\nTotal DB P&L  : {fmt_pnl(total_db)}")
    print(f"Total API P&L : {fmt_pnl(total_api)}")
    print(f"Discrepancy   : {fmt_pnl(discrepancy)}  ({pct:.1f}%)")

    if args.apply:
        with engine.begin() as conn:
            for row in matched_rows:
                conn.execute(
                    text("UPDATE kalshi_live_orders SET pnl = :pnl WHERE id = :id"),
                    {"pnl": row["api_pnl"], "id": row["id"]},
                )
        zeroed = sum(1 for r in matched_rows if r["unplaced"])
        print(f"\nApplied: updated {len(matched_rows)} rows ({zeroed} zeroed as unplaced)")
    else:
        unplaced_count = sum(1 for r in matched_rows if r["unplaced"])
        print(f"\nDry-run: {len(matched_rows)} rows would be updated ({unplaced_count} zeroed as unplaced). Use --apply to commit.")


if __name__ == "__main__":
    main()
