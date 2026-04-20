"""
Fix April 19 Kalshi PnL Bug
============================
21 NBA bets placed on Apr 19 2026 have NULL fill_price in the DB, causing
resolve_settled() to use fill_price=0 and produce garbage PnL.

Steps:
  1. Show current state
  2. Fetch real fill prices from Kalshi API (fallback to yes_price if no fills)
  3. Reset all Apr 19 orders to status='filled' so resolution can re-process
  4. Re-run resolve_settled() via KalshiLiveTrader
  5. Print corrected results and net PnL delta

Usage:
    cd /c/Users/Chase/Projects/GameFlowData
    python scripts/fix_apr19_pnl.py
"""

import sys
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

sys.path.insert(0, ".")

from src.db.client import get_engine  # noqa: E402
from src.paper_trading.kalshi_live_trader import KalshiLiveTrader  # noqa: E402
from src.scrapers.kalshi.kalshi_client import KalshiClient  # noqa: E402
from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee  # noqa: E402

TARGET_DATE = date(2026, 4, 19)


def step1_show_current_state(engine) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print(f"STEP 1 — Current state for {TARGET_DATE}")
    print("=" * 60)
    with engine.connect() as conn:
        orders = pd.read_sql(
            text("""
                SELECT id, kalshi_order_id, ticker, side, contracts,
                       kalshi_implied, fill_price, fill_count, total_cost, fee_paid,
                       status, pnl, stat_type
                FROM kalshi_live_orders
                WHERE game_date = :d
                ORDER BY placed_at
            """),
            conn,
            params={"d": TARGET_DATE},
        )

    print(f"Found {len(orders)} orders for {TARGET_DATE}")
    print(
        orders[["id", "ticker", "side", "kalshi_implied", "fill_price", "fill_count",
                "status", "pnl"]].to_string(index=False)
    )
    null_fp = orders["fill_price"].isna().sum()
    zero_fp = (orders["fill_price"] == 0).sum()
    pending = (orders["status"] == "pending").sum()
    print(f"\nNull fill_price: {null_fp}  |  Zero fill_price: {zero_fp}  |  Pending: {pending}")
    return orders


def step2_fix_fill_prices(engine, orders: pd.DataFrame, client: KalshiClient):
    print(f"\n{'='*60}")
    print("STEP 2 — Fetching real fill prices from Kalshi API")
    print("=" * 60)

    fixed = 0
    fallback = 0

    for _, order in orders.iterrows():
        order_id = order["kalshi_order_id"]
        row_id = int(order["id"])
        side = order["side"]
        # kalshi_implied is a decimal (0.0–1.0); convert to cents for fallback
        kalshi_implied = float(order["kalshi_implied"]) if pd.notna(order["kalshi_implied"]) else 0.50
        yes_price_snapshot = round(kalshi_implied * 100)

        # Fetch fills from Kalshi API
        fills = client.get_fills(order_id=order_id) if order_id else []

        if fills:
            # Compute weighted average yes_price from fills
            total_count = sum(f.get("count", 1) for f in fills)
            weighted_sum = sum(
                f.get("yes_price", f.get("price", yes_price_snapshot)) * f.get("count", 1)
                for f in fills
            )
            avg_price = round(weighted_sum / total_count) if total_count > 0 else yes_price_snapshot
            fill_count = total_count
            print(f"  Order {row_id} ({order_id[:8] if order_id else '?'}): "
                  f"{len(fills)} fill(s), avg_price={avg_price}, count={fill_count}")
            fixed += 1
        else:
            # Fallback to yes_price snapshot — better than 0
            avg_price = yes_price_snapshot
            fill_count = int(order["contracts"]) if pd.notna(order["contracts"]) else 1
            print(f"  Order {row_id} ({order_id[:8] if order_id else '?'}): "
                  f"no fills from API → fallback yes_price={avg_price}")
            fallback += 1

        # Recompute cost and fee
        if side == "yes":
            total_cost = fill_count * avg_price / 100.0
            fee_paid = kalshi_taker_fee(avg_price) * fill_count
        else:
            total_cost = fill_count * (100 - avg_price) / 100.0
            fee_paid = kalshi_taker_fee(100 - avg_price) * fill_count

        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE kalshi_live_orders
                    SET fill_price = :price,
                        fill_count = :count,
                        total_cost = :cost,
                        fee_paid = :fee
                    WHERE id = :id
                """),
                {
                    "price": avg_price,
                    "count": fill_count,
                    "cost": round(total_cost, 4),
                    "fee": round(fee_paid, 4),
                    "id": row_id,
                },
            )

    print(f"\nFixed {fixed} from API fills, {fallback} via yes_price fallback")


def step3_reset_to_filled(engine):
    print(f"\n{'='*60}")
    print("STEP 3 — Resetting all Apr 19 orders to status='filled'")
    print("=" * 60)
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE kalshi_live_orders
                SET status = 'filled',
                    pnl = 0,
                    resolved_at = NULL,
                    actual_value = NULL
                WHERE game_date = :d
            """),
            {"d": TARGET_DATE},
        )
    print(f"Reset {result.rowcount} orders to 'filled'")


def step4_resolve(trader: KalshiLiveTrader):
    print(f"\n{'='*60}")
    print("STEP 4 — Re-running resolve_settled()")
    print("=" * 60)
    result = trader.resolve_settled()
    print(f"Resolution result: {result}")

    print("\nUpdating daily log...")
    trader._update_daily_log(TARGET_DATE)
    print("Daily log updated")


def step5_print_results(engine, original_pnl_map: dict):
    print(f"\n{'='*60}")
    print("STEP 5 — Corrected results")
    print("=" * 60)
    with engine.connect() as conn:
        orders = pd.read_sql(
            text("""
                SELECT id, ticker, side, fill_price, fill_count, status, pnl, actual_value
                FROM kalshi_live_orders
                WHERE game_date = :d
                ORDER BY id
            """),
            conn,
            params={"d": TARGET_DATE},
        )

    print(orders[["id", "ticker", "side", "fill_price", "fill_count",
                   "status", "actual_value", "pnl"]].to_string(index=False))

    # Compute delta
    old_total = sum(v for v in original_pnl_map.values() if v is not None)
    new_total = orders["pnl"].fillna(0).sum()
    wins = (orders["status"] == "won").sum()
    losses = (orders["status"] == "lost").sum()
    cancelled = (orders["status"] == "cancelled").sum()
    still_pending = (orders["status"] == "pending").sum()

    print(f"\n{'='*60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"  {wins}W  {losses}L  {cancelled}C  {still_pending} still pending")
    print(f"  Old net PnL : ${old_total:.2f}")
    print(f"  New net PnL : ${new_total:.2f}")
    print(f"  Delta       : ${new_total - old_total:+.2f}")
    if still_pending:
        print(f"\n  WARNING: {still_pending} orders still 'pending' — actuals may be missing")


def main():
    engine = get_engine()
    client = KalshiClient()

    if not client.is_authenticated:
        print("ERROR: Kalshi client not authenticated — check KALSHI_PRIVATE_KEY_PATH / KALSHI_PRIVATE_KEY_B64")
        sys.exit(1)

    # Step 1
    orders = step1_show_current_state(engine)
    original_pnl_map = dict(zip(orders["id"], orders["pnl"]))

    # Step 2
    step2_fix_fill_prices(engine, orders, client)

    # Step 3
    step3_reset_to_filled(engine)

    # Step 4
    trader = KalshiLiveTrader(resolve_only=True)
    step4_resolve(trader)

    # Step 5
    step5_print_results(engine, original_pnl_map)


if __name__ == "__main__":
    main()
