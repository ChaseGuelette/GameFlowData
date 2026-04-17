"""
Kalshi Bet Audit Script
=======================
Validates P&L accuracy by reconciling three things:
  Part 1 — Settlement: our resolution vs Kalshi's official market result
  Part 2 — Slippage: assumed fill price vs simulated fill from orderbook depth
  Part 3 — Adjusted P&L: recalculated using Kalshi settlement + estimated real fill

The paper trader assumes instant fill at the displayed YES price with zero slippage.
This script checks both assumptions using Kalshi's API and stored orderbook snapshots.

Usage:
    python scripts/audit_kalshi_bets.py
    python scripts/audit_kalshi_bets.py --paper-only --sport nba
    python scripts/audit_kalshi_bets.py --skip-api
    python scripts/audit_kalshi_bets.py --date-from 2026-03-01 --date-to 2026-04-01
    python scripts/audit_kalshi_bets.py --csv-out my_audit.csv
"""

import argparse
import logging
import math
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee

load_dotenv()

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

W = 96
STALE_HOURS = 2.0  # Flag snapshots more than this many hours from placed_at

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def sep(char="─", width=W):
    print(char * width)


def header(title: str):
    print()
    print("━" * W)
    print(f"  {title}")
    print("━" * W)


def price_bucket(actual_price_cents: float) -> str:
    """Bucket by contract price (YES price for YES bets, NO price for NO bets)."""
    p = int(actual_price_cents)
    if p <= 10:
        return "01-10¢"
    if p <= 20:
        return "11-20¢"
    if p <= 35:
        return "21-35¢"
    if p <= 65:
        return "36-65¢"
    if p <= 80:
        return "66-80¢"
    return "81-99¢"


# ---------------------------------------------------------------------------
# Core business logic helpers
# ---------------------------------------------------------------------------


def derive_our_result(side: str, status: str) -> str:
    """Convert bet side + resolution status to a Kalshi market result ("yes"/"no").

    Resolution logic: actual >= line → YES wins.
      won + yes  → YES wins  → "yes"
      lost + yes → NO wins   → "no"
      won + no   → NO wins   → "no"
      lost + no  → YES wins  → "yes"
    """
    is_won = status in ("won", "overflow_won")
    if side == "yes":
        return "yes" if is_won else "no"
    else:
        return "no" if is_won else "yes"


def recalc_pnl(side: str, adj_fill_price: float, contracts: int, result: str) -> float:
    """Recalculate P&L using an adjusted fill price and a given settlement result.

    Mirrors the paper trader P&L formulas exactly (including taker fee).

    Args:
        side: "yes" or "no"
        adj_fill_price: Adjusted YES price in cents (int 1-99)
        contracts: Number of contracts
        result: "yes" or "no" (the market settlement result)

    Returns:
        P&L in dollars.
    """
    fill = adj_fill_price  # YES price in cents
    if side == "yes":
        fee_total = contracts * kalshi_taker_fee(fill)
        if result == "yes":  # YES wins
            return contracts * (100 - fill) / 100.0 - fee_total
        else:  # YES loses
            return -(contracts * fill / 100.0)
    else:  # side == "no"
        fee_total = contracts * kalshi_taker_fee(100 - fill)
        if result == "no":  # NO wins
            return contracts * fill / 100.0 - fee_total
        else:  # NO loses
            return -(contracts * (100 - fill) / 100.0)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_bets(engine, sport=None, date_from=None, date_to=None) -> pd.DataFrame:
    """Load resolved paper bets from kalshi_paper_bets."""
    resolved_statuses = ("won", "lost", "overflow_won", "overflow_lost")
    placeholders = ", ".join(f"'{s}'" for s in resolved_statuses)
    filters = [f"status IN ({placeholders})"]
    params: dict = {}
    if sport:
        filters.append("sport = :sport")
        params["sport"] = sport
    if date_from:
        filters.append("game_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        filters.append("game_date <= :date_to")
        params["date_to"] = date_to

    query = text(f"""
        SELECT id, game_date, sport, ticker, player_name, stat_type, side,
               price, fill_price, contracts, fee_adjusted_edge, expected_fee,
               pnl, status, placed_at, actual_value, line
        FROM kalshi_paper_bets
        WHERE {" AND ".join(filters)}
        ORDER BY game_date, placed_at
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    # Derived columns
    df["actual_price"] = df.apply(
        lambda r: int(r["fill_price"]) if r["side"] == "yes" else 100 - int(r["fill_price"]),
        axis=1,
    )
    df["our_result"] = df.apply(
        lambda r: derive_our_result(r["side"], r["status"]), axis=1
    )
    df["is_overflow"] = df["status"].str.startswith("overflow")
    df["price_bucket"] = df["actual_price"].apply(price_bucket)
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["placed_at"] = pd.to_datetime(df["placed_at"], utc=True)
    df["pnl"] = df["pnl"].astype(float)
    df["fill_price"] = df["fill_price"].astype(float)

    return df


def load_orderbook_snapshots(engine, tickers: list[str]) -> pd.DataFrame:
    """Batch load all orderbook snapshots for the given tickers."""
    if not tickers:
        return pd.DataFrame()
    query = text("""
        SELECT ticker, snapshot_time, depth, yes_bid, yes_ask
        FROM kalshi_orderbook_snapshots
        WHERE ticker = ANY(:tickers)
        ORDER BY ticker, snapshot_time
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"tickers": tickers})
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)
    return df


# ---------------------------------------------------------------------------
# Orderbook walking
# ---------------------------------------------------------------------------


def walk_orderbook(depth, side: str, contracts: int) -> tuple[float | None, bool]:
    """Simulate filling contracts against an orderbook, returning VWAP in YES-price space.

    Orderbook format (from kalshi_refresh_job.py):
      depth["yes"] = [[yes_price_cents, size], ...]  sorted highest-first  (YES bids)
      depth["no"]  = [[no_price_cents, size], ...]   sorted highest-first  (NO bids)

    For a YES bet (taker buying YES):
      Cross NO bids → each [no_bid, size] → YES fill price = 100 - no_bid
      (highest NO bid = lowest YES ask = best fill for YES buyer)

    For a NO bet (taker buying NO):
      Cross YES bids → each [yes_bid, size] → YES price in fill_price convention = yes_bid
      (highest YES bid = lowest NO ask = best fill for NO buyer)

    Slippage sign convention:
      YES bet: slippage = vwap - fill_price  (positive = paid more YES = bad)
      NO bet:  slippage = fill_price - vwap  (positive = lower YES price = higher NO cost = bad)

    Returns:
        (vwap_yes_price, insufficient_liquidity)
        vwap_yes_price: Weighted average YES price across filled levels (None if book empty)
        insufficient_liquidity: True if total available depth < contracts requested
    """
    import json

    if depth is None or contracts <= 0:
        return None, True

    if isinstance(depth, str):
        try:
            depth = json.loads(depth)
        except Exception:
            return None, True

    if not isinstance(depth, dict):
        return None, True

    if side == "yes":
        # Cross NO bids; fill YES at (100 - no_bid_price)
        levels = depth.get("no", [])

        def get_yes_price(lv):
            return 100 - lv[0]
    else:
        # Cross YES bids; YES price in fill_price convention = yes_bid_price
        levels = depth.get("yes", [])

        def get_yes_price(lv):
            return lv[0]

    if not levels:
        return None, True

    filled = 0
    total_weighted = 0.0

    for level in levels:
        if len(level) < 2:
            continue
        lvl_size = int(level[1])
        yes_price = float(get_yes_price(level))
        take = min(lvl_size, contracts - filled)
        total_weighted += take * yes_price
        filled += take
        if filled >= contracts:
            break

    if filled == 0:
        return None, True

    vwap = total_weighted / filled
    return vwap, filled < contracts


def find_nearest_snapshot(
    ticker: str,
    placed_at: pd.Timestamp,
    snapshots_by_ticker: dict[str, pd.DataFrame],
) -> tuple["pd.Series | None", "float | None"]:
    """Find the orderbook snapshot closest in time to placed_at for a ticker.

    Returns:
        (snapshot_row, hours_diff)  where hours_diff = abs(snapshot_time - placed_at) in hours.
    """
    snaps = snapshots_by_ticker.get(ticker)
    if snaps is None or snaps.empty:
        return None, None

    deltas = (snaps["snapshot_time"] - placed_at).abs()
    idx = deltas.idxmin()
    row = snaps.loc[idx]
    hours_diff = deltas[idx].total_seconds() / 3600.0
    return row, hours_diff


# ---------------------------------------------------------------------------
# Part 1: Settlement Reconciliation
# ---------------------------------------------------------------------------


def run_settlement_reconciliation(df: pd.DataFrame, client: KalshiClient) -> pd.DataFrame:
    """Fetch Kalshi's official settlement for every resolved bet.

    Calls client.get_market(ticker) for each unique ticker.
    Compares our derived result to Kalshi's result field.

    Returns:
        df with added columns:
          kalshi_result         — "yes" / "no" / None (unsettled)
          settlement_mismatch   — True / False / None (no kalshi result)
          settlement_pnl_impact — P&L delta from any mismatch (dollars)
    """
    unique_tickers = df["ticker"].unique().tolist()
    print(f"\n  Fetching settlement for {len(unique_tickers):,} unique tickers...")
    print(f"  (Estimated time: ~{len(unique_tickers) * 0.1:.0f}s at 0.1s/request)")

    try:
        from tqdm import tqdm
        ticker_iter = tqdm(unique_tickers, desc="  Settlement lookup", ncols=W)
    except ImportError:
        ticker_iter = unique_tickers

    ticker_results: dict[str, str | None] = {}
    for ticker in ticker_iter:
        result = client.get_market(ticker)
        if result is None:
            ticker_results[ticker] = None
            continue
        market = result.get("market", {})
        raw_result = market.get("result", "")
        ticker_results[ticker] = raw_result if raw_result in ("yes", "no") else None

    df = df.copy()
    df["kalshi_result"] = df["ticker"].map(ticker_results)

    # Determine mismatches for tickers that have settled on Kalshi.
    # Must use pd.isna() — pandas converts None → NaN (float) in Series,
    # so `is None` is always False for missing values.
    def check_mismatch(row):
        if pd.isna(row["kalshi_result"]):
            return None  # Not settled / API returned no result — unverifiable
        return bool(row["our_result"] != row["kalshi_result"])

    df["settlement_mismatch"] = df.apply(check_mismatch, axis=1)

    # P&L impact: delta between our P&L and the P&L we'd have with correct settlement
    def calc_settlement_impact(row):
        if not row["settlement_mismatch"]:
            return 0.0
        adj = recalc_pnl(
            row["side"], float(row["fill_price"]), int(row["contracts"]), row["kalshi_result"]
        )
        return adj - float(row["pnl"])

    df["settlement_pnl_impact"] = df.apply(
        lambda r: calc_settlement_impact(r) if r["settlement_mismatch"] else 0.0,
        axis=1,
    )

    settled_count = df["kalshi_result"].notna().sum()
    mismatch_count = df["settlement_mismatch"].sum()
    rate = mismatch_count / settled_count if settled_count > 0 else 0.0
    print(f"  Settled: {settled_count:,}  /  Mismatches: {mismatch_count:,}  ({rate:.2%})")

    return df


# ---------------------------------------------------------------------------
# Part 2: Slippage Estimation
# ---------------------------------------------------------------------------


def run_slippage_estimation(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Estimate fill price slippage by walking the stored orderbook depth.

    For each bet, finds the nearest kalshi_orderbook_snapshots row by
    (ticker, snapshot_time closest to placed_at), then simulates filling
    the contract count against that snapshot's depth.

    Returns:
        df with added columns:
          vwap_yes_price       — simulated VWAP in YES-price space (None if no data)
          slippage_cents       — fill_price delta (positive = worse than assumed)
          snap_hours_diff      — hours between snapshot and placed_at
          insufficient_liquidity — True if snapshot depth < contracts
    """
    unique_tickers = df["ticker"].unique().tolist()
    print(f"\n  Loading orderbook snapshots for {len(unique_tickers):,} tickers...")

    snaps_df = load_orderbook_snapshots(engine, unique_tickers)

    # Initialize output columns
    df = df.copy()
    df["vwap_yes_price"] = None
    df["slippage_cents"] = None
    df["snap_hours_diff"] = None
    df["insufficient_liquidity"] = None

    if snaps_df.empty:
        print("  No orderbook snapshots found in DB.")
        return df

    print(f"  Found {len(snaps_df):,} snapshot rows across {snaps_df['ticker'].nunique():,} tickers.")

    # Group by ticker for O(1) lookup
    snapshots_by_ticker: dict[str, pd.DataFrame] = {
        ticker: grp.reset_index(drop=True)
        for ticker, grp in snaps_df.groupby("ticker")
    }

    vwap_prices = []
    slippages = []
    hours_diffs = []
    insufficient = []

    for _, row in df.iterrows():
        snap, hours_diff = find_nearest_snapshot(
            row["ticker"], row["placed_at"], snapshots_by_ticker
        )

        if snap is None:
            vwap_prices.append(None)
            slippages.append(None)
            hours_diffs.append(None)
            insufficient.append(None)
            continue

        vwap, insuff = walk_orderbook(snap["depth"], row["side"], int(row["contracts"]))

        if vwap is None:
            vwap_prices.append(None)
            slippages.append(None)
            hours_diffs.append(hours_diff)
            insufficient.append(True)
            continue

        fill_price = float(row["fill_price"])
        if row["side"] == "yes":
            slippage = vwap - fill_price      # positive = paid more for YES (bad)
        else:
            slippage = fill_price - vwap      # positive = got lower YES price = higher NO cost (bad)

        vwap_prices.append(vwap)
        slippages.append(slippage)
        hours_diffs.append(hours_diff)
        insufficient.append(insuff)

    df["vwap_yes_price"] = vwap_prices
    df["slippage_cents"] = slippages
    df["snap_hours_diff"] = hours_diffs
    df["insufficient_liquidity"] = insufficient

    with_slip = df["vwap_yes_price"].notna().sum()
    print(f"  Slippage estimated for {with_slip:,}/{len(df):,} bets.")

    return df


# ---------------------------------------------------------------------------
# Part 3: Adjusted P&L
# ---------------------------------------------------------------------------


def run_adjusted_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """Compute adjusted P&L using best available settlement and fill price.

    Settlement:  Kalshi API result if available, else our resolution.
    Fill price:  VWAP from orderbook walk if available, else original fill_price.

    Returns:
        df with added columns:
          adj_settlement — settlement used for adjusted calc
          adj_fill_price — fill price used for adjusted calc
          adj_pnl        — recalculated P&L
          pnl_delta      — adj_pnl - original pnl
    """
    df = df.copy()
    adj_settlements = []
    adj_fills = []
    adj_pnls = []

    for _, row in df.iterrows():
        # Settlement
        kr = row["kalshi_result"]
        settlement = kr if kr in ("yes", "no") else row["our_result"]

        # Fill price
        vwap = row["vwap_yes_price"]
        if vwap is not None and not math.isnan(float(vwap)):
            adj_fill = float(vwap)
        else:
            adj_fill = float(row["fill_price"])

        adj_pnl = recalc_pnl(row["side"], adj_fill, int(row["contracts"]), settlement)
        adj_settlements.append(settlement)
        adj_fills.append(adj_fill)
        adj_pnls.append(round(adj_pnl, 4))

    df["adj_settlement"] = adj_settlements
    df["adj_fill_price"] = adj_fills
    df["adj_pnl"] = adj_pnls
    df["pnl_delta"] = df["adj_pnl"] - df["pnl"]

    return df


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------


def report_settlement(df: pd.DataFrame):
    """Part 1 — Settlement reconciliation."""
    header("PART 1 — SETTLEMENT RECONCILIATION")

    checkable = df[df["settlement_mismatch"].notna()]
    unsettled = df[df["kalshi_result"].isna()]
    mismatches = df[df["settlement_mismatch"]]

    total = len(df)
    print(f"  Total bets           : {total:,}")
    print(f"  Kalshi settled       : {len(checkable):,}  ({len(checkable)/total*100:.1f}%)")
    print(f"  Not yet settled/API  : {len(unsettled):,}  ({len(unsettled)/total*100:.1f}%)")
    print()

    if len(checkable) == 0:
        print("  No settled markets found via API (all unsettled or --skip-api mode).")
        sep()
        return

    mismatch_rate = len(mismatches) / len(checkable)
    print(f"  Settlement mismatches: {len(mismatches):,}  ({mismatch_rate:.2%} of settled)")

    if len(mismatches) > 0:
        pnl_impact = mismatches["settlement_pnl_impact"].sum()
        print(f"  P&L impact of mismatches: ${pnl_impact:+.2f}")
        print()

        # Per-mismatch detail (up to 20 rows)
        print("  Mismatch detail:")
        col_widths = [12, 42, 5, 5, 8, 12, 10]
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print("  " + fmt.format("Date", "Ticker", "Side", "Our", "Kalshi", "Orig P&L", "Delta"))
        sep("·")
        for _, row in mismatches.head(20).iterrows():
            print("  " + fmt.format(
                str(row["game_date"]),
                row["ticker"][:42],
                row["side"],
                row["our_result"],
                str(row["kalshi_result"]),
                f"${row['pnl']:+.2f}",
                f"${row['settlement_pnl_impact']:+.2f}",
            ))
        if len(mismatches) > 20:
            print(f"  ... and {len(mismatches) - 20} more (see CSV output)")
        sep()
    else:
        print()
        print("  ✓  ZERO settlement mismatches — our stat-based resolution logic is correct.")
        sep()


def report_slippage(df: pd.DataFrame):
    """Part 2 — Slippage estimation."""
    header("PART 2 — SLIPPAGE ESTIMATION  (positive slippage = worse than assumed)")

    with_vwap = df[df["vwap_yes_price"].notna()]
    no_snap = df[df["snap_hours_diff"].isna()]
    snap_found = df[df["snap_hours_diff"].notna()]
    stale = snap_found[snap_found["snap_hours_diff"] > STALE_HOURS]
    insuff = df[df["insufficient_liquidity"]]

    total = len(df)
    print(f"  Total bets              : {total:,}")
    print(f"  With VWAP estimate      : {len(with_vwap):,}  ({len(with_vwap)/total*100:.1f}%)")
    print(f"  No snapshot in DB       : {len(no_snap):,}")
    print(f"  Stale snapshot (>{STALE_HOURS:.0f}h)  : {len(stale):,}")
    print(f"  Insufficient liquidity  : {len(insuff):,}")
    print()

    if with_vwap.empty:
        print("  No orderbook data available for slippage estimation.")
        print("  Run kalshi_refresh_job.py to populate kalshi_orderbook_snapshots.")
        sep()
        return

    avg_slip = with_vwap["slippage_cents"].mean()
    med_slip = with_vwap["slippage_cents"].median()
    print(f"  Overall slippage (¢)    : avg={avg_slip:+.3f}  median={med_slip:+.3f}")
    print()

    # By price bucket
    bucket_order = ["01-10¢", "11-20¢", "21-35¢", "36-65¢", "66-80¢", "81-99¢"]
    print("  SLIPPAGE BY PRICE BUCKET:")
    col_widths = [10, 6, 9, 9, 9, 8, 10]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print("  " + fmt.format("Bucket", "Bets", "AvgSlip", "MedSlip", "MaxSlip", "Insuff", "PnL Delta"))
    sep("·")
    for bucket in bucket_order:
        grp = with_vwap[with_vwap["price_bucket"] == bucket]
        if len(grp) == 0:
            continue
        avg_s = grp["slippage_cents"].mean()
        med_s = grp["slippage_cents"].median()
        max_s = grp["slippage_cents"].max()
        n_insuff = int(grp["insufficient_liquidity"].sum())
        pdelta = grp["pnl_delta"].sum()
        flag = "  ⚠" if avg_s > 2.5 else ""
        print("  " + fmt.format(
            bucket, len(grp),
            f"{avg_s:+.2f}¢", f"{med_s:+.2f}¢", f"{max_s:+.2f}¢",
            str(n_insuff),
            f"${pdelta:+.2f}",
        ) + flag)

    sep("·")

    # Deep dive: 0-10c bucket
    low = with_vwap[with_vwap["price_bucket"] == "01-10¢"]
    print()
    if not low.empty:
        print(f"  ★  0-10¢ BUCKET DEEP DIVE  ({len(low):,} bets)")
        print(f"     Avg slippage     : {low['slippage_cents'].mean():+.3f}¢")
        print(f"     Median slippage  : {low['slippage_cents'].median():+.3f}¢")
        print(f"     Max slippage     : {low['slippage_cents'].max():+.3f}¢")
        n_insuff_low = int(low["insufficient_liquidity"].sum())
        print(f"     Insuff liquidity : {n_insuff_low}/{len(low)}")
        print(f"     P&L delta        : ${low['pnl_delta'].sum():+.2f}")
        print()
        # Slippage > 3¢ threshold
        big_slip = low[low["slippage_cents"] > 3.0]
        print(f"     Bets with >3¢ slippage: {len(big_slip)}/{len(low)}")
        if avg_slip > 2.0:
            print()
            print("     ⚠  Avg slippage in 0-10¢ bucket exceeds 2¢ — paper P&L is likely overstated.")
    else:
        print("  No bets in 0-10¢ bucket found in snapshot data.")

    sep()


def report_adjusted_pnl(df: pd.DataFrame):
    """Part 3 — Adjusted P&L."""
    header("PART 3 — ADJUSTED P&L  (settlement corrections + slippage)")

    orig_total = df["pnl"].sum()
    adj_total = df["adj_pnl"].sum()
    delta_total = adj_total - orig_total
    delta_pct = delta_total / abs(orig_total) * 100 if orig_total != 0 else 0.0

    print(f"  Original P&L    : ${orig_total:>+12,.2f}")
    print(f"  Adjusted P&L    : ${adj_total:>+12,.2f}")
    print(f"  Total delta     : ${delta_total:>+12,.2f}  ({delta_pct:+.1f}% of original)")
    print()

    # Decompose: settlement vs slippage
    settle_delta = df["settlement_pnl_impact"].sum() if "settlement_pnl_impact" in df.columns else 0.0
    slip_delta = delta_total - settle_delta
    print(f"  ↳ Settlement corrections : ${settle_delta:>+10,.2f}")
    print(f"  ↳ Slippage adjustments   : ${slip_delta:>+10,.2f}")
    print()

    # By price bucket
    bucket_order = ["01-10¢", "11-20¢", "21-35¢", "36-65¢", "66-80¢", "81-99¢"]
    print("  BY PRICE BUCKET:")
    col_widths = [10, 6, 12, 12, 10, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print("  " + fmt.format("Bucket", "Bets", "Orig P&L", "Adj P&L", "Delta", "Delta%"))
    sep("·")
    for bucket in bucket_order:
        grp = df[df["price_bucket"] == bucket]
        if len(grp) == 0:
            continue
        op = grp["pnl"].sum()
        ap = grp["adj_pnl"].sum()
        d = ap - op
        dpct = d / abs(op) * 100 if op != 0 else 0.0
        flag = "  ⚠" if abs(dpct) > 10 else ""
        print("  " + fmt.format(
            bucket, len(grp),
            f"${op:>+,.2f}", f"${ap:>+,.2f}",
            f"${d:>+,.2f}", f"{dpct:>+.1f}%",
        ) + flag)
    sep("·")

    # By stat type
    print()
    print("  BY STAT TYPE:")
    fmt2 = "  ".join(f"{{:<{w}}}" for w in [28, 6, 12, 12, 10])
    print("  " + fmt2.format("Stat", "Bets", "Orig P&L", "Adj P&L", "Delta"))
    sep("·")
    for stat in sorted(df["stat_type"].unique()):
        grp = df[df["stat_type"] == stat]
        op = grp["pnl"].sum()
        ap = grp["adj_pnl"].sum()
        print("  " + fmt2.format(stat, len(grp), f"${op:>+,.2f}", f"${ap:>+,.2f}", f"${ap-op:>+,.2f}"))
    sep("·")

    # By sport
    print()
    print("  BY SPORT:")
    fmt3 = "  ".join(f"{{:<{w}}}" for w in [10, 6, 12, 12, 10])
    print("  " + fmt3.format("Sport", "Bets", "Orig P&L", "Adj P&L", "Delta"))
    sep("·")
    for sport in sorted(df["sport"].unique()):
        grp = df[df["sport"] == sport]
        op = grp["pnl"].sum()
        ap = grp["adj_pnl"].sum()
        print("  " + fmt3.format(sport.upper(), len(grp), f"${op:>+,.2f}", f"${ap:>+,.2f}", f"${ap-op:>+,.2f}"))
    sep("·")

    # Reliability verdict
    print()
    abs_delta_pct = abs(delta_pct)
    if abs_delta_pct < 5:
        verdict = "✓  AUDIT PASSED — P&L adjustment < 5%. Results are reliable."
    elif abs_delta_pct < 10:
        verdict = "~  MINOR CONCERN — P&L adjustment 5-10%. Monitor slippage in low-price buckets."
    else:
        verdict = "✗  SIGNIFICANT CONCERN — P&L adjustment > 10%. Paper results may overstate performance."

    print(f"  {'═' * 60}")
    print(f"  VERDICT: {verdict}")
    print(f"  {'═' * 60}")
    sep()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------


def save_csv(df: pd.DataFrame, path: str):
    """Export per-bet audit details to CSV."""
    cols = [
        "id", "game_date", "sport", "ticker", "player_name", "stat_type",
        "side", "fill_price", "actual_price", "contracts",
        "our_result", "kalshi_result", "settlement_mismatch", "settlement_pnl_impact",
        "vwap_yes_price", "slippage_cents", "snap_hours_diff", "insufficient_liquidity",
        "pnl", "adj_fill_price", "adj_settlement", "adj_pnl", "pnl_delta",
        "price_bucket", "status", "is_overflow",
    ]
    out_cols = [c for c in cols if c in df.columns]
    df[out_cols].to_csv(path, index=False, float_format="%.4f")
    print(f"\n  CSV exported: {path}  ({len(df):,} rows, {len(out_cols)} columns)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kalshi bet audit: settlement reconciliation + slippage + adjusted P&L"
    )
    parser.add_argument(
        "--paper-only", action="store_true", dest="paper_only",
        help="Only include paper bets (default: true; live orders not yet in DB)",
    )
    parser.add_argument(
        "--skip-api", action="store_true", dest="skip_api",
        help="Skip Kalshi API calls (Part 1 settlement lookup). Slippage still runs.",
    )
    parser.add_argument(
        "--sport", type=str, default=None, choices=["nba", "mlb"],
        help="Filter by sport",
    )
    parser.add_argument(
        "--date-from", type=str, default=None, dest="date_from",
        help="Start date (YYYY-MM-DD inclusive)",
    )
    parser.add_argument(
        "--date-to", type=str, default=None, dest="date_to",
        help="End date (YYYY-MM-DD inclusive)",
    )
    parser.add_argument(
        "--csv-out", type=str, default="audit_results.csv", dest="csv_out",
        help="Output CSV filename (default: audit_results.csv)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print()
    print("═" * W)
    print("  KALSHI BET AUDIT  —  Settlement · Slippage · Adjusted P&L")
    print("═" * W)

    engine = get_engine()

    # Load bets
    print("\n  Loading resolved paper bets...")
    df = load_bets(
        engine,
        sport=args.sport,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    if df.empty:
        print("  No resolved bets found for the given filters.")
        sys.exit(0)

    print(f"  Loaded {len(df):,} bets  ({df['game_date'].min()} → {df['game_date'].max()})")
    print(f"  Sports     : {', '.join(sorted(df['sport'].unique()))}")
    print(f"  Stat types : {', '.join(sorted(df['stat_type'].unique()))}")
    print(f"  Tickers    : {df['ticker'].nunique():,} unique")
    real_bets = df[~df["is_overflow"]]
    print(f"  Real bets  : {len(real_bets):,}  /  Overflow: {len(df) - len(real_bets):,}")

    # Part 1: Settlement Reconciliation
    if not args.skip_api:
        client = KalshiClient()
        if not client.is_authenticated:
            print(
                "\n  WARNING: No Kalshi credentials — skipping Part 1.\n"
                "  Set KALSHI_API_KEY + KALSHI_PRIVATE_KEY_PATH (or _B64) to enable."
            )
            df["kalshi_result"] = None
            df["settlement_mismatch"] = None
            df["settlement_pnl_impact"] = 0.0
        else:
            df = run_settlement_reconciliation(df, client)
    else:
        print("\n  [--skip-api] Skipping Part 1 — settlement reconciliation.")
        df["kalshi_result"] = None
        df["settlement_mismatch"] = None
        df["settlement_pnl_impact"] = 0.0

    # Part 2: Slippage Estimation
    df = run_slippage_estimation(df, engine)

    # Part 3: Adjusted P&L (must run before reports to populate pnl_delta)
    df = run_adjusted_pnl(df)

    # Reports
    report_settlement(df)
    report_slippage(df)
    report_adjusted_pnl(df)

    # CSV export
    save_csv(df, args.csv_out)

    print()
    print("═" * W)
    print("  Audit complete.")
    print("═" * W)
    print()


if __name__ == "__main__":
    main()
