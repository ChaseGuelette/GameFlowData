"""
Kalshi Bet Category Analysis
=============================
Per-category diagnostic breakdown of model performance for Kalshi paper
and live trading. Identifies degrading categories, miscalibrated price
zones, and specific losing players.

Sections:
  1. Category breakdown (stat_type x line x side)
  2. Model calibration per category (model_prob vs actual win rate)
  3. Yes_price bucket analysis (batter_hits focus)
  4. Weekly trend per category (last 3 weeks)
  5. Player-level losers (top losers by PnL)

Usage:
    python scripts/kalshi_bet_category_analysis.py --days 21
    python scripts/kalshi_bet_category_analysis.py --sport mlb --stat-type batter_hits
    python scripts/kalshi_bet_category_analysis.py --table live
    python scripts/kalshi_bet_category_analysis.py --csv-out report.csv
"""

import argparse
import math
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine

load_dotenv()

# ---------------------------------------------------------------------------
# Helpers (consistent with analyze_kalshi_paper_bets.py)
# ---------------------------------------------------------------------------


def taker_fee_per_contract(price_cents: float) -> float:
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def break_even_win_rate(price_cents: float) -> float:
    cost_per = price_cents / 100.0
    fee_per = taker_fee_per_contract(price_cents)
    return cost_per / (1.0 - fee_per) if (1.0 - fee_per) > 0 else cost_per


def fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def fmt_roi(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.1f}%"


W = 110


def sep(char="─", width=W):
    print(char * width)


def header(title: str):
    print()
    print("━" * W)
    print(f"  {title}")
    print("━" * W)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_paper_bets(engine, sport=None, date_start=None, date_end=None,
                    stat_type=None) -> pd.DataFrame:
    """Load resolved paper bets from kalshi_paper_bets."""
    resolved_statuses = ("won", "lost", "overflow_won", "overflow_lost")
    placeholders = ", ".join(f"'{s}'" for s in resolved_statuses)
    filters = [f"status IN ({placeholders})"]
    params: dict = {}
    if sport:
        filters.append("sport = :sport")
        params["sport"] = sport
    if date_start:
        filters.append("game_date >= :date_start")
        params["date_start"] = date_start
    if date_end:
        filters.append("game_date <= :date_end")
        params["date_end"] = date_end
    if stat_type:
        filters.append("stat_type = :stat_type")
        params["stat_type"] = stat_type

    query = text(f"""
        SELECT id, game_date, sport, player_id, player_name, stat_type, line, side,
               price, contracts, model_prob, kalshi_implied, fee_adjusted_edge,
               pnl, status
        FROM kalshi_paper_bets
        WHERE {" AND ".join(filters)}
        ORDER BY game_date, placed_at
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return df

    # `price` column = market YES price in cents
    df["market_yes_price"] = df["price"].astype(int)
    # actual_price = what the bettor pays per contract
    df["actual_price"] = df.apply(
        lambda r: int(r["price"]) if r["side"] == "yes" else 100 - int(r["price"]),
        axis=1,
    )
    df["cost_per"] = df["actual_price"] / 100.0
    df["fee_per"] = df["actual_price"].apply(taker_fee_per_contract)
    df["break_even"] = df["actual_price"].apply(break_even_win_rate)
    df["is_won"] = df["status"].isin(["won", "overflow_won"])
    df["total_cost"] = df["cost_per"] * df["contracts"]
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["week"] = df["game_date"].apply(lambda d: d - timedelta(days=d.weekday()))
    df["category"] = df.apply(
        lambda r: f"{r['stat_type']} {r['line']} {r['side'].upper()}", axis=1
    )
    return df


def load_live_orders(engine, sport=None, date_start=None, date_end=None,
                     stat_type=None) -> pd.DataFrame:
    """Load resolved live orders from kalshi_live_orders."""
    filters = ["status IN ('won', 'lost')"]
    params: dict = {}
    if sport:
        filters.append("sport = :sport")
        params["sport"] = sport
    if date_start:
        filters.append("game_date >= :date_start")
        params["date_start"] = date_start
    if date_end:
        filters.append("game_date <= :date_end")
        params["date_end"] = date_end
    if stat_type:
        filters.append("stat_type = :stat_type")
        params["stat_type"] = stat_type

    query = text(f"""
        SELECT id, game_date, sport, player_id, player_name, stat_type, line, side,
               fill_price, fill_count, total_cost, fee_paid,
               model_prob, kalshi_implied, edge, fee_adjusted_edge,
               pnl, status
        FROM kalshi_live_orders
        WHERE {" AND ".join(filters)}
          AND fill_price IS NOT NULL
        ORDER BY game_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return df

    # fill_price = YES price at fill time
    df["market_yes_price"] = df["fill_price"].astype(int)
    df["price"] = df["fill_price"]  # normalize column name
    df["contracts"] = df["fill_count"]
    df["actual_price"] = df.apply(
        lambda r: int(r["fill_price"]) if r["side"] == "yes"
        else 100 - int(r["fill_price"]),
        axis=1,
    )
    df["cost_per"] = df["actual_price"] / 100.0
    df["fee_per"] = df["actual_price"].apply(taker_fee_per_contract)
    df["break_even"] = df["actual_price"].apply(break_even_win_rate)
    df["is_won"] = df["status"] == "won"
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["week"] = df["game_date"].apply(lambda d: d - timedelta(days=d.weekday()))
    df["category"] = df.apply(
        lambda r: f"{r['stat_type']} {r['line']} {r['side'].upper()}", axis=1
    )
    return df


# ---------------------------------------------------------------------------
# Section 1: Category breakdown
# ---------------------------------------------------------------------------


def section_category_breakdown(df: pd.DataFrame):
    header("CATEGORY BREAKDOWN  (stat_type x line x side)")

    total_bets = len(df)
    rows = []
    for cat in df["category"].unique():
        grp = df[df["category"] == cat]
        n = len(grp)
        wins = int(grp["is_won"].sum())
        wr = wins / n if n else 0
        pnl = float(grp["pnl"].sum())
        cost = float(grp["total_cost"].sum())
        roi = pnl / cost if cost > 0 else 0
        avg_be = float(grp["break_even"].mean())
        alpha = wr - avg_be
        vol_pct = n / total_bets if total_bets else 0
        avg_mp = float(grp["model_prob"].mean()) if not grp["model_prob"].isna().all() else None
        rows.append((cat, n, wins, wr, avg_be, alpha, pnl, roi, vol_pct, avg_mp))

    # Sort by PnL descending
    rows.sort(key=lambda r: r[6], reverse=True)

    col_w = [32, 6, 6, 7, 7, 7, 10, 8, 7, 7]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format("Category", "Bets", "Won", "Win%", "BE%", "Alpha",
                      "P&L", "ROI", "Vol%", "AvgMP"))
    sep("·")

    for cat, n, wins, wr, avg_be, alpha, pnl, roi, vol_pct, avg_mp in rows:
        mp_str = fmt_pct(avg_mp) if avg_mp is not None else "—"
        print(fmt.format(
            cat[:32], n, wins,
            fmt_pct(wr), fmt_pct(avg_be), fmt_pct(alpha),
            f"${pnl:+.0f}", fmt_roi(roi), fmt_pct(vol_pct), mp_str,
        ))
    sep()

    # Summary
    total_pnl = float(df["pnl"].sum())
    total_cost = float(df["total_cost"].sum())
    total_roi = total_pnl / total_cost if total_cost > 0 else 0
    total_wr = float(df["is_won"].mean())
    print(f"  TOTAL: {len(df)} bets | Win%: {fmt_pct(total_wr)} | "
          f"P&L: ${total_pnl:+,.0f} | ROI: {fmt_roi(total_roi)}")
    sep()


# ---------------------------------------------------------------------------
# Section 2: Model calibration per category
# ---------------------------------------------------------------------------


def section_model_calibration(df: pd.DataFrame):
    header("MODEL CALIBRATION PER CATEGORY  (model_prob vs actual win rate)")

    if "model_prob" not in df.columns or df["model_prob"].isna().all():
        print("  (no model_prob data available)")
        sep()
        return

    categories = sorted(df["category"].unique())

    for cat in categories:
        grp = df[df["category"] == cat].copy()
        if len(grp) < 20 or grp["model_prob"].isna().all():
            continue

        print(f"\n  -- {cat} ({len(grp)} bets) --")

        # bet_win_prob: model's probability that THIS bet wins.
        # model_prob = P(over). YES wins if over, NO wins if under.
        grp["bet_win_prob"] = grp.apply(
            lambda r: float(1 - r["model_prob"]) if r["side"] == "no"
            else float(r["model_prob"]),
            axis=1,
        )

        # Bucket by bet_win_prob in bands
        bins = [0, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        labels = ["0-20%", "20-30%", "30-40%", "40-50%", "50-60%",
                  "60-70%", "70-80%", "80-90%", "90-100%"]
        grp["prob_bucket"] = pd.cut(grp["bet_win_prob"], bins=bins,
                                     labels=labels, include_lowest=True)

        col_w = [10, 6, 7, 7, 7, 6]
        fmt = "  ".join(f"{{:<{w}}}" for w in col_w)
        print("    " + fmt.format("Model P", "Bets", "Actual", "Model",
                                   "Gap", "Flag"))
        print("    " + "·" * 52)

        for bucket in labels:
            bucket_grp = grp[grp["prob_bucket"] == bucket]
            if len(bucket_grp) < 5:
                continue
            actual_wr = float(bucket_grp["is_won"].mean())
            model_avg = float(bucket_grp["bet_win_prob"].mean())
            gap = actual_wr - model_avg
            flag = " !!" if abs(gap) > 0.10 else ""
            print("    " + fmt.format(
                bucket, len(bucket_grp),
                fmt_pct(actual_wr), fmt_pct(model_avg),
                f"{gap:+.1%}", flag,
            ))

    sep()


# ---------------------------------------------------------------------------
# Section 3: Yes_price bucket analysis
# ---------------------------------------------------------------------------


def _yes_price_bucket(p: int) -> str:
    if p < 50:
        return "<50"
    if p < 60:
        return "50-59"
    if p < 65:
        return "60-64"
    if p < 68:
        return "65-67"
    if p < 70:
        return "68-69"
    if p < 72:
        return "70-71"
    if p < 75:
        return "72-74"
    if p < 80:
        return "75-79"
    return "80+"


_YP_BUCKET_ORDER = ["<50", "50-59", "60-64", "65-67", "68-69",
                     "70-71", "72-74", "75-79", "80+"]


def section_yes_price_analysis(df: pd.DataFrame):
    header("YES_PRICE BUCKET ANALYSIS  (batter_hits NO bets)")

    hits = df[(df["stat_type"] == "batter_hits") & (df["side"] == "no")]
    if hits.empty:
        # Fall back to all batter_hits if no NO bets
        hits = df[df["stat_type"] == "batter_hits"]
    if hits.empty:
        print("  (no batter_hits data)")
        sep()
        return

    hits = hits.copy()
    hits["yp_bucket"] = hits["market_yes_price"].apply(_yes_price_bucket)

    col_w = [10, 6, 6, 7, 10, 8, 10]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format("Yes Price", "Bets", "Won", "Win%", "P&L", "PnL/Bet",
                      "Verdict"))
    sep("·")

    for bucket in _YP_BUCKET_ORDER:
        grp = hits[hits["yp_bucket"] == bucket]
        if grp.empty:
            continue
        n = len(grp)
        wins = int(grp["is_won"].sum())
        wr = wins / n
        pnl = float(grp["pnl"].sum())
        pnl_per = pnl / n

        if pnl > 50 and wr > 0.40:
            verdict = "Strong"
        elif pnl > 0:
            verdict = "OK"
        elif pnl > -50:
            verdict = "Break-even"
        else:
            verdict = "LOSING"

        print(fmt.format(
            bucket, n, wins, fmt_pct(wr),
            f"${pnl:+.0f}", f"${pnl_per:+.2f}", verdict,
        ))
    sep()

    # Highlight zones
    problem = hits[hits["market_yes_price"].between(65, 71)]
    if not problem.empty:
        p_pnl = float(problem["pnl"].sum())
        p_wr = float(problem["is_won"].mean())
        print(f"  !!  PROBLEM ZONE (yes_price 65-71): {len(problem)} bets | "
              f"Win%: {fmt_pct(p_wr)} | P&L: ${p_pnl:+.0f}")

    safe = hits[hits["market_yes_price"] < 65]
    if not safe.empty:
        s_pnl = float(safe["pnl"].sum())
        s_wr = float(safe["is_won"].mean())
        print(f"  OK  SAFE ZONE (yes_price <65): {len(safe)} bets | "
              f"Win%: {fmt_pct(s_wr)} | P&L: ${s_pnl:+.0f}")

    cheap = hits[hits["market_yes_price"] >= 72]
    if not cheap.empty:
        c_pnl = float(cheap["pnl"].sum())
        c_wr = float(cheap["is_won"].mean())
        mark = "OK" if c_pnl > 0 else "!!"
        print(f"  {mark}  CHEAP NO ZONE (yes_price 72+): {len(cheap)} bets | "
              f"Win%: {fmt_pct(c_wr)} | P&L: ${c_pnl:+.0f}")
    sep()

    # Also run the same analysis for other stat types with enough volume
    other_stats = [s for s in df["stat_type"].unique() if s != "batter_hits"]
    for stat in sorted(other_stats):
        stat_df = df[(df["stat_type"] == stat) & (df["side"] == "no")]
        if len(stat_df) < 30:
            continue
        stat_df = stat_df.copy()
        stat_df["yp_bucket"] = stat_df["market_yes_price"].apply(_yes_price_bucket)

        print(f"\n  -- {stat} NO bets ({len(stat_df)}) --")
        print("    " + fmt.format("Yes Price", "Bets", "Won", "Win%", "P&L",
                                   "PnL/Bet", "Verdict"))
        print("    " + "·" * 66)

        for bucket in _YP_BUCKET_ORDER:
            grp = stat_df[stat_df["yp_bucket"] == bucket]
            if grp.empty:
                continue
            n = len(grp)
            wins = int(grp["is_won"].sum())
            wr = wins / n
            pnl = float(grp["pnl"].sum())
            pnl_per = pnl / n
            verdict = "Strong" if pnl > 50 and wr > 0.40 else (
                "OK" if pnl > 0 else ("Break-even" if pnl > -50 else "LOSING"))
            print("    " + fmt.format(
                bucket, n, wins, fmt_pct(wr),
                f"${pnl:+.0f}", f"${pnl_per:+.2f}", verdict,
            ))
        sep("·")
    sep()


# ---------------------------------------------------------------------------
# Section 4: Weekly trend per category
# ---------------------------------------------------------------------------


def section_weekly_trends(df: pd.DataFrame):
    header("WEEKLY TREND PER CATEGORY  (last 3 weeks)")

    weeks = sorted(df["week"].unique())
    if len(weeks) < 2:
        print("  Not enough weeks to show trends.")
        sep()
        return

    recent_weeks = weeks[-3:] if len(weeks) >= 3 else weeks
    categories = sorted(df["category"].unique())

    # Build per-week column width dynamically
    wk_col_w = 25
    col_w = [32] + [wk_col_w] * len(recent_weeks) + [8]
    fmt_str = "  ".join(f"{{:<{w}}}" for w in col_w)
    header_labels = ["Category"] + [str(w) for w in recent_weeks] + ["Trend"]
    print(fmt_str.format(*header_labels))
    sep("·")

    degrading = []

    for cat in categories:
        grp = df[df["category"] == cat]
        if len(grp) < 10:
            continue

        row_parts = [cat[:32]]
        win_rates: list[float | None] = []

        for w in recent_weeks:
            wk_grp = grp[grp["week"] == w]
            if wk_grp.empty:
                row_parts.append("--")
                win_rates.append(None)
            else:
                n = len(wk_grp)
                wins = int(wk_grp["is_won"].sum())
                wr = wins / n
                pnl = float(wk_grp["pnl"].sum())
                row_parts.append(f"{fmt_pct(wr)} ({n}b) ${pnl:+.0f}")
                win_rates.append(wr)

        # Trend: compare last two non-None win rates
        valid_wr = [(i, wr) for i, wr in enumerate(win_rates) if wr is not None]
        trend = "--"
        if len(valid_wr) >= 2:
            prev_wr = valid_wr[-2][1]
            last_wr = valid_wr[-1][1]
            delta = last_wr - prev_wr
            if delta < -0.05:
                trend = f"v{abs(delta)*100:.0f}pp"
                degrading.append((cat, delta, last_wr))
            elif delta > 0.05:
                trend = f"^{delta*100:.0f}pp"
            else:
                trend = "->"

        row_parts.append(trend)
        print(fmt_str.format(*row_parts))

    sep()

    if degrading:
        print("  !!  DEGRADING CATEGORIES (>5pp WoW decline):")
        for cat, delta, last_wr in sorted(degrading, key=lambda x: x[1]):
            print(f"     {cat}: {delta*100:+.1f}pp -> now {fmt_pct(last_wr)}")
    else:
        print("  No categories degrading >5pp WoW.")
    sep()


# ---------------------------------------------------------------------------
# Section 5: Player-level losers
# ---------------------------------------------------------------------------


def section_player_losers(df: pd.DataFrame):
    header("TOP LOSING PLAYERS  (by total P&L)")

    player_groups = []
    for (name, stat), grp in df.groupby(["player_name", "stat_type"]):
        n = len(grp)
        wins = int(grp["is_won"].sum())
        wr = wins / n if n else 0
        pnl = float(grp["pnl"].sum())
        avg_mp = (float(grp["model_prob"].mean())
                  if "model_prob" in grp.columns and not grp["model_prob"].isna().all()
                  else None)
        avg_yp = float(grp["market_yes_price"].mean())
        player_groups.append((name, stat, n, wins, wr, pnl, avg_mp, avg_yp))

    # Sort by PnL ascending (worst first)
    player_groups.sort(key=lambda r: r[5])

    top_losers = [p for p in player_groups if p[5] < 0][:15]

    col_w = [22, 22, 5, 5, 7, 10, 8, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_w)

    if not top_losers:
        print("  No losing players found.")
        sep()
        return

    print(fmt.format("Player", "Stat", "Bets", "Won", "Win%", "P&L",
                      "Avg MP", "Avg YP"))
    sep("·")

    for name, stat, n, wins, wr, pnl, avg_mp, avg_yp in top_losers:
        mp_str = fmt_pct(avg_mp) if avg_mp is not None else "--"
        yp_str = f"{avg_yp:.0f}" if avg_yp is not None else "--"
        print(fmt.format(
            name[:22], stat[:22], n, wins, fmt_pct(wr),
            f"${pnl:+.0f}", mp_str, yp_str,
        ))
    sep()

    # Top 10 winners for contrast
    top_winners = [p for p in reversed(player_groups) if p[5] > 0][:10]
    if top_winners:
        print()
        print("  Top 10 winning players:")
        print(fmt.format("Player", "Stat", "Bets", "Won", "Win%", "P&L",
                          "Avg MP", "Avg YP"))
        sep("·")
        for name, stat, n, wins, wr, pnl, avg_mp, avg_yp in top_winners:
            mp_str = fmt_pct(avg_mp) if avg_mp is not None else "--"
            yp_str = f"{avg_yp:.0f}" if avg_yp is not None else "--"
            print(fmt.format(
                name[:22], stat[:22], n, wins, fmt_pct(wr),
                f"${pnl:+.0f}", mp_str, yp_str,
            ))
        sep()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kalshi bet category analysis — per-category model diagnostics"
    )
    parser.add_argument("--sport", type=str, default=None,
                        choices=["nba", "mlb"])
    parser.add_argument("--days", type=int, default=21,
                        help="Last N days (default: 21)")
    parser.add_argument("--date-start", type=str, default=None,
                        dest="date_start")
    parser.add_argument("--date-end", type=str, default=None,
                        dest="date_end")
    parser.add_argument("--table", type=str, default="paper",
                        choices=["paper", "live", "both"],
                        help="Data source: paper, live, or both")
    parser.add_argument("--stat-type", type=str, default=None,
                        dest="stat_type",
                        help="Filter to one stat type (e.g., batter_hits)")
    parser.add_argument("--csv-out", type=str, default=None,
                        dest="csv_out",
                        help="Export category breakdown to CSV")
    return parser.parse_args()


def main():
    args = parse_args()

    date_start = args.date_start
    date_end = args.date_end
    if args.days and not date_start:
        date_start = (date.today() - timedelta(days=args.days)).isoformat()

    engine = get_engine()

    dfs = []
    if args.table in ("paper", "both"):
        paper = load_paper_bets(engine, sport=args.sport,
                                date_start=date_start, date_end=date_end,
                                stat_type=args.stat_type)
        if not paper.empty:
            paper["source"] = "paper"
            dfs.append(paper)
            print(f"  Loaded {len(paper)} paper bets")

    if args.table in ("live", "both"):
        live = load_live_orders(engine, sport=args.sport,
                                date_start=date_start, date_end=date_end,
                                stat_type=args.stat_type)
        if not live.empty:
            live["source"] = "live"
            dfs.append(live)
            print(f"  Loaded {len(live)} live orders")

    if not dfs:
        print("No resolved bets found for the given filters.")
        sys.exit(0)

    df = pd.concat(dfs, ignore_index=True)

    # Top-line summary
    print()
    print("=" * W)
    print("  KALSHI BET CATEGORY ANALYSIS")
    print("=" * W)
    print(f"  Date range   : {df['game_date'].min()}  ->  {df['game_date'].max()}")
    print(f"  Total bets   : {len(df):,}")
    print(f"  Sources      : {', '.join(sorted(df['source'].unique()))}")
    if args.sport:
        print(f"  Sport filter : {args.sport}")
    if args.stat_type:
        print(f"  Stat filter  : {args.stat_type}")
    print(f"  Sports       : {', '.join(sorted(df['sport'].dropna().unique()))}")
    print(f"  Stat types   : {', '.join(sorted(df['stat_type'].unique()))}")
    total_pnl = float(df["pnl"].sum())
    total_wr = float(df["is_won"].mean())
    print(f"  Overall      : Win%: {fmt_pct(total_wr)} | P&L: ${total_pnl:+,.0f}")
    print("=" * W)

    # Run all sections
    section_category_breakdown(df)
    section_model_calibration(df)
    section_yes_price_analysis(df)
    section_weekly_trends(df)
    section_player_losers(df)

    # CSV export
    if args.csv_out:
        rows = []
        for cat in sorted(df["category"].unique()):
            grp = df[df["category"] == cat]
            n = len(grp)
            wins = int(grp["is_won"].sum())
            cost = float(grp["total_cost"].sum())
            pnl = float(grp["pnl"].sum())
            rows.append({
                "category": cat,
                "bets": n,
                "wins": wins,
                "win_rate": round(wins / n, 4) if n else 0,
                "pnl": round(pnl, 2),
                "total_cost": round(cost, 2),
                "roi": round(pnl / cost, 4) if cost > 0 else 0,
                "avg_model_prob": (round(float(grp["model_prob"].mean()), 4)
                                   if not grp["model_prob"].isna().all() else None),
                "avg_break_even": round(float(grp["break_even"].mean()), 4),
            })
        csv_df = pd.DataFrame(rows)
        csv_df.to_csv(args.csv_out, index=False)
        print(f"\n  Category breakdown exported to {args.csv_out}")


if __name__ == "__main__":
    main()
