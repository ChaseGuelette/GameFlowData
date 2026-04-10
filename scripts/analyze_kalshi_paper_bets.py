"""
Kalshi Paper Bet Performance Analyzer
======================================
Analyzes all resolved Kalshi paper bets (real + overflow) and prints:
  - Top-line summary: real vs overflow, YES vs NO
  - Side comparison (real bets only)
  - Breakdown by stat type (NO bets)
  - Breakdown by cost bucket (NO bets)
  - Breakdown by edge bucket (NO bets)
  - Sport breakdown
  - Statistical significance (Z-scores)

Usage:
    python scripts/analyze_kalshi_paper_bets.py
    python scripts/analyze_kalshi_paper_bets.py --sport nba
    python scripts/analyze_kalshi_paper_bets.py --days 14
    python scripts/analyze_kalshi_paper_bets.py --date-start 2026-01-01 --date-end 2026-04-10
    python scripts/analyze_kalshi_paper_bets.py --no-only  # Only NO bets in all tables
"""

import argparse
import math
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv

load_dotenv()

from src.db.client import get_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def taker_fee_per_contract(price_cents: float) -> float:
    """Taker fee in dollars for one contract at given price (cents)."""
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def break_even_win_rate(price_cents: float) -> float:
    """Minimum win rate to be profitable at taker pricing."""
    cost_per = price_cents / 100.0
    fee_per = taker_fee_per_contract(price_cents)
    return cost_per / (1.0 - fee_per)


def price_bucket(price_cents: float) -> str:
    p = int(price_cents)
    if p <= 14:   return "05-14c"
    if p <= 24:   return "15-24c"
    if p <= 34:   return "25-34c"
    if p <= 44:   return "35-44c"
    if p <= 55:   return "45-55c"
    return       "56c+"


def edge_bucket(edge: float) -> str:
    e = edge * 100
    if e < 3:     return "<3%"
    if e < 5:     return "3-5%"
    if e < 10:    return "5-10%"
    if e < 15:    return "10-15%"
    if e < 20:    return "15-20%"
    return               "20%+"


def z_score(wins: int, total: int, be_rate: float) -> float:
    """Z-score vs break-even win rate (normal approximation)."""
    if total < 5:
        return float("nan")
    win_rate = wins / total
    sigma = math.sqrt(be_rate * (1 - be_rate) / total)
    return (win_rate - be_rate) / sigma if sigma > 0 else float("nan")


def fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def fmt_z(v: float) -> str:
    if math.isnan(v):
        return "  —  "
    return f"{v:+.1f}σ"


def sep(char="─", width=90):
    print(char * width)


def print_table(headers: list[str], rows: list[list], col_widths: list[int]):
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    sep()
    print(fmt.format(*headers))
    sep("·")
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))
    sep()


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------

def build_metrics(df: pd.DataFrame) -> dict:
    """Compute metrics for a group of resolved bets."""
    total = len(df)
    wins = (df["is_won"]).sum()
    losses = total - wins
    win_rate = wins / total if total > 0 else 0.0
    pnl = df["pnl"].sum()
    roi = pnl / df["total_cost"].sum() if df["total_cost"].sum() > 0 else 0.0
    avg_be = df["break_even"].mean() if total > 0 else 0.0
    alpha = win_rate - avg_be
    z = z_score(int(wins), total, avg_be)
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "break_even": avg_be,
        "alpha": alpha,
        "pnl": pnl,
        "roi": roi,
        "z": z,
    }


def print_metrics_table(
    title: str,
    groups: list[tuple[str, pd.DataFrame]],
    label_header: str = "Group",
    label_width: int = 20,
):
    """Print a metrics table for a list of (label, df) groups."""
    print(f"\n{'━' * 90}")
    print(f"  {title}")
    print(f"{'━' * 90}")

    headers = [label_header, "Bets", "Won", "Win%", "BE%", "Alpha", "P&L", "ROI", "Z-score"]
    col_widths = [label_width, 6, 6, 7, 7, 7, 10, 8, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)

    sep("─")
    print(fmt.format(*headers))
    sep("·")

    for label, grp_df in groups:
        if len(grp_df) == 0:
            continue
        m = build_metrics(grp_df)
        row = [
            label[:label_width],
            m["total"],
            m["wins"],
            fmt_pct(m["win_rate"]),
            fmt_pct(m["break_even"]),
            fmt_pct(m["alpha"]),
            f"${m['pnl']:+.0f}",
            fmt_pct(m["roi"]),
            fmt_z(m["z"]),
        ]
        print(fmt.format(*[str(c) for c in row]))

    sep("─")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_bets(engine, sport=None, date_start=None, date_end=None) -> pd.DataFrame:
    resolved_statuses = ("won", "lost", "overflow_won", "overflow_lost")
    placeholders = ", ".join(f"'{s}'" for s in resolved_statuses)

    filters = [f"status IN ({placeholders})"]
    params = {}

    if sport:
        filters.append("sport = :sport")
        params["sport"] = sport
    if date_start:
        filters.append("game_date >= :date_start")
        params["date_start"] = date_start
    if date_end:
        filters.append("game_date <= :date_end")
        params["date_end"] = date_end

    where = " AND ".join(filters)

    query = text(f"""
        SELECT id, game_date, sport, player_name, stat_type, side,
               price, contracts, fee_adjusted_edge, pnl, status, placed_at
        FROM kalshi_paper_bets
        WHERE {where}
        ORDER BY game_date, placed_at
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    # `price` column = YES market price (e.g., 65 = 65¢ for YES / 35¢ for NO).
    # For YES bets: you pay `price` cents. For NO bets: you pay `100 - price` cents.
    df["actual_price"] = df.apply(
        lambda r: int(r["price"]) if r["side"] == "yes" else 100 - int(r["price"]), axis=1
    )
    df["cost_per"] = df["actual_price"] / 100.0
    # Fee formula is symmetric: ceil(0.07 × p × (1-p) × 100) / 100 — same for YES and NO.
    df["fee_per"] = df["actual_price"].apply(taker_fee_per_contract)
    df["break_even"] = df["actual_price"].apply(break_even_win_rate)
    df["is_overflow"] = df["status"].str.startswith("overflow")
    df["is_won"] = df["status"].isin(["won", "overflow_won"])
    df["total_cost"] = df["cost_per"] * df["contracts"]
    df["price_bucket"] = df["actual_price"].apply(price_bucket)
    df["edge_bucket"] = df["fee_adjusted_edge"].apply(edge_bucket)

    return df


def run_analysis(df: pd.DataFrame, no_only: bool = False):
    real = df[~df["is_overflow"]]
    overflow = df[df["is_overflow"]]

    real_no = real[real["side"] == "no"]
    real_yes = real[real["side"] == "yes"]
    overflow_no = overflow[overflow["side"] == "no"]
    overflow_yes = overflow[overflow["side"] == "yes"]

    # ── Top-line ────────────────────────────────────────────────────────────
    print("\n" + "═" * 90)
    print("  KALSHI PAPER BET ANALYSIS")
    print("═" * 90)

    date_range = f"{df['game_date'].min()} → {df['game_date'].max()}"
    print(f"  Date range  : {date_range}")
    print(f"  Total bets  : {len(df):,}  (real: {len(real):,}  /  overflow: {len(overflow):,})")
    print(f"  Sports      : {', '.join(sorted(df['sport'].unique()))}")
    print(f"  Mode filter : {'NO-only' if no_only else 'ALL bets'}")
    print("═" * 90)

    # ── Side comparison (real bets) ─────────────────────────────────────────
    print_metrics_table(
        "SIDE COMPARISON — REAL BETS ONLY",
        [
            ("NO (real)", real_no),
            ("YES (real)", real_yes),
        ],
        label_header="Side",
        label_width=12,
    )

    # ── Side comparison including overflow ──────────────────────────────────
    print_metrics_table(
        "SIDE COMPARISON — REAL + OVERFLOW (hypothetical)",
        [
            ("NO (real)", real_no),
            ("NO (overflow)", overflow_no),
            ("NO (combined)", pd.concat([real_no, overflow_no])),
            ("YES (real)", real_yes),
            ("YES (overflow)", overflow_yes),
            ("YES (combined)", pd.concat([real_yes, overflow_yes])),
        ],
        label_header="Side + Type",
        label_width=16,
    )

    # For remaining tables, filter to NO bets only (or all per flag)
    analysis_df = df[df["side"] == "no"] if not no_only else df[df["side"] == "no"]
    real_analysis = analysis_df[~analysis_df["is_overflow"]]
    overflow_analysis = analysis_df[analysis_df["is_overflow"]]
    combined_analysis = analysis_df

    # ── By stat type (NO, combined) ─────────────────────────────────────────
    stat_groups = []
    for stat in sorted(combined_analysis["stat_type"].unique()):
        grp = combined_analysis[combined_analysis["stat_type"] == stat]
        stat_groups.append((stat, grp))
    print_metrics_table(
        "BY STAT TYPE — NO BETS (real + overflow)",
        stat_groups,
        label_header="Stat Type",
        label_width=28,
    )

    # ── By cost bucket (NO, combined) ──────────────────────────────────────
    bucket_order = ["05-14c", "15-24c", "25-34c", "35-44c", "45-55c", "56c+"]
    bucket_groups = []
    for b in bucket_order:
        grp = combined_analysis[combined_analysis["price_bucket"] == b]
        if len(grp) > 0:
            # Show break-even for this bucket
            be = grp["break_even"].mean()
            bucket_groups.append((f"{b}  (BE={fmt_pct(be)})", grp))
    print_metrics_table(
        "BY COST BUCKET — NO BETS (real + overflow)",
        bucket_groups,
        label_header="Price Bucket",
        label_width=22,
    )

    # ── By edge bucket (NO, combined) ──────────────────────────────────────
    edge_order = ["<3%", "3-5%", "5-10%", "10-15%", "15-20%", "20%+"]
    edge_groups = []
    for e in edge_order:
        grp = combined_analysis[combined_analysis["edge_bucket"] == e]
        if len(grp) > 0:
            edge_groups.append((e, grp))
    print_metrics_table(
        "BY EDGE BUCKET — NO BETS (real + overflow)",
        edge_groups,
        label_header="Edge Bucket",
        label_width=14,
    )

    # ── By edge bucket (NO, real only) — key signal ─────────────────────────
    real_edge_groups = []
    for e in edge_order:
        grp = real_analysis[real_analysis["edge_bucket"] == e]
        if len(grp) > 0:
            real_edge_groups.append((e, grp))
    print_metrics_table(
        "BY EDGE BUCKET — NO BETS (real only)",
        real_edge_groups,
        label_header="Edge Bucket",
        label_width=14,
    )

    # ── By sport (NO, combined) ─────────────────────────────────────────────
    sport_groups = []
    for s in sorted(combined_analysis["sport"].unique()):
        grp = combined_analysis[combined_analysis["sport"] == s]
        sport_groups.append((s.upper(), grp))
    print_metrics_table(
        "BY SPORT — NO BETS (real + overflow)",
        sport_groups,
        label_header="Sport",
        label_width=8,
    )

    # ── Overflow impact ─────────────────────────────────────────────────────
    print("\n" + "━" * 90)
    print("  OVERFLOW IMPACT — What we LEFT ON THE TABLE (NO bets, hypothetical)")
    print("━" * 90)
    if len(overflow_no) > 0:
        m_real = build_metrics(real_no)
        m_of = build_metrics(overflow_no)
        m_comb = build_metrics(pd.concat([real_no, overflow_no]))
        print(f"  Real trades placed  : {m_real['total']:>5} bets  P&L: ${m_real['pnl']:>+8.0f}  ROI: {fmt_pct(m_real['roi'])}")
        print(f"  Overflow (skipped)  : {m_of['total']:>5} bets  P&L: ${m_of['pnl']:>+8.0f}  ROI: {fmt_pct(m_of['roi'])}")
        print(f"  Combined (if taken) : {m_comb['total']:>5} bets  P&L: ${m_comb['pnl']:>+8.0f}  ROI: {fmt_pct(m_comb['roi'])}")
        print(f"  Unrealized P&L      : ${m_of['pnl']:>+8.0f}  ({fmt_pct(m_of['pnl'] / abs(m_real['pnl']) if m_real['pnl'] != 0 else 0)} of real P&L)")
    else:
        print("  No overflow NO bets in this date range.")
    print("━" * 90)

    # ── Daily trend (NO, combined) ──────────────────────────────────────────
    print("\n" + "━" * 90)
    print("  DAILY TREND — NO BETS (real + overflow)")
    print("━" * 90)
    headers = ["Date", "Bets", "Won", "Win%", "BE%", "Alpha", "P&L", "Cumul P&L"]
    col_widths = [12, 6, 5, 7, 7, 7, 9, 10]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    sep("·")
    cumul = 0.0
    for d, grp in combined_analysis.groupby("game_date"):
        m = build_metrics(grp)
        cumul += m["pnl"]
        print(fmt.format(
            str(d), m["total"], m["wins"],
            fmt_pct(m["win_rate"]), fmt_pct(m["break_even"]),
            fmt_pct(m["alpha"]), f"${m['pnl']:+.0f}", f"${cumul:+.0f}",
        ))
    sep("─")

    # ── Statistical significance banner ────────────────────────────────────
    print()
    if len(combined_analysis) >= 30:
        m = build_metrics(combined_analysis)
        print("═" * 90)
        print("  STATISTICAL SIGNIFICANCE — NO BETS (combined)")
        print("═" * 90)
        print(f"  Sample size  : {m['total']:,} resolved bets")
        print(f"  Win rate     : {fmt_pct(m['win_rate'])}  ({m['wins']} / {m['total']})")
        print(f"  Break-even   : {fmt_pct(m['break_even'])}  (avg across bets)")
        print(f"  Alpha        : {fmt_pct(m['alpha'])}  above break-even")
        print(f"  Z-score      : {fmt_z(m['z'])}  vs break-even win rate")
        se = math.sqrt(m["win_rate"] * (1 - m["win_rate"]) / m["total"])
        ci_lo = m["win_rate"] - 1.96 * se
        ci_hi = m["win_rate"] + 1.96 * se
        print(f"  95% CI       : [{fmt_pct(ci_lo)},  {fmt_pct(ci_hi)}]")
        print(f"  Total P&L    : ${m['pnl']:+,.0f}")
        print(f"  ROI          : {fmt_pct(m['roi'])}")
        verdict = "STRONG EDGE" if m["z"] > 3 else ("LIKELY EDGE" if m["z"] > 2 else "WEAK / INCONCLUSIVE")
        print(f"\n  Verdict      : {verdict}")
        print("═" * 90)


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze Kalshi paper bet performance")
    parser.add_argument("--sport", type=str, default=None, choices=["nba", "mlb"], help="Filter by sport")
    parser.add_argument("--days", type=int, default=None, help="Only last N days")
    parser.add_argument("--date-start", type=str, default=None, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--date-end", type=str, default=None, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--no-only", action="store_true", help="Focus NO bets in all tables (default behavior, kept for clarity)")
    return parser.parse_args()


def main():
    args = parse_args()

    date_start = args.date_start
    date_end = args.date_end

    if args.days:
        date_start = (date.today() - timedelta(days=args.days)).isoformat()

    engine = get_engine()
    df = load_bets(engine, sport=args.sport, date_start=date_start, date_end=date_end)

    if df.empty:
        print("No resolved bets found for the given filters.")
        sys.exit(0)

    run_analysis(df, no_only=args.no_only)


if __name__ == "__main__":
    main()
