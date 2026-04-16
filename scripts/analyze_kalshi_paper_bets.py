"""
Kalshi Paper Bet Performance Analyzer
======================================
Comprehensive go-live and ongoing health analysis for the Kalshi paper trading bot.

Sections produced:
  1.  Top-line summary (real vs overflow, YES vs NO)
  2.  Side comparison — real only
  3.  Side comparison — real + overflow
  4.  Before vs After NO-only deployment (--split-date)
  5.  By stat type — real only + overflow, side by side
  6.  Cross-sectional consistency check
  7.  By edge bucket — combined (15-20 / 20-25 / 25-30 / 30%+)
  8.  By edge bucket — real only (monotonicity check)
  9.  By sport — real only + combined
  10. Overflow impact (left-on-table analysis)
  11. Weekly performance comparison
  12. Daily P&L trend + bankroll trajectory
  13. Statistical significance — real, combined, split Z-scores
  14. Go-live / scale-up readiness verdict

Usage:
    python scripts/analyze_kalshi_paper_bets.py
    python scripts/analyze_kalshi_paper_bets.py --sport nba
    python scripts/analyze_kalshi_paper_bets.py --days 14
    python scripts/analyze_kalshi_paper_bets.py --date-start 2026-04-01 --date-end 2026-04-15
    python scripts/analyze_kalshi_paper_bets.py --split-date 2026-04-11   # before/after split
    python scripts/analyze_kalshi_paper_bets.py --no-bankroll              # skip daily_log query
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
# Constants
# ---------------------------------------------------------------------------

# Startup playbook thresholds
ROI_GO_LIVE_THRESHOLD = 0.08      # 8% ROI → go-live
ROI_SCALE_UP_1_THRESHOLD = 0.08   # 2 weeks at 8% → $500
ROI_SCALE_UP_2_THRESHOLD = 0.08   # 4 weeks at 8% → $1,000
Z_STRONG_EDGE = 3.0
Z_LIKELY_EDGE = 2.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def taker_fee_per_contract(price_cents: float) -> float:
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def break_even_win_rate(price_cents: float) -> float:
    cost_per = price_cents / 100.0
    fee_per = taker_fee_per_contract(price_cents)
    return cost_per / (1.0 - fee_per) if (1.0 - fee_per) > 0 else cost_per


def edge_bucket(edge: float) -> str:
    """Go-live relevant edge buckets (min_edge is 15%)."""
    e = edge * 100
    if e < 15:
        return "<15%"
    if e < 20:
        return "15-20%"
    if e < 25:
        return "20-25%"
    if e < 30:
        return "25-30%"
    return "30%+"


def price_bucket(price_cents: float) -> str:
    p = int(price_cents)
    if p <= 14:
        return "05-14¢"
    if p <= 24:
        return "15-24¢"
    if p <= 34:
        return "25-34¢"
    if p <= 44:
        return "35-44¢"
    if p <= 55:
        return "45-55¢"
    return "56¢+"


def z_score(wins: int, total: int, be_rate: float) -> float:
    if total < 5:
        return float("nan")
    win_rate = wins / total
    sigma = math.sqrt(be_rate * (1 - be_rate) / total)
    return (win_rate - be_rate) / sigma if sigma > 0 else float("nan")


def pnl_z_score(df: pd.DataFrame) -> float:
    """Z-score of mean P&L vs zero (t-test style)."""
    if len(df) < 5:
        return float("nan")
    mean = df["pnl"].mean()
    std = df["pnl"].std()
    if std == 0:
        return float("nan")
    return mean / (std / math.sqrt(len(df)))


def fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def fmt_z(v: float) -> str:
    if math.isnan(v):
        return "  —  "
    return f"{v:+.2f}σ"


def fmt_roi(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.1f}%"


W = 96

def sep(char="─", width=W):
    print(char * width)


def header(title: str):
    print()
    print("━" * W)
    print(f"  {title}")
    print("━" * W)


# ---------------------------------------------------------------------------
# Core metrics builder
# ---------------------------------------------------------------------------

def build_metrics(df: pd.DataFrame) -> dict:
    total = len(df)
    if total == 0:
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "break_even": 0,
                "alpha": 0, "pnl": 0, "cost": 0, "roi": 0, "z": float("nan"), "pnl_z": float("nan")}
    wins = int(df["is_won"].sum())
    losses = total - wins
    win_rate = wins / total
    pnl = float(df["pnl"].sum())
    cost = float(df["total_cost"].sum())
    roi = pnl / cost if cost > 0 else 0.0
    avg_be = float(df["break_even"].mean())
    alpha = win_rate - avg_be
    z = z_score(wins, total, avg_be)
    pz = pnl_z_score(df)
    return {
        "total": total, "wins": wins, "losses": losses,
        "win_rate": win_rate, "break_even": avg_be, "alpha": alpha,
        "pnl": pnl, "cost": cost, "roi": roi, "z": z, "pnl_z": pz,
    }


def print_metrics_table(
    title: str,
    groups: list[tuple[str, pd.DataFrame]],
    label_header: str = "Group",
    label_width: int = 22,
    show_pnl_z: bool = False,
):
    header(title)
    extra = ["PnL-Z"] if show_pnl_z else []
    headers = [label_header, "Bets", "Won", "Win%", "BE%", "Alpha", "P&L", "Cost", "ROI", "Z-score"] + extra
    col_widths = [label_width, 6, 6, 7, 7, 7, 10, 9, 8, 8] + ([7] * len(extra))
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    sep("·")
    for label, grp_df in groups:
        if len(grp_df) == 0:
            continue
        m = build_metrics(grp_df)
        row = [
            label[:label_width],
            m["total"], m["wins"],
            fmt_pct(m["win_rate"]),
            fmt_pct(m["break_even"]),
            fmt_pct(m["alpha"]),
            f"${m['pnl']:+.0f}",
            f"${m['cost']:.0f}",
            fmt_roi(m["roi"]),
            fmt_z(m["z"]),
        ]
        if show_pnl_z:
            row.append(fmt_z(m["pnl_z"]))
        print(fmt.format(*[str(c) for c in row]))
    sep()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_bets(engine, sport=None, date_start=None, date_end=None) -> pd.DataFrame:
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

    query = text(f"""
        SELECT id, game_date, sport, player_name, stat_type, side,
               price, contracts, fee_adjusted_edge, pnl, status, placed_at
        FROM kalshi_paper_bets
        WHERE {" AND ".join(filters)}
        ORDER BY game_date, placed_at
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    df["actual_price"] = df.apply(
        lambda r: int(r["price"]) if r["side"] == "yes" else 100 - int(r["price"]), axis=1
    )
    df["cost_per"] = df["actual_price"] / 100.0
    df["fee_per"] = df["actual_price"].apply(taker_fee_per_contract)
    df["break_even"] = df["actual_price"].apply(break_even_win_rate)
    df["is_overflow"] = df["status"].str.startswith("overflow")
    df["is_won"] = df["status"].isin(["won", "overflow_won"])
    df["total_cost"] = df["cost_per"] * df["contracts"]
    df["price_bucket"] = df["actual_price"].apply(price_bucket)
    df["edge_bucket"] = df["fee_adjusted_edge"].apply(edge_bucket)
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["week"] = df["game_date"].apply(lambda d: d - timedelta(days=d.weekday()))  # Monday of week
    return df


def load_daily_log(engine, date_start=None, date_end=None) -> pd.DataFrame:
    filters = []
    params: dict = {}
    if date_start:
        filters.append("game_date >= :date_start")
        params["date_start"] = date_start
    if date_end:
        filters.append("game_date <= :date_end")
        params["date_end"] = date_end
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = text(f"""
        SELECT game_date, total_bets, bets_won, bets_lost,
               total_pnl, cumulative_pnl, bankroll_after
        FROM kalshi_paper_trading_daily_log
        {where}
        ORDER BY game_date
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Analysis sections
# ---------------------------------------------------------------------------

def section_top_line(df: pd.DataFrame):
    real = df[~df["is_overflow"]]
    overflow = df[df["is_overflow"]]
    no_bets = df[df["side"] == "no"]
    yes_bets = df[df["side"] == "yes"]

    print()
    print("═" * W)
    print("  KALSHI PAPER BET ANALYSIS  —  GO-LIVE HEALTH REPORT")
    print("═" * W)
    print(f"  Date range   : {df['game_date'].min()}  →  {df['game_date'].max()}")
    print(f"  Total bets   : {len(df):,}  (real: {len(real):,}  /  overflow: {len(overflow):,})")
    print(f"  Side split   : NO: {len(no_bets):,}  /  YES: {len(yes_bets):,}")
    print(f"  Sports       : {', '.join(sorted(df['sport'].unique()))}")
    print(f"  Stat types   : {', '.join(sorted(df['stat_type'].unique()))}")
    print("═" * W)


def section_side_comparison(df: pd.DataFrame):
    real = df[~df["is_overflow"]]
    overflow = df[df["is_overflow"]]
    real_no = real[real["side"] == "no"]
    real_yes = real[real["side"] == "yes"]
    of_no = overflow[overflow["side"] == "no"]
    of_yes = overflow[overflow["side"] == "yes"]

    print_metrics_table(
        "SIDE COMPARISON — REAL BETS ONLY",
        [("NO (real)", real_no), ("YES (real)", real_yes)],
        label_header="Side", label_width=12,
    )
    print_metrics_table(
        "SIDE COMPARISON — REAL + OVERFLOW",
        [
            ("NO  real",      real_no),
            ("NO  overflow",  of_no),
            ("NO  combined",  pd.concat([real_no, of_no])),
            ("YES real",      real_yes),
            ("YES overflow",  of_yes),
            ("YES combined",  pd.concat([real_yes, of_yes])),
        ],
        label_header="Side", label_width=14,
    )


def section_before_after(df: pd.DataFrame, split_date: str):
    """Before vs after a deployment date (NO bets, real + overflow)."""
    import datetime
    sd = datetime.date.fromisoformat(split_date)
    no = df[df["side"] == "no"]
    before = no[no["game_date"] < sd]
    after = no[no["game_date"] >= sd]
    before_real = before[~before["is_overflow"]]
    after_real = after[~after["is_overflow"]]
    before_comb = before
    after_comb = after

    header(f"BEFORE vs AFTER NO-ONLY DEPLOYMENT  (split: {split_date})")
    print(f"  Before: {before['game_date'].min()} → {before['game_date'].max() if len(before) else '—'}")
    print(f"  After : {after['game_date'].min() if len(after) else '—'} → {after['game_date'].max() if len(after) else '—'}")
    print()

    groups = [
        ("Before  (real)",     before_real),
        ("Before  (combined)", before_comb),
        ("After   (real)",     after_real),
        ("After   (combined)", after_comb),
    ]
    print_metrics_table(
        "BEFORE vs AFTER — NO BETS",
        groups,
        label_header="Period",
        label_width=20,
    )

    # Win rate delta
    if len(before_comb) > 0 and len(after_comb) > 0:
        mb = build_metrics(before_comb)
        ma = build_metrics(after_comb)
        wr_delta = ma["win_rate"] - mb["win_rate"]
        roi_delta = ma["roi"] - mb["roi"]
        print(f"  Win rate delta (after - before): {fmt_pct(wr_delta)}  ({'▲' if wr_delta >= 0 else '▼'})")
        print(f"  ROI delta      (after - before): {fmt_roi(roi_delta)}  ({'▲' if roi_delta >= 0 else '▼'})")
        sep()


def section_by_stat(df: pd.DataFrame):
    """Stat type breakdown: real vs overflow side by side, then combined."""
    no = df[df["side"] == "no"]
    real_no = no[~no["is_overflow"]]
    comb_no = no

    stats = sorted(comb_no["stat_type"].unique())

    # Combined table
    print_metrics_table(
        "BY STAT TYPE — NO BETS  (real + overflow combined)",
        [(s, comb_no[comb_no["stat_type"] == s]) for s in stats],
        label_header="Stat Type", label_width=30,
    )

    # Real-only table
    print_metrics_table(
        "BY STAT TYPE — NO BETS  (real only)",
        [(s, real_no[real_no["stat_type"] == s]) for s in stats],
        label_header="Stat Type", label_width=30,
    )


def section_cross_sectional(df: pd.DataFrame):
    """Check if every stat type is profitable (strongest evidence of real edge)."""
    no_comb = df[df["side"] == "no"]
    stats = sorted(no_comb["stat_type"].unique())
    header("CROSS-SECTIONAL CONSISTENCY CHECK — ALL STATS PROFITABLE?")

    all_positive = True
    results = []
    for stat in stats:
        grp = no_comb[no_comb["stat_type"] == stat]
        m = build_metrics(grp)
        profitable = m["pnl"] > 0
        if not profitable:
            all_positive = False
        flag = "✓" if profitable else "✗"
        results.append((stat, m, flag, profitable))

    col_widths = [30, 6, 7, 10, 8, 4]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Stat Type", "Bets", "Win%", "P&L", "ROI", ""))
    sep("·")
    for stat, m, flag, _ in results:
        print(fmt.format(
            stat, m["total"],
            fmt_pct(m["win_rate"]),
            f"${m['pnl']:+.0f}",
            fmt_roi(m["roi"]),
            flag,
        ))
    sep()

    if all_positive:
        print(f"  ✓  ALL {len(stats)} STAT TYPES PROFITABLE — strong cross-sectional evidence of real edge")
    else:
        n_neg = sum(1 for _, _, _, p in results if not p)
        print(f"  ✗  {n_neg}/{len(stats)} stat types NEGATIVE — cross-sectional consistency broken")
    sep()


def section_by_edge_bucket(df: pd.DataFrame):
    no = df[df["side"] == "no"]
    real_no = no[~no["is_overflow"]]
    comb_no = no

    edge_order = ["<15%", "15-20%", "20-25%", "25-30%", "30%+"]

    print_metrics_table(
        "BY EDGE BUCKET — NO BETS  (real + overflow)  |  expect monotonic ROI increase",
        [(e, comb_no[comb_no["edge_bucket"] == e]) for e in edge_order],
        label_header="Edge Bucket", label_width=14,
    )
    print_metrics_table(
        "BY EDGE BUCKET — NO BETS  (real only)",
        [(e, real_no[real_no["edge_bucket"] == e]) for e in edge_order],
        label_header="Edge Bucket", label_width=14,
    )

    # Monotonicity check (combined, skip <15%)
    valid_buckets = ["15-20%", "20-25%", "25-30%", "30%+"]
    rois = []
    for e in valid_buckets:
        grp = comb_no[comb_no["edge_bucket"] == e]
        if len(grp) >= 5:
            m = build_metrics(grp)
            rois.append((e, m["roi"]))

    if len(rois) >= 2:
        monotonic = all(rois[i][1] <= rois[i+1][1] for i in range(len(rois) - 1))
        direction = "✓  MONOTONIC — higher edge → higher ROI  (model is well-calibrated)" \
                    if monotonic else \
                    "~  NOT STRICTLY MONOTONIC — check if gaps are statistically meaningful"
        print(f"  {direction}")
        sep()


def section_by_sport(df: pd.DataFrame):
    no = df[df["side"] == "no"]
    real_no = no[~no["is_overflow"]]
    comb_no = no
    sports = sorted(comb_no["sport"].unique())

    print_metrics_table(
        "BY SPORT — NO BETS  (real only)",
        [(s.upper(), real_no[real_no["sport"] == s]) for s in sports],
        label_header="Sport", label_width=8,
    )
    print_metrics_table(
        "BY SPORT — NO BETS  (real + overflow)",
        [(s.upper(), comb_no[comb_no["sport"] == s]) for s in sports],
        label_header="Sport", label_width=8,
    )


def section_overflow_impact(df: pd.DataFrame):
    real_no = df[(df["side"] == "no") & (~df["is_overflow"])]
    of_no   = df[(df["side"] == "no") & (df["is_overflow"])]

    header("OVERFLOW IMPACT — EDGE LEFT ON TABLE (NO bets)")
    if len(of_no) == 0:
        print("  No overflow NO bets in this date range.")
        sep()
        return

    mr = build_metrics(real_no)
    mo = build_metrics(of_no)
    mc = build_metrics(pd.concat([real_no, of_no]))

    print(f"  Real   placed : {mr['total']:>5} bets  P&L: ${mr['pnl']:>+8.0f}  ROI: {fmt_roi(mr['roi'])}")
    print(f"  Overflow skip : {mo['total']:>5} bets  P&L: ${mo['pnl']:>+8.0f}  ROI: {fmt_roi(mo['roi'])}")
    print(f"  Combined      : {mc['total']:>5} bets  P&L: ${mc['pnl']:>+8.0f}  ROI: {fmt_roi(mc['roi'])}")

    if mr["pnl"] != 0:
        pct_missed = mo["pnl"] / (mr["pnl"] + mo["pnl"]) * 100
        print(f"\n  {pct_missed:.0f}% of total edge was lost to exposure cap  (${mo['pnl']:+.0f} / ${mc['pnl']:+.0f})")
        print("  → Increasing exposure captures this edge directly")

    # Daily-level overflow capture rate
    daily_real = real_no.groupby("game_date")["pnl"].sum()
    daily_of   = of_no.groupby("game_date")["pnl"].sum()
    combined_idx = daily_real.index.union(daily_of.index)
    print(f"\n  Daily breakdown  ({len(combined_idx)} days):")
    col_widths = [12, 9, 10, 9]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print("  " + fmt.format("Date", "Real P&L", "Overflow", "% Captured"))
    sep("·")
    for d in sorted(combined_idx):
        r = daily_real.get(d, 0.0)
        o = daily_of.get(d, 0.0)
        total_d = r + o
        pct = f"{r / total_d * 100:.0f}%" if total_d != 0 else "—"
        print("  " + fmt.format(str(d), f"${r:+.0f}", f"${o:+.0f}", pct))
    sep()


def section_weekly(df: pd.DataFrame):
    no = df[df["side"] == "no"]
    real_no = no[~no["is_overflow"]]
    comb_no = no

    header("WEEKLY PERFORMANCE — NO BETS")
    weeks = sorted(comb_no["week"].unique())
    if len(weeks) < 2:
        print("  Not enough weeks to compare.")
        sep()
        return

    col_widths = [14, 6, 6, 7, 7, 10, 8, 8, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    headers = ["Week (Mon)", "Bets-R", "Bets-C", "Win%-R", "Win%-C", "PnL-R", "PnL-C", "ROI-R", "ROI-C"]
    print(fmt.format(*headers))
    sep("·")

    for w in weeks:
        r = real_no[real_no["week"] == w]
        c = comb_no[comb_no["week"] == w]
        mr = build_metrics(r)
        mc = build_metrics(c)
        print(fmt.format(
            str(w),
            mr["total"], mc["total"],
            fmt_pct(mr["win_rate"]) if mr["total"] > 0 else "—",
            fmt_pct(mc["win_rate"]) if mc["total"] > 0 else "—",
            f"${mr['pnl']:+.0f}", f"${mc['pnl']:+.0f}",
            fmt_roi(mr["roi"]) if mr["total"] > 0 else "—",
            fmt_roi(mc["roi"]) if mc["total"] > 0 else "—",
        ))

    # Week-over-week delta (last two weeks with data)
    weeks_with_data = [w for w in weeks if len(real_no[real_no["week"] == w]) > 0]
    if len(weeks_with_data) >= 2:
        sep("·")
        w_prev = weeks_with_data[-2]
        w_last = weeks_with_data[-1]
        mp = build_metrics(real_no[real_no["week"] == w_prev])
        ml = build_metrics(real_no[real_no["week"] == w_last])
        roi_d = ml["roi"] - mp["roi"]
        wr_d = ml["win_rate"] - mp["win_rate"]
        print(f"  WoW (real): ROI {fmt_roi(mp['roi'])} → {fmt_roi(ml['roi'])}  ({'+' if roi_d >= 0 else ''}{roi_d*100:.1f}pp)   "
              f"Win% {fmt_pct(mp['win_rate'])} → {fmt_pct(ml['win_rate'])}  ({'+' if wr_d >= 0 else ''}{wr_d*100:.1f}pp)")
    sep()


def section_daily_trend(df: pd.DataFrame, engine, date_start=None, date_end=None, show_bankroll: bool = True):
    no = df[df["side"] == "no"]

    header("DAILY P&L TREND + BANKROLL TRAJECTORY")

    # Load daily log for bankroll column
    bankroll_map: dict = {}
    if show_bankroll:
        log_df = load_daily_log(engine, date_start=date_start, date_end=date_end)
        if not log_df.empty:
            log_df["game_date"] = pd.to_datetime(log_df["game_date"]).dt.date
            for _, row in log_df.iterrows():
                bankroll_map[row["game_date"]] = {
                    "bankroll": row.get("bankroll_after"),
                    "log_pnl": row.get("total_pnl"),
                }

    col_widths = [12, 6, 5, 7, 7, 10, 10, 11, 10]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    headers = ["Date", "Bets", "Won", "Win%", "BE%", "P&L(comb)", "Cumul(comb)", "Bankroll", "Real P&L"]
    print(fmt.format(*headers))
    sep("·")

    cumul = 0.0
    for d, grp in no.groupby("game_date"):
        m = build_metrics(grp)
        cumul += m["pnl"]
        bk_info = bankroll_map.get(d, {})
        bankroll = bk_info.get("bankroll")
        real_pnl = bk_info.get("log_pnl")
        bk_str = f"${bankroll:.0f}" if bankroll is not None else "—"
        rp_str = f"${real_pnl:+.2f}" if real_pnl is not None else "—"
        print(fmt.format(
            str(d), m["total"], m["wins"],
            fmt_pct(m["win_rate"]), fmt_pct(m["break_even"]),
            f"${m['pnl']:+.0f}", f"${cumul:+.0f}",
            bk_str, rp_str,
        ))
    sep()


def section_significance(df: pd.DataFrame):
    """Z-scores: real-only, combined, and PnL-based."""
    real_no = df[(df["side"] == "no") & (~df["is_overflow"])]
    comb_no = df[df["side"] == "no"]

    header("STATISTICAL SIGNIFICANCE")

    for label, grp in [("Real bets only", real_no), ("Real + overflow (combined)", comb_no)]:
        m = build_metrics(grp)
        if m["total"] < 5:
            print(f"  {label}: insufficient data ({m['total']} bets)")
            continue

        se = math.sqrt(m["win_rate"] * (1 - m["win_rate"]) / m["total"]) if m["total"] > 0 else 0
        ci_lo = m["win_rate"] - 1.96 * se
        ci_hi = m["win_rate"] + 1.96 * se

        verdict = "STRONG EDGE ✓" if m["z"] > Z_STRONG_EDGE else \
                  ("LIKELY EDGE" if m["z"] > Z_LIKELY_EDGE else "WEAK / INCONCLUSIVE")

        print(f"  ── {label} ──")
        print(f"     N = {m['total']:,}   Win = {m['wins']} / {m['total']}   Win% = {fmt_pct(m['win_rate'])}")
        print(f"     Break-even = {fmt_pct(m['break_even'])}   Alpha = {fmt_pct(m['alpha'])}")
        print(f"     Z-score (vs BE) = {fmt_z(m['z'])}   PnL-Z = {fmt_z(m['pnl_z'])}")
        print(f"     95% CI = [{fmt_pct(ci_lo)},  {fmt_pct(ci_hi)}]")
        print(f"     P&L = ${m['pnl']:+,.0f}   ROI = {fmt_roi(m['roi'])}")
        print(f"     Verdict: {verdict}")
        print()
    sep()


def section_go_live_verdict(df: pd.DataFrame, split_date: str | None = None):
    """Go-live / scale-up readiness verdict based on startup playbook thresholds."""
    real_no = df[(df["side"] == "no") & (~df["is_overflow"])]
    comb_no = df[df["side"] == "no"]

    mr = build_metrics(real_no)
    mc = build_metrics(comb_no)

    # Cross-sectional check
    stats = sorted(comb_no["stat_type"].unique())
    stat_pnls = {s: build_metrics(comb_no[comb_no["stat_type"] == s])["pnl"] for s in stats}
    all_stats_profitable = all(p > 0 for p in stat_pnls.values())
    n_profitable_stats = sum(1 for p in stat_pnls.values() if p > 0)

    # After-split metrics (if split_date provided)
    after_mr = mr
    if split_date:
        import datetime
        sd = datetime.date.fromisoformat(split_date)
        after_real = real_no[real_no["game_date"] >= sd]
        after_comb = comb_no[comb_no["game_date"] >= sd]
        after_mr = build_metrics(after_real)
        after_mc = build_metrics(after_comb)
    else:
        after_mc = mc

    # Scoring
    checks = []
    checks.append(("ROI > 8% (real, after NO-only)",  after_mr["roi"] > ROI_GO_LIVE_THRESHOLD, fmt_roi(after_mr["roi"])))
    checks.append(("ROI > 8% (combined, after NO-only)", after_mc["roi"] > ROI_GO_LIVE_THRESHOLD, fmt_roi(after_mc["roi"])))
    checks.append(("Z-score > 2 (real)",  not math.isnan(mr["z"]) and mr["z"] > Z_LIKELY_EDGE, fmt_z(mr["z"])))
    checks.append(("Z-score > 3 (combined)", not math.isnan(mc["z"]) and mc["z"] > Z_STRONG_EDGE, fmt_z(mc["z"])))
    checks.append(("All stats profitable", all_stats_profitable, f"{n_profitable_stats}/{len(stats)} profitable"))
    checks.append(("≥ 30 real bets (after split)", after_mr["total"] >= 30, f"{after_mr['total']} bets"))

    n_pass = sum(1 for _, passed, _ in checks if passed)
    n_total = len(checks)

    header("GO-LIVE / SCALE-UP READINESS VERDICT")
    col_widths = [45, 10, 20]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Check", "Result", "Value"))
    sep("·")
    for name, passed, val in checks:
        mark = "✓  PASS" if passed else "✗  FAIL"
        print(fmt.format(name, mark, val))
    sep()

    print(f"  Score: {n_pass}/{n_total} checks passed")
    print()

    if n_pass == n_total:
        verdict = "GO LIVE"
        detail = "All checks pass. Fund the account and set KALSHI_LIVE_TRADING_ENABLED=true."
    elif n_pass >= 4:
        verdict = "LIKELY READY"
        detail = "Most checks pass. Review the failing checks — likely just sample size."
    elif n_pass >= 3:
        verdict = "MONITOR — NOT YET"
        detail = f"Only {n_pass}/{n_total} checks pass. Continue paper trading and re-check in 3-5 days."
    else:
        verdict = "NOT READY"
        detail = "Edge is not yet proven. Keep paper trading."

    print(f"  {'═' * 60}")
    print(f"  VERDICT: {verdict}")
    print(f"  {detail}")
    print(f"  {'═' * 60}")

    # Scale-up milestones
    print()
    print("  Scaling milestones (startup playbook):")
    print("    2 weeks at ROI > 8%  →  increase to $500 bankroll")
    print("    4 weeks at ROI > 8%  →  increase to $1,000 bankroll")

    if after_mr["total"] > 0:
        days_of_data = (df["game_date"].max() - df["game_date"].min()).days + 1
        weeks_of_data = days_of_data / 7
        print(f"    Current data window: ~{weeks_of_data:.1f} weeks")
    sep()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_analysis(df: pd.DataFrame, engine, args):
    split_date = getattr(args, "split_date", None)
    show_bankroll = not getattr(args, "no_bankroll", False)
    date_start = getattr(args, "date_start", None)
    date_end = getattr(args, "date_end", None)

    section_top_line(df)
    section_side_comparison(df)

    if split_date:
        section_before_after(df, split_date)

    section_by_stat(df)
    section_cross_sectional(df)
    section_by_edge_bucket(df)
    section_by_sport(df)
    section_overflow_impact(df)
    section_weekly(df)
    section_daily_trend(df, engine, date_start=date_start, date_end=date_end, show_bankroll=show_bankroll)
    section_significance(df)
    section_go_live_verdict(df, split_date=split_date)


def parse_args():
    parser = argparse.ArgumentParser(description="Kalshi paper bet go-live health report")
    parser.add_argument("--sport", type=str, default=None, choices=["nba", "mlb"])
    parser.add_argument("--days", type=int, default=None, help="Last N days")
    parser.add_argument("--date-start", type=str, default=None, dest="date_start")
    parser.add_argument("--date-end", type=str, default=None, dest="date_end")
    parser.add_argument("--split-date", type=str, default="2026-04-11", dest="split_date",
                        help="Before/after split date (default: 2026-04-11 = NO-only deployment)")
    parser.add_argument("--no-split", action="store_true", dest="no_split",
                        help="Disable before/after split section")
    parser.add_argument("--no-bankroll", action="store_true", dest="no_bankroll",
                        help="Skip daily_log bankroll column")
    parser.add_argument("--no-only", action="store_true", dest="no_only",
                        help="(legacy flag, kept for compatibility)")
    return parser.parse_args()


def main():
    args = parse_args()

    date_start = args.date_start
    date_end = args.date_end
    if args.days:
        date_start = (date.today() - timedelta(days=args.days)).isoformat()

    if args.no_split:
        args.split_date = None

    engine = get_engine()
    df = load_bets(engine, sport=args.sport, date_start=date_start, date_end=date_end)

    if df.empty:
        print("No resolved bets found for the given filters.")
        sys.exit(0)

    run_analysis(df, engine, args)


if __name__ == "__main__":
    main()
