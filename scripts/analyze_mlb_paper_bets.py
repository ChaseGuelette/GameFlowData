"""
MLB Sportsbook Paper Bet Performance Analyzer
==============================================
Comprehensive health analysis for the MLB sportsbook paper trading bot.

Sections produced:
  1.  Top-line summary (date range, bet counts, stat/direction split, W-L-P)
  2.  Direction comparison — OVER vs UNDER
  3.  By stat type
  4.  By stat + direction (cross-tab)
  5.  Cross-sectional consistency check
  6.  By edge bucket (8-10% / 10-15% / 15-20% / 20%+)
  7.  Win margin analysis (actual_value vs line)
  8.  Weekly performance comparison
  9.  Daily P&L + bankroll trajectory
  10. By odds bucket
  11. Statistical significance (Z-scores, CI, PnL-Z)
  12. Overall verdict (GO / MONITOR / NOT READY)

Usage:
    python scripts/analyze_mlb_paper_bets.py
    python scripts/analyze_mlb_paper_bets.py --days 30
    python scripts/analyze_mlb_paper_bets.py --date-start 2026-04-01 --date-end 2026-04-30
    python scripts/analyze_mlb_paper_bets.py --stat pitcher_strikeouts
    python scripts/analyze_mlb_paper_bets.py --direction under
    python scripts/analyze_mlb_paper_bets.py --no-bankroll
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

ROI_GO_THRESHOLD = 0.08       # 8% ROI → positive verdict
ROI_MONITOR_THRESHOLD = 0.03  # 3% → monitor (not yet good)
Z_STRONG_EDGE = 3.0
Z_LIKELY_EDGE = 2.0
MIN_BETS_VERDICT = 30

# Allowed directions per stat (for context in output)
STAT_DIRECTION_CONTEXT = {
    "pitcher_strikeouts": "UNDER-only",
    "batter_hits": "both",
    "batter_hrr": "both",
    "batter_rbis": "OVER-only",  # kept for historical data analysis
}

W = 96


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def fmt_z(v: float) -> str:
    if math.isnan(v):
        return "  —  "
    return f"{v:+.2f}σ"


def fmt_roi(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.1f}%"


def fmt_odds(american: float) -> str:
    if american >= 0:
        return f"+{american:.0f}"
    return f"{american:.0f}"


def sep(char="─", width=W):
    print(char * width)


def header(title: str):
    print()
    print("━" * W)
    print(f"  {title}")
    print("━" * W)


# ---------------------------------------------------------------------------
# American odds helpers
# ---------------------------------------------------------------------------

def american_to_implied(american: float) -> float:
    """Convert American odds to implied probability (no vig)."""
    if american >= 100:
        return 100.0 / (american + 100.0)
    else:
        return abs(american) / (abs(american) + 100.0)


def american_break_even(american: float) -> float:
    """Break-even win rate needed to profit at these odds."""
    return american_to_implied(american)


def odds_bucket(american: float) -> str:
    if american <= -200:
        return "≤-200"
    if american <= -150:
        return "-200 to -150"
    if american <= -110:
        return "-150 to -110"
    if american < 0:
        return "-110 to -100"
    if american <= 150:
        return "+100 to +150"
    return "+150+"


def edge_bucket(edge: float) -> str:
    e = edge * 100
    if e < 8:
        return "<8%"
    if e < 10:
        return "8-10%"
    if e < 15:
        return "10-15%"
    if e < 20:
        return "15-20%"
    return "20%+"


# ---------------------------------------------------------------------------
# Z-score functions
# ---------------------------------------------------------------------------

def z_score(wins: int, total: int, be_rate: float) -> float:
    if total < 5:
        return float("nan")
    win_rate = wins / total
    sigma = math.sqrt(be_rate * (1 - be_rate) / total)
    return (win_rate - be_rate) / sigma if sigma > 0 else float("nan")


def pnl_z_score(df: pd.DataFrame) -> float:
    if len(df) < 5:
        return float("nan")
    mean = df["pnl"].mean()
    std = df["pnl"].std()
    if std == 0:
        return float("nan")
    return mean / (std / math.sqrt(len(df)))


# ---------------------------------------------------------------------------
# Core metrics builder
# ---------------------------------------------------------------------------

def build_metrics(df: pd.DataFrame) -> dict:
    """
    Compute core performance metrics for a group of resolved bets.
    Expects columns: is_won, pnl, stake, break_even, edge_bucket_col.
    """
    settled = df[df["status"].isin(["won", "lost"])]
    total = len(settled)
    pushes = int((df["status"] == "push").sum())
    cancelled = int((df["status"] == "cancelled").sum())

    if total == 0:
        return {
            "total": 0, "wins": 0, "losses": 0, "pushes": pushes, "cancelled": cancelled,
            "win_rate": 0.0, "break_even": 0.0, "alpha": 0.0,
            "total_staked": 0.0, "total_pnl": 0.0, "roi": 0.0,
            "z": float("nan"), "pnl_z": float("nan"),
        }

    wins = int(settled["is_won"].sum())
    losses = total - wins
    win_rate = wins / total
    total_pnl = float(df["pnl"].sum())
    total_staked = float(settled["stake"].sum())
    roi = total_pnl / total_staked if total_staked > 0 else 0.0
    avg_be = float(settled["break_even"].mean())
    alpha = win_rate - avg_be
    z = z_score(wins, total, avg_be)
    pz = pnl_z_score(settled)

    return {
        "total": total, "wins": wins, "losses": losses, "pushes": pushes, "cancelled": cancelled,
        "win_rate": win_rate, "break_even": avg_be, "alpha": alpha,
        "total_staked": total_staked, "total_pnl": total_pnl, "roi": roi,
        "z": z, "pnl_z": pz,
    }


def print_metrics_table(
    title: str,
    groups: list[tuple[str, pd.DataFrame]],
    label_header: str = "Group",
    label_width: int = 26,
    show_pnl_z: bool = False,
):
    header(title)
    extra = ["PnL-Z"] if show_pnl_z else []
    headers = [label_header, "Bets", "Won", "Win%", "BE%", "Alpha", "P&L", "Staked", "ROI", "Z-score"] + extra
    col_widths = [label_width, 6, 6, 7, 7, 7, 10, 9, 8, 8] + ([7] * len(extra))
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    sep("·")
    for label, grp_df in groups:
        if len(grp_df) == 0:
            continue
        m = build_metrics(grp_df)
        if m["total"] == 0:
            continue
        row = [
            label[:label_width],
            m["total"], m["wins"],
            fmt_pct(m["win_rate"]),
            fmt_pct(m["break_even"]),
            fmt_pct(m["alpha"]),
            f"${m['total_pnl']:+.0f}",
            f"${m['total_staked']:.0f}",
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

def load_bets(engine, stat=None, direction=None, date_start=None, date_end=None) -> pd.DataFrame:
    resolved_statuses = ("won", "lost", "push", "cancelled")
    placeholders = ", ".join(f"'{s}'" for s in resolved_statuses)
    filters = [f"status IN ({placeholders})"]
    params: dict = {}

    if stat:
        filters.append("stat_type = :stat")
        params["stat"] = stat
    if direction:
        filters.append("bet_direction = :direction")
        params["direction"] = direction
    if date_start:
        filters.append("game_date >= :date_start")
        params["date_start"] = date_start
    if date_end:
        filters.append("game_date <= :date_end")
        params["date_end"] = date_end

    query = text(f"""
        SELECT game_date, player_name, stat_type, bet_direction,
               line, odds_at_bet, implied_prob, model_prob, edge,
               stake, status, actual_value, pnl
        FROM mlb_paper_bets
        WHERE {" AND ".join(filters)}
        ORDER BY game_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["week"] = df["game_date"].apply(lambda d: d - timedelta(days=d.weekday()))
    df["is_won"] = df["status"] == "won"
    df["break_even"] = df["odds_at_bet"].apply(american_break_even)
    df["odds_bucket"] = df["odds_at_bet"].apply(odds_bucket)
    df["edge_bucket_col"] = df["edge"].apply(edge_bucket)
    df["margin"] = df.apply(
        lambda r: (r["actual_value"] - r["line"]) if pd.notna(r["actual_value"])
        else float("nan"),
        axis=1,
    )
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
               total_staked, total_pnl, roi_pct, cumulative_pnl, bankroll_after
        FROM mlb_paper_trading_daily_log
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
    settled = df[df["status"].isin(["won", "lost"])]
    wins = int((settled["status"] == "won").sum())
    losses = int((settled["status"] == "lost").sum())
    pushes = int((df["status"] == "push").sum())
    cancelled = int((df["status"] == "cancelled").sum())
    over_bets = df[df["bet_direction"] == "over"]
    under_bets = df[df["bet_direction"] == "under"]

    print()
    print("═" * W)
    print("  MLB SPORTSBOOK PAPER BET ANALYSIS  —  PERFORMANCE HEALTH REPORT")
    print("═" * W)
    print(f"  Date range    : {df['game_date'].min()}  →  {df['game_date'].max()}")
    print(f"  Total bets    : {len(df):,}  (W: {wins}  L: {losses}  P: {pushes}  C: {cancelled})")
    print(f"  Direction     : OVER: {len(over_bets):,}  /  UNDER: {len(under_bets):,}")
    print(f"  Stat types    : {', '.join(sorted(df['stat_type'].unique()))}")

    total_staked = float(settled["stake"].sum())
    total_pnl = float(df["pnl"].sum())
    roi = total_pnl / total_staked if total_staked > 0 else 0.0
    print(f"  Total staked  : ${total_staked:,.2f}")
    print(f"  Total P&L     : ${total_pnl:+,.2f}  (ROI: {fmt_roi(roi)})")
    print("═" * W)


def section_direction_comparison(df: pd.DataFrame):
    over_df = df[df["bet_direction"] == "over"]
    under_df = df[df["bet_direction"] == "under"]
    print_metrics_table(
        "DIRECTION COMPARISON — OVER vs UNDER",
        [("OVER", over_df), ("UNDER", under_df), ("COMBINED", df)],
        label_header="Direction", label_width=12,
        show_pnl_z=True,
    )


def section_by_stat(df: pd.DataFrame):
    stats = sorted(df["stat_type"].unique())
    print_metrics_table(
        "BY STAT TYPE — all directions",
        [(s, df[df["stat_type"] == s]) for s in stats],
        label_header="Stat Type", label_width=26,
    )


def section_stat_direction_crosstab(df: pd.DataFrame):
    """Cross-tab: each stat × over/under."""
    stats = sorted(df["stat_type"].unique())
    directions = ["over", "under"]

    header("BY STAT + DIRECTION CROSS-TAB")
    col_widths = [28, 6, 6, 7, 7, 10, 8, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Stat + Direction", "Bets", "Won", "Win%", "BE%", "P&L", "ROI", "Z"))
    sep("·")

    for stat in stats:
        ctx = STAT_DIRECTION_CONTEXT.get(stat, "both")
        for direction in directions:
            grp = df[(df["stat_type"] == stat) & (df["bet_direction"] == direction)]
            if len(grp) == 0:
                continue
            m = build_metrics(grp)
            if m["total"] == 0:
                continue
            label = f"{stat}  [{direction.upper()}]"
            note = f"  ← {ctx}" if ctx != "both" else ""
            print(fmt.format(
                label[:28],
                m["total"], m["wins"],
                fmt_pct(m["win_rate"]),
                fmt_pct(m["break_even"]),
                f"${m['total_pnl']:+.0f}",
                fmt_roi(m["roi"]),
                fmt_z(m["z"]),
            ) + note)
        sep("·")
    sep()


def section_cross_sectional(df: pd.DataFrame):
    """Check if every stat type is profitable."""
    stats = sorted(df["stat_type"].unique())
    header("CROSS-SECTIONAL CONSISTENCY CHECK — ALL STATS PROFITABLE?")

    all_positive = True
    results = []
    for stat in stats:
        grp = df[df["stat_type"] == stat]
        m = build_metrics(grp)
        profitable = m["total_pnl"] > 0
        if not profitable:
            all_positive = False
        flag = "✓" if profitable else "✗"
        results.append((stat, m, flag, profitable))

    col_widths = [28, 6, 7, 10, 8, 4]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Stat Type", "Bets", "Win%", "P&L", "ROI", ""))
    sep("·")
    for stat, m, flag, _ in results:
        print(fmt.format(
            stat, m["total"],
            fmt_pct(m["win_rate"]) if m["total"] > 0 else "—",
            f"${m['total_pnl']:+.0f}",
            fmt_roi(m["roi"]) if m["total"] > 0 else "—",
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
    edge_order = ["<8%", "8-10%", "10-15%", "15-20%", "20%+"]
    print_metrics_table(
        "BY EDGE BUCKET — expect monotonic ROI increase",
        [(e, df[df["edge_bucket_col"] == e]) for e in edge_order],
        label_header="Edge Bucket", label_width=14,
    )

    # Monotonicity check (skip <8%)
    valid_buckets = ["8-10%", "10-15%", "15-20%", "20%+"]
    rois = []
    for e in valid_buckets:
        grp = df[df["edge_bucket_col"] == e]
        settled = grp[grp["status"].isin(["won", "lost"])]
        if len(settled) >= 5:
            m = build_metrics(grp)
            rois.append((e, m["roi"]))

    if len(rois) >= 2:
        monotonic = all(rois[i][1] <= rois[i + 1][1] for i in range(len(rois) - 1))
        direction = (
            "✓  MONOTONIC — higher edge → higher ROI  (model is well-calibrated)"
            if monotonic else
            "~  NOT STRICTLY MONOTONIC — check if gaps are statistically meaningful"
        )
        print(f"  {direction}")
        sep()


def section_win_margin(df: pd.DataFrame):
    """Analyze actual_value vs line — how much do bets win/lose by."""
    settled = df[df["status"].isin(["won", "lost"])].copy()
    if "margin" not in settled.columns or settled["margin"].isna().all():
        return

    settled = settled.dropna(subset=["margin"])
    won = settled[settled["is_won"]]
    lost = settled[~settled["is_won"]]

    header("WIN MARGIN ANALYSIS  (actual_value - line)")

    col_widths = [14, 8, 8, 8, 10, 10]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Group", "N", "Mean", "Median", "Min", "Max"))
    sep("·")

    for label, grp in [("Won bets", won), ("Lost bets", lost), ("All settled", settled)]:
        if len(grp) == 0:
            continue
        margins = grp["margin"]
        # For OVER bets: positive margin = win (actual > line)
        # For UNDER bets: negative margin = win (actual < line)
        # We show raw margin so positive = above line
        print(fmt.format(
            label, len(grp),
            f"{margins.mean():+.2f}",
            f"{margins.median():+.2f}",
            f"{margins.min():+.2f}",
            f"{margins.max():+.2f}",
        ))

    # Per-stat margin breakdown
    stats = sorted(settled["stat_type"].unique())
    if len(stats) > 1:
        print()
        print(fmt.format("By Stat (won)", "N", "Mean", "Median", "Min", "Max"))
        sep("·")
        for stat in stats:
            grp = won[won["stat_type"] == stat]
            if len(grp) == 0:
                continue
            m = grp["margin"]
            print(fmt.format(
                stat[:14], len(grp),
                f"{m.mean():+.2f}", f"{m.median():+.2f}",
                f"{m.min():+.2f}", f"{m.max():+.2f}",
            ))
    sep()


def section_weekly(df: pd.DataFrame):
    header("WEEKLY PERFORMANCE")
    weeks = sorted(df["week"].unique())
    if len(weeks) < 2:
        print("  Not enough weeks to compare.")
        sep()
        return

    col_widths = [14, 6, 7, 10, 8, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Week (Mon)", "Bets", "Win%", "P&L", "ROI", "Z"))
    sep("·")

    week_metrics = []
    for w in weeks:
        grp = df[df["week"] == w]
        m = build_metrics(grp)
        week_metrics.append((w, m))
        print(fmt.format(
            str(w),
            m["total"],
            fmt_pct(m["win_rate"]) if m["total"] > 0 else "—",
            f"${m['total_pnl']:+.0f}",
            fmt_roi(m["roi"]) if m["total"] > 0 else "—",
            fmt_z(m["z"]),
        ))

    # Week-over-week delta (last two weeks with data)
    weeks_with_data = [(w, m) for w, m in week_metrics if m["total"] > 0]
    if len(weeks_with_data) >= 2:
        sep("·")
        w_prev, mp = weeks_with_data[-2]
        w_last, ml = weeks_with_data[-1]
        roi_d = ml["roi"] - mp["roi"]
        wr_d = ml["win_rate"] - mp["win_rate"]
        vol_d = ml["total"] - mp["total"]
        print(
            f"  WoW: ROI {fmt_roi(mp['roi'])} → {fmt_roi(ml['roi'])}  "
            f"({'+' if roi_d >= 0 else ''}{roi_d * 100:.1f}pp)   "
            f"Win% {fmt_pct(mp['win_rate'])} → {fmt_pct(ml['win_rate'])}  "
            f"({'+' if wr_d >= 0 else ''}{wr_d * 100:.1f}pp)   "
            f"Volume: {mp['total']} → {ml['total']}  ({'+' if vol_d >= 0 else ''}{vol_d})"
        )
    sep()


def section_daily_pnl(df: pd.DataFrame, engine, date_start=None, date_end=None, show_bankroll: bool = True):
    header("DAILY P&L TREND + BANKROLL TRAJECTORY")

    bankroll_map: dict = {}
    if show_bankroll:
        log_df = load_daily_log(engine, date_start=date_start, date_end=date_end)
        if not log_df.empty:
            log_df["game_date"] = pd.to_datetime(log_df["game_date"]).dt.date
            for _, row in log_df.iterrows():
                bankroll_map[row["game_date"]] = {
                    "bankroll": row.get("bankroll_after"),
                    "log_pnl": row.get("total_pnl"),
                    "log_roi": row.get("roi_pct"),
                }

    col_widths = [12, 6, 5, 7, 7, 10, 11, 10, 8]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    headers = ["Date", "Bets", "Won", "Win%", "BE%", "P&L", "Cumul P&L", "Bankroll", "ROI"]
    print(fmt.format(*headers))
    sep("·")

    cumul = 0.0
    for d, grp in df.groupby("game_date"):
        m = build_metrics(grp)
        cumul += m["total_pnl"]
        bk_info = bankroll_map.get(d, {})
        bankroll = bk_info.get("bankroll")
        log_roi = bk_info.get("log_roi")
        bk_str = f"${bankroll:.0f}" if bankroll is not None else "—"
        roi_str = f"{log_roi:.1f}%" if log_roi is not None else (fmt_roi(m["roi"]) if m["total"] > 0 else "—")
        print(fmt.format(
            str(d), m["total"], m["wins"],
            fmt_pct(m["win_rate"]) if m["total"] > 0 else "—",
            fmt_pct(m["break_even"]) if m["total"] > 0 else "—",
            f"${m['total_pnl']:+.0f}",
            f"${cumul:+.0f}",
            bk_str,
            roi_str,
        ))
    sep()


def section_by_odds_bucket(df: pd.DataFrame):
    odds_order = ["≤-200", "-200 to -150", "-150 to -110", "-110 to -100", "+100 to +150", "+150+"]
    groups = [(o, df[df["odds_bucket"] == o]) for o in odds_order if (df["odds_bucket"] == o).any()]
    print_metrics_table(
        "BY ODDS BUCKET — where does edge concentrate?",
        groups,
        label_header="Odds Range", label_width=18,
    )


def section_significance(df: pd.DataFrame):
    """Z-scores, CI, PnL-Z, and verdict."""
    header("STATISTICAL SIGNIFICANCE")

    m = build_metrics(df)
    if m["total"] < 5:
        print(f"  Insufficient data ({m['total']} bets). Need at least 5 resolved bets.")
        sep()
        return

    se = math.sqrt(m["win_rate"] * (1 - m["win_rate"]) / m["total"]) if m["total"] > 0 else 0
    ci_lo = m["win_rate"] - 1.96 * se
    ci_hi = m["win_rate"] + 1.96 * se

    verdict = (
        "STRONG EDGE ✓" if m["z"] > Z_STRONG_EDGE else
        ("LIKELY EDGE" if m["z"] > Z_LIKELY_EDGE else "WEAK / INCONCLUSIVE")
    )

    print(f"  N = {m['total']:,}   Won = {m['wins']} / {m['total']}   Win% = {fmt_pct(m['win_rate'])}")
    print(f"  Break-even = {fmt_pct(m['break_even'])}   Alpha = {fmt_pct(m['alpha'])}")
    print(f"  Z-score (vs BE) = {fmt_z(m['z'])}   PnL-Z = {fmt_z(m['pnl_z'])}")
    print(f"  95% CI = [{fmt_pct(ci_lo)},  {fmt_pct(ci_hi)}]")
    print(f"  P&L = ${m['total_pnl']:+,.0f}   ROI = {fmt_roi(m['roi'])}")
    print(f"  Verdict: {verdict}")
    print()

    # Per-stat significance
    stats = sorted(df["stat_type"].unique())
    if len(stats) > 1:
        print("  Per-stat significance:")
        col_widths = [26, 6, 7, 8, 8, 14]
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print("  " + fmt.format("Stat", "Bets", "Win%", "ROI", "Z", "Verdict"))
        sep("·")
        for stat in stats:
            grp = df[df["stat_type"] == stat]
            ms = build_metrics(grp)
            if ms["total"] < 5:
                sv = "insufficient data"
            elif ms["z"] > Z_STRONG_EDGE:
                sv = "STRONG EDGE ✓"
            elif ms["z"] > Z_LIKELY_EDGE:
                sv = "LIKELY EDGE"
            else:
                sv = "weak"
            print("  " + fmt.format(
                stat[:26], ms["total"],
                fmt_pct(ms["win_rate"]) if ms["total"] > 0 else "—",
                fmt_roi(ms["roi"]) if ms["total"] > 0 else "—",
                fmt_z(ms["z"]),
                sv,
            ))
        sep()
    else:
        sep()


def section_overall_verdict(df: pd.DataFrame):
    """Scorecard verdict: GO / MONITOR / NOT READY."""
    m = build_metrics(df)

    # Cross-sectional
    stats = sorted(df["stat_type"].unique())
    stat_pnls = {s: build_metrics(df[df["stat_type"] == s])["total_pnl"] for s in stats}
    all_stats_profitable = all(p > 0 for p in stat_pnls.values())
    n_profitable_stats = sum(1 for p in stat_pnls.values() if p > 0)

    checks = [
        ("ROI > 8%",                   m["roi"] > ROI_GO_THRESHOLD,   fmt_roi(m["roi"])),
        ("Z-score > 2.0 (likely edge)", not math.isnan(m["z"]) and m["z"] > Z_LIKELY_EDGE, fmt_z(m["z"])),
        ("Z-score > 3.0 (strong edge)", not math.isnan(m["z"]) and m["z"] > Z_STRONG_EDGE, fmt_z(m["z"])),
        ("All stats profitable",        all_stats_profitable,          f"{n_profitable_stats}/{len(stats)} profitable"),
        (f"≥ {MIN_BETS_VERDICT} resolved bets",    m["total"] >= MIN_BETS_VERDICT,       f"{m['total']} bets"),
        ("Win rate > break-even",       m["alpha"] > 0,                fmt_pct(m["alpha"])),
    ]

    n_pass = sum(1 for _, passed, _ in checks if passed)
    n_total = len(checks)

    header("OVERALL VERDICT — GO / MONITOR / NOT READY")
    col_widths = [40, 12, 22]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format("Check", "Result", "Value"))
    sep("·")
    for name, passed, val in checks:
        mark = "✓  PASS" if passed else "✗  FAIL"
        print(fmt.format(name, mark, val))
    sep()

    print(f"  Score: {n_pass}/{n_total} checks passed")
    print()

    if n_pass >= n_total - 1 and m["roi"] > ROI_GO_THRESHOLD and m["z"] > Z_LIKELY_EDGE:
        verdict = "GO"
        detail = "Strong evidence of edge. Consider live deployment."
    elif m["roi"] > ROI_GO_THRESHOLD and m["total"] >= MIN_BETS_VERDICT:
        verdict = "MONITOR"
        detail = "ROI positive. Continue tracking — need more bets and/or higher Z-score."
    elif m["roi"] > ROI_MONITOR_THRESHOLD:
        verdict = "MONITOR — NOT YET"
        detail = f"ROI {fmt_roi(m['roi'])} is positive but below threshold. Accumulate more data."
    else:
        verdict = "NOT READY"
        detail = "Edge not yet proven. Continue paper trading."

    print(f"  {'═' * 60}")
    print(f"  VERDICT: {verdict}")
    print(f"  {detail}")
    print(f"  {'═' * 60}")

    # Days and weeks of data
    days_of_data = (df["game_date"].max() - df["game_date"].min()).days + 1
    weeks_of_data = days_of_data / 7
    print(f"\n  Data window: {days_of_data} days (~{weeks_of_data:.1f} weeks)")
    print("  Scaling milestones:")
    print("    2 weeks at ROI > 8%  →  increase stake size")
    print("    4 weeks at ROI > 8%  →  further scale")
    sep()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_analysis(df: pd.DataFrame, engine, args):
    show_bankroll = not getattr(args, "no_bankroll", False)
    date_start = getattr(args, "date_start", None)
    date_end = getattr(args, "date_end", None)

    section_top_line(df)
    section_direction_comparison(df)
    section_by_stat(df)
    section_stat_direction_crosstab(df)
    section_cross_sectional(df)
    section_by_edge_bucket(df)
    section_win_margin(df)
    section_weekly(df)
    section_daily_pnl(df, engine, date_start=date_start, date_end=date_end, show_bankroll=show_bankroll)
    section_by_odds_bucket(df)
    section_significance(df)
    section_overall_verdict(df)


def parse_args():
    parser = argparse.ArgumentParser(description="MLB sportsbook paper bet performance report")
    parser.add_argument("--days", type=int, default=None, help="Last N days (default: all)")
    parser.add_argument("--date-start", type=str, default=None, dest="date_start")
    parser.add_argument("--date-end", type=str, default=None, dest="date_end")
    parser.add_argument("--stat", type=str, default=None,
                        choices=["pitcher_strikeouts", "batter_hits", "batter_hrr", "batter_rbis"],
                        help="Filter to a single stat type")
    parser.add_argument("--direction", type=str, default=None, choices=["over", "under"],
                        help="Filter to over or under only")
    parser.add_argument("--no-bankroll", action="store_true", dest="no_bankroll",
                        help="Skip daily_log bankroll column")
    return parser.parse_args()


def main():
    args = parse_args()

    date_start = args.date_start
    date_end = args.date_end
    if args.days:
        date_start = (date.today() - timedelta(days=args.days)).isoformat()

    engine = get_engine()
    df = load_bets(
        engine,
        stat=args.stat,
        direction=args.direction,
        date_start=date_start,
        date_end=date_end,
    )

    if df.empty:
        print("No resolved bets found for the given filters.")
        sys.exit(0)

    run_analysis(df, engine, args)


if __name__ == "__main__":
    main()
