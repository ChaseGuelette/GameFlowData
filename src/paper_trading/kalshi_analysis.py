"""
Kalshi Paper Bet Analysis Module
=================================
Importable module for computing Kalshi paper trading performance metrics.
Extracted from scripts/analyze_kalshi_paper_bets.py for use in the daily
summary job and Discord reporting.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fee / break-even helpers (mirrors scripts/analyze_kalshi_paper_bets.py)
# ---------------------------------------------------------------------------

def _taker_fee_per_contract(price_cents: float) -> float:
    """Taker fee in dollars for one contract at given price (cents)."""
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def _break_even_win_rate(price_cents: float) -> float:
    """Minimum win rate to be profitable at taker pricing."""
    cost_per = price_cents / 100.0
    fee_per = _taker_fee_per_contract(price_cents)
    return cost_per / (1.0 - fee_per)


def _edge_bucket(edge: float) -> str:
    e = edge * 100
    if e < 3:
        return "<3%"
    if e < 5:
        return "3-5%"
    if e < 10:
        return "5-10%"
    if e < 15:
        return "10-15%"
    if e < 20:
        return "15-20%"
    return "20%+"


def _z_score(wins: int, total: int, be_rate: float) -> float:
    if total < 5:
        return float("nan")
    win_rate = wins / total
    sigma = math.sqrt(be_rate * (1 - be_rate) / total)
    return (win_rate - be_rate) / sigma if sigma > 0 else float("nan")


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class KalshiAnalysisMetrics:
    n_bets: int                    # Total resolved NO bets (real only)
    date_range: tuple[str, str]    # (start_date, end_date)
    win_rate: float                # Overall win rate
    break_even: float              # Average break-even rate across bets
    alpha: float                   # win_rate - break_even
    z_score: float                 # Z-score for the lookback window (14d default)
    total_pnl: float
    roi: float                     # ROI on stake: pnl / total contract cost
    bankroll_roi: float | None  # ROI on bankroll: pnl / starting bankroll for window
    z_score_alltime: float = float("nan")  # Z-score across ALL resolved NO bets ever
    n_bets_alltime: int = 0                # Sample size for all-time Z-score
    by_stat: list[dict] = field(default_factory=list)        # [{stat, wins, total, win_rate, pnl, roi}]
    by_edge_bucket: list[dict] = field(default_factory=list) # [{bucket, wins, total, win_rate, pnl}]
    mode: str = "NO-ONLY"

    @property
    def verdict(self) -> str:
        if math.isnan(self.z_score):
            return "INSUFFICIENT DATA"
        if self.z_score > 3:
            return "STRONG EDGE"
        if self.z_score > 2:
            return "LIKELY EDGE"
        return "INCONCLUSIVE"

    @property
    def severity(self) -> str:
        """Returns embed color hint: healthy / warning / critical."""
        if math.isnan(self.z_score) or self.z_score < 1.0:
            return "critical"
        if self.z_score < 2.0:
            return "warning"
        return "healthy"

    @property
    def ci_95(self) -> tuple[float, float]:
        """95% Wilson-ish CI (normal approximation)."""
        if self.n_bets < 5:
            return (0.0, 1.0)
        se = math.sqrt(self.win_rate * (1 - self.win_rate) / self.n_bets)
        return (self.win_rate - 1.96 * se, self.win_rate + 1.96 * se)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_kalshi_analysis(lookback_days: int = 14) -> KalshiAnalysisMetrics | None:
    """Compute Kalshi paper trading analysis over the last N days.

    Queries resolved real NO bets from kalshi_paper_bets and computes:
    - Win rate, break-even, alpha, Z-score
    - By-stat breakdown
    - By-edge-bucket breakdown (10-15%, 15-20%, 20%+)

    Args:
        lookback_days: Number of days to look back (default: 14).

    Returns:
        KalshiAnalysisMetrics or None if insufficient data.
    """
    date_start = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = text("""
        SELECT id, game_date, sport, player_name, stat_type, side,
               price, contracts, fee_adjusted_edge, pnl, status, placed_at
        FROM kalshi_paper_bets
        WHERE status IN ('won', 'lost')
          AND side = 'no'
          AND game_date >= :date_start
        ORDER BY game_date, placed_at
    """)

    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"date_start": date_start})
    except Exception as e:
        logger.error(f"Failed to query kalshi_paper_bets: {e}")
        return None

    if df.empty:
        logger.info(f"No resolved Kalshi NO bets in the last {lookback_days} days")
        return None

    # Compute derived columns (query filters side = 'no', so actual_price = 100 - yes_price)
    df["actual_price"] = 100 - df["price"].astype(int)
    df["cost_per"] = df["actual_price"] / 100.0
    df["fee_per"] = df["actual_price"].apply(_taker_fee_per_contract)
    df["break_even"] = df["actual_price"].apply(_break_even_win_rate)
    df["is_won"] = df["status"] == "won"
    df["total_cost"] = df["cost_per"] * df["contracts"]
    df["edge_bucket"] = df["fee_adjusted_edge"].apply(_edge_bucket)

    # Overall metrics
    total = len(df)
    wins = int(df["is_won"].sum())
    win_rate = wins / total
    avg_be = float(df["break_even"].mean())
    alpha = win_rate - avg_be
    z = _z_score(wins, total, avg_be)
    total_pnl = float(df["pnl"].sum())
    total_cost_sum = float(df["total_cost"].sum())
    roi = total_pnl / total_cost_sum if total_cost_sum > 0 else 0.0

    date_min = str(df["game_date"].min())[:10]
    date_max = str(df["game_date"].max())[:10]

    # Bankroll ROI: pnl / bankroll at the start of the window
    bankroll_roi: float | None = None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT bankroll_after
                    FROM kalshi_paper_trading_daily_log
                    WHERE game_date < :date_start
                    ORDER BY game_date DESC
                    LIMIT 1
                """),
                {"date_start": date_start},
            ).fetchone()
        if row and row[0] and float(row[0]) > 0:
            bankroll_roi = total_pnl / float(row[0])
    except Exception as e:
        logger.debug(f"Could not compute bankroll ROI: {e}")

    # All-time Z-score (all resolved real NO bets, no date filter)
    z_alltime = float("nan")
    n_alltime = 0
    try:
        alltime_query = text("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
                   AVG(CASE WHEN side = 'no' THEN (100 - price::int)::float / 100.0
                            ELSE price::int::float / 100.0 END) as avg_cost
            FROM kalshi_paper_bets
            WHERE status IN ('won', 'lost')
              AND side = 'no'
        """)
        with engine.connect() as conn:
            at_row = conn.execute(alltime_query).fetchone()
        if at_row and at_row[0] and int(at_row[0]) >= 5:
            at_total = int(at_row[0])
            at_wins = int(at_row[1])
            at_cost = float(at_row[2])
            # Approximate avg break-even from avg cost (close enough for a headline stat)
            at_be = at_cost / (1.0 - math.ceil(0.07 * at_cost * (1 - at_cost) * 100) / 100)
            z_alltime = _z_score(at_wins, at_total, at_be)
            n_alltime = at_total
    except Exception as e:
        logger.debug(f"Could not compute all-time Z-score: {e}")

    # By stat
    by_stat = []
    for stat, grp in df.groupby("stat_type"):
        grp_wins = int(grp["is_won"].sum())
        grp_total = len(grp)
        grp_pnl = float(grp["pnl"].sum())
        grp_cost = float(grp["total_cost"].sum())
        grp_roi = grp_pnl / grp_cost if grp_cost > 0 else 0.0
        by_stat.append({
            "stat": str(stat),
            "wins": grp_wins,
            "total": grp_total,
            "win_rate": grp_wins / grp_total,
            "pnl": grp_pnl,
            "roi": grp_roi,
        })
    by_stat.sort(key=lambda x: x["total"], reverse=True)

    # By edge bucket (only meaningful buckets)
    relevant_buckets = ["10-15%", "15-20%", "20%+"]
    by_edge_bucket = []
    for bucket in relevant_buckets:
        grp = df[df["edge_bucket"] == bucket]
        if len(grp) == 0:
            continue
        grp_wins = int(grp["is_won"].sum())
        grp_total = len(grp)
        grp_pnl = float(grp["pnl"].sum())
        by_edge_bucket.append({
            "bucket": bucket,
            "wins": grp_wins,
            "total": grp_total,
            "win_rate": grp_wins / grp_total,
            "pnl": grp_pnl,
        })

    return KalshiAnalysisMetrics(
        n_bets=total,
        date_range=(date_min, date_max),
        win_rate=win_rate,
        break_even=avg_be,
        alpha=alpha,
        z_score=z,
        total_pnl=total_pnl,
        roi=roi,
        bankroll_roi=bankroll_roi,
        z_score_alltime=z_alltime,
        n_bets_alltime=n_alltime,
        by_stat=by_stat,
        by_edge_bucket=by_edge_bucket,
        mode="NO-ONLY",
    )
