"""
Calibration drift monitor for paper trading.

Computes calibration metrics from resolved paper bets joined with
daily_predictions, and flags drift when predictions deviate from reality.

Runs after bet resolution in the daily stats job. Lightweight — all
vectorized pandas, typically < 5 seconds.

Usage (standalone):
    python -m src.paper_trading.calibration_monitor
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger(__name__)


def get_model_activation_date() -> date | None:
    """Return the activation date of the current production model.

    Reads NBA_PLAYOFF_MODE env var to pick production_playoffs or production
    subdir, then parses the timestamp from run_config.json.
    Returns None on any error (graceful failure disables date capping).
    """
    try:
        playoff_mode = os.environ.get("NBA_PLAYOFF_MODE", "").lower() in ("1", "true", "yes")
        subdir = "production_playoffs" if playoff_mode else "production"
        config_path = (
            Path(__file__).resolve().parents[2]
            / "src" / "models" / "artifacts" / subdir / "run_config.json"
        )
        with open(config_path) as f:
            config = json.load(f)
        return date.fromisoformat(config["timestamp"][:10])
    except Exception:
        return None


QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]
QUANTILE_COLS = {q: f"pred_q{int(q * 100)}" for q in QUANTILES}

# Thresholds (tightened Session 75 — catch drift earlier)
MIN_RESOLVED_BETS = 20
QUANTILE_GAP_THRESHOLD = 0.03  # flag if coverage gap > 3% (was 5%)
BIAS_REL_THRESHOLD = 4.0  # flag if relative bias > 4% (was 5%)
EDGE_GAP_THRESHOLD = 0.08  # flag if edge accuracy gap > 8pp (was 10pp)
EDGE_MIN_SAMPLE = 10  # minimum bets in an edge bucket
ECE_THRESHOLD = 0.03  # flag if ECE > 0.03 (was 0.05)

# Sample size below which metrics are too noisy for reliable conclusions
LOW_CONFIDENCE_THRESHOLD = 75

# Relaxed thresholds for low-confidence windows (< 75 bets)
LOW_CONF_QUANTILE_GAP = 0.08
LOW_CONF_ECE = 0.08
LOW_CONF_BIAS_REL = 10.0
LOW_CONF_EDGE_GAP = 0.15


@dataclass
class CalibrationMetrics:
    """Container for all calibration drift metrics."""

    n_bets: int = 0
    date_range: tuple[str, str] = ("", "")

    # Quantile coverage: {stat: {q: coverage}} and global
    quantile_coverage: dict = field(default_factory=dict)
    quantile_alerts: list[str] = field(default_factory=list)

    # Probability calibration
    brier_score: float = 0.0
    ece: float = 0.0
    prob_alerts: list[str] = field(default_factory=list)

    # Bias by stat: {stat: {bias, rel_bias_pct, mean_pred, mean_actual}}
    bias_by_stat: dict = field(default_factory=dict)
    bias_alerts: list[str] = field(default_factory=list)

    # Edge accuracy: [{bucket, expected_win_rate, actual_win_rate, n, gap}]
    edge_accuracy: list[dict] = field(default_factory=list)
    edge_alerts: list[str] = field(default_factory=list)

    # Model context label (e.g. "playoff model, active since 2026-04-15")
    model_context: str = ""

    @property
    def all_alerts(self) -> list[str]:
        """Alerts that drive severity. Bias excluded (systematic under-prediction is expected)."""
        return self.quantile_alerts + self.prob_alerts + self.edge_alerts

    @property
    def low_confidence(self) -> bool:
        """True when sample size is too small for reliable calibration."""
        return self.n_bets < LOW_CONFIDENCE_THRESHOLD

    @property
    def has_drift(self) -> bool:
        return len(self.all_alerts) > 0

    @property
    def severity(self) -> str:
        """'healthy', 'warning', or 'critical' based on alert count.

        Caps at 'warning' when sample size is below LOW_CONFIDENCE_THRESHOLD
        because small samples produce noisy metrics that look like drift.
        """
        n = len(self.all_alerts)
        if n == 0:
            return "healthy"
        elif n < 3 or self.low_confidence:
            return "warning"
        return "critical"


def _load_resolved_bets(lookback_days: int = 14) -> pd.DataFrame:
    """Load resolved paper bets joined with daily_predictions.

    Returns DataFrame with columns needed for calibration:
    - stat_type, actual_value, line, model_prob, edge, bet_direction, status
    - pred_q10..pred_q90, over_prob (from daily_predictions)
    """
    from src.db.client import get_engine

    engine = get_engine()
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = """
        SELECT
            pb.game_date,
            pb.stat_type,
            pb.actual_value,
            pb.line,
            pb.model_prob,
            pb.edge,
            pb.bet_direction,
            pb.status,
            dp.pred_q10,
            dp.pred_q25,
            dp.pred_q50,
            dp.pred_q75,
            dp.pred_q90,
            dp.over_prob
        FROM paper_bets pb
        JOIN daily_predictions dp ON pb.prediction_id = dp.id
        WHERE pb.status IN ('won', 'lost', 'push')
          AND pb.game_date >= :cutoff
          AND pb.actual_value IS NOT NULL
    """

    from sqlalchemy import text

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"cutoff": cutoff})

    return df


def _compute_quantile_coverage(df: pd.DataFrame) -> tuple[dict, list[str]]:
    """Check P(actual <= pred_qXX) ~= target for each quantile.

    Returns (coverage_dict, alerts).
    """
    results = {}
    alerts = []

    # Per stat + global
    groups = list(df.groupby("stat_type")) + [("GLOBAL", df)]

    for label, group in groups:
        coverages = {}
        for q in QUANTILES:
            col = QUANTILE_COLS[q]
            if col not in group.columns:
                continue
            valid = group[group[col].notna()]
            if valid.empty:
                continue
            coverages[q] = float((valid["actual_value"] <= valid[col]).mean())

        if not coverages:
            continue

        results[label] = coverages

        # Flag gaps
        for q, cov in coverages.items():
            gap = abs(cov - q)
            if gap > QUANTILE_GAP_THRESHOLD:
                direction = "over" if cov > q else "under"
                q_label = f"Q{int(q * 100)}"
                if label == "GLOBAL":
                    alerts.append(
                        f"Global {q_label}: {direction}-coverage "
                        f"({cov:.1%} vs {q:.0%} target, gap {gap:.1%})"
                    )
                else:
                    alerts.append(
                        f"{label.upper()} {q_label}: {direction}-coverage "
                        f"({cov:.1%} vs {q:.0%} target)"
                    )

    return results, alerts


def _compute_prob_calibration(df: pd.DataFrame) -> tuple[float, float, list[str]]:
    """Compute Brier score and ECE.

    Returns (brier, ece, alerts).
    """
    valid = df[
        df["over_prob"].notna() & df["line"].notna() & df["actual_value"].notna()
    ].copy()

    if valid.empty:
        return 0.0, 0.0, []

    valid["hit"] = (valid["actual_value"] > valid["line"]).astype(float)

    # Brier score
    brier = float(((valid["over_prob"] - valid["hit"]) ** 2).mean())

    # ECE (10 bins)
    n_bins = 10
    bin_edges = np.linspace(0, 1, n_bins + 1)
    weighted_gaps = 0.0
    total = len(valid)

    for i in range(n_bins):
        mask = (valid["over_prob"] >= bin_edges[i]) & (valid["over_prob"] < bin_edges[i + 1])
        if i == n_bins - 1:
            mask = mask | (valid["over_prob"] == bin_edges[i + 1])
        count = int(mask.sum())
        if count == 0:
            continue
        pred_mean = float(valid.loc[mask, "over_prob"].mean())
        actual_rate = float(valid.loc[mask, "hit"].mean())
        weighted_gaps += abs(actual_rate - pred_mean) * count

    ece = weighted_gaps / total if total > 0 else 0.0

    alerts = []
    if ece > ECE_THRESHOLD:
        alerts.append(f"ECE {ece:.3f} exceeds threshold ({ECE_THRESHOLD})")
    if brier > 0.25:
        alerts.append(f"Brier score {brier:.3f} is high (> 0.25)")

    return brier, ece, alerts


def _compute_bias_by_stat(df: pd.DataFrame) -> tuple[dict, list[str]]:
    """Compute mean(pred_q50) - mean(actual) per stat.

    Returns (bias_dict, alerts).
    """
    results = {}
    alerts = []

    if "pred_q50" not in df.columns:
        return results, alerts

    groups = list(df.groupby("stat_type")) + [("GLOBAL", df)]

    for label, group in groups:
        valid = group[group["pred_q50"].notna() & group["actual_value"].notna()]
        if valid.empty:
            continue

        mean_pred = float(valid["pred_q50"].mean())
        mean_actual = float(valid["actual_value"].mean())
        bias = mean_pred - mean_actual
        rel_bias = (bias / mean_actual * 100) if mean_actual != 0 else 0.0

        results[label] = {
            "mean_pred": round(mean_pred, 1),
            "mean_actual": round(mean_actual, 1),
            "bias": round(bias, 2),
            "rel_bias_pct": round(rel_bias, 1),
            "n": len(valid),
        }

        if abs(rel_bias) > BIAS_REL_THRESHOLD and label != "GLOBAL":
            direction = "high" if rel_bias > 0 else "low"
            alerts.append(
                f"{label.upper()}: predictions {direction} by {abs(rel_bias):.1f}% "
                f"(pred {mean_pred:.1f} vs actual {mean_actual:.1f})"
            )

    return results, alerts


def _compute_edge_accuracy(df: pd.DataFrame) -> tuple[list[dict], list[str]]:
    """Bucket bets by edge size, compare actual win rate to model_prob.

    Returns (buckets_list, alerts).
    """
    results = []
    alerts = []

    valid = df[
        df["edge"].notna()
        & df["model_prob"].notna()
        & df["status"].isin(["won", "lost"])
    ].copy()

    if valid.empty:
        return results, alerts

    valid["won"] = (valid["status"] == "won").astype(float)

    # Edge buckets: 5-10%, 10-15%, 15%+
    buckets = [
        ("5-10%", 0.05, 0.10),
        ("10-15%", 0.10, 0.15),
        ("15%+", 0.15, 1.0),
    ]

    for label, lo, hi in buckets:
        mask = (valid["edge"] >= lo) & (valid["edge"] < hi)
        group = valid[mask]
        n = len(group)
        if n < EDGE_MIN_SAMPLE:
            continue

        expected = float(group["model_prob"].mean())
        actual = float(group["won"].mean())
        gap = actual - expected

        entry = {
            "bucket": label,
            "expected_win_rate": round(expected, 3),
            "actual_win_rate": round(actual, 3),
            "n": n,
            "gap": round(gap, 3),
        }
        results.append(entry)

        if abs(gap) > EDGE_GAP_THRESHOLD:
            direction = "outperforming" if gap > 0 else "underperforming"
            alerts.append(
                f"Edge {label}: {direction} "
                f"(actual {actual:.1%} vs expected {expected:.1%}, n={n})"
            )

    return results, alerts


def compute_calibration_drift(lookback_days: int = 7) -> CalibrationMetrics | None:
    """Compute all calibration drift metrics from resolved paper bets.

    Returns CalibrationMetrics, or None if insufficient data (< 20 bets).
    """
    logger.info(f"Computing calibration drift (lookback={lookback_days} days)")

    df = _load_resolved_bets(lookback_days)

    # Cap window to active model's activation date to avoid stale prior-model data
    activation = get_model_activation_date()
    if activation is not None:
        df = df[pd.to_datetime(df["game_date"]).dt.date >= activation]
        if len(df) < MIN_RESOLVED_BETS:
            logger.info(
                f"Model active since {activation}, {len(df)} resolved bets — "
                f"skipping calibration (need >= {MIN_RESOLVED_BETS})"
            )
            return None

    if len(df) < MIN_RESOLVED_BETS:
        logger.info(
            f"Insufficient data for calibration: {len(df)} bets "
            f"(need >= {MIN_RESOLVED_BETS})"
        )
        return None

    metrics = CalibrationMetrics(n_bets=len(df))

    # Model context label
    if activation is not None:
        playoff_mode = os.environ.get("NBA_PLAYOFF_MODE", "").lower() in ("1", "true", "yes")
        model_name = "playoff model" if playoff_mode else "regular model"
        metrics.model_context = f"{model_name}, active since {activation}"

    # Date range
    if "game_date" in df.columns and not df["game_date"].isna().all():
        dates = pd.to_datetime(df["game_date"])
        metrics.date_range = (str(dates.min().date()), str(dates.max().date()))

    # Use relaxed thresholds when sample size is too small for reliable conclusions
    global QUANTILE_GAP_THRESHOLD, ECE_THRESHOLD, BIAS_REL_THRESHOLD, EDGE_GAP_THRESHOLD
    _orig = (QUANTILE_GAP_THRESHOLD, ECE_THRESHOLD, BIAS_REL_THRESHOLD, EDGE_GAP_THRESHOLD)
    if len(df) < LOW_CONFIDENCE_THRESHOLD:
        logger.info(
            f"Low confidence mode ({len(df)} < {LOW_CONFIDENCE_THRESHOLD} bets) "
            f"— using relaxed thresholds"
        )
        QUANTILE_GAP_THRESHOLD = LOW_CONF_QUANTILE_GAP
        ECE_THRESHOLD = LOW_CONF_ECE
        BIAS_REL_THRESHOLD = LOW_CONF_BIAS_REL
        EDGE_GAP_THRESHOLD = LOW_CONF_EDGE_GAP

    try:
        # Compute all metrics
        metrics.quantile_coverage, metrics.quantile_alerts = _compute_quantile_coverage(df)
        metrics.brier_score, metrics.ece, metrics.prob_alerts = _compute_prob_calibration(df)
        metrics.bias_by_stat, metrics.bias_alerts = _compute_bias_by_stat(df)
        metrics.edge_accuracy, metrics.edge_alerts = _compute_edge_accuracy(df)
    finally:
        # Restore default thresholds so subsequent calls aren't affected
        QUANTILE_GAP_THRESHOLD, ECE_THRESHOLD, BIAS_REL_THRESHOLD, EDGE_GAP_THRESHOLD = _orig

    alert_count = len(metrics.all_alerts)
    logger.info(
        f"Calibration computed: {metrics.n_bets} bets, "
        f"{alert_count} alert(s), severity={metrics.severity}"
    )
    if metrics.all_alerts:
        for alert in metrics.all_alerts:
            logger.warning(f"  Drift: {alert}")

    return metrics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    result = compute_calibration_drift()
    if result is None:
        print("Insufficient data for calibration check.")
    else:
        print(f"\nCalibration Report ({result.n_bets} bets, {result.date_range[0]} to {result.date_range[1]})")
        confidence_note = " (LOW CONFIDENCE — metrics may be noisy)" if result.low_confidence else ""
        print(f"Severity: {result.severity}{confidence_note}")
        print(f"Brier: {result.brier_score:.3f} | ECE: {result.ece:.3f}")

        if result.quantile_coverage:
            print("\nQuantile Coverage:")
            for stat, covs in result.quantile_coverage.items():
                parts = [f"Q{int(q*100)}={c:.1%}" for q, c in covs.items()]
                print(f"  {stat}: {', '.join(parts)}")

        if result.bias_by_stat:
            print("\nBias by Stat:")
            for stat, b in result.bias_by_stat.items():
                print(f"  {stat}: pred={b['mean_pred']}, actual={b['mean_actual']}, bias={b['rel_bias_pct']:+.1f}%")

        if result.edge_accuracy:
            print("\nEdge Accuracy:")
            for ea in result.edge_accuracy:
                print(f"  {ea['bucket']}: expected={ea['expected_win_rate']:.1%}, actual={ea['actual_win_rate']:.1%} (n={ea['n']})")

        if result.all_alerts:
            print("\nDrift Alerts:")
            for a in result.all_alerts:
                print(f"  - {a}")
        else:
            print("\nNo drift detected.")
