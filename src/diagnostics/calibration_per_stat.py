"""
Per-stat calibration diagnostic tool.

Produces a calibration report broken down by stat (PTS, REB, AST) with
quantile coverage, bias, probability calibration (ECE/Brier), and interval
sharpness. Reads from a backtest predictions CSV or from the production DB.

Usage:
    python -m src.diagnostics.calibration_per_stat --csv backtest_results/predictions.csv
    python -m src.diagnostics.calibration_per_stat --db --start 2025-01-01 --end 2025-02-15
    python -m src.diagnostics.calibration_per_stat --csv predictions.csv --output report.json
    python -m src.diagnostics.calibration_per_stat --csv predictions.csv --tolerance 0.05
"""

import argparse
import io
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure stdout can handle unicode on Windows (cp1252 doesn't support ⚠)
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.append(str(Path(__file__).resolve().parents[2]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]
QUANTILE_COLS = {q: f"pred_q{int(q * 100)}" for q in QUANTILES}
DEFAULT_STATS = ["pts", "reb", "ast"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_from_csv(csv_path: str, stats: list[str] | None = None) -> pd.DataFrame:
    """Load predictions from a backtest CSV and normalize columns."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path)
    return _normalize(df, stats)


def load_from_db(
    start_date: date,
    end_date: date,
    stats: list[str] | None = None,
) -> pd.DataFrame:
    """Load predictions from daily_predictions + player_game_stats."""
    from src.db.client import get_engine

    engine = get_engine()

    query = """
        SELECT
            dp.prediction_date,
            dp.player_id,
            dp.game_id,
            dp.stat,
            dp.pred_q10,
            dp.pred_q25,
            dp.pred_q50,
            dp.pred_q75,
            dp.pred_q90,
            dp.over_prob,
            dp.line,
            CASE dp.stat
                WHEN 'pts' THEN pgs.pts
                WHEN 'reb' THEN pgs.reb
                WHEN 'ast' THEN pgs.ast
            END AS actual
        FROM daily_predictions dp
        JOIN player_game_stats pgs
            ON dp.player_id = pgs.player_id
            AND dp.game_id = pgs.game_id
        WHERE dp.prediction_date >= :start_date
          AND dp.prediction_date <= :end_date
          AND pgs.did_not_play IS NOT TRUE
    """

    with engine.connect() as conn:
        from sqlalchemy import text

        df = pd.read_sql(
            text(query),
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )

    return _normalize(df, stats)


def _normalize(df: pd.DataFrame, stats: list[str] | None = None) -> pd.DataFrame:
    """Ensure required columns exist and filter to requested stats."""
    required = ["stat", "actual"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Filter to rows with actual values and requested stats
    df = df[df["actual"].notna()].copy()

    stats = stats or DEFAULT_STATS
    df["stat"] = df["stat"].str.lower()
    df = df[df["stat"].isin(stats)]

    if df.empty:
        raise ValueError("No valid rows after filtering. Check stat names and actual values.")

    return df


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_quantile_coverage(df: pd.DataFrame) -> dict:
    """Compute P(actual <= pred_q) for each quantile, per stat and global."""
    results = {}

    groups = list(df.groupby("stat")) + [("GLOBAL", df)]

    for label, group in groups:
        coverages = {}
        for q in QUANTILES:
            col = QUANTILE_COLS[q]
            if col not in group.columns:
                continue
            valid = group[group[col].notna()]
            if valid.empty:
                continue
            coverages[q] = float((valid["actual"] <= valid[col]).mean())

        if not coverages:
            continue

        gaps = {q: coverages[q] - q for q in coverages}
        abs_gaps = [abs(g) for g in gaps.values()]
        mean_abs_gap = float(np.mean(abs_gaps))
        worst_q = max(gaps, key=lambda q: abs(gaps[q]))

        results[label] = {
            "coverages": coverages,
            "gaps": gaps,
            "mean_abs_gap": mean_abs_gap,
            "worst_quantile": worst_q,
            "worst_gap": gaps[worst_q],
            "n": len(group),
        }

    return results


def compute_bias(df: pd.DataFrame) -> dict:
    """Compute mean predicted vs mean actual, per stat and global."""
    results = {}

    # Use pred_q50 (median) as the point prediction for bias
    pred_col = "pred_q50"
    if pred_col not in df.columns:
        return results

    groups = list(df.groupby("stat")) + [("GLOBAL", df)]

    for label, group in groups:
        valid = group[group[pred_col].notna() & group["actual"].notna()]
        if valid.empty:
            continue
        mean_pred = float(valid[pred_col].mean())
        mean_actual = float(valid["actual"].mean())
        bias = mean_pred - mean_actual
        rel_bias = bias / mean_actual * 100 if mean_actual != 0 else 0.0
        results[label] = {
            "mean_pred": mean_pred,
            "mean_actual": mean_actual,
            "bias": bias,
            "rel_bias_pct": rel_bias,
            "n": len(valid),
        }

    return results


def compute_sharpness(df: pd.DataFrame) -> dict:
    """Compute 80% and 50% interval widths, per stat and global."""
    results = {}

    groups = list(df.groupby("stat")) + [("GLOBAL", df)]

    for label, group in groups:
        entry = {"n": len(group)}

        # 80% interval: Q90 - Q10
        if "pred_q90" in group.columns and "pred_q10" in group.columns:
            valid = group[group["pred_q90"].notna() & group["pred_q10"].notna()]
            if not valid.empty:
                entry["width_80"] = float((valid["pred_q90"] - valid["pred_q10"]).mean())

        # 50% interval: Q75 - Q25
        if "pred_q75" in group.columns and "pred_q25" in group.columns:
            valid = group[group["pred_q75"].notna() & group["pred_q25"].notna()]
            if not valid.empty:
                entry["width_50"] = float((valid["pred_q75"] - valid["pred_q25"]).mean())

        if len(entry) > 1:  # has at least one width
            results[label] = entry

    return results


def compute_brier_score(df: pd.DataFrame) -> dict:
    """Compute Brier score: mean((over_prob - hit)^2), per stat and global."""
    results = {}
    if "over_prob" not in df.columns or "line" not in df.columns:
        return results

    valid = df[df["over_prob"].notna() & df["line"].notna() & df["actual"].notna()].copy()
    if valid.empty:
        return results

    valid["hit"] = (valid["actual"] > valid["line"]).astype(float)

    groups = list(valid.groupby("stat")) + [("GLOBAL", valid)]

    for label, group in groups:
        brier = float(((group["over_prob"] - group["hit"]) ** 2).mean())
        results[label] = {"brier": brier, "n": len(group)}

    return results


def compute_ece(df: pd.DataFrame, n_bins: int = 10) -> dict:
    """Compute Expected Calibration Error with binned probabilities."""
    results = {}
    if "over_prob" not in df.columns or "line" not in df.columns:
        return results

    valid = df[df["over_prob"].notna() & df["line"].notna() & df["actual"].notna()].copy()
    if valid.empty:
        return results

    valid["hit"] = (valid["actual"] > valid["line"]).astype(float)

    groups = list(valid.groupby("stat")) + [("GLOBAL", valid)]

    for label, group in groups:
        ece, curve = _ece_and_curve(group["over_prob"].values, group["hit"].values, n_bins)
        results[label] = {"ece": ece, "reliability_curve": curve, "n": len(group)}

    return results


def _ece_and_curve(
    probs: np.ndarray, hits: np.ndarray, n_bins: int
) -> tuple[float, list[dict]]:
    """Compute ECE and reliability curve data from arrays."""
    bin_edges = np.linspace(0, 1, n_bins + 1)
    curve = []
    weighted_gaps = 0.0
    total = len(probs)

    for i in range(n_bins):
        mask = (probs >= bin_edges[i]) & (probs < bin_edges[i + 1])
        if i == n_bins - 1:  # include right edge in last bin
            mask = mask | (probs == bin_edges[i + 1])

        count = int(mask.sum())
        if count == 0:
            continue

        pred_mean = float(probs[mask].mean())
        actual_rate = float(hits[mask].mean())
        curve.append({
            "bin_start": float(bin_edges[i]),
            "bin_end": float(bin_edges[i + 1]),
            "predicted_prob": pred_mean,
            "actual_rate": actual_rate,
            "count": count,
        })
        weighted_gaps += abs(actual_rate - pred_mean) * count

    ece = weighted_gaps / total if total > 0 else 0.0
    return float(ece), curve


# ---------------------------------------------------------------------------
# Diagnosis generator
# ---------------------------------------------------------------------------

def generate_diagnoses(
    quantile_results: dict,
    bias_results: dict,
    ece_results: dict,
    tolerance: float,
    bias_threshold: float = 5.0,
    ece_threshold: float = 0.05,
) -> list[str]:
    """Flag stats that exceed calibration tolerances."""
    diagnoses = []

    for stat in DEFAULT_STATS:
        # Quantile coverage issues
        qr = quantile_results.get(stat)
        if qr and qr["mean_abs_gap"] > tolerance:
            wq = qr["worst_quantile"]
            wg = qr["worst_gap"]
            q_label = f"Q{int(wq * 100)}"
            direction = "over-coverage" if wg > 0 else "under-coverage"
            diagnoses.append(
                f"{stat.upper()}: {q_label} {direction} ({wg:+.3f}) — "
                f"model {'over' if wg > 0 else 'under'}-predicts {stat}"
            )

        # Bias issues
        br = bias_results.get(stat)
        if br and abs(br["rel_bias_pct"]) > bias_threshold:
            direction = "upward" if br["rel_bias_pct"] > 0 else "downward"
            diagnoses.append(
                f"{stat.upper()}: Relative bias {br['rel_bias_pct']:+.1f}% — "
                f"systematic {direction} shift"
            )

        # ECE issues
        er = ece_results.get(stat)
        if er and er["ece"] > ece_threshold:
            diagnoses.append(
                f"{stat.upper()}: ECE {er['ece']:.3f} — probability calibration needs work"
            )

    return diagnoses


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(
    df: pd.DataFrame,
    quantile_results: dict,
    bias_results: dict,
    sharpness_results: dict,
    brier_results: dict,
    ece_results: dict,
    diagnoses: list[str],
    data_source: str,
    tolerance: float,
) -> str:
    """Format the full console report."""
    lines = []
    sep = "=" * 60

    # Header
    lines.append(sep)
    lines.append("PER-STAT CALIBRATION DIAGNOSTIC REPORT")
    lines.append(sep)
    lines.append(f"Data source: {data_source} | {len(df):,} predictions")

    # Date range
    date_col = _detect_date_col(df)
    if date_col:
        dates = pd.to_datetime(df[date_col])
        lines.append(f"Date range: {dates.min().date()} to {dates.max().date()}")

    # Per-stat counts
    stat_counts = df["stat"].value_counts()
    count_parts = [f"{s.upper()}: {stat_counts.get(s, 0):,}" for s in DEFAULT_STATS]
    lines.append(" | ".join(count_parts))
    lines.append(sep)

    # --- QUANTILE COVERAGE ---
    lines.append("")
    lines.append("--- QUANTILE COVERAGE ---")
    q_labels = [f"Q{int(q * 100)}" for q in QUANTILES]
    header = f"{'':>7}" + "".join(f"{ql:>8}" for ql in q_labels) + f"{'Mean|Gap|':>11}  Worst"
    lines.append(header)

    stat_order = [s for s in DEFAULT_STATS if s in quantile_results] + ["GLOBAL"]
    for stat in stat_order:
        qr = quantile_results.get(stat)
        if not qr:
            continue
        label = stat.upper() if stat != "GLOBAL" else "GLOBAL"
        parts = [f"{label:>7}"]
        for q in QUANTILES:
            cov = qr["coverages"].get(q)
            parts.append(f"{cov:8.3f}" if cov is not None else f"{'N/A':>8}")
        parts.append(f"{qr['mean_abs_gap']:9.3f}")
        # Worst quantile
        wq = qr["worst_quantile"]
        wg = qr["worst_gap"]
        flag = " \u26a0" if abs(wg) > tolerance else ""
        parts.append(f"  Q{int(wq * 100)} {wg:+.3f}{flag}")
        lines.append("".join(parts))

    # --- BIAS ---
    if bias_results:
        lines.append("")
        lines.append("--- BIAS ---")
        lines.append(f"{'':>7}{'Mean Pred':>11}{'Mean Actual':>13}{'Bias':>7}{'Rel Bias':>10}")
        for stat in stat_order:
            br = bias_results.get(stat)
            if not br:
                continue
            label = stat.upper() if stat != "GLOBAL" else "GLOBAL"
            flag = " \u26a0" if abs(br["rel_bias_pct"]) > 5.0 else ""
            lines.append(
                f"{label:>7}{br['mean_pred']:11.1f}{br['mean_actual']:13.1f}"
                f"{br['bias']:+7.1f}{br['rel_bias_pct']:+9.1f}%{flag}"
            )

    # --- INTERVAL SHARPNESS ---
    if sharpness_results:
        lines.append("")
        lines.append("--- INTERVAL SHARPNESS ---")
        has_80 = any("width_80" in v for v in sharpness_results.values())
        has_50 = any("width_50" in v for v in sharpness_results.values())
        header_parts = [f"{'':>7}"]
        if has_80:
            header_parts.append(f"{'80% Width':>11}")
        if has_50:
            header_parts.append(f"{'50% Width':>11}")
        lines.append("".join(header_parts))

        for stat in stat_order:
            sr = sharpness_results.get(stat)
            if not sr:
                continue
            label = stat.upper() if stat != "GLOBAL" else "GLOBAL"
            parts = [f"{label:>7}"]
            if has_80:
                w = sr.get("width_80")
                parts.append(f"{w:11.2f}" if w is not None else f"{'N/A':>11}")
            if has_50:
                w = sr.get("width_50")
                parts.append(f"{w:11.2f}" if w is not None else f"{'N/A':>11}")
            lines.append("".join(parts))

    # --- PROBABILITY CALIBRATION ---
    if brier_results or ece_results:
        lines.append("")
        lines.append("--- PROBABILITY CALIBRATION ---")
        lines.append(f"{'':>7}{'ECE':>8}{'Brier':>8}")
        for stat in stat_order:
            label = stat.upper() if stat != "GLOBAL" else "GLOBAL"
            ece_val = ece_results.get(stat, {}).get("ece")
            brier_val = brier_results.get(stat, {}).get("brier")
            ece_str = f"{ece_val:8.3f}" if ece_val is not None else f"{'N/A':>8}"
            brier_str = f"{brier_val:8.3f}" if brier_val is not None else f"{'N/A':>8}"
            # Flag high ECE or Brier
            flag = ""
            if ece_val is not None and ece_val > 0.05:
                flag = " \u26a0"
            elif brier_val is not None and brier_val > 0.25:
                flag = " \u26a0"
            lines.append(f"{label:>7}{ece_str}{brier_str}{flag}")

    # --- DIAGNOSIS ---
    lines.append("")
    lines.append(f"--- DIAGNOSIS (tolerance={tolerance}) ---")
    if diagnoses:
        for d in diagnoses:
            lines.append(f"\u2022 {d}")
    else:
        lines.append("All stats within tolerance. No issues detected.")

    lines.append("")
    return "\n".join(lines)


def _detect_date_col(df: pd.DataFrame) -> str | None:
    """Find the best date column in the DataFrame."""
    for col in ["game_date", "prediction_date", "date"]:
        if col in df.columns:
            return col
    return None


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------

def build_json_report(
    df: pd.DataFrame,
    quantile_results: dict,
    bias_results: dict,
    sharpness_results: dict,
    brier_results: dict,
    ece_results: dict,
    diagnoses: list[str],
    data_source: str,
    tolerance: float,
) -> dict:
    """Build a structured JSON-serialisable report."""
    date_col = _detect_date_col(df)
    date_range = None
    if date_col:
        dates = pd.to_datetime(df[date_col])
        date_range = {"start": str(dates.min().date()), "end": str(dates.max().date())}

    return {
        "data_source": data_source,
        "n_predictions": len(df),
        "date_range": date_range,
        "stat_counts": {s: int(c) for s, c in df["stat"].value_counts().items()},
        "tolerance": tolerance,
        "quantile_coverage": quantile_results,
        "bias": bias_results,
        "sharpness": sharpness_results,
        "brier": brier_results,
        "ece": ece_results,
        "diagnoses": diagnoses,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Per-stat calibration diagnostic report",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--csv", type=str, help="Path to backtest predictions CSV")
    source.add_argument("--db", action="store_true", help="Load from production DB")

    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD) for DB mode")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD) for DB mode")
    parser.add_argument("--output", type=str, help="Export report to JSON file")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.03,
        help="Quantile coverage gap tolerance (default: 0.03)",
    )
    parser.add_argument(
        "--stats",
        nargs="+",
        default=None,
        help="Stats to analyse (default: pts reb ast)",
    )

    args = parser.parse_args()

    # Load data
    if args.csv:
        data_source = Path(args.csv).name
        logger.info(f"Loading predictions from {args.csv}")
        df = load_from_csv(args.csv, stats=args.stats)
    else:
        if not args.start or not args.end:
            parser.error("--db requires --start and --end dates")
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        data_source = f"DB ({args.start} to {args.end})"
        logger.info(f"Loading predictions from DB: {start} to {end}")
        df = load_from_db(start, end, stats=args.stats)

    logger.info(f"Loaded {len(df):,} predictions across {df['stat'].nunique()} stats")

    # Compute metrics
    quantile_results = compute_quantile_coverage(df)
    bias_results = compute_bias(df)
    sharpness_results = compute_sharpness(df)
    brier_results = compute_brier_score(df)
    ece_results = compute_ece(df)

    # Generate diagnoses
    diagnoses = generate_diagnoses(
        quantile_results, bias_results, ece_results, tolerance=args.tolerance
    )

    # Console report
    report = format_report(
        df,
        quantile_results,
        bias_results,
        sharpness_results,
        brier_results,
        ece_results,
        diagnoses,
        data_source,
        args.tolerance,
    )
    print(report)

    # JSON export
    if args.output:
        json_report = build_json_report(
            df,
            quantile_results,
            bias_results,
            sharpness_results,
            brier_results,
            ece_results,
            diagnoses,
            data_source,
            args.tolerance,
        )
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(json_report, f, indent=2, default=str)
        logger.info(f"JSON report saved to {args.output}")


if __name__ == "__main__":
    main()
