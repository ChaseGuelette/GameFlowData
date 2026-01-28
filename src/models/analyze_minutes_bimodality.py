"""
Analyze minutes distribution bimodality in blowout vs close games.

Tests whether starters' minutes in high-spread games follow a bimodal distribution
(full game ~32min vs pulled early ~24min) or a unimodal distribution shifted down.

Usage:
    python src/models/analyze_minutes_bimodality.py
    python src/models/analyze_minutes_bimodality.py --model-dir src/models/artifacts/run_20260126_180535
    python src/models/analyze_minutes_bimodality.py --seasons 22024 --output results.json
    python src/models/analyze_minutes_bimodality.py --min-starter-avg 24
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.feature_store import FeatureStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- Distribution Analysis ---


def bimodality_coefficient(data: np.ndarray) -> float:
    """
    Compute the Bimodality Coefficient (BC).

    BC = (skewness^2 + 1) / (kurtosis + 3)

    Interpretation:
        BC > 5/9 (~0.555) suggests bimodality.
        The uniform distribution has BC = 5/9 (the boundary).
        A normal distribution has BC = 1/3 (~0.333).
    """
    skew = scipy_stats.skew(data)
    kurt = scipy_stats.kurtosis(data, fisher=True)  # excess kurtosis
    return float((skew**2 + 1) / (kurt + 3))


def run_dip_test(data: np.ndarray) -> dict | None:
    """Run Hartigan's dip test if the diptest package is installed."""
    try:
        import diptest

        dip_stat, p_value = diptest.diptest(np.asarray(data, dtype=np.float64))
        return {"dip_statistic": round(float(dip_stat), 5), "p_value": round(float(p_value), 5)}
    except ImportError:
        return None


def compute_histogram(data: np.ndarray, bin_width: int = 2) -> dict:
    """Compute histogram with fixed-width bins."""
    bins = list(range(0, 52, bin_width))
    counts, edges = np.histogram(data, bins=bins)
    return {
        "bin_edges": [float(e) for e in edges],
        "counts": [int(c) for c in counts],
    }


def analyze_segment(minutes: np.ndarray, label: str) -> dict:
    """Compute full distribution analysis for a minutes segment."""
    result = {
        "label": label,
        "n": len(minutes),
        "mean": round(float(np.mean(minutes)), 2),
        "std": round(float(np.std(minutes)), 2),
        "median": round(float(np.median(minutes)), 2),
        "q10": round(float(np.percentile(minutes, 10)), 2),
        "q25": round(float(np.percentile(minutes, 25)), 2),
        "q75": round(float(np.percentile(minutes, 75)), 2),
        "q90": round(float(np.percentile(minutes, 90)), 2),
        "skewness": round(float(scipy_stats.skew(minutes)), 4),
        "kurtosis": round(float(scipy_stats.kurtosis(minutes, fisher=True)), 4),
        "bimodality_coefficient": round(bimodality_coefficient(minutes), 4),
        "histogram": compute_histogram(minutes),
    }

    dip = run_dip_test(minutes)
    if dip:
        result["dip_test"] = dip

    return result


def print_histogram(hist: dict) -> None:
    """Print ASCII histogram to logger."""
    counts = hist["counts"]
    edges = hist["bin_edges"]
    max_count = max(counts) if counts else 1
    bar_width = 40

    for i, count in enumerate(counts):
        if count == 0:
            continue
        bar_len = int(count / max_count * bar_width)
        bar = "#" * bar_len
        logger.info(f"  {edges[i]:4.0f}-{edges[i + 1]:4.0f} | {bar} ({count:,})")


def print_segment_summary(result: dict) -> None:
    """Print a segment's analysis to terminal."""
    bc = result["bimodality_coefficient"]
    bc_verdict = "BIMODAL" if bc > 0.555 else "UNIMODAL"

    logger.info(f"  N:      {result['n']:,}")
    logger.info(f"  Mean:   {result['mean']:.1f} min  (std={result['std']:.1f})")
    logger.info(
        f"  Q10={result['q10']:.1f}  Q25={result['q25']:.1f}  "
        f"Q50={result['median']:.1f}  Q75={result['q75']:.1f}  Q90={result['q90']:.1f}"
    )
    logger.info(f"  Skew:   {result['skewness']:.3f}")
    logger.info(f"  Kurt:   {result['kurtosis']:.3f}")
    logger.info(f"  BC:     {bc:.3f}  -> {bc_verdict}")

    if "dip_test" in result:
        dip = result["dip_test"]
        dip_verdict = "BIMODAL" if dip["p_value"] < 0.05 else "UNIMODAL"
        logger.info(f"  Dip:    stat={dip['dip_statistic']:.4f}  p={dip['p_value']:.4f}  -> {dip_verdict}")


# --- Core Analysis ---


SPREAD_SEGMENTS = {
    "close": {"min": 0, "max": 5, "label": "|spread| <= 5"},
    "moderate": {"min": 5, "max": 10, "label": "5 < |spread| <= 10"},
    "blowout": {"min": 10, "max": 99, "label": "|spread| > 10"},
    "extreme_blowout": {"min": 15, "max": 99, "label": "|spread| > 15"},
}


def analyze_bimodality(df: pd.DataFrame, min_starter_avg: float = 20.0) -> dict:
    """
    Main analysis: compare minutes distributions across spread segments.

    Args:
        df: Training dataset with actual_minutes and line_spread columns
        min_starter_avg: Minimum average minutes to classify as a starter

    Returns:
        Dict with analysis results per segment
    """
    # Filter to players with enough minutes data
    valid = df[df["actual_minutes"] > 0].copy()
    logger.info(f"Total game rows with minutes > 0: {len(valid):,}")

    # Identify starters by average minutes
    player_avg = valid.groupby("player_id")["actual_minutes"].agg(["mean", "count"])
    starter_ids = player_avg[(player_avg["mean"] >= min_starter_avg) & (player_avg["count"] >= 10)].index
    starters = valid[valid["player_id"].isin(starter_ids)].copy()

    logger.info(
        f"Starters (avg >= {min_starter_avg} min, 10+ games): {len(starter_ids):,} players, {len(starters):,} games"
    )

    if "line_spread" not in starters.columns:
        logger.error("line_spread column not found in dataset. Cannot segment by spread.")
        return {}

    # Drop rows without spread data
    starters = starters[starters["line_spread"].notna()].copy()
    starters["abs_spread"] = starters["line_spread"].abs()
    logger.info(f"Games with spread data: {len(starters):,}")

    # Analyze each segment
    logger.info("\n" + "=" * 60)
    logger.info("MINUTES DISTRIBUTION BY SPREAD SEGMENT")
    logger.info("=" * 60)

    results = {}
    for seg_name, seg_def in SPREAD_SEGMENTS.items():
        seg_df = starters[(starters["abs_spread"] > seg_def["min"]) & (starters["abs_spread"] <= seg_def["max"])]
        # For "close" segment, include spread == 0
        if seg_def["min"] == 0:
            seg_df = starters[starters["abs_spread"] <= seg_def["max"]]

        if len(seg_df) < 50:
            logger.warning(f"Skipping {seg_name}: only {len(seg_df)} samples")
            continue

        minutes = seg_df["actual_minutes"].values
        result = analyze_segment(minutes, seg_name)
        results[seg_name] = result

        logger.info(f"\n--- {seg_name.upper()} ({seg_def['label']}) ---")
        print_segment_summary(result)
        logger.info("  Distribution:")
        print_histogram(result["histogram"])

    # Cross-segment comparison
    if "close" in results and "blowout" in results:
        logger.info("\n" + "=" * 60)
        logger.info("CROSS-SEGMENT COMPARISON (Close vs Blowout)")
        logger.info("=" * 60)

        close = results["close"]
        blowout = results["blowout"]

        mean_diff = close["mean"] - blowout["mean"]
        std_diff = blowout["std"] - close["std"]
        q25_diff = close["q25"] - blowout["q25"]
        q50_diff = close["median"] - blowout["median"]

        logger.info(f"  Mean minutes diff:    {mean_diff:+.1f} min (close higher)")
        logger.info(f"  Std dev diff:         {std_diff:+.1f} (blowout {'wider' if std_diff > 0 else 'narrower'})")
        logger.info(f"  Q25 diff:             {q25_diff:+.1f} min")
        logger.info(f"  Q50 diff:             {q50_diff:+.1f} min")
        logger.info(f"  BC close:             {close['bimodality_coefficient']:.3f}")
        logger.info(f"  BC blowout:           {blowout['bimodality_coefficient']:.3f}")

        bc_increase = blowout["bimodality_coefficient"] - close["bimodality_coefficient"]
        if bc_increase > 0.05:
            logger.info(f"  -> Blowout BC is {bc_increase:.3f} higher than close - evidence of bimodality emerging")
        else:
            logger.info(f"  -> BC difference is small ({bc_increase:+.3f}) - distribution shape is similar")

    return results


# --- Model Comparison ---


def compare_model_vs_actuals(df: pd.DataFrame, model_dir: str) -> dict:
    """
    Compare the minutes model's predicted quantiles vs actuals,
    segmented by spread.

    Answers: Does the model capture blowout effects, or does it just shift the mean?
    """
    from src.models.quantile_trainer import PlayerPropsModelPipeline

    logger.info("\n" + "=" * 60)
    logger.info("MODEL MINUTES PREDICTIONS VS ACTUALS BY SPREAD")
    logger.info("=" * 60)

    engine = get_engine()
    feature_store = FeatureStore(engine)
    pipeline = PlayerPropsModelPipeline.load_all(model_dir, feature_store)

    if pipeline.minutes_model is None:
        logger.error("No minutes model found in artifacts.")
        return {}

    # Get feature columns the model expects
    feature_names = pipeline.minutes_model.all_feature_names

    # Filter to rows that have all needed features
    valid = df.dropna(subset=["actual_minutes", "line_spread"]).copy()
    missing_features = [f for f in feature_names if f not in valid.columns]

    if missing_features:
        logger.warning(f"Missing features (will use 0): {missing_features}")

    # Prepare feature matrix
    X = pd.DataFrame()
    for f in feature_names:
        if f in valid.columns:
            X[f] = valid[f].values
        else:
            X[f] = 0.0
    X = X.fillna(0).astype(np.float32)

    # Predict quantiles (batch)
    logger.info(f"Predicting minutes quantiles for {len(X):,} games...")
    pred_quantiles = pipeline.minutes_model.predict_quantiles(X)
    # pred_quantiles columns: q10, q25, q50, q75, q90

    valid = valid.reset_index(drop=True)
    valid["pred_q10"] = pred_quantiles["q10"].values
    valid["pred_q25"] = pred_quantiles["q25"].values
    valid["pred_q50"] = pred_quantiles["q50"].values
    valid["pred_q75"] = pred_quantiles["q75"].values
    valid["pred_q90"] = pred_quantiles["q90"].values
    valid["abs_spread"] = valid["line_spread"].abs()

    # Compute calibration per segment
    quantile_targets = {"q10": 0.10, "q25": 0.25, "q50": 0.50, "q75": 0.75, "q90": 0.90}

    results = {}
    for seg_name, seg_def in SPREAD_SEGMENTS.items():
        if seg_def["min"] == 0:
            seg_df = valid[valid["abs_spread"] <= seg_def["max"]]
        else:
            seg_df = valid[(valid["abs_spread"] > seg_def["min"]) & (valid["abs_spread"] <= seg_def["max"])]

        if len(seg_df) < 50:
            continue

        seg_results = {"n": len(seg_df), "label": seg_def["label"]}
        logger.info(f"\n--- {seg_name.upper()} ({seg_def['label']}, n={len(seg_df):,}) ---")

        for q_col, target in quantile_targets.items():
            pred_col = f"pred_{q_col}"
            actual_below = (seg_df["actual_minutes"] < seg_df[pred_col]).mean()
            gap = actual_below - target

            status = "OK" if abs(gap) < 0.05 else "DRIFT" if abs(gap) < 0.10 else "BAD"
            seg_results[q_col] = {
                "target": target,
                "actual_coverage": round(float(actual_below), 4),
                "gap": round(float(gap), 4),
                "status": status,
            }

            logger.info(
                f"  {q_col.upper()}: target={target:.0%}  actual={actual_below:.1%}  gap={gap:+.1%}  [{status}]"
            )

        # Mean prediction error
        mean_pred = seg_df["pred_q50"].mean()
        mean_actual = seg_df["actual_minutes"].mean()
        bias = mean_pred - mean_actual
        seg_results["median_prediction_bias"] = round(float(bias), 2)
        logger.info(f"  Median pred mean: {mean_pred:.1f}  Actual mean: {mean_actual:.1f}  Bias: {bias:+.1f} min")

        # Residual std (how scattered are prediction errors?)
        residuals = seg_df["pred_q50"].values - seg_df["actual_minutes"].values
        seg_results["residual_std"] = round(float(np.std(residuals)), 2)
        logger.info(f"  Residual std: {np.std(residuals):.1f} min")

        results[seg_name] = seg_results

    # Summary verdict
    if "close" in results and "blowout" in results:
        logger.info("\n--- VERDICT ---")
        close_q50_gap = results["close"]["q50"]["gap"]
        blowout_q50_gap = results["blowout"]["q50"]["gap"]
        drift = blowout_q50_gap - close_q50_gap

        if abs(drift) < 0.03:
            logger.info("  Model handles blowouts similarly to close games - no additional correction needed.")
        elif drift > 0:
            logger.info(
                f"  Model OVER-predicts minutes in blowouts by {drift:.1%} more than close games."
                " -> Consider a blowout mixture or conditional adjustment."
            )
        else:
            logger.info(
                f"  Model UNDER-predicts minutes in blowouts by {abs(drift):.1%} more than close games."
                " -> Model may be overcorrecting for spread."
            )

    return results


# --- Entry Point ---


def main():
    parser = argparse.ArgumentParser(description="Analyze minutes distribution bimodality across spread segments")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=["22024"],
        help="Season IDs to analyze (default: 22024 = 2024-25)",
    )
    parser.add_argument(
        "--model-dir",
        type=str,
        default=None,
        help="Path to model artifacts for prediction comparison (optional)",
    )
    parser.add_argument(
        "--min-starter-avg",
        type=float,
        default=20.0,
        help="Minimum average minutes to classify as starter (default: 20)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Save results to JSON file",
    )

    args = parser.parse_args()

    # Load data
    engine = get_engine()
    feature_store = FeatureStore(engine)

    logger.info(f"Loading training data for seasons: {args.seasons}")
    df = feature_store.get_training_dataset(args.seasons)
    logger.info(f"Loaded {len(df):,} rows")

    # Run distribution analysis
    distribution_results = analyze_bimodality(df, min_starter_avg=args.min_starter_avg)

    # Run model comparison if artifacts provided
    model_results = {}
    if args.model_dir:
        model_results = compare_model_vs_actuals(df, args.model_dir)

    # Save results
    if args.output:
        output = {
            "distribution_analysis": distribution_results,
            "model_comparison": model_results,
            "config": {
                "seasons": args.seasons,
                "min_starter_avg": args.min_starter_avg,
                "model_dir": args.model_dir,
            },
        }
        # Strip histogram data from JSON (too verbose)
        for seg in output["distribution_analysis"].values():
            seg.pop("histogram", None)

        with open(args.output, "w") as f:
            json.dump(output, f, indent=2, default=str)
        logger.info(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
