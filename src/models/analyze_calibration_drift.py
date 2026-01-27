"""
Analyze calibration drift between individual models and combined predictions.

This script investigates why backtest calibration differs from training calibration:
1. Minutes-Rate correlation analysis
2. Combined calibration check (minutes × rate vs actual totals)
3. Bias analysis by player minutes bucket

Usage:
    python -m src.models.analyze_calibration_drift --model-path src/models/artifacts/run_XXXXXXXX
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor
from src.models.quantile_trainer import PlayerPropsModelPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def analyze_minutes_rate_correlation(df: pd.DataFrame) -> dict:
    """Analyze correlation between actual minutes and per-minute rates."""
    logger.info("\n" + "=" * 60)
    logger.info("MINUTES-RATE CORRELATION ANALYSIS")
    logger.info("=" * 60)

    valid_mask = df["actual_minutes"] >= 10
    analysis_df = df[valid_mask].copy()

    results = {}

    for stat in ["pts", "reb", "ast"]:
        rate_col = f"{stat}_per_min"
        actual_col = f"actual_{stat}"

        if rate_col not in analysis_df.columns:
            # Compute rate if not present
            if actual_col in analysis_df.columns:
                analysis_df[rate_col] = analysis_df[actual_col] / analysis_df["actual_minutes"]
            else:
                continue

        # Overall correlation
        corr = analysis_df["actual_minutes"].corr(analysis_df[rate_col])
        results[stat] = {"overall_correlation": float(corr)}

        logger.info(f"\n{stat.upper()}:")
        logger.info(f"  Overall minutes-rate correlation: {corr:.3f}")

        # By minutes bucket
        analysis_df["min_bucket"] = pd.cut(
            analysis_df["actual_minutes"],
            bins=[0, 15, 20, 25, 30, 35, 40, 50],
            labels=["10-15", "15-20", "20-25", "25-30", "30-35", "35-40", "40+"],
        )

        bucket_data = []
        logger.info(f"  By minutes bucket:")

        for bucket in ["10-15", "15-20", "20-25", "25-30", "30-35", "35-40", "40+"]:
            bucket_df = analysis_df[analysis_df["min_bucket"] == bucket]
            if len(bucket_df) < 30:
                continue

            mean_rate = bucket_df[rate_col].mean()
            std_rate = bucket_df[rate_col].std()
            mean_total = bucket_df[actual_col].mean() if actual_col in bucket_df.columns else 0

            bucket_data.append({
                "bucket": bucket,
                "n": len(bucket_df),
                "mean_rate": float(mean_rate),
                "std_rate": float(std_rate),
                "mean_total": float(mean_total),
            })

            logger.info(
                f"    {bucket:>6} min: n={len(bucket_df):>5}, "
                f"mean_rate={mean_rate:.3f}, std={std_rate:.3f}, mean_total={mean_total:.1f}"
            )

        results[stat]["by_bucket"] = bucket_data

        # Check for systematic trend
        if len(bucket_data) >= 3:
            rates = [b["mean_rate"] for b in bucket_data]
            trend = rates[-1] - rates[0]
            if abs(trend) > 0.02:
                direction = "INCREASES" if trend > 0 else "DECREASES"
                logger.warning(
                    f"  ⚠ {stat.upper()} rate {direction} with minutes! "
                    f"({rates[0]:.3f} → {rates[-1]:.3f}, delta={trend:+.3f})"
                )
                logger.warning(
                    f"    This causes bias when multiplying predicted minutes × predicted rate"
                )

    return results


def analyze_combined_calibration(
    pipeline: PlayerPropsModelPipeline,
    df: pd.DataFrame,
    sample_size: int = 2000,
) -> dict:
    """Evaluate Monte Carlo (minutes × rate) calibration against actual totals."""
    logger.info("\n" + "=" * 60)
    logger.info("COMBINED CALIBRATION ANALYSIS (Minutes × Rate → Total)")
    logger.info("=" * 60)

    # Filter valid rows (use actual_pts, actual_reb, actual_ast column names)
    valid_mask = df["actual_minutes"] >= 10
    for stat in ["pts", "reb", "ast"]:
        actual_col = f"actual_{stat}"
        if actual_col in df.columns:
            valid_mask &= df[actual_col].notna()

    eval_df = df[valid_mask].copy()

    if len(eval_df) > sample_size:
        eval_df = eval_df.sample(sample_size, random_state=42)

    logger.info(f"Evaluating {len(eval_df)} samples...")

    mc = MonteCarloPredictor(pipeline, n_samples=1000, random_state=42)

    results = {stat: {"predictions": [], "actuals": []} for stat in ["pts", "reb", "ast"]}

    # Progress tracking
    total = len(eval_df)
    for i, (idx, row) in enumerate(eval_df.iterrows()):
        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i + 1}/{total}")

        # Build features
        features = {}
        for col in pipeline.minutes_model.all_feature_names:
            if col in row.index:
                features[col] = row[col]

        for stat in ["pts", "reb", "ast"]:
            if stat in pipeline.rate_models:
                for feat in pipeline.rate_models[stat].all_feature_names:
                    if feat in row.index:
                        features[feat] = row[feat]

        try:
            preds = mc.predict(
                player_id=int(row["player_id"]),
                game_id=str(row["game_id"]),
                features=features,
                stats=["pts", "reb", "ast"],
            )

            for stat in ["pts", "reb", "ast"]:
                actual_col = f"actual_{stat}"
                if actual_col in df.columns:
                    results[stat]["predictions"].append({
                        "q10": preds[stat].q10,
                        "q25": preds[stat].q25,
                        "q50": preds[stat].q50,
                        "q75": preds[stat].q75,
                        "q90": preds[stat].q90,
                        "mean": preds[stat].mean,
                    })
                    results[stat]["actuals"].append(row[actual_col])
        except Exception as e:
            continue

    # Calculate calibration
    reports = {}
    quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]

    for stat in ["pts", "reb", "ast"]:
        if not results[stat]["predictions"]:
            continue

        actuals = np.array(results[stat]["actuals"])
        preds_df = pd.DataFrame(results[stat]["predictions"])

        logger.info(f"\n{stat.upper()} Combined Calibration:")
        stat_reports = []

        for q in quantiles:
            pred_col = f"q{int(q * 100):02d}"
            coverage = (actuals <= preds_df[pred_col].values).mean()
            gap = coverage - q

            status = "OK" if abs(gap) <= 0.05 else f"⚠ GAP"
            logger.info(f"  Q{q:.2f}: coverage={coverage:.3f}, target={q:.2f}, gap={gap:+.3f} [{status}]")

            stat_reports.append({
                "quantile": q,
                "coverage": float(coverage),
                "target": q,
                "gap": float(gap),
            })

        reports[stat] = stat_reports

        # Summary metrics
        mean_pred = preds_df["mean"].mean()
        mean_actual = actuals.mean()
        bias = mean_pred - mean_actual
        logger.info(f"  Mean predicted: {mean_pred:.2f}, Mean actual: {mean_actual:.2f}, Bias: {bias:+.2f}")

    # Overall summary
    all_gaps = [r["gap"] for stat_reports in reports.values() for r in stat_reports]
    if all_gaps:
        worst_gap = max(abs(g) for g in all_gaps)
        mean_abs_gap = np.mean([abs(g) for g in all_gaps])

        logger.info(f"\n{'=' * 40}")
        logger.info(f"SUMMARY: worst_gap={worst_gap:.3f}, mean_abs_gap={mean_abs_gap:.3f}")

        if worst_gap > 0.05:
            logger.warning(
                "\n⚠ CALIBRATION DRIFT DETECTED!"
                "\n  Individual models (minutes, rate) may be well-calibrated,"
                "\n  but combined prediction (minutes × rate) shows drift."
                "\n  Possible causes:"
                "\n    1. Minutes-rate correlation not captured"
                "\n    2. Variance underestimation in multiplication"
                "\n    3. Sample selection differences between training and evaluation"
            )

    return reports


def main():
    parser = argparse.ArgumentParser(description="Analyze calibration drift")
    parser.add_argument(
        "--model-path",
        type=str,
        required=True,
        help="Path to model artifacts directory",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=["22024", "22025"],
        help="Seasons to analyze",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=2000,
        help="Number of samples for combined calibration",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file for results",
    )

    args = parser.parse_args()

    # Load model
    logger.info(f"Loading model from {args.model_path}")
    engine = get_engine()
    feature_store = FeatureStore(engine)
    pipeline = PlayerPropsModelPipeline.load_all(args.model_path, feature_store)

    # Load data
    logger.info(f"Loading data for seasons: {args.seasons}")
    df = feature_store.get_training_dataset(args.seasons)
    logger.info(f"Loaded {len(df):,} rows")

    # Ensure rate columns exist
    for stat in ["pts", "reb", "ast"]:
        rate_col = f"{stat}_per_min"
        if rate_col not in df.columns and stat in df.columns:
            df[rate_col] = df[stat] / df["actual_minutes"].replace(0, np.nan)

    # Run analyses
    correlation_results = analyze_minutes_rate_correlation(df)
    calibration_results = analyze_combined_calibration(pipeline, df, args.sample_size)

    # Save results
    if args.output:
        output = {
            "correlation_analysis": correlation_results,
            "combined_calibration": calibration_results,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        logger.info(f"\nResults saved to {args.output}")

    logger.info("\nAnalysis complete!")


if __name__ == "__main__":
    main()
