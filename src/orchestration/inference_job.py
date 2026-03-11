#!/usr/bin/env python3
"""
Inference Job - Generate Daily Predictions
===========================================
Run once daily (recommended: 6:30 PM ET) after lines_job, before games start.

This job:
1. Loads trained model artifacts
2. Generates predictions for today's games
3. Stores predictions to database
4. Exports CSV backup

Usage:
    python src/orchestration/inference_job.py [--date YYYY-MM-DD] [--dry-run] [--model-dir PATH]

Examples:
    # Normal run for today
    python src/orchestration/inference_job.py

    # Run for specific date
    python src/orchestration/inference_job.py --date 2026-02-05

    # Use specific model artifacts
    python src/orchestration/inference_job.py --model-dir src/models/artifacts/run_20260131_112534

    # Dry run (load models but don't store)
    python src/orchestration/inference_job.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configure logging
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "inference.log"),
    ],
)
logger = logging.getLogger("InferenceJob")


def main():
    parser = argparse.ArgumentParser(
        description="Inference Job - Generate Daily Predictions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate predictions but don't store to database",
    )
    parser.add_argument(
        "--model-dir",
        type=str,
        default="src/models/artifacts",
        help="Path to model artifacts directory",
    )
    parser.add_argument(
        "--stats",
        type=str,
        nargs="+",
        default=["pts", "reb", "ast"],
        help="Stats to predict (default: pts reb ast)",
    )
    parser.add_argument(
        "--skip-discord",
        action="store_true",
        help="Skip sending Discord alert after predictions",
    )
    parser.add_argument(
        "--skip-bets",
        action="store_true",
        help="Skip automatic paper bet placement",
    )
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"INFERENCE JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {target_date}")
    logger.info(f"Stats: {args.stats}")
    logger.info("=" * 60)

    try:
        # Import here to avoid slow imports when just checking --help
        from sqlalchemy import text

        from src.db.client import get_engine
        from src.models.daily_runner import DailyPredictionRunner
        from src.models.feature_store import FeatureStore
        from src.models.monte_carlo import MonteCarloPredictor, load_combined_calibration_offsets, load_copula_params
        from src.models.prediction_store import PredictionStore
        from src.models.quantile_trainer import PlayerPropsModelPipeline

        # Find model artifacts
        artifacts_path = Path(args.model_dir)
        if not artifacts_path.exists():
            raise FileNotFoundError(f"Model artifacts directory not found: {artifacts_path}")

        # Check if artifacts_path contains models directly or has run_* subdirs
        if (artifacts_path / "minutes_model.joblib").exists():
            # Direct path to model artifacts (e.g., --model-dir src/models/artifacts/run_xxx)
            model_path = artifacts_path
        elif (artifacts_path / "production" / "minutes_model.joblib").exists():
            # Use production model (preferred for Railway/deployed environments)
            model_path = artifacts_path / "production"
        else:
            # Fall back to latest run_* directory (local development)
            # Filter out _incomplete directories (training in progress)
            runs = sorted([
                d for d in artifacts_path.iterdir()
                if d.is_dir()
                and d.name.startswith("run_")
                and not d.name.endswith("_incomplete")
            ])
            if not runs:
                raise FileNotFoundError(
                    f"No model found. Checked:\n"
                    f"  - {artifacts_path}/minutes_model.joblib\n"
                    f"  - {artifacts_path}/production/\n"
                    f"  - {artifacts_path}/run_*/\n"
                    "Run training first or promote a model to production."
                )
            model_path = runs[-1]

        logger.info(f"Using model artifacts: {model_path.name}")

        # Initialize components
        logger.info("Initializing database connection...")
        engine = get_engine()
        feature_store = FeatureStore(engine)

        logger.info("Loading model pipeline...")
        pipeline = PlayerPropsModelPipeline.load_all(str(model_path), feature_store)

        logger.info("Initializing Monte Carlo predictor (10,000 samples)...")
        copula_params = load_copula_params(str(model_path))
        if copula_params:
            logger.info("Loaded Gaussian copula params for correlated sampling")
        combined_cal_offsets = load_combined_calibration_offsets(str(model_path))
        if combined_cal_offsets:
            logger.info(f"Loaded combined calibration offsets for: {list(combined_cal_offsets.keys())}")
        predictor = MonteCarloPredictor(
            pipeline, n_samples=10000, copula_params=copula_params,
            combined_calibration_offsets=combined_cal_offsets,
        )

        # Check upstream data freshness — warn if rolling averages are stale
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT MAX(game_date)::date FROM player_average_game_stats"
                )).scalar()
                if result:
                    days_stale = (target_date - result).days
                    if days_stale > 2:
                        logger.warning(
                            f"Rolling averages may be stale! Latest game_date in "
                            f"player_average_game_stats: {result} ({days_stale} days ago). "
                            f"Check if daily_stats_job ran successfully."
                        )
                    else:
                        logger.info(f"Rolling averages up to date (latest: {result})")
                else:
                    logger.warning("No data found in player_average_game_stats — daily_stats_job may not have run")
        except Exception as e:
            logger.warning(f"Could not check data freshness: {e}")

        # Create runner and generate predictions
        runner = DailyPredictionRunner(engine, feature_store, pipeline, predictor)

        logger.info(f"Generating predictions for {target_date}...")
        preds, samples = runner.run_for_date(target_date, stats=args.stats)

        if preds.empty:
            logger.warning("No predictions generated (no games or no data)")
            elapsed = time.time() - start_time
            logger.info(f"INFERENCE JOB COMPLETED ({elapsed:.1f}s) - No predictions")
            return

        logger.info(f"Generated {len(preds)} predictions")

        # Show summary
        if "over_edge" in preds.columns:
            pos_edge = preds[preds["over_edge"] > 0.05]
            neg_edge = preds[preds["under_edge"] > 0.05]
            logger.info(f"  Positive over edges (>5%): {len(pos_edge)}")
            logger.info(f"  Positive under edges (>5%): {len(neg_edge)}")

        # Store to database
        if not args.dry_run:
            logger.info("Storing predictions to database...")
            store = PredictionStore(engine)
            store.store_predictions(preds, target_date)
            store.store_samples(samples, target_date)
            logger.info(f"Stored {len(preds)} predictions + {len(samples)} sample arrays")

        # Export CSV backup
        output_dir = Path("predictions")
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"predictions_{target_date}.csv"
        preds.to_csv(output_file, index=False)
        logger.info(f"Exported CSV: {output_file}")

        # Place paper bets on recommended predictions
        if not args.dry_run and not args.skip_bets:
            try:
                from src.paper_trading.paper_trader import PaperTrader

                logger.info("Placing paper bets on recommended predictions...")
                trader = PaperTrader()
                bets = trader.select_bets(target_date)
                if bets:
                    count = trader.place_bets(bets)
                    logger.info(f"Placed {count} paper bets for {target_date}")
                else:
                    logger.info("No predictions meet edge threshold for paper bets")
            except Exception as e:
                logger.warning(f"Paper bet placement failed: {e} (non-fatal)")

        # Send Discord alert
        if not args.dry_run and not args.skip_discord:
            try:
                import os
                if os.getenv("DISCORD_BOT_TOKEN"):
                    from src.discord_bot.alerts import send_predictions_alert_sync
                    logger.info("Sending Discord alert...")
                    success = send_predictions_alert_sync(preds, target_date)
                    if success:
                        logger.info("Discord alert sent successfully")
                    else:
                        logger.warning("Discord alert failed (non-fatal)")
                else:
                    logger.debug("Discord not configured, skipping alert")
            except Exception as e:
                logger.warning(f"Discord alert failed: {e} (non-fatal)")

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"INFERENCE JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
        logger.info(f"  Predictions: {len(preds)}")
        logger.info(f"  Output: {output_file}")
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("=" * 60)
        logger.error(f"INFERENCE JOB FAILED ({elapsed:.1f}s)")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
