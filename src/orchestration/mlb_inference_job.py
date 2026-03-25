#!/usr/bin/env python3
"""
MLB Inference Job - Generate Daily Predictions
===============================================
Run after MLB lines scrape, before games start.

This job:
1. Loads trained MLB model artifacts
2. Generates predictions for today's games
3. Stores predictions to database
4. Places paper bets
5. Exports CSV backup

Usage:
    python src/orchestration/mlb_inference_job.py [--date YYYY-MM-DD] [--dry-run] [--model-dir PATH]

Examples:
    # Normal run for today
    python src/orchestration/mlb_inference_job.py

    # Run for specific date
    python src/orchestration/mlb_inference_job.py --date 2024-09-15

    # Dry run (load models but don't store)
    python src/orchestration/mlb_inference_job.py --dry-run

    # Use specific model artifacts
    python src/orchestration/mlb_inference_job.py --model-dir src/models/mlb/artifacts/run_pitcher_k_YYYYMMDD
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
        logging.FileHandler(LOG_DIR / "mlb_inference.log"),
    ],
)
logger = logging.getLogger("MLBInferenceJob")


def main():
    parser = argparse.ArgumentParser(
        description="MLB Inference Job - Generate Daily Predictions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date", type=str, default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't store to database")
    parser.add_argument(
        "--model-dir", type=str, default="src/models/mlb/artifacts",
        help="Path to MLB model artifacts directory",
    )
    parser.add_argument(
        "--stats", type=str, nargs="+", default=None,
        help="Stats to predict (default: all configured in mlb_stat_config)",
    )
    parser.add_argument("--skip-discord", action="store_true", help="Skip Discord alert")
    parser.add_argument("--skip-bets", action="store_true", help="Skip paper bet placement")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"MLB INFERENCE JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {target_date}")
    logger.info("=" * 60)

    try:
        from src.db.client import get_engine
        from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
        from src.models.mlb.mlb_daily_runner import MLBDailyPredictionRunner
        from src.models.mlb.mlb_feature_store import MLBFeatureStore
        from src.models.mlb.mlb_model_suite import MLBModelSuite
        from src.models.mlb.mlb_prediction_store import MLBPredictionStore

        # Find model artifacts
        artifacts_path = Path(args.model_dir)
        if not artifacts_path.exists():
            raise FileNotFoundError(f"MLB model artifacts directory not found: {artifacts_path}")

        # Resolve artifact directory: prefer production/, then direct, then latest run
        if (artifacts_path / "production").exists():
            model_path = artifacts_path / "production"
        elif (artifacts_path / "pitcher_k_model.joblib").exists():
            model_path = artifacts_path
        else:
            runs = sorted([
                d for d in artifacts_path.iterdir()
                if d.is_dir()
                and d.name.startswith("mlb_run_")
                and not d.name.endswith("_incomplete")
            ])
            if not runs:
                raise FileNotFoundError(
                    f"No MLB model found. Checked:\n"
                    f"  - {artifacts_path}/production/\n"
                    f"  - {artifacts_path}/pitcher_k_model.joblib\n"
                    f"  - {artifacts_path}/mlb_run_*/\n"
                    "Run MLB training first or promote a model to production."
                )
            model_path = runs[-1]

        logger.info(f"Using MLB model artifacts: {model_path}")

        # Initialize components
        logger.info("Initializing database connection...")
        engine = get_engine()

        # Load unified model suite (discovers all available models)
        logger.info("Loading MLB model suite from %s...", model_path)
        suite = MLBModelSuite.from_directory(model_path, n_samples=10_000)
        logger.info(f"Available models: {suite.available_stats}")

        # Feature stores
        pitcher_feature_store = MLBFeatureStore(engine)
        batter_feature_store = MLBBatterFeatureStore(engine)

        # Determine stats to predict
        if args.stats:
            stats = args.stats
        else:
            from src.models.mlb.mlb_stat_config import MLB_STATS
            stats = list(MLB_STATS.keys())

        logger.info(f"Stats: {stats}")

        # Create runner with unified suite
        runner = MLBDailyPredictionRunner(
            engine=engine,
            suite=suite,
            pitcher_feature_store=pitcher_feature_store,
            batter_feature_store=batter_feature_store,
        )

        logger.info(f"Generating MLB predictions for {target_date}...")
        preds, samples = runner.run_for_date(target_date, stats=stats)

        if preds.empty:
            logger.warning("No MLB predictions generated (no games or no data)")
            elapsed = time.time() - start_time
            logger.info(f"MLB INFERENCE JOB COMPLETED ({elapsed:.1f}s) - No predictions")
            return

        logger.info(f"Generated {len(preds)} MLB predictions")

        # Show summary
        if "over_edge" in preds.columns:
            over_edge_count = len(preds[preds["over_edge"] > 0.05])
            under_edge_count = len(preds[preds["under_edge"] > 0.05])
            logger.info(f"  Over edges >5%: {over_edge_count}")
            logger.info(f"  Under edges >5%: {under_edge_count}")

        # Store to database
        if not args.dry_run:
            logger.info("Storing MLB predictions to database...")
            store = MLBPredictionStore(engine)
            store.store_predictions(preds, target_date)
            store.store_samples(samples, target_date)
            logger.info(f"Stored {len(preds)} predictions + {len(samples)} sample arrays")

        # Export CSV backup
        output_dir = Path("predictions") / "mlb"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"mlb_predictions_{target_date}.csv"
        preds.to_csv(output_file, index=False)
        logger.info(f"Exported CSV: {output_file}")

        # Place paper bets
        if not args.dry_run and not args.skip_bets:
            try:
                from src.paper_trading.mlb_paper_trader import MLBPaperTrader

                logger.info("Placing MLB paper bets...")
                trader = MLBPaperTrader()
                bets = trader.select_bets(target_date)
                if bets:
                    count = trader.place_bets(bets)
                    logger.info(f"Placed {count} MLB paper bets for {target_date}")
                else:
                    logger.info("No MLB predictions meet edge threshold for paper bets")
            except Exception as e:
                logger.warning(f"MLB paper bet placement failed: {e} (non-fatal)")

        # Send Discord alert
        if not args.dry_run and not args.skip_discord:
            try:
                import os
                if os.getenv("DISCORD_BOT_TOKEN"):
                    logger.info("Discord MLB alerts not yet implemented — skipping")
            except Exception as e:
                logger.warning(f"Discord alert failed: {e} (non-fatal)")

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"MLB INFERENCE JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
        logger.info(f"  Predictions: {len(preds)}")
        logger.info(f"  Output: {output_file}")
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("=" * 60)
        logger.error(f"MLB INFERENCE JOB FAILED ({elapsed:.1f}s)")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
