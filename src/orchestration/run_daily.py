import argparse
import logging
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.daily_runner import DailyPredictionRunner
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor, load_combined_calibration_offsets, load_copula_params
from src.models.prediction_store import PredictionStore
from src.models.quantile_trainer import PlayerPropsModelPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DailyOrchestrator")


def run_command(command, description):
    """Run a shell command and check for errors."""
    logger.info(f"STARTING: {description}")
    try:
        # Split command into list for shell=False
        cmd_list = command.split()
        subprocess.run(cmd_list, check=True, capture_output=True, text=True, shell=False)
        logger.info(f"COMPLETED: {description}")
    except subprocess.CalledProcessError as e:
        logger.error(f"FAILED: {description}")
        logger.error(f"Error Output: {e.stderr}")
        raise e


def main():
    parser = argparse.ArgumentParser(description="Daily Orchestration Script")
    parser.add_argument("--date", type=str, default=str(date.today()), help="Target date for predictions (YYYY-MM-DD)")
    parser.add_argument("--skip-scraping", action="store_true", help="Skip the scraping step")
    parser.add_argument(
        "--scrape-live-odds", action="store_true", help="Scrape LIVE odds (current lines) into raw_game_lines_live"
    )
    parser.add_argument(
        "--scrape-daily-props",
        action="store_true",
        help="Scrape daily player props (12pm/6pm) into raw_player_props_combined",
    )
    parser.add_argument(
        "--scrape-live-props", action="store_true", help="Scrape LIVE player props into raw_player_props_live"
    )
    parser.add_argument("--scrape-injuries", action="store_true", help="Scrape injuries from RapidAPI and link player IDs")
    parser.add_argument("--skip-processing", action="store_true", help="Skip the processing/update step")
    parser.add_argument("--skip-inference", action="store_true", help="Skip the prediction step")
    parser.add_argument("--skip-storage", action="store_true", help="Skip storing predictions to database")
    parser.add_argument("--model-dir", type=str, default="src/models/artifacts", help="Path to model artifacts")

    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info("=" * 60)
    logger.info(f"DAILY PIPELINE START: {target_date}")
    logger.info("=" * 60)

    # 1. Scraping (Get latest game results)
    if not args.skip_scraping:
        # Run nba_unified_scraper for current season
        run_command(f"{sys.executable} src/scrapers/nba_unified_scraper.py", "Scraping Latest Game Results")

        # Scrape Daily Game Lines (Odds)
        run_command(
            f"{sys.executable} src/scrapers/daily_game_lines_scraper.py --date {args.date}", "Scraping Daily Game Lines (Odds)"
        )

        # Scrape Daily Player Props (12pm/6pm snapshots)
        if args.scrape_daily_props:
            run_command(
                f"{sys.executable} src/scrapers/daily_player_props_scraper.py --date {args.date}",
                "Scraping Daily Player Props (Historical Snapshots)",
            )

        # Scrape LIVE Odds (Optional)
        if args.scrape_live_odds:
            run_command(f"{sys.executable} src/scrapers/live_odds_scraper.py", "Scraping LIVE Odds to 'raw_game_lines_live'")

        # Scrape LIVE Player Props (Optional)
        if args.scrape_live_props:
            run_command(
                f"{sys.executable} src/scrapers/daily_player_props_scraper.py --live",
                "Scraping LIVE Player Props to 'raw_player_props_live'",
            )

        # Scrape Injuries (Optional) - Uses RapidAPI → rapidapi_injuries table
        if args.scrape_injuries:
            # Fetch today's injuries from RapidAPI into rapidapi_injuries table
            run_command(
                f"{sys.executable} src/scrapers/rapidapi_injury_backfill.py --start {args.date} --end {args.date}",
                "Scraping RapidAPI Injuries"
            )
            # Link player names to player_id for feature generation and injury filtering
            run_command(
                f"{sys.executable} src/processing/link_injury_data.py",
                "Linking Injury Player IDs"
            )

    else:
        logger.info("Skipping Scraping Step")

    # 2. Processing (Update Derived Stats)
    if not args.skip_processing:
        # Order matters!
        run_command(f"{sys.executable} src/processing/nba_linker_local.py incremental", "Linking Players (Incremental)")
        run_command(f"{sys.executable} src/processing/backfill_team_ids.py", "Backfilling Team IDs")
        run_command(f"{sys.executable} src/scrapers/update_player_position_history.py", "Updating Player Position History")
        run_command(f"{sys.executable} src/scrapers/update_league_position_averages.py", "Updating League Position Averages")
        run_command(f"{sys.executable} src/processing/populate_average_stats.py", "Populating Rolling Average Stats")
        run_command(f"{sys.executable} src/processing/backfill_opponent_allowed.py", "Updating Opponent Allowed Stats")
    else:
        logger.info("Skipping Processing Step")

    # 3. Inference (Generate Predictions)
    if not args.skip_inference:
        logger.info("Starting Inference Step...")

        try:
            artifacts_path = Path(args.model_dir)
            if not artifacts_path.exists():
                raise FileNotFoundError(f"Model artifacts directory not found: {artifacts_path}")

            if (artifacts_path / "minutes_model.joblib").exists():
                model_path = artifacts_path
            else:
                runs = sorted([d for d in artifacts_path.iterdir() if d.is_dir() and d.name.startswith("nba_run_")])
                if not runs:
                    raise FileNotFoundError(f"No nba_run_* directories found in {artifacts_path}")
                model_path = runs[-1]
                logger.info(f"Using latest model artifacts from: {model_path.name}")

            engine = get_engine()
            feature_store = FeatureStore(engine)

            logger.info("Loading Model Pipeline...")
            pipeline = PlayerPropsModelPipeline.load_all(str(model_path), feature_store)

            logger.info("Initializing Predictor...")
            copula_params = load_copula_params(str(model_path))
            if copula_params:
                logger.info("Loaded Gaussian copula params from model artifacts")
            combined_cal_offsets = load_combined_calibration_offsets(str(model_path))
            if combined_cal_offsets:
                logger.info(f"Loaded combined calibration offsets for: {list(combined_cal_offsets.keys())}")
            predictor = MonteCarloPredictor(
                pipeline, n_samples=10000, copula_params=copula_params,
                combined_calibration_offsets=combined_cal_offsets,
            )

            runner = DailyPredictionRunner(engine, feature_store, pipeline, predictor)

            preds, samples = runner.run_for_date(target_date)

            if not preds.empty:
                logger.info(f"Generated {len(preds)} predictions.")

                # Store to database
                if not args.skip_storage:
                    store = PredictionStore(engine)
                    store.store_predictions(preds, target_date)
                    store.store_samples(samples, target_date)
                    logger.info(f"Stored {len(preds)} predictions + {len(samples)} sample arrays to database.")

                # Save CSV for backwards compat
                output_file = f"predictions_{target_date}.csv"
                preds.to_csv(output_file, index=False)
                logger.info(f"Saved predictions to {output_file}")
            else:
                logger.warning("No predictions generated (No games or no data?)")

        except Exception as e:
            logger.error(f"Inference Failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        logger.info("Skipping Inference Step")

    logger.info("=" * 60)
    logger.info("DAILY PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
