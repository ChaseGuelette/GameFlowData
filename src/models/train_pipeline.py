import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor
from src.models.quantile_trainer import PlayerPropsModelPipeline
from src.processing.feature_selection import FeatureSelector, get_candidate_columns

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TrainingPipeline")


class TrainingOrchestrator:
    CALIBRATION_TOLERANCE = 0.05
    CALIBRATION_HARD_FAIL = 0.10

    def __init__(self, base_artifacts_dir: str = "src/models/artifacts"):
        self.engine = get_engine()
        self.feature_store = FeatureStore(self.engine)

        # Create timestamped run directory
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self.run_dir = Path(base_artifacts_dir) / f"run_{timestamp_str}"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.feature_config_path = self.run_dir / "selected_features.json"

        logger.info(f"Initialized Training Run: {timestamp_str}")
        logger.info(f"Artifacts will be saved to: {self.run_dir}")

    def run(self, train_seasons: list[str], calibration_season: str):
        """
        Execute full training pipeline with strict train/calibration split.
        """
        # Save run configuration
        self._save_run_config(train_seasons, calibration_season)

        logger.info(f"Training Seasons: {train_seasons}")
        logger.info(f"Calibration Season: {calibration_season}")

        # 1. Load Data
        logger.info("Loading datasets...")
        # Load Training Data
        train_df = self.feature_store.get_training_dataset(train_seasons)
        logger.info(f"Loaded Training Data: {len(train_df):,} rows")

        # Load Calibration Data (Holdout)
        cal_df = self.feature_store.get_training_dataset([calibration_season])
        logger.info(f"Loaded Calibration Data: {len(cal_df):,} rows")

        # 2. Feature Selection (on Training Data ONLY)
        selected_features = self._run_feature_selection(train_df)

        # 3. Train Models
        pipeline = self._train_models(train_df, selected_features)

        # 4. Calibration Evaluation (on Holdout Season)
        self._evaluate_calibration(pipeline, cal_df)

        # 5. Save Artifacts
        pipeline.save_all(str(self.run_dir))

        # Save feature config explicitly
        with open(self.feature_config_path, "w") as f:
            json.dump(selected_features, f, indent=4)

        # 6. Sanity Check (End-to-End Inference)
        self._run_sanity_check(pipeline, cal_df)

        logger.info(f"Training pipeline completed successfully. Artifacts in {self.run_dir}")

    def _save_run_config(self, train_seasons, calibration_season):
        """Save run configuration for reproducibility."""
        config = {
            "train_seasons": train_seasons,
            "calibration_season": calibration_season,
            "timestamp": self.timestamp.isoformat(),
            "calibration_tolerance": self.CALIBRATION_TOLERANCE,
            "calibration_hard_fail": self.CALIBRATION_HARD_FAIL,
        }
        with open(self.run_dir / "run_config.json", "w") as f:
            json.dump(config, f, indent=4)

    def _run_feature_selection(self, df: pd.DataFrame) -> dict:
        """Run feature selection on the training dataframe."""
        logger.info("Running Feature Selection Pipeline (Training Data Only)...")
        selector = FeatureSelector(n_splits=3)
        features = {}

        # Minutes
        logger.info("Selecting Minutes features...")
        target = "actual_minutes"
        candidates = get_candidate_columns(df, target)
        minutes_df = df[df["actual_minutes"] > 0].fillna(0)

        features["minutes_features"] = selector.select_features(minutes_df, target, candidates, model_name="Minutes")

        # Rate Stats
        for stat in ["pts", "reb", "ast", "threes"]:
            logger.info(f"Selecting {stat.upper()} features...")
            target = f"{stat}_per_min"
            rate_df = df[(df["actual_minutes"] >= 10) & (df[target].notna())].fillna(0)
            candidates = get_candidate_columns(rate_df, target)

            features[f"{stat}_rate_features"] = selector.select_features(
                rate_df, target, candidates, model_name=f"{stat.upper()} Rate"
            )

        return features

    def _train_models(self, df: pd.DataFrame, feature_config: dict) -> PlayerPropsModelPipeline:
        """Initialize and train the model pipeline with injected features."""
        pipeline = PlayerPropsModelPipeline(self.feature_store)

        # Inject features BEFORE training
        # The modified PlayerPropsModelPipeline will respect these
        pipeline.minutes_features = feature_config["minutes_features"]

        for stat in ["pts", "reb", "ast", "threes"]:
            pipeline.rate_features[stat] = feature_config[f"{stat}_rate_features"]

        # Train
        pipeline.train_minutes_model(df)
        pipeline.train_rate_models(df, stats=["pts", "reb", "ast", "threes"])

        return pipeline

    def _evaluate_calibration(self, pipeline: PlayerPropsModelPipeline, df: pd.DataFrame) -> dict:
        """Generate predictions on holdout and evaluate calibration."""
        logger.info("\n=== Calibration Evaluation (Holdout Season) ===")

        all_reports = {}

        # Minutes
        logger.info("Evaluating Minutes Model...")
        reports = self._calibrate_model(
            model=pipeline.minutes_model,
            features=pipeline.minutes_features,
            df=df,
            actual_col="actual_minutes",
            filter_mask=(df["actual_minutes"] > 0),
            name="minutes",
        )
        all_reports["minutes"] = reports

        # Rate models
        for stat, model in pipeline.rate_models.items():
            logger.info(f"Evaluating {stat.upper()} Rate Model...")
            mask = (df["actual_minutes"] >= 10) & (df[f"{stat}_per_min"].notna())
            reports = self._calibrate_model(
                model=model,
                features=pipeline.rate_features[stat],
                df=df,
                actual_col=f"{stat}_per_min",
                filter_mask=mask,
                name=f"{stat}_rate",
            )
            all_reports[stat] = reports

        # Check for failures
        all_gaps = [r["gap"] for model_reports in all_reports.values() for r in model_reports]
        worst_gap = max((abs(g) for g in all_gaps), default=0)

        logger.info(f"Worst calibration gap: {worst_gap:.1%}")

        # Save report
        self._save_calibration_report(all_reports)

        if worst_gap > self.CALIBRATION_HARD_FAIL:
            logger.warning(f"Calibration FAILED: worst gap = {worst_gap:.1%} (threshold: {self.CALIBRATION_HARD_FAIL:.0%})")
            with open(self.run_dir / "CALIBRATION_FAILED.txt", "w") as f:
                f.write(f"Worst calibration gap: {worst_gap:.1%}\nHard fail threshold: {self.CALIBRATION_HARD_FAIL:.0%}\nDO NOT deploy without review.")
        elif worst_gap > self.CALIBRATION_TOLERANCE:
            logger.warning(f"Calibration warning: worst gap = {worst_gap:.1%}")
            with open(self.run_dir / "CALIBRATION_WARNING.txt", "w") as f:
                f.write(f"Worst calibration gap: {worst_gap:.1%}\nReview before deployment.")

        return all_reports

    def _calibrate_model(self, model, features, df, actual_col, filter_mask, name) -> list[dict]:
        """Evaluate calibration for a single model."""
        # Use reset_index(drop=True) to align indices for assignment
        filtered = df[filter_mask].copy().reset_index(drop=True)
        X = filtered[features].fillna(0)
        y_actual = filtered[actual_col].values

        if X.empty:
            logger.warning(f"No validation data for {name}")
            return []

        # Predict quantiles
        preds = model.predict_quantiles(X)
        # preds is a new DataFrame with range index (0..n-1), matches filtered

        reports = []
        for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
            pred_col = f"q{int(q * 100):02d}"

            # Check coverage
            coverage = (y_actual <= preds[pred_col].values).mean()
            gap = coverage - q

            status = "OK" if abs(gap) <= self.CALIBRATION_TOLERANCE else f"GAP {gap:+.3f}"
            logger.info(f"  Q{q:.2f}: Act={coverage:.3f} [{status}]")

            reports.append({"quantile": q, "coverage": coverage, "gap": gap})

        return reports

    def _save_calibration_report(self, reports: dict):
        with open(self.run_dir / "calibration_report.json", "w") as f:
            json.dump(reports, f, indent=4)

    def _run_sanity_check(self, pipeline: PlayerPropsModelPipeline, df: pd.DataFrame):
        """
        Run a single prediction using FeatureStore.get_player_game_features
        to ensure end-to-end inference connectivity.
        """
        logger.info("\n=== Sanity Check: Single Game Inference ===")

        # Pick a random game from the holdout set
        if df.empty:
            logger.error("Calibration dataframe is empty. Cannot run sanity check.")
            return

        sample = df.sample(1).iloc[0]
        player_id = sample["player_id"]
        game_id = sample["game_id"]
        game_date = sample["game_date"]  # This is usually a date object or string

        logger.info(f"Testing inference for Player {player_id}, Game {game_id}, Date {game_date}")

        try:
            # Fetch features "as of" game time
            features = self.feature_store.get_player_game_features(
                player_id=int(player_id), game_id=str(game_id), as_of_date=game_date
            )

            if not features:
                logger.error("FeatureStore returned None for valid game. Check data availability.")
                # We don't fail hard here because sometimes data gaps happen,
                # but in a strict pipeline maybe we should.
                return

            # Predict using Monte Carlo
            # Reduced samples for sanity check speed, production uses more (e.g. 10000)
            mc = MonteCarloPredictor(pipeline, n_samples=100)
            preds = mc.predict(player_id, game_id, features, stats=["pts", "reb", "ast"])

            for stat, p in preds.items():
                logger.info(
                    f"  {stat.upper()}: Median={p.median:.2f}, Mean={p.mean:.2f}, Q10={p.q10:.2f}, Q90={p.q90:.2f}"
                )

                # Bounds checks
                if p.mean < 0:
                    raise ValueError(f"Negative mean for {stat}: {p.mean}")
                if p.q10 > p.q90:
                    raise ValueError(f"Quantile inversion for {stat}: Q10={p.q10} > Q90={p.q90}")
                if p.median > 100:
                    raise ValueError(f"Unreasonably high median for {stat}: {p.median}")

            logger.info("Sanity check PASSED.")

        except Exception as e:
            logger.error(f"Sanity check FAILED: {e}", exc_info=True)
            raise  # Re-raise to fail the pipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Player Props Models")
    parser.add_argument("--train-seasons", nargs="+", default=["22022", "22023"], help="Seasons for training")
    parser.add_argument("--cal-season", default="22024", help="Season for calibration (holdout)")

    args = parser.parse_args()

    orchestrator = TrainingOrchestrator()
    orchestrator.run(train_seasons=args.train_seasons, calibration_season=args.cal_season)
