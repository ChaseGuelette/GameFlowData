import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

import numpy as np

from src.db.client import get_engine
from src.models.feature_store import FeatureStore
from src.models.hyperparameter_tuner import QuantileHyperparameterTuner
from src.models.monte_carlo import MonteCarloPredictor, compute_copula_params_from_data
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

    def __init__(
        self,
        base_artifacts_dir: str = "src/models/artifacts",
        tune_hyperparams: bool = False,
        tuning_trials: int = 50,
        tuning_timeout: int | None = None,
        tuning_per_quantile: bool = False,
        hyperparams_path: str | None = None,
    ):
        self.engine = get_engine()
        self.feature_store = FeatureStore(self.engine)

        # Tuning settings
        self.tune_hyperparams = tune_hyperparams
        self.tuning_trials = tuning_trials
        self.tuning_timeout = tuning_timeout
        self.tuning_per_quantile = tuning_per_quantile
        self.hyperparams_path = hyperparams_path

        # Create timestamped run directory
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self.run_dir = Path(base_artifacts_dir) / f"run_{timestamp_str}"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.feature_config_path = self.run_dir / "selected_features.json"
        self.hyperparams_config_path = self.run_dir / "best_hyperparams.json"

        logger.info(f"Initialized Training Run: {timestamp_str}")
        logger.info(f"Artifacts will be saved to: {self.run_dir}")
        if tune_hyperparams:
            logger.info(f"Hyperparameter tuning ENABLED: {tuning_trials} trials")
        elif hyperparams_path:
            logger.info(f"Loading hyperparams from: {hyperparams_path}")

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

        # 3. Hyperparameter Tuning (Optional)
        tuned_hyperparams = self._run_hyperparameter_tuning(train_df, selected_features)

        # 4. Train Models
        pipeline = self._train_models(train_df, selected_features, hyperparams=tuned_hyperparams)

        # 5. Calibration Evaluation (on Holdout Season)
        self._evaluate_calibration(pipeline, cal_df)

        # 5b. Combined Calibration (Minutes × Rate → Total)
        self._evaluate_combined_calibration(pipeline, cal_df)

        # 5c. Minutes-Rate Correlation Analysis
        self._analyze_minutes_rate_correlation(cal_df)

        # 5d. Compute and save Gaussian copula parameters
        self._compute_copula_params(train_df)

        # 6. Save Artifacts
        pipeline.save_all(str(self.run_dir))

        # Save feature config explicitly
        with open(self.feature_config_path, "w") as f:
            json.dump(selected_features, f, indent=4)

        # 7. Sanity Check (End-to-End Inference)
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

    def _run_hyperparameter_tuning(self, df: pd.DataFrame, feature_config: dict) -> dict | None:
        """
        Run hyperparameter tuning if enabled, or load from existing file.

        Returns:
            Dict of hyperparameters per model, or None to use defaults.
        """
        # Load from existing file if specified
        if self.hyperparams_path:
            logger.info(f"Loading hyperparameters from {self.hyperparams_path}")
            with open(self.hyperparams_path) as f:
                hyperparams = json.load(f)
            # Save a copy to run directory
            with open(self.hyperparams_config_path, "w") as f:
                json.dump(hyperparams, f, indent=4)
            return hyperparams

        # Skip tuning if not enabled
        if not self.tune_hyperparams:
            logger.info("Hyperparameter tuning DISABLED, using defaults")
            return None

        logger.info("Running Hyperparameter Tuning...")

        tuner = QuantileHyperparameterTuner(
            n_trials=self.tuning_trials,
            timeout=self.tuning_timeout,
            per_quantile=self.tuning_per_quantile,
            pruning=True,
            val_fraction=0.15,
        )

        hyperparams = {}

        # Tune Minutes Model
        logger.info("Tuning Minutes Model...")
        minutes_df = df[df["actual_minutes"] > 0].copy()
        minutes_features = feature_config["minutes_features"]

        # For per-quantile features, use the union of all features for tuning
        if isinstance(minutes_features, dict):
            all_minutes_features = list(set(f for feats in minutes_features.values() for f in feats))
        else:
            all_minutes_features = minutes_features

        X_minutes = minutes_df[all_minutes_features].fillna(0)
        y_minutes = minutes_df["actual_minutes"]

        if self.tuning_per_quantile:
            minutes_configs = tuner.tune_per_quantile(X_minutes, y_minutes)
            hyperparams["minutes"] = {str(q): cfg.to_dict() for q, cfg in minutes_configs.items()}
        else:
            minutes_config = tuner.tune(X_minutes, y_minutes)
            hyperparams["minutes"] = minutes_config.to_dict()

        # Tune Rate Models
        for stat in ["pts", "reb", "ast", "threes"]:
            logger.info(f"Tuning {stat.upper()} Rate Model...")
            target = f"{stat}_per_min"
            rate_df = df[(df["actual_minutes"] >= 10) & (df[target].notna())].copy()

            rate_features = feature_config[f"{stat}_rate_features"]
            if isinstance(rate_features, dict):
                all_rate_features = list(set(f for feats in rate_features.values() for f in feats))
            else:
                all_rate_features = rate_features

            X_rate = rate_df[all_rate_features].fillna(0)
            y_rate = rate_df[target]

            if self.tuning_per_quantile:
                rate_configs = tuner.tune_per_quantile(X_rate, y_rate)
                hyperparams[stat] = {str(q): cfg.to_dict() for q, cfg in rate_configs.items()}
            else:
                rate_config = tuner.tune(X_rate, y_rate)
                hyperparams[stat] = rate_config.to_dict()

        # Save hyperparameters
        with open(self.hyperparams_config_path, "w") as f:
            json.dump(hyperparams, f, indent=4)
        logger.info(f"Saved tuned hyperparameters to {self.hyperparams_config_path}")

        return hyperparams

    def _run_feature_selection(self, df: pd.DataFrame) -> dict:
        """Run per-quantile feature selection on the training dataframe."""
        logger.info("Running Per-Quantile Feature Selection Pipeline (Training Data Only)...")
        selector = FeatureSelector(n_splits=3)
        features = {}

        # Minutes
        logger.info("Selecting Minutes features (per quantile)...")
        target = "actual_minutes"
        candidates = get_candidate_columns(df, target)
        minutes_df = df[df["actual_minutes"] > 0].fillna(0)

        features["minutes_features"] = selector.select_features_per_quantile(
            minutes_df, target, candidates, model_name="Minutes"
        )

        # Rate Stats
        for stat in ["pts", "reb", "ast", "threes"]:
            logger.info(f"Selecting {stat.upper()} features (per quantile)...")
            target = f"{stat}_per_min"
            rate_df = df[(df["actual_minutes"] >= 10) & (df[target].notna())].fillna(0)
            candidates = get_candidate_columns(rate_df, target)

            features[f"{stat}_rate_features"] = selector.select_features_per_quantile(
                rate_df, target, candidates, model_name=f"{stat.upper()} Rate"
            )

        return features

    def _train_models(
        self, df: pd.DataFrame, feature_config: dict, hyperparams: dict | None = None
    ) -> PlayerPropsModelPipeline:
        """Initialize and train the model pipeline with injected features."""
        pipeline = PlayerPropsModelPipeline(self.feature_store)

        # Inject features BEFORE training
        # The modified PlayerPropsModelPipeline will respect these
        pipeline.minutes_features = feature_config["minutes_features"]

        for stat in ["pts", "reb", "ast", "threes"]:
            pipeline.rate_features[stat] = feature_config[f"{stat}_rate_features"]

        # Extract hyperparams for each model if provided
        minutes_hyperparams = hyperparams.get("minutes") if hyperparams else None
        rate_hyperparams = (
            {stat: hyperparams.get(stat) for stat in ["pts", "reb", "ast", "threes"]} if hyperparams else {}
        )

        # Train
        pipeline.train_minutes_model(df, hyperparams=minutes_hyperparams)
        pipeline.train_rate_models(df, stats=["pts", "reb", "ast", "threes"], hyperparams=rate_hyperparams)

        return pipeline

    def _evaluate_calibration(self, pipeline: PlayerPropsModelPipeline, df: pd.DataFrame) -> dict:
        """Generate predictions on holdout and evaluate calibration."""
        logger.info("\n=== Calibration Evaluation (Holdout Season) ===")

        all_reports = {}

        # Minutes
        logger.info("Evaluating Minutes Model...")
        reports = self._calibrate_model(
            model=pipeline.minutes_model,
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
            logger.warning(
                f"Calibration FAILED: worst gap = {worst_gap:.1%} (threshold: {self.CALIBRATION_HARD_FAIL:.0%})"
            )
            with open(self.run_dir / "CALIBRATION_FAILED.txt", "w") as f:
                f.write(
                    f"Worst calibration gap: {worst_gap:.1%}\nHard fail threshold: {self.CALIBRATION_HARD_FAIL:.0%}\nDO NOT deploy without review."
                )
        elif worst_gap > self.CALIBRATION_TOLERANCE:
            logger.warning(f"Calibration warning: worst gap = {worst_gap:.1%}")
            with open(self.run_dir / "CALIBRATION_WARNING.txt", "w") as f:
                f.write(f"Worst calibration gap: {worst_gap:.1%}\nReview before deployment.")

        return all_reports

    def _calibrate_model(self, model, df, actual_col, filter_mask, name) -> list[dict]:
        """Evaluate calibration for a single model."""
        # Use reset_index(drop=True) to align indices for assignment
        filtered = df[filter_mask].copy().reset_index(drop=True)

        # Use the model's all_feature_names property
        X = filtered[model.all_feature_names].fillna(0)
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

    def _save_calibration_report(self, reports: dict, suffix: str = ""):
        filename = f"calibration_report{suffix}.json" if suffix else "calibration_report.json"
        with open(self.run_dir / filename, "w") as f:
            json.dump(reports, f, indent=4)

    def _evaluate_combined_calibration(
        self, pipeline: PlayerPropsModelPipeline, df: pd.DataFrame, sample_size: int = 2000
    ) -> dict:
        """
        Evaluate end-to-end calibration: Monte Carlo (minutes × rate) vs actual totals.

        This catches calibration drift from multiplying two uncertain quantities.
        """
        logger.info("\n=== Combined Calibration (Minutes × Rate → Total) ===")

        # Filter to valid rows with actual stats
        valid_mask = df["actual_minutes"] >= 10
        for stat in ["pts", "reb", "ast"]:
            valid_mask &= df[f"actual_{stat}"].notna()

        eval_df = df[valid_mask].copy()

        # Sample for speed (full eval is slow)
        if len(eval_df) > sample_size:
            eval_df = eval_df.sample(sample_size, random_state=42)

        logger.info(f"Evaluating {len(eval_df)} samples...")

        # Create predictor
        mc = MonteCarloPredictor(pipeline, n_samples=1000, random_state=42)

        # Collect predictions and actuals
        results = {stat: {"predictions": [], "actuals": []} for stat in ["pts", "reb", "ast"]}

        for idx, row in eval_df.iterrows():
            # Build feature dict from row
            features = {col: row[col] for col in pipeline.minutes_model.all_feature_names if col in row.index}
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
                    results[stat]["predictions"].append(
                        {
                            "q10": preds[stat].q10,
                            "q25": preds[stat].q25,
                            "q50": preds[stat].q50,
                            "q75": preds[stat].q75,
                            "q90": preds[stat].q90,
                            "mean": preds[stat].mean,
                        }
                    )
                    results[stat]["actuals"].append(row[f"actual_{stat}"])
            except Exception as e:
                logger.debug(f"Prediction failed for row {idx}: {e}")
                continue

        # Calculate calibration for each stat
        reports = {}

        for stat in ["pts", "reb", "ast"]:
            if not results[stat]["predictions"]:
                logger.warning(f"No predictions for {stat}")
                continue

            actuals = np.array(results[stat]["actuals"])
            preds_df = pd.DataFrame(results[stat]["predictions"])

            stat_reports = []
            logger.info(f"\n  {stat.upper()} (Combined):")

            for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
                pred_col = f"q{int(q * 100):02d}"
                coverage = (actuals <= preds_df[pred_col].values).mean()
                gap = coverage - q

                status = "OK" if abs(gap) <= self.CALIBRATION_TOLERANCE else f"GAP {gap:+.3f}"
                logger.info(f"    Q{q:.2f}: Act={coverage:.3f} Target={q:.2f} [{status}]")

                stat_reports.append(
                    {
                        "quantile": q,
                        "coverage": float(coverage),
                        "target": q,
                        "gap": float(gap),
                    }
                )

            reports[stat] = stat_reports

        # Save combined calibration report
        self._save_calibration_report(reports, suffix="_combined")

        # Summary
        all_gaps = [r["gap"] for stat_reports in reports.values() for r in stat_reports]
        if all_gaps:
            worst_gap = max(abs(g) for g in all_gaps)
            mean_gap = np.mean([abs(g) for g in all_gaps])
            logger.info(f"\n  Combined calibration: worst_gap={worst_gap:.3f}, mean_gap={mean_gap:.3f}")

            if worst_gap > self.CALIBRATION_TOLERANCE:
                logger.warning(
                    "Combined calibration shows drift! "
                    "Individual models are calibrated but combined prediction is not. "
                    "Consider variance inflation or correlation modeling."
                )

        return reports

    def _analyze_minutes_rate_correlation(self, df: pd.DataFrame) -> dict:
        """
        Analyze correlation between minutes and per-minute rates.

        If there's significant correlation, independent sampling in Monte Carlo
        will produce biased combined predictions.
        """
        logger.info("\n=== Minutes-Rate Correlation Analysis ===")

        # Filter to valid rows
        valid_mask = df["actual_minutes"] >= 10
        analysis_df = df[valid_mask].copy()

        correlations = {}

        for stat in ["pts", "reb", "ast"]:
            rate_col = f"{stat}_per_min"
            if rate_col not in analysis_df.columns:
                continue

            # Overall correlation
            corr = analysis_df["actual_minutes"].corr(analysis_df[rate_col])
            correlations[stat] = {"overall": float(corr)}

            # Correlation by minutes bucket
            analysis_df["minutes_bucket"] = pd.cut(
                analysis_df["actual_minutes"], bins=[0, 20, 30, 40, 50], labels=["10-20", "20-30", "30-40", "40+"]
            )

            bucket_stats = []
            for bucket in ["10-20", "20-30", "30-40", "40+"]:
                bucket_df = analysis_df[analysis_df["minutes_bucket"] == bucket]
                if len(bucket_df) > 30:
                    bucket_corr = bucket_df["actual_minutes"].corr(bucket_df[rate_col])
                    mean_rate = bucket_df[rate_col].mean()
                    bucket_stats.append(
                        {
                            "bucket": bucket,
                            "n": len(bucket_df),
                            "correlation": float(bucket_corr) if not pd.isna(bucket_corr) else 0.0,
                            "mean_rate": float(mean_rate),
                        }
                    )

            correlations[stat]["by_bucket"] = bucket_stats

            # Log findings
            logger.info(f"  {stat.upper()}: overall_corr={corr:.3f}")
            for bs in bucket_stats:
                logger.info(
                    f"    {bs['bucket']} min: corr={bs['correlation']:.3f}, mean_rate={bs['mean_rate']:.3f}, n={bs['n']}"
                )

        # Check for systematic pattern
        for stat, data in correlations.items():
            if abs(data["overall"]) > 0.1:
                logger.warning(
                    f"  ⚠ {stat.upper()} has notable minutes-rate correlation ({data['overall']:.3f}). "
                    f"Independent sampling may introduce bias."
                )

        # Check if rate increases/decreases with minutes (systematic bias)
        for stat, data in correlations.items():
            if "by_bucket" in data and len(data["by_bucket"]) >= 2:
                rates = [b["mean_rate"] for b in data["by_bucket"]]
                if len(rates) >= 2:
                    rate_trend = rates[-1] - rates[0]  # High minutes - low minutes
                    if abs(rate_trend) > 0.05:
                        direction = "increases" if rate_trend > 0 else "decreases"
                        logger.warning(
                            f"  ⚠ {stat.upper()} rate {direction} with minutes "
                            f"(low min: {rates[0]:.3f}, high min: {rates[-1]:.3f}). "
                            f"This will bias combined predictions."
                        )

        # Save analysis
        with open(self.run_dir / "correlation_analysis.json", "w") as f:
            json.dump(correlations, f, indent=4)

        return correlations

    def _compute_copula_params(self, df: pd.DataFrame) -> dict:
        """
        Compute Gaussian copula parameters from training data and save as artifact.

        Computes Spearman rank correlations between actual_minutes and per-minute rates
        for each stat. These are used by MonteCarloPredictor for correlated sampling
        that preserves marginal distributions while capturing minutes-rate dependency.
        """
        logger.info("\n=== Computing Gaussian Copula Parameters ===")

        copula_params = compute_copula_params_from_data(df)

        for stat, rho in copula_params.items():
            logger.info(f"  {stat.upper()}: Spearman ρ = {rho:.4f}")

        # Save as artifact
        with open(self.run_dir / "copula_params.json", "w") as f:
            json.dump(copula_params, f, indent=4)

        logger.info(f"Saved copula parameters to {self.run_dir / 'copula_params.json'}")
        return copula_params

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

    # Hyperparameter tuning options
    parser.add_argument("--tune", action="store_true", help="Enable hyperparameter tuning before training")
    parser.add_argument("--tuning-trials", type=int, default=50, help="Number of Optuna trials (default: 50)")
    parser.add_argument(
        "--tuning-timeout", type=int, default=None, help="Max tuning time in seconds (default: no limit)"
    )
    parser.add_argument("--tuning-per-quantile", action="store_true", help="Tune separately for each quantile")
    parser.add_argument(
        "--hyperparams-path", type=str, default=None, help="Path to existing hyperparams JSON to load (skips tuning)"
    )

    args = parser.parse_args()

    orchestrator = TrainingOrchestrator(
        tune_hyperparams=args.tune,
        tuning_trials=args.tuning_trials,
        tuning_timeout=args.tuning_timeout,
        tuning_per_quantile=args.tuning_per_quantile,
        hyperparams_path=args.hyperparams_path,
    )
    orchestrator.run(train_seasons=args.train_seasons, calibration_season=args.cal_season)
