import argparse
import json
import logging
import sys
from datetime import date, datetime
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

        # Create timestamped run directory with _incomplete suffix
        # This prevents the inference job from picking up a partially-trained model
        # The directory is renamed to remove _incomplete after all artifacts are saved
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self._final_run_dir_name = f"run_{timestamp_str}"
        self.run_dir = Path(base_artifacts_dir) / f"run_{timestamp_str}_incomplete"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.feature_config_path = self.run_dir / "selected_features.json"
        self.hyperparams_config_path = self.run_dir / "best_hyperparams.json"

        logger.info(f"Initialized Training Run: {timestamp_str}")
        logger.info(f"Artifacts will be saved to: {self.run_dir} (renamed on completion)")
        if tune_hyperparams:
            logger.info(f"Hyperparameter tuning ENABLED: {tuning_trials} trials")
        elif hyperparams_path:
            logger.info(f"Loading hyperparams from: {hyperparams_path}")

    def run(self, train_seasons: list[str], calibration_season: str, cal_end_date: date | None = None):
        """
        Execute full training pipeline with strict train/calibration split.

        Args:
            train_seasons: Season IDs for training data.
            calibration_season: Season ID for calibration holdout.
            cal_end_date: Optional end date for calibration data (exclusive).
                          Filters cal data to game_date < cal_end_date.
                          Useful for reserving recent games for backtesting.
        """
        # Save run configuration
        self._save_run_config(train_seasons, calibration_season, cal_end_date)

        logger.info(f"Training Seasons: {train_seasons}")
        logger.info(f"Calibration Season: {calibration_season}")
        if cal_end_date:
            logger.info(f"Calibration End Date: {cal_end_date} (exclusive)")

        # 1. Load Data
        logger.info("Loading datasets...")
        # Load Training Data
        train_df = self.feature_store.get_training_dataset(train_seasons)
        logger.info(f"Loaded Training Data: {len(train_df):,} rows")

        # Load Calibration Data (Holdout)
        cal_df = self.feature_store.get_training_dataset([calibration_season])
        if cal_end_date:
            pre_filter = len(cal_df)
            cal_df = cal_df[cal_df["game_date"] < cal_end_date].reset_index(drop=True)
            logger.info(f"Loaded Calibration Data: {len(cal_df):,} rows (filtered from {pre_filter:,}, end date {cal_end_date})")
        else:
            logger.info(f"Loaded Calibration Data: {len(cal_df):,} rows")

        # 2. Feature Selection (on Training Data ONLY)
        selected_features = self._run_feature_selection(train_df)

        # 3. Hyperparameter Tuning (Optional)
        tuned_hyperparams = self._run_hyperparameter_tuning(train_df, selected_features)

        # 4. Train Models
        pipeline = self._train_models(train_df, selected_features, hyperparams=tuned_hyperparams)

        # 5. Calibration Evaluation (on Holdout Season)
        self._evaluate_calibration(pipeline, cal_df)

        # 5b. Compute and save Gaussian copula parameters (before combined calibration)
        copula_params = self._compute_copula_params(train_df)

        # 5c. Combined Calibration (Minutes × Rate → Total)
        self._evaluate_combined_calibration(pipeline, cal_df, copula_params=copula_params)

        # 5d. Minutes-Rate Correlation Analysis
        self._analyze_minutes_rate_correlation(cal_df)

        # 6. Save Artifacts
        pipeline.save_all(str(self.run_dir))

        # Save feature config explicitly
        with open(self.feature_config_path, "w") as f:
            json.dump(selected_features, f, indent=4)

        # 7. Sanity Check (End-to-End Inference)
        self._run_sanity_check(pipeline, cal_df)

        # 8. Finalize: Rename directory to remove _incomplete suffix
        # This atomic rename ensures inference job only sees complete models
        final_run_dir = self.run_dir.parent / self._final_run_dir_name
        self.run_dir.rename(final_run_dir)
        self.run_dir = final_run_dir
        logger.info(f"Training pipeline completed successfully. Artifacts in {self.run_dir}")

    def run_partial(
        self,
        train_seasons: list[str],
        calibration_season: str,
        cal_end_date: date | None,
        base_model_dir: str,
        retrain_stats: list[str] | None = None,
        retrain_minutes: bool = False,
        reselect_features: bool = False,
    ):
        """
        Surgical retrain: load an existing pipeline, retrain only specified
        components, evaluate calibration on *all* models, and save the
        mixed (frozen + retrained) result.

        Args:
            train_seasons: Season IDs for training data.
            calibration_season: Season ID for calibration holdout.
            cal_end_date: Optional end date for calibration data (exclusive).
            base_model_dir: Path to existing production pipeline directory.
            retrain_stats: List of stats to retrain (e.g. ["reb", "ast"]).
            retrain_minutes: Whether to retrain the minutes model.
            reselect_features: Whether to re-run feature selection for
                retrained components.
        """
        # Save run config
        config = {
            "mode": "partial_retrain",
            "train_seasons": train_seasons,
            "calibration_season": calibration_season,
            "cal_end_date": str(cal_end_date) if cal_end_date else None,
            "base_model_dir": base_model_dir,
            "retrain_stats": retrain_stats,
            "retrain_minutes": retrain_minutes,
            "reselect_features": reselect_features,
            "timestamp": self.timestamp.isoformat(),
            "calibration_tolerance": self.CALIBRATION_TOLERANCE,
            "calibration_hard_fail": self.CALIBRATION_HARD_FAIL,
        }
        with open(self.run_dir / "run_config.json", "w") as f:
            json.dump(config, f, indent=4)

        logger.info("=== Surgical Retrain Mode ===")
        logger.info(f"Base model: {base_model_dir}")
        logger.info(f"Retrain stats: {retrain_stats or 'none'}")
        logger.info(f"Retrain minutes: {retrain_minutes}")
        logger.info(f"Training Seasons: {train_seasons}")
        logger.info(f"Calibration Season: {calibration_season}")
        if cal_end_date:
            logger.info(f"Calibration End Date: {cal_end_date} (exclusive)")

        # 1. Load existing pipeline from base_model_dir
        logger.info(f"Loading existing pipeline from {base_model_dir}...")
        pipeline = PlayerPropsModelPipeline.load_all(base_model_dir, self.feature_store)
        logger.info("Loaded existing pipeline (frozen models preserved)")

        # 2. Load training + calibration data
        logger.info("Loading datasets...")
        train_df = self.feature_store.get_training_dataset(train_seasons)
        logger.info(f"Loaded Training Data: {len(train_df):,} rows")

        cal_df = self.feature_store.get_training_dataset([calibration_season])
        if cal_end_date:
            pre_filter = len(cal_df)
            cal_df = cal_df[cal_df["game_date"] < cal_end_date].reset_index(drop=True)
            logger.info(f"Loaded Calibration Data: {len(cal_df):,} rows (filtered from {pre_filter:,})")
        else:
            logger.info(f"Loaded Calibration Data: {len(cal_df):,} rows")

        # 3. Optionally reselect features for retrained components only
        if reselect_features:
            new_features = self._run_feature_selection_partial(train_df, retrain_stats, retrain_minutes)
            if retrain_minutes and "minutes_features" in new_features:
                pipeline.minutes_features = new_features["minutes_features"]
            if retrain_stats:
                for stat in retrain_stats:
                    key = f"{stat}_rate_features"
                    if key in new_features:
                        pipeline.rate_features[stat] = new_features[key]

        # 3b. Hyperparameter tuning for retrained components
        hyperparams = self._resolve_hyperparams_partial(
            train_df, pipeline, retrain_stats, retrain_minutes, base_model_dir
        )

        # 4. Retrain specified components
        if retrain_minutes:
            logger.info("Retraining minutes model...")
            minutes_hp = hyperparams.get("minutes")
            pipeline.train_minutes_model(train_df, hyperparams=minutes_hp)

        if retrain_stats:
            logger.info(f"Retraining rate models: {[s.upper() for s in retrain_stats]}")
            rate_hp = {s: hyperparams.get(s) for s in retrain_stats}
            pipeline.train_rate_models(train_df, stats=retrain_stats, hyperparams=rate_hp)

        # 5. Evaluate calibration (all models, not just retrained)
        self._evaluate_calibration(pipeline, cal_df)

        # 6. Recompute copula params from training data
        copula_params = self._compute_copula_params(train_df)

        # 7. Combined calibration
        self._evaluate_combined_calibration(pipeline, cal_df, copula_params=copula_params)

        # 8. Correlation analysis
        self._analyze_minutes_rate_correlation(cal_df)

        # 9. Save all (frozen + retrained)
        pipeline.save_all(str(self.run_dir))

        # Save feature config
        feature_config = {"minutes_features": pipeline.minutes_features}
        for stat in pipeline.rate_models:
            feature_config[f"{stat}_rate_features"] = pipeline.rate_features.get(stat, {})
        with open(self.feature_config_path, "w") as f:
            json.dump(feature_config, f, indent=4)

        # 10. Sanity check
        self._run_sanity_check(pipeline, cal_df)

        # 11. Finalize: rename _incomplete → final
        final_run_dir = self.run_dir.parent / self._final_run_dir_name
        self.run_dir.rename(final_run_dir)
        self.run_dir = final_run_dir
        logger.info(f"Surgical retrain complete. Artifacts in {self.run_dir}")

    def _resolve_hyperparams_partial(
        self,
        train_df: pd.DataFrame,
        pipeline: PlayerPropsModelPipeline,
        retrain_stats: list[str] | None,
        retrain_minutes: bool,
        base_model_dir: str,
    ) -> dict:
        """
        Resolve hyperparameters for surgical retrain.

        Priority:
        1. If --hyperparams-path provided, load from that file
        2. If --tune enabled, run tuning for retrained components only
        3. Otherwise, load from base model's best_hyperparams.json
        4. If no hyperparams found anywhere, return empty (use XGBoost defaults)
        """
        # Option 1: Explicit hyperparams file
        if self.hyperparams_path:
            logger.info(f"Loading hyperparameters from {self.hyperparams_path}")
            with open(self.hyperparams_path) as f:
                hyperparams = json.load(f)
            with open(self.hyperparams_config_path, "w") as f:
                json.dump(hyperparams, f, indent=4)
            return hyperparams

        # Option 2: Run fresh tuning for retrained components only
        if self.tune_hyperparams:
            logger.info("Running hyperparameter tuning for retrained components...")
            tuner = QuantileHyperparameterTuner(
                n_trials=self.tuning_trials,
                timeout=self.tuning_timeout,
                per_quantile=self.tuning_per_quantile,
                pruning=True,
                val_fraction=0.15,
            )

            hyperparams = {}

            if retrain_minutes:
                logger.info("Tuning Minutes Model...")
                minutes_df = train_df[train_df["actual_minutes"] > 0].copy()
                minutes_features = pipeline.minutes_features
                if isinstance(minutes_features, dict):
                    all_features = list(set(f for feats in minutes_features.values() for f in feats))
                else:
                    all_features = minutes_features
                X = minutes_df[all_features].fillna(0)
                y = minutes_df["actual_minutes"]
                if self.tuning_per_quantile:
                    configs = tuner.tune_per_quantile(X, y)
                    hyperparams["minutes"] = {str(q): cfg.to_dict() for q, cfg in configs.items()}
                else:
                    config = tuner.tune(X, y)
                    hyperparams["minutes"] = config.to_dict()

            if retrain_stats:
                for stat in retrain_stats:
                    logger.info(f"Tuning {stat.upper()} Rate Model...")
                    target = f"{stat}_per_min"
                    rate_df = train_df[(train_df["actual_minutes"] >= 10) & (train_df[target].notna())].copy()
                    rate_features = pipeline.rate_features.get(stat, {})
                    if isinstance(rate_features, dict):
                        all_features = list(set(f for feats in rate_features.values() for f in feats))
                    else:
                        all_features = rate_features
                    X = rate_df[all_features].fillna(0)
                    y = rate_df[target]
                    if self.tuning_per_quantile:
                        configs = tuner.tune_per_quantile(X, y)
                        hyperparams[stat] = {str(q): cfg.to_dict() for q, cfg in configs.items()}
                    else:
                        config = tuner.tune(X, y)
                        hyperparams[stat] = config.to_dict()

            with open(self.hyperparams_config_path, "w") as f:
                json.dump(hyperparams, f, indent=4)
            logger.info(f"Saved tuned hyperparameters to {self.hyperparams_config_path}")
            return hyperparams

        # Option 3: Load from base model directory
        base_hp_path = Path(base_model_dir) / "best_hyperparams.json"
        if base_hp_path.exists():
            logger.info(f"Loading existing hyperparameters from {base_hp_path}")
            with open(base_hp_path) as f:
                hyperparams = json.load(f)
            with open(self.hyperparams_config_path, "w") as f:
                json.dump(hyperparams, f, indent=4)
            return hyperparams

        # Option 4: No hyperparams found
        logger.info("No hyperparameters found, using XGBoost defaults")
        return {}

    def _save_run_config(self, train_seasons, calibration_season, cal_end_date=None):
        """Save run configuration for reproducibility."""
        config = {
            "train_seasons": train_seasons,
            "calibration_season": calibration_season,
            "cal_end_date": str(cal_end_date) if cal_end_date else None,
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
        for stat in ["pts", "reb", "ast"]:
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
        for stat in ["pts", "reb", "ast"]:
            logger.info(f"Selecting {stat.upper()} features (per quantile)...")
            target = f"{stat}_per_min"
            rate_df = df[(df["actual_minutes"] >= 10) & (df[target].notna())].fillna(0)
            candidates = get_candidate_columns(rate_df, target)

            features[f"{stat}_rate_features"] = selector.select_features_per_quantile(
                rate_df, target, candidates, model_name=f"{stat.upper()} Rate"
            )

        return features

    def _run_feature_selection_partial(
        self, df: pd.DataFrame, retrain_stats: list[str] | None, retrain_minutes: bool
    ) -> dict:
        """Run feature selection only for specified components."""
        logger.info("Running Partial Feature Selection (retrained components only)...")
        selector = FeatureSelector(n_splits=3)
        features = {}

        if retrain_minutes:
            logger.info("Selecting Minutes features (per quantile)...")
            target = "actual_minutes"
            candidates = get_candidate_columns(df, target)
            minutes_df = df[df["actual_minutes"] > 0].fillna(0)
            features["minutes_features"] = selector.select_features_per_quantile(
                minutes_df, target, candidates, model_name="Minutes"
            )

        if retrain_stats:
            for stat in retrain_stats:
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

        for stat in ["pts", "reb", "ast"]:
            pipeline.rate_features[stat] = feature_config[f"{stat}_rate_features"]

        # Extract hyperparams for each model if provided
        minutes_hyperparams = hyperparams.get("minutes") if hyperparams else None
        rate_hyperparams = (
            {stat: hyperparams.get(stat) for stat in ["pts", "reb", "ast"]} if hyperparams else {}
        )

        # Train
        pipeline.train_minutes_model(df, hyperparams=minutes_hyperparams)
        pipeline.train_rate_models(df, stats=["pts", "reb", "ast"], hyperparams=rate_hyperparams)

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

        # Hurdle models (e.g., THREES legacy C3)
        hurdle_models = getattr(pipeline, "hurdle_models", {})
        for stat, hurdle_model in hurdle_models.items():
            logger.info(f"Evaluating {stat.upper()} Hurdle Model...")
            mask = (df["actual_minutes"] >= 10) & (df[f"{stat}_per_min"].notna())
            reports = self._calibrate_hurdle_model(
                model=hurdle_model,
                df=df,
                actual_col=f"{stat}_per_min",
                filter_mask=mask,
                name=f"{stat}_hurdle",
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

    def _calibrate_hurdle_model(self, model, df, actual_col, filter_mask, name) -> list[dict]:
        """Evaluate calibration for a hurdle model (e.g., THREES)."""
        filtered = df[filter_mask].copy().reset_index(drop=True)

        X = filtered[model.all_feature_names].fillna(0)
        y_actual = filtered[actual_col].values

        if X.empty:
            logger.warning(f"No validation data for {name}")
            return []

        # Predict quantiles using hurdle model
        preds = model.predict_quantiles(X)

        # Analyze zero prediction accuracy
        p_zero = model.predict_p_zero(X)
        actual_zeros = (y_actual == 0)
        predicted_zeros = p_zero > 0.5  # Threshold at 0.5
        zero_accuracy = (actual_zeros == predicted_zeros).mean()
        actual_zero_rate = actual_zeros.mean()
        pred_zero_rate = p_zero.mean()

        logger.info(f"  Zero prediction: accuracy={zero_accuracy:.3f}, actual_rate={actual_zero_rate:.3f}, pred_rate={pred_zero_rate:.3f}")

        reports = []
        for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
            pred_col = f"q{int(q * 100):02d}"

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

    def _evaluate_combined_calibration(  # noqa: C901
        self, pipeline: PlayerPropsModelPipeline, df: pd.DataFrame, sample_size: int = 2000,
        copula_params: dict | None = None,
    ) -> dict:
        """
        Evaluate end-to-end calibration: Monte Carlo (minutes × rate) vs actual totals.

        This catches calibration drift from multiplying two uncertain quantities.
        """
        logger.info("\n=== Combined Calibration (Minutes × Rate → Total) ===")

        # Evaluate all stats that have trained rate models (including hurdle models)
        hurdle_models = getattr(pipeline, "hurdle_models", {})
        eval_stats = [
            s for s in ["pts", "reb", "ast"]
            if s in pipeline.rate_models or s in hurdle_models
        ]

        # Filter stats to only those with actual columns in the data
        eval_stats = [s for s in eval_stats if f"actual_{s}" in df.columns]

        # Filter to valid rows with actual stats
        valid_mask = df["actual_minutes"] >= 10
        for stat in eval_stats:
            valid_mask &= df[f"actual_{stat}"].notna()

        eval_df = df[valid_mask].copy()

        # Sample for speed (full eval is slow)
        if len(eval_df) > sample_size:
            eval_df = eval_df.sample(sample_size, random_state=42)

        logger.info(f"Evaluating {len(eval_df)} samples...")

        # Create predictor
        mc = MonteCarloPredictor(pipeline, n_samples=1000, random_state=42, copula_params=copula_params)

        # Collect predictions and actuals
        results = {stat: {"predictions": [], "actuals": []} for stat in eval_stats}
        failure_count = 0

        for idx, row in eval_df.iterrows():
            # Build feature dict from row
            features = {col: row[col] for col in pipeline.minutes_model.all_feature_names if col in row.index}
            for stat in eval_stats:
                if stat in pipeline.rate_models:
                    for feat in pipeline.rate_models[stat].all_feature_names:
                        if feat in row.index:
                            features[feat] = row[feat]
                elif stat in hurdle_models:
                    # Use all_feature_names (classifier + positive models) for MC sampling
                    for feat in hurdle_models[stat].all_feature_names:
                        if feat in row.index:
                            features[feat] = row[feat]

            try:
                preds = mc.predict(
                    player_id=int(row["player_id"]),
                    game_id=str(row["game_id"]),
                    features=features,
                    stats=eval_stats,
                )

                for stat in eval_stats:
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
                failure_count += 1
                if failure_count <= 5:  # Log first 5 failures at INFO level
                    logger.info(f"Prediction failed for row {idx}: {e}")
                continue

        if failure_count > 0:
            logger.warning(
                f"{failure_count} of {len(eval_df)} predictions failed during combined calibration"
            )

        # Calculate calibration for each stat
        reports = {}

        for stat in eval_stats:
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
    parser.add_argument("--train-seasons", nargs="+", default=["22023", "22024"], help="Seasons for training")
    parser.add_argument("--cal-season", default="22025", help="Season for calibration (holdout)")
    parser.add_argument(
        "--cal-end-date",
        type=str,
        default=None,
        help="End date for calibration data (exclusive, YYYY-MM-DD). Reserves later dates for backtesting.",
    )

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

    # Surgical retrain options
    parser.add_argument(
        "--retrain-stats",
        nargs="+",
        default=None,
        help="Surgically retrain only these stat rate models (e.g. --retrain-stats reb ast). Requires --base-model-dir.",
    )
    parser.add_argument(
        "--retrain-minutes",
        action="store_true",
        help="Surgically retrain only the minutes model. Requires --base-model-dir.",
    )
    parser.add_argument(
        "--base-model-dir",
        type=str,
        default=None,
        help="Path to existing production pipeline to load frozen models from (e.g. src/models/artifacts/production).",
    )
    parser.add_argument(
        "--reselect-features",
        action="store_true",
        help="Re-run feature selection for retrained components (default: keep existing features).",
    )

    args = parser.parse_args()

    # Determine mode: surgical retrain vs full retrain
    is_partial = args.retrain_stats is not None or args.retrain_minutes
    if is_partial and not args.base_model_dir:
        parser.error("--base-model-dir is required when using --retrain-stats or --retrain-minutes")
    if args.base_model_dir and not is_partial:
        parser.error("--base-model-dir requires --retrain-stats and/or --retrain-minutes")

    orchestrator = TrainingOrchestrator(
        tune_hyperparams=args.tune,
        tuning_trials=args.tuning_trials,
        tuning_timeout=args.tuning_timeout,
        tuning_per_quantile=args.tuning_per_quantile,
        hyperparams_path=args.hyperparams_path,
    )
    cal_end = datetime.strptime(args.cal_end_date, "%Y-%m-%d").date() if args.cal_end_date else None

    if is_partial:
        orchestrator.run_partial(
            train_seasons=args.train_seasons,
            calibration_season=args.cal_season,
            cal_end_date=cal_end,
            base_model_dir=args.base_model_dir,
            retrain_stats=args.retrain_stats,
            retrain_minutes=args.retrain_minutes,
            reselect_features=args.reselect_features,
        )
    else:
        orchestrator.run(
            train_seasons=args.train_seasons,
            calibration_season=args.cal_season,
            cal_end_date=cal_end,
        )
