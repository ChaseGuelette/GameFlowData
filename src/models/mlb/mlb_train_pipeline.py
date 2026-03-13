"""MLB Pitcher Strikeout Training Pipeline.

End-to-end orchestrator for training pitcher K quantile models:
load data, select features, optionally tune hyperparameters, train,
calibrate on holdout, sanity check, and save artifacts.

Analogous to src/models/train_pipeline.py (NBA) but simplified:
- Single stat (pitcher strikeouts) instead of minutes × rate
- No copula (single prediction target)
- Direct SO prediction (no minutes decomposition)

Usage:
    python src/models/mlb/mlb_train_pipeline.py \\
        --train-seasons 2023 2024 \\
        --cal-season 2025 \\
        --cal-end-date 2025-07-01 \\
        --tune --tuning-trials 50
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.models.hyperparameter_tuner import QuantileHyperparameterTuner
from src.models.mlb.mlb_feature_store import PITCHER_K_FEATURES, MLBFeatureStore
from src.models.mlb.mlb_monte_carlo import MLBMonteCarloPredictor
from src.models.mlb.mlb_quantile_trainer import (
    MLB_PITCHER_K_CONFIG,
    MLBPitcherKPipeline,
)
from src.processing.feature_selection import ImprovedFeatureSelector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBTrainingPipeline")


class MLBTrainingOrchestrator:
    """Orchestrates end-to-end MLB pitcher K model training."""

    CALIBRATION_TOLERANCE = 0.05
    CALIBRATION_HARD_FAIL = 0.10

    def __init__(
        self,
        base_artifacts_dir: str = "src/models/mlb/artifacts",
        tune_hyperparams: bool = False,
        tuning_trials: int = 50,
        tuning_timeout: int | None = None,
        feature_tolerance: float = 0.02,
    ):
        self.engine = get_engine()
        self.feature_store = MLBFeatureStore(self.engine)
        self.feature_tolerance = feature_tolerance

        self.tune_hyperparams = tune_hyperparams
        self.tuning_trials = tuning_trials
        self.tuning_timeout = tuning_timeout

        # Create timestamped run directory with _incomplete suffix
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self._final_run_dir_name = f"run_{timestamp_str}"
        self.run_dir = Path(base_artifacts_dir) / f"run_{timestamp_str}_incomplete"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized MLB Training Run: {timestamp_str}")
        logger.info(f"Artifacts will be saved to: {self.run_dir} (renamed on completion)")
        if tune_hyperparams:
            logger.info(f"Hyperparameter tuning ENABLED: {tuning_trials} trials")

    def run(
        self,
        train_seasons: list[int],
        cal_season: int,
        cal_end_date: str | None = None,
        n_simulations: int = 10_000,
    ):
        """Execute the full 10-step MLB training pipeline.

        Args:
            train_seasons: Seasons for training data (e.g. [2023, 2024]).
            cal_season: Season for calibration holdout (e.g. 2025).
            cal_end_date: Optional cutoff within cal season (YYYY-MM-DD).
            n_simulations: MC simulations for sanity check.
        """
        self._save_run_config(train_seasons, cal_season, cal_end_date)

        # Step 1: Load training data
        logger.info("Step 1: Loading training data...")
        train_df = self.feature_store.get_training_dataset(seasons=train_seasons)
        train_df = self.feature_store.enrich_with_matchup_features(train_df)
        logger.info(f"Training data: {len(train_df):,} rows")

        # Step 2: Load calibration data
        logger.info("Step 2: Loading calibration data...")
        cal_df = self.feature_store.get_training_dataset(seasons=[cal_season])
        cal_df = self.feature_store.enrich_with_matchup_features(cal_df)
        if cal_end_date:
            pre_filter = len(cal_df)
            cal_end_date = pd.Timestamp(cal_end_date).date()
            cal_df = cal_df[cal_df["game_date"] <= cal_end_date].reset_index(drop=True)
            logger.info(f"Calibration data: {len(cal_df):,} rows (filtered from {pre_filter:,}, end={cal_end_date})")
        else:
            logger.info(f"Calibration data: {len(cal_df):,} rows")

        # Step 3: Feature selection
        selected_features = self._run_feature_selection(train_df)

        # Step 4: Optional hyperparameter tuning
        config = self._run_hyperparameter_tuning(train_df, selected_features)

        # Step 5: Train quantile models
        pipeline = self._train_models(train_df, selected_features, config)

        # Step 6: Calibrate on holdout
        self._calibrate_on_holdout(pipeline, cal_df)

        # Step 7: Combined calibration report
        cal_report = self._evaluate_calibration(pipeline, cal_df)

        # Step 8: Sanity check with Monte Carlo
        self._run_sanity_check(pipeline, cal_df, n_simulations)

        # Step 9: Save artifacts
        pipeline.save(str(self.run_dir))
        self._save_feature_manifest(selected_features)
        self._save_calibration_report(cal_report)
        self._save_training_metadata(train_seasons, cal_season, cal_end_date, train_df, cal_df)

        # Step 10: Finalize
        final_dir = self.run_dir.parent / self._final_run_dir_name
        self.run_dir.rename(final_dir)
        self.run_dir = final_dir
        logger.info(f"Training pipeline completed successfully. Artifacts in {self.run_dir}")

    # ------------------------------------------------------------------
    # Step 3: Feature Selection
    # ------------------------------------------------------------------

    def _run_feature_selection(self, df: pd.DataFrame) -> dict[float, list[str]]:
        """Run per-quantile feature selection on training data."""
        logger.info("Step 3: Running per-quantile feature selection...")
        selector = ImprovedFeatureSelector(
            n_splits=3,
            tolerance=self.feature_tolerance,
        )

        # Identify candidate columns (exclude IDs, target, non-numeric)
        excluded = {
            "game_id",
            "player_id",
            "game_date",
            "season",
            "team_id",
            "opp_team_id",
            "actual_so",
            "player_name",
        }
        candidates = [
            c for c in df.columns if c not in excluded and df[c].dtype in ("float64", "float32", "int64", "int32")
        ]

        logger.info(f"Candidate features: {len(candidates)}")

        valid_df = df[df["actual_so"].notna() & (df["actual_so"] >= 0)].fillna(0)
        selected = selector.select_features_per_quantile(valid_df, "actual_so", candidates, model_name="Pitcher K")

        for q, feats in selected.items():
            logger.info(f"  Q{q:.2f}: {len(feats)} features selected")

        return selected

    # ------------------------------------------------------------------
    # Step 4: Hyperparameter Tuning
    # ------------------------------------------------------------------

    def _run_hyperparameter_tuning(
        self,
        df: pd.DataFrame,
        selected_features: dict[float, list[str]],
    ) -> MLB_PITCHER_K_CONFIG.__class__ | None:
        """Optionally tune hyperparameters. Returns config or None for defaults."""
        if not self.tune_hyperparams:
            logger.info("Step 4: Hyperparameter tuning DISABLED, using defaults")
            return None

        logger.info("Step 4: Running hyperparameter tuning...")

        # Use union of all per-quantile features for tuning
        all_features = sorted(set(f for feats in selected_features.values() for f in feats))

        valid_df = df[df["actual_so"].notna() & (df["actual_so"] >= 0)].copy()
        X = valid_df[all_features].fillna(0)
        y = valid_df["actual_so"]

        tuner = QuantileHyperparameterTuner(
            n_trials=self.tuning_trials,
            timeout=self.tuning_timeout,
            pruning=True,
            val_fraction=0.15,
        )

        config = tuner.tune(X, y, quantiles=MLB_PITCHER_K_CONFIG.quantiles, feature_names=all_features)

        # Save tuning results
        tuner.save_best_config(str(self.run_dir / "best_hyperparams.json"))
        logger.info(f"Best calibration gap: {tuner.result.best_calibration_gap:.4f}")

        return config

    # ------------------------------------------------------------------
    # Step 5: Train Models
    # ------------------------------------------------------------------

    def _train_models(
        self,
        df: pd.DataFrame,
        selected_features: dict[float, list[str]],
        config=None,
    ) -> MLBPitcherKPipeline:
        """Train pitcher K quantile models."""
        logger.info("Step 5: Training quantile models...")

        pipeline = MLBPitcherKPipeline(config=config)
        pipeline.train(df, feature_names_per_quantile=selected_features)

        return pipeline

    # ------------------------------------------------------------------
    # Step 6: Calibrate on Holdout
    # ------------------------------------------------------------------

    def _calibrate_on_holdout(self, pipeline: MLBPitcherKPipeline, cal_df: pd.DataFrame):
        """Compute conformal calibration offsets on holdout data.

        The QuantileModelSuite already computes offsets during training on
        its internal validation split. Here we recompute on the dedicated
        calibration set for a more robust estimate.
        """
        logger.info("Step 6: Computing calibration offsets on holdout...")

        if pipeline.model is None:
            raise RuntimeError("Pipeline has no trained model")

        model = pipeline.model
        X = cal_df[model.all_feature_names].fillna(0)
        y = cal_df["actual_so"].values

        preds = model.predict_quantiles(X)

        for q in model.config.quantiles:
            pred_col = f"q{int(q * 100):02d}"
            pred_vals = preds[pred_col].values
            coverage = (y <= pred_vals).mean()
            gap = abs(coverage - q)

            if gap > model.RECALIBRATION_GAP_THRESHOLD:
                residuals = y - pred_vals
                delta = float(np.quantile(residuals, q))
                model.calibration_offsets[q] = delta
                recal_coverage = (y <= (pred_vals + delta)).mean()
                logger.info(f"  Q{q:.2f}: coverage={coverage:.3f} -> {recal_coverage:.3f} (delta={delta:+.4f})")
            else:
                model.calibration_offsets[q] = 0.0
                logger.info(f"  Q{q:.2f}: coverage={coverage:.3f} (OK, no offset needed)")

    # ------------------------------------------------------------------
    # Step 7: Calibration Report
    # ------------------------------------------------------------------

    def _evaluate_calibration(self, pipeline: MLBPitcherKPipeline, cal_df: pd.DataFrame) -> dict:
        """Generate calibration report on holdout data (post-offset)."""
        logger.info("Step 7: Evaluating calibration (post-offset)...")

        model = pipeline.model
        X = cal_df[model.all_feature_names].fillna(0)
        y = cal_df["actual_so"].values

        # predict_quantiles now applies calibration offsets
        preds = model.predict_quantiles(X)

        reports = []
        for q in model.config.quantiles:
            pred_col = f"q{int(q * 100):02d}"
            coverage = float((y <= preds[pred_col].values).mean())
            gap = coverage - q
            offset = model.calibration_offsets.get(q, 0.0)

            status = "OK" if abs(gap) <= self.CALIBRATION_TOLERANCE else f"GAP {gap:+.3f}"
            logger.info(f"  Q{q:.2f}: coverage={coverage:.3f} [{status}] offset={offset:+.4f}")

            reports.append(
                {
                    "quantile": q,
                    "coverage": coverage,
                    "target": q,
                    "gap": float(gap),
                    "calibration_offset": offset,
                }
            )

        worst_gap = max(abs(r["gap"]) for r in reports) if reports else 0
        logger.info(f"Worst calibration gap: {worst_gap:.3f}")

        if worst_gap > self.CALIBRATION_HARD_FAIL:
            logger.warning(
                f"Calibration FAILED: worst gap = {worst_gap:.1%} (threshold: {self.CALIBRATION_HARD_FAIL:.0%})"
            )
            with open(self.run_dir / "CALIBRATION_FAILED.txt", "w") as f:
                f.write(
                    f"Worst calibration gap: {worst_gap:.1%}\n"
                    f"Hard fail threshold: {self.CALIBRATION_HARD_FAIL:.0%}\n"
                    f"DO NOT deploy without review."
                )
        elif worst_gap > self.CALIBRATION_TOLERANCE:
            logger.warning(f"Calibration warning: worst gap = {worst_gap:.1%}")

        return {"pitcher_strikeouts": reports}

    # ------------------------------------------------------------------
    # Step 8: Sanity Check
    # ------------------------------------------------------------------

    def _run_sanity_check(
        self,
        pipeline: MLBPitcherKPipeline,
        cal_df: pd.DataFrame,
        n_simulations: int,
    ):
        """Run Monte Carlo sanity check on a small sample."""
        logger.info("Step 8: Running Monte Carlo sanity check...")

        if cal_df.empty:
            logger.warning("Calibration data is empty, skipping sanity check")
            return

        sample_size = min(5, len(cal_df))
        sample_df = cal_df.sample(sample_size, random_state=42)

        mc = MLBMonteCarloPredictor(pipeline, n_samples=n_simulations, random_state=42)

        feature_names = pipeline.model.all_feature_names
        player_games = []
        for _, row in sample_df.iterrows():
            features = {f: row[f] for f in feature_names if f in row.index}
            player_games.append((int(row["player_id"]), int(row["game_id"]), features))

        preds = mc.predict_batch(player_games)

        for i, pred in enumerate(preds):
            row = sample_df.iloc[i]
            actual = row["actual_so"]
            logger.info(
                f"  Player {pred.player_id}: "
                f"Q50={pred.q50:.1f}, Mean={pred.mean:.1f}, "
                f"Q10={pred.q10:.1f}, Q90={pred.q90:.1f}, "
                f"Actual={actual:.0f}"
            )

            # Bounds checks
            if pred.mean < 0:
                raise ValueError(f"Negative mean: {pred.mean}")
            if pred.q10 > pred.q90:
                raise ValueError(f"Quantile inversion: Q10={pred.q10} > Q90={pred.q90}")
            if pred.median > 25:
                logger.warning(f"Unusually high median: {pred.median}")

        logger.info("Sanity check PASSED")

    # ------------------------------------------------------------------
    # Artifact Saving Helpers
    # ------------------------------------------------------------------

    def _save_run_config(self, train_seasons, cal_season, cal_end_date):
        config = {
            "train_seasons": train_seasons,
            "cal_season": cal_season,
            "cal_end_date": cal_end_date,
            "timestamp": self.timestamp.isoformat(),
            "tune_hyperparams": self.tune_hyperparams,
            "tuning_trials": self.tuning_trials,
            "feature_tolerance": self.feature_tolerance,
            "calibration_tolerance": self.CALIBRATION_TOLERANCE,
            "calibration_hard_fail": self.CALIBRATION_HARD_FAIL,
        }
        with open(self.run_dir / "run_config.json", "w") as f:
            json.dump(config, f, indent=4)

    def _save_feature_manifest(self, selected_features: dict[float, list[str]]):
        manifest = {str(q): feats for q, feats in selected_features.items()}
        with open(self.run_dir / "feature_manifest.json", "w") as f:
            json.dump(manifest, f, indent=4)
        logger.info(f"Saved feature manifest to {self.run_dir / 'feature_manifest.json'}")

    def _save_calibration_report(self, reports: dict):
        with open(self.run_dir / "calibration_report_combined.json", "w") as f:
            json.dump(reports, f, indent=4)
        logger.info(f"Saved calibration report to {self.run_dir / 'calibration_report_combined.json'}")

    def _save_training_metadata(self, train_seasons, cal_season, cal_end_date, train_df, cal_df):
        git_hash = _get_git_hash()
        metadata = {
            "train_seasons": train_seasons,
            "cal_season": cal_season,
            "cal_end_date": cal_end_date,
            "train_rows": len(train_df),
            "cal_rows": len(cal_df),
            "feature_count": len(PITCHER_K_FEATURES),
            "timestamp": self.timestamp.isoformat(),
            "git_hash": git_hash,
        }
        with open(self.run_dir / "training_metadata.json", "w") as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Saved training metadata to {self.run_dir / 'training_metadata.json'}")


def _get_git_hash() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train MLB Pitcher Strikeout Models")
    parser.add_argument(
        "--train-seasons",
        nargs="+",
        type=int,
        default=[2023, 2024],
        help="Seasons for training data (e.g. 2023 2024)",
    )
    parser.add_argument(
        "--cal-season",
        type=int,
        default=2025,
        help="Season for calibration holdout (e.g. 2025)",
    )
    parser.add_argument(
        "--cal-end-date",
        type=str,
        default=None,
        help="Optional cutoff within calibration season (YYYY-MM-DD)",
    )
    parser.add_argument("--tune", action="store_true", help="Enable Optuna hyperparameter tuning")
    parser.add_argument("--tuning-trials", type=int, default=50, help="Number of Optuna trials")
    parser.add_argument(
        "--tuning-timeout",
        type=int,
        default=None,
        help="Max tuning time in seconds",
    )
    parser.add_argument(
        "--feature-tolerance",
        type=float,
        default=0.02,
        help="Tolerance for feature selection stability (higher = more features)",
    )
    parser.add_argument(
        "--n-simulations",
        type=int,
        default=10_000,
        help="MC simulations for sanity check",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="src/models/mlb/artifacts",
        help="Base output directory for artifacts",
    )

    args = parser.parse_args()

    orchestrator = MLBTrainingOrchestrator(
        base_artifacts_dir=args.output_dir,
        tune_hyperparams=args.tune,
        tuning_trials=args.tuning_trials,
        tuning_timeout=args.tuning_timeout,
        feature_tolerance=args.feature_tolerance,
    )

    orchestrator.run(
        train_seasons=args.train_seasons,
        cal_season=args.cal_season,
        cal_end_date=args.cal_end_date,
        n_simulations=args.n_simulations,
    )
