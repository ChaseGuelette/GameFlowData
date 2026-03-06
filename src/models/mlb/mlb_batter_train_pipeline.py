"""MLB Batter Training Pipeline.

End-to-end orchestrator for training batter prop models:
load data, select features, optionally tune hyperparameters, train,
calibrate on holdout, sanity check, and save artifacts.

Supports both NegBin stats (hits, TB, RBI, runs) and binary (HR).

Usage:
    python src/models/mlb/mlb_batter_train_pipeline.py \\
        --stat hits \\
        --train-seasons 2023 2024 \\
        --cal-season 2025 \\
        --cal-end-date 2025-07-01
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

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.models.mlb.mlb_batter_feature_store import (
    BATTER_FEATURE_MAP,
    BATTER_STAT_TARGET,
    MLBBatterFeatureStore,
    get_features_for_stat,
)
from src.models.mlb.mlb_binary_model import MLBBinaryModel
from src.processing.feature_selection import ImprovedFeatureSelector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBatterTrainingPipeline")


class MLBBatterTrainingOrchestrator:
    """Orchestrates end-to-end MLB batter model training."""

    CALIBRATION_TOLERANCE = 0.05
    CALIBRATION_HARD_FAIL = 0.10

    def __init__(
        self,
        stat: str = "hits",
        base_artifacts_dir: str = "src/models/mlb/artifacts",
        tune_hyperparams: bool = False,
        tuning_trials: int = 50,
        feature_tolerance: float = 0.02,
    ):
        if stat not in BATTER_FEATURE_MAP:
            raise ValueError(f"Unknown stat: {stat}. Valid: {list(BATTER_FEATURE_MAP.keys())}")

        self.stat = stat
        self.engine = get_engine()
        self.feature_store = MLBBatterFeatureStore(self.engine)
        self.feature_tolerance = feature_tolerance
        self.tune_hyperparams = tune_hyperparams
        self.tuning_trials = tuning_trials
        self.is_binary = stat == "home_runs"

        # Create timestamped run directory
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self._final_run_dir_name = f"run_batter_{stat}_{timestamp_str}"
        self.run_dir = Path(base_artifacts_dir) / f"run_batter_{stat}_{timestamp_str}_incomplete"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Initialized MLB Batter Training Run: %s, stat=%s", timestamp_str, stat)

    def run(
        self,
        train_seasons: list[int],
        cal_season: int,
        cal_end_date: str | None = None,
    ):
        """Execute the full training pipeline."""
        self._save_run_config(train_seasons, cal_season, cal_end_date)

        # Step 1: Load training data
        logger.info("Step 1: Loading training data...")
        train_df = self.feature_store.get_training_dataset(seasons=train_seasons, stat=self.stat)
        train_df = self.feature_store.enrich_with_matchup_features(train_df)
        logger.info("Training data: %d rows", len(train_df))

        # Step 2: Load calibration data
        logger.info("Step 2: Loading calibration data...")
        cal_df = self.feature_store.get_training_dataset(seasons=[cal_season], stat=self.stat)
        cal_df = self.feature_store.enrich_with_matchup_features(cal_df)
        if cal_end_date:
            cal_df = cal_df[cal_df["game_date"] <= cal_end_date].reset_index(drop=True)
        logger.info("Calibration data: %d rows", len(cal_df))

        if self.is_binary:
            self._run_binary_pipeline(train_df, cal_df, train_seasons, cal_season, cal_end_date)
        else:
            self._run_negbin_pipeline(train_df, cal_df, train_seasons, cal_season, cal_end_date)

        # Finalize
        final_dir = self.run_dir.parent / self._final_run_dir_name
        self.run_dir.rename(final_dir)
        self.run_dir = final_dir
        logger.info("Training pipeline completed. Artifacts in %s", self.run_dir)

    # ------------------------------------------------------------------
    # Binary pipeline (HR)
    # ------------------------------------------------------------------

    def _run_binary_pipeline(self, train_df, cal_df, train_seasons, cal_season, cal_end_date):
        """Train binary classifier for HR >= 1."""
        # Prepare features
        feature_list = get_features_for_stat(self.stat)
        available = [f for f in feature_list if f in train_df.columns]
        missing = set(feature_list) - set(available)
        if missing:
            logger.warning("Missing features (will be 0-filled): %s", missing)
            for f in missing:
                train_df[f] = 0
                cal_df[f] = 0
            available = feature_list

        # Step 3: Feature selection
        logger.info("Step 3: Feature selection...")
        valid_df = train_df[train_df["actual"].notna()].copy()
        valid_df["target_binary"] = (valid_df["actual"] >= 1).astype(int)

        selector = ImprovedFeatureSelector(n_splits=3, tolerance=self.feature_tolerance)
        candidates = [c for c in available if c in valid_df.columns
                      and valid_df[c].dtype in ("float64", "float32", "int64", "int32")]
        # Use all candidates for binary (feature selection is simpler)
        selected = candidates
        logger.info("Using %d features for HR binary model", len(selected))

        # Step 5: Train
        logger.info("Step 5: Training HR binary model...")
        X_train = valid_df[selected].fillna(0)
        y_train = valid_df["target_binary"]

        model = MLBBinaryModel()
        metrics = model.fit(X_train, y_train)
        logger.info("Training metrics: %s", metrics)

        # Step 6-7: Evaluate on calibration set
        logger.info("Step 6-7: Evaluating on calibration set...")
        cal_valid = cal_df[cal_df["actual"].notna()].copy()
        for f in selected:
            if f not in cal_valid.columns:
                cal_valid[f] = 0
        X_cal = cal_valid[selected].fillna(0)
        y_cal = (cal_valid["actual"] >= 1).astype(int)

        proba = model.predict_proba(X_cal)
        actual_rate = y_cal.mean()
        predicted_rate = proba.mean()

        logger.info(
            "Calibration: actual_hr_rate=%.3f, predicted_rate=%.3f, gap=%.3f",
            actual_rate, predicted_rate, abs(actual_rate - predicted_rate),
        )

        # Step 8: Sanity check
        logger.info("Step 8: Sanity check...")
        sample_idx = np.random.RandomState(42).choice(len(X_cal), min(5, len(X_cal)), replace=False)
        for i in sample_idx:
            p = proba[i]
            actual = y_cal.iloc[i]
            logger.info("  Sample: P(HR)=%.3f, actual=%d", p, actual)
            if p < 0 or p > 1:
                raise ValueError(f"Invalid probability: {p}")

        # Step 9: Save
        logger.info("Step 9: Saving artifacts...")
        model.save(str(self.run_dir))

        self._save_feature_manifest({"binary": selected})
        self._save_calibration_report({
            "hr_binary": {
                "actual_rate": float(actual_rate),
                "predicted_rate": float(predicted_rate),
                "gap": float(abs(actual_rate - predicted_rate)),
                "n_cal": len(cal_valid),
            }
        })
        self._save_training_metadata(train_seasons, cal_season, cal_end_date, train_df, cal_df)

    # ------------------------------------------------------------------
    # NegBin pipeline (hits, TB, RBI, runs)
    # ------------------------------------------------------------------

    def _run_negbin_pipeline(self, train_df, cal_df, train_seasons, cal_season, cal_end_date):
        """Train quantile model for count stats."""
        from src.models.mlb.mlb_quantile_trainer import MLBPitcherKPipeline
        from src.models.quantile_trainer import QuantileModelConfig

        config = QuantileModelConfig(
            quantiles=(0.10, 0.25, 0.50, 0.75, 0.90),
            n_estimators=1000,
            max_depth=5,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            early_stopping_rounds=50,
            val_fraction=0.15,
        )

        # Step 3: Feature selection
        logger.info("Step 3: Running per-quantile feature selection...")
        excluded = {
            "game_id", "player_id", "game_date", "season", "team_id",
            "opp_team_id", "actual", "player_name",
        }
        candidates = [
            c for c in train_df.columns
            if c not in excluded and train_df[c].dtype in ("float64", "float32", "int64", "int32")
        ]

        valid_df = train_df[train_df["actual"].notna() & (train_df["actual"] >= 0)].fillna(0)
        selector = ImprovedFeatureSelector(n_splits=3, tolerance=self.feature_tolerance)
        selected = selector.select_features_per_quantile(
            valid_df, "actual", candidates, model_name=f"Batter {self.stat}"
        )

        for q, feats in selected.items():
            logger.info("  Q%.2f: %d features selected", q, len(feats))

        # Step 5: Train
        logger.info("Step 5: Training quantile models...")
        # Rename actual -> actual_so temporarily so the pipeline works
        # (MLBPitcherKPipeline expects 'actual_so')
        train_copy = valid_df.rename(columns={"actual": "actual_so"})
        pipeline = MLBPitcherKPipeline(config=config)
        pipeline.train(train_copy, feature_names_per_quantile=selected)

        # Step 6: Calibrate on holdout
        logger.info("Step 6: Computing calibration offsets...")
        model = pipeline.model
        cal_valid = cal_df[cal_df["actual"].notna() & (cal_df["actual"] >= 0)].fillna(0)
        X_cal = cal_valid[model.all_feature_names].fillna(0)
        y_cal = cal_valid["actual"].values

        preds = model.predict_quantiles(X_cal)
        for q in model.config.quantiles:
            pred_col = f"q{int(q * 100):02d}"
            pred_vals = preds[pred_col].values
            coverage = (y_cal <= pred_vals).mean()
            gap = abs(coverage - q)

            if gap > model.RECALIBRATION_GAP_THRESHOLD:
                residuals = y_cal - pred_vals
                delta = float(np.quantile(residuals, q))
                model.calibration_offsets[q] = delta
                recal_coverage = (y_cal <= (pred_vals + delta)).mean()
                logger.info("  Q%.2f: coverage=%.3f -> %.3f (delta=%+.4f)", q, coverage, recal_coverage, delta)
            else:
                model.calibration_offsets[q] = 0.0
                logger.info("  Q%.2f: coverage=%.3f (OK)", q, coverage)

        # Step 7: Calibration report
        logger.info("Step 7: Evaluating calibration...")
        preds = model.predict_quantiles(X_cal)
        reports = []
        for q in model.config.quantiles:
            pred_col = f"q{int(q * 100):02d}"
            coverage = float((y_cal <= preds[pred_col].values).mean())
            gap = coverage - q
            offset = model.calibration_offsets.get(q, 0.0)
            reports.append({
                "quantile": q, "coverage": coverage, "target": q,
                "gap": float(gap), "calibration_offset": offset,
            })
            logger.info("  Q%.2f: coverage=%.3f [gap=%+.3f]", q, coverage, gap)

        # Step 8: Sanity check
        logger.info("Step 8: Sanity check...")
        sample_size = min(5, len(cal_valid))
        sample = cal_valid.sample(sample_size, random_state=42)
        for _, row in sample.iterrows():
            feats = pd.DataFrame([{f: row.get(f, 0) for f in model.all_feature_names}])
            pred = model.predict_quantiles(feats)
            actual = row["actual"]
            logger.info(
                "  Player %d: Q50=%.1f, Q10=%.1f, Q90=%.1f, Actual=%.0f",
                row["player_id"], pred["q50"].iloc[0], pred["q10"].iloc[0],
                pred["q90"].iloc[0], actual,
            )

        # Step 9: Save
        logger.info("Step 9: Saving artifacts...")
        pipeline.save(str(self.run_dir))
        self._save_feature_manifest(selected)
        self._save_calibration_report({f"batter_{self.stat}": reports})
        self._save_training_metadata(train_seasons, cal_season, cal_end_date, train_df, cal_df)

    # ------------------------------------------------------------------
    # Artifact Saving Helpers
    # ------------------------------------------------------------------

    def _save_run_config(self, train_seasons, cal_season, cal_end_date):
        config = {
            "stat": self.stat,
            "model_type": "binary" if self.is_binary else "negbin",
            "train_seasons": train_seasons,
            "cal_season": cal_season,
            "cal_end_date": cal_end_date,
            "timestamp": self.timestamp.isoformat(),
            "tune_hyperparams": self.tune_hyperparams,
            "feature_tolerance": self.feature_tolerance,
        }
        with open(self.run_dir / "run_config.json", "w") as f:
            json.dump(config, f, indent=4)

    def _save_feature_manifest(self, selected_features):
        manifest = {str(q): feats for q, feats in selected_features.items()}
        with open(self.run_dir / "feature_manifest.json", "w") as f:
            json.dump(manifest, f, indent=4)

    def _save_calibration_report(self, reports):
        with open(self.run_dir / "calibration_report_combined.json", "w") as f:
            json.dump(reports, f, indent=4)

    def _save_training_metadata(self, train_seasons, cal_season, cal_end_date, train_df, cal_df):
        git_hash = _get_git_hash()
        metadata = {
            "stat": self.stat,
            "train_seasons": train_seasons,
            "cal_season": cal_season,
            "cal_end_date": cal_end_date,
            "train_rows": len(train_df),
            "cal_rows": len(cal_df),
            "feature_count": len(get_features_for_stat(self.stat)),
            "timestamp": self.timestamp.isoformat(),
            "git_hash": git_hash,
        }
        with open(self.run_dir / "training_metadata.json", "w") as f:
            json.dump(metadata, f, indent=4)


def _get_git_hash() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train MLB Batter Prop Models")
    parser.add_argument("--stat", type=str, default="hits",
                        choices=list(BATTER_FEATURE_MAP.keys()),
                        help="Batter stat to train (default: hits)")
    parser.add_argument("--train-seasons", nargs="+", type=int, default=[2023, 2024])
    parser.add_argument("--cal-season", type=int, default=2025)
    parser.add_argument("--cal-end-date", type=str, default=None)
    parser.add_argument("--tune", action="store_true")
    parser.add_argument("--tuning-trials", type=int, default=50)
    parser.add_argument("--feature-tolerance", type=float, default=0.02)
    parser.add_argument("--output-dir", type=str, default="src/models/mlb/artifacts")

    args = parser.parse_args()

    orchestrator = MLBBatterTrainingOrchestrator(
        stat=args.stat,
        base_artifacts_dir=args.output_dir,
        tune_hyperparams=args.tune,
        tuning_trials=args.tuning_trials,
        feature_tolerance=args.feature_tolerance,
    )

    orchestrator.run(
        train_seasons=args.train_seasons,
        cal_season=args.cal_season,
        cal_end_date=args.cal_end_date,
    )
