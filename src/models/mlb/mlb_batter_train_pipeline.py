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
        self._final_run_dir_name = f"mlb_run_batter_{stat}_{timestamp_str}"
        self.run_dir = Path(base_artifacts_dir) / f"mlb_run_batter_{stat}_{timestamp_str}_incomplete"
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
            from datetime import datetime as _dt
            _cutoff = _dt.strptime(cal_end_date, "%Y-%m-%d").date() if isinstance(cal_end_date, str) else cal_end_date
            cal_df = cal_df[cal_df["game_date"] <= _cutoff].reset_index(drop=True)
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

        candidates = [c for c in available if c in valid_df.columns
                      and valid_df[c].dtype in ("float64", "float32", "int64", "int32")]
        # Use all numeric candidates for binary
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
        """Train NegBin model for count stats (hits, TB, RBI, runs)."""
        from scipy.stats import nbinom as _nbinom

        from src.models.mlb.mlb_batter_feature_store import BATTER_STAT_MARKET_KEY
        from src.models.negbin_model import NegBinModel

        model_name = f"batter_{self.stat}"

        # Step 3: Feature selection — NLL-based (single feature set for NegBin)
        logger.info("Step 3: Running NLL-based feature selection for NegBin model...")
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
        selected_features = selector.select_features_nll(
            valid_df, "actual", candidates, model_name=f"Batter {self.stat}"
        )
        logger.info("NegBin feature set: %d features (NLL-based)", len(selected_features))

        X_train = valid_df[selected_features].fillna(0)
        y_train = valid_df["actual"]

        # Step 4: Hyperparameter tuning (optional)
        negbin_config = self._resolve_negbin_config(X_train, y_train)

        # Step 5: Train
        display_name = model_name.upper().replace("_", " ")
        print("\n" + "=" * 60)
        print(f"TRAINING MLB {display_name} NEGBIN MODEL")
        print("=" * 60)

        model = NegBinModel(config=negbin_config, model_name=model_name)
        fit_info = model.fit(X_train, y_train)
        logger.info("Fit info: %s", fit_info)

        # Step 6-7: PMF-based calibration (vectorized, ~5 sec)
        logger.info("Step 6-7: Computing PMF-based calibration metrics...")
        cal_valid = cal_df[cal_df["actual"].notna() & (cal_df["actual"] >= 0)].fillna(0)
        X_cal = cal_valid[selected_features].fillna(0)
        y_cal = cal_valid["actual"].values

        mu, alpha = model.predict_params(X_cal)
        cal_report = self._compute_negbin_calibration(
            y_cal, mu, alpha, cal_valid, self.stat
        )

        # Log key metrics
        logger.info("  Mean NLL: %.4f", cal_report["mean_nll"])
        logger.info("  Bias: predicted_mu=%.3f, actual_mean=%.3f, ratio=%.4f",
                     cal_report["mean_predicted_mu"], cal_report["mean_actual"],
                     cal_report["mu_actual_ratio"])
        logger.info("  Zero fraction: predicted=%.3f, actual=%.3f, gap=%+.3f",
                     cal_report["predicted_zero_frac"], cal_report["actual_zero_frac"],
                     cal_report["zero_frac_gap"])
        for entry in cal_report.get("prop_line_calibration", []):
            logger.info("  Line %.1f: model_over=%.3f, actual_over=%.3f, gap=%+.3f (n=%d)",
                         entry["line"], entry["model_over_rate"], entry["actual_over_rate"],
                         entry["gap"], entry["n"])
        logger.info("  Mu percentiles: %s", cal_report.get("mu_percentiles"))
        logger.info("  Alpha percentiles: %s", cal_report.get("alpha_percentiles"))

        # Step 8: Sanity check — show (mu, alpha, P(over line), actual)
        logger.info("Step 8: Sanity check (distributional parameters)...")
        market_key = BATTER_STAT_MARKET_KEY.get(self.stat, f"batter_{self.stat}")
        prop_line_col = f"prop_line_{market_key}"

        sample_size = min(5, len(cal_valid))
        sample_indices = np.random.RandomState(42).choice(len(cal_valid), sample_size, replace=False)
        for i in sample_indices:
            mu_i, alpha_i = mu[i], alpha[i]
            actual_i = y_cal[i]

            # P(over line) via scipy CDF
            line_val = cal_valid.iloc[i].get(prop_line_col, 0)
            if line_val and line_val > 0:
                n_param = 1.0 / alpha_i
                p_param = n_param / (n_param + mu_i)
                p_over = 1.0 - _nbinom.cdf(line_val, n_param, p_param)
            else:
                p_over = float("nan")

            logger.info(
                "  Row %d: mu=%.2f, alpha=%.2f, P(over %.1f)=%.3f, actual=%.0f",
                i, mu_i, alpha_i, line_val if line_val else 0, p_over, actual_i,
            )
            if mu_i < 0:
                raise ValueError(f"Negative mu prediction: mu={mu_i}")

        # Step 9: Save
        logger.info("Step 9: Saving artifacts...")
        model.save(self.run_dir)
        self._save_feature_manifest({"negbin": selected_features})
        self._save_calibration_report({f"batter_{self.stat}": cal_report})
        self._save_training_metadata(train_seasons, cal_season, cal_end_date, train_df, cal_df)

    # ------------------------------------------------------------------
    # NegBin Hyperparameter Tuning
    # ------------------------------------------------------------------

    def _resolve_negbin_config(self, X: pd.DataFrame, y: pd.Series) -> "NegBinConfig":
        """Resolve NegBin hyperparameters: tune if enabled, else use defaults."""
        from src.models.negbin_model import NegBinConfig

        if not self.tune_hyperparams:
            logger.info("Step 4: Hyperparameter tuning DISABLED, using defaults")
            return NegBinConfig(
                n_estimators=1000,
                max_depth=5,
                learning_rate=0.03,
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_weight=3,
                early_stopping_rounds=50,
            )

        from src.models.negbin_tuner import NegBinHyperparameterTuner

        logger.info("Step 4: Running Optuna hyperparameter tuning (%d trials)...", self.tuning_trials)

        tuner = NegBinHyperparameterTuner(
            n_trials=self.tuning_trials,
            val_fraction=0.15,
            pruning=True,
            random_state=42,
        )
        best_config = tuner.tune(X, y)

        # Save tuning results to run directory
        tuner.save_best_config(self.run_dir / "best_hyperparams.json")
        logger.info(
            "Tuning complete: val NLL=%.4f, depth=%d, lr=%.4f, n_est=%d",
            tuner.best_nll, best_config.max_depth,
            best_config.learning_rate, best_config.n_estimators,
        )

        return best_config

    # ------------------------------------------------------------------
    # NegBin PMF-based Calibration
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_negbin_calibration(
        y_actual: np.ndarray,
        mu: np.ndarray,
        alpha: np.ndarray,
        cal_df: pd.DataFrame,
        stat: str,
    ) -> dict:
        """Compute calibration metrics using the NegBin PMF/CDF directly.

        Returns a dict with: mean_nll, bias metrics, zero fraction comparison,
        per-prop-line calibration, binned P(over) calibration, and distribution stats.
        """
        from scipy.stats import nbinom as _nbinom

        from src.models.mlb.mlb_batter_feature_store import BATTER_STAT_MARKET_KEY

        n_samples = len(y_actual)
        y_int = y_actual.astype(int)

        # NB parameters: n = 1/alpha, p = n/(n+mu)
        n_param = 1.0 / np.clip(alpha, 1e-10, None)
        p_param = n_param / (n_param + np.clip(mu, 1e-10, None))
        p_param = np.clip(p_param, 1e-10, 1 - 1e-10)

        # --- Mean NLL ---
        log_probs = _nbinom.logpmf(y_int, n_param, p_param)
        log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
        mean_nll = -float(np.mean(log_probs))

        # --- Bias ---
        mean_predicted_mu = float(np.mean(mu))
        mean_actual = float(np.mean(y_actual))
        mu_actual_ratio = mean_predicted_mu / max(mean_actual, 1e-6)

        # --- Zero fraction ---
        predicted_zero_frac = float(np.mean(_nbinom.pmf(0, n_param, p_param)))
        actual_zero_frac = float(np.mean(y_actual == 0))
        zero_frac_gap = predicted_zero_frac - actual_zero_frac

        # --- Per-prop-line calibration ---
        market_key = BATTER_STAT_MARKET_KEY.get(stat, f"batter_{stat}")
        prop_line_col = f"prop_line_{market_key}"
        prop_line_calibration = []

        if prop_line_col in cal_df.columns:
            lines = cal_df[prop_line_col].values
            unique_lines = sorted(set(lines[lines > 0]))

            for line_val in unique_lines:
                mask = lines == line_val
                if mask.sum() < 20:
                    continue

                # Model P(Y >= line) = 1 - CDF(line - 1)  [for integer line]
                # But prop lines can be X.5, so P(over X.5) = P(Y >= ceil(X.5)) = 1 - CDF(floor(X.5))
                threshold = int(np.floor(line_val))
                model_over = 1.0 - _nbinom.cdf(threshold, n_param[mask], p_param[mask])
                model_over_rate = float(np.mean(model_over))

                actual_over_rate = float(np.mean(y_actual[mask] > line_val))

                prop_line_calibration.append({
                    "line": float(line_val),
                    "model_over_rate": round(model_over_rate, 4),
                    "actual_over_rate": round(actual_over_rate, 4),
                    "gap": round(model_over_rate - actual_over_rate, 4),
                    "n": int(mask.sum()),
                })

        # --- Binned calibration (P(over) in decile bins) ---
        # Compute P(over) for each row using its prop line
        binned_calibration = []
        if prop_line_col in cal_df.columns:
            lines = cal_df[prop_line_col].values
            has_line_mask = lines > 0
            if has_line_mask.sum() > 50:
                thresholds = np.floor(lines[has_line_mask]).astype(int)
                p_over = 1.0 - _nbinom.cdf(thresholds, n_param[has_line_mask], p_param[has_line_mask])
                actual_over = (y_actual[has_line_mask] > lines[has_line_mask]).astype(float)

                # Bin into deciles
                bin_edges = np.arange(0, 1.1, 0.1)
                for j in range(len(bin_edges) - 1):
                    lo, hi = bin_edges[j], bin_edges[j + 1]
                    in_bin = (p_over >= lo) & (p_over < hi)
                    if in_bin.sum() < 5:
                        continue
                    binned_calibration.append({
                        "bin_lo": round(float(lo), 2),
                        "bin_hi": round(float(hi), 2),
                        "mean_predicted": round(float(np.mean(p_over[in_bin])), 4),
                        "mean_actual": round(float(np.mean(actual_over[in_bin])), 4),
                        "n": int(in_bin.sum()),
                    })

        # --- Distribution stats ---
        pcts = [10, 25, 50, 75, 90]
        mu_percentiles = {f"P{p}": round(float(np.percentile(mu, p)), 3) for p in pcts}
        alpha_percentiles = {f"P{p}": round(float(np.percentile(alpha, p)), 3) for p in pcts}

        return {
            "mean_nll": round(mean_nll, 4),
            "mean_predicted_mu": round(mean_predicted_mu, 4),
            "mean_actual": round(mean_actual, 4),
            "mu_actual_ratio": round(mu_actual_ratio, 4),
            "predicted_zero_frac": round(predicted_zero_frac, 4),
            "actual_zero_frac": round(actual_zero_frac, 4),
            "zero_frac_gap": round(zero_frac_gap, 4),
            "prop_line_calibration": prop_line_calibration,
            "binned_calibration": binned_calibration,
            "mu_percentiles": mu_percentiles,
            "alpha_percentiles": alpha_percentiles,
            "n_calibration_samples": n_samples,
        }

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
