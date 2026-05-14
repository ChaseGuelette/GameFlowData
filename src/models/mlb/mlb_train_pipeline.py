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
import json as json_module
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.models.hyperparameter_tuner import QuantileHyperparameterTuner
from src.models.mlb.mlb_feature_store import (
    MLBFeatureStore,
    PITCHER_K_EXCLUDED_TRAINING_FEATURES,
    PITCHER_K_FEATURES,
    PITCHER_K_PHASE3B_ADDED_FEATURES,
    PITCHER_K_TRAINING_FEATURES,
)
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

SINGLE_HOOK_ABLATION_FEATURES = {
    "hook_avg_ip_l30": "team_starter_avg_ip_l30",
    "hook_short_hook_l30": "team_starter_short_hook_rate_l30",
    "hook_deep_start_l30": "team_starter_deep_start_rate_l30",
}
ABLATION_VARIANTS = ("none", "static_no_l30", "hook_only", "ip_only", "ip_hook", *SINGLE_HOOK_ABLATION_FEATURES.keys())
L30_HOOK_FEATURES = [
    "team_starter_avg_ip_l30",
    "team_starter_short_hook_rate_l30",
    "team_starter_deep_start_rate_l30",
]
PREDICTED_IP_FEATURES = [
    "predicted_ip_q25",
    "predicted_ip_q50",
    "predicted_ip_spread",
    "predicted_ip_q25_delta",
]


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
        local: bool = False,
        copula: bool = False,
        ablation_variant: str = "none",
    ):
        self.engine = get_engine(local=local)
        self.feature_store = MLBFeatureStore(self.engine)
        self.feature_tolerance = feature_tolerance

        self.tune_hyperparams = tune_hyperparams
        self.tuning_trials = tuning_trials
        self.tuning_timeout = tuning_timeout
        self.copula = copula
        if ablation_variant not in ABLATION_VARIANTS:
            raise ValueError(f"Unknown ablation_variant={ablation_variant!r}; expected one of {ABLATION_VARIANTS}")
        self.ablation_variant = ablation_variant
        self.forced_features: list[str] = []
        self.ip_feature_correlations: dict[str, dict[str, float | None]] = {}
        self.ip_feature_manifest: dict[float, list[str]] = {}

        # Create timestamped run directory with _incomplete suffix
        self.timestamp = datetime.now()
        timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        self._final_run_dir_name = f"mlb_run_{timestamp_str}"
        self.run_dir = Path(base_artifacts_dir) / f"mlb_run_{timestamp_str}_incomplete"
        self.run_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized MLB Training Run: {timestamp_str}")
        logger.info(f"Artifacts will be saved to: {self.run_dir} (renamed on completion)")
        if tune_hyperparams:
            logger.info(f"Hyperparameter tuning ENABLED: {tuning_trials} trials")
        logger.info("IP feature-source ablation variant: %s", self.ablation_variant)

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
        train_df = self.feature_store._add_interaction_features(train_df)
        logger.info(f"Training data: {len(train_df):,} rows")

        # Step 2: Load calibration data
        logger.info("Step 2: Loading calibration data...")
        cal_df = self.feature_store.get_training_dataset(seasons=[cal_season])
        cal_df = self.feature_store.enrich_with_matchup_features(cal_df)
        cal_df = self.feature_store._add_interaction_features(cal_df)
        if cal_end_date:
            pre_filter = len(cal_df)
            cal_end_date = pd.Timestamp(cal_end_date).date()
            cal_df = cal_df[cal_df["game_date"] <= cal_end_date].reset_index(drop=True)
            logger.info(f"Calibration data: {len(cal_df):,} rows (filtered from {pre_filter:,}, end={cal_end_date})")
        else:
            logger.info(f"Calibration data: {len(cal_df):,} rows")

        # Optional IP-feature-source ablation: train direct IP model and append predictions
        if self.ablation_variant in ("ip_only", "ip_hook"):
            train_df, cal_df = self._add_predicted_ip_features(train_df, cal_df)

        # Step 3: Feature selection
        if self.ablation_variant in ("ip_only", "static_no_l30"):
            excluded_for_k = L30_HOOK_FEATURES
        elif self.ablation_variant in SINGLE_HOOK_ABLATION_FEATURES:
            selected_hook = SINGLE_HOOK_ABLATION_FEATURES[self.ablation_variant]
            excluded_for_k = [f for f in L30_HOOK_FEATURES if f != selected_hook]
        else:
            excluded_for_k = None
        selected_features = self._run_feature_selection(train_df, excluded_features=excluded_for_k)
        selected_features = self._apply_ablation_forced_features(selected_features, train_df)
        if self.ablation_variant in ("ip_only", "ip_hook"):
            self._save_ip_feature_source_metadata(self.ip_feature_manifest)

        # Copula decomposition (if enabled)
        if self.copula:
            logger.info("=== COPULA MODE: Training IP + K-rate decomposition ===")

            logger.info("Step 3a: Feature selection for IP model...")
            ip_features = self._run_feature_selection_for_target(train_df, "actual_ip")

            logger.info("Step 3b: Feature selection for K-rate model...")
            krate_df_tmp = train_df[train_df["actual_ip"] > 0].copy()
            krate_df_tmp["actual_krate"] = krate_df_tmp["actual_so"] / krate_df_tmp["actual_ip"]
            krate_features = self._run_feature_selection_for_target(krate_df_tmp, "actual_krate")

            ip_pipeline = self._train_ip_model(train_df, ip_features)

            krate_pipeline = self._train_krate_model(train_df, krate_features)

            copula_rho = self._compute_copula_params(train_df)

            self._save_copula_artifacts(ip_pipeline, krate_pipeline, copula_rho, ip_features, krate_features)

            logger.info("=== Also training single direct model for comparison ===")

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

    def _run_feature_selection(
        self,
        df: pd.DataFrame,
        excluded_features: list[str] | None = None,
    ) -> dict[float, list[str]]:
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
            "actual_ip",
            "player_name",
        }
        if excluded_features:
            excluded.update(excluded_features)

        candidates = [
            c
            for c in PITCHER_K_TRAINING_FEATURES
            if c not in excluded and c in df.columns and df[c].dtype in ("float64", "float32", "int64", "int32")
        ]

        locked_out_present = sorted(PITCHER_K_EXCLUDED_TRAINING_FEATURES.intersection(df.columns))
        if locked_out_present:
            logger.info("Locked out rejected/non-3B training features: %s", locked_out_present)

        missing_phase3b = [f for f in PITCHER_K_PHASE3B_ADDED_FEATURES if f not in candidates]
        if missing_phase3b:
            raise ValueError(f"Missing Phase 3B feature-store columns: {missing_phase3b}")

        logger.info(f"Candidate features: {len(candidates)}")

        valid_df = df[df["actual_so"].notna() & (df["actual_so"] >= 0)].fillna(0)
        selected = selector.select_features_per_quantile(valid_df, "actual_so", candidates, model_name="Pitcher K")

        for q, feats in selected.items():
            logger.info(f"  Q{q:.2f}: {len(feats)} features selected")

        return selected

    def _apply_ablation_forced_features(
        self,
        selected: dict[float, list[str]],
        df: pd.DataFrame,
    ) -> dict[float, list[str]]:
        """Force ablation feature groups into every K quantile when present."""
        requested: list[str] = []
        if self.ablation_variant in ("hook_only", "ip_hook"):
            requested.extend(L30_HOOK_FEATURES)
        if self.ablation_variant in SINGLE_HOOK_ABLATION_FEATURES:
            requested.append(SINGLE_HOOK_ABLATION_FEATURES[self.ablation_variant])
        if self.ablation_variant in ("ip_only", "ip_hook"):
            requested.extend(PREDICTED_IP_FEATURES)

        present = [f for f in requested if f in df.columns]
        self.forced_features = present
        if not present:
            return selected

        forced: dict[float, list[str]] = {}
        for q, feats in selected.items():
            q_feats = [f for f in feats if not (self.ablation_variant == "ip_only" and f in L30_HOOK_FEATURES)]
            for feat in present:
                if feat not in q_feats:
                    q_feats.append(feat)
            forced[q] = q_feats

        logger.info("Forced ablation features into K model: %s", present)
        return forced

    def _add_predicted_ip_features(
        self,
        train_df: pd.DataFrame,
        cal_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Train a direct actual_ip quantile source model and append its predictions."""
        logger.info("Training direct IP feature-source model for ablation variant=%s", self.ablation_variant)
        ip_features = self._run_feature_selection_for_target(train_df, "actual_ip")
        self.ip_feature_manifest = ip_features
        ip_pipeline = self._train_ip_model(train_df, ip_features)

        ip_dir = self.run_dir / "ip_feature_model"
        ip_dir.mkdir(exist_ok=True)
        ip_pipeline.save(str(ip_dir))

        train_aug = self._append_ip_predictions(train_df, ip_pipeline)
        cal_aug = self._append_ip_predictions(cal_df, ip_pipeline)
        self.ip_feature_correlations = {
            "train": self._compute_ip_feature_correlations(train_aug),
            "cal": self._compute_ip_feature_correlations(cal_aug),
        }

        return train_aug, cal_aug

    def _append_ip_predictions(self, df: pd.DataFrame, ip_pipeline: MLBPitcherKPipeline) -> pd.DataFrame:
        df = df.copy()
        feature_names = ip_pipeline.model.all_feature_names
        X = df.reindex(columns=feature_names, fill_value=0).fillna(0)
        preds = ip_pipeline.predict(X)
        df["predicted_ip_q25"] = preds["q25"].values
        df["predicted_ip_q50"] = preds["q50"].values
        df["predicted_ip_spread"] = preds["q75"].values - preds["q25"].values
        baseline = df.get("pitcher_avg_ip_l5", pd.Series(0.0, index=df.index)).fillna(0)
        df["predicted_ip_q25_delta"] = df["predicted_ip_q25"] - baseline
        return df

    def _compute_ip_feature_correlations(self, df: pd.DataFrame) -> dict[str, float | None]:
        if "pitcher_avg_ip_l5" not in df.columns:
            return {feat: None for feat in PREDICTED_IP_FEATURES}
        out: dict[str, float | None] = {}
        base = pd.to_numeric(df["pitcher_avg_ip_l5"], errors="coerce")
        for feat in PREDICTED_IP_FEATURES:
            if feat not in df.columns:
                out[feat] = None
                continue
            vals = pd.to_numeric(df[feat], errors="coerce")
            valid = base.notna() & vals.notna()
            out[feat] = float(vals[valid].corr(base[valid])) if valid.sum() >= 3 else None
        return out

    def _save_ip_feature_source_metadata(self, ip_features: dict[float, list[str]]) -> None:
        metadata = {
            "ablation_variant": self.ablation_variant,
            "forced_features": self.forced_features,
            "predicted_ip_features": PREDICTED_IP_FEATURES,
            "l30_hook_features": L30_HOOK_FEATURES,
            "ip_feature_manifest": {str(k): v for k, v in ip_features.items()},
            "ip_feature_correlations_vs_pitcher_avg_ip_l5": self.ip_feature_correlations,
        }
        with open(self.run_dir / "ip_feature_source_metadata.json", "w") as f:
            json.dump(metadata, f, indent=4)

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
    # Copula decomposition: IP model + K-rate model
    # ------------------------------------------------------------------

    def _run_feature_selection_for_target(
        self, df: pd.DataFrame, target_col: str
    ) -> dict[float, list[str]]:
        """Run feature selection for an arbitrary target column."""
        selector = ImprovedFeatureSelector(
            n_splits=3,
            tolerance=self.feature_tolerance,
        )

        excluded = {
            "game_id", "player_id", "game_date", "season", "team_id",
            "opp_team_id", "actual_so", "actual_ip", "actual_krate", "player_name",
        }
        candidates = [
            c
            for c in PITCHER_K_TRAINING_FEATURES
            if c in df.columns and df[c].dtype in ("float64", "float32", "int64", "int32")
        ]

        valid_df = df[df[target_col].notna() & (df[target_col] >= 0)].fillna(0)

        selected = selector.select_features_per_quantile(
            valid_df, target_col, candidates, model_name=f"Pitcher {target_col}"
        )

        for q, feats in selected.items():
            logger.info(f"  {target_col} Q{q:.2f}: {len(feats)} features")

        return selected

    def _train_ip_model(
        self, df: pd.DataFrame, selected_features: dict[float, list[str]], config=None
    ) -> MLBPitcherKPipeline:
        """Train quantile model for innings pitched (IP)."""
        logger.info("Training IP sub-model...")

        ip_df = df[df["actual_ip"] > 0].copy()

        ip_df = ip_df.rename(columns={"actual_so": "_actual_so_backup", "actual_ip": "actual_so"})

        pipeline = MLBPitcherKPipeline(config=config)
        pipeline.train(ip_df, feature_names_per_quantile=selected_features)

        logger.info(f"IP model trained on {len(ip_df)} rows")
        return pipeline

    def _train_krate_model(
        self, df: pd.DataFrame, selected_features: dict[float, list[str]], config=None
    ) -> MLBPitcherKPipeline:
        """Train quantile model for K-rate (SO/IP)."""
        logger.info("Training K-rate sub-model...")

        krate_df = df[df["actual_ip"] > 0].copy()

        krate_df["actual_krate"] = krate_df["actual_so"] / krate_df["actual_ip"]

        krate_df = krate_df.rename(columns={"actual_so": "_actual_so_backup", "actual_krate": "actual_so"})

        pipeline = MLBPitcherKPipeline(config=config)
        pipeline.train(krate_df, feature_names_per_quantile=selected_features)

        logger.info(f"K-rate model trained on {len(krate_df)} rows")
        return pipeline

    def _compute_copula_params(self, df: pd.DataFrame) -> float:
        """Compute Spearman ρ between IP and K-rate for Gaussian copula."""
        valid = df[(df["actual_ip"] >= 3) & (df["actual_so"].notna())].copy()
        valid["krate"] = valid["actual_so"] / valid["actual_ip"]

        if len(valid) < 50:
            logger.warning("Insufficient data for copula params (%d rows), defaulting ρ=0.0", len(valid))
            return 0.0

        rho_s, p_value = spearmanr(valid["actual_ip"], valid["krate"])
        logger.info(f"Copula Spearman ρ(IP, K-rate): {rho_s:.4f} (p={p_value:.4f}, n={len(valid)})")

        return float(rho_s)

    def _save_copula_artifacts(
        self,
        ip_pipeline: MLBPitcherKPipeline,
        krate_pipeline: MLBPitcherKPipeline,
        copula_rho: float,
        ip_features: dict[float, list[str]],
        krate_features: dict[float, list[str]],
    ):
        """Save IP model, K-rate model, and copula params to run directory."""
        ip_dir = self.run_dir / "ip_model"
        ip_dir.mkdir(exist_ok=True)
        ip_pipeline.save(str(ip_dir))

        krate_dir = self.run_dir / "krate_model"
        krate_dir.mkdir(exist_ok=True)
        krate_pipeline.save(str(krate_dir))

        copula_path = self.run_dir / "pitcher_k_copula_params.json"
        with open(copula_path, "w") as f:
            json_module.dump({"pitcher_strikeouts": copula_rho}, f, indent=2)

        ip_manifest_path = self.run_dir / "ip_feature_manifest.json"
        krate_manifest_path = self.run_dir / "krate_feature_manifest.json"

        with open(ip_manifest_path, "w") as f:
            json_module.dump({str(k): v for k, v in ip_features.items()}, f, indent=2)
        with open(krate_manifest_path, "w") as f:
            json_module.dump({str(k): v for k, v in krate_features.items()}, f, indent=2)

        logger.info(f"Saved copula artifacts: IP model, K-rate model, ρ={copula_rho:.4f}")

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
            "ablation_variant": self.ablation_variant,
            "copula": self.copula,
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
            "cal_end_date": cal_end_date.isoformat() if hasattr(cal_end_date, "isoformat") else cal_end_date,
            "train_rows": len(train_df),
            "cal_rows": len(cal_df),
            "feature_count": len(PITCHER_K_FEATURES),
            "training_feature_count": len(PITCHER_K_TRAINING_FEATURES),
            "phase3b_added_features": PITCHER_K_PHASE3B_ADDED_FEATURES,
            "locked_out_training_features": sorted(PITCHER_K_EXCLUDED_TRAINING_FEATURES),
            "ablation_variant": self.ablation_variant,
            "forced_features": self.forced_features,
            "predicted_ip_features": PREDICTED_IP_FEATURES if self.ablation_variant in ("ip_only", "ip_hook") else [],
            "l30_hook_features": L30_HOOK_FEATURES,
            "ip_feature_correlations_vs_pitcher_avg_ip_l5": self.ip_feature_correlations,
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

    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (LOCAL_DATABASE_URL) instead of Supabase",
    )
    parser.add_argument(
        "--copula",
        action="store_true",
        help="Train IP + K-rate copula decomposition alongside single model",
    )
    parser.add_argument(
        "--ablation-variant",
        choices=ABLATION_VARIANTS,
        default="none",
        help="Pitcher K IP-feature-source ablation: none, hook_only, ip_only, ip_hook",
    )

    args = parser.parse_args()

    orchestrator = MLBTrainingOrchestrator(
        base_artifacts_dir=args.output_dir,
        tune_hyperparams=args.tune,
        tuning_trials=args.tuning_trials,
        tuning_timeout=args.tuning_timeout,
        feature_tolerance=args.feature_tolerance,
        local=args.local,
        copula=args.copula,
        ablation_variant=args.ablation_variant,
    )

    orchestrator.run(
        train_seasons=args.train_seasons,
        cal_season=args.cal_season,
        cal_end_date=args.cal_end_date,
        n_simulations=args.n_simulations,
    )
