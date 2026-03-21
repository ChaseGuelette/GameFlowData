"""
NCAAB Game-Level Model Trainer
================================
Trains XGBoost quantile regression models for:
  - home_margin (spread prediction)
  - total_score (total prediction)

Uses existing QuantileModelSuite from quantile_trainer.py.
Moneyline probability derived from spread distribution.

Usage:
    python -m src.models.ncaab_trainer --seasons 2022 2023 2024 --cal-season 2025
    python -m src.models.ncaab_trainer --seasons 2022 2023 2024 2025
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy import stats as scipy_stats
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.models.ncaab_feature_store import (
    SPREAD_FEATURES,
    TOTAL_FEATURES,
    NCAABFeatureStore,
)
from src.models.quantile_trainer import QuantileModelConfig, QuantileModelSuite

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

ARTIFACTS_DIR = Path("src/models/artifacts/ncaab")


class NCAABTrainingOrchestrator:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.feature_store = NCAABFeatureStore(self.engine)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = ARTIFACTS_DIR / f"ncaab_run_{self.timestamp}_incomplete"
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def run(self, train_seasons: list[int], cal_season: int | None = None) -> None:
        """Full training pipeline."""
        logger.info(f"Training on seasons {train_seasons}, calibration: {cal_season}")

        # Load data
        all_seasons = train_seasons + ([cal_season] if cal_season else [])
        df = self.feature_store.get_training_dataset(all_seasons)

        if df.empty:
            logger.error("No training data. Run scrapers and processing first.")
            return

        logger.info(f"Dataset: {len(df):,} games")

        # Split train/cal
        train_df = df[df["season"].isin(train_seasons)].copy()
        cal_df = df[df["season"] == cal_season].copy() if cal_season else pd.DataFrame()

        logger.info(f"Train: {len(train_df):,} games, Cal: {len(cal_df):,} games")

        # Train spread model
        logger.info("--- Training SPREAD model ---")
        spread_suite = self._train_model(train_df, "home_margin", SPREAD_FEATURES)

        # Train total model
        logger.info("--- Training TOTAL model ---")
        total_suite = self._train_model(train_df, "total_score", TOTAL_FEATURES)

        # Evaluate calibration
        if not cal_df.empty:
            logger.info("--- Evaluating calibration ---")
            self._evaluate(spread_suite, cal_df, SPREAD_FEATURES, "home_margin", "Spread")
            self._evaluate(total_suite, cal_df, TOTAL_FEATURES, "total_score", "Total")

        # Save artifacts
        self._save(spread_suite, total_suite)

        # Rename run dir (remove _incomplete)
        final_dir = ARTIFACTS_DIR / f"ncaab_run_{self.timestamp}"
        self.run_dir.rename(final_dir)
        logger.info(f"Artifacts saved to {final_dir}")

    def _train_model(self, train_df: pd.DataFrame, target: str,
                     features: list[str]) -> QuantileModelSuite:
        """Train a quantile model for the given target."""
        config = QuantileModelConfig(
            n_estimators=800,
            max_depth=4,
            learning_rate=0.04,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            early_stopping_rounds=40,
        )

        suite = QuantileModelSuite(config)

        # Filter to available features
        available = [f for f in features if f in train_df.columns]
        missing = [f for f in features if f not in train_df.columns]
        if missing:
            logger.warning(f"Missing features: {missing}")

        X = train_df[available].copy()
        y = train_df[target].copy()

        # Drop rows with NaN target
        valid = y.notna()
        X = X[valid]
        y = y[valid]

        # Fill NaN features with 0
        X = X.fillna(0)

        logger.info(f"Training on {len(X):,} rows, {len(available)} features, target={target}")

        feature_names_per_q = {q: available for q in config.quantiles}
        suite.train(X, y, feature_names_per_quantile=feature_names_per_q)

        return suite

    def _evaluate(self, suite: QuantileModelSuite, cal_df: pd.DataFrame,
                  features: list[str], target: str, label: str) -> None:
        """Check quantile coverage on calibration set."""
        available = [f for f in features if f in cal_df.columns]
        X = cal_df[available].fillna(0)
        y = cal_df[target].dropna()
        X = X.loc[y.index]

        predictions = {}
        for q in suite.config.quantiles:
            predictions[q] = suite.predict(X, quantile=q)

        logger.info(f"{label} calibration (season {cal_df['season'].iloc[0]}):")
        for q in suite.config.quantiles:
            coverage = (y <= predictions[q]).mean()
            status = "OK" if abs(coverage - q) <= 0.05 else "WARN"
            logger.info(f"  Q{q:.2f}: coverage={coverage:.3f} (target={q:.2f}) [{status}]")

    def _save(self, spread_suite: QuantileModelSuite,
              total_suite: QuantileModelSuite) -> None:
        """Save model artifacts."""
        joblib.dump(spread_suite, self.run_dir / "spread_model.joblib")
        joblib.dump(total_suite, self.run_dir / "total_model.joblib")

        config = {
            "spread_features": SPREAD_FEATURES,
            "total_features": TOTAL_FEATURES,
            "timestamp": self.timestamp,
        }
        with open(self.run_dir / "feature_config.json", "w") as f:
            json.dump(config, f, indent=2)

        logger.info("Saved spread_model.joblib, total_model.joblib, feature_config.json")

    @staticmethod
    def derive_moneyline_probability(spread_suite: QuantileModelSuite,
                                     X: pd.DataFrame) -> pd.Series:
        """Estimate P(home wins) from spread distribution.

        Uses Q25/Q50/Q75 to fit a normal distribution,
        then computes P(home_margin > 0).
        """
        q25 = spread_suite.predict(X, quantile=0.25)
        q50 = spread_suite.predict(X, quantile=0.50)
        q75 = spread_suite.predict(X, quantile=0.75)

        # Estimate sigma from IQR
        iqr = q75 - q25
        sigma = iqr / 1.35  # IQR = 1.35 * sigma for normal distribution
        sigma = np.clip(sigma, 1.0, 30.0)  # Safety bounds

        # P(home wins) = P(margin > 0) = 1 - CDF(0)
        p_home_wins = 1 - scipy_stats.norm.cdf(0, loc=q50, scale=sigma)

        return pd.Series(p_home_wins, index=X.index, name="p_home_wins")


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="NCAAB Model Trainer")
    parser.add_argument("--seasons", nargs="+", type=int, required=True,
                        help="Training seasons (e.g., 2022 2023 2024)")
    parser.add_argument("--cal-season", type=int, default=None,
                        help="Calibration season (e.g., 2025)")

    args = parser.parse_args()

    orchestrator = NCAABTrainingOrchestrator()
    orchestrator.run(args.seasons, args.cal_season)
