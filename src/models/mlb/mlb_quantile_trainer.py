"""MLB Pitcher K quantile model pipeline.

Wraps the existing QuantileModelSuite with MLB-specific configuration.
No minutes decomposition — pitcher strikeouts are predicted directly.
"""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import pandas as pd

from src.models.quantile_trainer import QuantileModelConfig, QuantileModelSuite

from .mlb_feature_store import PITCHER_K_FEATURES

logger = logging.getLogger(__name__)

MLB_PITCHER_K_CONFIG = QuantileModelConfig(
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


class MLBPitcherKPipeline:
    """Single quantile model for pitcher strikeouts (no minutes decomposition).

    Unlike NBA's PlayerPropsModelPipeline which combines minutes + rate models,
    this pipeline trains a single QuantileModelSuite directly on SO counts.
    """

    def __init__(self, config: QuantileModelConfig | None = None):
        self.config = config or MLB_PITCHER_K_CONFIG
        self.model: QuantileModelSuite | None = None
        self.feature_names: dict[float, list[str]] = {}

    def train(
        self,
        df: pd.DataFrame,
        feature_names_per_quantile: dict[float, list[str]] | None = None,
        sample_weight: pd.Series | None = None,
    ) -> dict:
        """Train pitcher K quantile models.

        Args:
            df: Training DataFrame with PITCHER_K_FEATURES + 'actual_so'.
            feature_names_per_quantile: Per-quantile feature sets. If None,
                uses PITCHER_K_FEATURES for all quantiles.
            sample_weight: Optional sample weights.

        Returns:
            Dict of {quantile: training_results}.
        """
        print("\n" + "=" * 60)
        print("TRAINING MLB PITCHER K MODEL")
        print("=" * 60)

        # Default: same features for all quantiles
        if feature_names_per_quantile is None:
            feature_names_per_quantile = {
                q: list(PITCHER_K_FEATURES) for q in self.config.quantiles
            }

        self.feature_names = feature_names_per_quantile

        # Compute union of all per-quantile features
        all_features: set[str] = set()
        for feat_list in feature_names_per_quantile.values():
            all_features.update(feat_list)

        available_features = sorted([f for f in all_features if f in df.columns])
        missing = all_features - set(df.columns)
        if missing:
            print(f"Warning: Missing features: {sorted(missing)}")

        # Build per-quantile available features
        per_q_available: dict[float, list[str]] = {}
        for q, feat_list in feature_names_per_quantile.items():
            per_q_available[q] = [f for f in feat_list if f in df.columns]
            print(f"  Q{q:.2f}: {len(per_q_available[q])} features")

        # Filter to valid rows (starter actually pitched)
        valid_mask = df["actual_so"].notna() & (df["actual_so"] >= 0)
        X = df.loc[valid_mask, available_features].fillna(0)
        y = df.loc[valid_mask, "actual_so"]

        w = None
        if sample_weight is not None:
            w = sample_weight.loc[valid_mask]

        print(f"Training on {len(X):,} samples")

        # Train
        self.model = QuantileModelSuite(self.config)
        results = self.model.train(X, y, per_q_available, sample_weight=w)

        return results

    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Predict quantiles for pitcher K.

        Args:
            features_df: DataFrame with PITCHER_K_FEATURES columns.

        Returns:
            DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        """
        if self.model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        return self.model.predict_quantiles(features_df)

    def save(self, directory: str) -> None:
        """Save model and feature config to directory."""
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)

        if self.model:
            self.model.save(str(path / "pitcher_k_model.joblib"))

        joblib.dump(
            {"feature_names": self.feature_names},
            path / "pitcher_k_feature_config.joblib",
        )
        print(f"MLB Pitcher K pipeline saved to {directory}")

    @classmethod
    def load(cls, directory: str) -> MLBPitcherKPipeline:
        """Load model and feature config from directory."""
        path = Path(directory)

        pipeline = cls()

        model_path = path / "pitcher_k_model.joblib"
        if model_path.exists():
            pipeline.model = QuantileModelSuite.load(str(model_path))

        config_path = path / "pitcher_k_feature_config.joblib"
        if config_path.exists():
            config = joblib.load(config_path)
            pipeline.feature_names = config["feature_names"]

        return pipeline
