# models/quantile_trainer.py

from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.isotonic import IsotonicRegression
from sklearn.model_selection import train_test_split

from .feature_store import (
    MINUTES_FEATURES,
    RATE_FEATURES_AST,
    RATE_FEATURES_PTS,
    RATE_FEATURES_REB,
    RATE_FEATURES_THREES,
)


@dataclass
class QuantileModelConfig:
    """Configuration for quantile model training."""

    quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)

    # XGBoost parameters
    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50

    # Training config
    val_fraction: float = 0.15
    random_state: int = 42


# Map stat names to their feature lists
STAT_FEATURES = {
    "pts": RATE_FEATURES_PTS,
    "reb": RATE_FEATURES_REB,
    "ast": RATE_FEATURES_AST,
    "threes": RATE_FEATURES_THREES,
}


class QuantileModelSuite:
    """
    Trains and manages a suite of quantile regression models.
    """

    def __init__(self, config: QuantileModelConfig | None = None):
        self.config = config or QuantileModelConfig()
        self.models: dict[float, xgb.XGBRegressor] = {}
        self.feature_names: list[str] = []

    def train(self, X: pd.DataFrame, y: pd.Series, feature_names: list[str] | None = None) -> dict[str, float]:
        """
        Train separate models for each quantile.

        Returns dict of {quantile: validation_loss}.
        """
        self.feature_names = feature_names or list(X.columns)

        # Split for early stopping
        X_train, X_val, y_train, y_val = train_test_split(
            X,
            y,
            test_size=self.config.val_fraction,
            shuffle=False,  # Preserve temporal order
        )

        results = {}

        for q in self.config.quantiles:
            print(f"\nTraining quantile {q:.2f}...")

            model = xgb.XGBRegressor(
                objective="reg:quantileerror",
                quantile_alpha=q,
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                min_child_weight=self.config.min_child_weight,
                random_state=self.config.random_state,
                n_jobs=-1,
            )

            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

            # Evaluate calibration
            val_preds = model.predict(X_val)
            coverage = (y_val <= val_preds).mean()

            print(f"  Quantile {q:.2f}: Target coverage = {q:.2f}, Actual coverage = {coverage:.3f}")

            self.models[q] = model
            results[q] = coverage

        return results

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict all quantiles for input data.

        Returns DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        """
        predictions = {}

        for q, model in self.models.items():
            predictions[f"q{int(q * 100):02d}"] = model.predict(X)

        result = pd.DataFrame(predictions)

        # Enforce monotonicity
        result = self._enforce_monotonicity(result)

        return result

    def _enforce_monotonicity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure quantile predictions are monotonically increasing.
        Uses isotonic regression row-by-row.
        """
        quantile_cols = sorted(df.columns)  # ['q10', 'q25', ...]
        quantile_values = [int(c[1:]) / 100 for c in quantile_cols]

        result = df.copy()

        for idx in df.index:
            values = df.loc[idx, quantile_cols].values

            # Check if already monotonic
            if np.all(np.diff(values) >= 0):
                continue

            # Apply isotonic regression
            ir = IsotonicRegression()
            fixed = ir.fit_transform(quantile_values, values)
            result.loc[idx, quantile_cols] = fixed

        return result

    def save(self, path: str):
        """Save all models to disk."""
        save_dict = {
            "models": {q: model for q, model in self.models.items()},
            "config": self.config,
            "feature_names": self.feature_names,
        }
        joblib.dump(save_dict, path)
        print(f"Saved quantile model suite to {path}")

    @classmethod
    def load(cls, path: str) -> "QuantileModelSuite":
        """Load models from disk."""
        save_dict = joblib.load(path)

        suite = cls(config=save_dict["config"])
        suite.models = save_dict["models"]
        suite.feature_names = save_dict["feature_names"]

        return suite


class PlayerPropsModelPipeline:
    """
    Complete pipeline for training minutes and rate models.
    Uses centralized feature definitions from feature_store.
    """

    def __init__(self, feature_store, config: QuantileModelConfig | None = None):
        self.feature_store = feature_store
        self.config = config or QuantileModelConfig()

        # Model suites
        self.minutes_model: QuantileModelSuite | None = None
        self.rate_models: dict[str, QuantileModelSuite] = {}

        # Feature lists (populated during training)
        self.minutes_features: list[str] = []
        self.rate_features: dict[str, list[str]] = {}

    def train_minutes_model(self, df: pd.DataFrame) -> dict:
        """Train the minutes prediction model."""
        print("\n" + "=" * 60)
        print("TRAINING MINUTES MODEL")
        print("=" * 60)

        # Use centralized feature list
        self.minutes_features = MINUTES_FEATURES

        # Filter to available features
        available_features = [f for f in self.minutes_features if f in df.columns]
        missing_features = [f for f in self.minutes_features if f not in df.columns]

        if missing_features:
            print(f"Warning: Missing features: {missing_features}")

        print(f"Using {len(available_features)} features: {available_features}")

        # Filter to valid rows (player played)
        valid_mask = df["actual_minutes"] > 0
        X = df.loc[valid_mask, available_features].fillna(0)
        y = df.loc[valid_mask, "actual_minutes"]

        print(f"Training on {len(X):,} samples")

        # Train
        self.minutes_model = QuantileModelSuite(self.config)
        results = self.minutes_model.train(X, y, available_features)

        return results

    def train_rate_models(self, df: pd.DataFrame, stats: list[str] | None = None) -> dict:
        """Train rate models for each stat using stat-specific features."""
        stats = stats or ["pts", "reb", "ast", "threes"]

        print("\n" + "=" * 60)
        print("TRAINING RATE MODELS")
        print("=" * 60)

        all_results = {}

        for stat in stats:
            print(f"\n--- Training {stat.upper()} rate model ---")

            # Get stat-specific features
            stat_features = STAT_FEATURES.get(stat, RATE_FEATURES_PTS)
            self.rate_features[stat] = stat_features

            # Filter to available features
            available_features = [f for f in stat_features if f in df.columns]
            missing_features = [f for f in stat_features if f not in df.columns]

            if missing_features:
                print(f"Warning: Missing features for {stat}: {missing_features}")

            print(f"Using {len(available_features)} features: {available_features}")

            # Filter to valid rows (minimum minutes for rate calculation)
            rate_col = f"{stat}_per_min"
            valid_mask = df[rate_col].notna() & (df["actual_minutes"] >= 10)

            X = df.loc[valid_mask, available_features].fillna(0)
            y = df.loc[valid_mask, rate_col]

            print(f"Training on {len(X):,} samples")

            # Train
            model_suite = QuantileModelSuite(self.config)
            results = model_suite.train(X, y, available_features)

            self.rate_models[stat] = model_suite
            all_results[stat] = results

        return all_results

    def save_all(self, directory: str):
        """Save all models."""
        path = Path(directory)
        path.mkdir(exist_ok=True)

        if self.minutes_model:
            self.minutes_model.save(path / "minutes_model.joblib")

        for stat, model in self.rate_models.items():
            model.save(path / f"{stat}_rate_model.joblib")

        # Save feature lists
        joblib.dump(
            {
                "minutes_features": self.minutes_features,
                "rate_features": self.rate_features,
            },
            path / "feature_config.joblib",
        )

        print(f"\nAll models saved to {directory}")

    @classmethod
    def load_all(cls, directory: str, feature_store) -> "PlayerPropsModelPipeline":
        """Load all models."""
        path = Path(directory)

        pipeline = cls(feature_store)

        # Load minutes model
        minutes_path = path / "minutes_model.joblib"
        if minutes_path.exists():
            pipeline.minutes_model = QuantileModelSuite.load(minutes_path)

        # Load rate models
        for stat in ["pts", "reb", "ast", "threes"]:
            rate_path = path / f"{stat}_rate_model.joblib"
            if rate_path.exists():
                pipeline.rate_models[stat] = QuantileModelSuite.load(rate_path)

        # Load feature config
        config_path = path / "feature_config.joblib"
        if config_path.exists():
            config = joblib.load(config_path)
            pipeline.minutes_features = config["minutes_features"]
            pipeline.rate_features = config["rate_features"]

        return pipeline
