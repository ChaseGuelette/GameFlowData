# models/quantile_trainer.py

from __future__ import annotations

from dataclasses import asdict, dataclass
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

    def to_dict(self) -> dict:
        """Convert config to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> QuantileModelConfig:
        """Create config from dictionary."""
        # Filter out unknown keys
        valid_keys = cls.__annotations__.keys()
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}

        # Handle tuple conversion for quantiles if it comes back as list
        if "quantiles" in filtered_data:
            filtered_data["quantiles"] = tuple(filtered_data["quantiles"])

        return cls(**filtered_data)


# Map stat names to their feature lists
STAT_FEATURES = {
    "pts": RATE_FEATURES_PTS,
    "reb": RATE_FEATURES_REB,
    "ast": RATE_FEATURES_AST,
}


class QuantileModelSuite:
    """
    Trains and manages a suite of quantile regression models.
    Supports per-quantile feature sets.
    """

    # Only apply recalibration when coverage gap exceeds this threshold
    RECALIBRATION_GAP_THRESHOLD = 0.03

    def __init__(self, config: QuantileModelConfig | None = None):
        self.config = config or QuantileModelConfig()
        self.models: dict[float, xgb.XGBRegressor] = {}
        self.feature_names_per_quantile: dict[float, list[str]] = {}
        # Conformal calibration offsets: {quantile: delta}
        # Applied at predict time: calibrated = raw + delta
        self.calibration_offsets: dict[float, float] = {}

    @property
    def all_feature_names(self) -> list[str]:
        """Union of all per-quantile feature names (sorted for determinism)."""
        all_names = set()
        for names in self.feature_names_per_quantile.values():
            all_names.update(names)
        return sorted(all_names)

    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        feature_names_per_quantile: dict[float, list[str]],
        sample_weight: pd.Series | None = None,
    ) -> dict:
        """
        Train separate models for each quantile using per-quantile feature sets.

        Args:
            X: DataFrame containing the union of all per-quantile features.
            y: Target series.
            feature_names_per_quantile: Dict mapping quantile -> list of feature names.
            sample_weight: Optional sample weights for training.

        Returns dict of {quantile: validation_loss}.
        """
        self.feature_names_per_quantile = feature_names_per_quantile

        # Split for early stopping (preserve temporal order)
        arrays = [X, y]
        if sample_weight is not None:
            arrays.append(sample_weight)

        split_results = train_test_split(
            *arrays,
            test_size=self.config.val_fraction,
            shuffle=False,
        )

        X_train, X_val = split_results[0], split_results[1]
        y_train, y_val = split_results[2], split_results[3]

        w_train = None
        if sample_weight is not None:
            w_train = split_results[4]

        results = {}

        for q in self.config.quantiles:
            q_features = self.feature_names_per_quantile[q]
            print(f"\nTraining quantile {q:.2f} ({len(q_features)} features)...")

            X_train_q = X_train[q_features]
            X_val_q = X_val[q_features]

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
                early_stopping_rounds=self.config.early_stopping_rounds,
                n_jobs=-1,
            )

            model.fit(
                X_train_q,
                y_train,
                sample_weight=w_train,
                eval_set=[(X_val_q, y_val)],
                verbose=False,
            )

            train_preds = model.predict(X_train_q)
            train_coverage = (y_train <= train_preds).mean()

            val_preds = model.predict(X_val_q)
            val_coverage = (y_val <= val_preds).mean()

            print(
                f"  Quantile {q:.2f}: Target={q:.2f} | "
                f"Train cov={train_coverage:.3f} | Val cov={val_coverage:.3f} | "
                f"Gap={train_coverage - val_coverage:.3f}"
            )

            self.models[q] = model

            # Conformal recalibration: compute offset from validation residuals
            # delta = q-th quantile of (y_val - pred), so P(Y <= pred + delta) ≈ q
            coverage_gap = abs(val_coverage - q)
            if coverage_gap > self.RECALIBRATION_GAP_THRESHOLD:
                residuals = y_val.values - val_preds
                delta = float(np.quantile(residuals, q))
                self.calibration_offsets[q] = delta
                recal_coverage = (y_val <= (val_preds + delta)).mean()
                print(
                    f"  Recalibration applied: delta={delta:+.4f} | "
                    f"Adjusted cov={recal_coverage:.3f} (was {val_coverage:.3f})"
                )
            else:
                self.calibration_offsets[q] = 0.0

            results[q] = {
                "train_coverage": train_coverage,
                "val_coverage": val_coverage,
                "gap": train_coverage - val_coverage,
                "calibration_offset": self.calibration_offsets[q],
            }

        return results

    def get_learning_curve_data(
        self, X: pd.DataFrame, y: pd.Series, train_sizes: list[float] | None = None
    ) -> dict[float, list[dict]]:
        """
        Generate learning curve data for diagnosis.
        Returns dict: {quantile: [{train_size, fraction, train_coverage, val_coverage}, ...]}
        """
        train_sizes = train_sizes or np.linspace(0.1, 1.0, 5)

        # Split for evaluation (Preserve temporal order)
        X_train, X_val, y_train, y_val = train_test_split(
            X,
            y,
            test_size=self.config.val_fraction,
            shuffle=False,
        )

        results = {}

        for q in self.config.quantiles:
            q_features = self.feature_names_per_quantile.get(q, list(X.columns))
            print(f"\nGenerating learning curve for Q{q:.2f} ({len(q_features)} features)...")
            q_results = []

            for fraction in train_sizes:
                size = int(fraction * len(X_train))
                if size < 50:
                    continue

                subset_idx = np.random.RandomState(42).choice(len(X_train), size=size, replace=False)
                X_subset = X_train.iloc[subset_idx][q_features]
                y_subset = y_train.iloc[subset_idx]

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

                model.fit(X_subset, y_subset, verbose=False)

                train_cov = (y_subset <= model.predict(X_subset)).mean()
                val_cov = (y_val <= model.predict(X_val[q_features])).mean()

                q_results.append(
                    {
                        "train_size": size,
                        "fraction": fraction,
                        "train_coverage": train_cov,
                        "val_coverage": val_cov,
                    }
                )
                print(f"  Size {size} ({fraction:.0%}): Train={train_cov:.3f}, Val={val_cov:.3f}")

            results[q] = q_results

        return results

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict all quantiles for input data.
        X should contain the union of all per-quantile features (use all_feature_names).

        Returns DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        Applies conformal calibration offsets if they were computed during training.
        """
        predictions = {}

        for q, model in self.models.items():
            q_features = self.feature_names_per_quantile[q]
            X_q = X[q_features]
            # validate_features=False works around pandas 3.0 compat issue
            # where XGBoost DMatrix can't extract feature names from the new
            # str-typed Index. Feature order is guaranteed correct by the caller.
            raw_preds = model.predict(X_q, validate_features=False)

            # Apply conformal calibration offset
            offset = self.calibration_offsets.get(q, 0.0)
            if offset != 0.0:
                raw_preds = raw_preds + offset

            predictions[f"q{int(q * 100):02d}"] = raw_preds

        result = pd.DataFrame(predictions)

        # Enforce monotonicity
        result = self._enforce_monotonicity(result)

        return result

    def _enforce_monotonicity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure quantile predictions are monotonically increasing.
        Uses vectorized numpy cumulative max (Q10 <= Q25 <= ... <= Q90).
        """
        quantile_cols = sorted(df.columns)  # ['q10', 'q25', ...]
        values = df[quantile_cols].values  # (n_players, n_quantiles)

        # np.maximum.accumulate along axis=1 enforces monotonicity in one op
        fixed = np.maximum.accumulate(values, axis=1)

        return pd.DataFrame(fixed, columns=quantile_cols, index=df.index).astype(np.float32)

    def save(self, path: str):
        """Save all models to disk."""
        save_dict = {
            "models": {q: model for q, model in self.models.items()},
            "config": self.config,
            "feature_names_per_quantile": self.feature_names_per_quantile,
            "calibration_offsets": self.calibration_offsets,
        }
        joblib.dump(save_dict, path)
        print(f"Saved quantile model suite to {path}")

    @classmethod
    def load(cls, path: str) -> QuantileModelSuite:
        """Load models from disk."""
        save_dict = joblib.load(path)

        suite = cls(config=save_dict["config"])
        suite.models = save_dict["models"]
        suite.feature_names_per_quantile = save_dict.get("feature_names_per_quantile", {})
        suite.calibration_offsets = save_dict.get("calibration_offsets", {})

        return suite


class PlayerPropsModelPipeline:
    """
    Complete pipeline for training minutes and rate models.
    Supports per-quantile feature sets for optimal calibration.
    """

    def __init__(self, feature_store, config: QuantileModelConfig | None = None):
        self.feature_store = feature_store
        self.config = config or QuantileModelConfig()

        # Model suites
        self.minutes_model: QuantileModelSuite | None = None
        self.rate_models: dict[str, QuantileModelSuite] = {}

        # Per-quantile feature dicts (populated during training)
        # minutes_features: {0.1: [...], 0.25: [...], ...}
        self.minutes_features: dict[float, list[str]] = {}
        # rate_features: {"pts": {0.1: [...], ...}, "reb": {...}, ...}
        self.rate_features: dict[str, dict[float, list[str]]] = {}

    def train_minutes_model(self, df: pd.DataFrame, hyperparams: dict | None = None) -> dict:
        """Train the minutes prediction model with per-quantile features."""
        print("\n" + "=" * 60)
        print("TRAINING MINUTES MODEL")
        print("=" * 60)

        # Use tuned hyperparams if provided
        if hyperparams:
            config = QuantileModelConfig.from_dict(hyperparams)
            print(
                f"Using tuned hyperparams: lr={config.learning_rate}, depth={config.max_depth}, n_est={config.n_estimators}"
            )
        else:
            config = self.config

        # Fallback if no per-quantile features set
        if not self.minutes_features:
            self.minutes_features = {q: list(MINUTES_FEATURES) for q in config.quantiles}

        # Compute union of all per-quantile features
        all_features = set()
        for feat_list in self.minutes_features.values():
            all_features.update(feat_list)

        available_features = sorted([f for f in all_features if f in df.columns])
        missing = all_features - set(df.columns)
        if missing:
            print(f"Warning: Missing features: {sorted(missing)}")

        # Build per-quantile available features
        per_q_available = {}
        for q, feat_list in self.minutes_features.items():
            per_q_available[q] = [f for f in feat_list if f in df.columns]
            print(f"  Q{q:.2f}: {len(per_q_available[q])} features")

        # Filter to valid rows
        valid_mask = df["actual_minutes"] > 0
        X = df.loc[valid_mask, available_features].fillna(0)
        y = df.loc[valid_mask, "actual_minutes"]

        print(f"Training on {len(X):,} samples")

        # Train
        self.minutes_model = QuantileModelSuite(config)
        results = self.minutes_model.train(X, y, per_q_available)

        return results

    def train_rate_models(
        self,
        df: pd.DataFrame,
        stats: list[str] | None = None,
        sample_weight: pd.Series | None = None,
        hyperparams: dict | None = None,
    ) -> dict:
        """Train rate models for each stat with per-quantile features."""
        stats = stats or ["pts", "reb", "ast"]
        hyperparams = hyperparams or {}

        print("\n" + "=" * 60)
        print("TRAINING RATE MODELS")
        print("=" * 60)

        all_results = {}

        for stat in stats:
            print(f"\n--- Training {stat.upper()} rate model ---")

            # Use tuned hyperparams if provided for this stat
            if stat in hyperparams and hyperparams[stat]:
                config = QuantileModelConfig.from_dict(hyperparams[stat])
                print(
                    f"Using tuned hyperparams: lr={config.learning_rate}, depth={config.max_depth}, n_est={config.n_estimators}"
                )
            else:
                config = self.config

            # Fallback if not set
            if stat not in self.rate_features:
                default_features = STAT_FEATURES.get(stat, RATE_FEATURES_PTS)
                self.rate_features[stat] = {q: list(default_features) for q in config.quantiles}

            stat_features_per_q = self.rate_features[stat]

            # Compute union
            all_features = set()
            for feat_list in stat_features_per_q.values():
                all_features.update(feat_list)

            available_features = sorted([f for f in all_features if f in df.columns])
            missing = all_features - set(df.columns)
            if missing:
                print(f"Warning: Missing features for {stat}: {sorted(missing)}")

            # Build per-quantile available features
            per_q_available = {}
            for q, feat_list in stat_features_per_q.items():
                per_q_available[q] = [f for f in feat_list if f in df.columns]
                print(f"  Q{q:.2f}: {len(per_q_available[q])} features")

            # Filter to valid rows
            rate_col = f"{stat}_per_min"
            valid_mask = df[rate_col].notna() & (df["actual_minutes"] >= 10)

            X = df.loc[valid_mask, available_features].fillna(0)
            y = df.loc[valid_mask, rate_col]

            w = None
            if sample_weight is not None:
                w = sample_weight.loc[valid_mask]

            print(f"Training on {len(X):,} samples")

            # Train regular quantile model suite
            model_suite = QuantileModelSuite(config)
            results = model_suite.train(X, y, per_q_available, sample_weight=w)
            self.rate_models[stat] = model_suite
            all_results[stat] = results

        return all_results

    def save_all(self, directory: str):
        """Save all models."""
        path = Path(directory)
        path.mkdir(exist_ok=True)

        if self.minutes_model:
            self.minutes_model.save(str(path / "minutes_model.joblib"))

        for stat, model in self.rate_models.items():
            model.save(str(path / f"{stat}_rate_model.joblib"))

        # Save per-quantile feature config
        joblib.dump(
            {
                "minutes_features": self.minutes_features,
                "rate_features": self.rate_features,
            },
            path / "feature_config.joblib",
        )

        print(f"\nAll models saved to {directory}")

    @classmethod
    def load_all(cls, directory: str, feature_store) -> PlayerPropsModelPipeline:
        """Load all models."""
        path = Path(directory)

        pipeline = cls(feature_store)

        # Load minutes model
        minutes_path = path / "minutes_model.joblib"
        if minutes_path.exists():
            pipeline.minutes_model = QuantileModelSuite.load(str(minutes_path))

        # Load feature config
        config_path = path / "feature_config.joblib"
        if config_path.exists():
            config = joblib.load(config_path)
            pipeline.minutes_features = config["minutes_features"]
            pipeline.rate_features = config["rate_features"]

        # Load rate models
        for stat in ["pts", "reb", "ast"]:
            rate_path = path / f"{stat}_rate_model.joblib"
            if rate_path.exists():
                pipeline.rate_models[stat] = QuantileModelSuite.load(str(rate_path))

        return pipeline
