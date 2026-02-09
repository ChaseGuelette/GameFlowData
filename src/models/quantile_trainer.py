# models/quantile_trainer.py

from __future__ import annotations

import json
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

    def to_dict(self) -> dict:
        """Convert config to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "QuantileModelConfig":
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
    "threes": RATE_FEATURES_THREES,
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
            result.loc[idx, quantile_cols] = fixed.astype(np.float32)

        return result

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
    def load(cls, path: str) -> "QuantileModelSuite":
        """Load models from disk."""
        save_dict = joblib.load(path)

        suite = cls(config=save_dict["config"])
        suite.models = save_dict["models"]
        suite.feature_names_per_quantile = save_dict.get("feature_names_per_quantile", {})
        suite.calibration_offsets = save_dict.get("calibration_offsets", {})

        return suite


@dataclass
class HurdleQuantileModel:
    """
    Two-stage hurdle model for zero-inflated distributions (e.g., THREES).

    Stage 1: Binary classifier predicts P(stat = 0)
    Stage 2: Quantile regression on positive samples only

    Combined inference uses: if q <= p_zero, return 0; else interpolate positive quantiles.
    """

    zero_classifier: xgb.XGBClassifier
    zero_calibrator: IsotonicRegression
    positive_quantile_models: dict[float, xgb.XGBRegressor]
    quantiles: list[float]
    feature_names: list[str]
    positive_feature_names_per_quantile: dict[float, list[str]]
    calibration_offsets: dict[float, float]  # Conformal offsets for positive models

    @property
    def all_feature_names(self) -> list[str]:
        """Return union of all features needed for prediction."""
        all_feats = set(self.feature_names)
        for names in self.positive_feature_names_per_quantile.values():
            all_feats.update(names)
        return sorted(all_feats)

    def predict_p_zero(self, X: pd.DataFrame) -> np.ndarray:
        """Predict calibrated probability of zero."""
        X_clf = X[self.feature_names]
        p_raw = self.zero_classifier.predict_proba(X_clf)[:, 1]
        return self.zero_calibrator.predict(p_raw)

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict quantiles combining both stages.

        Returns DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        """
        p_zero = self.predict_p_zero(X)
        n = len(X)

        results = {}
        for q in self.quantiles:
            preds = np.zeros(n)

            for i in range(n):
                if q <= p_zero[i]:
                    preds[i] = 0.0
                else:
                    # Map q to the positive distribution
                    adjusted_q = (q - p_zero[i]) / (1 - p_zero[i])
                    preds[i] = self._interpolate_positive_quantile(X.iloc[[i]], adjusted_q)

            results[f"q{int(q * 100):02d}"] = preds

        result = pd.DataFrame(results)
        return self._enforce_monotonicity(result)

    def predict_positive_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict raw quantiles from positive-only models (for MC sampling).

        Unlike predict_quantiles(), this does NOT apply the p_zero adjustment.
        Use this for Monte Carlo sampling where the Bernoulli draw handles
        zero mass separately.

        Returns DataFrame with columns ['q10', 'q25', 'q50', 'q75', 'q90'].
        """
        n = len(X)
        results = {}

        for q in self.quantiles:
            q_features = self.positive_feature_names_per_quantile[q]
            preds = self.positive_quantile_models[q].predict(X[q_features])
            preds = preds + self.calibration_offsets.get(q, 0.0)
            results[f"q{int(q * 100):02d}"] = np.maximum(preds, 0)

        result = pd.DataFrame(results)
        return self._enforce_monotonicity(result)

    def _interpolate_positive_quantile(self, X: pd.DataFrame, target_q: float) -> float:
        """Interpolate positive distribution quantile."""
        # Clamp to valid range
        target_q = np.clip(target_q, 0.001, 0.999)

        # Find bracketing quantiles
        lower_q = max((q for q in self.quantiles if q <= target_q), default=self.quantiles[0])
        upper_q = min((q for q in self.quantiles if q >= target_q), default=self.quantiles[-1])

        # Get predictions from positive models
        lower_features = self.positive_feature_names_per_quantile[lower_q]
        lower_val = self.positive_quantile_models[lower_q].predict(X[lower_features])[0]
        lower_val += self.calibration_offsets.get(lower_q, 0.0)

        if lower_q == upper_q:
            return max(0.0, lower_val)

        upper_features = self.positive_feature_names_per_quantile[upper_q]
        upper_val = self.positive_quantile_models[upper_q].predict(X[upper_features])[0]
        upper_val += self.calibration_offsets.get(upper_q, 0.0)

        # Linear interpolation
        weight = (target_q - lower_q) / (upper_q - lower_q)
        result = lower_val + weight * (upper_val - lower_val)
        return max(0.0, result)

    def _enforce_monotonicity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure quantile predictions are monotonically increasing."""
        quantile_cols = sorted(df.columns)
        quantile_values = [int(c[1:]) / 100 for c in quantile_cols]

        result = df.copy()
        for idx in df.index:
            values = df.loc[idx, quantile_cols].values
            if np.all(np.diff(values) >= 0):
                continue
            ir = IsotonicRegression()
            fixed = ir.fit_transform(quantile_values, values)
            result.loc[idx, quantile_cols] = fixed.astype(np.float32)

        return result

    def save(self, directory: Path) -> None:
        """Save all components to disk."""
        directory = Path(directory)
        joblib.dump(self.zero_classifier, directory / "threes_zero_classifier.joblib")
        joblib.dump(self.zero_calibrator, directory / "threes_zero_calibrator.joblib")
        joblib.dump(
            {
                "models": self.positive_quantile_models,
                "feature_names_per_quantile": self.positive_feature_names_per_quantile,
                "calibration_offsets": self.calibration_offsets,
            },
            directory / "threes_rate_model.joblib",
        )
        with open(directory / "threes_is_hurdle.json", "w") as f:
            json.dump(
                {
                    "is_hurdle": True,
                    "quantiles": self.quantiles,
                    "feature_names": self.feature_names,
                },
                f,
                indent=2,
            )
        print(f"Saved hurdle model to {directory}")

    @classmethod
    def load(cls, directory: Path) -> HurdleQuantileModel:
        """Load from disk."""
        directory = Path(directory)

        # Check if this is a hurdle model
        hurdle_flag_path = directory / "threes_is_hurdle.json"
        if not hurdle_flag_path.exists():
            raise FileNotFoundError(f"Not a hurdle model: {directory}")

        with open(hurdle_flag_path) as f:
            meta = json.load(f)

        zero_clf = joblib.load(directory / "threes_zero_classifier.joblib")
        zero_cal = joblib.load(directory / "threes_zero_calibrator.joblib")
        rate_data = joblib.load(directory / "threes_rate_model.joblib")

        return cls(
            zero_classifier=zero_clf,
            zero_calibrator=zero_cal,
            positive_quantile_models=rate_data["models"],
            quantiles=meta["quantiles"],
            feature_names=meta["feature_names"],
            positive_feature_names_per_quantile=rate_data["feature_names_per_quantile"],
            calibration_offsets=rate_data.get("calibration_offsets", {}),
        )

    @classmethod
    def is_hurdle_model(cls, directory: Path) -> bool:
        """Check if a directory contains a hurdle model."""
        return (Path(directory) / "threes_is_hurdle.json").exists()


def train_hurdle_model(
    df: pd.DataFrame,
    feature_names: list[str],
    feature_names_per_quantile: dict[float, list[str]],
    config: QuantileModelConfig | None = None,
) -> HurdleQuantileModel:
    """
    Train a hurdle model for zero-inflated THREES distribution.

    Args:
        df: Training data with 'threes_per_min' target and 'actual_minutes' filter column.
        feature_names: List of feature names for the classifier.
        feature_names_per_quantile: Dict mapping quantile -> feature list for positive models.
        config: Training configuration.

    Returns:
        Trained HurdleQuantileModel.
    """
    config = config or QuantileModelConfig()

    print("\n" + "=" * 60)
    print("TRAINING THREES HURDLE MODEL")
    print("=" * 60)

    # Filter to valid rows
    valid_mask = df["threes_per_min"].notna() & (df["actual_minutes"] >= 10)
    df_valid = df.loc[valid_mask].copy()

    # Compute union of all features
    all_features = set(feature_names)
    for feat_list in feature_names_per_quantile.values():
        all_features.update(feat_list)

    available_features = sorted([f for f in all_features if f in df_valid.columns])
    X = df_valid[available_features].fillna(0)
    y = df_valid["threes_per_min"]

    # Create binary target: is_zero
    is_zero = (y == 0).astype(int)
    print(f"Total samples: {len(X):,}")
    print(f"Zero samples: {is_zero.sum():,} ({is_zero.mean():.1%})")
    print(f"Positive samples: {(~is_zero.astype(bool)).sum():,}")

    # Split for validation (preserve temporal order)
    X_train, X_val, y_train, y_val, is_zero_train, is_zero_val = train_test_split(
        X, y, is_zero, test_size=config.val_fraction, shuffle=False
    )

    # =========================================================================
    # Stage 1: Train zero classifier
    # =========================================================================
    print("\n--- Stage 1: Zero Classifier ---")
    clf_features = [f for f in feature_names if f in X_train.columns]
    print(f"Using {len(clf_features)} features")

    zero_clf = xgb.XGBClassifier(
        objective="binary:logistic",
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        subsample=config.subsample,
        colsample_bytree=config.colsample_bytree,
        min_child_weight=config.min_child_weight,
        random_state=config.random_state,
        early_stopping_rounds=config.early_stopping_rounds,
        n_jobs=-1,
        eval_metric="logloss",
    )

    zero_clf.fit(
        X_train[clf_features],
        is_zero_train,
        eval_set=[(X_val[clf_features], is_zero_val)],
        verbose=False,
    )

    # Calibrate with isotonic regression
    p_zero_raw_val = zero_clf.predict_proba(X_val[clf_features])[:, 1]
    zero_calibrator = IsotonicRegression(out_of_bounds="clip")
    zero_calibrator.fit(p_zero_raw_val, is_zero_val)

    p_zero_cal_val = zero_calibrator.predict(p_zero_raw_val)
    print(f"Raw classifier accuracy: {(p_zero_raw_val.round() == is_zero_val).mean():.3f}")
    print(f"Mean p_zero (raw): {p_zero_raw_val.mean():.3f}, (calibrated): {p_zero_cal_val.mean():.3f}")
    print(f"Actual zero rate (val): {is_zero_val.mean():.3f}")

    # =========================================================================
    # Stage 2: Train positive quantile models
    # =========================================================================
    print("\n--- Stage 2: Positive Quantile Models ---")
    positive_mask_train = is_zero_train == 0
    positive_mask_val = is_zero_val == 0

    X_pos_train = X_train.loc[positive_mask_train]
    y_pos_train = y_train.loc[positive_mask_train]
    X_pos_val = X_val.loc[positive_mask_val]
    y_pos_val = y_val.loc[positive_mask_val]

    print(f"Positive training samples: {len(X_pos_train):,}")
    print(f"Positive validation samples: {len(X_pos_val):,}")

    positive_models = {}
    calibration_offsets = {}
    positive_feature_names_per_q = {}

    for q in config.quantiles:
        q_features = [f for f in feature_names_per_quantile.get(q, feature_names) if f in X_pos_train.columns]
        positive_feature_names_per_q[q] = q_features
        print(f"\nQuantile {q:.2f} ({len(q_features)} features)...")

        model = xgb.XGBRegressor(
            objective="reg:quantileerror",
            quantile_alpha=q,
            n_estimators=config.n_estimators,
            max_depth=config.max_depth,
            learning_rate=config.learning_rate,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            min_child_weight=config.min_child_weight,
            random_state=config.random_state,
            early_stopping_rounds=config.early_stopping_rounds,
            n_jobs=-1,
        )

        model.fit(
            X_pos_train[q_features],
            y_pos_train,
            eval_set=[(X_pos_val[q_features], y_pos_val)],
            verbose=False,
        )

        val_preds = model.predict(X_pos_val[q_features])
        val_coverage = (y_pos_val <= val_preds).mean()
        print(f"  Val coverage: {val_coverage:.3f} (target: {q:.2f})")

        # Conformal recalibration for positive model
        coverage_gap = abs(val_coverage - q)
        if coverage_gap > 0.03:
            residuals = y_pos_val.values - val_preds
            delta = float(np.quantile(residuals, q))
            calibration_offsets[q] = delta
            recal_coverage = (y_pos_val <= (val_preds + delta)).mean()
            print(f"  Recalibration: delta={delta:+.4f}, adjusted cov={recal_coverage:.3f}")
        else:
            calibration_offsets[q] = 0.0

        positive_models[q] = model

    # =========================================================================
    # Create and return hurdle model
    # =========================================================================
    hurdle_model = HurdleQuantileModel(
        zero_classifier=zero_clf,
        zero_calibrator=zero_calibrator,
        positive_quantile_models=positive_models,
        quantiles=list(config.quantiles),
        feature_names=clf_features,
        positive_feature_names_per_quantile=positive_feature_names_per_q,
        calibration_offsets=calibration_offsets,
    )

    print("\n--- Hurdle Model Training Complete ---")
    return hurdle_model


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
        self.hurdle_models: dict[str, HurdleQuantileModel] = {}  # For zero-inflated stats

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
        stats = stats or ["pts", "reb", "ast", "threes"]
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

            # Use count model for THREES (C4 architecture)
            if stat == "threes":
                # Train hurdle + count model for zero-inflated discrete distribution
                base_features = per_q_available.get(0.5, available_features)
                count_results = self._train_threes_count_model(
                    df=df,
                    feature_names=base_features,
                    config=config,
                )
                all_results[stat] = count_results
            else:
                # Train regular quantile model suite
                model_suite = QuantileModelSuite(config)
                results = model_suite.train(X, y, per_q_available, sample_weight=w)
                self.rate_models[stat] = model_suite
                all_results[stat] = results

        return all_results

    def _train_threes_count_model(
        self,
        df: pd.DataFrame,
        feature_names: list[str],
        config: QuantileModelConfig | None = None,
    ) -> dict:
        """
        Train two-stage hurdle + count model for THREES (C4 architecture).

        Stage 1: Zero classifier (P(threes = 0))
        Stage 2: Truncated NegBin count model (conditional on threes > 0)

        This replaces the C3 hurdle + quantile model that had 25.6% Q10 calibration gap.
        """
        from src.models.truncated_negbin import TruncatedNegBinModel

        print("\n" + "=" * 60)
        print("TRAINING THREES COUNT MODEL (C4)")
        print("=" * 60)

        config = config or self.config

        # Filter to valid rows (min >= 10, threes not null)
        valid_mask = df['actual_threes'].notna() & (df['actual_minutes'] >= 10)
        df_valid = df[valid_mask].copy()

        # Get available features
        available_features = [f for f in feature_names if f in df_valid.columns]

        X = df_valid[available_features].fillna(0)
        y_count = df_valid['actual_threes'].astype(int)  # Integer counts
        is_zero = (y_count == 0).astype(int)

        print(f"Total samples: {len(X):,}")
        print(f"Zero samples: {is_zero.sum():,} ({is_zero.mean():.1%})")
        print(f"Positive samples: {(~is_zero.astype(bool)).sum():,}")

        # === Stage 1: Zero Classifier ===
        print("\n--- Stage 1: Zero Classifier ---")

        # Temporal split for calibration
        n_train = int(len(X) * (1 - 0.15))
        X_train = X.iloc[:n_train]
        X_val = X.iloc[n_train:]
        is_zero_train = is_zero.iloc[:n_train]
        is_zero_val = is_zero.iloc[n_train:]
        y_train = y_count.iloc[:n_train]
        y_val = y_count.iloc[n_train:]

        zero_clf = xgb.XGBClassifier(
            objective='binary:logistic',
            n_estimators=config.n_estimators,
            max_depth=config.max_depth,
            learning_rate=config.learning_rate,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            early_stopping_rounds=config.early_stopping_rounds,
            n_jobs=-1,
            eval_metric='logloss',
        )

        zero_clf.fit(
            X_train, is_zero_train,
            eval_set=[(X_val, is_zero_val)],
            verbose=False
        )

        # Calibrate zero classifier with isotonic regression
        p_zero_raw = zero_clf.predict_proba(X_val)[:, 1]
        zero_calibrator = IsotonicRegression(out_of_bounds='clip')
        zero_calibrator.fit(p_zero_raw, is_zero_val)

        p_zero_cal = zero_calibrator.predict(p_zero_raw)
        print(f"Zero classifier - Raw p_zero: {p_zero_raw.mean():.3f}, Calibrated: {p_zero_cal.mean():.3f}")
        print(f"Actual zero rate (val): {is_zero_val.mean():.3f}")

        # === Stage 2: Truncated NegBin Count Model ===
        print("\n--- Stage 2: Truncated NegBin Count Model ---")

        positive_mask = y_count > 0
        X_pos = X[positive_mask]
        y_pos = y_count[positive_mask]

        print(f"Training count model on {len(X_pos):,} positive samples")

        count_model = TruncatedNegBinModel()
        count_results = count_model.fit(X_pos, y_pos)

        print(f"Global mu: {count_results['global_mu']:.3f}")
        print(f"Global alpha (overdispersion): {count_results['global_alpha']:.3f}")

        # Store models on pipeline
        self.threes_zero_classifier = zero_clf
        self.threes_zero_calibrator = zero_calibrator
        self.threes_count_model = count_model
        self.threes_zero_feature_names = available_features  # Store feature names explicitly

        return {
            'model_type': 'count',
            'zero_rate': is_zero.mean(),
            'global_mu': count_results['global_mu'],
            'global_alpha': count_results['global_alpha'],
            'n_positive_samples': len(X_pos),
        }

    def save_all(self, directory: str):
        """Save all models."""
        path = Path(directory)
        path.mkdir(exist_ok=True)

        if self.minutes_model:
            self.minutes_model.save(path / "minutes_model.joblib")

        for stat, model in self.rate_models.items():
            model.save(path / f"{stat}_rate_model.joblib")

        # Save hurdle models (these save multiple files) - legacy C3
        for stat, hurdle_model in self.hurdle_models.items():
            hurdle_model.save(path)

        # Save threes count model (C4) if present
        has_count_model = hasattr(self, 'threes_count_model') and self.threes_count_model is not None
        if has_count_model:
            # Save zero classifier and its feature names
            joblib.dump(self.threes_zero_classifier, path / "threes_zero_classifier.joblib")
            joblib.dump(self.threes_zero_calibrator, path / "threes_zero_calibrator.joblib")
            joblib.dump(self.threes_zero_feature_names, path / "threes_zero_feature_names.joblib")

            # Save count model
            self.threes_count_model.save(path)

            # Save flag file indicating count model (C4)
            with open(path / "threes_is_hurdle.json", "w") as f:
                json.dump({
                    "is_hurdle": True,
                    "model_type": "count",  # C4 architecture
                }, f, indent=2)
            print("Saved THREES count model (C4)")

        # Save per-quantile feature config
        joblib.dump(
            {
                "minutes_features": self.minutes_features,
                "rate_features": self.rate_features,
                "hurdle_stats": list(self.hurdle_models.keys()),
                "count_model_stats": ["threes"] if has_count_model else [],
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

        # Load feature config first to check for hurdle stats and count models
        config_path = path / "feature_config.joblib"
        hurdle_stats = []
        count_model_stats = []
        if config_path.exists():
            config = joblib.load(config_path)
            pipeline.minutes_features = config["minutes_features"]
            pipeline.rate_features = config["rate_features"]
            hurdle_stats = config.get("hurdle_stats", [])
            count_model_stats = config.get("count_model_stats", [])

        # Check for C4 count model via threes_is_hurdle.json
        is_count_model = False
        hurdle_json_path = path / "threes_is_hurdle.json"
        if hurdle_json_path.exists():
            with open(hurdle_json_path, "r") as f:
                hurdle_info = json.load(f)
                is_count_model = hurdle_info.get("model_type") == "count"

        # Load threes count model (C4) if present
        if is_count_model or "threes" in count_model_stats:
            from src.models.truncated_negbin import TruncatedNegBinModel

            # Load zero classifier, calibrator, and feature names
            zero_clf_path = path / "threes_zero_classifier.joblib"
            zero_cal_path = path / "threes_zero_calibrator.joblib"
            zero_feat_path = path / "threes_zero_feature_names.joblib"

            if zero_clf_path.exists() and zero_cal_path.exists():
                pipeline.threes_zero_classifier = joblib.load(zero_clf_path)
                pipeline.threes_zero_calibrator = joblib.load(zero_cal_path)
                if zero_feat_path.exists():
                    pipeline.threes_zero_feature_names = joblib.load(zero_feat_path)

                # Load count model
                if TruncatedNegBinModel.exists(path):
                    pipeline.threes_count_model = TruncatedNegBinModel.load(path)
                    print("Loaded THREES count model (C4)")
                else:
                    print("WARNING: Count model flag set but model files not found")

        # Load rate models
        for stat in ["pts", "reb", "ast", "threes"]:
            # Skip threes if we loaded a count model
            if stat == "threes" and hasattr(pipeline, 'threes_count_model'):
                continue

            # Check if this is a hurdle model (legacy C3)
            if stat in hurdle_stats or HurdleQuantileModel.is_hurdle_model(path):
                if stat == "threes" and HurdleQuantileModel.is_hurdle_model(path):
                    pipeline.hurdle_models[stat] = HurdleQuantileModel.load(path)
                    print(f"Loaded hurdle model for {stat}")
                    continue

            rate_path = path / f"{stat}_rate_model.joblib"
            if rate_path.exists():
                pipeline.rate_models[stat] = QuantileModelSuite.load(rate_path)

        return pipeline
