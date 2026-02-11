"""
Multiclass Classifier for THREES Count Prediction.

This module implements a vanilla XGBoost multiclass classifier that directly
predicts P(X=k) for k in {0, 1, 2, ..., 8+} (9 classes).

Key design decisions:
- Uses vanilla softmax multiclass, NOT ordinal regression
- Class 8 is a catch-all for 8+ made threes
- For betting P(X > line), just sum the tail of the predicted PMF
- Sampling is categorical from the predicted PMF

Replaces the broken C4 truncated NegBin architecture which had multiple
implementation issues causing poor calibration.
"""

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
import json
import logging

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb

logger = logging.getLogger(__name__)


@dataclass
class ThreesMulticlassConfig:
    """Configuration for the THREES multiclass classifier."""

    # XGBoost parameters
    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50

    # Number of classes: 0, 1, 2, 3, 4, 5, 6, 7, 8+
    num_classes: int = 9

    # Validation split
    val_size: float = 0.15

    # Random state
    random_state: int = 42


class ThreesMulticlassModel:
    """
    XGBoost multiclass classifier for THREES count prediction.

    Predicts P(X=k) for k in {0, 1, 2, ..., 8+}.
    Class 8 is a catch-all for 8+ made threes.

    Advantages over truncated NegBin:
    - Naturally discrete output (no continuous->discrete conversion)
    - No truncation math issues
    - Single model instead of zero classifier + count model
    - Well-established technique for ordinal/count outcomes
    """

    def __init__(self, config: Optional[ThreesMulticlassConfig] = None):
        self.config = config or ThreesMulticlassConfig()
        self.model: Optional[xgb.XGBClassifier] = None
        self.feature_names: list[str] = []

        # Store class distribution from training for diagnostics
        self._class_distribution: np.ndarray = np.zeros(self.config.num_classes)

    def fit(
        self,
        X: pd.DataFrame,
        y: np.ndarray,
        sample_weight: Optional[np.ndarray] = None,
    ) -> dict:
        """
        Train multiclass classifier on threes counts.

        Args:
            X: Features DataFrame
            y: Integer threes counts (0, 1, 2, ...)
            sample_weight: Optional sample weights

        Returns:
            Dict with training info
        """
        self.feature_names = list(X.columns)
        n_samples = len(X)

        # Bin 8+ into class 8 (catch-all)
        y_binned = np.clip(y.astype(int), 0, self.config.num_classes - 1)

        # Store class distribution for diagnostics
        for k in range(self.config.num_classes):
            self._class_distribution[k] = (y_binned == k).mean()

        logger.info(f"Fitting ThreesMulticlassModel on {n_samples:,} samples")
        logger.info(f"Class distribution: {dict(enumerate(self._class_distribution.round(3)))}")

        # Train/val split (temporal ordering preserved)
        X_train, X_val, y_train, y_val = train_test_split(
            X,
            y_binned,
            test_size=self.config.val_size,
            shuffle=False,
        )

        if sample_weight is not None:
            sw_train, _ = train_test_split(
                sample_weight,
                test_size=self.config.val_size,
                shuffle=False,
            )
        else:
            sw_train = None

        # Create and train classifier
        self.model = xgb.XGBClassifier(
            objective='multi:softprob',
            num_class=self.config.num_classes,
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            min_child_weight=self.config.min_child_weight,
            eval_metric='mlogloss',
            random_state=self.config.random_state,
            n_jobs=-1,
        )

        self.model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            sample_weight=sw_train,
            verbose=False,
        )

        # Log validation metrics
        val_probs = self.model.predict_proba(X_val)
        val_pred = val_probs.argmax(axis=1)
        val_accuracy = (val_pred == y_val).mean()

        # best_iteration is only set when early stopping triggers
        try:
            best_iter = self.model.best_iteration
        except AttributeError:
            best_iter = self.config.n_estimators

        logger.info(f"Validation accuracy: {val_accuracy:.3f}")
        logger.info(f"Best iteration: {best_iter}")

        return {
            'n_samples': n_samples,
            'n_features': len(self.feature_names),
            'val_accuracy': val_accuracy,
            'best_iteration': best_iter,
            'class_distribution': self._class_distribution.tolist(),
        }

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict class probabilities P(X=k) for k in {0,...,8}.

        Args:
            X: Feature DataFrame

        Returns:
            Array of shape (n_samples, 9) with probabilities
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")

        # Align features
        missing_cols = set(self.feature_names) - set(X.columns)
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}, filling with 0")

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)

        return self.model.predict_proba(X_aligned)

    def sample(
        self,
        X: pd.DataFrame,
        n_samples: int = 10000,
        rng: Optional[np.random.Generator] = None,
    ) -> np.ndarray:
        """
        Sample threes counts from predicted categorical distribution.

        Args:
            X: Feature DataFrame (single row or batch)
            n_samples: Number of MC samples per row
            rng: Random generator for reproducibility

        Returns:
            Integer samples, shape (n_rows, n_samples) or (n_samples,) if single row
        """
        if rng is None:
            rng = np.random.default_rng(self.config.random_state)

        probs = self.predict_proba(X)  # (n_rows, 9)
        n_rows = len(X)

        if n_rows == 1:
            # Single row - return flat array
            return rng.choice(self.config.num_classes, size=n_samples, p=probs[0])

        # Batch - return 2D array
        samples = np.zeros((n_rows, n_samples), dtype=int)
        for i, p in enumerate(probs):
            # Ensure probabilities sum to 1 (numerical stability)
            p = p / p.sum()
            samples[i] = rng.choice(self.config.num_classes, size=n_samples, p=p)

        return samples

    def sample_single(
        self,
        features: dict,
        n_samples: int = 10000,
        rng: Optional[np.random.Generator] = None,
    ) -> np.ndarray:
        """
        Sample for a single player given feature dict.

        Args:
            features: Dict of feature name -> value
            n_samples: Number of MC samples
            rng: Random generator

        Returns:
            Integer samples, shape (n_samples,)
        """
        X = pd.DataFrame([features])[self.feature_names].fillna(0)
        return self.sample(X, n_samples=n_samples, rng=rng).flatten()

    def prob_over(self, X: pd.DataFrame, line: float) -> np.ndarray:
        """
        Compute P(X > line) by summing tail of PMF.

        For betting: over 2.5 means >= 3, so threshold = floor(line) + 1

        Args:
            X: Feature DataFrame
            line: The betting line (e.g., 2.5)

        Returns:
            P(X > line) for each row
        """
        probs = self.predict_proba(X)  # (n_samples, 9)
        threshold = int(np.floor(line)) + 1  # Over 2.5 means >= 3

        if threshold >= self.config.num_classes:
            return np.zeros(len(X))  # Can't exceed 8+ class

        return probs[:, threshold:].sum(axis=1)

    def prob_under(self, X: pd.DataFrame, line: float) -> np.ndarray:
        """
        Compute P(X < line) by summing left tail of PMF.

        For betting: under 2.5 means <= 2, so threshold = floor(line)

        Args:
            X: Feature DataFrame
            line: The betting line (e.g., 2.5)

        Returns:
            P(X < line) for each row
        """
        probs = self.predict_proba(X)  # (n_samples, 9)
        threshold = int(np.floor(line))  # Under 2.5 means <= 2

        if threshold < 0:
            return np.zeros(len(X))

        # Sum classes 0 through threshold (inclusive)
        return probs[:, :threshold + 1].sum(axis=1)

    def get_quantiles(
        self,
        X: pd.DataFrame,
        quantiles: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90],
    ) -> np.ndarray:
        """
        Get quantiles from the predicted PMF.

        Uses CDF inversion: find smallest k where CDF(k) >= q

        Args:
            X: Feature DataFrame
            quantiles: List of quantile levels

        Returns:
            Array of shape (n_samples, len(quantiles))
        """
        probs = self.predict_proba(X)  # (n_rows, 9)
        cdf = np.cumsum(probs, axis=1)  # (n_rows, 9)

        result = np.zeros((len(X), len(quantiles)), dtype=int)
        for i, q in enumerate(quantiles):
            # Find first k where CDF(k) >= q
            result[:, i] = (cdf >= q).argmax(axis=1)

        return result

    def save(self, directory: Path) -> None:
        """Save model to disk."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        # Save XGBoost model
        joblib.dump(self.model, directory / "threes_multiclass_model.joblib")

        # Save config and metadata
        metadata = {
            'feature_names': self.feature_names,
            'class_distribution': self._class_distribution.tolist(),
            'config': asdict(self.config),
        }

        with open(directory / "threes_multiclass_meta.json", 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved ThreesMulticlassModel to {directory}")

    @classmethod
    def load(cls, directory: Path) -> 'ThreesMulticlassModel':
        """Load model from disk."""
        directory = Path(directory)

        # Load metadata
        with open(directory / "threes_multiclass_meta.json") as f:
            metadata = json.load(f)

        # Create config
        config_dict = metadata.get('config', {})
        config = ThreesMulticlassConfig(
            n_estimators=config_dict.get('n_estimators', 1000),
            max_depth=config_dict.get('max_depth', 5),
            learning_rate=config_dict.get('learning_rate', 0.03),
            subsample=config_dict.get('subsample', 0.8),
            colsample_bytree=config_dict.get('colsample_bytree', 0.8),
            min_child_weight=config_dict.get('min_child_weight', 3),
            early_stopping_rounds=config_dict.get('early_stopping_rounds', 50),
            num_classes=config_dict.get('num_classes', 9),
            val_size=config_dict.get('val_size', 0.15),
            random_state=config_dict.get('random_state', 42),
        )

        model = cls(config=config)
        model.feature_names = metadata['feature_names']
        model._class_distribution = np.array(metadata.get('class_distribution', [0] * 9))

        # Load XGBoost model
        model.model = joblib.load(directory / "threes_multiclass_model.joblib")

        logger.info(f"Loaded ThreesMulticlassModel from {directory}")

        return model

    @staticmethod
    def exists(directory: Path) -> bool:
        """Check if a multiclass model exists in the directory."""
        directory = Path(directory)
        return (directory / "threes_multiclass_meta.json").exists()
