"""MLB Binary Classification Model — XGBClassifier for P(HR >= 1).

Uses isotonic calibration to produce well-calibrated probabilities
from XGBoost's raw binary:logistic output.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.isotonic import IsotonicRegression

logger = logging.getLogger(__name__)


@dataclass
class BinaryModelConfig:
    """Configuration for the binary HR classifier."""

    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50
    val_fraction: float = 0.15
    random_state: int = 42

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: dict) -> BinaryModelConfig:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


MLB_HR_BINARY_CONFIG = BinaryModelConfig()


class MLBBinaryModel:
    """XGBClassifier with isotonic calibration for P(HR >= 1).

    Handles ~7-10% positive rate via scale_pos_weight.
    """

    def __init__(self, config: BinaryModelConfig | None = None):
        self.config = config or MLB_HR_BINARY_CONFIG
        self.model: xgb.XGBClassifier | None = None
        self.calibrator: IsotonicRegression | None = None
        self.feature_names: list[str] = []
        self._metadata: dict = {}

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series | np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> dict:
        """Train binary classifier with auto-computed class weights.

        Args:
            X: Feature matrix.
            y: Binary target (1 if HR >= 1, else 0).
            sample_weight: Optional per-sample weights.

        Returns:
            Dict with training metrics.
        """
        y_arr = np.asarray(y, dtype=int)
        n_pos = y_arr.sum()
        n_neg = len(y_arr) - n_pos
        spw = n_neg / max(n_pos, 1)

        self.feature_names = list(X.columns)

        # Temporal train/val split (no shuffle for time-series)
        val_size = int(len(X) * self.config.val_fraction)
        X_train, X_val = X.iloc[:-val_size], X.iloc[-val_size:]
        y_train, y_val = y_arr[:-val_size], y_arr[-val_size:]

        sw_train = sample_weight[:-val_size] if sample_weight is not None else None

        self.model = xgb.XGBClassifier(
            objective="binary:logistic",
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            min_child_weight=self.config.min_child_weight,
            scale_pos_weight=spw,
            random_state=self.config.random_state,
            eval_metric="logloss",
            early_stopping_rounds=self.config.early_stopping_rounds,
        )

        self.model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            sample_weight=sw_train,
            verbose=False,
        )

        # Isotonic calibration on validation set
        raw_proba = self.model.predict_proba(X_val)[:, 1]
        self.calibrator = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
        self.calibrator.fit(raw_proba, y_val)

        # Compute metrics
        cal_proba = self.calibrator.predict(raw_proba)
        val_pos_rate = y_val.mean()

        self._metadata = {
            "n_train": len(X_train),
            "n_val": len(X_val),
            "n_features": len(self.feature_names),
            "positive_rate_train": float(y_train.mean()),
            "positive_rate_val": float(val_pos_rate),
            "scale_pos_weight": float(spw),
            "mean_calibrated_proba_val": float(cal_proba.mean()),
            "best_iteration": self.model.best_iteration,
        }

        logger.info(
            "HR binary model trained: %d train, %d val, pos_rate=%.3f, spw=%.1f, best_iter=%d",
            len(X_train), len(X_val), val_pos_rate, spw, self.model.best_iteration,
        )

        return self._metadata

    def predict_proba(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        """Predict calibrated P(HR >= 1).

        Returns 1D array of probabilities.
        """
        if self.model is None:
            raise RuntimeError("Model not fitted")

        raw_proba = self.model.predict_proba(X)[:, 1]

        if self.calibrator is not None:
            return self.calibrator.predict(raw_proba)

        return raw_proba

    def save(self, directory: str | Path) -> None:
        """Save model, calibrator, and metadata to directory."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.model, directory / "hr_binary_model.joblib")
        joblib.dump(self.calibrator, directory / "hr_calibrator.joblib")

        meta = {
            **self._metadata,
            "feature_names": self.feature_names,
            "config": self.config.to_dict(),
        }
        with open(directory / "hr_binary_meta.json", "w") as f:
            json.dump(meta, f, indent=4)

        logger.info("Saved HR binary model to %s", directory)

    @classmethod
    def load(cls, directory: str | Path) -> MLBBinaryModel:
        """Load a saved binary model."""
        directory = Path(directory)

        with open(directory / "hr_binary_meta.json") as f:
            meta = json.load(f)

        config = BinaryModelConfig.from_dict(meta.get("config", {}))
        instance = cls(config=config)
        instance.model = joblib.load(directory / "hr_binary_model.joblib")
        instance.calibrator = joblib.load(directory / "hr_calibrator.joblib")
        instance.feature_names = meta.get("feature_names", [])
        instance._metadata = {k: v for k, v in meta.items() if k not in ("feature_names", "config")}

        logger.info("Loaded HR binary model from %s", directory)
        return instance
