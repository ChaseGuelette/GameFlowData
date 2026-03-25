"""Binomial Model for integer count prediction (hits in at-bats).

Predicts the hit probability p of a Binomial(n, p) distribution where
n (at-bats) is externally provided.  Unlike NegBin (which assumes
overdispersion), this correctly handles underdispersed data where
variance < mean — the canonical case for baseball hits.

Architecture:
    - Single XGBoost booster with custom binomial NLL objective
    - Predicts p (hit probability) via logit link: p = sigmoid(raw_pred)
    - At-bats n passed via DMatrix weights
    - Closed-form probability: P(hits > line) = 1 - binom.cdf(floor(line), n, p)
    - Sampling via scipy.stats.binom.rvs

Gradient: n*p - y
Hessian:  n*p*(1-p)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from scipy.stats import binom
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


# ======================================================================
# Helpers
# ======================================================================

def _sigmoid(x: np.ndarray) -> np.ndarray:
    """Numerically stable sigmoid."""
    x = np.clip(x, -500, 500)
    return np.clip(1.0 / (1.0 + np.exp(-x)), 1e-7, 1 - 1e-7)


# ======================================================================
# Custom XGBoost objective & eval metric
# ======================================================================

def _binomial_obj(preds: np.ndarray, dtrain: xgb.DMatrix):
    """XGBoost custom objective for binomial regression.

    preds: raw predictions (logit scale), shape (n_samples,)
    DMatrix weights encode at-bats n.
    DMatrix labels encode hits y.

    Returns (grad, hess) each shape (n_samples,).
    """
    p = _sigmoid(preds)
    y = dtrain.get_label()
    n = dtrain.get_weight()  # at-bats

    # Gradient of NLL w.r.t. raw prediction (logit scale)
    # NLL = -(y*log(p) + (n-y)*log(1-p))
    # dNLL/d(raw) = dNLL/dp * dp/d(raw) = (-(y/p) + (n-y)/(1-p)) * p*(1-p)
    #             = -y*(1-p) + (n-y)*p = n*p - y
    grad = n * p - y

    # Hessian: d2NLL/d(raw)2 = n*p*(1-p)
    hess = np.maximum(n * p * (1 - p), 1e-6)

    # Handle NaN/Inf
    grad = np.nan_to_num(grad, nan=0.0, posinf=10.0, neginf=-10.0)
    hess = np.nan_to_num(hess, nan=1e-6, posinf=100.0, neginf=1e-6)

    # Clip extreme gradients
    grad = np.clip(grad, -50, 50)

    return grad, hess


def _binomial_nll_eval(preds: np.ndarray, dtrain: xgb.DMatrix):
    """Custom eval metric: mean binomial NLL."""
    p = np.clip(_sigmoid(preds), 1e-7, 1 - 1e-7)
    y = dtrain.get_label()
    n = dtrain.get_weight()  # at-bats

    nll = -(y * np.log(p) + (n - y) * np.log(1 - p))
    nll = nll[np.isfinite(nll)]

    return "binom_nll", float(np.mean(nll))


# ======================================================================
# Config
# ======================================================================

@dataclass
class BinomialConfig:
    """Configuration for the Binomial model."""

    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50

    # Validation split
    val_size: float = 0.15

    # Random state
    random_state: int = 42


# ======================================================================
# Model
# ======================================================================

class BinomialModel:
    """Predicts hit probability p of a Binomial(n, p) distribution.

    At-bats n is externally supplied (actual AB for training/backtesting,
    projected AB for live inference).

    Uses a single XGBoost booster with custom binomial NLL objective.
    The logit link maps raw predictions to valid p ∈ (0, 1).
    """

    def __init__(
        self,
        config: BinomialConfig | None = None,
        model_name: str = "binomial",
    ):
        self.config = config or BinomialConfig()
        self.model_name = model_name
        self.feature_names: list[str] = []

        # Global MLE p (fallback)
        self._global_p: float = 0.25
        self._global_n_mean: float = 3.5

        # XGBoost booster
        self._booster: xgb.Booster | None = None

        # Base margin init value (logit of global p)
        self._raw_p_init: float = 0.0

    # ------------------------------------------------------------------
    # Fitting
    # ------------------------------------------------------------------

    def _fit_global_params(self, y: np.ndarray, at_bats: np.ndarray) -> tuple[float, float]:
        """Fit global binomial parameters via MLE.

        Returns (p_global, mean_n).
        """
        total_hits = float(y.sum())
        total_ab = float(at_bats.sum())
        p_global = total_hits / max(total_ab, 1.0)
        mean_n = float(at_bats.mean())

        logger.info(
            "Global MLE: p=%.4f (BA=%.3f), mean_AB=%.1f, total_H=%d, total_AB=%d",
            p_global, p_global, mean_n, int(total_hits), int(total_ab),
        )
        return p_global, mean_n

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        at_bats: np.ndarray,
    ) -> dict:
        """Fit the binomial model.

        Args:
            X: Feature DataFrame.
            y: Integer hits >= 0.
            at_bats: Integer at-bats for each row (used as binomial n).

        Returns:
            Dict with training metadata.
        """
        y_np = y.values.astype(float)
        at_bats = np.asarray(at_bats, dtype=float)

        if y_np.min() < 0:
            raise ValueError(f"BinomialModel requires y >= 0, got min={y_np.min()}")

        # Validate: hits <= at_bats
        violations = (y_np > at_bats).sum()
        if violations > 0:
            logger.warning(
                "%d rows have hits > at_bats — clamping y to at_bats", violations
            )
            y_np = np.minimum(y_np, at_bats)

        # Filter out rows with 0 at-bats (no plate appearances)
        valid_mask = at_bats > 0
        if (~valid_mask).sum() > 0:
            logger.warning(
                "Dropping %d rows with 0 at-bats", (~valid_mask).sum()
            )
            X = X[valid_mask].reset_index(drop=True)
            y_np = y_np[valid_mask]
            at_bats = at_bats[valid_mask]

        self.feature_names = list(X.columns)
        n_samples = len(X)

        logger.info("Fitting BinomialModel (%s) on %s samples", self.model_name, f"{n_samples:,}")
        logger.info(
            "Target stats: mean_hits=%.3f, var=%.3f, zero_frac=%.3f, mean_AB=%.1f",
            y_np.mean(), y_np.var(), (y_np == 0).mean(), at_bats.mean(),
        )
        logger.info(
            "Dispersion ratio (var/mean): %.3f (< 1 = underdispersed, binomial is correct)",
            y_np.var() / max(y_np.mean(), 1e-6),
        )

        # Global MLE for initialisation
        self._global_p, self._global_n_mean = self._fit_global_params(y_np, at_bats)

        # Raw init (logit of global p)
        self._raw_p_init = float(np.log(
            max(self._global_p, 1e-6) / max(1 - self._global_p, 1e-6)
        ))
        logger.info("Init: p=%.4f (raw=%.4f)", self._global_p, self._raw_p_init)

        # Train/val split (preserve temporal order)
        X_train, X_val, y_train, y_val, ab_train, ab_val = train_test_split(
            X, y_np, at_bats,
            test_size=self.config.val_size,
            shuffle=False,
        )

        # Build DMatrices
        # Labels = hits, Weights = at-bats (used in objective)
        dtrain = xgb.DMatrix(X_train, label=y_train, weight=ab_train)
        dval = xgb.DMatrix(X_val, label=y_val, weight=ab_val)

        # Set base margin (logit init for all rows)
        dtrain.set_base_margin(np.full(len(X_train), self._raw_p_init))
        dval.set_base_margin(np.full(len(X_val), self._raw_p_init))

        # XGBoost params (single output)
        params = {
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "subsample": self.config.subsample,
            "colsample_bytree": self.config.colsample_bytree,
            "min_child_weight": self.config.min_child_weight,
            "base_score": 0,
            "disable_default_eval_metric": True,
            "tree_method": "hist",
            "seed": self.config.random_state,
        }

        logger.info("Training binomial model...")
        self._booster = xgb.train(
            params,
            dtrain,
            num_boost_round=self.config.n_estimators,
            evals=[(dtrain, "train"), (dval, "val")],
            obj=_binomial_obj,
            custom_metric=_binomial_nll_eval,
            early_stopping_rounds=self.config.early_stopping_rounds,
            verbose_eval=50,
        )

        # Log validation metrics
        val_pred = self._booster.predict(dval)
        val_p = _sigmoid(val_pred)
        val_expected_hits = val_p * ab_val

        logger.info(
            "Val predictions: p_mean=%.4f, expected_hits_mean=%.3f, actual_mean=%.3f",
            val_p.mean(), val_expected_hits.mean(), y_val.mean(),
        )

        return {
            "global_p": self._global_p,
            "global_n_mean": self._global_n_mean,
            "n_samples": n_samples,
            "n_features": len(self.feature_names),
            "zero_fraction": float((y_np == 0).mean()),
            "mean_at_bats": float(at_bats.mean()),
        }

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict_params(
        self, X: pd.DataFrame, at_bats: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """Predict (p, n) for each row.

        Args:
            X: Feature DataFrame.
            at_bats: At-bats for each row.

        Returns:
            p — shape (n,), hit probability
            n — shape (n,), at-bats (passed through)
        """
        if self._booster is None:
            raise ValueError("Model not fitted. Call fit() first.")

        # Align features
        missing_cols = set(self.feature_names) - set(X.columns)
        if missing_cols:
            logger.warning("Missing columns: %s, filling with 0", missing_cols)

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)
        at_bats = np.asarray(at_bats, dtype=float)

        dmat = xgb.DMatrix(X_aligned)
        dmat.set_base_margin(np.full(len(X_aligned), self._raw_p_init))

        raw_preds = self._booster.predict(dmat)
        p = _sigmoid(raw_preds)

        return p, at_bats

    def predict_proba(
        self,
        X: pd.DataFrame,
        at_bats: np.ndarray,
        line: float | np.ndarray,
    ) -> np.ndarray:
        """Predict P(hits > line) for each row.

        Uses closed-form binomial CDF:
            P(hits > line) = 1 - binom.cdf(floor(line), n, p)

        Args:
            X: Feature DataFrame.
            at_bats: At-bats for each row.
            line: Prop line(s) — scalar or array matching rows.

        Returns:
            Array of P(over) for each row.
        """
        p, n = self.predict_params(X, at_bats)

        if np.isscalar(line):
            line = np.full(len(X), line)
        else:
            line = np.asarray(line, dtype=float)

        threshold = np.floor(line).astype(int)

        # Vectorized CDF
        p_over = 1.0 - binom.cdf(threshold, n.astype(int), p)

        return np.clip(p_over, 0.0, 1.0)

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def sample(
        self,
        X: pd.DataFrame,
        at_bats: np.ndarray,
        n_samples: int = 10_000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """Draw MC samples from binomial for each row.

        Args:
            X: Features (single row or batch).
            at_bats: At-bats per row.
            n_samples: Samples per row.
            rng: Random state for reproducibility.

        Returns:
            Integer samples, shape ``(n_rows, n_samples)``
            or ``(n_samples,)`` when a single row is passed.
        """
        rng = rng or np.random.RandomState(self.config.random_state)

        p, n = self.predict_params(X, at_bats)
        n_rows = len(X)

        if n_rows == 1:
            return binom.rvs(
                n=int(max(n[0], 1)), p=p[0], size=n_samples, random_state=rng
            )

        samples = np.zeros((n_rows, n_samples), dtype=int)
        for i in range(n_rows):
            n_i = int(max(n[i], 1))
            samples[i, :] = binom.rvs(n=n_i, p=p[i], size=n_samples, random_state=rng)

        return samples

    def sample_single(
        self,
        features: dict,
        at_bats: float,
        n_samples: int = 10_000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """Sample for a single observation given a feature dict."""
        X = pd.DataFrame([features]).reindex(columns=self.feature_names, fill_value=0)
        ab = np.array([at_bats])
        return self.sample(X, ab, n_samples=n_samples, rng=rng).flatten()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, directory: Path | str) -> None:
        """Save model artifacts to *directory*.

        Format::
            {model_name}_binomial_booster.json — XGBoost booster
            {model_name}_binomial_meta.json    — metadata + config
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        if self._booster is not None:
            booster_path = directory / f"{self.model_name}_binomial_booster.json"
            self._booster.save_model(str(booster_path))

        metadata = {
            "model_name": self.model_name,
            "model_type": "binomial",
            "feature_names": self.feature_names,
            "global_p": self._global_p,
            "global_n_mean": self._global_n_mean,
            "raw_p_init": self._raw_p_init,
            "distribution": "Binomial",
            "n_dist_params": 1,
            "param_names": ["hit_probability"],
            "response_fns": ["sigmoid"],
            "config": {
                "n_estimators": self.config.n_estimators,
                "max_depth": self.config.max_depth,
                "learning_rate": self.config.learning_rate,
                "subsample": self.config.subsample,
                "colsample_bytree": self.config.colsample_bytree,
                "min_child_weight": self.config.min_child_weight,
                "early_stopping_rounds": self.config.early_stopping_rounds,
            },
        }
        with open(directory / f"{self.model_name}_binomial_meta.json", "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info("Saved BinomialModel (%s) to %s", self.model_name, directory)

    @classmethod
    def load(cls, directory: Path | str, model_name: str = "binomial") -> BinomialModel:
        """Load model from *directory*."""
        directory = Path(directory)

        with open(directory / f"{model_name}_binomial_meta.json") as f:
            metadata = json.load(f)

        cfg = metadata.get("config", {})
        config = BinomialConfig(
            n_estimators=cfg.get("n_estimators", 1000),
            max_depth=cfg.get("max_depth", 5),
            learning_rate=cfg.get("learning_rate", 0.03),
            subsample=cfg.get("subsample", 0.8),
            colsample_bytree=cfg.get("colsample_bytree", 0.8),
            min_child_weight=cfg.get("min_child_weight", 3),
            early_stopping_rounds=cfg.get("early_stopping_rounds", 50),
        )

        model = cls(config=config, model_name=model_name)
        model.feature_names = metadata["feature_names"]
        model._global_p = metadata["global_p"]
        model._global_n_mean = metadata["global_n_mean"]
        model._raw_p_init = metadata.get("raw_p_init", 0.0)

        booster_path = directory / f"{model_name}_binomial_booster.json"
        if booster_path.exists():
            model._booster = xgb.Booster()
            model._booster.load_model(str(booster_path))
            logger.info("Loaded BinomialModel (%s) from %s", model_name, directory)
        else:
            raise FileNotFoundError(f"Booster not found at {booster_path}")

        return model

    @staticmethod
    def exists(directory: Path | str, model_name: str = "binomial") -> bool:
        """Check whether a saved BinomialModel exists in *directory*."""
        directory = Path(directory)
        return (directory / f"{model_name}_binomial_meta.json").exists()
