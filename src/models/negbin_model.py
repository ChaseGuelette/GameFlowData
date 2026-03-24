"""Standard Negative Binomial Model for integer count prediction.

Predicts the parameters (mu, alpha) of a NegBin distribution, then
samples integer counts via inverse CDF.  Unlike TruncatedNegBinModel
(used for NBA 3PM where y >= 1), this version handles zeros natively
— making it appropriate for MLB batter stats (hits, TB, RBI, runs)
where ~40 % of games produce zero.

Architecture:
    - Two XGBoost regressors predicting log(mu) and log(alpha)
    - MLE-based global parameter estimation for initialisation
    - Inverse CDF sampling via scipy.stats.nbinom.ppf
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
from scipy.optimize import minimize
from scipy.stats import nbinom
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


@dataclass
class NegBinConfig:
    """Configuration for the standard Negative Binomial model."""

    # XGBoost parameters for mu model
    mu_n_estimators: int = 1000
    mu_max_depth: int = 5
    mu_learning_rate: float = 0.03
    mu_subsample: float = 0.8
    mu_colsample_bytree: float = 0.8
    mu_min_child_weight: int = 3
    mu_early_stopping_rounds: int = 50

    # XGBoost parameters for alpha model (simpler — less variable)
    alpha_n_estimators: int = 500
    alpha_max_depth: int = 3
    alpha_learning_rate: float = 0.05
    alpha_subsample: float = 0.8
    alpha_colsample_bytree: float = 0.8

    # Validation split
    val_size: float = 0.15

    # Clamping ranges for predictions
    log_mu_min: float = -3.0  # mu >= ~0.05  (lower than truncated — need small mu for low-count stats)
    log_mu_max: float = 3.0   # mu <= ~20
    log_alpha_min: float = -2.0  # alpha >= ~0.14
    log_alpha_max: float = 2.0   # alpha <= ~7.4

    # Random state
    random_state: int = 42


class NegBinModel:
    """Predicts parameters of a standard (non-truncated) negative binomial.

    Parameterisation:
        mu   — mean of the distribution (mu > 0)
        alpha — overdispersion (alpha > 0; variance = mu + alpha * mu^2)

    Two XGBoost regressors predict log(mu) and log(alpha) from features.
    Samples are drawn via ``scipy.stats.nbinom.ppf`` (inverse CDF).
    """

    def __init__(
        self,
        config: NegBinConfig | None = None,
        model_name: str = "negbin",
    ):
        self.config = config or NegBinConfig()
        self.model_name = model_name
        self.mu_model: xgb.XGBRegressor | None = None
        self.alpha_model: xgb.XGBRegressor | None = None
        self.feature_names: list[str] = []

        # MLE-fitted global parameters (used as fallback / regularisation)
        self._global_alpha: float = 1.0
        self._global_mu: float = 1.0

    # ------------------------------------------------------------------
    # Fitting
    # ------------------------------------------------------------------

    def _fit_global_params(self, y: np.ndarray) -> tuple[float, float]:
        """Fit standard NegBin parameters via MLE.

        Returns (mu, alpha) global estimates.
        """

        def negbin_nll(params):
            mu, alpha = params
            if mu <= 0 or alpha <= 0.01:
                return 1e10

            n = 1.0 / alpha
            p = 1.0 / (1.0 + alpha * mu)

            if p <= 0 or p >= 1 or n <= 0:
                return 1e10

            log_probs = nbinom.logpmf(y.astype(int), n, p)
            log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
            return -np.sum(log_probs)

        # Moment-based initial guess
        mu_init = max(0.1, float(y.mean()))
        var_init = float(y.var())
        alpha_init = max(0.1, (var_init - mu_init) / (mu_init ** 2 + 1e-6))

        result = minimize(
            negbin_nll,
            x0=[mu_init, alpha_init],
            method="Nelder-Mead",
            options={"maxiter": 1000},
        )

        mu_mle = max(0.05, result.x[0])
        alpha_mle = max(0.01, result.x[1])

        logger.info(
            "Global MLE: mu=%.3f, alpha=%.3f, success=%s",
            mu_mle, alpha_mle, result.success,
        )
        return mu_mle, alpha_mle

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> dict:
        """Fit the standard NegBin model.

        Args:
            X: Feature DataFrame.
            y: Integer counts >= 0.
            sample_weight: Optional sample weights.

        Returns:
            Dict with training metadata.
        """
        y_np = y.values.astype(float)

        if y_np.min() < 0:
            raise ValueError(
                f"NegBinModel requires y >= 0, got min={y_np.min()}"
            )

        self.feature_names = list(X.columns)
        n_samples = len(X)

        logger.info("Fitting NegBinModel (%s) on %s samples", self.model_name, f"{n_samples:,}")
        logger.info("Target stats: mean=%.3f, var=%.3f, zero_frac=%.3f",
                     y_np.mean(), y_np.var(), (y_np == 0).mean())

        # Stage 1: Global MLE
        self._global_mu, self._global_alpha = self._fit_global_params(y_np)

        # Stage 2: Prepare XGBoost targets
        # mu target: the expected value for each observation.
        # For a standard NegBin the MLE mu == sample mean, so we use y
        # directly (with epsilon for log stability on zeros).
        EPSILON = 0.01
        log_mu_target = np.log(y_np + EPSILON)

        # alpha target: residual-based overdispersion estimate
        mu_baseline = y_np.mean()
        residuals_sq = (y_np - mu_baseline) ** 2
        overdispersion_ratio = residuals_sq / (mu_baseline ** 2 + 1e-6)
        log_alpha_target = np.log(np.clip(overdispersion_ratio, 0.1, 10.0))

        # Train/val split (preserve temporal order)
        split = train_test_split(
            X,
            log_mu_target,
            log_alpha_target,
            test_size=self.config.val_size,
            shuffle=False,
        )
        X_train, X_val, y_mu_train, y_mu_val, y_alpha_train, y_alpha_val = split

        if sample_weight is not None:
            sw_train, _ = train_test_split(
                sample_weight,
                test_size=self.config.val_size,
                shuffle=False,
            )
        else:
            sw_train = None

        # Stage 3: Train mu model
        logger.info("Training mu model...")
        self.mu_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=self.config.mu_n_estimators,
            max_depth=self.config.mu_max_depth,
            learning_rate=self.config.mu_learning_rate,
            subsample=self.config.mu_subsample,
            colsample_bytree=self.config.mu_colsample_bytree,
            min_child_weight=self.config.mu_min_child_weight,
            random_state=self.config.random_state,
            n_jobs=-1,
        )
        self.mu_model.fit(
            X_train,
            y_mu_train,
            eval_set=[(X_val, y_mu_val)],
            sample_weight=sw_train,
            verbose=False,
        )

        mu_train_pred = np.exp(self.mu_model.predict(X_train))
        mu_val_pred = np.exp(self.mu_model.predict(X_val))
        logger.info(
            "  Train mu: pred_mean=%.3f, val mu: pred_mean=%.3f",
            mu_train_pred.mean(), mu_val_pred.mean(),
        )

        # Stage 4: Train alpha model
        logger.info("Training alpha model...")
        self.alpha_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=self.config.alpha_n_estimators,
            max_depth=self.config.alpha_max_depth,
            learning_rate=self.config.alpha_learning_rate,
            subsample=self.config.alpha_subsample,
            colsample_bytree=self.config.alpha_colsample_bytree,
            random_state=self.config.random_state,
            n_jobs=-1,
        )
        self.alpha_model.fit(
            X_train,
            y_alpha_train,
            eval_set=[(X_val, y_alpha_val)],
            sample_weight=sw_train,
            verbose=False,
        )

        alpha_val_pred = np.exp(self.alpha_model.predict(X_val))
        logger.info("  Val alpha: pred_mean=%.3f", alpha_val_pred.mean())

        return {
            "global_mu": self._global_mu,
            "global_alpha": self._global_alpha,
            "n_samples": n_samples,
            "n_features": len(self.feature_names),
            "zero_fraction": float((y_np == 0).mean()),
        }

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict_params(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Predict (mu, alpha) for each row.

        Returns:
            mu  — shape (n,), mean of NegBin
            alpha — shape (n,), dispersion
        """
        if self.mu_model is None or self.alpha_model is None:
            raise ValueError("Model not fitted. Call fit() first.")

        # Align features
        missing_cols = set(self.feature_names) - set(X.columns)
        if missing_cols:
            logger.warning("Missing columns: %s, filling with 0", missing_cols)

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)

        log_mu = self.mu_model.predict(X_aligned)
        log_alpha = self.alpha_model.predict(X_aligned)

        # Handle NaN/Inf
        log_mu = np.nan_to_num(
            log_mu,
            nan=np.log(self._global_mu),
            posinf=self.config.log_mu_max,
            neginf=self.config.log_mu_min,
        )
        log_alpha = np.nan_to_num(
            log_alpha,
            nan=np.log(self._global_alpha),
            posinf=self.config.log_alpha_max,
            neginf=self.config.log_alpha_min,
        )

        mu = np.exp(np.clip(log_mu, self.config.log_mu_min, self.config.log_mu_max))
        alpha = np.exp(np.clip(log_alpha, self.config.log_alpha_min, self.config.log_alpha_max))

        return mu, alpha

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    @staticmethod
    def _negbin_invcdf(
        u: np.ndarray,
        mu: float,
        alpha: float,
    ) -> np.ndarray:
        """Inverse CDF for standard NegBin.

        Maps uniform samples u ∈ (0,1) to integers >= 0.
        """
        n = 1.0 / alpha
        p = n / (n + mu)
        u_safe = np.clip(u, 1e-10, 1 - 1e-10)
        samples = nbinom.ppf(u_safe, n, p)
        return np.maximum(samples, 0).astype(int)

    def sample(
        self,
        X: pd.DataFrame,
        n_samples: int = 10_000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """Draw MC samples from the NegBin for each row.

        Args:
            X: Features (single row or batch).
            n_samples: Samples per row.
            rng: Random state for reproducibility.

        Returns:
            Integer samples, shape ``(n_rows, n_samples)``
            or ``(n_samples,)`` when a single row is passed.
        """
        rng = rng or np.random.RandomState(self.config.random_state)

        mu, alpha = self.predict_params(X)
        n_rows = len(X)

        if n_rows == 1:
            u = rng.random(n_samples)
            return self._negbin_invcdf(u, mu[0], alpha[0])

        samples = np.zeros((n_rows, n_samples), dtype=int)
        for i in range(n_rows):
            u = rng.random(n_samples)
            try:
                samples[i, :] = self._negbin_invcdf(u, mu[i], alpha[i])
            except Exception as e:
                logger.warning(
                    "Sampling failed for row %d (mu=%.3f, alpha=%.3f): %s",
                    i, mu[i], alpha[i], e,
                )
                samples[i, :] = self._negbin_invcdf(
                    u, self._global_mu, self._global_alpha,
                )
        return samples

    def sample_single(
        self,
        features: dict,
        n_samples: int = 10_000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """Sample for a single observation given a feature dict."""
        X = pd.DataFrame([features]).reindex(columns=self.feature_names, fill_value=0)
        return self.sample(X, n_samples=n_samples, rng=rng).flatten()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, directory: Path | str) -> None:
        """Save model artifacts to *directory*.

        Files produced::
            {model_name}_mu_model.joblib
            {model_name}_alpha_model.joblib
            {model_name}_negbin_meta.json
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.mu_model, directory / f"{self.model_name}_mu_model.joblib")
        joblib.dump(self.alpha_model, directory / f"{self.model_name}_alpha_model.joblib")

        metadata = {
            "model_name": self.model_name,
            "feature_names": self.feature_names,
            "global_mu": self._global_mu,
            "global_alpha": self._global_alpha,
            "config": {
                "mu_n_estimators": self.config.mu_n_estimators,
                "mu_max_depth": self.config.mu_max_depth,
                "mu_learning_rate": self.config.mu_learning_rate,
                "alpha_n_estimators": self.config.alpha_n_estimators,
                "alpha_max_depth": self.config.alpha_max_depth,
                "log_mu_min": self.config.log_mu_min,
                "log_mu_max": self.config.log_mu_max,
                "log_alpha_min": self.config.log_alpha_min,
                "log_alpha_max": self.config.log_alpha_max,
            },
        }
        with open(directory / f"{self.model_name}_negbin_meta.json", "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info("Saved NegBinModel (%s) to %s", self.model_name, directory)

    @classmethod
    def load(cls, directory: Path | str, model_name: str = "negbin") -> NegBinModel:
        """Load model from *directory*."""
        directory = Path(directory)

        with open(directory / f"{model_name}_negbin_meta.json") as f:
            metadata = json.load(f)

        cfg = metadata.get("config", {})
        config = NegBinConfig(
            mu_n_estimators=cfg.get("mu_n_estimators", 1000),
            mu_max_depth=cfg.get("mu_max_depth", 5),
            mu_learning_rate=cfg.get("mu_learning_rate", 0.03),
            alpha_n_estimators=cfg.get("alpha_n_estimators", 500),
            alpha_max_depth=cfg.get("alpha_max_depth", 3),
            log_mu_min=cfg.get("log_mu_min", -3.0),
            log_mu_max=cfg.get("log_mu_max", 3.0),
            log_alpha_min=cfg.get("log_alpha_min", -2.0),
            log_alpha_max=cfg.get("log_alpha_max", 2.0),
        )

        model = cls(config=config, model_name=model_name)
        model.feature_names = metadata["feature_names"]
        model._global_mu = metadata["global_mu"]
        model._global_alpha = metadata["global_alpha"]

        model.mu_model = joblib.load(directory / f"{model_name}_mu_model.joblib")
        model.alpha_model = joblib.load(directory / f"{model_name}_alpha_model.joblib")

        logger.info("Loaded NegBinModel (%s) from %s", model_name, directory)
        return model

    @staticmethod
    def exists(directory: Path | str, model_name: str = "negbin") -> bool:
        """Check whether a saved NegBinModel exists in *directory*."""
        directory = Path(directory)
        return (directory / f"{model_name}_negbin_meta.json").exists()
