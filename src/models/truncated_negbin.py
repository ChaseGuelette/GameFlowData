"""
Truncated Negative Binomial Model for THREES Count Prediction.

This module implements a zero-truncated negative binomial model for predicting
the count of made three-pointers, conditioned on making at least one.

The model uses:
- mu/alpha parameterization (mean and overdispersion)
- Two XGBoost regressors predicting log(mu) and log(alpha)
- MLE-based global alpha estimation for initialization
- Inverse CDF sampling for integer count generation

Part of C4 implementation to fix the 25.6% Q10 calibration gap in the
hurdle quantile model.
"""

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
class TruncatedNegBinConfig:
    """Configuration for the Truncated Negative Binomial model."""

    # XGBoost parameters for mu model
    mu_n_estimators: int = 1000
    mu_max_depth: int = 5
    mu_learning_rate: float = 0.03
    mu_subsample: float = 0.8
    mu_colsample_bytree: float = 0.8
    mu_min_child_weight: int = 3
    mu_early_stopping_rounds: int = 50

    # XGBoost parameters for alpha model (simpler - less variable)
    alpha_n_estimators: int = 500
    alpha_max_depth: int = 3
    alpha_learning_rate: float = 0.05
    alpha_subsample: float = 0.8
    alpha_colsample_bytree: float = 0.8

    # Validation split
    val_size: float = 0.15

    # Clamping ranges for predictions
    log_mu_min: float = -2.0  # mu >= 0.14
    log_mu_max: float = 3.0   # mu <= 20
    log_alpha_min: float = -2.0  # alpha >= 0.14
    log_alpha_max: float = 2.0   # alpha <= 7.4

    # Random state
    random_state: int = 42


class TruncatedNegBinModel:
    """
    Predicts parameters of a zero-truncated negative binomial distribution.

    The NegBin is parameterized as:
    - mu: mean of the untruncated distribution (mu > 0)
    - alpha: dispersion parameter (alpha > 0, higher = more variance)

    Variance = mu + alpha * mu^2 (overdispersion when alpha > 0)

    Training uses two XGBoost regressors:
    - log(mu) regressor: ensures mu > 0
    - log(alpha) regressor: ensures alpha > 0

    Sampling uses inverse CDF of truncated distribution.
    """

    def __init__(self, config: TruncatedNegBinConfig | None = None):
        self.config = config or TruncatedNegBinConfig()
        self.mu_model: xgb.XGBRegressor | None = None
        self.alpha_model: xgb.XGBRegressor | None = None
        self.feature_names: list[str] = []

        # Store MLE-fitted global alpha as fallback/regularization
        self._global_alpha: float = 1.0
        self._global_mu: float = 2.0

    def _fit_global_params(self, y: np.ndarray) -> tuple[float, float]:
        """
        Fit truncated NegBin parameters via MLE to get robust global estimates.

        Args:
            y: Positive integer counts (>= 1)

        Returns:
            Tuple of (mu, alpha) MLE estimates
        """

        def truncated_negbin_nll(params):
            """Negative log-likelihood for zero-truncated NegBin."""
            mu, alpha = params

            if mu <= 0 or alpha <= 0.01:
                return 1e10

            # Convert to scipy params
            n = 1.0 / alpha
            p = 1.0 / (1.0 + alpha * mu)

            if p <= 0 or p >= 1 or n <= 0:
                return 1e10

            # P(X=0) under full NegBin
            p_zero = nbinom.pmf(0, n, p)
            if p_zero >= 0.999:
                return 1e10

            # Log-likelihood of truncated distribution
            log_probs = nbinom.logpmf(y.astype(int), n, p) - np.log(1 - p_zero)
            log_probs = np.where(np.isfinite(log_probs), log_probs, -100)

            return -np.sum(log_probs)

        # Initial guess from moments
        mu_init = float(y.mean())
        var_init = float(y.var())

        # For NegBin: variance = mu + alpha * mu^2
        # Solving: alpha = (var - mu) / mu^2
        alpha_init = max(0.1, (var_init - mu_init) / (mu_init ** 2 + 1e-6))

        result = minimize(
            truncated_negbin_nll,
            x0=[mu_init, alpha_init],
            method='Nelder-Mead',
            options={'maxiter': 1000}
        )

        mu_mle = max(0.1, result.x[0])
        alpha_mle = max(0.01, result.x[1])

        logger.info(
            f"Global MLE: mu={mu_mle:.3f}, alpha={alpha_mle:.3f}, "
            f"success={result.success}"
        )

        return mu_mle, alpha_mle

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: np.ndarray | None = None,
    ) -> dict:
        """
        Fit the truncated NegBin model.

        Args:
            X: Features (positive samples only, where y >= 1)
            y: Integer counts (fg3m >= 1)
            sample_weight: Optional sample weights

        Returns:
            Dict with training info (global_alpha, global_mu, etc.)
        """
        y_np = y.values.astype(float)

        if y_np.min() < 1:
            raise ValueError(
                f"TruncatedNegBinModel requires y >= 1, got min={y_np.min()}"
            )

        self.feature_names = list(X.columns)
        n_samples = len(X)

        logger.info(f"Fitting TruncatedNegBinModel on {n_samples:,} samples")
        logger.info(f"Target stats: mean={y_np.mean():.3f}, var={y_np.var():.3f}")

        # Stage 1: Fit global parameters via MLE
        self._global_mu, self._global_alpha = self._fit_global_params(y_np)

        # Stage 2: Prepare targets for XGBoost
        # Key insight: observed values y are from the TRUNCATED distribution.
        # E[X | X > 0] = mu / (1 - P(X=0)), so observed values are inflated.
        # We need to train mu to predict the UNTRUNCATED mean, which is lower.
        # Adjustment: mu = observed * (1 - P(X=0))
        #
        # Compute P(X=0) using global MLE params
        n_global = 1.0 / self._global_alpha
        p_global = n_global / (n_global + self._global_mu)
        p_zero_global = nbinom.pmf(0, n_global, p_global)
        truncation_factor = 1.0 - p_zero_global

        logger.info(
            f"Truncation adjustment: P(X=0)={p_zero_global:.3f}, "
            f"factor={truncation_factor:.3f}"
        )

        # Target for mu model: adjusted to untruncated mean
        # mu = observed * (1 - p_zero) since E[X|X>0] = mu / (1 - p_zero)
        # IMPORTANT: Apply truncation factor FIRST, then add small epsilon for log stability
        # The old code had: log((y + 0.5) * factor) = log(y*factor + 0.5*factor)
        # This added 0.37 bias to mu! Correct: log(y*factor + small_epsilon)
        mu_target = y_np * truncation_factor
        logger.info(
            f"Adjusted mu target: mean={mu_target.mean():.3f} "
            f"(should be close to MLE mu={self._global_mu:.3f})"
        )
        log_mu_target = np.log(mu_target + 0.01)  # 0.01 epsilon for numerical stability

        # Target for alpha model: residual-based overdispersion estimate
        # Use squared residuals relative to mean squared as a ratio
        mu_baseline = y_np.mean()
        residuals_sq = (y_np - mu_baseline) ** 2
        overdispersion_ratio = residuals_sq / (mu_baseline ** 2 + 1e-6)

        # Clamp and log-transform
        log_alpha_target = np.log(np.clip(overdispersion_ratio, 0.1, 10.0))

        # Train/val split (temporal ordering preserved)
        X_train, X_val, y_mu_train, y_mu_val, y_alpha_train, y_alpha_val = train_test_split(
            X,
            log_mu_target,
            log_alpha_target,
            test_size=self.config.val_size,
            shuffle=False,
        )

        if sample_weight is not None:
            sw_train, sw_val = train_test_split(
                sample_weight,
                test_size=self.config.val_size,
                shuffle=False,
            )
        else:
            sw_train = None

        # Stage 3: Train mu model
        logger.info("Training mu model...")
        self.mu_model = xgb.XGBRegressor(
            objective='reg:squarederror',
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
            f"  Train mu: pred_mean={mu_train_pred.mean():.3f}, "
            f"val mu: pred_mean={mu_val_pred.mean():.3f}"
        )

        # Stage 4: Train alpha model
        logger.info("Training alpha model...")
        self.alpha_model = xgb.XGBRegressor(
            objective='reg:squarederror',
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
        logger.info(f"  Val alpha: pred_mean={alpha_val_pred.mean():.3f}")

        return {
            'global_mu': self._global_mu,
            'global_alpha': self._global_alpha,
            'n_samples': n_samples,
            'n_features': len(self.feature_names),
        }

    def predict_params(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """
        Predict (mu, alpha) for each row.

        Args:
            X: Feature DataFrame

        Returns:
            mu: Mean of untruncated NegBin, shape (n,)
            alpha: Dispersion parameter, shape (n,)
        """
        if self.mu_model is None or self.alpha_model is None:
            raise ValueError("Model not fitted. Call fit() first.")

        # Align features
        missing_cols = set(self.feature_names) - set(X.columns)
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}, filling with 0")

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)

        # Predict in log space
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

        # Clamp to valid ranges and exponentiate
        mu = np.exp(np.clip(log_mu, self.config.log_mu_min, self.config.log_mu_max))
        alpha = np.exp(np.clip(log_alpha, self.config.log_alpha_min, self.config.log_alpha_max))

        return mu, alpha

    def _truncated_negbin_invcdf(
        self,
        u: np.ndarray,
        mu: float,
        alpha: float,
    ) -> np.ndarray:
        """
        Inverse CDF for zero-truncated negative binomial.

        Maps uniform samples u in (0, 1) to positive integers >= 1.

        Args:
            u: Uniform samples in (0, 1)
            mu: Mean of untruncated NegBin
            alpha: Dispersion parameter

        Returns:
            Integer samples >= 1
        """
        # Convert to scipy params
        # NegBin variance = mu + alpha * mu^2
        # scipy uses: n = 1/alpha, p = n/(n+mu)
        n = 1.0 / alpha
        p = n / (n + mu)

        # P(X=0) under full NegBin
        p_zero = nbinom.pmf(0, n, p)

        # Map uniform to truncated distribution
        # For truncated dist: P(X <= k | X > 0) = (P(X <= k) - P(X=0)) / (1 - P(X=0))
        # Inverting: P(X <= k) = u * (1 - p_zero) + p_zero
        adjusted_u = u * (1 - p_zero) + p_zero

        # Clamp to avoid edge issues
        adjusted_u = np.clip(adjusted_u, 1e-10, 1 - 1e-10)

        # Use scipy's inverse CDF (ppf)
        samples = nbinom.ppf(adjusted_u, n, p)

        # Ensure minimum of 1 (truncation) and convert to int
        return np.maximum(samples, 1).astype(int)

    def sample(
        self,
        X: pd.DataFrame,
        n_samples: int = 10000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """
        Draw samples from the truncated NegBin for each row.

        Args:
            X: Features (single row or batch)
            n_samples: Number of MC samples per row
            rng: Random state for reproducibility

        Returns:
            Integer samples, shape (n_rows, n_samples) or (n_samples,) if single row
        """
        rng = rng or np.random.RandomState(self.config.random_state)

        mu, alpha = self.predict_params(X)
        n_rows = len(X)

        if n_rows == 1:
            # Single row - return flat array
            u = rng.random(n_samples)
            return self._truncated_negbin_invcdf(u, mu[0], alpha[0])

        # Batch - return 2D array
        samples = np.zeros((n_rows, n_samples), dtype=int)
        for i in range(n_rows):
            u = rng.random(n_samples)
            try:
                samples[i, :] = self._truncated_negbin_invcdf(u, mu[i], alpha[i])
            except Exception as e:
                logger.warning(
                    f"Sampling failed for row {i} (mu={mu[i]:.3f}, alpha={alpha[i]:.3f}): {e}"
                )
                # Fallback: sample from global distribution
                samples[i, :] = self._truncated_negbin_invcdf(
                    u, self._global_mu, self._global_alpha
                )

        return samples

    def sample_single(
        self,
        features: dict,
        n_samples: int = 10000,
        rng: np.random.RandomState | None = None,
    ) -> np.ndarray:
        """
        Sample for a single player given feature dict.

        Args:
            features: Dict of feature name -> value
            n_samples: Number of MC samples
            rng: Random state

        Returns:
            Integer samples, shape (n_samples,)
        """
        X = pd.DataFrame([features])[self.feature_names].fillna(0)
        return self.sample(X, n_samples=n_samples, rng=rng).flatten()

    def save(self, directory: Path) -> None:
        """Save model to disk."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        # Save XGBoost models
        joblib.dump(self.mu_model, directory / "threes_mu_model.joblib")
        joblib.dump(self.alpha_model, directory / "threes_alpha_model.joblib")

        # Save config and metadata
        metadata = {
            'feature_names': self.feature_names,
            'global_mu': self._global_mu,
            'global_alpha': self._global_alpha,
            'config': {
                'mu_n_estimators': self.config.mu_n_estimators,
                'mu_max_depth': self.config.mu_max_depth,
                'mu_learning_rate': self.config.mu_learning_rate,
                'alpha_n_estimators': self.config.alpha_n_estimators,
                'alpha_max_depth': self.config.alpha_max_depth,
                'log_mu_min': self.config.log_mu_min,
                'log_mu_max': self.config.log_mu_max,
                'log_alpha_min': self.config.log_alpha_min,
                'log_alpha_max': self.config.log_alpha_max,
            }
        }

        with open(directory / "threes_count_model_meta.json", 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved TruncatedNegBinModel to {directory}")

    @classmethod
    def load(cls, directory: Path) -> 'TruncatedNegBinModel':
        """Load model from disk."""
        directory = Path(directory)

        # Load metadata
        with open(directory / "threes_count_model_meta.json") as f:
            metadata = json.load(f)

        # Create config
        config_dict = metadata.get('config', {})
        config = TruncatedNegBinConfig(
            mu_n_estimators=config_dict.get('mu_n_estimators', 1000),
            mu_max_depth=config_dict.get('mu_max_depth', 5),
            mu_learning_rate=config_dict.get('mu_learning_rate', 0.03),
            alpha_n_estimators=config_dict.get('alpha_n_estimators', 500),
            alpha_max_depth=config_dict.get('alpha_max_depth', 3),
            log_mu_min=config_dict.get('log_mu_min', -2.0),
            log_mu_max=config_dict.get('log_mu_max', 3.0),
            log_alpha_min=config_dict.get('log_alpha_min', -2.0),
            log_alpha_max=config_dict.get('log_alpha_max', 2.0),
        )

        model = cls(config=config)
        model.feature_names = metadata['feature_names']
        model._global_mu = metadata['global_mu']
        model._global_alpha = metadata['global_alpha']

        # Load XGBoost models
        model.mu_model = joblib.load(directory / "threes_mu_model.joblib")
        model.alpha_model = joblib.load(directory / "threes_alpha_model.joblib")

        logger.info(f"Loaded TruncatedNegBinModel from {directory}")

        return model

    @staticmethod
    def exists(directory: Path) -> bool:
        """Check if a count model exists in the directory."""
        directory = Path(directory)
        return (directory / "threes_count_model_meta.json").exists()
