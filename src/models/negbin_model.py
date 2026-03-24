"""Standard Negative Binomial Model for integer count prediction.

Predicts the parameters (mu, alpha) of a NegBin distribution, then
samples integer counts via inverse CDF.  Unlike TruncatedNegBinModel
(used for NBA 3PM where y >= 1), this version handles zeros natively
— making it appropriate for MLB batter stats (hits, TB, RBI, runs)
where ~40 % of games produce zero.

Architecture (v2 — distributional regression):
    - Single XGBoost booster with custom NB NLL objective
    - Jointly learns both distributional parameters (r, p)
    - Response functions: exp(·) for r > 0, sigmoid(·) for p ∈ (0,1)
    - MLE-based global parameter estimation for initialisation
    - Inverse CDF sampling via scipy.stats.nbinom.ppf

Backward-compatible with v1 (two-separate-XGBRegressor) artifacts.
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
from scipy.special import digamma, gammaln, polygamma
from scipy.stats import nbinom
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


# ======================================================================
# NB NLL custom objective (no PyTorch needed)
# ======================================================================

def _sigmoid(x: np.ndarray) -> np.ndarray:
    """Numerically stable sigmoid."""
    x = np.clip(x, -500, 500)
    return np.clip(1.0 / (1.0 + np.exp(-x)), 1e-6, 1 - 1e-6)


def _nb_nll(y: np.ndarray, r: np.ndarray, p: np.ndarray) -> np.ndarray:
    """Per-sample NB negative log-likelihood.

    NegBin(y | r, p): P(Y=y) = C(y+r-1,y) p^r (1-p)^y
    NLL = -(lgamma(r+y) - lgamma(r) - lgamma(y+1) + r log(p) + y log(1-p))
    """
    return -(
        gammaln(r + y) - gammaln(r) - gammaln(y + 1)
        + r * np.log(np.clip(p, 1e-10, None))
        + y * np.log(np.clip(1 - p, 1e-10, None))
    )


def _nb_objective(predt: np.ndarray, dtrain: xgb.DMatrix):
    """XGBoost custom objective for NB distributional regression.

    predt: shape (n_samples, 2) — raw predictions [raw_r, raw_p].
    Returns (grad, hess) each shape (n_samples, 2).
    """
    y = dtrain.get_label()
    n = len(y)

    # Reshape — XGBoost passes flat when num_target=2
    if predt.ndim == 1:
        predt = predt.reshape(n, 2)

    raw_r = predt[:, 0]
    raw_p = predt[:, 1]

    # Apply response functions
    r = np.exp(np.clip(raw_r, -10, 10))  # r > 0
    p = _sigmoid(raw_p)                   # 0 < p < 1

    # Clamp for numerical stability
    r = np.clip(r, 1e-4, 1e4)

    # ------------------------------------------------------------------
    # Gradients of NLL w.r.t. (r, p)
    # ------------------------------------------------------------------
    #   dNLL/dr = -(digamma(r+y) - digamma(r) + log(p))
    #   dNLL/dp = -(r/p - y/(1-p))
    grad_r_param = -(digamma(r + y) - digamma(r) + np.log(np.clip(p, 1e-10, None)))
    grad_p_param = -(r / np.clip(p, 1e-10, None) - y / np.clip(1 - p, 1e-10, None))

    # ------------------------------------------------------------------
    # Hessians of NLL w.r.t. (r, p)
    # ------------------------------------------------------------------
    #   d2NLL/dr2 = -(trigamma(r+y) - trigamma(r))  [always >= 0]
    #   d2NLL/dp2 = r/p^2 + y/(1-p)^2               [always > 0]
    hess_r_param = -(polygamma(1, r + y) - polygamma(1, r))
    hess_p_param = r / np.clip(p ** 2, 1e-10, None) + y / np.clip((1 - p) ** 2, 1e-10, None)

    # ------------------------------------------------------------------
    # Chain rule for response functions: raw → param
    # ------------------------------------------------------------------
    # For exp: dr/d(raw_r) = r,  d2r/d(raw_r)2 = r
    #   grad_raw_r = grad_r_param * r
    #   hess_raw_r = hess_r_param * r^2 + grad_r_param * r
    #
    # For sigmoid: dp/d(raw_p) = p(1-p),  d2p/d(raw_p)2 = p(1-p)(1-2p)
    #   grad_raw_p = grad_p_param * p(1-p)
    #   hess_raw_p = hess_p_param * (p(1-p))^2 + grad_p_param * p(1-p)(1-2p)

    # r chain rule (exp response)
    grad_raw_r = grad_r_param * r
    hess_raw_r = hess_r_param * (r ** 2) + grad_r_param * r

    # p chain rule (sigmoid response)
    sp = p * (1 - p)
    grad_raw_p = grad_p_param * sp
    hess_raw_p = hess_p_param * (sp ** 2) + grad_p_param * sp * (1 - 2 * p)

    # XGBoost requires positive hessians
    hess_raw_r = np.maximum(hess_raw_r, 1e-6)
    hess_raw_p = np.maximum(hess_raw_p, 1e-6)

    # Handle NaN/Inf
    grad_raw_r = np.nan_to_num(grad_raw_r, nan=0.0, posinf=10.0, neginf=-10.0)
    grad_raw_p = np.nan_to_num(grad_raw_p, nan=0.0, posinf=10.0, neginf=-10.0)
    hess_raw_r = np.nan_to_num(hess_raw_r, nan=1e-6, posinf=100.0, neginf=1e-6)
    hess_raw_p = np.nan_to_num(hess_raw_p, nan=1e-6, posinf=100.0, neginf=1e-6)

    # Clip extreme gradients for stability
    grad_raw_r = np.clip(grad_raw_r, -50, 50)
    grad_raw_p = np.clip(grad_raw_p, -50, 50)

    grad = np.column_stack([grad_raw_r, grad_raw_p])
    hess = np.column_stack([hess_raw_r, hess_raw_p])

    return grad, hess


def _nb_eval_metric(predt: np.ndarray, dtrain: xgb.DMatrix):
    """Custom eval metric: mean NB NLL."""
    y = dtrain.get_label()
    n = len(y)

    if predt.ndim == 1:
        predt = predt.reshape(n, 2)

    r = np.exp(np.clip(predt[:, 0], -10, 10))
    p = _sigmoid(predt[:, 1])
    r = np.clip(r, 1e-4, 1e4)

    nll = _nb_nll(y, r, p)
    nll = nll[np.isfinite(nll)]

    return "nb_nll", float(np.mean(nll))


# ======================================================================
# Config
# ======================================================================

@dataclass
class NegBinConfig:
    """Configuration for the standard Negative Binomial model."""

    # XGBoost distributional regression parameters (single multi-output model)
    n_estimators: int = 1000
    max_depth: int = 5
    learning_rate: float = 0.03
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 3
    early_stopping_rounds: int = 50

    # Legacy fields — kept for backward-compat with old configs/metadata
    mu_n_estimators: int = 1000
    mu_max_depth: int = 5
    mu_learning_rate: float = 0.03
    mu_subsample: float = 0.8
    mu_colsample_bytree: float = 0.8
    mu_min_child_weight: int = 3
    mu_early_stopping_rounds: int = 50
    alpha_n_estimators: int = 500
    alpha_max_depth: int = 3
    alpha_learning_rate: float = 0.05
    alpha_subsample: float = 0.8
    alpha_colsample_bytree: float = 0.8

    # Validation split
    val_size: float = 0.15

    # Clamping ranges for predictions
    log_mu_min: float = -3.0   # mu >= ~0.05
    log_mu_max: float = 3.0    # mu <= ~20
    log_alpha_min: float = -2.0  # alpha >= ~0.14
    log_alpha_max: float = 2.0   # alpha <= ~7.4

    # Random state
    random_state: int = 42


# ======================================================================
# Model
# ======================================================================

class NegBinModel:
    """Predicts parameters of a standard (non-truncated) negative binomial.

    Parameterisation:
        mu   — mean of the distribution (mu > 0)
        alpha — overdispersion (alpha > 0; variance = mu + alpha * mu^2)

    v2: A single XGBoost booster with custom NB NLL objective jointly
    learns both distributional parameters (total_count r, probs p) via
    multi-output training.  Response functions (exp, sigmoid) map raw
    predictions to valid parameter ranges.

    Inference requires only xgboost + scipy (no PyTorch).

    Samples are drawn via ``scipy.stats.nbinom.ppf`` (inverse CDF).
    """

    def __init__(
        self,
        config: NegBinConfig | None = None,
        model_name: str = "negbin",
    ):
        self.config = config or NegBinConfig()
        self.model_name = model_name
        self.feature_names: list[str] = []

        # MLE-fitted global parameters (used as fallback / regularisation)
        self._global_alpha: float = 1.0
        self._global_mu: float = 1.0

        # v2: native XGBoost booster (distributional regression)
        self._native_booster: xgb.Booster | None = None

        # v1 (legacy): separate mu/alpha XGBRegressors
        self.mu_model: xgb.XGBRegressor | None = None
        self.alpha_model: xgb.XGBRegressor | None = None

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
        """Fit the NegBin model via distributional regression.

        Uses a single XGBoost booster with a custom NB NLL objective
        to jointly learn both distributional parameters (r, p).

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

        # Stage 1: Global MLE for initialisation + fallback
        self._global_mu, self._global_alpha = self._fit_global_params(y_np)

        # Convert global (mu, alpha) → (r, p) for base_margin
        r_init = 1.0 / self._global_alpha
        p_init = r_init / (r_init + self._global_mu)

        # Raw start values (inverse of response functions)
        raw_r_init = float(np.log(max(r_init, 1e-4)))
        raw_p_init = float(np.log(max(p_init, 1e-6) / max(1 - p_init, 1e-6)))  # logit

        logger.info(
            "Init: r=%.3f (raw=%.3f), p=%.3f (raw=%.3f)",
            r_init, raw_r_init, p_init, raw_p_init,
        )

        # Train/val split (preserve temporal order)
        X_train, X_val, y_train, y_val = train_test_split(
            X, y_np,
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

        # Build DMatrices with base_margin
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dval = xgb.DMatrix(X_val, label=y_val)

        base_margin_train = np.tile([raw_r_init, raw_p_init], (len(X_train), 1)).reshape(-1)
        base_margin_val = np.tile([raw_r_init, raw_p_init], (len(X_val), 1)).reshape(-1)
        dtrain.set_base_margin(base_margin_train)
        dval.set_base_margin(base_margin_val)

        if sw_train is not None:
            dtrain.set_weight(sw_train)

        # XGBoost params for multi-output distributional regression
        params = {
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "subsample": self.config.subsample,
            "colsample_bytree": self.config.colsample_bytree,
            "min_child_weight": self.config.min_child_weight,
            "num_target": 2,
            "base_score": 0,
            "disable_default_eval_metric": True,
            "tree_method": "hist",
            "seed": self.config.random_state,
        }

        logger.info("Training distributional NegBin model (num_target=2)...")
        self._native_booster = xgb.train(
            params,
            dtrain,
            num_boost_round=self.config.n_estimators,
            evals=[(dtrain, "train"), (dval, "val")],
            obj=_nb_objective,
            custom_metric=_nb_eval_metric,
            early_stopping_rounds=self.config.early_stopping_rounds,
            verbose_eval=50,
        )

        # Log validation metrics
        val_pred = self._native_booster.predict(dval)
        if val_pred.ndim == 1:
            val_pred = val_pred.reshape(-1, 2)
        val_r = np.exp(np.clip(val_pred[:, 0], -10, 10))
        val_p = _sigmoid(val_pred[:, 1])
        val_mu = val_r * (1 - val_p) / np.clip(val_p, 1e-10, None)
        val_alpha = 1.0 / np.clip(val_r, 1e-10, None)

        logger.info(
            "Val predictions: mu_mean=%.3f, alpha_mean=%.3f, actual_mean=%.3f",
            val_mu.mean(), val_alpha.mean(), y_val.mean(),
        )

        # Store base_margin init values for save/load
        self._raw_r_init = raw_r_init
        self._raw_p_init = raw_p_init

        # Clear legacy models
        self.mu_model = None
        self.alpha_model = None

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
        # Align features
        missing_cols = set(self.feature_names) - set(X.columns)
        if missing_cols:
            logger.warning("Missing columns: %s, filling with 0", missing_cols)

        X_aligned = X.reindex(columns=self.feature_names, fill_value=0)

        if self._native_booster is not None:
            # v2 path: native booster with (r, p) output
            dmat = xgb.DMatrix(X_aligned)

            # Set base margin for prediction
            raw_r_init = getattr(self, "_raw_r_init", 0.0)
            raw_p_init = getattr(self, "_raw_p_init", 0.0)
            base_margin = np.tile([raw_r_init, raw_p_init], (len(X_aligned), 1)).reshape(-1)
            dmat.set_base_margin(base_margin)

            raw_preds = self._native_booster.predict(dmat)
            if raw_preds.ndim == 1:
                raw_preds = raw_preds.reshape(-1, 2)

            # Apply response functions
            r = np.exp(np.clip(raw_preds[:, 0], -10, 10))
            p = _sigmoid(raw_preds[:, 1])

            # Convert (r, p) → (mu, alpha)
            mu = r * (1 - p) / np.clip(p, 1e-10, None)
            alpha = 1.0 / np.clip(r, 1e-10, None)

        elif self.mu_model is not None and self.alpha_model is not None:
            # v1 path: legacy separate regressors
            log_mu = self.mu_model.predict(X_aligned)
            log_alpha = self.alpha_model.predict(X_aligned)

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

        else:
            raise ValueError("Model not fitted. Call fit() first.")

        # Clamp to config ranges
        mu = np.clip(mu, np.exp(self.config.log_mu_min), np.exp(self.config.log_mu_max))
        alpha = np.clip(alpha, np.exp(self.config.log_alpha_min), np.exp(self.config.log_alpha_max))

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

        v2 format::
            {model_name}_xgblss_booster.json   — native XGBoost booster
            {model_name}_negbin_meta.json       — metadata + config
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        if self._native_booster is not None:
            # v2: save native booster
            booster_path = directory / f"{self.model_name}_xgblss_booster.json"
            self._native_booster.save_model(str(booster_path))
        elif self.mu_model is not None and self.alpha_model is not None:
            # v1 fallback: save legacy joblib models
            joblib.dump(self.mu_model, directory / f"{self.model_name}_mu_model.joblib")
            joblib.dump(self.alpha_model, directory / f"{self.model_name}_alpha_model.joblib")

        metadata = {
            "model_name": self.model_name,
            "model_version": 2 if self._native_booster is not None else 1,
            "feature_names": self.feature_names,
            "global_mu": self._global_mu,
            "global_alpha": self._global_alpha,
            "distribution": "NegativeBinomial",
            "n_dist_params": 2,
            "param_names": ["total_count", "probs"],
            "response_fns": ["exp", "sigmoid"],
            "raw_r_init": getattr(self, "_raw_r_init", 0.0),
            "raw_p_init": getattr(self, "_raw_p_init", 0.0),
            "config": {
                "n_estimators": self.config.n_estimators,
                "max_depth": self.config.max_depth,
                "learning_rate": self.config.learning_rate,
                "subsample": self.config.subsample,
                "colsample_bytree": self.config.colsample_bytree,
                "min_child_weight": self.config.min_child_weight,
                "early_stopping_rounds": self.config.early_stopping_rounds,
                "log_mu_min": self.config.log_mu_min,
                "log_mu_max": self.config.log_mu_max,
                "log_alpha_min": self.config.log_alpha_min,
                "log_alpha_max": self.config.log_alpha_max,
                # Legacy fields for backward compat
                "mu_n_estimators": self.config.mu_n_estimators,
                "mu_max_depth": self.config.mu_max_depth,
                "mu_learning_rate": self.config.mu_learning_rate,
                "alpha_n_estimators": self.config.alpha_n_estimators,
                "alpha_max_depth": self.config.alpha_max_depth,
            },
        }
        with open(directory / f"{self.model_name}_negbin_meta.json", "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info("Saved NegBinModel v%d (%s) to %s",
                     metadata["model_version"], self.model_name, directory)

    @classmethod
    def load(cls, directory: Path | str, model_name: str = "negbin") -> NegBinModel:
        """Load model from *directory*.

        Supports both v2 (native booster) and v1 (joblib) formats.
        """
        directory = Path(directory)

        with open(directory / f"{model_name}_negbin_meta.json") as f:
            metadata = json.load(f)

        cfg = metadata.get("config", {})
        config = NegBinConfig(
            n_estimators=cfg.get("n_estimators", 1000),
            max_depth=cfg.get("max_depth", 5),
            learning_rate=cfg.get("learning_rate", 0.03),
            subsample=cfg.get("subsample", 0.8),
            colsample_bytree=cfg.get("colsample_bytree", 0.8),
            min_child_weight=cfg.get("min_child_weight", 3),
            early_stopping_rounds=cfg.get("early_stopping_rounds", 50),
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

        # Try v2 format first (native booster)
        booster_path = directory / f"{model_name}_xgblss_booster.json"
        if booster_path.exists():
            model._native_booster = xgb.Booster()
            model._native_booster.load_model(str(booster_path))
            model._raw_r_init = metadata.get("raw_r_init", 0.0)
            model._raw_p_init = metadata.get("raw_p_init", 0.0)
            logger.info("Loaded NegBinModel v2 (%s) from %s", model_name, directory)
        else:
            # v1 fallback: legacy joblib models
            model.mu_model = joblib.load(directory / f"{model_name}_mu_model.joblib")
            model.alpha_model = joblib.load(directory / f"{model_name}_alpha_model.joblib")
            logger.info("Loaded NegBinModel v1 (%s) from %s", model_name, directory)

        return model

    @staticmethod
    def exists(directory: Path | str, model_name: str = "negbin") -> bool:
        """Check whether a saved NegBinModel exists in *directory*."""
        directory = Path(directory)
        return (directory / f"{model_name}_negbin_meta.json").exists()
