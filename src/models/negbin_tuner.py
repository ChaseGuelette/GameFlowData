"""Optuna-Based Hyperparameter Tuning for NegBin Distributional Models.

Optimizes XGBoost hyperparameters for the 2-output distributional NegBin
model by minimizing validation NB NLL.  Unlike the quantile tuner (which
optimizes calibration gaps), this tunes the joint (r, p) model directly.

Usage:
    from src.models.negbin_tuner import NegBinHyperparameterTuner
    tuner = NegBinHyperparameterTuner(n_trials=50)
    best_config = tuner.tune(X, y)
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
import xgboost as xgb
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler
from scipy.optimize import minimize
from scipy.stats import nbinom
from sklearn.model_selection import train_test_split

from src.models.negbin_model import (
    NegBinConfig,
    _nb_eval_metric,
    _nb_objective,
    _sigmoid,
)

logger = logging.getLogger(__name__)

# Search space for NegBin distributional model
NEGBIN_SEARCH_SPACE = {
    "max_depth": (3, 8),
    "min_child_weight": (1, 20),
    "learning_rate": (0.01, 0.10),
    "n_estimators": (300, 2000),
    "subsample": (0.6, 0.95),
    "colsample_bytree": (0.5, 0.95),
    "early_stopping_rounds": (30, 100),
}


class NegBinHyperparameterTuner:
    """Optuna-based hyperparameter tuner for NegBin distributional models.

    Trains the full 2-output XGBoost model with custom NB NLL objective
    for each trial, evaluating on a temporal validation split.
    """

    def __init__(
        self,
        n_trials: int = 50,
        timeout: int | None = None,
        val_fraction: float = 0.15,
        pruning: bool = True,
        random_state: int = 42,
        search_space: dict | None = None,
        study_name: str | None = None,
    ):
        self.n_trials = n_trials
        self.timeout = timeout
        self.val_fraction = val_fraction
        self.pruning = pruning
        self.random_state = random_state
        self.search_space = search_space or NEGBIN_SEARCH_SPACE
        self.study_name = study_name or f"negbin_tuning_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.study: optuna.Study | None = None
        self.best_params: dict | None = None
        self.best_nll: float | None = None

    def _sample_params(self, trial: optuna.Trial) -> dict:
        """Sample hyperparameters from search space."""
        ss = self.search_space
        return {
            "max_depth": trial.suggest_int("max_depth", ss["max_depth"][0], ss["max_depth"][1]),
            "min_child_weight": trial.suggest_int(
                "min_child_weight", ss["min_child_weight"][0], ss["min_child_weight"][1]
            ),
            "learning_rate": trial.suggest_float(
                "learning_rate", ss["learning_rate"][0], ss["learning_rate"][1], log=True
            ),
            "n_estimators": trial.suggest_int(
                "n_estimators", ss["n_estimators"][0], ss["n_estimators"][1]
            ),
            "subsample": trial.suggest_float(
                "subsample", ss["subsample"][0], ss["subsample"][1]
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree", ss["colsample_bytree"][0], ss["colsample_bytree"][1]
            ),
            "early_stopping_rounds": trial.suggest_int(
                "early_stopping_rounds",
                ss["early_stopping_rounds"][0],
                ss["early_stopping_rounds"][1],
            ),
        }

    @staticmethod
    def _fit_global_params(y: np.ndarray) -> tuple[float, float]:
        """Fit global NegBin MLE parameters for base_margin init."""
        mu_init = max(0.1, float(y.mean()))
        var_init = float(y.var())
        alpha_init = max(0.1, (var_init - mu_init) / (mu_init ** 2 + 1e-6))

        def negbin_nll(params):
            mu, alpha = params
            if mu <= 0 or alpha <= 0.01:
                return 1e10
            n = 1.0 / alpha
            p = n / (n + mu)
            if p <= 0 or p >= 1 or n <= 0:
                return 1e10
            log_probs = nbinom.logpmf(y.astype(int), n, p)
            log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
            return -np.sum(log_probs)

        result = minimize(
            negbin_nll, x0=[mu_init, alpha_init],
            method="Nelder-Mead", options={"maxiter": 1000},
        )
        return max(0.05, result.x[0]), max(0.01, result.x[1])

    def tune(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        feature_names: list[str] | None = None,
    ) -> NegBinConfig:
        """Run hyperparameter tuning study.

        Args:
            X: Feature DataFrame (training + val combined).
            y: Integer counts >= 0.
            feature_names: Subset of columns to use (default: all).

        Returns:
            NegBinConfig with best hyperparameters.
        """
        feature_names = feature_names or list(X.columns)
        y_np = y.values.astype(float)

        logger.info("Starting NegBin hyperparameter tuning: %d trials", self.n_trials)
        logger.info("Features: %d, Samples: %s", len(feature_names), f"{len(X):,}")

        # Temporal train/val split
        X_train, X_val, y_train, y_val = train_test_split(
            X[feature_names].fillna(0), y_np,
            test_size=self.val_fraction, shuffle=False,
        )
        logger.info("Train: %s, Val: %s", f"{len(X_train):,}", f"{len(X_val):,}")

        # Fit global MLE once (shared across all trials for base_margin)
        global_mu, global_alpha = self._fit_global_params(y_np)
        r_init = 1.0 / global_alpha
        p_init = r_init / (r_init + global_mu)
        raw_r_init = float(np.log(max(r_init, 1e-4)))
        raw_p_init = float(np.log(max(p_init, 1e-6) / max(1 - p_init, 1e-6)))

        logger.info("Global MLE: mu=%.3f, alpha=%.3f", global_mu, global_alpha)

        # Pre-build DMatrices (reused across trials)
        dtrain = xgb.DMatrix(X_train, label=y_train.astype(float))
        dval = xgb.DMatrix(X_val, label=y_val.astype(float))

        base_margin_train = np.tile([raw_r_init, raw_p_init], (len(X_train), 1)).reshape(-1)
        base_margin_val = np.tile([raw_r_init, raw_p_init], (len(X_val), 1)).reshape(-1)
        dtrain.set_base_margin(base_margin_train)
        dval.set_base_margin(base_margin_val)

        # Create objective
        def objective(trial: optuna.Trial) -> float:
            hp = self._sample_params(trial)
            n_estimators = hp.pop("n_estimators")
            early_stopping = hp.pop("early_stopping_rounds")

            params = {
                **hp,
                "num_target": 2,
                "base_score": 0,
                "disable_default_eval_metric": True,
                "tree_method": "hist",
                "seed": self.random_state,
            }

            try:
                booster = xgb.train(
                    params,
                    dtrain,
                    num_boost_round=n_estimators,
                    evals=[(dval, "val")],
                    obj=_nb_objective,
                    custom_metric=_nb_eval_metric,
                    early_stopping_rounds=early_stopping,
                    verbose_eval=False,
                )

                # Compute val NLL
                val_pred = booster.predict(dval)
                if val_pred.ndim == 1:
                    val_pred = val_pred.reshape(-1, 2)

                r = np.exp(np.clip(val_pred[:, 0], -10, 10))
                p = _sigmoid(val_pred[:, 1])
                r = np.clip(r, 1e-4, 1e4)

                n_param = r
                p_param = p

                log_probs = nbinom.logpmf(y_val.astype(int), n_param, p_param)
                log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                val_nll = -float(np.mean(log_probs))

                # Report for pruning
                if self.pruning:
                    trial.report(val_nll, 0)
                    if trial.should_prune():
                        raise optuna.TrialPruned()

                return val_nll

            except Exception as e:
                logger.warning("Trial %d failed: %s", trial.number, e)
                return float("inf")

        # Create and run study
        sampler = TPESampler(seed=self.random_state)
        pruner = MedianPruner() if self.pruning else optuna.pruners.NopPruner()

        self.study = optuna.create_study(
            study_name=self.study_name,
            sampler=sampler,
            pruner=pruner,
            direction="minimize",
        )

        optuna.logging.set_verbosity(optuna.logging.INFO)
        self.study.optimize(
            objective, n_trials=self.n_trials, timeout=self.timeout, show_progress_bar=True
        )

        # Extract best
        self.best_params = self.study.best_params
        self.best_nll = self.study.best_value

        logger.info("Best trial: %d", self.study.best_trial.number)
        logger.info("Best val NLL: %.4f", self.best_nll)
        logger.info("Best params: %s", self.best_params)

        return self._params_to_config(self.best_params)

    def _params_to_config(self, params: dict) -> NegBinConfig:
        """Convert Optuna params to NegBinConfig."""
        return NegBinConfig(
            n_estimators=params.get("n_estimators", 1000),
            max_depth=params.get("max_depth", 5),
            learning_rate=params.get("learning_rate", 0.03),
            subsample=params.get("subsample", 0.8),
            colsample_bytree=params.get("colsample_bytree", 0.8),
            min_child_weight=params.get("min_child_weight", 3),
            early_stopping_rounds=params.get("early_stopping_rounds", 50),
        )

    def save_best_config(self, path: str | Path) -> None:
        """Save best hyperparameters to JSON."""
        if self.best_params is None:
            raise ValueError("No tuning result. Run tune() first.")

        output = {
            "best_params": self.best_params,
            "best_val_nll": self.best_nll,
            "n_trials": len(self.study.trials) if self.study else 0,
            "study_name": self.study_name,
            "timestamp": datetime.now().isoformat(),
            "config": asdict(self._params_to_config(self.best_params)),
        }

        path = Path(path)
        with open(path, "w") as f:
            json.dump(output, f, indent=4)
        logger.info("Saved best NegBin config to %s", path)

    @staticmethod
    def load_best_config(path: str | Path) -> NegBinConfig:
        """Load best hyperparameters from JSON and return NegBinConfig."""
        with open(path) as f:
            data = json.load(f)

        cfg = data.get("config", data.get("best_params", {}))
        return NegBinConfig(
            n_estimators=cfg.get("n_estimators", 1000),
            max_depth=cfg.get("max_depth", 5),
            learning_rate=cfg.get("learning_rate", 0.03),
            subsample=cfg.get("subsample", 0.8),
            colsample_bytree=cfg.get("colsample_bytree", 0.8),
            min_child_weight=cfg.get("min_child_weight", 3),
            early_stopping_rounds=cfg.get("early_stopping_rounds", 50),
        )
