"""Optuna-Based Hyperparameter Tuning for Binomial Models.

Optimizes XGBoost hyperparameters for the single-output binomial model
by minimizing validation binomial NLL.  At-bats (n) are passed via
DMatrix weights; the booster learns hit probability p via logit link.

Usage:
    from src.models.binomial_tuner import BinomialHyperparameterTuner
    tuner = BinomialHyperparameterTuner(n_trials=50)
    best_config = tuner.tune(X, y, at_bats)
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
from scipy.stats import binom
from sklearn.model_selection import train_test_split

from src.models.binomial_model import (
    BinomialConfig,
    _binomial_nll_eval,
    _binomial_obj,
    _sigmoid,
)

logger = logging.getLogger(__name__)

BINOMIAL_SEARCH_SPACE = {
    "max_depth": (3, 8),
    "min_child_weight": (1, 20),
    "learning_rate": (0.01, 0.10),
    "n_estimators": (300, 2000),
    "subsample": (0.6, 0.95),
    "colsample_bytree": (0.5, 0.95),
    "early_stopping_rounds": (30, 100),
}


class BinomialHyperparameterTuner:
    """Optuna-based hyperparameter tuner for binomial models.

    Trains the single-output XGBoost model with custom binomial NLL
    objective for each trial, evaluating on a temporal validation split.
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
        self.search_space = search_space or BINOMIAL_SEARCH_SPACE
        self.study_name = study_name or f"binomial_tuning_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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

    def tune(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        at_bats: np.ndarray,
        feature_names: list[str] | None = None,
    ) -> BinomialConfig:
        """Run hyperparameter tuning study.

        Args:
            X: Feature DataFrame (training + val combined).
            y: Integer hits >= 0.
            at_bats: At-bats per row.
            feature_names: Subset of columns to use (default: all).

        Returns:
            BinomialConfig with best hyperparameters.
        """
        feature_names = feature_names or list(X.columns)
        y_np = y.values.astype(float) if hasattr(y, "values") else np.asarray(y, dtype=float)
        at_bats = np.asarray(at_bats, dtype=float)

        logger.info("Starting binomial hyperparameter tuning: %d trials", self.n_trials)
        logger.info("Features: %d, Samples: %s", len(feature_names), f"{len(X):,}")

        # Temporal train/val split
        X_train, X_val, y_train, y_val, ab_train, ab_val = train_test_split(
            X[feature_names].fillna(0), y_np, at_bats,
            test_size=self.val_fraction, shuffle=False,
        )
        logger.info("Train: %s, Val: %s", f"{len(X_train):,}", f"{len(X_val):,}")

        # Global MLE for base_margin init
        total_hits = float(y_np.sum())
        total_ab = float(at_bats.sum())
        global_p = total_hits / max(total_ab, 1.0)
        raw_p_init = float(np.log(max(global_p, 1e-6) / max(1 - global_p, 1e-6)))

        logger.info("Global MLE: p=%.4f (BA), raw_p_init=%.4f", global_p, raw_p_init)

        # Pre-build DMatrices (reused across trials)
        dtrain = xgb.DMatrix(X_train, label=y_train, weight=ab_train)
        dval = xgb.DMatrix(X_val, label=y_val, weight=ab_val)
        dtrain.set_base_margin(np.full(len(X_train), raw_p_init))
        dval.set_base_margin(np.full(len(X_val), raw_p_init))

        def objective(trial: optuna.Trial) -> float:
            hp = self._sample_params(trial)
            n_estimators = hp.pop("n_estimators")
            early_stopping = hp.pop("early_stopping_rounds")

            params = {
                **hp,
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
                    obj=_binomial_obj,
                    custom_metric=_binomial_nll_eval,
                    early_stopping_rounds=early_stopping,
                    verbose_eval=False,
                )

                # Compute val binomial NLL
                val_pred = booster.predict(dval)
                p = np.clip(_sigmoid(val_pred), 1e-7, 1 - 1e-7)

                log_probs = binom.logpmf(y_val.astype(int), ab_val.astype(int), p)
                log_probs = np.where(np.isfinite(log_probs), log_probs, -100)
                val_nll = -float(np.mean(log_probs))

                if self.pruning:
                    trial.report(val_nll, 0)
                    if trial.should_prune():
                        raise optuna.TrialPruned()

                return val_nll

            except optuna.TrialPruned:
                raise
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

        self.best_params = self.study.best_params
        self.best_nll = self.study.best_value

        logger.info("Best trial: %d", self.study.best_trial.number)
        logger.info("Best val NLL: %.4f", self.best_nll)
        logger.info("Best params: %s", self.best_params)

        return self._params_to_config(self.best_params)

    def _params_to_config(self, params: dict) -> BinomialConfig:
        """Convert Optuna params to BinomialConfig."""
        return BinomialConfig(
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
        logger.info("Saved best binomial config to %s", path)

    @staticmethod
    def load_best_config(path: str | Path) -> BinomialConfig:
        """Load best hyperparameters from JSON and return BinomialConfig."""
        with open(path) as f:
            data = json.load(f)

        cfg = data.get("config", data.get("best_params", {}))
        return BinomialConfig(
            n_estimators=cfg.get("n_estimators", 1000),
            max_depth=cfg.get("max_depth", 5),
            learning_rate=cfg.get("learning_rate", 0.03),
            subsample=cfg.get("subsample", 0.8),
            colsample_bytree=cfg.get("colsample_bytree", 0.8),
            min_child_weight=cfg.get("min_child_weight", 3),
            early_stopping_rounds=cfg.get("early_stopping_rounds", 50),
        )
