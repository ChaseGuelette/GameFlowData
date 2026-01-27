
"""
Optuna-Based Hyperparameter Tuning for Quantile Regression Models

Optimizes for calibration performance (minimizes maximum calibration gap)
rather than raw quantile loss. Supports per-quantile or shared hyperparameters.

Usage:
    # Standalone
    python -m src.models.hyperparameter_tuner --n-trials 50 --timeout 3600

    # As part of pipeline
    from src.models.hyperparameter_tuner import QuantileHyperparameterTuner
    tuner = QuantileHyperparameterTuner(n_trials=100)
    best_config = tuner.tune(X, y, quantiles)
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
import xgboost as xgb
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler
from sklearn.model_selection import train_test_split

from .quantile_trainer import QuantileModelConfig

logger = logging.getLogger(__name__)


# Default search space bounds
SEARCH_SPACE = {
    # Tree structure
    "max_depth": (3, 8),
    "min_child_weight": (1, 20),
    # Learning
    "learning_rate": (0.01, 0.15),
    "n_estimators": (200, 1500),
    # Regularization
    "subsample": (0.6, 0.95),
    "colsample_bytree": (0.6, 0.95),
    "reg_alpha": (1e-8, 10.0),
    "reg_lambda": (1e-8, 10.0),
    # Early stopping
    "early_stopping_rounds": (30, 100),
}


@dataclass
class TuningResult:
    """Results from a hyperparameter tuning study."""

    best_params: dict
    best_calibration_gap: float
    n_trials: int
    study_name: str
    timestamp: str
    quantiles: tuple[float, ...]
    per_quantile: bool = False
    per_quantile_params: dict | None = None  # Only set if per_quantile=True


class QuantileHyperparameterTuner:
    """
    Optuna-based hyperparameter tuner for quantile regression models.

    Optimizes for calibration (minimizes worst calibration gap across quantiles)
    using temporal train/validation splits.
    """

    def __init__(
        self,
        n_trials: int = 100,
        timeout: int | None = None,
        per_quantile: bool = False,
        pruning: bool = True,
        val_fraction: float = 0.15,
        n_jobs: int = -1,
        study_name: str | None = None,
        storage: str | None = None,
        random_state: int = 42,
        search_space: dict | None = None,
    ):
        """Initialize the tuner."""

        self.n_trials = n_trials
        self.timeout = timeout
        self.per_quantile = per_quantile
        self.pruning = pruning
        self.val_fraction = val_fraction
        self.n_jobs = n_jobs
        self.study_name = study_name or f"quantile_tuning_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.storage = storage
        self.random_state = random_state
        self.search_space = search_space or SEARCH_SPACE

        # Will be populated after tuning
        self.study: optuna.Study | None = None
        self.studies_per_quantile: dict[float, optuna.Study] = {}
        self.result: TuningResult | None = None

    def _sample_params(self, trial: optuna.Trial) -> dict:
        """Sample hyperparameters from search space."""
        params = {
            "max_depth": trial.suggest_int(
                "max_depth", self.search_space["max_depth"][0], self.search_space["max_depth"][1]
            ),
            "min_child_weight": trial.suggest_int(
                "min_child_weight",
                self.search_space["min_child_weight"][0],
                self.search_space["min_child_weight"][1],
            ),
            "learning_rate": trial.suggest_float(
                "learning_rate",
                self.search_space["learning_rate"][0],
                self.search_space["learning_rate"][1],
                log=True,
            ),
            "n_estimators": trial.suggest_int(
                "n_estimators",
                self.search_space["n_estimators"][0],
                self.search_space["n_estimators"][1],
            ),
            "subsample": trial.suggest_float(
                "subsample", self.search_space["subsample"][0], self.search_space["subsample"][1]
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                self.search_space["colsample_bytree"][0],
                self.search_space["colsample_bytree"][1],
            ),
            "reg_alpha": trial.suggest_float(
                "reg_alpha", self.search_space["reg_alpha"][0], self.search_space["reg_alpha"][1], log=True
            ),
            "reg_lambda": trial.suggest_float(
                "reg_lambda", self.search_space["reg_lambda"][0], self.search_space["reg_lambda"][1], log=True
            ),
            "early_stopping_rounds": trial.suggest_int(
                "early_stopping_rounds",
                self.search_space["early_stopping_rounds"][0],
                self.search_space["early_stopping_rounds"][1],
            ),
        }
        return params

    def _calculate_calibration_gap(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        quantile: float,
    ) -> float:
        """Calculate calibration gap for a single quantile."""
        coverage = (y_true <= y_pred).mean()
        return abs(coverage - quantile)

    def _create_objective(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        quantiles: tuple[float, ...],
        feature_names: list[str],
    ) -> Callable[[optuna.Trial], float]:
        """Create objective function for shared hyperparameters across quantiles."""

        def objective(trial: optuna.Trial) -> float:
            params = self._sample_params(trial)
            early_stopping_rounds = params.pop("early_stopping_rounds")

            gaps = []

            for i, q in enumerate(quantiles):
                model = xgb.XGBRegressor(
                    objective="reg:quantileerror",
                    quantile_alpha=q,
                    **params,
                    random_state=self.random_state,
                    n_jobs=self.n_jobs,
                )

                model.fit(
                    X_train[feature_names],
                    y_train,
                    eval_set=[(X_val[feature_names], y_val)],
                    verbose=False,
                )

                val_preds = model.predict(X_val[feature_names])
                gap = self._calculate_calibration_gap(y_val.values, val_preds, q)
                gaps.append(gap)

                # Report intermediate value for pruning
                if self.pruning:
                    trial.report(max(gaps), i)
                    if trial.should_prune():
                        raise optuna.TrialPruned()

            # Return worst gap (Optuna minimizes)
            return max(gaps)

        return objective

    def _create_single_quantile_objective(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        quantile: float,
        feature_names: list[str],
    ) -> Callable[[optuna.Trial], float]:
        """Create objective function for a single quantile."""

        def objective(trial: optuna.Trial) -> float:
            params = self._sample_params(trial)
            early_stopping_rounds = params.pop("early_stopping_rounds")

            model = xgb.XGBRegressor(
                objective="reg:quantileerror",
                quantile_alpha=quantile,
                **params,
                random_state=self.random_state,
                n_jobs=self.n_jobs,
            )

            model.fit(
                X_train[feature_names],
                y_train,
                eval_set=[(X_val[feature_names], y_val)],
                verbose=False,
            )

            val_preds = model.predict(X_val[feature_names])
            gap = self._calculate_calibration_gap(y_val.values, val_preds, quantile)

            return gap

        return objective

    def tune(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90),
        feature_names: list[str] | None = None,
    ) -> QuantileModelConfig:
        """Run hyperparameter tuning study."""

        feature_names = feature_names or list(X.columns)

        logger.info(f"Starting hyperparameter tuning with {self.n_trials} trials")
        logger.info(f"Quantiles: {quantiles}")
        logger.info(f"Features: {len(feature_names)}")
        logger.info(f"Samples: {len(X):,}")

        # Temporal train/val split (no shuffle to preserve time order)
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=self.val_fraction, shuffle=False, random_state=self.random_state
        )

        logger.info(f"Train: {len(X_train):,}, Val: {len(X_val):,}")

        # Create study
        sampler = TPESampler(seed=self.random_state)
        pruner = MedianPruner() if self.pruning else optuna.pruners.NopPruner()

        self.study = optuna.create_study(
            study_name=self.study_name,
            storage=self.storage,
            sampler=sampler,
            pruner=pruner,
            direction="minimize",
            load_if_exists=True,
        )

        # Create objective
        objective = self._create_objective(X_train, y_train, X_val, y_val, quantiles, feature_names)

        # Run optimization
        optuna.logging.set_verbosity(optuna.logging.INFO)
        self.study.optimize(objective, n_trials=self.n_trials, timeout=self.timeout, show_progress_bar=True)

        # Extract best parameters
        best_params = self.study.best_params
        best_gap = self.study.best_value

        logger.info(f"Best trial: {self.study.best_trial.number}")
        logger.info(f"Best calibration gap: {best_gap:.4f}")
        logger.info(f"Best parameters: {best_params}")

        # Create result
        self.result = TuningResult(
            best_params=best_params,
            best_calibration_gap=best_gap,
            n_trials=len(self.study.trials),
            study_name=self.study_name,
            timestamp=datetime.now().isoformat(),
            quantiles=quantiles,
            per_quantile=False,
        )

        # Convert to QuantileModelConfig
        return self._params_to_config(best_params, quantiles)

    def tune_per_quantile(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        quantiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90),
        feature_names: list[str] | None = None,
    ) -> dict[float, QuantileModelConfig]:
        """Run separate tuning study for each quantile."""

        feature_names = feature_names or list(X.columns)

        logger.info("Starting per-quantile hyperparameter tuning")
        logger.info(f"Quantiles: {quantiles}")
        logger.info(f"Trials per quantile: {self.n_trials}")

        # Temporal train/val split
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=self.val_fraction, shuffle=False, random_state=self.random_state
        )

        configs = {}
        per_quantile_params = {}

        for q in quantiles:
            logger.info(f"\n--- Tuning quantile {q:.2f} ---")

            study_name = f"{self.study_name}_q{int(q * 100):02d}"
            sampler = TPESampler(seed=self.random_state)
            pruner = MedianPruner() if self.pruning else optuna.pruners.NopPruner()

            study = optuna.create_study(
                study_name=study_name,
                storage=self.storage,
                sampler=sampler,
                pruner=pruner,
                direction="minimize",
                load_if_exists=True,
            )

            objective = self._create_single_quantile_objective(X_train, y_train, X_val, y_val, q, feature_names)

            optuna.logging.set_verbosity(optuna.logging.INFO)
            study.optimize(objective, n_trials=self.n_trials, timeout=self.timeout, show_progress_bar=True)

            self.studies_per_quantile[q] = study
            per_quantile_params[q] = study.best_params

            logger.info(f"Q{q:.2f} best gap: {study.best_value:.4f}")

            configs[q] = self._params_to_config(study.best_params, (q,))

        # Store result
        self.result = TuningResult(
            best_params={},
            best_calibration_gap=max(s.best_value for s in self.studies_per_quantile.values()),
            n_trials=len(quantiles) * self.n_trials,
            study_name=self.study_name,
            timestamp=datetime.now().isoformat(),
            quantiles=quantiles,
            per_quantile=True,
            per_quantile_params=per_quantile_params,
        )

        return configs

    def _params_to_config(self, params: dict, quantiles: tuple[float, ...]) -> QuantileModelConfig:
        """Convert Optuna params dict to QuantileModelConfig."""
        return QuantileModelConfig(
            quantiles=quantiles,
            n_estimators=params.get("n_estimators", 1000),
            max_depth=params.get("max_depth", 5),
            learning_rate=params.get("learning_rate", 0.03),
            subsample=params.get("subsample", 0.8),
            colsample_bytree=params.get("colsample_bytree", 0.8),
            min_child_weight=params.get("min_child_weight", 3),
            early_stopping_rounds=params.get("early_stopping_rounds", 50),
        )

    def get_trials_dataframe(self) -> pd.DataFrame:
        """Get DataFrame of all trials for analysis."""
        if self.study is None:
            raise ValueError("No study available. Run tune() first.")
        return self.study.trials_dataframe()

    def save_study(self, path: str):
        """Save the Optuna study to a pickle file."""
        import joblib

        if self.study is None and not self.studies_per_quantile:
            raise ValueError("No study available. Run tune() first.")

        save_dict = {
            "study": self.study,
            "studies_per_quantile": self.studies_per_quantile,
            "result": self.result,
        }
        joblib.dump(save_dict, path)
        logger.info(f"Saved study to {path}")

    def save_best_config(self, path: str):
        """Save best parameters to JSON file."""
        if self.result is None:
            raise ValueError("No tuning result available. Run tune() first.")

        output = {
            "best_params": self.result.best_params,
            "best_calibration_gap": self.result.best_calibration_gap,
            "n_trials": self.result.n_trials,
            "study_name": self.result.study_name,
            "timestamp": self.result.timestamp,
            "quantiles": list(self.result.quantiles),
            "per_quantile": self.result.per_quantile,
        }

        if self.result.per_quantile and self.result.per_quantile_params:
            # Convert quantile keys to strings for JSON
            output["per_quantile_params"] = {str(k): v for k, v in self.result.per_quantile_params.items()}

        with open(path, "w") as f:
            json.dump(output, f, indent=4)

        logger.info(f"Saved best config to {path}")

    @classmethod
    def load_best_config(cls, path: str) -> QuantileModelConfig | dict[float, QuantileModelConfig]:
        """Load best parameters from JSON and return as QuantileModelConfig."""
        with open(path) as f:
            data = json.load(f)

        quantiles = tuple(data["quantiles"])

        if data.get("per_quantile", False):
            # Return dict of configs per quantile
            configs = {}
            for q_str, params in data["per_quantile_params"].items():
                q = float(q_str)
                configs[q] = QuantileModelConfig(
                    quantiles=(q,),
                    n_estimators=params.get("n_estimators", 1000),
                    max_depth=params.get("max_depth", 5),
                    learning_rate=params.get("learning_rate", 0.03),
                    subsample=params.get("subsample", 0.8),
                    colsample_bytree=params.get("colsample_bytree", 0.8),
                    min_child_weight=params.get("min_child_weight", 3),
                    early_stopping_rounds=params.get("early_stopping_rounds", 50),
                )
            return configs
        else:
            # Return single shared config
            params = data["best_params"]
            return QuantileModelConfig(
                quantiles=quantiles,
                n_estimators=params.get("n_estimators", 1000),
                max_depth=params.get("max_depth", 5),
                learning_rate=params.get("learning_rate", 0.03),
                subsample=params.get("subsample", 0.8),
                colsample_bytree=params.get("colsample_bytree", 0.8),
                min_child_weight=params.get("min_child_weight", 3),
                early_stopping_rounds=params.get("early_stopping_rounds", 50),
            )


def run_standalone_tuning(
    n_trials: int = 50,
    timeout: int | None = None,
    per_quantile: bool = False,
    output_dir: str = "src/models/artifacts",
):
    """Run hyperparameter tuning as a standalone script.
    Loads data from database and saves results.
    """
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

    from src.db.client import get_engine
    from src.models.feature_store import MINUTES_FEATURES, FeatureStore

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting standalone hyperparameter tuning...")

    # Load data
    engine = get_engine()
    feature_store = FeatureStore(engine)

    seasons = ["22023", "22024", "22025"]
    logger.info(f"Loading data for seasons: {seasons}")

    df = feature_store.get_training_dataset(seasons)
    df["game_date"] = pd.to_datetime(df["game_date"])

    # Filter to valid minutes data
    valid_mask = df["actual_minutes"] > 0
    df = df[valid_mask].copy()

    # Get available features
    available_features = [f for f in MINUTES_FEATURES if f in df.columns]
    logger.info(f"Using {len(available_features)} features")

    X = df[available_features].fillna(0)
    y = df["actual_minutes"]

    # Create tuner
    tuner = QuantileHyperparameterTuner(
        n_trials=n_trials,
        timeout=timeout,
        per_quantile=per_quantile,
        pruning=True,
    )

    # Run tuning
    if per_quantile:
        configs = tuner.tune_per_quantile(X, y)
        logger.info(f"Completed per-quantile tuning for {len(configs)} quantiles")
    else:
        config = tuner.tune(X, y)
        logger.info("Completed shared tuning")

    # Save results
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    config_path = output_path / f"best_hyperparams_{timestamp}.json"
    study_path = output_path / f"tuning_study_{timestamp}.pkl"
    trials_path = output_path / f"tuning_trials_{timestamp}.csv"

    tuner.save_best_config(str(config_path))
    tuner.save_study(str(study_path))

    if tuner.study:
        trials_df = tuner.get_trials_dataframe()
        trials_df.to_csv(trials_path, index=False)
        logger.info(f"Saved trials to {trials_path}")

    logger.info(f"Tuning complete. Results saved to {output_path}")

    return tuner


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Hyperparameter tuning for quantile models")
    parser.add_argument("--n-trials", type=int, default=50, help="Number of Optuna trials")
    parser.add_argument("--timeout", type=int, default=None, help="Max tuning time in seconds")
    parser.add_argument("--per-quantile", action="store_true", help="Tune separately for each quantile")
    parser.add_argument("--output-dir", type=str, default="src/models/artifacts", help="Output directory")

    args = parser.parse_args()

    run_standalone_tuning(
        n_trials=args.n_trials,
        timeout=args.timeout,
        per_quantile=args.per_quantile,
        output_dir=args.output_dir,
    )
