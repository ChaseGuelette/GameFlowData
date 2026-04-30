"""MLB Model Suite — Unified container for all MLB model types.

Discovers and loads pitcher quantile, batter NegBin, and batter binary models
from a single production directory. The daily runner and inference job interact
with the suite rather than individual model objects.

Artifact directory layout (production/)::

    pitcher_k_model.joblib              # Quantile model (pitcher Ks)
    pitcher_k_feature_config.joblib
    batter_hits_xgblss_booster.json     # NegBin model (hits)
    batter_hits_negbin_meta.json
    batter_total_bases_xgblss_booster.json
    batter_total_bases_negbin_meta.json
    hr_binary_model.joblib              # Binary model (HR)
    hr_calibrator.joblib
    hr_binary_meta.json
    suite_manifest.json                 # Optional: lists available stats
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.models.binomial_model import BinomialModel
from src.models.monte_carlo import PropPrediction
from src.models.negbin_model import NegBinModel

from .mlb_binary_model import MLBBinaryModel
from .mlb_monte_carlo import (
    MLBBinomialPredictor,
    MLBCompoundBinomialPredictor,
    MLBMonteCarloPredictor,
    MLBNegBinPredictor,
    MLBPitcherKCopulaPredictor,
)
from .mlb_quantile_trainer import MLBPitcherKPipeline
from .mlb_stat_config import MLB_STATS

logger = logging.getLogger(__name__)

# Mapping from MLB_STATS stat keys to NegBin model_name prefixes.
# NegBinModel saves as {model_name}_xgblss_booster.json / {model_name}_negbin_meta.json
STAT_TO_NEGBIN_MODEL_NAME: dict[str, str] = {
    "batter_hits": "batter_hits",
    "batter_total_bases": "batter_total_bases",
    "batter_rbis": "batter_rbis",
    "batter_runs_scored": "batter_runs",
    "batter_hrr": "batter_hrr",
}

# Short stat name used by NegBinPredictor (for PropPrediction.stat field)
STAT_TO_NEGBIN_SHORT: dict[str, str] = {
    "batter_hits": "hits",
    "batter_total_bases": "total_bases",
    "batter_rbis": "rbis",
    "batter_runs_scored": "runs",
    "batter_hrr": "hrr",
}


class MLBBinaryPredictor:
    """Wraps MLBBinaryModel to produce PropPrediction objects.

    For HR, the line is always 0.5, so P(over 0.5) = P(HR >= 1) = model output.
    Generates Bernoulli MC samples for compatibility with the edge calculator's
    empirical CDF: ``(samples > line).mean()``.
    """

    def __init__(
        self,
        model: MLBBinaryModel,
        stat: str = "batter_home_runs",
        n_samples: int = 10_000,
        random_state: int = 42,
    ):
        self.model = model
        self.stat = stat
        self.n_samples = n_samples
        self.rng = np.random.default_rng(random_state)

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Generate prediction for a single batter's HR prop."""
        X = self._prepare_features(features)
        prob = float(self.model.predict_proba(X)[0])

        # Bernoulli samples: 0 or 1
        samples = self.rng.binomial(1, prob, self.n_samples).astype(float)

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat=self.stat,
            mean=prob,
            median=float(np.median(samples)),
            q10=0.0,
            q25=0.0,
            q50=float(np.median(samples)),
            q75=float(np.percentile(samples, 75)),
            q90=float(np.percentile(samples, 90)),
            samples=samples,
        )

    def predict_batch(
        self,
        player_games: list[tuple[int, int, dict]],
    ) -> list[PropPrediction]:
        """Batch predict for multiple batter-games."""
        if not player_games:
            return []

        # Batch the XGBoost call
        all_features = [f for _, _, f in player_games]
        feature_names = self.model.feature_names

        rows = []
        for features in all_features:
            row = {f: features.get(f, 0) for f in feature_names}
            rows.append(row)

        X_batch = pd.DataFrame(rows)
        X_batch = X_batch.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)

        probas = self.model.predict_proba(X_batch)

        results = []
        for i, (player_id, game_id, _) in enumerate(player_games):
            prob = float(probas[i])
            samples = self.rng.binomial(1, prob, self.n_samples).astype(float)

            results.append(PropPrediction(
                player_id=player_id,
                game_id=str(game_id),
                stat=self.stat,
                mean=prob,
                median=float(np.median(samples)),
                q10=0.0,
                q25=0.0,
                q50=float(np.median(samples)),
                q75=float(np.percentile(samples, 75)),
                q90=float(np.percentile(samples, 90)),
                samples=samples,
            ))

        return results

    def _prepare_features(self, features: dict) -> pd.DataFrame:
        feature_names = self.model.feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df


class MLBModelSuite:
    """Unified container for all MLB model artifacts.

    Discovers and loads whatever models exist in a directory, wrapping each
    in the appropriate predictor class. Prediction calls are routed by stat key.
    """

    def __init__(self):
        self.predictors: dict[str, Any] = {}  # stat_key → predictor instance
        self._directory: Path | None = None

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        n_samples: int = 10_000,
        random_state: int = 42,
    ) -> MLBModelSuite:
        """Discover and load all available models from a production directory.

        Loads whatever models exist — gracefully skips missing ones.
        """
        directory = Path(directory)
        suite = cls()
        suite._directory = directory

        # 1a. Pitcher K copula (IP + K-rate decomposition) — preferred if available
        ip_model_dir = directory / "ip_model"
        krate_model_dir = directory / "krate_model"
        copula_params_path = directory / "pitcher_k_copula_params.json"

        if ip_model_dir.exists() and krate_model_dir.exists() and copula_params_path.exists():
            try:
                ip_pipeline = MLBPitcherKPipeline.load(str(ip_model_dir))
                krate_pipeline = MLBPitcherKPipeline.load(str(krate_model_dir))

                with open(copula_params_path) as f:
                    copula_params = json.load(f)
                copula_rho = copula_params.get("pitcher_strikeouts", 0.3)

                predictor = MLBPitcherKCopulaPredictor(
                    ip_pipeline=ip_pipeline,
                    krate_pipeline=krate_pipeline,
                    copula_rho=copula_rho,
                    n_samples=n_samples,
                    random_state=random_state,
                )
                suite.predictors["pitcher_strikeouts"] = predictor
                logger.info("Loaded pitcher_strikeouts (copula: IP × K-rate) model, ρ=%.3f", copula_rho)
            except Exception as e:
                logger.error("Failed to load pitcher K copula model: %s", e)

        # 1b. Pitcher K single model (fallback if copula not available)
        if "pitcher_strikeouts" not in suite.predictors:
            pitcher_k_path = directory / "pitcher_k_model.joblib"
            if pitcher_k_path.exists():
                try:
                    pipeline = MLBPitcherKPipeline.load(str(directory))
                    predictor = MLBMonteCarloPredictor(
                        pipeline, n_samples=n_samples, random_state=random_state,
                    )
                    suite.predictors["pitcher_strikeouts"] = predictor
                    logger.info("Loaded pitcher_strikeouts (single quantile) model")
                except Exception as e:
                    logger.error("Failed to load pitcher K model: %s", e)

        # 2. Batter count models (binomial for hits, negbin for others)
        for stat_key, model_name in STAT_TO_NEGBIN_MODEL_NAME.items():
            short_stat = STAT_TO_NEGBIN_SHORT[stat_key]

            # 2a. Compound Binomial (AB NegBin + hit Binomial) — preferred for hits
            if stat_key == "batter_hits" and NegBinModel.exists(directory, model_name="batter_ab"):
                if BinomialModel.exists(directory, model_name=model_name):
                    try:
                        predictor = MLBCompoundBinomialPredictor.from_directory(
                            directory,
                            stat=short_stat,
                            n_samples=n_samples,
                            random_state=random_state,
                        )
                        predictor.stat = stat_key
                        suite.predictors[stat_key] = predictor
                        logger.info("Loaded %s (compound binomial: AB NegBin + hit Binomial)", stat_key)
                        continue  # Skip to next stat
                    except Exception as e:
                        logger.error("Failed to load %s compound model, falling back: %s", stat_key, e)

            # 2b. Check for binomial model first (hits)
            if BinomialModel.exists(directory, model_name=model_name):
                try:
                    predictor = MLBBinomialPredictor.from_directory(
                        directory,
                        stat=short_stat,
                        n_samples=n_samples,
                        random_state=random_state,
                    )
                    predictor.stat = stat_key
                    suite.predictors[stat_key] = predictor
                    logger.info("Loaded %s (binomial) model", stat_key)
                except Exception as e:
                    logger.error("Failed to load %s binomial model: %s", stat_key, e)
            elif NegBinModel.exists(directory, model_name=model_name):
                try:
                    predictor = MLBNegBinPredictor.from_directory(
                        directory,
                        stat=short_stat,
                        n_samples=n_samples,
                        random_state=random_state,
                    )
                    predictor.stat = stat_key
                    suite.predictors[stat_key] = predictor
                    logger.info("Loaded %s (negbin) model", stat_key)
                except Exception as e:
                    logger.error("Failed to load %s negbin model: %s", stat_key, e)

        # 3. Binary HR model — removed (Session 19/20: HR model non-viable)

        logger.info(
            "MLBModelSuite loaded %d models: %s",
            len(suite.predictors),
            list(suite.predictors.keys()),
        )

        return suite

    @property
    def available_stats(self) -> list[str]:
        """Return list of stat keys with loaded models."""
        return list(self.predictors.keys())

    @property
    def pitcher_stats(self) -> list[str]:
        return [s for s in self.predictors if s.startswith("pitcher_")]

    @property
    def batter_stats(self) -> list[str]:
        return [s for s in self.predictors if s.startswith("batter_")]

    def has_stat(self, stat: str) -> bool:
        return stat in self.predictors

    def predict(
        self,
        stat: str,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Route prediction to the appropriate predictor for the given stat."""
        if stat not in self.predictors:
            raise KeyError(f"No model loaded for stat '{stat}'. Available: {self.available_stats}")
        return self.predictors[stat].predict(player_id, game_id, features)

    def predict_batch(
        self,
        stat: str,
        player_games: list[tuple[int, int, dict]],
    ) -> list[PropPrediction]:
        """Route batch prediction to the appropriate predictor."""
        if stat not in self.predictors:
            raise KeyError(f"No model loaded for stat '{stat}'. Available: {self.available_stats}")
        return self.predictors[stat].predict_batch(player_games)

    def get_model_type(self, stat: str) -> str:
        """Return the model type string for a given stat."""
        return MLB_STATS.get(stat, {}).get("model_type", "unknown")

    def get_predictor(self, stat: str):
        """Return the raw predictor for a stat (for advanced use)."""
        return self.predictors.get(stat)

    def write_manifest(self) -> None:
        """Write suite_manifest.json to the artifact directory."""
        if self._directory is None:
            return
        manifest = {
            "stats": {
                stat: {
                    "model_type": self.get_model_type(stat),
                    "predictor_class": type(pred).__name__,
                }
                for stat, pred in self.predictors.items()
            }
        }
        manifest_path = self._directory / "suite_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        logger.info("Wrote suite manifest: %s", manifest_path)
