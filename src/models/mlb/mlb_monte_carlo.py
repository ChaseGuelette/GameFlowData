"""MLB Monte Carlo predictors.

Contains predictors:
- MLBMonteCarloPredictor:      Quantile-interpolation sampler for pitcher Ks
- MLBNegBinPredictor:          NegBin-distribution sampler for batter count stats
- MLBBinomialPredictor:        Binomial sampler for batter hits (hits in at-bats)
- MLBCompoundBinomialPredictor: AB NegBin + hit Binomial compound sampler

All produce PropPrediction objects compatible with the backtest harness.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.models.binomial_model import BinomialModel
from src.models.monte_carlo import PropPrediction
from src.models.negbin_model import NegBinModel

from .mlb_quantile_trainer import MLBPitcherKPipeline

logger = logging.getLogger(__name__)


@dataclass
class MLBTailConfig:
    """Configuration for tail extrapolation."""

    lower_tail_multiplier: float = 1.0
    upper_tail_multiplier: float = 1.2


class MLBMonteCarloPredictor:
    """Monte Carlo sampler for pitcher strikeout predictions.

    Generates probability distributions by:
    1. Getting quantile predictions (Q10-Q90) from the pitcher K model
    2. Interpolating + extrapolating to build an inverse CDF
    3. Drawing n_samples via inverse transform sampling
    4. Flooring at 0, rounding to integers
    """

    # Values below this threshold are snapped to 0
    ZERO_SNAP_THRESHOLD = 0.3

    def __init__(
        self,
        pipeline: MLBPitcherKPipeline,
        n_samples: int = 10_000,
        random_state: int = 42,
        tail_config: MLBTailConfig | None = None,
    ):
        self.pipeline = pipeline
        self.n_samples = n_samples
        self.rng = np.random.default_rng(random_state)
        self.tail_config = tail_config or MLBTailConfig()
        self.quantile_probs = np.array(
            [q for q in self.pipeline.config.quantiles]
        )

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Generate Monte Carlo prediction for a single pitcher's strikeouts.

        Args:
            player_id: MLB player ID.
            game_id: MLB game ID.
            features: Dict of feature values (PITCHER_K_FEATURES keys).

        Returns:
            PropPrediction with samples, quantiles, and betting methods.
        """
        # Get quantile predictions
        X = self._prepare_features(features)
        quantiles_df = self.pipeline.predict(X)
        quantile_values = quantiles_df.iloc[0].values.astype(np.float64)

        # Sample via inverse transform
        samples = self._inverse_transform_sample(
            self.quantile_probs, quantile_values
        )

        # Floor at 0, round to integers (strikeouts are whole numbers)
        samples = np.maximum(samples, 0)
        samples = np.round(samples).astype(int)

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat="pitcher_strikeouts",
            mean=float(samples.mean()),
            median=float(np.median(samples)),
            q10=float(quantile_values[0]),
            q25=float(quantile_values[1]),
            q50=float(quantile_values[2]),
            q75=float(quantile_values[3]),
            q90=float(quantile_values[4]),
            samples=samples,
        )

    def predict_batch(
        self,
        player_games: list[tuple[int, int, dict]],
    ) -> list[PropPrediction]:
        """Batch predict for multiple pitcher-games.

        Args:
            player_games: List of (player_id, game_id, features) tuples.

        Returns:
            List of PropPrediction objects.
        """
        if not player_games:
            return []

        # Batch the XGBoost predictions for efficiency
        all_features = [features for _, _, features in player_games]
        feature_names = self.pipeline.model.all_feature_names

        rows = []
        for features in all_features:
            row = {f: features.get(f, 0) for f in feature_names}
            rows.append(row)

        X_batch = pd.DataFrame(rows)
        X_batch = X_batch.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)

        quantiles_df = self.pipeline.predict(X_batch)

        # Per-player sampling loop
        results = []
        for i, (player_id, game_id, _) in enumerate(player_games):
            quantile_values = quantiles_df.iloc[i].values.astype(np.float64)

            samples = self._inverse_transform_sample(
                self.quantile_probs, quantile_values
            )
            samples = np.maximum(samples, 0)
            samples = np.round(samples).astype(int)

            results.append(
                PropPrediction(
                    player_id=player_id,
                    game_id=str(game_id),
                    stat="pitcher_strikeouts",
                    mean=float(samples.mean()),
                    median=float(np.median(samples)),
                    q10=float(quantile_values[0]),
                    q25=float(quantile_values[1]),
                    q50=float(quantile_values[2]),
                    q75=float(quantile_values[3]),
                    q90=float(quantile_values[4]),
                    samples=samples,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Internal sampling
    # ------------------------------------------------------------------

    def _inverse_transform_sample(
        self, quantile_probs: np.ndarray, quantile_values: np.ndarray
    ) -> np.ndarray:
        """Sample from distribution defined by quantiles via inverse CDF.

        Uses linear interpolation between quantile points and exponential
        decay extrapolation for tails.
        """
        extended_probs, extended_values = self._build_extended_quantile_fn(
            quantile_probs, quantile_values
        )

        # Draw uniform samples and map through inverse CDF
        u = self.rng.uniform(0.001, 0.999, self.n_samples)
        samples = np.interp(u, extended_probs, extended_values)

        return samples

    def _build_extended_quantile_fn(
        self, quantile_probs: np.ndarray, quantile_values: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """Build extended probability and value arrays for inverse CDF.

        Extrapolates tails to p=0.001 and p=0.999 using linear extrapolation
        with configurable tail multipliers.
        """
        lower_mult = self.tail_config.lower_tail_multiplier
        upper_mult = self.tail_config.upper_tail_multiplier

        extended_probs = np.concatenate([[0.001], quantile_probs, [0.999]])

        # Compute slopes for tail extrapolation
        lower_slope = (quantile_values[1] - quantile_values[0]) / (
            quantile_probs[1] - quantile_probs[0]
        )
        upper_slope = (quantile_values[-1] - quantile_values[-2]) / (
            quantile_probs[-1] - quantile_probs[-2]
        )

        lower_value = quantile_values[0] - (
            lower_slope * (quantile_probs[0] - 0.001) * lower_mult
        )
        upper_value = quantile_values[-1] + (
            upper_slope * (0.999 - quantile_probs[-1]) * upper_mult
        )

        extended_values = np.concatenate(
            [
                [max(0, lower_value)],
                quantile_values,
                [upper_value],
            ]
        )

        # Snap near-zero values to exactly 0
        extended_values = np.where(
            extended_values < self.ZERO_SNAP_THRESHOLD,
            0.0,
            extended_values,
        )

        return extended_probs, extended_values

    def _prepare_features(self, features: dict) -> pd.DataFrame:
        """Prepare feature dict as DataFrame for model input."""
        feature_names = self.pipeline.model.all_feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df


class MLBPitcherKCopulaPredictor:
    """Copula-based pitcher K predictor: K = IP × K_rate.

    Uses Gaussian copula to generate correlated (IP, K-rate) samples,
    preserving the relationship between innings pitched and strikeout rate.
    This mirrors the NBA minutes × rate pattern from MonteCarloPredictor.
    """

    ZERO_SNAP_THRESHOLD = 0.3

    def __init__(
        self,
        ip_pipeline: MLBPitcherKPipeline,
        krate_pipeline: MLBPitcherKPipeline,
        copula_rho: float = 0.3,
        n_samples: int = 10_000,
        random_state: int = 42,
        tail_config: MLBTailConfig | None = None,
    ):
        self.ip_pipeline = ip_pipeline
        self.krate_pipeline = krate_pipeline
        self.copula_rho = copula_rho
        self.n_samples = n_samples
        self.rng = np.random.default_rng(random_state)
        self.tail_config = tail_config or MLBTailConfig()

        self.rho_p = 2.0 * np.sin(np.pi * copula_rho / 6.0)
        self.rho_p = np.clip(self.rho_p, -0.999, 0.999)

        self.ip_quantile_probs = np.array(ip_pipeline.config.quantiles)
        self.krate_quantile_probs = np.array(krate_pipeline.config.quantiles)

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        from scipy.stats import norm as sp_norm

        X_ip = self._prepare_features(features, self.ip_pipeline)
        ip_quantiles_df = self.ip_pipeline.predict(X_ip)
        ip_qvals = ip_quantiles_df.iloc[0].values.astype(np.float64)

        X_krate = self._prepare_features(features, self.krate_pipeline)
        krate_quantiles_df = self.krate_pipeline.predict(X_krate)
        krate_qvals = krate_quantiles_df.iloc[0].values.astype(np.float64)

        ip_probs, ip_vals = self._build_extended_quantile_fn(
            self.ip_quantile_probs, ip_qvals
        )
        krate_probs, krate_vals = self._build_extended_quantile_fn(
            self.krate_quantile_probs, krate_qvals
        )

        z_ip = self.rng.standard_normal(self.n_samples)
        z_indep = self.rng.standard_normal(self.n_samples)
        z_krate = self.rho_p * z_ip + np.sqrt(1 - self.rho_p ** 2) * z_indep

        u_ip = sp_norm.cdf(z_ip)
        u_krate = sp_norm.cdf(z_krate)

        ip_samples = np.interp(u_ip, ip_probs, ip_vals)
        krate_samples = np.interp(u_krate, krate_probs, krate_vals)

        ip_samples = np.maximum(ip_samples, 0)
        krate_samples = np.maximum(krate_samples, 0)
        k_samples = np.round(ip_samples * krate_samples).astype(int)
        k_samples = np.maximum(k_samples, 0)

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat="pitcher_strikeouts",
            mean=float(k_samples.mean()),
            median=float(np.median(k_samples)),
            q10=float(np.percentile(k_samples, 10)),
            q25=float(np.percentile(k_samples, 25)),
            q50=float(np.percentile(k_samples, 50)),
            q75=float(np.percentile(k_samples, 75)),
            q90=float(np.percentile(k_samples, 90)),
            samples=k_samples,
        )

    def predict_batch(
        self,
        player_games: list[tuple[int, int, dict]],
    ) -> list[PropPrediction]:
        if not player_games:
            return []
        results = []
        for player_id, game_id, features in player_games:
            results.append(self.predict(player_id, game_id, features))
        return results

    def _build_extended_quantile_fn(
        self, quantile_probs: np.ndarray, quantile_values: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        lower_mult = self.tail_config.lower_tail_multiplier
        upper_mult = self.tail_config.upper_tail_multiplier

        extended_probs = np.concatenate([[0.001], quantile_probs, [0.999]])

        lower_slope = (quantile_values[1] - quantile_values[0]) / (
            quantile_probs[1] - quantile_probs[0]
        )
        upper_slope = (quantile_values[-1] - quantile_values[-2]) / (
            quantile_probs[-1] - quantile_probs[-2]
        )

        lower_value = quantile_values[0] - (
            lower_slope * (quantile_probs[0] - 0.001) * lower_mult
        )
        upper_value = quantile_values[-1] + (
            upper_slope * (0.999 - quantile_probs[-1]) * upper_mult
        )

        extended_values = np.concatenate(
            [
                [max(0, lower_value)],
                quantile_values,
                [upper_value],
            ]
        )

        extended_values = np.where(
            extended_values < self.ZERO_SNAP_THRESHOLD,
            0.0,
            extended_values,
        )

        return extended_probs, extended_values

    def _prepare_features(self, features: dict, pipeline: MLBPitcherKPipeline) -> pd.DataFrame:
        feature_names = pipeline.model.all_feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df


# ======================================================================
# NegBin predictor for batter count stats
# ======================================================================


class MLBNegBinPredictor:
    """Monte Carlo predictor backed by a NegBinModel.

    For each player-game, draws ``n_samples`` from the fitted NegBin
    distribution and derives quantiles / point estimates from the samples.

    If the model was trained with an exposure/offset (e.g. ``projected_ab``),
    the predictor automatically extracts it from the features dict and passes
    it to the sampling routine.
    """

    def __init__(
        self,
        model: NegBinModel,
        stat: str = "hits",
        n_samples: int = 10_000,
        random_state: int = 42,
    ):
        self.model = model
        self.stat = stat
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        stat: str,
        n_samples: int = 10_000,
        random_state: int = 42,
    ) -> MLBNegBinPredictor:
        """Load a NegBinModel from *directory* and wrap it."""
        model_name = f"batter_{stat}"
        model = NegBinModel.load(directory, model_name=model_name)
        return cls(model=model, stat=stat, n_samples=n_samples, random_state=random_state)

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def _extract_exposure(self, features: dict) -> float | None:
        """Extract exposure value from features if model uses exposure."""
        if not self.model._uses_exposure:
            return None
        col = self.model._exposure_col or "projected_ab"
        val = features.get(col)
        if val is None or val <= 0:
            return max(features.get("batter_avg_ab_l5", 3.5), 1.0)
        return max(float(val), 1.0)

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Generate MC prediction for a single batter."""
        exposure_val = self._extract_exposure(features)
        X = self._prepare_features(features)
        exp_arr = np.array([exposure_val]) if exposure_val is not None else None
        samples = self.model.sample(
            X, n_samples=self.n_samples, rng=self.rng, exposure=exp_arr,
        ).flatten()

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat=self.stat,
            mean=float(samples.mean()),
            median=float(np.median(samples)),
            q10=float(np.percentile(samples, 10)),
            q25=float(np.percentile(samples, 25)),
            q50=float(np.percentile(samples, 50)),
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

        results: list[PropPrediction] = []
        for player_id, game_id, features in player_games:
            results.append(self.predict(player_id, game_id, features))
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _prepare_features(self, features: dict) -> pd.DataFrame:
        feature_names = self.model.feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df


class MLBBinomialPredictor:
    """Monte Carlo predictor backed by a BinomialModel.

    For each player-game, draws ``n_samples`` from the fitted Binomial(n, p)
    distribution where n = at-bats (from features) and p = learned hit probability.
    """

    def __init__(
        self,
        model: BinomialModel,
        stat: str = "batter_hits",
        n_samples: int = 10_000,
        random_state: int = 42,
        at_bats_feature: str = "projected_ab",
    ):
        self.model = model
        self.stat = stat
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)
        self.at_bats_feature = at_bats_feature

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        stat: str = "hits",
        n_samples: int = 10_000,
        random_state: int = 42,
    ) -> MLBBinomialPredictor:
        """Load a BinomialModel from *directory* and wrap it."""
        model_name = f"batter_{stat}"
        model = BinomialModel.load(directory, model_name=model_name)
        return cls(model=model, stat=stat, n_samples=n_samples, random_state=random_state)

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Generate MC prediction for a single batter's hits."""
        X = self._prepare_features(features)
        at_bats = np.array([max(features.get(self.at_bats_feature, 3.5), 1.0)])

        samples = self.model.sample(X, at_bats, n_samples=self.n_samples, rng=self.rng).flatten()

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat=self.stat,
            mean=float(samples.mean()),
            median=float(np.median(samples)),
            q10=float(np.percentile(samples, 10)),
            q25=float(np.percentile(samples, 25)),
            q50=float(np.percentile(samples, 50)),
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

        results: list[PropPrediction] = []
        for player_id, game_id, features in player_games:
            results.append(self.predict(player_id, game_id, features))
        return results

    def _prepare_features(self, features: dict) -> pd.DataFrame:
        feature_names = self.model.feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df


class MLBCompoundBinomialPredictor:
    """Compound Binomial predictor: AB ~ NegBin, then Hits ~ Binom(AB, p).

    Propagates at-bat uncertainty into the hit distribution by:
    1. Drawing AB samples from a NegBin model
    2. Getting hit probability from a Binomial model
    3. For each sample: hits = Binom(n=AB_sample, p=p_hit)

    This improves tail calibration compared to using a point estimate for AB.
    """

    def __init__(
        self,
        ab_model: NegBinModel,
        hit_model: BinomialModel,
        stat: str = "batter_hits",
        n_samples: int = 10_000,
        random_state: int = 42,
    ):
        self.ab_model = ab_model
        self.hit_model = hit_model
        self.stat = stat
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        stat: str = "hits",
        n_samples: int = 10_000,
        random_state: int = 42,
    ) -> MLBCompoundBinomialPredictor:
        """Load AB NegBin model and hit Binomial model from directory."""
        ab_model = NegBinModel.load(directory, model_name="batter_ab")
        hit_model = BinomialModel.load(directory, model_name=f"batter_{stat}")
        return cls(
            ab_model=ab_model,
            hit_model=hit_model,
            stat=stat,
            n_samples=n_samples,
            random_state=random_state,
        )

    def predict(
        self,
        player_id: int,
        game_id: int,
        features: dict,
    ) -> PropPrediction:
        """Generate compound Binomial prediction for hits."""
        from scipy.stats import binom as sp_binom

        # 1. Draw AB samples from NegBin
        X_ab = self._prepare_features_ab(features)
        ab_samples = self.ab_model.sample(
            X_ab, n_samples=self.n_samples, rng=self.rng
        ).flatten()
        ab_samples = np.maximum(ab_samples, 1).astype(int)  # At least 1 AB

        # 2. Get hit probability from Binomial model
        X_hit = self._prepare_features_hit(features)
        # projected_ab is needed as the "n" for predict_params, but we'll only use p
        projected_ab = np.array([max(features.get("projected_ab", 3.5), 1.0)])
        p_pred, _ = self.hit_model.predict_params(X_hit, projected_ab)
        p_hit = float(np.clip(p_pred[0], 0.01, 0.99))

        # 3. Compound sampling: for each AB draw, sample hits ~ Binom(AB, p)
        hit_samples = sp_binom.rvs(n=ab_samples, p=p_hit, random_state=self.rng)
        hit_samples = hit_samples.astype(float)

        return PropPrediction(
            player_id=player_id,
            game_id=str(game_id),
            stat=self.stat,
            mean=float(hit_samples.mean()),
            median=float(np.median(hit_samples)),
            q10=float(np.percentile(hit_samples, 10)),
            q25=float(np.percentile(hit_samples, 25)),
            q50=float(np.percentile(hit_samples, 50)),
            q75=float(np.percentile(hit_samples, 75)),
            q90=float(np.percentile(hit_samples, 90)),
            samples=hit_samples,
        )

    def predict_batch(
        self,
        player_games: list[tuple[int, int, dict]],
    ) -> list[PropPrediction]:
        """Batch predict for multiple batter-games."""
        if not player_games:
            return []
        results = []
        for player_id, game_id, features in player_games:
            results.append(self.predict(player_id, game_id, features))
        return results

    def _prepare_features_ab(self, features: dict) -> pd.DataFrame:
        """Prepare features for the AB NegBin model."""
        feature_names = self.ab_model.feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df

    def _prepare_features_hit(self, features: dict) -> pd.DataFrame:
        """Prepare features for the hit Binomial model."""
        feature_names = self.hit_model.feature_names
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df
