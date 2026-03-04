"""MLB Monte Carlo predictor for pitcher strikeouts.

Adapted from src/models/monte_carlo.py but simplified:
- No minutes-rate decomposition (pitcher K predicted directly)
- No copula (single stat, no correlation to model)
- Integer-valued output (floor at 0, round to integers)

Reuses PropPrediction from the NBA monte_carlo module (sport-agnostic).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.models.monte_carlo import PropPrediction

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
