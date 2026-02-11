# inference/monte_carlo.py

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from scipy.stats import norm as sp_norm

if TYPE_CHECKING:
    pass  # Type imports removed with THREES model archival

logger = logging.getLogger(__name__)


@dataclass
class PropPrediction:
    """Structured prediction output for a player prop."""

    player_id: int
    game_id: str
    stat: str

    # Point estimates
    mean: float
    median: float

    # Quantiles
    q10: float
    q25: float
    q50: float
    q75: float
    q90: float

    # Distribution
    samples: np.ndarray

    # Betting outputs
    def prob_over(self, line: float) -> float:
        return (self.samples > line).mean()

    def prob_under(self, line: float) -> float:
        return (self.samples < line).mean()

    def expected_value_over(self, line: float, odds: int) -> float:
        """Calculate EV of betting over at given American odds."""
        prob = self.prob_over(line)

        if odds > 0:
            profit = odds / 100
        else:
            profit = 100 / abs(odds)

        return prob * profit - (1 - prob)

    def expected_value_under(self, line: float, odds: int) -> float:
        """Calculate EV of betting under at given American odds."""
        prob = self.prob_under(line)

        if odds > 0:
            profit = odds / 100
        else:
            profit = 100 / abs(odds)

        return prob * profit - (1 - prob)


# Default variance inflation config
# Inflates the distribution around the median to account for underestimated variance
# Use for stats where correlation adjustment doesn't apply (e.g., REB)
# Values > 1.0 widen the distribution, < 1.0 narrow it
DEFAULT_VARIANCE_INFLATION = {
    "pts": 1.0,  # PTS is handled by correlation adjustment
    "reb": 1.15,  # REB needs ~15% wider distribution (no correlation to leverage)
    "ast": 1.0,  # AST is handled by correlation adjustment
}


# Default tail adjustment config
# Extends the lower tail more aggressively to capture fat-tailed distributions
# lower_tail_multiplier > 1.0 extends the lower tail further (fixes Q10 over-coverage)
# upper_tail_multiplier > 1.0 extends the upper tail further (fixes Q90 under-coverage)
# NOTE: Start conservative - these interact with variance_inflation
DEFAULT_TAIL_ADJUSTMENT = {
    "lower_tail_multiplier": 1.15,  # Extend lower tail 15% more
    "upper_tail_multiplier": 1.0,  # Keep upper tail as-is
}


# Default blowout/foul factor config
# Models unexpected minutes reduction due to blowouts, foul trouble, or minor injuries
# probability: chance of a "bad game" occurring (0.0 to 1.0)
# minutes_reduction: how much to reduce minutes in bad games (0.0 to 1.0)
# NOTE: Start conservative and tune based on backtest results
DEFAULT_BLOWOUT_CONFIG = {
    "enabled": False,  # Disabled by default - enable after tuning
    "probability": 0.05,  # 5% of games have unexpected minutes reduction
    "minutes_reduction": 0.30,  # Reduce minutes by 30% in those games
}


# Default correlation config based on empirical analysis
# Maps minutes midpoints to rate adjustment factors (relative to mean)
# Derived from: rate_at_bucket / overall_mean_rate
DEFAULT_CORRELATION_CONFIG = {
    "pts": {
        # Minutes midpoints for interpolation
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        # Rate adjustment factors (mean rate ~0.45, range 0.338-0.581)
        # factor = observed_rate / mean_rate
        "rate_factors": [0.75, 0.84, 0.94, 1.06, 1.18, 1.30, 1.29],
        "enabled": True,
    },
    "reb": {
        # Rebounds show slight negative correlation (-0.039)
        # Enable with subtle adjustment to fix under-prediction bias
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        "rate_factors": [1.02, 1.01, 1.00, 0.99, 0.99, 0.98, 0.97],
        "enabled": True,  # Enable to help fix bias
    },
    "ast": {
        # Minutes midpoints for interpolation
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        # Rate adjustment factors - slightly reduced to fix Q10 over-coverage
        # Original: [0.75, 0.84, 0.89, 1.00, 1.12, 1.21, 1.18]
        "rate_factors": [0.82, 0.88, 0.93, 1.00, 1.08, 1.14, 1.12],
        "enabled": True,
    },
}


class MonteCarloPredictor:
    """
    Generates probability distributions for player props
    using Monte Carlo simulation from quantile predictions.

    Supports correlated sampling to account for the relationship between
    minutes played and per-minute rates (e.g., star players play more AND
    score at higher rates).
    """

    def __init__(
        self,
        model_pipeline,
        n_samples: int = 10000,
        random_state: int = 42,
        correlation_config: dict | None = None,
        use_correlated_sampling: bool = True,
        variance_inflation: dict | None = None,
        tail_adjustment: dict | None = None,
        blowout_config: dict | None = None,
        copula_params: dict | None = None,
    ):
        """
        Initialize the Monte Carlo predictor.

        Args:
            model_pipeline: Trained PlayerPropsModelPipeline
            n_samples: Number of Monte Carlo samples
            random_state: Random seed for reproducibility
            correlation_config: Dict mapping stat -> {minutes_points, rate_factors, enabled}
                               If None, uses DEFAULT_CORRELATION_CONFIG
            use_correlated_sampling: Whether to apply correlation adjustment (legacy).
                                    Ignored when copula_params is provided.
            variance_inflation: Dict mapping stat -> inflation factor (e.g., {"reb": 1.15})
                               Values > 1.0 widen distribution, < 1.0 narrow it.
                               If None, uses DEFAULT_VARIANCE_INFLATION
            tail_adjustment: Dict with lower_tail_multiplier and upper_tail_multiplier
                            to extend tails more aggressively. If None, uses DEFAULT_TAIL_ADJUSTMENT
            blowout_config: Dict with {enabled, probability, minutes_reduction} for modeling
                           unexpected minutes cuts. If None, uses DEFAULT_BLOWOUT_CONFIG
            copula_params: Dict mapping stat -> Spearman rank correlation between minutes
                          and that stat's per-minute rate. When provided, uses Gaussian copula
                          sampling instead of the legacy post-hoc correlation adjustment.
                          Example: {"pts": 0.314, "reb": -0.046, "ast": 0.176}
        """
        self.pipeline = model_pipeline
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)
        self.use_correlated_sampling = use_correlated_sampling

        # Quantile probabilities for interpolation
        self.quantile_probs = np.array([0.10, 0.25, 0.50, 0.75, 0.90])

        # Load correlation config (legacy)
        # Use dict() to copy, avoiding shared mutable state with module-level defaults
        self.correlation_config = dict(correlation_config or DEFAULT_CORRELATION_CONFIG)

        # Load variance inflation config
        self.variance_inflation = dict(variance_inflation or DEFAULT_VARIANCE_INFLATION)

        # Load tail adjustment config
        self.tail_adjustment = dict(tail_adjustment or DEFAULT_TAIL_ADJUSTMENT)

        # Load blowout/foul config
        self.blowout_config = dict(blowout_config or DEFAULT_BLOWOUT_CONFIG)

        # Gaussian copula parameters (replaces legacy correlation adjustment when set)
        self.copula_params = copula_params

    def predict(
        self, player_id: int, game_id: str, features: dict, stats: list[str] = None
    ) -> dict[str, PropPrediction]:
        """
        Generate predictions for all requested stats.

        Args:
            player_id: Player identifier
            game_id: Game identifier
            features: Feature dictionary from FeatureStore
            stats: List of stats to predict (default: ['pts', 'reb', 'ast'])

        Returns:
            Dict mapping stat name to PropPrediction object
        """
        stats = stats or ["pts", "reb", "ast"]

        # Use copula path if params are available
        if self.copula_params:
            return self._predict_copula(player_id, game_id, features, stats)

        # Legacy path: independent sampling with optional post-hoc adjustment
        # 1. Predict minutes distribution
        minutes_samples = self._sample_minutes(features)

        # 2. For each stat, predict rate and combine
        predictions = {}

        for stat in stats:
            # Sample base rate distribution
            rate_samples = self._sample_rate(features, stat)

            # 3. Apply correlation adjustment if enabled
            if self.use_correlated_sampling:
                rate_samples = self._apply_correlation_adjustment(rate_samples, minutes_samples, stat)

            # 4. Combine: stat = minutes × rate
            stat_samples = minutes_samples * rate_samples

            # 5. Apply variance inflation if configured
            stat_samples = self._apply_variance_inflation(stat_samples, stat)

            # 6. Build prediction object
            predictions[stat] = PropPrediction(
                player_id=player_id,
                game_id=game_id,
                stat=stat,
                mean=stat_samples.mean(),
                median=np.median(stat_samples),
                q10=np.percentile(stat_samples, 10),
                q25=np.percentile(stat_samples, 25),
                q50=np.percentile(stat_samples, 50),
                q75=np.percentile(stat_samples, 75),
                q90=np.percentile(stat_samples, 90),
                samples=stat_samples,
            )

        return predictions

    def _predict_copula(
        self, player_id: int, game_id: str, features: dict, stats: list[str]
    ) -> dict[str, PropPrediction]:
        """
        Generate predictions using Gaussian copula for correlated sampling.

        Instead of sampling minutes and rates independently and applying a post-hoc
        adjustment, this generates correlated (minutes, rate) pairs that respect the
        empirical rank correlation while preserving both marginal distributions exactly.

        Algorithm:
          1. Get minutes quantile predictions → build inverse CDF
          2. Generate shared z_minutes ~ N(0,1) (same latent minutes for all stats)
          3. For each stat:
             a. Generate z_rate = ρ·z_minutes + √(1-ρ²)·z_independent
             b. Transform to uniform: u = Φ(z)
             c. Map through marginal inverse CDFs
             d. Multiply: stat = minutes × rate

        """
        predictions = {}

        # If no other stats to process, return early
        if not stats:
            return predictions

        # Get minutes quantile predictions
        X_min = self._prepare_features(features, self.pipeline.minutes_model.all_feature_names)
        min_qdf = self.pipeline.minutes_model.predict_quantiles(X_min)
        min_qvals = min_qdf.iloc[0].values
        min_ext_probs, min_ext_vals = self._build_extended_quantile_fn(self.quantile_probs, min_qvals)

        # Generate shared latent normal for minutes (same across all stats)
        z_minutes = self.rng.standard_normal(self.n_samples)

        for stat in stats:
            if stat not in self.pipeline.rate_models:
                continue

            # Get rank correlation for this stat (default 0 = independent)
            rho_s = self.copula_params.get(stat, 0.0)

            # Convert Spearman ρ to Gaussian copula parameter (Pearson ρ of the latent normals)
            rho_p = 2 * np.sin(np.pi * rho_s / 6)
            rho_p = np.clip(rho_p, -0.999, 0.999)

            # Generate correlated latent normal for rate
            z_indep = self.rng.standard_normal(self.n_samples)
            z_rate = rho_p * z_minutes + np.sqrt(max(0, 1 - rho_p ** 2)) * z_indep

            # Transform to uniform via Gaussian CDF
            u_minutes = sp_norm.cdf(z_minutes)
            u_rate = sp_norm.cdf(z_rate)

            # Map minutes through inverse CDF
            minutes_samples = self._map_uniforms_to_samples(u_minutes, min_ext_probs, min_ext_vals)
            minutes_samples = np.maximum(minutes_samples, 0)
            if self.blowout_config.get("enabled", False):
                minutes_samples = self._apply_blowout_factor(minutes_samples)

            # Get rate quantile predictions and map through inverse CDF
            rate_model = self.pipeline.rate_models[stat]
            X_rate = self._prepare_features(features, rate_model.all_feature_names)
            rate_qdf = rate_model.predict_quantiles(X_rate)
            rate_qvals = rate_qdf.iloc[0].values
            rate_ext_probs, rate_ext_vals = self._build_extended_quantile_fn(self.quantile_probs, rate_qvals)

            rate_samples = self._map_uniforms_to_samples(u_rate, rate_ext_probs, rate_ext_vals)
            rate_samples = np.maximum(rate_samples, 0)

            # Combine: stat = minutes × rate
            stat_samples = minutes_samples * rate_samples

            # Apply variance inflation if configured
            stat_samples = self._apply_variance_inflation(stat_samples, stat)

            predictions[stat] = PropPrediction(
                player_id=player_id,
                game_id=game_id,
                stat=stat,
                mean=stat_samples.mean(),
                median=np.median(stat_samples),
                q10=np.percentile(stat_samples, 10),
                q25=np.percentile(stat_samples, 25),
                q50=np.percentile(stat_samples, 50),
                q75=np.percentile(stat_samples, 75),
                q90=np.percentile(stat_samples, 90),
                samples=stat_samples,
            )

        return predictions

    def predict_batch_for_date(
        self, features_df: pd.DataFrame, stats: list[str] | None = None
    ) -> tuple[list[dict], dict[tuple, np.ndarray]]:
        """
        Batch predict for all players on a date.

        Uses batched XGBoost calls (4 total: 1 minutes + 3 rates) instead of
        N individual calls per player.

        Args:
            features_df: Multi-row DataFrame with all player features for one date.
                         Must have player_id, game_id columns.
            stats: Stats to predict (default: ['pts', 'reb', 'ast'])

        Returns:
            (predictions_list, samples_dict) where:
            - predictions_list: list of dicts with prediction fields
            - samples_dict: dict[(player_id, game_id, stat)] -> samples array
        """
        stats = stats or ["pts", "reb", "ast"]
        n_players = len(features_df)

        predictions_list = []
        samples_dict = {}

        player_ids = features_df["player_id"].values
        game_ids = features_df["game_id"].values
        player_names = features_df["player_name"].values if "player_name" in features_df.columns else [None] * n_players
        team_ids = features_df["team_id"].values if "team_id" in features_df.columns else [None] * n_players

        # If no other stats to process, return early
        if not stats:
            return predictions_list, samples_dict

        # 1. Batch minutes prediction (1 XGBoost call for ALL players)
        X_minutes = self._prepare_features_batch(features_df, self.pipeline.minutes_model.all_feature_names)
        minutes_quantiles_df = self.pipeline.minutes_model.predict_quantiles(X_minutes)

        # 2. Batch rate predictions (1 XGBoost call per stat)
        rate_quantiles = {}

        for stat in stats:
            if stat in self.pipeline.rate_models:
                X_rate = self._prepare_features_batch(features_df, self.pipeline.rate_models[stat].all_feature_names)
                rate_quantiles[stat] = self.pipeline.rate_models[stat].predict_quantiles(X_rate)

        # 3. Per-player sampling loop (fast numpy, not XGBoost)
        use_copula = self.copula_params is not None

        for i in range(n_players):
            minutes_qvals = minutes_quantiles_df.iloc[i].values

            if use_copula:
                # Copula path: build minutes inverse CDF, generate shared latent normal
                min_ext_probs, min_ext_vals = self._build_extended_quantile_fn(
                    self.quantile_probs, minutes_qvals
                )
                z_minutes = self.rng.standard_normal(self.n_samples)
            else:
                # Legacy path: sample minutes independently
                minutes_samples = self._inverse_transform_sample(self.quantile_probs, minutes_qvals)
                if self.blowout_config.get("enabled", False):
                    minutes_samples = self._apply_blowout_factor(minutes_samples)
                minutes_samples = np.maximum(minutes_samples, 0)

            for stat in stats:
                if stat not in rate_quantiles:
                    continue

                if use_copula:
                    # Generate correlated (minutes, rate) via Gaussian copula
                    rho_s = self.copula_params.get(stat, 0.0)
                    rho_p = 2 * np.sin(np.pi * rho_s / 6)
                    rho_p = np.clip(rho_p, -0.999, 0.999)

                    z_indep = self.rng.standard_normal(self.n_samples)
                    z_rate = rho_p * z_minutes + np.sqrt(max(0, 1 - rho_p ** 2)) * z_indep

                    u_minutes = sp_norm.cdf(z_minutes)
                    u_rate = sp_norm.cdf(z_rate)

                    minutes_samples = self._map_uniforms_to_samples(u_minutes, min_ext_probs, min_ext_vals)
                    minutes_samples = np.maximum(minutes_samples, 0)
                    if self.blowout_config.get("enabled", False):
                        minutes_samples = self._apply_blowout_factor(minutes_samples)

                    # Rate model sampling
                    rate_qvals = rate_quantiles[stat].iloc[i].values
                    rate_ext_probs, rate_ext_vals = self._build_extended_quantile_fn(
                        self.quantile_probs, rate_qvals
                    )
                    rate_samples = self._map_uniforms_to_samples(u_rate, rate_ext_probs, rate_ext_vals)
                    rate_samples = np.maximum(rate_samples, 0)
                else:
                    # Legacy path: independent rate + post-hoc adjustment
                    rate_qvals = rate_quantiles[stat].iloc[i].values
                    rate_samples = self._inverse_transform_sample(self.quantile_probs, rate_qvals)
                    rate_samples = np.maximum(rate_samples, 0)
                    if self.use_correlated_sampling:
                        rate_samples = self._apply_correlation_adjustment(rate_samples, minutes_samples, stat)

                # Combine: stat = minutes * rate
                stat_samples = minutes_samples * rate_samples

                # Apply variance inflation
                stat_samples = self._apply_variance_inflation(stat_samples, stat)

                predictions_list.append(
                    {
                        "player_id": player_ids[i],
                        "player_name": player_names[i],
                        "game_id": game_ids[i],
                        "team_id": team_ids[i],
                        "stat": stat,
                        "pred_mean": float(stat_samples.mean()),
                        "pred_std": float(stat_samples.std()),
                        "pred_median": float(np.median(stat_samples)),
                        "pred_q10": float(np.percentile(stat_samples, 10)),
                        "pred_q25": float(np.percentile(stat_samples, 25)),
                        "pred_q50": float(np.percentile(stat_samples, 50)),
                        "pred_q75": float(np.percentile(stat_samples, 75)),
                        "pred_q90": float(np.percentile(stat_samples, 90)),
                    }
                )
                samples_dict[(player_ids[i], game_ids[i], stat)] = stat_samples

        return predictions_list, samples_dict

    def _prepare_features_batch(self, features_df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
        """Prepare multi-row feature DataFrame for model input."""
        present = [f for f in feature_names if f in features_df.columns]
        missing = [f for f in feature_names if f not in features_df.columns]

        df = features_df[present].copy()
        for col in missing:
            df[col] = 0

        df = df[feature_names]
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df

    def _apply_variance_inflation(
        self,
        samples: np.ndarray,
        stat: str,
    ) -> np.ndarray:
        """
        Inflate variance of samples around the median.

        This widens the distribution to account for underestimated variance,
        particularly useful for high-variance stats like rebounds where
        correlation adjustment doesn't apply.

        Formula: inflated = median + (sample - median) * inflation_factor

        Args:
            samples: Combined stat samples (minutes × rate)
            stat: The stat being predicted

        Returns:
            Variance-inflated samples
        """
        inflation_factor = self.variance_inflation.get(stat, 1.0)

        # Skip if no inflation needed
        if inflation_factor == 1.0:
            return samples

        median = np.median(samples)

        # Inflate around median: push values away from center
        inflated = median + (samples - median) * inflation_factor

        # Ensure non-negative (can't have negative stats)
        return np.maximum(inflated, 0)

    def _apply_correlation_adjustment(
        self,
        rate_samples: np.ndarray,
        minutes_samples: np.ndarray,
        stat: str,
    ) -> np.ndarray:
        """
        Adjust rate samples based on minutes samples to capture correlation.

        For stats like PTS and AST, players who play more minutes tend to have
        higher per-minute rates. This adjustment scales rate samples up/down
        based on the corresponding minutes sample.

        Args:
            rate_samples: Base rate samples from the rate model
            minutes_samples: Minutes samples from the minutes model
            stat: The stat being predicted

        Returns:
            Adjusted rate samples
        """
        config = self.correlation_config.get(stat)

        if config is None or not config.get("enabled", False):
            return rate_samples

        minutes_points = np.array(config["minutes_points"])
        rate_factors = np.array(config["rate_factors"])

        # For each minutes sample, interpolate the adjustment factor
        # Clamp minutes to valid range for interpolation
        clamped_minutes = np.clip(minutes_samples, minutes_points[0], minutes_points[-1])

        # Interpolate adjustment factors
        adjustment_factors = np.interp(clamped_minutes, minutes_points, rate_factors)

        # Apply adjustment
        adjusted_rates = rate_samples * adjustment_factors

        # Ensure non-negative
        return np.maximum(adjusted_rates, 0)

    def _sample_minutes(self, features: dict) -> np.ndarray:
        """
        Sample from the minutes distribution.

        Applies blowout/foul factor to model unexpected minutes reductions
        (blowouts, foul trouble, minor injuries).
        """
        # Get quantile predictions
        X = self._prepare_features(features, self.pipeline.minutes_model.all_feature_names)
        quantiles_df = self.pipeline.minutes_model.predict_quantiles(X)

        quantile_values = quantiles_df.iloc[0].values

        # Sample using inverse transform
        samples = self._inverse_transform_sample(self.quantile_probs, quantile_values)

        # Apply blowout/foul factor if enabled
        if self.blowout_config.get("enabled", False):
            samples = self._apply_blowout_factor(samples)

        # Floor at 0 (can't play negative minutes)
        return np.maximum(samples, 0)

    def _apply_blowout_factor(self, minutes_samples: np.ndarray) -> np.ndarray:
        """
        Apply blowout/foul factor to minutes samples.

        Models scenarios where a player's minutes are unexpectedly cut:
        - Blowouts (team winning/losing big, starters rest in 4th quarter)
        - Foul trouble (player fouls out or sits with 5 fouls)
        - Minor injuries (tweaks something, doesn't return)

        This creates a mixture distribution where most games are normal,
        but some have significantly reduced minutes.
        """
        probability = self.blowout_config.get("probability", 0.08)
        reduction = self.blowout_config.get("minutes_reduction", 0.35)

        # Randomly select which samples get the blowout treatment
        blowout_mask = self.rng.random(len(minutes_samples)) < probability

        # Apply reduction to those samples
        minutes_samples = minutes_samples.copy()
        minutes_samples[blowout_mask] *= 1 - reduction

        return minutes_samples

    # Rate values below this threshold are snapped to exactly 0.
    # Handles distributions where many true values are 0 but inverse CDF interpolation
    # produces tiny positive values.
    # 1e-3 per minute × 30 min = 0.03 combined — negligible for integer-valued stats.
    ZERO_SNAP_THRESHOLD = 1e-3

    def _build_extended_quantile_fn(
        self, quantile_probs: np.ndarray, quantile_values: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Build extended probability and value arrays for inverse CDF mapping.

        Extrapolates tails to p=0.01 and p=0.99 using linear extrapolation
        with configurable tail multipliers.

        Handles zero-inflated distributions: when lower quantile values are at
        or near zero, snaps them to exactly 0 so that uniform samples in that
        range map to 0 instead of tiny positive interpolated values.

        Returns:
            (extended_probs, extended_values) arrays for use with np.interp
        """
        lower_mult = self.tail_adjustment.get("lower_tail_multiplier", 1.0)
        upper_mult = self.tail_adjustment.get("upper_tail_multiplier", 1.0)

        extended_probs = np.concatenate([[0.01], quantile_probs, [0.99]])

        lower_slope = (quantile_values[1] - quantile_values[0]) / (quantile_probs[1] - quantile_probs[0])
        upper_slope = (quantile_values[-1] - quantile_values[-2]) / (quantile_probs[-1] - quantile_probs[-2])

        lower_value = quantile_values[0] - (lower_slope * (quantile_probs[0] - 0.01) * lower_mult)
        upper_value = quantile_values[-1] + (upper_slope * (0.99 - quantile_probs[-1]) * upper_mult)

        extended_values = np.concatenate(
            [
                [max(0, lower_value)],
                quantile_values,
                [upper_value],
            ]
        )

        # Zero-inflation handling: snap near-zero values to exactly 0.
        # This ensures uniform samples in the zero-mass region map to 0 instead
        # of tiny positive values from linear interpolation.
        extended_values = np.where(
            extended_values < self.ZERO_SNAP_THRESHOLD,
            0.0,
            extended_values,
        )

        return extended_probs, extended_values

    def _map_uniforms_to_samples(
        self, uniforms: np.ndarray, extended_probs: np.ndarray, extended_values: np.ndarray
    ) -> np.ndarray:
        """
        Map uniform samples through an inverse CDF defined by extended quantile arrays.

        Args:
            uniforms: Uniform(0,1) samples (from copula or direct)
            extended_probs: Probability grid (from _build_extended_quantile_fn)
            extended_values: Value grid (from _build_extended_quantile_fn)

        Returns:
            Samples in the original stat/minutes space
        """
        clipped = np.clip(uniforms, 0.01, 0.99)
        return np.interp(clipped, extended_probs, extended_values)

    def _sample_rate(self, features: dict, stat: str) -> np.ndarray:
        """Sample from the rate distribution for a specific stat."""
        if stat not in self.pipeline.rate_models:
            raise ValueError(f"No rate model for stat: {stat}")

        # Get quantile predictions (use stat-specific feature list)
        X = self._prepare_features(features, self.pipeline.rate_models[stat].all_feature_names)
        quantiles_df = self.pipeline.rate_models[stat].predict_quantiles(X)

        quantile_values = quantiles_df.iloc[0].values

        # Sample using inverse transform
        samples = self._inverse_transform_sample(self.quantile_probs, quantile_values)

        # Floor at 0 (can't have negative rate)
        return np.maximum(samples, 0)

    def _inverse_transform_sample(self, quantile_probs: np.ndarray, quantile_values: np.ndarray) -> np.ndarray:
        """
        Sample from a distribution defined by quantiles
        using inverse transform sampling with linear interpolation.

        Uses _build_extended_quantile_fn for tail extrapolation and zero-snap handling.
        """
        extended_probs, extended_values = self._build_extended_quantile_fn(quantile_probs, quantile_values)

        # Sample uniform and interpolate
        u = self.rng.uniform(0.01, 0.99, self.n_samples)
        samples = np.interp(u, extended_probs, extended_values)

        return samples

    def _prepare_features(self, features: dict, feature_names: list[str]) -> pd.DataFrame:
        """Prepare feature dict as DataFrame for model input."""
        row = {f: features.get(f, 0) for f in feature_names}
        df = pd.DataFrame([row])
        df = df.apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.float32)
        return df

    def batch_predict(self, player_games: list[tuple[int, str, dict]], stats: list[str] = None) -> pd.DataFrame:
        """
        Batch predict for multiple player-games.

        Args:
            player_games: List of (player_id, game_id, features) tuples
            stats: Stats to predict

        Returns:
            DataFrame with predictions
        """
        results = []

        for player_id, game_id, features in player_games:
            preds = self.predict(player_id, game_id, features, stats)

            for stat, pred in preds.items():
                results.append(
                    {
                        "player_id": player_id,
                        "game_id": game_id,
                        "stat": stat,
                        "mean": pred.mean,
                        "median": pred.median,
                        "q10": pred.q10,
                        "q25": pred.q25,
                        "q50": pred.q50,
                        "q75": pred.q75,
                        "q90": pred.q90,
                    }
                )

        return pd.DataFrame(results)


def compute_copula_params_from_data(df: pd.DataFrame) -> dict:
    """
    Compute Gaussian copula parameters (Spearman rank correlations) from training data.

    These parameters are used by MonteCarloPredictor to generate correlated
    (minutes, rate) samples that preserve both marginal distributions while
    capturing the empirical dependency structure.

    Args:
        df: Training dataframe with actual_minutes and {stat}_per_min columns

    Returns:
        Dict mapping stat -> Spearman rank correlation with minutes.
        Example: {"pts": 0.314, "reb": -0.046, "ast": 0.176}
    """
    from scipy.stats import spearmanr

    valid_mask = df["actual_minutes"] >= 10
    analysis_df = df[valid_mask].copy()

    params = {}
    for stat in ["pts", "reb", "ast"]:
        rate_col = f"{stat}_per_min"

        # Compute rate if needed
        if rate_col not in analysis_df.columns:
            actual_col = f"actual_{stat}"
            if actual_col in analysis_df.columns:
                analysis_df[rate_col] = analysis_df[actual_col] / analysis_df["actual_minutes"]
            else:
                continue

        # Drop NaN/inf for clean correlation
        valid = analysis_df[["actual_minutes", rate_col]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(valid) < 50:
            params[stat] = 0.0
            continue

        rho, _ = spearmanr(valid["actual_minutes"], valid[rate_col])
        params[stat] = round(float(rho), 4)

    return params


def load_copula_params(model_dir: str) -> dict | None:
    """
    Load copula parameters from a model artifacts directory.

    Returns None if no copula_params.json exists (backward compatible).
    """
    import json
    from pathlib import Path

    path = Path(model_dir) / "copula_params.json"
    if not path.exists():
        return None

    with open(path) as f:
        return json.load(f)


def compute_correlation_config_from_data(df: pd.DataFrame) -> dict:
    """
    Compute correlation config from training data.

    This can be used to generate custom correlation factors instead of
    using the defaults.

    Args:
        df: Training dataframe with actual_minutes and {stat}_per_min columns

    Returns:
        Dict suitable for use as correlation_config in MonteCarloPredictor
    """
    config = {}

    valid_mask = df["actual_minutes"] >= 10
    analysis_df = df[valid_mask].copy()

    for stat in ["pts", "reb", "ast"]:
        rate_col = f"{stat}_per_min"

        # Compute rate if needed
        if rate_col not in analysis_df.columns:
            actual_col = f"actual_{stat}"
            if actual_col in analysis_df.columns:
                analysis_df[rate_col] = analysis_df[actual_col] / analysis_df["actual_minutes"]
            else:
                continue

        # Skip if no data
        if rate_col not in analysis_df.columns:
            continue

        # Overall correlation
        corr = analysis_df["actual_minutes"].corr(analysis_df[rate_col])
        overall_mean_rate = analysis_df[rate_col].mean()

        # Compute rate by minutes bucket
        minutes_points = []
        rate_factors = []

        buckets = [(10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40), (40, 50)]

        for low, high in buckets:
            bucket_df = analysis_df[(analysis_df["actual_minutes"] >= low) & (analysis_df["actual_minutes"] < high)]

            if len(bucket_df) < 30:
                continue

            midpoint = (low + high) / 2
            mean_rate = bucket_df[rate_col].mean()
            factor = mean_rate / overall_mean_rate if overall_mean_rate > 0 else 1.0

            minutes_points.append(midpoint)
            rate_factors.append(round(factor, 3))

        # Only enable if significant correlation
        enabled = abs(corr) > 0.1 and len(minutes_points) >= 3

        config[stat] = {
            "minutes_points": minutes_points,
            "rate_factors": rate_factors,
            "enabled": enabled,
            "correlation": round(corr, 3),
            "overall_mean_rate": round(overall_mean_rate, 4),
        }

    return config
