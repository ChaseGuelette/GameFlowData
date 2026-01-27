# inference/monte_carlo.py

from dataclasses import dataclass

import numpy as np
import pandas as pd


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
    "pts": 1.0,    # PTS is handled by correlation adjustment
    "reb": 1.15,   # REB needs ~15% wider distribution (no correlation to leverage)
    "ast": 1.0,    # AST is handled by correlation adjustment
    "threes": 1.0, # Threes handled by correlation adjustment
}


# Default tail adjustment config
# Extends the lower tail more aggressively to capture fat-tailed distributions
# lower_tail_multiplier > 1.0 extends the lower tail further (fixes Q10 over-coverage)
# upper_tail_multiplier > 1.0 extends the upper tail further (fixes Q90 under-coverage)
# NOTE: Start conservative - these interact with variance_inflation
DEFAULT_TAIL_ADJUSTMENT = {
    "lower_tail_multiplier": 1.15,  # Extend lower tail 15% more
    "upper_tail_multiplier": 1.0,   # Keep upper tail as-is
}


# Default blowout/foul factor config
# Models unexpected minutes reduction due to blowouts, foul trouble, or minor injuries
# probability: chance of a "bad game" occurring (0.0 to 1.0)
# minutes_reduction: how much to reduce minutes in bad games (0.0 to 1.0)
# NOTE: Start conservative and tune based on backtest results
DEFAULT_BLOWOUT_CONFIG = {
    "enabled": False,          # Disabled by default - enable after tuning
    "probability": 0.05,       # 5% of games have unexpected minutes reduction
    "minutes_reduction": 0.30, # Reduce minutes by 30% in those games
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
    "threes": {
        # Default to similar pattern as PTS (3-pointers correlate with playing time)
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        "rate_factors": [0.80, 0.88, 0.95, 1.05, 1.12, 1.18, 1.15],
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
    ):
        """
        Initialize the Monte Carlo predictor.

        Args:
            model_pipeline: Trained PlayerPropsModelPipeline
            n_samples: Number of Monte Carlo samples
            random_state: Random seed for reproducibility
            correlation_config: Dict mapping stat -> {minutes_points, rate_factors, enabled}
                               If None, uses DEFAULT_CORRELATION_CONFIG
            use_correlated_sampling: Whether to apply correlation adjustment
            variance_inflation: Dict mapping stat -> inflation factor (e.g., {"reb": 1.15})
                               Values > 1.0 widen distribution, < 1.0 narrow it.
                               If None, uses DEFAULT_VARIANCE_INFLATION
            tail_adjustment: Dict with lower_tail_multiplier and upper_tail_multiplier
                            to extend tails more aggressively. If None, uses DEFAULT_TAIL_ADJUSTMENT
            blowout_config: Dict with {enabled, probability, minutes_reduction} for modeling
                           unexpected minutes cuts. If None, uses DEFAULT_BLOWOUT_CONFIG
        """
        self.pipeline = model_pipeline
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)
        self.use_correlated_sampling = use_correlated_sampling

        # Quantile probabilities for interpolation
        self.quantile_probs = np.array([0.10, 0.25, 0.50, 0.75, 0.90])

        # Load correlation config
        self.correlation_config = correlation_config or DEFAULT_CORRELATION_CONFIG

        # Load variance inflation config
        self.variance_inflation = variance_inflation or DEFAULT_VARIANCE_INFLATION

        # Load tail adjustment config
        self.tail_adjustment = tail_adjustment or DEFAULT_TAIL_ADJUSTMENT

        # Load blowout/foul config
        self.blowout_config = blowout_config or DEFAULT_BLOWOUT_CONFIG

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

        # 1. Predict minutes distribution
        minutes_samples = self._sample_minutes(features)

        # 2. For each stat, predict rate and combine
        predictions = {}

        for stat in stats:
            # Sample base rate distribution
            rate_samples = self._sample_rate(features, stat)

            # 3. Apply correlation adjustment if enabled
            if self.use_correlated_sampling:
                rate_samples = self._apply_correlation_adjustment(
                    rate_samples, minutes_samples, stat
                )

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
        minutes_samples[blowout_mask] *= (1 - reduction)

        return minutes_samples

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

        Uses tail_adjustment to extend tails more aggressively for fat-tailed distributions.
        """
        # Get tail adjustment multipliers
        lower_mult = self.tail_adjustment.get("lower_tail_multiplier", 1.0)
        upper_mult = self.tail_adjustment.get("upper_tail_multiplier", 1.0)

        # Extend to handle tails
        # Add pseudo-quantiles at 0.01 and 0.99 using linear extrapolation
        extended_probs = np.concatenate([[0.01], quantile_probs, [0.99]])

        # Extrapolate values for tails with adjustment multipliers
        lower_slope = (quantile_values[1] - quantile_values[0]) / (quantile_probs[1] - quantile_probs[0])
        upper_slope = (quantile_values[-1] - quantile_values[-2]) / (quantile_probs[-1] - quantile_probs[-2])

        # Apply tail multipliers to extend tails more aggressively
        lower_value = quantile_values[0] - (lower_slope * (quantile_probs[0] - 0.01) * lower_mult)
        upper_value = quantile_values[-1] + (upper_slope * (0.99 - quantile_probs[-1]) * upper_mult)

        extended_values = np.concatenate(
            [
                [max(0, lower_value)],  # Floor at 0
                quantile_values,
                [upper_value],
            ]
        )

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

    for stat in ["pts", "reb", "ast", "threes"]:
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
            bucket_df = analysis_df[
                (analysis_df["actual_minutes"] >= low) & (analysis_df["actual_minutes"] < high)
            ]

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
