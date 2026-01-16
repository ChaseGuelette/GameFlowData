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


class MonteCarloPredictor:
    """
    Generates probability distributions for player props
    using Monte Carlo simulation from quantile predictions.
    """

    def __init__(self, model_pipeline, n_samples: int = 10000, random_state: int = 42):
        self.pipeline = model_pipeline
        self.n_samples = n_samples
        self.rng = np.random.RandomState(random_state)

        # Quantile probabilities for interpolation
        self.quantile_probs = np.array([0.10, 0.25, 0.50, 0.75, 0.90])

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
            rate_samples = self._sample_rate(features, stat)

            # 3. Combine: stat = minutes × rate
            stat_samples = minutes_samples * rate_samples

            # 4. Build prediction object
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

    def _sample_minutes(self, features: dict) -> np.ndarray:
        """Sample from the minutes distribution."""
        # Get quantile predictions
        X = self._prepare_features(features, self.pipeline.minutes_features)
        quantiles_df = self.pipeline.minutes_model.predict_quantiles(X)

        quantile_values = quantiles_df.iloc[0].values

        # Sample using inverse transform
        samples = self._inverse_transform_sample(self.quantile_probs, quantile_values)

        # Floor at 0 (can't play negative minutes)
        return np.maximum(samples, 0)

    def _sample_rate(self, features: dict, stat: str) -> np.ndarray:
        """Sample from the rate distribution for a specific stat."""
        if stat not in self.pipeline.rate_models:
            raise ValueError(f"No rate model for stat: {stat}")

        # Get quantile predictions
        X = self._prepare_features(features, self.pipeline.rate_features)
        quantiles_df = self.pipeline.rate_models[stat].predict_quantiles(X)

        quantile_values = quantiles_df.iloc[0].values

        # Sample using inverse transform
        samples = self._inverse_transform_sample(self.quantile_probs, quantile_values)

        # Floor at 0 (can't have negative rate)
        return np.maximum(samples, 0)

    def _inverse_transform_sample(
        self, quantile_probs: np.ndarray, quantile_values: np.ndarray
    ) -> np.ndarray:
        """
        Sample from a distribution defined by quantiles
        using inverse transform sampling with linear interpolation.
        """
        # Extend to handle tails
        # Add pseudo-quantiles at 0.01 and 0.99 using linear extrapolation
        extended_probs = np.concatenate([[0.01], quantile_probs, [0.99]])

        # Extrapolate values for tails
        lower_slope = (quantile_values[1] - quantile_values[0]) / (
            quantile_probs[1] - quantile_probs[0]
        )
        upper_slope = (quantile_values[-1] - quantile_values[-2]) / (
            quantile_probs[-1] - quantile_probs[-2]
        )

        lower_value = quantile_values[0] - lower_slope * (quantile_probs[0] - 0.01)
        upper_value = quantile_values[-1] + upper_slope * (0.99 - quantile_probs[-1])

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
        return pd.DataFrame([row])

    def batch_predict(
        self, player_games: list[tuple[int, str, dict]], stats: list[str] = None
    ) -> pd.DataFrame:
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
