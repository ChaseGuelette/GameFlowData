"""
Black-Litterman probability blending for sports betting.

Anchors overconfident model probabilities to the market's well-calibrated prior,
only deviating when the model shows high-confidence disagreement.

Uses log-odds space blending (not linear probability) to handle boundary effects
correctly. Devigging via multiplicative normalization removes bookmaker vig from
the market prior. For 2-outcome markets (over/under), this is equivalent to
Shin's method and the additive method.

Reference:
    - Black & Litterman (1992), "Global Portfolio Optimization"
    - Idzorek (2005), "A Step-by-Step Guide to the Black-Litterman Model"
    - Hubacek et al. (2019), "Exploiting Sports-Betting Market Using Machine Learning"
"""

import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class BLConfig:
    """Configuration for Black-Litterman blending."""

    tau: float = 0.05
    """Global scaling of model influence (0.01=conservative, 0.30=aggressive)."""

    max_weight: float = 0.50
    """Hard cap on blending weight. Never trust model more than market."""

    min_prob: float = 0.01
    """Floor for probabilities to avoid log(0)."""

    max_prob: float = 0.99
    """Ceiling for probabilities to avoid log(0)."""

    z_max: float = 1.0
    """Z-score at which confidence saturates to 1.0. Lower = more aggressive."""


class BlackLittermanBlender:
    """Blends model probabilities with market prior using log-odds BL.

    The posterior probability is computed as:
        market_logit = log(p_market / (1 - p_market))
        model_logit  = log(p_model / (1 - p_model))
        w = min(tau * confidence, max_weight)
        posterior_logit = market_logit + w * (model_logit - market_logit)
        posterior = sigmoid(posterior_logit)

    When confidence is low (model distribution centered on the line),
    posterior ~= market and no bet is placed. When confidence is high
    (model distribution far from the line), posterior shifts toward the model.
    """

    def __init__(self, config: BLConfig | None = None):
        self.config = config or BLConfig()

    @staticmethod
    def american_to_decimal(american_odds: int | float) -> float:
        """Convert American odds to decimal odds.

        Args:
            american_odds: American format odds (e.g., -110, +150).

        Returns:
            Decimal odds (e.g., 1.909, 2.50).
        """
        if american_odds > 0:
            return 1.0 + american_odds / 100.0
        elif american_odds < 0:
            return 1.0 + 100.0 / abs(american_odds)
        else:
            # Odds of 0 is degenerate — return even money
            return 2.0

    def devig(self, over_odds: int | float, under_odds: int | float) -> tuple[float, float]:
        """Remove bookmaker vig using multiplicative normalization.

        For 2-outcome markets (over/under), Shin's method, additive, and
        multiplicative devigging all produce identical results. This implements
        the standard approach: divide each raw implied probability by the booksum.

        Args:
            over_odds: American odds for the over.
            under_odds: American odds for the under.

        Returns:
            Tuple of (devigged_over_prob, devigged_under_prob) summing to 1.0.
        """
        dec_over = self.american_to_decimal(over_odds)
        dec_under = self.american_to_decimal(under_odds)

        # Raw implied probabilities (sum to > 1.0 due to vig)
        raw_over = 1.0 / max(dec_over, 1.01)
        raw_under = 1.0 / max(dec_under, 1.01)

        # Normalize to remove vig
        booksum = raw_over + raw_under
        if booksum <= 0:
            return 0.5, 0.5

        return float(raw_over / booksum), float(raw_under / booksum)

    def compute_confidence(self, samples: np.ndarray, line: float) -> float:
        """Compute per-prediction confidence from MC distribution properties.

        Uses linear ramp based on z-score of the line relative to distribution center:
            z = |mean(samples) - line| / std(samples)
            confidence = min(z / z_max, 1.0)

        This gives meaningful weight even at low z-scores where profitable edges exist:
            - z=0 (line at center): confidence=0 -> posterior = market
            - z=0.5*z_max: confidence=0.5
            - z>=z_max: confidence=1.0 -> full model weight (capped by max_weight)

        Args:
            samples: Monte Carlo samples from the predictor (shape: (n_samples,)).
            line: The sportsbook prop line.

        Returns:
            Confidence in [0.0, 1.0].
        """
        if len(samples) == 0:
            return 0.0

        mean = float(np.mean(samples))
        std = float(np.std(samples))

        if std < 1e-6:
            return 0.0

        z = abs(mean - line) / std
        return float(min(z / self.config.z_max, 1.0))

    def blend(self, model_prob: float, market_prob: float, confidence: float) -> float:
        """Blend model and market probabilities in log-odds space.

        Args:
            model_prob: Model's empirical P(over) or P(under) from MC samples.
            market_prob: Shin-devigged market probability.
            confidence: Per-prediction confidence (0.0 to 1.0).

        Returns:
            Posterior probability in (min_prob, max_prob).
        """
        # Clamp to avoid log(0)
        model_p = np.clip(model_prob, self.config.min_prob, self.config.max_prob)
        market_p = np.clip(market_prob, self.config.min_prob, self.config.max_prob)

        # Convert to log-odds
        market_logit = np.log(market_p / (1.0 - market_p))
        model_logit = np.log(model_p / (1.0 - model_p))

        # Compute blending weight (capped)
        w = min(self.config.tau * confidence, self.config.max_weight)

        # Blend in log-odds space
        posterior_logit = market_logit + w * (model_logit - market_logit)

        # Convert back to probability
        posterior = 1.0 / (1.0 + np.exp(-posterior_logit))
        return float(posterior)

    def blend_prediction(
        self,
        samples: np.ndarray,
        line: float,
        over_odds: int | float,
        under_odds: int | float,
    ) -> dict:
        """Full BL blending pipeline for a single prediction.

        Takes raw MC samples and sportsbook odds, returns devigged market probs,
        raw model probs, confidence, and blended posterior probs.

        Args:
            samples: Monte Carlo samples (shape: (n_samples,)).
            line: Sportsbook prop line.
            over_odds: American odds for over.
            under_odds: American odds for under.

        Returns:
            Dict with keys:
                market_over, market_under: Shin-devigged market probabilities.
                model_over, model_under: Raw MC empirical probabilities.
                confidence: BL per-prediction confidence.
                posterior_over, posterior_under: Blended posterior probabilities.
        """
        # Step 1: Devig market odds
        market_over, market_under = self.devig(over_odds, under_odds)

        # Step 2: Compute raw model probabilities from MC samples
        model_over = float((samples > line).mean())
        model_under = float((samples < line).mean())

        # Step 3: Compute confidence
        confidence = self.compute_confidence(samples, line)

        # Step 4: Blend
        posterior_over = self.blend(model_over, market_over, confidence)
        posterior_under = self.blend(model_under, market_under, confidence)

        return {
            "market_over": market_over,
            "market_under": market_under,
            "model_over": model_over,
            "model_under": model_under,
            "confidence": confidence,
            "posterior_over": posterior_over,
            "posterior_under": posterior_under,
        }
