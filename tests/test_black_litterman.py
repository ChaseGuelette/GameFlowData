"""Tests for Black-Litterman probability blending module."""

import numpy as np
import pytest

from src.models.black_litterman import BlackLittermanBlender, BLConfig


class TestAmericanToDecimal:
    """Tests for American-to-decimal odds conversion."""

    def test_negative_odds(self):
        """Standard negative American odds."""
        # -110 -> 1.909
        result = BlackLittermanBlender.american_to_decimal(-110)
        assert result == pytest.approx(1.909, rel=0.01)

    def test_positive_odds(self):
        """Standard positive American odds."""
        # +150 -> 2.50
        result = BlackLittermanBlender.american_to_decimal(150)
        assert result == pytest.approx(2.50)

    def test_even_money(self):
        """Even money (+100)."""
        result = BlackLittermanBlender.american_to_decimal(100)
        assert result == pytest.approx(2.0)

    def test_heavy_favorite(self):
        """-500 -> 1.20"""
        result = BlackLittermanBlender.american_to_decimal(-500)
        assert result == pytest.approx(1.20)

    def test_big_underdog(self):
        """+400 -> 5.00"""
        result = BlackLittermanBlender.american_to_decimal(400)
        assert result == pytest.approx(5.0)

    def test_zero_odds_fallback(self):
        """Zero odds should return even money (2.0)."""
        result = BlackLittermanBlender.american_to_decimal(0)
        assert result == 2.0


class TestDevig:
    """Tests for Shin's method devigging."""

    def setup_method(self):
        self.blender = BlackLittermanBlender()

    def test_standard_line(self):
        """-110/-110 should devig to approximately 50/50."""
        over_p, under_p = self.blender.devig(-110, -110)
        assert over_p == pytest.approx(0.50, abs=0.01)
        assert under_p == pytest.approx(0.50, abs=0.01)
        assert over_p + under_p == pytest.approx(1.0, abs=1e-6)

    def test_favorite_longshot(self):
        """-200/+170: favorite should have higher probability."""
        over_p, under_p = self.blender.devig(-200, 170)
        assert over_p > under_p
        assert over_p + under_p == pytest.approx(1.0, abs=1e-6)

    def test_heavy_favorite(self):
        """-500/+400: heavy favorite should be around 80%+."""
        over_p, under_p = self.blender.devig(-500, 400)
        assert over_p > 0.75
        assert over_p + under_p == pytest.approx(1.0, abs=1e-6)

    def test_symmetry(self):
        """Swapping over/under odds should swap probabilities."""
        over_p1, under_p1 = self.blender.devig(-150, 130)
        over_p2, under_p2 = self.blender.devig(130, -150)
        assert over_p1 == pytest.approx(under_p2, abs=1e-6)
        assert under_p1 == pytest.approx(over_p2, abs=1e-6)

    def test_sum_to_one(self):
        """Output should always sum to 1.0 regardless of vig level."""
        test_cases = [
            (-110, -110),
            (-115, -105),
            (-200, 170),
            (-300, 250),
            (100, -120),
        ]
        for over, under in test_cases:
            over_p, under_p = self.blender.devig(over, under)
            assert over_p + under_p == pytest.approx(1.0, abs=1e-6), f"Failed for {over}/{under}"


class TestComputeConfidence:
    """Tests for z-score based confidence computation."""

    def setup_method(self):
        self.blender = BlackLittermanBlender()

    def test_line_at_center(self):
        """Line at distribution center → confidence ≈ 0."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        confidence = self.blender.compute_confidence(samples, 20.0)
        assert confidence < 0.05  # Very low confidence

    def test_line_one_std_away(self):
        """Line 1 std from center → confidence = 1.0 (z=1, z_max=1)."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        confidence = self.blender.compute_confidence(samples, 25.0)  # 1 std away
        assert confidence == pytest.approx(1.0, abs=0.05)

    def test_line_two_std_away(self):
        """Line 2 std from center → confidence = 1.0 (z=2, capped at 1.0)."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        confidence = self.blender.compute_confidence(samples, 30.0)  # 2 std away
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_line_three_std_away(self):
        """Line 3 std from center → confidence = 1.0 (z=3, capped at 1.0)."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        confidence = self.blender.compute_confidence(samples, 35.0)  # 3 std away
        assert confidence == pytest.approx(1.0, abs=0.01)

    def test_degenerate_std_zero(self):
        """All identical samples (std=0) → confidence = 0 (safe fallback)."""
        samples = np.full(1000, 20.0)
        confidence = self.blender.compute_confidence(samples, 25.0)
        assert confidence == 0.0

    def test_empty_samples(self):
        """Empty samples array → confidence = 0."""
        samples = np.array([])
        confidence = self.blender.compute_confidence(samples, 20.0)
        assert confidence == 0.0

    def test_confidence_bounded(self):
        """Confidence should always be in [0, 1]."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        for line in [0.0, 10.0, 20.0, 30.0, 50.0, 100.0]:
            confidence = self.blender.compute_confidence(samples, line)
            assert 0.0 <= confidence <= 1.0, f"Out of bounds for line={line}"

    def test_symmetric(self):
        """Confidence is symmetric around the mean (direction doesn't matter)."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        conf_above = self.blender.compute_confidence(samples, 25.0)
        conf_below = self.blender.compute_confidence(samples, 15.0)
        assert conf_above == pytest.approx(conf_below, abs=0.05)

    def test_linear_confidence_at_half_z_max(self):
        """Line 0.5 std from center → confidence = 0.5 (linear ramp)."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        # 0.5 std away = 2.5 points
        confidence = self.blender.compute_confidence(samples, 22.5)
        assert confidence == pytest.approx(0.5, abs=0.05)

    def test_custom_z_max(self):
        """Custom z_max=2.0 → confidence at z=1 should be 0.5."""
        blender = BlackLittermanBlender(BLConfig(z_max=2.0))
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        # 1 std away (z=1), with z_max=2.0 → confidence = 0.5
        confidence = blender.compute_confidence(samples, 25.0)
        assert confidence == pytest.approx(0.5, abs=0.05)

    def test_linear_ramp_proportional(self):
        """Confidence should be proportional to z for z < z_max."""
        rng = np.random.RandomState(42)
        samples = rng.normal(20.0, 5.0, size=10000)
        # Test at z=0.2 (1 point away from mean of 20, std=5)
        conf_02 = self.blender.compute_confidence(samples, 21.0)  # z ≈ 0.2
        conf_04 = self.blender.compute_confidence(samples, 22.0)  # z ≈ 0.4
        # conf_04 should be approximately 2x conf_02
        assert conf_04 == pytest.approx(2 * conf_02, abs=0.1)


class TestBlend:
    """Tests for log-odds space blending."""

    def setup_method(self):
        self.blender = BlackLittermanBlender(BLConfig(tau=0.10))

    def test_tau_zero_returns_market(self):
        """With tau=0, posterior should equal market regardless of model."""
        blender = BlackLittermanBlender(BLConfig(tau=0.0))
        posterior = blender.blend(model_prob=0.80, market_prob=0.50, confidence=1.0)
        assert posterior == pytest.approx(0.50, abs=1e-6)

    def test_model_agrees_with_market(self):
        """When model ≈ market, posterior ≈ market."""
        posterior = self.blender.blend(model_prob=0.50, market_prob=0.50, confidence=0.5)
        assert posterior == pytest.approx(0.50, abs=0.01)

    def test_low_confidence_stays_near_market(self):
        """Low confidence → posterior stays near market."""
        posterior = self.blender.blend(model_prob=0.80, market_prob=0.50, confidence=0.1)
        # w = 0.10 * 0.1 = 0.01, very small shift
        assert abs(posterior - 0.50) < 0.02

    def test_high_confidence_shifts_toward_model(self):
        """High confidence → posterior shifts toward model."""
        posterior = self.blender.blend(model_prob=0.80, market_prob=0.50, confidence=0.9)
        # w = 0.10 * 0.9 = 0.09
        assert posterior > 0.50  # Shifted toward model
        assert posterior < 0.80  # But not all the way to model

    def test_weight_cap(self):
        """Weight should never exceed max_weight."""
        blender = BlackLittermanBlender(BLConfig(tau=1.0, max_weight=0.50))
        # tau=1.0, confidence=1.0 → w = 1.0, but capped at 0.50
        posterior = blender.blend(model_prob=0.90, market_prob=0.50, confidence=1.0)
        # At w=0.50, posterior should be midpoint in log-odds space
        # log-odds(0.50) = 0, log-odds(0.90) = 2.197
        # posterior_logit = 0 + 0.5 * (2.197 - 0) = 1.099
        # sigmoid(1.099) ≈ 0.75
        assert posterior == pytest.approx(0.75, abs=0.02)

    def test_boundary_near_zero(self):
        """Probabilities near 0 should not produce NaN/Inf."""
        posterior = self.blender.blend(model_prob=0.02, market_prob=0.05, confidence=0.5)
        assert 0.01 <= posterior <= 0.99
        assert not np.isnan(posterior)

    def test_boundary_near_one(self):
        """Probabilities near 1 should not produce NaN/Inf."""
        posterior = self.blender.blend(model_prob=0.98, market_prob=0.95, confidence=0.5)
        assert 0.01 <= posterior <= 0.99
        assert not np.isnan(posterior)

    def test_exact_zero_clamped(self):
        """Model prob of exactly 0.0 should be clamped safely."""
        posterior = self.blender.blend(model_prob=0.0, market_prob=0.50, confidence=0.5)
        assert not np.isnan(posterior)
        assert not np.isinf(posterior)

    def test_exact_one_clamped(self):
        """Model prob of exactly 1.0 should be clamped safely."""
        posterior = self.blender.blend(model_prob=1.0, market_prob=0.50, confidence=0.5)
        assert not np.isnan(posterior)
        assert not np.isinf(posterior)

    def test_posterior_between_market_and_model(self):
        """Posterior should always be between market and model (in log-odds sense)."""
        for model_p, market_p in [(0.7, 0.5), (0.3, 0.5), (0.9, 0.6), (0.2, 0.4)]:
            posterior = self.blender.blend(model_p, market_p, confidence=0.5)
            low = min(model_p, market_p)
            high = max(model_p, market_p)
            assert low - 0.01 <= posterior <= high + 0.01, (
                f"Posterior {posterior} not between {low} and {high} "
                f"for model={model_p}, market={market_p}"
            )


class TestBlendPrediction:
    """Tests for the full blend_prediction pipeline."""

    def setup_method(self):
        self.blender = BlackLittermanBlender(BLConfig(tau=0.10))
        self.rng = np.random.RandomState(42)

    def test_output_keys(self):
        """blend_prediction should return all expected keys."""
        samples = self.rng.normal(20.0, 5.0, size=5000)
        result = self.blender.blend_prediction(
            samples=samples, line=20.5, over_odds=-110, under_odds=-110,
        )
        expected_keys = {
            "market_over", "market_under",
            "model_over", "model_under",
            "confidence",
            "posterior_over", "posterior_under",
        }
        assert set(result.keys()) == expected_keys

    def test_valid_probability_ranges(self):
        """All probabilities should be in (0, 1)."""
        samples = self.rng.normal(20.0, 5.0, size=5000)
        result = self.blender.blend_prediction(
            samples=samples, line=20.5, over_odds=-110, under_odds=-110,
        )
        for key in ["market_over", "market_under", "model_over", "model_under",
                     "posterior_over", "posterior_under"]:
            assert 0.0 < result[key] < 1.0, f"{key}={result[key]} out of range"

    def test_confidence_range(self):
        """Confidence should be in [0, 1]."""
        samples = self.rng.normal(20.0, 5.0, size=5000)
        result = self.blender.blend_prediction(
            samples=samples, line=20.5, over_odds=-110, under_odds=-110,
        )
        assert 0.0 <= result["confidence"] <= 1.0

    def test_market_sums_to_one(self):
        """Devigged market probs should sum to 1.0."""
        samples = self.rng.normal(20.0, 5.0, size=5000)
        result = self.blender.blend_prediction(
            samples=samples, line=20.5, over_odds=-110, under_odds=-110,
        )
        assert result["market_over"] + result["market_under"] == pytest.approx(1.0, abs=1e-6)

    def test_centered_samples_posterior_near_market(self):
        """When samples are centered on the line, posterior ≈ market."""
        samples = self.rng.normal(20.5, 5.0, size=10000)  # centered on line
        result = self.blender.blend_prediction(
            samples=samples, line=20.5, over_odds=-110, under_odds=-110,
        )
        # Confidence should be very low (line at center)
        assert result["confidence"] < 0.1
        # Posterior should be very close to market
        assert result["posterior_over"] == pytest.approx(result["market_over"], abs=0.02)

    def test_shifted_samples_posterior_deviates(self):
        """When samples are far from line, posterior should shift toward model."""
        samples = self.rng.normal(30.0, 5.0, size=10000)  # 2 std above line=20
        result = self.blender.blend_prediction(
            samples=samples, line=20.0, over_odds=-110, under_odds=-110,
        )
        # Model should strongly favor over
        assert result["model_over"] > 0.95
        # Confidence should be high
        assert result["confidence"] > 0.8
        # Posterior should be above market (shifted toward model)
        assert result["posterior_over"] > result["market_over"]

    def test_realistic_nba_scenario(self):
        """Realistic NBA pts scenario: player averages 22 pts, line at 21.5."""
        samples = self.rng.normal(22.0, 6.0, size=10000)  # typical pts distribution
        result = self.blender.blend_prediction(
            samples=samples, line=21.5, over_odds=-115, under_odds=-105,
        )
        # Model slightly favors over (mean > line)
        assert result["model_over"] > 0.50
        # But with tau=0.10 and moderate confidence, posterior stays near market
        assert abs(result["posterior_over"] - result["market_over"]) < 0.05


class TestBLConfig:
    """Tests for BLConfig defaults and customization."""

    def test_default_config(self):
        """Default config values."""
        config = BLConfig()
        assert config.tau == 0.05
        assert config.max_weight == 0.50
        assert config.min_prob == 0.01
        assert config.max_prob == 0.99
        assert config.z_max == 1.0

    def test_custom_config(self):
        """Custom config values propagate correctly."""
        config = BLConfig(tau=0.20, max_weight=0.30, z_max=2.0)
        blender = BlackLittermanBlender(config)
        assert blender.config.tau == 0.20
        assert blender.config.max_weight == 0.30
        assert blender.config.z_max == 2.0

    def test_none_config_uses_defaults(self):
        """Passing None creates default config."""
        blender = BlackLittermanBlender(None)
        assert blender.config.tau == 0.05
