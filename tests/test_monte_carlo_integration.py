"""
Integration tests for MonteCarloPredictor.

These tests verify the full prediction flow works end-to-end,
including stat-specific feature routing and quantile sampling.
"""

import pandas as pd
import pytest

from src.models.monte_carlo import MonteCarloPredictor, PropPrediction


class FakeQuantileModel:
    """Minimal model that returns fixed quantile predictions."""

    def __init__(self, base_value: float):
        self.base_value = base_value

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        """Return monotonically increasing quantiles around base_value."""
        n = len(X)
        return pd.DataFrame(
            {
                "q10": [self.base_value * 0.6] * n,
                "q25": [self.base_value * 0.8] * n,
                "q50": [self.base_value * 1.0] * n,
                "q75": [self.base_value * 1.2] * n,
                "q90": [self.base_value * 1.4] * n,
            }
        )


class FakePipeline:
    """Minimal pipeline with minutes + rate models and stat-specific features."""

    def __init__(self):
        self.minutes_model = FakeQuantileModel(base_value=30.0)  # ~30 min
        self.minutes_features = ["player_avg_min_l5", "player_avg_min_l15", "is_home"]

        self.rate_models = {
            "pts": FakeQuantileModel(base_value=0.7),  # ~0.7 pts/min
            "reb": FakeQuantileModel(base_value=0.25),  # ~0.25 reb/min
            "ast": FakeQuantileModel(base_value=0.2),  # ~0.2 ast/min
        }

        # Each stat has DIFFERENT features — this is what the bug was about
        self.rate_features = {
            "pts": ["player_avg_pts_l5", "player_avg_usg_pct_l5", "opp_avg_def_rtg_l5"],
            "reb": ["player_avg_reb_l5", "player_avg_reb_pct_l5", "opp_pos_reb_allowed_l5"],
            "ast": ["player_avg_ast_l5", "player_avg_ast_pct_l5", "opp_pos_ast_allowed_l5"],
        }


@pytest.fixture
def predictor():
    pipeline = FakePipeline()
    return MonteCarloPredictor(pipeline, n_samples=1000, random_state=42)


@pytest.fixture
def sample_features():
    """Feature dict as returned by FeatureStore.get_player_game_features."""
    return {
        # Minutes features
        "player_avg_min_l5": 32.0,
        "player_avg_min_l15": 31.5,
        "is_home": 1,
        # PTS features
        "player_avg_pts_l5": 25.0,
        "player_avg_usg_pct_l5": 0.28,
        "opp_avg_def_rtg_l5": 112.0,
        # REB features
        "player_avg_reb_l5": 8.0,
        "player_avg_reb_pct_l5": 0.15,
        "opp_pos_reb_allowed_l5": 7.5,
        # AST features
        "player_avg_ast_l5": 6.0,
        "player_avg_ast_pct_l5": 0.30,
        "opp_pos_ast_allowed_l5": 5.8,
    }


def test_predict_returns_all_requested_stats(predictor, sample_features):
    """Predict returns a PropPrediction for each requested stat."""
    result = predictor.predict(
        player_id=123, game_id="0022400001", features=sample_features, stats=["pts", "reb", "ast"]
    )

    assert set(result.keys()) == {"pts", "reb", "ast"}
    for stat, pred in result.items():
        assert isinstance(pred, PropPrediction)
        assert pred.player_id == 123
        assert pred.game_id == "0022400001"
        assert pred.stat == stat


def test_predictions_are_positive(predictor, sample_features):
    """All quantile predictions should be non-negative."""
    result = predictor.predict(
        player_id=123, game_id="0022400001", features=sample_features, stats=["pts", "reb", "ast"]
    )

    for stat, pred in result.items():
        assert pred.q10 >= 0, f"{stat} q10 is negative: {pred.q10}"
        assert pred.mean >= 0, f"{stat} mean is negative: {pred.mean}"
        assert pred.samples.min() >= 0, f"{stat} has negative samples"


def test_quantiles_are_monotonic(predictor, sample_features):
    """Quantile ordering: q10 <= q25 <= q50 <= q75 <= q90."""
    result = predictor.predict(player_id=123, game_id="0022400001", features=sample_features, stats=["pts"])

    pred = result["pts"]
    assert pred.q10 <= pred.q25
    assert pred.q25 <= pred.q50
    assert pred.q50 <= pred.q75
    assert pred.q75 <= pred.q90


def test_stat_specific_features_are_used(sample_features):
    """Each stat model receives only its own features, not the full dict keys."""
    calls_log = {}

    class LoggingModel:
        def __init__(self, stat_name):
            self.stat_name = stat_name

        def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
            calls_log[self.stat_name] = list(X.columns)
            n = len(X)
            return pd.DataFrame(
                {
                    "q10": [5.0] * n,
                    "q25": [10.0] * n,
                    "q50": [15.0] * n,
                    "q75": [20.0] * n,
                    "q90": [25.0] * n,
                }
            )

    pipeline = FakePipeline()
    pipeline.minutes_model = LoggingModel("minutes")
    pipeline.rate_models = {
        "pts": LoggingModel("pts"),
        "reb": LoggingModel("reb"),
    }

    predictor = MonteCarloPredictor(pipeline, n_samples=100)
    predictor.predict(player_id=1, game_id="g1", features=sample_features, stats=["pts", "reb"])

    # Minutes model should get minutes features
    assert calls_log["minutes"] == ["player_avg_min_l5", "player_avg_min_l15", "is_home"]

    # Each rate model should get its OWN features, not the dict keys
    assert calls_log["pts"] == ["player_avg_pts_l5", "player_avg_usg_pct_l5", "opp_avg_def_rtg_l5"]
    assert calls_log["reb"] == ["player_avg_reb_l5", "player_avg_reb_pct_l5", "opp_pos_reb_allowed_l5"]


def test_prob_over_under(predictor, sample_features):
    """PropPrediction.prob_over and prob_under should sum to ~1.0."""
    result = predictor.predict(player_id=123, game_id="0022400001", features=sample_features, stats=["pts"])

    pred = result["pts"]
    line = pred.median  # Use median as line

    prob_over = pred.prob_over(line)
    prob_under = pred.prob_under(line)

    # They should sum to approximately 1 (not exactly due to == cases)
    assert 0.95 <= prob_over + prob_under <= 1.05


def test_expected_value_calculation(predictor, sample_features):
    """EV calculation should be mathematically consistent."""
    result = predictor.predict(player_id=123, game_id="0022400001", features=sample_features, stats=["pts"])

    pred = result["pts"]

    # At -110 odds (standard juice), EV should be:
    # prob * (100/110) - (1-prob) * 1
    prob = pred.prob_over(pred.median)
    expected_ev = prob * (100 / 110) - (1 - prob)
    actual_ev = pred.expected_value_over(pred.median, -110)

    assert abs(actual_ev - expected_ev) < 0.001


def test_missing_features_default_to_zero(predictor):
    """Features not in the dict should default to 0, not crash."""
    sparse_features = {"player_avg_min_l5": 28.0}  # Missing most features

    # Should not raise
    result = predictor.predict(player_id=123, game_id="0022400001", features=sparse_features, stats=["pts"])

    assert "pts" in result
    assert result["pts"].mean >= 0


def test_batch_predict(predictor, sample_features):
    """batch_predict should return a DataFrame with all player-stat combinations."""
    player_games = [
        (101, "g1", sample_features),
        (102, "g2", sample_features),
    ]

    result = predictor.batch_predict(player_games, stats=["pts", "reb"])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4  # 2 players x 2 stats
    assert set(result.columns) >= {"player_id", "game_id", "stat", "mean", "median", "q10", "q90"}
