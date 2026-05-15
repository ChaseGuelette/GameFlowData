"""Regression tests for order-invariant MLB Monte Carlo sampling."""

import numpy as np
import pandas as pd

from src.models.mlb.mlb_monte_carlo import MLBNegBinPredictor


class _FakeNegBinModel:
    feature_names = ["feature_a"]
    _uses_exposure = False
    _exposure_col = None

    def sample(self, X, n_samples, rng, exposure=None):
        return rng.poisson(lam=2.0, size=(len(X), n_samples))


def _features(value=1.0):
    return {"feature_a": value}


def _prob_over_one(prediction):
    return float((prediction.samples > 1).mean())


def test_negbin_predictions_are_identical_for_same_player_game_across_repeated_calls():
    predictor = MLBNegBinPredictor(_FakeNegBinModel(), stat="hits", n_samples=256, random_state=123)

    first = predictor.predict(player_id=17, game_id=9001, features=_features())
    second = predictor.predict(player_id=17, game_id=9001, features=_features())

    assert np.array_equal(first.samples, second.samples)
    assert _prob_over_one(first) == _prob_over_one(second)


def test_negbin_predictions_are_order_invariant_for_frozen_feature_rows():
    predictor_a = MLBNegBinPredictor(_FakeNegBinModel(), stat="hits", n_samples=256, random_state=123)
    predictor_b = MLBNegBinPredictor(_FakeNegBinModel(), stat="hits", n_samples=256, random_state=123)
    rows = [
        (17, 9001, _features(1.0)),
        (18, 9002, _features(2.0)),
    ]

    forward = {pid: pred for pid, pred in zip([17, 18], predictor_a.predict_batch(rows))}
    reverse = {pid: pred for pid, pred in zip([18, 17], predictor_b.predict_batch(list(reversed(rows))))}

    assert np.array_equal(forward[17].samples, reverse[17].samples)
    assert np.array_equal(forward[18].samples, reverse[18].samples)
    assert _prob_over_one(forward[17]) == _prob_over_one(reverse[17])
    assert _prob_over_one(forward[18]) == _prob_over_one(reverse[18])
