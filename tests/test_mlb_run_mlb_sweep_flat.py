"""Tests for MLB sweep flat-stake configuration display."""

from src.backtesting.mlb.sweep_config import SweepConfig


def test_sweep_config_label_shows_flat_stake_instead_of_kelly():
    config = SweepConfig(
        tau=None,
        edge_threshold=0.15,
        kelly_fraction=0.125,
        flat_bet_size=100.0,
    )

    assert config.label == "no_BL | edge=0.15 | flat=$100"
    assert config.to_dict()["flat_bet_size"] == 100.0
