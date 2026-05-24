"""Shared default policies for MLB feature source loaders."""

from __future__ import annotations

PARK_FACTOR_DEFAULTS = {
    "park_so_factor": 1.0,
    "park_hits_factor": 1.0,
    "park_hr_factor": 1.0,
    "park_runs_factor": 1.0,
}
WEATHER_DEFAULTS = {"air_density_idx": 1.0, "wind_out_mph": 0.0, "has_precip": 0.0}
GAME_TOTAL_DEFAULT = 0.0
UMPIRE_DEFAULTS = {"umpire_avg_k_per_game_l20": 0.0}


def default_park_factors() -> dict[str, float]:
    return dict(PARK_FACTOR_DEFAULTS)


def default_weather_features() -> dict[str, float]:
    return dict(WEATHER_DEFAULTS)


def default_umpire_features() -> dict[str, float]:
    return dict(UMPIRE_DEFAULTS)
