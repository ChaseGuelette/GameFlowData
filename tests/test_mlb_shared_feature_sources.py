from src.models.mlb.features.shared_sources import (
    default_park_factors,
    default_umpire_features,
    default_weather_features,
)


def test_shared_defaults_are_documented_and_copy_safe():
    park = default_park_factors()
    weather = default_weather_features()
    umpire = default_umpire_features()

    assert park["park_so_factor"] == 1.0
    assert weather == {"air_density_idx": 1.0, "wind_out_mph": 0.0, "has_precip": 0.0}
    assert umpire == {"umpire_avg_k_per_game_l20": 0.0}

    park["park_so_factor"] = 2.0
    assert default_park_factors()["park_so_factor"] == 1.0
