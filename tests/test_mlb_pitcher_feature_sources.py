from src.models.mlb.features.pitcher_sources import (
    build_pitcher_rolling_stats_query,
    default_pitcher_source_features,
)


def test_pitcher_rolling_query_uses_strict_previous_game_predicate():
    query = build_pitcher_rolling_stats_query(player_id=11, target_game_date="2026-05-23")
    sql = str(query.sql)
    assert "game_date < :target_game_date" in sql
    assert "ORDER BY game_date DESC" in sql
    assert query.params == {"player_id": 11, "target_game_date": "2026-05-23"}


def test_pitcher_source_defaults_preserved_shape():
    defaults = default_pitcher_source_features()
    assert defaults["pitcher_avg_so_l5"] == 0.0
    assert defaults["pitcher_avg_ip_l5"] == 0.0
