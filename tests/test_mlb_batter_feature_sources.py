from src.models.mlb.features.batter_sources import (
    build_batter_rolling_stats_query,
    default_batter_source_features,
    market_key_for_stat,
    target_for_stat,
)


def test_batter_rolling_query_uses_strict_previous_game_predicate():
    query = build_batter_rolling_stats_query(player_id=22, target_game_date="2026-05-23")
    sql = str(query.sql)
    assert "game_date < :target_game_date" in sql
    assert "ORDER BY game_date DESC" in sql
    assert query.params == {"player_id": 22, "target_game_date": "2026-05-23"}


def test_batter_stat_and_market_mappings_preserved():
    assert target_for_stat("hits") == "h"
    assert target_for_stat("hrr") == "h + bgs.r + bgs.rbi"
    assert market_key_for_stat("hits") == "batter_hits"


def test_batter_source_defaults_preserved_shape():
    defaults = default_batter_source_features()
    assert defaults["batter_avg_ab_l5"] == 3.5
    assert defaults["is_same_hand"] == 0.0
