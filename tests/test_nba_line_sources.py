"""Tests for NBA betting line source helpers."""

from __future__ import annotations

from types import SimpleNamespace

from src.models.features.nba.line_sources import (
    default_game_lines,
    default_player_prop_lines,
    game_lines_query,
    player_prop_lines_query,
    row_to_game_lines,
    row_to_player_prop_lines,
)


def _sql(query) -> str:
    return str(query).lower()


def test_game_lines_query_keeps_current_temporal_and_source_predicates():
    sql = _sql(game_lines_query())

    assert "from raw_game_lines_staging" in sql
    assert "nba_game_id = :game_id" in sql
    assert "bookmaker in ('pinnacle', 'draftkings')" in sql
    assert ":as_of_date is null" in sql
    assert "coalesce(snapshot_time, inserted_at)::date <= :as_of_date" in sql
    assert "coalesce(snapshot_time, inserted_at) < commence_time" in sql
    assert "market_key = 'spreads'" in sql
    assert "market_key = 'totals'" in sql


def test_player_prop_lines_query_keeps_current_temporal_source_and_distinct_market_predicates():
    sql = _sql(player_prop_lines_query())

    assert "from raw_player_props_combined" in sql
    assert "player_id = :player_id" in sql
    assert "game_id = :game_id" in sql
    assert "bookmaker in ('pinnacle', 'draftkings')" in sql
    assert ":as_of_date is null" in sql
    assert "coalesce(snapshot_time, inserted_at)::date <= :as_of_date" in sql
    assert "coalesce(snapshot_time, inserted_at) < commence_time" in sql
    assert "select distinct on (market_key) market_key, line" in sql
    assert "order by market_key, coalesce(snapshot_time, inserted_at) desc nulls last" in sql
    assert "group by" not in sql


def test_line_source_defaults_preserve_feature_store_contract():
    assert default_game_lines() == {"line_spread_raw": 0, "line_total": 0}
    assert default_player_prop_lines() == {
        "prop_line_pts": 0,
        "prop_line_reb": 0,
        "prop_line_ast": 0,
        "prop_line_threes": 0,
    }


def test_line_source_row_mappers_preserve_zero_fallbacks():
    assert row_to_game_lines(None) == default_game_lines()
    assert row_to_game_lines(SimpleNamespace(spread=None, total=228.5)) == {
        "line_spread_raw": 0,
        "line_total": 228.5,
    }

    assert row_to_player_prop_lines(None) == default_player_prop_lines()
    assert row_to_player_prop_lines(
        SimpleNamespace(
            prop_line_pts=27.5,
            prop_line_reb=None,
            prop_line_ast=7.5,
            prop_line_threes=3.5,
        )
    ) == {
        "prop_line_pts": 27.5,
        "prop_line_reb": 0,
        "prop_line_ast": 7.5,
        "prop_line_threes": 3.5,
    }
