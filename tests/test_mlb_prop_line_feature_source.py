"""Tests for MLB prop-line feature source query ownership."""

from __future__ import annotations

from datetime import datetime, timezone

from src.models.mlb.features.prop_line_feature_source import (
    build_lateral_prop_line_join,
    build_single_prop_line_query,
)
from src.models.mlb.features.temporal_contracts import FeatureAsOfPolicy


def _sql(query) -> str:
    return str(query.sql)


def test_single_prop_line_query_applies_as_of_and_pre_commence_guards():
    as_of = datetime(2026, 5, 23, 17, 0, tzinfo=timezone.utc)
    query = build_single_prop_line_query(
        player_id=1,
        game_id=2,
        market_key="batter_hits",
        as_of_time=as_of,
    )

    sql = _sql(query)
    assert "market_last_update <= :as_of_time" in sql
    assert "market_last_update < commence_time" in sql
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in sql
    assert "ORDER BY market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in sql
    assert query.params == {"player_id": 1, "game_id": 2, "market_key": "batter_hits", "as_of_time": as_of}
    assert query.policy is FeatureAsOfPolicy.AS_OF_DECISION_TIME


def test_single_prop_line_query_preserves_legacy_none_as_of_behavior_explicitly():
    query = build_single_prop_line_query(
        player_id=1,
        game_id=2,
        market_key="pitcher_strikeouts",
        as_of_time=None,
    )

    assert ":as_of_time IS NULL" in _sql(query)
    assert query.params["as_of_time"] is None
    assert query.policy is FeatureAsOfPolicy.LEGACY_LATEST


def test_lateral_prop_line_join_applies_as_of_and_pre_commence_guards():
    sql = build_lateral_prop_line_join(
        row_alias="bgs",
        market_key_sql=":market_key",
    )

    assert "LEFT JOIN LATERAL" in sql
    assert "FROM mlb_raw_player_props" in sql
    assert "player_id = bgs.player_id" in sql
    assert "game_id = bgs.game_id" in sql
    assert "market_key = :market_key" in sql
    assert "bookmaker IN ('pinnacle', 'draftkings')" in sql
    assert "market_last_update <= :as_of_time" in sql
    assert "market_last_update < commence_time" in sql
    assert "COALESCE(snapshot_time, inserted_at) < commence_time" in sql
    assert "ORDER BY market_key, market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST" in sql
    assert sql.rstrip().endswith(") props ON TRUE")


def test_lateral_prop_line_join_supports_literal_pitcher_market_key():
    sql = build_lateral_prop_line_join(
        row_alias="pgs",
        market_key_sql="'pitcher_strikeouts'",
    )

    assert "player_id = pgs.player_id" in sql
    assert "game_id = pgs.game_id" in sql
    assert "market_key = 'pitcher_strikeouts'" in sql
