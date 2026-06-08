"""Tests for NBA injury-context source helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.models.features.nba.injury_context import (
    aggregate_team_injury_features,
    build_injury_context,
    bulk_injury_features_query,
    default_injury_context,
    opponent_injury_query,
    player_injury_status_bulk_query,
    player_injury_status_query,
    same_position_injury_query,
    status_flags,
    team_injury_advanced_query,
    team_injury_game_query,
)


def _sql(query) -> str:
    return str(query).lower()


def test_default_injury_context_preserves_feature_store_contract():
    assert default_injury_context() == {
        "team_out_count": 0,
        "team_out_min_sum": 0,
        "team_out_pts_sum": 0,
        "team_out_reb_sum": 0,
        "team_out_ast_sum": 0,
        "team_out_usg_sum": 0,
        "opp_out_count": 0,
        "opp_out_min_sum": 0,
        "player_is_questionable": 0,
        "player_is_probable": 0,
        "team_out_same_pos_count": 0,
        "team_out_same_pos_min_sum": 0,
        "team_out_same_pos_usg_sum": 0,
        "team_out_same_pos_starter_sum": 0,
    }


def test_status_flags_preserve_questionable_probable_mapping():
    assert status_flags("Questionable") == {"player_is_questionable": 1, "player_is_probable": 0}
    assert status_flags("Probable") == {"player_is_questionable": 0, "player_is_probable": 1}
    assert status_flags("Out") == {"player_is_questionable": 0, "player_is_probable": 0}
    assert status_flags(None) == {"player_is_questionable": 0, "player_is_probable": 0}


def test_build_injury_context_maps_all_single_player_result_rows():
    context = build_injury_context(
        team_game_result=SimpleNamespace(
            out_count=2,
            out_min_sum=55,
            out_pts_sum=31.5,
            out_reb_sum=None,
            out_ast_sum=8,
        ),
        team_advanced_result=SimpleNamespace(out_usg_sum=0.43),
        opponent_result=SimpleNamespace(out_count=1, out_min_sum=22.5),
        player_status_result=SimpleNamespace(status="Questionable"),
        same_position_result=SimpleNamespace(
            out_count=1,
            out_min_sum=28,
            out_usg_sum=0.24,
            out_starter_sum=0.8,
        ),
    )

    assert context == {
        "team_out_count": 2,
        "team_out_min_sum": 55.0,
        "team_out_pts_sum": 31.5,
        "team_out_reb_sum": 0.0,
        "team_out_ast_sum": 8.0,
        "team_out_usg_sum": 0.43,
        "opp_out_count": 1,
        "opp_out_min_sum": 22.5,
        "player_is_questionable": 1,
        "player_is_probable": 0,
        "team_out_same_pos_count": 1,
        "team_out_same_pos_min_sum": 28.0,
        "team_out_same_pos_usg_sum": 0.24,
        "team_out_same_pos_starter_sum": 0.8,
    }


def test_single_player_injury_queries_keep_current_temporal_and_status_predicates():
    team_sql = _sql(team_injury_game_query())
    assert "from rapidapi_injuries ri" in team_sql
    assert "ri.nba_team_id = :team_id" in team_sql
    assert "ri.report_date = :game_date" in team_sql
    assert "ri.status = 'out'" in team_sql
    assert "pags_inj.game_date < :game_date" in team_sql
    assert "select distinct on (ri.player_id)" in team_sql
    assert "order by ri.player_id, pags_inj.game_date desc" in team_sql

    adv_sql = _sql(team_injury_advanced_query())
    assert "from rapidapi_injuries ri" in adv_sql
    assert "paas_inj.game_date < :game_date" in adv_sql
    assert "ri.status = 'out'" in adv_sql

    opp_sql = _sql(opponent_injury_query())
    assert "ri.nba_team_id = :opponent_id" in opp_sql
    assert "ri.report_date = :game_date" in opp_sql
    assert "ri.status = 'out'" in opp_sql

    player_sql = _sql(player_injury_status_query())
    assert "where player_id = :player_id" in player_sql
    assert "and report_date = :game_date" in player_sql
    assert "order by id desc limit 1" in player_sql


def test_same_position_query_keeps_exclusion_position_and_starter_formula():
    sql = _sql(same_position_injury_query())

    assert "ri.player_id != :player_id" in sql
    assert "pph.position_group = :position_group" in sql
    assert "snapshot_date < :game_date" in sql
    assert "least(inj_sub.games_started_l5 / 5.0, 1.0)" in sql
    assert "ri.status = 'out'" in sql


def test_bulk_queries_and_aggregation_preserve_schema_and_out_filter():
    bulk_sql = _sql(bulk_injury_features_query())
    assert "from rapidapi_injuries" in bulk_sql
    assert "where status = 'out'" in bulk_sql
    assert "report_date = any(:dates)" in bulk_sql
    assert "select distinct on (nba_team_id, report_date, player_id)" in bulk_sql

    status_sql = _sql(player_injury_status_bulk_query())
    assert "select distinct on (player_id, report_date)" in status_sql
    assert "report_date = any(:dates)" in status_sql
    assert "order by player_id, report_date, id desc" in status_sql

    empty_agg = aggregate_team_injury_features(pd.DataFrame())
    assert list(empty_agg.columns) == [
        "nba_team_id",
        "report_date",
        "out_count",
        "out_min_sum",
        "out_pts_sum",
        "out_reb_sum",
        "out_ast_sum",
        "out_usg_sum",
    ]

    rows = pd.DataFrame(
        {
            "nba_team_id": [1, 1, 1],
            "report_date": ["2025-01-01", "2025-01-01", "2025-01-01"],
            "inj_player_id": [10, 10, 11],
            "avg_min_l5": [20.0, 20.0, None],
            "avg_pts_l5": [12.0, 12.0, 9.0],
            "avg_reb_l5": [4.0, 4.0, 5.0],
            "avg_ast_l5": [2.0, 2.0, 3.0],
            "avg_usg_pct_l5": [0.2, 0.2, 0.15],
        }
    )
    agg = aggregate_team_injury_features(rows)

    assert agg.loc[0, "out_count"] == 2
    assert agg.loc[0, "out_min_sum"] == 40.0
    assert agg.loc[0, "out_pts_sum"] == 33.0
    assert agg.loc[0, "out_usg_sum"] == 0.55
