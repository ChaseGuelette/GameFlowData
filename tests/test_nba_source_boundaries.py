"""Tests for NBA player/team/opponent/context source boundaries."""

from __future__ import annotations

from types import SimpleNamespace

from src.models.features.nba.context_sources import (
    context_snapshots_query,
    get_context_snapshots_from_row,
    player_position_from_row,
    player_position_query,
)
from src.models.features.nba.opponent_sources import (
    default_opponent_positional_stats,
    opponent_positional_stats_query,
    row_to_opponent_positional_stats,
)
from src.models.features.nba.player_sources import player_rolling_stats_query
from src.models.features.nba.team_sources import (
    default_team_rolling_stats,
    row_to_team_rolling_stats,
    team_rolling_stats_query,
)


def _sql(query) -> str:
    return str(query).lower()


def test_context_source_queries_preserve_position_and_snapshot_predicates():
    pos_sql = _sql(player_position_query())
    assert "from player_position_history" in pos_sql
    assert "player_id = :player_id" in pos_sql
    assert "snapshot_date < :as_of_date" in pos_sql
    assert "order by snapshot_date desc limit 1" in pos_sql

    ctx_sql = _sql(context_snapshots_query())
    assert "from player_game_stats pgs" in ctx_sql
    assert "join team_game_stats tgs" in ctx_sql
    assert "pgs.game_id = :game_id" in ctx_sql
    assert "pgs.player_id = :player_id" in ctx_sql
    assert "ph.snapshot_date < :as_of_date" in ctx_sql
    assert "case when pgs.matchup like '%vs.%' then 1 else 0 end as is_home" in ctx_sql


def test_context_source_row_mappers_preserve_none_behavior():
    assert player_position_from_row(None) is None
    assert player_position_from_row(("G",)) == "G"

    assert get_context_snapshots_from_row(None) is None
    assert get_context_snapshots_from_row(SimpleNamespace(position_group=None)) is None

    row = SimpleNamespace(team_id=1, season_id="22025", opponent_id=2, is_home=1, position_group="W")
    assert get_context_snapshots_from_row(row) == {
        "team_id": 1,
        "season_id": "22025",
        "opponent_id": 2,
        "is_home": 1,
        "position_group": "W",
    }


def test_player_rolling_source_query_preserves_pre_game_average_predicates():
    sql = _sql(player_rolling_stats_query())

    assert "from player_average_game_stats pags" in sql
    assert "left join lateral" in sql
    assert "from player_average_advanced_stats" in sql
    assert "game_date < :as_of_date" in sql
    assert "pags.player_id = :player_id" in sql
    assert "pags.game_date < :as_of_date" in sql
    assert "order by pags.game_date desc limit 1" in sql
    assert "pags.rest_days as stored_rest_days" in sql
    assert "pags.games_last_7d" in sql


def test_team_source_query_and_defaults_preserve_prefix_behavior():
    sql = _sql(team_rolling_stats_query())
    assert "from team_average_game_stats" in sql
    assert "team_id = :team_id" in sql
    assert "game_date < :as_of_date" in sql
    assert "order by game_date desc limit 1" in sql

    assert default_team_rolling_stats(prefix="team") == {
        "team_avg_pace_l5": 99.5,
        "team_avg_def_rtg_l5": 112.0,
        "team_avg_fg3a_l5": 34.0,
        "team_avg_fg3_pct_l5": 0.36,
    }
    assert default_team_rolling_stats(prefix="opp")["opp_avg_pace_l5"] == 99.5

    row = SimpleNamespace(avg_pace_l5=101.0, avg_def_rtg_l5=None, avg_fg3a_l5=36.5, avg_fg3_pct_l5=None)
    assert row_to_team_rolling_stats(row, prefix="opp") == {
        "opp_avg_pace_l5": 101.0,
        "opp_avg_def_rtg_l5": 112.0,
        "opp_avg_fg3a_l5": 36.5,
        "opp_avg_fg3_pct_l5": 0.36,
    }


def test_opponent_positional_source_query_and_defaults_preserve_contract():
    sql = _sql(opponent_positional_stats_query())
    assert "from team_allowed_by_position" in sql
    assert "team_id = :opponent_id" in sql
    assert "position_group = :position_group" in sql
    assert "game_date < :as_of_date" in sql
    assert "order by game_date desc limit 1" in sql

    assert default_opponent_positional_stats() == {
        "opp_pos_off_rtg_allowed_l5": 112.0,
        "opp_pos_reb_allowed_l5": 0,
        "opp_pos_ast_allowed_l5": 0,
        "opp_pos_threes_allowed_l5": 0,
        "opp_pos_threes_per100_allowed_l5": 0,
        "opp_pos_reb_per100_allowed_l5": 0,
        "opp_pos_ast_per100_allowed_l5": 0,
        "opp_pos_off_rtg_allowed_l15": 112.0,
        "opp_pos_reb_allowed_l15": 0,
        "opp_pos_ast_allowed_l15": 0,
        "opp_pos_threes_allowed_l15": 0,
    }

    row = SimpleNamespace(
        _mapping={
            "off_rtg_allowed_l5": None,
            "reb_allowed_l5": 44.0,
            "ast_allowed_l5": 25.0,
            "threes_allowed_l5": 12.0,
            "threes_per100_allowed_l5": None,
            "reb_per100_allowed_l5": 47.0,
            "ast_per100_allowed_l5": 26.0,
            "off_rtg_allowed_l15": None,
            "reb_allowed_l15": 43.0,
            "ast_allowed_l15": 24.0,
            "threes_allowed_l15": 11.0,
        }
    )
    mapped = row_to_opponent_positional_stats(row)
    assert mapped["opp_pos_off_rtg_allowed_l5"] == 112.0
    assert mapped["opp_pos_off_rtg_allowed_l15"] == 112.0
    assert mapped["opp_pos_reb_allowed_l5"] == 44.0
    assert mapped["opp_pos_threes_per100_allowed_l5"] == 0
