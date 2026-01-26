"""
Tests for populate_average_stats module.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

# Add src to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))


@pytest.fixture
def module(monkeypatch):
    mock_client = Mock()
    monkeypatch.setitem(sys.modules, "src.db.client", mock_client)
    monkeypatch.setitem(sys.modules, "src.db", Mock())
    if "processing.populate_average_stats" in sys.modules:
        del sys.modules["processing.populate_average_stats"]
    return importlib.import_module("processing.populate_average_stats")


def test_calculate_games_in_window_counts(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1],
            "season_id": ["2024", "2024", "2024"],
            "game_date": [
                pd.Timestamp("2024-10-03"),
                pd.Timestamp("2024-10-01"),
                pd.Timestamp("2024-10-02"),
            ],
        }
    )

    result = module.calculate_games_in_window(df, ["player_id", "season_id"])

    assert list(result["game_number"]) == [1, 2, 3]
    assert list(result["games_l5"]) == [0, 1, 2]
    assert list(result["games_l15"]) == [0, 1, 2]
    assert list(result["games_szn"]) == [0, 1, 2]


def test_calculate_rolling_averages_no_leakage(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1],
            "season_id": ["2024", "2024", "2024"],
            "game_date": [
                pd.Timestamp("2024-10-01"),
                pd.Timestamp("2024-10-02"),
                pd.Timestamp("2024-10-03"),
            ],
            "pts": [10, 20, 30],
        }
    )

    result = module.calculate_rolling_averages(df, ["player_id", "season_id"], ["pts"])

    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 10
    assert result["avg_pts_l5"].iloc[2] == 15
    assert result["avg_pts_szn"].iloc[2] == 15


def test_rolling_with_groupby_respects_groups(module):
    series = pd.Series([1, 2, 10, 20])
    groups = pd.Series(["A", "A", "B", "B"])

    rolled = module.rolling_with_groupby(series, groups, window=2)

    assert list(rolled) == [1.0, 1.5, 10.0, 15.0]


def test_calculate_player_basic_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "min": [30, 32],
            "pts": [10, 20],
        }
    )

    result = module.calculate_player_basic_averages(df)

    assert "avg_min_l5" in result.columns
    assert "avg_pts_l5" in result.columns
    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 10


def test_calculate_player_advanced_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "offensive_rating": [100, 110],
            "pace": [98, 101],
        }
    )

    result = module.calculate_player_advanced_averages(df)

    assert "avg_off_rtg_l5" in result.columns
    assert pd.isna(result["avg_off_rtg_l5"].iloc[0])
    assert result["avg_off_rtg_l5"].iloc[1] == 100


def test_calculate_team_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "team_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_pts": [100, 105],
            "offensive_rating": [110, 112],
        }
    )

    result = module.calculate_team_averages(df)

    assert "avg_pts_l5" in result.columns
    assert "avg_off_rtg_l5" in result.columns
    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 100


def test_fetch_player_game_stats_season_filter(module, monkeypatch):
    captured = {}

    def fake_read_sql(query, engine):
        captured["query"] = query
        return pd.DataFrame()

    monkeypatch.setattr(module.pd, "read_sql", fake_read_sql)

    module.fetch_player_game_stats(Mock(), season_id="2024-25")

    assert "season_id = '2024-25'" in captured["query"]


def test_insert_player_basic_averages_truncates_and_batches(module, monkeypatch):
    module.BATCH_SIZE = 1
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "game_id": ["g1", "g2"],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "game_number": [1, 2],
            "games_l5": [0, 1],
            "games_l15": [0, 1],
            "games_szn": [0, 1],
            "avg_min_l5": [1.23456, 2.34567],
            "avg_min_l15": [1.23456, 2.34567],
            "avg_min_szn": [1.23456, 2.34567],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    captured_batches = []

    def fake_to_sql(self, name, conn, **kwargs):
        captured_batches.append(self.copy())

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_player_basic_averages(engine, df)

    assert any("TRUNCATE TABLE player_average_game_stats" in str(call[0][0]) for call in conn.execute.call_args_list)
    assert len(captured_batches) == 2
    assert captured_batches[0]["avg_min_l5"].iloc[0] == 1.2346


def test_insert_player_advanced_averages_inserts(module, monkeypatch):
    module.BATCH_SIZE = 2
    df = pd.DataFrame(
        {
            "player_id": [1],
            "game_id": ["g1"],
            "season_id": ["2024"],
            "game_date": [pd.Timestamp("2024-10-01")],
            "team_id": [10],
            "game_number": [1],
            "games_l5": [0],
            "games_l15": [0],
            "games_szn": [0],
            "avg_off_rtg_l5": [101.12345],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    to_sql_calls = []

    def fake_to_sql(self, name, conn, **kwargs):
        to_sql_calls.append(name)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_player_advanced_averages(engine, df)

    assert "player_average_advanced_stats" in to_sql_calls
    assert any(
        "TRUNCATE TABLE player_average_advanced_stats" in str(call[0][0]) for call in conn.execute.call_args_list
    )


def test_insert_team_averages_inserts(module, monkeypatch):
    module.BATCH_SIZE = 2
    df = pd.DataFrame(
        {
            "team_id": [1],
            "game_id": ["g1"],
            "season_id": ["2024"],
            "game_date": [pd.Timestamp("2024-10-01")],
            "game_number": [1],
            "games_l5": [0],
            "games_l15": [0],
            "games_szn": [0],
            "avg_pts_l5": [99.9999],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    to_sql_calls = []

    def fake_to_sql(self, name, conn, **kwargs):
        to_sql_calls.append(name)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_team_averages(engine, df)

    assert "team_average_game_stats" in to_sql_calls
    assert any("TRUNCATE TABLE team_average_game_stats" in str(call[0][0]) for call in conn.execute.call_args_list)


def test_main_player_only_calls_player_flow(module, monkeypatch):
    engine = Mock()
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))

    player_df = pd.DataFrame({"player_id": [1]})
    fetch_basic = Mock(return_value=player_df)
    calc_basic = Mock(return_value=player_df)
    monkeypatch.setattr(module, "fetch_player_game_stats", fetch_basic)
    monkeypatch.setattr(module, "calculate_player_basic_averages", calc_basic)
    insert_basic = Mock()
    monkeypatch.setattr(module, "insert_player_basic_averages", insert_basic)

    fetch_adv = Mock()
    calc_adv = Mock()
    insert_adv = Mock()
    fetch_team = Mock()
    calc_team = Mock()
    insert_team = Mock()

    monkeypatch.setattr(module, "fetch_player_advanced_stats", fetch_adv)
    monkeypatch.setattr(module, "calculate_player_advanced_averages", calc_adv)
    monkeypatch.setattr(module, "insert_player_advanced_averages", insert_adv)
    monkeypatch.setattr(module, "fetch_team_game_stats", fetch_team)
    monkeypatch.setattr(module, "calculate_team_averages", calc_team)
    monkeypatch.setattr(module, "insert_team_averages", insert_team)

    monkeypatch.setattr(sys, "argv", ["prog", "--table", "player", "--season", "2024-25"])

    module.main()

    fetch_basic.assert_called_once_with(engine, "2024-25", None)
    calc_basic.assert_called_once_with(player_df)
    insert_basic.assert_called_once()
    fetch_adv.assert_not_called()
    fetch_team.assert_not_called()
