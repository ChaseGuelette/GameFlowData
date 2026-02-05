"""
Tests for nba_linker_local module.
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


class DummyResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class DummyConnection:
    def execute(self, query):
        query_str = str(query)
        if "raw_game_lines_staging" in query_str:
            return DummyResult((1, 1))
        if "raw_player_props_combined" in query_str:
            return DummyResult((1, 1))
        return DummyResult((None, None))


@pytest.fixture
def module(monkeypatch):
    mock_client = Mock()
    monkeypatch.setitem(sys.modules, "src.db.client", mock_client)
    monkeypatch.setitem(sys.modules, "src.db", Mock())
    if "processing.nba_linker_local" in sys.modules:
        del sys.modules["processing.nba_linker_local"]
    return importlib.import_module("processing.nba_linker_local")


def test_normalize_team_aliases(module):
    # Updated: normalize_team now returns 3-letter abbreviations for all teams
    assert module.normalize_team("LA Lakers") == "LAL"
    assert module.normalize_team("Los Angeles Lakers") == "LAL"
    assert module.normalize_team("New Jersey Nets") == "BKN"
    assert module.normalize_team("Boston Celtics") == "BOS"
    assert module.normalize_team("ATL") == "ATL"  # Abbreviations pass through
    assert pd.isna(module.normalize_team(pd.NA))


def test_find_closest_game_date(module):
    candidates = [("game1", "2025-01-15"), ("game2", "2025-01-20")]

    game_id, diff = module.find_closest_game_date(candidates, "2025-01-16", max_days=2)
    assert game_id == "game1"
    assert diff == 1

    game_id, diff = module.find_closest_game_date(candidates, "2025-03-01", max_days=2)
    assert game_id is None
    assert diff is None

    game_id, diff = module.find_closest_game_date(candidates, "bad-date")
    assert game_id is None
    assert diff is None


def test_download_tables_writes_csvs(module, monkeypatch, tmp_path):
    module.DATA_DIR = tmp_path

    conn = DummyConnection()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine = Mock()
    engine.connect.return_value = connect_cm
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))

    def fake_read_sql(query, _conn, params=None):
        query_str = str(query)
        if "FROM teams" in query_str:
            return pd.DataFrame([{"team_id": 1, "team_name": "Los Angeles Lakers"}])
        if "FROM players" in query_str:
            return pd.DataFrame([{"player_id": 100, "player_name": "LeBron James"}])
        if "FROM team_game_stats" in query_str:
            return pd.DataFrame(
                [
                    {
                        "game_id": "0022400001",
                        "team_id": 1,
                        "team_name": "Los Angeles Lakers",
                        "team_game_date": "2025-01-15",
                        "team_matchup": "LAL vs. BOS",
                        "opponent_id": 2,
                    }
                ]
            )
        if "FROM player_game_stats" in query_str:
            return pd.DataFrame([{"player_id": 100, "game_id": "0022400001", "team_id": 1}])
        if "raw_game_lines_staging" in query_str:
            return pd.DataFrame(
                [
                    {
                        "staging_id": 1,
                        "home_team": "LA Lakers",
                        "commence_time": "2025-01-15T23:00:00Z",
                        "nba_game_id": pd.NA,
                    }
                ]
            )
        if "raw_player_props_combined" in query_str:
            return pd.DataFrame(
                [
                    {
                        "staging_id": 10,
                        "api_player_name": "LeBron J.",
                        "home_team": "LA Lakers",
                        "away_team": "Boston Celtics",
                        "commence_time": "2025-01-17T00:00:00Z",
                        "game_id": pd.NA,
                        "player_id": pd.NA,
                        "team_id": pd.NA,
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(module.pd, "read_sql", fake_read_sql)

    module.download_tables()

    assert (tmp_path / "teams.csv").exists()
    assert (tmp_path / "players.csv").exists()
    assert (tmp_path / "team_game_stats.csv").exists()
    assert (tmp_path / "player_game_stats.csv").exists()
    assert (tmp_path / "game_lines.csv").exists()
    assert (tmp_path / "player_props.csv").exists()


def test_process_local_matches_and_outputs(module, monkeypatch, tmp_path):
    module.DATA_DIR = tmp_path

    teams = pd.DataFrame(
        [
            {"team_id": 1, "team_name": "Los Angeles Lakers"},
            {"team_id": 2, "team_name": "Boston Celtics"},
        ]
    )
    players = pd.DataFrame(
        [
            {"player_id": 100, "player_name": "LeBron James"},
            {"player_id": 101, "player_name": "Jayson Tatum"},
        ]
    )
    team_game_stats = pd.DataFrame(
        [
            {
                "game_id": "0022400001",
                "team_id": 1,
                "team_name": "Los Angeles Lakers",
                "team_game_date": "2025-01-15",
                "team_matchup": "LAL vs. BOS",
                "opponent_id": 2,
            }
        ]
    )
    player_game_stats = pd.DataFrame([{"player_id": 100, "game_id": "0022400001", "team_id": 1}])
    game_lines = pd.DataFrame(
        [
            {
                "staging_id": 1,
                "home_team": "LA Lakers",
                "commence_time": "2025-01-15T23:00:00Z",
                "nba_game_id": pd.NA,
            }
        ]
    )
    player_props = pd.DataFrame(
        [
            {
                "staging_id": 10,
                "api_player_name": "LeBron J.",
                "home_team": "LA Lakers",
                "away_team": "Boston Celtics",
                "commence_time": "2025-01-17T00:00:00Z",
                "game_id": pd.NA,
                "player_id": pd.NA,
                "team_id": pd.NA,
            },
            {
                "staging_id": 11,
                "api_player_name": "Unknown Guy",
                "home_team": "LA Lakers",
                "away_team": "Boston Celtics",
                "commence_time": "2025-01-17T00:00:00Z",
                "game_id": pd.NA,
                "player_id": pd.NA,
                "team_id": pd.NA,
            },
        ]
    )

    teams.to_csv(tmp_path / "teams.csv", index=False)
    players.to_csv(tmp_path / "players.csv", index=False)
    team_game_stats.to_csv(tmp_path / "team_game_stats.csv", index=False)
    player_game_stats.to_csv(tmp_path / "player_game_stats.csv", index=False)
    game_lines.to_csv(tmp_path / "game_lines.csv", index=False)
    player_props.to_csv(tmp_path / "player_props.csv", index=False)

    pd.DataFrame([{"api_name": "LeBron J.", "player_id": 100}]).to_csv(tmp_path / "player_mappings.csv", index=False)

    module.process_local()

    game_lines_updates = pd.read_csv(tmp_path / "game_lines_updates.csv")
    game_id = str(game_lines_updates.loc[0, "nba_game_id"]).zfill(10)
    assert game_id == "0022400001"

    props_game_updates = pd.read_csv(tmp_path / "props_game_updates.csv")
    assert set(props_game_updates["staging_id"]) == {10, 11}
    prop_game_id = str(props_game_updates["game_id"].iloc[0]).zfill(10)
    assert prop_game_id == "0022400001"

    props_player_updates = pd.read_csv(tmp_path / "props_player_updates.csv")
    assert list(props_player_updates["staging_id"]) == [10]
    assert props_player_updates["player_id"].iloc[0] == 100

    props_full_updates = pd.read_csv(tmp_path / "props_full_updates.csv")
    assert props_full_updates["team_id"].iloc[0] == 1

    unmatched_players_file = tmp_path / "unmatched_players.csv"
    assert unmatched_players_file.exists()
    unmatched_players = pd.read_csv(unmatched_players_file)
    assert "Unknown Guy" in unmatched_players["api_name"].values

    assert not (tmp_path / "unmatched_game_lines.csv").exists()
    assert not (tmp_path / "unmatched_games.csv").exists()


def test_init_mappings_creates_template(module, tmp_path):
    module.DATA_DIR = tmp_path

    module.init_mappings()

    mappings_file = tmp_path / "player_mappings.csv"
    assert mappings_file.exists()
    df = pd.read_csv(mappings_file)
    assert list(df.columns) == ["api_name", "player_id"]


def test_upload_results_runs_updates(module, monkeypatch, tmp_path):
    module.DATA_DIR = tmp_path

    pd.DataFrame(
        [
            {"staging_id": 1, "nba_game_id": "0022400001", "nba_home_team_id": 1, "nba_away_team_id": 2},
            {"staging_id": 2, "nba_game_id": pd.NA, "nba_home_team_id": 1, "nba_away_team_id": 2},
        ]
    ).to_csv(tmp_path / "game_lines_updates.csv", index=False)

    pd.DataFrame([{"staging_id": 10, "game_id": "0022400001"}]).to_csv(tmp_path / "props_game_updates.csv", index=False)

    pd.DataFrame([{"staging_id": 10, "player_id": 100, "team_id": 1}]).to_csv(
        tmp_path / "props_full_updates.csv", index=False
    )

    conn = Mock()
    conn.execute.return_value = Mock(rowcount=1)
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))

    to_sql_calls = []

    def fake_to_sql(self, name, conn, **kwargs):
        to_sql_calls.append((name, len(self)))

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.upload_results()

    assert ("temp_game_lines_updates", 1) in to_sql_calls
    assert ("temp_props_game_updates", 1) in to_sql_calls
    assert ("temp_props_player_updates", 1) in to_sql_calls
