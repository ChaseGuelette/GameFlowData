"""
Tests for update_league_position_averages module.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

# Add src to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))


@pytest.fixture
def module(monkeypatch):
    mock_client = Mock()
    monkeypatch.setitem(sys.modules, "src.db.client", mock_client)
    monkeypatch.setitem(sys.modules, "src.db", Mock())
    if "scrapers.update_league_position_averages" in sys.modules:
        del sys.modules["scrapers.update_league_position_averages"]
    return importlib.import_module("scrapers.update_league_position_averages")


def test_normalize_season_id_formats(module):
    assert module.normalize_season_id("2024-25") == "22024"
    assert module.normalize_season_id("22024") == "22024"
    assert module.normalize_season_id("2024") == "22024"
    assert module.normalize_season_id(" 2024-25 ") == "22024"
    assert module.normalize_season_id("playoffs") == "playoffs"


def test_update_league_averages_executes_query(module):
    engine = Mock()
    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine.begin.return_value = begin_cm

    module.update_league_averages(engine, ["2024-25", "22023"])

    engine.begin.assert_called_once()
    conn.execute.assert_called_once()

    call_args = conn.execute.call_args
    query = str(call_args[0][0])
    params = call_args[1] if call_args[1] else call_args[0][1]

    assert params["seasons"] == ("22024", "22023")
    assert "league_position_averages" in query
    assert "player_game_stats" in query
    assert "player_position_history" in query
    assert "ON CONFLICT" in query


def test_main_with_season_calls_update(module, monkeypatch):
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    conn.execute.return_value = [("22024", 3, 10)]

    update_mock = Mock()
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "update_league_averages", update_mock)
    monkeypatch.setattr(sys, "argv", ["prog", "--season", "2024-25"])

    module.main()

    update_mock.assert_called_once_with(engine, ["2024-25"])


def test_main_default_seasons(module, monkeypatch):
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    conn.execute.return_value = [("22024", 3, 10)]

    update_mock = Mock()
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "update_league_averages", update_mock)
    monkeypatch.setattr(sys, "argv", ["prog"])

    module.main()

    update_mock.assert_called_once_with(engine, module.DEFAULT_SEASONS)
