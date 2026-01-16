"""
Tests for nba_player_position module.
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

# Add src/scrapers to path for imports
SCRAPERS_PATH = Path(__file__).parent.parent / "src" / "scrapers"
sys.path.insert(0, str(SCRAPERS_PATH))


@pytest.fixture
def module(monkeypatch):
    nba_api = ModuleType("nba_api")
    nba_api_stats = ModuleType("nba_api.stats")
    nba_api_endpoints = ModuleType("nba_api.stats.endpoints")
    commonplayerinfo_module = ModuleType("nba_api.stats.endpoints.commonplayerinfo")

    class DummyCommonPlayerInfo:
        def __init__(self, player_id):
            self.player_id = player_id

        def get_data_frames(self):
            return [pd.DataFrame()]

    commonplayerinfo_module.CommonPlayerInfo = DummyCommonPlayerInfo
    nba_api_endpoints.commonplayerinfo = commonplayerinfo_module
    nba_api_stats.endpoints = nba_api_endpoints
    nba_api.stats = nba_api_stats

    monkeypatch.setitem(sys.modules, "nba_api", nba_api)
    monkeypatch.setitem(sys.modules, "nba_api.stats", nba_api_stats)
    monkeypatch.setitem(sys.modules, "nba_api.stats.endpoints", nba_api_endpoints)
    monkeypatch.setitem(sys.modules, "nba_api.stats.endpoints.commonplayerinfo", commonplayerinfo_module)

    nba_unified_scraper = ModuleType("nba_unified_scraper")
    nba_unified_scraper.get_engine = Mock()
    nba_unified_scraper.rate_limit_delay = Mock()
    monkeypatch.setitem(sys.modules, "nba_unified_scraper", nba_unified_scraper)

    if "nba_player_position" in sys.modules:
        del sys.modules["nba_player_position"]
    return importlib.import_module("nba_player_position")


def test_get_position_group_maps_positions(module):
    assert module.get_position_group(None) is None
    assert module.get_position_group("Guard") == "Guard"
    assert module.get_position_group("Forward-Center") == "Big"
    assert module.get_position_group("Forward") == "Forward"
    assert module.get_position_group("Center") == "Big"
    assert module.get_position_group("Swingman") == "Other"


def test_update_player_positions_updates_db(module, monkeypatch):
    engine = Mock()
    conn = Mock()

    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine.begin.return_value = begin_cm

    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module.pd, "read_sql", Mock(return_value=pd.DataFrame([{"player_id": 123}])))

    player_info = Mock()
    player_info.get_data_frames.return_value = [pd.DataFrame([{"POSITION": "Guard"}])]
    module.commonplayerinfo.CommonPlayerInfo = Mock(return_value=player_info)

    rate_limit_mock = Mock()
    monkeypatch.setattr(module, "rate_limit_delay", rate_limit_mock)

    module.update_player_positions()

    conn.execute.assert_called_once()
    params = conn.execute.call_args[0][1]
    assert params["pos"] == "Guard"
    assert params["grp"] == "Guard"
    assert params["id"] == 123
    rate_limit_mock.assert_called_once_with(1)


def test_update_player_positions_skips_empty_data(module, monkeypatch):
    engine = Mock()
    conn = Mock()

    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine.begin.return_value = begin_cm

    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module.pd, "read_sql", Mock(return_value=pd.DataFrame([{"player_id": 456}])))

    player_info = Mock()
    player_info.get_data_frames.return_value = [pd.DataFrame()]
    module.commonplayerinfo.CommonPlayerInfo = Mock(return_value=player_info)

    rate_limit_mock = Mock()
    monkeypatch.setattr(module, "rate_limit_delay", rate_limit_mock)

    module.update_player_positions()

    engine.begin.assert_not_called()
    rate_limit_mock.assert_called_once_with(1)
