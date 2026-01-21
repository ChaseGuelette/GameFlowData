"""
Tests for update_player_position_history module.
"""

import importlib
import sys
from datetime import date as real_date
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
    if "scrapers.update_player_position_history" in sys.modules:
        del sys.modules["scrapers.update_player_position_history"]
    return importlib.import_module("scrapers.update_player_position_history")


def test_update_snapshot_executes_query_with_date(module):
    engine = Mock()
    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine.begin.return_value = begin_cm

    snapshot_date = real_date(2025, 1, 15)
    module.update_snapshot(engine, snapshot_date)

    engine.begin.assert_called_once()
    conn.execute.assert_called_once()

    call_args = conn.execute.call_args
    query = str(call_args[0][0])
    params = call_args[1] if call_args[1] else call_args[0][1]

    assert params["snap_date"] == "2025-01-15"
    assert "player_position_history" in query
    assert "ranked_positions" in query
    assert "CASE pc.position" in query


def test_backfill_all_snapshots_executes_query(module):
    engine = Mock()
    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine.begin.return_value = begin_cm

    module.backfill_all_snapshots(engine)

    engine.begin.assert_called_once()
    conn.execute.assert_called_once()

    query = str(conn.execute.call_args[0][0])
    assert "generate_series(2018, 2026)" in query
    assert "MAKE_DATE" in query
    assert "player_windows" in query
    assert "ON CONFLICT" in query


def test_main_backfill_calls_backfill(module, monkeypatch):
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    result = Mock()
    result.fetchone.return_value = (10, 2)
    conn.execute.return_value = result

    backfill_mock = Mock()
    update_mock = Mock()

    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "backfill_all_snapshots", backfill_mock)
    monkeypatch.setattr(module, "update_snapshot", update_mock)
    monkeypatch.setattr(sys, "argv", ["prog", "--backfill"])

    module.main()

    backfill_mock.assert_called_once_with(engine)
    update_mock.assert_not_called()


def test_main_with_date_calls_update(module, monkeypatch):
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    result = Mock()
    result.fetchone.return_value = (5, 1)
    conn.execute.return_value = result

    update_mock = Mock()
    backfill_mock = Mock()

    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "update_snapshot", update_mock)
    monkeypatch.setattr(module, "backfill_all_snapshots", backfill_mock)
    monkeypatch.setattr(sys, "argv", ["prog", "--date", "2025-01-15"])

    module.main()

    update_mock.assert_called_once()
    args = update_mock.call_args[0]
    assert args[0] is engine
    assert args[1].isoformat() == "2025-01-15"
    backfill_mock.assert_not_called()


def test_main_default_date_uses_today(module, monkeypatch):
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    result = Mock()
    result.fetchone.return_value = (1, 1)
    conn.execute.return_value = result

    update_mock = Mock()
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "update_snapshot", update_mock)
    monkeypatch.setattr(module, "backfill_all_snapshots", Mock())

    fake_date = Mock()
    fake_date.today.return_value = real_date(2025, 2, 1)
    monkeypatch.setattr(module, "date", fake_date)
    monkeypatch.setattr(sys, "argv", ["prog"])

    module.main()

    update_mock.assert_called_once()
    args = update_mock.call_args[0]
    assert args[1] == real_date(2025, 2, 1)
