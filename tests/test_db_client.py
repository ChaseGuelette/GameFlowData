"""
Tests for db.client module.
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, Mock

import pytest

# Ensure repo root is on path for src package
ROOT_PATH = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_PATH))


def import_client(monkeypatch, database_url):
    sqlalchemy_stub = ModuleType("sqlalchemy")
    engine = Mock()
    sqlalchemy_stub.create_engine = Mock(return_value=engine)
    sqlalchemy_stub.text = lambda sql: sql
    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_stub)

    dotenv_stub = ModuleType("dotenv")
    dotenv_stub.load_dotenv = Mock()
    monkeypatch.setitem(sys.modules, "dotenv", dotenv_stub)

    if database_url is None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("DATABASE_URL", database_url)

    monkeypatch.delitem(sys.modules, "src.db.client", raising=False)
    monkeypatch.delitem(sys.modules, "src.db", raising=False)

    module = importlib.import_module("src.db.client")
    monkeypatch.setitem(sys.modules, "src.db.client", module)
    return module, engine


def test_import_raises_without_database_url(monkeypatch):
    with pytest.raises(ValueError, match="DATABASE_URL not found"):
        import_client(monkeypatch, None)


def test_get_engine_returns_singleton(monkeypatch):
    module, engine = import_client(monkeypatch, "postgresql://test")

    assert module.get_engine() is engine


def test_verify_connection_success(monkeypatch):
    module, engine = import_client(monkeypatch, "postgresql://test")

    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    result = module.verify_connection()

    assert result is True
    conn.execute.assert_called_once_with("SELECT 1")


def test_verify_connection_failure(monkeypatch):
    module, engine = import_client(monkeypatch, "postgresql://test")
    engine.connect.side_effect = Exception("boom")

    result = module.verify_connection()

    assert result is False
