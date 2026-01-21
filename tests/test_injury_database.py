"""
Tests for injury_database module.
"""

import importlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

# Add src/scrapers to path for imports
SCRAPERS_PATH = Path(__file__).parent.parent / "src" / "scrapers"
sys.path.insert(0, str(SCRAPERS_PATH))

from espn_injury_scraper import InjuryRecord


class FakeResult:
    def __init__(self, data=None, count=None):
        self.data = data or []
        self.count = count


def build_table_mock():
    table = Mock()
    table.select.return_value = table
    table.eq.return_value = table
    table.order.return_value = table
    table.limit.return_value = table
    table.insert.return_value = table
    table.gte.return_value = table
    table.lte.return_value = table
    table.delete.return_value = table
    table.lt.return_value = table
    return table


@pytest.fixture
def module(monkeypatch):
    supabase_stub = SimpleNamespace(Client=object, create_client=lambda url, key: Mock())
    monkeypatch.setitem(sys.modules, "supabase", supabase_stub)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    if "injury_database" in sys.modules:
        del sys.modules["injury_database"]
    return importlib.import_module("injury_database")


@pytest.fixture
def sample_injury():
    return InjuryRecord(
        espn_injury_id="123",
        espn_player_id="456",
        player_name="Player One",
        team_id="1",
        team_name="Team A",
        status="out",
        injury_type="ankle",
        injury_location="left",
        injury_side="left",
        injury_detail="sprain",
        return_date="2025-01-20",
        date_reported="2025-01-15",
        short_comment="Out for a week",
        long_comment="Injured during practice",
        fantasy_status="OUT",
        scrape_timestamp="2025-01-15T12:00:00Z",
    )


def test_init_requires_credentials(module, monkeypatch):
    monkeypatch.delenv("SUPABASE_URL")
    monkeypatch.delenv("SUPABASE_KEY")
    create_mock = Mock()
    monkeypatch.setattr(module, "create_client", create_mock)

    with pytest.raises(ValueError):
        module.InjuryDatabase()

    create_mock.assert_not_called()


def test_store_injuries_empty_returns_zero(module):
    db = module.InjuryDatabase("url", "key")

    inserted, updated = db.store_injuries([])

    assert inserted == 0
    assert updated == 0


def test_store_injuries_inserts_new_record(module, monkeypatch, sample_injury):
    table = build_table_mock()
    table.execute.side_effect = [FakeResult([]), FakeResult([]), FakeResult([{"id": 1}])]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    inserted, updated = db.store_injuries([sample_injury])

    assert inserted == 1
    assert updated == 0
    table.insert.assert_called_once_with(sample_injury.to_dict())


def test_store_injuries_updates_on_change(module, monkeypatch, sample_injury):
    table = build_table_mock()
    old_record = sample_injury.to_dict()
    old_record["status"] = "questionable"
    table.execute.side_effect = [FakeResult([]), FakeResult([old_record]), FakeResult([{"id": 2}])]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    inserted, updated = db.store_injuries([sample_injury])

    assert inserted == 0
    assert updated == 1
    table.insert.assert_called_once()


def test_store_injuries_skips_duplicate(module, monkeypatch, sample_injury):
    table = build_table_mock()
    table.execute.side_effect = [FakeResult([{"id": 1}])]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    inserted, updated = db.store_injuries([sample_injury])

    assert inserted == 0
    assert updated == 0
    table.insert.assert_not_called()


def test_store_injuries_no_change_skips_update(module, monkeypatch, sample_injury):
    table = build_table_mock()
    table.execute.side_effect = [FakeResult([]), FakeResult([sample_injury.to_dict()])]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    inserted, updated = db.store_injuries([sample_injury])

    assert inserted == 0
    assert updated == 0
    table.insert.assert_not_called()


def test_has_changed_detects_field_updates(module):
    db = module.InjuryDatabase("url", "key")
    old = {"status": "out", "return_date": "2025-01-20"}
    new = {"status": "questionable", "return_date": "2025-01-20"}

    assert db._has_changed(old, new) is True


def test_has_changed_no_updates(module):
    db = module.InjuryDatabase("url", "key")
    old = {"status": "out", "return_date": "2025-01-20"}
    new = {"status": "out", "return_date": "2025-01-20"}

    assert db._has_changed(old, new) is False


def test_get_latest_injuries_no_records(module, monkeypatch):
    table = build_table_mock()
    table.execute.return_value = FakeResult([])
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    result = db.get_latest_injuries()

    assert result == []
    assert table.execute.call_count == 1


def test_get_latest_injuries_returns_latest(module, monkeypatch):
    table = build_table_mock()
    table.execute.side_effect = [
        FakeResult([{"scrape_timestamp": "2025-01-15T12:00:00Z"}]),
        FakeResult([{"espn_injury_id": "1"}]),
    ]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    result = db.get_latest_injuries()

    assert result == [{"espn_injury_id": "1"}]


def test_get_injuries_for_date_filters_day(module, monkeypatch):
    table = build_table_mock()
    table.execute.return_value = FakeResult([{"espn_injury_id": "1"}])
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    target_date = datetime(2025, 1, 15, 12, 30)
    result = db.get_injuries_for_date(target_date)

    start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    table.gte.assert_called_once_with("scrape_timestamp", start_of_day.isoformat())
    table.lte.assert_called_once_with("scrape_timestamp", end_of_day.isoformat())
    assert result == [{"espn_injury_id": "1"}]


def test_get_active_injuries_for_team_filters_status(module, monkeypatch):
    db = module.InjuryDatabase("url", "key")
    active = [
        {"team_id": "1", "status": "out"},
        {"team_id": "1", "status": "questionable"},
        {"team_id": "1", "status": "active"},
        {"team_id": "2", "status": "out"},
    ]
    monkeypatch.setattr(db, "get_injuries_for_date", Mock(return_value=active))

    result = db.get_active_injuries_for_team("1", as_of_date=datetime.now(UTC))

    assert len(result) == 2
    assert all(item["team_id"] == "1" for item in result)
    assert all(item["status"] in {"out", "questionable"} for item in result)


def test_get_injury_history_uses_cutoff(module, monkeypatch):
    table = build_table_mock()
    table.execute.return_value = FakeResult([{"espn_injury_id": "1"}])
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    fixed_now = datetime(2025, 1, 20, tzinfo=UTC)

    class FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(module, "datetime", FixedDateTime)

    result = db.get_injury_history("456", days_back=10)

    cutoff = (fixed_now - timedelta(days=10)).isoformat()
    table.gte.assert_called_once_with("scrape_timestamp", cutoff)
    assert result == [{"espn_injury_id": "1"}]


def test_get_scrape_stats_calculates_counts(module, monkeypatch):
    table = build_table_mock()
    scrape_data = [
        {"scrape_timestamp": "2025-01-01T00:00:00Z"},
        {"scrape_timestamp": "2025-01-01T00:00:00Z"},
        {"scrape_timestamp": "2025-01-02T00:00:00Z"},
    ]
    table.execute.side_effect = [
        FakeResult(scrape_data),
        FakeResult([], count=3),
        FakeResult([], count=1),
    ]
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    fixed_now = datetime(2025, 1, 10, tzinfo=UTC)

    class FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(module, "datetime", FixedDateTime)

    stats = db.get_scrape_stats(days_back=7)

    assert stats["total_scrapes"] == 2
    assert stats["total_records"] == 3
    assert stats["avg_injuries_per_scrape"] == 2
    assert stats["min_injuries"] == 1
    assert stats["max_injuries"] == 3
    assert stats["period_days"] == 7


def test_cleanup_old_data_returns_deleted_count(module, monkeypatch):
    table = build_table_mock()
    table.execute.return_value = FakeResult([{"id": 1}, {"id": 2}])
    client = Mock()
    client.table.return_value = table
    monkeypatch.setattr(module, "create_client", Mock(return_value=client))

    db = module.InjuryDatabase("url", "key")
    deleted = db.cleanup_old_data(days_to_keep=30)

    assert deleted == 2
