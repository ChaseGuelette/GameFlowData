"""
Tests for injury_scraper_job module.
"""

import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

# Add src/scrapers to path for imports
SCRAPERS_PATH = Path(__file__).parent.parent / "src" / "scrapers"
sys.path.insert(0, str(SCRAPERS_PATH))

from espn_injury_scraper import InjuryRecord


def make_injury_record(
    injury_id: str,
    player_id: str,
    player_name: str,
    team_id: str,
    team_name: str,
    status: str = "out",
):
    return InjuryRecord(
        espn_injury_id=injury_id,
        espn_player_id=player_id,
        player_name=player_name,
        team_id=team_id,
        team_name=team_name,
        status=status,
        injury_type="ankle",
        injury_location="left",
        injury_side="left",
        injury_detail="sprain",
        return_date=None,
        date_reported="2025-01-15",
        short_comment=None,
        long_comment=None,
        fantasy_status=None,
        scrape_timestamp="2025-01-15T12:00:00Z",
    )


@pytest.fixture
def module(monkeypatch):
    supabase_stub = SimpleNamespace(Client=object, create_client=lambda url, key: Mock())
    monkeypatch.setitem(sys.modules, "supabase", supabase_stub)
    if "injury_scraper_job" in sys.modules:
        del sys.modules["injury_scraper_job"]
    return importlib.import_module("injury_scraper_job")


@pytest.fixture
def job_with_mocks(module, monkeypatch, tmp_path):
    scraper_mock = Mock()
    db_mock = Mock()
    change_mock = Mock()

    monkeypatch.setattr(module, "ESPNInjuryScraper", Mock(return_value=scraper_mock))
    monkeypatch.setattr(module, "InjuryDatabase", Mock(return_value=db_mock))
    monkeypatch.setattr(module, "InjuryChangeDetector", Mock(return_value=change_mock))
    monkeypatch.setattr(module.InjuryScraperJob, "setup_logging", lambda self: None)

    job = module.InjuryScraperJob(log_dir=str(tmp_path), alert_email="alerts@example.com")
    return job, scraper_mock, db_mock, change_mock


def test_run_success_with_changes(job_with_mocks):
    job, scraper_mock, db_mock, change_mock = job_with_mocks

    current_injuries = [
        make_injury_record("1", "10", "Player A", "1", "Team A", status="out"),
        make_injury_record("2", "11", "Player B", "1", "Team A", status="questionable"),
        make_injury_record("3", "12", "Player C", "2", "Team B", status="probable"),
    ]

    previous_record = make_injury_record("0", "9", "Player Z", "3", "Team C").to_dict()
    previous_record["id"] = 999

    scraper_mock.scrape.return_value = current_injuries
    db_mock.get_latest_injuries.return_value = [previous_record]
    db_mock.store_injuries.return_value = (2, 1)

    changes = {
        "new": [current_injuries[0]],
        "resolved": [current_injuries[1]],
        "updated": [current_injuries[2]],
    }
    change_mock.detect_changes.return_value = changes

    job.send_change_notification = Mock()

    results = job.run()

    assert results["success"] is True
    assert results["stats"]["total_injuries"] == 3
    assert results["stats"]["inserted"] == 2
    assert results["stats"]["updated"] == 1
    assert results["stats"]["new_injuries"] == 1
    assert results["stats"]["resolved_injuries"] == 1
    assert results["stats"]["status_updated"] == 1
    assert results["stats"]["most_injured_team"]["name"] == "Team A"
    job.send_change_notification.assert_called_once()

    results_files = list(job.log_dir.glob("results_*.json"))
    assert results_files


def test_run_no_injuries_returns_error(job_with_mocks, monkeypatch):
    job, scraper_mock, db_mock, change_mock = job_with_mocks
    scraper_mock.scrape.return_value = []
    db_mock.get_latest_injuries.return_value = []
    job.send_alert = Mock()
    job.save_results = Mock()

    results = job.run()

    assert results["success"] is False
    assert results["errors"]
    job.send_alert.assert_called_once()
    job.save_results.assert_called_once()


def test_run_exception_sends_alert(job_with_mocks):
    job, scraper_mock, db_mock, change_mock = job_with_mocks
    scraper_mock.scrape.side_effect = RuntimeError("boom")
    job.send_alert = Mock()
    job.save_results = Mock()

    results = job.run()

    assert results["success"] is False
    assert results["errors"]
    job.send_alert.assert_called_once()
    job.save_results.assert_called_once()


def test_save_results_writes_file(module, monkeypatch, tmp_path):
    monkeypatch.setattr(module, "ESPNInjuryScraper", Mock(return_value=Mock()))
    monkeypatch.setattr(module, "InjuryDatabase", Mock(return_value=Mock()))
    monkeypatch.setattr(module, "InjuryChangeDetector", Mock(return_value=Mock()))
    monkeypatch.setattr(module.InjuryScraperJob, "setup_logging", lambda self: None)

    job = module.InjuryScraperJob(log_dir=str(tmp_path), alert_email=None)
    results = {"success": True, "stats": {"total_injuries": 0}}

    job.save_results(results)

    files = list(Path(tmp_path).glob("results_*.json"))
    assert len(files) == 1
    content = json.loads(files[0].read_text())
    assert content["success"] is True


def test_send_alert_no_email_returns(job_with_mocks):
    job, scraper_mock, db_mock, change_mock = job_with_mocks
    job.alert_email = None
    job.logger.info = Mock()
    job.logger.debug = Mock()

    job.send_alert("Subject", "Message")

    job.logger.info.assert_not_called()
    job.logger.debug.assert_not_called()


def test_send_alert_with_email_logs(job_with_mocks):
    job, scraper_mock, db_mock, change_mock = job_with_mocks
    job.logger.info = Mock()
    job.logger.debug = Mock()

    job.send_alert("Subject", "Message")

    job.logger.info.assert_called_once()
    job.logger.debug.assert_called_once()


def test_send_change_notification_builds_message(job_with_mocks):
    job, scraper_mock, db_mock, change_mock = job_with_mocks
    job.send_alert = Mock()

    changes = {
        "new": [
            make_injury_record("1", "10", "Player A", "1", "Team A"),
            make_injury_record("2", "11", "Player B", "1", "Team A"),
        ],
        "resolved": [make_injury_record("3", "12", "Player C", "2", "Team B")],
        "updated": [],
    }
    stats = {"total_injuries": 3}

    job.send_change_notification(changes, stats)

    job.send_alert.assert_called_once()
    subject, message = job.send_alert.call_args[0]
    assert subject == "NBA Injury Changes Detected"
    assert "New Injuries (2)" in message
    assert "Resolved Injuries (1)" in message
    assert "Player A" in message


def test_run_scheduled_job_exits(module, monkeypatch):
    job_instance = Mock()
    job_instance.run.return_value = {"success": True}
    monkeypatch.setattr(module, "InjuryScraperJob", Mock(return_value=job_instance))
    exit_mock = Mock()
    monkeypatch.setattr(module.sys, "exit", exit_mock)

    module.run_scheduled_job()

    exit_mock.assert_called_once_with(0)
