"""NBA schedule refresh is opt-in and cannot write before schema approval."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from src.orchestration import scheduler


def test_scheduler_refresh_is_off_by_default(monkeypatch):
    monkeypatch.delenv("NBA_SCHEDULE_INGEST_ENABLED", raising=False)
    with patch.object(scheduler, "run_job") as run_job:
        scheduler.run_nba_schedule_ingest()
    run_job.assert_not_called()


def test_scheduler_refresh_invokes_bounded_job_when_enabled(monkeypatch):
    monkeypatch.setenv("NBA_SCHEDULE_INGEST_ENABLED", "true")
    with patch.object(scheduler, "run_job") as run_job:
        scheduler.run_nba_schedule_ingest()
    run_job.assert_called_once_with("nba_schedule_job.py", silent_on_success=True, timeout=180)


def test_nba_schedule_job_requires_opt_in(monkeypatch):
    from src.orchestration import nba_schedule_job

    monkeypatch.setenv("NBA_SCHEDULE_INGEST_ENABLED", "false")
    with patch.object(nba_schedule_job, "ingest_schedule") as ingest:
        nba_schedule_job.main()
    ingest.assert_not_called()


def test_nba_schedule_job_processes_rows_when_enabled(monkeypatch, capsys):
    from src.orchestration import nba_schedule_job

    monkeypatch.setenv("NBA_SCHEDULE_INGEST_ENABLED", "true")
    with patch.object(nba_schedule_job, "ingest_schedule", return_value=1206) as ingest:
        nba_schedule_job.main()
    ingest.assert_called_once_with()
    assert "schedule_rows_processed=1206" in capsys.readouterr().out


@pytest.mark.parametrize("enabled", [False, True])
def test_scheduler_registration_is_gated(monkeypatch, enabled):
    fake_scheduler = MagicMock()
    fake_scheduler.get_jobs.return_value = []
    monkeypatch.setattr(sys, "argv", ["scheduler.py"])
    monkeypatch.setattr(scheduler, "BlockingScheduler", lambda **_kwargs: fake_scheduler)
    monkeypatch.setattr(scheduler, "_validate_environment", lambda: None)
    monkeypatch.setattr(scheduler.signal, "signal", lambda *_args: None)
    monkeypatch.setenv("NBA_SCHEDULE_INGEST_ENABLED", "true" if enabled else "false")

    scheduler.main()

    refreshes = [
        call for call in fake_scheduler.add_job.call_args_list
        if call.kwargs["id"].startswith("nba_schedule_")
    ]
    if enabled:
        assert {call.kwargs["id"] for call in refreshes} == {
            "nba_schedule_945am", "nba_schedule_1205pm", "nba_schedule_405pm"
        }
        assert all(call.args[0] is scheduler.run_nba_schedule_ingest for call in refreshes)
    else:
        assert not refreshes
