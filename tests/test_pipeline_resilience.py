"""Tests for pipeline resilience: dependency checks, retry logic, and job status tracking."""

import subprocess
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest


class TestCheckDependency:
    """Tests for scheduler.check_dependency()."""

    @pytest.fixture(autouse=True)
    def _disable_db_dependency_fallback(self, monkeypatch):
        """Keep these unit tests isolated from live job_executions rows."""
        import src.db.client as db_client

        def fail_get_engine():
            raise RuntimeError("unit test disables DB fallback")

        monkeypatch.setattr(db_client, "get_engine", fail_get_engine)

    def setup_method(self):
        # Import and reset JOB_STATUS before each test
        from src.orchestration.scheduler import JOB_STATUS

        JOB_STATUS.clear()

    def test_returns_false_when_no_status(self):
        from src.orchestration.scheduler import check_dependency

        assert check_dependency("daily_stats_job.py") is False

    def test_returns_false_when_status_is_failed(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "failed",
            "end_time": datetime.now(UTC),
            "duration": 100.0,
        }
        assert check_dependency("daily_stats_job.py") is False

    def test_returns_false_when_status_is_timeout(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "timeout",
            "end_time": datetime.now(UTC),
            "duration": 1800.0,
        }
        assert check_dependency("daily_stats_job.py") is False

    def test_returns_true_when_success_and_recent(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "success",
            "end_time": datetime.now(UTC) - timedelta(hours=1),
            "duration": 300.0,
        }
        assert check_dependency("daily_stats_job.py") is True

    def test_returns_false_when_success_but_too_old(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "success",
            "end_time": datetime.now(UTC) - timedelta(hours=10),
            "duration": 300.0,
        }
        assert check_dependency("daily_stats_job.py", max_age_hours=8) is False

    def test_custom_max_age(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "success",
            "end_time": datetime.now(UTC) - timedelta(hours=3),
            "duration": 300.0,
        }
        assert check_dependency("daily_stats_job.py", max_age_hours=2) is False
        assert check_dependency("daily_stats_job.py", max_age_hours=4) is True

    def test_returns_false_when_end_time_is_none(self):
        from src.orchestration.scheduler import JOB_STATUS, check_dependency

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "success",
            "end_time": None,
            "duration": 300.0,
        }
        assert check_dependency("daily_stats_job.py") is False


class TestRunCommandRetry:
    """Tests for daily_stats_job.run_command() retry behavior."""

    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_no_retry_on_success(self, mock_run):
        from src.orchestration.daily_stats_job import run_command

        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        result = run_command("echo hello", "Test step", max_retries=2)

        assert result is True
        assert mock_run.call_count == 1

    @patch("src.orchestration.daily_stats_job.time.sleep")
    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_retries_on_failure_then_succeeds(self, mock_run, mock_sleep):
        from src.orchestration.daily_stats_job import run_command

        # First call fails, second succeeds
        mock_run.side_effect = [
            subprocess.CalledProcessError(1, "cmd", stderr="error"),
            MagicMock(returncode=0, stdout="OK", stderr=""),
        ]
        result = run_command("echo hello", "Test step", max_retries=1, retry_delay=1)

        assert result is True
        assert mock_run.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 1 * 2^0

    @patch("src.orchestration.daily_stats_job.time.sleep")
    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_retries_exhausted_returns_false(self, mock_run, mock_sleep):
        from src.orchestration.daily_stats_job import run_command

        # All calls fail
        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="error")
        result = run_command("echo hello", "Test step", max_retries=2, retry_delay=1)

        assert result is False
        assert mock_run.call_count == 3  # 1 initial + 2 retries

    @patch("src.orchestration.daily_stats_job.time.sleep")
    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_exponential_backoff_delays(self, mock_run, mock_sleep):
        from src.orchestration.daily_stats_job import run_command

        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="error")
        run_command("echo hello", "Test step", max_retries=2, retry_delay=15)

        # Delays: 15 * 2^0 = 15, 15 * 2^1 = 30
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(15)
        mock_sleep.assert_any_call(30)

    @patch("src.orchestration.daily_stats_job.time.sleep")
    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_retries_on_timeout(self, mock_run, mock_sleep):
        from src.orchestration.daily_stats_job import run_command

        # First call times out, second succeeds
        mock_run.side_effect = [
            subprocess.TimeoutExpired("cmd", 600),
            MagicMock(returncode=0, stdout="OK", stderr=""),
        ]
        result = run_command("echo hello", "Test step", max_retries=1, retry_delay=1)

        assert result is True
        assert mock_run.call_count == 2

    def test_dry_run_skips_execution(self):
        from src.orchestration.daily_stats_job import run_command

        result = run_command("echo hello", "Test step", dry_run=True, max_retries=2)
        assert result is True

    @patch("src.orchestration.daily_stats_job.subprocess.run")
    def test_zero_retries_is_default_behavior(self, mock_run):
        from src.orchestration.daily_stats_job import run_command

        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="error")
        result = run_command("echo hello", "Test step")

        assert result is False
        assert mock_run.call_count == 1  # No retries


class TestJobStatusTracking:
    """Tests for JOB_STATUS updates in run_job()."""

    def setup_method(self):
        from src.orchestration.scheduler import JOB_LOCKS, JOB_STATUS

        JOB_STATUS.clear()
        JOB_LOCKS.clear()

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_success_updates_job_status(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import JOB_STATUS, run_job

        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        run_job("test_job.py")

        assert "test_job.py" in JOB_STATUS
        assert JOB_STATUS["test_job.py"]["status"] == "success"
        assert JOB_STATUS["test_job.py"]["end_time"] is not None

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_failure_updates_job_status(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import JOB_STATUS, run_job

        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
        run_job("test_job.py")

        assert JOB_STATUS["test_job.py"]["status"] == "failed"

    def test_deferred_nba_lines_failure_is_tagged_for_alerts(self):
        from src.orchestration.scheduler import _decorate_deferred_failure, _display_job_name

        decorated = _decorate_deferred_failure("lines_job.py", False, "linker failed")
        assert _display_job_name("lines_job.py", success=False) == "Lines Scraper (NBA deferred)"
        assert decorated is not None and "NBA deferred" in decorated
        assert _decorate_deferred_failure("mlb_lines_job.py", False, "linker failed") == "linker failed"

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_timeout_updates_job_status(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import JOB_STATUS, run_job

        mock_run.side_effect = subprocess.TimeoutExpired("cmd", 2700)
        run_job("test_job.py")

        assert JOB_STATUS["test_job.py"]["status"] == "timeout"

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_timeout_persists_partial_stderr_for_deferred_lines_job(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import run_job

        mock_run.side_effect = subprocess.TimeoutExpired(
            "cmd",
            2700,
            output="partial stdout with 123 props",
            stderr="partial stderr tail",
        )

        run_job("lines_job.py")

        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args.kwargs
        assert call_kwargs["status"] == "timeout"
        assert "NBA deferred" in call_kwargs["error_message"]
        assert "partial stderr tail" in call_kwargs["error_message"]
        assert "Job timed out after 45 minutes" in call_kwargs["error_message"]

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_lines_job_overlap_is_skipped_without_subprocess(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import JOB_STATUS, _get_job_lock, run_job

        lock = _get_job_lock("lines_job.py")
        assert lock is not None
        assert lock.acquire(blocking=False)
        try:
            run_job("lines_job.py", extra_args="--live --props-only")
        finally:
            lock.release()

        mock_run.assert_not_called()
        mock_alert.assert_not_called()
        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args.kwargs
        assert call_kwargs["status"] == "skipped"
        assert "another lines_job.py run is still active" in call_kwargs["error_message"]
        assert JOB_STATUS["lines_job.py"]["status"] == "skipped"

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_lines_job_lock_releases_after_completion(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import _get_job_lock, run_job

        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        run_job("lines_job.py")

        lock = _get_job_lock("lines_job.py")
        assert lock is not None
        assert lock.acquire(blocking=False)
        lock.release()

    @patch("src.orchestration.scheduler.record_job_execution")
    @patch("src.orchestration.scheduler._send_job_alert")
    @patch("src.orchestration.scheduler.subprocess.run")
    def test_record_job_execution_called(self, mock_run, mock_alert, mock_record):
        from src.orchestration.scheduler import run_job

        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        run_job("test_job.py")

        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args
        assert call_kwargs[1]["job_name"] == "test_job.py"
        assert call_kwargs[1]["status"] == "success"


class TestDailyStatsRetry:
    """Tests for the 9:30 retry logic."""

    def setup_method(self):
        from src.orchestration.scheduler import JOB_STATUS

        JOB_STATUS.clear()

    @patch("src.orchestration.scheduler.run_job")
    @patch("src.orchestration.scheduler._send_job_alert")
    def test_skips_retry_when_already_succeeded(self, mock_alert, mock_run_job):
        from src.orchestration.scheduler import JOB_STATUS, run_daily_stats_retry

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "success",
            "end_time": datetime.now(UTC),
            "duration": 300.0,
        }
        run_daily_stats_retry()

        mock_run_job.assert_not_called()

    @patch("src.orchestration.scheduler.run_job")
    @patch("src.orchestration.scheduler._send_job_alert")
    def test_retries_when_failed(self, mock_alert, mock_run_job):
        from src.orchestration.scheduler import JOB_STATUS, run_daily_stats_retry

        JOB_STATUS["daily_stats_job.py"] = {
            "status": "failed",
            "end_time": datetime.now(UTC),
            "duration": 100.0,
        }
        run_daily_stats_retry()

        mock_run_job.assert_called_once_with("daily_stats_job.py")

    @patch("src.orchestration.scheduler.run_job")
    @patch("src.orchestration.scheduler._send_job_alert")
    def test_retries_when_no_status(self, mock_alert, mock_run_job):
        from src.orchestration.scheduler import run_daily_stats_retry

        run_daily_stats_retry()

        mock_run_job.assert_called_once_with("daily_stats_job.py")
