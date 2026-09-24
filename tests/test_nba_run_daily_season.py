"""The local daily orchestrator must not invoke non-CDN scraping with an implicit season."""

import sys
from unittest.mock import Mock

import pytest

from src.orchestration import run_daily


def test_local_scraping_requires_explicit_season_before_side_effects(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["run_daily.py", "--date", "2026-10-20", "--skip-processing", "--skip-inference"]
    )
    run_command = Mock(side_effect=AssertionError("no command should run"))
    monkeypatch.setattr(run_daily, "run_command", run_command)

    with pytest.raises(SystemExit) as exc:
        run_daily.main()

    assert exc.value.code == 2
    run_command.assert_not_called()


def test_local_scraping_passes_explicit_season_to_unified_scraper(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_daily.py", "--date", "2026-10-20", "--season", "2026-27", "--skip-processing", "--skip-inference"],
    )
    run_command = Mock()
    monkeypatch.setattr(run_daily, "run_command", run_command)

    run_daily.main()

    first_command = run_command.call_args_list[0].args[0]
    assert "nba_unified_scraper.py --season 2026-27" in first_command
