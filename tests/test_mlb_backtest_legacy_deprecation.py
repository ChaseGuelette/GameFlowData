from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_run_mlb_backtest_requires_allow_legacy_for_execution() -> None:
    script = ROOT / "src" / "backtesting" / "mlb" / "run_mlb_backtest.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--start",
            "2026-04-13",
            "--end",
            "2026-04-13",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert "run_mlb_sweep.py --quote-clean" in combined
    assert "--allow-legacy" in combined


def test_run_mlb_backtest_allow_legacy_help_still_works() -> None:
    script = ROOT / "src" / "backtesting" / "mlb" / "run_mlb_backtest.py"
    result = subprocess.run(
        [sys.executable, str(script), "--allow-legacy", "--help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--allow-legacy" in result.stdout


def test_run_mlb_sweep_help_exposes_quote_clean_flags() -> None:
    script = ROOT / "src" / "backtesting" / "mlb" / "run_mlb_sweep.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--quote-clean" in result.stdout
    assert "--quote-cutoff-time-et" in result.stdout
