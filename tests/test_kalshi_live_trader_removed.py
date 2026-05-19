from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OLD_TRADER_PATH = PROJECT_ROOT / "src" / "paper_trading" / "kalshi_live_trader.py"


def test_old_kalshi_live_trader_facade_file_is_removed():
    assert not OLD_TRADER_PATH.exists(), "KalshiLiveTrader facade file should be fully removed"


def test_production_code_does_not_reference_old_kalshi_live_trader():
    offenders: list[str] = []
    for path in (PROJECT_ROOT / "src").rglob("*.py"):
        if any(part in {"venv", "node_modules", ".git", ".claude-flow"} for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8")
        if "KalshiLiveTrader" in text or "kalshi_live_trader" in text:
            offenders.append(str(path.relative_to(PROJECT_ROOT)))

    assert offenders == []
