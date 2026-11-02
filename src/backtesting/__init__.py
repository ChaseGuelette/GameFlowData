"""
Backtesting module for evaluating prediction models on historical data.

This module provides:
- BacktestHarness: Main orchestrator for running backtests
- BetSimulator: P&L tracking and bet simulation
- PerformanceMetrics: ROI, Sharpe, calibration metrics
"""

from importlib import import_module

__all__ = [
    "BacktestHarness",
    "BacktestResult",
    "BetSimulator",
    "Bet",
    "PerformanceMetrics",
]

_EXPORT_MODULES = {
    "BacktestHarness": "src.backtesting.backtest_harness",
    "BacktestResult": "src.backtesting.backtest_harness",
    "BetSimulator": "src.backtesting.bet_simulator",
    "Bet": "src.backtesting.bet_simulator",
    "PerformanceMetrics": "src.backtesting.performance_metrics",
}


def __getattr__(name: str):
    """Preserve public exports without loading replay machinery for audit CLIs."""
    module = _EXPORT_MODULES.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
