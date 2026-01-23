"""
Backtesting module for evaluating prediction models on historical data.

This module provides:
- BacktestHarness: Main orchestrator for running backtests
- BetSimulator: P&L tracking and bet simulation
- PerformanceMetrics: ROI, Sharpe, calibration metrics
"""

from src.backtesting.backtest_harness import BacktestHarness, BacktestResult
from src.backtesting.bet_simulator import Bet, BetSimulator
from src.backtesting.performance_metrics import PerformanceMetrics

__all__ = [
    "BacktestHarness",
    "BacktestResult",
    "BetSimulator",
    "Bet",
    "PerformanceMetrics",
]
