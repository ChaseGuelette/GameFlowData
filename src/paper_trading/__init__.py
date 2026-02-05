"""
Paper Trading Infrastructure for NBA Player Props.

Provides standalone CLI scripts to convert daily predictions into paper bets
with bet selection, outcome resolution, and P&L tracking.
"""

from src.paper_trading.paper_trader import PaperTrader

__all__ = ["PaperTrader"]
