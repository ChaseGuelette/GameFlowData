"""Actual-stat lookup adapter for Kalshi settlement.

This adapter owns the side-effectful NBA/MLB stat reads needed to resolve
filled Kalshi orders.  Settlement keeps PnL/status policy; this adapter only
returns actual values keyed by ``(player_id, stat_type)``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.paper_trading.kalshi_paper_trader import MLB_STAT_RESOLUTION, NBA_STAT_RESOLUTION

logger = logging.getLogger(__name__)


class KalshiActualsAdapter:
    """Fetch actual stat values for Kalshi live-order resolution."""

    def __init__(self, engine: Any):
        self.engine = engine

    def fetch_actuals(
        self,
        game_date: date,
        orders_df: pd.DataFrame,
        sport: str,
    ) -> dict[tuple[int, str], float | None]:
        """Fetch actual stat values using the legacy paper-trader mappings."""
        actuals: dict[tuple[int, str], float | None] = {}
        stats_needed = orders_df["stat_type"].unique()

        for stat_type in stats_needed:
            nba_res = NBA_STAT_RESOLUTION.get(stat_type)
            if nba_res is not None:
                actuals.update(self._fetch_nba_actuals(game_date, stat_type, nba_res))
                continue

            mlb_res = MLB_STAT_RESOLUTION.get(stat_type)
            if mlb_res is not None:
                actuals.update(self._fetch_mlb_actuals(game_date, stat_type, mlb_res))
                continue

            logger.warning(f"No resolution mapping for stat_type: {stat_type}")

        return actuals

    def _fetch_nba_actuals(
        self,
        game_date: date,
        stat_type: str,
        resolution: tuple[str, list[str]],
    ) -> dict[tuple[int, str], float | None]:
        table, columns = resolution
        col_expr = " + ".join(f"s.{c}" for c in columns)
        with self.engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT s.player_id, ({col_expr}) as actual_value
                FROM {table} s
                WHERE s.game_date = :game_date AND s.player_id IS NOT NULL
                  AND s.min > 0
            """), {"game_date": game_date}).fetchall()
        return {
            (int(row[0]), stat_type): float(row[1]) if row[1] is not None else None
            for row in rows
        }

    def _fetch_mlb_actuals(
        self,
        game_date: date,
        stat_type: str,
        resolution: tuple[str, list[str]],
    ) -> dict[tuple[int, str], float | None]:
        table, columns = resolution
        col_expr = " + ".join(f"s.{c}" for c in columns)
        with self.engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT s.player_id, {col_expr} as actual_value, s.did_not_play
                FROM {table} s
                WHERE s.game_date = :game_date
            """), {"game_date": game_date}).fetchall()

        actuals: dict[tuple[int, str], float | None] = {}
        for row in rows:
            if row[2]:
                actuals[(int(row[0]), stat_type)] = None
            else:
                actuals[(int(row[0]), stat_type)] = float(row[1]) if row[1] is not None else None
        return actuals
