"""Data-loading helpers for the MLB backtest sweep.

This module owns the database read boundaries used by the sweep runner. It does
not generate model features or calculate betting edges; it only returns the raw
schedule/actuals records needed by the prediction cache phase.
"""

from __future__ import annotations

from datetime import date
from typing import Mapping

from sqlalchemy import text

from src.backtesting.mlb.mlb_backtest_harness import STAT_ACTUALS


def fetch_game_dates(engine, start_date: date, end_date: date) -> list[date]:
    """Return non-cancelled MLB game dates in the requested inclusive window."""
    query = text("""
        SELECT DISTINCT game_date
        FROM mlb_game_schedule
        WHERE game_date BETWEEN :start_date AND :end_date
          AND status != 'Cancelled'
        ORDER BY game_date
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
        return [row[0] for row in result]


def fetch_actuals_by_date(
    engine,
    start_date: date,
    end_date: date,
    stats: list[str],
    *,
    stat_actuals: Mapping[str, tuple[str, str]] | None = None,
) -> dict[date, dict[tuple[int, str], float]]:
    """Load actual stat results keyed by date and (player_id, stat)."""
    actual_sources = STAT_ACTUALS if stat_actuals is None else stat_actuals
    requested_stats = set(stats)
    date_actuals: dict[date, dict[tuple[int, str], float]] = {}

    for stat, (table, column) in actual_sources.items():
        if stat not in requested_stats:
            continue
        query = text(f"""
            SELECT game_date, player_id, {column} as actual_value
            FROM {table}
            WHERE game_date BETWEEN :start_date AND :end_date
              AND did_not_play IS NOT TRUE
              AND {column} IS NOT NULL
        """)
        with engine.connect() as conn:
            for row in conn.execute(query, {"start_date": start_date, "end_date": end_date}):
                game_date = row[0]
                if game_date not in date_actuals:
                    date_actuals[game_date] = {}
                date_actuals[game_date][(int(row[1]), stat)] = float(row[2])

    return date_actuals


def fetch_games_for_date(engine, game_date: date) -> list[dict]:
    """Return non-cancelled schedule rows for one game date as dictionaries."""
    query = text("""
        SELECT s.game_id, s.home_team_id, s.away_team_id,
               s.probable_pitcher_home_id, s.probable_pitcher_away_id,
               s.venue_id, s.season, s.game_time_utc
        FROM mlb_game_schedule s
        WHERE s.game_date = :game_date
          AND s.status != 'Cancelled'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"game_date": game_date})
        return [dict(row._mapping) for row in result]
