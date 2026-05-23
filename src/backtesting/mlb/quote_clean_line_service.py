"""Quote-clean line-fetch orchestration for MLB backtest sweeps."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.backtesting.mlb.line_selection import fetch_lines_at_decision_time
from src.backtesting.mlb.quote_decision_policy import decision_time_for_game


def fetch_lines_for_date(
    engine,
    games: list[dict] | None = None,
    market_keys: list[str] | None = None,
    quote_clean_cutoff_ts: datetime | None = None,
    quote_clean_cutoff_time_et: str | None = None,
    quote_decision_policy: str = "fixed_et",
    quote_relative_minutes: int = 60,
    line_source: str = "mlb_raw_player_props",
    *,
    game_ids: list[int] | None = None,
) -> pd.DataFrame:
    """Fetch quote-clean prop lines for a set of games.

    Fixed mode uses one date-level decision timestamp. Per-game modes compute a
    decision timestamp per game so early starts are handled explicitly.

    `quote_clean_cutoff_time_et` is accepted for compatibility with the prior
    runner helper signature; timestamp construction is handled upstream.
    """
    _ = quote_clean_cutoff_time_et

    if games is None and game_ids is not None:
        games = [{"game_id": int(game_id)} for game_id in game_ids]
    if not games or not market_keys:
        return pd.DataFrame()

    resolved_game_ids = [int(g["game_id"]) for g in games]

    if quote_clean_cutoff_ts is not None and quote_decision_policy != "fixed_et":
        parts = []
        for game in games:
            decision_ts = decision_time_for_game(
                game,
                policy=quote_decision_policy,
                fixed_cutoff_ts=quote_clean_cutoff_ts,
                relative_minutes=quote_relative_minutes,
            )
            if decision_ts is None:
                continue
            part = fetch_lines_at_decision_time(
                engine,
                game_ids=[int(game["game_id"])],
                market_keys=market_keys,
                as_of_time=decision_ts,
                allow_latest_without_as_of=False,
                bookmakers=None,
                source_table=line_source,
            )
            if not part.empty:
                part = part.copy()
                part["selected_decision_time"] = decision_ts
                part["quote_decision_policy"] = quote_decision_policy
                parts.append(part)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    out = fetch_lines_at_decision_time(
        engine,
        game_ids=resolved_game_ids,
        market_keys=market_keys,
        as_of_time=quote_clean_cutoff_ts,
        allow_latest_without_as_of=quote_clean_cutoff_ts is None,
        bookmakers=None,
        source_table=line_source,
    )
    if not out.empty and quote_clean_cutoff_ts is not None:
        out = out.copy()
        out["selected_decision_time"] = quote_clean_cutoff_ts
        out["quote_decision_policy"] = quote_decision_policy
    return out
