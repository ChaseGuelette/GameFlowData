"""Matchup-cache precompute helpers for the MLB backtest sweep.

This module owns the season-level batter matchup feature precompute used by the
shared prediction phase. It deliberately does not build predictions, fetch prop
lines, compute edges, or run bet simulation.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Callable

import pandas as pd

logger = logging.getLogger("MLBBacktestSweep")

MatchupCache = dict[int, tuple[pd.DataFrame, pd.DataFrame]]
ComputeBulkFn = Callable[[object, int], pd.DataFrame]


def _default_compute_opposing_starter_bulk(engine, season: int) -> pd.DataFrame:
    from src.processing.mlb.mlb_batter_matchup_features import compute_opposing_starter_bulk

    return compute_opposing_starter_bulk(engine, season)


def _default_compute_platoon_splits_bulk(engine, season: int) -> pd.DataFrame:
    from src.processing.mlb.mlb_batter_matchup_features import compute_platoon_splits_bulk

    return compute_platoon_splits_bulk(engine, season)


def build_matchup_cache(
    *,
    engine,
    game_dates: list[date],
    enabled: bool,
    compute_opposing_starter_bulk: ComputeBulkFn | None = None,
    compute_platoon_splits_bulk: ComputeBulkFn | None = None,
) -> MatchupCache | None:
    """Precompute season-level matchup features once per season when enabled."""
    if not enabled:
        return None

    compute_opp = compute_opposing_starter_bulk or _default_compute_opposing_starter_bulk
    compute_platoon = compute_platoon_splits_bulk or _default_compute_platoon_splits_bulk

    matchup_cache: MatchupCache = {}
    seasons = sorted({gd.year for gd in game_dates})
    for season in seasons:
        logger.info(f"  Precomputing matchup features for season {season}...")
        t_mc = time.time()
        opp_df = compute_opp(engine, season)
        logger.info(f"    Opposing starter features: {len(opp_df)} rows ({time.time() - t_mc:.1f}s)")
        t_mc = time.time()
        plat_df = compute_platoon(engine, season)
        logger.info(f"    Platoon splits: {len(plat_df)} rows ({time.time() - t_mc:.1f}s)")
        matchup_cache[season] = (opp_df, plat_df)

    return matchup_cache
