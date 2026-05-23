"""Tests for MLB sweep matchup-cache precompute seam."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.backtesting.mlb.matchup_cache import build_matchup_cache


def test_build_matchup_cache_returns_none_when_disabled_without_computing():
    calls: list[tuple[str, int]] = []

    def compute_opp(engine, season):
        calls.append(("opp", season))
        return pd.DataFrame()

    def compute_platoon(engine, season):
        calls.append(("platoon", season))
        return pd.DataFrame()

    cache = build_matchup_cache(
        engine=object(),
        game_dates=[date(2025, 7, 1)],
        enabled=False,
        compute_opposing_starter_bulk=compute_opp,
        compute_platoon_splits_bulk=compute_platoon,
    )

    assert cache is None
    assert calls == []


def test_build_matchup_cache_precomputes_once_per_unique_season_in_sorted_order():
    calls: list[tuple[str, int]] = []

    def compute_opp(engine, season):
        calls.append(("opp", season))
        return pd.DataFrame({"season": [season], "kind": ["opp"]})

    def compute_platoon(engine, season):
        calls.append(("platoon", season))
        return pd.DataFrame({"season": [season], "kind": ["platoon"]})

    cache = build_matchup_cache(
        engine="engine",
        game_dates=[date(2026, 4, 1), date(2025, 7, 1), date(2025, 7, 2)],
        enabled=True,
        compute_opposing_starter_bulk=compute_opp,
        compute_platoon_splits_bulk=compute_platoon,
    )

    assert calls == [("opp", 2025), ("platoon", 2025), ("opp", 2026), ("platoon", 2026)]
    assert cache is not None
    assert sorted(cache) == [2025, 2026]
    opp_2025, platoon_2025 = cache[2025]
    assert opp_2025.to_dict("records") == [{"season": 2025, "kind": "opp"}]
    assert platoon_2025.to_dict("records") == [{"season": 2025, "kind": "platoon"}]


def test_build_matchup_cache_returns_empty_cache_when_enabled_but_no_game_dates():
    cache = build_matchup_cache(engine=object(), game_dates=[], enabled=True)

    assert cache == {}
