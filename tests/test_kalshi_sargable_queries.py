from __future__ import annotations

from pathlib import Path

KALSHI_PRODUCTION_QUERY_FILES = [
    Path("src/trading/kalshi/selection_loader.py"),
    Path("src/models/kalshi_edge.py"),
    Path("src/paper_trading/kalshi_paper_trader.py"),
    Path("src/arbitrage/market_matcher.py"),
    Path("src/orchestration/kalshi_refresh_job.py"),
]


def test_production_market_queries_do_not_cast_snapshot_time_to_et_date():
    forbidden = "snapshot_time AT TIME ZONE 'America/New_York'"

    offenders = [
        str(path)
        for path in KALSHI_PRODUCTION_QUERY_FILES
        if forbidden in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_production_market_queries_use_sargable_utc_bounds():
    for path in KALSHI_PRODUCTION_QUERY_FILES:
        content = path.read_text(encoding="utf-8")
        if "kalshi_markets" not in content and "polymarket_markets" not in content:
            continue

        assert "snapshot_time >= :start_utc" in content, path
        assert "snapshot_time < :end_utc" in content, path
