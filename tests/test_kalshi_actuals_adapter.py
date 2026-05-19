from datetime import date

import pandas as pd

from src.trading.kalshi.actuals_adapter import KalshiActualsAdapter


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params or {}))
        return FakeResult(self.engine.results.pop(0))


class FakeEngine:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def connect(self):
        return FakeConnection(self)


def orders_df(*stat_types):
    return pd.DataFrame({"stat_type": list(stat_types)})


def test_fetch_actuals_uses_nba_resolution_mapping_and_min_filter(monkeypatch):
    monkeypatch.setattr(
        "src.trading.kalshi.actuals_adapter.NBA_STAT_RESOLUTION",
        {"pra": ("nba_player_stats", ["points", "rebounds", "assists"])},
    )
    monkeypatch.setattr("src.trading.kalshi.actuals_adapter.MLB_STAT_RESOLUTION", {})
    engine = FakeEngine(results=[[(42, 31.0), (43, None)]])

    actuals = KalshiActualsAdapter(engine).fetch_actuals(date(2026, 5, 17), orders_df("pra"), "nba")

    sql, params = engine.calls[0]
    assert "FROM nba_player_stats s" in sql
    assert "(s.points + s.rebounds + s.assists) as actual_value" in sql
    assert "s.min > 0" in sql
    assert params == {"game_date": date(2026, 5, 17)}
    assert actuals == {(42, "pra"): 31.0, (43, "pra"): None}


def test_fetch_actuals_uses_mlb_resolution_mapping_and_treats_dnp_as_missing(monkeypatch):
    monkeypatch.setattr("src.trading.kalshi.actuals_adapter.NBA_STAT_RESOLUTION", {})
    monkeypatch.setattr(
        "src.trading.kalshi.actuals_adapter.MLB_STAT_RESOLUTION",
        {"batter_hrr": ("mlb_batter_daily_stats", ["hits", "runs", "rbi"])},
    )
    engine = FakeEngine(results=[[(7, 3.0, False), (8, 1.0, True), (9, None, False)]])

    actuals = KalshiActualsAdapter(engine).fetch_actuals(
        date(2026, 5, 17), orders_df("batter_hrr"), "mlb"
    )

    sql, params = engine.calls[0]
    assert "FROM mlb_batter_daily_stats s" in sql
    assert "s.hits + s.runs + s.rbi as actual_value, s.did_not_play" in sql
    assert params == {"game_date": date(2026, 5, 17)}
    assert actuals == {
        (7, "batter_hrr"): 3.0,
        (8, "batter_hrr"): None,
        (9, "batter_hrr"): None,
    }
