"""Behavior tests for the MLB sweep data-loading seam."""

from __future__ import annotations

from datetime import date


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.engine.calls.append({"query": str(query), "params": params})
        return self.engine.responses.pop(0)


class FakeEngine:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def connect(self):
        return FakeConnection(self)


def test_fetch_game_dates_uses_schedule_window_and_returns_dates():
    from src.backtesting.mlb.backtest_data_loader import fetch_game_dates

    start = date(2025, 7, 1)
    end = date(2025, 7, 3)
    engine = FakeEngine(responses=[[(date(2025, 7, 1),), (date(2025, 7, 2),)]])

    assert fetch_game_dates(engine, start, end) == [date(2025, 7, 1), date(2025, 7, 2)]

    assert engine.calls[0]["params"] == {"start_date": start, "end_date": end}
    assert "FROM mlb_game_schedule" in engine.calls[0]["query"]
    assert "status != 'Cancelled'" in engine.calls[0]["query"]


def test_fetch_actuals_by_date_only_queries_requested_stats_and_keys_by_date_player_stat():
    from src.backtesting.mlb.backtest_data_loader import fetch_actuals_by_date

    start = date(2025, 7, 1)
    end = date(2025, 7, 2)
    stat_actuals = {
        "pitcher_strikeouts": ("mlb_pitcher_game_logs", "strikeouts"),
        "batter_hits": ("mlb_batter_game_logs", "hits"),
    }
    engine = FakeEngine(
        responses=[
            [
                (date(2025, 7, 1), "101", "7"),
                (date(2025, 7, 2), 102, 5.0),
            ]
        ]
    )

    actuals = fetch_actuals_by_date(
        engine,
        start,
        end,
        stats=["pitcher_strikeouts"],
        stat_actuals=stat_actuals,
    )

    assert actuals == {
        date(2025, 7, 1): {(101, "pitcher_strikeouts"): 7.0},
        date(2025, 7, 2): {(102, "pitcher_strikeouts"): 5.0},
    }
    assert len(engine.calls) == 1
    assert "mlb_pitcher_game_logs" in engine.calls[0]["query"]
    assert "mlb_batter_game_logs" not in engine.calls[0]["query"]


def test_fetch_games_for_date_returns_schedule_rows_as_dicts():
    from src.backtesting.mlb.backtest_data_loader import fetch_games_for_date

    game_date = date(2025, 7, 4)
    engine = FakeEngine(
        responses=[
            [
                FakeRow(
                    {
                        "game_id": 10,
                        "home_team_id": 1,
                        "away_team_id": 2,
                        "probable_pitcher_home_id": 1001,
                        "probable_pitcher_away_id": 1002,
                        "venue_id": 55,
                        "season": 2025,
                        "game_time_utc": "2025-07-04T23:05:00Z",
                    }
                )
            ]
        ]
    )

    games = fetch_games_for_date(engine, game_date)

    assert games == [
        {
            "game_id": 10,
            "home_team_id": 1,
            "away_team_id": 2,
            "probable_pitcher_home_id": 1001,
            "probable_pitcher_away_id": 1002,
            "venue_id": 55,
            "season": 2025,
            "game_time_utc": "2025-07-04T23:05:00Z",
        }
    ]
    assert engine.calls[0]["params"] == {"game_date": game_date}
    assert "FROM mlb_game_schedule" in engine.calls[0]["query"]
    assert "status != 'Cancelled'" in engine.calls[0]["query"]
