from datetime import date, timedelta

import pandas as pd

from src.trading.kalshi.settlement_service import KalshiSettlementService


class FakeResult:
    def __init__(self, scalar_value=None, row=None):
        self.scalar_value = scalar_value
        self.row = row

    def scalar(self):
        return self.scalar_value

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params or {}))
        return self.engine.next_result()

    def commit(self):
        self.engine.commits += 1


class FakeEngine:
    def __init__(self, results=None):
        self.calls = []
        self.results = list(results or [])
        self.commits = 0

    def connect(self):
        return FakeConnection(self)

    def next_result(self):
        if self.results:
            return self.results.pop(0)
        return FakeResult()


class FakeClient:
    def __init__(self, balance_cents=12345):
        self.balance_cents = balance_cents
        self.balance_calls = 0

    def get_balance(self):
        self.balance_calls += 1
        return {"balance": self.balance_cents}


def order_df(rows):
    return pd.DataFrame(rows, columns=[
        "id",
        "game_date",
        "ticker",
        "player_id",
        "player_name",
        "stat_type",
        "line",
        "side",
        "fill_price",
        "fill_count",
        "total_cost",
        "fee_paid",
        "sport",
    ])


def settlement_service(engine, client=None, orders=None, actuals=None):
    calls = {"fetch_actuals": [], "daily_log": [], "alerts": [], "streak_updates": []}

    def fetch_actuals(game_date, orders_df, sport):
        calls["fetch_actuals"].append((game_date, sport, list(orders_df["id"])))
        return actuals or {}

    def send_resolution_alert(order, status, actual, pnl, balance):
        calls["alerts"].append({
            "id": int(order["id"]),
            "status": status,
            "actual": actual,
            "pnl": pnl,
            "balance": balance,
        })

    def update_daily_log(game_date):
        calls["daily_log"].append(game_date)

    def get_consecutive_losses():
        calls["get_streak"] = True
        return 2

    def update_streak(streak):
        calls["streak_updates"].append(streak)

    service = KalshiSettlementService(
        engine=engine,
        client=client or FakeClient(),
        fetch_actuals=fetch_actuals,
        send_resolution_alert=send_resolution_alert,
        update_daily_log=update_daily_log,
        get_consecutive_losses=get_consecutive_losses,
        update_streak=update_streak,
    )
    return service, calls


def test_resolve_settled_returns_zero_when_no_filled_orders(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(
        "src.trading.kalshi.settlement_service.pd.read_sql",
        lambda statement, conn: order_df([]),
    )
    service, calls = settlement_service(engine)

    assert service.resolve_settled() == {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}
    assert calls["daily_log"] == []
    assert calls["streak_updates"] == []


def test_resolve_settled_updates_yes_win_and_sends_resolution_alert(monkeypatch):
    game_date = date.today() - timedelta(days=1)
    orders = order_df([[
        1, game_date, "TICKER", 42, "Player A", "points", 20.5, "yes", 70, 3, 2.10, 0.04, "nba"
    ]])
    monkeypatch.setattr("src.trading.kalshi.settlement_service.pd.read_sql", lambda statement, conn: orders)
    engine = FakeEngine()
    service, calls = settlement_service(engine, actuals={(42, "points"): 21.0})

    result = service.resolve_settled()

    assert result == {"resolved": 1, "won": 1, "lost": 0, "cancelled": 0}
    update_sql, params = engine.calls[0]
    assert "UPDATE kalshi_live_orders" in update_sql
    assert params == {"status": "won", "actual": 21.0, "pnl": 0.86, "id": 1}
    assert engine.commits == 1
    assert calls["alerts"] == [{"id": 1, "status": "won", "actual": 21.0, "pnl": 0.86, "balance": 123.45}]
    assert calls["daily_log"] == [game_date]
    assert calls["streak_updates"] == [2]


def test_resolve_settled_updates_no_loss_without_fee_subtraction(monkeypatch):
    game_date = date.today() - timedelta(days=1)
    orders = order_df([[
        2, game_date, "TICKER", 42, "Player A", "points", 20.5, "no", 70, 3, 0.90, 0.04, "nba"
    ]])
    monkeypatch.setattr("src.trading.kalshi.settlement_service.pd.read_sql", lambda statement, conn: orders)
    engine = FakeEngine()
    service, calls = settlement_service(engine, actuals={(42, "points"): 22.0})

    result = service.resolve_settled()

    assert result == {"resolved": 1, "won": 0, "lost": 1, "cancelled": 0}
    update_sql, params = engine.calls[0]
    assert params["status"] == "lost"
    assert params["pnl"] == -0.9
    assert calls["alerts"][0]["status"] == "lost"


def test_resolve_settled_marks_missing_actual_as_cancelled_without_alert(monkeypatch):
    game_date = date.today() - timedelta(days=1)
    orders = order_df([[
        3, game_date, "TICKER", 42, "Player A", "points", 20.5, "yes", 70, 3, 2.10, 0.04, "nba"
    ]])
    monkeypatch.setattr("src.trading.kalshi.settlement_service.pd.read_sql", lambda statement, conn: orders)
    engine = FakeEngine()
    service, calls = settlement_service(engine, actuals={})

    result = service.resolve_settled()

    assert result == {"resolved": 1, "won": 0, "lost": 0, "cancelled": 1}
    update_sql, params = engine.calls[0]
    assert params == {"status": "cancelled", "actual": None, "pnl": 0.0, "id": 3}
    assert calls["alerts"] == []
    assert calls["daily_log"] == [game_date]


def test_resolve_settled_skips_null_fill_data_and_does_not_update_order(monkeypatch):
    game_date = date.today() - timedelta(days=1)
    orders = order_df([[
        4, game_date, "TICKER", 42, "Player A", "points", 20.5, "yes", None, None, 2.10, 0.04, "nba"
    ]])
    monkeypatch.setattr("src.trading.kalshi.settlement_service.pd.read_sql", lambda statement, conn: orders)
    engine = FakeEngine()
    service, calls = settlement_service(engine, actuals={(42, "points"): 21.0})

    result = service.resolve_settled()

    assert result == {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}
    assert engine.calls == []
    assert calls["daily_log"] == [game_date]
    assert calls["alerts"] == []


def test_resolve_settled_does_not_resolve_todays_orders(monkeypatch):
    today = date.today()
    orders = order_df([[
        5, today, "TICKER", 42, "Player A", "points", 20.5, "yes", 70, 3, 2.10, 0.04, "nba"
    ]])
    monkeypatch.setattr("src.trading.kalshi.settlement_service.pd.read_sql", lambda statement, conn: orders)
    engine = FakeEngine()
    service, calls = settlement_service(engine, actuals={(42, "points"): 21.0})

    result = service.resolve_settled()

    assert result == {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}
    assert calls["fetch_actuals"] == []
    assert calls["daily_log"] == []
    assert calls["streak_updates"] == [2]
