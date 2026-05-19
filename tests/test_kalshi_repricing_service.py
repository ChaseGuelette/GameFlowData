from datetime import date, datetime

from src.trading.kalshi.repricing_service import KalshiRepricingService


class FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


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

    def begin(self):
        return FakeConnection(self)

    def next_result(self):
        if self.results:
            return self.results.pop(0)
        return FakeResult()


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping


class FakeClient:
    def __init__(self, resting=None, cancel_result=None, create_result=None):
        self.resting = resting or []
        self.cancel_result = {} if cancel_result is None else cancel_result
        self.create_result = create_result
        self.cancel_calls = []
        self.create_calls = []

    def list_orders(self, **kwargs):
        self.list_kwargs = kwargs
        return self.resting

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        return self.cancel_result

    def create_order(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.create_result


def db_order(**overrides):
    row = {
        "kalshi_order_id": "old-1",
        "ticker": "TICKER",
        "side": "yes",
        "model_prob": 0.80,
        "fee_adjusted_edge": 0.20,
        "yes_price": 70,
        "game_date": date(2026, 5, 18),
        "sport": "nba",
        "player_id": 42,
        "player_name": "Player A",
        "stat_type": "points",
        "line": 20.5,
        "edge": 0.25,
        "kalshi_implied": 0.70,
        "contracts": 3,
        "game_start_time": datetime(2026, 5, 18, 19, 0),
    }
    row.update(overrides)
    return FakeRow(row)


def service(engine, client, actual_price=72, sweep_max_cents=10, sweep_edge_retention=0.50):
    price_calls = []

    def get_best_available_price(ticker, side, resting_price):
        price_calls.append((ticker, side, resting_price))
        return actual_price

    return KalshiRepricingService(
        engine=engine,
        client=client,
        get_best_available_price=get_best_available_price,
        sweep_max_cents=sweep_max_cents,
        sweep_edge_retention=sweep_edge_retention,
    ), price_calls


def test_reprice_stale_orders_returns_zero_when_no_resting_orders():
    engine = FakeEngine()
    client = FakeClient(resting=[])
    repricer, price_calls = service(engine, client)

    assert repricer.reprice_stale_orders() == 0
    assert client.list_kwargs == {"status": "resting"}
    assert engine.calls == []
    assert price_calls == []


def test_reprice_stale_orders_returns_zero_when_no_pending_db_matches():
    engine = FakeEngine([FakeResult([])])
    client = FakeClient(resting=[{"order_id": "old-1"}])
    repricer, price_calls = service(engine, client)

    assert repricer.reprice_stale_orders() == 0
    query_sql, params = engine.calls[0]
    assert "WHERE kalshi_order_id = ANY(:ids)" in query_sql
    assert "AND status = 'pending'" in query_sql
    assert params == {"ids": ["old-1"]}
    assert price_calls == []


def test_reprice_stale_orders_skips_when_price_move_exceeds_sweep_limit(monkeypatch):
    engine = FakeEngine([FakeResult([db_order()])])
    client = FakeClient(resting=[{"order_id": "old-1"}])
    repricer, price_calls = service(engine, client, actual_price=85, sweep_max_cents=10)

    assert repricer.reprice_stale_orders() == 0
    assert price_calls == [("TICKER", "yes", 70)]
    assert client.cancel_calls == []
    assert len(engine.calls) == 1


def test_reprice_stale_orders_cancels_and_replaces_yes_order_when_edge_retained(monkeypatch):
    monkeypatch.setattr("src.trading.kalshi.repricing_service.fee_adjusted_edge", lambda *args, **kwargs: 0.15)
    monkeypatch.setattr("src.trading.kalshi.repricing_service.kalshi_taker_fee", lambda price: 0.01)
    engine = FakeEngine([FakeResult([db_order()])])
    client = FakeClient(
        resting=[{"order_id": "old-1"}],
        cancel_result={"ok": True},
        create_result={"order": {"order_id": "new-1", "status": "executed", "yes_price": 72, "count": 3}},
    )
    repricer, price_calls = service(engine, client, actual_price=72)

    assert repricer.reprice_stale_orders() == 1
    assert price_calls == [("TICKER", "yes", 70)]
    assert client.cancel_calls == ["old-1"]
    assert client.create_calls == [{
        "ticker": "TICKER",
        "action": "buy",
        "side": "yes",
        "order_type": "market",
        "count": 3,
        "yes_price": 75,
    }]
    cancel_sql, cancel_params = engine.calls[1]
    insert_sql, insert_params = engine.calls[2]
    assert "SET status = 'cancelled'" in cancel_sql
    assert cancel_params == {"oid": "old-1"}
    assert "INSERT INTO kalshi_live_orders" in insert_sql
    assert insert_params["order_id"] == "new-1"
    assert insert_params["status"] == "filled"
    assert insert_params["fill_price"] == 72
    assert insert_params["fill_count"] == 3
    assert insert_params["total_cost"] == 2.16
    assert insert_params["fee_paid"] == 0.03
    assert insert_params["fee_adjusted_edge"] == 0.15


def test_reprice_stale_orders_places_no_replacement_at_inverse_price(monkeypatch):
    monkeypatch.setattr("src.trading.kalshi.repricing_service.fee_adjusted_edge", lambda *args, **kwargs: 0.15)
    monkeypatch.setattr("src.trading.kalshi.repricing_service.kalshi_taker_fee", lambda price: 0.01)
    engine = FakeEngine([FakeResult([db_order(side="no")])])
    client = FakeClient(
        resting=[{"order_id": "old-1"}],
        create_result={"order": {"order_id": "new-no", "status": "resting"}},
    )
    repricer, _ = service(engine, client, actual_price=68)

    assert repricer.reprice_stale_orders() == 1
    assert client.create_calls[0] == {
        "ticker": "TICKER",
        "action": "buy",
        "side": "no",
        "order_type": "market",
        "count": 3,
        "no_price": 35,
    }
    assert engine.calls[2][1]["status"] == "pending"
    assert engine.calls[2][1]["fill_price"] is None


def test_reprice_stale_orders_marks_old_order_cancelled_if_replacement_fails(monkeypatch):
    monkeypatch.setattr("src.trading.kalshi.repricing_service.fee_adjusted_edge", lambda *args, **kwargs: 0.15)
    engine = FakeEngine([FakeResult([db_order()])])
    client = FakeClient(resting=[{"order_id": "old-1"}], cancel_result={"ok": True}, create_result=None)
    repricer, _ = service(engine, client, actual_price=72)

    assert repricer.reprice_stale_orders() == 0
    assert client.cancel_calls == ["old-1"]
    assert len(engine.calls) == 2
    update_sql, params = engine.calls[1]
    assert "SET status = 'cancelled'" in update_sql
    assert params == {"oid": "old-1"}
