from datetime import UTC, date, datetime

from src.trading.kalshi.execution_service import KalshiExecutionService


class FakeResult:
    def __init__(self, scalar_value=0):
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.engine.calls.append((sql, params or {}))
        return self.engine.next_result()

    def commit(self):
        self.committed = True
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
    def __init__(self, balances=None, order_result=None):
        self.balances = list(balances or [{"balance": 10000}])
        self.order_result = order_result or {
            "order": {"order_id": "ord-1", "status": "executed", "yes_price": 70, "count": 2}
        }
        self.orders = []

    def get_balance(self):
        if self.balances:
            return self.balances.pop(0)
        return {"balance": 10000}

    def create_order(self, **kwargs):
        self.orders.append(kwargs)
        return self.order_result


def sample_trade(**overrides):
    trade = {
        "game_date": date(2026, 5, 18),
        "ticker": "KXMLBHIT-26MAY181900PLAYER",
        "sport": "mlb",
        "player_id": 7,
        "player_name": "Test Batter",
        "stat_type": "batter_hits",
        "line": 1.0,
        "side": "yes",
        "yes_price": 70,
        "contracts": 2,
        "expected_cost": 1.4,
        "expected_fee": 0.02,
        "model_prob": 0.8,
        "kalshi_implied": 0.70,
        "edge": 0.10,
        "fee_adjusted_edge": 0.18,
        "game_start_time": datetime(2026, 5, 18, 19, 0, tzinfo=UTC),
    }
    trade.update(overrides)
    return trade


def service_with(engine=None, client=None, **kwargs):
    alerts = []
    return KalshiExecutionService(
        engine=engine or FakeEngine([FakeResult(0)]),
        client=client or FakeClient(),
        get_best_available_price=kwargs.get("get_best_available_price", lambda ticker, side, snapshot: snapshot),
        calculate_kelly_contracts=kwargs.get("calculate_kelly_contracts", lambda model_prob, yes_price, side, balance: 2),
        send_trade_placed_alert=kwargs.get("send_trade_placed_alert", lambda *args, **kw: alerts.append((args, kw))),
        daily_exposure_pct=kwargs.get("daily_exposure_pct", 0.60),
        min_daily_exposure=kwargs.get("min_daily_exposure", 80.0),
        max_daily_exposure=kwargs.get("max_daily_exposure", 500.0),
        sweep_max_cents=kwargs.get("sweep_max_cents", 10),
        sweep_edge_retention=kwargs.get("sweep_edge_retention", 0.50),
    ), alerts


def test_execute_trades_places_yes_market_order_records_fill_and_sends_alert():
    engine = FakeEngine([FakeResult(0)])
    client = FakeClient(balances=[{"balance": 10000}, {"balance": 10000}, {"balance": 9860}])
    service, alerts = service_with(engine=engine, client=client)

    results = service.execute_trades([sample_trade()])

    assert client.orders == [{
        "ticker": "KXMLBHIT-26MAY181900PLAYER",
        "action": "buy",
        "side": "yes",
        "order_type": "market",
        "count": 2,
        "yes_price": 73,
    }]
    assert results == [{
        "ticker": "KXMLBHIT-26MAY181900PLAYER",
        "side": "yes",
        "contracts": 2,
        "fill_price": 70,
        "total_cost": 1.4,
        "order_id": "ord-1",
        "status": "filled",
    }]
    insert_sql, params = engine.calls[1]
    assert "INSERT INTO kalshi_live_orders" in insert_sql
    assert params["order_id"] == "ord-1"
    assert params["status"] == "filled"
    assert params["filled_at"] is not None
    assert params["game_start_time"] == datetime(2026, 5, 18, 19, 0, tzinfo=UTC)
    assert engine.commits == 1
    assert len(alerts) == 1


def test_execute_trades_places_no_market_order_with_no_price_buffer():
    engine = FakeEngine([FakeResult(0)])
    client = FakeClient(order_result={"order": {"id": "ord-no", "status": "resting"}})
    service, _alerts = service_with(engine=engine, client=client)

    results = service.execute_trades([sample_trade(side="no", yes_price=70, expected_cost=0.6)])

    assert client.orders[0] == {
        "ticker": "KXMLBHIT-26MAY181900PLAYER",
        "action": "buy",
        "side": "no",
        "order_type": "market",
        "count": 2,
        "no_price": 33,
    }
    assert results[0]["order_id"] == "ord-no"
    assert results[0]["status"] == "pending"
    assert results[0]["fill_price"] == 70
    assert results[0]["total_cost"] == 0.6


def test_execute_trades_skips_when_balance_is_insufficient():
    engine = FakeEngine([FakeResult(0)])
    client = FakeClient(balances=[{"balance": 10000}, {"balance": 50}])
    service, alerts = service_with(engine=engine, client=client)

    results = service.execute_trades([sample_trade(expected_cost=1.4)])

    assert results == []
    assert client.orders == []
    assert len(engine.calls) == 1  # only existing exposure lookup
    assert alerts == []


def test_record_order_preserves_legacy_insert_shape_and_rounding():
    engine = FakeEngine()
    service, _alerts = service_with(engine=engine)

    service.record_order(
        sample_trade(),
        order_id="ord-2",
        status="pending",
        fill_price=70,
        fill_count=2,
        total_cost=1.444,
        fee_paid=0.123456,
    )

    sql, params = engine.calls[0]
    assert "INSERT INTO kalshi_live_orders" in sql
    assert "'market'" in sql
    assert params["total_cost"] == 1.44
    assert params["fee_paid"] == 0.1235
    assert params["filled_at"] is None
    assert engine.commits == 1
