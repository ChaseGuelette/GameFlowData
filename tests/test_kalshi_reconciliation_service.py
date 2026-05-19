from datetime import date

from src.trading.kalshi.reconciliation_service import KalshiReconciliationService


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

    def next_result(self):
        if self.results:
            return self.results.pop(0)
        return FakeResult()


class FakeRow:
    def __init__(self, values):
        self.values = values

    def __getitem__(self, index):
        return self.values[index]


def order_row(
    *,
    db_id=1,
    order_id="ord-1",
    ticker="TICKER",
    side="yes",
    contracts=2,
    total_cost=None,
    fill_count=None,
    status="pending",
    fill_price=None,
):
    return FakeRow([db_id, order_id, ticker, side, contracts, total_cost, fill_count, status, fill_price])


class FakeClient:
    def __init__(self, resting=None, fills_by_order=None):
        self.resting = resting or []
        self.fills_by_order = fills_by_order or {}
        self.fill_calls = []

    def list_orders(self, **kwargs):
        self.list_kwargs = kwargs
        return self.resting

    def get_fills(self, order_id):
        self.fill_calls.append(order_id)
        return self.fills_by_order.get(order_id, [])


def test_reconcile_fills_returns_zero_when_no_candidate_orders():
    engine = FakeEngine([FakeResult([])])
    client = FakeClient()
    service = KalshiReconciliationService(engine=engine, client=client)

    assert service.reconcile_fills() == {"reconciled": 0}
    assert len(engine.calls) == 1
    assert "WHERE status = 'pending' OR (status = 'filled' AND fill_price IS NULL)" in engine.calls[0][0]
    assert not hasattr(client, "list_kwargs")


def test_reconcile_fills_filters_by_target_date():
    target = date(2026, 5, 18)
    engine = FakeEngine([FakeResult([])])
    service = KalshiReconciliationService(engine=engine, client=FakeClient())

    assert service.reconcile_fills(target) == {"reconciled": 0}
    sql, params = engine.calls[0]
    assert "WHERE game_date = :d" in sql
    assert params == {"d": target}


def test_reconcile_fills_promotes_pending_with_fill_data_when_not_resting_without_api_fill_lookup():
    row = order_row(db_id=7, order_id="ord-filled", ticker="ABC", fill_count=2, status="pending", fill_price=70)
    engine = FakeEngine([FakeResult([row])])
    client = FakeClient(resting=[])
    service = KalshiReconciliationService(engine=engine, client=client)

    result = service.reconcile_fills()

    assert result == {"reconciled": 0, "promoted": 1, "derived": 0, "cancelled": 0}
    assert client.fill_calls == []
    update_sql, params = engine.calls[1]
    assert "SET status = 'filled', filled_at = COALESCE(filled_at, now())" in update_sql
    assert params == {"id": 7}
    assert engine.commits == 1


def test_reconcile_fills_derives_missing_fill_price_for_filled_no_side_when_api_has_no_fills():
    row = order_row(db_id=8, order_id="ord-derived", side="no", total_cost=0.60, fill_count=2, status="filled", fill_price=None)
    engine = FakeEngine([FakeResult([row])])
    client = FakeClient(resting=[])
    service = KalshiReconciliationService(engine=engine, client=client)

    result = service.reconcile_fills()

    assert result == {"reconciled": 0, "promoted": 0, "derived": 1, "cancelled": 0}
    assert client.fill_calls == ["ord-derived"]
    update_sql, params = engine.calls[1]
    assert "SET fill_price = :price" in update_sql
    assert params == {"price": 70, "id": 8}


def test_reconcile_fills_cancels_not_resting_order_without_fills_or_fill_data():
    row = order_row(db_id=9, order_id="ord-cancel", status="pending", fill_price=None, fill_count=None)
    engine = FakeEngine([FakeResult([row])])
    client = FakeClient(resting=[])
    service = KalshiReconciliationService(engine=engine, client=client)

    result = service.reconcile_fills()

    assert result == {"reconciled": 0, "promoted": 0, "derived": 0, "cancelled": 1}
    update_sql, params = engine.calls[1]
    assert "SET status = 'cancelled', pnl = 0.0, resolved_at = now()" in update_sql
    assert params == {"id": 9}


def test_reconcile_fills_updates_filled_order_from_api_fills_with_weighted_price_and_fee():
    row = order_row(db_id=10, order_id="ord-api", side="yes", status="pending")
    fills = [{"yes_price": 70, "count": 1}, {"yes_price": 80, "count": 3}]
    engine = FakeEngine([FakeResult([row])])
    client = FakeClient(resting=[{"order_id": "ord-api"}], fills_by_order={"ord-api": fills})
    service = KalshiReconciliationService(engine=engine, client=client)

    result = service.reconcile_fills()

    assert result == {"reconciled": 1, "promoted": 0, "derived": 0, "cancelled": 0}
    update_sql, params = engine.calls[1]
    assert "SET status = 'filled'" in update_sql
    assert params["price"] == 77
    assert params["count"] == 4
    assert params["cost"] == 3.08
    assert params["id"] == 10
    assert params["fee"] >= 0
