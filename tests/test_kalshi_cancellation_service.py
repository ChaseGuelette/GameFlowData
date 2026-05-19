from datetime import date, datetime
from zoneinfo import ZoneInfo

from src.trading.kalshi.cancellation_service import (
    KalshiCancellationService,
    parse_game_time_from_ticker,
)

ET = ZoneInfo("America/New_York")


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


class FakeEngine:
    def __init__(self, results=None):
        self.calls = []
        self.results = list(results or [])

    def connect(self):
        return FakeConnection(self)

    def begin(self):
        return FakeConnection(self)

    def next_result(self):
        if self.results:
            return self.results.pop(0)
        return FakeResult()


class FakeRow:
    def __init__(self, values, mapping):
        self.values = values
        self._mapping = mapping

    def __getitem__(self, index):
        return self.values[index]


def pending_order(
    *,
    kalshi_order_id="order-1",
    ticker="KXMLBHIT-26APR251415SEASTL-PLAYER",
    game_date=date(2026, 4, 25),
    game_start_time=None,
):
    mapping = {
        "id": 101,
        "kalshi_order_id": kalshi_order_id,
        "game_date": game_date,
        "ticker": ticker,
        "sport": "mlb",
        "player_id": 42,
        "player_name": "Player A",
        "stat_type": "batter_hits",
        "line": 1.5,
        "side": "yes",
        "contracts": 4,
        "total_cost": 2.8,
        "game_start_time": game_start_time,
    }
    values = [
        mapping["id"],
        mapping["kalshi_order_id"],
        mapping["game_date"],
        mapping["ticker"],
        mapping["sport"],
        mapping["player_id"],
        mapping["player_name"],
        mapping["stat_type"],
        mapping["line"],
        mapping["side"],
        mapping["contracts"],
        mapping["total_cost"],
        mapping["game_start_time"],
    ]
    return FakeRow(values, mapping)


class FakeClient:
    def __init__(self, *, authenticated=True, fail_ids=None):
        self.is_authenticated = authenticated
        self.fail_ids = set(fail_ids or [])
        self.cancel_calls = []

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        if order_id in self.fail_ids:
            raise RuntimeError(f"boom {order_id}")
        return {"ok": True}


def test_parse_game_time_from_ticker_extracts_et_start_time():
    parsed = parse_game_time_from_ticker("KXMLBHIT-26APR251415SEASTL-PLAYER")

    assert parsed == datetime(2026, 4, 25, 14, 15, tzinfo=ET)
    assert parse_game_time_from_ticker("not-a-kalshi-ticker") is None


def test_enqueue_stale_orders_for_review_detects_three_stale_paths_and_alerts():
    now = datetime(2026, 4, 25, 15, 0, tzinfo=ET)
    stale_by_db_time = pending_order(
        kalshi_order_id="db-time",
        game_start_time=datetime(2026, 4, 25, 14, 0, tzinfo=ET),
    )
    not_stale = pending_order(
        kalshi_order_id="future",
        ticker="KXMLBHIT-26APR251800SEASTL-PLAYER",
        game_start_time=datetime(2026, 4, 25, 18, 0, tzinfo=ET),
    )
    stale_by_ticker = pending_order(kalshi_order_id="ticker-time")
    stale_by_date = pending_order(
        kalshi_order_id="old-date",
        ticker="NO_PARSE",
        game_date=date(2026, 4, 24),
    )
    engine = FakeEngine([
        FakeResult([stale_by_db_time, not_stale, stale_by_ticker, stale_by_date]),
        FakeResult([]),
    ])
    alerts = []
    service = KalshiCancellationService(engine=engine, alert_stale_orders=alerts.append)

    assert service.enqueue_stale_orders_for_review(now=now) == 3

    select_pending_sql, _ = engine.calls[0]
    select_queued_sql, _ = engine.calls[1]
    assert "FROM kalshi_live_orders" in select_pending_sql
    assert "WHERE status = 'pending'" in select_pending_sql
    assert "FROM kalshi_cancel_queue" in select_queued_sql
    assert "WHERE status != 'rejected'" in select_queued_sql
    inserted_ids = [params["kalshi_order_id"] for _, params in engine.calls[2:]]
    assert inserted_ids == ["db-time", "ticker-time", "old-date"]
    for insert_sql, params in engine.calls[2:]:
        assert "INSERT INTO kalshi_cancel_queue" in insert_sql
        assert "'pending_review'" in insert_sql
        assert "ON CONFLICT (kalshi_order_id) DO NOTHING" in insert_sql
        assert params["expected_cost"] == 2.8
    assert [row["kalshi_order_id"] for row in alerts[0]] == inserted_ids


def test_enqueue_stale_orders_for_review_skips_orders_already_waiting_for_human_review():
    now = datetime(2026, 4, 25, 15, 0, tzinfo=ET)
    engine = FakeEngine([
        FakeResult([
            pending_order(kalshi_order_id="already-queued"),
            pending_order(kalshi_order_id="new-stale"),
        ]),
        FakeResult([("already-queued",)]),
    ])
    service = KalshiCancellationService(engine=engine)

    assert service.enqueue_stale_orders_for_review(now=now) == 1
    assert len(engine.calls) == 3
    assert engine.calls[2][1]["kalshi_order_id"] == "new-stale"


def test_execute_approved_cancellations_skips_api_when_client_not_authenticated():
    engine = FakeEngine([FakeResult([(1, "order-1", "Player A", "hits", "yes", 4)])])
    client = FakeClient(authenticated=False)
    service = KalshiCancellationService(engine=engine, client=client)

    assert service.execute_approved_cancellations() == {"cancelled": 0, "failed": 0, "skipped_auth": 1}
    assert client.cancel_calls == []
    assert len(engine.calls) == 1


def test_execute_approved_cancellations_marks_review_queue_executed_and_live_order_cancelled():
    engine = FakeEngine([FakeResult([(7, "order-7", "Player A", "hits", "yes", 4)])])
    client = FakeClient()
    service = KalshiCancellationService(engine=engine, client=client)

    assert service.execute_approved_cancellations() == {"cancelled": 1, "failed": 0, "skipped_auth": 0}
    assert client.cancel_calls == ["order-7"]
    queue_sql, queue_params = engine.calls[1]
    live_sql, live_params = engine.calls[2]
    assert "UPDATE kalshi_cancel_queue" in queue_sql
    assert "SET status = 'executed', executed_at = now()" in queue_sql
    assert queue_params == {"id": 7}
    assert "UPDATE kalshi_live_orders" in live_sql
    assert "SET status = 'cancelled'" in live_sql
    assert live_params == {"oid": "order-7"}


def test_execute_approved_cancellations_marks_queue_failed_without_touching_live_order_on_api_error():
    engine = FakeEngine([FakeResult([(9, "order-9", "Player B", "reb", "no", 2)])])
    client = FakeClient(fail_ids={"order-9"})
    service = KalshiCancellationService(engine=engine, client=client)

    assert service.execute_approved_cancellations() == {"cancelled": 0, "failed": 1, "skipped_auth": 0}
    assert client.cancel_calls == ["order-9"]
    assert len(engine.calls) == 2
    fail_sql, fail_params = engine.calls[1]
    assert "UPDATE kalshi_cancel_queue" in fail_sql
    assert "SET status = 'failed', cancel_error = :error" in fail_sql
    assert fail_params["id"] == 9
    assert "boom order-9" in fail_params["error"]
