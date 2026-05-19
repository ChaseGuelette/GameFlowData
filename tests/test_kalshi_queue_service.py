from datetime import date, datetime, timedelta

from src.trading.kalshi.queue_service import KalshiQueueService, split_executable_approved_rows
from src.trading.kalshi.statuses import TradeQueueStatus


class FakeResult:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class FakeRow:
    def __init__(self, **values):
        self._mapping = values


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.engine.calls.append((sql, params or {}))
        return self.engine.next_result()


class FakeEngine:
    def __init__(self, results=None):
        self.calls = []
        self.results = list(results or [])

    def begin(self):
        return FakeConnection(self)

    def connect(self):
        return FakeConnection(self)

    def next_result(self):
        if self.results:
            return self.results.pop(0)
        return FakeResult()


def sample_trade():
    return {
        "game_date": date(2026, 5, 18),
        "ticker": "KXMLBHIT-26MAY181900PLAYER",
        "sport": "mlb",
        "player_id": 7,
        "player_name": "Test Batter",
        "stat_type": "batter_hits",
        "line": 1.0,
        "side": "no",
        "yes_price": 70,
        "contracts": 15,
        "expected_cost": 4.5,
        "expected_fee": 0.15,
        "model_prob": 0.45,
        "kalshi_implied": 0.70,
        "edge": 0.08,
        "fee_adjusted_edge": 0.23,
        "sportsbook_consensus_line": 0.5,
    }


def test_propose_trades_inserts_pending_approval_rows_with_existing_shape():
    engine = FakeEngine()
    service = KalshiQueueService(engine)

    proposed = service.propose_trades([sample_trade()])

    assert proposed == 1
    assert len(engine.calls) == 1
    sql, params = engine.calls[0]
    assert "INSERT INTO kalshi_trade_queue" in sql
    assert TradeQueueStatus.PENDING_APPROVAL.value in sql
    assert params["ticker"] == "KXMLBHIT-26MAY181900PLAYER"
    assert params["expected_cost"] == 4.5
    assert params["sportsbook_consensus_line"] == 0.5


def test_renew_expired_pending_trades_extends_only_pending_approval_rows():
    engine = FakeEngine([FakeResult(rowcount=3)])
    service = KalshiQueueService(engine)

    renewed = service.renew_expired_pending_trades(date(2026, 5, 18), "mlb")

    assert renewed == 3
    sql, params = engine.calls[0]
    assert "UPDATE kalshi_trade_queue" in sql
    assert "market_status = 'open'" in sql
    assert TradeQueueStatus.PENDING_APPROVAL.value in sql
    assert params == {"d": date(2026, 5, 18), "sport": "mlb"}


def test_fetch_pending_approval_trades_returns_mapping_dicts_for_alerts():
    rows = [FakeRow(player_name="A", stat_type="batter_hits", side="no", contracts=2, expected_cost=1.2, fee_adjusted_edge=0.2)]
    engine = FakeEngine([FakeResult(rows=rows)])
    service = KalshiQueueService(engine)

    pending = service.fetch_pending_approval_trades(date(2026, 5, 18), "mlb")

    assert pending == [{"player_name": "A", "stat_type": "batter_hits", "side": "no", "contracts": 2, "expected_cost": 1.2, "fee_adjusted_edge": 0.2}]
    sql, params = engine.calls[0]
    assert "status = 'pending_approval'" in sql
    assert "expires_at > now()" in sql
    assert params == {"sport": "mlb", "d": date(2026, 5, 18)}


def test_split_executable_approved_rows_converts_rows_and_partitions_expired_ids():
    now = datetime(2026, 5, 18, 12, 0, 0)
    rows = [
        FakeRow(
            id=1,
            game_date=date(2026, 5, 18),
            ticker="LIVE",
            sport="mlb",
            player_id=7,
            player_name="A",
            stat_type="batter_hits",
            line=1,
            side="no",
            yes_price=70,
            contracts=15,
            expected_cost=4.5,
            expected_fee=0.15,
            model_prob=0.45,
            kalshi_implied=0.70,
            edge=0.08,
            fee_adjusted_edge=0.23,
            expires_at=now + timedelta(minutes=5),
        ),
        FakeRow(
            id=2,
            game_date=date(2026, 5, 18),
            ticker="EXPIRED",
            sport="mlb",
            player_id=8,
            player_name="B",
            stat_type="batter_hits",
            line=1,
            side="no",
            yes_price=70,
            contracts=10,
            expected_cost=3.0,
            expected_fee=None,
            model_prob=None,
            kalshi_implied=None,
            edge=None,
            fee_adjusted_edge=None,
            expires_at=now - timedelta(seconds=1),
        ),
    ]

    trades, expired_ids = split_executable_approved_rows(rows, now=now)

    assert expired_ids == [2]
    assert len(trades) == 1
    assert trades[0]["ticker"] == "LIVE"
    assert trades[0]["_queue_id"] == 1
    assert trades[0]["line"] == 1.0
    assert trades[0]["expected_fee"] == 0.15


def test_mark_execution_results_marks_executed_tickers_and_failed_others():
    engine = FakeEngine()
    service = KalshiQueueService(engine)
    trades = [
        {"_queue_id": 1, "ticker": "PLACED"},
        {"_queue_id": 2, "ticker": "FAILED"},
    ]
    results = [{"ticker": "PLACED"}]

    service.mark_execution_results(trades, results)

    assert len(engine.calls) == 2
    assert "SET status = 'executed', executed_at = now()" in engine.calls[0][0]
    assert engine.calls[0][1] == {"id": 1}
    assert "SET status = 'failed'" in engine.calls[1][0]
    assert engine.calls[1][1] == {"id": 2}
