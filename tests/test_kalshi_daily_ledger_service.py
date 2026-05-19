from datetime import date

from src.trading.kalshi.daily_ledger_service import KalshiDailyLedgerService


class FakeResult:
    def __init__(self, row=None):
        self.row = row

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
        row = self.engine.rows.pop(0) if self.engine.rows else None
        return FakeResult(row)

    def commit(self):
        self.engine.commits += 1


class FakeEngine:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []
        self.commits = 0

    def connect(self):
        return FakeConnection(self)


def test_update_daily_log_returns_without_upsert_when_no_orders():
    game_date = date(2026, 5, 17)
    engine = FakeEngine(rows=[None])

    KalshiDailyLedgerService(engine=engine, starting_bankroll=100.0).update_daily_log(game_date)

    assert len(engine.calls) == 1
    assert "FROM kalshi_live_orders" in engine.calls[0][0]
    assert engine.calls[0][1] == {"d": game_date}
    assert engine.commits == 0


def test_update_daily_log_preserves_rollup_roi_and_cumulative_balance_formulas():
    game_date = date(2026, 5, 17)
    # total, won, lost, cancelled, pending, total_cost, total_pnl
    aggregate_row = (4, 2, 1, 1, 0, 12.345, 3.456)
    previous_row = (10.0, 110.0)
    engine = FakeEngine(rows=[aggregate_row, previous_row, None])

    KalshiDailyLedgerService(engine=engine, starting_bankroll=100.0).update_daily_log(game_date)

    assert len(engine.calls) == 3
    aggregate_sql, aggregate_params = engine.calls[0]
    assert "COUNT(*) FILTER (WHERE status = 'won')" in aggregate_sql
    assert "COALESCE(SUM(total_cost) FILTER (WHERE status IN ('won', 'lost')), 0)" in aggregate_sql
    assert aggregate_params == {"d": game_date}

    previous_sql, previous_params = engine.calls[1]
    assert "FROM kalshi_live_trading_daily_log" in previous_sql
    assert "WHERE game_date < :d" in previous_sql
    assert previous_params == {"d": game_date}

    upsert_sql, upsert_params = engine.calls[2]
    assert "INSERT INTO kalshi_live_trading_daily_log" in upsert_sql
    assert upsert_params == {
        "d": game_date,
        "total": 4,
        "won": 2,
        "lost": 1,
        "cancelled": 1,
        "pending": 0,
        "cost": 12.35,
        "pnl": 3.46,
        "roi": 28.0,
        "cum_pnl": 13.46,
        "bal": 113.46,
    }
    assert engine.commits == 1


def test_update_daily_log_uses_starting_bankroll_when_no_previous_log():
    game_date = date(2026, 5, 17)
    aggregate_row = (1, 1, 0, 0, 0, 0, 2.0)
    engine = FakeEngine(rows=[aggregate_row, None, None])

    KalshiDailyLedgerService(engine=engine, starting_bankroll=250.0).update_daily_log(game_date)

    upsert_params = engine.calls[2][1]
    assert upsert_params["roi"] == 0
    assert upsert_params["cum_pnl"] == 2.0
    assert upsert_params["bal"] == 252.0
