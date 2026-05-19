from datetime import date

from src.trading.kalshi.risk_service import KalshiRiskService


class FakeResult:
    def __init__(self, rows=None, scalar_value=None, row=None):
        self._rows = rows or []
        self._scalar_value = scalar_value
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row

    def scalar(self):
        return self._scalar_value


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
        self.results = list(results or [])
        self.calls = []
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

    def __getitem__(self, index):
        return list(self._mapping.values())[index]


class FakeClient:
    def __init__(self, balance_data):
        self.balance_data = balance_data
        self.calls = 0

    def get_balance(self):
        self.calls += 1
        return self.balance_data


def service(engine, client=None, **overrides):
    alerts = []
    risk = KalshiRiskService(
        engine=engine,
        client=client or FakeClient({"balance": 10000, "portfolio_value": 0}),
        starting_bankroll=100.0,
        drawdown_limit=0.30,
        daily_loss_limit=15.0,
        consec_loss_limit=5,
        send_circuit_breaker_alert=lambda reason, balance, action: alerts.append((reason, balance, action)),
        **overrides,
    )
    return risk, alerts


def test_ensure_config_inserts_singleton_when_missing():
    engine = FakeEngine([FakeResult(row=None)])
    risk, _ = service(engine)

    risk.ensure_config()

    assert "SELECT id FROM kalshi_live_trading_config WHERE id = 1" in engine.calls[0][0]
    insert_sql, params = engine.calls[1]
    assert "INSERT INTO kalshi_live_trading_config" in insert_sql
    assert params == {"bankroll": 100.0}
    assert engine.commits == 1


def test_check_circuit_breakers_returns_halted_without_force_resume():
    engine = FakeEngine([FakeResult(row=FakeRow({"is_halted": True, "halt_reason": "Manual stop"}))])
    risk, alerts = service(engine, force_resume=False)

    assert risk.check_circuit_breakers() == (False, "Halted: Manual stop")
    assert alerts == []


def test_check_circuit_breakers_clears_halt_when_force_resume_enabled():
    engine = FakeEngine([
        FakeResult(row=FakeRow({"is_halted": True, "halt_reason": "Manual stop", "hwm_dollars": 100.0})),
        FakeResult(scalar_value=0),
        FakeResult(rows=[]),
    ])
    risk, _ = service(engine, force_resume=True)

    assert risk.check_circuit_breakers() == (True, "")
    clear_sql, _ = engine.calls[1]
    assert "SET is_halted = false" in clear_sql


def test_check_circuit_breakers_blocks_when_balance_api_unavailable():
    engine = FakeEngine([FakeResult(row=FakeRow({"is_halted": False, "hwm_dollars": 100.0}))])
    risk, _ = service(engine, client=FakeClient(None))

    assert risk.check_circuit_breakers() == (False, "Cannot check balance — API error")


def test_check_circuit_breakers_updates_high_water_mark_on_new_portfolio_high():
    engine = FakeEngine([
        FakeResult(row=FakeRow({"is_halted": False, "hwm_dollars": 100.0})),
        FakeResult(scalar_value=0),
        FakeResult(rows=[]),
    ])
    risk, _ = service(engine, client=FakeClient({"balance": 11000, "portfolio_value": 5000}))

    assert risk.check_circuit_breakers() == (True, "")
    hwm_sql, hwm_params = engine.calls[1]
    assert "SET hwm_dollars = :hwm" in hwm_sql
    assert hwm_params == {"hwm": 160.0}


def test_check_circuit_breakers_halts_on_hwm_drawdown_and_sends_alert():
    engine = FakeEngine([FakeResult(row=FakeRow({"is_halted": False, "hwm_dollars": 200.0}))])
    risk, alerts = service(engine, client=FakeClient({"balance": 12000, "portfolio_value": 0}))

    can_trade, reason = risk.check_circuit_breakers()

    assert can_trade is False
    assert "Drawdown limit reached" in reason
    halt_sql, halt_params = engine.calls[1]
    assert "SET is_halted = true" in halt_sql
    assert halt_params == {"reason": reason}
    assert alerts == [(reason, 120.0, "All trading HALTED. Manual review required.")]


def test_check_circuit_breakers_pauses_on_daily_loss_without_setting_halt():
    engine = FakeEngine([
        FakeResult(row=FakeRow({"is_halted": False, "hwm_dollars": 100.0})),
        FakeResult(scalar_value=-16.25),
    ])
    risk, alerts = service(engine)

    can_trade, reason = risk.check_circuit_breakers(today=date(2026, 5, 18))

    assert can_trade is False
    assert reason == "Daily loss limit reached: $-16.25 (limit: -$15)"
    assert alerts == [(reason, 100.0, "Pausing until tomorrow.")]
    assert not any("SET is_halted = true" in sql for sql, _ in engine.calls)


def test_check_circuit_breakers_pauses_on_consecutive_loss_streak():
    engine = FakeEngine([
        FakeResult(row=FakeRow({"is_halted": False, "hwm_dollars": 100.0})),
        FakeResult(scalar_value=0),
        FakeResult(rows=[("lost",), ("lost",), ("lost",), ("lost",), ("lost",)]),
    ])
    risk, alerts = service(engine)

    can_trade, reason = risk.check_circuit_breakers(today=date(2026, 5, 18))

    assert can_trade is False
    assert reason == "5 consecutive losses (limit: 5)"
    assert alerts == [(reason, 100.0, "Pausing for review.")]


def test_get_consecutive_losses_stops_counting_at_first_win():
    engine = FakeEngine([FakeResult(rows=[("lost",), ("lost",), ("won",), ("lost",)])])
    risk, _ = service(engine)

    assert risk.get_consecutive_losses(date(2026, 5, 18)) == 2
    sql, params = engine.calls[0]
    assert "ORDER BY resolved_at DESC" in sql
    assert params == {"d": date(2026, 5, 18), "limit": 5}
