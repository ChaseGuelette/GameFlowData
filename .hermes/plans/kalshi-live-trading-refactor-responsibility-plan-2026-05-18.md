# Kalshi Live Trading Refactor Responsibility Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Refactor Kalshi live trading from a god class plus distributed job state into explicit, testable responsibilities while preserving live-money behavior.

**Architecture:** This must be behavior-preserving first. Do not rewrite the trader in one pass. Add typed statuses, state-machine tests, pure strategy tests, and service boundaries, then migrate callers gradually. `KalshiLiveTrader` can remain temporarily as a facade while responsibilities move out.

**Tech Stack:** Python, SQLAlchemy, pytest, existing Kalshi API client, existing DB tables.

---

## Executive diagnosis

The central problem is that `KalshiLiveTrader` and its jobs are not one system with clear ownership. They are a distributed live-money state machine implemented through:

- one 1,863-line class
- many raw SQL status updates
- multiple scheduled jobs
- direct Kalshi API calls
- direct Discord alert calls
- string statuses spread across files
- scheduler timing assumptions

This makes it hard to answer basic safety questions:

- What statuses are valid?
- Which transitions are allowed?
- What job owns each transition?
- Is an operation idempotent?
- Can two jobs race on the same order?
- Does paper/live use the same selection policy?
- Can we test strategy without DB/API/Discord?
- Can we test reconciliation without placing live orders?

---

## Current ownership problems

### 1. `KalshiLiveTrader` is a god class

File:
- `src/paper_trading/kalshi_live_trader.py`

Class:
- `KalshiLiveTrader`, lines 106-1968, 1,863 class lines, 26 methods.

It currently owns:

- environment/config loading
- trading halt/circuit breaker config
- daily P&L lookup
- consecutive loss lookup
- circuit breaker alerting
- orderbook price lookup
- Kelly sizing
- trade candidate selection
- game start time lookup
- direct live order execution
- trade proposal queue insertion
- expired queue renewal
- approved queue execution
- stale order repricing
- live order DB recording
- placed-trade alerting
- fill reconciliation
- settlement resolution
- actuals fetching
- resolution alerting
- daily log rollup

Worst methods by responsibility density:

- `select_trades`, lines 431-782, 352 lines
- `execute_trades`, lines 821-1086, 266 lines
- `reprice_stale_orders`, lines 1261-1458, 198 lines
- `reconcile_fills`, lines 1541-1690, 150 lines
- `resolve_settled`, lines 1696-1815, 120 lines
- `check_circuit_breakers`, lines 200-289, 90 lines
- `_update_daily_log`, lines 1889-1968, 80 lines

Why this is wrong:

- It mixes pure policy, persistence, external API, alerts, and state transitions.
- It is hard to unit test without DB/API credentials.
- It has many reasons to change.
- A small change in one live-money behavior risks unrelated behaviors.
- `resolve_only=True` is a smell: the class is too broad, so construction needs mode flags to avoid initializing unrelated responsibilities.

---

### 2. Statuses are stringly typed and scattered

Examples from `kalshi_live_trader.py` and orchestration jobs:

- `pending_approval`
- `approved`
- `expired`
- `executed`
- `failed`
- `pending`
- `filled`
- `won`
- `lost`
- `cancelled`
- `pending_review`
- `rejected`

Tables involved:

- `kalshi_trade_queue`
- `kalshi_live_orders`
- `kalshi_cancel_queue`
- daily log tables

Why this is wrong:

- No central list of valid statuses.
- No central list of valid transitions.
- A typo can create an invalid state.
- Jobs rely on matching hard-coded strings.
- It is difficult to audit what state a record can be in and who can move it.

---

### 3. The live trade lifecycle is distributed across jobs

Files:

- `src/orchestration/kalshi_refresh_job.py`
- `src/orchestration/kalshi_execute_approved_job.py`
- `src/orchestration/kalshi_pending_fills_job.py`
- `src/orchestration/kalshi_stale_fills_job.py`
- `src/orchestration/kalshi_execute_cancellations_job.py`
- `src/orchestration/scheduler.py`
- `src/paper_trading/kalshi_live_trader.py`

Approximate current lifecycle:

1. `kalshi_refresh_job.py`
   - resolves/reconciles existing live orders
   - selects new trades
   - inserts pending approval trades
   - sends approval/reminder alerts
2. dashboard/API/user approval changes queue status
3. `kalshi_execute_approved_job.py`
   - polls `kalshi_trade_queue` for `approved`
   - calls `KalshiLiveTrader.execute_approved_trades`
4. `KalshiLiveTrader.execute_trades`
   - sends live orders to Kalshi
   - records `kalshi_live_orders`
   - marks queue records executed/failed/expired
5. `kalshi_pending_fills_job.py`
   - checks `kalshi_live_orders status='pending'`
   - calls `reconcile_fills`
   - sends filled alerts
6. `kalshi_stale_fills_job.py`
   - detects pending orders after game start
   - inserts into cancellation queue
7. `kalshi_execute_cancellations_job.py`
   - executes approved cancellation queue records
8. `resolve_settled`
   - resolves filled orders using actuals
   - updates daily log

Why this is wrong:

- The state machine exists, but only implicitly across code paths and cron timing.
- Jobs can overlap responsibilities.
- Race conditions are plausible around repricing, fill polling, stale cancellation, and settlement.
- Idempotency is not obvious.
- Testing one transition requires invoking broad jobs/classes.

---

### 4. Strategy selection is coupled to DB/API and execution concerns

Current location:
- `KalshiLiveTrader.select_trades`, lines 431-782.

Responsibilities inside/near selection:

- DB reads from edge/market/prediction tables
- existing live order checks
- existing queue checks
- edge thresholds
- side selection
- YES/NO policy
- price/liquidity/spread filters
- sportsbook alignment rules
- Kelly sizing
- exposure caps
- game start lookups
- prediction context assembly

Why this is wrong:

- We cannot test the trading policy as a pure function.
- Paper/live parity is hard because `KalshiPaperTrader.select_bets` has similar but separate logic.
- Selection should output trade intents; it should not own persistence or execution.

---

### 5. Execution, recording, and queue updates are coupled

Current locations:
- `execute_trades`, lines 821-1086
- `execute_approved_trades`, lines 1170-1259
- `_record_order`, lines 1460-1509

Why this is wrong:

- API order placement and DB persistence are interleaved.
- Queue status updates are coupled to ticker matching.
- Failure modes are hard to classify: API failure, partial fill, DB write failure, queue update failure.
- Idempotency/duplicate prevention is not a named concept.

---

### 6. Repricing/cancellation/fill reconciliation are not separated

Current locations:
- `reprice_stale_orders`, lines 1261-1458
- `reconcile_fills`, lines 1541-1690
- `kalshi_stale_fills_job.py`
- `kalshi_execute_cancellations_job.py`

Why this is wrong:

- Reprice can cancel and replace orders.
- Reconciliation can mark pending as filled/cancelled.
- Stale-fill job can enqueue cancellation review for pending orders.
- Cancellation execution can alter API state independently.
- These are separate lifecycle responsibilities but currently overlap through table statuses.

---

### 7. Settlement and actuals fetching are embedded in the live trader

Current locations:
- `resolve_settled`, lines 1696-1815
- `_fetch_actuals`, lines 1817-1864
- `_update_daily_log`, lines 1889-1968

Why this is wrong:

- Settlement is logically separate from live order placement.
- Actuals fetching should be an adapter/service.
- Daily log rollup should be shared ledger behavior.
- Resolution alerting should be separate from P&L computation.

---

### 8. Alerts are mixed into business logic

Current locations:
- `_send_circuit_breaker_alert`, lines 323-361
- `_send_trade_placed_alert`, lines 1511-1535
- `_send_resolution_alert`, lines 1866-1883
- approval/reminder/failure alerts in orchestration jobs

Why this is wrong:

- Business services directly know Discord payload shapes.
- Alert failures are mixed into trading code.
- It is hard to test behavior without patching alert functions.
- Alert payloads should be generated from domain events, not embedded throughout lifecycle code.

---

## Target design by responsibility

### A. Statuses and state machine

New files:

- `src/trading/kalshi/statuses.py`
- `src/trading/kalshi/state_machine.py`

Responsibilities:

- Define queue/order/cancel statuses.
- Define lifecycle names:
  - trade queue lifecycle
  - live order lifecycle
  - cancellation queue lifecycle
- Validate allowed transitions.
- Provide helper functions like:
  - `assert_transition(entity, old, new)`
  - `is_terminal_order_status(status)`
  - `is_executable_queue_status(status)`

Initial expected transitions:

Trade queue:

- `pending_approval -> approved`
- `pending_approval -> rejected`
- `pending_approval -> expired`
- `approved -> executed`
- `approved -> expired`
- `approved -> failed`

Live order:

- `pending -> filled`
- `pending -> cancelled`
- `filled -> won`
- `filled -> lost`
- `filled -> cancelled` for no-action/missing-actual cases that current settlement behavior cancels

Cancel queue:

- `pending_review -> approved`
- `pending_review -> rejected`
- `approved -> executed`
- `approved -> failed`

Validation:

- `tests/test_kalshi_status_inventory.py`
- `tests/test_kalshi_state_machine.py`

Implementation order:

1. Create statuses as constants/enums.
2. Add transition map.
3. Add tests for current known transitions.
4. Do not replace all callers yet; first make the new source of truth exist.

---

### B. Strategy policy

New file:

- `src/trading/kalshi/strategy.py`

Responsibilities:

- Pure selection and sizing from candidate rows/config.
- No DB.
- No Kalshi API.
- No Discord.
- No SQLAlchemy.

Inputs:

- candidate market/edge rows
- existing exposure summary
- existing queued/live dedupe keys
- strategy config
- bankroll/caps
- optional current prices/liquidity fields already loaded

Outputs:

- `TradeIntent` dataclass list with fields such as:
  - ticker
  - market_id
  - sport
  - stat
  - player_id/player_name
  - side
  - price/expected_price
  - contracts
  - expected_cost
  - edge/probability context
  - reason/skip_reason if using diagnostics

Why this helps:

- Strategy becomes unit-testable.
- Paper/live parity becomes possible.
- Execution and queue code can consume the same intent object.

Validation:

- `tests/test_kalshi_strategy_policy.py`
- Later: `tests/test_kalshi_paper_live_strategy_parity.py`

Migration path:

1. Wrap current selection behavior in fixtures.
2. Extract the smallest pure parts first: side/price/Kelly/skip decisions.
3. Leave DB query assembly in `KalshiLiveTrader` initially.
4. Convert DB rows to `TradeCandidate` then call strategy.

---

### C. Queue service

New file:

- `src/trading/kalshi/queue_service.py`

Responsibilities:

- Insert pending approval proposals.
- Renew still-open pending approval trades.
- Fetch approved trades ready for execution.
- Expire approved/pending approval trades when stale.
- Mark approved trades executed/failed.
- Use state machine transitions.

Moves out of:

- `KalshiLiveTrader.propose_trades`
- `KalshiLiveTrader.renew_expired_queue_trades`
- `KalshiLiveTrader.execute_approved_trades` queue-update portion
- `_get_pending_queue_trades` in `kalshi_refresh_job.py`
- some logic from `kalshi_execute_approved_job.py`

Validation:

- `tests/test_kalshi_queue_service.py`

Migration path:

1. Implement queue service using the same SQL as current code.
2. Call it from `KalshiLiveTrader` facade.
3. Move orchestration jobs to call the service directly.
4. Later dashboard approval API can use same service/types.

---

### D. Execution service

New file:

- `src/trading/kalshi/execution_service.py`

Responsibilities:

- Execute approved `TradeIntent` / queue rows against Kalshi API.
- Compute expected/actual fill fields from Kalshi response.
- Persist live orders or return `OrderRecord` objects for persistence service.
- Handle direct API failures distinctly from DB failures.
- Provide idempotency/duplicate guard.

Moves out of:

- `KalshiLiveTrader.execute_trades`
- `KalshiLiveTrader.execute_approved_trades` execution portion
- `KalshiLiveTrader._record_order`

Validation:

- `tests/test_kalshi_execution_service.py`

Migration path:

1. Extract response normalization into pure helper.
2. Add mock Kalshi client tests.
3. Extract DB order recording.
4. Keep `execute_trades` as a facade that delegates.

---

### E. Repricing service

New file:

- `src/trading/kalshi/repricing_service.py`

Responsibilities:

- Identify pending orders eligible for repricing.
- Query current orderbook/price via adapter.
- Cancel old order when replacement is needed.
- Place replacement order.
- Mark old order cancelled and insert replacement order.
- Return structured result counts/events.

Moves out of:

- `KalshiLiveTrader.reprice_stale_orders`

Validation:

- `tests/test_kalshi_repricing_service.py`

Important safety concern:

- Repricing is high-risk because it cancels live orders and creates replacements.
- This should be refactored after statuses/state machine and execution service exist.
- Tests must mock Kalshi API and assert idempotency behavior.

---

### F. Reconciliation service

New file:

- `src/trading/kalshi/reconciliation_service.py`

Responsibilities:

- Find live orders needing fill reconciliation.
- Query Kalshi fills/order status.
- Promote pending orders with already-recorded fill data to filled.
- Derive fill price/count from fills.
- Mark no-fill non-resting orders cancelled if current behavior requires it.
- Do not settle game outcome/P&L.

Moves out of:

- `KalshiLiveTrader.reconcile_fills`
- part of `kalshi_pending_fills_job.py`

Validation:

- `tests/test_kalshi_reconciliation_service.py`

Migration path:

1. Preserve current counts result shape:
   - reconciled
   - promoted
   - derived
   - cancelled
2. Move method behind facade.
3. Update pending fills job to call service directly.

---

### G. Stale order / cancellation service

New files:

- `src/trading/kalshi/stale_order_service.py`
- `src/trading/kalshi/cancellation_service.py`

Responsibilities:

Stale order service:

- Detect pending orders past game start.
- Queue cancellation review records.
- Avoid duplicate cancel queue entries.

Cancellation service:

- Execute approved cancellation records.
- Call Kalshi API cancel endpoint.
- Update cancel queue and live order statuses.

Moves out of:

- `kalshi_stale_fills_job.py`
- `kalshi_execute_cancellations_job.py`

Validation:

- `tests/test_kalshi_stale_order_service.py`
- `tests/test_kalshi_cancellation_service.py`

Important safety concern:

- Stale detection and cancellation should not be coupled to fill reconciliation.
- The stale detection service should produce cancel intents; cancellation service should execute only approved intents.

---

### H. Settlement service

New file:

- `src/trading/kalshi/settlement_service.py`

Responsibilities:

- Load filled unresolved live orders.
- Fetch actual game/player outcomes through an actuals adapter.
- Compute win/loss/cancelled/no-action P&L.
- Update live order status/P&L/resolved_at.
- Update daily log through ledger service.
- Emit domain events for alerts.

Moves out of:

- `KalshiLiveTrader.resolve_settled`
- `KalshiLiveTrader._fetch_actuals`
- `KalshiLiveTrader._update_daily_log`

Validation:

- `tests/test_kalshi_settlement_service.py`
- `tests/test_kalshi_live_daily_log.py`

Migration path:

1. Extract P&L computation as pure function.
2. Extract actuals lookup adapter.
3. Extract daily log update helper.
4. Update facade and jobs.

---

### I. Circuit breaker / risk service

New file:

- `src/trading/kalshi/risk_service.py`

Responsibilities:

- Load risk config.
- Compute current daily P&L/streak/exposure.
- Decide whether trading is halted.
- Set halted config if threshold breached.
- Emit risk domain event.

Moves out of:

- `_ensure_config`
- `_get_config`
- `_set_halted`
- `_update_streak`
- `check_circuit_breakers`
- `_get_daily_pnl`
- `_get_consecutive_losses`
- `_send_circuit_breaker_alert` alert portion

Validation:

- `tests/test_kalshi_risk_service.py`

Important boundary:

- Risk service decides; alert service notifies.
- Strategy/execution should call risk service before proposing/executing trades.

---

### J. Alert/event adapter

New file:

- `src/trading/kalshi/events.py`
- `src/trading/kalshi/alert_adapter.py`

Responsibilities:

- Define domain events:
  - TradeProposed
  - TradeApproved
  - TradeExecuted
  - OrderFilled
  - OrderCancelled
  - OrderResolved
  - CircuitBreakerTripped
- Convert events into existing Discord alert calls.
- Keep Discord-specific formatting outside core services.

Moves out of:

- `_send_circuit_breaker_alert`
- `_send_trade_placed_alert`
- `_send_resolution_alert`
- alert logic in `kalshi_refresh_job.py`
- alert logic in `kalshi_execute_approved_job.py`
- alert logic in `kalshi_pending_fills_job.py`
- alert logic in `kalshi_stale_fills_job.py`

Validation:

- `tests/test_kalshi_alert_adapter.py`

---

## Refactor phases

### Phase 0: Safety baseline, no behavior changes

Tasks:

1. Add status inventory tests.
2. Add method-level characterization tests where feasible.
3. Add state machine module but do not enforce it everywhere yet.
4. Run existing Kalshi tests.

Goal:
- Make current behavior explicit before moving code.

---

### Phase 1: Pure, low-side-effect extractions

Tasks:

1. Extract statuses.
2. Extract state machine.
3. Extract Kelly/risk-free pure helpers where possible.
4. Extract strategy selection dataclasses and pure filtering helpers.

Goal:
- Move deterministic code first.

---

### Phase 2: Queue and strategy boundaries

Tasks:

1. Extract queue service.
2. Make `propose_trades` delegate to queue service.
3. Make `renew_expired_queue_trades` delegate to queue service.
4. Make approved execution job fetch via queue service.
5. Keep `KalshiLiveTrader` as facade for compatibility.

Goal:
- The approval queue becomes one owned subsystem.

---

### Phase 3: Execution boundary

Tasks:

1. Extract response normalization.
2. Extract live order recording.
3. Extract execution service with mock Kalshi client tests.
4. Delegate `execute_trades` and `execute_approved_trades` through service.

Goal:
- Placing live orders becomes isolated and mockable.

---

### Phase 4: Reconciliation/repricing/cancellation boundaries

Tasks:

1. Extract reconciliation service.
2. Update pending fills job.
3. Extract repricing service.
4. Extract stale order service.
5. Extract cancellation service.

Goal:
- Order lifecycle maintenance becomes separate from strategy/execution.

---

### Phase 5: Settlement/risk/alerts boundaries

Tasks:

1. Extract settlement service.
2. Extract daily log helper.
3. Extract risk service.
4. Extract event/alert adapter.

Goal:
- Outcome settlement, risk control, and notification are independently testable.

---

### Phase 6: Shrink or retire `KalshiLiveTrader`

Options:

1. Keep as thin facade for backward compatibility:
   - `select_trades` delegates to strategy service.
   - `execute_trades` delegates to execution service.
   - `reconcile_fills` delegates to reconciliation service.
   - `resolve_settled` delegates to settlement service.

2. Replace callers entirely:
   - orchestration jobs call services directly.
   - dashboard/API calls service layer.
   - `KalshiLiveTrader` is deleted or left as deprecated wrapper.

Preferred path:
- Keep facade through migration, then remove only after all callers are updated and tests pass.

---

## Files likely touched

Create:

- `src/trading/__init__.py`
- `src/trading/kalshi/__init__.py`
- `src/trading/kalshi/statuses.py`
- `src/trading/kalshi/state_machine.py`
- `src/trading/kalshi/strategy.py`
- `src/trading/kalshi/queue_service.py`
- `src/trading/kalshi/execution_service.py`
- `src/trading/kalshi/repricing_service.py`
- `src/trading/kalshi/reconciliation_service.py`
- `src/trading/kalshi/stale_order_service.py`
- `src/trading/kalshi/cancellation_service.py`
- `src/trading/kalshi/settlement_service.py`
- `src/trading/kalshi/risk_service.py`
- `src/trading/kalshi/events.py`
- `src/trading/kalshi/alert_adapter.py`

Modify gradually:

- `src/paper_trading/kalshi_live_trader.py`
- `src/orchestration/kalshi_refresh_job.py`
- `src/orchestration/kalshi_execute_approved_job.py`
- `src/orchestration/kalshi_pending_fills_job.py`
- `src/orchestration/kalshi_stale_fills_job.py`
- `src/orchestration/kalshi_execute_cancellations_job.py`
- dashboard approval API later, if this lane expands to UI/API state transitions.

Tests:

- `tests/test_kalshi_status_inventory.py`
- `tests/test_kalshi_state_machine.py`
- `tests/test_kalshi_strategy_policy.py`
- `tests/test_kalshi_queue_service.py`
- `tests/test_kalshi_execution_service.py`
- `tests/test_kalshi_repricing_service.py`
- `tests/test_kalshi_reconciliation_service.py`
- `tests/test_kalshi_stale_order_service.py`
- `tests/test_kalshi_cancellation_service.py`
- `tests/test_kalshi_settlement_service.py`
- `tests/test_kalshi_risk_service.py`
- `tests/test_kalshi_alert_adapter.py`

---

## Validation commands

Start with existing focused tests:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation)" -q`

After Phase 0/1:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py -q`

After service extraction:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_queue_service.py tests/test_kalshi_execution_service.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_settlement_service.py -q`

Use broader regression before any deployment:

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

---

## Risk controls / non-goals

Non-goals:

- Do not change live trading policy during structural refactor.
- Do not change thresholds, Kelly sizing, YES/NO policy, or exposure caps unless a test proves current behavior is wrong.
- Do not modify DB schema in the first pass.
- Do not replace all jobs at once.
- Do not remove `KalshiLiveTrader` until all callers are updated.

Safety controls:

- Preserve existing method return shapes while facade exists.
- Use mock Kalshi client for execution/repricing tests.
- Make service extraction behavior-preserving with before/after fixture comparisons.
- Keep circuit breaker and cancellation paths fail-closed.
- For live execution code, require explicit tests for duplicate prevention/idempotency before changing DB writes.

---

## First implementation PR recommendation

Start with a small, low-risk PR:

1. Add `src/trading/kalshi/statuses.py`.
2. Add `src/trading/kalshi/state_machine.py`.
3. Add tests for known valid/invalid transitions.
4. Replace a few low-risk status string references with constants only if tests pass.
5. Do not move execution/reconciliation logic yet.

Why first:

- It creates the language for the rest of the refactor.
- It has almost no runtime behavior risk.
- It makes future service extraction safer.

---

## Progress log

### 2026-05-18 initial compatibility-first slice

Created without modifying legacy `KalshiLiveTrader` behavior:

- `src/trading/__init__.py`
- `src/trading/kalshi/__init__.py`
- `src/trading/kalshi/statuses.py`
- `src/trading/kalshi/state_machine.py`
- `src/trading/kalshi/strategy.py`
- `tests/test_kalshi_status_inventory.py`
- `tests/test_kalshi_state_machine.py`
- `tests/test_kalshi_strategy_policy.py`

Validation run:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py -q`

Result: 14 passed, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; import src.trading.kalshi.statuses as statuses; import src.trading.kalshi.state_machine as sm; import src.trading.kalshi.strategy as strategy; print(old.KalshiLiveTrader.__name__, statuses.LiveOrderStatus.PENDING, sm.can_transition(sm.EntityType.LIVE_ORDER, 'pending', 'filled'), strategy.calculate_kelly_contracts(model_prob=0.45, yes_price=70, side='no', bankroll=100, kelly_fraction=0.125, max_contracts=50))"`

Result: `KalshiLiveTrader pending True 15`.

Important note: legacy class callers are untouched. New strategy module is side-effect-free and not wired into live execution yet.

### 2026-05-18 queue service slice

Created without modifying legacy `KalshiLiveTrader` behavior:

- `src/trading/kalshi/queue_service.py`
- `tests/test_kalshi_queue_service.py`

Queue service now owns the first extracted approval-queue seam:

- inserting selected trades into `kalshi_trade_queue` as `pending_approval`
- renewing recently expired `pending_approval` rows while markets are still open
- fetching visible pending approval rows for approval/reminder alerts
- fetching approved rows for execution
- partitioning approved rows into executable trade dicts vs expired queue IDs
- marking expired queue IDs
- marking execution results as `executed` or `failed`

Validation run:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py -q`

Result: 19 passed, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.queue_service import KalshiQueueService; from src.trading.kalshi.statuses import TradeQueueStatus; print(old.KalshiLiveTrader.__name__, KalshiQueueService.__name__, TradeQueueStatus.PENDING_APPROVAL)"`

Result: `KalshiLiveTrader KalshiQueueService pending_approval`.

Important note: legacy class callers are still untouched. Queue service is built and tested as the migration target, but not yet wired into live execution.

### 2026-05-18 queue service facade wiring slice

Wired the old `KalshiLiveTrader` queue-facing methods through `KalshiQueueService` while preserving method names and return shapes:

- `KalshiLiveTrader.propose_trades(...)` now delegates queue insertion to `KalshiQueueService.propose_trades(...)`.
- `KalshiLiveTrader.renew_expired_queue_trades(...)` now delegates queue renewal to `KalshiQueueService.renew_expired_pending_trades(...)`.
- `KalshiLiveTrader.execute_approved_trades(...)` now delegates approved-row fetch, expiry marking, and execution status marking to `KalshiQueueService`, while still using the existing `execute_trades(...)` live-order path.

Added/updated tests:

- `tests/test_kalshi_live_trader_queue_facade.py`
- `tests/test_kalshi_queue_service.py` now explicitly preserves the legacy rule that any returned result ticker is marked `executed`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_live_trader_queue_facade.py -q`

Result: 4 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py -q`

Result: 23 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation)" -q`

Result: 17 passed, 790 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.queue_service import KalshiQueueService; from src.trading.kalshi.statuses import TradeQueueStatus; print(old.KalshiLiveTrader.__name__, KalshiQueueService.__name__, TradeQueueStatus.PENDING_APPROVAL)"`

Result: `KalshiLiveTrader KalshiQueueService pending_approval`.

Important note: live order placement is still handled by the existing `execute_trades(...)` path. This slice only moved queue persistence/status responsibilities behind the new queue service.

### 2026-05-18 execution service slice

Extracted live order execution and live-order recording into `KalshiExecutionService` while keeping `KalshiLiveTrader` as the compatibility facade.

Created/updated:

- `src/trading/kalshi/execution_service.py`
- `tests/test_kalshi_execution_service.py`
- `tests/test_kalshi_live_trader_execution_facade.py`
- `src/paper_trading/kalshi_live_trader.py`

Execution service now owns:

- live taker order placement through the existing Kalshi client
- final balance checks before each order
- execution-time daily exposure cap enforcement
- existing 3-cent market-order sweep buffer behavior
- existing orderbook sweep acceptance/rejection/resizing logic
- fill/pending response normalization into legacy result dicts
- insertion into `kalshi_live_orders`
- trade-placed alert callback invocation

Facade behavior:

- `KalshiLiveTrader.execute_trades(...)` now delegates to `KalshiExecutionService.execute_trades(...)`.
- `KalshiLiveTrader._record_order(...)` now delegates to `KalshiExecutionService.record_order(...)` for compatibility with any direct/internal callers.
- `execute_approved_trades(...)` still routes through the facade, so approved queue execution now composes queue service + execution service.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py -q`

Result: 6 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py -q`

Result: 29 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution)" -q`

Result: 23 passed, 790 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.execution_service import KalshiExecutionService; from src.trading.kalshi.queue_service import KalshiQueueService; print(old.KalshiLiveTrader.__name__, KalshiExecutionService.__name__, KalshiQueueService.__name__)"`

Result: `KalshiLiveTrader KalshiExecutionService KalshiQueueService`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/execution_service.py src/trading/kalshi/queue_service.py src/paper_trading/kalshi_live_trader.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py`

Result: passed.

Important note: this is still a behavior-preserving extraction. It does not add new idempotency guards or change live-money thresholds/policy; those should be a separately tested execution-hardening follow-up.

### 2026-05-18 reconciliation service slice

Extracted Kalshi fill reconciliation into `KalshiReconciliationService` while keeping `KalshiLiveTrader` as the compatibility facade.

Created/updated:

- `src/trading/kalshi/reconciliation_service.py`
- `tests/test_kalshi_reconciliation_service.py`
- `tests/test_kalshi_live_trader_reconciliation_facade.py`
- `src/paper_trading/kalshi_live_trader.py`

Reconciliation service now owns:

- candidate query for pending orders and filled orders with missing `fill_price`
- optional `target_date` filtering
- resting-order lookup through the existing Kalshi client
- fast-path promotion of pending orders that already have fill data and are no longer resting
- derived fill-price repair for filled rows with cost/count but missing price
- safety rule: never cancel an order that already has fill data
- cancellation marking for non-resting orders with no fills and no fill data
- API fill aggregation with weighted average price, cost, fee, and `filled` status update

Facade behavior:

- `KalshiLiveTrader.reconcile_fills(...)` now delegates to `KalshiReconciliationService.reconcile_fills(...)`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py -q`

Result: 7 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py -q`

Result: 36 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution or reconciliation)" -q`

Result: 30 passed, 790 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.reconciliation_service import KalshiReconciliationService; from src.trading.kalshi.execution_service import KalshiExecutionService; print(old.KalshiLiveTrader.__name__, KalshiReconciliationService.__name__, KalshiExecutionService.__name__)"`

Result: `KalshiLiveTrader KalshiReconciliationService KalshiExecutionService`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/reconciliation_service.py src/paper_trading/kalshi_live_trader.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py`

Result: passed.

Important note: this is behavior-preserving extraction only. Stale-order review/cancellation queue logic remains a separate service boundary.

### 2026-05-18 settlement service slice

Extracted Kalshi live order settlement into `KalshiSettlementService` while keeping `KalshiLiveTrader` as the compatibility facade.

Created/updated:

- `src/trading/kalshi/settlement_service.py`
- `tests/test_kalshi_settlement_service.py`
- `tests/test_kalshi_live_trader_settlement_facade.py`
- `src/paper_trading/kalshi_live_trader.py`

Settlement service now owns:

- filled live-order candidate query from `kalshi_live_orders`
- date grouping and skip-today guard
- actual-stat callback coordination through the legacy `_fetch_actuals` seam
- legacy YES/NO win/loss PnL formulas
- null fill data guard that skips resolution and warns to run `reconcile_fills()` first
- missing-actual cancellation behavior with zero PnL
- live-order status/actual/PnL/resolved timestamp update
- resolution alert callback for won/lost outcomes
- daily log and streak update callback coordination

Facade behavior:

- `KalshiLiveTrader.resolve_settled(...)` now delegates to `KalshiSettlementService.resolve_settled(...)`.
- Legacy helper methods remain in `KalshiLiveTrader` and are injected as callbacks: `_fetch_actuals`, `_send_resolution_alert`, `_update_daily_log`, `_get_consecutive_losses`, `_update_streak`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py -q`

Result: 7 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py -q`

Result: 43 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution or reconciliation or settlement or resolved)" -q`

Result: 37 passed, 792 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.settlement_service import KalshiSettlementService; from src.trading.kalshi.reconciliation_service import KalshiReconciliationService; print(old.KalshiLiveTrader.__name__, KalshiSettlementService.__name__, KalshiReconciliationService.__name__)"`

Result: `KalshiLiveTrader KalshiSettlementService KalshiReconciliationService`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/settlement_service.py src/paper_trading/kalshi_live_trader.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py`

Result: passed.

Important note: this is behavior-preserving extraction only. Actual-stat SQL lookup, daily log aggregation, and alert formatting remain as legacy callback seams; the next extraction target is repricing/stale cancellation ownership.

### 2026-05-18 repricing service slice

Extracted stale resting-order repricing into `KalshiRepricingService` while keeping `KalshiLiveTrader` as the compatibility facade.

Created/updated:

- `src/trading/kalshi/repricing_service.py`
- `tests/test_kalshi_repricing_service.py`
- `tests/test_kalshi_live_trader_repricing_facade.py`
- `src/paper_trading/kalshi_live_trader.py`

Repricing service now owns:

- resting-order lookup through the existing Kalshi client
- pending DB row matching by `kalshi_order_id`
- orderbook price lookup via injected `_get_best_available_price`
- skip behavior for unavailable books, unchanged prices, large moves, and edge-retention failures
- cancel-and-replace behavior for retained-edge reprices
- YES/NO replacement order price fields with the preserved 3-cent buffer
- old-order cancellation marking when replacement fails
- old-order cancellation plus new `kalshi_live_orders` insertion when replacement succeeds
- filled vs pending replacement normalization and fee/cost calculation

Facade behavior:

- `KalshiLiveTrader.reprice_stale_orders(...)` now delegates to `KalshiRepricingService.reprice_stale_orders(...)`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py -q`

Result: 7 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py -q`

Result: 50 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution or reconciliation or settlement or resolved or reprice or repricing)" -q`

Result: 44 passed, 792 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.repricing_service import KalshiRepricingService; from src.trading.kalshi.settlement_service import KalshiSettlementService; print(old.KalshiLiveTrader.__name__, KalshiRepricingService.__name__, KalshiSettlementService.__name__)"`

Result: `KalshiLiveTrader KalshiRepricingService KalshiSettlementService`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/repricing_service.py src/paper_trading/kalshi_live_trader.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py`

Result: passed.

Important note: this is behavior-preserving extraction only. The human cancellation review/execution queue is still a separate boundary.

### 2026-05-18 stale-order/cancellation service slice

Extracted stale pending-order review and approved cancellation execution into `KalshiCancellationService`, and rewired the two orchestration jobs to call the service directly.

Created/updated:

- `src/trading/kalshi/cancellation_service.py`
- `tests/test_kalshi_cancellation_service.py`
- `tests/test_kalshi_cancellation_jobs.py`
- `src/orchestration/kalshi_stale_fills_job.py`
- `src/orchestration/kalshi_execute_cancellations_job.py`

Cancellation service now owns:

- ticker game-time parsing fallback for stale detection
- stale pending-order detection via DB `game_start_time`, ticker parse, or old `game_date`
- dedupe against non-rejected `kalshi_cancel_queue` rows
- inserting newly stale orders as `pending_review` for human review
- optional alert callback for review notifications
- fetching `approved` cancel queue rows after human approval
- authenticated Kalshi API cancellation execution
- marking cancel queue rows `executed` on success and `failed` with `cancel_error` on API failure
- marking the corresponding pending live order `cancelled` only after API cancellation succeeds

Job behavior:

- `kalshi_stale_fills_job.py` now delegates detection/enqueue to `KalshiCancellationService.enqueue_stale_orders_for_review(...)` and keeps Discord formatting in a job-level adapter callback.
- `kalshi_execute_cancellations_job.py` now delegates approved cancellation execution to `KalshiCancellationService.execute_approved_cancellations(...)`.
- Human review and execution are now separate service entrypoints: stale detection only creates review rows; API cancellation only touches rows already approved by the dashboard.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_cancellation_service.py tests/test_kalshi_cancellation_jobs.py -q`

Result: 8 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py tests/test_kalshi_cancellation_service.py tests/test_kalshi_cancellation_jobs.py -q`

Result: 58 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution or reconciliation or stale)" -q`

Result: 46 passed, 802 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "from src.trading.kalshi.cancellation_service import KalshiCancellationService, parse_game_time_from_ticker; import src.orchestration.kalshi_stale_fills_job as stale; import src.orchestration.kalshi_execute_cancellations_job as execute; print(KalshiCancellationService.__name__, callable(parse_game_time_from_ticker), callable(stale.main), callable(execute.main))"`

Result: `KalshiCancellationService True True True`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/cancellation_service.py src/orchestration/kalshi_stale_fills_job.py src/orchestration/kalshi_execute_cancellations_job.py src/paper_trading/kalshi_live_trader.py`

Result: passed.

Important note: `kalshi_cancel_queue` successful execution now uses the centralized state-machine terminal status `executed`; the corresponding live order remains `cancelled`. The previous job-local SQL used `status='cancelled'` for the cancel queue, which conflicted with the existing `CancelQueueStatus` inventory and transition tests.

### 2026-05-18 risk service slice

Extracted live-money risk checks and config/halt helpers into `KalshiRiskService` while keeping `KalshiLiveTrader` as the compatibility facade.

Created/updated:

- `src/trading/kalshi/risk_service.py`
- `tests/test_kalshi_risk_service.py`
- `tests/test_kalshi_live_trader_risk_facade.py`
- `src/paper_trading/kalshi_live_trader.py`

Risk service now owns:

- singleton `kalshi_live_trading_config` bootstrap
- config readback
- persistent halt flag setting and force-resume clearing
- consecutive-loss streak persistence
- daily realized P&L lookup
- consecutive-loss streak calculation
- portfolio high-water-mark ratchet
- drawdown floor calculation using total portfolio value, not cash only
- balance API failure block
- manual halt block
- drawdown halt plus persistent `is_halted=true`
- daily loss pause without persistent halt
- consecutive-loss pause without persistent halt

Facade behavior:

- `KalshiLiveTrader.check_circuit_breakers(...)` now delegates to `KalshiRiskService.check_circuit_breakers(...)`.
- Legacy helper methods now delegate to `KalshiRiskService`: `_ensure_config`, `_get_config`, `_set_halted`, `_update_streak`, `_get_daily_pnl`, `_get_consecutive_losses`.
- Circuit-breaker alert formatting/dedup remains in the legacy `_send_circuit_breaker_alert` callback seam for the later alert adapter slice.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_risk_service.py tests/test_kalshi_live_trader_risk_facade.py -q`

Result: 11 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py tests/test_kalshi_cancellation_service.py tests/test_kalshi_cancellation_jobs.py tests/test_kalshi_risk_service.py tests/test_kalshi_live_trader_risk_facade.py -q`

Result: 69 passed, 1 existing pytest-asyncio warning.

Broader Kalshi-targeted regression:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue or approval or cancellation or execution or reconciliation or stale or risk or circuit or halt)" -q`

Result: 57 passed, 802 deselected, 1 existing pytest-asyncio warning.

Compatibility smoke check:

`venv/Scripts/python.exe -c "import src.paper_trading.kalshi_live_trader as old; from src.trading.kalshi.risk_service import KalshiRiskService; from src.trading.kalshi.cancellation_service import KalshiCancellationService; print(old.KalshiLiveTrader.__name__, KalshiRiskService.__name__, KalshiCancellationService.__name__)"`

Result: `KalshiLiveTrader KalshiRiskService KalshiCancellationService`.

Compile check:

`venv/Scripts/python.exe -m py_compile src/trading/kalshi/risk_service.py src/paper_trading/kalshi_live_trader.py tests/test_kalshi_risk_service.py tests/test_kalshi_live_trader_risk_facade.py`

Result: passed.

Important note: this is behavior-preserving extraction. Alert formatting/dedup still lives in `KalshiLiveTrader._send_circuit_breaker_alert` and should move in the alert adapter slice.

### 2026-05-18 alert/event adapter slice

Extracted Kalshi notification formatting/routing into domain events plus `KalshiAlertAdapter`, and wired the first alert callsites through the adapter while preserving existing Discord sender behavior.

Created/updated:

- `src/trading/kalshi/events.py`
- `src/trading/kalshi/alert_adapter.py`
- `tests/test_kalshi_alert_adapter.py`
- `src/paper_trading/kalshi_live_trader.py`
- `src/orchestration/kalshi_refresh_job.py`

Alert/event adapter now owns:

- domain event dataclasses for trade placed, order resolved, order filled, circuit breaker, approval needed, approval reminder, trade execution failure, stale-order review, and high-edge markets
- conversion from domain events to the existing Discord alert senders
- circuit-breaker daily dedupe through `kalshi_live_trading_config.last_circuit_alert_at`
- non-fatal alert dispatch failure handling/logging
- shared queue-approval payload formatting

Current wiring:

- `KalshiLiveTrader._send_circuit_breaker_alert(...)` emits `CircuitBreakerTripped` through `KalshiAlertAdapter`.
- `KalshiLiveTrader._send_trade_placed_alert(...)` emits `TradePlaced` through `KalshiAlertAdapter`.
- `KalshiLiveTrader._send_resolution_alert(...)` emits `OrderResolved` through `KalshiAlertAdapter`.
- `kalshi_refresh_job.py` emits `TradeApprovalNeeded`, `TradeApprovalReminder`, and `HighEdgeMarketsFound` through `KalshiAlertAdapter`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py tests/test_kalshi_queue_service.py tests/test_kalshi_live_trader_queue_facade.py tests/test_kalshi_execution_service.py tests/test_kalshi_live_trader_execution_facade.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py tests/test_kalshi_settlement_service.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py tests/test_kalshi_cancellation_service.py tests/test_kalshi_cancellation_jobs.py tests/test_kalshi_risk_service.py tests/test_kalshi_live_trader_risk_facade.py tests/test_kalshi_alert_adapter.py -q`

Result: 73 passed, 1 existing pytest-asyncio warning.

Important note: this is still a compatibility-first adapter slice. Some lifecycle jobs and service callback seams may still instantiate or call through `KalshiLiveTrader`; the remaining migration work is to wire more jobs directly to focused services, split selection/actuals/daily-log responsibilities, and thin or retire the facade.

### 2026-05-18 strategy facade wiring slice

Wired `KalshiLiveTrader.select_trades(...)` through the pure strategy service while preserving the existing public method and returned legacy trade-dict shape.

Created/updated:

- `src/paper_trading/kalshi_live_trader.py`
- `src/trading/kalshi/strategy.py`
- `tests/test_kalshi_live_trader_strategy_facade.py`

Current ownership split:

- `KalshiLiveTrader.select_trades(...)` still owns side-effectful DB/API loading:
  - per-sport environment gate
  - real Kalshi balance lookup
  - open position lookup
  - candidate SQL loading from `kalshi_markets`
  - existing live order / pending queue lookups
  - daily exposure lookup
  - best-effort game-start-time lookup
- `src/trading/kalshi/strategy.py` now owns deterministic policy:
  - side choice
  - supported-stat filtering
  - existing/queued player-stat dedupe
  - volume/spread/price filters
  - structural skip direction filtering
  - MLB allowed-direction filtering
  - star-hitter NO filter
  - max-edge sanity filter
  - sportsbook-aligned line preference/override rule
  - Kelly sizing, position-cap, bankroll, and daily-exposure cap sizing
  - legacy-compatible `TradeIntent.as_legacy_dict()` output

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_live_trader_strategy_facade.py tests/test_kalshi_strategy_policy.py -q`

Result: 6 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 74 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is the first live `select_trades` strategy wiring slice. The facade still does DB/API loading by design; future work should add richer parity fixtures and then continue thinning any remaining selection-adjacent helper code.

### 2026-05-18 selection input loader slice

Extracted the side-effectful `select_trades(...)` loading path into a dedicated selection/input loader while preserving the facade method and live-money selection policy.

Created/updated:

- `src/trading/kalshi/selection_loader.py`
- `src/paper_trading/kalshi_live_trader.py`
- `tests/test_kalshi_live_trader_strategy_facade.py`

Current ownership split:

- `KalshiLiveTrader.select_trades(...)` now only:
  - constructs `KalshiSelectionInputLoader`
  - passes legacy strategy knobs from the facade
  - calls `select_trade_intents(...)`
  - converts `TradeIntent` objects back to legacy trade dictionaries
  - logs selected count/exposure
- `src/trading/kalshi/selection_loader.py` now owns side-effectful pre-strategy loading:
  - per-sport environment gate
  - Kalshi balance lookup
  - open position lookup
  - candidate SQL loading from `kalshi_markets`
  - existing live order / pending queue lookups
  - current daily exposure lookup
  - MLB allowed-direction config loading
  - game-start-time lookup
  - conversion from DB rows to `TradeCandidate` objects
  - construction of `StrategyConfig`
- `src/trading/kalshi/strategy.py` remains side-effect-free and owns deterministic selection/sizing policy.

TDD note:

- Added `test_select_trades_delegates_side_effect_loading_to_selection_input_loader` and verified RED because `KalshiSelectionInputLoader` was not yet imported/wired on the facade module.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_live_trader_strategy_facade.py tests/test_kalshi_strategy_policy.py -q`

Result: 7 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/paper_trading/kalshi_live_trader.py src/trading/kalshi/strategy.py src/trading/kalshi/selection_loader.py tests/test_kalshi_live_trader_strategy_facade.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 75 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is still behavior-preserving extraction. It does not change edge math, Kelly sizing, YES/NO policy, exposure caps, duplicate filters, or legacy trade dict shapes. The next selection-adjacent cleanup would be either adding direct unit tests for `KalshiSelectionInputLoader` edge cases or extracting actuals/daily-log callback seams from settlement.

### 2026-05-18 actuals adapter slice

Extracted the filled-order actual-stat lookup callback from `KalshiLiveTrader` into a focused adapter while preserving settlement behavior.

Created/updated:

- `src/trading/kalshi/actuals_adapter.py`
- `src/paper_trading/kalshi_live_trader.py`
- `tests/test_kalshi_actuals_adapter.py`
- `tests/test_kalshi_live_trader_settlement_facade.py`

Current ownership split:

- `src/trading/kalshi/actuals_adapter.py` now owns actual-stat reads for settlement:
  - uses the existing `NBA_STAT_RESOLUTION` / `MLB_STAT_RESOLUTION` mappings from the paper trader
  - preserves NBA `s.min > 0` filtering
  - preserves MLB `did_not_play` handling as missing actuals
  - returns `{(player_id, stat_type): actual_or_none}` for settlement
- `KalshiLiveTrader.resolve_settled(...)` now injects `KalshiActualsAdapter(self.engine).fetch_actuals` into `KalshiSettlementService`.
- `KalshiLiveTrader._fetch_actuals(...)` remains only as a compatibility wrapper delegating to the adapter.
- `KalshiSettlementService` still owns settlement status/PnL policy and continues to call the injected `fetch_actuals` callback.

TDD note:

- Added `tests/test_kalshi_actuals_adapter.py` and verified RED first because `src.trading.kalshi.actuals_adapter` did not exist.
- Updated facade wiring test to require the actuals adapter injection rather than the legacy `_fetch_actuals` method.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_actuals_adapter.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_settlement_service.py -q`

Result: 9 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/paper_trading/kalshi_live_trader.py src/trading/kalshi/actuals_adapter.py src/trading/kalshi/settlement_service.py tests/test_kalshi_actuals_adapter.py tests/test_kalshi_live_trader_settlement_facade.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 77 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving. It does not change settlement formulas, terminal statuses, missing-actual behavior, alert behavior, daily-log updates, or streak updates. Next cleanup is step 3: extract the daily log / ledger service.

### 2026-05-18 daily ledger service slice

Extracted the live daily log / ledger rollup callback from `KalshiLiveTrader` into a focused service while preserving the existing aggregation and balance formulas.

Created/updated:

- `src/trading/kalshi/daily_ledger_service.py`
- `src/paper_trading/kalshi_live_trader.py`
- `tests/test_kalshi_daily_ledger_service.py`
- `tests/test_kalshi_live_trader_settlement_facade.py`

Current ownership split:

- `src/trading/kalshi/daily_ledger_service.py` now owns live daily ledger aggregation:
  - reads resolved/pending counts from `kalshi_live_orders`
  - preserves `total_cost` as won/lost order cost only
  - preserves `roi_pct = total_pnl / total_cost * 100` with zero-cost guard
  - preserves previous-log cumulative P&L and balance-after carry-forward
  - preserves fallback to `starting_bankroll` when there is no prior daily log row
  - upserts into `kalshi_live_trading_daily_log`
- `KalshiLiveTrader.resolve_settled(...)` now injects `KalshiDailyLedgerService(...).update_daily_log` into `KalshiSettlementService`.
- `KalshiLiveTrader._update_daily_log(...)` remains only as a compatibility wrapper delegating to the ledger service.
- `KalshiSettlementService` still owns when to call the injected daily-log callback after each settled game date.

TDD note:

- Added `tests/test_kalshi_daily_ledger_service.py` and verified RED first because `src.trading.kalshi.daily_ledger_service` did not exist.
- Updated facade wiring test to require `KalshiDailyLedgerService` injection rather than the legacy `_update_daily_log` method.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_daily_ledger_service.py tests/test_kalshi_live_trader_settlement_facade.py tests/test_kalshi_settlement_service.py -q`

Result: 10 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/paper_trading/kalshi_live_trader.py src/trading/kalshi/daily_ledger_service.py src/trading/kalshi/settlement_service.py tests/test_kalshi_daily_ledger_service.py tests/test_kalshi_live_trader_settlement_facade.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 80 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving. It does not change settlement formulas, terminal statuses, actuals lookup, alert behavior, streak updates, or the daily ledger rollup formulas.

### 2026-05-18 pending fills job direct-service slice

Started the step-4 orchestration migration by moving the pending-fills polling job off the `KalshiLiveTrader` facade and onto the focused reconciliation service.

Created/updated:

- `src/orchestration/kalshi_pending_fills_job.py`
- `tests/test_kalshi_pending_fills_job.py`

Current ownership split:

- `kalshi_pending_fills_job.py` still owns job-level concerns:
  - env gating for `KALSHI_API_KEY` / `DATABASE_URL`
  - zero-API-call early exit when there are no pending orders
  - logging and recent-filled alert query
- `KalshiReconciliationService` now owns the fill reconciliation behavior for this job directly:
  - candidate row lookup
  - Kalshi resting/fill API checks
  - promote/derive/cancel/update DB behavior
- `KalshiLiveTrader` is no longer instantiated by the pending-fills job.

TDD note:

- Added `tests/test_kalshi_pending_fills_job.py` and verified RED first because the job did not expose direct `create_engine` / `KalshiClient` / `KalshiReconciliationService` wiring for monkeypatching and still instantiated the facade.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_pending_fills_job.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_live_trader_reconciliation_facade.py -q`

Result: 8 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/orchestration/kalshi_pending_fills_job.py src/trading/kalshi/reconciliation_service.py tests/test_kalshi_pending_fills_job.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 81 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is the first step-4 orchestration-job slice, not a behavior change. Remaining direct-facade orchestration callsites still include approved execution, reprice stale, and live resolution/trading portions of refresh.

### 2026-05-18 approved execution job direct-service slice

Continued step-4 orchestration migration by moving the approved-trade execution job off the `KalshiLiveTrader` facade and onto focused services.

Created/updated:

- `src/orchestration/kalshi_execute_approved_job.py`
- `tests/test_kalshi_execute_approved_job.py`

Current ownership split:

- `kalshi_execute_approved_job.py` still owns job-level concerns:
  - `KALSHI_LIVE_TRADING_ENABLED` gating
  - approved-preview logging and dry-run output
  - Kalshi client credential gating
  - elapsed-time/result-count logging
- `KalshiRiskService` owns circuit-breaker/config checks before execution.
- `KalshiQueueService` owns approved-row fetch, expiry marking, and executed/failed queue status updates.
- `KalshiExecutionService` owns live order placement and `kalshi_live_orders` recording.
- `KalshiAlertAdapter` owns failed-execution notification dispatch for job-level failures.
- `KalshiLiveTrader` is no longer instantiated by the approved execution job.

TDD note:

- Added `tests/test_kalshi_execute_approved_job.py` coverage that forbids `KalshiLiveTrader` instantiation and verifies direct queue/risk/execution service wiring.
- Added a RED test for failed execution alerts going through `KalshiAlertAdapter`; it initially failed because the job still imported Discord alert senders directly, then passed after routing through `TradeExecutionFailed`.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_execute_approved_job.py -q`

Result: 2 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_execute_approved_job.py tests/test_kalshi_queue_service.py tests/test_kalshi_execution_service.py tests/test_kalshi_risk_service.py tests/test_kalshi_alert_adapter.py -q`

Result: 24 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/orchestration/kalshi_execute_approved_job.py tests/test_kalshi_execute_approved_job.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 83 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving orchestration migration. Remaining direct-facade orchestration callsites still include stale repricing and live resolution/trading portions of refresh.

### 2026-05-18 stale repricing job direct-service slice

Continued step-4 orchestration migration by moving the stale resting-order repricing job off the `KalshiLiveTrader` facade and onto `KalshiRepricingService`.

Created/updated:

- `src/orchestration/kalshi_reprice_stale_job.py`
- `tests/test_kalshi_reprice_stale_job.py`

Current ownership split:

- `kalshi_reprice_stale_job.py` still owns job-level concerns:
  - `KALSHI_LIVE_TRADING_ENABLED` gating
  - Kalshi client credential gating
  - dry-run logging of current resting orders
  - elapsed-time/result-count logging
- `KalshiRepricingService` owns cancel-and-replace behavior:
  - resting-order lookup and pending DB match
  - best-available-price callback use
  - sweep max and edge-retention checks
  - API cancel and replacement order placement
  - old-order cancellation and replacement DB row insertion
- `KalshiLiveTrader` is no longer instantiated by the stale repricing job.

TDD note:

- Added `tests/test_kalshi_reprice_stale_job.py` coverage that forbids `KalshiLiveTrader` instantiation and verifies direct `KalshiRepricingService` wiring.
- Added dry-run coverage that also forbids facade instantiation and verifies the job lists resting orders from `KalshiClient` directly.
- Both tests were verified RED first because the job still instantiated `KalshiLiveTrader`, then passed after direct-service wiring.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_reprice_stale_job.py -q`

Result: 2 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_reprice_stale_job.py tests/test_kalshi_repricing_service.py tests/test_kalshi_live_trader_repricing_facade.py -q`

Result: 9 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/orchestration/kalshi_reprice_stale_job.py tests/test_kalshi_reprice_stale_job.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 85 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving orchestration migration. Remaining direct-facade orchestration callsites are now concentrated in the live resolution/trading portions of `kalshi_refresh_job.py` and any final compatibility-only `KalshiLiveTrader` consumers.

### 2026-05-18 refresh job live-resolution direct-service slice

Started the final `kalshi_refresh_job.py` migration by moving the live resolution/reconciliation portions off the `KalshiLiveTrader(resolve_only=True)` facade.

Created/updated:

- `src/orchestration/kalshi_refresh_job.py`
- `tests/test_kalshi_refresh_job_direct_services.py`

Current ownership split:

- `kalshi_refresh_job.py` still owns orchestration and summary behavior:
  - resolve-only mode routing
  - step 4.5a live-resolution timing
  - error handling and summary updates
- `_run_live_resolution(...)` now composes focused services directly:
  - `KalshiReconciliationService` for fill reconciliation
  - `KalshiSettlementService` for filled-order settlement
  - `KalshiActualsAdapter` for actual-stat lookup
  - `KalshiDailyLedgerService` for daily ledger rollup
  - `KalshiRiskService` for consecutive-loss streak read/update callbacks
  - `KalshiAlertAdapter` via `OrderResolved` for resolution alerts
- `KalshiLiveTrader(resolve_only=True)` is no longer instantiated by resolve-only mode or step 4.5a.

TDD note:

- Added tests that install a forbidden `KalshiLiveTrader` and verify:
  - resolve-only mode uses direct services and reconciles all dates (`target_date=None`), matching prior resolve-only behavior
  - normal step 4.5a uses direct services and reconciles the active `target_date`
- Both tests were verified RED first because refresh still instantiated `KalshiLiveTrader(resolve_only=True)`, then passed after direct service composition.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py -q`

Result: 2 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py tests/test_kalshi_reconciliation_service.py tests/test_kalshi_settlement_service.py tests/test_kalshi_actuals_adapter.py tests/test_kalshi_daily_ledger_service.py tests/test_kalshi_risk_service.py tests/test_kalshi_alert_adapter.py -q`

Result: 32 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/orchestration/kalshi_refresh_job.py tests/test_kalshi_refresh_job_direct_services.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 87 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving orchestration migration for live resolution only. `kalshi_refresh_job.py` still uses `KalshiLiveTrader` in step 4.5b for live trade selection/queue proposal; that should be the next sub-slice.

### 2026-05-18 refresh job live-trading direct-service slice

Completed the step 4.5b `kalshi_refresh_job.py` migration by moving live risk checks, selection, pending-queue lookup/renewal, and queue proposal off `KalshiLiveTrader` and onto focused services.

Created/updated:

- `src/orchestration/kalshi_refresh_job.py`
- `tests/test_kalshi_refresh_job_direct_services.py`

Current ownership split:

- `kalshi_refresh_job.py` still owns orchestration and summary behavior:
  - global `KALSHI_LIVE_TRADING_ENABLED` gating
  - per-sport `{SPORT}_TRADING_ENABLED` gating before queue renew/proposal
  - step 4.5b result summaries
  - approval/reminder alert routing
  - live-trading error handling
- `_run_live_trading(...)` now composes focused services directly:
  - `KalshiRiskService` for config row initialization and circuit-breaker checks
  - `KalshiQueueService` for expired pending-trade renewal, visible pending-approval lookup, and trade proposal insertion
  - `KalshiSelectionInputLoader` for side-effectful market/API/env loading
  - `select_trade_intents(...)` for pure strategy selection
  - `KalshiAlertAdapter` via `CircuitBreakerTripped`, `TradeApprovalNeeded`, and `TradeApprovalReminder`
- `KalshiLiveTrader` is no longer instantiated by `kalshi_refresh_job.py` for either live resolution or live trade selection/queue proposal.

TDD note:

- Added `tests/test_kalshi_refresh_job_direct_services.py::test_refresh_live_trading_step_uses_direct_risk_selection_and_queue_services`.
- Verified RED first: the test failed because step 4.5b still instantiated `KalshiLiveTrader`.
- Implemented the smallest direct-service wiring and verified GREEN.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py::test_refresh_live_trading_step_uses_direct_risk_selection_and_queue_services -q`

RED result before implementation: failed with `kalshi_refresh_job must not instantiate KalshiLiveTrader`.

GREEN result after implementation: 1 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py -q`

Result: 3 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py tests/test_kalshi_queue_service.py tests/test_kalshi_risk_service.py tests/test_kalshi_live_trader_strategy_facade.py tests/test_kalshi_strategy_policy.py -q`

Result: 24 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m py_compile src/orchestration/kalshi_refresh_job.py tests/test_kalshi_refresh_job_direct_services.py && git diff --check -- src/orchestration/kalshi_refresh_job.py tests/test_kalshi_refresh_job_direct_services.py`

Result: passed.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 88 passed, 797 deselected, 1 existing pytest-asyncio warning.

Important note: this is behavior-preserving orchestration migration for live trading. It keeps global and per-sport env gating, circuit-breaker/config initialization, queue renewal before selection, pending-reminder behavior when no new trades are selected, and legacy trade dict queue shapes.

### 2026-05-18 old live-trader facade removal

Completed the decision/removal slice for the old `KalshiLiveTrader` class. After proving orchestration no longer imports the facade, the old source file and facade-only tests were removed.

Created/updated/removed:

- Added `tests/test_kalshi_live_trader_removed.py`
- Removed `src/paper_trading/kalshi_live_trader.py`
- Removed facade-only tests:
  - `tests/test_kalshi_live_trader_execution_facade.py`
  - `tests/test_kalshi_live_trader_queue_facade.py`
  - `tests/test_kalshi_live_trader_reconciliation_facade.py`
  - `tests/test_kalshi_live_trader_repricing_facade.py`
  - `tests/test_kalshi_live_trader_risk_facade.py`
  - `tests/test_kalshi_live_trader_settlement_facade.py`
  - `tests/test_kalshi_live_trader_strategy_facade.py`
- Updated service docstrings/comments to remove stale facade language.

Current ownership split:

- There is no production `KalshiLiveTrader` class/file anymore.
- Orchestration jobs call focused services directly.
- `tests/test_kalshi_live_trader_removed.py` now guards against accidental reintroduction by asserting:
  - `src/paper_trading/kalshi_live_trader.py` does not exist
  - production `src/**/*.py` does not reference `KalshiLiveTrader` or `kalshi_live_trader`

TDD note:

- Added the removal/inventory test before deleting the facade.
- Verified RED first: it failed because `src/paper_trading/kalshi_live_trader.py` still existed and service comments still referenced `KalshiLiveTrader`.
- Deleted the facade and facade-only tests, then removed stale production references until the inventory test passed.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_live_trader_removed.py -q`

RED result before implementation: 2 failed because the old facade file existed and production references remained.

GREEN result after implementation: 2 passed, 1 existing pytest-asyncio warning.

Important note: this slice intentionally removes backward compatibility for importing `src.paper_trading.kalshi_live_trader.KalshiLiveTrader`. All production callsites had already been migrated to focused services before this removal.

### 2026-05-18 shared live-trading config/docs cleanup

Completed the duplicated-helper cleanup and service documentation refresh after removing the old live-trader facade.

Created/updated:

- Added `src/trading/kalshi/live_trading_config.py`
- Added `tests/test_kalshi_live_trading_config.py`
- Updated `src/orchestration/kalshi_refresh_job.py`
- Updated `src/trading/kalshi/selection_loader.py`
- Updated `src/trading/kalshi/strategy.py`
- Re-checked service docstrings/comments for stale `KalshiLiveTrader` facade language.

Current ownership split:

- `src/trading/kalshi/live_trading_config.py` now owns shared live-trading config/constants and ticker-time helpers:
  - `SUPPORTED_STATS`
  - `SPORTSBOOK_LINE_FALLBACK_GAP`
  - `parse_game_time_from_ticker(...)`
  - `get_game_start_time(...)`
- `kalshi_refresh_job.py` imports shared config/helpers rather than defining duplicate supported stats, fallback gap, regex/month maps, or ticker parser helpers.
- `selection_loader.py` and `strategy.py` import the shared sportsbook-line fallback constant rather than redefining it independently.
- Service docstrings now describe the focused-service ownership directly, without saying the legacy facade remains.

TDD note:

- Added `tests/test_kalshi_live_trading_config.py` before implementation.
- Verified RED first: collection failed because `src.trading.kalshi.live_trading_config` did not exist.
- Implemented the shared config module and rewired refresh/selection/strategy imports until the duplication guard passed.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_live_trading_config.py -q`

RED result before implementation: collection error because `src.trading.kalshi.live_trading_config` did not exist.

GREEN result after implementation: 2 passed, 1 existing pytest-asyncio warning.

Important note: this is a behavior-preserving cleanup. It only centralizes shared constants/helpers and removes stale comments; it does not change live-money thresholds, selection policy, queue behavior, status semantics, or settlement/execution formulas.

### 2026-05-18 final paper/live parity guard and full Kalshi validation

Added final parity coverage after the facade removal and shared-config cleanup.

Created/updated:

- Added `tests/test_kalshi_paper_live_strategy_parity.py`
- Updated `src/paper_trading/kalshi_paper_trader.py`

Current ownership split:

- `src.trading.kalshi.live_trading_config.SUPPORTED_STATS` is now shared by live orchestration strategy code and the paper trader.
- `src.trading.kalshi.live_trading_config.SPORTSBOOK_LINE_FALLBACK_GAP` is now the single source for the sportsbook-aligned line fallback gap used by live strategy and paper selection.
- `tests/test_kalshi_paper_live_strategy_parity.py` verifies:
  - paper trader reuses the shared live-trading constants instead of keeping duplicated literals
  - paper `_kelly_contracts(...)` remains numerically identical to the live `calculate_kelly_contracts(...)` helper for representative YES/NO cases

TDD note:

- Added the parity test before implementation.
- Verified RED first: `test_paper_trader_uses_shared_live_trading_config_constants` failed because paper trading still had its own duplicated `SUPPORTED_STATS` dict.
- Rewired paper trading to import the shared config constants, then verified GREEN.

Validation runs:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_paper_live_strategy_parity.py -q`

GREEN result after implementation: 2 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests/test_kalshi_strategy_policy.py tests/test_kalshi_live_trading_config.py tests/test_kalshi_paper_live_strategy_parity.py tests/test_kalshi_live_trader_removed.py tests/test_kalshi_refresh_job_direct_services.py tests/test_kalshi_execute_approved_job.py tests/test_kalshi_reprice_stale_job.py tests/test_kalshi_pending_fills_job.py tests/test_kalshi_cancellation_jobs.py -q`

Result: 21 passed, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests -k "kalshi" -q`

Result: 81 passed, 797 deselected, 1 existing pytest-asyncio warning.

`venv/Scripts/python.exe -m pytest tests -q`

Result: 878 passed, 11 warnings.

Full-suite note:

- The first full-suite run surfaced an unrelated MLB quote-clean compatibility failure: `_fetch_lines_for_date(...)` no longer accepted the historical `game_ids=` keyword used by `tests/test_mlb_feature_store_as_of.py`.
- Added backward-compatible `game_ids=` support in `src/backtesting/mlb/run_mlb_sweep.py`; the targeted failing test then passed and the full suite passed.

`venv/Scripts/python.exe -m py_compile src/paper_trading/kalshi_paper_trader.py src/backtesting/mlb/run_mlb_sweep.py tests/test_kalshi_paper_live_strategy_parity.py && git diff --check -- src/paper_trading/kalshi_paper_trader.py src/backtesting/mlb/run_mlb_sweep.py tests/test_kalshi_paper_live_strategy_parity.py .hermes/plans/kalshi-live-trading-refactor-responsibility-plan-2026-05-18.md`

Result: passed.

Important note: this is still behavior-preserving. It does not route paper selection through live strategy yet; it adds a guard for the shared constants and Kelly sizing parity needed before that follow-up lane.

## Done when

- Live trading statuses and transitions are centralized and tested.
- Strategy can be tested without DB/API/Discord.
- Queue lifecycle has a single owner.
- Execution/repricing/reconciliation/settlement each have a focused service.
- Scheduled jobs call focused services instead of a god trader.
- `KalshiLiveTrader` is removed and guarded against reintroduction.
- Paper/live strategy parity can be enforced in the follow-up paper/live policy lane.
