# Kalshi Live Trading State Machine Refactor Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Decompose `KalshiLiveTrader` and the surrounding scheduled jobs into an explicit, testable live-trade/order state machine.

**Architecture:** Preserve live behavior first. Introduce typed statuses and transition functions before moving execution/reconciliation logic. Keep external side effects behind adapters.

**Tech Stack:** Python, pytest, existing Kalshi client/jobs/tables.

---

## Problem summary

Current high-risk files:

- `src/paper_trading/kalshi_live_trader.py` — ~1,863 class lines, 26 methods.
- `src/orchestration/kalshi_refresh_job.py`
- `src/orchestration/kalshi_execute_approved_job.py`
- `src/orchestration/kalshi_pending_fills_job.py`
- `src/orchestration/kalshi_stale_fills_job.py`
- `src/orchestration/kalshi_execute_cancellations_job.py`
- `src/orchestration/scheduler.py`

Core issue:

The live-money lifecycle is distributed across jobs, raw DB statuses, and a god class. Scheduler timing and string status values are part of correctness.

---

## Target boundaries

Potential modules:

- `src/trading/kalshi/statuses.py`
  - enums/constants for queue/order/cancel statuses.
- `src/trading/kalshi/state_machine.py`
  - allowed transitions and validation.
- `src/trading/kalshi/strategy.py`
  - pure selection and sizing policy.
- `src/trading/kalshi/execution_service.py`
  - creates/cancels/reprices live orders through Kalshi API adapter.
- `src/trading/kalshi/reconciliation_service.py`
  - fill reconciliation and stale order handling.
- `src/trading/kalshi/settlement_service.py`
  - actuals fetch and settlement updates.
- `src/trading/kalshi/queue_service.py`
  - approval queue lifecycle.
- `src/trading/kalshi/alerts.py`
  - alert payload adapter, not Discord transport itself.

`KalshiLiveTrader` should either disappear or become a thin facade over these services.

---

## Bite-sized tasks

### Task 1: Inventory current status strings and transitions

**Objective:** Produce a test-backed status inventory without behavior changes.

**Files:**
- Create: `src/trading/kalshi/statuses.py`
- Test: `tests/test_kalshi_status_inventory.py`

**Acceptance:**
- Queue/order/cancel statuses are defined centrally.
- Existing modules import or at least are tested against the central definitions before behavior changes.

---

### Task 2: Add pure transition validation

**Objective:** Encode allowed lifecycle transitions in a pure module.

**Files:**
- Create: `src/trading/kalshi/state_machine.py`
- Test: `tests/test_kalshi_state_machine.py`

**Acceptance:**
- Tests cover expected transitions, e.g.:
  - pending_approval -> approved
  - approved -> executed
  - approved -> expired
  - pending -> filled
  - pending -> cancelled
  - filled -> resolved
- Invalid transitions fail loudly.

---

### Task 3: Extract pure strategy selection inputs/outputs

**Objective:** Separate selection policy from DB/API side effects.

**Files:**
- Create: `src/trading/kalshi/strategy.py`
- Test: `tests/test_kalshi_strategy_policy.py`
- Modify: `src/paper_trading/kalshi_live_trader.py` minimally.

**Acceptance:**
- Strategy function takes plain candidate rows/config and returns proposed trades.
- No DB/API/Discord calls inside strategy function.
- Existing `select_trades` output remains equivalent for a small fixture.

---

### Task 4: Extract queue service

**Objective:** Own proposal/approval/expiration behavior in one service.

**Files:**
- Create: `src/trading/kalshi/queue_service.py`
- Modify:
  - `src/paper_trading/kalshi_live_trader.py`
  - `src/orchestration/kalshi_execute_approved_job.py`
- Test: `tests/test_kalshi_queue_service.py`

**Acceptance:**
- Queue transitions use central state machine.
- HTTP/dashboard approval route can later call same service.

---

### Task 5: Extract execution/repricing service

**Objective:** Move live order execution and stale order repricing out of `KalshiLiveTrader`.

**Files:**
- Create: `src/trading/kalshi/execution_service.py`
- Modify:
  - `src/paper_trading/kalshi_live_trader.py`
  - `src/orchestration/kalshi_reprice_stale_job.py` if present / relevant caller.
- Test: `tests/test_kalshi_execution_service.py`

**Acceptance:**
- Execution service has a clear idempotency key or duplicate-prevention strategy.
- API adapter is mockable.

---

### Task 6: Extract reconciliation/settlement services

**Objective:** Separate fill polling from settlement resolution.

**Files:**
- Create:
  - `src/trading/kalshi/reconciliation_service.py`
  - `src/trading/kalshi/settlement_service.py`
- Modify:
  - `src/orchestration/kalshi_pending_fills_job.py`
  - `src/orchestration/kalshi_stale_fills_job.py`
  - `src/paper_trading/kalshi_live_trader.py`
- Tests:
  - `tests/test_kalshi_reconciliation_service.py`
  - `tests/test_kalshi_settlement_service.py`

**Acceptance:**
- Fill reconciliation does not settle markets.
- Settlement does not poll Kalshi fills.
- Both use central statuses/transitions.

---

## Validation commands

Start with existing focused tests if available:

`venv/Scripts/python.exe -m pytest tests -k "kalshi and (live or trader or fills or queue)" -q`

Then new tests:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_status_inventory.py tests/test_kalshi_state_machine.py tests/test_kalshi_strategy_policy.py -q`

---

## Done when

- Allowed statuses and transitions are centralized and tested.
- `KalshiLiveTrader` no longer owns strategy, queue, execution, reconciliation, settlement, persistence, and alerts all at once.
- Scheduled jobs call focused services.
- Paper/live strategy parity can be addressed in the next lane.
