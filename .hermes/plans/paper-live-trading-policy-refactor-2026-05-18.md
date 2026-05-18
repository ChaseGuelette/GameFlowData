# Paper/Live Trading Policy Refactor Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Reduce drift between paper trading, live trading, and backtest-style evaluation by extracting shared strategy and ledger primitives.

**Architecture:** Do not start with inheritance. Start with pure policy functions and small adapter interfaces. Paper/live should differ in execution backend, not in selection math unless explicitly configured.

**Tech Stack:** Python, pytest, existing `src/paper_trading` modules.

---

## Problem summary

Current high-risk files:

- `src/paper_trading/kalshi_live_trader.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/paper_trading/paper_trader.py`
- `src/paper_trading/mlb_paper_trader.py`
- `src/paper_trading/dfs_paper_trader.py`
- `src/paper_trading/arb_paper_trader.py`

Core issue:

Multiple traders independently implement:

- candidate selection
- price/edge filtering
- Kelly sizing
- duplicate prevention
- placement/persistence
- resolution
- daily log updates
- bankroll/performance summaries

This makes paper/live comparison fragile. A policy update can land in live but not paper, or NBA but not MLB.

---

## Target boundaries

Potential modules:

- `src/trading/policy/staking.py`
  - Kelly and bankroll stake sizing.
- `src/trading/policy/selection.py`
  - common edge/price/liquidity filters where applicable.
- `src/trading/ledger/paper_ledger.py`
  - insert/update/read paper bets/entries.
- `src/trading/ledger/resolution.py`
  - common win/loss/push and P&L math.
- `src/trading/ledger/daily_log.py`
  - daily rollup update semantics.
- `src/trading/adapters/`
  - sport/market-specific adapters for actuals and candidate rows.

---

## Bite-sized tasks

### Task 1: Extract shared staking/Kelly math

**Objective:** Centralize stake sizing used across paper/live/UI where possible.

**Files:**
- Create: `src/trading/policy/staking.py`
- Tests: `tests/test_trading_staking_policy.py`
- Modify one low-risk caller first, probably `src/paper_trading/mlb_paper_trader.py`.

**Acceptance:**
- Same input odds/probability/bankroll returns same stake as current implementation.
- Behavior is characterized by tests before replacing callers.

---

### Task 2: Extract common paper ledger result math

**Objective:** Centralize American odds P&L and win/loss/push calculations.

**Files:**
- Create: `src/trading/ledger/resolution.py`
- Tests: `tests/test_trading_resolution_math.py`
- Modify one caller after tests pass.

**Acceptance:**
- Tests cover over/under, push, positive/negative odds, and void/no-action if applicable.

---

### Task 3: Extract daily log update helper

**Objective:** Reduce duplicated daily summary update logic across traders.

**Files:**
- Create: `src/trading/ledger/daily_log.py`
- Tests: `tests/test_trading_daily_log.py`
- Modify one paper trader only.

**Acceptance:**
- Daily log fields match previous behavior for a fixture.
- Caller-specific table names/config are passed in, not hardcoded globally.

---

### Task 4: Define paper trader adapter protocol

**Objective:** Identify what differs by domain without creating an inheritance trap.

**Files:**
- Create: `src/trading/ledger/adapters.py`
- Tests: small type/behavior tests if useful.

**Adapter responsibilities:**
- candidate source
- actuals source
- table names
- domain-specific stat mapping
- domain-specific duplicate key

---

### Task 5: Align Kalshi paper/live strategy after state-machine work

**Objective:** Make paper and live Kalshi use one strategy policy.

**Files:**
- Likely modify:
  - `src/paper_trading/kalshi_live_trader.py`
  - `src/paper_trading/kalshi_paper_trader.py`
  - new `src/trading/kalshi/strategy.py`
- Tests:
  - `tests/test_kalshi_paper_live_strategy_parity.py`

**Acceptance:**
- Given the same candidate fixture, paper and live strategy produce equivalent trade intent unless an explicit paper/live config differs.

---

## Validation commands

`venv/Scripts/python.exe -m pytest tests/test_trading_staking_policy.py tests/test_trading_resolution_math.py tests/test_trading_daily_log.py -q`

For Kalshi parity:

`venv/Scripts/python.exe -m pytest tests/test_kalshi_paper_live_strategy_parity.py -q`

---

## Done when

- Stake sizing and P&L math live in shared tested modules.
- At least NBA/MLB paper traders share ledger primitives.
- Kalshi paper/live strategy policy has one source of truth.
- Domain-specific traders are adapters/orchestrators, not full independent ledgers.
