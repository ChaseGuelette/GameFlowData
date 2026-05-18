# God-Class Tech Debt Refactor Index

> **For Hermes:** Use subagent-driven-development skill to implement these plans task-by-task after Chase approves a specific lane.

**Goal:** Turn the repo-wide god-class/god-module audit into actionable refactor lanes.

**Core diagnosis:** Yes — the essential issue is that several parts of GameFlowData have grown into god classes, god modules, or script-service hybrids that are hard to reach, test, refactor, and maintain. The problem is not just file size; it is mixed responsibility and ambiguous ownership of business-critical behavior.

**Important nuance:** The fix is not simply “make subclasses” or “split into more files.” The fix is to create clear responsibility boundaries:

- pure domain logic modules for deterministic rules/math
- typed configs/result contracts for promotion-critical workflows
- service classes for DB/API side effects
- adapters for external systems
- explicit state machines for lifecycle workflows
- thin orchestration entrypoints and API routes
- focused UI components/hooks for dashboard logic

---

## What counts as the anti-pattern

A class/module is high-risk when it owns 3+ of these at once:

- config parsing
- DB reads/writes
- external API calls
- business policy
- line/model/artifact discovery
- state transitions
- result serialization
- alert formatting/sending
- CLI/API route handling
- UI rendering
- test/promotion evidence semantics

This is exactly what made the MLB sweep hard to reason about: promotion-grade behavior was spread across a large entrypoint, duplicated config/result types, line-selection helpers, legacy harnesses, and artifact heuristics.

---

## Refactor lanes

### 1. MLB promotion/backtest architecture

Plan file:
- `.hermes/plans/mlb-promotion-backtest-architecture-refactor-2026-05-18.md`

Main issue:
- Duplicate `SweepConfig` / `SweepResult`, giant `run_mlb_sweep.py`, legacy harness ambiguity, and heuristic model/artifact discovery.

Primary goal:
- Make MLB promotion replay a typed, manifest-pinned, quote-clean-only path.

---

### 2. Kalshi live trading state machine

Plan file:
- `.hermes/plans/kalshi-live-trading-state-machine-refactor-2026-05-18.md`

Main issue:
- `KalshiLiveTrader` is a live-money god class and the trade lifecycle is distributed across multiple scheduled jobs and raw DB statuses.

Primary goal:
- Centralize live trade/order lifecycle as an explicit state machine with idempotent services.

---

### 3. Shared paper/live trading policy

Plan file:
- `.hermes/plans/paper-live-trading-policy-refactor-2026-05-18.md`

Main issue:
- Paper/live selection, sizing, resolution, and daily-log logic are duplicated across NBA, MLB, Kalshi, DFS, and arb traders.

Primary goal:
- Make paper/live evaluation use shared strategy and ledger primitives so paper results track live behavior.

---

### 4. Feature-store boundaries and train/backtest/inference parity

Plan file:
- `.hermes/plans/feature-store-boundary-refactor-2026-05-18.md`

Main issue:
- Training pipelines call feature-store private helpers; canonical feature assembly is not encoded as public API.

Primary goal:
- Expose explicit public feature assembly contracts and parity tests.

---

### 5. Dashboard god routes/components

Plan file:
- `.hermes/plans/dashboard-god-route-component-refactor-2026-05-18.md`

Main issue:
- Large route/component files combine fetching, mapping, math, mutation, and rendering. `dfs/page.tsx` also has a likely hook-order bug.

Primary goal:
- Extract pure domain functions/hooks/services and keep API routes/pages/components thin.

---

## Recommended execution order

1. Fix dashboard DFS hook-order issue first if touching frontend; it is a correctness bug, not just cleanup.
2. MLB promotion/backtest architecture next; it directly affects model promotion confidence.
3. Kalshi live trading state machine next; it affects live-money safety.
4. Shared paper/live trading policy after Kalshi boundaries are clear.
5. Feature-store boundary/parity work before more model architecture experimentation.
6. Scheduler and alert module cleanup can happen opportunistically after the trading/backtest lanes have stable service boundaries.

---

## Non-goals

- Do not rewrite everything at once.
- Do not split files mechanically just to reduce line count.
- Do not add inheritance trees unless there is a proven shared contract.
- Do not change model/trading behavior while refactoring unless a test proves the current behavior is wrong.
- Do not treat old legacy harness outputs as promotion evidence.

---

## Global validation standard

Every refactor lane should preserve behavior with tests before changing structure:

1. Characterize current behavior with focused tests or snapshot artifacts.
2. Extract pure logic/service boundary.
3. Re-run the same tests and compare outputs.
4. Add one regression test for the bug/ambiguity the refactor is meant to prevent.
5. For promotion/trading flows, include an explicit manifest/state/status assertion.
