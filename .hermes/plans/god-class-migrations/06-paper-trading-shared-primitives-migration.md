# Paper-Trading Shared Primitives Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to alter bankroll policy, staking formulas, resolution P&L, live/Kalshi selection rules, or database schemas.

**Goal:** Rebuild the paper-trading code around shared primitives for bankroll, staking, bet lifecycle, persistence, resolution, daily logs, and alerts while preserving each domain trader's current behavior.

**Architecture:** Preserve `PaperTrader`, `MLBPaperTrader`, `KalshiPaperTrader`, `ArbPaperTrader`, and DFS/user resolver entry points as compatibility facades. Extract shared calculations and lifecycle primitives first, then domain-specific selection/resolution adapters one at a time. Treat paper-trading behavior changes as separate evidence-backed strategy changes, not refactor side effects.

**Tech Stack:** Python, pandas, SQLAlchemy, existing Supabase/Postgres tables, Discord alert helpers, pytest, current orchestration jobs.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `infrastructure/railway-setup`
- GBrain query for paper trading / scheduler / arbitrage shared primitives returned no focused canonical page, so this doc is grounded primarily in live code plus global invariants.

Applied lessons:

1. Paper trading is not production order execution, but it drives strategy evidence and live-trading confidence; preserve semantics first.
2. Kalshi invariant: YES side is structurally negative by default; `KALSHI_ALLOW_YES_BETS` must remain false by default.
3. Kalshi invariant: unsupported stat types must remain blocked.
4. Empirical-CDF and Q10 lessons apply upstream to recommendations/edges; paper trader must not “correct” model probabilities during extraction.
5. Scheduler/Railway context matters because resolution and placement jobs run unattended.
6. DB writes should remain idempotent and scoped; no schema/DDL changes in this structural migration.

---

## Executive diagnosis

Paper trading is currently split by domain, but the domains duplicate the same lifecycle primitives with slightly different behavior:

- `src/paper_trading/paper_trader.py`
  - 790 total lines
  - 643 non-comment LOC
  - `PaperTrader`: 716 class lines, 14 methods
- `src/paper_trading/mlb_paper_trader.py`
  - 594 total lines
  - 493 non-comment LOC
  - `MLBPaperTrader`: 530 class lines, 9 methods
- `src/paper_trading/kalshi_paper_trader.py`
  - 1,253 total lines
  - 987 non-comment LOC
  - `KalshiPaperTrader`: 1,177 class lines, 13 methods
- `src/paper_trading/arb_paper_trader.py`
  - 488 total lines
  - 389 non-comment LOC
  - `ArbPaperTrader`: 458 class lines, 8 methods

Major callsites and consumers include:

- `src/orchestration/inference_job.py`
- `src/orchestration/mlb_inference_job.py`
- `src/orchestration/edge_refresh_job.py`
- `src/orchestration/mlb_daily_stats_job.py`
- `src/orchestration/daily_stats_job.py`
- `src/orchestration/kalshi_refresh_job.py`
- `src/orchestration/kalshi_daily_summary_job.py`
- `src/orchestration/arb_scan_job.py`
- `src/discord_bot/alerts.py`
- `src/discord_bot/services/paper_trading.py`
- `src/social/data_provider.py`
- paper-trading CLI scripts and analysis scripts under `src/paper_trading/` and `scripts/`

Existing tests are concentrated around NBA `PaperTrader` and Kalshi parity:

- `tests/test_paper_trader.py`
- `tests/test_kalshi_paper_live_strategy_parity.py`
- `tests/test_pipeline_resilience.py`

The migration needs to reduce duplication without collapsing intentionally different semantics between sportsbook, MLB, Kalshi, DFS, user bets, and arbitrage paper trading.

---

## Current ownership problems

### 1. Staking and odds math are duplicated

Current examples:

- `PaperTrader._american_to_decimal(...)`
- `PaperTrader._calculate_kelly_stake(...)`
- `MLBPaperTrader._american_to_decimal(...)`
- `MLBPaperTrader._calculate_kelly_stake(...)`
- `KalshiPaperTrader._kelly_contracts(...)`
- `ArbPaperTrader` has its own arb stake/cost model

Why this is wrong:

- Same bankroll/staking concepts drift by domain.
- Tests currently cover NBA calculations but not every duplicated variant.
- Kalshi contract sizing is intentionally different from sportsbook stake sizing and needs a distinct primitive, not copy-paste.

Target owners:

- `src/paper_trading/primitives/odds.py`
- `src/paper_trading/primitives/staking.py`
- `src/paper_trading/primitives/kalshi_contracts.py`
- `src/paper_trading/primitives/arb_staking.py`

Required compatibility:

- Existing private methods remain as wrappers during migration.
- Existing edge thresholds and max-bet caps remain unchanged.

---

### 2. Bet lifecycle statuses are implicit and table-specific

Current table/status families:

- `paper_bets`: `pending`, `won`, `lost`, `cancelled`
- `mlb_paper_bets`: similar sportsbook statuses
- `kalshi_paper_bets`: `pending`, `won`, `lost`, `cancelled`, `overflow`, `overflow_won`, `overflow_lost`, `overflow_cancelled`
- `arb_paper_bets`: arbitrage-specific pending/resolved statuses
- DFS/user tables have separate lifecycle conventions

Why this is wrong:

- Status strings are spread across selection, placement, resolution, daily logs, alert formatting, and analysis scripts.
- Overflow status handling is easy to break while refactoring Kalshi.
- User-bet resolvers explicitly mirror `PaperTrader.resolve_bets()` behavior but do not share primitives.

Target owners:

- `src/paper_trading/primitives/statuses.py`
- `src/paper_trading/primitives/lifecycle.py`

Tests:

- status families are enumerated and table-specific.
- overflow status transitions preserve current Kalshi behavior.
- daily-log grouping includes/excludes statuses exactly as current behavior does.

---

### 3. Placement persistence is duplicated and idempotency is fragile

Current insert/upsert owners:

- `PaperTrader.place_bets(...)`
- `MLBPaperTrader.place_bets(...)`
- `KalshiPaperTrader.place_bets(...)`
- `ArbPaperTrader.place_arbs(...)`
- `DfsPaperTrader` entry/leg writers

Why this is wrong:

- Each class builds raw SQL and idempotency rules inline.
- Table-specific `ON CONFLICT` behavior is hard to audit.
- Alert side effects are mixed with persistence.

Target owners:

- `src/paper_trading/storage/base.py`
- `src/paper_trading/storage/sportsbook_bets.py`
- `src/paper_trading/storage/mlb_bets.py`
- `src/paper_trading/storage/kalshi_bets.py`
- `src/paper_trading/storage/arb_bets.py`
- `src/paper_trading/storage/dfs_entries.py` later, only after core primitives are stable

Safety:

- No DB schema changes.
- Preserve current conflict keys.
- Preserve “only update pending rows” where currently present.

---

### 4. Resolution/P&L formulas are repeated and domain-sensitive

Current resolution owners:

- `PaperTrader.resolve_bets(...)`
- `MLBPaperTrader.resolve_bets(...)`
- `KalshiPaperTrader.resolve_bets(...)`
- `ArbPaperTrader.resolve_arbs(...)`
- `user_bet_resolver.py` mirrors `PaperTrader.resolve_bets()`
- `user_dfs_resolver.py` and `dfs_paper_trader.py` have separate contest/leg semantics

Kalshi example currently encodes:

- YES wins when `actual >= line`
- NO wins when `actual < line`
- no data older than 48h cancels
- YES winner P&L: `contracts * (100 - fill_price) / 100 - fee`
- NO winner P&L: `contracts * fill_price / 100 - fee`
- loser loses premium/cost side

Why this is wrong:

- P&L formulas are too important to remain duplicated in class bodies.
- Cancellation/staleness policy is strategy evidence, not incidental code.
- User bet resolution should reuse the same tested primitive instead of comments saying it mirrors.

Target owners:

- `src/paper_trading/resolution/sportsbook.py`
- `src/paper_trading/resolution/kalshi.py`
- `src/paper_trading/resolution/arb.py`
- `src/paper_trading/resolution/actuals.py`

Tests:

- fixture-level P&L parity for win/loss/cancel for each bet type.
- stale/no-data behavior preserved.
- overflow resolution prefixes preserved.

---

### 5. Daily log updates are repeated

Current methods:

- `PaperTrader._update_daily_log(...)`
- `MLBPaperTrader._update_daily_log(...)`
- `KalshiPaperTrader._update_daily_log(...)`
- `ArbPaperTrader._update_daily_log(...)`
- `DfsPaperTrader` daily log updates

Why this is wrong:

- Bankroll roll-forward and daily summaries can drift by domain.
- Alert and dashboard consumers rely on daily log table shapes.
- Some analysis jobs query logs and bet rows directly.

Target owners:

- `src/paper_trading/logging/daily_logs.py`
- table-specific adapters for current schemas

Safety:

- Preserve current log table names and columns.
- Do not rewrite dashboard/Discord query paths in the first pass.

---

### 6. Selection logic mixes strategy policy, SQL, staking, exposure caps, and alerts

Worst example:

- `KalshiPaperTrader.select_bets(...)`: 419-line method.

It owns:

- dynamic bankroll exposure cap
- NO-only default policy
- latest-market snapshot query
- same-day dedup/position accumulation
- existing exposure query and `prior_exposure`
- supported stat whitelist
- liquidity/spread/price filters
- BL probability fallback policy
- fee-adjusted edge calculations
- MLB allowed directions
- shared recommendation sanity filter
- star-batter filter
- sportsbook-alignment filter
- per-player/stat best candidate pool
- overflow storage

Target owners:

- `src/paper_trading/selection/kalshi_selector.py`
- `src/paper_trading/selection/sportsbook_selector.py`
- `src/paper_trading/selection/mlb_selector.py`
- `src/paper_trading/selection/exposure.py`
- `src/paper_trading/selection/filters.py`
- `src/paper_trading/selection/reasoning.py`

Non-goal:

- Do not tune selection filters during extraction.

---

### 7. Alerting side effects are embedded in placement/resolution

Current examples:

- `KalshiPaperTrader.place_bets(...)` sends Discord after DB write.
- `KalshiPaperTrader._send_trade_alert(...)`
- `discord_bot/alerts.py` formats many summaries from result dicts.

Target owner:

- `src/paper_trading/alerts.py`

Initial rule:

- Extract alert payload construction before changing alert delivery.

---

## Target design by responsibility

### A. `paper_trading/primitives/odds.py`

Pure odds conversion helpers.

### B. `paper_trading/primitives/staking.py`

Sportsbook Kelly stake policy and caps.

### C. `paper_trading/primitives/kalshi_contracts.py`

Kalshi fee-adjusted edge, contract sizing, premium/cost helpers, NO-only invariant helpers.

### D. `paper_trading/primitives/statuses.py`

Typed status constants and status-family helpers.

### E. `paper_trading/primitives/lifecycle.py`

Generic pending/resolved/cancelled lifecycle concepts.

### F. `paper_trading/storage/*.py`

Table-specific persistence adapters.

### G. `paper_trading/resolution/*.py`

P&L calculators and actuals lookup boundaries.

### H. `paper_trading/selection/*.py`

Domain-specific selection engines and filters.

### I. `paper_trading/logging/daily_logs.py`

Daily-log update adapters.

### J. Compatibility facades

Keep existing public classes and methods:

- `PaperTrader`
- `MLBPaperTrader`
- `KalshiPaperTrader`
- `ArbPaperTrader`
- CLI scripts under `src/paper_trading/`
- orchestration imports

---

## Refactor phases

### Phase 0: Characterization and inventory tests

Objective: Lock current public shape and high-value behavior before extraction.

Files:

- Existing: `tests/test_paper_trader.py`
- Existing: `tests/test_kalshi_paper_live_strategy_parity.py`
- Create: `tests/test_paper_trading_inventory.py`

Tests:

- public trader classes importable.
- key private calculation methods still exist until wrappers are migrated.
- table-specific status families characterized.
- Kalshi NO-only default characterized.
- Kalshi supported-stat whitelist remains wired.
- current placement conflict-key SQL snippets characterized if feasible with query-builder extraction.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_paper_trader.py tests/test_kalshi_paper_live_strategy_parity.py tests/test_paper_trading_inventory.py -q`

---

### Phase 1: Extract pure odds/staking primitives

Objective: Move the safest shared pure math first.

Files:

- Create: `src/paper_trading/primitives/__init__.py`
- Create: `src/paper_trading/primitives/odds.py`
- Create: `src/paper_trading/primitives/staking.py`
- Create: `src/paper_trading/primitives/kalshi_contracts.py`
- Create: `tests/test_paper_trading_primitives.py`
- Modify: `paper_trader.py`, `mlb_paper_trader.py`, `kalshi_paper_trader.py` wrappers only

TDD tests:

- American-to-decimal positive/negative/none parity.
- Kelly stake positive/no-edge/zero-odds parity.
- Kalshi contract sizing parity using existing `test_kalshi_paper_live_strategy_parity.py` fixtures.

---

### Phase 2: Extract statuses and lifecycle primitives

Objective: Make table-specific lifecycle explicit.

Files:

- Create: `src/paper_trading/primitives/statuses.py`
- Create: `src/paper_trading/primitives/lifecycle.py`
- Create: `tests/test_paper_trading_statuses.py`

Tests:

- pending/resolved/cancelled sets for each domain.
- Kalshi overflow transitions.
- daily-log include/exclude status groups.

---

### Phase 3: Extract resolution calculators

Objective: Move P&L formulas and stale/no-data policy out of god classes.

Files:

- Create: `src/paper_trading/resolution/__init__.py`
- Create: `src/paper_trading/resolution/sportsbook.py`
- Create: `src/paper_trading/resolution/kalshi.py`
- Create: `src/paper_trading/resolution/arb.py`
- Create: `tests/test_paper_trading_resolution.py`
- Modify existing trader methods to delegate formula decisions but keep SQL loops initially.

Tests:

- sportsbook over/under win/loss/cancel parity.
- Kalshi YES/NO win/loss/cancel/overflow parity.
- arb two-leg resolution parity.
- stale cutoff behavior preserved.

---

### Phase 4: Extract daily-log adapters

Objective: Reduce repeated bankroll roll-forward and summary SQL.

Files:

- Create: `src/paper_trading/logging/__init__.py`
- Create: `src/paper_trading/logging/daily_logs.py`
- Create: `tests/test_paper_trading_daily_logs.py`

Tests:

- current summary fields preserved per domain.
- current bankroll-after behavior preserved.
- no table schema changes.

---

### Phase 5: Extract storage adapters

Objective: Move insert/upsert/update SQL behind explicit table adapters.

Files:

- Create: `src/paper_trading/storage/__init__.py`
- Create table-specific adapter modules.
- Create `tests/test_paper_trading_storage_sql.py` using fake engines/query text assertions.

Tests:

- existing conflict targets preserved.
- pending-only updates preserved.
- commit behavior preserved.
- no alert side effects in storage layer.

---

### Phase 6: Extract selection services one domain at a time

Order:

1. NBA sportsbook selector from `PaperTrader.select_bets(...)`.
2. MLB sportsbook selector from `MLBPaperTrader.select_bets(...)`.
3. Kalshi selector from `KalshiPaperTrader.select_bets(...)`.
4. Arb selector from `ArbPaperTrader.select_arbs(...)`.
5. DFS selector later, only after core primitives stabilize.

Kalshi sub-slices:

- exposure cap service
- existing position/exposure query adapter
- liquidity/spread/price filters
- supported-stat and allowed-direction filters
- star-hitter filter
- sportsbook-alignment filter
- overflow storage policy
- reasoning payload builder

Tests:

- current fixture selections unchanged.
- NO-only default preserved.
- MLB-first `prior_exposure` semantics preserved.

---

### Phase 7: Extract alert payload builders

Objective: Separate alert payload shape from trader persistence.

Files:

- Create: `src/paper_trading/alerts.py`
- Create: `tests/test_paper_trading_alert_payloads.py`

Tests:

- current alert fields preserved.
- no alert sent before DB commit.

---

### Phase 8: Shrink compatibility facades and add anti-regrowth guards

Recommended thresholds after extraction:

- `paper_trader.py` under 350 non-comment LOC.
- `mlb_paper_trader.py` under 300 non-comment LOC.
- `kalshi_paper_trader.py` under 500 non-comment LOC.
- `arb_paper_trader.py` under 250 non-comment LOC.

Guards:

- no duplicate American odds conversion in trader classes.
- no inline Kalshi P&L formulas outside `resolution/kalshi.py`.
- no status string families duplicated across methods.
- compatibility wrappers remain only as delegation layers.

---

## Files likely touched

Core existing:

- `src/paper_trading/paper_trader.py`
- `src/paper_trading/mlb_paper_trader.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/paper_trading/arb_paper_trader.py`
- `src/paper_trading/dfs_paper_trader.py` later only
- `src/paper_trading/user_bet_resolver.py` later only
- `src/paper_trading/user_dfs_resolver.py` later only

New packages:

- `src/paper_trading/primitives/*.py`
- `src/paper_trading/storage/*.py`
- `src/paper_trading/resolution/*.py`
- `src/paper_trading/selection/*.py`
- `src/paper_trading/logging/*.py`
- `src/paper_trading/alerts.py`

Callsites to preserve:

- `src/orchestration/inference_job.py`
- `src/orchestration/mlb_inference_job.py`
- `src/orchestration/edge_refresh_job.py`
- `src/orchestration/daily_stats_job.py`
- `src/orchestration/mlb_daily_stats_job.py`
- `src/orchestration/kalshi_refresh_job.py`
- `src/orchestration/kalshi_daily_summary_job.py`
- `src/orchestration/arb_scan_job.py`
- `src/discord_bot/alerts.py`
- `src/discord_bot/services/paper_trading.py`
- `src/social/data_provider.py`

Tests:

- `tests/test_paper_trader.py`
- `tests/test_kalshi_paper_live_strategy_parity.py`
- new primitive/storage/resolution/selector tests listed by phase

---

## Validation commands

Focused baseline:

`venv/Scripts/python.exe -m pytest tests/test_paper_trader.py tests/test_kalshi_paper_live_strategy_parity.py -q`

After primitives extraction:

`venv/Scripts/python.exe -m pytest tests/test_paper_trader.py tests/test_kalshi_paper_live_strategy_parity.py tests/test_paper_trading_primitives.py -q`

After resolution extraction:

`venv/Scripts/python.exe -m pytest tests/test_paper_trader.py tests/test_paper_trading_resolution.py -q`

After storage extraction:

`venv/Scripts/python.exe -m pytest tests/test_paper_trading_storage_sql.py tests/test_paper_trader.py -q`

Lane-wide:

`venv/Scripts/python.exe -m pytest tests -k "paper_trader or paper_trading or kalshi_paper" -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/paper_trading/*.py src/paper_trading/primitives/*.py src/paper_trading/storage/*.py src/paper_trading/resolution/*.py src/paper_trading/selection/*.py src/paper_trading/logging/*.py`

Diff hygiene:

`git diff --check -- src/paper_trading tests .hermes/plans/god-class-migrations/06-paper-trading-shared-primitives-migration.md`

---

## Risk controls / non-goals

Non-goals:

- Do not change bankroll defaults.
- Do not change Kelly fractions or max-bet caps.
- Do not change Kalshi NO-only default.
- Do not add YES betting without explicit controlled experiment.
- Do not change supported-stat whitelist.
- Do not change conflict keys or DB schema.
- Do not change settlement/P&L formulas.
- Do not merge DFS/user-bet overhaul into the first core primitive PR.
- Do not rewrite dashboard/Discord consumers until storage/output parity is proven.

Hard rules:

- Table names and public classes remain stable.
- Existing orchestration imports keep working.
- Placement remains idempotent.
- Resolution remains deterministic and audited by fixture tests.
- Strategy behavior changes require separate RED tests and Chase approval.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A table has a hidden status value used by dashboard/Discord queries.
2. A daily-log summary includes/excludes overflow or cancelled rows differently than selection does.
3. A conflict target differs by table and cannot use a generic storage adapter.
4. A user/DFS resolver mirrors sportsbook logic but has schema-specific exceptions.
5. A Kalshi filter is live-strategy parity critical.
6. An alert payload field is consumed by Discord/dashboard formatting.
7. A callsite depends on a private trader method.
8. A P&L formula differs because cost basis differs by market type.
9. A behavior-changing strategy fix appears; split it from structural extraction.
10. A parity guard is needed between paper and live Kalshi strategy.

Progress log entries must distinguish: primitive created, facade delegates, storage adapter introduced, selector extracted, old duplicate removed, parity verified, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with pure primitives only:

1. Add inventory tests for public trader classes and status families.
2. Extract `odds.py`, `staking.py`, and `kalshi_contracts.py`.
3. Keep private methods as wrappers.
4. Run `tests/test_paper_trader.py` and `tests/test_kalshi_paper_live_strategy_parity.py`.
5. Do not touch SQL, placement, resolution, daily logs, alerts, or selection filters yet.

This creates a safe shared seam without risking paper-trading evidence integrity.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- AST/method inventory for `paper_trader.py`, `mlb_paper_trader.py`, `kalshi_paper_trader.py`, and `arb_paper_trader.py`.
- Targeted reads of `KalshiPaperTrader.select_bets`, `place_bets`, and `resolve_bets`.
- Existing `tests/test_paper_trader.py` and `tests/test_kalshi_paper_live_strategy_parity.py` coverage surface.
- Callsite scan across `src`, `scripts`, and `tests` for paper-trading classes/tables.
- GBrain hard facts, critical invariants, and Railway setup.

Current status:

- Documentation only.
- No production code changed.
- No DB queries, jobs, or paper-trading actions run.

---

## Done when

- Odds/staking/status/P&L primitives have one tested owner.
- Placement storage and daily-log updates are table adapters, not god-class methods.
- Domain traders are thin compatibility facades.
- Paper/live Kalshi strategy parity guards still pass.
- Existing orchestration, Discord, dashboard, and analysis consumers remain compatible.
- Any strategy behavior changes are isolated in separate approved experiment docs/PRs.
