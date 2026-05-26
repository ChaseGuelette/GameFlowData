# GameFlow God-Class Migration Plan Index

> **For Hermes:** Use this index as the routing document for the ten GameFlow structural migrations. For each migration, read the lane-specific plan, then implement with strict TDD and scoped diffs. Do not execute multiple migration lanes in the same commit unless Chase explicitly approves.

**Goal:** Build Kalshi-style migration documentation for the major GameFlow god classes/modules that need responsibility-based rebuilding.

**Architecture:** The Kalshi migration proved that these plans must start with a diagnosis and target ownership model, but also leave room for expansion. The Kalshi plan grew from facade extraction into statuses, state machine, services, direct job wiring, facade removal, shared config, docs cleanup, and parity guards. These docs therefore include an explicit "expansion checkpoints" section so unexpected needs are captured instead of being bolted on silently.

**Tech Stack:** Python, TypeScript/Next.js, SQLAlchemy, pandas, pytest, React Testing Library where applicable, GameFlow SQL-runner pattern for DB truth.

---

## Kalshi migration lessons to apply to every lane

1. A god-class migration is not a line-count split. Migrate ownership of responsibilities.
2. Start with characterization/inventory tests before moving behavior.
3. Separate pure policy from DB/API/alert/UI side effects.
4. Move one caller or job at a time; verify direct-service wiring before deleting compatibility paths.
5. Keep behavior-preserving migrations separate from behavior-changing hardening.
6. Add a removal guard when retiring a class/file so it cannot be reintroduced accidentally.
7. Expect the plan to expand. Add progress-log entries and expansion checkpoints rather than rewriting history.
8. Validate with focused tests, then lane-wide regression, then scoped diff review.
9. Keep commits narrow. Do not mix MLB modeling, Kalshi, dashboard, and scheduler changes in one migration commit.
10. For GameFlow model/backtest lanes, cite relevant lessons/invariants before proposing implementation.

## The ten migration docs

| # | Migration lane | Plan file | Current priority | Notes |
|---|---|---|---|---|
| 01 | MLB quote-clean/backtest sweep architecture | `01-mlb-quote-clean-backtest-sweep-migration.md` | Complete | Implementation complete as of 2026-05-26; future work is artifact validation, not structural migration. |
| 02 | MLB feature-store boundary migration | `02-mlb-feature-store-boundary-migration.md` | High | Temporal integrity + train/backtest/inference parity. |
| 03 | NBA/general feature-store migration | `03-nba-feature-store-boundary-migration.md` | Medium | Production-sensitive; avoid casual behavior changes. |
| 04 | Training orchestrator migration | `04-training-orchestrator-migration.md` | Medium | Separate fitting, validation, artifact writing, promotion metadata. |
| 05 | Daily prediction runner migration | `05-daily-prediction-runner-migration.md` | Medium | Separate discovery, inference, line loading, persistence, alerts. |
| 06 | Paper-trading shared primitives migration | `06-paper-trading-shared-primitives-migration.md` | Medium | Follow-up to Kalshi; shared staking/ledger/status primitives. |
| 07 | Scheduler/job registry migration | `07-scheduler-job-registry-migration.md` | Medium-low | Scheduler should own timing, not domain internals. |
| 08 | Arbitrage matcher/scanner migration | `08-arbitrage-matcher-scanner-migration.md` | Medium-low | Separate parsing, normalization, matching, verification, opportunity calc. |
| 09 | Dashboard Ask API route migration | `09-dashboard-ask-api-route-migration.md` | Medium | Biggest API route; split auth/data/prompt/response. |
| 10 | Dashboard god components/pages migration | `10-dashboard-god-components-pages-migration.md` | Medium-low | Analysis modal, dashboard/performance/DFS pages and tables. |

## Work protocol

For each plan:

1. Re-read the plan and current code before editing; these files may drift.
2. Run the plan's inventory/characterization tests first.
3. Implement exactly one phase/slice at a time.
4. Update the plan's progress log after each slice with:
   - files changed
   - tests added
   - RED result
   - GREEN result
   - validation commands
   - behavior-preservation notes
   - any expansion checkpoint triggered
5. If a slice expands, add a new named sub-slice rather than absorbing it into the current task.
6. Only delete old classes/modules after production callsites are migrated and a removal guard passes.

## Relevant global GameFlow invariants

- Never deploy global conformal recalibration offsets.
- Q10 miscalibration is the edge; do not blindly correct it.
- Probabilities use empirical CDF `(samples > line).mean()`, never Gaussian CDF.
- Temporal integrity is end-to-end; raw timestamps alone do not prove a path is leak-free.
- Use GameFlow SQL-runner isolation for DB truth; do not run Supabase MCP in main context.
- Avoid broad remote odds-table aggregates; use keyed/chunked/index-aware audits.
