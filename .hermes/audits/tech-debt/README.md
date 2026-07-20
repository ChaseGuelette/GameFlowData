# GameFlow Technical-Debt Audit

Status: discovery in progress
Started: 2026-07-18

This directory contains evidence reports from bounded, read-only audit lanes. Reports identify and verify debt; they do not authorize refactors, database changes, deployments, model promotion, or live-trading changes.

## Rules

- Evidence before registration.
- Audit by subsystem lane, not one unbounded repository scan.
- Exact paths, symbols, tests, logs, or canonical GBrain pages are required.
- File size alone is not evidence of debt; findings must identify mixed ownership or a concrete failure mode.
- Existing plans are reconciled before new plans or debt IDs are created.
- Audit workers write only their assigned report.
- Findings remain candidates until independently reviewed.
- No direct Supabase MCP, DB writes, broad queries, production jobs, scrapes, deployments, training, backtests, or live-trading changes.

## Reports

- `00-existing-inventory-reconciliation.md` — current register, known issues, migration plans, and stale/superseded records.
- `01-python-architecture.md` — Python module boundaries, orchestration, compatibility paths, and ownership.
- `02-testing-ci-verification.md` — tests, CI, E2E contracts, and anti-regrowth coverage.
- `03-database-data-lifecycle.md` — schema/data lifecycle and query risks; requires separately approved SQL-runner evidence.
- `04-scheduler-ingestion-operations.md` — scheduler, ingestion jobs, retries, overlap, recovery, and observability.
- `05-model-mlops-promotion.md` — training, artifacts, evaluation, promotion, and rollback.
- `06-trading-market-safety.md` — paper/live parity, lifecycle, bankroll, and market integrations.
- `07-dashboard-product.md` — APIs, auth, dashboard architecture, UX, and product debt.
- `08-infra-security-dependencies.md` — deployment, config drift, security, dependencies, backup, and recovery.
- `09-agent-knowledge-workflow.md` — Hermes, GBrain, documentation, and agent workflow.
- `10-performance-storage-cost.md` — storage, generated artifacts, query/compute cost, and repository bloat.
- `11-system-overhaul-adjudication.md` — final cross-report deduplication, severity/sequence review, and proposed execution roadmap.
- `12-kalshi-deprecation-and-project-pruning.md` — Michigan sports-market closure impact, Kalshi finding disposition, and evidence-backed remove/archive/retain boundaries.
- `13-broader-project-pruning-candidates.md` — non-Kalshi obsolete, superseded, duplicated, generated, or unsupported surfaces proposed for removal/archive/retention review.

## Current product constraint

As of 2026-07-18, Chase reports that Kalshi sports markets are closed to the project in Michigan. The audit therefore treats Kalshi sports trading and related app surfaces as a decommissioning target, not an active feature-development lane. Kalshi findings must be reclassified as one of:

- **Remove now:** deployed or reachable code that creates security, trading, billing, scheduler, or operational exposure.
- **Remove after dependency check:** Kalshi-specific code with shared consumers or migrations that require an ordered extraction.
- **Archive evidence:** experiments, reports, plans, and tests worth preserving outside active runtime/product surfaces.
- **Retain shared primitive:** provider-neutral logic demonstrably used by supported sportsbook, paper-trading, or analytics paths.
- **Needs product decision:** non-sports Kalshi or broader cross-market arbitrage capability whose future value is not established by this audit.

This constraint supersedes remediation recommendations that would invest in making Kalshi sports execution production-ready. Safety findings remain relevant only to disablement, containment, data preservation, and removal sequencing.

## Finding lifecycle

1. **Raw evidence:** subsystem findings stay in this audit directory with paths, failure modes, confidence, rejected suspicions, and coverage gaps.
2. **Adjudication:** `11-system-overhaul-adjudication.md` will deduplicate overlaps, distinguish correctness bugs from structural debt, and assign Confirm/Shelf/Reject/Needs-Evidence plus sequencing.
3. **Canonical debt:** only independently verified Confirm items are proposed for `docs/understanding/tech-debt-register.md`; existing TD entries are updated before new IDs are considered.
4. **Execution planning:** only approved remediation slices receive or update `.hermes/plans/` artifacts.
5. **Kanban:** only approved, prioritized execution slices become cards. Raw audit findings are not bulk-imported.
6. **Knowledge continuity:** final decisions and durable operating facts are synced to canonical GameFlow GBrain during wrap-up; rejected/stale evidence remains in the audit reports so it is not rediscovered.

## Execution status

Wave 1 used full `default` Hermes workers on `gpt-5.6-sol`, not the small-context delegation model, and completed:

1. Existing inventory reconciliation.
2. Python architecture.
3. Scheduler and ingestion operations.

Wave 2 completed on the same model:

1. Testing, CI, and verification boundaries.
2. Trading and market-safety architecture.
3. Dashboard and product architecture.

Wave 3 completed:

1. Model/MLOps artifact, evaluation, promotion, and rollback architecture.
2. Infrastructure, security, dependency, backup, and recovery posture.
3. Agent, GBrain, documentation, and knowledge-workflow architecture.

The Kalshi product constraint added a parallel deprecation review:

1. Reclassify every Kalshi-related audit finding around containment/removal rather than remediation.
2. Produce a dependency-ordered Kalshi sports decommission map.
3. Review the broader repository for additional evidence-backed cuts without deleting anything during the audit.

Wave 4 completed the remaining evidence gaps:

1. Database/schema/data-lifecycle ownership from tracked code and migrations only; live SQL remains separately approval-gated.
2. Performance, storage, generated-artifact, query/compute-cost, and repository-bloat review using bounded tracked-file inventories.
3. Final cross-report adjudication in `11-system-overhaul-adjudication.md`.

Each report stops at an evidence-review gate. The canonical register at `docs/understanding/tech-debt-register.md` will not be changed until findings are independently reviewed and adjudicated.
