# NCAAB design handoff

- Updated: 2026-11-03.
- Owner: Chase.
- Status: seven-layer design specification completed; implementation, migrations, data acquisition, training and production activation remain separate work.

## Priority and concurrent-worktree ownership

**NBA remains the primary project priority.** NCAAB is a new, simultaneous design/worktree lane, not a replacement for the active NBA readiness and production work. Do not let this handoff redirect the default resume into NCAAB implementation or consume resources needed by NBA.

At the 2026-11-03 wrap-up, these design changes are in `C:\Users\Chase\Projects\GameFlowData` on `main`. No dedicated NCAAB worktree is registered in that checkout's `git worktree list`; this session did not create or move worktrees. Establish an explicitly isolated NCAAB worktree before implementation, preserving the current documents and all active NBA work. The parallel-lane designation is not proof that physical Git isolation already exists.

## Purpose

Build men's college basketball game-level distribution models and a reproducible research/forward-paper system. Initial markets are spreads and totals. Moneyline wagering is deferred until final-tie/OT probability handling is validated. Profitability, capacity and freedom from account limits are not established outcomes.

This is a new isolated design lane. It does not restore archived NCAAB code, alter the active NBA/MLB runtime or authorize long jobs, paid data calls, database writes, deployment, real wagering, commits or pushes.

## Read in this order

Latest continuity record: [session wrap-up — NBA first, NCAAB parallel lane](HANDOFF.md).

1. [System architecture and shared contracts](architecture-overview.md) — authoritative cross-layer DTOs, defaults, dependency direction, table inventory and gates.
2. [Model architecture decision](model-architecture-decision.md) — why the baseline and later Bayesian/neural alternatives are being tested.
3. [Data sources](data-sources.md) — source roles, actual historical/local-live evidence and unresolved coverage limitations.
4. The seven layer specifications below, sequentially.

## Detailed layer specifications

| Layer | Contents |
|---|---|
| [1. Data ingestion](layers/01-data-ingestion.md) | Files/adapters, historical/live normalization, request budgets, resume, source receipts and failure classifications |
| [2. Database and identity linking](layers/02-database-and-identity-linking.md) | Repositories, proposed tables/columns/keys, revisions, source IDs, coherent quotes, migrations and transaction rules |
| [3. Feature builders and feature store](layers/03-feature-builders-and-feature-store.md) | Exact baseline feature order/formulas, missingness, as-of reads, market isolation, roster/state extensions and dataset export |
| [4. Training and artifact management](layers/04-training-and-artifact-management.md) | Fixed initial model settings, chronological folds, optional backends, manifest/serialization/release lifecycle |
| [5. Prediction and betting decisions](layers/05-prediction-and-betting-decisions.md) | Deterministic sampling, quote routing, win/push/loss math, flat-stake selection and shared inference |
| [6. Evaluation and forward tracking](layers/06-evaluation-and-forward-tracking.md) | Replay, settlement, metrics/uncertainty, CLV, paired comparisons, paper idempotency and immutable reports |
| [7. Daily orchestration and data quality](layers/07-daily-orchestration-and-data-quality.md) | Scheduler integration, fenced leases, readiness, corrections, alerting, deployment acceptance and recovery |

Every layer names proposed files/classes/interfaces, upstream/downstream dependencies, persistence interactions, implementation slices and tests. The files are no longer empty placeholders. Proposed source/test/migration paths are not claims that those files currently exist.

## Initial implementation target

- Python-only isolated `src/ncaab/` domain package and small retained-scheduler bridges.
- SportsDataverse ESPN historical files and live ESPN adapters for statistical facts; a reviewed season-classification reference; separate Odds API ingestion for quotes.
- KenPom and Kaggle excluded. NCAA lineup/possession and CBBD enrichment are optional later needs, not baseline blockers.
- Initial ending-season data target 2022-2026 plus 2027 updates; acquisition depth is not the training window.
- Direct XGBoost quantile margin/total models: paired independent/aware variants, 31/35-field initial schemas, two-versus-three-season history comparison.
- Raw probabilities and flat one-unit research decisions; no inherited conformal offsets, BL, Kelly, props, parlays or automated wagering.
- Dynamic team-state, same-feature neural and roster/matchup challengers reuse the same contracts. They do not require rebuilding the whole system.

## Important design findings

The retained quantile suite computes/applies conformal offsets; it is not safe to import unchanged under project invariants. The retained artifact validator and replay harness are NBA-specific. Reuse their patterns without weakening them or changing NBA behavior.

Historical unversioned statistics require an explicitly reconstructed research mode. Once an old file is actually observed, it can support later strict decisions; its receipt cannot be backdated to prove earlier availability. Historical provider quote snapshots have a separate clock contract.

The retained scheduler has script-based subprocess identity and process-local locks. Four thin NCAAB wrappers plus DB-fenced leases avoid status collisions and overlapping writers. Existing Nixpacks installs the runtime requirements and starts this scheduler; any future dependency-overlay edit is scoped and separately deployed.

## Remaining gates, not unspecified architecture

Before affected execution, supply/verify: season-specific D1 evidence; historical/live field parity; book allowlists and settlement policies; request/credit/resource budgets; live schema/DB target; durable storage; deployment-host source access; and explicit paper artifact/release approval. Numerical defaults are research controls, not measured optimal settings.

First implementation should deliver a small controlled ingestion-to-replay path, then bounded real-source acceptance. Long acquisitions/training/sweeps remain Chase-launched. No source-wide audit, refactor, background training or migration is implied by this handoff.

## Continuity and safety

Use the [overview](architecture-overview.md) as the cross-layer authority; update it and affected consumers when a contract changes. Keep state fitting, feature building, probability conversion and settlement ownership distinct. Historical replay and daily paper call the same engines.

Preserve active NBA/MLB work. Main-context agents do not call Supabase directly; future DB checks use the isolated SQL-runner pattern. The canonical wrap-up summary is published and verified as `handoffs/handoff-139`; the full application design changes remain local/uncommitted. See the [session handoff](HANDOFF.md) for publication evidence and broader GBrain maintenance caveats.

[Return to documentation index](../README.md).
