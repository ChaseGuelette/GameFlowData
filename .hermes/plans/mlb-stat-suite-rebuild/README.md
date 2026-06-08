# MLB Stat Suite Rebuild Plan

> For Hermes: implement with small approved slices. Do not run long training/backtests or broad DB work from this plan without Chase approval. Prefer direct edits for docs/tests; use the GameFlow implementation-worker lane for multi-file code slices.

Goal: rebuild the MLB model training/ablation operating system so pitcher_strikeouts can use the same modern validation discipline as batter_hits without cloning batter_hits-specific scripts or deepening tech debt.

Architecture: move from stat-specific one-off scripts and duplicated training orchestrators to a stat-profile driven suite. Shared code should own lifecycle, artifacts, audit/ranker/book tooling, and command assembly; stat-specific strategy/profile code should own model objective, feature contracts, default directions, and calibration semantics.

Tech stack: Python, PowerShell wrappers, pytest, pandas, existing GameFlow MLB feature stores/backtest sweep/audit scripts.

## Documents in this plan

1. `00-current-state-and-inventory.md`
   - file inventory, what is reusable, what is duplicated, exact current responsibilities.
2. `01-target-architecture-and-patterns.md`
   - target architecture, software patterns, boundaries, non-goals.
3. `02-operational-runner-rebuild.md`
   - generic stat ablation/resume runner plan replacing batter_hits one-off wrapper cloning.
4. `03-clv-audit-genericization.md`
   - generic CLV analyzer/audit suite decoupling plan.
5. `04-training-suite-refactor.md`
   - shared training base/profile plan for pitcher and batter training entrypoints.
6. `05-pitcher-k-port-and-baseline.md`
   - pitcher_strikeouts port plan, family controls, baseline restoration, validation gates.
7. `06-tests-and-validation.md`
   - characterization tests, unit tests, smoke tests, validation commands, done criteria.
8. `07-implementation-sequence.md`
   - PR/slice order, approval gates, rollback strategy, and worker specs.

## Executive decision

Do not copy `scripts/run_batter_hits_family_ablation.ps1` into a pitcher-specific near-clone.

Do build a shared stat-profile operational runner and generic CLV boundary first, then port pitcher K onto it. After the runner/audit path is stable, refactor the duplicated training orchestrators behind a shared base + stat strategies.

## Why this is needed

The current repo has two separate large training suites:

- `src/models/mlb/mlb_train_pipeline.py` — pitcher K only, large `MLBTrainingOrchestrator`.
- `src/models/mlb/mlb_batter_train_pipeline.py` — batter models, large `MLBBatterTrainingOrchestrator`.

Both own the same lifecycle responsibilities, but batter_hits has newer feature-family controls and validation metadata while pitcher K is still hard-coded around older ablation variants. The operational wrapper layer mirrors this asymmetry: batter_hits has mature PowerShell wrappers, but they hard-code batter_hits and should not be cloned.

## Guiding principles

- Preserve behavior first; refactor behind compatibility shims.
- Characterization tests before moving logic.
- Profile/strategy over copy-paste subclasses.
- Shared lifecycle, stat-specific math.
- Explicit stat contracts; no hidden filename inference as the only source of truth.
- No broad DB writes or long runs during scaffold work.
- Keep Phase 3A pitcher rejected features locked out by default.
- Empirical CDF probabilities stay invariant.
- CLV/ranker/book gates decide promotion, not selector output or calibration alone.

## Approval gates

Need Chase approval before:

- multi-file code implementation beyond docs/tests;
- changing training entrypoint behavior;
- running long training/backtests;
- adding DB migrations/indexes/backfills;
- changing production artifact loading semantics;
- retiring compatibility wrappers.
