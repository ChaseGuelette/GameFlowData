# Approved implementation: NBA chronology and historical training execution

Owner authorization: Chase explicitly requested implementation of items 1 and 3 while an isolated SELECT-only subagent investigates data. This spec narrows those approved items; no further planning approval loop is required. Source edits are allowed ONLY in this worktree. No commit/merge/push/deploy, source repair, DB query/write, scraping, model job or paid API use.

## Workspace and evidence

- Worktree: C:/Users/Chase/Projects/GameFlowData-worktrees/nba-pretraining-hardening
- Branch: repair/nba-pretraining-hardening; base 3f7d80c6.
- Main has independent data/props-linker work. NEVER edit main, other worktrees, src/processing/nba_linker_local.py, NCAAB/MLB sources, production artifacts or credentials.
- Read main's docs only as context if needed: nba-pretraining-hardening-scope-2026-11-03.md; nba-training-readiness-reconciliation-2026-11-03.md. The base already includes them.
- Parent offline baseline: 96 passed in 0.85s (seven existing suites below), zero live DB/network/model-load/model-fit attempts. A separate initial inclusion of test_hyperparameter_tuner.py produced 12 safety-guard failures because those tests intentionally fit real estimators; those are not source regressions. Do not repeat/claim them green. Validate new tuner routing with controlled models and run its non-fitting existing tests only, or report real-fit coverage excluded.

## Owner choices and invariants

- Intended fitting seasons 22023/22024; calibration22025; current22026 evaluated separately. Do NOT change default numeric model behavior or introduce current-season fitting.
- Fixed incumbent hyperparams baseline; separately supported optional tuning, no launch. Tune/feature-select only within chronological fitting data.
- Preserve empirical CDF, quantile monotonicity, existing model architecture and numerical calibration ownership; never deploy global conformal offsets.
- Target/rolling5/8 and traded-player/valid historical-position choices unchanged. Cold starts explicitly deferred. Do not fix/redefine structural-zero rules, feature math, missingness policy, source-authenticity semantics or production serving.
- A source-ready policy remains required; do not fabricate one or make a success-shaped production baseline YAML. Controlled probe policy is labeled synthetic and stays in scratch.

## Allowed edit scope (touch only needed files)

- src/models/train_pipeline.py
- src/models/training_preflight.py
- src/models/quantile_trainer.py
- src/models/hyperparameter_tuner.py
- src/processing/feature_selection.py
- new src/models/nba_training_chronology.py (small shared date/group split helpers)
- src/models/nba_artifacts.py only if existing manifest schema/validation cannot carry true chronology/provenance
- src/models/feature_store.py
- src/models/features/nba/requests.py
- src/models/features/nba/historical_loader.py
- src/models/features/nba/historical_sources.py only for bounds plumbing, NOT source eligibility or feature values
- src/models/features/nba/date_batch_loader.py, date_range_loader.py, training_loader.py only if required for bounds/role forwarding
- new src/models/features/nba/acquisition.py (small immutable validated limits/budget helper)
- src/backtesting/nba/historical_execution.py
- src/backtesting/run_backtest.py
- src/backtesting/nba/sweep_config.py, run_nba_sweep.py, sweep_runner.py (coordinator corrected the parser owner after discovery; `sweep_cli.py` does not exist)
- src/models/nba/lifecycle/config.py, adapters.py, runner.py
- src/backtesting/nba/evidence.py only for bounded-receipt version compatibility if necessary (never weaken ledger/hash/outcome checks)
- docs/understanding/nba-pretraining-hardening.md (new concise operator wiki)
- docs/operations/nba-pretraining-hardening-implementation-2026-11-03.md (new delivery/evidence record)

NO new permanent unit tests or general fixture edits. Owner explicitly approved only updating `tests/test_train_pipeline.py::TestRunIntegration.test_full_run_completes` input setup to separate train/calibration seasons/dates; parent has applied that isolated change, preserving all assertions and test count. Keep that parent-owned diff untouched and include it in final verification. NO dependency additions. Do not create extra frameworks or broad refactors. Ask parent via delivery if an indispensable path is outside allowlist; do independent in-scope work meanwhile.

## 1. Actual chronology and inner splits

1. Reject duplicate/malformed season identifiers and train/calibration season intersection in actual training CLI preflight and direct orchestrator entrypoints, not lifecycle alone. Existing lifecycle Training already rejects intersection; reuse/align instead of duplicating different policy rules. Preserve literal tokens; do not coerce malformed IDs.
2. After loading and before feature selection/tuning/model fit, require actual nonempty fit/calibration frames with valid game_date/game_id/player_id/season_id; validate requested-season membership, duplicate keys, cross-frame game/player keys and strict chronological order (max fit date < min calibration date). Sort deterministically by game_date/game_id/player_id. Preserve legitimate return-value schema and pandas attrs needed for historical evidence. No hidden filtering to repair malformed/overlapping inputs.
3. Historical policy supplies train/calibration boundaries. Apply role/window predicates before expensive native source assembly, not merely posthoc. CLI season/cutoff values must agree with attached policy or fail before engine/artifact creation. Exclusive calibration end semantics stay exclusive. Do not introduce a separate test-training loader: existing lifecycle replay/evaluation stage is the isolated current-season test route.
4. Inner early-stopping/tuning/feature-count folds must be chronological by date, so all players/games on a date stay on one side. A deterministic row sort plus shuffle=False alone is insufficient. Thread optional group/date metadata only through the NBA training callers; preserve shared/MLB caller behavior when metadata absent. Cover full and partial model training, tuner shared/per-quantile, and both feature ranking/count-CV paths. No change to objectives, math or tuning search space.
5. Record true selected fit/calibration population ranges/counts/key identities and chronology proof in existing run config/manifest provenance. Do not falsely mark source_verified or decision_grade. On partial historical retraining, reject a frozen parent whose training/calibration provenance overlaps evaluation or cannot establish compatibility; do not silently certify legacy parent history.
6. A supplied hyperparameter file is fixed mode; tune is a separate mode. Conflicting flags fail, including direct orchestrator use where relevant. Both use fitting-only data.

## 3A. Historical training CLI and lifecycle

1. Reuse load_attachment, validate_saved_attachment, recheck_attachment, source_policy, make_sources from historical_execution. They already work without an artifact. Do NOT call replay-only validate_artifact_window before a model exists. Add a narrow training-window validator rather than weaken replay validate_window.
2. Actual train_pipeline CLI accepts explicit historical policy + optional metadata sidecar + acquisition limits, local only. Dry-run/preflight reads/validates files but must create no engine/output directory/model object and never load a parent model. Mutated attached files invalidate execution and finalization.
3. Instantiate the actual FeatureStore with the existing native sources and pass the exact policy through full and applicable partial training/calibration loads. No fallback to legacy loader when historical policy is given. Historical calibration diagnostics must not silently ignore policy: wire correctly or explicitly reject unsupported mode before side effects, documenting it.
4. Enable lifecycle artifact.mode=train with historical inputs. Preserve training/calibration/evaluation validation, local-only targeting, output/input disjointness, hyperparam identity, artifact validation, stage reuse and fail-closed readiness. Build_training_command forwards exact captured policy/metadata/budget arguments; build_sweep_command carries same effective budgets. Retain legacy nonhistorical behavior except approved invalid-overlap rejection.
5. Include new helper/source identities and effective limits in lifecycle stage identities. Changes to policy, metadata, budget or relevant source invalidate reuse. Verify trained artifact historical inputs/chronology match requested lifecycle configuration before evaluation; an unrelated artifact must not satisfy the run.

## 3B. Narrow acquisition guard (shared historical routes)

1. Explicit positive validated integer limits: maximum dates, games, population player-game rows, initial-population query timeout milliseconds. Reject bool/zero/negative/partial configurations. Do not choose data policy from these operational limits.
2. Initial population SQL must fetch at most max_population_rows+1, deterministic order, within selected seasons and relevant role/date windows. Fail on overflow before PostgresHistoricalSources.acquire, feature assembly or model prediction. Never silently truncate.
3. Check distinct games/dates before source assembly. Date-range requests also reject obviously over-budget requested spans before querying. Limit identities are persisted; document whether a limit applies per requested population/run. Do not permit per-date/per-season loops to reset one declared run budget unnoticed; centralize or account cumulatively.
4. Apply timeout transaction-locally on the actual population connection without leaking pooled session settings; no committed DB mutation. Timeout/error yields explicit failed/blocked outcome, never partial-success rows. No production connection in verification.
5. Thread limits through new historical training and existing historical replay/sweep/lifecycle, retaining one shared input acquisition per sweep, not per cell. Preserve legacy saved evidence compatibility; old receipts can remain readable but must not silently bypass limits on new native executions.
6. This is NOT a guarantee of absolute per-source memory/runtime bounds beyond initial population acquisition. Do not scope a full resource scheduler or scrape budgeting framework.

## Verification (reuse, bounded)

Native Python: C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe
Guard: C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py
Unique scratch: C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-pretraining-hardening-worker
Run existing suites through guard ROOT pytest:
- tests/test_train_pipeline.py
- tests/test_nba_mode_loaders.py
- tests/test_nba_feature_store_inventory.py
- tests/test_nba_source_boundaries.py
- tests/test_run_backtest.py
- tests/test_run_sweep.py
- tests/test_feature_selection.py
Then affected existing suites identified by exact filename search (do not claim none without checking). Preserve fixture contracts unless genuinely incompatible; report failures rather than bypass enforcement for mocks.

Write only a few scratch controlled probes using existing tiny policy/producers where possible. Must exercise:
- actual CLI preflight/dry-run no engine/output and overlap/mismatch rejection;
- full and partial orchestrator dataset gate rejects overlap/unordered or sorts valid data before fit, date-group split keeps a game/date together; fitting/tuning/calibration receive correct populations;
- actual historical loader SQL LIMIT budget+1 and transaction timeout, max dates/games/rows overflow BEFORE sources acquire; malformed limits rejected;
- lifecycle actual command construction and dry-run training policy/budget flow; changed budget/metadata invalidates identity/reuse;
- actual trained-manifest producer (controlled model-boundary payloads, never fabricated 'real' artifacts) records chronology/attachment; downstream artifact matching validated.
Use controlled model/DB boundaries; no real fits, DB/network/model loads. Existing small controlled fixture producers at scratch nba-s8c-closeout and nba-s8d may help but must inspect root/shape before use. The guard does not propagate into child processes: stub child launch during lifecycle probes or explicitly wrap guard.

Run scoped Ruff and compile/AST checks, git diff --check. Do not broadly reformat files. Return full changed-file list, concise rationale, exact validations/results, artifact/DB/live limits, wiki path, and issues. No commit. Stop only after implementing the approved scope, bounded verification and documentation or an explicit material blocker; do not stop after a plan/stub.
