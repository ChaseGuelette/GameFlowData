# Python Architecture Audit

Status: read-only evidence report; findings are candidates pending independent review
Date: 2026-07-18
Scope: `src/models`, `src/models/mlb`, `src/backtesting`, `src/processing`, `src/db`, and targeted tests only

## Executive summary

The completed migrations are visible and should not be reopened: MLB quote-clean sweep ownership (Lane 01), MLB feature-store boundaries (Lane 02), and the NBA feature-store core boundary (Lane 03) all have real extraction and anti-regrowth evidence. The MLB stat-suite rebuild also delivered shared profiles, feature controls, artifact writers, a lifecycle base, generic operational wrappers, and CLV tooling. Those changes reduced debt, but they did not complete the broader training-orchestrator, daily-runner, artifact-consumer, or NBA backtesting boundaries.

The highest-risk current architecture issue is not file size by itself. It is that promotion-sensitive probability/calibration policy remains executable in several owners with inconsistent fallbacks. The same audit also found artifact identity based on directory shape rather than a validated manifest, NBA sweep code coupled to private harness methods while maintaining a second fast policy path, concrete training and daily runners that still own most workflow policy, a monolithic NBA linker with two separate operating modes, and import-time DB configuration capture.

No code, plans, register, DB state, production state, training, or backtest output was changed or executed.

## Method and mechanical inventory

Tracked Python files were enumerated with `git ls-files`; artifacts and the excluded heavy/generated paths were omitted before AST parsing.

- Audited tracked Python surface: 249 files / 64,557 LOC.
- Source in the allowed directories: 124 files / 40,668 LOC.
- Targeted tests included in the inventory: 125 files / 23,889 LOC.
- Largest current classes relevant to this audit:
  - `src/models/mlb/features/legacy_pitcher_feature_store.py:60-1595` — `MLBFeatureStore`, 1,536 lines, 22 methods.
  - `src/models/mlb/features/legacy_batter_feature_store.py:73-1333` — `MLBBatterFeatureStore`, 1,261 lines, 21 methods.
  - `src/models/mlb/mlb_daily_runner.py:82-1122` — `MLBDailyPredictionRunner`, 1,041 lines, 13 methods.
  - `src/models/daily_runner.py:74-1091` — `DailyPredictionRunner`, 1,018 lines, 17 methods.
  - `src/models/train_pipeline.py:29-1022` — `TrainingOrchestrator`, 994 lines, 17 methods.
  - `src/models/mlb/mlb_batter_train_pipeline.py:61-1052` — `MLBBatterTrainingOrchestrator`, 992 lines, 23 methods.
  - `src/backtesting/backtest_harness.py:63-934` — `BacktestHarness`, 872 lines, 14 methods.
  - `src/models/mlb/mlb_train_pipeline.py:90-808` — `MLBTrainingOrchestrator`, 719 lines, 23 methods.
- Largest functions relevant to uncompleted ownership boundaries:
  - `src/processing/nba_linker_local.py:312-766` — `process_local`, 455 lines.
  - `src/processing/nba_linker_local.py:1043-1401` — `link_incremental`, 359 lines.
  - `src/backtesting/run_sweep.py:873-1151` — `main`, 279 lines.
  - `src/backtesting/backtest_harness.py:671-866` — `_calculate_edges`, 196 lines.

These metrics were used only to route targeted reads. They are not findings by themselves.

## Existing-plan reconciliation

| Existing lane/program | Current-tree conclusion | Audit treatment |
|---|---|---|
| God-class Lane 01 — MLB quote-clean/backtest sweep | Complete. The runner delegates to typed config/services, promotion contracts exist, and inventory tests guard regrowth. | Do not rediscover the old `run_mlb_sweep.py` god module. Consumer-side artifact identity remains a separate cross-lane finding below. |
| God-class Lane 02 — MLB feature-store boundary | Complete at the planned structural boundary. Public facades are thin and inventory tests prevent SQL/helper regrowth. | The large `features/legacy_*_feature_store.py` implementations are recorded as a rejected size-only suspicion, not a new migration finding. |
| God-class Lane 03 — NBA feature-store boundary | Core complete. Focused loaders/sources exist; stable facade callsite cleanup is explicitly optional/deferred. | Do not report facade imports alone as debt. |
| God-class Lane 04 — training orchestrator | Documentation only in its progress log. | Findings 3 and 4 are current evidence for this lane. The MLB stat-suite base extraction is credited as partial implementation, not ignored. |
| God-class Lane 05 — daily prediction runner | Documentation only in its progress log. | Finding 5 is current evidence for this lane. |
| MLB stat-suite rebuild | Slices 1-7 are present in the current tree: generic CLV, generic wrappers, profiles/controls, shared artifact helpers, and lifecycle-only base extraction. Long training/sweeps were intentionally outside the rebuild. | Do not rediscover missing generic wrappers or shared writers. Findings focus on consumer validation and ownership left intentionally in concrete orchestrators. |

## Findings

### PA-01 — Executable global conformal-offset path contradicts the production invariant

Severity: Critical
Confidence: High

Evidence:

- `src/models/train_pipeline.py:74-144`, `TrainingOrchestrator.run`, always calls `_evaluate_combined_calibration` at lines 123-124 during a full run.
- `src/models/train_pipeline.py:705-864`, `_evaluate_combined_calibration`, computes and writes `combined_calibration_offsets.json` at lines 840-843.
- `src/models/train_pipeline.py:1049-1052` exposes `--calibrate-only`; the completion path at lines 1163-1166 writes offsets into the selected model directory.
- `src/models/monte_carlo.py:137-859`, `MonteCarloPredictor`, accepts the offsets at lines 158 and 182-185, applies them in prediction paths at lines 254-255, 354-355, and 484-485, and owns the sample-warping implementation at lines 560-628.
- `src/backtesting/run_backtest.py:188-198` and `src/backtesting/run_sweep.py:1031-1038` automatically load and activate the artifact when present.
- No targeted test surfaced that forbids offset activation or characterizes this path as report-only.

Concrete failure mode:

A training or calibrate-only run can leave an offset file in an otherwise loadable model directory; both NBA backtest entrypoints then silently alter generated samples merely because the file exists. That makes artifact presence an implicit policy switch and permits validation/evaluation with a recalibration policy that the project invariant says must never be deployed. It also obscures whether measured backtest differences came from the model or from post-hoc sample warping.

Interaction with existing plans:

- Directly belongs to Lane 04's unresolved separation of fitting, calibration reports, artifact writing, and promotion metadata.
- It is not covered by completed MLB Lanes 01-02 or the MLB stat-suite rebuild; this is the NBA/general training and inference path.

Safe first evidence step:

Add a no-DB unit/AST characterization test that constructs a temporary artifact directory containing `combined_calibration_offsets.json` and proves the intended supported behavior for each consumer. Start report-only: inventory all loaders and assert production/backtest constructors do not activate offsets implicitly. Do not retrain or compare ROI in this first step.

Done condition:

There is one explicit owner for calibration-report artifacts; global offsets cannot be activated by file presence; supported loaders require an explicit, typed, non-production experiment mode if the legacy analysis path must remain; and anti-regrowth tests fail if default training, backtest, or inference paths reintroduce automatic offset application.

### PA-02 — Probability policy has divergent silent fallbacks across inference and backtesting

Severity: Critical
Confidence: High

Evidence:

- `src/backtesting/backtest_harness.py:671-866`, `_calculate_edges`, uses empirical samples when available but falls back to a Gaussian survival function at lines 801-813 when samples are absent.
- `src/models/daily_runner.py:876-952`, `_calculate_edges`, uses empirical samples at lines 909-915 but falls back to five-quantile interpolation at lines 917-928.
- `src/models/mlb/mlb_daily_runner.py:799-870`, `_calculate_edges`, owns another probability/devig implementation.
- `src/backtesting/mlb/edge_engine.py:1-5` declares the MLB promotion-critical edge seam and clips empirical probabilities at lines 132-134; the legacy MLB harness still contains a parallel empirical implementation at `src/backtesting/mlb/mlb_backtest_harness.py:458-462`.
- Existing `tests/test_daily_runner.py:200-269` positively tests the NBA quantile fallback. No cross-path parity test surfaced for NBA daily versus backtest probability/edge output, and no MLB daily-runner edge parity test surfaced.

Concrete failure mode:

The same prediction with missing or differently keyed samples can produce a Gaussian probability in NBA backtesting, an interpolated probability in daily inference, or a skipped/defaulted decision in another path. A backtest can therefore validate a policy that production does not execute. The Gaussian branch directly conflicts with the empirical-CDF invariant, while positive tests around the daily fallback make the skew durable rather than accidental.

Interaction with existing plans:

- Lane 05 explicitly calls for one shared empirical-CDF edge owner and remains documentation-only.
- The completed Lane 01 MLB edge extraction is the positive pattern; it should not be reopened.
- Lane 04 matters only insofar as sample availability and artifact contracts must be explicit.

Safe first evidence step:

Create table-driven, no-DB parity tests using one prediction/line/sample fixture across NBA daily and NBA backtest owners. Include empty, missing-key, and populated-sample cases and record current outputs before any extraction. Separately assert that promotion-capable paths never call `scipy.stats.norm.sf`.

Done condition:

A shared typed probability/edge policy owns sample lookup, clipping, no-sample behavior, devigging, and output fields; daily and backtest adapters pass parity fixtures; unsupported no-sample cases fail closed or are explicitly non-promotion/report-only; and an inventory test prevents Gaussian or local interpolation fallbacks from regrowing in promotion-capable paths.

### PA-03 — NBA artifact identity and completeness are inferred differently by each entrypoint

Severity: High
Confidence: High

Evidence:

- `src/backtesting/run_backtest.py:46-61`, `find_latest_model_dir`, returns the lexically latest `nba_run_*` directory without excluding `_incomplete` or checking any required artifact.
- `tests/test_run_backtest.py:25-32` locks in “latest directory” behavior using empty directories; there is no incomplete/missing-artifact rejection case.
- `src/backtesting/run_sweep.py:838-870`, its separate `find_latest_model_dir`, instead treats `minutes_model.joblib` as the entire completeness contract.
- `src/models/train_pipeline.py:55-68` creates `_incomplete` directories and lines 139-143 rename on completion, but `run_backtest.py` does not honor that lifecycle marker.
- NBA training writes several loosely related files, while no shared NBA manifest consumer was found in either backtest resolver.

Concrete failure mode:

A failed run with a newer `_incomplete` directory can be selected by `run_backtest.py`, while `run_sweep.py` may accept a partially mixed run as soon as one sentinel file exists. Two commands pointed at the same artifact root can therefore load different runs or fail at different later files, making train-backtest identity and reproducibility ambiguous.

Interaction with existing plans:

- Lane 04 calls for one owner for run lifecycle/artifact writing and explicit promotion metadata; this is an unresolved consumer half of that lane.
- Lane 03's feature-store facade does not address artifact identity.

Safe first evidence step:

Add filesystem-only resolver characterization tests with complete, incomplete, missing-rate-model, and explicit-directory fixtures. Record the exact current divergence between `run_backtest.py` and `run_sweep.py`; do not touch production artifacts.

Done condition:

NBA training emits one versioned manifest/completeness contract; all inference/backtest/sweep resolvers share one validator; `_incomplete` and partial/mixed runs are rejected with actionable errors; selected artifact identity is serialized into result metadata; and tests enforce resolver parity.

### PA-04 — MLB manifests are writer-only while suite loading remains permissive and shape-driven

Severity: High
Confidence: High

Evidence:

- The MLB stat-suite rebuild added `src/models/mlb/training/artifacts.py:74-102`, `write_model_manifest`, and concrete pipelines call it at `src/models/mlb/mlb_train_pipeline.py:801-808` and `src/models/mlb/mlb_batter_train_pipeline.py:1044-1052`.
- `src/backtesting/mlb/sweep_bootstrap.py:35-62`, `find_latest_model_dir`, unconditionally prefers any `production/` directory, otherwise accepts direct sentinel files or the lexically latest non-`_incomplete` `mlb_run_*` directory.
- `src/models/mlb/mlb_model_suite.py:167-303`, `MLBModelSuite.from_directory`, does not read `model_manifest.json` or `suite_manifest.json`; it discovers by filenames, catches load exceptions at lines 216-217, 243-244, 264-265, and 279-293, and returns whatever subset loaded.
- `suite_manifest.json` is described as optional at line 18 and written by `write_manifest` at lines 351-366, but no consumer reference surfaced.
- Targeted tests cover manifest writing and bootstrap ordering, not rejection of inconsistent manifests, wrong stat/model type, stale mixed files, or partial-suite loads.

Concrete failure mode:

A production directory can contain stale files from different runs or one corrupt stat model. The suite logs an error, returns a partial set, and downstream execution can proceed with a different model mix than the run manifest intended. The shared manifest added by the rebuild therefore documents identity but does not enforce it at the promotion/backtest/inference boundary.

Interaction with existing plans:

- Credits MLB stat-suite Slices 5-6 as completed writer/lifecycle scaffolding; this is the unfinished consumer-validation seam, not a request to redo those slices.
- Lane 01 remains structurally complete; its bootstrap should consume a stronger contract without regrowing runner ownership.
- Lane 04 owns promotion/artifact lifecycle semantics shared with training.

Safe first evidence step:

Add temporary-directory tests for `MLBModelSuite.from_directory` and `sweep_bootstrap.find_latest_model_dir` covering manifest/file disagreement, a corrupt requested stat, stale extra files, and a requested stat absent from the manifest. Capture whether current behavior returns a partial suite.

Done condition:

A versioned suite/run manifest is authoritative; required requested stats and artifact hashes/files are validated before execution; partial loading is opt-in diagnostic behavior rather than the default promotion path; model path plus manifest identity is written to sweep/daily outputs; and consumer tests enforce writer-reader parity.

### PA-05 — Concrete training orchestrators still mix data, private feature transforms, fitting, evaluation, and artifacts

Severity: High
Confidence: High

Evidence:

- `src/models/train_pipeline.py:29-1022`, `TrainingOrchestrator`, still owns DB/feature-store construction, split loading, selection, tuning, fitting, calibration, combined calibration, correlation analysis, artifact serialization, sanity checks, partial retrain, and CLI-adjacent behavior.
- `src/models/mlb/mlb_train_pipeline.py:90-808`, `MLBTrainingOrchestrator`, owns the same lifecycle plus ablation controls and copula/IP variants. Its `run` method at lines 158-258 directly calls private feature-store behavior `_add_interaction_features` at lines 179 and 186.
- `src/models/mlb/mlb_batter_train_pipeline.py:61-1052`, `MLBBatterTrainingOrchestrator`, branches among binary/binomial/negbin pipelines at lines 193-229 and directly calls `_add_batter_interaction_features` at lines 206 and 213.
- `src/models/mlb/training/base_orchestrator.py:27-86` is intentionally lifecycle-only; its module contract explicitly leaves loading, selection, objectives, calibration, and promotion in concrete orchestrators.

Concrete failure mode:

A change to feature preparation, split boundaries, objective-specific fitting, calibration reporting, or artifact metadata must be repeated or coordinated inside large concrete workflows. Private feature-store calls bypass the focused loader/source API introduced by Lane 02, so a legacy implementation refactor can break training even while public facade tests stay green. The three orchestrators also cannot be compared at a shared stage contract, making train-suite skew difficult to detect without running full workflows.

Interaction with existing plans:

- Lane 04 is still documentation-only and directly owns this debt.
- The MLB stat-suite rebuild completed a conservative lifecycle-only base extraction and explicitly deferred strategy modules; this finding starts after that completed seam rather than proposing another base class from scratch.
- Lane 02 is complete, but the private calls are a cross-boundary leak worth resolving through public transforms/loaders, not by reopening facade SQL extraction.

Safe first evidence step:

Add no-DB stage-contract characterization tests around each orchestrator with fake loaders/models/writers. First capture ordered stage calls and payload contracts, then add inventory assertions for calls to feature-store private methods. Do not move model math or run training.

Done condition:

Concrete CLI/orchestrator classes are thin workflow facades; dataset/split policy, public feature preparation, fitting strategy, evaluation/report generation, artifact assembly, and promotion evidence have named owners; private feature-store coupling is removed; existing CLI behavior remains compatible; and stage parity plus anti-regrowth tests cover NBA, MLB pitcher, and MLB batter paths.

### PA-06 — NBA sweep is coupled to private harness internals and maintains a second unguarded fast policy path

Severity: High
Confidence: High

Evidence:

- `src/backtesting/run_sweep.py:135-217`, `run_shared_phases`, calls `BacktestHarness` private methods `_get_game_dates`, `_prefetch_all_lines`, `_get_actuals`, and `_get_voids` at lines 163-176.
- The legacy per-config path calls private `_calculate_edges`, `_filter_best_bets`, and `_merge_actuals` at lines 538-588.
- The active fast path separately implements probability precomputation at lines 224-305 and line-shopping/dedup policy in `_filter_best_bets_fast` at lines 312-346.
- `src/backtesting/backtest_harness.py:63-934` remains an 872-line owner of loading, prediction, line selection, edge math, filtering, actuals, and voids.
- No targeted NBA sweep inventory test or fast-versus-harness parity test surfaced.

Concrete failure mode:

Renaming or changing a private harness method breaks the sweep. More importantly, a correctness fix in `BacktestHarness._calculate_edges` or `_filter_best_bets` does not automatically reach `precompute_base_probabilities` or `_filter_best_bets_fast`. A fast sweep can therefore rank configurations using semantics that differ from the single backtest, with no parity gate detecting drift.

Interaction with existing plans:

- This is the NBA counterpart to the now-complete MLB Lane 01 extraction; Lane 01 itself should not be reopened.
- It overlaps Lane 05 only at shared edge/recommendation policy; backtest data loading and sweep execution should remain a separate bounded responsibility.

Safe first evidence step:

Build no-DB golden fixtures that feed identical predictions, samples, lines, actuals, and voids through the harness and fast functions, then compare selected rows, probabilities, edges, and bet decisions. Add a static inventory of private harness calls before extraction.

Done condition:

NBA sweep orchestration depends on public typed services/contracts; one owner defines probability, line-shopping, dedup, and settlement semantics; optimized implementations are covered by equivalence tests against the canonical policy; the CLI is thin; and inventory tests prevent new private harness coupling.

### PA-07 — `nba_linker_local.py` combines two linkers and all side-effect layers in one module

Severity: Medium
Confidence: High

Evidence:

- `src/processing/nba_linker_local.py:312-766`, `process_local`, combines CSV validation/loading/writing, normalization, manual mappings, fuzzy game/player matching, progress/stat reporting, and update-file assembly.
- `src/processing/nba_linker_local.py:1043-1401`, `link_incremental`, separately combines engine creation, reference and operational SQL reads, NBA API lookup, cache invalidation, fuzzy matching, pagination, updates, and progress control.
- Normalization and matching helpers are partly top-level and partly nested inside `process_local`, including `normalize_player` at lines 398-407 and row matching closures beginning at lines 444 and 497.
- `tests/test_nba_linker_local.py:144-291` exercises local CSV processing/download/upload surfaces, but no targeted `link_incremental` test surfaced.

Concrete failure mode:

The local/offline and incremental/production modes can drift in normalization, fuzzy date, manual mapping, and unmatched handling. Incremental updates cannot be characterized without mocking DB/API/cache behavior together, while pure match-policy changes are buried inside side-effectful functions. A partial batch failure also crosses policy, pagination, cache, and persistence concerns in one frame, making safe retry semantics difficult to prove.

Interaction with existing plans:

- Not covered by god-class Lanes 01-05 or the MLB stat-suite rebuild.
- This should remain a separate processing/linking candidate rather than being folded into feature-store or training work.

Safe first evidence step:

Extract no code initially. Add pure fixture-based characterization for team/player normalization, closest-game selection, manual/exact/fuzzy precedence, and equivalent outputs between local and incremental-shaped inputs. Add a mocked incremental batch test that records update boundaries and cursor behavior without DB access.

Done condition:

Normalization/match policy is pure and shared; CSV, DB, API, cache, and update persistence are adapters; local and incremental modes pass parity fixtures; pagination/retry/idempotency boundaries have focused tests; and the CLI functions only assemble and invoke these owners.

### PA-08 — DB configuration and engine creation are captured at import time

Severity: Medium
Confidence: High

Evidence:

- `src/db/client.py:11-18` registers a global psycopg2 adapter, calls `load_dotenv()`, and captures `DATABASE_URL` during import.
- `src/db/client.py:69-77` creates the default SQLAlchemy engine during import whenever that captured URL is present and exposes it as compatibility global `engine`.
- `src/db/client.py:80-110`, `get_engine`, is lazy only for the local engine; the default engine cannot observe environment changes after import.
- `tests/test_db_client.py:18-39` deliberately removes/reimports the module to test different URLs, demonstrating that configuration identity is tied to import order. No test covers changing config after import, disposal/reconfiguration, or concurrent local/default selection.

Concrete failure mode:

Test order, CLI bootstrap order, or a process that updates environment/config before selecting a runtime can retain an engine built from stale settings. Importing a seemingly passive module also mutates dotenv and psycopg2 global state and allocates an engine, making dependency injection and isolated tests harder. Compatibility consumers of module-level `engine` can bypass the `local` selection contract entirely.

Interaction with existing plans:

- Not directly covered by Lanes 01-05. It affects all lanes as infrastructure coupling, but should be fixed independently so migration diffs do not combine DB lifecycle changes with model behavior.

Safe first evidence step:

Add import-isolation tests proving current behavior when `DATABASE_URL` changes after import and inventory any direct `engine` imports. No connection should be opened. Decide and document whether configuration is intentionally immutable per process before changing behavior.

Done condition:

Engine creation is owned by an explicit lazy factory/config object; configuration timing is documented and tested; local/default engines have clear cache/disposal semantics; compatibility global access is removed or guarded; importing `src.db.client` does not create an engine; and no consumer bypasses the factory.

## Rejected suspicions

### R-01 — Large legacy MLB feature-store implementations require a new god-class lane

Rejected as a new finding. `src/models/mlb/features/legacy_pitcher_feature_store.py:60-1595` and `legacy_batter_feature_store.py:73-1333` are still large, but Lane 02 deliberately moved them behind thin stable facades and added source/request modules plus anti-regrowth tests (`tests/test_mlb_feature_store_inventory.py`). Size alone is insufficient. Reopen only with a concrete correctness or ownership failure not already bounded by Lane 02.

### R-02 — `run_mlb_sweep.py` remains the original MLB sweep god module

Rejected. Lane 01's progress and current tests show typed config, bootstrap, execution, cache, edge, result, line, and promotion seams plus inventory guards. Current artifact-consumer ambiguity is captured in PA-04 rather than mislabeling the runner migration incomplete.

### R-03 — NBA `FeatureStore` facade imports mean Lane 03 failed

Rejected. Lane 03 explicitly permits the stable thin facade and marks direct callsite import cleanup optional. No evidence in this audit showed raw source ownership regrowing into the facade.

### R-04 — The MLB stat-suite rebuild still needs generic wrappers, profiles, or artifact writers

Rejected as stale. The current tree contains the generic PowerShell runner/resume layer, `training/profiles.py`, `training/feature_controls.py`, `training/artifacts.py`, and `training/base_orchestrator.py`, with focused tests. PA-04 and PA-05 begin at the remaining consumer and strategy/workflow boundaries instead of repeating completed slices.

### R-05 — The legacy MLB single-config backtest path is silently promotion-capable

Rejected on current evidence. `src/backtesting/mlb/mlb_backtest_harness.py:16-20` labels it legacy/debug-only, runtime warnings repeat that at lines 116-118, and `tests/test_mlb_backtest_legacy_deprecation.py:10-38` requires an explicit `--allow-legacy` path. Keep the guard, but do not register a new dead-compatibility finding without a bypassing callsite.

### R-06 — Module-level `logging.basicConfig` calls are independently production-breaking

Not promoted to a finding. They are widespread in CLI-oriented modules and can affect embedding applications, but this bounded audit did not establish a concrete production failure or an ownership conflict beyond the stronger findings above. Treat as cleanup evidence only if a real importer/logging conflict is reproduced.

## Prioritized evidence queue

1. PA-01: offset activation inventory/guard test; no training.
2. PA-02: NBA daily/backtest probability parity fixtures, especially missing samples.
3. PA-03 and PA-04: filesystem-only artifact resolver/manifest consumer tests.
4. PA-06: NBA fast-sweep versus harness golden parity.
5. PA-05: orchestrator stage-contract and private-coupling inventory tests.
6. PA-07: local/incremental linker policy fixtures.
7. PA-08: DB client import/config timing characterization.

This queue gathers evidence only. It does not authorize implementation, model evaluation, artifact promotion, or production changes.
