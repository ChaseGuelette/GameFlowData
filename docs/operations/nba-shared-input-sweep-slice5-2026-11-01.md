# NBA shared-input sweep — coding slice 5 implementation plan

> For Hermes: use the GameFlow implementation-worker lane after the launch gates below pass; independent specification review precedes quality review. Do not start a writer from this planning document alone.

Goal: compare explicitly declared raw/BL settings using one identified prediction/sample/quote input bundle and the existing corrected NBA betting logic.

Architecture: a sequential acquisition phase produces a process-local shared bundle; independent cells invoke the final slice-3 harness edge/selection methods, fresh simulators and existing metrics/export APIs. Keep legacy sweep behavior separate rather than importing its unsafe fast path. No persistent cache or second probability/simulator implementation.

Tech stack: existing Python, pandas, NumPy, argparse, artifact validation, pytest/Ruff; no dependencies added.

## Status and owner decisions

- November 1, 2026: Chase requested scoping/setup now, with implementation waiting for coding slice 3 to complete.
- Historical S1 is deferred by Chase. Do not merge, modify, copy or make S1 integration a prerequisite for this slice.
- This is a prepared implementation scope, not approved numerical policies, implementation launch, merge, or historical-data certification.
- Coding slices 1–2 remain uncommitted in `repair/nba-cli-preflight`; slice 3 remains active in `repair/nba-flat-replay`. Main currently does not contain their complete source. Completion of slice 3 alone does not make main a usable launch base.
- All grid values are operator inputs, not recommended trading parameters. Raw/no-BL and flat staking are the comparison controls. BL experiments remain optional and finalist-only for model evaluation.

## Launch gates — STOP until all pass

### Owner authorization update — November 1, 2026

Chase subsequently authorized proceeding with slice 5 against the available slice-3 plumbing before its last review closes. This supersedes only the completed-review/fully-integrated-base prerequisites below for isolated implementation, not final integration or historical acceptance. The coordinator created `repair/nba-shared-input-sweep` at `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-shared-input-sweep`, base `10d9c2258f9a26eb3ff5ef424cbeb66cd3093ff3`, and copied exactly two slice-3 source dependencies (`backtest_harness.py`, `bet_simulator.py`) plus slice-2's self-contained `nba_artifacts.py`. These copies are inherited read-only prerequisites, not slice-5 edits or an integration receipt. No replay CLI, training source, test fixtures or S1 changes were copied. Source snapshots/hashes are in `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice5/dependency-baseline.json`; before acceptance check for upstream drift and reconcile only with explicit ownership, then rerun affected validation. Final slice-3/source integration remains separate. The implementation file allowlist and no-test-edit/no-job/no-merge restrictions remain unchanged.

1. Slice 3 owner delivers completed source, final specification/quality review, and actual controlled execution results. Do not trust a stale in-progress copy.
2. An explicitly approved integration/checkpoint supplies slices 1–2 and final slice 3 together. Record the exact base SHA and source inventory; reconcile their `run_backtest.py` overlap first. Do not silently perform that merge under this scope.
3. Recheck registered worktrees and scoped status/diffs; confirm the final interfaces listed below. No duplicate writer and no edits to another owner's worktree.
4. Chase approves this exact implementation scope, including the choices in the next section. If final slice 3 requires additional source edits, revise the scope before launch.
5. Create a fresh `repair/nba-shared-input-sweep` worktree from that reviewed base, not today's stale main. Record its native Windows path in this document. Do not create it or start an automatic waiting/polling job now.

S1, sync completion and historical source-policy approval are NOT prerequisites for controlled offline implementation verification. Real data/model acceptance remains a later gate and cannot be inferred from these checks.

## Proposed scope choices for approval

- Flat staking only in the first slice-5 execution interface: explicit positive finite flat amount and starting bankroll. No Kelly/ranker optimization or mixed sizing comparisons. Preserve existing flat caps/settlement semantics.
- Fixed acquisition settings for the whole sweep: exact model, ordered stats, bookmaker list, dates (both endpoints inclusive), minimum-minutes filter, sample count, seed and all predictor settings. No per-cell model/stat/source/feature/filter/sample/RNG changes.
- Cell dimensions: global edge threshold and optional global BL `tau`, `z_max`, `max_weight`. No per-stat Cartesian grid or configuration-precedence redesign. Record effective values; irrelevant BL dimensions do not multiply raw cells.
- Include a raw/no-BL control for each distinct requested edge threshold, at the same flat stake, same allowed directions and same input bundle. No silent optimizer or automatic best-cell promotion.
- Sequential acquisition and cells; reuse a single newly initialized predictor during acquisition, never during cell evaluation. Explicitly pass seed to its existing `random_state` constructor.
- Process-local cache only. Save identity/evidence receipts, not a reusable serialized sample cache. Cross-run loading/resume/persistent invalidation are deferred.
- Existing historical SQL remains unchanged and unverified. Any future actual-data execution requires explicit diagnostic acknowledgement and always emits `historical_readiness=unverified`, `decision_grade=false`. No quote-clean/source-policy claim or production consumer wiring.

## Allowed implementation files

Create only after approval:

- `src/backtesting/nba/__init__.py` — package marker if still absent at launch.
- `src/backtesting/nba/sweep_config.py` — typed shared/cell settings, validation, canonical plan/cell identities, deterministic grid expansion.
- `src/backtesting/nba/sweep_runner.py` — shared acquisition/bundle, isolated cell evaluation, evidence/export and failure boundaries.
- `src/backtesting/nba/run_nba_sweep.py` — importable parser/main, DB-free dry-run, explicit execution bootstrap.

Documentation owned by coordinator:

- This file and new `docs/understanding/nba-shared-input-sweep.md`.
- Narrow slice-5/status/link updates in `docs/understanding/nba-training-evaluation-slices.md` and `docs/operations/nba-coding-readiness-plan-2026-11-01.md`.

Read/reuse, do not modify:

- `src/backtesting/backtest_harness.py`, including `BacktestResult`, `_calculate_edges`, `_filter_best_bets`, `_get_game_dates`, `_prefetch_all_lines`, `_get_actuals`, `_get_voids`, `_merge_actuals`.
- `src/backtesting/bet_simulator.py`, `performance_metrics.py`.
- Integrated `src/models/nba_artifacts.py`, `training_preflight.py`, `quantile_trainer.py`, `monte_carlo.py`, and final `run_backtest.py` bootstrap as reference.
- `src/config/stat_config.py`, existing `BLConfig`/`BlackLittermanBlender` definitions and actual consumer signatures.

Protected: harness `run` and `_run_date`; all S1 files/changes; all tests/fixtures; training/inference/model math/calibration; source SQL, clocks/cohort policies, quote primitives/consumers; legacy `src/backtesting/run_sweep.py`; MLB code; sync scripts. Stop and request scope expansion if any protected implementation must change. No test additions or edits, commits, pushes, merges, DB access, training or deployment during the implementation task.

## Verified starting seams — revalidate against final base

Read-only inspection of slice-3 worktree found:

- `BacktestHarness._run_date` currently combines sorted/minutes-filtered features, `predict_batch_for_date`, `derive_combo_samples`, date tagging, quote merge, `_calculate_edges` and `_filter_best_bets`. Calling it per cell would regenerate samples; do not do that.
- `BacktestResult` carries prediction/bet/candidate-edge frames, metrics, dates and config. `to_csv` writes predictions/bets/metrics and conditionally candidate edges. The sweep must additionally write a schema-correct empty candidate file for empty cells.
- `BetSimulator` owns mutable bets/bankroll/day-start state. Build a fresh instance per cell, not a reset/deepcopy of the full harness.
- `MetricsCalculator.calculate(predictions_df, bets_df, starting_bankroll=...)` expects actuals for calibration and uses existing betting/risk logic.
- Slice-2 `validate_artifact_bundle(bundle_dir, requested_stats, allow_legacy=False)` returns an `ArtifactInspection`; strict execution requires `verified_manifest` and its content identity. No copied validator, legacy opt-in or latest-model discovery in the new command.
- `MonteCarloPredictor` already accepts `n_samples` and `random_state`; its RNG advances with call order. No `monte_carlo.py` change is needed for sequential fresh-predictor execution.
- Legacy NBA `run_sweep.py` already has shared acquisition and a slow harness-backed cell path, but its acquisition catches prediction errors and continues, does not derive combos there, and its fast path substitutes 0.5 for absent samples, reimplements selection/BL, and omits all-edge output. Do not import those execution paths into the new command or claim legacy behavior repaired.
- Harness per-stat blender construction supplies only tau, whereas a full `BLConfig` can carry tau/z_max/max_weight. For global BL cells use the existing full global blender path with `stat_config=None`; do not let a tau-only per-stat override erase effective z_max/max_weight. Verify the final slice-3 implementation before choosing the call construction.

## Implementation tasks

### 1. Freeze config and plan contracts

Implement strict typed inputs and an importable parser/main. Proposed flags (not yet executable): `--model-dir`, `--start`, `--end`, `--stats`, `--bookmakers`, `--n-samples`, `--seed`, `--min-minutes-avg`, `--starting-bankroll`, `--flat-bet-size`, `--allowed-bets`, `--edge-thresholds`, `--bl-taus`, `--bl-z-max`, `--bl-max-weight`, `--max-cells`, `--output-dir`, `--local`, `--diagnostic-history`, `--dry-run`.

Require an explicit exact artifact directory, inclusive ordered dates, ordered unique supported stats/books, explicit finite controls, positive sample count/flat stake/bankroll, valid seed accepted by the predictor, and coherent allowed stat/direction pairs. Trace actual BLConfig domains and enforce them without changing BL math. Reject duplicate/conflicting config tokens rather than silently overwrite them. Preserve literal opaque identifiers.

`--dry-run` returns JSON with resolved settings, deterministic cell list/count, planned identity, dependencies and unverified readiness. It must not construct an engine, deserialize a model, make a predictor, acquire data or create output directories. Distinguish a filesystem-validated artifact from loaded-model proof. Missing dependencies/invalid metadata fail before effects, not a success-shaped empty plan. Use a bounded default maximum of 64 distinct cells; requests above the explicit cap fail before any runtime side effects.

### 2. Strict bootstrap and shared acquisition

Validate the explicit artifact with the integrated slice-2 API before model loading or engine construction. Read only runtime model settings needed by the existing bootstrap; preserve embedded per-model calibration, never load combined/global offsets. No fallback to another artifact. Actual-data execution requires `--diagnostic-history`; do not run it in this implementation session.

Acquire game dates, range features, quote candidates, actuals and voids once. For each sorted date, apply the same final replay feature filtering/order, invoke prediction once and derive combo samples once using the existing helper. Preserve keys `(player_id, game_id, stat)`, draw alignment, full prediction fields, quote provenance and raw candidate evidence. Do not calculate cell-specific selection during acquisition.

Track expected dates, explicit legitimate empty/no-feature/filtered/no-quote outcomes and observed counts. Unexpected query/inference/assembly exceptions abort the sweep with sanitized date/stage context before any accepted cell outputs. Do not copy legacy catch-and-continue behavior. This boundary does not repair hidden partial returns inside incumbent loaders; record that historical completeness is unverified while S1 is deferred.

### 3. Bundle identity and immutability

Keep request identity separate from observed input-content identity. Bind the latter to artifact manifest/file identity, ordered feature identity/settings, dates, fixed filters/stats/books, sample count/seed/actual predictor settings, source/policy identifiers (unknown explicitly), observed dates/counts and content digests of predictions, sample keys/dtypes/shapes/bytes, quote candidates, actuals and voids.

Define deterministic encoding for supported date/datetime/numeric/array/DataFrame values, explicit nulls/nonfinite evidence, column order, row order and timezone information. Do not stringify arbitrary objects or invent source watermarks. Reject unsupported identity values clearly. The current query ordering is not certified unique: digest the actual observed traversal and do not promise identical new acquisitions merely from equal seeds.

Only the single bundle acquired for this execution is eligible for cell reuse. A mismatched request, changed evidence or mutation invalidates it; no lookup by path/mtime/seed alone. Arrays are shared read-only; frames/config/blenders are cell-local. Check input digests before/after evaluation and reject mutation. Do not deepcopy engines/models or per-cell copy every sample array unnecessarily. Revalidate artifact identity at completion so mid-run artifact drift cannot yield an accepted bundle.

### 4. Evaluate cells through existing owners

For each deterministic cell create a lightweight harness over shared runtime references with a cell-local full BL config and fresh simulator. Do not call harness `run`/`_run_date`, DB loaders, model predict or combo generation from the cell phase.

Use copies of raw frames and the immutable samples with `_calculate_edges`; retain the entire pre-selection candidate result including sample rejection evidence. Then invoke `_filter_best_bets`, preserving slice-3 chosen-side/quote metadata. Missing/invalid samples must remain non-actionable; never substitute Gaussian/0.5/market-prior probabilities. No quote means no wager, not actionable unmerged predictions.

Apply the same stable date/row ordering and `resolve_voids` -> `resolve_bets` -> `evaluate_predictions`, followed by final settlement, actual merge and `MetricsCalculator.calculate`. Flat dollar amount and allowed directions are fixed across cells. No hidden opposite-side fallback or routing changes. Preserve all existing clipping/BL math and Q10 behavior. Prove an equivalent single-cell direct replay produces the same candidates/selected sides/bets/settlement/metrics on controlled inputs.

### 5. Export truthful, failure-safe evidence

Use collision-safe new output paths: never overwrite existing sweep/cell results. Persist an incomplete execution record before work, finalize only after every requested cell and expected output verifies. An exception or output failure leaves incomplete/failed state and returns nonzero; never produce a completed aggregate from a subset of cells.

Proposed output contract:

- `sweep_manifest.json`: schema, run state, source/base identity, fixed config, observed bundle identity, cells/counts, expected/acquired dates, diagnostic status, failure/no-op reasons, artifact verification and output digests.
- `sweep_summary.csv`: one row per completed cell, identity/config, explicit raw-control flag, counts, existing metrics and readiness state. Publish complete summary only when all cells succeed.
- `cells/<cell-id>/`: effective `config.json`, `predictions.csv`, `bets.csv`, `all_bookmaker_edges.csv`, `metrics.json`; all required files exist even for legitimate empty results, with documented headers/counts.

Use existing result/metrics serialization where applicable; verify JSON-safe/nonfinite handling explicitly. Cell IDs derive from effective canonical config, not rounded display labels. Planned config, observed identity and actual effective config must agree. All executed output remains diagnostic/not-decision-grade, with unresolved timing/source/completeness listed. No CLV audit, lifecycle/resume, ranking, trading decision or promotion.

## Validation and independent review

Use the existing native interpreter; no dependencies installed and no DB/network/model fitting or production model deserialization. From the future integration worktree, PowerShell-safe commands:

- `$env:PYTHON_DOTENV_DISABLED='1'; & 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m pytest tests/test_run_sweep.py tests/test_run_backtest.py tests/test_bet_simulator.py tests/test_backtest_harness.py tests/test_nba_feature_store_inventory.py -q`
- `& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m ruff check src/backtesting/nba`
- `& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m compileall -q src/backtesting/nba`
- `git diff --check -- src/backtesting/nba docs/operations/nba-shared-input-sweep-slice5-2026-11-01.md docs/understanding/nba-shared-input-sweep.md`

Confirm named existing test paths and any environment/network requirements before running; establish baseline then compare final results. Do not silently repair fixtures or weaken assertions. Existing suites do not establish new sweep acceptance.

Disposable real-path probes go under Hermes scratch, not permanent repo tests. Required cases:

1. Actual help/dry-run/invalid CLI with engine/model/predict/output tripwires; malformed grids/caps fail before effects.
2. Multi-date, multi-cell shared acquisition counts: one acquisition phase, one prediction call per eligible date, no inference/DB calls in cells. Combo values/draw alignment preserved.
3. Raw control and BL cell parity with the corrected actual harness/simulator; full tau/z_max/max_weight reach the actual blender. Flat controls, chosen direction, allowed sides, voids/pushes/final settlement and bankroll depletion retain behavior.
4. Missing/empty/nonfinite/malformed samples stay rejected across raw/BL; no blender receives invalid samples and no phantom wager occurs.
5. Cell-order reversal gives identical per-cell outputs; input hashes remain unchanged; cell state never leaks. Same controlled request/content yields same identity. Change artifact bytes, quotes, sample bytes/seed/count, feature/source/policy settings, dates or actuals and invalidate identity as appropriate.
6. Legitimate empty slate/no-feature/no-quote/no-bet cases produce complete explicit empty evidence. Acquisition failure, failed middle cell/export, collision, input mutation and artifact drift yield nonzero/incomplete, not a complete summary.
7. Required files/config/counts/digests read back and agree with actual rows. New untracked source files get full review; scoped `git diff` alone omits them.

Save receipts with actual observed results, not expected/fabricated outputs. Independent spec review then quality review; coordinator reruns affected checks after corrections. Stop on source/fixture scope expansion. Offline results certify mechanics only, never historical ROI or model quality.

## Relevant prior lessons/invariants

Canonical Hard Facts, Critical Invariants, lesson-tag inventory and keyword search were retrieved for this scope. Apply `lessons/empirical-cdf-for-probabilities`: reuse empirical sample probabilities, no Gaussian/fake fallback. Apply `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: source columns and a common bundle do not certify historical consumer queries or artifacts. Preserve Q10/no-global-offset invariants. Long runs and real data checks remain owner-launched/separately scoped; SQL remains isolated. No S1 integration is implied.

## Next action

Review/approve this scope. Wait for final slice 3 and explicit slices-1–3 integration checkpoint; refresh exact APIs/base SHA, then prepare the worker launch prompt and start only after owner authorization. Do not schedule an automatic launch or treat this document as an implementation receipt.
