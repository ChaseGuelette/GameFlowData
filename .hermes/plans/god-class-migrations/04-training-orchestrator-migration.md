# Training Orchestrator Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to retrain models, promote artifacts, alter model math, or run long training/backtest jobs.

**Goal:** Split NBA and MLB training god-orchestrators into explicit training workflow components while preserving current artifact formats, feature semantics, calibration/reporting behavior, and command-line compatibility.

**Architecture:** Preserve current script entry points and orchestrator classes as compatibility facades. Extract config parsing, dataset loading, feature selection, hyperparameter resolution, model fitting, calibration evaluation, artifact writing, and sanity checks into focused services. Treat training behavior changes as separate experiments, not refactor side effects.

**Tech Stack:** Python, pandas, XGBoost model wrappers, existing feature stores, pytest, artifact JSON/manifests, GameFlow SQL-runner pattern for any DB truth.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `models/calibration-guide`
- `lessons/q10-miscalibration-is-edge`
- `lessons/empirical-cdf-for-probabilities`
- `lessons/feature-selector-is-not-an-ablation`
- `lessons/correlated-feature-family-validation`
- `lessons/cheap-baseline-before-architecture`

Applied lessons:

1. Full retrains are risky and must be validated with backtests; this migration must not silently change fit/eval behavior.
2. Never deploy global conformal recalibration offsets; calibration extraction must preserve evaluation/reporting, not add correction.
3. Q10 low-tail behavior is edge-bearing; do not “clean up” quantile behavior as part of structural work.
4. Feature selection is not an ablation; feature-selection services must label outputs as selector diagnostics, not model-promotion proof.
5. Correlated feature families need force-include/force-exclude downstream validation if changed.
6. Cheap-baseline lesson applies to future architecture changes, but this lane is structural extraction only.
7. Artifact directories under production are promotion-critical; atomic incomplete-to-final rename semantics must be preserved.

---

## Executive diagnosis

Training orchestration is currently concentrated in three large script/classes:

- `src/models/train_pipeline.py`
  - 1,195 total lines
  - 883 non-comment LOC
  - `TrainingOrchestrator`: 994 class lines, 18 methods
- `src/models/mlb/mlb_train_pipeline.py`
  - 834 total lines
  - 650 non-comment LOC
  - `MLBTrainingOrchestrator`: 656 class lines, 22 methods
- `src/models/mlb/mlb_batter_train_pipeline.py`
  - 953 total lines
  - 726 non-comment LOC
  - `MLBBatterTrainingOrchestrator`: 853 class lines, 14 methods

Together they own:

- CLI parsing and run mode validation
- run directory creation and atomic finalization
- feature-store construction
- training/calibration dataset loading
- cal-end-date filtering
- feature selection
- force-feature and no-prop-line variant policy
- hyperparameter tuning/loading
- model fitting
- component-level calibration evaluation
- combined/Monte Carlo calibration evaluation
- copula artifact generation
- sanity checks
- artifact manifests/configs/reports/metadata
- MLB pitcher IP/K-rate/copula submodels
- MLB batter binary/binomial/negbin variants

This creates a high-risk migration target because the orchestrators are the last line of defense before artifacts become production candidates.

---

## Current ownership problems

### 1. Orchestrators own both workflow and implementation details

Examples:

- NBA `run(...)` directly loads data, selects features, tunes, trains, evaluates, computes copula, saves, sanity-checks, and finalizes.
- NBA `run_partial(...)` additionally owns frozen-model loading, surgical retrain logic, force-feature mutation, partial hyperparam resolution, and mixed artifact saving.
- MLB pitcher `run(...)` owns IP feature injection, feature selection, tuning, training, calibration, sanity checks, copula, manifests, metadata.
- MLB batter `run(...)` owns stat-family dispatch and then separate binary/binomial/negbin pipelines.

Target split:

- Orchestrator facade: only validates request and executes ordered steps.
- Step services: one owner per training responsibility.

---

### 2. Dataset loading and temporal split policy are embedded in pipelines

NBA:

- `train_df = self.feature_store.get_training_dataset(train_seasons)`
- `cal_df = self.feature_store.get_training_dataset([calibration_season])`
- `cal_df = cal_df[cal_df["game_date"] < cal_end_date]`

MLB batter:

- `cal_df = cal_df[cal_df["game_date"] <= _cutoff]`

This difference may be intentional or accidental; the migration must document and characterize it before any change.

Target owner:

- `src/models/training/data_splits.py`
- `src/models/training/dataset_loaders.py`

Tests:

- NBA exclusive cal-end-date behavior preserved.
- MLB batter current inclusive behavior characterized before any decision to change.
- No leakage-affecting date policy change without separate approved fix.

---

### 3. Feature selection, forced features, and variants are mixed with model fitting

NBA:

- `_run_feature_selection(...)`
- `_run_feature_selection_partial(...)`
- `force_features` mutation inside `run_partial(...)`

MLB pitcher:

- `_run_feature_selection(...)`
- `_apply_ablation_forced_features(...)`
- `_add_predicted_ip_features(...)`

MLB batter:

- `_numeric_model_feature_candidates(...)`
- `exclude_prop_line` variant filtering
- binomial/negbin feature selection in model-specific methods

Target owner:

- `src/models/training/feature_selection_service.py`
- MLB specializations under `src/models/mlb/training/feature_selection.py`

Safety:

- Do not reinterpret selector output as feature-family value.
- Preserve `exclude_prop_line` metadata and preregistered comparison rule.

---

### 4. Calibration evaluation/reporting is tangled with training

NBA methods:

- `_evaluate_calibration(...)`
- `_calibrate_model(...)`
- `_calibrate_hurdle_model(...)`
- `_evaluate_combined_calibration(...)`
- `_analyze_minutes_rate_correlation(...)`

MLB pitcher methods:

- `_calibrate_on_holdout(...)`
- `_evaluate_calibration(...)`
- `_compute_copula_params(...)`
- `_save_copula_artifacts(...)`

MLB batter methods:

- `_compute_binomial_calibration(...)`
- `_compute_negbin_calibration(...)`

Target owner:

- `src/models/training/calibration_reports.py`
- `src/models/training/combined_calibration.py`
- `src/models/mlb/training/calibration_reports.py`

Non-goal:

- No global offsets, no automatic correction, no Q10 smoothing.

---

### 5. Artifact writing and finalization need a single owner

Current artifact responsibilities are scattered across:

- `_save_run_config(...)`
- `_save_feature_manifest(...)`
- `_save_calibration_report(...)`
- `_save_training_metadata(...)`
- `pipeline.save_all(...)`
- `model.save(...)`
- `self.run_dir.rename(final_dir)`

Target owner:

- `src/models/training/artifact_writer.py`
- `src/models/mlb/training/artifact_writer.py`

Required semantics:

- incomplete run directory naming remains atomic guard
- final rename happens only after successful validation/sanity checks
- run_config schema remains compatible
- feature manifest schema remains compatible
- metadata preserves git hash/run timestamp/training date info

---

### 6. CLI parsing and workflow config belong outside training internals

Each script mixes argparse, mode validation, local DB selection, and orchestrator construction.

Target owner:

- `src/models/training/cli.py` for NBA shared helpers
- `src/models/mlb/training/cli.py` for MLB shared helpers
- entry-point scripts remain import-compatible

Tests:

- parser rejects invalid mode combinations
- parser preserves existing defaults
- local flag still routes through existing `get_engine(local=True)` behavior where present

---

## Target design by responsibility

### A. `training/config.py`

Request/config dataclasses:

- `NbaTrainingRequest`
- `NbaPartialRetrainRequest`
- `CalibrationOnlyRequest`
- `TrainingRunContext`

MLB equivalents can live in `src/models/mlb/training/config.py`.

---

### B. `training/run_lifecycle.py`

Owns run directory, incomplete suffix, finalization, cleanup hooks, run identifiers.

---

### C. `training/dataset_loaders.py` and `training/data_splits.py`

Own train/calibration dataset loading and cal-end-date filtering.

---

### D. `training/feature_selection_service.py`

Own selector invocation, partial selector behavior, force-feature policy, selected-feature schemas.

---

### E. `training/hyperparams.py`

Own hyperparameter loading/tuning/partial resolution.

---

### F. `training/model_fitters.py`

Own fitting calls into existing model classes/pipelines.

---

### G. `training/calibration_reports.py`

Own component calibration reports and combined calibration calculations.

---

### H. `training/artifact_writer.py`

Own JSON outputs, manifests, reports, metadata, finalization gates.

---

### I. Compatibility facades

Keep these classes callable during migration:

- `src.models.train_pipeline.TrainingOrchestrator`
- `src.models.mlb.mlb_train_pipeline.MLBTrainingOrchestrator`
- `src.models.mlb.mlb_batter_train_pipeline.MLBBatterTrainingOrchestrator`

Endpoint scripts should continue to work.

---

## Refactor phases

### Phase 0: Characterization and inventory tests

Objective: Lock existing public shape and artifact schemas before extraction.

Files:

- Create: `tests/test_training_orchestrator_inventory.py`
- Existing: `tests/test_train_pipeline.py`
- Existing: `tests/test_mlb_batter_train_pipeline_variants.py`

Tests:

- public orchestrator classes importable
- key methods still exist
- artifact run_config keys characterized
- incomplete directory naming/finalization behavior characterized with tmp dirs
- NBA cal-end-date exclusive behavior characterized
- MLB batter cal-end-date inclusive behavior characterized as current behavior

Validation:

`venv/Scripts/python.exe -m pytest tests/test_train_pipeline.py tests/test_mlb_batter_train_pipeline_variants.py tests/test_training_orchestrator_inventory.py -q`

---

### Phase 1: Extract lifecycle and artifact writer first

Objective: Move low-math filesystem/report code without touching training semantics.

Files:

- Create: `src/models/training/__init__.py`
- Create: `src/models/training/run_lifecycle.py`
- Create: `src/models/training/artifact_writer.py`
- Create: `tests/test_training_run_lifecycle.py`
- Create: `tests/test_training_artifact_writer.py`
- Modify: `src/models/train_pipeline.py`

TDD:

1. Write tests against expected directory names and JSON keys.
2. Extract code.
3. Keep `TrainingOrchestrator._save_*` wrappers delegating to writer initially.

No model fitting changes.

---

### Phase 2: Extract config and CLI parsing

Objective: Separate mode validation from training execution.

Files:

- Create: `src/models/training/config.py`
- Create: `src/models/training/cli.py`
- Create: `tests/test_training_cli_config.py`
- Modify: `src/models/train_pipeline.py`
- Later mirror for MLB under `src/models/mlb/training/`.

Tests:

- invalid `--calibrate-only` + retrain combination still errors
- partial retrain still requires `--base-model-dir`
- existing defaults preserved

---

### Phase 3: Extract dataset loading and split policy

Objective: Make temporal split semantics explicit and testable.

Files:

- Create: `src/models/training/data_splits.py`
- Create: `src/models/training/dataset_loaders.py`
- Create: `tests/test_training_data_splits.py`
- Modify: `src/models/train_pipeline.py`

Tests:

- `filter_calibration_before(df, cutoff)` uses `< cutoff` for NBA current behavior
- no mutation of input DataFrame unless current behavior requires it
- empty calibration data handling preserved

MLB checkpoint:

- Do not normalize MLB batter `<= cutoff` to NBA `< cutoff` during extraction. Characterize first; fix later only if approved.

---

### Phase 4: Extract feature-selection service

Objective: Move selector orchestration without changing selected features.

Files:

- Create: `src/models/training/feature_selection_service.py`
- Create: `tests/test_training_feature_selection_service.py`
- Modify: `src/models/train_pipeline.py`

Tests:

- disabled/reused selection paths preserve output schema
- force-feature append order preserves current behavior
- partial retrain reselect updates only requested components

Model lesson guard:

- Test names/docs must state selector output is not promotion evidence.

---

### Phase 5: Extract hyperparameter resolver

Objective: Move tuning/load/partial fallback behavior.

Files:

- Create: `src/models/training/hyperparams.py`
- Create: `tests/test_training_hyperparams.py`
- Modify: `src/models/train_pipeline.py`

Tests:

- disabled tuning returns `None`
- JSON hyperparams load and are copied to run dir
- partial retrain resolves component hyperparams from existing artifacts/current behavior

---

### Phase 6: Extract calibration reports

Objective: Move report calculations without correction behavior.

Files:

- Create: `src/models/training/calibration_reports.py`
- Create: `src/models/training/combined_calibration.py`
- Create: `tests/test_training_calibration_reports.py`
- Modify: `src/models/train_pipeline.py`

Tests:

- current `tests/test_train_pipeline.py::TestCalibrateModel` keeps passing
- combined calibration report schema preserved
- no offsets generated/applied unless existing calibrate-only path explicitly does so
- quantile monotonicity/inversion checks preserved in sanity path

---

### Phase 7: NBA facade shrink

Objective: Reduce `TrainingOrchestrator` to workflow adapter.

Guardrail threshold:

- after extraction, `src/models/train_pipeline.py` non-comment LOC should be under an agreed threshold, recommended initial threshold: 500 excluding CLI compatibility.

---

### Phase 8: Repeat pattern for MLB pitcher and MLB batter

Order:

1. MLB batter artifact/config/variant services first because no-prop-line variant tests already exist.
2. MLB pitcher lifecycle/artifacts/config.
3. MLB pitcher IP/K-rate/copula services.
4. MLB batter binary/binomial/negbin strategy services.

MLB-specific target modules:

- `src/models/mlb/training/config.py`
- `src/models/mlb/training/artifact_writer.py`
- `src/models/mlb/training/feature_selection.py`
- `src/models/mlb/training/pitcher_pipeline.py`
- `src/models/mlb/training/batter_binary_pipeline.py`
- `src/models/mlb/training/batter_binomial_pipeline.py`
- `src/models/mlb/training/batter_negbin_pipeline.py`
- `src/models/mlb/training/calibration_reports.py`

---

## Files likely touched

NBA core:

- `src/models/train_pipeline.py`
- `src/models/training/__init__.py` (new)
- `src/models/training/config.py` (new)
- `src/models/training/cli.py` (new)
- `src/models/training/run_lifecycle.py` (new)
- `src/models/training/artifact_writer.py` (new)
- `src/models/training/data_splits.py` (new)
- `src/models/training/dataset_loaders.py` (new)
- `src/models/training/feature_selection_service.py` (new)
- `src/models/training/hyperparams.py` (new)
- `src/models/training/model_fitters.py` (new)
- `src/models/training/calibration_reports.py` (new)
- `src/models/training/combined_calibration.py` (new)

MLB core:

- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `src/models/mlb/training/*.py` (new package)

Tests:

- `tests/test_train_pipeline.py`
- `tests/test_mlb_batter_train_pipeline_variants.py`
- `tests/test_training_orchestrator_inventory.py` (new)
- `tests/test_training_run_lifecycle.py` (new)
- `tests/test_training_artifact_writer.py` (new)
- `tests/test_training_cli_config.py` (new)
- `tests/test_training_data_splits.py` (new)
- `tests/test_training_feature_selection_service.py` (new)
- `tests/test_training_hyperparams.py` (new)
- `tests/test_training_calibration_reports.py` (new)
- `tests/test_mlb_training_artifact_writer.py` (new)
- `tests/test_mlb_training_variant_services.py` (new)

---

## Validation commands

Focused NBA baseline:

`venv/Scripts/python.exe -m pytest tests/test_train_pipeline.py -q`

Focused MLB baseline:

`venv/Scripts/python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q`

Training lane after phase 1-3:

`venv/Scripts/python.exe -m pytest tests/test_train_pipeline.py tests/test_training_run_lifecycle.py tests/test_training_artifact_writer.py tests/test_training_cli_config.py tests/test_training_data_splits.py -q`

Training lane after calibration extraction:

`venv/Scripts/python.exe -m pytest tests/test_train_pipeline.py tests/test_training_calibration_reports.py -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/models/train_pipeline.py src/models/training/*.py src/models/mlb/mlb_train_pipeline.py src/models/mlb/mlb_batter_train_pipeline.py src/models/mlb/training/*.py`

Diff hygiene:

`git diff --check -- src/models/train_pipeline.py src/models/training src/models/mlb/mlb_train_pipeline.py src/models/mlb/mlb_batter_train_pipeline.py src/models/mlb/training tests .hermes/plans/god-class-migrations/04-training-orchestrator-migration.md`

Optional characterization only, not default:

- Use small fixture/unit tests rather than launching long retrains.
- Chase prefers to run training/backtest sweeps manually.

---

## Risk controls / non-goals

Non-goals:

- Do not train or promote models as part of extraction.
- Do not change hyperparameters.
- Do not change feature selection tolerance defaults.
- Do not change no-prop-line variant behavior.
- Do not introduce global calibration offsets.
- Do not normalize NBA/MLB split semantics without separate approval.
- Do not change artifact schema names unless compatibility adapters/tests exist.
- Do not merge feature-store migration into training orchestrator migration.

Hard rules:

- Existing CLI commands should keep working.
- Existing artifact JSON keys should remain stable.
- Incomplete-to-final directory finalization must remain atomic.
- Behavior-changing leakage fixes require a separate RED test and Chase approval.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A training method owns a hidden lifecycle/state transition.
2. A run_config key is consumed by another job or dashboard path.
3. A feature manifest schema differs by sport/stat/model family.
4. A private orchestrator method is used by tests or scripts.
5. A calibration report is both diagnostic and promotion gate.
6. A cal-end-date split differs across NBA/MLB and may be a leak fix.
7. A model-family variant has pre-registered comparison metadata.
8. A compatibility facade needs a future deletion/removal guard.
9. A behavior-changing model fix appears; split it from structural extraction.
10. A long training/backtest is needed for evidence; stop and ask Chase to run/approve.

Progress log entries must distinguish: module exists, wrapper delegates, old logic removed, CLI migrated, artifact schema parity verified, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with training lifecycle/artifact writer inventory, not model fitting:

1. Add inventory tests for public classes, run directory naming, run_config keys, and current split policy.
2. Extract `run_lifecycle.py` and `artifact_writer.py` for NBA only.
3. Keep old `_save_*` methods as wrappers.
4. Run existing `tests/test_train_pipeline.py`.
5. Do not touch feature selection, model training, calibration math, or MLB yet.

This creates the same kind of safe seam that made the Kalshi migration tractable.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- AST/method inventory for `src/models/train_pipeline.py`.
- AST/method inventory for `src/models/mlb/mlb_train_pipeline.py`.
- AST/method inventory for `src/models/mlb/mlb_batter_train_pipeline.py`.
- `tests/test_train_pipeline.py` coverage shape.
- `tests/test_mlb_batter_train_pipeline_variants.py` no-prop-line variant tests.
- Callsite scan across `src`, `scripts`, and `tests`.
- GBrain hard facts, critical invariants, calibration guide, and model lessons.

Current status:

- Documentation only.
- No production code changed.
- No model training launched.

---

## Done when

- Training orchestrator classes are thin workflow facades.
- Run lifecycle/artifact writing has one owner.
- Dataset split policy is explicit and tested.
- Feature selection, hyperparams, model fitting, calibration reports, and sanity checks have focused owners.
- NBA and MLB training scripts keep existing CLI compatibility.
- No model math, artifact schema, or calibration policy changes occur without separate approved experiments.
