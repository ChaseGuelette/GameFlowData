# Feature Store Boundary Refactor Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Make train/backtest/daily feature assembly explicit and testable, with no pipeline calling private feature-store helpers directly.

**Architecture:** Preserve existing outputs. First characterize parity, then expose public APIs that wrap existing private transformations. Only after parity tests pass should internal implementation be reorganized.

**Tech Stack:** Python, pytest, pandas, existing MLB feature stores.

---

## Problem summary

Current high-risk files:

- `src/models/mlb/mlb_feature_store.py`
- `src/models/mlb/mlb_batter_feature_store.py`
- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `src/models/mlb/mlb_daily_runner.py`

Core issue:

Training pipelines manually call private helpers such as:

- `_add_interaction_features`
- `_add_batter_interaction_features`

Daily/inference paths also apply transformations. That means the canonical feature assembly path is implicit and split across callers.

---

## Target boundaries

Feature stores should expose public methods like:

- `build_training_features(...)`
- `build_backtest_features(...)`
- `build_inference_features(...)`
- `apply_canonical_transforms(...)`

Private helpers can remain private, but only the feature store should call them.

---

## Bite-sized tasks

### Task 1: Add characterization tests for current transform parity

**Objective:** Lock current behavior before reorganizing.

**Files:**
- Create: `tests/test_mlb_feature_store_transform_parity.py`

**Acceptance:**
- Tests use tiny pandas fixtures where possible.
- Tests verify public/private current transform outputs for pitcher and batter features.

---

### Task 2: Add public canonical transform methods

**Objective:** Wrap existing private transformation sequence in public APIs.

**Files:**
- Modify:
  - `src/models/mlb/mlb_feature_store.py`
  - `src/models/mlb/mlb_batter_feature_store.py`
- Test: `tests/test_mlb_feature_store_transform_parity.py`

**Acceptance:**
- Public methods produce identical output to previous private-helper call sequence.

---

### Task 3: Stop training pipelines from calling private helpers

**Objective:** Move callers to public feature-store APIs.

**Files:**
- Modify:
  - `src/models/mlb/mlb_train_pipeline.py`
  - `src/models/mlb/mlb_batter_train_pipeline.py`
- Test:
  - `tests/test_mlb_feature_store_transform_parity.py`
  - relevant training pipeline tests if present.

**Acceptance:**
- No direct calls to `_add_interaction_features` or `_add_batter_interaction_features` outside their defining classes.

---

### Task 4: Add train/inference feature list parity checks

**Objective:** Ensure training and daily inference use compatible feature columns.

**Files:**
- Create: `tests/test_mlb_train_inference_feature_parity.py`

**Acceptance:**
- Test compares required model feature names to generated inference columns for a small fixture or mocked feature store output.
- Missing features fail loudly.

---

### Task 5: Document canonical feature assembly contract

**Objective:** Prevent future drift.

**Files:**
- Create or update: `docs/development_docs/mlb_feature_assembly_contract.md`

**Document should state:**
- Which public methods are canonical.
- Which callers should use which method.
- Private helper calls from pipelines/runners are forbidden.
- Feature parity tests must be updated when features change.

---

## Validation commands

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_transform_parity.py tests/test_mlb_train_inference_feature_parity.py -q`

Also run any existing MLB model/feature tests relevant to touched files.

---

## Done when

- Pipelines no longer call feature-store private helpers.
- Canonical train/backtest/inference feature APIs are public and documented.
- Parity tests fail if feature assembly drifts.
