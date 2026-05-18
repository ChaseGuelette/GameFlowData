# MLB Promotion Backtest Architecture Refactor Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Replace the current MLB sweep/backtest ambiguity with a typed, manifest-pinned, quote-clean promotion replay path.

**Architecture:** Keep behavior stable first. Extract contracts and artifact writing around the existing sweep before moving logic. The desired end state is a thin CLI over explicit services: config, data loading, line selection, edge calculation, simulation, artifact writing, and promotion evidence metadata.

**Tech Stack:** Python, pytest, existing `src/backtesting/mlb` modules.

---

## Problem summary

Current high-risk files:

- `src/backtesting/mlb/run_mlb_sweep.py` — ~1,508 LOC god module.
- `src/backtesting/run_sweep.py` — parallel generic implementation with duplicate class names.
- `src/backtesting/mlb/run_mlb_backtest.py` — legacy entrypoint, gated but still runnable.
- `src/backtesting/mlb/mlb_backtest_harness.py` — legacy/debug harness with full orchestration.
- `src/models/mlb/mlb_model_suite.py` — heuristic model loading based on files present.

Observed risks:

- Duplicate `SweepConfig` and `SweepResult` names with divergent schemas.
- Quote-clean promotion status is mostly a CLI/runtime convention.
- Legacy/debug paths can still generate metrics that look like promotion evidence.
- Model/artifact selection is implicit and heuristic.
- MLB line-selection constants are duplicated across sweep, daily runner, and line selection.

---

## Target boundaries

Create or evolve toward these boundaries:

- `src/backtesting/contracts.py`
  - shared dataclasses/protocols for sweep config/result/artifact metadata.
- `src/backtesting/mlb/promotion_config.py`
  - explicit `MLBPromotionBacktestConfig`, requiring quote-clean fields.
- `src/backtesting/mlb/artifact_writer.py`
  - writes promotion evidence bundle in one consistent layout.
- `src/backtesting/mlb/model_manifest.py`
  - loads and validates exact model suite manifest.
- `src/backtesting/mlb/line_config.py`
  - central line/stat/bookmaker config imported by sweep and daily runner.
- `src/backtesting/mlb/run_mlb_sweep.py`
  - eventually thin CLI/orchestrator only.

---

## Bite-sized tasks

### Task 1: Characterize existing promotion output

**Objective:** Add a focused test that documents the current MLB quote-clean output contract without changing behavior.

**Files:**
- Test: `tests/test_mlb_sweep_artifact_contract.py`
- Read/fixture source: existing small sweep result helpers if available.

**Validation:**
- `venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_artifact_contract.py -q`

---

### Task 2: Introduce explicit promotion config dataclass

**Objective:** Add a config object that makes quote-clean promotion requirements explicit.

**Files:**
- Create: `src/backtesting/mlb/promotion_config.py`
- Test: `tests/test_mlb_promotion_config.py`

**Acceptance:**
- Promotion config cannot be constructed without:
  - `quote_clean=True`
  - `quote_cutoff_time_et`
  - explicit model manifest path or artifact directory
  - explicit stat/config identity

---

### Task 3: Centralize MLB line-selection constants

**Objective:** Remove drift between daily runner, sweep, and line-selection modules.

**Files:**
- Create: `src/backtesting/mlb/line_config.py` or `src/models/mlb/line_config.py`
- Modify:
  - `src/backtesting/mlb/run_mlb_sweep.py`
  - `src/backtesting/mlb/line_selection.py`
  - `src/models/mlb/mlb_daily_runner.py`
- Test: `tests/test_mlb_line_config_parity.py`

**Acceptance:**
- One source of truth for:
  - excluded bookmakers
  - stat-to-market-key mapping
  - supported promotion stats

---

### Task 4: Extract artifact writer

**Objective:** Make artifact layout and promotion evidence metadata explicit.

**Files:**
- Create: `src/backtesting/mlb/artifact_writer.py`
- Test: `tests/test_mlb_sweep_artifact_writer.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

**Acceptance:**
- Saved result bundle includes metadata declaring:
  - quote-clean status
  - quote cutoff time
  - model manifest/artifact identity
  - line-selection config version/hash if practical
  - legacy vs promotion evidence flag

---

### Task 5: Add manifest-pinned model loading for promotion replay

**Objective:** Make promotion replay fail closed when exact artifacts are missing.

**Files:**
- Create: `src/backtesting/mlb/model_manifest.py`
- Modify: `src/models/mlb/mlb_model_suite.py` only as needed.
- Test: `tests/test_mlb_model_manifest.py`

**Acceptance:**
- Promotion replay does not silently load “latest” models.
- Missing required model artifacts are fatal for promotion mode.
- Non-promotion debug mode may keep heuristic loading but must label outputs debug-only.

---

### Task 6: Thin the CLI entrypoint

**Objective:** Start moving logic out of `run_mlb_sweep.py` without behavior changes.

**Files:**
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`
- Create modules only after tests cover current behavior.

**Extraction order:**
1. artifact writer
2. config validation
3. line-selection config
4. model manifest loading
5. edge computation
6. date processing/shared phases

---

## Validation commands

Use scoped tests first:

`venv/Scripts/python.exe -m pytest tests/test_mlb_quote_clean_line_selection.py tests/test_mlb_run_mlb_sweep_flat.py tests/test_mlb_backtest_legacy_deprecation.py -q`

Then add new tests as created:

`venv/Scripts/python.exe -m pytest tests/test_mlb_promotion_config.py tests/test_mlb_line_config_parity.py tests/test_mlb_sweep_artifact_writer.py tests/test_mlb_model_manifest.py -q`

---

## Done when

- MLB promotion replay has a clearly named typed config.
- Quote-clean is required for promotion evidence.
- Legacy/debug outputs are unmistakably labeled non-promotion.
- Model artifacts are loaded by manifest, not latest-directory heuristics.
- Line-selection constants are centralized.
- `run_mlb_sweep.py` is reduced toward orchestration rather than owning every responsibility.
