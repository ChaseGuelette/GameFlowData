# 07 — Implementation Sequence and Worker Specs

## Goal

Define the safe implementation order, review gates, rollback strategy, and worker-sized slices.

## Sequence overview

### Slice 0 — Docs only

Status: this plan set.

Files:

- `.hermes/plans/mlb-stat-suite-rebuild/*.md`

Validation:

```bash
git diff --check -- .hermes/plans/mlb-stat-suite-rebuild
```

### Slice 1 — CLV analyzer genericization

Status: complete locally on 2026-06-07; pending Chase review/commit.

Why first: low model risk, unblocks stat-generic audit suite, avoids pitcher CLV fork.

Files:

- [x] Create: `scripts/analyze_mlb_clv.py`
- [x] Modify: `scripts/analyze_mlb_batter_hits_clv.py`
- [x] Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- [x] Add/modify tests:
  - [x] `tests/test_analyze_mlb_clv.py`
  - [x] `tests/test_analyze_mlb_batter_hits_clv.py`
  - [x] `tests/test_run_mlb_quote_clean_audit_suite.py`

Worker spec summary:

- [x] Preserve all output file names and CLV semantics.
- [x] Add pitcher-shaped tests.
- [x] Keep backward-compatible batter script.

Validation:

```bash
./venv/Scripts/python.exe -m pytest tests/test_analyze_mlb_clv.py tests/test_analyze_mlb_batter_hits_clv.py tests/test_run_mlb_quote_clean_audit_suite.py -q
```

Result: passed locally on 2026-06-07 (`36 passed, 1 warning`).

### Slice 2 — Generic operational runner dry-run

Status: complete locally on 2026-06-21; pending Chase review/commit.

Why second: gives a clean user-facing path without touching training internals.

Files:

- [x] Create: `scripts/run_mlb_stat_ablation.ps1`
- [x] Create: `scripts/run_pitcher_k_ablation.ps1`
- [x] Create: `scripts/resume_mlb_stat_ablation_audit.ps1`
- [x] Add: `tests/test_mlb_stat_ablation_runner_static.py`

Worker spec summary:

- Implement dry-run and command generation first.
- Do not change existing batter wrapper yet.
- No training/backtest execution in tests.

Validation:

```bash
./venv/Scripts/python.exe -m pytest tests/test_mlb_stat_ablation_runner_static.py -q
```

Manual PowerShell dry-run after approval.

### Slice 3 — Training profiles and feature controls helpers

Status: complete locally on 2026-06-21; pending Chase review/commit.

Why third: pure helpers before editing large training files.

Files:

- [x] Create: `src/models/mlb/training/__init__.py`
- [x] Create: `src/models/mlb/training/profiles.py`
- [x] Create: `src/models/mlb/training/feature_controls.py`
- [x] Add: `tests/test_mlb_training_profiles.py`
- [x] Add: `tests/test_mlb_training_feature_controls.py`

Worker spec summary:

- [x] Encode batter_hits and pitcher_strikeouts profiles.
- [x] Add pitcher feature-family registry.
- [x] No existing pipeline changes yet.

Validation:

```bash
./venv/Scripts/python.exe -m pytest tests/test_mlb_training_profiles.py tests/test_mlb_training_feature_controls.py -q
```

### Slice 4 — Pitcher K feature controls in current pipeline

Why fourth: gives pitcher parity while minimizing refactor surface.

Files:

- Modify: `src/models/mlb/mlb_train_pipeline.py`
- Modify: `src/models/mlb/features/contracts.py` if profile cannot fully own family lists
- Add: `tests/test_mlb_pitcher_train_pipeline_variants.py`

Worker spec summary:

- Add CLI args matching batter pattern.
- Preserve legacy `--ablation-variant` behavior.
- Keep Phase 3A rejected features locked out by default.
- Persist metadata.

Validation:

```bash
./venv/Scripts/python.exe -m pytest tests/test_mlb_pitcher_train_pipeline_variants.py tests/test_mlb_training_pipeline_feature_loaders.py -q
./venv/Scripts/python.exe src/models/mlb/mlb_train_pipeline.py --help
```

### Slice 5 — Shared artifact helpers

Files:

- Create: `src/models/mlb/training/artifacts.py`
- Modify: `src/models/mlb/mlb_train_pipeline.py`
- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`
- Add/modify metadata tests.

Worker spec summary:

- Extract JSON writing and run-dir lifecycle.
- Preserve existing fields.
- Add optional `model_manifest.json` for new runs.

### Slice 6 — Base orchestrator extraction

Highest risk; do only after prior slices settle.

Files:

- Create: `src/models/mlb/training/base_orchestrator.py`
- Create strategy modules if needed.
- Modify both training pipelines.

Worker spec summary:

- Preserve CLI entrypoints and behavior.
- Move lifecycle, not model objective.
- Keep old class names importing/working if possible.

Validation:

- focused unit tests;
- CLI help checks;
- no long training unless separately approved.

### Slice 7 — Pitcher baseline restoration run docs

Files:

- Create/update: `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
- Create/update: `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md`

Needs Chase approval to run actual sweeps/training.

## Rollback strategy

- Each slice should be independently revertible.
- Keep compatibility wrappers until after repeated successful use.
- Avoid modifying production artifact directories.
- Do not delete old scripts in this rebuild.

## Worker-use rules

Given Chase's preference, subagents/workers must be tightly bounded:

- one slice per worker max;
- include exact files and tests;
- no broad repo scans;
- if the worker needs context compaction or becomes huge, stop and narrow the spec;
- main agent reviews scoped diff and runs focused validation.

## First recommended approval request

Ask Chase to approve Slice 1 only:

“Approve genericizing the CLV analyzer boundary (`analyze_mlb_clv.py`) and updating the audit suite to call it, with backward-compatible `analyze_mlb_batter_hits_clv.py` wrapper and pitcher-shaped tests?”

This is the safest first code change because it has no model-training behavior change.
