# 06 — Tests and Validation Plan

## Goal

Define tests and validation commands for the rebuild, with characterization before extraction.

## Test categories

### 1. Static/characterization tests

Purpose: lock current behavior before refactor.

Existing:

- `tests/test_mlb_training_pipeline_feature_loaders.py`
- `tests/test_mlb_batter_train_pipeline_variants.py`
- `tests/test_mlb_sweep_results.py`

Add:

- `tests/test_mlb_training_profiles.py`
- `tests/test_mlb_training_feature_controls.py`
- `tests/test_mlb_pitcher_train_pipeline_variants.py`
- `tests/test_mlb_stat_ablation_runner_static.py`

### 2. CLV genericization tests

Existing:

- `tests/test_analyze_mlb_batter_hits_clv.py`
- `tests/test_analyze_mlb_clv_ranking_diagnostics.py`
- `tests/test_run_mlb_quote_clean_audit_suite.py`

Add/modify:

- `tests/test_analyze_mlb_clv.py`
- pitcher-shaped rows for `pitcher_strikeouts`
- compatibility wrapper test for `analyze_mlb_batter_hits_clv.py`
- audit suite command assembly test verifying generic analyzer path.

### 3. Operational runner dry-run tests

PowerShell is harder to unit test from Python, so start with static and dry-run checks:

- script exists;
- accepted profiles are present;
- dry-run command includes expected fragments;
- pitcher profile emits `--stats pitcher_strikeouts` and `--direction under`;
- batter profile emits `--stats batter_hits` and batter train command.

### 4. Training refactor tests

Add tests around pure helpers first:

- profile fields for `batter_hits` and `pitcher_strikeouts`;
- feature-family expansion and conflict behavior;
- missing/non-numeric forced features fail loudly;
- artifact metadata writer produces expected JSON;
- run dir finalization preserves `_incomplete` behavior.

## Focused validation commands

Use Git Bash terminal syntax unless running PowerShell manually.

### Syntax / import checks

```bash
./venv/Scripts/python.exe -m py_compile \
  src/models/mlb/mlb_train_pipeline.py \
  src/models/mlb/mlb_batter_train_pipeline.py \
  src/models/mlb/mlb_model_suite.py \
  src/backtesting/mlb/run_mlb_sweep.py \
  scripts/run_mlb_quote_clean_audit_suite.py \
  scripts/analyze_mlb_clv.py
```

### Focused pytest set

```bash
./venv/Scripts/python.exe -m pytest \
  tests/test_mlb_training_pipeline_feature_loaders.py \
  tests/test_mlb_batter_train_pipeline_variants.py \
  tests/test_mlb_pitcher_train_pipeline_variants.py \
  tests/test_mlb_training_profiles.py \
  tests/test_mlb_training_feature_controls.py \
  tests/test_analyze_mlb_clv.py \
  tests/test_analyze_mlb_batter_hits_clv.py \
  tests/test_run_mlb_quote_clean_audit_suite.py \
  tests/test_analyze_mlb_clv_ranking_diagnostics.py \
  tests/test_mlb_sweep_results.py \
  tests/test_mlb_sweep_config.py \
  tests/test_mlb_sweep_execution.py \
  tests/test_mlb_sweep_edge_metadata_contract.py \
  -q
```

### CLI help checks

```bash
./venv/Scripts/python.exe src/models/mlb/mlb_train_pipeline.py --help
./venv/Scripts/python.exe src/models/mlb/mlb_batter_train_pipeline.py --help
./venv/Scripts/python.exe src/backtesting/mlb/run_mlb_sweep.py --help
./venv/Scripts/python.exe scripts/run_mlb_quote_clean_audit_suite.py --help
./venv/Scripts/python.exe scripts/analyze_mlb_clv.py --help
```

### Runner dry-run checks

Run from PowerShell manually after code exists:

```powershell
.\scripts\run_mlb_stat_ablation.ps1 -Profile pitcher_strikeouts -Variant none -DryRun
.\scripts\run_pitcher_k_ablation.ps1 -Variant none -DryRun
.\scripts\run_mlb_stat_ablation.ps1 -Profile batter_hits -Families market -Mode include -Base no_prop_line -DryRun
```

Expected:

- commands print only;
- no DB calls;
- no training/backtest starts;
- profile defaults visible.

## Done criteria for the whole rebuild

- No new large stat-specific cloned wrapper.
- Generic runner dry-run works for batter and pitcher.
- Generic CLV analyzer handles batter and pitcher shaped fixtures.
- Existing batter workflow remains compatible.
- Pitcher K family controls are tested.
- Shared training helper tests pass.
- Existing production model loading remains backward compatible.
- Docs name exact commands and gates for baseline restoration.
