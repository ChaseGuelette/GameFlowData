# 02 — Operational Runner Rebuild

## Goal

Replace one-off batter_hits operational wrappers with profile-driven runners that can support pitcher_strikeouts without copy-paste tech debt.

## Current files

- `scripts/run_batter_hits_family_ablation.ps1`
- `scripts/resume_batter_hits_ablation_audit.ps1`
- `src/backtesting/mlb/run_mlb_sweep.py`
- `src/backtesting/mlb/sweep_config.py`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- `scripts/analyze_mlb_clv_book_sensitivity.py`

## New files

- Create: `scripts/run_mlb_stat_ablation.ps1`
- Create: `scripts/resume_mlb_stat_ablation_audit.ps1`
- Create: `scripts/run_pitcher_k_ablation.ps1`
- Later modify: `scripts/run_batter_hits_family_ablation.ps1` to delegate to generic runner.

## Desired runner behavior

`run_mlb_stat_ablation.ps1` should support:

- stat/profile selection:
  - `-Profile batter_hits`
  - `-Profile pitcher_strikeouts`
- feature experiment controls:
  - `-Families`
  - `-Features`
  - `-Mode include|exclude`
  - `-Variant` for legacy pitcher variants initially
- lifecycle controls:
  - `-SkipTrain`
  - `-SkipSweep`
  - `-SkipAudit`
  - `-DryRun`
- dates/windows:
  - `-Start`
  - `-End`
  - `-CalEndDate`
  - `-TrainSeasons`
- sweep knobs:
  - `-Direction`
  - `-Edge`
  - `-Kelly`
  - `-FlatBet`
  - `-BookRoutingPolicy`
  - `-LineSource`
  - `-QuoteDecisionPolicy`
  - `-QuoteCutoffTimeEt`
  - `-QuoteRelativeMinutes`
- audit/ranker controls:
  - `-MinDecisionGradeBets`
  - `-SkipDropoutAudit`
  - `-RunBookSensitivity`

## Profile defaults

### batter_hits

- train command: `.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --stat hits`
- sweep stat: `batter_hits`
- artifact pattern: `mlb_run_batter_hits_*`
- default direction: `both`
- baseline modes: `with_prop_line`, `no_prop_line`
- feature families from `BATTER_FORCE_FEATURE_FAMILIES`

### pitcher_strikeouts

- train command initially: `.\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py`
- sweep stat: `pitcher_strikeouts`
- artifact pattern initially: `mlb_run_*`, disambiguated by run metadata/profile manifest
- default direction: `under` for baseline restoration / first validation lane
- legacy variants initially: `none`, `static_no_l30`, `hook_only`, `ip_only`, `ip_hook`, `hook_avg_ip_l30`, `hook_short_hook_l30`, `hook_deep_start_l30`

## Implementation tasks

### Task 1: Characterize current batter wrapper dry-run behavior

Files:
- Test/create: `tests/test_mlb_stat_ablation_runner_static.py` or equivalent script-level test helper if PowerShell tests are not existing.
- Read-only reference: `scripts/run_batter_hits_family_ablation.ps1`

Steps:
1. Add a static test that asserts current batter wrapper contains expected command fragments.
2. Run: `./venv/Scripts/python.exe -m pytest tests/test_mlb_stat_ablation_runner_static.py -q`
3. Expected: pass.

### Task 2: Create generic runner with dry-run only

Files:
- Create: `scripts/run_mlb_stat_ablation.ps1`

Requirements:
- `-DryRun` prints train/sweep/audit/ranker/book commands without executing.
- Supports `batter_hits` and `pitcher_strikeouts` profiles.
- Does not train or touch DB in tests.

Validation:
- `powershell -ExecutionPolicy Bypass -File scripts/run_mlb_stat_ablation.ps1 -Profile pitcher_strikeouts -Mode include -Families workload_leash -DryRun`
- Expected: prints `--stats pitcher_strikeouts`, `--direction under`, dense CLV/source flags, and pitcher train entrypoint.

### Task 3: Add thin pitcher wrapper

Files:
- Create: `scripts/run_pitcher_k_ablation.ps1`

Requirements:
- Passes `-Profile pitcher_strikeouts` into generic runner.
- Keeps pitcher-friendly parameter names.
- No duplicated command assembly.

### Task 4: Add generic resume/audit runner

Files:
- Create: `scripts/resume_mlb_stat_ablation_audit.ps1`

Requirements:
- Accepts `-Profile`, `-RunLabel`, `-SweepDir`, `-ModelDir`.
- Runs audit/ranker/book-sensitivity only.
- Uses same profile metadata as main runner.

### Task 5: Convert batter wrapper to compatibility shim

Files:
- Modify: `scripts/run_batter_hits_family_ablation.ps1`

Requirements:
- Either delegate to `run_mlb_stat_ablation.ps1` or leave unchanged until the new runner is proven.
- If modified, keep same public params.

## Done criteria

- Generic runner dry-run works for both `batter_hits` and `pitcher_strikeouts`.
- No copied 300+ line pitcher wrapper exists.
- Existing batter wrapper still works or remains untouched.
- Tests/static checks cover hard-coded stat command fragments.
