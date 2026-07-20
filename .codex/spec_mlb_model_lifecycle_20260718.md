# Implementation Spec: MLB model lifecycle runner

## Goal
Build a model/stat-agnostic, YAML-driven MLB lifecycle system that orchestrates training, independent-window quote-clean sweeps, CLV audit, ranker diagnostics, and report-only Confirm/Shelf/Exclude plus paper/live posture. It must support batter_hits, pitcher_strikeouts, and batter_rbis without hardcoding stat branches in the lifecycle engine.

## Existing foundations to reuse
- `src/models/mlb/training/profiles.py`: canonical stat/model profile registry.
- `src/models/mlb/training/feature_controls.py`: family/feature resolution.
- `src/models/mlb/training/artifacts.py` or existing manifest/artifact helpers if applicable.
- `src/backtesting/mlb/run_mlb_sweep.py`: existing sweep implementation; do not rewrite math.
- `scripts/run_mlb_quote_clean_audit_suite.py`: existing audit.
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`: existing ranker diagnostics.
- `scripts/diagnose_mlb_clv_failure_modes.py`: existing report-only diagnosis.
- `scripts/run_mlb_stat_ablation.ps1`: behavior reference only; new implementation is Python/YAML.

## Allowed files
Create/modify only these paths unless a necessary adjacent test/import file is justified:
- `src/models/mlb/training/profiles.py`
- `src/models/mlb/lifecycle/__init__.py`
- `src/models/mlb/lifecycle/config.py`
- `src/models/mlb/lifecycle/adapters.py`
- `src/models/mlb/lifecycle/runner.py`
- `src/models/mlb/lifecycle/decision.py`
- `scripts/run_mlb_model_lifecycle.py`
- `configs/mlb/batter_hits/platoon_contact_independent.yaml`
- `configs/mlb/pitcher_strikeouts/baseline_independent.yaml`
- `configs/mlb/batter_rbis/baseline_independent.yaml`
- `tests/test_mlb_lifecycle_config.py`
- `tests/test_mlb_lifecycle_adapters.py`
- `tests/test_mlb_lifecycle_runner.py`
- `tests/test_mlb_lifecycle_decision.py`
- `tests/test_mlb_training_profiles.py`
- `docs/development_docs/mlb_model_lifecycle_runner.md`
- `pyproject.toml`
- `requirements.txt`

Do not edit existing unrelated modified/untracked files. Do not commit.

## Architecture

### Profile-driven, not stat-branch-driven
The lifecycle engine must call `get_training_profile(profile_name)` and use an adapter selected by `train_entrypoint_kind`. No `if profile == 'batter_hits'` / `elif pitcher...` logic in runner/config/decision. Architecture-specific behavior belongs in adapter classes or a registry keyed by entrypoint kind.

Extend `MLBTrainingProfile` only as needed to expose declarative execution metadata. Add a real `batter_rbis` profile using the existing batter trainer with `--stat rbis`, NegBin artifacts (`batter_rbis_xgblss_booster.json`, `batter_rbis_negbin_meta.json`), default direction both, dense CLV defaults, and current batter feature families when valid. Adding a future stat that uses an existing adapter should require only a profile entry and tests.

### YAML configuration
Use PyYAML (`yaml.safe_load`) and Pydantic models. Declare PyYAML as a direct dependency in both `pyproject.toml` and `requirements.txt` using the project's existing dependency style.

Required top-level sections:
- `experiment`: name, profile, purpose (`discovery`, `independent_validation`, `finalist_certification`)
- `model`: base (`no_prop_line`, `with_prop_line`), optional existing `artifact_dir`, tune, feature_tolerance, variant, feature controls (`mode`, families, features)
- `training`: seasons, calibration_season, calibration_end
- `evaluation`: start, end, direction optional (profile default), edge_thresholds, flat_bet, optional tau/kelly values
- `quotes`: clean, line_source optional (profile default), decision_policy optional, relative_minutes, routing optional
- `audit`: minimum_bets optional (profile default), bootstrap_samples, mode (`clv_only`, `full`)
- `decision`: max_drawdown, require_positive_roi, require_positive_mean_clv_ci_low, require_positive_ranker_ci_low, require_edge_bucket_monotonicity, require_independent_window

Validation:
- Unknown profile/family fails loudly with valid names.
- Evaluation start must be after calibration_end.
- Promotion/finalist purposes require quote-clean.
- `artifact_dir` means attach/reuse and suppress training; otherwise output artifact root is generated.
- Resolve feature family names to their concrete feature lists and write counts/names into resolved config/manifest.
- Never call the platoon+contact artifact a four-feature model. It is 2 families and currently 20 unique forced features.

### Commands and adapters
Adapters return argv lists, never shell strings. Initial adapters:
- `batter`: invokes `src/models/mlb/mlb_batter_train_pipeline.py`, includes `--stat <train_short_stat>` and profile-compatible feature controls/base flags.
- `pitcher_quantile`: invokes `src/models/mlb/mlb_train_pipeline.py`, supports shared family/feature controls and optional `--ablation-variant`.

The generic runner constructs the common sweep/audit/ranker argv from the profile/config and invokes subprocesses without `shell=True`.

### Lifecycle and resumability
Stages in stable order:
1. validate
2. train_or_attach
3. artifact_identity
4. sweep
5. audit
6. ranker
7. decision

CLI:
- `--config PATH`
- `--dry-run`
- `--status`
- `--from-stage STAGE`
- `--force-stage STAGE` optional

Output root defaults to `backtest_results/lifecycle/<experiment-name>/` (allow config override only if truly needed). Write:
- `resolved_config.yaml`
- `run_manifest.json`
- `stage_status.json`
- `artifact_identity.json`
- `commands.json`
- `promotion_decision.json`
- `promotion_decision.md`

Each stage records pending/running/completed/failed/skipped, timestamps, command argv, exit code, and key output paths. On rerun, completed stages are skipped only when expected outputs still exist. `--from-stage` resumes without rerunning earlier completed stages. Never silently reuse `_incomplete` artifacts.

Dry-run must perform validation, profile/family expansion, path resolution, and command construction without training, DB calls, or subprocess execution. It must write/print enough resolved information to verify all three profiles.

### Existing sweep attachment
Support attaching both an existing artifact and existing sweep directory in YAML so the current batter_hits independent-window result can resume directly at audit/ranker without renaming directories or matching an encoded run-label convention.

### Decision system (report only)
Decision output must never deploy, copy production artifacts, enable Kelly, or toggle live trading.

Common classification:
- `Confirm`: required evidence gates pass for the configured purpose.
- `Shelf`: evidence is valid but underpowered/inconclusive or a nonfatal gate is unresolved.
- `Exclude`: invalid timing/artifact evidence or clear model/edge failure.

Separate posture:
- hypothesis_only
- independent_window_candidate
- flat_paper_candidate
- live_blocked
- live_ready

Be conservative: live_ready requires finalist certification, full audit, independent window, positive mean CLV CI low, positive ranker CI low, acceptable drawdown, edge-bucket monotonicity when required, and timing stability evidence. Missing evidence => live_blocked with reasons. Do not infer metrics. Read existing `sweep_summary.csv`, audit `suite_manifest.csv`, and ranker `ranking_score_summary.csv` when present. Allow profile-specific optional decision hooks/requirements without stat conditionals in the engine; the initial MVP may express these declaratively on the profile.

### Artifact identity
Verify completed artifact directory and expected required files. Read `run_config.json`, `training_metadata.json`, `feature_manifest.json`, and/or `model_manifest.json` when present. Report requested families, expanded features/counts, observed final model features/counts, profile, model type, dates, and pass/fail reasons. Attached artifacts must match requested profile/stat and calibration cutoff when metadata exposes them.

## Sample configs
1. Batter hits sample attaches the exact existing platoon+contact artifact and existing sweep:
- artifact: `src/models/mlb/artifacts/ablations/batter_hits_no_prop_line_include_platoon_contact_20260526_232005/mlb_run_batter_hits_20260526_232007_no_prop_line`
- sweep: `backtest_results/ablations/batter_hits_platoon_contact_independent_dense_20260518_20260621`
- from audit use case
- profile batter_hits, families platoon/contact_quality, no prop line, cal 2026-04-12, eval 2026-05-18..2026-06-21, preferred book, T-60, flat 100.
2. Pitcher K dry-run example using profile defaults and under direction.
3. RBI dry-run example proving generic batter adapter reuse.

## TDD requirements
Use strict TDD vertical slices. For every behavior:
1. Write failing focused test.
2. Run it and confirm expected failure.
3. Implement minimal code.
4. Rerun to green.

At minimum test:
- parsing/validating all three sample profiles
- invalid profile and invalid feature family
- temporal cutoff validation
- finalist/independent quote-clean validation
- adapter argv for batter_hits, batter_rbis, pitcher_strikeouts
- family expansion counts (platoon=3, contact_quality=17, combined unique=20)
- dry-run has no subprocess call
- resume skips valid completed stage and reruns when expected output missing
- attach existing artifact/sweep starts audit without encoded naming convention
- no `_incomplete` artifact selection
- decision Confirm/Shelf/Exclude and live-blocked safety
- runner contains no stat-name conditionals (behavioral/profile extensibility test preferred over brittle source text where possible)

## Non-goals
- Do not alter model math, probability calculation, calibration behavior, sweep simulation, line-selection logic, CLV math, or ranker math.
- Do not run training/backtests/audits against real DB during implementation.
- Do not deploy/promote artifacts or modify production config.
- Do not remove existing PowerShell wrappers in this slice; mark them legacy/compatibility in docs only.
- Do not generalize beyond MLB in this slice, but keep adapter/profile interfaces suitable for future extension.

## GameFlow invariants
- Empirical CDF remains `(samples > line).mean()`; never Gaussian CDF.
- Never globally recalibrate Q10/low-tail edge.
- Main-context and worker do not call Supabase MCP or mutate DB.
- Quote-clean timing and game-specific line selection remain unchanged.
- No Kelly/live permission from ROI alone.
- Preserve all unrelated local modifications/untracked files.

## Validation
Run focused tests only during implementation:
- `.\venv\Scripts\python.exe -m pytest tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py tests/test_mlb_lifecycle_runner.py tests/test_mlb_lifecycle_decision.py tests/test_mlb_training_profiles.py -q`
- `.\venv\Scripts\python.exe -m ruff check src/models/mlb/lifecycle scripts/run_mlb_model_lifecycle.py tests/test_mlb_lifecycle_*.py tests/test_mlb_training_profiles.py`
- Dry-run each sample YAML; no DB access.
- Run `git diff --check` on allowed files.

## Review criteria
- Lifecycle code is profile/adapter driven and adding an RBI profile did not require stat branching in runner.
- Existing tested CLIs are orchestrated, not reimplemented.
- Commands are argv arrays and subprocess uses `shell=False`/default.
- Resume/status behavior is deterministic and manifests are human-readable.
- Decision remains report-only and live defaults blocked on missing evidence.
- All focused tests and dry-runs pass.
- No unrelated files changed and no commit created.
