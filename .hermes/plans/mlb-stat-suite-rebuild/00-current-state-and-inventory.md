# 00 — Current State and File Inventory

## Goal

Record the literal files, entrypoints, responsibilities, and duplication that motivate the rebuild.

## Training entrypoints

### Pitcher K

File: `src/models/mlb/mlb_train_pipeline.py`

Primary class: `MLBTrainingOrchestrator`.

Current responsibilities:

- builds DB engine via `get_engine(local=...)`;
- constructs `MLBFeatureStore` and `PitcherTrainingLoader`;
- loads train/calibration data using `TrainingFeatureRequest`;
- calls feature-store enrichment methods:
  - `enrich_with_matchup_features(...)`
  - `_add_interaction_features(...)`
- hard-codes pitcher ablation variants:
  - `none`
  - `static_no_l30`
  - `hook_only`
  - `ip_only`
  - `ip_hook`
  - `hook_avg_ip_l30`
  - `hook_short_hook_l30`
  - `hook_deep_start_l30`
- hard-codes feature groups in the training file:
  - `L30_HOOK_FEATURES`
  - `PREDICTED_IP_FEATURES`
  - `SINGLE_HOOK_ABLATION_FEATURES`
- runs per-quantile feature selection against `PITCHER_K_TRAINING_FEATURES`;
- trains optional IP feature-source model;
- still contains optional copula branch;
- tunes hyperparameters;
- trains `MLBPitcherKPipeline`;
- computes holdout calibration offsets;
- writes calibration report, feature manifest, metadata, and `_incomplete` run dir finalization;
- owns CLI parsing.

Debt:

- one large class owns orchestration, policy, artifacts, calibration, feature controls, and CLI;
- no generic force include/exclude feature-family controls;
- ablations are hard-coded rather than profile/config driven;
- run config metadata is less explicit than batter_hits;
- filename `mlb_train_pipeline.py` is ambiguous but actually pitcher K specific.

### Batter models / batter_hits

File: `src/models/mlb/mlb_batter_train_pipeline.py`

Primary class: `MLBBatterTrainingOrchestrator`.

Current responsibilities:

- same lifecycle shape as pitcher K;
- supports multiple batter stats through `--stat`;
- supports newer controls:
  - `--exclude-prop-line`
  - `--force-include-families`
  - `--force-exclude-families`
  - `--force-include-features`
  - `--force-exclude-features`
- owns binary, binomial, and negbin pipelines;
- writes richer run config and feature experiment metadata;
- owns CLI parsing.

Debt:

- repeats run lifecycle and artifact helpers already present in pitcher K;
- mixes several model-family strategies in one class;
- newer feature controls live only here, not in a shared training contract.

## Feature stores and loaders

### Active facades

- `src/models/mlb/mlb_feature_store.py`
- `src/models/mlb/mlb_batter_feature_store.py`

These are intentionally thin compatibility facades.

### Legacy implementations

- `src/models/mlb/features/legacy_pitcher_feature_store.py`
- `src/models/mlb/features/legacy_batter_feature_store.py`

These still contain the heavy feature-store logic.

### Thin loaders

- `src/models/mlb/features/pitcher_training_loader.py`
- `src/models/mlb/features/batter_training_loader.py`
- `src/models/mlb/features/pitcher_inference_loader.py`
- `src/models/mlb/features/batter_inference_loader.py`

Debt:

- loaders are good seams but too thin today;
- training orchestrators still call feature-store enrichment methods directly;
- public interface for feature enrichment is not cleanly formalized.

## Feature contracts

File: `src/models/mlb/features/contracts.py`

Good current state:

- pitcher feature lists centralized:
  - `PITCHER_K_FEATURES`
  - `PITCHER_K_TRAINING_FEATURES`
  - `PITCHER_K_PHASE3A_REJECTED_FEATURES`
  - `PITCHER_K_PHASE3B_ADDED_FEATURES`
  - `PITCHER_K_EXCLUDED_TRAINING_FEATURES`
- batter stat maps/family helpers centralized:
  - `BATTER_FEATURE_MAP`
  - `BATTER_FORCE_FEATURE_FAMILIES`
  - `features_for_batter_families(...)`
  - `normalize_feature_family_names(...)`
  - `normalize_feature_names(...)`

Gap:

- no pitcher family registry equivalent;
- pitcher ablation feature groups live in `mlb_train_pipeline.py`, not contracts/profile.

## Model suite / inference loading

File: `src/models/mlb/mlb_model_suite.py`

Responsibilities:

- loads pitcher K copula if `ip_model`, `krate_model`, and `pitcher_k_copula_params.json` exist;
- otherwise loads single quantile pitcher K from `pitcher_k_model.joblib`;
- optionally wraps IP-feature-source model if `ip_feature_model` and metadata exist;
- loads batter models by hard-coded filename patterns;
- provides common `suite.predict(...)` / `suite.predict_batch(...)` surface.

Debt:

- model loading is mostly filename inference, not manifest/profile driven;
- backwards compatibility is necessary, but new artifacts should write explicit manifests.

## Operational scripts

### Batter-specific, not reusable as-is

- `scripts/run_batter_hits_family_ablation.ps1`
  - hard-codes `src\models\mlb\mlb_batter_train_pipeline.py --stat hits`;
  - hard-codes `--stats batter_hits`;
  - hard-codes batter feature-family validation;
  - hard-codes run labels with `batter_hits_...`.

- `scripts/resume_batter_hits_ablation_audit.ps1`
  - hard-codes `mlb_run_batter_hits_*` artifact discovery;
  - assumes batter_hits run labels and directory layout.

### Generic or nearly generic

- `src/backtesting/mlb/run_mlb_sweep.py`
  - accepts `--stats`; already defaults include `pitcher_strikeouts`.
- `src/backtesting/mlb/sweep_config.py`
  - typed sweep config, quote-clean config, grid config.
- `scripts/run_mlb_quote_clean_audit_suite.py`
  - accepts `--stats`, but internally calls `analyze_mlb_batter_hits_clv.py`.
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`
  - stat-agnostic post-hoc CSV diagnostics.
- `scripts/analyze_mlb_clv_book_sensitivity.py`
  - stat-agnostic post-hoc book slice diagnostics.

### Badly named but mostly generic

- `scripts/analyze_mlb_batter_hits_clv.py`
  - core CLV matching/summarization is mostly stat-agnostic;
  - filename, docstring, reports, and audit-suite coupling are batter_hits-specific.

## Existing relevant tests

- `tests/test_mlb_training_pipeline_feature_loaders.py`
- `tests/test_mlb_batter_train_pipeline_variants.py`
- `tests/test_mlb_pitcher_feature_sources.py`
- `tests/test_mlb_sweep_results.py`
- `tests/test_run_mlb_quote_clean_audit_suite.py`
- `tests/test_analyze_mlb_batter_hits_clv.py`
- `tests/test_analyze_mlb_clv_ranking_diagnostics.py`
- `tests/test_mlb_sweep_config.py`
- `tests/test_mlb_sweep_execution.py`
- `tests/test_mlb_sweep_edge_metadata_contract.py`

## Summary of what must change

1. Add profile-driven operational runner so batter_hits and pitcher K do not require cloned PowerShell scripts.
2. Genericize CLV analyzer boundary so the audit suite does not call a batter-named analyzer for all stats.
3. Add stat profiles/contracts for default direction, artifact naming, training command, model names, feature families, and gates.
4. Add pitcher family controls in contracts/profile, not hard-coded training-file variants.
5. Extract shared training lifecycle from the two large orchestrators after characterization tests exist.
6. Preserve existing CLI wrappers as compatibility shims during migration.
