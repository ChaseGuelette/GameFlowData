# 04 — Training Suite Refactor

## Goal

Remove duplicated training-suite lifecycle code while preserving pitcher and batter model-specific behavior.

## Current files

- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `src/models/mlb/mlb_quantile_trainer.py`
- `src/models/mlb/mlb_model_suite.py`
- `src/models/mlb/features/contracts.py`
- `src/models/mlb/features/pitcher_training_loader.py`
- `src/models/mlb/features/batter_training_loader.py`
- `src/models/mlb/features/pitcher_inference_loader.py`
- `src/models/mlb/features/batter_inference_loader.py`

## New files

- Create: `src/models/mlb/training/__init__.py`
- Create: `src/models/mlb/training/profiles.py`
- Create: `src/models/mlb/training/base_orchestrator.py`
- Create: `src/models/mlb/training/artifacts.py`
- Create: `src/models/mlb/training/feature_controls.py`
- Create: `src/models/mlb/training/strategies.py`
- Later optional:
  - `src/models/mlb/training/pitcher_k_strategy.py`
  - `src/models/mlb/training/batter_hits_strategy.py`

## Profiles

### `MLBTrainingProfile`

Fields:

- `stat_key: str`
- `display_name: str`
- `train_entrypoint_kind: Literal["pitcher_quantile", "batter"]`
- `model_type: str`
- `target_columns: tuple[str, ...]`
- `prop_line_feature: str | None`
- `default_direction: Literal["over", "under", "both"]`
- `artifact_prefix: str`
- `model_artifact_names: tuple[str, ...]`
- `feature_families: Mapping[str, tuple[str, ...]]`
- `locked_out_features: tuple[str, ...]`
- `default_quote_policy: str`
- `default_line_source: str`
- `default_book_routing_policy: str`
- `min_decision_grade_bets: int`

Initial profiles:

- `batter_hits`
- `pitcher_strikeouts`

## Shared artifact helpers

`training/artifacts.py` should own:

- run directory naming;
- `_incomplete` lifecycle;
- `run_config.json` writing;
- `feature_manifest.json` writing;
- `feature_experiment_metadata.json` writing;
- `calibration_report_combined.json` writing;
- `training_metadata.json` writing;
- optional `model_manifest.json` writing.

New `model_manifest.json` fields:

- `schema_version`
- `stat_key`
- `model_type`
- `profile_name`
- `created_at`
- `git_hash`
- `artifact_files`
- `feature_manifest_file`
- `calibration_report_file`
- `training_metadata_file`
- `compatibility_loader`

## Shared feature controls

`training/feature_controls.py` should own:

- normalize comma/repeated args;
- validate family names;
- expand family names to feature lists;
- detect include/exclude conflicts;
- fail loud on missing forced features;
- fail loud on non-numeric forced features;
- merge forced and selector-selected features preserving order.

Use existing batter behavior in `tests/test_mlb_batter_train_pipeline_variants.py` as characterization source.

## Base orchestrator

`BaseMLBTrainingOrchestrator` should own lifecycle, not model math.

Pseudo-interface:

```python
class BaseMLBTrainingOrchestrator:
    def run(self, train_seasons, cal_season, cal_end_date=None): ...
    def load_training_data(self, request): ...
    def load_calibration_data(self, request): ...
    def enrich_features(self, df): ...
    def resolve_features(self, train_df): ...
    def train_model(self, train_df, selected_features): ...
    def evaluate_calibration(self, model, cal_df): ...
    def sanity_check(self, model, cal_df): ...
    def save_outputs(...): ...
```

Stat strategies implement the hooks.

## Migration strategy

### Slice 1 — pure helpers only

Add `profiles.py`, `feature_controls.py`, `artifacts.py` with tests. Do not modify existing pipelines yet.

### Slice 2 — adopt feature controls in batter pipeline

Replace duplicated batter feature-control methods with shared helpers while preserving tests.

### Slice 3 — adopt feature controls in pitcher pipeline

Add pitcher family registry and CLI args, but keep `MLBTrainingOrchestrator` mostly intact.

### Slice 4 — artifact helper adoption

Move run_config/metadata/manifest writing into shared helper for both pipelines.

### Slice 5 — base orchestrator extraction

Only after tests pass, extract shared lifecycle behind compatibility wrappers.

## Files to modify by slice

### Slice 1

- Create `src/models/mlb/training/profiles.py`
- Create `src/models/mlb/training/feature_controls.py`
- Create `src/models/mlb/training/artifacts.py`
- Create `tests/test_mlb_training_profiles.py`
- Create `tests/test_mlb_training_feature_controls.py`

### Slice 2

- Modify `src/models/mlb/mlb_batter_train_pipeline.py`
- Modify `tests/test_mlb_batter_train_pipeline_variants.py`

### Slice 3

- Modify `src/models/mlb/features/contracts.py`
- Modify `src/models/mlb/mlb_train_pipeline.py`
- Add `tests/test_mlb_pitcher_train_pipeline_variants.py`

### Slice 4

- Modify both training pipelines
- Add/modify metadata tests

### Slice 5

- Create `src/models/mlb/training/base_orchestrator.py`
- Modify both training pipelines to become thin wrappers/subclasses
- Add high-level characterization tests

## Done criteria

- Existing training CLI behavior preserved.
- New pitcher feature-family controls exist and are tested.
- Artifact metadata fields are consistent across batter and pitcher.
- Existing production artifact loading still works through `MLBModelSuite`.
- New artifacts write explicit `model_manifest.json`.
- No model math changed unless separately approved.
