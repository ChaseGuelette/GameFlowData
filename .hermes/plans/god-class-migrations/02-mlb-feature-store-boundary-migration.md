# MLB Feature-Store Boundary Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change feature semantics, retrain models, run long backtests, or execute DB-heavy audits.

**Goal:** Rebuild the MLB pitcher and batter feature stores into explicit, testable boundaries for data access, temporal/as-of contracts, feature transforms, and caller-facing compatibility.

**Architecture:** Preserve behavior first. Keep the public `MLBFeatureStore` and `MLBBatterFeatureStore` APIs as compatibility facades while extracting pure transforms, SQL query builders, point-in-time prop-line selection, domain-specific loaders, and schema/feature contracts. Only after train/backtest/inference callsites are migrated and guarded should the god classes be thinned or retired.

**Tech Stack:** Python, pandas, SQLAlchemy, pytest, existing MLB train/daily/backtest callers, GameFlow SQL-runner pattern for any DB truth.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `models/mlb-model`
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`
- `lessons/quote-clean-clv-before-feature-work`
- `lessons/feature-selector-is-not-an-ablation`
- `lessons/correlated-feature-family-validation`
- `lessons/cheap-baseline-before-architecture`

Applied lessons:

1. Temporal integrity is end-to-end. Raw timestamp fields do not prove feature generation is leak-free.
2. Market-derived features such as `prop_line_*` require point-in-time/as-of contracts in training, backtesting, and inference.
3. Quote-clean/CLV evidence must be trusted before using feature-store changes to justify model promotion or feature expansion.
4. Feature selectors are not causal ablations; feature-family validation needs force-include/force-exclude and downstream betting evidence.
5. Correlated feature families should be validated as families before pruning individual proxies.
6. Prefer cheap controlled baselines before building richer feature architectures.
7. Feature generation must use only data where `game_date < target_game_date` unless the source is explicitly a pregame snapshot with as-of semantics.

---

## Executive diagnosis

The MLB feature-store layer is two god classes that own too many responsibility boundaries:

- `src/models/mlb/mlb_feature_store.py`
  - 1,939 total lines
  - 1,660 non-comment LOC
  - `MLBFeatureStore`: 1,722 class lines, 22 methods
- `src/models/mlb/mlb_batter_feature_store.py`
  - 1,533 total lines
  - 1,285 non-comment LOC
  - `MLBBatterFeatureStore`: 1,370 class lines, 21 methods

They currently own:

- feature-list constants and stat mappings
- DB/schema existence checks
- training dataset loading
- single-player inference feature loading
- date batch backtest/inference feature loading
- raw SQL for many feature families
- point-in-time prop-line SQL
- rolling average joins
- Statcast/FanGraphs joins
- park/weather/game-total/bullpen/umpire queries
- matchup and platoon feature enrichment
- Python-derived features
- default/imputation logic
- public caller compatibility across training, daily inference, backtests, scripts, and audits

This is risky because MLB feature-store behavior is promotion-critical and leakage-sensitive. Small changes can affect model artifacts, quote-clean replay, daily inference, and paper/live trading without a clear owner or isolated test.

---

## Current ownership problems

### 1. Public API mixes training, batch inference, and single-player inference

Pitcher public methods:

- `get_training_dataset(...)`, lines 243-265
- `_load_single_season_training(...)`, lines 267-611
- `get_player_game_features(...)`, lines 826-959
- `get_features_for_date(...)`, lines 965-1322

Batter public methods:

- `get_training_dataset(...)`, lines 188-222
- `_load_single_season_training(...)`, lines 323-576
- `get_player_game_features(...)`, lines 619-756
- `get_features_for_date(...)`, lines 762-1024

Why this is wrong:

- Training, backtest batch, and single-game inference have different temporal contracts.
- Callers cannot tell which path is safe for promotion-grade replay.
- Tests must patch huge classes even when they only need one query path.

Target owner:

- Keep compatibility facades initially:
  - `MLBFeatureStore`
  - `MLBBatterFeatureStore`
- Add explicit service modules:
  - `src/models/mlb/features/pitcher_training_loader.py`
  - `src/models/mlb/features/pitcher_inference_loader.py`
  - `src/models/mlb/features/batter_training_loader.py`
  - `src/models/mlb/features/batter_inference_loader.py`

Expansion checkpoint:

- If callsites require subtly different semantics, add an explicit `FeatureMode`/`FeatureRequest` rather than adding more optional booleans to the facades.

---

### 2. Temporal/as-of policy is embedded in raw SQL strings

Current evidence:

- Batter training prop-line SQL includes:
  - `market_last_update <= :as_of_time`
  - post-commence guards
  - `COALESCE(snapshot_time, inserted_at) < commence_time`
- Batter `_get_prop_line(...)`, lines 1286-1336, duplicates similar logic.
- Pitcher `_get_prop_line(...)`, lines 1725-1764, duplicates similar logic.
- `tests/test_mlb_feature_store_as_of.py` currently verifies some as-of and post-commence behavior.

Why this is wrong:

- Temporal integrity is a policy contract, not a random SQL clause.
- The same rule must apply consistently across training, daily inference, batch backtests, and quote-clean sweeps.
- Duplicated SQL makes it easy for one path to regress.

Target owner:

- `src/models/mlb/features/temporal_contracts.py`
  - `FeatureAsOfPolicy`
  - validation helpers for `as_of_time`, target game date, and source type
- `src/models/mlb/features/prop_line_feature_source.py`
  - single owner for prop-line feature SQL/query construction
  - pitcher and batter market keys parameterized

Required tests:

- prop-line query applies `market_last_update <= :as_of_time`
- prop-line query applies `COALESCE(snapshot_time, inserted_at) <= :as_of_time` when the intended policy requires snapshot cutoff
- post-commence rows excluded
- no prop-line feature path can silently use latest rows in promotion-grade mode
- current legacy `as_of_time=None` behavior remains explicit and labeled

---

### 3. Feature family SQL and transformation logic are coupled

Pitcher feature families currently live as methods on one class:

- `_get_pitcher_rolling_stats(...)`
- `_get_pitcher_ip_context_features(...)`
- `_get_team_starter_leash_features(...)`
- `_get_statcast_stats(...)`
- `_get_fangraphs_stats(...)`
- `_get_park_factor(...)`
- `_get_game_weather(...)`
- `_get_team_bullpen_stats(...)`
- `_get_game_total(...)`
- `_get_prop_line(...)`
- `_get_inning_fatigue_stats(...)`
- `_get_umpire_features(...)`
- `_compute_umpire_features_bulk(...)`

Batter feature families currently live as methods on one class:

- `_get_batter_rolling_stats(...)`
- `_get_batter_statcast_stats(...)`
- `_get_batter_fangraphs_stats(...)`
- `_get_park_factors(...)`
- `_get_batter_handedness(...)`
- `_get_game_weather(...)`
- `_get_game_total(...)`
- `_get_prop_line(...)`
- `_get_platoon_features(...)`
- `_get_opposing_bullpen_stats(...)`
- `_get_umpire_features(...)`
- `_compute_umpire_features_bulk(...)`

Why this is wrong:

- Feature families cannot be tested independently.
- It is hard to run force-include/force-exclude experiments at the family level.
- SQL, default values, and derived calculations are not separately owned.

Target owners:

- `src/models/mlb/features/pitcher_sources.py`
- `src/models/mlb/features/batter_sources.py`
- `src/models/mlb/features/shared_sources.py`
- `src/models/mlb/features/feature_families.py`

Each source should expose a small, injectable API and a documented default/imputation policy.

---

### 4. Feature contracts and feature lists are embedded beside query code

Current batter constants:

- `BATTER_BASE_FEATURES`
- `BATTER_HITS_FEATURES`
- `BATTER_RBIS_FEATURES`
- `BATTER_HRR_FEATURES`
- `BATTER_FEATURE_MAP`
- `BATTER_STAT_TARGET`
- `BATTER_STAT_MARKET_KEY`

Current pitcher feature list lives in `mlb_feature_store.py` alongside SQL.

Why this is wrong:

- Train/backtest/inference parity depends on stable feature contracts.
- Feature-list changes should be reviewable without reading 1,500-line SQL files.
- Feature-family validation needs named feature groups.

Target owner:

- `src/models/mlb/features/contracts.py`

Responsibilities:

- `PitcherFeatureContract`
- `BatterFeatureContract`
- stat-to-target mapping
- stat-to-market-key mapping
- family membership metadata
- feature list uniqueness validation
- optional artifact-manifest comparison helpers

Tests:

- no duplicate features
- each stat feature list includes base features
- every `prop_line_*` feature has a known market-key mapping or an explicit no-line explanation, e.g. HRR/Kalshi-only
- feature-family names cover all features

---

### 5. Python-derived transforms are hidden as class methods

Examples:

- `MLBFeatureStore._add_derived_features(...)`, 106 lines
- `MLBBatterFeatureStore._add_derived_features(...)`, 25 lines
- `MLBBatterFeatureStore._add_batter_interaction_features(...)`, 10 lines
- matchup enrichment methods also apply defaults and derived values.

Why this is wrong:

- Pure transforms should not require DB-backed class construction.
- Transform behavior is important for artifact parity.
- Tests should exercise transforms directly.

Target owner:

- `src/models/mlb/features/transforms.py`

Responsibilities:

- pure pitcher derived features
- pure batter derived features
- pure interaction features
- default filling/imputation helpers

Tests:

- projected AB formula for batter hits/binomial path
- zero-denominator ratios
- missing-column defaults
- no mutation surprises unless explicitly documented

---

### 6. SQL query construction is not inventory-testable

Current tests in `tests/test_mlb_feature_store_as_of.py` inspect SQL strings by monkeypatching `pd.read_sql`, which is useful but tied to private methods.

Why this is incomplete:

- It verifies some timestamp predicates but not ownership boundaries.
- It does not prevent new raw SQL from being added back into facades.
- It does not enforce that promotion-grade callers use the safe query builders.

Target owner:

- query-builder modules returning `(sql_text, params)` objects or small dataclasses.

Proposed files:

- `src/models/mlb/features/query_builders.py`
- `src/models/mlb/features/prop_line_feature_source.py`

Tests:

- query builders can be tested without fake engines.
- facades delegate to query builders.
- inventory guard prevents raw prop-line SQL duplication in facades after migration.

---

### 7. Callsite boundaries are implicit

Current callsites found in the bounded scan:

- Training:
  - `src/models/mlb/mlb_train_pipeline.py`
  - `src/models/mlb/mlb_batter_train_pipeline.py`
- Daily inference:
  - `src/models/mlb/mlb_daily_runner.py`
  - `src/orchestration/mlb_inference_job.py`
- Backtests:
  - `src/backtesting/mlb/run_mlb_sweep.py`
  - `src/backtesting/mlb/mlb_backtest_harness.py`
  - `src/backtesting/mlb/run_mlb_backtest.py`
- Diagnostics/scripts:
  - `scripts/audit_mlb_quote_clean_dropout.py`
  - `scripts/diagnose_mlb_phase3a_agreement.py`
  - `scripts/diagnose_mlb_quote_clean_red_flags.py`
  - `scripts/mlb_workload_leash_diagnostics.py`
  - `scripts/validate_mlb_lineup_phase3a.py`

Why this is wrong:

- Callers choose methods directly without declaring mode or temporal contract.
- Scripts may accidentally preserve old semantics after migration.

Target owner:

- explicit request/config objects:
  - `TrainingFeatureRequest`
  - `BatchInferenceFeatureRequest`
  - `SingleGameFeatureRequest`
  - `BacktestFeatureRequest`

Compatibility rule:

- Keep old method signatures until all production callsites are migrated and tests pass.
- Add deprecation comments only after direct-service callsites exist.

---

## Target design by responsibility

### A. `features/contracts.py`

Owns:

- stat mappings
- feature lists
- feature-family metadata
- feature contract dataclasses
- feature-list validation helpers

Validation:

- `tests/test_mlb_feature_contracts.py`

---

### B. `features/temporal_contracts.py`

Owns:

- `FeatureAsOfPolicy`
- mode-specific temporal validation
- safe defaults/warning language for legacy paths
- utility helpers for target dates vs decision timestamps

Validation:

- `tests/test_mlb_feature_temporal_contracts.py`

---

### C. `features/prop_line_feature_source.py`

Owns:

- pitcher/batter prop-line query construction
- market-key parameterization
- bookmaker policy for feature props
- as-of and pre-commence guards
- single-player and batch query variants

Validation:

- `tests/test_mlb_prop_line_feature_source.py`

---

### D. `features/transforms.py`

Owns pure pandas transforms:

- pitcher derived ratios/interactions/defaults
- batter projected AB
- batter interaction features
- default/imputation helpers

Validation:

- `tests/test_mlb_feature_transforms.py`

---

### E. `features/pitcher_sources.py`

Owns source-specific pitcher feature loaders:

- rolling pitching averages
- IP context
- team starter leash
- Statcast
- FanGraphs
- inning fatigue
- pitcher-specific source defaults

Validation:

- fake engine/query-builder tests first
- optional SQL-runner audit later only with approval

---

### F. `features/batter_sources.py`

Owns source-specific batter feature loaders:

- rolling batting averages
- Statcast batting
- FanGraphs batting
- handedness/platoon
- opposing starter/bullpen context
- batter-specific source defaults

Validation:

- fake engine/query-builder tests first

---

### G. `features/shared_sources.py`

Owns common game-context loaders:

- park factors
- weather
- game totals
- umpire features
- table-exists/schema capability checks

Validation:

- tests for fallback defaults when optional tables are absent

---

### H. `features/requests.py`

Owns request dataclasses:

- `TrainingFeatureRequest`
- `DateFeatureRequest`
- `PlayerGameFeatureRequest`
- `FeatureMode`

Validation:

- tests for required fields and mode-specific constraints

---

### I. Compatibility facades

Keep files initially:

- `src/models/mlb/mlb_feature_store.py`
- `src/models/mlb/mlb_batter_feature_store.py`

Final role:

- import contracts
- construct service dependencies
- implement old public methods by delegating to new services
- contain little/no raw SQL
- no long derived-transform logic

Removal/thinning guard:

- Add inventory tests that fail if facades regain raw prop-line SQL or exceed agreed non-comment LOC thresholds after migration.

---

## Refactor phases

### Phase 0: Safety baseline and inventory

Objective: Make current responsibilities explicit before extraction.

Tasks:

1. Add `tests/test_mlb_feature_store_inventory.py`.
2. Assert current god-class files and key public methods exist.
3. Assert existing as-of regression tests still cover pitcher/batter prop-line paths.
4. Add TODO-style expectations for eventual shrink thresholds but do not fail them yet.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py -q`

Expansion checkpoint:

- If inventory finds another production caller using private methods, add that caller to this plan before extraction.

---

### Phase 1: Extract feature contracts and stat mappings

Objective: Move feature lists/stat mappings out of query god classes without behavior change.

Files:

- Create: `src/models/mlb/features/__init__.py`
- Create: `src/models/mlb/features/contracts.py`
- Create: `tests/test_mlb_feature_contracts.py`
- Modify: `src/models/mlb/mlb_batter_feature_store.py`
- Modify: `src/models/mlb/mlb_feature_store.py`

TDD tests:

- batter feature lists remain identical to current exported constants
- stat target mapping remains identical
- stat market-key mapping remains identical
- no duplicate features
- every feature belongs to at least one named family

Compatibility rule:

- Existing imports such as `from src.models.mlb.mlb_batter_feature_store import BATTER_FEATURE_MAP` must continue to work until callers are migrated.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_contracts.py tests/test_mlb_batter_feature_store.py -q`

---

### Phase 2: Extract pure transforms

Objective: Move Python-only feature transforms to direct-testable functions.

Files:

- Create: `src/models/mlb/features/transforms.py`
- Create: `tests/test_mlb_feature_transforms.py`
- Modify: `src/models/mlb/mlb_batter_feature_store.py`
- Modify: `src/models/mlb/mlb_feature_store.py`

TDD tests:

- batter `projected_ab` formula matches current behavior
- batter hit L5/L10 ratio handles zero denominator
- batter interaction features match current products/defaults
- pitcher derived features match current calculations on a small DataFrame
- transforms do not require an engine

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_transforms.py tests/test_mlb_batter_feature_store.py -q`

Expansion checkpoint:

- If transform tests expose mutation/side-effect reliance, document it and preserve behavior first; do not clean it up silently.

---

### Phase 3: Extract temporal contracts and prop-line feature source

Objective: Make market-derived feature timing policy one owner.

Files:

- Create: `src/models/mlb/features/temporal_contracts.py`
- Create: `src/models/mlb/features/prop_line_feature_source.py`
- Create: `tests/test_mlb_feature_temporal_contracts.py`
- Create: `tests/test_mlb_prop_line_feature_source.py`
- Modify: `src/models/mlb/mlb_feature_store.py`
- Modify: `src/models/mlb/mlb_batter_feature_store.py`
- Modify: `tests/test_mlb_feature_store_as_of.py` as needed to test delegation instead of private SQL duplication

TDD tests:

- prop-line query builder includes `market_last_update <= :as_of_time`
- query builder includes effective snapshot ordering
- query builder excludes post-commence rows
- pitcher and batter wrappers pass correct market key
- legacy `as_of_time=None` remains explicit and tested
- batch training/inference prop-line paths use the same source module

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_prop_line_feature_source.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_feature_store_as_of.py -q`

Expansion checkpoint:

- If quote-clean sweep and feature-store prop-line semantics diverge, stop and add an explicit compatibility section linking this plan to migration #01.

---

### Phase 4: Extract shared sources and optional-table fallbacks

Objective: Move common game-context loaders out of both facades.

Files:

- Create: `src/models/mlb/features/shared_sources.py`
- Create: `tests/test_mlb_shared_feature_sources.py`
- Modify: both feature-store facades

Targets:

- park factors
- weather
- game totals
- umpire features
- table existence/capability checks

TDD tests:

- optional umpire table missing returns documented default
- weather missing returns documented defaults
- park factor fallback behavior preserved
- game total missing returns zero/default exactly as current code

---

### Phase 5: Extract pitcher source services

Objective: Move pitcher-specific SQL loaders to focused modules.

Files:

- Create: `src/models/mlb/features/pitcher_sources.py`
- Create: `tests/test_mlb_pitcher_feature_sources.py`
- Modify: `src/models/mlb/mlb_feature_store.py`

Targets:

- rolling pitching averages
- IP context
- team starter leash
- Statcast pitching
- FanGraphs pitching
- inning fatigue
- bullpen/game context delegation to shared sources

TDD tests:

- query builders use strict previous-game predicates where required (`game_date < target_game_date`)
- rolling average latest snapshot behavior preserved when current code uses `<=`
- defaults preserved for missing rows
- source service can be constructed with fake engine

Safety:

- Do not add new feature families in this phase.

---

### Phase 6: Extract batter source services

Objective: Move batter-specific SQL loaders to focused modules.

Files:

- Create: `src/models/mlb/features/batter_sources.py`
- Create: `tests/test_mlb_batter_feature_sources.py`
- Modify: `src/models/mlb/mlb_batter_feature_store.py`

Targets:

- rolling batting averages
- Statcast batting
- FanGraphs batting
- handedness/platoon
- opposing starter and bullpen context
- batter lineup/projected-AB dependencies

TDD tests:

- target/stat mapping preserved
- market-key mapping preserved
- rolling/statcast/FanGraphs source date predicates preserved
- platoon defaults preserved
- opposing bullpen defaults preserved

---

### Phase 7: Introduce request objects and mode-specific loaders

Objective: Make caller intent explicit: training vs date batch vs single player/game.

Files:

- Create: `src/models/mlb/features/requests.py`
- Create: `src/models/mlb/features/pitcher_training_loader.py`
- Create: `src/models/mlb/features/pitcher_inference_loader.py`
- Create: `src/models/mlb/features/batter_training_loader.py`
- Create: `src/models/mlb/features/batter_inference_loader.py`
- Create corresponding tests
- Modify facades to delegate

TDD tests:

- `TrainingFeatureRequest` requires seasons/stat and allows optional as-of policy
- `DateFeatureRequest` requires date and mode
- `PlayerGameFeatureRequest` requires player/game/date/opponent context
- facades translate old method signatures into request objects
- old public methods return same columns for small fake data

Expansion checkpoint:

- If request objects need quote decision time from migration #01, add a cross-plan note rather than duplicating quote-decision policy.

---

### Phase 8: Migrate callsites one lane at a time

Objective: Move production callers from old facades to explicit loaders/services only after behavior is characterized.

Suggested order:

1. Diagnostic scripts first.
2. Backtest sweep after migration #01 line/prediction boundaries are stable.
3. Training pipelines.
4. Daily runner/inference job last because they affect production/paper outputs.

Callsites:

- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `src/models/mlb/mlb_daily_runner.py`
- `src/orchestration/mlb_inference_job.py`
- `src/backtesting/mlb/run_mlb_sweep.py`
- `src/backtesting/mlb/mlb_backtest_harness.py`
- scripts listed above

TDD pattern:

- Add a callsite-level test that monkeypatches the old facade path to raise if directly used.
- Verify RED.
- Rewire callsite to the focused loader/service.
- Verify GREEN.

Do not migrate all callsites in one PR.

---

### Phase 9: Thin facades and add anti-regrowth guards

Objective: Prevent the god classes from returning.

Files:

- Modify: `src/models/mlb/mlb_feature_store.py`
- Modify: `src/models/mlb/mlb_batter_feature_store.py`
- Modify: `tests/test_mlb_feature_store_inventory.py`

Inventory assertions after migration:

- no raw prop-line SQL in facades
- facades do not define large source-specific helper methods
- facades delegate to feature services
- non-comment LOC below agreed thresholds, recommended starting thresholds:
  - `mlb_feature_store.py` < 600 non-comment LOC
  - `mlb_batter_feature_store.py` < 600 non-comment LOC
- public compatibility methods remain or have explicit deprecation/removal tests

Removal rule:

- Do not delete the facade files until every production caller is migrated and a removal guard similar to Kalshi exists.
- Unlike Kalshi, complete deletion may not be desirable because many callers use these names as stable API boundaries. Thinning to compatibility adapters may be the endpoint.

---

## Files likely touched

Core files:

- `src/models/mlb/mlb_feature_store.py`
- `src/models/mlb/mlb_batter_feature_store.py`
- `src/models/mlb/features/__init__.py` (new)
- `src/models/mlb/features/contracts.py` (new)
- `src/models/mlb/features/temporal_contracts.py` (new)
- `src/models/mlb/features/prop_line_feature_source.py` (new)
- `src/models/mlb/features/transforms.py` (new)
- `src/models/mlb/features/shared_sources.py` (new)
- `src/models/mlb/features/pitcher_sources.py` (new)
- `src/models/mlb/features/batter_sources.py` (new)
- `src/models/mlb/features/requests.py` (new)
- `src/models/mlb/features/pitcher_training_loader.py` (new)
- `src/models/mlb/features/pitcher_inference_loader.py` (new)
- `src/models/mlb/features/batter_training_loader.py` (new)
- `src/models/mlb/features/batter_inference_loader.py` (new)

Potential callsites, migrate only in later phases:

- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `src/models/mlb/mlb_daily_runner.py`
- `src/orchestration/mlb_inference_job.py`
- `src/backtesting/mlb/run_mlb_sweep.py`
- `src/backtesting/mlb/mlb_backtest_harness.py`
- `src/backtesting/mlb/run_mlb_backtest.py`
- `scripts/audit_mlb_quote_clean_dropout.py`
- `scripts/diagnose_mlb_phase3a_agreement.py`
- `scripts/diagnose_mlb_quote_clean_red_flags.py`
- `scripts/mlb_workload_leash_diagnostics.py`
- `scripts/validate_mlb_lineup_phase3a.py`

Tests:

- `tests/test_mlb_feature_store_inventory.py` (new)
- `tests/test_mlb_feature_contracts.py` (new)
- `tests/test_mlb_feature_temporal_contracts.py` (new)
- `tests/test_mlb_prop_line_feature_source.py` (new)
- `tests/test_mlb_feature_transforms.py` (new)
- `tests/test_mlb_shared_feature_sources.py` (new)
- `tests/test_mlb_pitcher_feature_sources.py` (new)
- `tests/test_mlb_batter_feature_sources.py` (new)
- `tests/test_mlb_feature_requests.py` (new)
- existing: `tests/test_mlb_feature_store_as_of.py`
- existing: `tests/test_mlb_batter_feature_store.py`
- existing: `tests/test_mlb_batter_train_pipeline_variants.py`

---

## Validation commands

Baseline before any extraction:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline_variants.py -q`

After contracts/transforms:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_batter_feature_store.py -q`

After temporal/prop-line extraction:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_feature_store_as_of.py -q`

After source-loader extraction:

`venv/Scripts/python.exe -m pytest tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py -q`

After request/loader/facade migration:

`venv/Scripts/python.exe -m pytest tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py -q`

Lane-wide regression:

`venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or batter_train_pipeline or run_mlb_sweep or quote_clean)" -q`

Compile check:

`venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_feature_store.py src/models/mlb/mlb_batter_feature_store.py src/models/mlb/features/*.py`

Diff hygiene:

`git diff --check -- src/models/mlb src/backtesting/mlb tests .hermes/plans/god-class-migrations/02-mlb-feature-store-boundary-migration.md`

---

## Risk controls / non-goals

Non-goals:

- Do not add new features in this migration.
- Do not retrain models automatically.
- Do not promote or demote MLB models from this migration alone.
- Do not change feature semantics during extraction unless a RED test captures a known bug and Chase approves the behavior change.
- Do not alter quote-clean line-selection policy from migration #01 in this lane.
- Do not run remote DB-heavy audits in main context.
- Do not broad-scan artifacts or backtest result directories.

Hard rules:

- Preserve public API compatibility until callsites are migrated.
- Preserve current defaults in behavior-preserving phases.
- All DB truth/audits use GameFlow SQL-runner/delegated pattern.
- Treat old artifacts trained before temporal fixes as potentially contaminated until explicitly retrained and validated.

---

## Expansion checkpoints learned from Kalshi

The Kalshi migration expanded from facade extraction into status inventory, state machine, direct job wiring, facade removal, shared config, and parity guards. Expect this feature-store migration to expand similarly.

Trigger a new named sub-slice if you discover:

1. A feature family is duplicated between pitcher and batter stores.
2. A callsite relies on private methods or undocumented columns.
3. A SQL query path has different temporal semantics than the public method docs claim.
4. `as_of_time=None` is being used in a promotion-grade backtest path.
5. Feature-list constants are imported widely and need compatibility re-exports.
6. An optional data source/table fallback is not tested.
7. A transform mutates input DataFrames in a caller-dependent way.
8. A source extraction reveals a true leakage bug. Stop, add a failing regression, and separate behavior-changing fix from migration.
9. A new feature-family validation need appears. Route it to a separate modeling experiment; do not fold it into structural extraction.
10. Integration with migration #01 is needed. Update both plans' progress logs rather than duplicating policy.

Progress log entries must distinguish:

- module exists and has tests
- facade delegates to it
- production caller uses it directly
- old compatibility wrapper remains
- old helper removed
- behavior-changing hardening deferred

---

## First implementation PR recommendation

Start with contracts + transforms, not SQL.

First PR scope:

1. Create `src/models/mlb/features/contracts.py`.
2. Re-export existing feature constants from old modules so imports remain stable.
3. Create `src/models/mlb/features/transforms.py`.
4. Move pure derived feature logic only.
5. Add tests proving feature lists and pure transforms match current behavior.
6. Do not touch raw SQL or caller wiring in this PR.

Why first:

- Lowest risk.
- No DB required.
- Establishes new module namespace.
- Creates a safe place for feature-family metadata before touching temporal SQL.

Expected commit shape:

- `src/models/mlb/features/__init__.py`
- `src/models/mlb/features/contracts.py`
- `src/models/mlb/features/transforms.py`
- `tests/test_mlb_feature_contracts.py`
- `tests/test_mlb_feature_transforms.py`
- small import/re-export changes in existing feature store modules
- progress-log update in this plan

---

## Progress log

### 2026-05-23 slices 00-07A — contracts, transforms, prop-line source, source/request shells

Files changed:

- Created `src/models/mlb/features/__init__.py`.
- Created `src/models/mlb/features/contracts.py`.
- Created `src/models/mlb/features/transforms.py`.
- Created `src/models/mlb/features/temporal_contracts.py`.
- Created `src/models/mlb/features/prop_line_feature_source.py`.
- Created `src/models/mlb/features/shared_sources.py`.
- Created `src/models/mlb/features/pitcher_sources.py`.
- Created `src/models/mlb/features/batter_sources.py`.
- Created `src/models/mlb/features/requests.py`.
- Created compatibility loader shells:
  - `src/models/mlb/features/pitcher_training_loader.py`
  - `src/models/mlb/features/pitcher_inference_loader.py`
  - `src/models/mlb/features/batter_training_loader.py`
  - `src/models/mlb/features/batter_inference_loader.py`
- Modified `src/models/mlb/mlb_feature_store.py` to re-export contract constants, delegate pure pitcher transforms, and delegate single-player prop-line fetches.
- Modified `src/models/mlb/mlb_batter_feature_store.py` to re-export contract constants, delegate pure batter transforms, and delegate single-player prop-line fetches.
- Created focused tests:
  - `tests/test_mlb_feature_store_inventory.py`
  - `tests/test_mlb_feature_contracts.py`
  - `tests/test_mlb_feature_transforms.py`
  - `tests/test_mlb_feature_temporal_contracts.py`
  - `tests/test_mlb_prop_line_feature_source.py`
  - `tests/test_mlb_shared_feature_sources.py`
  - `tests/test_mlb_pitcher_feature_sources.py`
  - `tests/test_mlb_batter_feature_sources.py`
  - `tests/test_mlb_feature_requests.py`
- Updated Lane 01 compatibility imports in existing tests discovered by Lane 02 validation:
  - `tests/test_mlb_feature_store_as_of.py`
  - `tests/test_mlb_run_mlb_sweep_flat.py`
- Created companion findings log: `02-mlb-feature-store-boundary-research-log.html`.

RED result:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py tests/test_mlb_feature_requests.py -q` initially failed as expected before the new modules existed. A delegated worker hit its tool-call cap after discovery, so implementation resumed directly in the controller session.
- Lane-wide regression initially failed at collection because existing tests imported removed Lane 01 compatibility names from `run_mlb_sweep.py`; those tests now import the new owner modules directly.

GREEN result:

- Focused Lane 02 + existing facade tests passed:
  - `venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline_variants.py -q`
  - Result: 66 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or batter_train_pipeline or run_mlb_sweep or quote_clean)" -q`
  - Result: 104 passed, 871 deselected, 1 warning.
- Compile passed:
  - `venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_feature_store.py src/models/mlb/mlb_batter_feature_store.py src/models/mlb/features/*.py`

Behavior-preservation notes:

- Public constants and `get_features_for_stat(...)` remain importable from the legacy feature-store modules.
- Pure transform extraction preserves legacy in-place DataFrame mutation and default-filling behavior.
- `MLBFeatureStore._get_prop_line(...)` and `MLBBatterFeatureStore._get_prop_line(...)` now delegate to `features.prop_line_feature_source.fetch_single_prop_line(...)` while preserving legacy zero fallback, bookmaker filter, as-of predicate, post-commence guards, and ordering.
- Training/date-batch prop-line lateral SQL was deferred from slices 00-07A because it is promotion-critical; the 2026-05-24 follow-up moved this prop-line lateral SQL into the shared source owner with explicit tests.
- Shared/pitcher/batter source modules and request/loader modules are established and tested, but production callsites still mostly pass through compatibility facades.

Expansion checkpoint status:

- A Lane 01 compatibility drift was discovered in tests and recorded in the HTML findings log.
- Phase 8 production callsite migration is intentionally not done in this slice; the plan says not to migrate all callsites in one PR, and daily-runner/inference rewires are production-sensitive.
- Phase 9 full facade shrink thresholds are documented but not enforced; current inventory guards keep the destination visible until all source-specific SQL is migrated safely.

### 2026-05-24 slice 07B — training/date-batch prop-line source ownership and inventory tightening

Files changed:

- Modified `src/models/mlb/features/prop_line_feature_source.py`:
  - added `build_lateral_prop_line_join(...)` as the single owner for training/date-batch prop-line lateral SQL;
  - preserved the existing bookmaker filter, `market_last_update <= :as_of_time`, pre-commence `market_last_update < commence_time`, snapshot/inserted-at pre-commence guard, and ordering.
- Modified `src/models/mlb/mlb_feature_store.py`:
  - pitcher training and date-batch SQL now replace a `{prop_line_lateral_join}` placeholder with `build_lateral_prop_line_join(row_alias="pgs", market_key_sql="'pitcher_strikeouts'")`.
- Modified `src/models/mlb/mlb_batter_feature_store.py`:
  - batter training and date-batch SQL now replace a `{prop_line_lateral_join}` placeholder with `build_lateral_prop_line_join(row_alias="bgs", market_key_sql=":market_key")`.
- Tightened tests:
  - `tests/test_mlb_prop_line_feature_source.py` covers batch/lateral SQL construction for batter bind params and pitcher literal market keys.
  - `tests/test_mlb_feature_store_inventory.py` now asserts prop-line lateral SQL is owned by the feature source and raw `mlb_raw_player_props` SQL is absent from both facades.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_mlb_prop_line_feature_source.py tests/test_mlb_feature_store_inventory.py -q`
- Expected failure before implementation: import error for missing `build_lateral_prop_line_join`.

GREEN result:

- Compile + smoke passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_feature_store.py src/models/mlb/mlb_batter_feature_store.py src/models/mlb/features/prop_line_feature_source.py && ./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_as_of.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_feature_store_inventory.py tests/test_mlb_run_mlb_sweep_flat.py -q`
  - Result: 16 passed, 1 warning.
- Focused Lane 02 suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline_variants.py -q`
  - Result: 69 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or batter_train_pipeline or run_mlb_sweep or quote_clean)" -q`
  - Result: 107 passed, 877 deselected, 1 warning.
- Diff hygiene passed:
  - `git diff --check -- src/models/mlb/mlb_feature_store.py src/models/mlb/mlb_batter_feature_store.py src/models/mlb/features/prop_line_feature_source.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_feature_store_inventory.py`

Behavior-preservation notes:

- This slice intentionally did not change quote/as-of semantics; it centralized the duplicated lateral SQL string only.
- Inventory guards are now stricter for prop-line SQL ownership but still do not enforce the `<600` non-comment LOC facade thresholds. Those thresholds remain deferred until remaining SQL/source helpers are safe.

### 2026-06-07 Phase 8A — backtest sweep prediction-cache callsite uses explicit feature loaders

Files changed:

- Modified `src/backtesting/mlb/prediction_cache.py`:
  - pitcher prediction feature loading now builds a `PlayerGameFeatureRequest` and calls `PitcherInferenceLoader.load_player_game(...)`;
  - batter prediction feature loading now builds a `DateFeatureRequest(mode=FeatureMode.BACKTEST)` and calls `BatterInferenceLoader.load_date(...)`.
- Modified `src/models/mlb/features/pitcher_inference_loader.py` and `src/models/mlb/features/batter_inference_loader.py`:
  - player-game facade delegation now uses keyword arguments to preserve legacy call compatibility;
  - batter date loading now accepts and forwards `matchup_cache`, preserving the sweep's season-level matchup-cache behavior.
- Modified `tests/test_mlb_prediction_cache.py`:
  - added a RED/GREEN guard that fails if `prediction_cache` calls feature-store facades directly instead of the explicit loaders.
- Modified `tests/test_mlb_sweep_inventory.py`:
  - updated the ownership guard so `prediction_cache` is expected to own prediction-loop orchestration through explicit loader/request seams rather than direct facade method calls.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_mlb_prediction_cache.py::test_build_predictions_for_date_uses_explicit_feature_loaders -q`
- Expected failure before implementation: predictions were empty because direct feature-store method calls raised `AssertionError` instead of going through `PitcherInferenceLoader` / `BatterInferenceLoader`.

GREEN result:

- RED test passed after wiring the loaders:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_prediction_cache.py::test_build_predictions_for_date_uses_explicit_feature_loaders -q`
  - Result: 1 passed, 1 warning.
- Focused Lane 02 + prediction-cache suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_prediction_cache.py -q`
  - Result: 87 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or prediction_cache or run_mlb_sweep or quote_clean)" -q`
  - Result: 120 passed, 896 deselected, 1 warning.
- Compile + diff hygiene passed:
  - `./venv/Scripts/python.exe -m py_compile src/backtesting/mlb/prediction_cache.py src/models/mlb/features/pitcher_inference_loader.py src/models/mlb/features/batter_inference_loader.py && git diff --check -- src/backtesting/mlb/prediction_cache.py src/models/mlb/features/pitcher_inference_loader.py src/models/mlb/features/batter_inference_loader.py tests/test_mlb_prediction_cache.py tests/test_mlb_sweep_inventory.py`

Behavior-preservation notes:

- No model math, feature semantics, line fetching, DB queries, artifact loading, or result serialization changed.
- Existing `build_predictions_for_date(...)` call signature is unchanged; callers still pass the same feature-store objects.
- The backtest sweep's `matchup_cache` is still forwarded for batter date features.
- This completes the first Phase 8 callsite lane: canonical quote-clean sweep prediction generation now uses the explicit request/loader seam. Training pipelines remain intentionally unmigrated because the plan requires callsites to move one lane at a time.

### 2026-06-07 Phase 8B — daily runner player-game feature callsites use explicit loaders

Files changed:

- Modified `src/models/mlb/mlb_daily_runner.py`:
  - pitcher daily prediction feature loading now builds `PlayerGameFeatureRequest` and calls `PitcherInferenceLoader.load_player_game(...)`;
  - batter daily prediction feature loading now builds `PlayerGameFeatureRequest` and calls `BatterInferenceLoader.load_player_game(...)` with existing `opp_pitcher_id`, lineup position, and `stat="hits"` behavior preserved.
- Created `tests/test_mlb_daily_runner_feature_loaders.py`:
  - added RED/GREEN guards for pitcher and batter daily-runner callsites that fail if direct feature-store methods are used.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_mlb_daily_runner_feature_loaders.py -q`
- Expected failure before implementation after fixture setup was corrected: both tests returned no predictions because direct feature-store method calls raised `AssertionError` instead of going through the explicit loaders.

GREEN result:

- Daily-runner feature-loader tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_daily_runner_feature_loaders.py -q`
  - Result: 2 passed, 1 warning.
- Focused daily/prediction/request/inventory suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_daily_runner_feature_loaders.py tests/test_mlb_prediction_cache.py tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_requests.py -q`
  - Result: 14 passed, 1 warning.
- Lane-wide filtered regression including daily runner passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or prediction_cache or daily_runner or run_mlb_sweep or quote_clean)" -q`
  - Result: 122 passed, 896 deselected, 1 warning.
- Compile + diff hygiene passed for the touched files.

Behavior-preservation notes:

- No daily-runner edge math, BL blending, line loading, prop-line lookup, model-suite behavior, or persistence path changed.
- Existing runner constructor and public methods are unchanged; the feature-store objects are still accepted, but daily player-game feature access now crosses the explicit request/loader seam.
- This moves the production daily inference feature callsites onto Phase 8 loader/request boundaries without changing feature semantics.

### 2026-06-07 Phase 8C — training pipelines and legacy harness cross loader/request seams

Files changed:

- Modified `src/models/mlb/mlb_train_pipeline.py` and `src/models/mlb/mlb_batter_train_pipeline.py`:
  - training/calibration dataset loading now calls `PitcherTrainingLoader.load(...)` / `BatterTrainingLoader.load(...)` with explicit `TrainingFeatureRequest` objects.
- Modified `src/backtesting/mlb/mlb_backtest_harness.py`:
  - the legacy/debug pitcher prediction path now builds a `PlayerGameFeatureRequest` and calls `PitcherInferenceLoader.load_player_game(...)`.
- Created `tests/test_mlb_training_pipeline_feature_loaders.py`:
  - structural RED/GREEN guards ensure the MLB pitcher and batter training pipelines use training loaders and no longer call `self.feature_store.get_training_dataset(...)` directly.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_mlb_training_pipeline_feature_loaders.py -q`
- Expected failure before implementation: both pipeline source guards failed because the loader/request imports and `self.training_loader.load(...)` calls did not exist.

GREEN result:

- Training-pipeline loader guards passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_training_pipeline_feature_loaders.py -q`
  - Result: 2 passed, 1 warning.
- Focused training/daily/prediction/request/inventory suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_training_pipeline_feature_loaders.py tests/test_mlb_daily_runner_feature_loaders.py tests/test_mlb_prediction_cache.py tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_inventory.py -q`
  - Result: 33 passed, 1 warning.
- Legacy harness deprecation tests passed after harness loader wiring:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_backtest_legacy_deprecation.py -q`
  - Result: 3 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or prediction_cache or daily_runner or train_pipeline or backtest_legacy or run_mlb_sweep or quote_clean)" -q`
  - Result: 131 passed, 889 deselected, 1 warning.
- Compile and diff hygiene passed for touched files.

Behavior-preservation notes:

- Training feature enrichment, interaction-feature addition, feature selection, model fitting, artifact writing, and calibration logic were not changed.
- The legacy/debug harness remains legacy/debug-only; this slice only routes its pitcher feature access through the request/loader seam.
- Phase 8's major planned callsite categories are now moved to explicit loader/request seams: canonical quote-clean sweep prediction generation, daily player-game inference, training dataset loading, and the legacy/debug backtest harness. Diagnostic scripts still contain direct facade usage and should be handled as cleanup or rewritten around the migrated callsites if they are still used.

### 2026-06-07 Phase 9 — thin facades and anti-regrowth guards

Files changed:

- Modified `src/models/mlb/mlb_feature_store.py`:
  - reduced the public pitcher facade to a thin compatibility adapter that re-exports stable constants/config and subclasses the legacy implementation.
- Modified `src/models/mlb/mlb_batter_feature_store.py`:
  - reduced the public batter facade to a thin compatibility adapter that re-exports stable constants/config and subclasses the legacy implementation.
- Created `src/models/mlb/features/legacy_pitcher_feature_store.py` and `src/models/mlb/features/legacy_batter_feature_store.py`:
  - moved the behavior-preserving legacy implementations behind the facade boundary while callsites continue migrating to explicit loaders/source modules.
- Modified `tests/test_mlb_feature_store_inventory.py`:
  - tightened facade LOC guards from documented-only to enforced `<600` non-comment LOC thresholds;
  - added anti-regrowth guards that fail if the facade classes define source-specific helper methods again;
  - kept prop-line ownership assertions against the legacy implementation plus raw-SQL absence checks on the thin facades.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py -q`
- Expected failures before implementation: facade LOC thresholds failed (`1383` pitcher NLOC and `1124` batter NLOC), helper-method guard failed, and facade MRO did not point at legacy implementation modules.

GREEN result:

- Facade inventory guards passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py -q`
  - Result: 7 passed, 1 warning.
- Focused Lane 02 suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_feature_store.py src/models/mlb/mlb_batter_feature_store.py src/models/mlb/features/legacy_pitcher_feature_store.py src/models/mlb/features/legacy_batter_feature_store.py src/models/mlb/features/*.py && ./venv/Scripts/python.exe -m pytest tests/test_mlb_feature_store_inventory.py tests/test_mlb_feature_contracts.py tests/test_mlb_feature_transforms.py tests/test_mlb_feature_temporal_contracts.py tests/test_mlb_prop_line_feature_source.py tests/test_mlb_shared_feature_sources.py tests/test_mlb_pitcher_feature_sources.py tests/test_mlb_batter_feature_sources.py tests/test_mlb_feature_requests.py tests/test_mlb_feature_store_as_of.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_training_pipeline_feature_loaders.py tests/test_mlb_daily_runner_feature_loaders.py tests/test_mlb_prediction_cache.py tests/test_mlb_backtest_legacy_deprecation.py -q`
  - Result: 96 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "mlb and (feature_store or feature or prediction_cache or daily_runner or train_pipeline or backtest_legacy or run_mlb_sweep or quote_clean)" -q`
  - Result: 133 passed, 889 deselected, 1 warning.

Behavior-preservation notes:

- Public imports from `src.models.mlb.mlb_feature_store` and `src.models.mlb.mlb_batter_feature_store` remain stable.
- No model math, feature semantics, query text, as-of behavior, line-selection policy, or feature defaults changed in this slice.
- This is a structural facade boundary: the legacy behavior is now behind explicit `features/legacy_*_feature_store.py` modules, and the public facade files are protected from regrowing SQL/source helper ownership.

### 2026-05-19 initial migration documentation

Created this plan from a bounded code/brain deep dive.

Evidence inspected:

- `src/models/mlb/mlb_feature_store.py` AST/method inventory.
- `src/models/mlb/mlb_batter_feature_store.py` AST/method inventory.
- Existing temporal regression tests in `tests/test_mlb_feature_store_as_of.py`.
- Existing batter feature/list/transform tests in `tests/test_mlb_batter_feature_store.py`.
- Bounded callsite scan across `src/models/mlb`, `src/backtesting/mlb`, `src/orchestration`, `src/paper_trading`, `scripts`, and `tests`.
- GBrain hard facts, critical invariants, MLB model page, and relevant lessons.
- Kalshi migration plan expansion pattern and current god-class migration index.

Current status:

- Documentation only.
- No production code changed.
- No tests run for this doc beyond file write/lint skip.

---

## Current status

**Status:** Complete as of 2026-06-07.

Lane 02 reached its planned structural endpoint: feature contracts, pure transforms, temporal/prop-line source ownership, source/request modules, major train/backtest/inference callsite loader seams, thin compatibility facades, and anti-regrowth inventory guards are all in place and validated. Diagnostic scripts may still import the stable facade names, but production/training/backtest major paths are migrated or pass through thin adapters.

---

## Done when

- [x] MLB pitcher/batter feature contracts live outside SQL god classes.
- [x] Pure feature transforms are tested without DB/class construction.
- [x] Prop-line feature source has a single tested owner for as-of and pre-commence guards.
- [x] Pitcher, batter, and shared feature source loaders have focused modules and tests.
- [x] Training/date/single-game requests make caller intent explicit.
- [x] Existing training, backtest, and inference callsites either use focused loaders directly or pass through thin compatibility facades.
- [x] Facades no longer contain raw prop-line SQL or giant feature-family helper methods.
- [x] Inventory tests prevent the facades from regrowing into god classes.
- [x] Quote-clean/backtest migration #01 and this migration agree on temporal semantics and metadata.
