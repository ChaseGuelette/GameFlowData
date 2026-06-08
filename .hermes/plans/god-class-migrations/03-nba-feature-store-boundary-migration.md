# NBA / General Feature-Store Boundary Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change NBA production feature semantics, retrain models, run long backtests, or execute DB-heavy audits.

**Goal:** Rebuild the NBA/general `FeatureStore` into explicit, testable boundaries for feature contracts, temporal-safe data access, pure transforms, injury context, and caller-facing compatibility.

**Architecture:** Preserve production behavior first. Keep `src/models/feature_store.py::FeatureStore` as a stable compatibility facade while extracting feature contracts, query builders, source loaders, injury/context services, and pure transforms into focused modules. Because NBA is production-profitable, deletion is not the goal; a thin facade with anti-regrowth guards is the likely endpoint.

**Tech Stack:** Python, pandas, SQLAlchemy, pytest, existing NBA train/backtest/inference callers, GameFlow SQL-runner pattern for any DB truth.

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

1. NBA is production-sensitive; feature-store extraction must preserve model inputs before any behavior change.
2. Never deploy global conformal recalibration offsets.
3. Q10 miscalibration is edge-bearing; do not “fix” low-tail features/calibration because metrics look cleaner.
4. Probabilities downstream of MC samples must use empirical CDF.
5. Feature lists and feature-family changes are not proof of value; downstream ROI/calibration/backtest gates decide.
6. Feature generation must use only pre-target data and explicit pregame odds/injury snapshots.
7. `raw_player_props_combined` is large; no broad aggregates or non-concurrent index work from this lane.

---

## Executive diagnosis

The NBA/general feature store is a production-critical god class:

- `src/models/feature_store.py`
  - 1,918 total lines
  - 1,675 non-comment LOC
  - `FeatureStore`: 1,695 class lines, 17 methods

It currently owns:

- feature-list constants for minutes/rate models
- feature config defaults
- single-player feature assembly
- date batch feature assembly
- date-range feature assembly
- training dataset loading
- raw SQL for player/team/opponent/game-line/prop-line/injury context
- injury bulk loading and single-player injury context
- context snapshots
- positional/opponent stats
- prop-line/game-line retrieval
- train/backtest/inference compatibility

This is risky because NBA production profitability depends on exact feature behavior. The migration must reduce coupling without changing the feature contract silently.

---

## Current ownership problems

### 1. Feature constants live beside raw SQL and DB orchestration

Current constants in `src/models/feature_store.py` include:

- `MINUTES_FEATURES`
- `RATE_FEATURES_PTS`
- `RATE_FEATURES_REB`
- `RATE_FEATURES_AST`
- `RATE_FEATURES_THREES`
- `RATE_FEATURES_BLOCKS`
- `RATE_FEATURES_STEALS`
- `RATE_FEATURES_TURNOVERS`
- combo/stat maps and model compatibility names

Why this is wrong:

- Feature contracts should be reviewable without reading a 1,900-line SQL/orchestration file.
- Artifact compatibility depends on exact names, including historical names kept for compatibility.
- Family-level validation is hard when families are not first-class metadata.

Target owner:

- `src/models/features/nba/contracts.py`

Responsibilities:

- feature lists
- stat-to-feature mappings
- compatibility aliases/comments
- feature-family metadata
- feature-list validation helpers

Compatibility rule:

- Existing imports from `src.models.feature_store` must continue to work until all callers are migrated.

---

### 2. Public API mixes training, inference, backtest, and utility queries

Current methods:

- `get_player_game_features(...)`, lines 238-323
- `get_features_for_date(...)`, lines 325-649
- `get_features_for_date_range(...)`, lines 663-997
- `get_training_dataset(...)`, lines 999-1150
- `_load_single_season_training(...)`, lines 1152-1394

Why this is wrong:

- Training and inference have different temporal and data-availability requirements.
- Backtest date-range assembly is not the same responsibility as live single-game feature fetch.
- Tests have to patch a huge object for narrow behavior.

Target owners:

- `src/models/features/nba/requests.py`
- `src/models/features/nba/training_loader.py`
- `src/models/features/nba/inference_loader.py`
- `src/models/features/nba/date_batch_loader.py`
- `src/models/features/nba/date_range_loader.py`

Keep `FeatureStore` as compatibility facade initially.

---

### 3. Injury context is large and mixed into base feature store

Current methods:

- `_load_injury_features_bulk(...)`, lines 1396-1470
- `_load_player_injury_status_bulk(...)`, lines 1472-1484
- `_get_injury_context(...)`, lines 1749-1918

Why this is wrong:

- Injury opportunity features are a distinct source family with meaningful defaults and positional logic.
- Injury data has availability/freshness semantics that should be tested separately.
- It is hard to reason about production recommendations if injury context silently fails or changes defaults.

Target owner:

- `src/models/features/nba/injury_context.py`

Responsibilities:

- bulk injury feature loading
- player status loading
- same-position opportunity metrics
- default/fallback policy
- freshness/availability warnings if applicable

Tests:

- missing injury rows preserve current defaults
- questionable/probable flags map correctly
- same-position sums match current behavior
- bulk and single-player paths agree on schema

---

### 4. Lines and prop-line features need isolated query ownership

Current methods:

- `_get_game_lines(...)`, lines 1689-1709
- `_get_player_prop_lines(...)`, lines 1711-1747

Why this is wrong:

- `raw_player_props_combined` is very large and query safety matters.
- Feature-store prop-line semantics must be distinct from edge-calculation line selection.
- Query predicates/snapshot cutoffs should be inventory-testable.

Target owner:

- `src/models/features/nba/line_sources.py`

Responsibilities:

- game spread/total query builder
- player prop-line query builder
- sargable snapshot/date predicates
- source metadata/defaults

Tests:

- prop-line query includes date/snapshot constraints required by current behavior
- no broad raw prop aggregate exists in feature source
- feature path and edge path remain separate concepts

---

### 5. Player/team/opponent context sources are not separate

Current methods:

- `_get_player_position(...)`
- `_get_context_snapshots(...)`
- `_get_player_rolling_stats(...)`
- `_get_team_rolling_stats(...)`
- `_get_opponent_positional_stats(...)`

Why this is wrong:

- Source-specific defaults and temporal predicates are not independently testable.
- Team/opponent/player windows can regress without a focused test.
- Feature-family validation needs named sources.

Target owners:

- `src/models/features/nba/player_sources.py`
- `src/models/features/nba/team_sources.py`
- `src/models/features/nba/opponent_sources.py`
- `src/models/features/nba/context_sources.py`

---

### 6. Pure transforms/defaults are hidden in DB methods

The current class builds final feature dicts/DataFrames while also performing DB reads. Any pure derivation/default fill should be moved out.

Target owner:

- `src/models/features/nba/transforms.py`

Responsibilities:

- ratios/trend features
- starter probability/defaults
- rest/schedule derived fields
- final schema/default fill helpers

Tests:

- zero denominator behavior
- missing-column defaults
- historical compatibility column names preserved

---

## Target design by responsibility

### A. `features/nba/contracts.py`

Owns feature lists, stat maps, feature-family metadata, compatibility aliases.

Validation:

- `tests/test_nba_feature_contracts.py`

---

### B. `features/nba/requests.py`

Owns request dataclasses:

- `NbaTrainingFeatureRequest`
- `NbaDateFeatureRequest`
- `NbaDateRangeFeatureRequest`
- `NbaPlayerGameFeatureRequest`

Validation:

- `tests/test_nba_feature_requests.py`

---

### C. `features/nba/line_sources.py`

Owns game/prop line query builders and safe source policies.

Validation:

- `tests/test_nba_line_feature_sources.py`

---

### D. `features/nba/injury_context.py`

Owns injury feature loading/defaults.

Validation:

- `tests/test_nba_injury_context.py`

---

### E. `features/nba/player_sources.py`, `team_sources.py`, `opponent_sources.py`, `context_sources.py`

Own source-specific SQL/query-builder boundaries.

Validation:

- fake-engine/query-builder tests; no remote DB-heavy tests in the migration slice.

---

### F. `features/nba/transforms.py`

Owns pure feature transforms/defaults.

Validation:

- `tests/test_nba_feature_transforms.py`

---

### G. Thin compatibility facade

Final role for `src/models/feature_store.py`:

- imports/re-exports constants for compatibility
- translates old public method calls into request objects
- delegates to focused loaders/sources
- no raw prop-line SQL
- no giant injury logic

---

## Refactor phases

### Phase 0: Safety baseline and inventory

Objective: Make current behavior and callsites observable before extraction.

Tasks:

1. Add `tests/test_nba_feature_store_inventory.py`.
2. Assert current public methods exist.
3. Assert feature constants are exported from `src.models.feature_store`.
4. Add non-failing inventory notes for eventual shrink thresholds.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_feature_store.py tests/test_nba_feature_store_inventory.py -q`

---

### Phase 1: Extract contracts

Objective: Move feature lists/stat mappings without behavior change.

Files:

- Create: `src/models/features/__init__.py`
- Create: `src/models/features/nba/__init__.py`
- Create: `src/models/features/nba/contracts.py`
- Create: `tests/test_nba_feature_contracts.py`
- Modify: `src/models/feature_store.py` re-export constants

TDD tests:

- every current feature list is identical after re-export
- no duplicate features
- compatibility names/comments are preserved
- each feature belongs to a named family

---

### Phase 2: Extract pure transforms/defaults

Objective: Move no-DB feature derivations out first.

Files:

- Create: `src/models/features/nba/transforms.py`
- Create: `tests/test_nba_feature_transforms.py`
- Modify: `src/models/feature_store.py`

TDD tests:

- existing tests in `tests/test_feature_store.py` still pass
- pure transforms can run without engine construction
- missing/default behaviors are preserved

---

### Phase 3: Extract line feature sources

Objective: Give game/prop lines one tested owner.

Files:

- Create: `src/models/features/nba/line_sources.py`
- Create: `tests/test_nba_line_feature_sources.py`
- Modify: `src/models/feature_store.py`

TDD tests:

- prop-line query uses current constraints
- no broad aggregate against `raw_player_props_combined`
- feature-store line source is separate from edge/recommendation line selection

Safety:

- Do not add indexes or run DB DDL.

---

### Phase 4: Extract injury context

Objective: Isolate injury opportunity logic and defaults.

Files:

- Create: `src/models/features/nba/injury_context.py`
- Create: `tests/test_nba_injury_context.py`
- Modify: `src/models/feature_store.py`

TDD tests:

- bulk loading schema matches current feature columns
- single-player injury context preserves defaults
- same-position opportunity metrics match current behavior on small fixtures

---

### Phase 5: Extract player/team/opponent/context source loaders

Objective: Move DB source families into focused modules.

Files:

- Create source modules and tests listed above.
- Modify `FeatureStore` to delegate.

TDD tests:

- each source query builder can be tested without a real DB
- temporal predicates are present
- fallback defaults match current behavior

---

### Phase 6: Introduce request objects and mode-specific loaders

Objective: Make training/date/date-range/player-game intent explicit.

Files:

- Create `features/nba/requests.py`
- Create `training_loader.py`, `inference_loader.py`, `date_batch_loader.py`, `date_range_loader.py`
- Add tests for request validation and facade translation.

Compatibility:

- Old public methods must still work until callsites migrate.

---

### Phase 7: Migrate callsites one lane at a time

Suggested order:

1. scripts/tools
2. backtest harness/sweep
3. training pipeline
4. daily inference jobs last

Known callsites include:

- `src/backtesting/run_backtest.py`
- `src/backtesting/run_sweep.py`
- `src/models/train_pipeline.py`
- `src/models/analyze_calibration_drift.py`
- `src/models/analyze_minutes_bimodality.py`
- `src/orchestration/inference_job.py`
- `src/orchestration/run_daily.py`
- `src/orchestration/edge_refresh_job.py`
- `src/tools/backfill_prediction_features.py`
- `src/tools/compare_models.py`

TDD pattern:

- callsite-level test forbids old direct path only for the slice being migrated
- verify RED
- rewire to focused loader/source
- verify GREEN

---

### Phase 8: Thin facade and anti-regrowth guards

Inventory assertions after migration:

- `src/models/feature_store.py` has no raw prop-line SQL
- no giant injury-context method remains
- source-specific loaders live outside the facade
- non-comment LOC below agreed threshold, recommended initial threshold: 700
- public compatibility methods remain or have explicit deprecation/removal tests

---

## Files likely touched

Core:

- `src/models/feature_store.py`
- `src/models/features/nba/contracts.py` (new)
- `src/models/features/nba/requests.py` (new)
- `src/models/features/nba/transforms.py` (new)
- `src/models/features/nba/line_sources.py` (new)
- `src/models/features/nba/injury_context.py` (new)
- `src/models/features/nba/player_sources.py` (new)
- `src/models/features/nba/team_sources.py` (new)
- `src/models/features/nba/opponent_sources.py` (new)
- `src/models/features/nba/context_sources.py` (new)
- `src/models/features/nba/training_loader.py` (new)
- `src/models/features/nba/inference_loader.py` (new)
- `src/models/features/nba/date_batch_loader.py` (new)
- `src/models/features/nba/date_range_loader.py` (new)

Tests:

- `tests/test_feature_store.py` existing
- `tests/test_nba_feature_store_inventory.py` new
- `tests/test_nba_feature_contracts.py` new
- `tests/test_nba_feature_transforms.py` new
- `tests/test_nba_line_feature_sources.py` new
- `tests/test_nba_injury_context.py` new
- `tests/test_nba_feature_requests.py` new

---

## Validation commands

Baseline:

`venv/Scripts/python.exe -m pytest tests/test_feature_store.py -q`

Contracts/transforms:

`venv/Scripts/python.exe -m pytest tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`

Line/injury sources:

`venv/Scripts/python.exe -m pytest tests/test_nba_line_feature_sources.py tests/test_nba_injury_context.py tests/test_feature_store.py -q`

Lane-wide:

`venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/*.py`

Diff hygiene:

`git diff --check -- src/models/feature_store.py src/models/features tests .hermes/plans/god-class-migrations/03-nba-feature-store-boundary-migration.md`

---

## Risk controls / non-goals

Non-goals:

- Do not change NBA production feature semantics.
- Do not retrain or promote NBA models.
- Do not change recalibration policy.
- Do not touch database indexes/DDL.
- Do not run heavy remote DB audits from main context.
- Do not combine this with daily runner or training orchestrator migration in one commit.

Hard rules:

- Preserve current feature names and artifact compatibility.
- Preserve production defaults first.
- Any behavior-changing fix requires a failing regression and Chase approval.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A constant is imported widely and needs compatibility re-export.
2. A feature has a historical artifact-compatible name that cannot be renamed.
3. Injury context has different bulk vs single-player behavior.
4. A source query has different temporal semantics than method docs claim.
5. A caller depends on private methods or incidental columns.
6. A feature-family validation question appears; route to a modeling experiment, not structural extraction.
7. A behavior-changing leakage fix appears; split it from extraction.

Progress log entries must distinguish module exists, facade delegates, callsite migrated, wrapper remains, old helper removed, behavior-changing hardening deferred.

---

## First implementation PR recommendation

Start with contracts only:

1. Create `src/models/features/nba/contracts.py`.
2. Re-export current constants from `src/models/feature_store.py`.
3. Add `tests/test_nba_feature_contracts.py`.
4. Do not touch SQL, injury logic, or callsites.

This is the smallest safe seam for a production-sensitive NBA migration.

---

## Progress log

### 2026-06-07 Phase 0/1 — safety inventory and NBA feature contracts

Scope for full Lane 03 migration:

- Phase 0: safety baseline and inventory guards for public API, compatibility exports, and current facade size.
- Phase 1: move NBA feature contracts/constants into `src/models/features/nba/contracts.py` while preserving legacy `src.models.feature_store` imports.
- Phase 2: extract pure transforms/defaults with no DB behavior changes.
- Phase 3: extract game/prop line feature sources without changing line semantics or adding DB indexes/DDL.
- Phase 4: extract injury context/defaults as a separate source family.
- Phase 5: extract player/team/opponent/context source query boundaries.
- Phase 6: introduce explicit request objects and training/date/date-range/player-game loaders.
- Phase 7: migrate callsites one lane at a time, with daily inference last.
- Phase 8: thin `FeatureStore` facade and enforce anti-regrowth guards.

Files changed in this low-risk slice:

- Created `src/models/features/__init__.py`.
- Created `src/models/features/nba/__init__.py`.
- Created `src/models/features/nba/contracts.py`:
  - owns `MINUTES_FEATURES`, `RATE_FEATURES_PTS`, `RATE_FEATURES_REB`, `RATE_FEATURES_AST`, and `RATE_FEATURES_THREES`;
  - adds `RATE_FEATURES_BY_STAT`, `NBA_FEATURE_LISTS`, coarse `NBA_FEATURE_FAMILIES`, `feature_family(...)`, and `validate_feature_lists()`.
- Modified `src/models/feature_store.py`:
  - re-exports the feature constants from the new contracts module for compatibility;
  - leaves `FeatureConfig`, SQL, injury logic, transforms, and public methods unchanged.
- Created `tests/test_nba_feature_contracts.py`:
  - verifies contract lists match the legacy re-exports by identity;
  - guards duplicate features, stat mapping, and family metadata coverage;
  - verifies `FeatureStore` no longer owns inline feature-list definitions.
- Created `tests/test_nba_feature_store_inventory.py`:
  - records public method availability and current facade size;
  - keeps later Phase 8 shrink/source-helper guards visible without enforcing them yet.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_feature_contracts.py tests/test_nba_feature_store_inventory.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features'` because the NBA feature contract package did not exist yet.

GREEN result:

- Focused contracts/inventory/existing feature-store suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_feature_contracts.py tests/test_nba_feature_store_inventory.py tests/test_feature_store.py -q`
  - Result: 101 passed, 1 warning.
- Compile + lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/contracts.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 187 passed, 917 deselected, 1 warning.

Behavior-preservation notes:

- No NBA production feature semantics, SQL, injury context, line-source behavior, training/inference caller wiring, recalibration policy, artifacts, or DB state changed.
- Existing imports from `src.models.feature_store` remain stable for all moved constants.
- This completes the first low-risk Lane 03 slice and sets up Phase 2 (`features/nba/transforms.py`) as the next safe seam.

### 2026-06-07 Phase 2 — player rolling pure transforms/defaults

Files changed:

- Created `src/models/features/nba/transforms.py`:
  - added DB-free helpers for `safe_ratio(...)`, `starter_probability(...)`, `rest_schedule_features(...)`, `default_player_rolling_features()`, and `build_player_rolling_features(...)`;
  - preserved artifact-compatible historical ratio names where only PTS uses L15 and REB/AST/THREES use L5 denominators.
- Modified `src/models/feature_store.py`:
  - `_get_player_rolling_stats(...)` now delegates fallback/default mapping and Python-derived player rolling features to the transform helpers after the same DB query;
  - SQL text, source predicates, public methods, and caller wiring are unchanged.
- Created `tests/test_nba_feature_transforms.py`:
  - covers safe division/default behavior, starter probability cap, rest/schedule defaults, no-row defaults, and row-to-feature mapping/ratios.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_feature_transforms.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.transforms'` because the transform owner module did not exist yet.

GREEN result:

- Transform + existing feature-store tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`
  - Result: 24 passed, 1 warning.
- Focused Lane 03 contracts/transforms/inventory suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/contracts.py src/models/features/nba/transforms.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_nba_feature_store_inventory.py tests/test_feature_store.py -q`
  - Result: 106 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 187 passed, 922 deselected, 1 warning.

Behavior-preservation notes:

- No NBA SQL, line-source behavior, injury context, production caller wiring, recalibration policy, artifacts, or DB state changed.
- The extraction is limited to pure Python mapping/default behavior for single-player player rolling stats.
- Training/date-batch SQL still computes equivalent ratios in SQL and remains intentionally untouched until later source/query phases.

### 2026-06-07 Phase 3 — line-source query ownership

Files changed:

- Created `src/models/features/nba/line_sources.py`:
  - added `game_lines_query()` and `player_prop_lines_query()` as explicit query-builder owners for single-player game-line and prop-line features;
  - added default row mappers and fetch helpers for `line_spread_raw`, `line_total`, `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, and `prop_line_threes`;
  - kept feature-source prop lines separate from downstream edge-calculation line selection.
- Modified `src/models/feature_store.py`:
  - `_get_game_lines(...)` and `_get_player_prop_lines(...)` now delegate to `features/nba/line_sources.py`;
  - method names/signatures remain available for compatibility.
- Created `tests/test_nba_line_sources.py`:
  - guards current source tables, bookmaker filters, as-of predicates, pre-commence predicates, and `DISTINCT ON (market_key)` latest-market selection;
  - verifies no broad `GROUP BY` aggregate was introduced on `raw_player_props_combined`;
  - verifies default and row-mapping behavior.
- Updated `tests/test_nba_feature_store_inventory.py`:
  - records the line-source module as part of the Lane 03 boundary package;
  - adds an anti-regrowth check that the facade methods no longer inline raw line-source table SQL.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_line_sources.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.line_sources'` because the line-source owner module did not exist yet.

GREEN result:

- Focused line-source + Lane 03 contract/transform/inventory suite passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_line_sources.py tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py -q`
  - Result: 92 passed, 1 warning.
- Compile + focused feature-store compatibility suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/contracts.py src/models/features/nba/transforms.py src/models/features/nba/line_sources.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_line_sources.py tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`
  - Result: 111 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 189 passed, 925 deselected, 1 warning.

Behavior-preservation notes:

- No source table, bookmaker, as-of date, snapshot/inserted fallback, commence-time, or latest-market ordering semantics changed for single-player line helpers.
- Date/date-range batch SQL still owns its embedded lateral line joins and remains intentionally untouched until a later query-builder/date-loader phase.
- No DB state, indexes, retraining, artifacts, calibration, injury context, or production caller wiring changed.

### 2026-06-07 Phase 4 — injury-context source ownership/defaults

Files changed:

- Created `src/models/features/nba/injury_context.py`:
  - added single-player injury query builders for team OUT-player game stats, team OUT-player advanced stats, opponent OUT-player stats, player injury status, and same-position opportunity metrics;
  - added bulk training enrichment query builders for OUT-player injury rows and player injury statuses;
  - added `default_injury_context()`, `status_flags(...)`, `build_injury_context(...)`, `aggregate_team_injury_features(...)`, `load_injury_features_bulk(...)`, `load_player_injury_status_bulk(...)`, and `get_injury_context(...)`.
- Modified `src/models/feature_store.py`:
  - `_get_injury_context(...)` now delegates to `features/nba/injury_context.py`;
  - `_load_injury_features_bulk(...)` now delegates to `load_injury_features_bulk(...)`;
  - `_load_player_injury_status_bulk(...)` now delegates to `load_player_injury_status_bulk(...)`;
  - public/private compatibility method names remain available for existing callers/tests.
- Created `tests/test_nba_injury_context.py`:
  - guards default injury schema and zero fallbacks;
  - verifies Questionable/Probable flag mapping;
  - verifies single-player row-to-feature mapping including same-position injury opportunity features;
  - guards current injury source predicates: `rapidapi_injuries`, `status = 'Out'`, report date equality, pre-game average lookback, latest injury status ordering, same-position player exclusion, and starter formula;
  - verifies bulk query predicates/schema and aggregation behavior.
- Updated `tests/test_nba_feature_store_inventory.py`:
  - records `injury_context.py` as part of the Lane 03 boundary package;
  - adds anti-regrowth checks that FeatureStore injury facade methods no longer inline `rapidapi_injuries` SQL.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_injury_context.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.injury_context'` because the injury-context owner module did not exist yet.

GREEN result:

- Injury-context tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_injury_context.py -q`
  - Result: 6 passed, 1 warning.
- Compile + focused Lane 03/feature-store compatibility suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/contracts.py src/models/features/nba/transforms.py src/models/features/nba/line_sources.py src/models/features/nba/injury_context.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_injury_context.py tests/test_nba_line_sources.py tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`
  - Result: 118 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 191 passed, 930 deselected, 1 warning.

Behavior-preservation notes:

- No injury source table, status filter, report-date, average lookback, position-history, same-position exclusion, starter formula, or default feature value semantics changed.
- Date/date-range/training merge logic still lives in `FeatureStore`; this phase only moves injury source query/default ownership and compatibility facade delegation.
- No DB state, indexes, retraining, artifacts, calibration, line-source behavior, or production caller wiring changed.

### 2026-06-07 Phase 5 — player/team/opponent/context source query boundaries

Files changed:

- Created `src/models/features/nba/context_sources.py`:
  - owns `player_position_query()` and `context_snapshots_query()`;
  - maps missing context/position rows to the existing `None` behavior;
  - fetches historical team/opponent/home/position context for the compatibility facade.
- Created `src/models/features/nba/player_sources.py`:
  - owns the single-player rolling stats query for `player_average_game_stats` plus pre-target `player_average_advanced_stats` lateral lookup;
  - delegates row/default mapping to the Phase 2 transform helpers.
- Created `src/models/features/nba/team_sources.py`:
  - owns the latest team/opponent rolling stats query and prefix/default mapping for `team_*` and `opp_*` features.
- Created `src/models/features/nba/opponent_sources.py`:
  - owns opponent positional-defense query and default mapping for `opp_pos_*` features.
- Modified `src/models/feature_store.py`:
  - `_get_player_position(...)`, `_get_context_snapshots(...)`, `_get_player_rolling_stats(...)`, `_get_team_rolling_stats(...)`, and `_get_opponent_positional_stats(...)` now delegate to source modules;
  - compatibility method names/signatures remain available for existing callers/tests.
- Created `tests/test_nba_source_boundaries.py`:
  - guards current context/player/team/opponent source tables and temporal predicates;
  - verifies player rolling source keeps pre-target average predicates and rest/schedule source aliases;
  - verifies team/opponent defaults and row mapping behavior.
- Updated `tests/test_nba_feature_store_inventory.py`:
  - records context/player/team/opponent source modules as part of the Lane 03 boundary package;
  - adds anti-regrowth checks that FeatureStore source facade methods no longer inline their raw source table SQL.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_source_boundaries.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.context_sources'` because the Phase 5 source owner modules did not exist yet.

GREEN result:

- Source-boundary + inventory tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_source_boundaries.py tests/test_nba_feature_store_inventory.py -q`
  - Result: 12 passed, 1 warning.
- Compile + focused Lane 03/feature-store compatibility suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/contracts.py src/models/features/nba/transforms.py src/models/features/nba/line_sources.py src/models/features/nba/injury_context.py src/models/features/nba/context_sources.py src/models/features/nba/player_sources.py src/models/features/nba/team_sources.py src/models/features/nba/opponent_sources.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_source_boundaries.py tests/test_nba_injury_context.py tests/test_nba_line_sources.py tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`
  - Result: 124 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 192 passed, 935 deselected, 1 warning.

Behavior-preservation notes:

- No source table, temporal predicate, default value, prefix mapping, row mapping, or production caller behavior changed for the extracted single-player helpers.
- Date/date-range/training embedded SQL still lives in `FeatureStore`; this phase only moves the single-player/private source helper boundaries.
- No DB state, indexes, retraining, artifacts, calibration, injury context semantics, or line-source behavior changed.

### 2026-06-07 Phase 6A — request objects and single-player inference loader

Files changed:

- Created `src/models/features/nba/requests.py`:
  - added `PlayerGameFeatureRequest`, `DateFeatureRequest`, `DateRangeFeatureRequest`, and `TrainingFeatureRequest` dataclasses;
  - added `PlayerGameFeatureRequest.is_scheduled_context` for scheduled-game team/opponent context detection.
- Created `src/models/features/nba/inference_loader.py`:
  - added `InferenceFeatureLoader` for `get_player_game_features(...)` behavior;
  - moved the single-player feature assembly orchestration out of `FeatureStore` while continuing to use the existing compatibility helper methods/source modules;
  - preserved deprecated travel/opp compatibility feature zeros and team-directional spread transformation.
- Modified `src/models/feature_store.py`:
  - `get_player_game_features(...)` now builds a `PlayerGameFeatureRequest` and delegates to `InferenceFeatureLoader(self).load(...)`;
  - public method signature and return behavior remain stable.
- Created `tests/test_nba_inference_loader.py`:
  - guards request object fields/scheduled-context behavior;
  - verifies mode request dataclasses exist for later date/range/training loaders;
  - verifies historical and scheduled-context single-player assembly behavior;
  - verifies `FeatureStore.get_player_game_features(...)` delegates through the inference loader.
- Updated `tests/test_nba_feature_store_inventory.py`:
  - records `requests.py` and `inference_loader.py` as Lane 03 boundary modules;
  - adds an anti-regrowth check that the player-game facade no longer owns spread/deprecated-feature assembly.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_inference_loader.py -q`
- Expected failure before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.inference_loader'` because the Phase 6 request/loader modules did not exist yet.

GREEN result:

- Inference-loader + existing player-game facade tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_inference_loader.py tests/test_feature_store.py::test_get_player_game_features_combines_outputs tests/test_feature_store.py::test_get_player_game_features_returns_none_when_context_missing tests/test_nba_feature_store_inventory.py -q`
  - Result: 15 passed, 1 warning.
- Compile + focused Lane 03/feature-store compatibility suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/requests.py src/models/features/nba/inference_loader.py src/models/features/nba/contracts.py src/models/features/nba/transforms.py src/models/features/nba/line_sources.py src/models/features/nba/injury_context.py src/models/features/nba/context_sources.py src/models/features/nba/player_sources.py src/models/features/nba/team_sources.py src/models/features/nba/opponent_sources.py src/models/features/nba/__init__.py src/models/features/__init__.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_inference_loader.py tests/test_nba_source_boundaries.py tests/test_nba_injury_context.py tests/test_nba_line_sources.py tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_feature_store.py -q`
  - Result: 130 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 195 passed, 938 deselected, 1 warning.

Behavior-preservation notes:

- No source queries, defaults, line/injury semantics, feature names, production caller API, DB state, indexes, retraining, artifacts, or calibration behavior changed.
- This is Phase 6A: request objects plus the single-player inference loader. Date/date-range/training loader extraction remains for the next Phase 6 slice because those methods still own large embedded SQL and merge orchestration.

### 2026-06-07 Phase 6B — date/range/training mode loaders and thin facade

Files changed:

- Created `src/models/features/nba/date_batch_loader.py`:
  - owns the existing single-date batch feature SQL from `FeatureStore.get_features_for_date(...)`;
  - preserves the legacy deprecated travel/opp compatibility zero-fill columns.
- Created `src/models/features/nba/date_range_loader.py`:
  - owns the existing date-range chunked batch SQL and `_get_game_dates_in_range(...)` query;
  - preserves chunk failure logging, chunk-size behavior, grouped `game_date -> DataFrame` return shape, and deprecated compatibility zero-fill columns.
- Created `src/models/features/nba/training_loader.py`:
  - owns `get_training_dataset(...)` orchestration and single-season training SQL;
  - preserves season progress prints, injury feature merge/default behavior, validation checks, deprecated compatibility zero-fill columns, and rate-target derivation.
- Modified `src/models/feature_store.py`:
  - `get_features_for_date(...)`, `_get_game_dates_in_range(...)`, `get_features_for_date_range(...)`, `get_training_dataset(...)`, and `_load_single_season_training(...)` now construct request objects and delegate to mode loaders;
  - `FeatureStore` is now a thin compatibility facade while preserving legacy public/private method names needed by callers/tests.
- Created `tests/test_nba_mode_loaders.py`:
  - verifies the single-date loader can execute the request path with a mocked SQL read;
  - verifies date/date-range/training facades translate legacy method args to request objects and delegate to the correct loaders.
- Updated `tests/test_nba_feature_store_inventory.py`:
  - records date/range/training loaders as boundary modules;
  - adds anti-regrowth checks that batch/training SQL table ownership no longer lives in the facade;
  - tightens the facade LOC guard to keep `feature_store.py` thin.

RED result:

- `./venv/Scripts/python.exe -m pytest tests/test_nba_mode_loaders.py -q`
- Expected failures before implementation: `ModuleNotFoundError: No module named 'src.models.features.nba.date_batch_loader'` and missing `DateBatchFeatureLoader`, `DateRangeFeatureLoader`, and `TrainingFeatureLoader` facade imports.

GREEN result:

- Mode-loader tests passed:
  - `./venv/Scripts/python.exe -m pytest tests/test_nba_mode_loaders.py -q`
  - Result: 4 passed, 1 warning.
- Compile + focused Lane 03 mode-loader/source compatibility suite passed:
  - `./venv/Scripts/python.exe -m py_compile src/models/feature_store.py src/models/features/nba/*.py && ./venv/Scripts/python.exe -m pytest tests/test_nba_inference_loader.py tests/test_nba_mode_loaders.py tests/test_nba_feature_store_inventory.py tests/test_nba_source_boundaries.py tests/test_nba_feature_transforms.py tests/test_nba_line_sources.py tests/test_nba_injury_context.py tests/test_nba_feature_contracts.py -q`
  - Result: 117 passed, 1 warning.
- Lane-wide filtered regression passed:
  - `./venv/Scripts/python.exe -m pytest tests -k "feature_store or daily_runner or train_pipeline or run_sweep or run_backtest" -q`
  - Result: 200 passed, 955 deselected, 1 warning.
- Diff hygiene passed:
  - `git diff --check -- src/models/feature_store.py src/models/features/nba/date_batch_loader.py src/models/features/nba/date_range_loader.py src/models/features/nba/training_loader.py tests/test_nba_mode_loaders.py tests/test_nba_feature_store_inventory.py .hermes/plans/god-class-migrations/03-nba-feature-store-boundary-migration.md`

Behavior-preservation notes:

- No source SQL text was intentionally changed; the existing date/date-range/training SQL moved behind mode-loader classes.
- Legacy `FeatureStore` method names/signatures remain available.
- No callsites were migrated, no DB state/indexes/DDL changed, no retraining/backtests/artifacts were produced, and no calibration/probability behavior changed.

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- `src/models/feature_store.py` AST/method inventory.
- Existing `tests/test_feature_store.py` coverage surface.
- Callsite scan across `src`, `scripts`, and `tests`.
- GBrain hard facts, critical invariants, calibration guide, and model lessons.

Current status:

- Documentation only.
- No production code changed.
- No tests run for this doc beyond write/read verification later.

---

## Done when

- NBA feature contracts live outside the god class.
- Injury, line, player/team/opponent, and transform responsibilities have explicit owners.
- `FeatureStore` is a thin compatibility facade.
- Inventory tests prevent regrowth.
- Production behavior and feature names remain stable unless a separate approved behavior-changing fix is made.
