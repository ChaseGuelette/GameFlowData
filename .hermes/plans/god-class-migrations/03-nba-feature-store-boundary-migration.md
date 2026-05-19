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
