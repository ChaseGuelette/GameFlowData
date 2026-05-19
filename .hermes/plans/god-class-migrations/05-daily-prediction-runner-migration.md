# Daily Prediction Runner Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change live recommendation policy, edge thresholds, model math, scheduler behavior, or deployed prediction writes.

**Goal:** Split NBA and MLB daily prediction runners into explicit, testable boundaries for slate discovery, participant selection, feature assembly, prediction, line retrieval, edge calculation, BL recommendation, and dashboard/persistence shaping.

**Architecture:** Preserve `DailyPredictionRunner` and `MLBDailyPredictionRunner` as compatibility facades while extracting shared primitives and sport-specific services. Edge/probability/recommendation logic must become shared and testable before callsites are rewired. Live-money behavior remains unchanged unless a separate approved RED test captures the intended behavior change.

**Tech Stack:** Python, pandas, NumPy, SQLAlchemy, ThreadPoolExecutor, Monte Carlo predictors, Black-Litterman blender, pytest, existing orchestration jobs.

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

1. Daily recommendations are production/live-money adjacent; preserve behavior before structural cleanup.
2. Probability calculation must use empirical CDF from MC samples when samples exist.
3. Quantile interpolation is a fallback only; do not replace MC empirical CDF with Gaussian CDF.
4. Q10 behavior is edge-bearing; do not “repair” low-tail predictions in edge/recommendation logic.
5. Black-Litterman thresholds/configs are production policy, not refactor cleanup.
6. Railway daily jobs must remain CDN-only for NBA stats; do not add stats.nba.com calls to daily runners.
7. Combo samples must remain derived on-the-fly from base samples, not persisted as DB sample blobs.

---

## Executive diagnosis

Daily prediction is concentrated in two large runners:

- `src/models/daily_runner.py`
  - 1,091 total lines
  - 833 non-comment LOC
  - `DailyPredictionRunner`: 1,018 class lines, 18 methods
  - module-level `should_skip_recommendation(...)`: shared by multiple production/trading paths
- `src/models/mlb/mlb_daily_runner.py`
  - 1,102 total lines
  - 872 non-comment LOC
  - `MLBDailyPredictionRunner`: 1,032 class lines, 13 methods

They currently own:

- slate discovery and DB/API/CDN fallbacks
- game-time enrichment
- player/probable starter/batter universe construction
- injury/lineup filtering
- parallel feature building
- batch prediction calls
- combo sample derivation
- prop-line retrieval and sharpest-book selection
- empirical-CDF edge calculation
- multiplicative devig
- Black-Litterman blending
- recommendation thresholds and sanity skips
- feature-to-prediction mapping for dashboard insight columns
- output schema shaping for storage/dashboard consumers

This is high risk because the same logic feeds daily automation, edge refresh, dashboard picks, Kalshi/paper selection gates, and comparison tools.

---

## Current ownership problems

### 1. Runner owns both workflow and live recommendation policy

NBA `run_for_date(...)` executes:

1. games
2. players
3. injury filtering
4. feature building
5. batch prediction
6. combo derivation
7. line retrieval
8. edge calculation
9. feature mapping
10. BL recommendations

MLB `run_for_date(...)` similarly combines slate discovery, pitcher/batter predictions, line retrieval, edge calculation, feature mapping, and BL recommendations.

Target split:

- runner facade: orchestrates a typed daily prediction request
- services: own slate, participants, features, prediction, market lines, edges, recommendations, output enrichment

---

### 2. Edge calculation is duplicated across live, backtest, and refresh paths

Known duplicated/related paths:

- `src/models/daily_runner.py::_calculate_edges`
- `src/models/mlb/mlb_daily_runner.py::_calculate_edges`
- `src/backtesting/backtest_harness.py::_calculate_edges`
- `src/backtesting/run_sweep.py` inline/vectorized edge logic
- `src/orchestration/edge_refresh_job.py` query and recommendation logic
- `src/orchestration/mlb_edge_refresh_job.py`

Target owner:

- `src/models/prediction/edge_calculator.py`
- `src/models/prediction/devig.py`
- sport adapters for stat/market-key mapping

Required behavior:

- empirical CDF `(samples > line).mean()` primary
- clamp current behavior to `[0.05, 0.95]` where current live path does
- quantile interpolation fallback preserved
- multiplicative devig preserved
- output column names preserved

---

### 3. `should_skip_recommendation` is shared policy hiding in NBA runner module

Current importers include:

- NBA daily runner
- MLB daily runner
- NBA edge refresh job
- MLB edge refresh job
- Kalshi paper trader
- Kalshi selection loader

Target owner:

- `src/models/prediction/recommendation_policy.py`

Compatibility:

- `src.models.daily_runner.should_skip_recommendation` must re-export/delegate initially.
- Do not break importers.

---

### 4. Black-Litterman recommendation code is runner-local and sport-specific

Current methods:

- NBA `_compute_bl_recommendations(...)`, lines 954-1091
- MLB `_compute_bl_recommendations(...)`, lines 852-1102

Policy embedded:

- NBA defaults via `DEFAULT_BL_TAU`, `DEFAULT_BL_Z_MAX`, `DEFAULT_BL_MAX_WEIGHT`, `DEFAULT_BL_EDGE_THRESHOLD`
- MLB per-stat config via `STAT_BL_CONFIGS`, `MLB_STATS`, allowed directions, thresholds
- sanity skip policy via `should_skip_recommendation`

Target owners:

- `src/models/prediction/bl_recommender.py`
- `src/models/prediction/recommendation_policy.py`
- `src/models/mlb/prediction/bl_config.py` if MLB stat-specific policy needs isolation

Non-goal:

- Do not change BL thresholds/configs during extraction.

---

### 5. Slate/participant discovery is mixed with SQL and feature generation

NBA methods:

- `_get_games_for_date(...)`
- `_get_games_from_nba_api(...)`
- `_get_games_from_cdn(...)`
- `_get_games_from_db(...)`
- `_enrich_game_times(...)`
- `_get_players_for_games(...)`
- `_filter_injured_players(...)`

MLB methods:

- `_get_games_for_date(...)`
- `_get_pitchers_for_games(...)`
- `_get_batters_for_games(...)`
- `_filter_batters_by_lineup(...)`

Target owners:

- `src/models/prediction/nba/slate_source.py`
- `src/models/prediction/nba/participant_source.py`
- `src/models/prediction/mlb/slate_source.py`
- `src/models/prediction/mlb/participant_source.py`

Safety:

- NBA Railway-safe behavior remains CDN-only for production daily stats paths. This runner may query NBA API in current code; do not add new stats.nba.com dependencies.
- MLB probable starter and lineup semantics must stay separate.

---

### 6. Feature assembly and prediction execution are hard to unit test

Current methods:

- NBA `_build_features_df(...)` parallel feature building
- MLB `_run_pitcher_predictions(...)`
- MLB `_run_batter_predictions(...)`
- MLB `_bulk_fetch_batter_prop_lines(...)`
- MLB/NBA `_map_features_to_predictions(...)`

Target owners:

- `src/models/prediction/feature_batch_builder.py`
- `src/models/prediction/nba/prediction_executor.py`
- `src/models/prediction/mlb/pitcher_prediction_executor.py`
- `src/models/prediction/mlb/batter_prediction_executor.py`
- `src/models/prediction/feature_projection.py`

Tests:

- concurrency wrapper tests should not need a real DB
- feature failures for one player do not fail entire slate unless current behavior does
- dashboard feature columns preserved

---

### 7. Current line retrieval is overloaded

NBA `_get_current_lines(...)` and MLB `_get_current_lines(...)` mix:

- game/stat filtering
- DB query construction
- bookmaker selection
- stat/market mapping
- output schema for edge calculator

Target owner:

- `src/models/prediction/market_lines.py`
- sport adapters under `src/models/prediction/nba/market_lines.py`, `src/models/prediction/mlb/market_lines.py`

Risk:

- Edge refresh job notes that it replicates `DailyPredictionRunner._get_current_lines()`; this duplication should be pulled into the shared line source before changing either path.

---

## Target design by responsibility

### A. `prediction/contracts.py`

Dataclasses and schema constants:

- `DailyPredictionRequest`
- `SlateGame`
- `Participant`
- `PredictionRecord`
- `MarketLineRecord`
- sample key helper `(player_id, game_id, stat)`

---

### B. `prediction/devig.py`

Owns odds to probabilities and multiplicative devig.

---

### C. `prediction/edge_calculator.py`

Owns empirical-CDF edge calculation and quantile fallback.

---

### D. `prediction/recommendation_policy.py`

Owns `should_skip_recommendation` and common sanity flags.

---

### E. `prediction/bl_recommender.py`

Owns BL blending orchestration and recommendation marking.

---

### F. `prediction/market_lines.py`

Owns shared line result schema and helpers.

---

### G. Sport-specific packages

- `src/models/prediction/nba/slate_source.py`
- `src/models/prediction/nba/participant_source.py`
- `src/models/prediction/nba/prediction_executor.py`
- `src/models/prediction/nba/feature_projection.py`
- `src/models/prediction/mlb/slate_source.py`
- `src/models/prediction/mlb/participant_source.py`
- `src/models/prediction/mlb/pitcher_prediction_executor.py`
- `src/models/prediction/mlb/batter_prediction_executor.py`
- `src/models/prediction/mlb/feature_projection.py`

---

### H. Compatibility facades

Keep public classes/methods while migrating:

- `src.models.daily_runner.DailyPredictionRunner`
- `src.models.mlb.mlb_daily_runner.MLBDailyPredictionRunner`
- `DailyPredictionRunner.run_for_date(...)`
- `MLBDailyPredictionRunner.run_for_date(...)`
- `should_skip_recommendation(...)` re-export from old module

---

## Refactor phases

### Phase 0: Characterization and inventory tests

Objective: Lock public shape, imports, and existing behavior before extraction.

Files:

- Existing: `tests/test_daily_runner.py`
- Create: `tests/test_daily_prediction_runner_inventory.py`
- Create: `tests/test_mlb_daily_runner_inventory.py`

Tests:

- public classes importable
- key private methods still exist until migrated
- `should_skip_recommendation` import path preserved
- sample-key tuple format characterized
- current output columns characterized for edge/BL paths

Validation:

`venv/Scripts/python.exe -m pytest tests/test_daily_runner.py tests/test_daily_prediction_runner_inventory.py tests/test_mlb_daily_runner_inventory.py -q`

---

### Phase 1: Extract devig and edge calculator

Objective: Move the highest-risk duplicated probability logic first with strict parity tests.

Files:

- Create: `src/models/prediction/__init__.py`
- Create: `src/models/prediction/devig.py`
- Create: `src/models/prediction/edge_calculator.py`
- Create: `tests/test_prediction_devig.py`
- Create: `tests/test_prediction_edge_calculator.py`
- Modify: `src/models/daily_runner.py`
- Modify: `src/models/mlb/mlb_daily_runner.py`

TDD tests:

- empirical CDF returns same value as current live runner for MC samples
- quantile fallback returns same value as current live runner
- missing line returns null probability
- multiplicative devig matches current columns
- MLB stat mapping behavior preserved

Safety:

- No Gaussian CDF.
- No threshold changes.

---

### Phase 2: Extract recommendation policy

Objective: Move `should_skip_recommendation` without breaking importers.

Files:

- Create: `src/models/prediction/recommendation_policy.py`
- Create: `tests/test_recommendation_policy.py`
- Modify: `src/models/daily_runner.py` re-export wrapper/delegate
- Modify direct importers only after wrapper test exists

Known importers:

- `src/orchestration/edge_refresh_job.py`
- `src/orchestration/mlb_edge_refresh_job.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/trading/kalshi/selection_loader.py`
- `src/models/mlb/mlb_daily_runner.py`

TDD tests:

- old import path works
- new import path works
- current skip reasons/flags preserved

---

### Phase 3: Extract BL recommender

Objective: Move BL blending and recommendation marking behind a shared service.

Files:

- Create: `src/models/prediction/bl_recommender.py`
- Create: `tests/test_bl_recommender.py`
- Modify: NBA/MLB daily runners to delegate

Tests:

- no samples => BL columns initialized and recommendations false
- NBA default config produces same columns/recommended flags on fixture
- MLB stat-specific config respects allowed directions and thresholds
- `should_skip_recommendation` reason is stored as current behavior stores it

---

### Phase 4: Extract market line sources and de-duplicate edge refresh

Objective: Give live runner and edge refresh one line-query owner.

Files:

- Create: `src/models/prediction/market_lines.py`
- Create: `src/models/prediction/nba/market_lines.py`
- Create: `src/models/prediction/mlb/market_lines.py`
- Create: `tests/test_prediction_market_lines.py`
- Modify: `src/models/daily_runner.py`
- Modify: `src/models/mlb/mlb_daily_runner.py`
- Later modify: edge refresh jobs after parity tests

TDD tests:

- NBA query output schema matches `_get_current_lines`
- MLB query output schema matches `_get_current_lines`
- bookmaker column preserved
- empty games returns empty DataFrame

DB safety:

- No DDL/index changes.
- No broad `raw_player_props_combined` scans introduced.

---

### Phase 5: Extract slate and participant sources

Objective: Isolate DB/API/CDN slate discovery and participants.

Files:

- Create sport-specific slate/participant modules listed above.
- Create `tests/test_nba_prediction_slate_source.py`
- Create `tests/test_mlb_prediction_slate_source.py`
- Create `tests/test_prediction_participant_sources.py`

Tests:

- NBA API primary / CDN or DB fallback behavior preserved
- injury filter behavior preserved
- MLB cancelled games excluded
- MLB probable starters map home/away opponent IDs correctly
- MLB batter recent-activity and lineup filters preserved

---

### Phase 6: Extract feature batch builders and prediction executors

Objective: Separate feature generation concurrency from prediction calls.

Files:

- Create `src/models/prediction/feature_batch_builder.py`
- Create NBA/MLB prediction executor modules.
- Create focused tests with fake feature stores/predictors.

Tests:

- one failed player feature build does not poison the whole batch
- max worker selection preserves current cap of 8
- NBA combo samples derived on-the-fly
- MLB pitcher/batter sample keys use current `(player_id, int(game_id), stat)` format

---

### Phase 7: Extract feature projection/output shaping

Objective: Move dashboard insight feature mapping to a dedicated owner.

Files:

- Create `src/models/prediction/feature_projection.py`
- Create sport-specific projection modules if needed.
- Create `tests/test_prediction_feature_projection.py`

Tests:

- expected `feat_*` columns preserved
- missing features leave current defaults/nulls
- opponent abbreviation mapping behavior preserved

---

### Phase 8: Shrink facades and add anti-regrowth guards

Objective: Runners become workflow adapters.

Recommended thresholds:

- `src/models/daily_runner.py` under 500 non-comment LOC after NBA extraction.
- `src/models/mlb/mlb_daily_runner.py` under 550 non-comment LOC after MLB extraction.

Guards:

- runners should not contain empirical-CDF implementation inline
- runners should not contain BL policy loops inline
- runners should not contain `should_skip_recommendation` implementation except re-export wrapper
- edge refresh should not replicate daily line query text after market-line extraction

---

## Files likely touched

Shared:

- `src/models/daily_runner.py`
- `src/models/prediction/__init__.py` (new)
- `src/models/prediction/contracts.py` (new)
- `src/models/prediction/devig.py` (new)
- `src/models/prediction/edge_calculator.py` (new)
- `src/models/prediction/recommendation_policy.py` (new)
- `src/models/prediction/bl_recommender.py` (new)
- `src/models/prediction/market_lines.py` (new)
- `src/models/prediction/feature_batch_builder.py` (new)
- `src/models/prediction/feature_projection.py` (new)

NBA-specific:

- `src/models/prediction/nba/slate_source.py` (new)
- `src/models/prediction/nba/participant_source.py` (new)
- `src/models/prediction/nba/market_lines.py` (new)
- `src/models/prediction/nba/prediction_executor.py` (new)
- `src/models/prediction/nba/feature_projection.py` (new)

MLB-specific:

- `src/models/mlb/mlb_daily_runner.py`
- `src/models/prediction/mlb/slate_source.py` (new)
- `src/models/prediction/mlb/participant_source.py` (new)
- `src/models/prediction/mlb/market_lines.py` (new)
- `src/models/prediction/mlb/pitcher_prediction_executor.py` (new)
- `src/models/prediction/mlb/batter_prediction_executor.py` (new)
- `src/models/prediction/mlb/feature_projection.py` (new)

Downstream importers to migrate carefully:

- `src/orchestration/inference_job.py`
- `src/orchestration/mlb_inference_job.py`
- `src/orchestration/run_daily.py`
- `src/orchestration/edge_refresh_job.py`
- `src/orchestration/mlb_edge_refresh_job.py`
- `src/tools/compare_models.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/trading/kalshi/selection_loader.py`

Tests:

- `tests/test_daily_runner.py`
- `tests/test_prediction_devig.py` (new)
- `tests/test_prediction_edge_calculator.py` (new)
- `tests/test_recommendation_policy.py` (new)
- `tests/test_bl_recommender.py` (new)
- `tests/test_prediction_market_lines.py` (new)
- `tests/test_nba_prediction_slate_source.py` (new)
- `tests/test_mlb_prediction_slate_source.py` (new)
- `tests/test_prediction_participant_sources.py` (new)
- `tests/test_prediction_feature_projection.py` (new)
- `tests/test_daily_prediction_runner_inventory.py` (new)
- `tests/test_mlb_daily_runner_inventory.py` (new)

---

## Validation commands

Focused baseline:

`venv/Scripts/python.exe -m pytest tests/test_daily_runner.py -q`

After edge/devig extraction:

`venv/Scripts/python.exe -m pytest tests/test_daily_runner.py tests/test_prediction_devig.py tests/test_prediction_edge_calculator.py -q`

After recommendation extraction:

`venv/Scripts/python.exe -m pytest tests/test_recommendation_policy.py tests/test_daily_runner.py -q`

After BL extraction:

`venv/Scripts/python.exe -m pytest tests/test_bl_recommender.py tests/test_daily_runner.py -q`

After line source extraction:

`venv/Scripts/python.exe -m pytest tests/test_prediction_market_lines.py tests/test_daily_runner.py -q`

Lane-wide:

`venv/Scripts/python.exe -m pytest tests -k "daily_runner or prediction_edge or recommendation_policy or bl_recommender or market_lines" -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/models/daily_runner.py src/models/mlb/mlb_daily_runner.py src/models/prediction/*.py src/models/prediction/nba/*.py src/models/prediction/mlb/*.py`

Diff hygiene:

`git diff --check -- src/models/daily_runner.py src/models/mlb/mlb_daily_runner.py src/models/prediction src/orchestration/edge_refresh_job.py src/orchestration/mlb_edge_refresh_job.py tests .hermes/plans/god-class-migrations/05-daily-prediction-runner-migration.md`

---

## Risk controls / non-goals

Non-goals:

- Do not change edge thresholds.
- Do not change BL configs.
- Do not change allowed directions.
- Do not change line source selection semantics.
- Do not change scheduler/job deployment behavior.
- Do not add stats.nba.com calls to Railway paths.
- Do not persist combo samples to DB.
- Do not replace empirical CDF with Gaussian/analytic approximations.
- Do not combine with feature-store or training-orchestrator migration in one PR.

Hard rules:

- Existing output columns must remain stable for `prediction_store.py` and dashboard consumers.
- Old import path for `should_skip_recommendation` remains until all importers migrate.
- Existing `run_for_date(...)` signatures remain until callers migrate.
- Any live-money behavior change must be isolated, tested, and approved.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. Edge refresh has diverged from live runner line selection.
2. Backtest harness edge logic differs from live runner edge logic.
3. A downstream DB writer depends on incidental column order/null defaults.
4. A paper/Kalshi selection path depends on old recommendation import paths.
5. A sport-specific BL config has hidden allowed-direction policy.
6. A feature projection column is dashboard-critical but undocumented.
7. A caller depends on a private runner method.
8. A source/query path has different temporal semantics than docs claim.
9. A behavior-changing fix is found while extracting; split it into a separate approved lane.
10. A parity guard is needed between old and new line/edge/recommendation paths.

Progress log entries must distinguish: shared primitive created, facade delegates, importer migrated, old duplicate removed, parity verified, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with shared probability primitives only:

1. Create `src/models/prediction/devig.py` and `src/models/prediction/edge_calculator.py`.
2. Add parity tests using fixtures from current `tests/test_daily_runner.py`.
3. Change NBA daily runner `_calculate_edges` to delegate.
4. Change MLB daily runner `_calculate_edges` to delegate only after NBA parity passes.
5. Do not touch slate discovery, line SQL, BL recommendations, edge refresh, or output storage yet.

This targets the highest-risk duplicated invariant, empirical-CDF edge calculation, with the smallest safe code movement.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- AST/method inventory for `src/models/daily_runner.py`.
- AST/method inventory for `src/models/mlb/mlb_daily_runner.py`.
- Targeted reads of NBA `run_for_date`, feature build, edge, and BL methods.
- Targeted reads of MLB slate/player, pitcher/batter prediction, edge, and BL methods.
- `tests/test_daily_runner.py` coverage shape.
- Callsite scan across `src`, `scripts`, and `tests` for daily runner, edge, and recommendation imports.
- GBrain hard facts, critical invariants, calibration guide, and model lessons.

Current status:

- Documentation only.
- No production code changed.
- No daily jobs or predictions run.

---

## Done when

- Daily runners are workflow facades.
- Empirical-CDF edge logic has one shared tested owner.
- Devig, recommendation skip policy, and BL recommendation policy have focused owners.
- Slate/participant/market-line/feature/prediction/output responsibilities are separate.
- Edge refresh and live runners no longer duplicate line/edge/recommendation primitives.
- Production output columns and behavior remain stable unless separate approved behavior changes are made.
