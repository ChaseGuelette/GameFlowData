# MLB Quote-Clean Backtest Sweep Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to run long backtests or DB-heavy audits.

**Goal:** Rebuild the MLB quote-clean/backtest sweep architecture so promotion evidence is owned by explicit, testable components rather than one giant CLI module.

**Architecture:** Preserve current behavior first. Extract quote decision policy, line-source selection, shared prediction caching, edge computation, result serialization, and CLI orchestration behind focused modules with characterization tests. Treat quote-clean temporal integrity and CLV audit semantics as first-class contracts, not incidental flags on `run_mlb_sweep.py`.

**Tech Stack:** Python, pandas, SQLAlchemy, pytest, existing MLB feature stores/model suite, `src/backtesting/mlb/line_selection.py`, GameFlow SQL-runner pattern for any DB validation.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`
- `lessons/implausibly-profitable-backtests-are-methodology-red-flags`
- `lessons/quote-clean-clv-before-feature-work`
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`

Applied lessons:

1. Temporal integrity is end-to-end. Raw `snapshot_time`, `market_last_update`, and `commence_time` fields do not prove the sweep is leak-free.
2. Implausibly profitable quote-clean replays are methodology red flags, not promotion evidence.
3. Quote-clean and CLV gates precede feature work or promotion.
4. Large odds-table audits must be keyed/chunked/index-aware; broad aggregate SQL is not acceptable.
5. Probabilities must use empirical CDF from samples, not Gaussian CDF.
6. Main-context agents must not run Supabase MCP directly for DB truth.

---

## Executive diagnosis

The MLB sweep architecture has become a promotion-critical god module. The current canonical file:

- `src/backtesting/mlb/run_mlb_sweep.py`
  - 1,633 total lines
  - 1,319 non-comment LOC
  - 18 top-level functions
  - `main(...)`: 275 lines

It currently owns too many responsibilities:

- CLI parsing
- sweep-grid construction
- model-dir discovery
- feature-store construction
- date/game discovery
- feature generation
- Monte Carlo prediction caching
- quote decision-time policy
- quote-clean line fetching orchestration
- line-source table selection
- sharpest-line selection
- Black-Litterman edge calculation
- bet simulation handoff
- metric aggregation
- comparison-table printing
- result serialization
- legacy-vs-quote-clean semantics
- dense CLV snapshot line-source support

This is risky because promotion-grade evidence depends on subtle interactions between quote timing, line source, feature timing, model artifacts, edge thresholds, and result serialization. The current module can be modified safely only if those responsibilities are made explicit and characterized first.

---

## Current ownership problems

### 1. CLI orchestration owns domain policy

Current file:

- `src/backtesting/mlb/run_mlb_sweep.py`

Examples:

- `main(...)`, lines 1355-1629, parses CLI flags and constructs feature stores/model suite, then drives all phases.
- Quote-clean flags are interpreted directly in the CLI path:
  - `--quote-clean`
  - `--quote-cutoff-time-et`
  - `--quote-decision-policy`
  - `--quote-relative-minutes`
  - `--line-source`

Why this is wrong:

- CLI parsing should create a typed config, not own promotion semantics.
- It is hard to unit test end-to-end CLI behavior without constructing DB/model dependencies.
- Policy defaults can change accidentally while editing argument parsing.

Target owner:

- New module: `src/backtesting/mlb/sweep_config.py`
- Owns typed config dataclasses and argument-to-config translation.
- CLI remains a thin adapter.

---

### 2. Quote decision policy is embedded in the sweep runner

Current functions:

- `_build_quote_clean_cutoff_ts(...)`, lines 471-487
- `_build_slate_decision_ts(...)`, lines 490-502
- `_game_decision_time(...)`, lines 505-527

Why this is wrong:

- Decision-time policy is promotion-critical and deserves isolated tests.
- Early-game fallback behavior should not live next to model loading and result writing.
- New CLV timing policies will likely expand beyond a single helper.

Target owner:

- New module: `src/backtesting/mlb/quote_decision_policy.py`
- Owns:
  - `QuoteDecisionPolicy` enum/constants
  - `build_fixed_cutoff_ts(...)`
  - `build_slate_or_tminus_decision_ts(...)`
  - `decision_time_for_game(...)`
  - validation for unsupported/unsafe policies

Characterization tests:

- Existing `tests/test_mlb_quote_clean_line_selection.py::test_slate_or_tminus_policy_uses_slate_and_fallback_for_early_games`
- Add focused tests for:
  - fixed ET cutoff
  - skip-early fixed ET returns `None` when cutoff >= commence
  - relative-to-commence
  - slate fallback for early games
  - timezone-aware ET output

---

### 3. Line-source selection and quote-clean fetch orchestration are split awkwardly

Current files:

- `src/backtesting/mlb/line_selection.py`
- `src/backtesting/mlb/run_mlb_sweep.py::_fetch_lines_for_date(...)`

Current `line_selection.py` is already a good seam. It owns same-book, same-snapshot line fetching and source-table handling.

Current problem:

- `_fetch_lines_for_date(...)` still owns date/game policy orchestration and backward-compatible `game_ids=` handling.
- `STAT_TO_MARKET_KEY` and excluded-bookmaker mappings exist both in sweep code and line selection.
- Decision-time metadata columns are added in the runner after line selection.

Target owner:

- Keep and extend: `src/backtesting/mlb/line_selection.py`
- New or extended service: `src/backtesting/mlb/quote_clean_line_service.py`

Responsibilities:

- Given games + stat keys + quote policy + line source, return selected lines with:
  - `selected_snapshot_time`
  - `selected_decision_time`
  - `quote_decision_policy`
  - source table metadata
- Delegate raw SQL fetch to `fetch_lines_at_decision_time(...)`.
- Preserve legacy `allow_latest_without_as_of=True` only as an explicit legacy/backfill escape hatch.

Expansion checkpoint:

- If dense CLV source handling expands, do not add more flags to `run_mlb_sweep.py`. Add source-specific adapter/config in the service.

---

### 4. Shared prediction cache mixes feature stores, models, and actuals

Current functions:

- `run_shared_phases(...)`, lines 187-298
- `_process_date_shared(...)`, lines 301-468
- `precompute_mlb_base_probs(...)`, lines 766-850

Why this is wrong:

- Feature-store calls, model suite prediction, actual-stat lookup, and line fetching are interleaved.
- Test setup for any one part requires patching many unrelated dependencies.
- It is not obvious which outputs are reusable across sweep configs and which depend on config.

Target owners:

- `src/backtesting/mlb/prediction_cache.py`
  - `DatePrediction`
  - `PredictionCacheBuilder`
  - model-suite prediction loop
- `src/backtesting/mlb/backtest_data_loader.py`
  - game date discovery
  - games for date
  - actuals loading
  - feature store input boundaries
- `src/backtesting/mlb/base_probability_cache.py`
  - precomputed base probabilities per line/stat/player/game

Non-goal:

- Do not rewrite `MLBFeatureStore`/`MLBBatterFeatureStore` in this lane. That is migration doc #02.

---

### 5. Edge computation owns both math and row assembly

Current functions:

- `_odds_to_prob(...)`, lines 601-605
- `_select_sharpest_line(...)`, lines 608-649
- `compute_edges_for_config(...)`, lines 652-759

Why this is wrong:

- Probability math, BL blending, line selection, direction filtering, and output row shape are coupled.
- It is easy to accidentally change promotion semantics while changing output columns.

Target owner:

- `src/backtesting/mlb/edge_engine.py`

Responsibilities:

- `odds_to_prob(...)`
- `select_lowest_vig_line(...)` or move to line service
- empirical CDF probability computation
- BL blending invocation
- edge/direction selection
- typed `EdgeCandidate` / `BacktestBetCandidate` output

Tests must assert:

- empirical CDF is used for samples: `(samples > line).mean()` for over and complement/under logic as currently implemented
- no Gaussian CDF import/use
- no excluded bookmaker line enters edge selection
- direction filters are preserved
- BL disabled (`tau=None`) behavior is unchanged

---

### 6. Result saving and comparison reporting are embedded in the runner

Current functions:

- `print_comparison_table(...)`, lines 1143-1226
- `save_results(...)`, lines 1229-1315

Why this is wrong:

- Result contract is promotion evidence. It should be stable and testable.
- Output schema changes should be intentional.
- A future audit suite should not parse incidental print output.

Target owner:

- `src/backtesting/mlb/sweep_results.py`

Responsibilities:

- serialize summary CSV
- serialize per-config bets/predictions
- save run metadata/config
- provide a stable result schema version
- print human comparison table as adapter only

Expansion checkpoint:

- If audit-suite output needs new metadata, add explicit versioned fields rather than ad hoc columns.

---

### 7. Legacy vs quote-clean mode is not a strong enough contract

Current behavior:

- Without `--quote-clean`, legacy optimistic aggregation is allowed and warns.
- With `--quote-clean`, as-of decision-time line selection is used.

Required target behavior:

- Legacy mode remains available only as hypothesis-generating/backward-compatible mode.
- Promotion-grade mode must require:
  - quote-clean enabled
  - non-null decision policy
  - line source recorded
  - pre-commence guards
  - output metadata identifying quote policy/source

Target owner:

- `src/backtesting/mlb/promotion_contracts.py`

Responsibilities:

- define `PromotionGradeBacktestContract`
- validate config before promotion-grade run
- emit warnings/errors for legacy mode
- centralize language used by CLI and audit suite

Non-goal:

- Do not make legacy mode impossible in the first migration. First preserve behavior; then optionally gate promotion-grade reporting harder in a behavior-changing follow-up.

---

## Target design by responsibility

### A. `sweep_config.py`

Owns:

- `SweepConfig`
- `SweepResult` if not moved to result module
- `SweepRunConfig`
- `QuoteCleanConfig`
- argument parsing helpers that can be tested without DB/model imports

Validation:

- `tests/test_mlb_sweep_config.py`

---

### B. `quote_decision_policy.py`

Owns:

- quote decision policy names
- fixed/slate/relative decision timestamp functions
- policy validation

Validation:

- `tests/test_mlb_quote_decision_policy.py`

---

### C. `quote_clean_line_service.py`

Owns:

- per-date/per-game quote-clean line fetching orchestration
- line source routing
- decision-time metadata on returned rows

Uses:

- `src/backtesting/mlb/line_selection.py::fetch_lines_at_decision_time`

Validation:

- `tests/test_mlb_quote_clean_line_service.py`

---

### D. `backtest_data_loader.py`

Owns:

- game date discovery
- games for date
- actuals lookup
- feature-store boundary calls

Validation:

- Unit tests with fake engine/feature stores.
- No DB-heavy integration tests in the migration slice.

---

### E. `prediction_cache.py`

Owns:

- `DatePrediction`
- per-date prediction generation
- feature-store/model-suite interaction boundary

Validation:

- fake feature stores + fake model suite
- verifies stats routing between pitcher and batter stores

---

### F. `edge_engine.py`

Owns:

- odds conversion
- lowest-vig/same-book candidate selection if not fully owned by line service
- empirical probability calculation
- BL/no-BL edge computation
- direction filtering

Validation:

- pure tests only, no DB.

---

### G. `sweep_runner.py`

Owns:

- phase sequencing across services
- no CLI parsing
- no raw SQL
- no print formatting

Validation:

- integration-style unit test with fakes verifying call order and config propagation.

---

### H. `sweep_results.py`

Owns:

- result schema
- file writes
- summary table generation
- metadata/versioning

Validation:

- temp-dir tests verifying files and required columns.

---

### I. `promotion_contracts.py`

Owns:

- promotion-grade validation rules
- legacy-mode warning text
- audit-suite compatibility hooks

Validation:

- tests for legacy allowed-but-warning vs promotion-grade required settings.

---

### J. Thin CLI adapter

Final state for:

- `src/backtesting/mlb/run_mlb_sweep.py`

Owns only:

- parse args
- build config
- call `SweepRunner`
- exit code/logging

Removal guard:

- Add an inventory test ensuring `run_mlb_sweep.py` stays below a chosen threshold after migration, e.g. fewer than 450 non-comment LOC and no raw SQL strings.

---

## Refactor phases

### Phase 0: Safety baseline and inventory

Objective: Make current behavior observable before extraction.

Tasks:

1. Add `tests/test_mlb_sweep_inventory.py`.
2. Assert current module responsibilities are visible:
   - `run_mlb_sweep.py` contains quote decision helpers
   - `run_mlb_sweep.py` contains `main`
   - `line_selection.py` is used by the sweep path
3. Add no production code in this phase.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_inventory.py tests/test_mlb_quote_clean_line_selection.py -q`

Expected: passes before extraction.

Expansion checkpoint:

- If inventory uncovers another production entrypoint using private helpers, add it to the touched-files list before extraction.

---

### Phase 1: Extract quote decision policy

Objective: Move decision-time policy out of `run_mlb_sweep.py` with no behavior change.

Files:

- Create: `src/backtesting/mlb/quote_decision_policy.py`
- Create: `tests/test_mlb_quote_decision_policy.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`
- Modify: `tests/test_mlb_quote_clean_line_selection.py`

TDD steps:

1. Write tests for existing `_game_decision_time(...)` behavior against new wished-for API.
2. Verify RED because module/function does not exist.
3. Move/copy the current helper behavior into the new module.
4. Rewire `run_mlb_sweep.py` imports.
5. Keep a temporary compatibility wrapper only if needed by existing tests; remove it in a later phase.

Focused validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_quote_decision_policy.py tests/test_mlb_quote_clean_line_selection.py -q`

---

### Phase 2: Extract sweep config and CLI parsing

Objective: Make CLI parsing produce a typed config without initializing DB/model dependencies.

Files:

- Create: `src/backtesting/mlb/sweep_config.py`
- Create: `tests/test_mlb_sweep_config.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

TDD tests:

- tau parsing preserves `none` as `None`
- flat-bet value propagates to all config points
- quote-clean config records policy/time/line source
- combined mode config is explicit
- invalid line source is rejected before DB work

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_config.py -q`

Expansion checkpoint:

- If current CLI defaults are unclear or unsafe, do not change them in this phase. Add a `promotion_contracts.py` follow-up instead.

---

### Phase 3: Extract quote-clean line service

Objective: Move `_fetch_lines_for_date(...)` orchestration into a service while preserving current SQL helper behavior.

Files:

- Create: `src/backtesting/mlb/quote_clean_line_service.py`
- Create: `tests/test_mlb_quote_clean_line_service.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`
- Keep: `src/backtesting/mlb/line_selection.py`

TDD tests:

- fixed ET policy calls `fetch_lines_at_decision_time(...)` once with all game ids
- per-game policy calls the fetch helper once per game
- `skip_early_fixed_et` omits games whose decision time is after commence
- dense CLV source table is passed through
- returned rows include `selected_decision_time` and `quote_decision_policy`
- backward-compatible `game_ids=` path remains covered or is removed only after callsites are migrated

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_quote_clean_line_service.py tests/test_mlb_quote_clean_line_selection.py -q`

---

### Phase 4: Extract data loader and prediction cache

Objective: Isolate date/game/actual loading from prediction generation.

Files:

- Create: `src/backtesting/mlb/backtest_data_loader.py`
- Create: `src/backtesting/mlb/prediction_cache.py`
- Create: `tests/test_mlb_backtest_data_loader.py`
- Create: `tests/test_mlb_prediction_cache.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

TDD tests:

- loader returns sorted non-cancelled game dates
- loader preserves stat-to-market mapping boundaries
- prediction cache routes pitcher stats to `MLBFeatureStore`
- prediction cache routes batter stats to `MLBBatterFeatureStore`
- missing batter feature store for batter stats fails clearly
- actuals are keyed by `(player_id, stat)` as current runner expects

Safety:

- Use fake engines/feature stores. Do not run remote DB queries in unit tests.

---

### Phase 5: Extract edge engine

Objective: Make edge computation pure/testable enough to catch probability and line-selection regressions.

Files:

- Create: `src/backtesting/mlb/edge_engine.py`
- Create: `tests/test_mlb_edge_engine.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

TDD tests:

- empirical over probability uses sample comparison to line
- no-BL config preserves model probability
- BL config invokes `BlackLittermanBlender` with same parameters as current code
- lowest-vig line is selected consistently
- direction filter removes disallowed side
- output candidate schema includes enough metadata for audit suite

Invariant test:

- Add a test or source scan guard that `edge_engine.py` does not import `scipy.stats.norm`.

---

### Phase 6: Extract result serialization

Objective: Make output schema stable and testable.

Files:

- Create: `src/backtesting/mlb/sweep_results.py`
- Create: `tests/test_mlb_sweep_results.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

TDD tests:

- summary CSV has required columns
- per-config bet CSVs are written
- metadata includes quote policy, line source, model path, stats, and config grid
- comparison output can be generated without writing files

Expansion checkpoint:

- If audit-suite needs additional fields, add explicit schema version rather than invisible ad hoc columns.

---

### Phase 7: Introduce sweep runner service

Objective: Replace the giant procedural runner with a `SweepRunner` that composes services.

Files:

- Create: `src/backtesting/mlb/sweep_runner.py`
- Create: `tests/test_mlb_sweep_runner.py`
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`

TDD tests:

- runner calls shared prediction/data phases once
- runner runs each sweep config against cached predictions
- combined mode path remains preserved
- output writer is invoked exactly once with expected metadata

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_runner.py tests/test_mlb_sweep_config.py tests/test_mlb_quote_clean_line_service.py tests/test_mlb_edge_engine.py -q`

---

### Phase 8: Add promotion contract guardrails

Objective: Separate behavior-preserving migration from promotion-grade interpretation.

Files:

- Create: `src/backtesting/mlb/promotion_contracts.py`
- Create: `tests/test_mlb_promotion_contracts.py`
- Modify: CLI/reporting only if needed

TDD tests:

- legacy mode is allowed but labeled hypothesis-only
- promotion-grade mode requires quote-clean config
- promotion-grade mode records line source and decision policy
- dense CLV line source requires explicit linked-coverage/audit note in metadata

Non-goal:

- Do not block existing scripts by default unless Chase approves the behavior change.

---

### Phase 9: Thin `run_mlb_sweep.py` and add removal/inventory guards

Objective: Prevent re-growth of the god module.

Files:

- Modify: `src/backtesting/mlb/run_mlb_sweep.py`
- Modify: `tests/test_mlb_sweep_inventory.py`

TDD/inventory assertions:

- `run_mlb_sweep.py` has no raw SQL strings
- `run_mlb_sweep.py` has no `pd.read_sql`
- `run_mlb_sweep.py` non-comment LOC below agreed threshold, recommended initial threshold 450
- quote decision helper functions are no longer defined in runner
- result writing functions are no longer defined in runner

Validation:

`venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_inventory.py -q`

---

## Files likely touched

Core migration files:

- `src/backtesting/mlb/run_mlb_sweep.py`
- `src/backtesting/mlb/line_selection.py`
- `src/backtesting/mlb/quote_decision_policy.py` (new)
- `src/backtesting/mlb/sweep_config.py` (new)
- `src/backtesting/mlb/quote_clean_line_service.py` (new)
- `src/backtesting/mlb/backtest_data_loader.py` (new)
- `src/backtesting/mlb/prediction_cache.py` (new)
- `src/backtesting/mlb/edge_engine.py` (new)
- `src/backtesting/mlb/sweep_results.py` (new)
- `src/backtesting/mlb/sweep_runner.py` (new)
- `src/backtesting/mlb/promotion_contracts.py` (new)

Likely tests:

- `tests/test_mlb_sweep_inventory.py` (new)
- `tests/test_mlb_quote_decision_policy.py` (new)
- `tests/test_mlb_sweep_config.py` (new)
- `tests/test_mlb_quote_clean_line_service.py` (new)
- `tests/test_mlb_backtest_data_loader.py` (new)
- `tests/test_mlb_prediction_cache.py` (new)
- `tests/test_mlb_edge_engine.py` (new)
- `tests/test_mlb_sweep_results.py` (new)
- `tests/test_mlb_sweep_runner.py` (new)
- `tests/test_mlb_promotion_contracts.py` (new)
- `tests/test_mlb_quote_clean_line_selection.py` (existing)
- `tests/test_mlb_run_mlb_sweep_flat.py` (existing)
- `tests/test_mlb_feature_store_as_of.py` (existing compatibility risk)
- `tests/test_run_mlb_quote_clean_audit_suite.py` (existing audit compatibility risk)

Files to avoid changing in this lane unless absolutely necessary:

- `src/models/mlb/mlb_feature_store.py`
- `src/models/mlb/mlb_batter_feature_store.py`
- training pipelines
- scraper/linker code
- database migrations

---

## Validation commands

Focused early-phase commands:

`venv/Scripts/python.exe -m pytest tests/test_mlb_quote_clean_line_selection.py tests/test_mlb_run_mlb_sweep_flat.py -q`

After extracting quote decision and line service:

`venv/Scripts/python.exe -m pytest tests/test_mlb_quote_decision_policy.py tests/test_mlb_quote_clean_line_service.py tests/test_mlb_quote_clean_line_selection.py -q`

After extracting edge/config/results:

`venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_config.py tests/test_mlb_edge_engine.py tests/test_mlb_sweep_results.py -q`

Before committing a slice:

`venv/Scripts/python.exe -m py_compile src/backtesting/mlb/run_mlb_sweep.py src/backtesting/mlb/line_selection.py`

Once new modules exist, extend compile command:

`venv/Scripts/python.exe -m py_compile src/backtesting/mlb/quote_decision_policy.py src/backtesting/mlb/sweep_config.py src/backtesting/mlb/quote_clean_line_service.py src/backtesting/mlb/backtest_data_loader.py src/backtesting/mlb/prediction_cache.py src/backtesting/mlb/edge_engine.py src/backtesting/mlb/sweep_results.py src/backtesting/mlb/sweep_runner.py src/backtesting/mlb/promotion_contracts.py`

Lane-wide regression:

`venv/Scripts/python.exe -m pytest tests -k "mlb and (sweep or quote_clean or line_selection or backtest or feature_store_as_of)" -q`

Full suite before final commit if this touches shared backtesting behavior:

`venv/Scripts/python.exe -m pytest tests -q`

Diff hygiene:

`git diff --check -- src/backtesting/mlb tests .hermes/plans/god-class-migrations/01-mlb-quote-clean-backtest-sweep-migration.md`

---

## Risk controls / non-goals

Non-goals for this migration:

- Do not run long MLB backtest sweeps automatically.
- Do not perform remote DB-heavy audits in main context.
- Do not change model promotion status.
- Do not change BL defaults, edge thresholds, Kelly sizing, or line-source defaults in behavior-preserving phases.
- Do not rewrite MLB feature stores here; that is migration #02.
- Do not use legacy-mode results as promotion evidence.

Hard safety rules:

- Any DB validation uses GameFlow SQL-runner/delegated pattern.
- Large odds table queries must be keyed/chunked/index-aware.
- If a query times out, treat it as an audit-design failure, not data evidence.
- Keep unrelated MLB CLV/scraper/linker dirty work out of this migration commit.

---

## Expansion checkpoints learned from Kalshi

The Kalshi plan expanded because each extraction exposed another implicit responsibility. Expect the same here.

Trigger a new named sub-slice if you discover:

1. A helper is used by another production entrypoint.
2. Legacy mode and quote-clean mode disagree on output schema.
3. Audit suite parses output files that would change shape.
4. `mlb_backtest_harness.py` relies on private sweep functions.
5. Dense CLV snapshot source needs coverage metadata beyond line-source flag.
6. A test requires remote DB to verify behavior; stop and create a fake/injected seam instead.
7. Feature-store temporal behavior must change; stop and route to migration #02.
8. A behavior-changing promotion gate seems necessary; add it to `promotion_contracts.py` as disabled/warning-only first unless Chase approves enforcement.

Progress log entries must distinguish:

- module exists and has tests
- runner calls module directly
- old helper remains as compatibility wrapper
- old helper removed
- behavior-changing hardening deferred

---

## First implementation PR recommendation

Start with a no-behavior-change PR:

1. Add `quote_decision_policy.py`.
2. Move decision-time helpers out of `run_mlb_sweep.py`.
3. Rewire existing tests.
4. Add inventory guard showing where responsibilities still remain.
5. Do not touch DB SQL, edge computation, result serialization, or feature stores.

Why first:

- Smallest high-value seam.
- Pure functions; easy RED/GREEN.
- Reduces risk before touching line selection or cached prediction paths.

Expected commit shape:

- `src/backtesting/mlb/quote_decision_policy.py`
- `tests/test_mlb_quote_decision_policy.py`
- small import rewiring in `src/backtesting/mlb/run_mlb_sweep.py`
- test update in `tests/test_mlb_quote_clean_line_selection.py`
- progress-log update in this plan

---

## Progress log

### 2026-05-23 slice 01A started — quote decision policy extraction

Files changed:

- Created `src/backtesting/mlb/quote_decision_policy.py`.
- Created `tests/test_mlb_quote_decision_policy.py`.
- Modified `src/backtesting/mlb/run_mlb_sweep.py` so private quote-decision helpers are compatibility wrappers around the new module.
- Modified `tests/test_mlb_quote_clean_line_selection.py` to import the new `decision_time_for_game(...)` seam directly.
- Created companion research log `01-mlb-quote-clean-backtest-sweep-research-log.html` for working decisions/findings that should not bloat this main plan.

RED result:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_quote_decision_policy.py -q` failed with `ModuleNotFoundError: No module named 'src.backtesting.mlb.quote_decision_policy'`, as expected before creating the new module.

GREEN result:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_quote_decision_policy.py tests/test_mlb_quote_clean_line_selection.py tests/test_mlb_run_mlb_sweep_flat.py -q` passed: 15 passed, 1 pytest-asyncio deprecation warning.
- `venv/Scripts/python.exe -m py_compile src/backtesting/mlb/run_mlb_sweep.py src/backtesting/mlb/quote_decision_policy.py` passed.

Behavior-preservation notes:

- Unknown quote-decision policy still falls back to the fixed cutoff, matching the previous runner behavior. Hard validation is deferred to the later config/promotion-contract phase because it would be behavior-changing.
- Temporary compatibility wrappers remain in `run_mlb_sweep.py` for `_build_quote_clean_cutoff_ts`, `_build_slate_decision_ts`, and `_game_decision_time` so any hidden private imports keep working during this first slice.
- No DB queries, long backtests, feature-store changes, edge math changes, BL default changes, or result schema changes were introduced.

Expansion checkpoint status:

- No additional production entrypoint was discovered during this slice.
- Inventory/removal guard is now started in `tests/test_mlb_sweep_inventory.py`; final thin-runner threshold remains `xfail` until later extraction phases make it realistic.

### 2026-05-23 slice 02A — typed sweep config and CLI parser extraction

Files changed:

- Created `src/backtesting/mlb/sweep_config.py`.
- Created `tests/test_mlb_sweep_config.py`.
- Modified `src/backtesting/mlb/run_mlb_sweep.py` to delegate parser construction, tau parsing, sweep-grid construction, date parsing, quote-clean config capture, output-dir parsing, and CLI direction filter construction to the new module.

RED result:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_config.py -q` failed with `ModuleNotFoundError: No module named 'src.backtesting.mlb.sweep_config'`, as expected before creating the new module.

GREEN result:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_config.py tests/test_mlb_sweep_inventory.py tests/test_mlb_quote_decision_policy.py tests/test_mlb_quote_clean_line_selection.py tests/test_mlb_run_mlb_sweep_flat.py -q` passed: 25 passed, 1 xfailed, 1 pytest-asyncio deprecation warning.
- `venv/Scripts/python.exe -m py_compile src/backtesting/mlb/run_mlb_sweep.py src/backtesting/mlb/quote_decision_policy.py src/backtesting/mlb/sweep_config.py` passed.

Behavior-preservation notes:

- Parser flags, defaults, and argparse `choices` are preserved in `build_arg_parser()`.
- `SweepConfig` and `build_sweep_grid` are imported back into `run_mlb_sweep.py` so existing private imports like `from run_mlb_sweep import SweepConfig` keep working during migration.
- `parse_sweep_cli_config(...)` is parse-only; it does not construct engines, feature stores, model suites, or touch DB/model state.
- `run_mlb_sweep.py` still uses `args` for downstream orchestration fields in this slice; the next runner/config slice can replace those references with the typed config once behavior is fully characterized.

Expansion checkpoint status:

- No behavior-changing validation was added beyond existing argparse `choices`; promotion-grade enforcement remains deferred to `promotion_contracts.py`.

### 2026-05-23 slice 01B — structural inventory harness

Files changed:

- Created `tests/test_mlb_sweep_inventory.py`.

Harness coverage:

- Verifies `quote_decision_policy.py` owns the expected public helper functions.
- Verifies runner quote-decision helper names are only compatibility wrappers and do not re-own `pd.to_datetime`, `ZoneInfo`, or `datetime_time` implementation details.
- Verifies `run_mlb_sweep.py` no longer imports quote-policy implementation dependencies directly.
- Verifies the sweep path still references the shared `line_selection.fetch_lines_at_decision_time` seam.
- Adds an `xfail` final-shape guard for the eventual thin-runner target: non-comment LOC below 450 and no raw SQL/`pd.read_sql`/`sqlalchemy.text` ownership in `run_mlb_sweep.py`.

Validation:

- `venv/Scripts/python.exe -m pytest tests/test_mlb_sweep_inventory.py tests/test_mlb_quote_decision_policy.py tests/test_mlb_quote_clean_line_selection.py tests/test_mlb_run_mlb_sweep_flat.py -q` passed: 19 passed, 1 xfailed, 1 pytest-asyncio deprecation warning.
- `venv/Scripts/python.exe -m py_compile src/backtesting/mlb/run_mlb_sweep.py src/backtesting/mlb/quote_decision_policy.py` passed.

Behavior-preservation notes:

- Test-only slice; no production code changed.
- The final shape guard is intentionally marked `xfail` because the migration is not done yet. It documents the destination without blocking current work.

### 2026-05-19 initial migration documentation

Created this plan from a bounded code/brain deep dive.

Evidence inspected:

- Kalshi migration plan outline and progress-log growth pattern.
- `handoffs/handoff-080` markdown handoff.
- `src/backtesting/mlb/run_mlb_sweep.py` structure and key helpers.
- `src/backtesting/mlb/line_selection.py` shared quote-clean SQL helper.
- `tests/test_mlb_quote_clean_line_selection.py` existing quote-clean regression coverage.
- GBrain hard facts, critical invariants, and MLB quote-clean/backtest lessons.

Current status:

- Documentation only.
- No production code changed.
- No tests run for this doc beyond file write/lint skip.

---

## Done when

- `run_mlb_sweep.py` is a thin CLI adapter around typed config + runner service.
- Quote decision policy is isolated and tested.
- Quote-clean line service owns decision-time/source-table orchestration.
- Prediction cache, data loading, edge computation, result writing, and promotion contracts have explicit owners.
- Existing quote-clean/as-of tests still pass.
- New inventory tests prevent `run_mlb_sweep.py` from regrowing raw SQL and domain policy.
- Legacy mode remains clearly labeled hypothesis-only.
- Promotion-grade runs record quote decision policy, line source, and temporal-integrity metadata.
