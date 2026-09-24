# NBA Temporal-Integrity Audit — 2026-09-24

**Lane:** A4 temporal integrity  
**Audit time:** 2026-09-24T11:44:31-04:00  
**Repository commit inspected:** `0c5b02f`  
**Mode:** read-only code/path audit; no database writes, backfills, training, deployments, or indexes  
**Verdict:** **FAIL — NBA outputs and historical backtests are not decision-time reproducible, and multiple active quote paths can select post-decision or post-commence rows.** Rolling-stat construction itself is prior-game and season-scoped, but that does not offset the quote and injury leakage risks.

## Scope and method

Traced the active NBA paths:

- training: `src/models/train_pipeline.py` → `FeatureStore` → `src/models/features/nba/training_loader.py`;
- rolling derivation: `src/processing/populate_average_stats.py` and opponent-allowed backfills;
- historical/date-range features: `src/models/features/nba/date_range_loader.py`;
- backtesting: `src/backtesting/run_backtest.py` → `src/backtesting/backtest_harness.py` → `src/backtesting/bet_simulator.py`;
- production inference: `src/orchestration/inference_job.py` → `src/models/daily_runner.py` → NBA feature source modules;
- edge refresh: `src/orchestration/edge_refresh_job.py`;
- persistence: `src/models/prediction_store.py`.

No SQL was required to establish the primary failures: the downstream predicates themselves do not enforce the necessary cutoffs. Raw timestamp coverage would not repair those code-path failures.

## Compact adjudication

| Requirement | Verdict | Evidence |
|---|---|---|
| Prior-game rolling behavior | **PASS** | `populate_average_stats.py:213-242, 287-330`; grouped by player/team and season, with `shift(1)`. Focused tests passed. |
| Stats/features available before target | **PARTIAL** | Inference rolling sources use strict `< as_of_date`, but training loaders use `<=` and rely on the derived-table contract; line-context features are not decision-time safe. |
| Injury/lineup as-of semantics | **FAIL / lineup not used** | Injury rows are chosen by date and latest ID, not by an observed/scraped timestamp at or before a decision. No NBA lineup feature path was found. |
| Quote at/before decision and pre-commence | **FAIL** | Production, edge-refresh, backtest, and model line-feature paths lack one or both required predicates. |
| No latest-row/post-decision leakage | **FAIL** | Active queries rank latest rows without a decision cutoff; edge refresh also lacks a commence guard. |
| Game/player joins scoped to slate/season | **MOSTLY PASS** | Core joins use `game_id + player_id/team_id`; rolling construction includes `season_id`. One scheduled-inference season ID is hard-coded stale. |
| Saved output reproducibility | **FAIL** | NBA prediction/backtest rows do not preserve selected quote timestamps or decision time; prediction upserts overwrite prior states. |

# Findings

## P0 — Blockers

### P0-1. Production inference can select the latest quote without a decision-time or pre-commence guard

`DailyPredictionRunner._get_current_lines()` ranks all matching rows by `snapshot_time DESC` and applies no `snapshot_time <= decision_time` and no `snapshot_time < commence_time` predicate (`src/models/daily_runner.py:783-846`). The query then selects the lowest-vig book after latest-row ranking (`:854-874`).

Consequences:

- a delayed/rerun inference can consume quotes recorded after the intended decision time;
- if rows continue arriving after tipoff, current edges can be computed from in-game/post-commence quotes;
- the result does not retain the selected snapshot timestamp, so the decision cannot be replayed.

The inference job logs a run start time (`src/orchestration/inference_job.py:103-110`) but does not pass that timestamp into feature or quote selection. It stores only the target date (`:228-241`).

**Classification:** P0 because it directly affects customer-facing line, probability, edge, and recommendation state.

### P0-2. Edge refresh explicitly uses latest recent rows but does not enforce decision-time or commence-time validity

`fetch_fresh_lines()` filters only to `snapshot_time > now() - interval '24 hours'`, then orders descending (`src/orchestration/edge_refresh_job.py:95-150`). It has no `snapshot_time <= refresh_started_at`, no `snapshot_time < commence_time`, and does not return quote timestamps (`:131-143`).

`recalculate_edges()` merges only player/game/stat/line/odds/bookmaker and overwrites recommendation fields (`src/orchestration/edge_refresh_job.py:514-669`). The main job runs this path and upserts the results (`:1134-1181`), including after selectively rebuilding features through the same date-only quote-feature path (`:402-424`).

This permits post-commence quote selection and destroys evidence of which snapshot caused an edge/recommendation change.

**Classification:** P0 because the scheduled refresh can mutate displayed/paper-trading decisions using temporally invalid prices.

### P0-3. NBA backtests are not quote-clean and can include post-commence quotes

The historical harness uses a fixed cutoff of `game_date + 23:30` and ranks latest rows before that cutoff (`src/backtesting/backtest_harness.py:475-537`; range-prefetch equivalent `:567-605`). There is no `snapshot_time < commence_time` predicate. Early games can therefore use quotes after tipoff. The fixed UTC cutoff also does not encode an explicit ET/DST decision policy, despite the comment describing a 6:30 PM ET production run (`:482-485`).

The backtest query does not return selected quote timestamps or commence time. Consequently, the downstream simulator cannot prove quote validity even though its data model supports optional timestamp fields (`src/backtesting/bet_simulator.py:69-77, 247-305, 531-540`). Searches found no NBA harness population of `selected_snapshot_time` or `selected_decision_time`.

**Classification:** P0 because historical ROI/calibration/betting claims from this harness are not promotion-grade temporal evidence.

### P0-4. Model line-context features can use post-decision information in both training and inference

Line context is in the active model feature contracts: `line_spread`, `line_total`, and per-stat `prop_line_*` (`src/models/features/nba/contracts.py:12-18, 59-64, 98-103, 137-142, 179-184`).

Training problems:

- game-line features aggregate `MAX()` over all matching rows with **no timestamp predicate at all** in the primary season training loader (`src/models/features/nba/training_loader.py:371-379`);
- prop-line features allow any row on the game date before commence, then take the latest per market (`:381-398`), not the latest at/before a declared training decision time;
- game-line `MAX()` can synthesize spread/total values from different snapshots/books instead of one executable decision snapshot.

Inference problems:

- `InferenceFeatureLoader.load()` calls `_get_game_lines()` and `_get_player_prop_lines()` without passing `request.as_of_date` (`src/models/features/nba/inference_loader.py:29-46`), so the helpers receive `NULL` and disable even their date predicate;
- the helper predicates are date-only when enabled, not timestamp-based (`src/models/features/nba/line_sources.py:30-68`).

Therefore model inputs and edge prices can be drawn from different, later quote states, and historical model training can learn from information unavailable at the simulated decision.

**Classification:** P0 because the leakage is inside feature generation, so fixing only edge-price selection would not make existing model/backtest evidence clean.

### P0-5. Historical injury features have no decision-time/as-of timestamp semantics

Injury feature queries constrain `report_date = game_date` and choose latest rows by `id DESC`, but do not constrain an injury row's observation/scrape/insert timestamp to a decision time (`src/models/features/nba/injury_context.py:85-200, 203-259`). The training loader merges those daily latest statuses into historical game rows (`src/models/features/nba/training_loader.py:44-149`). The date-range loader repeats latest-ID same-day logic (`src/models/features/nba/date_range_loader.py:243-310`).

Production filtering similarly picks the latest report date in the preceding seven-day window (`src/models/daily_runner.py:697-777`), and edge refresh repeats the same logic after a fresh injury scrape (`src/orchestration/edge_refresh_job.py:672-775, 1120-1128`). Runtime use of currently available data is reasonable, but the selected injury version and decision timestamp are not persisted, and historical training/backtests can consume a status learned after the intended decision.

No active NBA lineup feature/query path was found under `src/models/features/nba/`; lineup temporal integrity is therefore **not applicable to current model features**, not proven for any separate product surface.

**Classification:** P0 for historical training/backtest integrity; production state is additionally non-reproducible.

## P1 — High-priority integrity and reproducibility gaps

### P1-1. Saved NBA prediction state omits quote/decision/artifact identity and is overwritten in place

`PredictionStore.PREDICTION_COLS` stores target date, player/game/stat, line/odds/bookmaker, predictions, features, and edges, but no selected quote snapshot time, market update time, decision time, model artifact ID, feature as-of timestamp, or refresh-run ID (`src/models/prediction_store.py:19-48`).

The conflict key is `(prediction_date, player_id, game_id, stat)` and all other columns are updated in place (`:92-101`). Edge refresh calls the same upsert (`src/orchestration/edge_refresh_job.py:1177-1181`). Thus later refreshes erase the exact prior customer-visible decision state.

MC samples retain a `created_at` refreshed on conflict (`src/models/prediction_store.py:138-146`) but that timestamp identifies the latest sample write only; it does not identify the quote or feature decision state.

### P1-2. Backtest outputs omit model artifact identity and temporal policy

`BacktestResult.to_csv()` writes predictions, bets, optional all-book edges, and `metrics.json` (`src/backtesting/backtest_harness.py:31-59`). Its config records betting parameters only (`:268-285`): it omits model directory/artifact hash, run timestamp, quote decision policy/timezone, selected snapshot IDs/timestamps, and data-cutoff identity.

`run_backtest.py` resolves a model path and logs it (`src/backtesting/run_backtest.py:160-162`) but does not add it to the saved result config (`:219-245`). Directory timestamps identify execution folders, not the complete replay contract.

### P1-3. Scheduled inference hard-codes stale season identity

For scheduled context, `InferenceFeatureLoader._load_context()` returns `season_id: "22025"` (`src/models/features/nba/inference_loader.py:67-82`). This is not correct for the 2026–27 season. The current feature contracts do not appear to model directly on `season_id`, so this is not proven target leakage, but it is a stale identifier and weakens season/slate reproducibility.

### P1-4. Expected-player selection is not upper-bounded by the target date

`DailyPredictionRunner._get_players_for_games()` selects the latest player-average row for a team with `game_date >= target_date - 30 days` but no `game_date < target_date` / `<= target_date` upper bound (`src/models/daily_runner.py:635-672`). For ordinary same-day production, future rows should not exist. For reruns or historical/future-date shadow checks, later rows can determine team membership and eligibility for an earlier slate.

The final mapping to a slate is otherwise explicit by scheduled team → game/opponent (`:674-695`).

## P2 — Lower-priority gaps and test coverage

### P2-1. Temporal tests validate presence of weak predicates, not decision-time correctness

`tests/test_nba_line_sources.py:21-46` asserts date-level and pre-commence SQL fragments, including the nullable `:as_of_date` bypass. It does not require an exact timestamp cutoff, atomic paired snapshot selection, or returned audit metadata.

`tests/test_daily_runner.py:168-197` verifies that current lines load and retain bookmaker, but does not inspect SQL for decision/commence predicates or selected timestamps.

These tests passed, demonstrating current behavior rather than temporal safety.

### P2-2. No explicit automated test covers post-commence exclusion in the NBA backtest or edge refresh

The targeted test search found MLB timestamp/CLV tests and generic NBA runner tests, but no NBA assertion that:

- selected snapshot is `<= decision_time`;
- selected snapshot and market update are `< commence_time`;
- Over/Under are paired from a reproducible quote state;
- a post-start edge refresh cannot recommend or place a new bet.

# Proven-safe components

## Prior-game rolling features: PASS

`populate_average_stats.py` groups player rolling calculations by `(player_id, season_id)` and applies `shift(1)` before rolling/expanding statistics (`:213-242`). B2/B3/B4 calculations repeat the same grouping and shift behavior (`:276-330`). Games below eight minutes are masked from rolling stat values using the shifted minutes mask (`:219-230, 291-317`), while schedule counts still use prior appearances.

Opponent-allowed rollups also shift before rolling (`src/processing/backfill_opponent_allowed.py:96-121`; incremental equivalent `backfill_opponent_allowed_incremental.py:143-168`).

Focused test evidence:

```text
./venv/Scripts/python.exe -m pytest -q \
  tests/test_populate_average_stats.py \
  tests/test_nba_line_sources.py \
  tests/test_daily_runner.py

41 passed, 1 warning in 4.15s
```

`tests/test_populate_average_stats.py:49-68` specifically proves that game 2 sees game 1 only and game 3 sees the mean of games 1–2.

## Core game/player joins: mostly PASS

- Training target/context joins use `pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id` (`src/models/features/nba/training_loader.py:301-303`).
- Historical context uses exact `game_id + player_id`, then joins team context by the same game/team (`src/models/features/nba/context_sources.py:18-30`).
- Quotes are keyed by player/game/market in the prop paths (`src/models/features/nba/line_sources.py:47-67`; `src/models/daily_runner.py:807-840`).
- Prediction/actual merges use `player_id + game_id + stat` (`src/backtesting/backtest_harness.py:868-934`).
- Rolling construction includes `season_id`, preventing ordinary cross-season windows (`src/processing/populate_average_stats.py:213-214, 287-288`).

Caveats are the stale hard-coded season identifier and target-date-unbounded roster selection described above.

# Required repair order

1. **P0:** Introduce one explicit timezone-aware `decision_time` contract for NBA training replay, backtest, inference, and edge refresh.
2. **P0:** Require quote validity by both effective snapshot/update time `<= decision_time` and `< commence_time`; stop new recommendations after commence.
3. **P0:** Return and persist selected quote audit columns and use a reproducible paired Over/Under selection key.
4. **P0:** Remove line-feature leakage in training and inference; existing artifacts using line context require clean retraining before promotion claims.
5. **P0:** Add injury observation timestamps/version identity and enforce `injury_observed_at <= decision_time` for historical construction.
6. **P1:** Preserve append-only decision revisions or a separate audit table rather than overwriting the only prediction row.
7. **P1:** Save model artifact ID/hash, feature as-of time, quote policy, selected quote IDs/timestamps, and execution timestamp in backtest and production outputs.
8. **P1:** Replace hard-coded `22025` and upper-bound player/team membership selection by target/decision time.
9. **P2:** Add negative tests with post-decision and post-commence rows for every active path.

# Unknowns and limitations

- No live database timestamp-coverage query was run. Code predicates already disprove temporal integrity; a coverage count cannot convert an unsafe query into a safe one.
- The audit did not certify whether `rapidapi_injuries` currently has a usable ingestion timestamp column; the active consumers do not reference one.
- No NBA lineup feature consumer was found in the inspected active model paths. Separate dashboard or legacy lineup consumers, if reactivated, need their own trace.
- Existing production artifacts were not loaded or retrained; per the restart plan they are reference templates, not current candidates.
- Current dirty-tree changes were preserved; this report is the only file created by this lane.
