# MLB Quote-Clean Backtest Audit Hardening Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves. Keep SQL isolated through the GameFlow SQL runner pattern. Do not make broad repo-root searches.

**Goal:** Make MLB quote-clean backtest validation auditable for batter_hits now and pitcher_strikeouts next, while preventing future confusion between legacy and promotion-grade backtesting paths.

**Architecture:** Treat `src/backtesting/mlb/run_mlb_sweep.py` as the only promotion-grade MLB backtest entrypoint. Add reusable post-replay audit scripts rather than embedding all diagnostics in the sweep. Keep CLV computation separate from model retraining and quote-clean replay, but add a classifier that explains CLV failure modes from existing CLV outputs.

**Tech Stack:** Python, pandas, SQLAlchemy, existing GameFlow feature stores/model suite/backtest helpers, local Postgres via `--local`, pytest.

---

## Context and Decisions

### Canonical backtesting decision

Current state:
- `src/backtesting/mlb/run_mlb_sweep.py` is the canonical production-validation path.
- `src/backtesting/mlb/line_selection.py` now centralizes quote-clean line selection.
- `src/backtesting/mlb/mlb_backtest_harness.py` is retained for single-config/legacy/debugging usage.
- `src/backtesting/mlb/run_mlb_backtest.py` still routes through the legacy harness and is not promotion-grade.

Scope decision:
- Treat `run_mlb_sweep.py` as the only promotion-grade entrypoint.
- Either hard-deprecate `run_mlb_backtest.py`, or rewrite it as a thin wrapper around `run_mlb_sweep.py` / shared evaluation functions.
- Keep `mlb_backtest_harness.py` only for tests or one-off debugging, and prevent it from being used as promotion evidence.

### Pitcher K validation scope

Pitcher K shares the same quote-source issue class as batter_hits:
- Any MLB stat using historical prop lines can suffer from latest-row / post-cutoff / post-commence leakage if the harness grabs latest available rows instead of decision-time rows.
- `pitcher_strikeouts` also uses `mlb_raw_player_props`.
- The source audit found `pitcher_strikeouts` had 142,216 rows and 1,288 post-start rows (~0.91%) in the 2026-04-13 to 2026-05-10 window.
- Therefore pitcher K also requires quote-clean replay for promotion-grade evidence.

Differences from batter_hits:
- The current urgent leakage finding is specifically around `prop_line_batter_hits` and the with-prop-line vs no-prop-line comparison.
- Batter_hits CLV/edge ranking was already suspicious under old timing semantics.
- Pitcher K does not necessarily require the same prop-line ablation unless the evaluated feature path uses `prop_line_pitcher_strikeouts` as a model feature.
- The new audit tooling should be stat-generic and support both `batter_hits` and `pitcher_strikeouts`.

### Existing predecessor scripts

Existing:
- `scripts/diagnose_mlb_quote_clean_red_flags.py`
  - useful predecessor
  - hardcoded for older pitcher K static-vs-hook run
  - computes broad dropped prediction counts
  - not reusable
  - does not classify all required dropout buckets

Missing:
- A generic `scripts/audit_mlb_quote_clean_dropout.py` that supports arbitrary model dir, stat, date window, cutoff, and sweep output directory.

Existing CLV script:
- `scripts/analyze_mlb_batter_hits_clv.py`
  - computes CLV matches, summary, plus-odds bands, edge bins, bookmaker splits, timing stability, and phase1b decision
  - does not produce a clean failure-mode diagnosis report

Missing:
- A post-CLV classifier script, preferably `scripts/diagnose_mlb_clv_failure_modes.py`, that consumes CLV output files and explains which failure mode occurred without changing CLV math.

---

## Recommended Implementation Order

1. Build the quote-clean dropout audit first.
   - Directly supports the current batter_hits sweeps.
   - Tells us whether quote-clean replay coverage is sane.

2. Build the CLV failure-mode classifier second.
   - Useful once `bets.csv` and CLV outputs exist.
   - Keeps CLV math stable while making interpretation explicit.

3. Reconcile/deprecate legacy backtest suites third.
   - Important for long-term hygiene.
   - Lower immediate value than the two audit scripts unless we are already touching core backtesting internals.

---

## Task 1: Build generic quote-clean dropout audit script

**Objective:** Create a reusable post-replay/source audit that classifies why model predictions did or did not receive quote-clean lines and bets.

**Files:**
- Create: `scripts/audit_mlb_quote_clean_dropout.py`
- Create: `tests/test_audit_mlb_quote_clean_dropout.py`
- Reuse: `src/backtesting/mlb/run_mlb_sweep.py`
- Reuse: `src/backtesting/mlb/line_selection.py`

**CLI contract:**

```text
python scripts/audit_mlb_quote_clean_dropout.py --local --model-dir <artifact_dir> --start 2026-04-13 --end 2026-05-10 --stats batter_hits --direction under --quote-cutoff-time-et 13:30 --sweep-output-dir <backtest_result_dir> --output-dir <audit_output_dir>
```

**Required arguments:**
- `--model-dir`: artifact directory loaded by `MLBModelSuite.from_directory()`.
- `--start`, `--end`: replay window.
- `--stats`: one or more MLB stat keys; must support at least `batter_hits` and `pitcher_strikeouts`.
- `--quote-cutoff-time-et`: e.g. `13:30`.
- `--output-dir`: audit output path.

**Optional arguments:**
- `--local`: use local Postgres.
- `--direction`: `over`, `under`, or `both`; default `both`.
- `--sweep-output-dir`: optional existing quote-clean sweep result dir; if provided, validate saved files/columns and classify below-edge from saved predictions/bets when possible.
- `--edge`, `--tau`, `--z-max`, `--max-weight`: optional config used to classify `clean_quote_exists_below_edge` when no sweep output is supplied.
- `--batch-size`: game_id chunk size for raw prop queries.

**Dropout buckets:**

Use deterministic precedence:
1. `clean_quote_available`
2. `clean_quote_exists_below_edge`
3. `no_raw_prop_rows`
4. `only_excluded_books`
5. `only_after_cutoff`
6. `only_post_commence`
7. `no_paired_over_under`
8. `unknown_unclassified`

Definitions:
- `no_raw_prop_rows`: no rows in `mlb_raw_player_props` for `player_id/game_id/market_key`.
- `only_excluded_books`: raw rows exist but all are from excluded books/DFS platforms.
- `only_after_cutoff`: allowed-book rows exist but none are at or before decision cutoff.
- `only_post_commence`: rows exist but valid candidates are at/after `commence_time`.
- `no_paired_over_under`: allowed, pre-cutoff, pre-commence rows exist but no atomic Over/Under pair for same book/line/effective snapshot.
- `clean_quote_available`: paired clean quote exists.
- `clean_quote_exists_below_edge`: clean quote exists, but no bet is placed under the selected config/direction.

**Implementation notes:**
- Generate the all-prediction denominator with the same path as quote-clean replay:
  - `run_shared_phases(...)`
  - `quote_clean_cutoff_time_et=<cutoff>`
  - `MLBModelSuite.from_directory(<model_dir>)`
- Do not rely only on saved `predictions.csv`, because `precompute_mlb_base_probs()` skips predictions without matched lines.
- Query raw props by schedule-derived indexed `game_id` chunks; do not scan `commence_time` over the raw table.
- Reuse `DEFAULT_EXCLUDED_BOOKMAKERS` from `src/backtesting/mlb/line_selection.py`.
- Translate internal stat keys through `STAT_TO_MARKET_KEY` where needed.
- Use `COALESCE(snapshot_time, inserted_at)` as `effective_snapshot_time`.
- Treat `market_last_update` as primary when present, and still require `effective_snapshot_time <= cutoff`.
- For post-commence filtering, enforce both `market_last_update < commence_time` when present and `effective_snapshot_time < commence_time`.
- Atomic pairing requires same `player_id`, `game_id`, `market_key`, `bookmaker`, `line`, and `effective_snapshot_time`, with both Over and Under odds present.

**Outputs:**
- `<output-dir>/dropout_summary_by_bucket.csv`
- `<output-dir>/dropout_by_date.csv`
- `<output-dir>/dropout_by_game.csv`
- `<output-dir>/dropout_by_bookmaker.csv`
- `<output-dir>/dropout_rows.csv`
- `<output-dir>/selected_clean_quotes.csv`
- `<output-dir>/audit_summary.json`
- `<output-dir>/audit_summary.md`

**Pass/fail guidance in markdown summary:**
- PASS if selected clean quotes have zero cutoff/commence violations and dropout is stable/plausible.
- WARN if dropout is high but explained by market absence/cutoff timing.
- FAIL if selected quotes violate cutoff/commence rules, if pairing is synthetic/mismatched, or if missingness clusters by date/game without explanation.

**Tests:**
- Unit-test bucket classifier with small pandas DataFrames.
- Test atomic pairing requires same snapshot for Over/Under.
- Test excluded-book-only rows classify correctly.
- Test post-cutoff rows classify as `only_after_cutoff`.
- Test post-commence rows classify as `only_post_commence`.
- Test saved-output validator fails when required quote audit columns are missing.

**Validation command:**

```text
.\venv\Scripts\python.exe -m pytest tests\test_audit_mlb_quote_clean_dropout.py -q
```

---

## Task 2: Build post-CLV failure-mode classifier

**Objective:** Create a report-only script that consumes outputs from `scripts/analyze_mlb_batter_hits_clv.py` and classifies why CLV passed, failed, or is inconclusive.

**Files:**
- Create: `scripts/diagnose_mlb_clv_failure_modes.py`
- Create: `tests/test_diagnose_mlb_clv_failure_modes.py`
- Reuse outputs from: `scripts/analyze_mlb_batter_hits_clv.py`

**CLI contract:**

```text
python scripts/diagnose_mlb_clv_failure_modes.py --clv-output-dir <dir_from_analyze_mlb_batter_hits_clv> --output-dir <diagnosis_dir>
```

**Input files expected:**
- `clv_summary.csv`
- `clv_by_plus_odds_band.csv`
- `clv_by_edge_bin.csv`
- `clv_by_bookmaker.csv`
- `clv_timing_stability.csv`
- `clv_matches.csv`
- `phase1b_decision.csv`

**Failure modes to classify:**

- `negative_mean_clv`
  - mean CLV < 0.

- `underpowered_or_inconclusive`
  - mean CLV positive but confidence interval crosses zero, too few scored bets, or too few bootstrap blocks.

- `edge_ranking_failure`
  - Spearman(edge, CLV) weak/negative, especially if CI low is <= 0.

- `same_book_coverage_failure`
  - too many consensus fallbacks or not enough same-book close matches.

- `timing_stability_missing`
  - +15/+30/+60 minute stability is unavailable/sparse. Current base script emits +15 only; classifier should explicitly report missing horizons rather than pretending they passed.

- `bookmaker_cluster_failure`
  - CLV pass/fail is dominated by one bookmaker or one bookmaker has materially negative CLV.

- `odds_band_failure`
  - one odds band has materially negative CLV or negative upper confidence bound.

- `line_movement_mismatch`
  - line movement opposes bet direction even if odds CLV is noisy, when available from `clv_matches.csv`.

- `data_quality_failure`
  - close before bet, close after commence, unmatched rate too high, missing timestamps, or missing required CLV columns.

**Output:**
- `<output-dir>/clv_failure_modes.json`
- `<output-dir>/clv_failure_modes.md`

**Decision labels:**
- `pass`
- `fail_model_or_edge`
- `fail_data_or_timing`
- `inconclusive_underpowered`
- `invalid_missing_inputs`

**Interpretation standard:**
- CLV failure blocks promotion but does not automatically mean the model should be deleted.
- Negative mean CLV or strongly negative edge/CLV ranking is a model/edge validation failure if data quality is clean.
- High unmatched/fallback/timing problems are data/timing failures first.
- Positive mean CLV with weak edge ranking means production sizing/ranking is not validated.

**Tests:**
- Minimal positive/pass case.
- Negative mean CLV case.
- Positive mean but CI crosses zero case.
- Edge ranking failure case.
- Consensus fallback dominance case.
- Missing input files case.
- Missing required columns case.

**Validation command:**

```text
.\venv\Scripts\python.exe -m pytest tests\test_diagnose_mlb_clv_failure_modes.py -q
```

---

## Task 3: Scope pitcher K quote-clean validation

**Objective:** Ensure pitcher K can be validated with the same quote-clean/dropout tooling and is not trusted from legacy latest-row backtests.

**Files:**
- Modify or document: `docs/development_docs/mlb_pitcher_k_quote_clean_validation_scope.md` or include in implementation notes.
- Reuse: `scripts/audit_mlb_quote_clean_dropout.py`
- Reuse: `src/backtesting/mlb/run_mlb_sweep.py`

**Validation checklist for pitcher K:**
- Run quote-clean replay with `--stats pitcher_strikeouts` and explicit `--quote-clean --quote-cutoff-time-et <HH:MM>`.
- Run dropout audit with `--stats pitcher_strikeouts`.
- Confirm no selected quotes violate cutoff/commence rules.
- Confirm post-start source rows are filtered out.
- Confirm quote-clean pairing availability is stable/plausible by date/game.
- Check over/under side splits before production recommendations.
- Do not compare pitcher K legacy ROI directly to quote-clean ROI as equivalent evidence.

**Initial known source audit facts:**
- Window: 2026-04-13 to 2026-05-10.
- `pitcher_strikeouts` rows: 142,216.
- Timestamp coverage: 100% for existing rows.
- Post-start rows: 1,288 (~0.91%).
- Latest-row selection remains leakage-prone despite lower post-start rate than batter markets.

**Acceptance criteria:**
- The generic dropout script supports `pitcher_strikeouts` without hardcoding batter-only assumptions.
- The markdown summary explicitly labels pitcher K validation as quote-clean only.

---

## Task 4: Reconcile/deprecate MLB backtesting suites

**Objective:** Prevent future use of legacy MLB backtest entrypoints as promotion-grade evidence.

**Files:**
- Modify: `src/backtesting/mlb/run_mlb_backtest.py`
- Modify: `src/backtesting/mlb/mlb_backtest_harness.py`
- Possibly modify: `src/backtesting/mlb/__init__.py`
- Create/modify tests as needed.

**Preferred approach:**
- Do not duplicate quote-clean logic.
- Keep `run_mlb_sweep.py` as the canonical entrypoint.
- Convert `run_mlb_backtest.py` to either:
  1. hard-deprecated script that exits with a clear message unless `--allow-legacy` is provided, or
  2. thin wrapper around `run_mlb_sweep.py` shared functions for one fixed config.

**Recommended first implementation:**
- Add a hard deprecation warning and default exit to `run_mlb_backtest.py`.
- Message should say:
  - `run_mlb_sweep.py --quote-clean` is the only promotion-grade entrypoint.
  - `run_mlb_backtest.py` is legacy/debug-only.
  - Use `--allow-legacy` only for debugging, not promotion evidence.
- Add `--allow-legacy` to preserve emergency/debug usage.
- In `mlb_backtest_harness.py`, strengthen the architecture note/docstring and log a warning when instantiated.

**Later optional implementation:**
- Rewrite `run_mlb_backtest.py` as a fixed-config wrapper around `run_mlb_sweep.py` / `run_shared_phases()` / `run_single_config_fast_mlb()`.
- If rewritten, require explicit `--quote-clean` for promotion-grade mode and support `--local`.

**Tests:**
- Running `run_mlb_backtest.py` without `--allow-legacy` exits non-zero and prints the deprecation message.
- Running with `--allow-legacy --help` still works.
- `run_mlb_sweep.py --help` still exposes `--quote-clean` and `--quote-cutoff-time-et`.

**Validation commands:**

```text
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_backtest.py --start 2026-04-13 --end 2026-04-13
```
Expected: exits with deprecation/promotion-grade warning.

```text
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --help
```
Expected: includes `--quote-clean` and `--quote-cutoff-time-et`.

---

## Global Acceptance Criteria

- `run_mlb_sweep.py --quote-clean` remains the only promotion-grade MLB replay path.
- A generic dropout audit can be run for both `batter_hits` and `pitcher_strikeouts`.
- Dropout audit outputs include bucket-level, date-level, game-level, and row-level explanations.
- CLV failure-mode classifier consumes existing CLV outputs without changing CLV math.
- Legacy backtest entrypoints cannot be mistaken for promotion-grade evidence.
- All scripts are PowerShell-friendly and support local training/backtesting workflows.

---

## Suggested Immediate Commands After Implementation

For current batter_hits with-prop-line artifact:

```text
.\venv\Scripts\python.exe scripts\audit_mlb_quote_clean_dropout.py --local --model-dir src\models\mlb\artifacts\mlb_run_batter_hits_20260515_125316 --start 2026-04-13 --end 2026-05-10 --stats batter_hits --direction under --quote-cutoff-time-et 13:30 --sweep-output-dir backtest_results\mlb_batter_hits_with_prop_line_quote_clean_under_20260413_20260510_20260515 --output-dir reports\mlb_batter_hits_with_prop_line_dropout_20260515
```

For CLV diagnosis after running `analyze_mlb_batter_hits_clv.py`:

```text
.\venv\Scripts\python.exe scripts\diagnose_mlb_clv_failure_modes.py --clv-output-dir reports\<clv_output_dir> --output-dir reports\<clv_output_dir>\failure_modes
```

---

## Non-Goals

- Do not rerun model training from these audit scripts.
- Do not mutate the database.
- Do not replace CLV math in `analyze_mlb_batter_hits_clv.py` during the first pass.
- Do not promote either batter_hits variant from raw ROI alone.
- Do not rely on legacy latest-row MLB backtests as production evidence.
