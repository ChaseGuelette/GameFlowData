# NBA backtest harness check — November 7, 2026

## Requested MVP repair — completed November 7

The owner-requested drawdown, missing-quantile calibration and Sharpe repairs are implemented in `src/backtesting/performance_metrics.py`. Drawdown now uses each preceding equity peak on chronological daily P&L. Sharpe uses calendar-daily equity returns, includes intervening zero-action days, uses sample standard deviation and annualizes with 365; it retains the legacy zero/unavailable convention when dated observations or positive starting capital are insufficient. Calibration uses finite actual/prediction pairs and valid counts independently for each quantile. Existing saved metrics are not automatically recalculated.

Replay no longer queries `raw_player_props_combined`. Shared `src/backtesting/nba/compact_quotes.py` reads frozen `nba_player_prop_feature_lines` selections. Because that table has no prices, historical Over/Under prices must match the exact original cached event/player/market/book/line/source clocks; live rows use the small `nba_player_props_inference` table. Missing or contradictory prices stop the run—there is no invented price, API call or giant-table fallback. Direct/prefetched harness readers and native historical market/quote readers share this path. Existing historical policy/route gates remain; supported compact market routes are full-game/main. Pinnacle is accepted by the existing CLI/evidence allowlists, and the helper/parser are source-fingerprinted.

Keep `C:/Users/Chase/.gameflow/nba-props-t60/events` available for historical replay; override its parent directory with `NBA_PROPS_T60_STATE_DIR` if moved. No migration, new odds request, rescrape, model-stat expansion or broad run was needed. Replay now uses the compact table's one selected line/book at T-60, not the legacy all-book/latest-line shopping population. Combo-market prices are not present in this compact table.

Verification:
- 43 affected existing tests passed; scoped Ruff passed.
- Synthetic public-calculator checks: the reproduced drawdown is now 10%, valid-only Q50 coverage is 100% with n=1, and daily risk is invariant to shuffled input and splitting one day's bets into identical smaller bets.
- New reader: zero Pyright errors/warnings. Metrics: no introduced diagnostics against the pinned task-start commit `43c36c7f`; existing pandas typing diagnostics remain.
- Real local pilot, game `0022400061` on October 22, 2024: **44 supported-stat pairs matched exactly** across compact reader, direct harness, prefetched harness and native historical source (15 points, 15 rebounds, 14 assists). Full source reads also recovered 14 threes pairs, but current artifact support is only `pts/reb/ast`; no threes model support was claimed or added.
- Actual source prices and all T-60 clocks passed; zero HTTP calls, model loads/fits, legacy raw/archive queries or database writes. This pilot used DraftKings rows; the live-inference branch was not exercised against live rows.
- Receipts: Hermes scratch `compact_quote_local_pilot_supported_stats.json` and `nba_harness_metric_fix_probe.py`. The earlier 58-vs-44 comparison incorrectly injected unsupported threes into the harness and is superseded by the supported-stat comparison.

This completes the requested code/source repair, not full holdout evaluation or model promotion. Unrequested predictive-score expansion, empty-evaluation validity reporting and bounded training-loader CLI remain separate work. No full training or holdout sweep was launched.

## Original audit verdict (before this repair)

**Not yet trustworthy as the final multi-metric holdout evaluator.** Existing execution/identity/chronology plumbing is substantial, but the shared metric calculator has reproduced correctness defects. No real model holdout replay or model fitting was run; this is a bounded current-source audit plus existing tests and synthetic probes, not full end-to-end certification.

Inspected checkout: `43c36c7f`. Concurrent injury recovery/documentation changes were left untouched. No production source changes, new unit tests, DB writes, or model loads/fits were performed.

## Executed checks

- Backtest entrypoint/harness: 17 passed.
- NBA feature paths: 107 passed, 1 failed. `tests/test_nba_line_sources.py:37` still asserts `raw_player_props_combined`; the implementation now uses the intentionally migrated compact feature table. Do not reverse the migration just to satisfy this assertion.
- Bet simulator/training/feature selection: 51 passed.
- Total: **175 passed, 1 failed**. These existing tests are not sufficient coverage for the defects below.
- Public `MetricsCalculator.calculate()` synthetic probes reproduced incorrect drawdown and null-quantile accounting; empty inputs also return zeros/no invalid-status field.
- The exact default production artifact was rejected by the modern artifact validator because it lacks `artifact_manifest.json`. Its `run_config.json` also includes `22025` in both fit and calibration. This is a legacy artifact limitation, not evidence that the current chronology guard is broken.
- **Post-sync parity PASS:** independent read-only checks of the exact October 24, 2023–November 4, 2026 cohort matched 178,281 rows, 4,017 games, date bounds, ordered primary-key digest and full-row digest on local and remote. UTC-normalized full-row digest: `9d92c6b3d735fa3429dbebe44d1b65ef`; PK digest: `477b2475df79e3399b950f014c9a92a2`. Receipt: Hermes scratch `nba_compact_exact_window_parity_20261107.json`. A preliminary full-table comparison included 90 newer remote rows from November 5 and was not a valid test of this bounded sync; it is superseded by the matching-window result. No resync is required for the verified cohort.

Receipts/probe in Hermes scratch: `nba_harness_metric_audit_receipt.json`, `nba_harness_metric_audit_probe.py`. Probe values are synthetic accounting cases, not model performance.

## Confirmed metric defects and definition gaps

1. **Drawdown denominator:** `src/backtesting/performance_metrics.py:244-256` divides the largest dollar drawdown by the largest peak anywhere in the run. A synthetic 1,000 starting balance with profits -100, +100, +1,000 reports 5%, although the actual maximum peak-relative decline is 10%. Correct percentage drawdown is the maximum of each point's decline relative to its own preceding peak—not necessarily the index of the largest dollar decline.
2. **Chronological risk accounting:** `performance_metrics.py:228-271` trusts input row order. Reversing the same dated rows changes reported risk. The normal simulator emits chronological bets, but reusable metrics/export analysis should enforce or validate ordering.
3. **Sharpe basis:** `performance_metrics.py:239-242` uses dollar P&L per bet and annualizes as if observations were game days. There is no daily-return aggregation. Define the intended risk/return series explicitly before comparing this number across models or changing bet volumes/stakes.
4. **Null quantile coverage:** `performance_metrics.py:312-332` counts rows with a non-null actual even when that quantile is missing. Two actuals with one valid Q50 and one NaN produce 50% coverage/n=2 rather than valid-only 100%/n=1. Missingness must be reported, not counted as a quantile miss.
5. **Empty/incomplete outputs:** `performance_metrics.py:184-197,309-321` returns zero headline metrics and silently skips missing quantiles. Add an explicit insufficient/invalid evaluation status; this does not mean the entire sweep currently claims promotion, because its separate readiness metadata is already false.
6. **Exposure definitions:** ROI excludes push stakes (`performance_metrics.py:199-215`), while `total_bets` includes unresolved entries. Report action/handle and resolved/void/unresolved populations separately. Do not silently rename the current denominator or mix legacy and repaired metrics in comparisons.
7. **Prediction-quality coverage:** this calculator exports aggregate quantile coverage, financial outcomes/risk, stat betting slices and edge buckets. It does not export per-stat MAE/pinball, Brier/log loss or over/under slices. Existing fit/calibration helpers elsewhere are not equivalent to held-out replay scores.

The modern sweep uses this same calculator: `src/backtesting/nba/sweep_runner.py:554`. These are not merely unused legacy-helper concerns.

## Verified guards and corrections to preliminary findings

- Fit/calibration season overlap and population date/game overlap are rejected; complete-date inner splits and population identity hashes exist in `src/models/nba_training_chronology.py`.
- Historical replay **does have** policy evaluation-window and actual-artifact calibration guards: `src/backtesting/nba/historical_execution.py:145-159`; `sweep_config.py:353-356` invokes the artifact cutoff check.
- Historical training provenance checks observed fit/calibration bounds, exclusive calibration end and OOS start: `historical_execution.py:186-228`. Do not repeat the preliminary claim that this boundary is absent merely because the guard lives outside the entrypoint.
- Consuming a complete calibration season is not inherently leakage when evaluation is a later season. Missing `--cal-end-date` alone is not proof of overlap.
- Artifact integrity/finalization is not promotion approval. Current historical receipts intentionally retain `decision_grade=False`; do not weaken these gates just to produce a green run.
- A calibration-warning/failure sidecar alone is not a mandate to apply global offsets or reject edge-bearing Q10 behavior. Calibration is a diagnostic alongside predictive and economic evidence, under the existing invariants.
- American-odds settlement, refunds and flat-stake override are covered by source review and existing simulator tests. This does not establish every real-data settlement edge case.

## Quote/source compatibility still needs a real-window check

`make_sources()` constructs `PostgresHistoricalSources`. Its `quote_rows()` still reads `raw_player_props_combined` (`src/models/features/nba/historical_sources.py:209-233`), not the newly synced `nba_player_prop_feature_lines` or daily inference table. It requires separate source metadata, compatible pair clocks and historical-policy authorization. Its SQL caps candidates without an as-of predicate; downstream policy validation remains necessary, and large candidate populations can fail the cap rather than establish completeness.

The compact table is a training feature source, not paired executable odds. Syncing it does not certify betting replay availability. Verify paired odds only for the selected evaluation window; **do not require or rescrape executable odds for every fitting-season game merely to train**. Cached historical responses may already contain useful paired quotes; reuse verified sources before requesting new paid data.

CLV and intraday stability are explicitly unsupported in the current saved-output audit capabilities (`src/backtesting/nba/evidence.py:47-58`). An integrity-audit PASS is not source provenance, settlement, historical acceptance or model promotion.

## Loader-only smoke command

No existing CLI provides a genuinely bounded local training-loader-only smoke test. `train_pipeline.py:1498-1500` dry-run prints a plan without loading data. Normal execution proceeds into fitting. `FeatureStore.get_training_dataset()` accepts whole seasons, and the ordinary training loader rejects fewer than 1,000 rows. Do not give a fake `--smoke-test`/`--load-only` flag or use `.head()` after an unbounded full-season load as the claimed bounded smoke.

## Minimal next steps (not yet authorized implementation)

1. Repair shared metric correctness and explicit validity/missingness accounting, preserving settled model/feature policy. Use existing tests plus controlled probes; do not create a new evaluation framework.
2. Add a small local loader-only path with true query-level bounds and no model fitting, without weakening the production training population guard.
3. Validate actual chosen holdout quotes/features and exact fresh artifact through the real replay path; keep incomplete coverage visible. No full training/sweep is launched by this audit.
4. Evaluate the fixed candidate on the frozen holdout with per-stat prediction quality (MAE, pinball, quantile coverage/missingness), then flat-stake profit/ROI, volume, corrected drawdown and consistently defined daily risk, with stat/side slices. Do not tune the candidate or betting policy repeatedly on that final holdout; use development data for selection and a separate untouched window for confirmation.

## Relevant prior lessons/invariants

Canonical `models/nba-model` records the owner direction: fit `22023`/`22024`, calibrate `22025`, initial test `22026`. Hard Facts/critical invariants and lesson-tag enumeration/keyword retrieval were consulted. Apply `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`, `lessons/empirical-cdf-for-probabilities`, and `lessons/implausibly-profitable-backtests-are-methodology-red-flags`: verify consumers and artifact chronology, use empirical sample probabilities, and audit unusually attractive results rather than treating them as automatic validation. No global conformal recalibration or architecture change is proposed.
