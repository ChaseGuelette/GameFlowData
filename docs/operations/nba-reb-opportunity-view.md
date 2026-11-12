# NBA REB joined opportunity view

## Scope

Saved-file diagnostic of the **game-market-free research control**, `nba_feature_ablations/game_market_free/nba_run_20261108_150945`, not the earlier game-line-inclusive no-injury artifact or a direct-total challenger. The control excludes `line_spread`, `line_total` and injury inputs; player-prop inputs remain. This is a REB-only replay at raw/fixed-BL 5% edge, not the operational multi-stat/paper policy.

Builder: `scripts/build_nba_reb_opportunity_view.py`.
Produced bundle: `backtest_results/nba_reb_opportunity_view_nov2_v1`.
Input diagnostic: `backtest_results/nba_game_market_free_error_stages_nov2_v4`.
Input replay: CONTROL cells from `backtest_results/nba_direct_reb_sweep_nov2_v1`.

The original model identity and shared reference manifest bind both inputs. Original artifact/export files remain unchanged. No retraining, model loading, sampling, replay, database access or new context acquisition is required.

## Produced tables

- `player_games.csv`: 1,579 distinct eligible player-games over 93 games/12 scored dates, preserving all 546 without quotes. Includes minutes and rebound-rate quantiles, lagged references, combined rebound forecast, actuals/errors, `has_quote` and `quoted_occurrences`.
- `opportunities.csv`: 4,132 rows, 97 unique columns. Each retained quote appears for both sides and both probability treatments. These are 1,033 quotes, not 4,132 independent outcomes. Includes component/combined predictions, original executable prices, empirical versus replay versus BL probabilities, selection flags, timing/provenance and funded wager evidence.
- `selection_funnel.csv`: universe → quoted occurrences → side rows → saved best-side selection → minimum edge eligibility → funded wagers. Wager count/stake/P&L are populated only at the funded stage; earlier stages are populations, not separate portfolios.
- `data_dictionary.json`: units, row grains, probability semantics and a role for every field in both tables. All actuals, oracle flags, errors and outcomes are explicitly hindsight. Forecasts and lagged references are separate.
- `manifest.json` and `report.md`: source/output receipts, original model identity, counts, accounting and limitations.

## Verification

The integrated public CLI executed against real saved inputs under the offline guard. Independent parent readback verified source immutability against a pre-build snapshot; all output/source hashes; exact eligible and unquoted key populations; every component/combined forecast and actual; both-treatment quote probability/market parity; exact funded row identity/payoffs; unique headers/keys; field-role completeness; nonbet null P&L; and funnel accounting.

Raw: 612 funded wagers, $6,120 stake, -$461.461819517935 profit.
Fixed BL: 140 funded wagers, $1,400 stake, +$36.274261801531864 profit.
All threshold-qualified saved selections were funded in these inputs; do not infer nonexistent cash rejections.

Ruff, compilation and scoped Pyright passed (0 errors/warnings). Occupied output rejection passed. Independent source review found one portability blocker in the frozen worker copy: mandatory scratch-receipt loading. The coordinator removed that dependency before the successful main build, independently verified its absence and the standalone public invocation, and fixed upstream funnel accounting. The delivered script uses expected source-manifest receipts and checks them again before finalization. No other concrete blocker was found by the independent reviewer. This is coordinator-verified closure of the review finding, not a second independent review of changed bytes.

## Reading rules

- Filter one treatment and one appropriate stage before aggregating. Do not add policies' P&L or treat opposing sides as independent observations.
- Nonbets have null stake/outcome/profit, not hypothetical P&L. Event labels can be scored separately; pushes are excluded from binary labels.
- Diagnostic over probability is empirical. Diagnostic under uses an explicitly labeled complement, not strict empirical-under mass. Replay uses the existing clamp/complement; BL uses its saved posterior. No probability policy was changed.
- Only DraftKings/Pinnacle selections exist in these retained quotes. This view is not a complete all-book shopping universe.
- No injury, role-change or opponent-feature context was acquired. Do not infer those causes from absent fields.
- The October21–November2 window is reused discovery data. Receipt consistency is not historical-source certification, proof of causality or promotion approval.
- The builder is bounded to this frozen cohort/settings and rejects ambiguous quote identities; do not assume arbitrary-window support from path arguments.

## Rebuild recipe

The completed v1 output is immutable/occupied. To rerun the same view, choose a fresh output name; do not delete the existing evidence.

` .\venv\Scripts\python.exe -X utf8 scripts/build_nba_reb_opportunity_view.py --diagnostic-dir backtest_results/nba_game_market_free_error_stages_nov2_v4 --sweep-dir backtest_results/nba_direct_reb_sweep_nov2_v1 --output-dir backtest_results/nba_reb_opportunity_view_nov2_v2 `

## First concentrated analysis completed

Saved report: `backtest_results/nba_reb_opportunity_analysis_nov2_v1/report.md`, with receipt manifest and stage, conditional, date, same-cohort probability, component and saved-oracle tables. Source/output hashes, numeric readback, original accounting and all grouping population totals passed. No retraining, replay or new data acquisition occurred.

The principal finding is conditional overconfidence: raw funded wagers predict 59.37% wins but realize 48.04%; paired market probability averages 48.26%. Raw Brier trails the market on 9/12 dates and remains worse in an exploratory whole-date bootstrap. Every leave-one-date-out raw ROI remains negative. Low-line overs (<=3.5) are a particularly recurrent diagnostic subgroup: 146 wagers, predicted57.80%/observed43.84%, market-relative Brier worse on all12 represented dates. This is not an approved exclusion rule.

Both component medians still beat their L5 references, even on selected overs. Signed-error accounting and saved hindsight oracles motivate rate/line-probability inspection rather than a blind minutes retrain, but do not establish recoverable signal or a causal defect. On identical raw-funded opportunities, BL brings Brier near the market without establishing independent alpha. Since this REB-only evidence retains one quote per quoted player-game, side selection alone leaves complementary Brier unchanged; threshold filtering changes the cohort. Do not substitute the earlier multi-stat selection interpretation.

Initial bins and 5,000 whole-date resamples/seed42 are recorded in protocol.json; subsequent side refinements are explicitly exploratory. The remaining task is to identify a concrete pregame input or modeling assumption behind the rate/line disagreement, not automatically retrain or add calibration offsets.
