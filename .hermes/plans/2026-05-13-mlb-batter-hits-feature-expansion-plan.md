# MLB Batter Hits Residual-First Feature Expansion Plan

> **For Hermes:** This is a review draft. Do not implement phases until Chase approves the phase/spec. Use `gameflow-model-evaluation`, `gameflow-explore`, `test-driven-development`, and `subagent-driven-development` for implementation work.

**Goal:** Improve the MLB `batter_hits` model without feature-shopping against a strong backtest by using residual/calibration diagnostics to select one constrained feature class per phase.

**Architecture:** Freeze the current flat-stake under-only benchmark, build diagnostics that identify the largest calibrated error source, then add feature families in small gated phases. Each phase must beat the previous clean benchmark on calibration and betting quality, not just headline ROI.

**Tech Stack:** Python, pandas, existing MLB sweep outputs under `backtest_results/`, existing `run_mlb_sweep.py`, existing MLB model training/backtest pipelines, local Postgres via existing project CLIs.

---

## Current evidence from flat-stake direction splits

Window: `2026-04-13` to `2026-05-10`  
Stake: flat `$100` per bet  
Meaningful-volume rule: `>= 100` bets

Result directories:

- Both: `backtest_results/mlb_sweep_batter_hits_flat_both_20260413_20260510`
- Over: `backtest_results/mlb_sweep_batter_hits_flat_over_20260413_20260510`
- Under: `backtest_results/mlb_sweep_batter_hits_flat_under_20260413_20260510`

### Decision summary

**Winner:** `batter_hits` under-only.

**Deploy posture:** Promotion candidate for paper/live validation only after residual diagnostics and artifact boundary checks. Do not deploy overs. Do not add features yet.

**Why:** The both-direction flat result is effectively the under-only result. The over-only sweep has no 100+ bet configuration. Under-only preserves the strong high-edge signal under flat sizing.

### Flat raw no-BL edge ladder

Both directions, raw no-BL:

| Edge | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---:|---:|---:|---:|---:|---:|---:|
| 0.08 | 1405 | 43.84% | +4.08% | +$5,739 | 0.442 | 17.06% |
| 0.10 | 908 | 46.04% | +8.17% | +$7,422 | 0.887 | 17.70% |
| 0.12 | 551 | 49.36% | +14.22% | +$7,836 | 1.557 | 9.85% |
| 0.15 | 252 | 56.75% | +31.64% | +$7,974 | 3.481 | 3.93% |

Under-only, raw no-BL:

| Edge | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---:|---:|---:|---:|---:|---:|---:|
| 0.08 | 1365 | 43.44% | +3.02% | +$4,118 | 0.327 | 29.90% |
| 0.10 | 888 | 45.83% | +7.57% | +$6,726 | 0.823 | 17.41% |
| 0.12 | 543 | 48.99% | +13.08% | +$7,100 | 1.436 | 10.27% |
| 0.15 | 250 | 56.80% | +31.56% | +$7,889 | 3.477 | 3.95% |

Over-only, raw no-BL:

| Edge | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---:|---:|---:|---:|---:|---:|---:|
| 0.08 | 40 | 57.50% | +40.52% | +$1,621 | 4.306 | 2.89% |
| 0.10 | 20 | 55.00% | +34.80% | +$696 | 3.641 | 3.73% |
| 0.12 | 8 | 75.00% | +92.00% | +$736 | 10.499 | 0.93% |
| 0.15 | 2 | 50.00% | +42.50% | +$85 | 3.889 | 0.98% |

Interpretation:

- Under-only is the meaningful-volume lane.
- Over-only looks directionally positive but is tiny-sample only; no config reaches the 100-bet rule.
- The raw under-only edge ladder is strongly monotonic by ROI and hit rate from edge `0.08` to `0.15`.
- Raw under-only `edge=0.15` is the current simple flat benchmark: `250 bets`, `+31.56% ROI`, `3.477 Sharpe`, `3.95% MaxDD`.

### Under-only BL/risk-control candidates

Eligible under-only configs with strong risk-adjusted performance:

| Config | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---|---:|---:|---:|---:|---:|---:|
| tau=0.90 z=0.25 mw=0.65 edge=0.10 | 117 | 65.81% | +42.90% | +$5,019 | 5.093 | 1.81% |
| tau=0.75 z=0.25 mw=0.65 edge=0.10 | 103 | 64.08% | +33.62% | +$3,462 | 4.124 | 2.02% |
| raw no-BL edge=0.15 | 250 | 56.80% | +31.56% | +$7,889 | 3.477 | 3.95% |
| tau=0.90 z=0.25 mw=0.80 edge=0.12 | 110 | 60.00% | +26.07% | +$2,867 | 3.124 | 2.87% |
| tau=0.90 z=0.25 mw=0.80 edge=0.10 | 258 | 52.71% | +19.57% | +$5,049 | 2.145 | 6.20% |
| tau=0.75 z=0.25 mw=0.65 edge=0.08 | 278 | 52.16% | +18.97% | +$5,273 | 2.070 | 7.42% |
| tau=0.90 z=0.25 mw=0.65 edge=0.08 | 332 | 51.51% | +18.76% | +$6,228 | 2.038 | 6.25% |

Current benchmark set for future phases:

1. **Profit benchmark:** raw under-only `edge=0.15`, 250 bets, +31.56% flat ROI, +$7,889, Sharpe 3.477, MaxDD 3.95%.
2. **Risk-adjusted benchmark:** BL under-only `tau=0.90 z=0.25 mw=0.65 edge=0.10`, 117 bets, +42.90% flat ROI, +$5,019, Sharpe 5.093, MaxDD 1.81%.
3. **Volume/risk benchmark:** BL under-only `tau=0.90 z=0.25 mw=0.65 edge=0.08`, 332 bets, +18.76% flat ROI, +$6,228, Sharpe 2.038, MaxDD 6.25%.

---

## Non-negotiable principles

1. **Residual diagnostics before feature additions.** No new batter_hits feature family is implemented until diagnostics identify a large enough calibration/residual gap.
2. **One feature family per phase.** Avoid 60-80 feature expansions. Prefer compressed, auditable features.
3. **Temporal integrity over feature count.** Every feature must have a timestamp contract proving it exists before bet placement.
4. **Under-only is the meaningful lane until overs reach 100+ bets.** Overs remain exploratory.
5. **Flat-stake results are required for promotion decisions.** Kelly/compounding can be reported, not trusted alone.
6. **Every phase compares against the previous phase on the same clean window.** A phase must improve calibration and betting quality, not just one headline ROI number.
7. **Use empirical probabilities only.** Maintain GameFlow invariant: probabilities from samples use `(samples > line).mean()`, never Gaussian CDF.

---

## Global gates for every phase

A phase is eligible to advance only if it passes these gates:

### Data/temporal gates

- Backtest start date is strictly after model calibration cutoff.
- New features are available at prediction time in production.
- No final-close, post-game, or same-day future leakage.
- Sparse features must have explicit missingness behavior and coverage report.

### Model gates

- Calibration report does not worsen materially.
- Probability decile calibration improves or stays neutral in the target error bucket.
- Edge-bin ROI remains monotonic or improves in the production edge range.
- Under-only remains positive at 100+ bets.

### Betting gates

- At least one candidate config has `>= 100` bets.
- Compare raw and BL candidates.
- Report both Kelly and flat stake where relevant; flat is the decision-grade view.
- Compare:
  - total bets
  - hit rate
  - ROI
  - flat ROI
  - profit
  - Sharpe
  - max drawdown
  - edge-bin monotonicity
  - over/under split

---

## Phase 0: Freeze benchmark and artifact truth

**Objective:** Establish the immutable baseline all future batter_hits phases must beat.

**Files likely involved:**

- Read: `backtest_results/mlb_sweep_batter_hits_flat_both_20260413_20260510/sweep_summary.csv`
- Read: `backtest_results/mlb_sweep_batter_hits_flat_over_20260413_20260510/sweep_summary.csv`
- Read: `backtest_results/mlb_sweep_batter_hits_flat_under_20260413_20260510/sweep_summary.csv`
- Read: current batter_hits model artifact metadata under `src/models/mlb/artifacts/<run>/`
- Create: `docs/mlb/batter_hits_phase0_benchmark.md` or GBrain page after review

**Tasks:**

1. Identify exact model artifact used for the three flat sweeps.
2. Read artifact metadata and calibration cutoff.
3. Verify `2026-04-13` start is strictly post-calibration.
4. Save benchmark table with the three configs listed above.
5. Label over-only as exploratory due to no 100+ bet config.

**Exit criteria:**

- Current benchmark is documented with artifact path, calibration cutoff, date window, and sweep commands.
- No future phase can claim improvement without comparing to this benchmark.

---

## Phase 1: Residual, uncertainty, drift, and calibration diagnostics

**Objective:** Identify the binding error source before adding features, without mistaking small-sample noise for signal.

**Primary deliverable:** `scripts/analyze_mlb_batter_hits_residuals.py`

**Default scope:** saved sweep outputs first. Do not join database/context columns in the first implementation. If saved files do not contain enough context to distinguish the binding constraint, Phase 1 should recommend a scoped Phase 1B safe-context join rather than silently expanding scope.

**Inputs:**

- `--sweep-dir backtest_results/mlb_sweep_batter_hits_flat_under_20260413_20260510`
- optional comparison dirs for both/over:
  - `--both-dir backtest_results/mlb_sweep_batter_hits_flat_both_20260413_20260510`
  - `--over-dir backtest_results/mlb_sweep_batter_hits_flat_over_20260413_20260510`
- `--min-bets 30`
- `--production-min-bets 100`
- `--bootstrap-samples 1000`
- `--ci-level 0.95`
- `--flat-stake 100`
- `--high-edge-threshold 0.15`
- `--output-dir backtest_results/mlb_batter_hits_residual_diagnostics_<timestamp>`

**Core outputs:**

- `full_prediction_calibration.csv`
- `bet_probability_calibration_selected.csv`
- `edge_bin_performance.csv`
- `direction_summary.csv`
- `line_bucket_residuals.csv`
- `time_bucket_performance.csv`
- `bookmaker_performance.csv`
- `high_edge_win_loss_comparison.csv`
- `high_edge_bets.csv`
- `clv_proxy.csv` if line/price movement or a defensible proxy can be computed from saved files
- `park_residuals.csv` only if park is already available in saved files
- `starter_handedness_residuals.csv` only if starter handedness is already available in saved files
- `team_opponent_residuals.csv` only if team/opponent is already available in saved files
- `diagnostic_summary.md`

**Uncertainty requirements:**

Every reported ROI, flat ROI, hit rate, profit, calibration gap, and observed-vs-predicted gap must include block-bootstrap confidence intervals:

- Use at least 1000 bootstrap resamples by default.
- Use percentile intervals at the configured CI level.
- Use a block bootstrap by `game_date` for ROI/hit-rate/profit metrics: resample dates with replacement, then include all bets/predictions from sampled dates. This avoids falsely tight iid CIs because same-day bets share slate/weather/park/lineup effects. If `game_date` is missing, fall back to `game_id` blocks; if both are missing, fail closed unless `--allow-iid-bootstrap` is explicitly provided.
- For non-outcome descriptive metrics where block bootstrap is not applicable, use the same block index when possible and label the method.
- Report `n`, `n_blocks`, metric point estimate, `ci_low`, `ci_high`, and `bootstrap_method` for each stratum.
- Add a `sample_status` column:
  - `masked_small_n` for `n < 30`
  - `exploratory` for `30 <= n < 100`
  - `decision_eligible` for `n >= 100`
- The markdown summary must not treat masked/exploratory strata as hard signal. At `n=100`, ROI CIs are still wide; the summary should describe ranges, not just point estimates.

**Multiple-comparison discipline:**

- Phase 1 stratified findings are hypothesis-generating, not confirmatory.
- `diagnostic_summary.md` must state that deciles, line buckets, weekly buckets, bookmaker splits, and high-edge win/loss dimensions are scanned across many comparisons.
- Do not write claims like "bookmaker X is significantly worse" unless a defined statistical criterion is met.
- Any stratified finding that drives a Phase 2 feature decision requires a follow-up confirmation step on a held-out/newer window or a scoped Phase 1B context join.

**Push handling:**

- Pushes are excluded from hit-rate denominators.
- Pushes contribute zero profit and zero ROI numerator.
- Do not treat pushes as half-wins.
- Report push count separately.

**Analysis requirements:**

1. Full prediction calibration from `predictions.csv`, not only placed bets:
   - Compute decile calibration on the full prediction set with actuals.
   - Keep this separate from bet-level edge ROI.
   - For over probabilities, use `over_prob`; for under probabilities, use `under_prob` where applicable.
   - Include mean predicted probability, observed win rate, calibration gap, count, and bootstrap CIs.
   - Purpose: detect model miscalibration that is hidden by the edge-selected bet sample.
   - If full-prediction under calibration is biased while over-side bet volume is absent, explicitly discuss the possibility that the edge is a market-asymmetry artifact: the model may be less wrong than the market rather than absolutely well-calibrated.

2. Selected-bet calibration from `bets.csv`:
   - Compute the same calibration metrics only for placed bets.
   - Label this output as selected-sample calibration.
   - Use it to understand deployed selection behavior, not global model calibration.

3. Edge-bin performance from `bets.csv`:
   - Use adaptive bins by default so tiny fixed-bin slices are not overinterpreted.
   - Start from fixed bins (`0.00-0.05`, `0.05-0.08`, `0.08-0.10`, `0.10-0.12`, `0.12-0.15`, `0.15-0.18`, `0.18-0.22`, `0.22+`) but merge adjacent bins until each non-empty bin has `n >= 20`, or use quantile bins if merging still leaves unstable slices.
   - Report count, hit rate, ROI, flat ROI, profit, average odds, average model probability, average implied probability, average edge, and bootstrap CIs.
   - Purpose: decide whether high edge is monotonic or whether high-edge rollover implies calibration/selection/BL work before features.

4. Direction split:
   - Compare over/under/both when comparison dirs are provided.
   - Report 100+ bet eligibility, but do not equate 100 bets with certainty; include CIs and sample status.
   - Overs remain exploratory until a 100+ bet configuration has stable uncertainty bounds.

5. Line-bucket residuals:
   - Bucket by line (`0.5`, `1.5`, `2.5+`, or data-driven buckets if needed).
   - Include count, predicted win rate, actual win rate, calibration gap, ROI, flat ROI, average edge, and bootstrap CIs.
   - Interpret line-bucket results as candidate evidence for expected-PA/role/selection issues only if sample size and CI support it.

6. Time-bucket drift / time-decay analysis:
   - Split the backtest window into weekly buckets and into thirds.
   - Report count, hit rate, ROI, flat ROI, profit, average edge, and CIs by period.
   - If profit is concentrated early and late-window ROI is flat/negative, recommend retraining cadence, live recalibration, or market-adaptation checks before feature work.
   - If ROI is stable across periods, treat the edge as more durable.

7. High-edge wins vs losses, not losses only:
   - For bets with `edge >= high_edge_threshold`, produce a wins-vs-losses comparison table.
   - Compare observable dimensions available in saved files: line bucket, bookmaker, date/time bucket, odds bucket, model probability bucket, implied probability bucket, and edge bucket.
   - Use an operational definition of "differs materially":
     - continuous dimensions (`odds`, `edge`, `model_prob`, `implied_prob`): Mann-Whitney U test on wins vs losses, material if `p < 0.01`.
     - categorical dimensions (`bookmaker`, `line_bucket`, `time_bucket`, odds/model/edge buckets): chi-square test when expected counts are adequate, otherwise Fisher exact for 2x2 cases where practical; material if `p < 0.01`.
   - Report test name, statistic where available, p-value, n_wins, and n_losses.
   - This p-value threshold is a heuristic screen, not confirmation, because Phase 1 scans many comparisons.
   - If high-edge wins and losses are indistinguishable under this rule, classify losses as variance and do not select a feature family from anecdotes.
   - If wins/losses differ materially on dimension X, that dimension becomes the leading hypothesis for the binding constraint and still needs confirmation.

8. Bookmaker stratification:
   - Produce a standalone bookmaker performance table, not only a column in high-edge records.
   - Include bet count, share of total bets, share of profit, share of losses, hit rate, ROI, flat ROI, average edge, and CIs.
   - Flag fragility if any single bookmaker accounts for `>40%` of total profit or `>50%` of total losses.
   - Also flag if one bookmaker dominates high-edge losses.

9. CLV or CLV proxy:
   - Hardcoded definitions only; do not let runtime invent a "defensible" proxy.
   - If `odds_at_bet` and `odds_at_close` exist, compute true odds CLV in cents and label `clv_type=true_odds_clv_cents`.
   - If `odds_at_bet` and `consensus_close_implied_prob` exist, compute implied-probability CLV and label `clv_type=implied_clv_proxy`.
   - If neither closing field exists, emit no CLV table and write in `diagnostic_summary.md`: `CLV unavailable in saved sweeps; Phase 1B line-history/closing-price join is the next gate before Phase 2 feature work.`
   - Report correlation between predicted edge and CLV/proxy when available.
   - If predicted edge correlates with positive CLV, treat that as stronger signal than short-run ROI. If ROI is positive but CLV/proxy is absent or uncorrelated, downgrade confidence.
   - Until CLV is available, classify the system as plausibly profitable but unconfirmed; do not proceed to feature work.

10. Drift detection rule:
   - Compute Spearman rank correlation between week index and weekly ROI.
   - Treat non-significant negative correlations as underpowered warnings, not stability evidence. With ~4 weekly buckets, p-values have very low power.
   - If correlation is negative with `p < 0.05`, set `decay_detected=true` and recommend retraining cadence / live recalibration as a prerequisite before feature work.
   - If Spearman `r <= -0.7` at any sample size, set `decay_watchlist=true` and trigger retraining-review watchlist even if p-value is not significant.
   - If `-0.7 < r <= -0.5`, set `decay_watchlist=true` with severity `early_warning_underpowered`.
   - Also flag decay if the late-third ROI CI excludes the early-third point estimate on the downside.
   - Otherwise label drift as `not_detected`, while still treating the window as short.

11. Optional residual stratification only from columns already present:
   - park
   - team/opponent
   - starter handedness
   - recency bucket
   - If these columns are absent, write a limitation and recommend Phase 1B safe-context join only if core diagnostics cannot pick a next action.

**Decision structure in `diagnostic_summary.md`:**

The summary must produce a two-level recommendation:

- **Primary decision:** exactly one of:
  1. proceed to expected-PA / lineup-volume feature design,
  2. proceed to starter matchup feature design,
  3. proceed to park/environment feature design,
  4. prioritize calibration/selection/BL policy before features,
  5. no feature work yet; collect more data or extend diagnostics.
     - If this is selected, define a revisit trigger such as `+30 calendar days`, `+200 additional under-only bets`, or a specific Phase 1B context artifact.
- **Prerequisite, if needed:** one required next step before the primary decision can be implemented, e.g. Phase 1B safe-context join, artifact cutoff verification, or retraining-cadence check.

**Exit criteria:**

- Every material metric includes block-bootstrap CIs, `n_blocks`, bootstrap method, and sample-size status.
- Benchmark and edge-bin rows include average odds, average decimal odds, average implied probability, and average edge so hit-rate-to-ROI math is auditable.
- Full-prediction calibration and selected-bet calibration are reported separately.
- High-edge wins and losses are compared with defined statistical tests; losses are not inspected alone.
- High-edge win/loss comparison includes win-group and loss-group mean/median values for continuous dimensions so the direction of any Mann-Whitney result is interpretable.
- Multiple-comparison caveat is explicit: stratified findings are hypothesis-generating and require confirmation before driving feature work.
- Time-bucket performance is reported and interpreted with the defined Spearman / third-CI decay rule, plus underpowered drift watchlist flags.
- Bookmaker concentration/fragility is reported using `>40%` profit-share or `>50%` loss-share thresholds.
- CLV or hardcoded CLV-proxy section is included when possible; if not possible, the summary states the exact missing columns and elevates Phase 1B line-history join as the next gate before Phase 2.
- Diagnostic summary names one primary decision plus at most one prerequisite.
- If the decision is to collect more data, the summary defines a revisit trigger.
- If no bucket has enough count or clear uncertainty-adjusted gap, do not add features; extend diagnostics or collect more data first.

**Minimum test coverage for implementation:**

- Block-bootstrap CI sanity on a known synthetic distribution.
- Push exclusion correctness for hit rate and ROI/profit.
- Adaptive bin merging with sparse synthetic edge bins.
- `sample_status` threshold transitions at `n=29`, `n=30`, `n=99`, and `n=100`.
- High-edge win/loss statistical test selection for continuous and categorical dimensions.
- CLV behavior for true CLV columns, implied-CLV proxy columns, and unavailable CLV columns.
- Drift detection rule for negative weekly ROI trend and stable weekly ROI.
- Output CSV schema validation for required columns.

---

## Phase 1B: Line-history / CLV integration gate

**Objective:** Determine whether the under-only `batter_hits` edge is skill that beats the market, or short-window ROI/variance caused by plus-odds selection. No Phase 2 feature expansion is allowed until this gate is complete.

**Primary question:** Do higher predicted edges produce positive closing-line value (CLV), especially in the plus-odds bands where high-edge losses cluster?

**Approved posture:** Phase 1B is approved for coding. Keep scope limited to line-history/closing-price joins and CLV diagnostics; do not add model features, retrain models, or change selection rules in this phase.

### Data source and matching strategy

- Primary data source: The Odds API historical odds / line-history endpoint, using bookmaker-level prices when available.
- Primary match: same-book close.
  - Match each saved bet to the same bookmaker's closing price for the same player, market, game, side, and line.
  - This is the decision-grade CLV measurement because the bet was taken at that book's posted price.
- Fallback match: consensus close across available books.
  - Use when the same bookmaker's close is missing, stale, or unmatchable.
  - Consensus is more stable but must be labeled as fallback, not silently mixed into same-book CLV.
- Pull requirements:
  - Closing price/line near first pitch is required.
  - Opening price/line should also be captured when available to verify line-movement direction and detect stale/abnormal book behavior.
  - Full line history is allowed only if needed to compute bet-time or +15 minute snapshots; do not overexpand into unrelated odds history analysis.
- Output must retain match provenance:
  - `clv_source` = `same_book_close`, `consensus_close_fallback`, or `unmatched`.
  - `bookmaker_at_bet`, `bookmaker_at_close`, matched event/player/market IDs where available.
  - `line_at_bet`, `odds_at_bet`, `line_at_close`, `odds_at_close`, and whether the line changed.

### Production timing reality

- MLB close is approximated as first pitch / market lock.
- Production bets are expected to be placed roughly 1-3 hours before first pitch; therefore Phase 1B must compute two timing views:
  1. **Bet-to-close CLV:** price/line at saved bet time versus close. This is the gold-standard market-skill measurement.
  2. **Bet-time to +15 minute movement:** price/line at saved bet time versus the closest available snapshot 15 minutes later. This is a stability check for whether the bet price immediately moves favorably or whether the signal is fragile/stale.
- If exact bet timestamps are unavailable in saved artifacts, fail closed for +15 minute CLV and report the missing timestamp requirement. Do not infer bet time from game date alone.

### CLV definitions

For UNDER bets, compute CLV in both American-odds cents and implied-probability terms where possible:

- `same_book_clv_cents = bet_american_odds - close_american_odds` for same-line UNDER bets; positive means the taken UNDER price beat the close. Treat American-odds cents as a reporting convenience, not the only decision metric, because odds are not linear across zero.
- `clv_implied_prob = close_implied_probability - bet_implied_probability` after normalizing for the same UNDER side; positive means the market moved toward the model's side. This is the primary numeric CLV metric for decision gates.
- If the line changes, include a separate line-movement classification instead of pretending odds-only CLV is comparable:
  - `same_line_odds_clv`
  - `favorable_line_move`
  - `unfavorable_line_move`
  - `line_changed_unscored_odds_clv` when no defensible same-line conversion exists.

### Predefined decision rules

All confirmatory Phase 1B metrics must use block-bootstrap by `game_date` or `game_id`; iid bootstrap is not allowed unless explicitly marked exploratory and excluded from the decision.

Signal is confirmed only if both primary gates pass on the raw-config under-only benchmark (`edge=0.15`, 250 bets):

1. **Edge-to-CLV monotonicity:** Spearman correlation of predicted edge versus realized bet-to-close CLV is positive and the block-bootstrap CI lower bound is `> 0`.
2. **Mean CLV:** Mean bet-to-close CLV per bet is positive and the block-bootstrap CI excludes zero on the positive side.

Required stratification:

- Report CLV by plus-odds band:
  - `-110_to_+99`
  - `+100_to_+149`
  - `+150_plus`
- The `+150_plus` band is the suspected kill-zone because high-edge losses clustered around longer plus odds in Phase 1.
- If aggregate CLV is positive but `+150_plus` CLV is negative or has a CI entirely below zero, do not treat this as a global model failure; recommend excluding or separately validating that odds band.

### Predefined kill / restrict criteria

Interpret Phase 1B before seeing the numbers using these rules:

1. If mean bet-to-close CLV is `<= 0` or the CLV CI includes zero, and edge-to-CLV correlation is non-positive or CI includes zero:
   - Treat Phase 0/1 ROI as unconfirmed variance or market-selection artifact.
   - Stop feature expansion.
   - Continue paper/live collection until `+200` additional under-only bets or `+30` calendar days, whichever comes first, then rerun Phase 1B.
2. If aggregate CLV is positive but one plus-odds band has negative CLV with CI excluding zero:
   - Restrict or kill that band before any model feature work.
   - Rerun the under-only benchmark excluding the band and compare flat ROI, drawdown, and CLV.
3. If mean CLV is positive with CI excluding zero, but edge-to-CLV correlation is weak/uncertain:
   - Skill is plausible but ranking quality is unconfirmed.
   - Do not expand features yet; first review edge thresholding / BL selection policy and require another confirmation window.
4. If both primary gates pass and no plus-odds kill-zone fails:
   - Phase 2 feature work may proceed, using Phase 1 residual diagnostics to select the feature family.

### Required outputs

- `clv_matches.csv`: one row per saved bet with match provenance, line/odds snapshots, CLV fields, and unmatched reason.
- `clv_summary.csv`: aggregate CLV metrics with block-bootstrap CIs, `n`, `n_blocks`, and match-source counts.
- `clv_by_edge_bin.csv`: edge-to-CLV relationship and monotonicity diagnostics.
- `clv_by_plus_odds_band.csv`: required kill-zone stratification.
- `clv_by_bookmaker.csv`: same-book coverage and bookmaker-specific CLV/staleness checks.
- `clv_timing_stability.csv`: bet-to-close versus bet-to-+15-minute movement.
- `phase1b_clv_summary.md`: concise decision report that states pass/restrict/stop and the exact next phase allowed.

### Exit criteria

- Same-book close is attempted first and consensus fallback is separately labeled.
- Bet-to-close CLV and +15-minute stability view are both computed, or missing timestamp/history blockers are explicitly reported.
- Spearman edge-to-CLV and mean CLV use block-bootstrap CIs by date/game.
- Plus-odds bands are reported with the predefined `+150_plus` kill-zone rule.
- The markdown summary applies the predefined kill/restrict/proceed criteria without redefining success after seeing the data.
- If Phase 1B fails or is inconclusive, Phase 2 remains blocked.

### Phase 1B observed result and follow-up order

Current quote-clean Phase 1B result: `ranking_quality_unconfirmed`; Phase 2 remains blocked.

One-line posture: **plausibly profitable, ranking unconfirmed, production-readiness unconfirmed.** This lane is paper-only until intraday quote stability is validated.

Major findings to preserve:

1. **Legacy-vs-quote-clean discrepancy is a first-order result.**
   - Legacy raw under-only `edge=0.15`: 250 bets, 56.8% hit rate, +31.56% ROI, Sharpe 3.477, MaxDD 3.95%.
   - Quote-clean raw under-only `edge=0.15`: 719 bets, 45.9% hit rate, +8.11% ROI, Sharpe 0.88, MaxDD 13.3%.
   - Treat prior conclusions based on legacy-mode sweeps as suspect until rerun in quote-clean mode, including BL tuning and feature-improvement claims.
   - First follow-up is to explain the ROI drop and bet-count increase by comparing line/odds/edge distributions, bet overlap, quote coverage, and whether edge-threshold semantics shifted.

2. **Mean CLV passes, edge ranking does not.**
   - Quote-clean mean implied-prob CLV: `+0.004326`, block-bootstrap CI `[+0.001548, +0.007867]`.
   - Spearman(edge, CLV): `-0.020993`, block-bootstrap CI `[-0.105156, +0.072703]`.
   - Interpret raw edge as a possible binary bettable/not-bettable discriminator, not a calibrated quality score.
   - Do not use edge magnitude for aggressive sizing until a ranking signal is found.

3. **The `+150_plus` kill-zone hypothesis is not supported by CLV.**
   - `+150_plus`: 375 bets, mean CLV `+0.004776`, CI `[+0.002154, +0.007650]`.
   - Phase 1 win/loss loss-clustering at long plus odds should be treated as variance unless contradicted by future CLV evidence.
   - Do not kill `+150_plus` based on the earlier 250-bet win/loss stratification.

4. **+15 minute stability is a production-readiness gap.**
   - The current historical snapshot cadence did not provide matchable same-book/same-line 13:30→13:45 snapshots for quote-clean bets.
   - Start/verify 5-minute-cadence odds snapshot capture going forward.
   - Keep this lane paper-only until +15/+30/+60 minute stability is measured on new paper bets.

Required follow-up order before Phase 2:

1. **Legacy-vs-quote-clean discrepancy audit.** Explain the 4x ROI drop and 3x bet-count increase. Document whether legacy mode used unavailable prices, quote-clean admitted thin-coverage bets, edge distribution shifted, or some combination.
2. **Edge-ranking alternatives.** Compare CLV by `model_prob`, `implied_prob`, `logit(model_prob)-logit(implied_prob)`, and `model_prob * abs(edge)`. If none rank CLV, use flat/near-flat sizing for bettable signals.
3. **Intraday stability capture.** Turn on/verify 5-minute snapshot capture and rerun stability after 4-6 weeks or enough new paper bets.
4. **Quote-clean BL sweep.** Rerun BL grid in quote-clean mode; do not assume legacy-optimal `tau=0.90 z=0.25 mw=0.65` remains optimal.

---

## Phase 2: Expected-PA submodel, if diagnostics show PA/lineup-volume gap

**Objective:** Add one explicit expected plate appearances feature rather than many proxy lineup features.

**When this phase is allowed:**

- Phase 1 shows calibration/ROI errors concentrated by lineup spot, PA proxy, line bucket, team scoring context, or home/away PA-loss risk.

**Feature design:**

Add one primary model output:

- `expected_pa`

Optional uncertainty output only if clearly useful:

- `expected_pa_q25` or `expected_pa_sd`

Do not add separate raw features like five lineup indicators into the hits model unless the PA model needs them internally.

**Expected-PA model inputs:**

- confirmed lineup spot when available
- projected lineup spot when confirmed lineup is unavailable
- confirmed lineup flag
- home/away
- team implied runs
- game total
- spread/runline
- recent role / recent starts if temporally safe
- doubleheader flag only if diagnostically justified

**Model candidates:**

- Start simple: Poisson or negative-binomial style model if practical.
- Accept a simple gradient model if existing codebase patterns make that cheaper, but keep output as a single `expected_pa` feature.

**Validation:**

- PA MAE/RMSE by lineup spot.
- Calibration of expected PA buckets.
- Batter_hits under-only backtest against Phase 0 benchmark.

**Exit criteria:**

- `expected_pa` improves the Phase 1 identified PA/volume calibration gap.
- Under-only flat benchmark improves or stays equal while calibration improves.
- If betting improves but calibration worsens, do not promote without review.

---

## Phase 3: Starter quality-of-contact matchup with empirical-Bayes shrinkage

**Objective:** Add a small starter matchup feature class only if starter/opponent residuals are the binding gap.

**When this phase is allowed:**

- Phase 1 or post-Phase-2 diagnostics show errors concentrated by opposing starter quality, starter handedness, or contact-allowed buckets.

**Feature design:**

Small compressed set, not a 12+ split matrix:

- `starter_xba_allowed_regressed`
- `starter_hard_hit_allowed_regressed`
- `starter_k_rate_regressed`
- optional: `starter_contact_allowed_regressed`
- optional single handedness adjustment: `starter_handedness_matchup_delta_eb`

**Empirical Bayes requirement:**

- No raw platoon splits shipped directly.
- Shrink starter/batter handedness splits toward player overall and league handedness means.
- Report sample sizes and shrinkage weights.

**Validation:**

- Feature coverage and missingness.
- Residual improvement in starter quality buckets.
- Under-only flat sweep vs previous phase.
- Confirm overs still exploratory unless 100+ bets emerge.

**Exit criteria:**

- Starter residual bucket improves.
- No broad calibration regression.
- At least one 100+ bet under-only config beats or clearly complements prior benchmark.

---

## Phase 4: Regressed park hit factor

**Objective:** Add a small park context signal if park residuals are material.

**When this phase is allowed:**

- Diagnostics show park-specific calibration gaps with enough sample, or Phase 3 leaves park as the largest remaining error bucket.

**Feature design:**

- `park_hit_factor_regressed`
- optional for later markets only: `park_extra_base_factor_regressed`

Do not add a large weather/park bundle yet.

**Data requirements:**

- Multi-year park factor.
- Regressed toward league average.
- Park/team relocation/name changes handled explicitly.

**Validation:**

- Park-bucket residual improvement.
- Under-only flat sweep vs prior phase.
- Edge-bin monotonicity preserved.

**Exit criteria:**

- Park gap improves without weakening production edge bins.

---

## Phase 5: Residual-driven selector for next feature class

**Objective:** Decide whether pitch mix, bullpen, defense, or weather earns the next slot based on remaining errors.

**This is a decision phase, not implementation.**

**Inputs:**

- Phase 1 diagnostics
- Post-Phase-2 diagnostics
- Post-Phase-3 diagnostics
- Post-Phase-4 diagnostics

**Candidate next classes:**

1. Pitch mix matchup
2. Bullpen context
3. Defense context
4. Weather/roof context
5. Stop feature work and improve calibration/BL/selection instead

**Decision rules:**

- If edge-bin ROI rolls over above `0.15`, prioritize calibration/selection, not features.
- If high-edge losses cluster by starter pitch profile, choose pitch mix.
- If high-edge losses cluster after early starter exits or weak/strong bullpens, choose bullpen.
- If high-edge losses cluster by batted-ball/defense context, choose defense.
- If park/weather buckets dominate and weather is available before bet time, choose weather.

**Exit criteria:**

- A short decision memo picks exactly one class for Phase 6.

---

## Phase 6: Compressed pitch-mix matchup, only if earned

**Objective:** Add pitch-mix context without sparse rolling pitch-type overfit.

**When this phase is allowed:**

- Phase 5 selects pitch mix based on residual evidence.

**Feature design:**

Do not add raw rolling batter-vs-slider windows.

Potential compressed features:

- `pitch_mix_matchup_score_regressed`
- `primary_pitch_matchup_score_regressed`
- optional: `velo_band_matchup_score_regressed`

**Stability rules:**

- Prefer season+career regressed rates.
- Shrink sparse batter pitch-type performance toward league/player overall.
- Report coverage and shrinkage.

**Validation:**

- Pitch-mix residual bucket improvement.
- Under-only flat sweep vs prior phase.
- Feature importance stability check.

**Exit criteria:**

- Meaningful calibration improvement in pitch-mix buckets with no broad overfit signal.

---

## Phase 7: Bullpen/defense/weather context, one subphase at a time

**Objective:** Add late-context features only if diagnostics justify them.

**Important:** Do not implement all three together.

### Phase 7A: Bullpen context

Potential compressed features:

- `opp_bullpen_xba_allowed_regressed`
- `opp_bullpen_fatigue_index`
- `starter_early_exit_risk`

### Phase 7B: Defense context

Potential compressed features:

- `opp_defensive_efficiency_regressed`
- optional batted-ball-aligned score if batter batted-ball profile exists cleanly

### Phase 7C: Weather/roof context

Potential compressed features:

- `hit_environment_score`
- components only if needed internally: temp, wind, roof, humidity/air density

**Validation for each subphase:**

- Re-run diagnostics and backtests after each subphase separately.
- Keep only subphases that improve residuals and betting quality.

**Exit criteria:**

- Each accepted subphase earns its place independently.

---

## Phase 8: Market/operations/calibration hardening

**Objective:** Improve production usability and avoid subtle market/timestamp leakage.

**Market principle:**

- Market info should generally remain in implied probability, edge, and BL/shrinkage logic.
- Do not add raw market movement features to the predictive model unless there is a strict timestamp contract and a clear reason BL cannot handle the signal.

**Potential work:**

1. Timestamp contract for line snapshots:
   - opening line time
   - current line as of bet placement time
   - no final close unless production uses the same timestamp
2. Calibration layer review:
   - if edge-bin ROI rolls over at high edge, investigate calibration before adding features
3. Production selection policy:
   - under-only default
   - over-only exploratory tracker
   - raw high-edge vs BL risk-control policy
4. Monitoring:
   - daily edge-bin performance
   - calibration drift
   - under/over contribution
   - flat-stake paper P&L tracker

**Exit criteria:**

- Production/paper policy is explicit.
- Timestamp assumptions are documented.
- Monitoring catches calibration drift before model promotion.

---

## Work cadence across chats

Each chat should handle one bounded phase or subphase:

1. Start with `gameflow resume`.
2. Load this plan.
3. Confirm the active phase and exit criteria.
4. Do only that phase's discovery/implementation/evaluation.
5. Save results and update the plan/GBrain if the phase changes the roadmap.

Suggested next chat task:

> Implement Phase 1 residual diagnostics for current batter_hits under-only flat sweep, generate diagnostic summary, and recommend exactly one next feature class or calibration action.

---

## Open review questions for Chase

1. Should Phase 0 benchmark documentation live in `docs/mlb/`, GBrain, or both?
2. For Phase 1 diagnostics, should we include joins for park/starter/team context immediately, or first analyze only what is already present in saved `predictions.csv` / `bets.csv`?
3. For eventual paper/live settings, should raw under-only `edge=0.15` and BL under-only `tau=0.90 z=0.25 mw=0.65 edge=0.10` both be tracked, or should one be the primary candidate?
4. Do we want to preserve over-only as a separate watchlist despite sub-100 volume, or ignore it until more data accumulates?
