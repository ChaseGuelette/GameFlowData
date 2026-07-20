# MLB Pitcher K Phase 3B+ Pitcher-Side Extremes Roadmap

> **Historical design source:** The five-feature pitcher-side-downside thesis is retained,
> but current ordering, fixed-hyperparameter discovery rules, validation gates, and commands
> are canonical in [`mlb_pitcher_k_ablation_roadmap.md`](mlb_pitcher_k_ablation_roadmap.md).

Date: 2026-05-14
Status: planning / validation-gated

## Purpose

This document supersedes the broad post-Phase-3A feature-expansion idea with a tighter, validation-gated roadmap for MLB pitcher strikeout model improvement.

The core lesson from Phase 3A is that broad lineup/contact context can pull predictions toward a market-priced `lineup K% x expected batters faced` anchor. That hurt the Phase 2 edge because Phase 2's strongest under bets were contrarian against that baseline. Future feature work should therefore target pitcher-side extreme outcomes: short outings, quick hooks, pitch-count caps, degraded stuff, fatigue, or role uncertainty.

Do not start Phase 3B implementation until the Phase 2 baseline validation gate below is accepted.

## Relevant invariants and lessons

- Probabilities must use empirical CDF from Monte Carlo samples: `(samples > line).mean()`.
- Do not globally recalibrate away low-tail behavior; Q10 behavior may be edge-bearing.
- Feature selector inclusion is not a causal ablation or promotion gate.
- Validate correlated feature families with force-include / force-exclude comparisons and downstream betting results.
- Legacy line aggregation is hypothesis-generating only; quote-clean replay is required before production/promotion decisions.
- Use 100+ bets as the minimum meaningful-volume threshold for headline claims unless explicitly labeled as exploratory.

## Reconciliation with Phase 3A document

Existing document:

- `docs/development_docs/mlb_pitcher_k_phase3a_lineup_contact_expansion.md`

Phase 3A was useful as an experiment but should not be promoted. The diagnostic artifacts are saved at:

- `docs/analysis/mlb_phase3a_agreement_20260513/README.md`
- `docs/analysis/mlb_phase3a_agreement_20260513/final_causal_form.txt`

Phase 3A final causal form:

> Phase 3A lost because 46/110 (41.8%) of Phase 2 BL under bets were edge-compressed, including 34/70 (48.6%) of Phase 2 winners; 31/110 dropped below threshold and 10/110 flipped or were directionally invalidated. Mean under-edge drop was 0.0128, while the same BL config ROI fell from +34.68% to +15.08%; Phase 3A-only added bets returned -22.00%. Phase 3A added-bet ROI was positive only in the tiny high lineup/team-delta bucket, so that remains a hypothesis, not a design decision.

Interpretation update:

- Do not treat the +50.87% ROI on 10 high-delta added bets as evidence to build a lineup gate now.
- Do not add more team-context / lineup-context features until a future larger sample can test that hypothesis.
- Phase 3B should not be “Phase 3A but gated.” It should be a separate pitcher-side-extremes experiment.

## Highest-priority pending gate: Phase 2 non-overlap validation

The Phase 2 baseline must be sanity-checked before any Phase 3B feature work. If Phase 2's headline +34.68% BL ROI is mostly short-window luck or legacy-line artifact, future comparisons are comparing against noise.

### Validation run completed: earlier 2026 pre-window

Window:

- 2026-03-25 through 2026-04-12

Important caveat:

- This is non-overlap with the 2026-04-13 through 2026-05-10 Phase 2 / Phase 3A comparison window.
- It is not a pristine holdout if the artifact used 2026 calibration through 2026-04-12.
- Treat this as a pre-window sanity check, not final production-grade proof.

Artifact:

- `src/models/mlb/artifacts/mlb_run_20260513_111207`

Legacy-line output directory:

- `backtest_results/mlb_sweep_pitcher_k_phase2_nonoverlap_prewindow_20260325_20260412`

Command:

```bash
venv/Scripts/python.exe src/backtesting/mlb/run_mlb_sweep.py \
  --start 2026-03-25 \
  --end 2026-04-12 \
  --model-dir src/models/mlb/artifacts/mlb_run_20260513_111207 \
  --stats pitcher_strikeouts \
  --direction under \
  --tau none 0.90 \
  --edge 0.02 0.05 \
  --z-max 0.25 \
  --max-weight 0.80 \
  --kelly 0.125 \
  --local \
  --output-dir backtest_results/mlb_sweep_pitcher_k_phase2_nonoverlap_prewindow_20260325_20260412
```

Legacy-line results:

| Config | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---|---:|---:|---:|---:|---:|---:|
| no BL, edge 0.02 | 178 | 54.8% | +17.65% | +$4,506 | 1.63 | 11.7% |
| no BL, edge 0.05 | 132 | 57.7% | +22.16% | +$5,499 | 2.26 | 9.5% |
| BL tau 0.90 / z 0.25 / mw 0.80, edge 0.02 | 127 | 53.3% | +13.78% | +$2,131 | 1.30 | 13.3% |
| BL tau 0.90 / z 0.25 / mw 0.80, edge 0.05 | 81 | 53.8% | +17.58% | +$2,441 | 1.90 | 12.1% |

Quote-clean output directory:

- `backtest_results/mlb_sweep_pitcher_k_phase2_nonoverlap_prewindow_quote_clean_20260325_20260412`

Quote-clean command:

```bash
venv/Scripts/python.exe src/backtesting/mlb/run_mlb_sweep.py \
  --start 2026-03-25 \
  --end 2026-04-12 \
  --model-dir src/models/mlb/artifacts/mlb_run_20260513_111207 \
  --stats pitcher_strikeouts \
  --direction under \
  --tau none 0.90 \
  --edge 0.02 0.05 \
  --z-max 0.25 \
  --max-weight 0.80 \
  --kelly 0.125 \
  --local \
  --quote-clean \
  --output-dir backtest_results/mlb_sweep_pitcher_k_phase2_nonoverlap_prewindow_quote_clean_20260325_20260412
```

Quote-clean results:

| Config | Bets | Hit Rate | ROI | Profit | Sharpe | MaxDD |
|---|---:|---:|---:|---:|---:|---:|
| no BL, edge 0.02 | 178 | 56.4% | +9.77% | +$2,124 | 0.88 | 9.9% |
| no BL, edge 0.05 | 144 | 57.6% | +11.13% | +$2,417 | 1.11 | 9.3% |
| BL tau 0.90 / z 0.25 / mw 0.80, edge 0.02 | 130 | 53.6% | +8.74% | +$1,115 | 0.78 | 10.5% |
| BL tau 0.90 / z 0.25 / mw 0.80, edge 0.05 | 86 | 53.6% | +11.23% | +$1,387 | 1.22 | 9.3% |

### Baseline interpretation

The Phase 2 edge did reproduce directionally on an earlier non-overlap window, but the +34.68% BL headline should not be treated as the real forward bar.

Working bar for future feature phases:

- Legacy mode: Phase 2 is strong but inflated; useful for apples-to-apples experiment comparison only.
- Quote-clean mode: Phase 2 appears directionally profitable but much closer to an +8% to +12% ROI strategy on the checked pre-window.
- Phase 3B must clear Phase 2 under the same line-selection mode and same window. It should not be judged only against the +34.68% legacy headline.

Before live/promotion decisions, rerun Phase 2 and candidate phases on quote-clean mode and, where available, CLV.

## Phase 3B: five-feature pitcher-side-extremes batch

Goal:

Add exactly five cheap, interpretable features that all target pitcher-side downside / short-outing risk. These features should push predictions down for contrarian unders rather than compete with the Phase 2 edge by refining expected K from team/lineup context.

Do not expand this list during Phase 3B.

### Feature 1: `manager_starter_short_hook_rate_l30`

Definition:

- Team-level prior-30-day rate of starts where the starter was pulled before completing 5 IP and threw fewer than 80 pitches.
- Manager is implicit through team because MLB manager changes are rare mid-season.
- Time-safe: use only games before the target date; implement with shift(1) or equivalent date filter.

Why included:

- Canonical quick-hook feature.
- Directly targets reduced pitcher opportunity / fewer batters faced.

Do not add in 3B:

- eight variants of manager hook rate.
- score-state hook splits.

### Feature 2: `pitcher_pct_starts_under_5_ip_l10`

Definition:

- Pitcher's prior-10-start fraction of starts with fewer than 5.0 IP.
- Use starts only, not relief appearances.
- Time-safe: target game excluded.

Why included:

- Thresholded tail version of average IP.
- Captures whether the pitcher specifically fails to work deep, not merely a smoothed average.

### Feature 3: `pitcher_fastball_velo_delta_l3_vs_szn`

Definition:

- Pitcher's average fastball velocity over last 3 appearances/starts minus season-to-date average fastball velocity before target date.
- Prefer starts-only if current pitcher K model's feature population is starts-only; otherwise document exact inclusion rule.

Why included:

- Continuous stuff-degradation signal.
- Drops in velocity predict fewer Ks and/or shorter outings.

Do not add in 3B:

- 1 mph binary flag.
- 2 mph binary flag.
- multiple redundant velocity windows.

Trees can learn thresholds from the continuous feature.

### Feature 4: `team_bullpen_pitches_last_3d`

Definition:

- Sum of team bullpen pitches thrown over the prior 3 calendar days before target game.
- Exclude starter pitches.
- Time-safe: target game excluded.

Why included:

- Bullpen state affects starter leash.
- For under-only validation, the important test is whether bullpen availability / fatigue helps identify quick-hook or extended-leash contexts.

Caveat:

- Direction may be context-dependent. Tired bullpen can extend leash; fresh bullpen can enable quick hook. Keep only this simple feature in 3B and let the model/backtest decide.

### Feature 5: `pitcher_left_last_start_early_flag`

Definition:

Binary flag:

- 1 if the pitcher's previous start ended significantly below his season-to-date average IP before that start.
- Suggested initial threshold: previous-start IP <= season-to-date average IP - 1.5 innings.
- Alternative if easier: previous start under 4.0 IP when season-to-date average before that start was at least 5.0 IP.
- Use starts only.
- Time-safe: target game excluded.

Why included:

- Captures injury exit, shelling, hidden pitch-count reduction, or role demotion.
- High signal-to-noise relative to effort.

## Phase 3B implementation discipline

Feature count:

- Exactly 5 new features.
- Roughly small expansion from Phase 2, not a Phase 3A-sized feature block.

Implementation rules:

- No lineup/contact main effects.
- No team K-rate / opponent contact-rate refinements.
- No hand-crafted pitch-mix interactions.
- No prior-model-output features.
- No market-derived favorite/spread/total features.
- No global calibration offsets.

Validation protocol:

1. Run Phase 2 baseline on the target window and line mode.
2. Train Phase 3B with only the five new features added.
3. Retune hyperparameters for the new feature count.
4. Run raw under-only sweep.
5. Run focused BL under-only sweep.
6. Enforce 100+ bet threshold for headline decisions.
7. Run paired-bet diagnostic vs Phase 2.
8. Confirm whether Phase 3B helped low-line contrarian unders specifically.
9. Run force-include / force-exclude family comparison if selector behavior is ambiguous.
10. Repeat under quote-clean mode before any promotion decision.

Success pattern:

- Similar or lower bet count than Phase 2 at comparable thresholds.
- ROI and Sharpe improve or at least preserve Phase 2 while reducing drawdown.
- Paired diagnostic shows Phase 2 compressed/flipped winner buckets shrink, not grow.
- Added bets are explained by short-outing / degradation flags, not by central projection smoothing.

Failure pattern:

- Bet volume rises while ROI falls.
- Phase 2 winners get compressed below threshold.
- New features improve aggregate calibration but damage low-tail betting performance.
- Added bets are mostly marginal/noisy.

If Phase 3B fails, stop and run the diagnostic. Do not queue Phase 3C automatically.

## Phase 3C: conditional double-down, not broad expansion

Only build Phase 3C if Phase 3B shows meaningful ROI/Sharpe improvement under the same diagnostic protocol.

Pick exactly one branch based on which Phase 3B group contributed. Do not add all branches.

### If hook features worked best

Add:

- `manager_hook_rate_close_game_l30`
- `manager_hook_rate_with_lead_l30`

Purpose:

- Add simple game-state splits for the manager/team hook signal.

### If velo/stuff features worked best

Add:

- `pitcher_whiff_pct_delta_l3_vs_szn`
- `pitcher_csw_pct_delta_l3_vs_szn`

Purpose:

- Pair velocity deterioration with actual whiff / called-strike deterioration.

### If bullpen features worked best

Add:

- `team_high_leverage_bullpen_pitches_l3d`

Purpose:

- Sharper bullpen availability/fatigue signal than total bullpen pitches.

### If pitcher tail behavior worked best

Add:

- `pitcher_pct_starts_under_18_bf_l10`
- `pitcher_short_start_streak`

Purpose:

- More direct opportunity-tail features.

## Phase 3D: IL / role features

Build later because these likely need new data plumbing.

Candidate features:

- `pitcher_first_start_after_il_flag`
- `pitcher_days_since_il_activation`
- `pitcher_opener_risk_flag`

Data requirements:

- IL transaction table for IL flags.
- Reliable appearance-role history for opener risk.

Opener approximation if no explicit role feed exists:

- fire `pitcher_opener_risk_flag` when recent average appearance length over the prior 14 days is under 2 IP despite a listed start.

If an IL transactions table already exists and is reliable, these can move into Phase 3C after Phase 3B passes.

## Skip or defer

### Skip for now: high risk of repeating Phase 3A

Do not add:

- pitcher matchup vs lineup hand/contact-style features.
- broad opponent/team K-rate refinements.
- score/context market-derived features such as favorite/spread/moneyline.
- hand-crafted pitch-mix interaction products.

Reason:

These are likely to reintroduce the market-priced team-context anchor that compressed Phase 2's contrarian under edge.

### Defer indefinitely: low signal / effort ratio for under-only strategy

Defer:

- schedule/rest context except rare high-impact doubleheader flags.
- dominance-burst features aimed at over prediction.
- prior Phase 2 output as model training features.

Use Phase 2 contrarian distance as a diagnostic/gating concept only, not as a training input for now.

### Conditional / premature

Expected-K anchor residuals are interesting but premature:

- `phase2_q50_minus_lineup_bf_expected_k`
- `phase2_q10_minus_lineup_bf_expected_k`
- `market_line_minus_lineup_bf_expected_k`

Revisit only after Phase 3B/3C establish whether contrarian-vs-anchor is truly the mechanism.

## Throughput discipline

The limiting factor is not engineering time. It is bet sample size.

With roughly 100-150 meaningful under bets per short validation window, a 20- or 30-feature batch is too large to attribute. Five-feature phases give a better chance of learning which signal is real.

Every phase must preserve the same experiment loop:

1. Baseline first.
2. Small feature batch.
3. Retune.
4. Raw and BL sweeps.
5. 100+ bet threshold.
6. Paired-bet diagnostic.
7. Quote-clean replay.
8. CLV / intraday stability before promotion.

## Current next action

Do not implement Phase 3B yet if the team wants a stronger holdout than the pre-window sanity check above.

Recommended next action before code work:

1. Decide whether the 2026-03-25 through 2026-04-12 quote-clean sanity check is sufficient to proceed with Phase 3B planning.
2. If not sufficient, define a truly out-of-sample validation window that is not used for training or calibration.
3. Only then implement the five Phase 3B features.
