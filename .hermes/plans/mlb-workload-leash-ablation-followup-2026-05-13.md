# MLB Workload/Leash Ablation Follow-up Plan

> For Hermes: execute this plan in read-only diagnostic gates first. Do not promote or deploy model changes until the diagnostic gates and at least one independent validation window support the change.

Goal: Determine whether workload/leash information has incremental value for MLB pitcher strikeout betting after the initial predicted-IP/team-hook ablation lost to the leak-free static baseline.

Architecture: Treat the current leak-free static pitcher K model as the production candidate and benchmark. The initial ablation is provisionally rejected, but workload-as-a-concept remains open. Run diagnostics to separate feature redundancy, miscalibration, feature-set churn, and true absence of signal. Only then run a stripped-down single-hook-feature ablation.

Tech stack: Python MLB training pipeline, local Postgres via existing GameFlow `--local` paths / `get_engine(local=True)`, existing sweep harness, artifact manifests under `src/models/mlb/artifacts/`, sweep outputs under `backtest_results/`.

Canonical baseline for this investigation:
- Artifact: `src/models/mlb/artifacts/mlb_run_20260513_111207`
- Training seasons: 2024, 2025
- Calibration season: 2026
- Calibration cutoff: 2026-04-12
- Baseline sweep: `backtest_results/mlb_sweep_20260513_111808`
- Backtest window: 2026-04-13 through 2026-05-10

Initial ablation artifacts:
- Hook only: `src/models/mlb/artifacts/ip_ablation_hook_only/mlb_run_20260513_121138`
- Predicted-IP only: `src/models/mlb/artifacts/ip_ablation_ip_only/mlb_run_20260513_121143`
- Predicted-IP + hook: `src/models/mlb/artifacts/ip_ablation_ip_hook/mlb_run_20260513_121153`

Initial conclusion:
- The current static baseline beats all three ablation variants on ROI, Sharpe, and drawdown.
- Reject this specific ablation for promotion.
- Do not permanently reject workload/leash information as a concept.

---

## Decision principles

1. Do not anchor on +14.58% baseline ROI as long-run expected edge.
   - It is the winner of a 510-cell sweep on one 28-day window.
   - The reliable conclusion is the relative ranking, not the absolute point estimate.
   - Long-run realistic edge should be expected to regress materially.

2. Do not promote based on feature selection alone.
   - Promotion requires risk-adjusted betting improvement: ROI, Sharpe, max drawdown, and bet count.

3. Do not interpret this as “workload is captured.”
   - This only tested this parameterization: `predicted_ip_q25`, `predicted_ip_q50`, `predicted_ip_spread`, `predicted_ip_q25_delta`, and three L30 team hook features.
   - It did not test opponent/bullpen workload or projected reliever K quality.

4. Preserve GameFlow invariants.
   - Probabilities use empirical CDF `(samples > line).mean()`.
   - No global conformal recalibration offsets.
   - Use local DB access patterns; do not use Supabase MCP in main context.

---

## Task 1: Verify feature-set additivity

Objective: Determine whether the ablation variants are truly baseline + new features, or whether feature selection/regularization caused baseline feature churn.

Files:
- Read: `src/models/mlb/artifacts/mlb_run_20260513_111207/feature_manifest.json`
- Read: `src/models/mlb/artifacts/ip_ablation_hook_only/mlb_run_20260513_121138/feature_manifest.json`
- Read: `src/models/mlb/artifacts/ip_ablation_ip_only/mlb_run_20260513_121143/feature_manifest.json`
- Read: `src/models/mlb/artifacts/ip_ablation_ip_hook/mlb_run_20260513_121153/feature_manifest.json`

Steps:
1. Load each feature manifest.
2. Compare per-quantile feature sets.
3. Report:
   - baseline feature count by quantile
   - ablation feature count by quantile
   - baseline features missing from each ablation by quantile
   - new features present by quantile
4. Label the experiment:
   - “pure additive” if baseline features are retained and only new features are added
   - “full retrain ablation with feature churn” if selected feature sets changed materially

Acceptance criteria:
- If feature churn is material, do not attribute performance deltas solely to the new features.

---

## Task 2: Confirm hook feature semantics and leakage safety

Objective: Determine what the L30 hook features actually measure and whether they are pregame-safe.

Files:
- Inspect: `src/models/mlb/mlb_feature_store.py`
- Function of interest: `_get_team_starter_leash_features`
- Downstream call site: `get_player_game_features`

Questions:
1. Are hook features keyed to the starter’s own team or the opposing team?
2. Are they lagged strictly before the current game date?
3. Do they represent manager/team leash tendency, pitcher mix, or opponent behavior?
4. Are current-game innings excluded?
5. Are team IDs/names joined consistently with the starter’s team?

Report:
- Feature semantic label.
- Leak-safe or not leak-safe.
- Any key mismatch risk.
- Whether the mechanism should affect starter K opportunity.

Acceptance criteria:
- If keyed to starter team and lagged before game date, the feature has a plausible mechanism.
- If keyed to opponent or unlagged, reinterpret or fix before further testing.

---

## Task 3: Run hook/IP feature diagnostics on training and calibration data

Objective: Measure whether hook/IP features carry signal for actual innings pitched or strikeout count, and whether the signal is orthogonal to existing rolling IP features.

Data:
- Use local feature-generation path for the same train/cal setup as the leak-free models.
- Training seasons: 2024, 2025
- Calibration season/cutoff: 2026 through 2026-04-12

Candidate features:
- `team_starter_avg_ip_l30`
- `team_starter_short_hook_rate_l30`
- `team_starter_deep_start_rate_l30`
- `team_starter_avg_ip_l10`
- `pitcher_avg_ip_l5`
- `pitcher_avg_ip_szn`
- `pitcher_avg_pitches_per_start_l5`
- `predicted_ip_q25`
- `predicted_ip_q50`
- `predicted_ip_spread`
- `predicted_ip_q25_delta`

Targets:
- `actual_ip`
- `pitcher_strikeouts`
- residual `actual_ip` after controlling for `pitcher_avg_ip_l5` and `pitcher_avg_ip_szn`
- residual `pitcher_strikeouts` after controlling for rolling IP/K features if feasible

Metrics:
- Pearson correlation
- Spearman correlation
- mutual information with `actual_ip`
- mutual information with `pitcher_strikeouts`
- incremental/residual correlation after controlling for rolling IP

Acceptance criteria:
- If L30 hook features have near-zero actual_ip and residual actual_ip signal, shelf hook features.
- If exactly one hook feature has meaningful residual signal, test that one alone.
- If predicted-IP features are mostly redundant with `pitcher_avg_ip_l5`, do not rerun the full predicted-IP bundle.

---

## Task 4: Check standalone predicted-IP quantile calibration

Objective: Determine whether the IP source model is itself calibrated enough to be useful.

Artifact:
- `src/models/mlb/artifacts/ip_ablation_ip_only/mlb_run_20260513_121143/ip_feature_model/`

Checks:
1. Quantile coverage on calibration/holdout:
   - `P(actual_ip <= predicted_ip_q25)` should be near 0.25
   - `P(actual_ip <= predicted_ip_q50)` should be near 0.50
2. Pinball loss vs naive baselines:
   - `pitcher_avg_ip_l5`
   - `pitcher_avg_ip_szn`
   - `team_starter_avg_ip_l10`
   - `team_starter_avg_ip_l30`
3. Bias by buckets:
   - `pitcher_avg_ip_l5` bucket
   - team
   - K prop line bucket if available
   - early-season vs later-season if available

Acceptance criteria:
- If quantile coverage is poor or pinball loss does not beat naive rolling-IP baselines, reject two-stage predicted-IP as currently implemented.
- If spread is the only orthogonal value, consider a future spread-only test, not the full four-feature bundle.

---

## Task 5: Run stripped-down single-hook-feature ablation if diagnostics support it

Objective: Test whether one well-chosen hook feature helps without adding the noisy full hook/IP stack.

Precondition:
- Task 3 identifies one hook feature with the strongest training-only signal for actual_ip or residual actual_ip.

Variant:
- Baseline + exactly one hook feature.
- No predicted-IP features.
- Keep the same training and sweep setup:
  - train seasons: 2024, 2025
  - cal season: 2026
  - cal end date: 2026-04-12
  - backtest: 2026-04-13 through 2026-05-10
  - same 510-cell sweep grid

Decision rule:
- Promote only if it improves risk-adjusted quality over static baseline.
- Prefer improvement at the existing baseline-best/fixed configs over winning a new low-volume grid cell.
- Reject if it merely increases bet count/profit while worsening Sharpe/drawdown.

---

## Task 6: Validate on independent 2025 window

Objective: Avoid deciding from one April 2026 window.

Window:
- Last 4 weeks of 2025 regular season, exact dates to be selected from available game data.

Procedure:
1. Use the already-selected variant from Task 5.
2. Do not fish for new features on this window.
3. Compare static baseline vs single-hook candidate on the same grid/window.

Acceptance criteria:
- If static baseline beats the candidate on both windows, shelf this specific workload/leash lane.
- If candidate improves both windows with sane drawdown, consider further paper-trade validation.

---

## Task 7: Separate future lane for genuinely orthogonal workload

Objective: Avoid conflating pitcher-side rolling workload with opponent/bullpen workload.

Future candidate features:
- projected bullpen availability
- bullpen fatigue pressure
- projected reliever K quality
- probability of early hook due to bullpen state
- game context and team strategy

Do not start this lane until Tasks 1-6 are resolved.

---

## Current expected outcome

Most likely:
- Static baseline remains the production candidate.
- Full predicted-IP bundle remains rejected.
- Hook-only full bundle remains rejected.
- One single hook feature may or may not have enough signal to justify a small additive test.

Final promotion bar:
- Must beat static baseline on relative ranking, not just produce an attractive absolute ROI.
- Must survive at least one independent validation window.
- Must not violate empirical CDF betting probability invariant.


---

## Results Added 2026-05-13

### Diagnostics completed

Read-only diagnostics were saved to:

- `backtest_results/mlb_workload_leash_diagnostics_20260513.json`

Key findings:

- The original all-hook/IP ablations were not pure additive tests; feature manifests showed selected-feature churn.
- L30 hook semantics are leak-safe and keyed to the starter's own team: `team_id = pgs.team_id`, same season, prior games only via `game_date < pgs.game_date`.
- Training-only diagnostics selected `team_starter_deep_start_rate_l30` as the strongest single L30 hook candidate among the three new hook features.
- The four-feature predicted-IP bundle remains rejected; the useful follow-up was the one-feature hook test.

### Single-feature hook variant implemented

Added ablation variants in `src/models/mlb/mlb_train_pipeline.py`:

- `static_no_l30`: excludes all L30 hook features to preserve a clean static baseline while L30 columns exist in HEAD.
- `hook_avg_ip_l30`
- `hook_short_hook_l30`
- `hook_deep_start_l30`

The selected one-feature variant was:

- `hook_deep_start_l30`, forcing only `team_starter_deep_start_rate_l30` into each K quantile model and excluding the other L30 hook features.

Compile check passed:

- `venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_train_pipeline.py scripts/mlb_workload_leash_diagnostics.py`

### 2026 first-window validation

Window:

- train: 2024, 2025
- calibration: 2026 through 2026-04-12
- backtest: 2026-04-13 through 2026-05-10

Artifacts/sweeps:

- static baseline: `src/models/mlb/artifacts/mlb_run_20260513_111207`, sweep `backtest_results/mlb_sweep_20260513_111808`
- single hook: `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657`, sweep `backtest_results/ip_ablation_hook_deep_start_l30_sweep`

Best ROI with `bets >= 100` and `max_drawdown <= 25%`:

| Run | Config | Bets | ROI | Sharpe | Max DD | Hit Rate |
|---|---|---:|---:|---:|---:|---:|
| static | tau=0.75, z=0.25, w=0.65, edge=0.02 | 246 | +14.58% | 1.307 | 12.72% | 59.09% |
| single hook | tau=0.5, z=0.25, w=0.5, edge=0.02 | 218 | +16.01% | 1.412 | 15.28% | 61.86% |

Fixed config checks:

| Config | Static ROI / Sharpe / DD | Single-hook ROI / Sharpe / DD | Read |
|---|---:|---:|---|
| static best: tau=0.75, z=0.25, w=0.65, edge=0.02 | +14.58% / 1.307 / 12.72% | +11.94% / 1.106 / 19.51% | static wins at static-selected config |
| hook best: tau=0.5, z=0.25, w=0.5, edge=0.02 | +14.24% / 1.187 / 14.41% | +16.01% / 1.412 / 15.28% | hook wins at hook-selected config |
| hook Sharpe: tau=0.9, z=0.25, w=0.8, edge=0.05 | +12.97% / 1.364 / 18.36% | +13.20% / 1.439 / 19.98% | hook slightly wins |
| raw edge 0.02 | +7.46% / 0.725 / 19.73% | +8.48% / 0.829 / 16.69% | hook wins raw |
| raw edge 0.05 | +9.00% / 0.955 / 22.76% | +10.35% / 1.102 / 18.91% | hook wins raw |

2026 read:

- One-feature hook is meaningfully better than all noisy all-hook/IP bundles.
- It is not a slam dunk because it loses at the original static-best fixed config, but it wins multiple raw and hook-selected/risk-filtered comparisons.
- This justified second-window validation.

### 2025 independent-window validation

Window:

- train: 2024
- calibration: 2025 through 2025-08-31
- backtest: 2025-09-01 through 2025-09-28

Artifacts/sweeps:

- static baseline: `src/models/mlb/artifacts/validation_2025_static_no_l30/mlb_run_20260513_131137`, sweep `backtest_results/validation_2025_static_no_l30_sweep`
- single hook: `src/models/mlb/artifacts/validation_2025_hook_deep_start_l30/mlb_run_20260513_131302`, sweep `backtest_results/validation_2025_hook_deep_start_l30_sweep`

Manifest verification:

- `static_no_l30` selected no L30 hook features.
- `hook_deep_start_l30` selected exactly `team_starter_deep_start_rate_l30` from the L30 hook group in every quantile.

Best ROI with `bets >= 100` and `max_drawdown <= 25%`:

| Run | Config | Bets | ROI | Sharpe | Max DD | Hit Rate |
|---|---|---:|---:|---:|---:|---:|
| static | tau=0.9, z=0.75, w=0.8, edge=0.08 | 100 | +11.10% | 1.327 | 22.45% | 61.00% |
| single hook | tau=0.75, z=0.25, w=0.65, edge=0.10 | 100 | +18.30% | 2.176 | 17.21% | 62.00% |

Best ROI with `bets >= 150` and `max_drawdown <= 30%`:

| Run | Config | Bets | ROI | Sharpe | Max DD | Hit Rate |
|---|---|---:|---:|---:|---:|---:|
| static | tau=0.9, z=0.25, w=0.65, edge=0.05 | 220 | +10.33% | 1.150 | 20.75% | 60.45% |
| single hook | tau=0.75, z=0.25, w=0.8, edge=0.08 | 159 | +18.14% | 2.080 | 15.66% | 61.64% |

Fixed config checks:

| Config | Static ROI / Sharpe / DD | Single-hook ROI / Sharpe / DD | Read |
|---|---:|---:|---|
| 2026 static best: tau=0.75, z=0.25, w=0.65, edge=0.02 | +6.73% / 0.671 / 21.97% | +9.72% / 0.969 / 19.52% | hook wins |
| 2026 hook best: tau=0.5, z=0.25, w=0.5, edge=0.02 | +6.28% / 0.611 / 19.74% | +9.00% / 0.890 / 16.09% | hook wins |
| 2026 hook Sharpe: tau=0.9, z=0.25, w=0.8, edge=0.05 | +9.70% / 1.081 / 24.82% | +11.53% / 1.272 / 19.25% | hook wins |
| raw edge 0.02 | +7.07% / 0.726 / 35.84% | +10.25% / 1.036 / 26.30% | hook wins, both DD high |
| raw edge 0.05 | +7.16% / 0.792 / 38.36% | +9.99% / 1.089 / 30.59% | hook wins, both DD high |

2025 read:

- The one-feature hook result replicated on an independent September 2025 window.
- The replicated signal is stronger than the 2026 April window, especially in risk-filtered and fixed-config comparisons.
- The top unfiltered hook cells with 1-2 bets are artifacts and should be ignored.

### Feature status / concurrent Phase 2 note

Verified against HEAD source:

The following features are present in `src/models/mlb/mlb_feature_store.py` in HEAD:

- `pitcher_max_ip_l5`
- `pitcher_median_ip_l5`
- `pitcher_ip_range_l5`
- `pitcher_short_start_rate_l5`
- `pitcher_avg_batters_faced_l5`
- `pitcher_avg_batters_faced_szn`
- `pitcher_avg_pitches_per_start_l5`
- `pitcher_workload_spike_ratio`
- `pitcher_recent_pitch_count_trend`
- `rest_after_high_pitch_count`
- `team_starter_avg_ip_l10`
- `team_starter_short_start_rate_l10`
- `team_starter_avg_pitches_l10`
- `bullpen_fatigue_pressure`
- `opp_team_contact_rate_l10`
- `opp_team_chase_pct_l10`
- `opp_team_zone_contact_pct_l10`
- `pitcher_start_stability_l5`
- `pitcher_avg_pitches_thrown_l5`
- `team_starter_avg_ip_l30`
- `team_starter_short_hook_rate_l30`
- `team_starter_deep_start_rate_l30`

Manifest interpretation:

- The successful clean Phase 2 static artifact `mlb_run_20260513_111207` selected the expected pre-existing/concurrent Phase 2 features listed in the user's note, except the ambiguous/non-lineup group.
- The 2026 single-hook artifact selected the same Phase 2 group plus `team_starter_deep_start_rate_l30`.
- The 2025 validation static artifact selected no L30 hook features; the hook artifact selected only `team_starter_deep_start_rate_l30` from the L30 hook group.

### Updated recommendation

Previous recommendation was too conservative after only the all-hook/IP ablations. The refined result is:

- Reject the predicted-IP feature bundle.
- Reject the all-L30-hook bundle as too noisy.
- Keep and further validate the single L30 hook feature `team_starter_deep_start_rate_l30`.

Do not deploy immediately only from these two windows, because grid selection still inflates point estimates. But the single-feature hook result is now strong enough to become the preferred candidate for paper/live shadow comparison against the clean static baseline.

Suggested next gate:

1. Use one pre-committed production-ish config for both static and hook models, ideally either:
   - current static production-ish config: tau=0.75, z=0.25, w=0.65, edge=0.02, or
   - hook Sharpe-stable config: tau=0.9, z=0.25, w=0.8, edge=0.05.
2. Paper/shadow both for the next live period.
3. Promote only if the hook model preserves or improves ROI/Sharpe without expanding false-positive bet volume.
