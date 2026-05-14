# MLB Pitcher K Single-Hook Deep-Start Validation — 2026-05-13

## Executive Summary

The initial all-hook/IP workload ablations were correctly rejected, but the follow-up stripped-down test changed the conclusion for one specific feature.

Current recommendation:

- Reject the predicted-IP feature bundle.
- Reject the all-L30-hook feature bundle as too noisy.
- Keep and further validate the single L30 hook feature `team_starter_deep_start_rate_l30`.
- Do not deploy only from swept backtests; use a pre-committed live/paper shadow comparison against the clean static baseline.

The one-feature hook model replicated across two independent windows and improved ROI/Sharpe/drawdown in multiple risk-filtered and fixed-config comparisons.

## Scope

Feature under test:

- `team_starter_deep_start_rate_l30`

Interpretation:

- Starter's own team/manager recent tendency to allow deep starts.
- SQL semantics are leak-safe: same season, own starter team, prior games only via `game_date < pgs.game_date`.

Rejected feature bundles:

- Predicted IP bundle:
  - `predicted_ip_q25`
  - `predicted_ip_q50`
  - `predicted_ip_spread`
  - `predicted_ip_q25_delta`
- All L30 hook bundle:
  - `team_starter_avg_ip_l30`
  - `team_starter_short_hook_rate_l30`
  - `team_starter_deep_start_rate_l30`

## Generated Files and Artifacts

Plan/reporting:

- `.hermes/plans/mlb-workload-leash-ablation-followup-2026-05-13.md`
- `reports/mlb-single-hook-deep-start-validation-2026-05-13.md`
- `backtest_results/mlb_workload_leash_diagnostics_20260513.json`

Code touched:

- `scripts/mlb_workload_leash_diagnostics.py`
- `src/models/mlb/mlb_train_pipeline.py`

2026 artifacts/sweeps:

- Static baseline artifact: `src/models/mlb/artifacts/mlb_run_20260513_111207`
- Static baseline sweep: `backtest_results/mlb_sweep_20260513_111808`
- Single-hook artifact: `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657`
- Single-hook sweep: `backtest_results/ip_ablation_hook_deep_start_l30_sweep`

2025 validation artifacts/sweeps:

- Static baseline artifact: `src/models/mlb/artifacts/validation_2025_static_no_l30/mlb_run_20260513_131137`
- Static baseline sweep: `backtest_results/validation_2025_static_no_l30_sweep`
- Single-hook artifact: `src/models/mlb/artifacts/validation_2025_hook_deep_start_l30/mlb_run_20260513_131302`
- Single-hook sweep: `backtest_results/validation_2025_hook_deep_start_l30_sweep`

## Implementation Notes

Added ablation variants in `src/models/mlb/mlb_train_pipeline.py`:

- `static_no_l30`: excludes all L30 hook features, allowing a clean static baseline while L30 columns exist in HEAD.
- `hook_avg_ip_l30`
- `hook_short_hook_l30`
- `hook_deep_start_l30`

The selected one-feature variant was:

- `hook_deep_start_l30`

Behavior:

- Forces only `team_starter_deep_start_rate_l30` into each K quantile model.
- Excludes the other L30 hook features from the candidate pool.

Compile check passed:

```text
venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_train_pipeline.py scripts/mlb_workload_leash_diagnostics.py
```

## 2026 First-Window Validation

Window:

- Train: 2024, 2025
- Calibration: 2026 through 2026-04-12
- Backtest: 2026-04-13 through 2026-05-10

Best ROI with `bets >= 100` and `max_drawdown <= 25%`:

| Run | Config | Bets | ROI | Sharpe | Max DD | Hit Rate |
|---|---|---:|---:|---:|---:|---:|
| static | tau=0.75, z=0.25, w=0.65, edge=0.02 | 246 | +14.58% | 1.307 | 12.72% | 59.09% |
| single hook | tau=0.5, z=0.25, w=0.5, edge=0.02 | 218 | +16.01% | 1.412 | 15.28% | 61.86% |

Fixed-config checks:

| Config | Static ROI / Sharpe / DD | Single-hook ROI / Sharpe / DD | Read |
|---|---:|---:|---|
| Static best: tau=0.75, z=0.25, w=0.65, edge=0.02 | +14.58% / 1.307 / 12.72% | +11.94% / 1.106 / 19.51% | static wins at static-selected config |
| Hook best: tau=0.5, z=0.25, w=0.5, edge=0.02 | +14.24% / 1.187 / 14.41% | +16.01% / 1.412 / 15.28% | hook wins at hook-selected config |
| Hook Sharpe: tau=0.9, z=0.25, w=0.8, edge=0.05 | +12.97% / 1.364 / 18.36% | +13.20% / 1.439 / 19.98% | hook slightly wins |
| Raw edge 0.02 | +7.46% / 0.725 / 19.73% | +8.48% / 0.829 / 16.69% | hook wins raw |
| Raw edge 0.05 | +9.00% / 0.955 / 22.76% | +10.35% / 1.102 / 18.91% | hook wins raw |

Read:

- One-feature hook is meaningfully better than all noisy all-hook/IP bundles.
- It loses at the original static-best fixed config, so this is not a deployment-only signal.
- It wins multiple raw and hook-selected/risk-filtered comparisons, justifying independent validation.

## 2025 Independent-Window Validation

Window:

- Train: 2024
- Calibration: 2025 through 2025-08-31
- Backtest: 2025-09-01 through 2025-09-28

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

Fixed-config checks:

| Config | Static ROI / Sharpe / DD | Single-hook ROI / Sharpe / DD | Read |
|---|---:|---:|---|
| 2026 static best: tau=0.75, z=0.25, w=0.65, edge=0.02 | +6.73% / 0.671 / 21.97% | +9.72% / 0.969 / 19.52% | hook wins |
| 2026 hook best: tau=0.5, z=0.25, w=0.5, edge=0.02 | +6.28% / 0.611 / 19.74% | +9.00% / 0.890 / 16.09% | hook wins |
| 2026 hook Sharpe: tau=0.9, z=0.25, w=0.8, edge=0.05 | +9.70% / 1.081 / 24.82% | +11.53% / 1.272 / 19.25% | hook wins |
| raw edge 0.02 | +7.07% / 0.726 / 35.84% | +10.25% / 1.036 / 26.30% | hook wins, both DD high |
| raw edge 0.05 | +7.16% / 0.792 / 38.36% | +9.99% / 1.089 / 30.59% | hook wins, both DD high |

Read:

- The one-feature hook result replicated on an independent September 2025 window.
- Signal is stronger than the 2026 April window in risk-filtered and fixed-config comparisons.
- Top unfiltered hook cells with 1-2 bets are artifacts and should be ignored.

## Feature Status / Concurrent Phase 2 Note

Verified present in `src/models/mlb/mlb_feature_store.py` in HEAD:

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

- Clean Phase 2 static artifact `mlb_run_20260513_111207` selected the expected Phase 2 features from the user-provided list, excluding the ambiguous/non-lineup group.
- 2026 single-hook artifact selected the same Phase 2 group plus `team_starter_deep_start_rate_l30`.
- 2025 validation static artifact selected no L30 hook features.
- 2025 validation hook artifact selected only `team_starter_deep_start_rate_l30` from the L30 group.

## Decision

Refined recommendation:

- Reject predicted-IP features as currently parameterized.
- Reject the all-L30-hook bundle as too noisy.
- Keep `team_starter_deep_start_rate_l30` as the workload/leash candidate.

Deployment status:

- Not deploy-now.
- Strong enough for a pre-committed paper/live shadow comparison.

Suggested next gate:

1. Choose one pre-committed production-ish config for both static and hook models:
   - current static-ish config: tau=0.75, z=0.25, w=0.65, edge=0.02; or
   - hook Sharpe-stable config: tau=0.9, z=0.25, w=0.8, edge=0.05.
2. Shadow static vs hook over the next live period.
3. Promote only if hook preserves or improves ROI/Sharpe without inflating false-positive bet volume.

## Caveats

- Swept-grid ROI is selection-inflated. Do not anchor on +14-18% as a long-run edge.
- Two windows are enough to continue validation, not enough for final deployment.
- The test validates one starter-team deep-start leash feature, not workload as a broad concept.
- Empirical CDF probability invariant remains unchanged: `(samples > line).mean()`.
