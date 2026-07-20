---
title: Handoff 113 — Pitcher K load-bearing ablations through market review
type: handoff
domain: handoffs
status: completed
owner: Chase
effective_date: 2026-07-19
tags: [handoff, gbrain, mlb, pitcher-k, ablation]
---

# Handoff 113 — Pitcher K load-bearing ablations through market review

> Part of [[Handoffs]]

**Date**: 2026-07-19 13:52 ET

## Summary

The first two Pitcher K Track A force-exclude family ablations were reconstructed and reviewed. `workload_leash` and `market` are both load-bearing for the current frozen Black-Litterman flat-paper strategy and must be retained. The next session must start by locating and reviewing the newly run `team_hook` exclusion; do not start `pitcher_stuff` first.

## What Was Done

- Reconstructed the July 18 ablation work from conversation history and local artifacts because handoff 111 predated those runs.
- Confirmed and retained `workload_leash`: at the frozen configuration, exclusion fell from 146 bets / +17.19% ROI to 170 bets / +11.57% ROI and added weak bets.
- Reviewed the completed focused BL sweep for the `market` exclusion at:
  `backtest_results/ablations/pitcher_strikeouts_exclude_market_slice7_bl_under_20260413_20260621`
- Verified the comparison used the same 2026-04-13 through 2026-06-21 quote-clean dense window, Under-only direction, preferred-book routing, BL grid, and flat $100 stakes.
- At the exact frozen policy (`tau=.5`, `z_max=.25`, `max_weight=.50`, `edge=.02`), excluding `market` produced 203 bets, +6.20% ROI, 0.851 Sharpe, and 8.50% drawdown versus the baseline's 146 bets, +17.19% ROI, 2.338 Sharpe, and 5.32% drawdown.
- Paired-bet mechanism: 121 common bets were nearly unchanged; 25 baseline-only bets returned +39.75% ROI; 82 candidate-only bets returned -2.48% ROI. The exclusion removed strong selections and added losing volume.
- Classified `market` as confirmed load-bearing for the current BL strategy. Retain `prop_line_pitcher_strikeouts` and `line_total`; exclude the market-removal artifact from promotion.
- Identified a routing caveat: quote routing occurs before the simulator's `[-200,+200]` odds filter, with no reroute after rejection. It affected both artifacts under identical logic and did not change the market-family decision.

## Decisions Made

- Retain `workload_leash`.
- Retain `market`.
- Keep the frozen Pitcher K candidate flat-paper only. Kelly/live/Kalshi remain blocked by ranker uncertainty.
- Do not use post-hoc winning cells from the 108-point market-exclusion BL sweep to override the preregistered frozen-policy and paired-bet failure.
- Continue Track A one family at a time. The next family is `team_hook`.

## Next Session — Start Here

1. Locate the newest completed run whose label begins:
   `pitcher_strikeouts_exclude_load_bearing_exclude_team_hook_slice7_`
2. If no completed artifact/output exists, inspect whether the run is still active or failed; do not restart it blindly.
3. Review the exact artifact metadata, force-excluded features, calibration gate, raw preferred-book sweep, CLV audit, ranker reports, and `ablation_summary.md`.
4. Compare `team_hook` against the frozen baseline at fixed settings and inspect paired-bet movement.
5. Only if the raw result is plausible, run the focused 108-cell quote-clean BL sweep against the exact new artifact.
6. Classify `team_hook` as Confirm load-bearing, Shelf, or Exclude candidate.
7. Do not launch `pitcher_stuff` until the `team_hook` result is reviewed.

The launch command handed to Chase was:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode exclude -Families team_hook -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag load_bearing_exclude_team_hook_slice7
```

## Blockers and Open Questions

- The focused `market` BL sweep has no focused CLV/ranker audit, but rejection is decision-grade because it failed the frozen configuration and paired-bet mechanism. The earlier raw/no-BL CLV evidence does not rescue the underperforming BL policy.
- The candidate's retraining explicitly removed the `market` family, but the normal selector also changed some non-market features. Interpret this as the family removal plus the training pipeline's response, not static deletion from fixed trees.
- [[handoff-112]] records the separate technical-debt audit consolidation status completed concurrently on July 19.

## Files to Read on Resume

- `docs/development_docs/mlb_pitcher_k_ablation_roadmap.md`
- `backtest_results/ablations/pitcher_strikeouts_exclude_market_slice7_bl_under_20260413_20260621/sweep_summary.csv`
- `backtest_results/ablations/pitcher_strikeouts_exclude_market_slice7_bl_under_20260413_20260621/sweep_results.json`
- `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_market_slice7_20260718_174843/mlb_run_20260718_174844/run_config.json`
- `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_market_slice7_20260718_174843/mlb_run_20260718_174844/feature_manifest.json`
- The newest `team_hook` artifact and matching `backtest_results/ablations/` output.
