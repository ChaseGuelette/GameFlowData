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

The first two Pitcher K Track A force-exclude family ablations were reconstructed and reviewed. `workload_leash` and `market` are both load-bearing for the current frozen Black-Litterman flat-paper strategy and must be retained. The raw/no-BL `team_hook` exclusion was then completed and reviewed; it is plausible enough to advance to the exact-artifact focused BL sweep. The next Pitcher K step is that focused sweep, not `pitcher_stuff`.

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
- Reviewed the completed raw/no-BL `team_hook` exclusion artifact at:
  `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_team_hook_slice_20260719_135149/mlb_run_20260719_135150`
- Verified all three `team_hook` features were excluded, calibration passed with a 1.38% worst gap, and all six raw cells had at least 100 bets. Shared-edge ROI improved versus the raw baseline at `.02` (-3.88% to +0.26%), `.05` (-1.49% to +1.00%), and `.08` (-2.85% to +0.32%).
- At raw edge `.02`, the exclusion removed 50 baseline-only bets returning -8.34% ROI and added 49 candidate-only bets returning +10.11% ROI. Raw edge still failed the CLV ranker gate, while post-hoc `model_prob` barely passed at `.02` and `.03`; this supports focused BL evaluation but not Kelly/live promotion.

## Decisions Made

- Retain `workload_leash`.
- Retain `market`.
- Keep the frozen Pitcher K candidate flat-paper only. Kelly/live/Kalshi remain blocked by ranker uncertainty.
- Do not use post-hoc winning cells from the 108-point market-exclusion BL sweep to override the preregistered frozen-policy and paired-bet failure.
- Continue Track A one family at a time. The next family is `team_hook`.
- `team_hook` is not yet classified as load-bearing or safe to remove. Its raw result advances to the focused BL gate only.

## Next Session — Start Here

1. Run the focused 108-cell quote-clean BL sweep against the exact reviewed `team_hook` exclusion artifact:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir "src\models\mlb\artifacts\ablations\pitcher_strikeouts_exclude_load_bearing_exclude_team_hook_slice_20260719_135149\mlb_run_20260719_135150" --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-06-21 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir "backtest_results\ablations\pitcher_strikeouts_exclude_team_hook_slice7_bl_under_20260413_20260621"
```

2. Review the frozen operating point first: `tau=.5`, `z_max=.25`, `max_weight=.50`, `edge=.02`, compared with the certified baseline's 146 bets, +17.19% ROI, 2.338 Sharpe, and 5.32% drawdown.
3. Inspect paired common, baseline-only, and candidate-only bets before considering any swept winner.
4. Classify `team_hook` as Confirm load-bearing, Shelf, or Exclude candidate.
5. Do not launch `pitcher_stuff` until this focused `team_hook` result is reviewed.

## Blockers and Open Questions

- The focused `market` BL sweep has no focused CLV/ranker audit, but rejection is decision-grade because it failed the frozen configuration and paired-bet mechanism. The earlier raw/no-BL CLV evidence does not rescue the underperforming BL policy.
- The candidate's retraining explicitly removed the `market` family, but the normal selector also changed some non-market features. Interpret this as the family removal plus the training pipeline's response, not static deletion from fixed trees.
- The `team_hook` audit suite's overall FAIL reflects the intentionally skipped dropout audit and raw-edge ranker CI lows below zero. Mean CLV was positive in every cell, and the already-certified dense window makes a focused BL sweep the correct next gate rather than a raw rerun.
- [[handoff-112]] records the separate technical-debt audit consolidation status completed concurrently on July 19.

## Files to Read on Resume

- `docs/development_docs/mlb_pitcher_k_ablation_roadmap.md`
- `backtest_results/ablations/pitcher_strikeouts_exclude_market_slice7_bl_under_20260413_20260621/sweep_summary.csv`
- `backtest_results/ablations/pitcher_strikeouts_exclude_market_slice7_bl_under_20260413_20260621/sweep_results.json`
- `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_market_slice7_20260718_174843/mlb_run_20260718_174844/run_config.json`
- `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_market_slice7_20260718_174843/mlb_run_20260718_174844/feature_manifest.json`
- `src/models/mlb/artifacts/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_team_hook_slice_20260719_135149/mlb_run_20260719_135150/`
- `backtest_results/ablations/pitcher_strikeouts_exclude_load_bearing_exclude_team_hook_slice_20260719_135149_preferred_book/`
- After it runs: `backtest_results/ablations/pitcher_strikeouts_exclude_team_hook_slice7_bl_under_20260413_20260621/`
