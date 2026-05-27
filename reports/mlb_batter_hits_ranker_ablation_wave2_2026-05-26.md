# MLB batter_hits ranker ablation - wave 2 - 2026-05-26

## Decision

**First batter_hits ranker gate pass.** Two configs cleared `edge_clv_ci_low > 0`, the gate that has blocked all prior batter_hits work.

**Winner:** `no_prop_line + force_include platoon + contact_quality` at `edge=0.10`.

- bets: 1319
- ROI: +8.76%
- mean CLV CI low: +0.00426 (positive, mean-CLV gate passes)
- raw_edge CLV CI low: **+0.0152** (ranker gate PASSES)
- raw_edge Spearman: +0.0679
- top book: DraftKings ~38%, deconcentrated

**Runner-up:** `no_prop_line + force_include platoon` at `edge=0.10`.

- bets: 1314
- ROI: +8.50%
- mean CLV CI low: +0.00476
- raw_edge CLV CI low: **+0.00032** (passes by a hair, needs independent-window validation)
- raw_edge Spearman: +0.0629

**Status:** flat-paper candidate, ranker gate touched/passed but on the discovery window only. Needs independent-window validation before live promotion or edge-sized staking.

## Lessons applied

- Quote-clean CLV before feature work.
- Feature selector is not an ablation; family-level force-include is the right test.
- Correlated families require family-level validation; `is_same_hand` alone (`+0.0218` improvement) is weaker than the full platoon family (`+0.0973`), confirming family-level signal.
- Edge-ranking can be a binary filter even if mean CLV is positive; this is the first batter_hits config where the magnitude also ranks CLV.
- Empirical CDF only; no Gaussian substitution.

## What was tested

Wave 1 (earlier today, 5 ablations):
- Track A include: contact_quality, matchup_pitcher, platoon on `no_prop_line` baseline
- Track B exclude: market, matchup_pitcher on `with_prop_line` baseline

Wave 2 (now, 5 ablations):
- Track A include: recent_form, opportunity, environment on `no_prop_line`
- Combo: platoon + contact_quality on `no_prop_line`
- Single feature: `is_same_hand` only on `no_prop_line`

Total: 10 ablations.

## Full results - edge_clv_ci_low movement vs frozen baseline (edge=0.10)

```
Ablation                              edge_low    delta_vs_baseline   bets     gate
-----------------------------------------------------------------------------------
platoon+contact_quality (combo)      +0.0152      +0.1122             1319     PASS
platoon                              +0.0003      +0.0973             1314     PASS (boundary)
opportunity                          -0.0085      +0.0885             1097     close
contact_quality                      -0.0167      +0.0803             1093
is_same_hand only                    -0.0218      +0.0752             1196
exclude_matchup_pitcher (with_prop)  -0.0383      +0.0646              793
environment                          -0.0430      +0.0540              901
matchup_pitcher                      -0.0537      +0.0433              919
exclude_market (with_prop)           -0.0662      +0.0367              950
recent_form                          -0.0859      +0.0111              851
baseline (no_prop edge=0.10)         -0.0970       0.0000              783
```

All decision-grade ablations kept `mean_clv_ci_low > 0`. No ablation broke the mean-CLV gate.

## Key findings

### Family-level signal > single-feature signal

`is_same_hand` alone produced `edge_low = -0.0218` (delta +0.0752).
`platoon` family (`is_same_hand + batter_avg_h_vs_hand_l20 + batter_avg_ops_vs_hand_l20`) produced `edge_low = +0.0003` (delta +0.0973).
`platoon + contact_quality` combo produced `edge_low = +0.0152` (delta +0.1122).

Translation: the platoon family carries ranker information beyond `is_same_hand` alone, and contact_quality stacks additively with platoon. This validates the "correlated feature family validation" lesson — selectors prune correlated proxies, but force-include reveals additive value.

### Track B: prop_line and matchup_pitcher are load-bearing in with_prop baseline

- `exclude_market` at edge=0.12: ROI dropped from +25.76% to +11.61% (more than halved). Bets doubled (261 to 525).
- `exclude_matchup_pitcher` at edge=0.12: ROI dropped from +25.76% to +11.48%. Bets jumped (261 to 354).

Both features were doing filtering work at high edge thresholds. Removing them dilutes the winning set with marginal bets.

### Track A: platoon is the cleanest source of new ranker signal

Across all wave-1 + wave-2 Track A includes, platoon-related runs are the only configs that touched `edge_clv_ci_low > 0`. No other family did.

## Promotion stance

- No live-money deployment.
- No Kelly or tiered staking.
- Flat-paper candidate, threshold-only.
- Independent-window validation required next.

## Next session recommended steps

1. Independent-window validation of `no_prop_line + platoon + contact_quality`:
   - Train with earlier `--cal-end-date` and backtest on non-overlap window.
   - Suggested: `--cal-end-date 2026-03-14`, replay `2026-03-15 -> 2026-04-12`.
   - Command: `.\scripts\run_batter_hits_family_ablation.ps1 -Families platoon,contact_quality -Mode include -Base no_prop_line -CalEndDate 2026-03-14 -Start 2026-03-15 -End 2026-04-12 -LabelTag platoon_contact_indep`
2. If the gate holds on independent window:
   - Run book-sensitivity on the winning config.
   - Run flat-stake paper sweep for 2-4 weeks at edge=0.10.
   - Decide promotion to Kelly only after second-window confirmation.
3. If it fails on independent window:
   - Treat current edge_low=+0.0152 as window-specific noise.
   - Try `platoon + opportunity` (next-best ranker improver) combo.
   - Consider mid-tolerance retrain (0.01) only if needed.
4. Park bullpen, recent_form: did not improve ranker materially.
5. Update memory and frozen baselines once independent-window validation lands.

## Artifacts

- Frozen baselines (unchanged): `docs/development_docs/mlb_batter_hits_frozen_baselines.md`
- Iteration pipeline: `docs/development_docs/mlb_batter_hits_ablation_iteration_pipeline.md`
- Ablation runner: `scripts/run_batter_hits_family_ablation.ps1` (extended for -Families/-Features/-LabelTag)
- Resume helper: `scripts/resume_batter_hits_ablation_audit.ps1`
- 10 ablation sweeps + audits under: `backtest_results/ablations/`
- 10 trained artifacts under: `src/models/mlb/artifacts/ablations/`
