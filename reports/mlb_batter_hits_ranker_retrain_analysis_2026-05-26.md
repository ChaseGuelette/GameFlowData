# MLB batter_hits ranker retrain analysis — 2026-05-26

## Decision

**Evaluation run:** four clean `batter_hits` retrain variants, each swept with `lowest_vig` and `preferred_book_first` routing over 2026-04-13 through 2026-05-17, followed by audit suite, CLV/ranker diagnostics, and book sensitivity.

**Winner for flat-paper candidate:** `with_prop_line` at either tolerance (`0.02` or `0.005`) with `preferred_book_first`, `config_02_no_BL_edge0.12_kelly0.125`.

**Clean ablation baseline to keep:** `no_prop_line` at either tolerance with `preferred_book_first`, especially `config_02_no_BL_edge0.12_kelly0.125` for CLV/bet-count balance or `config_03_no_BL_edge0.15_kelly0.125` for ROI.

**Deploy/live-money recommendation:** **do not deploy / do not edge-size.** Every decision-grade config fails the edge-ranking gate (`edge_clv_ci_low <= 0`). These are still **flat-threshold paper candidates only**.

## Prior lessons / invariants applied

- Feature selector output is not an ablation.
- Correlated feature families require family-level force-include / force-exclude validation.
- Quote-clean CLV comes before feature work or promotion.
- Positive mean CLV is not enough if edge magnitude does not rank CLV.
- Empirical CDF probabilities only; no Gaussian CDF substitution.
- Treat `<100` bet configs as exploratory even if ROI is high.

## Artifact sanity

All four models are apples-to-apples on core metadata:

- stat: `hits`
- train seasons: 2024, 2025
- calibration season: 2026
- calibration cutoff: 2026-04-12
- evaluation window starts after calibration: 2026-04-13
- train rows: 87,300
- calibration rows: 4,176

Tolerance did not change selected features or outputs:

- `with_prop_line_tol002` and `with_prop_line_tol0005` have identical feature manifests and identical evaluation metrics.
- `no_prop_line_tol002` and `no_prop_line_tol0005` have identical feature manifests and identical evaluation metrics.

Selected feature counts:

| Variant | Binomial features | AB NegBin features | Notes |
|---|---:|---:|---|
| with_prop_line | 23 | 20 | includes `prop_line_batter_hits` in both |
| no_prop_line | 19 | 20 | excludes `prop_line_batter_hits` |

Calibration snapshot:

| Variant | Mean NLL | Mean predicted hits | Mean actual | Mean ratio | Zero frac gap | Line 0.5 gap | Line 1.5 gap |
|---|---:|---:|---:|---:|---:|---:|---:|
| with_prop_line | 1.0879 | 0.8437 | 0.8157 | 1.0344 | -0.0226 | +0.0231 | +0.0344 |
| no_prop_line | 1.0882 | 0.8559 | 0.8157 | 1.0493 | -0.0284 | +0.0309 | +0.0193 |

Calibration is broadly similar. With-prop has slightly better NLL and mean-ratio; no-prop has slightly better 1.5-line gap.

## Preferred-book routing: decision-grade configs

Preferred-book routing is operationally much cleaner: ESPNBet+ProphetX share drops from roughly 77-86% under lowest-vig to roughly 0-2% under preferred-book routing.

| Variant | Edge | Bets | ROI | Mean CLV | Mean CLV CI low | Edge Spearman | Edge CI low | Top book/share | ESPNBet share | ProphetX share |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|
| no_prop_line | 0.10 | 783 | 11.13% | 0.006688 | 0.005157 | -0.0223 | -0.0970 | DraftKings 41.0% | 0.89% | 0.51% |
| no_prop_line | 0.12 | 455 | 11.94% | 0.007061 | 0.005388 | -0.0676 | -0.1782 | DraftKings 40.7% | 0.44% | 0.00% |
| no_prop_line | 0.15 | 218 | 19.18% | 0.005114 | 0.002708 | 0.0139 | -0.1626 | DraftKings 44.5% | 0.46% | 0.92% |
| with_prop_line | 0.10 | 564 | 14.16% | 0.006255 | 0.004790 | -0.0141 | -0.1029 | DraftKings 34.4% | 1.06% | 0.71% |
| with_prop_line | 0.12 | 261 | 25.76% | 0.006497 | 0.003459 | 0.0828 | -0.0418 | DraftKings 40.2% | 0.77% | 0.77% |

Key read:

- `with_prop_line edge=0.12` has the strongest ROI and the least-bad edge-ranking CI low.
- `no_prop_line edge=0.12` has the strongest mean CLV CI low and more bets.
- `no_prop_line edge=0.10` is the broadest/most liquid paper candidate.
- No preferred-book config passes edge-ranking.

## Lowest-vig routing: decision-grade configs

Lowest-vig still has positive ROI and mean CLV, but remains book-concentrated.

| Variant | Edge | Bets | ROI | Mean CLV | Mean CLV CI low | Edge Spearman | Edge CI low | ESPNBet+ProphetX share |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| no_prop_line | 0.10 | 465 | 12.41% | 0.004085 | 0.002341 | -0.0353 | -0.1264 | 86.24% |
| no_prop_line | 0.12 | 289 | 15.42% | 0.004546 | 0.002465 | -0.0984 | -0.2577 | 85.47% |
| no_prop_line | 0.15 | 137 | 11.92% | 0.003807 | 0.000435 | -0.1020 | -0.2880 | 83.21% |
| with_prop_line | 0.10 | 275 | 18.90% | 0.005447 | 0.003353 | 0.0180 | -0.1100 | 82.91% |
| with_prop_line | 0.12 | 141 | 31.59% | 0.006203 | 0.002112 | 0.0223 | -0.1180 | 77.30% |

Key read:

- Lowest-vig is useful as a control but not the preferred operational policy because concentration remains high.
- Preferred-book keeps the signal while removing ESPNBet/ProphetX dependence.

## Ranker diagnostics

No ranker passes. The closest score is with-prop-line preferred-book edge=0.12:

| Variant | Routing | Edge | Score | N | Spearman | CI low | Top-bottom CLV | Monotonic bins | Pass |
|---|---|---:|---|---:|---:|---:|---:|---|---|
| with_prop_line | preferred_book | 0.12 | raw_edge | 261 | 0.0828 | -0.0371 | 0.00667 | true | false |
| with_prop_line | preferred_book | 0.12 | logit_edge | 261 | 0.0854 | -0.0327 | 0.00165 | true | false |
| no_prop_line | preferred_book | 0.15 | model_prob | 218 | 0.0810 | -0.0774 | -0.00170 | false | false |
| no_prop_line | preferred_book | 0.10 | model_prob | 783 | -0.0107 | -0.0833 | -0.00035 | false | false |

Interpretation:

- Edge can still be a useful binary threshold filter.
- Edge magnitude is not yet a valid quality score for sizing or prioritization.
- Kelly / tiered staking remains blocked.
- Future feature work should target **model-alpha/ranker stability**, not just ROI.

## Book-sensitivity findings

Preferred-book routing has deconcentrated the strategy:

- Top book becomes DraftKings or HardRock, not ESPNBet/ProphetX.
- ESPNBet and ProphetX shares are about 0-2% in preferred-book decision-grade configs.
- Excluding ESPNBet or ProphetX barely changes preferred-book results because they are already nearly absent.

Examples:

| Variant | Edge | Slice | N | ROI | Mean CLV CI low | Edge CI low | Top book/share |
|---|---:|---|---:|---:|---:|---:|---|
| with_prop_line | 0.12 | overall | 261 | 25.76% | 0.00346 | -0.0418 | DraftKings 40.2% |
| with_prop_line | 0.12 | exclude_espnbet | 259 | 25.92% | 0.00343 | -0.0369 | DraftKings 40.5% |
| with_prop_line | 0.12 | exclude_prophetx | 259 | 26.73% | 0.00333 | -0.0290 | DraftKings 40.5% |
| no_prop_line | 0.12 | overall | 455 | 11.94% | 0.00539 | -0.1782 | DraftKings 40.7% |
| no_prop_line | 0.12 | exclude_espnbet | 453 | 12.44% | 0.00536 | -0.1750 | DraftKings 40.8% |
| no_prop_line | 0.12 | exclude_prophetx | 455 | 11.94% | 0.00539 | -0.1782 | DraftKings 40.7% |

This answers the prior concentration question: the signal is not simply ESPNBet/ProphetX dependence after preferred-book routing.

## Model choice

### If the objective is highest near-term paper ROI

Use:

```text
with_prop_line, preferred_book_first, no_BL edge=0.12, flat staking
```

Why:

- 261 bets, enough for decision-grade analysis.
- Best ROI among preferred-book decision-grade configs: +25.76%.
- Positive mean CLV: +0.006497, CI low +0.003459.
- Best/least-bad ranking result: raw-edge CI low -0.0418 and logit-edge CI low -0.0327.
- Deconcentrated book mix: DraftKings top at ~40%, ESPNBet/ProphetX each <1%.

Status: **flat-paper candidate only**.

### If the objective is cleanest ablation baseline

Use:

```text
no_prop_line, preferred_book_first, no_BL edge=0.12 or edge=0.10
```

Why:

- `edge=0.12`: 455 bets, ROI +11.94%, mean CLV +0.007061, CI low +0.005388.
- `edge=0.10`: 783 bets, ROI +11.13%, mean CLV +0.006688, CI low +0.005157.
- Removes prop-line dependency from the model, which is cleaner for feature-family ablation interpretation.

Status: **clean baseline for next ablations, not a promotion candidate**.

### What not to optimize next

- Do not spend more time on tolerance 0.02 vs 0.005. It made no difference in selected features or outputs.
- Do not use BL/tau tiny-bet configs as winners. Several have very high ROI but too few bets and/or crash ranker bins.
- Do not promote edge/Kelly sizing. Ranker gate fails everywhere.

## Recommended next experiments

1. Freeze the two baselines:
   - operational paper baseline: `with_prop_line preferred_book edge=0.12`
   - clean ablation baseline: `no_prop_line preferred_book edge=0.12` or `edge=0.10`
2. Run the next feature-family ablation against the clean no-prop baseline first.
3. Use the fast iteration pipeline in `docs/development_docs/mlb_batter_hits_ablation_iteration_pipeline.md`:
   - compact sweep
   - CLV-only selected-config audit
   - ranker diagnostics
   - book sensitivity only if promising
   - full dropout certification only for finalists
4. Patch `scripts/analyze_mlb_clv_ranking_diagnostics.py` later so tiny/empty BL configs write empty bucket CSVs instead of throwing `KeyError`; this does not affect the current decision-grade analysis.

## Final classification

| Candidate | Classification |
|---|---|
| with_prop_line preferred_book edge=0.12 | Best flat-paper candidate; not deployable / not edge-sized |
| no_prop_line preferred_book edge=0.12 | Best clean CLV baseline for ablations |
| no_prop_line preferred_book edge=0.10 | Broadest clean paper baseline, lower ROI but highest bet count |
| lowest_vig variants | Useful controls only; book concentration remains high |
| BL/tau tiny configs | Exploratory/noisy; ignore for promotion decisions |
