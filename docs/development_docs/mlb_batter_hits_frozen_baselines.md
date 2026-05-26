# MLB batter_hits frozen baselines

Last updated: 2026-05-26

## Purpose

This file records the current frozen `batter_hits` baselines to use for future model reruns, feature-family ablations, and paper-trading comparisons.

These baselines come from the 2026-05-26 ranker-retrain analysis over:

- evaluation window: 2026-04-13 through 2026-05-17
- train seasons: 2024, 2025
- calibration season: 2026
- calibration cutoff: 2026-04-12
- stat: `hits` / `batter_hits`
- quote-clean replay
- CLV audit suite
- ranker diagnostics
- book-sensitivity diagnostics

Full analysis report:

- `reports/mlb_batter_hits_ranker_retrain_analysis_2026-05-26.md`

Iteration pipeline:

- `docs/development_docs/mlb_batter_hits_ablation_iteration_pipeline.md`

## Relevant invariants and lessons

- Quote-clean CLV comes before feature work or promotion.
- Feature selector output is not an ablation.
- Correlated feature families require family-level validation.
- Positive mean CLV is not enough if edge magnitude does not rank CLV.
- Treat configs with fewer than 100 bets as exploratory only.
- Edge can be a binary threshold filter even when edge magnitude is not safe for Kelly/tiered sizing.
- Empirical probabilities only: `(samples > line).mean()`.

## Frozen baseline 1: operational flat-paper candidate

Use this when the goal is near-term paper tracking with the strongest observed preferred-book performance.

```text
model variant: with_prop_line
selector tolerance: either 0.02 or 0.005; outputs were identical
routing policy: preferred_book_first
config: config_02_no_BL_edge0.12_kelly0.125
edge threshold: 0.12
staking: flat / threshold-only paper; do not Kelly-size
status: flat-paper candidate only; not live-money deployable
```

Representative model artifact:

```text
src/models/mlb/artifacts/ranker_retrains/with_prop_line_tol002/mlb_run_batter_hits_20260525_151340
```

Equivalent tolerance-0.005 artifact:

```text
src/models/mlb/artifacts/ranker_retrains/with_prop_line_tol0005/mlb_run_batter_hits_20260525_151349
```

Representative sweep/audit artifact:

```text
backtest_results/ranker_retrains/with_prop_line_tol002_preferred_book_20260413_20260517/audit_suite
```

Metrics:

| Metric | Value |
|---|---:|
| bets | 261 |
| ROI | +25.76% |
| mean CLV | +0.006497 |
| mean CLV CI low | +0.003459 |
| raw-edge Spearman | +0.0828 |
| raw-edge CI low | -0.0418 |
| top-minus-bottom CLV | +0.00667 |
| top book | DraftKings, ~40.2% |
| ESPNBet share | ~0.77% |
| ProphetX share | ~0.77% |

Interpretation:

- Best current preferred-book flat-paper candidate.
- Book concentration is no longer ESPNBet/ProphetX-dependent.
- Edge-ranking is closest to useful but still fails because CI low is below zero.
- Use flat threshold-only paper tracking. Do not use Kelly or edge-proportional sizing.

## Frozen baseline 2: clean ablation baseline

Use this when the goal is model/feature-family experimentation and cleaner causal interpretation without prop-line dependency.

```text
model variant: no_prop_line
selector tolerance: either 0.02 or 0.005; outputs were identical
routing policy: preferred_book_first
primary config: config_02_no_BL_edge0.12_kelly0.125
edge threshold: 0.12
staking: flat / threshold-only paper; do not Kelly-size
status: clean ablation baseline; not live-money deployable
```

Representative model artifact:

```text
src/models/mlb/artifacts/ranker_retrains/no_prop_line_tol002/mlb_run_batter_hits_20260525_151344_no_prop_line
```

Equivalent tolerance-0.005 artifact:

```text
src/models/mlb/artifacts/ranker_retrains/no_prop_line_tol000/mlb_run_batter_hits_20260525_151404_no_prop_line
```

Representative sweep/audit artifact:

```text
backtest_results/ranker_retrains/no_prop_line_tol002_preferred_book_20260413_20260517/audit_suite
```

Metrics for primary clean baseline, edge=0.12:

| Metric | Value |
|---|---:|
| bets | 455 |
| ROI | +11.94% |
| mean CLV | +0.007061 |
| mean CLV CI low | +0.005388 |
| raw-edge Spearman | -0.0676 |
| raw-edge CI low | -0.1782 |
| top book | DraftKings, ~40.7% |
| ESPNBet share | ~0.44% |
| ProphetX share | 0.00% |

Interpretation:

- Best clean baseline for future ablations where `prop_line_batter_hits` should not influence model behavior.
- Strongest mean CLV CI low among preferred-book decision-grade configs.
- Edge-ranking remains weak, so use as flat threshold-only baseline.

## Frozen baseline 3: broad clean paper baseline

Use this when sample size / broad monitoring matters more than ROI.

```text
model variant: no_prop_line
selector tolerance: either 0.02 or 0.005; outputs were identical
routing policy: preferred_book_first
config: config_01_no_BL_edge0.1_kelly0.125
edge threshold: 0.10
staking: flat / threshold-only paper; do not Kelly-size
status: broad clean paper baseline; not live-money deployable
```

Metrics:

| Metric | Value |
|---|---:|
| bets | 783 |
| ROI | +11.13% |
| mean CLV | +0.006688 |
| mean CLV CI low | +0.005157 |
| raw-edge Spearman | -0.0223 |
| raw-edge CI low | -0.0970 |
| top book | DraftKings, ~41.0% |
| ESPNBet share | ~0.89% |
| ProphetX share | ~0.51% |

Interpretation:

- Broadest clean preferred-book config.
- Useful for paper monitoring and detecting drift because it produces the most bets.
- Not a sizing signal.

## What is intentionally not frozen as a baseline

### Lowest-vig variants

Lowest-vig configs are useful as controls only.

Reason:

- They retain ESPNBet+ProphetX concentration around ~77-86%.
- Preferred-book routing keeps positive ROI/CLV while deconcentrating book exposure.

### BL/tau configs

BL/tau configs are not frozen as baselines.

Reason:

- Several are tiny or underpowered.
- Some ranker runs crashed on empty/tiny bucket rows.
- No BL/tau config changed the core conclusion that edge-ranking remains unconfirmed.

### Selector tolerance 0.005 vs 0.02

Tolerance is not a current experimental axis.

Reason:

- `with_prop_line_tol002` and `with_prop_line_tol0005` produced identical feature manifests and evaluation metrics.
- `no_prop_line_tol002` and `no_prop_line_tol0005` produced identical feature manifests and evaluation metrics.

## Required use in future ablations

For each future `batter_hits` ablation or model rerun:

1. Compare against the clean no-prop-line baseline first.
2. Use preferred-book routing as the primary operational routing policy.
3. Use compact fixed configs first:
   - edge 0.10
   - edge 0.12
   - edge 0.15 only if bet count remains >=100
4. Run CLV-only selected-config audit before full dropout/bucket certification.
5. Run ranker diagnostics before interpreting edge magnitude.
6. Run book sensitivity only for configs that survive mean-CLV and bet-count filters.
7. Keep live-money and Kelly/tiered sizing blocked until a ranker passes with positive block-bootstrap CI low.

## Current decision summary

| Candidate | Role | Status |
|---|---|---|
| with_prop_line preferred_book edge=0.12 | best operational flat-paper candidate | freeze |
| no_prop_line preferred_book edge=0.12 | clean ablation baseline | freeze |
| no_prop_line preferred_book edge=0.10 | broad clean paper baseline | freeze |
| lowest-vig variants | controls only | do not promote |
| BL/tau variants | exploratory only | do not promote |
