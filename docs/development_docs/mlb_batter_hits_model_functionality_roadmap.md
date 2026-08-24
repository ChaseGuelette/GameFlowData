# MLB Batter Hits Model Functionality Roadmap

> **Methodology update (2026-07-27):** This roadmap's mandatory CLV-ranker-before-feature-work
> ordering is superseded by the
> [flat-first lifecycle plan](../../.hermes/plans/2026-07-27_204057-flat-first-model-selection-lifecycle.md).
> Feature families are now compared rapidly under one common raw/no-BL, quote-clean, flat-stake
> protocol. Broad BL selection happens only after model finalists are chosen. Independent-window
> ROI/profit, adequate volume, Sharpe, drawdown, dropout/timing integrity, and frozen forward-paper
> performance are the core flat-certification evidence. Mean CLV remains a finalist diagnostic;
> CLV ranking, edge-bucket monotonicity, and Kelly belong to a separate optional sizing lane.
> Historical diagnostics and failure analyses below remain valid evidence but are no longer the
> default feature-iteration gate order.

**Purpose:** A future-facing roadmap for turning MLB batter_hits research/backtests into a functional, promotion-ready model and betting policy.

**Current posture:** Caution / paper-only. Dense CLV evidence suggests there may be mild average edge, but promotion is blocked by temporal-safety verification, bookmaker concentration, and edge-ranking failure.

**Primary current artifacts:**
- Sweep output: `backtest_results/mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517/`
- Audit wrapper: `scripts/run_mlb_quote_clean_audit_suite.py`
- Dropout audit: `scripts/audit_mlb_quote_clean_dropout.py`
- CLV failure diagnosis: `scripts/diagnose_mlb_clv_failure_modes.py`
- Line selection: `src/backtesting/mlb/line_selection.py`
- Backtest engine: `src/backtesting/mlb/run_mlb_sweep.py`

---

## Relevant prior lessons and invariants

These are the guardrails for every step in this roadmap.

1. **Empirical CDF for probabilities**
   - Always calculate probabilities from Monte Carlo samples with empirical CDF, e.g. `(samples > line).mean()`.
   - Never use Gaussian CDF for prop probabilities.

2. **Temporal integrity is mandatory**
   - Feature generation must only use information available before the target game / decision time.
   - For quote/CLV work, enforce: selected quote time <= decision time, and close quote time after selected quote time.

3. **Quote-clean CLV before feature work**
   - Do not add MLB features just because an ROI backtest is not promotion-grade.
   - First validate production-equivalent quote selection, CLV, timing, and selection policy.

4. **Positive mean CLV is not enough**
   - If average CLV is positive but edge does not rank CLV, raw edge may only be a binary bettable/not-bettable discriminator.
   - Do not use aggressive Kelly sizing from raw edge magnitude until a ranking signal is verified.

5. **Cheap baseline before architecture**
   - Before new model architectures or heavy feature work, test the simplest controlled baseline that can explain the failure mode.

6. **Feature selector is not an ablation**
   - A feature selector dropping a feature does not prove the feature family lacks betting value.
   - Validate feature families with force-include / force-exclude comparisons and downstream betting/CLV gates.

7. **Correlated feature families must be validated as families**
   - Correlated features can substitute for each other during selection.
   - Validate weather, lineup, pitcher matchup, recent form, and market-derived features as families first, then prune.

---

## Roadmap overview

The model becomes functional only after these gates pass in order:

1. **Audit and temporal-safety gate**
2. **Dense CLV quality gate**
3. **Bookmaker concentration / ESPNBet gate**
4. **Edge-ranking and staking gate**
5. **Feature-expansion decision gate**
6. **Paper-trading / production-readiness gate**

Do not skip ahead to feature work or live-money promotion while earlier gates are unresolved.

---

## Gate 1: Audit and temporal-safety gate

### Goal

Prove the current backtest uses the correct model artifacts and that selected quotes are temporally valid.

### Current issue

The dropout audit previously failed because the audit suite used:

```text
--model-dir src/models/mlb/artifacts
```

But production MLB artifacts are under:

```text
--model-dir src/models/mlb/artifacts/production
```

This was a command/setup issue, but it also exposed a script robustness issue. `scripts/audit_mlb_quote_clean_dropout.py` has now been patched to fail early when it loads zero requested models, instead of crashing later with a misleading `dropout_bucket` error.

### Required rerun

Run the audit suite with the production model directory:

```bash
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py \
  --local \
  --sweep-output-dir backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517 \
  --output-dir backtest_results\audits\suite_selected5_mlb_batter_hits_dense_slate_t60_20260413_20260517_prodmodel \
  --model-dir src\models\mlb\artifacts\production \
  --start 2026-04-13 \
  --end 2026-05-17 \
  --stats batter_hits \
  --quote-decision-policy slate_or_tminus \
  --quote-relative-minutes 60 \
  --line-source mlb_player_props_clv_snapshots \
  --snapshots-table mlb_player_props_clv_snapshots \
  --bets-csv backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517\config_02_tau0.5_edge0.05_kelly0.125\bets.csv \
  --bets-csv backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517\config_03_tau0.5_edge0.05_kelly0.125\bets.csv \
  --bets-csv backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517\config_04_tau0.5_edge0.08_kelly0.125\bets.csv \
  --bets-csv backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517\config_30_tau0.9_edge0.08_kelly0.125\bets.csv \
  --bets-csv backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517\config_31_tau0.9_edge0.1_kelly0.125\bets.csv
```

### Pass criteria

- Dropout audit exits 0.
- Required models load from `src/models/mlb/artifacts/production`.
- No selected quote has `selected_snapshot_time > selected_decision_time`.
- No or near-zero selected bets occur at or after game commence.
- Dense CLV coverage is acceptable.
- Any remaining warnings are explained and documented.

### Fail response

If temporal/audit gates fail, do not tune thresholds, add features, or promote the policy. Fix timing/model loading first.

---

## Gate 2: Dense CLV quality gate

### Goal

Confirm whether the current signal is real under dense, quote-clean CLV rather than legacy raw-line noise.

### Current working interpretation

The dense-table run appears materially cleaner than legacy raw-line runs. Current selected configs show positive ROI and mildly positive mean CLV, but edge-ranking is statistically inconclusive.

Known examples from current context:

- Config 02:
  - n = 206
  - scored = 183
  - mean CLV = +0.0029
  - Spearman CI low = -0.137

- Config 31:
  - n = 200
  - scored = 174
  - mean CLV = +0.0034
  - Spearman CI low = -0.169

### Pass criteria

A config can advance only if:

- Mean CLV is positive after quote-clean timing checks.
- CLV confidence interval is acceptable for the intended stage.
- Same-book CLV coverage is high enough to avoid excessive consensus fallback dependence.
- CLV result is not driven by obvious timing artifacts or a single broken book/odds band.

### Fail response

If mean CLV is not robust after audit cleanup, treat the model as not currently functional for betting. Return to source-data audits and model diagnostics before threshold tuning.

---

## Gate 3: Bookmaker concentration / ESPNBet gate

### Current diagnosis

We have not finalized a deconcentration policy yet. What we have is a diagnosis:

- Config 02 got `bookmaker_cluster_failure` because ESPNBet had >60% of rows.
- The diagnosis threshold is hardcoded in `scripts/diagnose_mlb_clv_failure_modes.py`:
  - top bookmaker share > 60% => `bookmaker_cluster_failure`

### Why ESPNBet concentration happens

- The current line selection path picks valid quote-clean lines, then the backtest chooses the best / lowest-vig / value line available.
- If ESPNBet frequently offers the selected side/price, selected bets naturally cluster there.
- Current `run_mlb_sweep.py` does not expose a clean CLI flag to cap or exclude a bookmaker for MLB sweeps, even though `line_selection.py` internally supports bookmaker allowlisting.

### Deconcentration options, least to most invasive

#### 1. Analysis-only deconcentration

Keep the sweep as-is and inspect:

- `clv_by_bookmaker.csv`
- bet counts by bookmaker
- mean CLV by bookmaker
- edge-ranking by bookmaker

Decision rule:

- If ESPNBet is positive and other books are flat/negative, this is not a broad model edge; it is a book-specific opportunity or artifact.
- If non-ESPNBet books also show positive CLV, the edge is more likely model/market-wide.

#### 2. Exclude ESPNBet sensitivity run

Add or patch an MLB sweep CLI option such as:

```text
--exclude-bookmakers espnbet
```

Then rerun candidate configs.

Decision rule:

- If edge survives without ESPNBet, it is more robust.
- If edge dies without ESPNBet, policy is book-specific and should not be generalized.

#### 3. Book cap policy

Add a cap such as:

```text
max 40-50% selected bets from any one book
```

This is more realistic for scalable deployment, but it changes the selection policy. It must be preregistered before interpreting ROI/CLV.

Decision rule:

- If capped policy keeps positive CLV and acceptable volume, it is a better production candidate than uncapped best-line chasing.
- If capped policy kills CLV, current edge is too concentrated for broad deployment.

#### 4. Consensus-first selection

Instead of selecting only the single best book line, select only when the model beats consensus/median book.

This naturally deconcentrates and tests whether the model has market-wide edge rather than one-book price anomalies.

Decision rule:

- If consensus-first survives, this is a stronger model-edge signal.
- If only best-book survives, treat the result as execution/book-specific.

### Recommended path

1. Inspect `clv_by_bookmaker.csv` across the selected configs.
2. Run an ESPNBet-excluded sensitivity.
3. If the signal survives, compare consensus-first and bookmaker-cap policies as production candidates.

### Pass criteria

- Top bookmaker share is below the agreed cap, or the concentration is explicitly accepted as a book-specific strategy.
- Non-ESPNBet subset does not collapse completely.
- CLV and edge-ranking diagnostics are reported separately for all books, ESPNBet only, and non-ESPNBet.

---

## Gate 4: Edge-ranking and staking gate

### Current failure meaning

The current failure is not necessarily “model has no edge.” It means:

- Average CLV is mildly positive.
- Edge magnitude does not rank CLV reliably.
- Higher edge does not reliably mean better subsequent market movement.

### Possible causes

#### 1. Edge score / calibration issue

Model probability might identify bettable vs non-bettable, but the numerical edge size is noisy.

If so, flat or near-flat staking beats Kelly-by-edge.

#### 2. Market-price issue

Some books or odds bands may move differently. Edge ranking may be polluted by bookmaker mix, plus-odds bands, line availability, or consensus fallback behavior.

#### 3. Model training / calibration issue

If model probabilities are poorly calibrated in the selected region, edge magnitude becomes unreliable.

Do not jump to retraining first. CLV says there may be signal; the failure is currently ranking quality, not necessarily raw model skill.

#### 4. Feature issue

Missing features can hurt ranking, but adding features before fixing/evaluating edge score is premature.

### Required ranking diagnostics

Test Spearman/CLV and bucket CLV by:

- `model_prob`
- `implied_prob`
- raw edge
- `logit(model_prob) - logit(implied_prob)`
- `model_prob * abs(edge)`
- selected price / odds band
- bookmaker-adjusted edge
- consensus-vs-selected-book edge
- Black-Litterman probability vs raw model probability, if both are stored

### Required segmentation

Run the ranking diagnostics separately for:

- all books
- ESPNBet only
- non-ESPNBet only
- same-book CLV only
- consensus fallback excluded
- plus-odds bands, especially `+150_plus`
- config-level results and pooled selected-config results

### Existing files to inspect

- `clv_by_plus_odds_band.csv`
- `clv_by_edge_bin.csv`
- `clv_by_bookmaker.csv`

Current note:

- Some `+150_plus` buckets look promising, especially config 31.
- Sample sizes are small and Spearman still fails.
- Do not kill or promote an odds band from this alone.

### Ranking pass criteria

A score can support edge-based staking only if:

- Spearman(edge-like score, CLV) has CI low > 0, or another preregistered threshold Chase accepts.
- Bucketed CLV is monotonic or directionally consistent enough to justify sizing.
- The signal survives bookmaker and odds-band segmentation.

### If no ranking score works

Use a simpler policy:

- flat stake
- low fixed fraction
- cap max bet
- threshold-only bet/no-bet filter

Do not use aggressive Kelly based on edge magnitude.

### Production implication

If ranking fails but average CLV survives, the model may still be functional as a binary selector. It is not yet functional as a Kelly/edge-sized strategy.

---

## Gate 5: Feature-expansion decision gate

### Principle

Do not add features just because ROI/CLV is not promotion-grade. Add features only after the selection-policy and edge-ranking failures are localized.

### Questions to answer before adding features

#### 1. Is the backtest temporally safe?

Need:

- dropout audit rerun with production model directory
- no selected quote after decision time
- no or near-zero selected bets after commence
- dense CLV coverage acceptable

#### 2. Is the current signal real but poorly ranked?

If mean CLV > 0 but edge ranking fails, first work on:

- scoring
- calibration
- ranking alternatives
- bookmaker/odds-band segmentation
- staking policy

Features are second, not first.

#### 3. Is there a residual pattern features could plausibly explain?

Look for misses or weak CLV clustering by:

- pitcher handedness
- lineup position
- ballpark/weather
- opposing pitcher quality
- batter recent contact quality
- bookmaker/price regime
- early vs night slate
- odds band

#### 4. Can a cheap baseline beat the feature idea?

Before training:

- run stratified diagnostics
- compare flat threshold policies
- compare consensus/market-only baseline
- compare simple book/odds filters

If a cheap market/selection policy fixes the issue, do not build model complexity to solve a policy problem.

#### 5. Can the feature family pass force-include / force-exclude validation?

Evaluate by family:

- weather
- lineup / slot
- pitcher matchup
- batter handedness / splits
- recent form / contact quality
- market-derived features

Use ablations. Do not trust feature selector decisions alone.

### Feature-work pass criteria

A feature family is worth advancing only if it improves at least one promotion-relevant target without breaking the rest:

- quote-clean mean CLV
- edge-ranking / CLV Spearman
- calibration in the selected betting region
- ROI under quote-clean replay
- drawdown / volatility
- bet volume
- bookmaker concentration
- temporal-safety audit

---

## Gate 6: Paper-trading / production-readiness gate

### Required before live-money promotion

- Gate 1 audit passes.
- Dense CLV remains positive after timing cleanup.
- Bookmaker concentration is either controlled or explicitly accepted as book-specific.
- Edge-ranking issue is resolved, or staking is downgraded to flat/threshold-only.
- Same-book CLV is healthy enough to avoid relying on consensus fallback artifacts.
- Paper trading verifies the same policy on fresh data.
- Intraday quote stability is measured at practical decision windows.

### Paper policy candidates

#### Candidate A: Flat-stake threshold policy

Use if average CLV survives but ranking fails.

- Bet when model passes selected threshold.
- Use flat stake or very low fixed fraction.
- No aggressive Kelly.

#### Candidate B: ESPNBet-excluded robust policy

Use if non-ESPNBet edge survives.

- Exclude or cap ESPNBet.
- Prefer stable same-book CLV.
- More scalable and less book-specific.

#### Candidate C: Consensus-first market-wide policy

Use if consensus-vs-model signal survives.

- Bet only when model beats consensus/median book enough.
- Naturally reduces single-book concentration.
- Stronger evidence of model edge.

#### Candidate D: Book-specific ESPNBet policy

Use only if ESPNBet is the clear source of edge and Chase accepts book-specific execution risk.

- Explicitly label as book-specific.
- Cap exposure.
- Monitor book-level CLV decay.
- Do not generalize to all books.

---

## Implementation backlog

### P0: Clear the audit blocker

- Rerun the selected-5 audit suite with `--model-dir src/models/mlb/artifacts/production`.
- Confirm the patched dropout audit no longer fails with misleading empty-prediction errors.
- Summarize audit status in the output directory.

### P1: Bookmaker concentration report

Create or extend a report that reads selected config audit outputs and produces:

- top bookmaker share by config
- mean CLV by bookmaker
- same-book CLV coverage by bookmaker
- edge-ranking by bookmaker
- ESPNBet vs non-ESPNBet summary

### P1: ESPNBet-excluded sensitivity

Patch MLB sweep CLI to support either:

```text
--exclude-bookmakers espnbet
```

or a more general pair:

```text
--include-bookmakers ...
--exclude-bookmakers ...
```

Then rerun the candidate configs without ESPNBet.

### P1: Edge-ranking diagnostics

Build a diagnostics script/report that evaluates alternative ranking scores and outputs:

- Spearman(score, CLV) with bootstrap CI
- bucketed CLV by score decile/bin
- segmentation by bookmaker
- segmentation by odds band
- same-book-only result
- consensus-fallback-excluded result

### P2: Consensus-first / bookmaker-cap policy experiments

Add preregistered selection-policy variants:

- current best-line policy
- ESPNBet-excluded policy
- max-book-share capped policy
- consensus-first policy

Compare on the same windows/configs.

### P2: Feature-residual diagnostics

Only after P0/P1 are complete, produce residual diagnostics by:

- pitcher handedness
- lineup slot
- ballpark/weather
- opposing pitcher quality
- batter recent form/contact
- odds band
- book
- slate timing

Use this to decide feature-family candidates.

### P3: Feature family experiments

For each feature family:

1. Define the exact hypothesis.
2. Build the cheapest baseline first.
3. Run force-include / force-exclude family ablation.
4. Compare downstream quote-clean CLV and ranking quality.
5. Promote only if it improves model function without creating new audit failures.

---

## Decision tree

```text
Start
  |
  v
Audit passes with production model dir?
  |-- No --> Fix timing/model loading. Stop.
  |
  v
Mean dense quote-clean CLV positive and credible?
  |-- No --> Source/model diagnostic. Stop threshold tuning.
  |
  v
Edge ranking passes?
  |-- Yes --> Consider edge-sized staking with caps.
  |-- No --> Treat edge as binary; test flat/threshold staking.
  |
  v
Book concentration acceptable?
  |-- No --> ESPNBet-excluded, bookmaker-cap, or consensus-first policy.
  |
  v
Does robust/capped/consensus policy survive?
  |-- Yes --> Paper trade candidate.
  |-- No --> Book-specific only or return to modeling.
  |
  v
Feature residuals show a plausible missing signal?
  |-- No --> Do not add features; improve policy/execution.
  |-- Yes --> Run cheap baseline + feature-family ablation.
```

---

## Near-term recommended sequence

1. Rerun audit suite with production model directory.
2. Compare `clv_by_bookmaker.csv` across selected configs.
3. Add or patch bookmaker exclude support for MLB sweeps.
4. Run ESPNBet-excluded sensitivity.
5. Build edge-ranking diagnostics for alternative ranking scores.
6. Decide staking mode:
   - ranking passes => capped edge-sized staking candidate
   - ranking fails but mean CLV survives => flat/threshold-only paper candidate
7. Only then evaluate new feature families from residual diagnostics.

---

## Promotion standard

A model/policy is “functional” only if it has:

- temporally safe quote selection
- dense quote-clean CLV evidence
- acceptable same-book CLV coverage
- explicit bookmaker concentration policy
- verified staking policy appropriate to ranking quality
- paper-trading evidence on fresh data
- feature changes justified by residual diagnostics and ablations

Until then, treat positive ROI as hypothesis-generating, not production-grade.
