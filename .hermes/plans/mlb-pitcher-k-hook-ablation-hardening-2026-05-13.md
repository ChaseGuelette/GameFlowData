# MLB Pitcher K Hook Ablation Hardening Plan

> **For Hermes:** Do not proceed to Phase 1 implementation until Chase explicitly approves this final plan file. If approved later, use GameFlow skills before acting: `gameflow-model-evaluation`, `gameflow-explore`, and `gameflow-implementation-worker` if code changes exceed tiny/single-file scope.

**Goal:** Harden the MLB pitcher strikeout `team_starter_deep_start_rate_l30` ablation before any live/paper shadow test, by fixing quote methodology, locking statistical decision rules, validating feature semantics/mechanism, and requiring paired/CLV analysis.

**Architecture:** Treat the hook feature as a promising but unpromoted candidate. First run quote-clean, fixed-config, production-sizing and flat-stake ablations; only if those pass do we design/start a shadow test. Shadow approval is explicitly out of scope for this plan until Phases 0–4 are complete and Chase separately approves.

**Tech Stack:** Python MLB model/backtest pipeline, GameFlowData MLB feature store, `run_mlb_sweep.py`, local/remote prop line tables, markdown report artifacts.

---

## Current grounded facts

### Feature semantics already checked

`team_starter_deep_start_rate_l30` is pitcher-side / own-team, not opponent-side.

Evidence from source discovery:

- `src/models/mlb/mlb_feature_store.py`
- Training path selects `COALESCE(team_leash.deep_start_rate_l30, 0) AS team_starter_deep_start_rate_l30`.
- The lateral source uses `WHERE team_id = pgs.team_id` and `game_date < pgs.game_date`.
- Backtest/date feature path uses the same own-team and prior-games-only semantics.
- Single-player inference path passes `team_id` into `_get_team_starter_leash_features()`.

Interpretation:

- Causal story is clean enough to continue: own team/manager starter leash tendency can affect expected starter IP/opportunity, which can affect pitcher strikeouts.
- Still verify all paths again in Phase 3 before any promotion.

### Current backtest quote issue

Existing MLB sweep/backtest odds selection is not promotion-grade for this decision.

Observed source behavior:

- `src/backtesting/mlb/run_mlb_sweep.py`
- `src/backtesting/mlb/mlb_backtest_harness.py`
- Queries group `mlb_raw_player_props` across available snapshots.
- They take `MAX` over odds and `MAX` under odds per grouped line/book.
- There is no production-time `snapshot_time` cutoff.
- There is no opening/closing/latest-before-game selector.
- This likely overstates executable price quality.

Implication:

- Any existing ROI/Sharpe from this quote logic is useful for candidate ranking only.
- Promotion-grade conclusions require quote-clean re-runs.

### Current staking behavior

- Default sweep/backtest sizing is fractional Kelly-style sizing.
- Default observed settings: `kelly_fraction=0.125`, `starting_bankroll=10000`.
- `run_mlb_sweep.py` already supports flat staking via `--flat` / `--flat-bet`.
- `run_mlb_backtest.py` does not need flat-stake CLI work for this plan; use sweep support instead.

---

## Relevant prior lessons and invariants

Apply these throughout the work:

- Feature selector is not an ablation. Use force-include / force-exclude downstream comparisons.
- Correlated feature families need family-level validation before pruning to a minimal representation.
- Cheap baseline before architecture. Do not escalate to survival/IP complexity unless the cheap hook baseline proves useful or exposes a specific failure mode.
- Probabilities from Monte Carlo samples must use empirical CDF: `(samples > line).mean()`.
- Never deploy global conformal recalibration offsets.
- Do not promote from short-window swept-grid point estimates.
- Treat absolute ROI above roughly 7% over short prop windows as likely hot/selection-inflated until quote-clean and confidence-checked.

---

## Phase 0 — Pre-commit evaluation contract

**Objective:** Lock the statistical and interpretation rules before running more tests.

### 0.1 Primary comparison config

Use this as the only promotion-grade config:

```text
tau=0.75
z_max=0.25
max_weight=0.65
edge=0.02
```

Rationale:

- This is the static-ish config selected from baseline/static behavior.
- It is the cleanest test of whether adding `team_starter_deep_start_rate_l30` improves the model.
- It avoids promoting based on a hook-optimized configuration.

### 0.2 Exploratory-only config

This config may be reported, but cannot drive promotion:

```text
tau=0.9
z_max=0.25
max_weight=0.8
edge=0.05
```

Rules:

- Label as exploratory.
- Do not use it alone for shadow approval or model promotion.

### 0.3 ROI interpretation rules

Pre-commit these interpretations:

- Absolute ROI point estimates are not trusted at face value over sub-30-day or ~100-bet windows.
- Expected realistic long-run hook lift is 1–3 percentage points ROI, not 5–8 points.
- A 1–3 pp lift can be practically meaningful, but may be statistically underpowered at 300 bets/arm.
- The relative hook-vs-static gap is more informative than either arm's raw ROI.

### 0.4 Statistical power rule

Back-of-envelope baseline:

- At 300 bets/arm, iid SE of ROI difference is about `sqrt(2/300) = 8.16 pp`.
- One-sided `P(hook > static) >= 80%` needs roughly a 6.9 pp observed gap under iid assumptions.
- Game-date/slate block correlation likely raises the effective SE; an 80% threshold may require roughly 7.7–9.7 pp observed gap depending on design effect.

Therefore:

- Do not combine a `>=2 pp` observed ROI threshold with `P>=80%` as if both are equally easy to clear.
- Before any shadow launch, compute a historical block-bootstrap detectable gap using actual daily clustering.

Promotion-grade statistical rule for a future shadow:

```text
required_gap = max(2.0 pp, detectable_gap_80_from_historical_block_bootstrap)
Promote only if:
  hook ROI - static ROI >= required_gap
  AND P(hook ROI > static ROI) >= 80% by game_date block bootstrap
```

Interpretation:

- If hook leads by 1–3 pp with positive CLV but fails the 80% bootstrap criterion, that is a continue/extend signal, not a promotion signal.
- Do not lower confidence to 70% unless Chase explicitly approves a faster, higher-false-positive-risk policy.

### 0.5 Approval gate

Do not start Phase 1 until Chase approves this final plan file.

---

## Phase 1 — Quote-clean backtest pricing

**Objective:** Re-run static vs hook with line and odds selection that matches production reality.

### 1.1 Discover production quote behavior

Find and document the production MLB pitcher K line-selection behavior.

Required questions:

1. What snapshot cutoff does production use?
   - latest at inference time?
   - latest before game start?
   - a specific scheduled job time?
2. What bookmaker universe does production use?
   - all allowed books?
   - excluded books?
   - specific preferred book?
3. What line selection rule does production use?
   - best available across allowed books?
   - lowest-vig line?
   - consensus line?
   - specific book line?
4. What side selection rule does production use?
   - model picks best edge by side?
   - allowed direction restrictions?
   - any stat-specific restrictions?
5. What happens when multiple books/lines are available at the same snapshot?
   - tiebreaker rule
   - stale-line filtering
   - minimum book count, if any

### 1.2 Define quote-clean backtest rule

Before re-running ablations, write the exact rule in the report.

Minimum required fields:

```text
Snapshot cutoff: <exact rule>
Bookmaker universe: <exact list/rule>
Book exclusions: <exact list/rule>
Line selection: <exact rule>
Side selection: <exact rule>
Tiebreakers: <exact rule>
Staleness filter: <exact rule or none>
```

Non-negotiable:

- No `MAX` odds across all historical/future snapshots.
- No closing-line leakage unless production actually bets closing lines, which it does not.
- No future snapshots relative to the simulated production decision time.

### 1.3 Implement or use re-quote helper

Preferred approach:

- Add the smallest possible re-quote helper/mode around the existing MLB sweep flow.
- Do not rewrite the backtest framework unless necessary.
- Use existing `run_mlb_sweep.py` for repeated comparisons.

Required behavior:

- Filter `mlb_raw_player_props` to snapshots available at the simulated production decision time.
- Select latest eligible snapshot per production-equivalent rule.
- Apply production-equivalent book/line/side selection.
- Save enough columns for audit:
  - `snapshot_time`
  - `bookmaker`
  - `line`
  - `over_odds`
  - `under_odds`
  - chosen side
  - chosen odds
  - game start time if available

### 1.4 Re-run quote-clean windows

Run primary fixed-config comparisons on:

1. 2026 April validation window
2. 2025 September independent validation window

Each window must compare:

- static baseline artifact
- hook-only artifact
- same config
- same quote rule
- same staking rule
- same date window

### 1.5 Phase 1 acceptance criteria

Pass only if:

- The report explicitly documents snapshot and line/book selection rules.
- Static and hook arms use identical quote logic.
- Results distinguish old optimistic quote logic from quote-clean logic.
- No promotion-grade conclusion depends on the old `MAX` odds across all snapshots.

---

## Phase 2 — Stake sanity with existing sweep support

**Objective:** Determine whether hook improvement is robust to staking assumptions.

### 2.1 Production-sizing comparison

Run fixed-config comparison with production-equivalent sizing.

Default unless production differs:

```text
kelly_fraction=0.125
starting_bankroll=10000
max_bet_pct=<production default or none>
```

### 2.2 Flat-stake comparison

Use existing `run_mlb_sweep.py` support:

```text
--flat 100
```

Do not add `run_mlb_backtest.py` flat CLI plumbing as part of this plan unless a later blocker proves it necessary.

### 2.3 Required metrics

For both production-sizing and flat-stake runs, report:

- bet count
- wins / losses / pushes if available
- ROI
- profit
- total staked
- return on capital
- Sharpe
- max drawdown
- average stake
- average line
- average odds
- average modeled edge
- Over/Under side split

### 2.4 Phase 2 acceptance criteria

- Hook improvement should not vanish under flat staking.
- If hook only wins under Kelly sizing but not flat stake, mark as exploratory/staking interaction.
- Promotion-grade evidence requires hook to be directionally better under both production sizing and flat stake, even if magnitude shrinks.

---

## Phase 3 — Causal and mechanism checks without double-dipping

**Objective:** Verify that the hook feature behaves like a workload/opportunity feature rather than a spurious confound.

### 3.1 Data-slice rule

Do mechanism slicing on:

- training-set data, or
- a separate non-promotion diagnostic slice.

Do not use the same validation windows for both:

- promotion-grade ablation decisions, and
- post-hoc causal/mechanism mining.

### 3.2 Required semantic checks

Verify across training, backtest, and inference paths:

- pitcher own team, not opponent team
- same season
- prior games only
- no current-game leakage
- non-default variation in train/calibration/test

### 3.3 Required mechanism checks

On training or separate diagnostic slice, evaluate:

- `team_starter_deep_start_rate_l30` vs actual starter IP
- `team_starter_deep_start_rate_l30` vs actual strikeouts
- feature buckets vs model residuals
- whether the hook feature reduces early-hook false positives
- whether gains concentrate in workload/leash-sensitive pitcher cohorts

### 3.4 Required confound checks

Look for concentration by:

- team
- opponent
- bookmaker
- line bucket
- side
- date/slate
- pitcher quality bucket

### 3.5 Phase 3 acceptance criteria

Pass only if:

- Feature has real non-default variation.
- Mechanism plausibly runs through starter opportunity/IP.
- No obvious single-team/book/line/date confound explains the edge.
- Any mechanism findings are clearly separated from promotion-window ablation results.

---

## Phase 4 — Fixed-config ablation matrix

**Objective:** Produce the final pre-shadow evidence package.

### 4.1 Promotion-grade matrix

Run all of these under quote-clean logic:

A. Static baseline, static-ish config, production sizing
B. Hook-only model, same static-ish config, production sizing
C. Static baseline, same static-ish config, flat stake
D. Hook-only model, same static-ish config, flat stake

Static-ish config:

```text
tau=0.75
z_max=0.25
max_weight=0.65
edge=0.02
```

### 4.2 Exploratory-only matrix

Optional, clearly labeled exploratory:

E. Hook model, hook-selected Sharpe-stable config
F. Static model, hook-selected Sharpe-stable config

Exploratory config:

```text
tau=0.9
z_max=0.25
max_weight=0.8
edge=0.05
```

### 4.3 Required paired/overlap analysis

This is required, not optional.

Report:

1. Overlap bets where both arms agree on player/game/side/line
   - ROI should be near-identical.
   - If not, investigate odds/stake mismatch.
2. Overlap games where both arms bet but differ
   - side differs
   - line differs
   - stake differs materially
3. Static-only bets
   - ROI
   - CLV if available historically
   - side/line/book distribution
4. Hook-only bets
   - ROI
   - CLV if available historically
   - side/line/book distribution
   - this is the key marginal-selection bucket.
5. Bet-vs-skip paired cases
   - hook bets where static skipped
   - static bets where hook skipped

Interpretation:

- The cleanest evidence for added signal is in paired and marginal-selection behavior.
- If hook only wins on aggregate shared bets, it is not adding useful selection signal.
- If hook-only bets are bad, do not promote even if aggregate arm ROI looks better.

### 4.4 Block bootstrap uncertainty

Use game_date block bootstrap for ROI differences.

Report:

- mean ROI difference
- median ROI difference
- 20th / 5th percentile downside
- `P(hook ROI > static ROI)`
- sample size by arm
- number of unique game_date blocks

### 4.5 Required report sections

The report must include:

- quote-clean headline results
- old optimistic quote results, clearly labeled non-promotion-grade if included
- production-sizing results
- flat-stake results
- side splits
- drawdown
- paired/overlap analysis
- unique hook/static bet analysis
- block-bootstrap uncertainty
- CLV availability status
- explicit recommendation: no-shadow / shadow-ready / continue research

### 4.6 Phase 4 acceptance criteria

Shadow test can be proposed only if:

- Hook survives quote-clean re-run.
- Hook is directionally better under primary static-ish config.
- Hook is directionally better under production sizing and flat stake.
- Paired/marginal analysis supports added signal.
- No obvious side/book/line/date confound dominates.
- Block-bootstrap results are not contradictory to the recommendation.

---

## Phase 5 — Shadow test specification, blocked until Phases 1–4 pass

**Objective:** Define future shadow requirements now, but do not start the shadow in this plan.

### 5.1 Shadow status

Blocked until:

- Phases 1–4 are complete.
- Chase reviews the evidence package.
- Chase explicitly approves a shadow test.

### 5.2 Future shadow arms

If approved later:

1. Static production candidate
2. Hook candidate

Use primary static-ish config only:

```text
tau=0.75
z_max=0.25
max_weight=0.65
edge=0.02
```

### 5.3 Identical conditions across arms

Both arms must share:

- frozen model artifact regime
- same line snapshot rule
- same book/line selection rule
- same bet eligibility filters
- same bankroll
- same staking method
- same empirical CDF probability logic
- same logging schema

### 5.4 Retraining rule

No retraining during the comparison window.

- Production can continue separately if needed.
- The evaluation arms must not change mid-test.
- If retraining becomes unavoidable, reset or segment the shadow test.
- Do not merge pre/post retrain periods into one clean sample.

### 5.5 Sample and calendar limits

Target sample:

- 300 bets/arm minimum before any promotion decision.

Calendar limit:

- 10 weeks maximum.

If 300 bets/arm is not reached within 10 weeks:

- Option A: stop and report reduced-confidence result.
- Option B: refresh artifacts and restart the shadow test.
- Do not silently extend to 14+ weeks with stale frozen artifacts.

### 5.6 CLV requirement

CLV tracking is required for any promotion-grade shadow.

Must log closing line for every placed/evaluated bet:

- closing line
- closing odds if available
- closing timestamp/source
- opening/chosen line if needed for comparison
- CLV by side and line

Interpretation:

- Hook positive CLV + static flat/negative CLV is strong leading evidence, even at modest realized ROI sample sizes.
- Hook negative CLV with positive realized ROI is likely variance/artifact.
- Both arms negative CLV means do not promote regardless of realized ROI.

### 5.7 Future promotion threshold

Before launch, compute:

```text
detectable_gap_80_from_historical_block_bootstrap
required_gap = max(2.0 pp, detectable_gap_80_from_historical_block_bootstrap)
```

Promote only if all hold:

- hook ROI - static ROI >= required_gap
- `P(hook ROI > static ROI) >= 80%` by game_date block bootstrap
- hook CLV > static CLV and preferably positive absolute CLV
- hook does not increase max drawdown by more than 25% relative to static
- side splits do not hide catastrophic degradation
- unique-to-hook bets are non-negative ROI/CLV or clearly better than static-only bets

### 5.8 Continue threshold

If hook leads by 1–3 pp and has better CLV but fails 80% bootstrap:

- classify as continue/extend, not promote.
- continue only if artifacts remain current.
- otherwise restart with refreshed artifacts.

### 5.9 Kill thresholds

Soft kill after 150 bets/arm:

- if hook trails static by >=5 pp ROI and `P(hook < static) >= 80%`, stop.

Hard kill at 300 bets/arm or 10 weeks:

- if hook trails static by >=2 pp ROI, reject.
- if hook CLV is materially worse, reject.
- if hook drawdown is materially worse with no CLV/ROI compensation, reject.
- if edge comes only from one fragile side/book/line bucket, reject or downgrade to exploratory.

---

## Phase 6 — Documentation and final evidence package

**Objective:** Keep all decisions auditable.

### 6.1 Plan file

This plan file:

```text
.hermes/plans/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md
```

### 6.2 Report file

Create/update during implementation:

```text
reports/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md
```

Report must include:

- feature semantics proof
- production quote rule
- production line/book selection rule
- staking method
- static-ish fixed-config results
- exploratory config clearly labeled
- flat-stake sanity
- production-sizing sanity
- paired/overlap analysis
- unique-to-hook / unique-to-static analysis
- CLV status and results
- block bootstrap uncertainty
- promotion/continue/kill recommendation

---

## Implementation discipline after approval

If Chase approves this final plan file:

1. Start Phase 1 only.
2. Use narrow source discovery first; do not broad-search repo root.
3. Prefer existing scripts and small helpers over ad-hoc shell loops.
4. Use `run_mlb_sweep.py` for repeated comparisons and flat-stake validation.
5. If implementation exceeds tiny/single-file scope, use the implementation-worker lane with a precise spec.
6. Verify every changed result against the original failure mode: quote-clean, production-equivalent line selection.
7. Do not start shadow testing without a separate approval after Phase 4 evidence is presented.

---

## Approval status

- Draft requested by Chase: approved to write.
- Final plan file approval: **pending Chase review**.
- Phase 1 implementation: **blocked until Chase explicitly approves this final plan file**.
- Shadow testing: **blocked until Phases 1–4 pass and Chase separately approves**.
