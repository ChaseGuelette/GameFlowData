# Flat-First MLB Model Selection Lifecycle Implementation Plan

> **For Hermes:** Use `subagent-driven-development` to implement this plan task-by-task. Long training, sweep, audit, and paper-trading runs remain Chase-launched.

**Goal:** Restructure the YAML-driven MLB model lifecycle so feature-family discovery is fast and measures underlying model value under a common raw/no-BL flat-stake policy, while BL policy selection, flat certification, and optional Kelly certification are separate later stages.

**Architecture:** Keep one lifecycle entrypoint, `scripts/run_mlb_model_lifecycle.py`, and make its YAML purpose select an explicit stage graph. Do not create disconnected training and BL systems. Preserve ranker/Kelly code as an optional dormant lane, but remove it from feature discovery, BL selection, and flat certification.

**Tech Stack:** Python 3, Pydantic YAML configuration, existing MLB training profiles, quote-clean MLB sweep engine, lifecycle adapters/runner/decision modules, pytest.

---

## 1. Core Operating Model

The lifecycle has four distinct purposes.

### Stage A — `feature_discovery`

Purpose: determine whether a feature family improves the underlying model.

Required behavior:

1. Train a baseline or force-include/force-exclude feature-family candidate with frozen training hyperparameters.
2. Verify artifact identity, calibration, temporal boundaries, feature-source coverage, and non-default historical feature variation.
3. Run the same raw/no-BL, quote-clean, flat-$100 backtest protocol for every artifact.
4. Use the same discovery window, direction, routing, quote cutoff, line source, and fixed edge-threshold set for all candidates.
5. Compare:
   - total profit;
   - ROI;
   - Sharpe ratio;
   - maximum drawdown;
   - bet count;
   - win rate with odds context;
   - Over/Under side splits;
   - paired overlap/disagreement diagnostics when finalists are close.
6. Classify each family as `CONFIRM_FOR_FINALIST_POOL`, `SHELF`, or `EXCLUDE_FROM_ACTIVE_SEARCH`.

Explicitly skip:

- BL parameter sweeps;
- CLV ranker/Spearman diagnostics;
- edge-bucket monotonicity;
- full CLV/dropout/timing audit suite;
- Kelly grids or stake concentration;
- paper-trading recommendations.

Important comparison contract:

- The existing frozen baseline must be separated into its model artifact and its historical BL policy.
- Feature discovery reruns the baseline artifact under the same no-BL policy as every candidate.
- The historical BL policy remains an operational benchmark, not the causal control for feature-family value.
- If the exact baseline artifact is unavailable, reproduce its feature set under the same frozen training/calibration contract and label the result as a reconstructed baseline.

### Stage B — `bl_policy_selection`

Purpose: determine how BL should convert a small set of selected model artifacts into flat-stake bets.

Required behavior:

1. Attach existing baseline/finalist artifacts; do not retrain.
2. Run the same broad quote-clean BL grid for the baseline artifact and each finalist.
3. Use flat $100 stakes only; Kelly remains disabled.
4. Require an adequate sample per cell; never pool bets across cells.
5. Compare qualified cells using:
   - total profit;
   - ROI;
   - Sharpe ratio;
   - maximum drawdown;
   - bet count;
   - side/date robustness;
   - no-BL control performance.
6. Select a small set of defensible model-plus-policy finalists.

Default Batter Hits broad-grid starting point:

- edge thresholds: `[0.05, 0.07, 0.08, 0.10]`;
- tau: `[null, 0.10, 0.25, 0.50, 0.90]`;
- z-max: `[0.25, 0.50, 1.00]`;
- max weight: `[0.50, 0.65]`;
- Kelly values: `[0.0]`;
- flat bet: `100`.

Do not build the previously proposed custom neighbor-stability analyzer or cached volume-scan path for the immediate decision. The current shared prediction/sweep engine is sufficient. Avoid isolated miracle cells through ordinary parameter-neighborhood review rather than a new subsystem.

### Stage C — `flat_certification`

Purpose: determine whether a preselected model-plus-BL policy is credible enough for fixed-dollar forward paper.

Required behavior:

1. Evaluate only preregistered finalists plus the no-BL control.
2. Use a genuinely untouched independent window.
3. Require:
   - strong independent-window profit/ROI;
   - adequate bet volume;
   - acceptable Sharpe;
   - controlled maximum drawdown;
   - quote-clean replay;
   - clean dropout classification;
   - valid decision-time and pre-commence timing evidence.
4. Keep group-level mean CLV and its block-bootstrap confidence interval as supporting evidence.
5. Do not require positive edge-to-CLV ranker confidence intervals.
6. Do not require monotonic edge buckets.
7. Freeze the winning artifact, feature set, BL parameters, edge threshold, directions, routing, timing, and flat stake.
8. Output one of:
   - `FLAT_STAKING_APPROVED`;
   - `FLAT_STAKING_SHELF`;
   - `FLAT_STAKING_REJECTED`.

Mean CLV policy:

- Mean CLV is not needed during ordinary feature-family discovery.
- It is useful as a finalist-level market-validation diagnostic.
- A finalist is not automatically rejected solely because mean CLV's lower confidence bound narrowly crosses zero when independent-window ROI, volume, drawdown, dropout/timing integrity, and forward-paper evidence are otherwise strong.
- Materially negative CLV remains a reason to investigate quote selection, market timing, and outcome variance before approval.

### Stage D — `kelly_certification` (optional and dormant)

Purpose: determine whether edge magnitude is reliable enough to vary stake size.

Only a `FLAT_STAKING_APPROVED` policy may enter this stage.

This stage owns:

- edge-to-CLV or edge-to-quality ranker diagnostics;
- ranker confidence intervals;
- edge-bucket monotonicity;
- flat-versus-capped-Kelly comparisons;
- stake concentration;
- bankroll and drawdown comparisons;
- capped-Kelly forward paper.

Possible outcomes:

- `CAPPED_KELLY_APPROVED`;
- `FLAT_ONLY`;
- `KELLY_UNDERPOWERED`;
- `KELLY_REJECTED`.

Kelly failure must never revoke an existing flat-staking approval.

---

## 2. Window and Selection Discipline

Use three evidence layers:

1. **Discovery window:** may be reused across many feature-family iterations.
2. **Independent finalist window:** remains untouched until the finalist set is preregistered.
3. **Forward-paper window:** evaluates the exact frozen model and policy prospectively.

Rules:

- Repeatedly using an “independent” window to choose the next feature family converts it into a discovery window.
- Do not change feature families, BL parameters, edge thresholds, routing, or directions during forward paper without ending the current certification and creating a new candidate.
- Do not compare each feature candidate using its separately optimized BL policy; that confounds feature-family value with policy-search luck.
- Feature discovery uses a common no-BL policy. BL optimization occurs only after model finalists are selected.

---

## 3. Implementation Tasks

### Task 1: Introduce explicit lifecycle purposes and stage contracts

**Objective:** Replace the conflated discovery/validation/finalist semantics with explicit feature, policy, flat-certification, and Kelly purposes.

**Files:**

- Modify: `src/models/mlb/lifecycle/config.py`
- Test: `tests/test_mlb_lifecycle_config.py`

**Steps:**

1. Add purpose values:
   - `feature_discovery`;
   - `bl_policy_selection`;
   - `flat_certification`;
   - `kelly_certification`.
2. Decide whether old purpose values remain as deprecated aliases or fail with a migration message.
3. Add mode-specific validation:
   - `feature_discovery` requires no-BL tau only, flat stake, and Kelly zero;
   - `bl_policy_selection` requires attached artifacts, flat stake, and Kelly zero;
   - `flat_certification` requires explicit selectors, independent window, and full dropout/timing evidence;
   - `kelly_certification` requires a flat-approved input policy and enables ranker/Kelly fields.
4. Change defaults so Kelly is not silently active; `evaluation.kelly_values` should default to `[0.0]` for flat modes.
5. Add explicit configuration for optional mean-CLV evidence rather than coupling it to the ranker.
6. Write failing tests for every allowed and forbidden purpose/stage combination.
7. Run:
   - `python -m pytest tests/test_mlb_lifecycle_config.py -v`

### Task 2: Make lifecycle stages purpose-aware

**Objective:** Ensure the runner executes only the stages required by the selected purpose.

**Files:**

- Modify: `src/models/mlb/lifecycle/runner.py`
- Test: `tests/test_mlb_lifecycle_runner.py`

**Steps:**

1. Replace the unconditional conceptual stage list with a purpose-to-stage plan.
2. Define expected stage graphs:
   - `feature_discovery`: validate → train/attach → identity → raw sweep → discovery decision;
   - `bl_policy_selection`: validate → attach → identity → broad BL sweep → policy selection;
   - `flat_certification`: validate → attach → identity → explicit independent sweep → dropout/timing audit → flat decision;
   - `kelly_certification`: attach frozen flat policy → CLV/ranker → Kelly comparison → Kelly decision.
3. Ensure skipped stages are recorded as intentionally skipped, not missing or failed.
4. Preserve resumability and stage identity checks.
5. Ensure `bl_policy_selection` cannot trigger training.
6. Ensure `feature_discovery` cannot trigger ranker, full audit, or staking stages.
7. Ensure `flat_certification` cannot trigger ranker or Kelly stages.
8. Add dry-run tests asserting the exact planned commands and skipped stages.
9. Run:
   - `python -m pytest tests/test_mlb_lifecycle_runner.py -v`

### Task 3: Separate raw sweep, dropout/timing, mean CLV, and ranker adapters

**Objective:** Stop treating the current bundled CLV audit suite as one indivisible requirement.

**Files:**

- Modify: `src/models/mlb/lifecycle/adapters.py`
- Possibly modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Test: `tests/test_mlb_lifecycle_adapters.py`
- Add or update focused audit-suite tests if the wrapper changes.

**Steps:**

1. Keep `build_sweep_command()` as the shared quote-clean flat backtest adapter.
2. Expose dropout/timing validation independently from ranker execution.
3. Allow mean CLV to run as an optional finalist diagnostic without automatically scheduling ranker diagnostics.
4. Keep `build_ranker_command()` reachable only from `kelly_certification`.
5. Preserve dense snapshot source, quote cutoff, preferred-book routing, and empirical-CDF semantics.
6. Add tests proving flat certification builds no ranker command.
7. Run:
   - `python -m pytest tests/test_mlb_lifecycle_adapters.py -v`

### Task 4: Split flat decisions from Kelly decisions

**Objective:** Make flat profitability and operational integrity sufficient for flat approval without ranker or monotonicity evidence.

**Files:**

- Modify: `src/models/mlb/lifecycle/decision.py`
- Test: `tests/test_mlb_lifecycle_decision.py`

**Steps:**

1. Replace the current finalist behavior that forces all gates through `or finalist`.
2. Add a flat decision evaluator that uses:
   - minimum bets;
   - profit/ROI;
   - Sharpe availability;
   - drawdown cap;
   - independent-window identity;
   - dropout pass;
   - timing integrity;
   - optional mean-CLV evidence.
3. Remove ranker-file presence and edge-bucket monotonicity from flat approval.
4. Add a separate Kelly evaluator that owns ranker and monotonicity requirements.
5. Replace ambiguous postures such as `live_ready` with explicit flat-paper and Kelly classifications. Flat certification does not authorize live betting.
6. Ensure Kelly rejection cannot mutate or revoke a flat approval.
7. Add tests proving:
   - flat approval succeeds with no ranker directory;
   - failed dropout/timing blocks flat approval;
   - underpowered samples shelf rather than reject the model concept;
   - mean CLV can be nonblocking when configured as supporting evidence;
   - Kelly certification still requires positive ranker and monotonicity evidence.
8. Run:
   - `python -m pytest tests/test_mlb_lifecycle_decision.py -v`

### Task 5: Add feature-discovery comparison output

**Objective:** Produce a compact apples-to-apples model comparison without BL or advanced audits.

**Files:**

- Modify: `src/models/mlb/lifecycle/runner.py` or create a narrowly scoped comparison module under `src/models/mlb/lifecycle/`
- Test: `tests/test_mlb_lifecycle_runner.py` or a new focused comparison test

**Required output fields:**

- artifact identity;
- feature controls and resolved feature count;
- training/calibration boundaries;
- edge threshold;
- total bets;
- wins/losses/pushes;
- hit rate;
- average odds when available;
- total profit;
- ROI;
- Sharpe ratio;
- maximum drawdown;
- Over/Under splits;
- classification and reasons.

**Steps:**

1. Read persisted sweep results rather than recomputing metrics.
2. Require the baseline and candidate protocols to match before comparison.
3. Fail closed on mismatched windows, quote policy, routing, direction, stake, or threshold grid.
4. Rank only adequately powered candidates.
5. Report top-profit and risk-adjusted winners separately when they differ.
6. Do not use feature-selector output as the ablation decision.

### Task 6: Create canonical YAML examples

**Objective:** Make each lifecycle purpose obvious and copyable.

**Files:**

- Create or update examples under `configs/mlb/examples/`
- Update relevant Batter Hits configs under `configs/mlb/batter_hits/`

**Required examples:**

1. `feature_discovery` baseline raw/no-BL config.
2. `feature_discovery` force-included feature-family config.
3. `bl_policy_selection` attached-artifact broad-grid config.
4. `flat_certification` explicit independent-window config.
5. `kelly_certification` optional config clearly marked dormant/not part of the normal loop.

**Rules:**

- Feature-discovery examples use fixed hyperparameters and no tuning.
- Baseline and candidates share the exact policy contract.
- BL-selection examples attach artifacts and do not train.
- Flat-certification examples omit ranker/monotonicity requirements.
- No config authorizes deployment or live betting.

### Task 7: Update documentation and supersede old plans

**Objective:** Make the flat-first process the canonical operator workflow and preserve the decision history.

**Files:**

- Modify: `docs/development_docs/mlb_model_lifecycle_runner.md`
- Modify: `docs/development_docs/mlb_model_lifecycle_usage_guide.md`
- Reference/supersede: `.hermes/plans/2026-07-22-flat-first-bl-and-kelly-certification.md`
- Reference/supersede selected portions of: `.hermes/plans/2026-07-22-batter-hits-broad-bl-volume-sweep.md`

**Steps:**

1. Document the four purposes and exact stage graphs.
2. State that feature-family iteration uses common raw/no-BL flat policies.
3. State that broad BL sweeps occur only after model finalists are selected.
4. Explain that mean CLV is a finalist diagnostic while the CLV ranker belongs to Kelly.
5. Mark the custom neighbor-stability analyzer and cached volume-scan tasks as not required for the immediate workflow.
6. Document that long jobs are Chase-launched after dry-run review.

### Task 8: End-to-end dry-run validation

**Objective:** Prove the new stage separation from the real CLI without launching long work.

**Steps:**

1. Dry-run one example of each purpose.
2. Verify `feature_discovery` schedules training/raw sweep only.
3. Verify `bl_policy_selection` attaches artifacts, schedules broad BL, and does not train.
4. Verify `flat_certification` schedules independent replay plus dropout/timing and no ranker.
5. Verify `kelly_certification` is the only purpose that schedules ranker/Kelly work.
6. Run focused tests:
   - `python -m pytest tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py tests/test_mlb_lifecycle_runner.py tests/test_mlb_lifecycle_decision.py -v`
7. Run the broader relevant lifecycle/backtest test set after focused tests pass.
8. Do not launch training, broad sweeps, audits, or paper trading automatically.

---

## 4. Acceptance Criteria

- Feature-family candidates and the baseline are compared under an identical raw/no-BL flat-stake protocol.
- Feature discovery does not run BL, CLV ranker, monotonicity, full audit, Kelly, or paper stages.
- BL selection attaches preexisting model artifacts and does not retrain.
- Broad BL search is applied only to a small baseline/finalist set.
- Flat certification requires independent-window, volume, risk, dropout, and timing evidence but no ranker.
- Mean CLV is available as supporting finalist evidence and is not conflated with the CLV ranker.
- Kelly diagnostics are isolated behind an explicit optional purpose.
- Kelly failure cannot revoke flat approval.
- The untouched finalist window remains protected from iterative feature selection.
- Forward paper uses one frozen model-plus-policy configuration with fixed-dollar stakes.
- No stage performs deployment or live betting.

---

## 5. Decisions and Logic That Led Here

### Decision 1: The current system is sufficient for flat-stake model development

The lifecycle already has the essential components: training, artifact identity, quote-clean replay, flat-stake sweeps, risk metrics, dropout/timing evidence, and report generation. The problem was not missing modeling infrastructure; it was an overly strict decision contract that required Kelly-readiness before recognizing flat profitability.

### Decision 2: Bet selection and bet sizing are different problems

A flat policy only needs edge to act as a useful bet/no-bet threshold. Kelly requires edge magnitude to rank opportunity quality accurately enough to vary stake size. A model can be profitable with flat stakes even when edge-to-CLV Spearman or edge-bucket monotonicity is weak or underpowered.

### Decision 3: Ranker and monotonicity belong only to Kelly certification

The CLV ranker, positive ranker confidence interval, edge-bucket monotonicity, stake concentration, and capped-Kelly comparisons answer sizing questions. They must not block `FLAT_STAKING_APPROVED`.

### Decision 4: Mean CLV and the CLV ranker are not the same thing

Mean CLV asks whether the selected group beat the closing market on average. The ranker asks whether larger model edges produce progressively better market outcomes. Mean CLV remains useful supporting evidence for finalists; the ranker is unnecessary unless variable sizing is being considered.

### Decision 5: Feature-family value must be measured before BL policy optimization

Comparing a raw candidate model against a baseline's historically optimized BL policy would be unfair. It could attribute BL search luck to baseline model quality. Therefore, feature-family discovery reruns both the baseline artifact and candidates under the same raw/no-BL policy. Only the selected finalists receive broad BL sweeps.

### Decision 6: Feature selection is not an ablation

A selector can prune correlated features even when the family improves downstream betting decisions. Feature families must be tested with true force-include/force-exclude comparisons under fixed hyperparameters. Selector output is diagnostic, not the promotion decision.

### Decision 7: Independent evidence must remain independent

If many feature iterations are evaluated on the same holdout and those results drive the next experiment, the holdout has become part of discovery. Iteration happens on the discovery window; only preregistered finalists touch the independent window.

### Decision 8: Paper trading is for frozen finalists, not every feature iteration

Many feature families may be tested before a model is ready for prospective operation. Running paper for each one would slow discovery and encourage reacting to tiny forward samples. Paper begins only after the artifact and BL policy are frozen.

### Decision 9: The narrow Batter Hits BL run was underpowered, not a BL rejection

The previous sweep varied three tau states and three edge thresholds while fixing z-max and max weight. The audited edge-0.12 BL cells produced only 19 and 21 bets, versus 670 for the no-BL control. This demonstrates inadequate post-shrinkage volume, not that BL failed. A broader lower-threshold BL grid is appropriate after model finalists are selected.

### Decision 10: Preserve dormant capabilities; remove them from the default path

Ranker and Kelly code may be useful later. Deleting or archiving it now would create needless recovery work. The correct change is to isolate it behind `kelly_certification` and keep the normal model-development loop flat-first.

---

## 6. Relevant Prior Lessons and Invariants

- Use empirical probabilities: `(samples > line).mean()`; never substitute a Gaussian CDF.
- Do not deploy global conformal recalibration offsets.
- Do not blindly correct Q10 miscalibration; it is a documented source of edge.
- Feature-selector output is not a causal ablation or promotion gate.
- Validate correlated features at the family level before pruning individual representations.
- Use quote-clean, decision-time-safe historical lines for promotion-grade comparisons.
- Treat fewer than 100 bets as exploratory/underpowered rather than proof that a method failed.
- Long GameFlow training, sweep, audit, and forward-paper launches remain Chase-controlled.

---

## 7. Immediate First Slice

Before any new Batter Hits sweep:

1. Implement the purpose/stage split through tests.
2. Create a raw/no-BL `feature_discovery` baseline config for the existing baseline artifact.
3. Create matching raw/no-BL configs for the current feature-family finalists.
4. Dry-run all configs and verify protocol equality.
5. Let Chase launch the bounded comparison runs.
6. Select the model finalist pool from flat raw metrics.
7. Only then create and review the broad BL policy-selection configs.
