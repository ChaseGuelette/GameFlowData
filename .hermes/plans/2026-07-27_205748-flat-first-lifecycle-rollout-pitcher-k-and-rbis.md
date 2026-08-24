# Flat-First Lifecycle Rollout for Pitcher K and Batter RBIs

> **For Hermes:** Implement only after the shared lifecycle changes in `.hermes/plans/2026-07-27_204057-flat-first-model-selection-lifecycle.md` are approved and complete. Use `subagent-driven-development` for implementation. Chase launches all long training, sweep, audit, and forward-paper runs.

**Goal:** Apply the flat-first model-selection contract consistently to every MLB stat currently registered with the YAML lifecycle: `batter_hits`, `batter_rbis`, and `pitcher_strikeouts`, using Batter Hits as the reference implementation and adding stat-specific controls only where evidence requires them.

**Architecture:** Keep one shared YAML lifecycle and profile registry. The four purposes—`feature_discovery`, `bl_policy_selection`, `flat_certification`, and optional `kelly_certification`—must work through profile defaults rather than stat-specific duplicate runners. Batter Hits proves the shared path; Pitcher K and Batter RBIs receive profile-specific baseline, direction, artifact, feature-family, and evidence configs.

**Supported profiles verified in `src/models/mlb/training/profiles.py`:**

- `batter_hits` (`hits`, `batter-hits` aliases);
- `batter_rbis` (`rbis`, `batter-rbis` aliases);
- `pitcher_strikeouts` (`pitcher_k`, `pitcher-k`, `pitcher_ks`, `pitcher-strikeouts` aliases).

---

## 1. Shared Contract for Every Stat

### Feature discovery

For the baseline artifact and every feature-family candidate:

1. Use fixed training hyperparameters; do not tune every variant.
2. Use true force-include/force-exclude family comparisons.
3. Verify train/serve feature coverage, temporal integrity, calibration boundaries, and artifact identity.
4. Run the same raw/no-BL, quote-clean, flat-$100 threshold grid.
5. Keep window, quote source, decision policy, routing, direction, sample count, and thresholds identical within the stat lane.
6. Compare profit, ROI, Sharpe, maximum drawdown, bet count, win rate with odds context, and side splits.
7. Do not run broad BL, CLV ranker, monotonicity, full dropout/timing, Kelly, or paper for every variant.

### BL policy selection

1. Attach the baseline and selected model-finalist artifacts; do not retrain.
2. Run the same broad flat-stake BL grid for every finalist within the stat lane.
3. Include a no-BL control.
4. Require adequate volume per cell; do not pool cells.
5. Select model-plus-policy finalists using profit, ROI, Sharpe, drawdown, volume, side/date behavior, and ordinary parameter-neighborhood review.

### Flat certification

1. Preregister finalists before touching the independent window.
2. Require adequate independent-window profit/ROI, volume, Sharpe, and drawdown.
3. Require quote-clean dropout and timing integrity.
4. Keep mean CLV optional/supporting rather than an automatic veto.
5. Do not require ranker CI or edge-bucket monotonicity.
6. Freeze artifact, policy, direction, routing, timing, thresholds, and flat stake for forward paper.

### Optional Kelly certification

Only a flat-approved policy may enter. This lane owns ranker CI, edge-bucket monotonicity, capped-Kelly comparison, stake concentration, and Kelly forward paper. Failure cannot revoke flat approval.

---

## 2. Pitcher Strikeouts Rollout

### Current profile contract

- Profile: `pitcher_strikeouts`
- Trainer kind: `pitcher_quantile`
- Default direction: `under`
- Default quote policy: `slate_or_tminus`
- Default line source: `mlb_player_props_clv_snapshots`
- Default routing: `preferred_book_first`
- Minimum decision-grade bets: `100`
- Required artifacts: `pitcher_k_model.joblib`, `pitcher_k_feature_config.joblib`

### Stat-specific rules

1. Keep Under as the primary discovery comparison unless Over independently reaches decision-grade volume.
2. Separate the frozen model artifact from the historical Slice 7 BL policy (`tau=0.5`, `z_max=0.25`, `max_weight=0.50`, edge `0.02`).
3. Rerun the frozen baseline artifact and every candidate under the same raw/no-BL flat protocol during feature discovery.
4. Preserve rejected/locked-out Phase 3A features unless a new preregistered experiment explicitly reopens them.
5. Evaluate force-include/force-exclude families including workload/leash, team hook, pitcher stuff, inning fatigue, opponent contact, environment, Phase 3B downside, and IP feature source.
6. Keep paired-bet agreement/disagreement diagnostics for close finalists because Pitcher K candidates can alter edge compression and side selection.
7. Run broad BL only after the model-finalist pool is selected.
8. Keep live, Kelly, and Kalshi blocked during flat discovery/certification; Kalshi sports remains unavailable in Michigan and is not a rollout target.

### Required configs

Create after shared lifecycle implementation:

- `configs/mlb/pitcher_strikeouts/baseline_feature_discovery.yaml`
- one `feature_discovery` config per approved family/variant;
- `configs/mlb/pitcher_strikeouts/model_finalists_bl_policy_selection.yaml`
- `configs/mlb/pitcher_strikeouts/flat_certification.yaml`
- optional `configs/mlb/pitcher_strikeouts/kelly_certification.yaml`, clearly dormant.

### Documentation updates

Keep these aligned with the shared lifecycle:

- `docs/development_docs/mlb_pitcher_k_ablation_roadmap.md`
- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
- `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md` as historical/superseded rationale
- `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md` as historical/superseded rationale

---

## 3. Batter RBIs Rollout

### Current profile contract

- Profile: `batter_rbis`
- Trainer kind: `batter`
- Model type: negative binomial
- Default direction: `both`
- Default quote policy: `slate_or_tminus`
- Default line source: `mlb_player_props_clv_snapshots`
- Default routing: `preferred_book_first`
- Minimum decision-grade bets: `100`
- Required artifacts: `batter_rbis_xgblss_booster.json`, `batter_rbis_negbin_meta.json`

### Stat-specific rules

1. Do not inherit Batter Hits thresholds, directions, or BL parameters without a stat-specific raw baseline.
2. First establish a frozen raw/no-BL RBI baseline under the profile's current training/calibration contract.
3. Verify that the shared batter feature-family registry is semantically valid for RBI targets and that every tested source varies historically.
4. Run force-include/force-exclude family comparisons under fixed hyperparameters.
5. Inspect Over and Under separately before accepting a both-direction aggregate; RBI market sparsity may make one side underpowered.
6. Select the model-finalist pool before any broad BL sweep.
7. Define RBI-specific BL edge thresholds from observed raw/BL volume rather than copying Batter Hits' grid blindly.
8. Use the same independent-window, dropout/timing, and frozen flat-paper rules as other stats.
9. Keep ranker/Kelly work dormant unless flat certification succeeds and Chase explicitly opens the sizing lane.

### Required configs

Create after shared lifecycle implementation:

- replace or migrate `configs/mlb/batter_rbis/baseline_independent.yaml` into an explicit flat-first purpose;
- `configs/mlb/batter_rbis/baseline_feature_discovery.yaml`;
- one `feature_discovery` config per approved family/variant;
- `configs/mlb/batter_rbis/model_finalists_bl_policy_selection.yaml`;
- `configs/mlb/batter_rbis/flat_certification.yaml`;
- optional dormant `configs/mlb/batter_rbis/kelly_certification.yaml`.

### Documentation requirement

Batter RBIs currently has no dedicated development-doc runbook. Create:

- `docs/development_docs/mlb_batter_rbis_ablation_roadmap.md`
- `docs/development_docs/mlb_batter_rbis_frozen_baseline.md`

Do not fabricate baseline metrics. Populate them only from verified artifact metadata and Chase-launched outputs.

---

## 4. Shared Implementation Tasks

### Task 1: Complete the shared purpose-aware lifecycle

Implement and validate the parent plan first:

- `src/models/mlb/lifecycle/config.py`
- `src/models/mlb/lifecycle/runner.py`
- `src/models/mlb/lifecycle/adapters.py`
- `src/models/mlb/lifecycle/decision.py`
- associated lifecycle tests and docs.

Done when all three registered profiles can dry-run every applicable purpose and only `kelly_certification` schedules ranker/Kelly stages.

### Task 2: Add profile-matrix contract tests

**Files:**

- modify `tests/test_mlb_lifecycle_config.py`;
- modify `tests/test_mlb_lifecycle_adapters.py`;
- modify `tests/test_mlb_lifecycle_runner.py`;
- add a profile-matrix test if clearer than expanding existing parametrization.

Test every profile for:

- artifact requirements;
- default direction;
- trainer adapter;
- default quote policy/source/routing;
- no-BL feature-discovery command;
- attached-artifact BL policy-selection command;
- flat certification without ranker;
- optional Kelly certification with ranker.

### Task 3: Migrate existing configs without pretending they already use the new contract

Review every YAML under:

- `configs/mlb/batter_hits/`
- `configs/mlb/batter_rbis/`
- `configs/mlb/pitcher_strikeouts/`
- `configs/mlb/examples/`

For each config, classify:

- migrate to a new purpose;
- retain as historical and mark superseded;
- replace with a canonical example;
- remove only if it is duplicate and has no historical value.

Do not silently reinterpret an old finalist config as feature discovery.

### Task 4: Establish per-stat raw baselines

For each profile:

1. Identify or train the exact baseline artifact.
2. Verify training/calibration metadata.
3. Dry-run the raw/no-BL quote-clean flat config.
4. Let Chase launch the bounded baseline run.
5. Record verified metrics and artifact identity in the stat's frozen-baseline page.

Do not compare cross-stat ROI as though the markets, odds, volume, and outcome distributions are equivalent.

### Task 5: Run stat-specific feature-family discovery

For each approved family:

1. Predeclare the family hypothesis.
2. Verify historical source variation and no leakage.
3. Train under fixed hyperparameters.
4. Run the same stat-specific raw/no-BL protocol.
5. Compare against that stat's raw baseline.
6. Confirm, shelf, or exclude.
7. Protect the independent window from iterative use.

### Task 6: Select policies only after models

For each stat's finalist artifact set:

1. Attach artifacts.
2. Use one reviewed broad BL grid appropriate to that stat's post-shrinkage volume.
3. Include no-BL controls.
4. Select adequately powered flat policies.
5. Do not run ranker or Kelly.

### Task 7: Certify and forward-paper frozen policies

For each stat:

1. Precommit the independent window and finalist selectors.
2. Run independent quote-clean replay.
3. Run dropout/timing certification.
4. Optionally inspect mean CLV.
5. Freeze one exact model-plus-policy candidate.
6. Let Chase launch flat forward paper.
7. Record `FLAT_STAKING_APPROVED`, `SHELF`, or `REJECTED` with evidence.

---

## 5. Validation

Focused tests:

`python -m pytest tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py tests/test_mlb_lifecycle_runner.py tests/test_mlb_lifecycle_decision.py -v`

Dry-run matrix:

- Batter Hits: every purpose;
- Batter RBIs: every purpose;
- Pitcher Strikeouts: every purpose;
- verify no DB/model work runs during dry-run;
- verify ranker is absent from all flat purposes;
- verify BL selection never trains;
- verify feature discovery uses no-BL and Kelly zero;
- verify required profile artifacts resolve correctly.

No long run is part of implementation verification.

---

## 6. Acceptance Criteria

- All three currently registered YAML profiles follow the same four-purpose lifecycle.
- Batter Hits remains the reference implementation without hardcoding Batter Hits assumptions into shared code.
- Pitcher K discovery no longer uses its historical BL policy as the feature-family control.
- Batter RBIs receives a verified raw baseline and dedicated runbook before family iteration.
- Every stat protects an untouched finalist window.
- Feature discovery is raw/no-BL, quote-clean, flat, and apples-to-apples within each stat.
- BL optimization happens only after model-finalist selection.
- Flat certification has no ranker or monotonicity veto.
- Mean CLV is optional supporting finalist evidence.
- Kelly/ranker capability remains available only through explicit opt-in.
- No process enables live betting or launches long work automatically.
