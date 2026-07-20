# MLB YAML model lifecycle usage guide

## What this runner does

Use `scripts/run_mlb_model_lifecycle.py` to run one MLB model experiment from a YAML preset. The runner keeps the complete experiment contract in one place and executes these stages in order:

1. validate the YAML and independent-window timing;
2. train a model or attach an exact existing artifact;
3. verify and hash the artifact identity;
4. run a quote-clean sweep or attach an existing sweep;
5. run the CLV/dropout audit;
6. run CLV ranker diagnostics for decision-grade configurations;
7. write a report-only Confirm / Shelf / Exclude decision and operational posture.

The lifecycle does not deploy models, copy artifacts into production, enable Kelly staking, change production configuration, or enable live trading.

Long training, sweep, and audit jobs should be launched manually by Chase from PowerShell. Always dry-run first.

## Existing presets

Use an existing preset as your starting point:

- `configs/mlb/batter_hits/platoon_contact_independent.yaml`
- `configs/mlb/batter_hits/platoon_contact_finalist.yaml` — full-audit finalist preset; do not run until independent evidence justifies finalist review
- `configs/mlb/pitcher_strikeouts/baseline_independent.yaml`
- `configs/mlb/batter_rbis/baseline_independent.yaml`

The batter-hits preset attaches an existing model artifact and completed independent-window sweep, then resumes at audit/ranker. The pitcher-K and RBI presets demonstrate new training lifecycles and must have their coverage notes replaced with real verified coverage evidence before expensive execution.

## Safe first use

Run the commands below from the repository root:

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData'
```

### 1. Inspect the resolved experiment

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --dry-run
```

Expected behavior:

- validates the YAML;
- resolves profile defaults and feature families;
- verifies attached artifact identity when files exist;
- constructs argv for stages that would execute; when audit output does not yet exist, the ranker plan uses a synthetic `<config>` placeholder;
- writes planning manifests under the isolated `backtest_results\lifecycle\<experiment-name>_dry_run\` root;
- does not train, query the database, backtest, audit, or run ranker diagnostics;
- reports `Shelf / live_blocked` because no evidence was executed by the dry-run.

Review these files before launching:

- `resolved_config.yaml`
- `commands.json`
- `artifact_identity.json`
- `run_manifest.json`
- `stage_status.json`

Dry-run state is isolated from the real lifecycle root. It is subprocess/DB-safe and cannot
overwrite completed real stage status or promotion evidence. Repeated dry-runs reuse only the
`*_dry_run` planning root.

### 2. Check existing lifecycle state

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --status
```

This reads `stage_status.json` without running a stage.

### 3. Run or resume the lifecycle

For a new training lifecycle:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\pitcher_strikeouts\baseline_independent.yaml
```

For the attached batter-hits artifact and sweep, resume at audit:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --from-stage audit
```

The batter-hits command must not retrain or rerun the attached sweep. It should run the missing audit, run ranker diagnostics for eligible configurations, and then write the consolidated decision.

## YAML reference

### `experiment`

```yaml
experiment:
  name: batter_hits_platoon_contact_independent
  profile: batter_hits
  purpose: independent_validation
```

Fields:

- `name`: stable experiment identifier and default output-directory name.
- `profile`: registered profile from `src/models/mlb/training/profiles.py`.
- `purpose`:
  - `discovery`: exploratory feature/strategy work;
  - `independent_validation`: fixed candidate on a post-calibration window;
  - `finalist_certification`: full audit and strict paper/live-readiness evidence.
- `output_root`: optional explicit lifecycle output directory.

### `model`

```yaml
model:
  base: no_prop_line
  artifact_dir: path/to/existing/artifact
  sweep_dir: path/to/existing/sweep
  sweep_artifact_identity_sha256: expected-artifact-hash
  tune: false
  feature_tolerance: 0.02
  variant: null
  tuning_trials: 50
  feature_controls:
    mode: include
    families: [platoon, contact_quality]
    features: []
```

Fields:

- `base`: `with_prop_line` or `no_prop_line`.
- `artifact_dir`: attach an existing artifact. Omit it to train a new artifact.
- `sweep_dir`: attach an existing sweep. Omit it to run a new sweep.
- `sweep_artifact_identity_sha256`: required with an attached sweep; this is a manually supplied provenance attestation that must match the current artifact identity. Historical sweeps do not independently embed or cryptographically prove this hash.
- `tune`: whether the underlying trainer should tune. Keep false during family-ablation discovery so the comparison remains apples-to-apples.
- `feature_tolerance`: trainer feature-selection tolerance.
- `variant`: must be null for batter profiles. For `pitcher_strikeouts`, supported values are `none`, `static_no_l30`, `hook_only`, `ip_only`, `ip_hook`, `hook_avg_ip_l30`, `hook_short_hook_l30`, and `hook_deep_start_l30`.
- `tuning_trials`: trainer tuning-trial count when tuning is enabled.
- `feature_controls.mode`: `include` or `exclude`.
- `feature_controls.families`: registered profile feature families.
- `feature_controls.features`: explicit feature names.

Family names are expanded in `resolved_config.yaml`. For example, batter-hits `platoon + contact_quality` is two families and twenty unique forced features, not a four-feature model.

### `training`

```yaml
training:
  seasons: [2024, 2025]
  calibration_season: 2026
  calibration_end: 2026-04-12
```

The evaluation start must be strictly after `calibration_end`.

### `evaluation`

```yaml
evaluation:
  start: 2026-05-18
  end: 2026-06-21
  direction: both
  edge_thresholds: [0.10, 0.12, 0.15]
  flat_bet: 100
  tau: [null]
  kelly_values: [0.125]
```

Use flat staking for discovery and independent validation until ranker evidence supports magnitude-based sizing. The Kelly values remain part of the sweep configuration identity even when `flat_bet` overrides stake sizing.

### `quotes`

```yaml
quotes:
  clean: true
  line_source: mlb_player_props_clv_snapshots
  decision_policy: slate_or_tminus
  relative_minutes: 60
  routing: preferred_book_first
  coverage_audit_note: Verified coverage note or artifact reference
```

Independent validation and finalist certification require `clean: true`.

`coverage_audit_note` is free-text operator attestation, not verified evidence. The runner only
checks that it is nonempty. Replace placeholder notes with a real reviewed result/reference before
execution; the note itself does not prove dense quote coverage.

Do not pool evidence from different timing policies. A T-30-only window is not interchangeable with a certified T-60/slate window.

### `audit`

```yaml
audit:
  minimum_bets: 100
  bootstrap_samples: 1000
  mode: clv_only
```

Modes:

- `clv_only`: skips the expensive dropout audit and is appropriate for discovery or preliminary independent-window analysis.
- `full`: requires persisted CLV, dropout/coverage, and timing-stability evidence. Use it for finalist certification.

A finalist configuration must use `mode: full`. YAML intent alone is not sufficient: the lifecycle verifies the audit outputs before allowing a finalist decision.

### `decision`

```yaml
decision:
  max_drawdown: 0.25
  require_positive_roi: true
  require_positive_mean_clv_ci_low: true
  require_positive_ranker_ci_low: true
  require_edge_bucket_monotonicity: true
  require_independent_window: true
```

Missing evidence produces Shelf/live-blocked. Populated failing evidence produces Exclude/live-blocked. Confirm requires one exact configuration to pass all configured gates; metrics from different sweep configurations are never combined.

## Output layout

Default root:

```text
backtest_results/lifecycle/<experiment-name>/
```

Expected lifecycle files:

```text
resolved_config.yaml
run_manifest.json
stage_status.json
commands.json
artifact_identity.json
promotion_decision.json
promotion_decision.md
artifacts/                 # when training a new model
sweep/                     # when running a new sweep
audit/
ranker/
```

Subprocess outputs are verified before their stage is marked completed.

### Training stage

The exact artifact filenames come from the selected training profile. The runner also requires identity metadata such as `model_manifest.json`, `run_config.json`, or `training_metadata.json` sufficient to prove profile, model type/base, dates, and requested feature controls.

Generated `_incomplete` artifacts are never accepted as completed models.

### Sweep stage

Required outputs:

- `sweep_summary.csv`
- `sweep_results.json`
- decision-grade `config_*` evidence such as `metrics.json` and `bets.csv`

When independent-window evidence is required, the decision stage checks `sweep_results.json` for the actual evaluation window, quote-clean status, line source, timing policy, and relative minutes. Sweep-stage completion itself validates structure/configuration correspondence, not promotion eligibility.

### Audit stage

Required common output:

- `audit/suite_manifest.csv`
- `audit/suite_manifest.json`
- `audit/suite_summary.md`

Required CLV output for each audited decision-grade configuration:

- `audit/clv/<config-label>/clv_matches.csv`
- `audit/clv/<config-label>/clv_timing_stability.csv`
- `audit/diagnosis/<config-label>/clv_failure_modes.json`

Full audit additionally requires the complete persisted dropout bundle: `audit_summary.json`, `audit_summary.md`, `dropout_summary_by_bucket.csv`, `dropout_rows.csv`, `selected_clean_quotes.csv`, `dropout_by_date.csv`, `dropout_by_game.csv`, and `dropout_by_bookmaker.csv`. It also requires explicit passing `+15m`, `+30m`, and `+60m` timing evidence. The CSV/JSON candidate labels and the manifest/diagnosis/dropout attestations must agree. A zero exit code without these outputs does not complete the stage.

`promotion_decision.json` records SHA-256 evidence for the finalist dropout summary and each candidate's timing CSV and diagnosis JSON. `stage_status.json` records the complete file set used to compute the decision identity, so the report remains tied to the exact persisted evidence it evaluated.

### Ranker stage

Required for each ranker command:

- `ranker/<config-label>/ranking_score_summary.csv`

The decision gate requires the positive confidence bound, pass status, and required monotonicity evidence to occur on a qualifying row for the same exact configuration.

## Resume behavior

A completed stage is reused only when:

- its resolved configuration/input identity is unchanged;
- its stage input identity is unchanged;
- its required outputs still exist and pass current structural/content validation;
- downstream ranker and decision identities still match the hashed audit evidence they consume;
- the artifact is finalized and is not `_incomplete`.

The runner does not persist an implementation-version or command-code fingerprint. After changing
stage implementation code, use `--force-stage <stage>` even when the YAML is unchanged.

If an upstream stage is forced or its inputs change, dependent downstream state is invalidated.

### Resume from a stage

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --from-stage audit
```

Stages before the requested start must already have matching completed or attached state. Their
required outputs are revalidated before any downstream stage or decision is allowed to proceed;
`--from-stage` is not an evidence-validation bypass.

### Force a corrected stage

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --force-stage audit
```

This reruns audit and invalidates/reruns dependent ranker and decision evidence.

Use `--force-stage` when the stage implementation changed, a prior output was corrupt, or a corrected input should replace old evidence. Do not use it merely because a long job is still running.

## Reading the decision

Open:

- `promotion_decision.md` for the human-readable result;
- `promotion_decision.json` for exact evidence and gate details.

Classifications:

- `Confirm`: one exact candidate passed every required evidence gate.
- `Shelf`: required evidence is missing, mismatched, underpowered, or inconclusive.
- `Exclude`: a populated gate clearly failed, such as ROI, drawdown, CLV, audit, finalist timing/coverage, ranker CI, or monotonicity.

Artifact-identity failure aborts the lifecycle and produces no promotion decision. Missing or
mismatched independent-window metadata normally produces `Shelf`, not `Exclude`.

Operational posture is separate from classification:

- `hypothesis_only`
- `flat_paper_candidate`
- `live_blocked`
- `live_ready`

`live_ready` is fail-closed and requires finalist certification, a full persisted audit, an independent quote-clean window, passing dropout/coverage and timing-stability evidence, positive CLV and ranker confidence bounds, acceptable drawdown, and required monotonicity evidence.

## Creating a new preset

1. Copy the closest preset under `configs/mlb/<profile>/`.
2. Give the experiment a unique, stable name.
3. Select a registered profile.
4. Set the training and calibration window.
5. Set an evaluation window strictly after calibration.
6. Choose one explicit quote timing/routing policy.
7. Use feature-family include/exclude controls rather than treating feature selection as an ablation.
8. Keep tuning disabled during family discovery.
9. Add an existing artifact/sweep only when exact identity provenance is available.
10. Dry-run and inspect `resolved_config.yaml`, `commands.json`, and `artifact_identity.json` before launching.

Unknown YAML fields, profiles, or feature families fail loudly.

## Recommended operating sequence

For feature-family discovery:

1. `purpose: discovery`
2. `audit.mode: clv_only`
3. fixed training hyperparameters
4. flat stakes
5. classify Confirm / Shelf / Exclude for the experiment question only

For independent validation:

1. preselect the candidate before opening the new window
2. `purpose: independent_validation`
3. quote-clean fixed timing/routing
4. flat stakes
5. require positive CLV/ranker evidence before paper candidacy

For finalist certification:

1. `purpose: finalist_certification`
2. `audit.mode: full`
3. independent quote-clean window
4. persisted dropout/coverage and timing-stability PASS evidence
5. positive CLV and ranker confidence bounds
6. acceptable drawdown and monotonicity
7. manually review the report before any separate production change

## Safety rules

- Probabilities remain empirical: `(samples > line).mean()`.
- Never globally recalibrate or conformal-shift the Q10/low-tail edge.
- Feature selection is not an ablation.
- Do not use ROI alone to enable Kelly or live betting.
- Do not mix evidence from different artifacts, windows, timing policies, routing policies, or sweep configurations.
- The lifecycle is report-only. Production promotion remains a separate, explicit, human-approved operation.
