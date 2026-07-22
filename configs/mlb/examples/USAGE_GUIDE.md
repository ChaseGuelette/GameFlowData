# MLB Lifecycle YAML Usage Guide

These lifecycle YAMLs are not limited to batter hits. The registered profiles are:

- `batter_hits`
- `batter_rbis`
- `pitcher_strikeouts`

The runner resolves the selected profile to its trainer, model artifacts, default direction, quote policy, and valid feature-family registry. Unknown profiles, YAML fields, variants, and feature families fail before expensive execution.

For the complete runner contract, see `docs/development_docs/mlb_model_lifecycle_usage_guide.md`. For every currently registered family and member feature, see `configs/mlb/examples/FEATURE_FAMILIES.md`.

## Included examples

### `start_from_scratch.yaml`

Use this shape when the lifecycle should train a new model and generate its own sweep. It intentionally omits:

- `model.artifact_dir`
- `model.sweep_dir`
- `model.sweep_artifact_identity_sha256`

The lifecycle performs training, artifact resolution, quote-clean sweep, audit, ranker diagnostics, Confirm/Shelf/Exclude decision, and report-only staking classification.

### `resume_existing.yaml`

Use this shape when an exact completed artifact and sweep already exist. It supplies all three attachment fields and resumes downstream evidence work without retraining or repeating the sweep.

The example uses the existing batter-hits platoon/contact artifact and independent-window sweep. When adapting it, replace `artifact_dir`, `sweep_dir`, and `sweep_artifact_identity_sha256` together. Never reuse an identity hash for a different artifact or sweep.

## Safe workflow

Run commands from the repository root in Windows PowerShell.

### 1. Copy an example

```powershell
Copy-Item configs\mlb\examples\start_from_scratch.yaml configs\mlb\batter_hits\my_ablation.yaml
```

For another profile, place the copied YAML under its matching directory, such as `configs\mlb\pitcher_strikeouts\` or `configs\mlb\batter_rbis\`.

### 2. Edit the experiment

At minimum, review:

- `experiment.name`: unique output identity; do not reuse a name for a different experiment.
- `experiment.profile`: one registered profile listed above.
- `experiment.purpose`: `discovery`, `independent_validation`, or `finalist_certification`.
- `model.base`: `with_prop_line` or `no_prop_line`.
- `model.variant`: omit/null for batter profiles; use only a registered pitcher-strikeouts variant.
- `model.feature_controls`: controlled family/feature includes or excludes.
- `training`: fixed train seasons and calibration cutoff.
- `evaluation`: window strictly after the calibration cutoff, fixed edge/tau/`z_max`/`max_weight` grid, and flat stakes.
- `quotes`: quote-clean line source, decision-time policy, routing, and a real coverage reference.
- `audit.mode`: `clv_only` for discovery/first-pass validation; `full` for finalists.
- `audit.selection`: the bounded sweep cells allowed to advance into CLV, ranker, and dropout/timing work.

Keep `model.tune: false` during family ablations. Tuning every candidate confounds feature-family value with hyperparameter search and increases runtime substantially. Tune only a surviving candidate later.

## Small BL sweep versus certification subset

The sweep grid and certification set are intentionally separate. A small discovery grid can use:

```yaml
evaluation:
  tau: [null, 0.50, 0.90]
  z_max: [0.25]
  max_weight: [0.50]
  edge_thresholds: [0.10, 0.12, 0.15]
```

This produces nine sweep cells, but discovery `risk_filtered_top_n` advances at most three: the best eligible no-BL control plus up to two eligible BL cells. The selection is deterministic, minimum-bet/drawdown/ROI filtered, and recorded in `audit_selection.json`.

Independent validation and finalist certification do not choose same-window winners automatically. They require `audit.selection.policy: explicit` with preregistered parameter cells. Only those cells flow into CLV analysis, expanded ranker diagnostics (`score-set all` with the cell's candidate-edge file), and full-mode dropout/timing analysis.

Use `audit.mode: full` when strict dropout and `+15m`/`+30m`/`+60m` timing certification is required.

## Profile-specific changes

### Batter profiles

For `batter_hits` or `batter_rbis`:

- omit `model.variant` or set it to `null`;
- choose families from the shared batter registry;
- use `direction: both` unless the experiment preregisters a narrower direction;
- use `base: no_prop_line` for cleaner feature-family attribution when appropriate.

### Pitcher strikeouts

For `pitcher_strikeouts`:

- use `direction: under` for the current baseline posture;
- set `model.variant` only when testing a registered named mechanism;
- choose families from the pitcher-strikeouts registry, not the batter registry;
- verify train/serve variation before training a new feature family.

Registered pitcher variants are:

- `none`
- `static_no_l30`
- `hook_only`
- `ip_only`
- `ip_hook`
- `hook_avg_ip_l30`
- `hook_short_hook_l30`
- `hook_deep_start_l30`

## Dry-run before real execution

Start-from-scratch example:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\examples\start_from_scratch.yaml --dry-run
```

Resume example:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\examples\resume_existing.yaml --dry-run
```

A dry-run is subprocess- and database-safe. Review the isolated `*_dry_run` directory, especially:

- `resolved_config.yaml`
- `commands.json`
- `artifact_identity.json`
- `audit_selection.json`
- `run_manifest.json`
- `stage_status.json`
- `promotion_decision.json`
- `staking_recommendation.json`

## Run or resume

A real start-from-scratch run is long and should be launched manually:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\my_ablation.yaml
```

After lifecycle implementation code changes or corrected audit inputs, force the affected stage and all downstream evidence:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\examples\resume_existing.yaml --force-stage audit
```

`--from-stage` reuses matching completed stages. `--force-stage` invalidates and reruns the named stage and its dependents. Neither option bypasses artifact or evidence validation.

## Safety and interpretation

- Probabilities remain empirical: `(samples > line).mean()`.
- Do not globally recalibrate or conformal-shift the low-tail/Q10 edge.
- Feature selector output is not an ablation; use controlled family include/exclude comparisons.
- Do not compare artifacts with overlapping calibration/evaluation windows.
- Do not mix timing policies, routing policies, artifacts, or sweep configurations.
- Treat fewer than 100 bets as exploratory.
- Use flat staking during discovery and validation.
- The lifecycle is report-only. It never deploys a model, places live bets, or executes Kelly staking.
