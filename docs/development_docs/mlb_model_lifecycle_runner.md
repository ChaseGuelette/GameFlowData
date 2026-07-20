# MLB model lifecycle runner

## Purpose

`scripts/run_mlb_model_lifecycle.py` replaces long, stat-specific command sequences with one profile-driven YAML lifecycle:

1. validate configuration and temporal boundaries;
2. train or attach an exact artifact;
3. verify artifact identity and expand feature-family counts;
4. run or attach an independent-window quote-clean sweep;
5. audit decision-grade bets against CLV;
6. run CLV ranking diagnostics;
7. write report-only Confirm / Shelf / Exclude and paper/live posture.

It never deploys an artifact, changes production configuration, enables Kelly, or enables live trading.

## One-command usage

Always inspect the resolved commands first:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --dry-run
```

Run or resume the lifecycle:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml
```

Resume an attached artifact/sweep at CLV audit:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --from-stage audit
```

Read durable stage state without running anything:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --status
```

Force one stage after correcting its inputs; dependent downstream stages are invalidated and rerun:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_independent.yaml --force-stage audit
```

## Supported profiles

The lifecycle engine does not branch on stat names. It resolves `MLBTrainingProfile` from `src/models/mlb/training/profiles.py`, then selects a training adapter by `train_entrypoint_kind`.

Initial profiles:

- `batter_hits` → generic batter adapter, compound binomial/AB artifacts;
- `batter_rbis` → the same batter adapter with `--stat rbis`, negative-binomial artifacts;
- `pitcher_strikeouts` → pitcher quantile adapter.

Adding a stat that uses an existing architecture requires one profile declaration and tests. A genuinely new training architecture requires one adapter registration in `TRAINING_ADAPTERS`; lifecycle stages remain unchanged.

## Sample configurations

- `configs/mlb/batter_hits/platoon_contact_independent.yaml`
- `configs/mlb/batter_hits/platoon_contact_finalist.yaml` (full-audit, report-only finalist preset)
- `configs/mlb/pitcher_strikeouts/baseline_independent.yaml`
- `configs/mlb/batter_rbis/baseline_independent.yaml`

The batter-hits example attaches the exact May 26 platoon/contact artifact and the May 18–June 21 independent-window sweep. Artifact identity expands the labels from live contracts:

- `platoon`: 3 forced features;
- `contact_quality`: 17 forced features;
- combined: 20 unique forced features.

This is a two-family, twenty-forced-feature candidate—not a “four-feature” model.

## YAML sections

- `experiment`: name, profile, purpose, optional output root.
- `model`: base, optional artifact/sweep attachments, attached-sweep artifact SHA-256, tuning, variant, feature controls.
- `training`: seasons and calibration cutoff.
- `evaluation`: independent window, direction, edge/tau/Kelly grid, flat stake.
- `quotes`: quote-clean policy, dense source, T-minus/slate policy, routing, coverage note.
- `audit`: minimum bets, bootstrap count, `clv_only` or `full` dropout audit.
- `decision`: ROI, CLV CI, ranker CI, monotonicity, drawdown, and independence gates.

Unknown profiles, unknown feature families, reversed or pre-calibration evaluation windows, incomplete artifact paths, and non-quote-clean promotion configurations fail before subprocess execution. For pitcher profiles, `base: no_prop_line` is implemented by forcing the registered pitcher prop-line feature out of training.

## Outputs and resume behavior

Default output:

```text
backtest_results/lifecycle/<experiment-name>/
```

Dry-run planning output is isolated at `backtest_results/lifecycle/<experiment-name>_dry_run/`
and does not overwrite real lifecycle evidence.

Key files:

```text
resolved_config.yaml
run_manifest.json
stage_status.json
commands.json
artifact_identity.json
audit/
ranker/
promotion_decision.json
promotion_decision.md
```

Artifact files, manifests, resolved configuration, sweep evidence, and downstream stage inputs are SHA-256 identified. Artifact metadata must provide and agree on stat/profile, model type and base, training/calibration seasons, calibration cutoff, ablation variant when requested, and the exact include-versus-exclude feature controls; extra or opposite-mode controls fail identity. An attached sweep must declare `model.sweep_artifact_identity_sha256`, and execution fails if it differs from the verified artifact identity.

A zero subprocess exit code is necessary but not sufficient. Before a stage is marked completed, the runner verifies:

- training: the finalized non-`_incomplete` artifact and every profile-required model file;
- sweep: non-empty `sweep_summary.csv`, structured `sweep_results.json`, matching `config_*` directories, each `metrics.json`, and `bets.csv` whenever `total_bets > 0`;
- audit: suite JSON/CSV/Markdown manifests plus CLV matches, timing-stability CSV, and diagnosis JSON for every requested decision-grade configuration;
- full audit: all eight persisted dropout summary, bucket, row, selection, date, game, and bookmaker outputs plus a complete full-audit attestation;
- ranker: a non-empty `ranking_score_summary.csv` with the decision columns for every ranker command.

Missing or malformed expected output records the stage as failed even when the child process returned zero. Completed stages are reused only while their input identity still matches and the full output contract still validates. Changing evidence or deleting a missing/corrupt stage output reruns that stage and invalidates dependent stages. `_incomplete` attachment directories are rejected. Resuming from a downstream stage revalidates every preceding required output and matching completed/attached state. The runner does not hash its own implementation version, so stage code changes require `--force-stage` even when YAML inputs are unchanged.

## Decision semantics

- `Confirm`: all configured evidence gates pass.
- `Shelf`: evidence is missing or underpowered, without a clear failure.
- `Exclude`: a populated evidence gate clearly fails.

Operational posture is separate:

- `hypothesis_only`
- `flat_paper_candidate`
- `live_blocked`
- `live_ready`

`live_ready` requires `purpose: finalist_certification`, `audit.mode: full`, an independent quote-clean window, a persisted passing full dropout audit, passing +15/+30/+60 timing-stability evidence for the same candidate, positive CLV and ranker confidence bounds, acceptable drawdown, and monotonic rank evidence. The positive ranker confidence bound and pass flag must occur on the same monotonic ranker row. Those finalist safety gates are mandatory even if their configurable report flags are disabled. Missing/incomplete full-audit or timing evidence shelves the candidate; explicit dropout or timing failure excludes it. Missing evidence always blocks live posture.

## Safety

The runner reuses existing training, sweep, audit, and ranker implementations. It does not change empirical-CDF probability math, calibration, quote selection, CLV math, or ranking math. A dry run builds manifests and argv arrays but performs no training, database access, backtest, audit, or ranker subprocess.

The older PowerShell wrappers remain available for compatibility, but new experiments should use this YAML lifecycle.
