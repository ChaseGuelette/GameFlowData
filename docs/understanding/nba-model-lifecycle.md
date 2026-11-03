# NBA model lifecycle

## What Slice 7 adds

**Status: implemented and controlled-path verified — SPEC PASS / QUALITY APPROVED. Ready for owner commit/integration review in `repair/nba-lifecycle`; uncommitted and not merged. Historical/model acceptance remains separate.**

Slices 1–6 give us separate commands and evidence contracts. Slice 7 connects them into one inspectable workflow:

    validate → train or attach → artifact identity → sweep → audit → decision

The optional ranker is explicitly disabled in version 1. It is not a fake passing step.

The lifecycle is a conductor, not another model or backtester. It builds real commands, records their inputs, verifies their saved outputs, and decides whether a stage can be reused.

## Three distinctions that matter

### A command finishing is not evidence passing

A child process can return zero without saving its required files. A saved audit can also be serialization-complete while its conclusion is FAIL or BLOCKED. The lifecycle checks process exit, required files, hashes, source binding, and requested audit conclusions separately.

### Resume is verified reuse

A completed stage is reusable only while its configuration, upstream evidence, source identity, and verified outputs still match. Changing an artifact invalidates downstream sweep, audit, and decision state. Missing evidence does not become a skipped success.

The resolved configuration is an immutable canonical snapshot. A frozen Python object alone is not enough: nested lists can still be mutable. Library callers receive detached views, so appending a season or removing an audit check after validation cannot change later commands or the saved configuration identity.

Forcing a stage creates a new exclusive attempt. Previous child outputs remain intact; a stale decision is no longer advertised as current.

### Status is not execution

Status reads recorded lifecycle state without loading a model, inspecting an external artifact, connecting to a database, or starting a child process. It reports saved state—not a fresh certification of the underlying data.

Dry-run is different: it resolves a configuration and writes a separate planning directory, but never runs training or a sweep. Planned stages are not completed stages.

## Attach versus train

- **Attach:** supply one exact manifested artifact. Its actual saved training/calibration metadata is authoritative; unrelated YAML cannot rewrite its history.
- **Train:** supply explicit full-training settings. Each attempt gets an empty, exclusive artifact root, and exactly one valid completed child must appear. The lifecycle never chooses whichever model directory happens to be newest.

Evaluation must start at or after the artifact's exclusive calibration cutoff. This boundary check does not by itself prove the complete historical cohort is clean.

## What this does not approve

Every version-1 decision remains report-only, historically unverified, non-decision-grade, and live-blocked. Even a complete controlled workflow proves orchestration mechanics, not profitability or promotion readiness.

[Slice 8](nba-training-evaluation-slices.md) still needs approved source clocks, historical cohort/split policies, feature parity, quote-clean consumer integration, and bounded real-data acceptance. Long training and sweeps remain Chase-launched. No model deployment or customer publication is automated here.

## Delivery and usage

Configuration, command adapters, runner, CLI and report-only decisions are implemented. The actual CLI help, isolated dry-run and read-only status were exercised. A tiny producer-exported controlled sweep also passed the lifecycle's integrity audit, verified resume and fresh audit-attempt behavior. Changing a saved export failed as required; missing clocks remained BLOCKED. No database or trained model was used.

The example configuration is at `configs/nba/lifecycle/example.yaml` in this worktree. Its values demonstrate the schema; they are not approved training windows, source coverage, or permission to launch a run.

### Safe planning commands

Run from the Slice 7 worktree in PowerShell. These are planning/status commands only; do not remove `--dry-run` to start the example's training configuration.

```powershell
& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' scripts/run_nba_model_lifecycle.py --config configs/nba/lifecycle/example.yaml --dry-run
& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' scripts/run_nba_model_lifecycle.py --status --run-root backtest_results/nba_lifecycle/nba-baseline-example_dry_run
```

Status without an existing run returns `missing` and exit 2, without creating anything. Configuration/blocked outcomes use exit 2; execution or evidence failures use exit 1. A discovery workflow whose requested checks all pass can return exit 0 while still remaining non-decision-grade and live-blocked.

Only one writer can own a real run. If an interrupted process leaves `.lifecycle.lock`, the next run reports a blocked writer-lock condition; confirm the old writer has stopped before manually removing that lifecycle-owned lock. The runner does not guess that a long-running job is dead or delete its evidence.

### Where to look in a run

- `resolved_config.json` / `.yaml`: exact resolved configuration and identity.
- `commands.json`: the actual argument lists and child exits, not shell strings or credentials.
- `stage_status.json`: input/output identities and recorded stage states.
- `artifact_identity.json`: the pinned artifact's byte and metadata identity.
- `training/attempt-NNNN`, `sweep/attempt-NNNN/output`, `audit/attempt-NNNN/output`: preserved, exclusive attempts.
- `decision.json` / `.md`: the current report-only conclusion; withdrawn when upstream state is invalidated.

The decision now binds the exact configured audit list. An omitted timing check cannot be hidden behind a passing integrity check. Attached artifacts are validated and checked against their actual calibration metadata before sweep execution.

- [Slice 7 delivery record](../operations/nba-lifecycle-slice7-2026-11-02.md)
- [Saved-evidence audits](nba-evidence-audits.md)
- [Shared-input sweeps](nba-shared-input-sweep.md)
- [Eight-slice map](nba-training-evaluation-slices.md)
