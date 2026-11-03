# NBA Coding Slice 7 — Lifecycle Runner Implementation Plan

> **For Hermes:** Use the GameFlow implementation-worker lane and subagent-driven-development review gates after Chase approves this scope. Slice 6 must be integrated and its final CLI/output contract verified before launching a writer.

**Goal:** Turn the completed NBA training, artifact, shared-input sweep, and evidence-audit commands into one strict, inspectable, resumable, report-only lifecycle without launching training automatically or upgrading controlled evidence into historical/model acceptance.

**Architecture:** Add an NBA-specific lifecycle package modeled on the durable orchestration patterns in `src/models/mlb/lifecycle`, but adapt only to the actual NBA Slice 1–6 contracts. Strict YAML resolves into pure command plans; a runner persists stage state and exact evidence identities; every executable stage requires both a zero exit code and semantic output verification. Status inspection is read-only, dry-run is isolated, downstream state is invalidated when upstream identity changes, and unsupported stages are explicit rather than success-returning stubs.

**Tech stack:** Existing Python, Pydantic, PyYAML, argparse, pathlib, hashlib/JSON helpers, subprocess, pytest, and Ruff. No SQL, new infrastructure, model changes, feature changes, or production deployment.

---

## Status and dependency gate

### Owner authorization and live preflight — November 2, 2026

### Implementation launch — supersedes earlier dependency block

**Delivery checkpoint:** Slice 7 implementation in `repair/nba-lifecycle` is controlled-path verified, SPEC PASS / QUALITY APPROVED, ready for owner commit/integration review. Source is uncommitted/unmerged. See the worktree's `docs/operations/nba-lifecycle-slice7-2026-11-02.md` and `docs/understanding/nba-model-lifecycle.md` for exact scope, actual checks, immutable-snapshot/required-check repairs and evidence limits. No unit tests added; existing 64 tests and short incremental probes passed. No real training, DB, historical acceptance, commit, push or deployment occurred.

Chase confirmed Slice 6 completion and explicitly requested implementing Slice 7 through merge readiness, no unit tests, minimal scratch validation, concurrent wikis, bounded agents and section-by-section validation. Main is now `bf7a3e970906ed2161d744ff9cd0fcf39e7fc086`, integrating accepted Slice 6 `aaf3918`. Its evidence/initializer bytes match the accepted source commit. The accepted auditor uses the simpler ordinary path/fingerprint checks; native locking requirements remain superseded.

Implementation worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-lifecycle`, branch `repair/nba-lifecycle`, created at that exact integrated base. The coordinator owns wiki/evidence/status docs; the code worker owns only the new source/config allowlist. No new tests, fixture edits or broad scratch suites. Actual baseline is 64 existing tests passing in 2.41 seconds; fingerprints cover 400 tracked source/config/script/test files. No upstream pending source is inherited.

Dependency-freeze strategy: use accepted public `inspect_sweep` for sweep integrity and `audit_sweep` for audit semantic recomputation, plus a thin lifecycle-owned persisted-export envelope verifier. No private auditor helpers or duplicate audit math. Accepted auditor source hash is `99ef8ddebe3ef52d91b064771df465db2b37f92d73b57c40ce5f86bc4aa1dfea`.

Spark canary failed once with deterministic unsupported-model HTTP 400. Explicit `gpt-6.1-sol-900k` / `openai-codex` availability canary passed; code worker uses per-launch overrides and bounded runtime. No profile configuration changes. Commit, merge, push, real training/DB/model/sweep jobs and deployment remain unauthorized.

The older preflight paragraphs immediately below are historical; they no longer describe the integrated dependency state.

Chase requested “Please begin work on slice 7”, authorizing this lane's implementation and controlled offline verification once its named dependency gate is closed. Commit, merge, push, real data/model runs and upstream integration remain separately authorized.

Live inspection still finds main at `3aa73a5`, without `src/backtesting/nba/evidence.py`. Slice 6 is uncommitted in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-evidence-audits`; its delivery record says specification review is in progress and records an unresolved measured-timing-FAIL versus unmeasurable-BLOCKED precedence defect. Its separately approved lazy `src/backtesting/__init__.py` change is also part of that pending dependency. No Slice 7 implementation writer has launched, and no upstream source is copied or integrated by this authorization.

The immediate safe work is independent plan/interface preflight. Launch remains BLOCKED until Slice 6 is accepted and integrated, or Chase explicitly authorizes development against a fingerprinted, read-only pending dependency snapshot. Never silently substitute the current mutable Slice 6 worktree for an integrated base.

Validation-scope correction: no permanent tests are authorized. Use scoped existing suites and disposable real-path probes; the original four proposed test files have been removed from the allowlist.

### Normative pre-implementation corrections

Independent plan review returned BLOCKED pending the dependency and contract corrections below. These rules supersede conflicting provisional wording later in this document. They are implementation requirements, not a claim that Slice 6 or Slice 7 is accepted.

1. **Strict input:** Use a `yaml.SafeLoader` subclass with a mapping constructor rejecting duplicate keys at every depth; plain `safe_load` is insufficient. Enforce strict numeric/boolean/string/list domains before identity calculation. Reject nonfinite values, bool-as-int, string-to-number coercion, duplicate list members, YAML sets, merge-key/alias ambiguity, and normalized/repaired identifiers. Resolve relative config paths against the repository root consistently; retain original config tokens separately from resolved paths. Parse dates explicitly as exact ISO dates or PyYAML date values, never datetime-to-date truncation.
2. **Attach versus train:** `training` is required in train mode and forbidden in attach mode. Attach reads actual metadata from the validated manifest/run config and records that metadata; unrelated YAML cannot substitute for it. In train mode bind all requested training settings and hyperparameter bytes to the produced artifact's actual metadata. `tune=true` with `hyperparams_path` is invalid. Missing/conflicting calibration or training provenance blocks the appropriate execution stage rather than inventing a cutoff. Compare evaluation start to the actual exclusive calibration end (`start >= end`) and require approved non-overlapping train/calibration/evaluation season/date evidence before a non-discovery historical claim. All v1 results still remain historically unverified.
3. **Exclusive training output:** Use a lifecycle-owned, exclusive, initially empty training-output root per attempt, never a shared artifact root. After the child returns, require exactly one child entry total: a final, nonlegacy, manifested training bundle with matching requested metadata. Zero/multiple/invalid entries or any incomplete residue fail verification. No mtime discovery.
4. **Sweep verification:** Use accepted public `inspect_sweep(path)` for integrity-only verification, then compare actual saved request/config/artifact/cell identities with this lifecycle's command plan. Require complete supported evidence and integrity PASS. Timing belongs to audit and must not be required for sweep completion. Do not resolve an archived producer plan via live path validators as a substitute for integrity verification.
5. **Audit export boundary:** Current pending Slice 6 exposes `inspect_sweep` and `audit_sweep`, but no public persisted-export verifier. No Slice 6 change is implicitly authorized. At dependency freeze, either use an accepted public verifier if one exists, or define the lifecycle-owned export envelope verifier in `runner.py` against the accepted versioned format. The latter checks exclusive contained regular-file layout, duplicate-free strict JSON, every output hash/size and schema, report/manifest agreement, exact requested checks, source sweep receipts, and auditor source hash; it uses public `audit_sweep` to recompute conclusions/tables from the exact sweep and compare exported semantic content. This must not import private export helpers or reimplement concentration, matching, coverage, timing, or exit precedence. If the final format/public return values cannot support this, stop for a narrow contract decision rather than fabricate acceptance.
6. **Audit state dimensions:** Persist `export_verified`, `report_state`, `audit_conclusion`, exact `requested_check_statuses`, child `exit_code`, and authoritative report exit code separately. A serialization-complete FAIL/BLOCKED export is evidence, not stage success. Every requested check must PASS, mandatory integrity must PASS, and process/report semantics must agree for audit completion. Preserve FAIL over BLOCKED/UNSUPPORTED; a nonzero child or contradiction can never pass due to output existence.
7. **Decision truth table:** Semantic FAIL, tampering, or execution failure yields `blocked_invalid_evidence`. Otherwise missing/stale/BLOCKED/UNSUPPORTED/unrequested required evidence yields `blocked_incomplete_lifecycle`. If every required controlled stage passes, discovery yields `controlled_mechanics_complete`; independent/finalist purpose yields `blocked_historical_integration_pending`. Optional disabled ranker is not a required stage and stays `not_requested`. Neither decision boolean can disable integrity or requested-audit enforcement in any purpose. Decision serialization completion is independent of its historical classification.
8. **Attempt and stale-decision safety:** Persist monotonically numbered, exclusive child attempts under `training/attempt-NNNN`, `sweep/attempt-NNNN`, and `audit/attempt-NNNN`, with exact paths in stage receipts. Never overwrite or delete child evidence. `--force-stage` allocates new attempts as needed. Before running an invalidated stage, atomically persist invalidation and mark the current decision unavailable; remove only lifecycle-owned `decision.json`/`decision.md` from the active run, not prior child evidence. Use an exclusive single-writer run lock and atomic replace for state writes. A crash/invalidation leaves no decision advertised current. Reuse verifies both `input_identity` and `output_identity` and rechecks actual outputs. Status displays recorded state as unverified and never claims live evidence validation; an invalidated/identity-inconsistent decision is not exposed as current.
9. **Status CLI:** `--status --run-root PATH` targets lifecycle-owned state directly and is mutually exclusive with `--config`, `--dry-run`, `--from-stage`, and `--force-stage`. Other modes require `--config`. Status performs no external artifact/path/source verification, no writer-lock creation, no output writes, and no downstream module imports. Missing/corrupt state is explicit missing/failed, not a new healthy run.
10. **Import and parser safety:** Lifecycle imports must not eagerly import training, sweep runtime, replay, or audit modules. Source fingerprints hash an explicit file inventory without importing it. Lazy imports for public integrity/audit calls are allowed only in execution/evidence verification paths after the accepted lazy package initializer is present. Parser-compatibility checks run in disposable guarded processes; do not rely on `PYTHON_DOTENV_DISABLED` alone because installed python-dotenv may ignore it. Disable mounted environment loading and block engine/network/deserialization/fitting in probes. These guards are verification containment, not production patches.
11. **Validation scope:** New permanent tests and existing fixture/assertion edits are prohibited. Use existing suites plus disposable probes that run actual library/CLI paths. A controlled positive fixture must be produced by the actual pinned upstream exporter with in-memory collaborators; do not manufacture success-shaped upstream receipts. Record source/test hashes and establish this lane's own baseline before writing source.

Required dependency-freeze receipt: exact accepted Slice 6 module and lazy initializer hashes; integration base; real parser arguments; `inspect_sweep`/`audit_sweep` return schema; report/export schema and status/exit matrix; named output/source commitments; selected export-verifier strategy. Re-read and freeze these after Slice 6 acceptance. Pending observed source is reference only and may change.

- Scope prepared November 2, 2026 while Slice 6 is completing in a separate worktree.
- Coding slices 1–5 are integrated on main at `3aa73a5`.
- Slice 6 currently owns `src/backtesting/nba/evidence.py` and its output schema. Slice 7 must not create, edit, or guess that module while the Slice 6 writer is active.
- Before implementation, rebase/create the Slice 7 worktree from the owner-approved main commit containing accepted Slice 6 source and documentation.
- Re-read the final Slice 6 parser, status/exit matrix, and audit output manifest. If they differ from the provisional contract below, update this plan before launching the writer.
- This plan authorizes no training, sweep, historical replay, DB access, sync, backfill, model loading, deployment, merge, or push.

## Writable allowlist after approval

Source:

- Create `src/models/nba/__init__.py`.
- Create `src/models/nba/lifecycle/__init__.py`.
- Create `src/models/nba/lifecycle/config.py`.
- Create `src/models/nba/lifecycle/adapters.py`.
- Create `src/models/nba/lifecycle/runner.py`.
- Create `src/models/nba/lifecycle/decision.py`.
- Create `scripts/run_nba_model_lifecycle.py`.
- Create `configs/nba/lifecycle/example.yaml`.

Validation: existing tests remain read-only; disposable probe files and receipts belong under Hermes scratch. No new permanent tests or smoke scripts.

Coordinator-owned documentation:

- Create `docs/understanding/nba-model-lifecycle.md`.
- Create `docs/operations/nba-lifecycle-slice7-2026-11-02.md` for actual implementation/review evidence.
- Make narrow status/link additions only to:
  - `docs/operations/nba-coding-readiness-plan-2026-11-01.md`
  - `docs/understanding/nba-training-evaluation-slices.md`

Read/reuse, do not modify unless a separately reviewed contract defect blocks Slice 7:

- `src/models/train_pipeline.py`
- `src/models/training_preflight.py`
- `src/models/nba_artifacts.py`
- `src/backtesting/nba/sweep_config.py`
- `src/backtesting/nba/sweep_runner.py`
- `src/backtesting/nba/run_nba_sweep.py`
- accepted Slice 6 `src/backtesting/nba/evidence.py`
- `src/backtesting/run_backtest.py`
- `src/models/mlb/lifecycle/*` as architecture reference only

Protected:

- All sync/reconciliation/linker files.
- Historical S1 worktree/source.
- NBA model math, calibration, feature store, loaders, harness, simulator, quote selectors, observation policies, SQL, and production artifacts.
- All MLB source and configs.
- All existing tests and fixtures; no new permanent tests without explicit owner approval.

## Non-goals

- No historical source-policy activation or Slice 8 consumer wiring.
- No training, tuning, sweep, or real-data command launched by the implementation agent.
- No automatic model promotion, artifact copying, deployment, publication, or live configuration write.
- No global conformal-offset loading or calibration changes.
- No Gaussian probability fallback or probability recomputation.
- No new ranker implementation.
- No staking recommendation stage.
- No generic framework shared with MLB in this slice.
- No “latest artifact” discovery by modification time.
- No resume inside an incomplete sweep; failed/incomplete child runs remain evidence, not checkpoints.

## Canonical stage flow

Schema version 1 uses this exact order:

1. `validate`
2. `train_or_attach`
3. `artifact_identity`
4. `sweep`
5. `audit`
6. `ranker`
7. `decision`

The runner must not silently skip a stage. Each stage ends in one of:

- `completed`
- `failed`
- `blocked`
- `unsupported`
- `not_requested`
- `planned` only in dry-run output

`not_requested` and `unsupported` are never treated as completed. Any required failed/blocked/unsupported stage prevents downstream execution except the report-only `decision` stage, which may serialize a blocked conclusion from already verified evidence.

## Strict YAML v1 contract

Unknown keys are rejected with Pydantic `extra="forbid"`. All lists that represent identities are ordered and duplicate-free.

```yaml
schema_version: 1

experiment:
  name: nba-baseline-example
  purpose: discovery
  output_root: backtest_results/nba_lifecycle

artifact:
  mode: train                  # attach requires exact path and forbids training

training:
  train_seasons: ["22023", "22024"]
  calibration_season: "22025"
  calibration_end: 2026-01-01
  tune: false
  tuning_trials: 50
  tuning_timeout: null
  hyperparams_path: null
  feature_tolerance: 0.005
  local: true

evaluation:
  start: 2026-01-01
  end: 2026-01-07
  stats: [pts, reb, ast]
  bookmakers: [draftkings, fanduel]
  n_samples: 5000
  seed: 42
  min_minutes_avg: 8.0
  starting_bankroll: 10000.0
  flat_bet_size: 100.0
  edge_thresholds: [0.05, 0.08, 0.10]
  allowed_bets: null
  bl_taus: []
  bl_z_max: [1.0]
  bl_max_weight: [0.5]
  max_cells: 64
  local: true
  diagnostic_history: true

audit:
  checks: [integrity, concentration, coverage, timing]

ranker:
  mode: disabled

decision:
  require_integrity_pass: true
  require_requested_audits_pass: true
```

### Configuration rules

- `schema_version` must equal `1`.
- `purpose` is initially `discovery | independent_validation | finalist_certification` for durable vocabulary, but v1 cannot produce a promotion-ready result for any purpose before Slice 8.
- `artifact.mode=attach` requires an exact manifested `artifact.path`; legacy artifacts are rejected.
- `artifact.mode=train` forbids `artifact.path` and plans a full training command only. Partial retraining and calibration-only modes are out of scope.
- Training seasons use the existing exact NBA season tokens; do not normalize them to calendar years.
- Evaluation end must not precede start.
- Stats are ordered unique values from `pts`, `reb`, `ast`; combo stats remain derived by existing sweep code.
- `diagnostic_history` must be true for executable sweeps.
- Numeric limits must reuse or match the existing training/sweep validators rather than invent looser bounds.
- `ranker.mode` accepts only `disabled` in schema v1. Any other value fails validation as unsupported. The stage is recorded `not_requested`, never `completed`.
- Decision gates cannot be disabled for independent/finalist purposes.
- The resolved run root is `<output_root>/<slugified experiment.name>` and must not collide with artifacts, sweep outputs, audit inputs, or an existing different resolved-config identity.

## Files and durable state

Normal run root:

```text
<run_root>/
  resolved_config.yaml
  run_manifest.json
  commands.json
  stage_status.json
  artifact_identity.json
  decision.json
  decision.md
  sweep/                  # child output path
  audit/                  # child output path
```

Dry-run root:

```text
<run_root>_dry_run/
```

Dry-run may write only planning artifacts under the dry-run root. It must not create the normal run root, artifact directories, sweep directories, or audit directories and must never call the subprocess adapter.

`--status` reads an existing `stage_status.json` and related static manifests without creating directories, rewriting timestamps, validating external artifacts, or running child verifiers.

Every command is persisted to `commands.json` before execution. Secrets and environment values are never persisted.

## Adapter contract

`adapters.py` owns pure argv construction; `runner.py` owns orchestration and output verification.

Public functions:

- `repo_root() -> Path`
- `build_training_command(resolved, *, artifact_root) -> list[str]`
- `build_sweep_command(resolved, *, artifact_dir, output_dir) -> list[str]`
- `build_audit_command(resolved, *, sweep_dir, output_dir) -> list[str]`

### Training command

Build against the actual parser in `src/models/train_pipeline.py`:

```text
python -m src.models.train_pipeline
  --train-seasons ...
  --cal-season ...
  --cal-end-date ...
  --artifacts-dir ...
  --feature-tolerance ...
  [--tune --tuning-trials ... --tuning-timeout ...]
  [--hyperparams-path ...]
  [--local]
```

Do not include `--calibrate-only`, partial-retrain, legacy, forced-feature, or diagnostics flags.

Use the exclusive per-attempt training-output root specified in the normative corrections. After a zero exit code require exactly one final child total passing `validate_artifact_bundle(..., requested_stats=resolved.stats, allow_legacy=False)`, with actual metadata matching the requested training configuration and no incomplete residue. Never scan a shared artifact root or choose by mtime.

### Sweep command

Build against `python -m src.backtesting.nba.run_nba_sweep` using only actual flags:

- exact manifested `--model-dir`
- evaluation dates/stats/books
- sample/RNG/minutes/bankroll/flat sizing
- edge/allowed-side/BL grid
- exact new `--output-dir`
- `--diagnostic-history`
- optional `--local`

The lifecycle resolves and pins the artifact once. No downstream command may rediscover an artifact.

### Audit command

After Slice 6 integration, build against its actual parser. Provisional accepted shape:

```text
python -m src.backtesting.nba.evidence --sweep-dir <exact sweep> --checks integrity concentration coverage timing --output-dir <exact audit>
```

Generated commands must parse through the real downstream parsers in guarded disposable probes. Do not shell-quote a string command; persist and execute argv lists.

## Stage behavior

### `validate`

- Load YAML through the duplicate-rejecting SafeLoader contract in the normative corrections.
- Reject empty/non-mapping YAML, duplicate/unknown fields, invalid paths, nonfinite numbers, invalid dates, unsupported modes, and output collisions.
- Resolve the config and write static manifests only in run/dry-run execution mode, not status mode.
- Record `config_identity = canonical_sha256(resolved_config)`.

### `train_or_attach`

Attach:

- Validate the exact bundle with `validate_artifact_bundle(..., allow_legacy=False)`.
- Record attached path and inspection identity.
- Do not deserialize models.

Train:

- Persist the exact command plan.
- In dry-run, record `planned` only.
- In real execution, require explicit user launch outside the implementation acceptance run.
- On execution, use the exact-one-new-valid-artifact rule above.
- Failure, `_incomplete`, legacy, empty, ambiguous, or invalid bundles block downstream stages.

### `artifact_identity`

- Re-run `validate_artifact_bundle` on the pinned exact path.
- Persist `ArtifactInspection.to_plan_dict()` plus manifest/file hashes to `artifact_identity.json`.
- Require `identity_verified=true`, `legacy=false`, `decision_grade=false`, and no global combined-offset consumption.
- The stage proves artifact identity/completion only. It must retain `model_load_verified=false` unless an actual downstream child receipt explicitly proves loading; even then, do not rewrite artifact preflight claims.

### `sweep`

- Input identity includes resolved config, exact artifact identity, and hashes of Slice 5 source/dependency evidence required by `build_plan`.
- Execute or dry-run the actual sweep command.
- Zero exit is insufficient: verify a complete final sweep directory through the Slice 5 manifest/receipt contract.
- Require exact artifact identity/config linkage, complete cell set, final state, no `_incomplete` sibling masquerading as success, `historical_readiness=unverified`, and `decision_grade=false`.
- A changed source/dependency hash invalidates reuse.

### `audit`

- Input identity includes config, artifact identity, the complete sweep manifest/receipts, and requested checks.
- Execute the accepted Slice 6 CLI.
- Verify the final audit manifest and every required requested output through the accepted Slice 6 reader/contract.
- Preserve Slice 6 exit semantics: FAIL outranks BLOCKED/UNSUPPORTED; requested blocked/unsupported is not success.
- A report may be serialization-complete while its audit conclusion is failed/blocked. Record those separately.
- Never reinterpret missing timing, CLV, dropout, or source observations as zero violations.

### `ranker`

- Schema v1 supports only `mode=disabled`.
- Record `not_requested` with reason `nba_ranker_not_scoped_in_slice7`.
- Do not create output files or a fake passing receipt.
- Future ranker support requires a new schema version or reviewed extension with a real adapter and output verifier.

### `decision`

- Pure report-only function in `decision.py`.
- Consume the persisted resolved config, artifact identity, sweep manifest, audit summary/manifest, and stage statuses.
- Write `decision.json` and `decision.md`.
- Initial classifications:
  - `blocked_invalid_evidence`
  - `blocked_incomplete_lifecycle`
  - `blocked_historical_integration_pending`
  - `controlled_mechanics_complete`
- Even `controlled_mechanics_complete` must retain:
  - `historical_readiness: unverified`
  - `decision_grade: false`
  - `live_posture: blocked`
  - `promotion_authorized: false`
  - `training_launch_authorized: false`
- No config value may turn these fields true in Slice 7.
- Profit/ROI/calibration values cannot override a failed/blocked artifact, sweep, or audit stage.

## Resume and invalidation semantics

A stage can be reused only when:

1. Recorded status is `completed`.
2. Recorded input identity equals the freshly computed identity.
3. Every required output exists.
4. The stage-specific semantic verifier passes now.

`--from-stage` means verified resume, not unconditional skipping. Every earlier stage must satisfy the reuse contract or the command fails closed.

`--force-stage <stage>` removes that stage and all downstream statuses before execution. It never deletes child evidence automatically and never overwrites existing output paths; the user must choose a new run/output or explicitly archive prior evidence outside this slice.

Before executing any stage, remove downstream completion statuses from in-memory state and persist the invalidation. Do not retain a previous decision after artifact, sweep, or audit identity changes.

Attached evidence is never trusted by existence alone. Attached artifacts and any future attached sweep support must pass the same verifiers as newly produced evidence. V1 does not need attached-sweep YAML support; add it only through explicit scope expansion.

## CLI contract

Entrypoint:

```text
python scripts/run_nba_model_lifecycle.py --config PATH [--dry-run] [--from-stage STAGE] [--force-stage STAGE]
python scripts/run_nba_model_lifecycle.py --status --run-root PATH
```

Rules:

- `--status` is mutually exclusive with `--dry-run`, `--from-stage`, and `--force-stage`.
- `--dry-run` never calls child commands or creates normal-run state.
- `--from-stage` and `--force-stage` use choices from the exact stage tuple.
- Print one sorted JSON result to stdout.
- Exit `0` only for successful status inspection, valid dry-run planning, or a completed controlled lifecycle whose required stages pass.
- Exit `1` for execution/output verification failure.
- Exit `2` for invalid configuration or a requested required stage that is blocked/unsupported without an overriding failure.
- Sanitize exceptions; do not print DB URLs, arbitrary child stderr, or source metadata that may contain credentials.

## Implementation tasks

### Task 1: Freeze config and resolved identity

Create the strict Pydantic schema, cross-field validators, path resolution, run-root rules, canonical resolved dictionary, and example YAML. Add tests for unknown keys, invalid modes, duplicate stats/books, temporal bounds, ranker rejection, attach/train exclusivity, output nesting/collisions, and immutable safety fields.

### Task 2: Build parser-compatible command adapters

Implement pure argv builders. Test every generated argv against the actual training, sweep, and accepted evidence parsers. Confirm no adapter imports or initializes DB/model runtime and no command includes legacy, calibration-only, partial-retrain, Kelly, deployment, or unsupported ranker flags.

### Task 3: Implement durable runner/state machine

Add static manifests, command persistence, atomic JSON writes, stage statuses, injected subprocess callable, dry-run isolation, status-only read path, verified `--from-stage`, `--force-stage`, and downstream invalidation. Test zero-exit-without-output failures and source/evidence identity drift.

### Task 4: Add NBA-specific stage verifiers

Use the canonical artifact validator, Slice 5 sweep contract, and accepted Slice 6 evidence contract. Do not duplicate model math or audit analysis. Test incomplete/legacy artifacts, ambiguous trained outputs, incomplete/failed sweep, changed receipts, failed/blocked audit, and output collisions.

### Task 5: Add report-only decision

Implement deterministic blocked/controlled classifications and Markdown/JSON output. Test that missing/failed evidence cannot be promoted, strong saved ROI cannot override failed integrity, ranker disabled stays not-requested, and no configuration can authorize promotion/training/live actions.

### Task 6: Add CLI and operator documentation

Implement parser/main, exact exit codes, sanitized output, and status/dry-run mutual exclusions. Document normal use, owner launch boundaries, resume semantics, artifacts, and why Slice 7 completion is not Slice 8 historical acceptance.

### Task 7: Independent review and coordinator verification

Run specification review first, repair in-scope misses, then code-quality review. The coordinator verifies exact scoped diffs, parser compatibility, test results, no overlap with Slice 6/reconciliation, and no protected-file modifications. Commit, merge, push, training, and real-data execution remain separately authorized.

## Required tests and controlled probes

Disposable real-path probes must cover:

1. Strict YAML and exact resolved identity.
2. Generated commands parse through actual downstream parsers.
3. Dry-run makes zero subprocess calls and writes only under `_dry_run`.
4. Status mode performs no writes or verifier/model/DB calls.
5. Attach rejects legacy, incomplete, malformed, changed, or wrong-stat artifacts.
6. Train output resolution accepts exactly one new valid manifested artifact and rejects zero/multiple/new-invalid outputs.
7. Zero child exit without semantic output fails the stage.
8. Sweep/evidence FAIL versus BLOCKED/UNSUPPORTED exit/status preservation.
9. `--from-stage` refuses stale or missing earlier evidence.
10. `--force-stage` invalidates the selected stage and all downstream statuses.
11. Config/artifact/sweep/audit identity changes invalidate reuse and prior decision output.
12. Ranker disabled is `not_requested`, not passing.
13. Decision outputs remain non-decision-grade/live-blocked under every v1 path.
14. No global conformal offsets are loaded or planned.
15. No protected source, production artifact, DB, network, or model fit is touched by acceptance tests.

Disposable integration probes may use controlled child-process fixtures or injected command runners. They must not launch real training, load real models, query a DB, or create outputs outside Hermes scratch/the isolated test temp directory.

## Validation commands

Run from the approved Slice 7 worktree after Slice 6 is integrated:

```text
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m pytest tests/test_train_pipeline.py tests/test_run_backtest.py tests/test_backtest_harness.py tests/test_bet_simulator.py -q
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m ruff check src/models/nba/lifecycle scripts/run_nba_model_lifecycle.py
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m compileall -q src/models/nba/lifecycle scripts/run_nba_model_lifecycle.py
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe scripts/run_nba_model_lifecycle.py --help
git diff --check -- src/models/nba scripts/run_nba_model_lifecycle.py configs/nba/lifecycle docs/understanding/nba-model-lifecycle.md docs/operations/nba-lifecycle-slice7-2026-11-02.md docs/operations/nba-coding-readiness-plan-2026-11-01.md docs/understanding/nba-training-evaluation-slices.md
```

The implementation acceptance run must also execute one controlled dry-run and one injected/fake-child complete lifecycle, then mutate one artifact/sweep/audit input at a time to prove downstream invalidation. Record actual commands and counts in the Slice 7 operations evidence document; do not predeclare expected pass counts.

## Acceptance criteria

Slice 7 is source-complete only when:

- The strict YAML and actual downstream parser contracts agree.
- Dry-run, status, normal execution planning, verified resume, force-stage invalidation, and semantic output verification pass controlled tests.
- A zero child exit cannot create a completed stage without valid outputs.
- Artifact identity is pinned once and propagated through sweep/audit identities.
- Slice 6 FAIL/BLOCKED/UNSUPPORTED results survive unchanged into lifecycle state and decision output.
- Ranker absence is explicit and non-passing.
- Decision artifacts are deterministic, report-only, historically unverified, non-decision-grade, and live-blocked.
- No protected files, existing tests, model math, DB state, sync lane, or production artifacts changed.
- Independent specification and quality reviews approve the exact scoped diff.

Slice 7 completion does **not** mean:

- historical policy is approved;
- sync/source coverage is verified;
- a model was trained or loaded;
- replay/sweep outputs are decision-grade;
- a baseline is accepted;
- promotion/deployment is authorized.

Those remain Slice 8 and Chase-launched acceptance gates.

## Relevant prior lessons and invariants

- `operations/critical-invariants` and `operations/hard-facts`: empirical CDF only, no global conformal offsets, preserve Q10 behavior, temporal integrity, SQL isolation, and owner-launched long jobs.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: lifecycle identity and timestamp fields do not certify source availability or historical query correctness.
- `lessons/quote-clean-clv-before-feature-work`: missing timing/close/source evidence stays blocked; attractive ROI cannot substitute for it.
- `lessons/implausibly-profitable-backtests-are-methodology-red-flags`: the decision report must privilege evidence integrity over profitability.
- `lessons/cheap-baseline-before-architecture`: keep v1 NBA-specific and minimal; do not build a generic cross-sport framework or ranker before the bounded lifecycle works.
- Engineering OS remains paused and is not a Slice 7 dependency.

## Ready-for-implementation definition

The scope is ready when accepted Slice 6 source is on the chosen base, its exact parser/output contract is reconciled into this plan, active worktrees/writers are rechecked, and Chase approves the writable allowlist and v1 decisions above. Implementation must then occur in a new isolated Slice 7 worktree with independent review.