# NBA lifecycle — Slice 7 delivery and evidence

## Current state

**Complete for controlled lifecycle mechanics — SPEC PASS / QUALITY APPROVED; ready for owner commit/integration review.** Branch `repair/nba-lifecycle`, worktree `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-lifecycle`. All changes remain uncommitted and unmerged. No commit, merge, push, real training, DB access, or deployment was performed.

## Owner decisions

Chase explicitly requested implementation through merge readiness, concise updates, a concurrent educational wiki, reusable skill capture, no unit tests, minimal scratch validation, bounded subagents, and section-by-section validation rather than one end-loaded validation phase.

The coordinator owns documentation/skills. One isolated code writer owns only the new NBA lifecycle modules, CLI and example YAML. Existing source, tests/fixtures, model artifacts and concurrent reconciliation/automation files are read-only.

## Integrated dependency base

- Base: `bf7a3e970906ed2161d744ff9cd0fcf39e7fc086`.
- Slice 6 source: `aaf3918ae8e916a192c48bd46f4644d38620b7b5`, merged at the base above.
- Actual accepted `src/backtesting/nba/evidence.py` SHA-256: `99ef8ddebe3ef52d91b064771df465db2b37f92d73b57c40ce5f86bc4aa1dfea`.
- Compared Slice 6 evidence module and lazy backtesting initializer between the accepted source commit and destination base: no differences.
- Protected tracked source/config/script/test fingerprints recorded before writer launch in Hermes scratch `nba-slice7/baseline.json`.
- No inherited uncommitted source snapshots were needed.

## Worker route

The Spark-profile tool-free canary returned deterministic unsupported-model HTTP 400. It was not retried. An explicit `gpt-6.1-sol-900k` / `openai-codex` tool-free canary passed. The worker uses explicit high-reasoning per-launch overrides and a wall-clock/turn budget; no provider/profile configuration was changed.

## Incremental verification

### Before implementation

- Actual isolated-worktree baseline: 64 existing tests passed across replay CLI, harness, simulator and training pipeline (2.41 seconds; existing asyncio deprecation warning).
- The short offline guard disables dotenv loading, removes DB connection environment variables without printing values, and blocks SQLAlchemy engine creation/socket connections. It does not patch application source.

### Core configuration, adapters and decisions

Implemented; coordinator's bounded core check and scoped Ruff pass. The check exercised import isolation, duplicate/alias YAML rejection, strict scalar rejection, blocked/failed decision classification and generated command compatibility with the actual training, sweep and audit parsers. It performed no child training/sweep, model loading or DB access. Independent section specification review is running while the writer builds runner/CLI.

Receipt: Hermes scratch `nba-slice7/core-check-receipt.json`; reusable short driver `core_check.py`. No permanent tests were written.

### Runner, evidence verification and CLI

Implemented. Coordinator checks passed:

- Native CLI help, planning dry-run and status; guarded API dry-run/status produced no normal-run directory, runtime imports or children.
- Actual Slice 5 exporter with reused tiny in-memory collaborators → Slice 6 integrity-only audit export → Slice 7 decision.
- Verified downstream resume invoked zero children.
- Forced audit preserved its first output and created a second exclusive attempt.
- The public decision API refused an omitted configured timing check.
- Tampered audit export failed and withdrew the old accepted conclusion.
- An actually exported missing-clock audit remained BLOCKED/exit 2.
- Scoped Ruff and compile passed. Existing affected suite passed again: 64 tests, 1.61 seconds, existing asyncio warning only.

The small integration driver covers six scenarios, not an exhaustive upstream retest. It uses dummy opaque artifact payloads that are never deserialized, the actual artifact builder/exporter/auditor, and guarded in-memory collaborators. It is not real model or historical evidence. An initial omission-check probe mistakenly read the already-corrupted state; it correctly yielded invalid evidence, not the expected incomplete classification. The corrected probe uses the verified pre-corruption stage state and passes; no application logic was weakened.

Receipts: Hermes scratch `nba-slice7/integration-receipt.json`, `core-check-receipt.json`, and `final-tests.log`. Source remains uncommitted; no upstream source or existing tests were changed.

### Independent review

Independent specification review `deleg_f12abda0` returned **PASS** after the required-audit-check repair. The earlier core review's exact-check-list finding is closed by an explicit configured-list argument and equality enforcement; the canonical nonlegacy attach/calibration validation runs before sweep. Initial independent quality review `deleg_45d5e3e8` led to the snapshot repair below.

Coordinator additionally checked native CLI invalid-mode/duplicate-option rejection and byte-unchanged lifecycle-owned files after status. A CLI Pyright override annotation issue was fixed locally (NoReturn error handler, duplicate detection in main); scoped Pyright now reports zero errors/warnings, and Ruff passes. Pyright emits the existing worktree venv-path and update notices; no dependencies/config were changed to hide them. All 400 protected tracked source/config/script/test hashes still match the launch baseline.

No readiness claim depends solely on worker/reviewer self-report; the coordinator inspected the new source and exercised its actual APIs/CLI. Final merge readiness still requires the quality verdict and final exact-scope check.

Quality review found one important issue: shallow frozen Pydantic/dataclass containers still exposed mutable nested configuration lists/receipt dictionaries. The coordinator replaced live mutable authority with immutable canonical snapshot bytes; `config`, `hyperparams_receipt` and `to_dict()` now return detached views. Construction revalidates the model and retains mode-exclusive field absence. The existing short core probe now reproduces post-validation list/dictionary mutation and confirms unchanged config/identity. Core/parser, dry-run/status, six integration scenarios, Ruff and Pyright pass again. Narrow quality re-review `deleg_5082fb71` subsequently approved the repair; no new test files or expanded scratch suite were introduced.

### Final acceptance checkpoint

Narrow quality re-review `deleg_5082fb71` returned **APPROVED**, closing its sole must-fix with a successful immutability probe. Its own interpreter lacked NumPy for a full sweep-config smoke; the coordinator's actual project-venv core/parser and six-scenario producer integration reruns passed, so that limitation was not treated as an executed pass. Specification PASS preceded quality acceptance.

Final scoped checks: Ruff and Pyright pass; native CLI help/dry-run/status and invalid-option checks pass; existing suite 64 passed; the six controlled integration scenarios pass; no new unit tests or fixture edits. All 400 protected tracked source/config/script/test hashes match the launch baseline. New source/config/CLI hashes are recorded in Hermes scratch `nba-slice7/reviewed-source.json`. Only eight new implementation/config files, two new wiki/evidence documents and narrow edits to two existing status documents belong to this slice.

The writer ran with a bounded budget and all independent reviews were narrow. The coordinator wrote the wiki concurrently and saved reusable procedures/corrections in `gameflow-implementation-worker` → `references/nba-lifecycle-lean-delivery.md`. No production source, model math, SQL, sync/reconciliation file or existing test changed.

Suggested commit subject: `feat(nba): add evidence-aware model lifecycle runner`.

- Add strict, immutable configuration and real command adapters.
- Add verified resume/retry, saved-evidence verification and report-only decisions.
- Document safe planning, operator state and controlled acceptance limits.

## Evidence limits

Version 1 remains report-only, historically unverified, non-decision-grade and live-blocked. Controlled tiny bundles prove lifecycle mechanics only. No real model load/training, historical data certification, profitability evaluation or promotion is claimed.

## Read next

- [Lifecycle wiki](../understanding/nba-model-lifecycle.md)
- [Coding readiness](nba-coding-readiness-plan-2026-11-01.md)
- [Evidence audit delivery](nba-evidence-slice6-2026-11-01.md)
