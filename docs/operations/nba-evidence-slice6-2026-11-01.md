# NBA evidence audits — coding slice 6

## Owner authorization and scope

Chase requested scoping, reviewed the prepared scope, then authorized implementation with “complete slice 6” on November 1, 2026, EST. The independently re-reviewed plan is `.hermes/plans/2026-11-01_213948-nba-slice6-evidence-audits.md`; its normative version-1 adapter rules are the acceptance contract.

Status: **complete for the owner-approved simpler saved-artifact scope — SPEC PASS / QUALITY APPROVED**. Controlled offline validation is complete. Source remains uncommitted in the isolated worktree; no merge, push or deployment was performed. Historical-data, model, profitability and promotion acceptance remain separate.

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-evidence-audits`.
Branch: `repair/nba-evidence-audits`.
Base: `3aa73a5fbed0ebd7a3974f1e0f76793a58890c27`.

Main merge `a44b09a` integrated the needed predecessor source before launch. Coordinator verified tracked paths and exact config/runner/artifact-helper bytes against the reviewed slice-5 hashes. No inherited dirty source copies were needed. S1, sync and reconciliation remain separate protected lanes.

Allowed source: new `src/backtesting/nba/evidence.py`, plus the subsequently owner-approved lazy-export change to `src/backtesting/__init__.py`. Coordinator-owned docs: this record, `docs/understanding/nba-evidence-audits.md`, and narrow link/status updates to the coding-readiness plan and numbered overview. No source/test/fixture/dependency changes outside this allowlist; no commits, pushes, merges, DB work, real model loading/fitting, historical runs or deployments.

## Intended deliverable

An artifact-only version-1 reader/CLI that validates file/config/count/identity commitments and occurrence-consuming row correspondence, describes concentration/coverage, and checks supported aware timestamp ordering. Unsupported CLV/horizon/source dropout remains explicit; no success-shaped adapter or inferred timing.

A report can be complete while its requested audit is FAIL or BLOCKED. Integrity-only PASS is not historical correctness. Raw samples/features are not fully persisted, so input-content reconstruction and runtime-setting application remain outside independently provable claims. `decision_grade=false` always.

## Launch verification

- Registered worktrees/status and actual implementation commit paths were inspected. Protected active reconciliation/S1 changes were not touched.
- Source/test/document hashes were recorded before implementation at `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/baseline.json`.
- Native project interpreter was used: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`.
- Existing offline baseline: 68 passed across `tests/test_run_sweep.py`, `test_run_backtest.py`, `test_bet_simulator.py`, `test_backtest_harness.py`; existing asyncio deprecation warnings only. This baseline does not establish new auditor correctness.
- Offline wrapper explicitly disables `dotenv.load_dotenv` because the installed version ignores `PYTHON_DOTENV_DISABLED`; it also removes DB URL variables without printing them and blocks network/DB connection/deserialization. This is verification isolation, not a production code change.
- Baseline driver/receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/verify_lane.py`, `baseline-pytest-receipt.json`, `baseline-pytest.log`.

## Implementation route

The first Spark canary received deterministic HTTP 400: the configured Spark model was unsupported on this ChatGPT subscription. No writer/source effect occurred. A tool-free `gpt-6.1-sol-900k` / `openai-codex` canary passed; the implementation process uses explicit per-launch model/provider/high-reasoning overrides. No profile/provider configuration was modified.

Worker is restricted to the one new source file plus disposable scratch checks. Coordinator owns documentation. Source review, actual execution, independent specification review and then quality review remain separate required gates.

## Current output-safety decision

Chase approved the simpler version after the native Windows locking repair complicated normal report publication. The accepted contract is ordinary before/after path and directory-identity checks, final input fingerprints, exclusive incomplete output and non-replacing publication. Remove the Windows native locking code; flag observed changes and return nonzero instead of claiming race-proof prevention. Concurrent replacement may cause writes before it is noticed, and transient changes between checks can escape detection. The auditor is an owner-run local tool, not a hostile-filesystem security boundary.

This explicitly supersedes the earlier review's no-write-before-detection requirement (SPEC-1). The other four findings remain required; earlier green counts do not substitute for checks against the final simpler source.

## Simpler-source confirmation — November 2, 2026

Implementation SHA-256: `99ef8ddebe3ef52d91b064771df465db2b37f92d73b57c40ce5f86bc4aa1dfea`. The native lock/handle class and ctypes/Win32/NT APIs are absent. The lazy initializer matches its approved pre-simplification bytes exactly: `f3d23431d073d7061bbe6b09cd4ebfec80c7e726f1e2d61bfb0fe7fd3e1d539d` (coordinator verified actual bytes, not the worker's inaccurate initializer hash).

Coordinator executed against those exact bytes:

- Existing scoped suite: 68 passed; protected source/dependency/test hashes verified by `verify_lane.py final`. Existing asyncio deprecation warnings remain.
- Original independent confirmation: 37 API and nine guarded CLI cases passed. Native alias and original five public-export identity checks passed.
- Non-export repair matrix: 81 assertions/cases passed, including original mixed FAIL/BLOCKED reproduction through API/module CLI, five reconstructable component contradictions, producer-generated typed payloads, malformed nested values and bounded recursion/node counts. Obsolete native-lock assertions were excluded explicitly, not weakened or relabeled as passes.
- Seven independent simpler-export cases passed: actual guarded module export with all six output hashes/sizes read back, collision/no-overwrite, new parent directories, source mutation, output identity replacement, output junction redirection, and injected I/O failure. Source mutation returns 2 with `source_changed_during_audit`; output replacement/redirection returns 1 with `output_path_changed`; no final report is published. No corrective receipt is written into replaced/redirected paths.
- Actual unchanged producer with a $10 bankroll placed a $10 stake under a $25 flat setting and passed the audit. Mixed timing violation remains FAIL/exit 1 despite BLOCKED provenance.
- Scoped Ruff, native module `--help` and tracked diff hygiene passed.

Receipts (absolute paths under `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/`): `spec-repair/run-7dffd7e8774340c8afd83f44c6eb0693/receipt.json`, `confirm-9039eae3e951460eb477b0c74bcc1a22/confirmation-receipt.json`, `paths-exports-8581bea7f7cf4ca0a0fbd23b5f3da31a/receipt.json`, `simpler-export-af57acb664f24cd68a90c9dada45d95e/receipt.json`, `clamp-b3da163700b746239ca17cd5a8cbe232/receipt.json`, `precedence-bbff925c25ed4af5bf2db7e4f2f781ea/reproduction-receipt.json`, and `final-pytest-receipt.json`.

Independent specification re-review returned **PASS** on the exact implementation/initializer hashes above. Its verdict was read back from `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/simpler-spec-review/verdict.json`; independent API/module CLI and export/readback/mutation probes are in its sibling `probe-results.json`. All four applicable prior findings are fixed; the owner-superseded race-prevention requirement was not revived.

Subsequent independent quality review returned **APPROVED**, with no must-fix findings. Verdict and actual probes: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/simpler-quality-review/verdict.json` and sibling `probe-results.json`. Coordinator read both gate verdicts and reverified the exact reviewed source hashes plus 17 protected source/dependency/test files after review. These results close controlled implementation acceptance, not historical/model/promotion acceptance or integration.

Suggested commit subject: `feat(nba): add saved-evidence audits`. Commit/integration remains Chase's decision; no successor implementation or historical run is authorized by this completion.

## Historical verification log

### Historical November 2 checkpoint — superseded by simpler-source confirmation above

After a provider timeout interrupted the first specification reviewer, the coordinator rechecked live worktrees rather than relying on its partial result. That reviewer produced no verdict. The renewed review subsequently returned five blockers; the simpler-source work above supersedes that checkpoint and does not treat the interrupted review as a pass.

The real package import guard reproduced eager replay imports before the auditor started. Chase explicitly approved the narrow initializer expansion. Lazy exports now preserve all five original public names, their actual object identities, star-import behavior, `dir` discovery and unknown-attribute errors, while artifact-only help/import no longer loads the replay modules.

Source tracing corrected the auditor's selected-to-bet metadata projection to match the existing `_selected_quote` fallback; the simulator/producer were not edited. Missing or naive timestamps still block unprovable ordering. A source-generated low-bankroll sweep also confirmed the existing simulator may place a $10 wager under a $25 flat configuration; the audit now permits positive below-flat stakes without claiming to independently reproduce bankroll settlement.

Current coordinator results (provisional until review/remaining repairs):

- 68 existing scoped tests pass; protected producer/dependency/test hashes match baseline. Existing asyncio deprecation warnings remain.
- 37 public API confirmation cases and nine guarded CLI cases pass, including actual import isolation, saved evidence mutation/metadata/schema failures, missing-clock/unsupported status, stdout-only source immutability, exclusive report hashes, output collisions/nesting and resource ceilings.
- Native Windows cell-junction escape, output alias into model evidence and incomplete-output alias rejection pass. Public export compatibility checks pass.
- Actual `python -m src.backtesting.nba.evidence --help`, scoped Ruff/compile and scoped diff hygiene pass.
- One additional mixed-case reproduction found an open status-precedence defect: a measured timing violation can be overwritten as BLOCKED when another row's provenance is unmeasurable. Expected exit 1, observed 2. This must be repaired and re-reviewed; the earlier green counts do not close it.

Receipts/drivers: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice6/latest-confirmation.json`, `latest-paths-exports.json`, `latest-clamp.json`, `precedence-latest.json`, and `final-pytest-receipt.json`. The initial confirmation-driver failure was a no-op side mutation; the corrected driver asserts each adversarial mutation actually changes its input, preserving the rejection assertion.

- Source-generated controlled raw/BL and empty bundles through actual slice-5 exporter with in-memory collaborators, not fabricated source/API responses.
- Independent schema/count/config/cell/date/provenance mutations, missing timing and unsupported observations.
- API/CLI exit parity, stdout-only no-write behavior, report readback and failure-safe output.
- Native path/alias/collision/resource/mutation handling and protected source/test hash equality.
- Final scoped existing tests, Ruff, compile and hygiene; spec PASS before quality APPROVED.

Real-data/model/source-policy and promotion checks remain separately gated even after these mechanics pass.

## Relevant prior lessons/invariants

Canonical Hard Facts/invariants and temporal/quote-clean lessons were retrieved during scoping. Raw timestamps and consistent artifacts do not establish temporal integrity; empirical-CDF/Q10/no-global-offset behavior is preserved because this slice does not modify modeling/replay math. Missing source measurements stay blocked. DB work is isolated and long jobs remain Chase-launched.

## Read next

- [Plain-English audit explanation](../understanding/nba-evidence-audits.md)
- [Eight-slice map](../understanding/nba-training-evaluation-slices.md)
- [Coding-readiness plan](nba-coding-readiness-plan-2026-11-01.md)
