# NBA slice 1 — CLI and DB-free preflight

## Kick off immediately

This slice makes the entrypoints safe to inspect before connecting to data or starting model work.

- This chat: slice 1 implementation/review and owner-approved fixture reconciliation are complete in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-cli-preflight`, branch `repair/nba-cli-preflight`. Hold integration pending owner approval; slice 2 is the next proposed coding scope.
- Other chat: continue sync recovery; no shared source edits. Preserve S1's separate worktree.
- You: review the [preflight wiki](../understanding/nba-training-preflight.md) and [all-slices overview](../understanding/nba-training-evaluation-slices.md). No training or integration is implied by fixture approval.
- Stop after this slice. Batches B-H are not included.

## Approved scope

Chase approved slice 1 on November 1, 2026, with no new unit tests, concise chat, educational wiki and reusable skill capture.

Allowed source changes:

- `src/models/train_pipeline.py`
- `src/backtesting/run_backtest.py`
- Optional new `src/models/training_preflight.py`

Base commit: `d478458`. Changes remain isolated; no automatic commit, integration or push.

## Required behavior

The dry-run proves argument/filesystem readiness, not data readiness or model quality.

1. Extract `build_arg_parser()` and `main(argv=None)` while preserving existing CLI flags/defaults and callable compatibility.
2. Add `--dry-run` / simple `--preflight` alias, returning a JSON plan before engine/store/orchestrator construction, model deserialization, fitting, subprocess dispatch or run/output directory creation.
3. Add training `--artifacts-dir`, default `src/models/artifacts`, and thread it to normal orchestrator execution.
4. Preserve normal data queries, model math, calibration/offset behavior, historical season defaults and staking. Flat sizing is a later slice.
5. Validate existing training mode combinations, requested stats/features, tuning controls, finite numeric values, ordinary config JSON and base-model directories.
6. Validate replay date order, finite positive bankroll/sample settings, stat/allowlist agreement, existing edge/BL controls and artifact file presence.
7. Preserve `find_latest_model_dir()`'s directory-discovery contract; use a separate execution validator. Incomplete artifacts cannot be executed. Do not claim old complete-looking artifacts are historically certified.
8. Validate target paths without creating probe files/directories. Report filesystem checks honestly; an access check is not a future write guarantee.
9. Print only LOCAL/REMOTE database labels, never URLs or credentials. Explicitly mark source/quote/data readiness not checked.
10. No unit-test changes, no S1 method changes, no DB access, scraping, fitting, sync, long jobs, commits or deployment.

## Verification contract

Validation uses existing suites plus actual CLI executions, with controlled safety probes clearly distinguished from real data.

- Interpreter: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`.
- Set `PYTHON_DOTENV_DISABLED=1` for execution checks.
- Existing scoped suites: `tests/test_train_pipeline.py`, `tests/test_run_backtest.py`, `tests/test_nba_feature_contracts.py`.
- Scoped Ruff, compile checks and scoped `git diff --check`.
- Actual child-process help/dry-run with temporary ordinary config/artifact paths; invalid inputs exit nonzero.
- Disposable safety probes in Hermes scratch must detect DB construction, deserialization, directory creation and training/job dispatch during planning.
- Fake artifact files prove presence validation only, never model-load success.
- Independent spec review precedes code-quality review; coordinator verifies actual scoped diffs and reruns checks.

## Progress and decisions

Source implementation passed independent specification and quality review. The subsequently owner-approved existing-fixture reconciliation is complete: 117 existing scoped tests pass. This is not integration approval or real-data readiness.

- Worktree created at the approved base; S1 and main source untouched at kickoff.
- Baseline scoped suites on main: **117 passed**, existing asyncio warning.
- First worker attempt used `codex-spark-worker`; provider rejected `gpt-5.3-codex-spark` with deterministic HTTP 400 for the subscription account. Worktree was verified clean afterward.
- Fallback: available delegated coding worker, same isolated scope. No credentials/profile changes or repeated unsupported-model attempts.
- Initial worker output passed 117 existing tests, but independent review found a private parser-marker validation bypass. That green count is superseded and must not be used as final acceptance evidence.
- Coordinator removed the bypass, corrected hyperparameter schema/range checks, rejected ignored forced-feature inputs and preserved null/unrestricted allowlist semantics.
- Post-correction existing suites: **114 passed, 3 failed**. The failures are the three mocked replay `TestMainArgParsing` scenarios, which supply fake model paths without required files. Strict artifact validation now rejects them. No tests were added/edited and no production bypass was retained to satisfy fixtures. Owner decision is needed before adapting existing fixtures; this is not a green integration gate.
- Coordinator actual CLI/safety checks: **20 passed**, including full/partial/calibration-only plans, invalid input rejection, unrestricted allowlist semantics and tripwires against runtime construction/model loading/mkdir. Controlled artifact bytes establish file-presence behavior only, not model correctness.
- Calibration-only now validates its actual output (the base model directory) and ignores the unused artifacts root. A controlled permission rejection and a real CLI plan with an unused invalid artifacts target both passed.
- Evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice1-coordinator-84sakw_j/evidence.json`. Disposable operator probe: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice1-coordinator-cli-check.py`.
- Scoped Ruff and diff hygiene passed after correction. Independent final review: **SPEC PASS / QUALITY APPROVED**. Existing-fixture resolution remains a separate owner gate; no green full-suite claim is made.
- Reusable workflow saved in `gameflow-implementation-worker`, reference `references/nba-cli-preflight.md`.

## Closeout and next approval

The isolated slice is review-complete and the scoped suite is green; main integration remains separately gated.

Changed source files are exactly the three allowed paths above. Chase then explicitly approved updating the three existing replay fixtures in `tests/test_run_backtest.py`: a class-local autouse fixture supplies ordinary artifact filenames for each affected test and existing cases point to that temporary directory. No new test cases or weakened assertions were added. No source changes were integrated into main, and no commit/push, DB query, training, sync or deployment was performed.

The previously failing cases were `TestMainArgParsing.test_main_runs_backtest_with_defaults`, `test_main_parses_allowed_bets` and `test_main_creates_timestamped_output_dir`. They now provide required file-presence inputs while pipeline loading remains mocked. Coordinator rerun: **117 passed, 1 existing warning**; scoped Ruff and `git diff --check` passed. Earlier 114/3 results above remain the historical reason for the explicit fixture approval, not the final result.

Suggested eventual commit subject: `feat(nba): add DB-free CLI preflight plans`. No commit has been made.

## Reusable knowledge and related pages

The wiki explains the subsystem; the skill captures the repeatable implementation/verification procedure after it is proven.

- [Coding backlog](nba-coding-readiness-plan-2026-11-01.md)
- [Core restart plan](nba-core-product-restart-plan.md)
- [S1 review](nba-s1-coordinator-review-2026-10-16.md)

The full worker prompt is currently preserved at `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-cli-preflight-spec-20261101.md`; the durable approved requirements and stop conditions are recorded above.
