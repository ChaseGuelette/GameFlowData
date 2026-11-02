# NBA coding slice 3 — flat-first replay correctness

## Approval and isolation

Chase approved this three-source-file scope through the explicit approval form on November 1, 2026. This is coding Batch C, not historical repair S3. Implementation and bounded offline verification are authorized; commit, push, merge, test edits, training, DB access and historical policy decisions are not.

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-flat-replay`
Branch: `repair/nba-flat-replay`
Base: `f81170f38e35e07123cbb47f276a63edd1e93b40`

Other owners retain `nba-cli-preflight` (uncommitted slices 1/2) and `nba-s1-fail-closed` (uncommitted S1). Do not edit or copy their implementation into this branch. Main has uncommitted planning context; this file is the approved self-contained slice scope. Later integration must reconcile source/docs with those owners, not replace their files wholesale.

Status: **coding slice 3 complete and controlled-path verified**. Independent **SPEC PASS / QUALITY APPROVED**; coordinator final rerun passes 34 existing tests and all bounded probes against the reviewed source hashes. Changes remain uncommitted in the isolated worktree. No integration or historical acceptance is claimed. This document and the educational wiki are coordinator-owned, not worker edit targets.

## Allowed source edits

- `src/backtesting/run_backtest.py`
- `src/backtesting/bet_simulator.py`
- `src/backtesting/backtest_harness.py`: only `_filter_best_bets`, `_calculate_edges`, narrowly required private helper/import seams. Never edit `run`, `_run_date`, SQL, loading/prefetch or feature construction.

No tests, fixtures, permanent smoke scripts, dependencies, model math, source-clock/quote policy, calibration, probability clipping, or S1 changes. No sibling legacy sweep changes unless separately approved. Read project manifests/conventions and actual usages before editing. Use patch with V4A diffs for existing code.

## Required behavior

### CLI and effective evidence

1. Expose `--flat-bet-size` as optional finite strictly positive dollars. With it, flat sizing overrides Kelly; without it, preserve existing Kelly defaults. Thread into the existing harness/simulator field. Invalid flat controls fail before engine/model/output side effects. Verify actual CLI, not only constructed namespaces.
2. Enrich `result.config` before CLI export with actual effective sizing mode/amount, applicable Kelly controls, requested stats, effective per-stat edge/BL configuration, allowlist, actual predictor sample settings and exact resolved artifact identity evidence available in this baseline. Trace real APIs; never invent attributes, sample counts, digests or policy acceptance. Keep legacy keys for compatibility. Sorted mappings/lists should be JSON-safe and deterministic.
3. Do not reimplement slice 2's manifest validator. Available identity may include resolved path and actual manifest hash if present; accurately label absent/unvalidated identity and historical readiness as unverified/not-decision-grade. Preserve existing artifact discovery/offset behavior outside this scope and explicitly report inherited slice-2 integration gaps; do not certify this branch as canonical safe artifact execution.
4. Direct `BacktestHarness.run()` config enrichment is deferred to coordinated S1 integration. Do not change its literal or wrap/monkeypatch run to bypass ownership. CLI metadata scope must be documented honestly.

### Chosen direction

5. `_filter_best_bets` copies the chosen row and sets the existing `selected_side` contract to the direction actually chosen. Preserve current line-shopping, tie, allowlist and one-player/game selection policies; no new routing/fallback policy.
6. `evaluate_predictions` honors a valid explicit `selected_side`, evaluating only that branch; if its direction is disallowed or fails threshold do not fall back to the opposite side. Missing/null direction preserves legacy evaluation behavior for callers that do not perform best-bet selection. Explicit non-null unrecognized direction is invalid and fails closed with a clear error (do not silently reinterpret it as absent). Selected side and selected quote metadata must agree with the actual Bet side; no hidden opposite-side selected_price/provenance leakage.

### Stake suppression

7. Suppress nonpositive/nonfinite computed, flat or explicitly supplied stakes before bankroll mutation, bet ID advancement or append. Validate nonfinite flat input before clamping so infinity cannot become an all-in bet. Preserve positive valid sizing/caps and explicit stake precedence. Depleted bankroll cannot produce a zero-dollar Bet.
8. `place_bet` may return `Bet | None`; trace all actual project consumers before changing this contract. Guard every in-scope dereference/append; if a required out-of-scope caller would break, report it and stop for scope resolution rather than silently broaden edits. Nonpositive/nonfinite bankroll/sizing should not corrupt state.

### Empirical samples

9. Validate sample bundles consistently for all four branches: raw/no BL; full/per-stat BL; non-BL stat in a mixed BL run; optional BL sizing. Require a nonempty one-dimensional numeric real finite sample vector; malformed/missing/empty/nonfinite samples are non-actionable. Reject booleans, complex/object coercion or other malformed data rather than manufacture numeric evidence. Preserve accepted real numeric arrays and ordinary real numeric sequences where appropriate.
10. No Gaussian estimate, market-prior substitute, fabricated 0.5 or copied actionable sizing probability for invalid samples. Do not call blenders with invalid samples. Preserve valid-sample empirical `(samples > line).mean()`, existing clipping where currently applied, under/push semantics and all valid BL/model math.
11. Retain invalid sample rows as non-actionable evidence with a stable explicit sample rejection/status field. Ensure no rejected row can survive best-bet selection into a wager or poison otherwise valid players/groups. Preserve raw candidate evidence rather than silently dropping every rejected row at the calculation seam.
12. No calibration or Q10 correction. Global conformal offsets remain prohibited; this slice does not activate or tune them or claim repo-wide eradication.

## Validation contract

Use `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe` from THIS worktree with `PYTHON_DOTENV_DISABLED=1` and local `PYTHONPATH` as needed. Inspect existing pytest/ruff configuration first. Do not read project .env/credentials, connect to any DB/network, load real production models or fit anything.

- Establish a baseline of existing scoped replay/simulator/harness tests and compare post-change results. Likely files: `tests/test_run_backtest.py`, `tests/test_bet_simulator.py`, `tests/test_backtest_harness.py`; verify existence. No test edits. Mock namespaces missing new fields and tests expecting unsafe Gaussian/fake-probability behavior must be reported, never bypassed by detecting tests or weakening runtime safety.
- Run scoped Ruff, compile/AST, `git diff --check` and allowed-file status/diff inspection.
- Disposable operator probes belong under `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/`. Exercise actual APIs and actual CLI entrypoint with bounded controlled collaborators: help, invalid flat CLI before DB/model/output effects, flat forwarding and persisted config, explicit selected direction with both sides eligible, blocked direction/no opposite fallback, valid legacy missing-direction behavior, invalid explicit direction, stake 0/negative/NaN/infinity, bankroll depletion and no state mutation on suppression.
- Exercise empirical missing/empty/NaN/infinite/non-numeric/scalar/multidimensional samples and valid empirical vectors through each raw/BL/mixed/sizing path. Spy blenders must never receive invalid samples; valid blenders/math retain behavior. Verify invalid sample groups cannot mask valid rows, and rejected rows carry explicit reason evidence.
- Probe selected direction from actual harness selection through actual simulator (not only each function alone), and metadata exported through real BacktestResult where practical. Disable external calls with tripwires. Controlled data proves mechanics, not historical returns or source eligibility.
- Save command outputs and machine-readable receipts in scratch. Report exact baseline/final failures, counts and limitations. Do not create a permanent test suite disguised as an operator probe.

## Review and completion gates

Worker implements only allowed source, validates and returns paths/results/receipts. Coordinator independently reviews scoped diffs and confirms protected S1 methods and test files unchanged, then obtains independent specification review followed by code-quality review. Rerun affected checks after corrections. No green historical/model readiness claims, commits, pushes or integration without separate authorization.

## Prior lessons and evidence

Canonical Hard Facts and Critical Invariants retrieved, lesson-tag inventory and keyword search performed. Apply `lessons/empirical-cdf-for-probabilities` and `lessons/q10-miscalibration-is-edge`: generated empirical distributions are the probability source; low-tail behavior is not to be smoothed away. Source plan: `docs/operations/nba-coding-readiness-plan-2026-11-01.md`, Batch C. This branch intentionally does not inherit its uncommitted whole-file snapshot.

## First implementation and coordinator rerun

- Worker: Spark availability canary rejected with deterministic unsupported-model HTTP 400; per-launch `gpt-6.1-sol-900k` / `openai-codex` / high passed a tool-free canary and completed implementation. No profile settings changed.
- Source edits stay within the three allowed files. Coordinator reran `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/final_verify.py` using the project venv. All harness methods except `_filter_best_bets` and `_calculate_edges` were verified exact-text/AST unchanged; tests are unchanged.
- Existing suites: baseline 34 passed; implementation 31 passed / 3 failed. The three `TestMainArgParsing` cases (`test_main_runs_backtest_with_defaults`, `test_main_parses_allowed_bets`, `test_main_creates_timestamped_output_dir`) fabricate a missing `flat_bet_size` attribute through MagicMock; actual numeric validation exits 2. No fixture repair or runtime mock bypass is authorized.
- Coordinator rerun reproduced 177 passing API cases, four passing additional mechanics cases and 11 passing controlled CLI scenarios. Scoped Ruff, compile/AST and diff hygiene passed. The driver intentionally expects the three existing suite failures; its exit 0 is NOT a green test-suite claim.
- Actual CLI/export probes used bounded controlled collaborators through unchanged `run`/`_run_date` and real result writers. No production artifact loading, DB/network, fitting or historical-source acceptance was performed.
- One reproduced upstream blocker: malformed multi-stat samples can raise `UFuncTypeError` during `derive_combo_samples` before the approved `_calculate_edges` validation seam. Otherwise valid players for that date can be lost. Protected `_run_date` and model combo derivation were not changed. Complete upstream isolation needs separately approved/owned work; this is not certified as end-to-end malformed-bundle isolation.
- Direct harness metadata and slice-2 artifact validation remain deferred. Inherited legacy discovery/combined-offset loading is unchanged and not certified. The inherited edge-BL route ignores optional sizing-BL results when edge BL is active; metadata reports that limitation rather than implying it is applied.
- Evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/final-receipt.json`, `commands.json`, `compile-scope-receipt.json`, `api-receipts.json`, `edge-receipts.json` and `cli-*-receipt.json`. Scratch can be pruned; this summary is the durable outcome, not the raw receipts.
- Independent specification review is underway. Code-quality review follows closed specification review. No commit, push, merge or deployment has occurred.

## Owner-approved narrow repair extension

Chase explicitly approved both repairs through the approval form: adapt the three existing CLI fixture setups and repair the replay-local combo-validation seam inside `_run_date`. This supersedes the original no-test-edit / entirely protected `_run_date` boundaries only to the extent listed here. No new test cases are authorized. No shared Monte Carlo/model or other S1 behavior changes are authorized.

Preserved first-pass source/test baseline: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/repair-baseline/` with four exact file copies, SHA-256/size manifest and scoped diff. Review the repair relative to those snapshots as well as HEAD. Do not overwrite the original probe receipts/first-pass review evidence; save repaired evidence separately.

### Additional allowed edit scope

- `tests/test_run_backtest.py`: only fixture/setup portions of the three existing `TestMainArgParsing` cases. Explicitly supply `flat_bet_size=None` and real typed configuration/evidence collaborators as required by the CLI. Preserve existing test functions, assertions and semantics. No helper that simply mocks out `_effective_cli_config`, no production mock detection, no fake JSON serialization via default=str. Prefer actual harness/predictor construction with fitting/load/network disabled where practical, or explicitly configured realistic typed fixture objects. Existing mocked run/export assertions must still execute meaningfully.
- `src/backtesting/backtest_harness.py`: add a narrowly focused replay-local combo-validation helper; replace only the immediate combo derivation call in `_run_date` with that helper. Other lines of `_run_date` and all `run` code remain protected. Existing imports and local private helper seams may change only as required. The shared `src/models/monte_carlo.py` and all other source/test files remain unchanged by this repair.

### Combo rejection contract

- Validate required component sample vectors using the same empirical input rule. A combo requires all components valid, nonempty and equal-length; do not rely on NumPy broadcasting, pad/truncate, coerce malformed samples, substitute zeros or manufacture an eligible combo.
- Isolate invalid components/combos so valid base-stat rows, independent valid combos and other players for that date survive. Do not reject an entire player merely because one component is invalid when another combination remains valid. Preserve valid combo math and metadata by calling the existing derivation implementation only on eligible components; do not reimplement statistics/quantile math or change the global helper.
- Keep the original base prediction/sample evidence available for `_calculate_edges` rejection reporting. Retain explicit rejected-combo evidence with stable reason and component context (including unequal lengths) through the existing all-candidate evidence path where matching quotes exist. Missing components retain honest no-actionable-combo behavior; no source policy is selected.
- Do not introduce broad exception swallowing or convert unrelated prediction/derivation failures into successful partial execution. The extension addresses declared invalid component data; genuine unexpected execution errors remain errors.
- Preserve deterministic outputs, input immutability and valid aligned sample arrays. Verify both prefetched and direct controlled `_run_date` paths without actual DB calls, then exercise actual `run`/result export as feasible with controlled collaborators.

### Repair validation and review

- Rerun existing 34 scoped tests; all must pass without removing assertions or adding test functions. Rerun the original 177 API cases and 11 CLI scenarios, updating only the prior deliberately-red/observed-blocker expectations in a separate repair driver, not reclassifying old red evidence as green.
- Add disposable operator checks for malformed/empty/nonfinite/multidimensional components, unequal lengths (including length-one broadcasting), a valid independent combo on the same player, another valid player, absent components, all-invalid bundles, unchanged valid combo outputs, rejection evidence export and no source input mutation. No permanent tests/scripts.
- Scoped Ruff/compile/diff checks include the one authorized fixture file. Verify shared Monte Carlo source and every unrelated harness method exact-text/AST unchanged; verify `_run_date` changes are only the approved call seam. Worker may not edit docs or earlier first-pass evidence. No commits/integration/jobs/DB/network.
- First-pass independent review remains historical evidence; repaired source requires independent specification re-review, then quality review and coordinator rerun. Fixture repairs or controlled checks do not certify historical correctness or remove separate slice-2/S1 integration gates.

## First independent specification review and correction

Result: **SPEC FAIL** against first-pass source (scoped diff SHA-256 `8e69d5cc6fe154a27476c2499fedee36149952f9f4863f2a32a811bce066bdbc`). Two blocking findings: effective CLI config claimed all combo stats even when requested components cannot produce them; malformed components can fail protected combo derivation before edge validation. The second finding is covered by the owner-approved repair extension now in progress.

Coordinator corrected only `_effective_cli_config` in `run_backtest.py`: report requested stats plus combo definitions whose components are all requested. Seven real typed-harness metadata cases covering all nonempty subsets of PTS/REB/AST passed; scoped Ruff and diff hygiene passed. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/coordinator-metadata-receipt.json`. This tiny in-scope source correction was made separately from the repair worker's two-file harness/fixture ownership; it must be included in final re-review and source identity.

The reviewer independently reproduced the three mock failures and the hidden `asdict(MagicMock)` failure after merely adding the missing flat-size field. The approved repair spec deliberately requires realistic typed fixture collaborators rather than the reviewer's alternative of mocking out metadata. All existing assertions remain required. First-pass review does not constitute acceptance of repaired source.

## Repair return, second review and overflow correction

- Fixture repairs now use typed namespaces and actual harness/predictor/config collaborators, not a metadata bypass. Coordinator reran all 34 existing scoped tests successfully; original function/decorator/assertion inventories remain unchanged. No new tests were added.
- Replay-local combo derivation validates each required vector and equal lengths, isolates bad combos without rejecting independent combinations/other players, and retains declared rejection/component evidence through the all-candidate export path. Shared model math remains unchanged; `_run_date` differs only at its approved immediate combo-call seam, while `run` is unchanged.
- Second independent review remained **SPEC FAIL**: finite integer components could silently wrap during addition and produce a phantom actionable under, while float-overflow rejections lacked combo-level context. Reviewed combined identity: `ddc2df34c02ab271a21387883af97a07b6768c6dbb33ad17754839eb321cc57c`.
- Coordinator corrected the same approved helper: check integer intermediates against exact Python-integer sums, trap floating arithmetic overflow, validate derived sample vectors before storing, and route numeric failures into explicit `arithmetic_overflow` / `invalid_derived_samples` rejection evidence. No padding/truncation or global model change. Six adversarial signed/unsigned/float/mixed/derived checks pass with no phantom wagers.
- Latest full coordinator rerun: 34 existing tests; 177 API, 279 combo, five mechanics, three typed-fixture metadata, six overflow, seven effective-stat metadata and 11 CLI checks pass. Scoped Ruff/compile/diff checks and protected-code/assertion checks pass. These are controlled mechanics checks, not historical/model certification.
- Current source identity: harness SHA-256 `81b73131a94a01d639c95fe56d901b35f3238f92c51e1daf125161e1f558d842`; replay CLI `c6630a7b6b71bf30de9c3a9bab14a1682c41351e1aea06b75d5b81328a670540`; simulator `dad00bae683c50551ed248a2cc724925c345b4cc08f8a98f57cb9b4ffcc2aaa3`; fixture file `0cb0f0ef6f4343726c1fcad0007033c7c238cf9f110e2018a9ac63872d153b06`.
- Latest receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/overflow/coordinator-final-receipt.json`; prior repair evidence archived in adjacent `previous-repair-evidence/`. Earlier red evidence/reviews are superseded only by corrected-source execution, not retroactively relabeled green.
- Third specification review is in progress against the overflow correction. Quality review follows a closed specification review. S1 fail-closed integration, direct-run metadata, slice-2 artifact/offset-policy reconciliation, historical acceptance and merge remain separate gates. No commit/push/merge.

## Mixed-integer promotion correction

Third specification review found one remaining precision bug: signed/unsigned integer operands can promote to float, bypassing a guard based only on the result dtype. `int64(2**53) + uint64(1)` lost one unit and could create a wrong-side under. The earlier signed/float-overflow fixes, metadata, fixtures and protected-code boundaries passed review.

Coordinator now retains exact Python-integer accumulated values across integer operands even after a NumPy float promotion. Any mismatch is rejected as `arithmetic_precision_loss` (or integer `arithmetic_overflow`) with component evidence and no derived samples. This also detects a later integer addition after an earlier exactly representable float promotion. Valid shared math is unchanged.

Latest harness SHA-256: `e80ff8770a1270982109ee92d1ba51caea3b5695e517bc48883b25ab16a59c5e`. Coordinator reran the full prior green check set and 146 signed/unsigned boundary/promotion checks successfully. Precision receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice3/overflow/precision-receipts.json`; current-source full receipt remains adjacent `coordinator-final-receipt.json`. A bounded independent specification re-review is running; quality acceptance remains pending.

## Specification closeout

Independent **SPEC PASS** on the current hashes above. Reviewer reproduced both signed/unsigned precision cases with explicit `arithmetic_precision_loss` evidence and zero selected rows/bets; ordinary combos matched unchanged shared derivation exactly. Reviewer independently reran all 34 existing scoped tests and diff hygiene successfully. No repository edits were made. Code-quality review has now started against the same source identity; integration approval remains separate.

## Final delivery

Independent **QUALITY APPROVED**, no blocking findings. Reviewer traced optional bet-return callers and independently verified side/provenance coherence, rejected-stake state preservation, sample rejection, valid-combo immutability, protected code and unchanged test/assertion inventories. Independent 34-test suite, Ruff and diff checks passed on the same four source/test hashes recorded above.

Coordinator then reran the full offline driver and precision matrix: 34 existing tests, 177 API, 279 combo, five mechanics, three typed-fixture, six overflow, seven metadata, 11 CLI and 146 precision cases passed. Source hashes exactly match specification and quality review. No source corrections followed approval. Scratch driver receipts retain a historical hardcoded review-pending label; this recorded review evidence supersedes that label, not the source/results.

Delivered behavior: flat-size CLI/config evidence; selected-side preservation; invalid-stake suppression; empirical-only invalid-sample handling; replay-local independent combo validation with explicit malformed/length/arithmetic rejection evidence; three approved existing fixture repairs. No new unit tests, global model math, SQL or source-policy changes.

Remaining separate gates: reconcile this branch with uncommitted slices 1/2 and S1 before any integration; direct harness metadata and inherited fail-closed/artifact/offset behavior remain their owners' work. Stable sync and approved historical policy are required for real-data acceptance. No model promotion, training, commit, push, merge or deployment is authorized by this delivery.

Suggested future commit subject: `fix(nba): enforce flat-first replay and empirical sample safety`.
