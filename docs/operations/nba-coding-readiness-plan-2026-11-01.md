# NBA Training and Evaluation Plumbing — Coding Readiness Plan

**Goal:** Complete the sync-independent NBA training, replay and lifecycle code so that a stable mirror can enter bounded validation immediately.

**Architecture:** Retain NBA minutes × rate and its existing feature facade. Mirror the canonical MLB lifecycle's configuration, artifact identity, shared sweep inputs, evidence and resume contracts—not its batter/pitcher models. Build in reviewable slices; unresolved source policies remain explicit blocked inputs rather than invented defaults.

**Tech stack:** Existing Python, pandas, NumPy, SQLAlchemy, Pydantic/PyYAML and pytest/Ruff. No new infrastructure is proposed.

**Source publication checkpoint:** Slice 7 is reviewed and integrated in main merge `83c121b`; partial Slice 8 policy/context and source adapters are checkpointed in `041c013`. This supersedes the Slice-7-under-review wording below, not the outstanding policy, consumer integration, real-data or baseline acceptance gates.

**Status:** Coding slices **1–6 are implemented and source-integrated on main**; Slice 6 is pushed in merge `bf7a3e9`. Slice 7 is under independent review. Slice 8A's bounded SELECT-only readiness receipt, conservative policy matrix and educational wiki are complete, but owner approval and explicit data gaps block 8B–8E. Historical S1 stays deferred. Source integration and read-only evidence are not historical/model/platform acceptance. See the [Slice 8A receipt](nba-s8-readiness-receipt-2026-11-02.md), [policy matrix](nba-s8-source-policy-matrix-2026-11-02.md), and [eight-slice overview](../understanding/nba-training-evaluation-slices.md). Earlier checkpoint descriptions below are historical where superseded.

## Chat decisions and scope

- Chase wants coding progress across the NBA harness and all model-training plumbing, rather than spending this period only on policy documentation.
- Another chat owns sync recovery; this lane must not restart sync, mutate its target tables or edit its recovery scripts.
- S1 is isolated in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-s1-fail-closed`, branch `repair/nba-s1-fail-closed`. Do not edit or integrate it implicitly.
- Maintain linked Markdown summaries of decisions, coding status, validation and remaining gates.
- This proposal distinguishes code-complete, controlled-path verified and real-data verified. None are interchangeable.

## Main conclusion

A large amount of code can be built and exercised without the sync. We should not wait to add safe CLI/preflight boundaries, artifact validation, flat staking, evidence schemas or lifecycle planning/resume. Pure policy-aware selectors can also be built against explicit supplied timestamps without selecting an approved historical policy or certifying source semantics.

Actual historical correctness, source-clock mappings, cohort choice and full training remain gated. A missing downstream tool must show BLOCKED/UNSUPPORTED, not an empty success placeholder.

## Verified starting point

- `src/models/train_pipeline.py:33-62`: orchestrator construction obtains an engine and creates a run directory. The CLI is inline at `1025`; there is no DB-free planning path there.
- `src/models/train_pipeline.py:129-143`: full training saves then sanity-checks and renames `_incomplete`; `969-998` permits empty calibration or absent sanity features to return without failure.
- `src/models/quantile_trainer.py:474-521`: saved NBA runtime files include minutes, stat-rate models and `feature_config.joblib`; loading tolerates missing files. Required-file validation must be separate from directory name discovery.
- `src/backtesting/run_backtest.py:46-61`: automatic resolution selects the last lexicographic `nba_run_*`, without excluding `_incomplete`. `82-91,220-235` exposes Kelly but not the simulator's existing flat size.
- `src/backtesting/backtest_harness.py:794-813`: missing samples can use Gaussian probabilities, and final probabilities are clipped. Gaussian fallback violates the empirical-CDF invariant; changing existing clipping is a separate semantic choice, not bundled silently.
- `src/backtesting/backtest_harness.py:378-440` chooses a winning side/stat but drops the chosen-side identity; `src/backtesting/bet_simulator.py:349-407` evaluates both directions again.
- `src/backtesting/bet_simulator.py:69-87`: selected quote/decision provenance fields already exist; current NBA query paths do not populate a full validated clock contract.
- `src/backtesting/run_backtest.py:188-198` loads combined offsets automatically. `src/models/monte_carlo.py:570-628` implements quantile-offset sample warping. This is a real safety review item, not a harmless filename to copy into the new lifecycle.
- Canonical MLB references: `src/models/mlb/lifecycle/config.py`, `runner.py`, `adapters.py`; `src/backtesting/mlb/run_mlb_sweep.py`; `src/backtesting/mlb/line_selection.py`. Do not port the legacy MLB harness.
- Existing relevant tests include `tests/test_run_backtest.py`, `tests/test_train_pipeline.py`, `tests/test_backtest_harness.py` and NBA feature-contract/store tests. The directory-discovery tests accept minimal/empty directories; preserve that helper contract and add a distinct execution validator rather than weakening those tests.

## Batch A — DB-free CLI and execution preflight

**Can implement before sync:** yes. **Recommended first batch.**

Existing entrypoints: `src/models/train_pipeline.py`, `src/backtesting/run_backtest.py`.
Proposed focused helper: `src/models/training_preflight.py` (new path; validate ownership before implementation).

- Extract importable `build_arg_parser()` / `main(argv=None)` boundaries while preserving existing callable behavior.
- Add training and replay `--dry-run` / preflight paths before engine construction, model deserialization, run directory creation or job dispatch.
- Resolve and print supported mode, explicit artifact path, stats, sizing and existing window values; report unapproved readiness/policy inputs as unresolved.
- Validate finite numeric controls, date order, supported stats, mode conflicts, requested-stat/allowlist agreement and config/hyperparameter path/schema.
- Expose configurable training artifact output location; do not silently relocate old runs or rewrite season defaults.
- Never claim DB connectivity/coverage from this preflight. Filesystem-only inspection is allowed; dry-run must not deserialize arbitrary model files or train.

**Acceptance:** actual help/dry-run CLI executions; controlled adapters that fail if engine construction, model loading or subprocess dispatch happens; no run/output directory created; invalid input exits nonzero. Run existing entrypoint tests and scoped Ruff.

## Batch B — NBA artifact identity and completion boundaries

**Can implement before sync:** yes; real-model acceptance waits.

Existing owners: `src/models/train_pipeline.py`, `src/models/quantile_trainer.py` (only if required after tracing consumers), `src/backtesting/run_backtest.py`.
Proposed helper: `src/models/nba_artifacts.py`.

- Separate artifact path discovery from stat-specific required-file/manifest validation.
- Reject `_incomplete` artifacts for execution. Preserve an explicit compatibility/inspection route for old template artifacts without describing them as decision-grade.
- Persist schema version, NBA minutes/rate architecture, requested/available stats, ordered feature contract identity, resolved configuration, parent identity for partial retrains and source Git identity when available.
- Hash actual artifact/config evidence; a JSON manifest listing filenames is not itself content identity.
- Record observed row/date counts and provenance when execution supplies them; use unknown for unavailable values rather than invented watermarks.
- Validate required outputs and actual sanity success before finalizing. Preserve incomplete status on execution failure, empty required data or missing inference features; distinguish calibration diagnostics from execution completeness.
- Give calibration-only diagnostics a separate output location rather than mutating an attached model directory by default.
- Trace and isolate combined/global conformal offsets from canonical execution. Do not blindly apply or tune offsets, nor change existing per-model calibration behavior without a scoped review.

**Acceptance:** disposable filesystem bundles exercise missing files, malformed metadata, changed hashes, incomplete artifacts, rename collisions and failure preservation. These prove validation behavior, not trained-model quality. Existing tests unchanged; real loader smoke after sync.

## Batch C — Flat-first replay and simulator correctness

**Can implement before sync:** yes; coordinate file overlap with S1.

Targets: `src/backtesting/run_backtest.py`, `src/backtesting/bet_simulator.py`, and only approved non-S1 methods/fields in `src/backtesting/backtest_harness.py`.

- Expose `--flat-bet-size` end-to-end; record explicit flat/Kelly mode and amount. Do not mislabel Kelly output as flat.
- Preserve chosen direction through selection to simulation so the documented one-bet-per-player/game path cannot place both sides.
- Suppress nonpositive/nonfinite stake bets with compatible return/caller handling; do not simply return None into callers that dereference a Bet.
- Reject or explicitly exclude missing/nonfinite empirical samples; no Gaussian or fabricated 0.5 actionable probability fallback. Check sibling BL paths for the same missing-sample class.
- Persist effective stat/edge/BL/sizing/allowlist/sample configuration and artifact identity.
- Do not change `_run_date` or `run` concurrently with S1. Integrate result metadata at a reviewed seam or after explicit S1 coordination.

**Acceptance:** existing replay/simulator checks plus bounded controlled execution for flat sizing, selected direction, bankroll depletion, absent samples and CLI threading. No database required. Any existing tests that encode prohibited Gaussian behavior must be reported for owner resolution, not weakened silently.

## Batch D — Policy-ready shared quote/observation primitives

**Current slice:** Batch D is coding slice 4, not historical repair S4. Implemented and coordinator-verified in `repair/nba-observation-primitives` (`b2fb86f`), with owner-approved merge into main. Worker, adversarial and independent-review-resolution controlled probes passed, plus 10 existing inventory tests and scoped lint/compile/type/file checks. Historical acceptance and consumer wiring remain open. See the [decision/evidence record](nba-batch-d-observation-primitives.md). No historical source policies or consumers are activated; newer Batch B planning status is preserved during integration.

**Can build before sync:** pure contract and selection logic. **Cannot certify or activate historical policy yet.**

Proposed owners already named in S3: `src/models/features/nba/observation_policy.py`, `quote_selection.py`.
Eventual consumers: training/date-range loaders and both NBA replay line-loading paths.

- Require caller-supplied timezone-aware decision and commence information; no hidden fixed UTC default.
- Define validated input/provenance and rejection schemas, including unknown source clocks, missing commence and missing/ambiguous history.
- Implement pure eligibility/pairing with explicit cutoff inputs, deterministic identity and rejection reasons.
- Preserve same-market/book/line/compatible snapshot paired outcomes and source timestamps.
- Keep source-specific clock interpretation injectable and unapproved until S2 evidence resolves it.
- No blanket source joins or injury fallback changes. No hidden activation of a T-minus value, source-age limit, cohort exclusion or reduced-feature policy.

**Acceptance before sync:** controlled records exercise DST-aware boundaries, after-decision/at-tip rejection, incompatible sides and duplicates. Clearly label them controlled inputs, not historical API data. Production SQL integration is S2/S3/S4-gated.

## Batch E — Shared replay inputs and sweep execution

**Delivery checkpoint — November 1, 2026:** [Slice 5 is implemented and controlled-path verified](nba-shared-input-sweep-slice5-2026-11-01.md), **SPEC PASS / QUALITY APPROVED** in `repair/nba-shared-input-sweep`; source is uncommitted/unmerged. Flat-only, process-local shared inputs invoke the corrected existing harness/simulator, not the legacy sweep fast path. Final checks passed 78 existing tests, 551 contract-repair checks, seven parity/failure groups with 24 CLI cases, and 27 output-location safety checks. Inherited dependency hashes match upstream. Main separately advanced with slice 3 merged; artifact-validator and slice-5 integration remain gated. Historical S1 remains deferred; no real-data acceptance is implied.

**Can build before sync:** orchestration/cache layer with controlled inputs. **Real historical comparisons wait.**

Existing seams: `BacktestHarness`, `BetSimulator`, `src/config/stat_config.py`.
Proposed owners: `src/backtesting/nba/sweep_config.py`, `sweep_runner.py`, `run_nba_sweep.py` (new paths, not existing APIs).

- Separate data/prediction acquisition from configurable simulation so a sweep loads inputs and generates samples once.
- Bind cached inputs to exact artifact, feature/policy identity, dates, source identity, sample count and RNG settings. Do not reuse stale predictions across changed inputs.
- Evaluate declared flat/raw control and optional approved parameter grids over identical base inputs; avoid repeated inference per cell.
- Save per-cell config, metrics, bets, candidate edges and shared input identity with explicit counts and exclusion/no-op reasons.
- Include an explicit planned/legacy/not-decision-grade state until quote-clean source integration passes.
- Scope reproducible RNG threading only after tracing existing generators; do not claim a CLI seed guarantees determinism without passing it through all stochastic owners.

**Acceptance:** bounded offline controlled sweep demonstrates common-input reuse, correct per-cell sizing/config and no stale-cache reuse. No tuning/training or expensive grid launch.

## Batch F — Evidence audits and evaluation parity

**Slice 6 checkpoint — November 2, 2026:** Complete for the owner-approved simpler saved-artifact scope: **SPEC PASS / QUALITY APPROVED** and integrated/pushed on main in `bf7a3e9`. The delivered source is `src/backtesting/nba/evidence.py` plus the approved lazy `src/backtesting/__init__.py`; no producer/test/DB/model changes. Output checks flag observed changes without native locks or a race-proof guarantee. See the [decision/evidence record](nba-evidence-slice6-2026-11-01.md) and [explanation](../understanding/nba-evidence-audits.md). Unsupported closing/source observations remain blocked. Integration and historical/model acceptance remain separate.

**Can build before sync:** schemas, artifact-only analyzers and command adapters; actual source joins wait.

Proposed owner: `src/backtesting/nba/evidence.py`; specific scripts only after inventorying existing diagnostics to avoid duplicate analyzers.

- Verify config/result/bet/candidate counts and identities; missing files or child failures are not successful stages.
- Summarize side/stat/book concentration, coverage/dropout, quote ordering and exclusion reasons from saved evidence.
- Add NBA adapters to reusable CLV/timing/dropout tools where genuinely generic. Do not copy MLB-named scripts and assume their SQL/schema is NBA-compatible.
- Keep missing source observations as unmeasurable/blocked, never passing timing or CLV.
- Profitability and calibration reports cannot override data/execution failure or the Q10/no-offset invariants.

**Acceptance:** malformed/mismatched evidence and absent timing data fail or block as designed; valid controlled bundles verify analyzer mechanics only.

## Batch G — NBA lifecycle configuration, planning and resume

**Can build before sync:** yes, after upstream command contracts exist. **Do not execute training or unsupported stages.**

Proposed owners: `src/models/nba/lifecycle/{config,adapters,runner}.py`, `scripts/run_nba_model_lifecycle.py`.

- Strict YAML with run purpose, training or exact attached artifact, explicit evaluation window, sizing, policy identity and bounded audit selection.
- Stages mirroring the MLB workflow: validate → train-or-attach → artifact identity → sweep → audit → optional ranker → report-only decision. Optional staking remains separate from flat-first certification.
- Persist resolved config, command plan, stage state, run manifest and evidence identities.
- Separate dry-run from real-run state. Plan generation may write explicitly requested planning artifacts but must never be marked executed.
- Resume only when input identity and required output verification match; changing an upstream input invalidates downstream stages.
- Status-only inspection must not start jobs or initialize/mutate real-run state.
- Unknown policies and absent executable adapters show BLOCKED/UNSUPPORTED. Do not implement success-returning stubs.
- Decision outputs remain report-only/live-blocked. No deployment, model promotion, DB repair or publication action.

**Acceptance:** CLI dry-run/status; changed evidence/config invalidates resume; missing output and failed commands stop downstream work. Use bounded controlled child processes only; no long job.

## Batch H — S2-approved data integration and post-sync validation

**Slice 8A checkpoint — November 2, 2026:** The exact 19-game, remote/local, SELECT-only evidence pass is complete without archive access or mutation. Identity parity passed; mapping, current props history, quote timing/provenance, injury editions and several feature families remain blocked or unknown. The [readiness receipt](nba-s8-readiness-receipt-2026-11-02.md), [source-policy matrix](nba-s8-source-policy-matrix-2026-11-02.md), and [educational wiki](../understanding/nba-historical-decision-policy.md) are ready for owner review. No policy YAML or 8B–8E writer is authorized yet.

**Not executable from this proposal alone.** These remain real coding tasks, not forgotten documentation:

- Wire approved observation/quote policy into training and date-range loaders, including defaults, injury as-of behavior, spread orientation and ordered feature parity.
- Replace direct and prefetch replay SQL with one selection contract, keyed ID handling and explicit failure propagation.
- Reconcile S1 before editing its methods; do not reintroduce swallowed execution errors or accepted partial outputs.
- Implement approved training/calibration/cohort boundaries and artifact provenance; actual tuning/feature-selection split semantics require an additional source trace before editing.
- Separately scope advanced rolling-producer fixes and any recomputation; table regeneration is not implied by source edits.
- Run small real feature/artifact/replay checks, then freeze the baseline command for Chase.

## Recommended implementation order and review burden

1. Approve Batch A only as the first source-change slice. Smallest useful foundation; no S1 conflict or policy selection.
2. Review A's scoped diff and actual checks, then approve B and C as separate slices.
3. Build D's pure contracts and E's common-input evaluation seams; do not activate unapproved historical semantics.
4. Add F/G against working upstream commands, not speculative placeholders.
5. After stable sync and S2 policy signoff, execute H and real-data acceptance of the earlier batches.

This is one coding lane, not permission for simultaneous writers across large files. Use a separate approved worktree for this lane and independently review each slice. The broad goal is endorsed by Chase; exact multi-file execution scopes still need approval under the project implementation contract.

## Verification and evidence limits

- Read-only audits were split across NBA replay, NBA training and canonical MLB lifecycle. Coordinator directly verified key CLI, artifact, probability, direction-selection and offset findings.
- No code was changed and no test suite, model fit, SQL query, sync or deployment was run for this audit.
- Existing test inventory includes `tests/test_run_backtest.py`; its absence in one initial explorer's narrow result was corrected by direct lookup.
- Before executing each batch, trace the remaining definitions/usages and inspect relevant existing tests. The bounded audit is not an exhaustive certification of every training/diagnostic path.
- Use `venv/Scripts/python.exe -m pytest` on the explicitly scoped existing tests after confirming that interpreter works; run scoped Ruff and `git diff --check`. Disposable probes belong in Hermes scratch, not new repo unit tests.

## Relevant prior lessons/invariants

- `operations/hard-facts` and `operations/critical-invariants`: empirical probabilities, no global conformal offsets, Q10 preservation, temporal integrity, isolated SQL and owner-launched long jobs.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: source columns alone do not certify consumer queries or old artifacts.
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: post-sync probes must bound real inputs and inspect indexes first.
- Historical training and customer publication are separate lanes. No frontend/Engineering OS dependency is added to the baseline gate.

## Related records

- [Core restart plan](nba-core-product-restart-plan.md)
- [S1-S5 repair specification](nba-historical-repair-spec-2026-10-16.md)
- [S1 review](nba-s1-coordinator-review-2026-10-16.md)
- [S2 source clocks](nba-s2-source-clock-review-2026-10-16.md)
- [Previous next-step summary](nba-restart-next-steps-2026-11-01.md)
