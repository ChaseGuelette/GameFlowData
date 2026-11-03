# NBA pretraining hardening implementation receipt — 2026-11-03

## Coordinator verification — supersedes the initial worker blocker

- Actual sweep parser/config owner `src/backtesting/nba/run_nba_sweep.py` now accepts/forwards all four limits. The initial allowlist typo is resolved, not an outstanding implementation gate.
- Preserved direct regular/playoff season tokens while retaining lifecycle's existing regular-season-only scope. Partial historical parents must match requested fitting/calibration seasons and limits before model loading. Missing requested fitting seasons fail; default sibling artifact-container usage remains compatible.
- Chase explicitly approved the one existing full-run test's input-only adaptation. The coordinator changed it; assertions and test count are unchanged.
- Final coordinator rerun: **197 existing offline tests passed in 0.92s**. Controlled full/partial training/manifest/CLI planning, date-group consumers, saved-evidence compatibility, and review regressions passed. Separate actual native replay CLI -> sweep CLI -> audit -> lifecycle completed with `controlled_mechanics_complete`; source-readiness remains unverified and decision-grade false. No genuine model fitting/loading or live DB/network calls occurred in these checks.
- Scoped Ruff/compile/whitespace passed across **19 changed/new Python files**, including the approved test fixture. Protected probability/model math, source eligibility, production artifacts and props-linker source are unchanged.
- Current receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-pretraining-hardening-parent/coordinator-checks.json`, `native-workflow-receipt.json`, `review-regression-receipt.json`, and `validation-scope.json`. Synthetic artifacts remain non-loadable/non-promotable evidence payloads.
- Initial independent spec review returned two localized findings; coordinator corrected and regression-tested both. Final independent review returned **SPEC PASS / QUALITY PASS** and independently reran the 96-test core subset successfully. On owner authorization, source was committed as `974b57ea` and merged into main as `4ffdf065`. Destination index bytes matched the reviewed branch; the 96-test core subset passed again from actual main imports with zero external I/O/model-fit attempts. Remote publication is established by Git-ref readback, not this document; deployment is not certified.
- The separate canonical read-only investigation is saved in main at `docs/operations/nba-pretraining-bounded-data-investigation-2026-11-03.md`: bounded triage is complete, but source acceptance is blocked. The actual local populated source-acceptance run and training remain unperformed.

## Initial worker disposition (historical; superseded by integration above)

Implemented the independent in-scope source sections and bounded offline verification in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-pretraining-hardening`, branch `repair/nba-pretraining-hardening`, starting at `3f7d80c6`. No commit, push, merge, deployment, actual training, live model loading, DB/network access, credentials, subagents, or production policy creation.

**Initial worker handoff limitation, now resolved above:** the allowlist originally named nonexistent `src/backtesting/nba/sweep_cli.py`. The worker correctly stopped at that boundary; the coordinator connected the actual `run_nba_sweep.py` owner and verified the populated controlled path rather than adding an unused substitute CLI.

The worker implemented chronology, historical training, native initial-population bounds, replay CLI forwarding, sweep Python config/runtime/evidence support, and lifecycle configuration/argv/artifact compatibility. Parent retains review; data investigation remains separate.

Operator wiki: [NBA pretraining hardening](../understanding/nba-pretraining-hardening.md).

## Exact worker-owned changed files

1. `src/models/train_pipeline.py`
2. `src/models/nba_training_chronology.py` — new shared gates/date-group splits/proof helper
3. `src/models/quantile_trainer.py`
4. `src/models/hyperparameter_tuner.py`
5. `src/processing/feature_selection.py`
6. `src/models/feature_store.py`
7. `src/models/features/nba/requests.py`
8. `src/models/features/nba/historical_loader.py`
9. `src/models/features/nba/acquisition.py` — new immutable limits and cumulative budget helper
10. `src/backtesting/nba/historical_execution.py`
11. `src/backtesting/run_backtest.py`
12. `src/backtesting/nba/sweep_config.py`
13. `src/backtesting/nba/sweep_runner.py`
14. `src/backtesting/nba/evidence.py` — bounded receipt compatibility only
15. `src/models/nba/lifecycle/config.py`
16. `src/models/nba/lifecycle/adapters.py`
17. `src/models/nba/lifecycle/runner.py`
18. `docs/understanding/nba-pretraining-hardening.md`
19. `docs/operations/nba-pretraining-hardening-implementation-2026-11-03.md`

No edit was needed to `nba_artifacts.py`: its existing per-population provenance dictionaries can carry the execution envelope without changing manifest schema. No changes to source eligibility/feature formulas in `historical_sources.py` or to model objectives, search space, empirical CDF, quantile monotonicity, or calibration math.

### Unowned working-tree changes

The approved `.hermes/plans/2026-11-03-nba-pretraining-hardening.md` was already untracked at startup. During execution, an independent edit appeared in `tests/test_train_pipeline.py`: its full-run fixture now supplies disjoint seasons and chronologically separated frames. The worker did not make, revert, or modify that edit. It is outside this delivery's owned file list and requires parent attribution/review.

## Validation results

All runtime verification used the specified native venv and reusable offline guard. Successful guarded runs reported zero live connection/model-load/model-fit attempts.

- Seven required suites: initial run **95 passed, 1 failed**. The old full-run fixture omitted `season_id` and reused one population for fitting and calibration; the new gate correctly rejected it. No bypass was added.
- After the independent fixture edit, repeated seven-suite runs passed; final result **96 passed in 0.85s**.
- Additional non-fitting tuner, feature-contract, and transform coverage: **101 passed in 0.24s**. The 12 original real-fitting tuner tests were not run; their exclusion is intentional, not a claim that real estimator fitting was validated.
- `probe_integration.py`: **PASS_CONTROLLED_WITH_SCOPE_BLOCKER**. Actual training CLI dry-run has no engine/model/output; malformed seasons, overlap, cutoff mismatch, conflicting fixed/tuning options and invalid budgets reject. Full/partial orchestrators sort and gate populations before fitting; controlled model boundaries feed the actual artifact producer. Exact attachment/chronology is recorded and downstream matched. Mutation during save blocks finalization. Native loader SQL includes deterministic `LIMIT` remaining allowance + 1, seasons/role predicates, transaction-local timeout and rollback; overflows stop before native source acquisition. Lifecycle dry-run forwards inputs; actual lifecycle → training CLI → controlled model boundary → manifest → artifact stage completes, then the real sweep parser reproduces the scope blocker. Resume reuses the verified training stage; changed budgets/metadata invalidate reuse without launching further children.
- `probe_splits.py`: **PASS_CONTROLLED_ONLY**, 201 controlled estimator calls. Actual suite/minutes/rate owners, full/partial shared/per-quantile tuner callers, feature ranking and count-CV paths keep complete dates together. No-metadata splitting matches the legacy row split. No real estimator fit occurred.
- `probe_evidence.py`: **PASS_CONTROLLED_ONLY**. New bounded plans validate, changing a budget changes acquisition identity, tampering fails, and an actual previously produced unbounded v2 saved sweep still returns integrity **PASS**. No ledger/hash/outcome verification was weakened.
- Scoped Ruff: **All checks passed**.
- Scoped `compileall` with bytecode redirected into scratch: **exit 0**.
- `git diff --check`: **exit 0**.

Filename discovery checked NBA/quantile/lifecycle/artifact/historical test patterns. No NBA quantile/lifecycle/artifact/historical-named suites beyond the listed applicable suites surfaced; unrelated MLB suites were not run.

An attempted combined AST command using Python `-c` was blocked by the terminal approval policy before execution. It was replaced with the successful scoped `python -m compileall` invocation below. No approval/config workaround was made. Whole-repository tests and a full Pyright/typecheck were not run.

## Exact check commands

Run from the worktree. The following Bash definitions expand to the exact interpreter, guard, root, and scratch used:

```bash
P='C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe'
G='C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py'
R='C:/Users/Chase/Projects/GameFlowData-worktrees/nba-pretraining-hardening'
S='C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-pretraining-hardening-worker'

"$P" "$G" "$R" pytest tests/test_train_pipeline.py tests/test_nba_mode_loaders.py tests/test_nba_feature_store_inventory.py tests/test_nba_source_boundaries.py tests/test_run_backtest.py tests/test_run_sweep.py tests/test_feature_selection.py

"$P" "$G" "$R" pytest tests/test_hyperparameter_tuner.py::TestTuningResult tests/test_hyperparameter_tuner.py::TestSearchSpace tests/test_hyperparameter_tuner.py::TestTunerInit tests/test_hyperparameter_tuner.py::TestCalibrationGap tests/test_hyperparameter_tuner.py::TestParamsToConfig tests/test_hyperparameter_tuner.py::TestGetTrialsDataframe::test_raises_without_study tests/test_hyperparameter_tuner.py::TestSaveAndLoad::test_save_study_raises_without_study tests/test_hyperparameter_tuner.py::TestSaveAndLoad::test_save_best_config_raises_without_result tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py

"$P" "$G" "$R" probe "$S/probe_integration.py"
"$P" "$G" "$R" probe "$S/probe_splits.py"
"$P" "$G" "$R" probe "$S/probe_evidence.py"

SOURCES=(
  src/models/train_pipeline.py src/models/nba_training_chronology.py
  src/models/quantile_trainer.py src/models/hyperparameter_tuner.py
  src/processing/feature_selection.py src/models/feature_store.py
  src/models/features/nba/requests.py src/models/features/nba/historical_loader.py
  src/models/features/nba/acquisition.py src/backtesting/nba/historical_execution.py
  src/backtesting/run_backtest.py src/backtesting/nba/sweep_config.py
  src/backtesting/nba/sweep_runner.py src/backtesting/nba/evidence.py
  src/models/nba/lifecycle/config.py src/models/nba/lifecycle/adapters.py
  src/models/nba/lifecycle/runner.py
)
"$P" -m ruff check "${SOURCES[@]}" --output-format concise
PYTHONPYCACHEPREFIX="$S/pycache" "$P" -m compileall -q "${SOURCES[@]}"
git diff --check
```

The integration probe precedes the evidence probe because it produces the controlled artifact whose current receipt the latter reads. The probes stay in scratch; no permanent tests/fixtures were created by the worker.

## Receipt/artifact paths

Scratch root: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-pretraining-hardening-worker`

- `integration-receipt.json`
- `split-receipt.json`
- `evidence-receipt.json`
- `probe_integration.py`, `probe_splits.py`, `probe_evidence.py`
- Last integration scenario: `integration-78960b8083204098b9ce93f55fe6a776/`
- Controlled full artifact: `integration-78960b8083204098b9ce93f55fe6a776/full/nba_run_20261103_120341/`
- The scenario also contains synthetic policy/metadata/config, captured population SQL, partial/failed-incomplete payloads, and lifecycle stage receipts.

These payloads explicitly contain controlled dummy model bytes, not trained models. Sanity/model/feature/source boundaries in probes are synthetic. Unsigned manifests establish consistency, not authenticity. Do not load or promote scratch payloads.

## Remaining limits and parent action

1. Resolved by the coordinator: actual `run_nba_sweep.py` parser/config now constructs and forwards `AcquisitionLimits`, and the controlled native workflow passes. No further parser implementation is outstanding.
2. A source-ready policy and valid historical source metadata remain external prerequisites. No data investigation or readiness certification was performed.
3. Historical calibration-only mode is intentionally rejected; historical full/partial holdout diagnostics are integrated.
4. Acquisition limits bound initial population, not every source's memory/runtime. The timeout transaction path was verified with controlled connection objects, not a production/local PostgreSQL session.
5. Direct legacy orchestrator construction still creates its engine/run directory, as before. Direct `run`/`run_partial` validate seasons before config/data/model work; CLI preflight validates the complete request before constructing the orchestrator.
6. Parent should review the independently changed permanent fixture separately. The final 96-pass result is for the current worktree with that edit, not a claim that the original malformed fixture became compatible.
7. Cold starts, structural zeros, target/rolling thresholds, historical-position/trade choices, missingness/source policy, production serving, and numeric model/calibration behavior remain deferred or unchanged. No current-season fitting route was added.
8. No GBrain/remote sync was attempted because this task explicitly prohibited network access.
