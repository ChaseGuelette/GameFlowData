# NBA Slice 2 — artifact identity and safe completion

## Kick off immediately

This is the executable specification authorized by Chase's request to scope and proceed with slice 2. Implement only this artifact boundary; do not expand to replay sizing or historical policy.

- This chat: one implementation worker, followed by independent spec/quality review and coordinator bounded execution checks.
- Other chat: sync recovery only; S1 stays separate.
- You: review the [artifact wiki](../understanding/nba-artifact-identity.md); no training, integration, commit or deployment is being requested.

Status: scoped implementation and the separately owner-approved fixture reconciliation are verified: **SPEC PASS / QUALITY APPROVED**, **72 controlled checks passed**, **117 existing scoped tests passed**. No new test cases or changed assertions. Source remains isolated and uncommitted; integration and real-data/platform acceptance are separate gates.

Scope amendment: after the initial implementation/reviews, Chase explicitly approved adapting only the four existing fixture setups in `tests/test_train_pipeline.py` and `tests/test_run_backtest.py`. This overrides the original no-test-edit restriction only for that reconciliation; no new test cases, weakened assertions or production changes were authorized by the exception.

## Workspace and baseline

Continue sequentially in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-cli-preflight`, branch `repair/nba-cli-preflight`. Do not commit slice 1 merely to obtain a base. Its reviewed uncommitted files are the baseline and must be preserved.

Baseline snapshots: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice2-baseline-20261101/` (five known source/test files, hashes and scoped slice 1 diff). Base Git HEAD is `d478458`; HEAD-to-working-tree diff contains both slices, so review slice 2 against snapshots as well as scoped Git diff.

## Allowed edits

- New `src/models/nba_artifacts.py` — filesystem/manifest/identity helpers.
- `src/models/train_pipeline.py` — narrow save/finalize/sanity, partial-base and diagnostic-output integration.
- `src/backtesting/run_backtest.py` — verified execution resolution, legacy control and diagnostic-offset isolation.
- `src/models/training_preflight.py` — only if needed to share the manifest boundary with file-only validation.

No tests, test cases, fixtures or repository smoke/test scripts may be created or edited. No changes to `quantile_trainer.py`, Monte Carlo math, harness/S1 methods, source SQL, season defaults, model features or calibration thresholds. Coordinator owns docs/wiki/skills.

## Artifact contract

A valid manifest verifies identity/completion mechanics; it never certifies historical policy, quality or profitability.

1. Use a versioned `artifact_manifest.json` for new saved training bundles. Keep existing model filenames/joblib schemas unchanged. Required files include minutes, feature config and available/requested stat-rate models; include relevant actual saved JSON/config/copula/diagnostic outputs in content identity. Stream hashes for large files. No model deserialization in validation/preflight.
2. Record architecture `minutes_x_rate`, mode, requested and available supported stats, ordered effective features/config with canonical SHA-256 identities, actual file sizes/SHA-256, explicit successful sanity evidence, train/calibration settings, parent identity for partial retrains, source Git commit and dirty state when available, and honest observed data-count/date provenance. Do not fabricate watermarks. Record sample weighting as none for this existing unweighted orchestrator path; no fictional weights file.
3. Canonical JSON sorts mapping keys but preserves list order, including features and seasons. Reject nonfinite/unsupported metadata rather than relying on default=str. Hash actual file bytes as well as metadata. Bind declared config/feature identities to their actual saved evidence. Manifest self-identity must exclude its own hash consistently.
4. Validation rejects unsupported/malformed schema, missing/changed required files, bad/missing digest entries, requested stats outside available stats, false sanity/completion claims, inconsistent config/features, and incomplete directory execution. Restrict inventory entries to safe expected bundle-relative filenames; reject traversal/absolute paths, escaping symlinks and diagnostic bundles masquerading as models. Do not trust metadata alone to decide required filenames. Unknown source provenance may be null; completion and required hashes cannot be unknown.
5. Preserve public `find_latest_model_dir()` behavior. Execution uses a separate resolver that checks manifests/required files and skips broken discovered runs, selecting the latest valid run; explicit invalid artifact targets fail, not silently replaced. Valid manifests are checked even in legacy-compatible mode. New normal CLI execution requires a manifest; add explicit `--allow-legacy-artifacts` to permit complete file-only older bundles, labeled legacy/unverified with hashes and never decision-grade. Never bulk-add manifests to legacy/production artifacts or silently bless them. No Namespace/mock/provenance-detection safety bypasses.
6. Validate partial/calibration-only input bundles before deserialization, respecting the same explicit legacy route. Return resolved identity/state in JSON plans without loading a model, constructing a DB object, mkdir or writing files. Manifest inspection is DB-free.

## Save and completion boundaries

Training must not finalize a run without successful existing execution checks.

7. Use exclusive incomplete-directory creation; reject collisions with incomplete or final destinations instead of reusing directories. Keep existing name prefix and caller contracts. No overwrite/merge of prior runs. Check rename destinations and do not use replacing renames. Do not change existing test assertions merely to accommodate a new timestamp format.
8. Full and partial runs capture actual sanity results and finalize through a shared helper only after required files exist and sanity explicitly passed. Empty calibration or missing inference features may return a structured not-passed result for compatibility with direct callers, but MUST NOT be accepted as successful finalization. Existing exceptions must propagate. Preserve incomplete state on any failure; final-path collision must not damage either bundle.
9. Write/verify the manifest last in the incomplete bundle, then non-overwriting finalize. Revalidate contents before execution. Do not claim data/model readiness from controlled fake files or a hand-built success marker. No monkeypatch-type or special fixture paths in production logic.

## Diagnostic and offset boundaries

Reports may not mutate attached model identity or silently deploy prohibited corrections.

10. Calibration-only output goes into a separate exclusive timestamped diagnostic directory under the selected artifacts root (or an explicit diagnostics-output option if simpler). Validate the actual output target in preflight. Never write report/offset files into base_model_dir. Emit diagnostic identity: source-model hash/manifest identity, mode, config, report/offset content hashes, diagnostic-only/non-deployable status. Diagnostic bundles must be rejected as training/replay artifacts. Allow outputs missing because execution failed to remain incomplete, not successful diagnostic bundles.
11. Canonical NBA replay must not load or pass combined/global conformal offsets into MonteCarloPredictor. Keep the existing optional copula path and embedded per-model calibration behavior unchanged. Generated combined offsets remain diagnostic only. Trace the training sanity/evaluation predictor paths too: diagnostic generation may compute reports, but completion sanity should represent canonical no-global-offset execution. Do not modify the numerical calibration algorithms or Q10 behavior.
12. No changes to daily inference/production callsites in this slice; report residual consumers honestly rather than claiming repo-wide offset eradication.

## Relevant prior lessons/invariants

Read GBrain `operations/hard-facts`, `operations/critical-invariants`, lesson-tag list, keyword search `conformal offsets`, and `lessons/q10-miscalibration-is-edge` before this scope. Global offsets must not be deployed; Q10 is not blindly corrected. Preserve empirical-sample probability math and per-model calibration. This is an artifact/execution-contract change, not model architecture/promotion or historical-policy approval.

## Validation and delivery

Use real bounded execution and report exact results, including any existing mock-fixture incompatibility.

- Interpreter: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`.
- Existing scoped baseline: 117 passed in `tests/test_train_pipeline.py tests/test_run_backtest.py tests/test_nba_feature_contracts.py` with `PYTHON_DOTENV_DISABLED=1`. Run them after changes; do not edit or detect tests to make them pass. If fixtures encode false-complete execution or missing manifests, report exact failures for owner resolution rather than weaken safety.
- Run scoped Ruff, syntax/compile and scoped diff hygiene. Inspect only target diffs; preserve baseline tests exactly.
- Exercise actual help/dry-run with and without manifests/explicit legacy flags and separate calibration outputs; no network/DB calls or output creation while planning.
- Disposable operator checks under `C:/Users/Chase/AppData/Local/hermes/cache/scratch` must cover valid identity inspection, mutation, missing files, malformed/path-unsafe inventory, incomplete/diagnostic rejection, feature-order changes, explicit legacy handling, final-name collision and failed-sanity preservation. Prefer one operator validation driver with receipts; do not create repository unit suites or disguised test modules.
- Exercise the real finalization helper with filesystem bundles and narrow real orchestrator save/finalize boundaries using controlled collaborators as feasible, without fitting. Fake bytes prove validation, not deserialization/inference.
- Verify attached source hashes unchanged by controlled calibration-only diagnostics; add runtime tripwires against DB/model load/mkdir in dry-run. Distinguish simulated adapter/permission checks from real OS/model acceptance.
- Worker returns changed paths, public helper signatures, actual commands/results, caveats and remaining fixture/real-data gates. No docs, commits/push, SQL/DB, network, package installs, long training, sweep, sync, deployment or promotion.

## Stop condition

Stop after slice 2 implementation and bounded checks. The coordinator independently reviews spec then quality and verifies outputs before acceptance. Slice 3, S1 integration and production changes remain unapproved.

## Implementation return and coordinator evidence

The stronger worker finished the scoped source changes; this is not acceptance or real-model validation.

- Worker route: per-launch `gpt-6.1-sol-900k`, `openai-codex`, reasoning `high`. A tool-free availability canary passed before replacing the stopped `gpt-5.6-sol` delegate. Partial work and the slice 1 snapshot were preserved; no simultaneous writers or global config changes.
- Changed relative to slice 1: `nba_artifacts.py`, `train_pipeline.py`, `run_backtest.py`, `training_preflight.py` only. Protected `quantile_trainer.py` and the slice 1 replay fixture file were verified byte-for-byte unchanged from the snapshot. No tests were written or edited for slice 2.
- Latest coordinator rerun: **72 controlled operator checks passed**, covering real CLI entrypoints, strict/legacy manifest handling, hashes/config/feature mutation, failure preservation, full/partial save-finalize boundaries, canonical no-global-offset replay wiring and diagnostics/source immutability with controlled collaborators.
- Initial independent spec review found partial manifests mislabeled all available models as requested stats. Coordinator corrected request threading and bound it to saved `retrain_stats`. Additional actual partial-orchestrator checks verify pts-only requests, minutes-only empty stat requests with all models available, and rejection of a re-signed request/config mismatch. Retraining methods were disabled in those controlled checks; no model fit occurred. Spec re-review precedes quality review.
- Latest coordinator evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice2-evidence-abcdf6be15/receipts.json`. Driver: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice2-operator-20261101.py`. Log: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice2-coordinator-operator.log`.
- Real Windows symlink creation was blocked by `WinError 1314`; the simulated rejection branch passed but is not real-OS symlink proof. Windows rename collision/race handling was exercised; Linux rename implementation remains unexecuted on this host.
- Coordinator rerun of existing suites: **113 passed, 4 failed, 1 existing warning**. Three replay fixtures lack required manifests/explicit legacy opt-in. The full-training fixture supplies MagicMock feature configuration and mocked saves producing no actual model files; the new completion boundary rejects that success-shaped setup. These are explicit owner-resolution gates; no validation bypass or test edit was made.
- Scoped Ruff and tracked-file diff hygiene passed. Operator source checks compiled/parsed the allowed files. Controlled saved bytes and predictors establish mechanics only, not serialization/inference or historical data readiness.
- Residual combined-offset consumers outside approved scope remain in `run_sweep.py`, `compare_models.py`, `inference_job.py`, `edge_refresh_job.py` and `run_daily.py`. This slice must not be described as repo-wide offset eradication.

Independent final review: **SPEC PASS / QUALITY APPROVED**, no blocking source-code findings. Reviewer independently exercised minutes-only and REB-only manifest subsets and rejected a false all-stats claim. The reviewed production source hashes remained unchanged during the subsequent fixture reconciliation. Earlier 113/4 results above document the reason for the separate approval; they are not the final scoped-suite result.

## Owner-approved fixture reconciliation and final evidence

The four incompatible fixture setups were repaired after Chase's explicit approval, without changing production safety or any existing assertion.

- `tests/test_train_pipeline.py::TestRunIntegration::test_full_run_completes`: fixture feature configuration now contains exactly the three available rate models, mock pipeline feature fields contain ordinary JSON-compatible settings, and its mocked `save_all` writes nonempty controlled artifact files. Real manifest/sanity/finalization paths are not bypassed. File bytes are explicitly not serialized models.
- The three existing `TestMainArgParsing` replay cases explicitly opt into the existing legacy-compatibility route and provide nonempty controlled files; model deserialization remains mocked. Strict manifest-required execution remains covered by the controlled operator checks.
- Final existing scoped suite: **117 passed, 1 existing warning**. Scoped Ruff and diff hygiene passed. AST comparison confirmed all existing test function names, `assert` statements and mock `assert_*` calls remain unchanged in both edited files. No cases were added.
- Final operator rerun: **72 passed, 1 real-OS symlink check privilege-blocked**. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-slice2-evidence-168dc058d9/receipts.json`. The same scratch driver now checks explicit post-approval fixture hashes instead of incorrectly demanding that approved fixtures remain at their pre-approval snapshot.
- Real serialized-model/data acceptance, actual Windows symlink privilege-dependent verification and Linux rename-path execution remain separately unverified. No production/global-offset safety claim extends beyond the approved canonical paths.

No commit, push, main/S1 integration, database access, fitting, training, sync, deployment or later-slice launch was performed. Suggested eventual commit subject after acceptance: `feat(nba): verify artifact identity and fail-closed completion`.
