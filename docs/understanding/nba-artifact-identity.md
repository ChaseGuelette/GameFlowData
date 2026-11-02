# NBA artifacts — identity and safe completion

## Kick off immediately

An artifact is the saved model bundle that replay and later lifecycle stages consume. Slice 2 makes its identity and completion claims verifiable.

- This chat: scope and implement owner-approved slice 2, then independently review its diff and execute bounded checks. Preserve slice 1 in the existing isolated worktree.
- Other chat: owns sync recovery. S1 remains separate; no overlapping writers are needed.
- You: read this explanation and the [eight-slice overview](nba-training-evaluation-slices.md). No training, integration, commit or deployment is implied.

Status: scoped implementation verified: independent spec/quality reviews passed, 72 controlled checks passed, and all 117 existing scoped tests pass after the separately owner-approved fixture adaptations. Read the [scope and actual evidence](../operations/nba-artifact-slice2-2026-11-01.md). No new cases or changed assertions; this lane is not committed/integrated. Real-data and platform limits remain explicit below.

## Three different questions

File presence, identity and readiness answer different questions.

1. Presence: are the filenames required for the requested stats there? Slice 1 checks this without loading model objects.
2. Identity: are these the same bytes, feature order and settings the run declared? Slice 2 adds a manifest and content hashes.
3. Readiness: did the model really execute, use historically safe data and pass evaluation? A manifest alone cannot answer all of this. Real-data and evaluation gates remain separate.

A directory named complete is not enough evidence. Empty fake files can exercise filename validation but cannot prove model inference succeeded.

## What goes into a manifest?

A manifest is a versioned inventory and description, not another model.

The target contract records NBA minutes × rate architecture, requested/available stats, feature/config identity, actual file hashes, parent identity for partial retrains, and source Git identity when available. Missing provenance stays explicitly unknown; we do not invent row counts or data watermarks.

A content hash fingerprints bytes. It detects changes relative to the recorded inventory, but it is not a signature or independent proof of trustworthy training. Someone able to rewrite both files and manifest can still change both; historical data/policy evidence must be checked separately.

Feature order matters: the same feature names in a different order can feed different inputs to a trained model. Identity must preserve order rather than sort lists merely to make hashes match.

## When is a run complete?

Completion means required saving and the existing execution/sanity checks actually succeeded.

The intended sequence is:

    Prepare incomplete run → execute existing training → save outputs
    → confirm required files and actual sanity success
    → write verified manifest → finalize without overwriting another run

Any required failure must leave an incomplete run rather than a success-shaped directory. We are not redefining calibration targets or profitability thresholds. Q10 behavior, per-model calibration and model math remain outside this mechanical identity change.

## Legacy model bundles

Older files need an honest compatibility label, not invented history.

Existing bundles may lack a manifest. Slice 2 must distinguish explicit legacy inspection/compatibility from verified manifest-bearing artifacts. We will not bulk-rewrite production artifacts or automatically claim old runs are historically certified.

The isolated implementation requires manifests for normal execution and provides `--allow-legacy-artifacts` for older complete file-only bundles. Invalid existing manifests still fail even with that flag. Legacy compatibility is labeled unverified and does not establish historical safety. The implementation is not accepted/integrated, so do not assume the flag exists in main.

## Calibration-only diagnostics

A report should not silently change the identity of an attached saved model.

The source trace confirmed calibration-only currently points its report writer at the attached model directory. The approved change separates that output under the selected output root and binds its diagnostic manifest to the source model identity. Diagnostic global/combined offsets are not authorization to apply them: global conformal offsets are prohibited by the project invariant. Replay's automatic combined-offset loading will be removed; embedded per-model calibration and copula behavior stay unchanged. This slice does not authorize a new recalibration experiment or changes to numerical calibration algorithms.

## How we verify without writing tests

We use existing suites plus bounded operational checks. No unit-test cases were created; Chase separately approved adapting four existing fixture setups after the stricter contract exposed them.

Disposable bundles can demonstrate content-hash mismatch detection, malformed manifests, missing files, incomplete-state rejection, collision safety and failure preservation. These checks run under Hermes scratch, without DB calls, real training or touching production artifacts. Existing training/replay suites must still pass, and independent review must compare slice 2 against the saved slice 1 baseline.

Passing these checks certifies artifact mechanics, not a trained model or a quote-clean betting result.

Coordinator rerun: 72 controlled checks passed after correcting a review finding: a partial retrain must record what was requested separately from every saved model. A pts-only retrain can have requested stats `["pts"]` while all three stat models remain available; a minutes-only retrain has no requested rate stats. Saved run configuration is independently checked for agreement. Independent final verdict is SPEC PASS / QUALITY APPROVED. The real Windows symlink probe was privilege-blocked and Linux rename remains unexecuted.

The four earlier fixture failures are resolved with owner approval: replay fixtures explicitly use legacy compatibility and nonempty controlled files; the full-training fixture supplies real JSON configuration and controlled save outputs. Existing assertions and test-case names were programmatically verified unchanged. All 117 existing scoped tests now pass. These mocks still do not certify actual model deserialization, training quality or historical correctness.

## Read next

Use the numbered map to keep coding slice 2 separate from the older historical-policy stage S2.

- [All eight coding slices](nba-training-evaluation-slices.md)
- [Approved slice 2 scope and validation contract](../operations/nba-artifact-slice2-2026-11-01.md)
- [Preflight boundary](nba-training-preflight.md)
- [Detailed coding plan — Batch B](../operations/nba-coding-readiness-plan-2026-11-01.md)
