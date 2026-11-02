# NBA training and evaluation — coding slices

## Kick off immediately

This is the plain-English map of the code we are completing while data sync is repaired.

- This chat: slices 1–2 are scoped-implementation verified in isolated `repair/nba-cli-preflight`; slice 2 passed independent spec/quality review, 72 controlled checks and all 117 existing scoped tests after owner-approved fixture adaptations. This lane is not committed/integrated.
- Other chat: slice 4's pure observation/quote primitives were separately owner-approved and merged into main as `f81170f` (source `b2fb86f`); no consumer wiring or historical policy activation. Sync recovery and S1 remain separate responsibilities; do not overlap their files.
- You: read the [artifact identity wiki](nba-artifact-identity.md) and [approved scope](../operations/nba-artifact-slice2-2026-11-01.md). Slice 2 approval does not authorize integration, training or later slices.

## Where we are

The command can now be checked safely; the next job is ensuring a saved model cannot masquerade as a complete, reproducible run.

Slice 1: independent specification/quality reviews passed, 20 CLI/safety checks passed, and the existing scoped suite is now **117 passed** after Chase approved adapting the three existing replay fixtures. No new test cases or weakened assertions were added. Changes remain in the isolated worktree, not main.

Cross-chat reconciliation: slice 4's [review record](../operations/nba-batch-d-observation-primitives.md) and main Git history confirm the separately approved merge `f81170f`. This covers pure primitives only, not historical repair stage S4, source-policy approval or consumer wiring. Slice 2's four incompatible existing fixtures were subsequently repaired with separate owner approval; no new cases or changed assertions, and its scoped suite is green.

## The eight coding slices

Each slice has a useful outcome; completing the code is different from validating it against real historical data.

| Slice | Purpose | What it gives us | Sync dependency / current status |
| --- | --- | --- | --- |
| 1 — CLI/preflight (Batch A) | Check a command before it starts work | Importable entrypoints, DB-free JSON dry-run, parameter/file checks, configurable training output root | Implemented and scoped checks green; not integrated |
| 2 — Artifact identity/completion (Batch B) | Know exactly which model was saved and whether saving really finished | Versioned manifest, file hashes, ordered feature/config identity, safe finalization and failed-run state | Scoped implementation verified: reviews, 72 controlled checks and 117 existing tests passed; integration/real-data acceptance remain separate |
| 3 — Flat-first replay correctness (Batch C) | Make simulated betting match the declared rules | Flat stake CLI, chosen-direction preservation, no zero-dollar bets, empirical-sample-only probabilities and effective configuration metadata | Coding/controlled verification before sync; coordinate harness overlap with S1 |
| 4 — Observation/quote primitives (Batch D) | Express what was known when a historical decision was made | Explicit timezone-aware cutoffs, quote pairing, provenance and rejection reasons | Implemented/controlled-input verified and separately merged into main; consumer wiring and historical policy still gated |
| 5 — Shared-input sweep harness (Batch E) | Compare settings without repeatedly generating different predictions | One prediction/sample input set, identity-bound caching, per-configuration outputs and raw/flat control | Controlled execution before sync; real comparisons after source/policy gates |
| 6 — Evidence audits (Batch F) | Detect misleading or incomplete result bundles | Count/identity checks, quote/dropout/timing/book/side diagnostics and reusable analyzer adapters | Artifact-only mechanics before sync; real-source measurements later |
| 7 — Lifecycle runner (Batch G) | Make the working commands one inspectable, resumable workflow | Strict YAML, command plans, stage status, evidence-aware resume and report-only decisions | After upstream command contracts exist; never success-returning stubs |
| 8 — Historical integration/validation (Batch H) | Prove the complete path works on the approved dataset and policy | Shared loader/replay semantics, real bounded feature/model/quote checks and a frozen baseline command | Needs stable sync, policy approval and separately scoped implementation |

These numbers map directly to Batches A-H in the [detailed coding plan](../operations/nba-coding-readiness-plan-2026-11-01.md).

## Why slice 2 is next

A directory existing does not prove it contains the intended, complete model.

Slice 1 checks basic file presence without deserializing models. Slice 2 strengthens the saved-run contract:

- Identify model architecture, supported stats, exact feature order, configuration and parent artifact for partial retrains.
- Hash actual files so later stages can detect changed evidence.
- Keep the run incomplete when required execution/sanity checks did not succeed.
- Distinguish legacy template compatibility from decision-grade evidence.
- Separate diagnostics from attached model outputs and review offset-consumption safety without silently changing model/calibration math.

This is useful before sync because manifest and failure-handling mechanics can be exercised using bounded filesystem/controlled execution checks. Real trained-artifact loading still needs its own acceptance evidence.

The [exact launch spec](../operations/nba-artifact-slice2-2026-11-01.md) has now been explicitly approved: one new helper with narrow training/replay/preflight integration, unchanged joblib serialization and no tests edited. Do not turn this into model promotion or a general artifact-storage migration.

## How the pieces connect

We are mirroring MLB's workflow, not its baseball models.

    Safe command → identified complete artifact → correct replay/sizing
    → explicit observation/quote rules → shared-input sweep
    → verified evidence → resumable report-only lifecycle
    → real-data acceptance → Chase-launched baseline

Some pure components can be prepared in parallel. There must still be one writer per source slice and no overlapping changes to S1 or sync-recovery files.

## Important naming distinction

Coding slice 2 is not the historical-policy stage named S2.

The older S1-S5 repair specification covers historical completeness, source policy, feature parity, quote-clean replay and baseline evaluation. This eight-slice map organizes the code needed to support that work. Passing coding slice 1 does not finish historical S1; building artifact slice 2 does not approve historical S2's decision-time/cohort policy.

## Read next

Use the wiki for understanding and the operations documents for exact scope/evidence.

- [Preflight wiki](nba-training-preflight.md)
- [Slice 1 evidence and decisions](../operations/nba-cli-preflight-slice1-2026-11-01.md)
- [Detailed coding plan](../operations/nba-coding-readiness-plan-2026-11-01.md)
- [Historical S1-S5 repair specification](../operations/nba-historical-repair-spec-2026-10-16.md)
