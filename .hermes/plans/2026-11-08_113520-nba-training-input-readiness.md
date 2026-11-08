# NBA Training-Input Readiness Implementation Plan

> **For Hermes:** Use subagent-driven-development for approved substantial implementation, following the GameFlow implementation-worker lane. This document is a proposed game plan, not authorization to run jobs or mutate databases.

**Goal:** Establish that the actual NBA training matrices are usable, temporally safe and explicit about missingness, then hand Chase a verified first-baseline command. Separate training-input acceptance from model/holdout/promotion acceptance.

**Architecture:** Reuse the existing ordinary training path and repaired injury/quote consumers. Add only a small prepare-only seam if no existing public path can safely stop before model fitting. Use one matrix build and one consolidated readiness report, not a new auditing framework.

**Tech stack:** Existing Python training/feature pipeline, local PostgreSQL through the isolated SQL-runner pattern, pytest, existing JSON/Markdown receipts. No new dependencies proposed.

## Evidence and authority

Canonical remote markdown was read through Tailscale SSH; GBrain MCP listing/tag tools were unavailable. Hard Facts, Critical Invariants, relevant atomic lessons, and handoffs 146/149 were retrieved. Hard Facts entries retain their pending-owner-validation status; active invariants remain binding. Local November 7 post-replay evidence supersedes the handoff's interrupted schedule snapshot.

Primary files:
- `docs/operations/nba-training-input-acceptance-2026-11-07.md` — current post-replay evidence.
- `docs/operations/nba-training-consumer-repair-2026-11-05.md` — ordinary training versus strict replay distinction; historical zero-coverage findings are superseded.
- `docs/operations/nba-training-readiness-reconciliation-2026-11-03.md` — owner decisions; later handoffs override obsolete linkage/source findings.
- Canonical `Handoffs/handoff-146.md` and `Handoffs/handoff-149.md`.

Recorded, not remeasured in this planning turn:
- Eligible fit: 46,249 rows / 2,452 games; calibration: 23,898 rows / 1,278 games; initial holdout through November 4: 1,962 rows / 104 games.
- Valid schedule starts for all 3,834 eligible games.
- Accepted injury evidence at game level: 65.12% for 22023, 75.35% for 22024, 28.17% for 22025, 0% for the initial 22026 holdout.
- The 3,325 repaired calibration observation keys matched the public injury consumer; this does not measure final player-feature nonmissing rates or representative coverage.
- Holdout: 626 existing rows lack game IDs and retained source observations; three lack player IDs. Inspected rows begin October 27. Fifteen cached report URLs were 403 failures.
- Props parity and harness repairs are completed evidence, not tasks to restart.

## What "ready" means

1. **Inputs ready:** actual fit/calibration matrices build through the real path; expected identities/features/chronology hold; missingness and exclusions are accounted for; residual limitations have an explicit disposition.
2. **First baseline complete:** owner-launched fitting, calibration, artifact save/reload and prediction checks pass. This cannot be guaranteed by a preflight.
3. **Evaluation ready:** a fixed artifact can be evaluated on accepted holdout inputs and executable historical quotes without tuning on those outcomes.
4. **Promotion ready:** separate later decision based on independent evaluation and paper evidence. This plan does not grant it.

There is no universal injury coverage percentage that proves representativeness. Full coverage is not required for exploratory training, and a complete-case injury-only population is not the default remedy.

## Step 1 — Reuse the actual training path, with a pre-fit stop

Inspect the current training CLI, its full/partial orchestrator and `src/models/features/nba/training_loader.py`, plus `src/models/features/nba/injury_context.py`. Resolve actual public symbols and parameter-artifact consumption before proposing code or commands. Do not assume a `--loader-only` flag exists: the recorded `--dry-run` is configuration-only.

Preserve settled scope: regular-season PTS/REB/AST, fit 22023/22024, calibrate 22025, initial evaluation 22026, fixed incumbent parameters first, established target/rolling thresholds and trade/position semantics. Do not reopen deferred cold-start design or structural-zero policy. Record exact current cohort behavior rather than inventing new exclusions.

If needed, propose the smallest prepare-only seam that invokes the same loader and preparation code as training, stops before selector fitting/tuning/model fitting, and emits actual output counts and feature diagnostics. No duplicate feature builder. Protect DB/network/model-fit boundaries in focused tests. Exact code/test allowlist must follow source discovery; paths not inspected in this planning turn are not frozen implementation targets.

Deliverable: verified preflight invocation and explicit local DB/resource/output scope. A configuration preview alone is not acceptance.

## Step 2 — Build once and audit the actual matrices

First exercise a deterministic bounded sample through the entire preparation path: each season, early/late dates, injury-positive and unknown cases, and existing edge-case cohorts. If this fails, repair only reproduced defects and rerun the same sample.

Then Chase launches the full fit/calibration prepare-only build. Probe RAM/disk and DB conditions first, agree resource limits, and avoid overlapping writers. Use a read-only source connection with verified transaction/timeouts via the isolated runner. Output-file writes must be explicit. Do not accidentally launch fitting as a smoke test.

Collect in one report:
- Input/output/excluded row and game counts by season/stat; exact duplicate/invalid identity checks and exclusion reasons.
- Feature names/order/dtypes at the actual model boundary; target validity and infinite values; distinguish legal NaN from invalid numeric values.
- Per-feature null/default/zero/distinct counts by season, with known structural zeros and deliberately unavailable injury aggregates classified rather than failed indiscriminately.
- Per-player feature availability, not only games with at least one injury report.
- Actual split membership/date ranges, no fit/calibration overlap, and existing chronological inner-fold guards retained.
- Source/cutoff checks through current consumers; do not require strict historical sidecar/complete-edition evidence for ordinary experiments by assumption.
- Injury NaN survives preparation; no missing-to-healthy coercion. Other established imputation stays unchanged.
- Seed/config/code identity and effective incumbent hyperparameters for reproducibility; don't confuse omitting tuning with loading incumbent settings.

PASS: full requested fit/calibration matrices materialize with reconciled counts and no unexplained identity, chronology, schema, or preparation failure. An intentionally all-unknown feature is a declared limitation, not usable historical signal.

## Step 3 — Diagnose missingness; repair only consequential defects

Reuse the Step 2 output rather than rerunning source scans. Cross-tab observed/unknown injury features by season, month, team, position, available pregame role/history bands, and source/rejection reason. Distinguish: no retained source, unsupported clock, after-cutoff report, identity/link failure, listed-player omission, incomplete aggregate semantics.

Use pregame descriptors for the coverage assessment. Do not inspect holdout outcomes to choose recovery scope, imputation or model settings. Thin subgroups remain inconclusive; no arbitrary representativeness certificate.

Disposition per issue:
- **Repair before launch:** leakage, wrong game/player identity, unintended row loss, NaN coerced to healthy, or a supposedly usable required source being absent due to a consumer defect.
- **Accept explicitly for exploration:** documented unknowns tolerated by the ordinary path, with quantified season/cohort gaps.
- **Conditional targeted recovery:** authentic retained evidence exists and fixing the bounded gap materially changes coverage. Obtain DB/source authorization separately.
- **Separate experiment if needed:** a predeclared injury-family-excluded challenger on development data; not silently dropping injured/unknown rows and not a mandatory new ablation matrix before the first baseline.

PASS: every material missingness pattern has a documented explanation or bounded unresolved limitation and an explicit launch disposition. The observed 28.17% calibration game coverage is a distribution-shift warning to investigate, not an automatic fail or proof of safety.

## Step 4 — Repair holdout source coverage in parallel where safe

Independently inspect authentic historical sources for October 20–November 4, 2026. Existing 403-only cache cannot support replay. Begin with a small source-availability check; no broad rescrape or provider-clock guessing. Retain source report identity/publication evidence, recover only genuinely supported statuses, resolve exact game/player links, then approve bounded writes and mirror.

Repeat exact public-consumer eligibility/readback and player-feature coverage after repair. Preserve all retained evidence and unknown omissions. If historical recovery is unavailable, report it honestly: proceed with a labeled missingness-stress diagnostic or collect a later fixed-policy evaluation window, subject to owner decision. Do not quietly redefine the holdout after viewing results.

This is an evaluation gate, not automatically a blocker to fitting the first baseline. Source repair may proceed alongside matrix inspection if resource/write ownership allows; DB writers stay coordinated/sequential.

## Step 5 — Issue one launch verdict, then train

Consolidate results into the existing acceptance report and link this plan from the active restart record during approved execution, not during this planning-only turn.

Launch packet:
- Input acceptance: PASS / conditional exploratory PASS / BLOCKED, with concrete reasons.
- Exact seasons/date boundaries, stat scope, parameter artifact, effective parameters, cohort/exclusion/missingness policy, code state and output location.
- Verified PowerShell-safe command using the actual current CLI; configuration-only dry-run labeled correctly.
- Known limitations and separate holdout status.

Chase launches training. Afterward verify the exact artifact's metadata, reload through the actual predictor, nonempty finite outputs, quantile order and empirical sample probabilities. Only then evaluate frozen settings on accepted holdout inputs with corrected flat-stake metrics and quote/dropout accounting. No automatic tuning, promotion or deployment.

## Prior lessons/invariants applied

- Cheap baseline before architecture: one acceptance pass and one controlled baseline; no redesign or new model family.
- Feature selector is not an ablation; correlated families require controlled group comparisons if their value becomes a question. Selector drops cannot certify injury usefulness or harmlessness.
- Temporal integrity is end-to-end; current source counts alone do not prove safe feature values.
- Preserve empirical CDF, Q10 edge and no global conformal offsets.
- Missing evidence is unknown, not healthy. Listed-player evidence is not complete-team proof.
- Isolated SQL, owner-launched long runs/backfills, and scoped approval before DB mutation.

## Effort and stop rules

Effort estimate only: initial source trace/pre-fit seam and bounded verification are a small engineering slice, roughly half to one working day if no new consumer defect surfaces. Full-matrix runtime and memory need a measured pilot; recovery time depends on source availability and is not estimated from 403 attempts. Training runtime remains separate.

Stop adding gates once the real matrix works and material gaps have dispositions. Do not require uniform missingness, complete injury history, new cold-start policy, another props parity project, strict replay certification, or model profitability to call the first exploratory training inputs ready.

Planning result: no source edits, SQL, external data acquisition, model loads/fits, backfill, commit, push or deployment. Only this plan was written.
