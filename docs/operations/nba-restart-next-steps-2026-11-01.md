# NBA restart — next steps and parallel work, November 1, 2026

## Plain-English conclusion

Fixing the sync gives us a stable local mirror to inspect; it does not make training or backtests trustworthy automatically. The immediate follow-up is S1 real-data acceptance and S2 historical-source evidence. While the other chat repairs the sync, the most useful parallel work is a documentation-only S2 decision packet and a bounded post-sync validation specification.

## Confirmed chat decisions and boundaries

- Chase is handling the failed NBA sync in another chat. This chat must not restart, repair, or compete with that sync.
- Chase needs Markdown summaries of chat decisions and next steps, not decisions left only in conversation.
- The NBA core-product restart remains the active lane; Engineering OS remains paused.
- Existing S1 work stays isolated pending acceptance and explicit integration approval. Do not relaunch S1 or edit its source files concurrently.
- This note records sequencing recommendations, not approval for S2 policies, S3-S5 implementation, DB mutation, training, integration, or deployment.

## What happens after the sync is fixed

### 1. Establish a usable mirror and a bounded validation scope

Purpose: ensure that validation measures a known, stable dataset rather than tables still being replaced.

- Obtain the other chat's final journal/completion summary, including the failed-table repair and any remaining exceptions.
- Confirm required tables are no longer being rewritten and identify the actual seasons/date ranges refreshed.
- Reconcile required-table success, key/schema availability and bounded source freshness. Process exit success alone is not dataset certification.
- If partial coverage remains, declare it explicitly; do not silently expand the intended cohort or relaunch a broad sync.

### 2. Complete S1's real-data/artifact acceptance

Purpose: prove that the reviewed fail-closed code works through the actual local feature/artifact path.

- Preserve `repair/nba-s1-fail-closed` in its isolated worktree.
- Use a declared small historical window and an exact existing artifact through an explicitly local, read-only, timeout-bounded connection.
- Check actual feature assembly, ordered artifact compatibility and load/predict behavior.
- Keep controlled failure-injection results separate from real-data results and from model-quality evidence.
- Report acceptance or a concrete blocker; integration remains a separate owner decision.

### 3. Finish S2 source evidence and request policy decisions

Purpose: determine what information was genuinely available when historical predictions would have been made.

- Use isolated read-only SQL runners with index-aware, keyed, bounded samples.
- Cover representative opening/middle/end regular-season windows; classify playoffs/play-in separately.
- Reconcile quote observation/update clocks, source provenance, compatible Over/Under identity, duplicate conflicts and historical commence mapping.
- Establish what retained injury report editions actually support; do not interpret a report-cycle label or first insertion as proven historical availability.
- Check required feature coverage and non-default variation through actual feature assembly, not row existence alone.
- Present a versioned source/field/policy matrix for owner approval: decision time, quote eligibility/staleness, missing injury/commence behavior, cohort and train/calibration/protected-OOS boundaries.

S1 acceptance and S2 evidence can proceed in parallel after the mirror gate, with separate ownership and bounded read load. Neither requires integrating S1 first. A successful sync does not approve an implicit decision time or missing-data fallback.

### 4. Approve and implement S3 historical feature parity/provenance

Purpose: make training and historical replay use the same approved information contract.

- Only after S2 evidence and policy approval, scope the shared decision/quote/injury rules and feature defaults.
- Verify ordered membership, values, spread direction, missingness, season policy and provenance across historical loaders.
- Treat any advanced rolling-producer repair and derived-table rebuild as a separate approval, not an automatic part of S3.

### 5. Approve and implement S4 quote-clean replay

Purpose: ensure both direct and prefetched replay select the same eligible quotes and report exclusions honestly.

- Reuse the approved S3 policy, with pre-decision and strictly pre-commence checks and coherent paired outcomes.
- Validate direct/prefetch equality on a bounded real window and retain rejection/dropout and exact artifact/policy metadata.
- Do not treat absent quotes as evidence of absent model edge.

### 6. Prepare S5; Chase launches the meaningful baseline

Purpose: train only after repaired data/contracts make the run meaningful and evaluation is trustworthy.

- Freeze actual cohort, training/calibration/protected-OOS windows and artifact identity.
- Prepare real supported commands; do not invent a flat-stake CLI flag. Scope any missing evaluation interface separately.
- Start evaluation raw/no-BL and flat-stake, with risk, volume, timing and dropout evidence. BL/Kelly are not the initial gate.
- Chase launches long training. Historical acceptance is not customer-pick publication approval.

## Work that does not require the sync

### Recommended first: S2 decision packet and post-sync acceptance checklist

Documentation-only; no data certification or policy implementation.

- Convert known source-clock findings into a short list of decisions, candidate approaches, tradeoffs and evidence still needed.
- Define the exact audit questions and desired outputs now; finalize actual keyed query inputs after inspecting the stable mirror's schema/indexes.
- Spell out pass/block/unknown conditions for S1 real-data acceptance and S2 evidence.
- Do not choose policy based on which variant has the highest retrospective ROI, or repeat already-completed source-clock research without a new question.

Suggested deliverable: `nba-s2-policy-decision-packet-2026-11-01.md` in this directory. This is a proposed output, not a file already produced or an approved implementation package.

### Other independent preparation

- Model-template inventory: document existing feature order, minutes/rate configuration, loader/output contracts and command interfaces from files. No fitting, DB reads or old-artifact promotion claims.
- Frontend inventory: identify required NBA MVP routes, stale/error/empty-state contracts and unsupported surfaces to hide. Actual data integration and signed-in acceptance remain later work; do not present fixtures as real picks.
- Publication-lane preparation: assemble the outstanding Layer 2 acceptance checklist and Layer 3 roster-source/schema questions. Schedule and roster source work still has separate approval boundaries; do not apply migrations or deploy.

These are independent preparation choices, not instructions to start all lanes. Prefer finishing one small decision packet over opening several implementation workstreams.

## What remains blocked while sync is unresolved

- S1 real historical-data/artifact acceptance against the changing mirror.
- S2 refreshed local coverage and actual feature-assembly certification.
- Evidence-backed final cohort/split and source-policy signoff where coverage is still unknown.
- Behavior-changing S3/S4 dispatch and S5 full training.

Read-only source/document review is not blocked. Existing source-clock conclusions need not be rediscovered from scratch.

## Relevant prior lessons and invariants

- `operations/hard-facts` and `operations/critical-invariants` were retrieved. Hard Facts review labels remain pending; old infrastructure descriptions are not used as current runtime proof.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: verify the consumer query and artifact path, not just timestamp columns. Apply the general lesson, not dated MLB readiness claims.
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: inspect schema/indexes, bound inputs and treat timeout as unknown/audit failure rather than missing data.
- Preserve empirical CDF probabilities, Q10 behavior, quantile ordering and CDN-only Railway stats. No global conformal offsets.
- No new unit tests under the current restart instruction; use existing checks and bounded real-path verification.
- Long jobs remain owner-launched. SQL stays isolated. DB writes, backfills, integration, push, deployment and promotion remain separately gated.

## Sources and evidence limits

Read in full: [restart plan](nba-core-product-restart-plan.md) and [historical repair specification](nba-historical-repair-spec-2026-10-16.md). Also reviewed: [S1 coordinator review](nba-s1-coordinator-review-2026-10-16.md), [S2 source-clock review](nba-s2-source-clock-review-2026-10-16.md), [watch status](nba-s2-watch-status-2026-10-16.md), and [blocked return summary](nba-s2-return-summary-2026-10-16.md).

The October evidence records the failed sync and prior checks; this chat has not independently verified the other chat's current sync repair, rerun S1, queried a database or certified historical readiness.
