# NBA Paper-Betting Backend and Job Readiness Implementation Plan

> **For Hermes:** Use subagent-driven-development only after owner approval of an implementation slice. This document records work items and scopes repairs; it does not authorize code changes, migrations, job launches, commits or deployments.

**Goal:** Reliably populate the frontend, settle personally selected NBA bets, and make automatic NBA paper results a reproducible forward test.

**Architecture:** Repair the existing job chain and paper-trading entrypoints. Reuse a single NBA settlement policy across manual/automatic callers, preserve pending evidence when results are incomplete, and freeze automatic entries at placement. Keep operational readiness separate from model performance acceptance.

**Tech Stack:** Existing Python/SQLAlchemy/Postgres backend, Railway/APScheduler, workstation advanced producer, Next.js/Supabase frontend.

## Scope and ownership

- **Owner priority at wrap-up (November 7, 2026):** the two headline next-session work items are (1) the paper-betting backend repair slices and (2) production job recovery, including the existing Daily Stats prerequisite and JOB-01 through JOB-04. Coordinate independent work without duplicating active owners.
- This is the **paper-betting backend**, not the historical backtest harness. The active `.hermes/plans/nba-harness-mvp-repair.md` and dirty `src/backtesting/*` / historical-source changes belong to the adjacent lane and are protected.
- NBA-first. Preserve MLB behavior and prevent NBA settlement from claiming MLB rows; no MLB model or doubleheader-certification expansion in this slice.
- Frontend work is in commit `914e2b9a`. Its plan is [frontend-paper-betting-2026-11-07.md](frontend-paper-betting-2026-11-07.md).
- Four job work items below are the four non-Daily-Stats concerns from the preceding status report. Existing **Daily Stats failures remain an explicit prerequisite**, not an omitted or newly duplicated fifth repair assignment.
- No Kanban cards were created. These are reviewable work items, not a dispatched execution queue.

## Evidence freshness

Inspection: November 7, 2026, approximately 13:45 ET. Active Railway deployment `8e6ca5f1-0f8d-4559-8d9b-a4d5837e7bad`, commit `914e2b9a`, started at 13:43 ET. Registration is verified; a successful full cycle under that deployment is not.

Recent failure evidence came from the preceding deployments: props linkage failures through 13:39 ET; edge-refresh no-ops at 13:17 and 13:36 ET; noon inference blocked because the advanced producer covered November 5 while the source target was November 6. Treat these as the latest observed symptoms, not proof the new deployment has reproduced or repaired each one.

The prior bounded remote audit covered November 1–7: 25 automatic bets, zero manual paper bets in that window. Automatic placement/settlement exists; zero manual rows and successful empty resolver runs do not prove the manual lifecycle.

## Four recorded job work items

### JOB-01 — Verify schedule publication and frontend retrieval

**Status:** OPEN / verification gap, not a proven ingestion outage.

- Producer: `src/orchestration/nba_schedule_job.py`, registered by `scheduler.py:588`; schedule refresh enabled at 09:45, 12:05 and 16:05 ET.
- Consumer: `dashboard/src/app/api/games/route.ts` independently fetches NBA CDN schedule. Persisted schedule health alone does not prove this HTTP path works.
- Bounded next action: inspect the next actual schedule-job result and target-date `nba_game_schedule` rows via isolated read-only SQL; request the deployed games endpoint from an authenticated session. Compare the exact date/game identities, source freshness, postponed status and timezone semantics.
- Repair only the reproduced failure. Decide on a persisted-schedule fallback only if direct CDN access actually fails in the deployed frontend; do not build another scraper by default.
- **Done:** real target-date games reach the frontend even without predictions; upstream failure stays unavailable, not falsely empty. A genuinely empty date remains empty.

### JOB-02 — Restore props scrape/link completion

**Status:** OPEN / recent repeated failures, post-deployment result unverified.

- Files: `src/orchestration/lines_job.py`, `src/processing/nba_linker_local.py`, `src/scrapers/daily_player_props_scraper.py`, `src/orchestration/scheduler.py`.
- A recent failure explicitly reported another `nba_inference` linker invocation holding the durable checkpoint lock. Other failures reached the same linker step; do not assume every failure has that cause.
- Bounded next action: capture the complete first failed-child diagnostic on the active deployment, identify the lock holder/checkpoint and overlapping producer, and examine capped exact current-slate linked records. No giant raw-table scans.
- Preserve shared scheduler serialization and DB lock protection. Do not kill active writers, bypass locks, reset checkpoints or launch competing jobs. Retry only a verified transient contention class.
- **Done:** deployed scrape/link chain completes with coherent current-game/player quotes in `nba_player_props_inference`; a subsequent scheduled invocation also succeeds without duplicate or wrong-date links.

### JOB-03 — Restore advanced-input readiness and inference

**Status:** OPEN / latest observed NBA inference was dependency-blocked.

- Files: `src/orchestration/nba_advanced_stats_job.py`, `scripts/run_advanced_scraper.bat`, `src/orchestration/scheduler.py:214`, `src/orchestration/inference_job.py`.
- Prerequisite: the separately owned Daily Stats repair must establish current basic source/derived data. Advanced ingestion stays workstation-only; Railway must never call `stats.nba.com`.
- Verify the existing local producer's execution/scheduling ownership and current source target before proposing a new automation mechanism. Chase launches any catch-up job.
- Verify both the successful `job_executions` through-date receipt and actual required advanced pairs. Do not bypass `check_nba_advanced_readiness` merely to obtain predictions.
- Then exercise the existing inference route under explicit no-bet/no-notification approval and verify date/game/stat prediction **and sample** outputs, including valid lines. Zero outputs for a real slate are not acceptance.
- **Done:** current advanced inputs and derived consumers pass, the deployed scheduled inference produces populated valid current-slate predictions/samples, and the frontend can display bettable props.

### JOB-04 — Make edge-refresh readiness meaningful

**Status:** OPEN / recent runs completed as no-ops due to missing samples; dependent on JOB-03.

- Files: `src/orchestration/edge_refresh_job.py`, `src/orchestration/scheduler.py`.
- Keep the scheduled `--skip-paper` behavior: the approved entry policy retains current placement windows. Policy approval does not enable paper placement in the frequent refresh loop.
- After JOB-02/03, verify a scheduled refresh consumes current samples and quote pairs and persists fresh probabilities/edges. Distinguish an expected pre-inference/no-slate no-op from blocked refresh after expected inference.
- If needed, report `blocked/no_samples` separately from productive success; do not invent a new heartbeat system or make every empty-slate run fail.
- **Done:** fresh edge outputs for supported slate records, a visible reason when blocked, and no accidental automatic placement from the frequent refresh loop.

## Backend repair slices

### Slice A — Consistent manual NBA settlement (first delivery)

**Likely files:**
- `src/paper_trading/user_bet_resolver.py`
- `src/orchestration/resolve_user_paper_bets.py`
- `src/orchestration/daily_stats_job.py` (only settlement call/summary)
- `src/orchestration/scheduler.py` (only settlement registration/dependency)
- Proposed, only if extraction is useful: `src/paper_trading/nba_settlement.py` for shared pure outcome rules and bounded lookup logic.

1. Define one NBA contract: played/final known actual -> won/lost/push; explicitly evidenced nonparticipation -> cancelled with zero P&L; absent/incomplete/ambiguous result -> retain pending with an actionable reason. Unknown is not DNP.
2. Identify the exact target game before grading. Prefer explicit bet/prediction game identity; legacy player/date fallback is allowed only for one unique matching fixture. Missing IDs, null player, conflicting matches or missing result components stay pending. Never infer a loss or cancellation from a lookup miss.
3. Remove competing NBA policy ownership: have both existing entrypoints delegate to the same idempotent resolver, or make the scheduled script a compatibility wrapper and remove duplicate scheduling. Prefer one scheduled owner plus safe compatibility callers, not two divergent implementations.
4. Scope NBA rows explicitly by supported stat family before conversion/lookup; the general NBA resolver must not cancel unsupported/MLB or unlinked manually entered rows. Preserve the existing MLB path without certifying it.
5. Resolve from final result evidence with per-game completeness, not just any `team_game_stats` row on the date. A failed or partial ingestion run must not settle missing games. An uncertain postponed/cancelled game stays pending absent explicit authoritative disposition.
6. Guard settlement updates with `status='pending'` and unchanged captured identity/line/side/odds/stake; record actual, P&L and `resolved_at` together. Respect manual user edits; don't overwrite a concurrently resolved row.
7. Keep next-morning timing, but decouple settlement safety from fixed ordering: retries use the same per-game checks. Log resolved/pending/blocked/failed counts honestly; wrapper success cannot hide a real resolver exception.

**Acceptance:** existing normal-result arithmetic passes; explicit DNP is consistently voided; missing/partial results remain pending; unsupported or ambiguous rows are untouched; repeat/concurrent invocation cannot double-settle. An owner-authorized personal paper bet appears pending and later shows its exact actual/result/P&L in Performance -> History.

### Slice B — Automatic entry and settlement integrity

**Likely files:** `src/paper_trading/paper_trader.py`, shared NBA settlement helper if added, `src/orchestration/inference_job.py`, `src/orchestration/edge_refresh_job.py`, `src/models/daily_runner.py`, `src/models/prediction_store.py`; additive migration only after schema preflight and approval.

1. Reuse Slice A settlement semantics for `paper_bets`; never cancel missing results merely because another game on that date has stats.
2. Stop rewriting previously accepted line/odds/stake/probability/placed_at. Count real inserted rows, not attempted list length. Duplicate and opposite-side policy is Decision 1 below; do not silently implement a new selection strategy.
3. Replace the old raw-table fail-open start lookup with the current authoritative schedule/quote path. Unknown, stale, conflicting or started games are ineligible. Recheck eligibility at the placement boundary, not only initial selection.
4. Preserve selected coherent quote identity from the actual reader: book, game, selected snapshot, quote availability timestamps and decision time. Use empirical probabilities; do not change edge/calibration formulas.
5. Persist immutable artifact/evaluation identity for new paper entries. Owner-approved scope: minimal additive game/book/quote-time/model-artifact/evaluation-run fields or equivalent small snapshot payload, with exact schema chosen after inspecting the actual producer metadata and bounded DB preflight. Never fabricate provenance for old rows or infer artifact identity from a mutable prediction FK. Applying the migration remains separately gated.
6. Pin a new forward evaluation period/config. Preserve previous history as legacy evidence. Do not automatically correct or delete settled historical records.
7. Use fixed $10 units for new automatic paper entries in the evaluation period (approved Decision 2). Kelly may remain a separately labeled diagnostic only; it must not size these entries. Preserve manual bet amounts and historical stakes. Flat-unit reporting does not substitute for correct settlement and frozen entries.
8. Recompute summaries from the same settled ledger. If an older date is deliberately settled later, update affected cumulative totals instead of leaving later bankroll entries inconsistent. Keep pushes/cancellations out of risked-stake denominators according to the chosen accounting contract.

**Acceptance:** rerun leaves original entry terms unchanged; missing start/quote evidence rejects placement; selected game/quote/artifact can be reconstructed; pending results survive partial data; actual inserted/settled counts agree with fresh readback and dashboard summaries.

## Owner-approved decisions

Chase approved Decisions 1–3 on November 7, 2026. This settles the policies below; the request was to update the plan, not to launch implementation, apply migrations or run jobs.

### Decision 1 — Automatic entry timing and duplicates

**APPROVED:** first eligible pregame recommendation per player/game/stat in the evaluation run wins. Freeze it; later refreshes do not replace it or add the opposite side. Keep current scheduled placement windows for the first repair.

**Not selected:** dedicated T-60 entry scheduling. T-60 model-input selection does not change this entry policy. Current morning/noon placement and afternoon `--skip-bets` behavior remain. No new numeric quote-freshness TTL was approved; surface a separate decision only if implementation requires one beyond existing validated eligibility rules.

### Decision 2 — Automatic stakes for the new evaluation period

**APPROVED:** fixed $10 paper units, with existing Kelly sizing available only as a separately labeled diagnostic. This makes the forward selection/result assessment easier to interpret without confounding stake changes.

**Not selected:** continuing fractional-Kelly sizing for the new automatic evaluation period. This is not live-money approval and does not change manually selected bet amounts or historical entries.

### Decision 3 — Strict-readiness metadata/schema scope

**APPROVED:** include the minimal additive provenance/evaluation fields in Slice B, after a bounded schema/row-count preflight. Retain the model/artifact, game, quote and evaluation-run identity needed to reconstruct each new entry. Do not backfill invented provenance into legacy history. Exact DDL follows preflight; migration execution remains a separate approval, as do implementation and deployment.

**Not reopened:** source-clock guessing, structural-zero policy, model architecture, global calibration offsets, live-money promotion, historical rescraping or backtest policy. No automatic repair of legacy cancellations; any later correction is its own evidenced, approved batch.

## Verification and approval gates

1. Recheck current commits/deployment and overlapping owners before editing. Protect the adjacent backtest harness lane.
2. Existing offline suite: `tests/test_paper_trader.py` (25 tests passed in the preceding audit). No new unit tests unless separately approved. Run through the existing offline guard under the GameFlow implementation skill, blocking DB/network at the driver boundary.
3. Reuse the controlled resolver probes from the prior audit (scratch `paper_readiness_probe.py`) as regression inputs, changing expected missing-result behavior only when the repair intentionally changes that contract. Add only short controlled checks for DNP, pending, failed lookup, duplicate and concurrent precondition cases. No expensive new scratch framework.
4. Scoped Ruff/compile and exact source-diff review. No changes to tests merely to hide a failure; request permission if an old fixture conflicts with the intended safety contract.
5. Isolated SQL runner: capped schema/index/column and chosen-current-day reads, verified READ ONLY + 15-second timeout. No giant raw odds scans. Migration preparation is not permission to execute it.
6. Owner-approved tiny real smoke: one personal paper entry and a separately scoped automatic candidate, read back exact original terms, rerun for no-op proof, then inspect final known results through Performance/History. Do not invent a settled production record or use a synthetic DB response as live proof.
7. Production gate: deploy only when authorized; verify active commit and exact job outputs, not merely scheduled registration or wrapper exit status. Broad jobs and training remain Chase-launched.

## Proposed order and stopping rule

- First: Slice A manual settlement; run job investigations independently only where no competing owner exists.
- Next: implement Slice B using approved Decisions 1–3 when the implementation slice is authorized; do not reopen these settled policy choices.
- Final: accept the real data chain JOB-01 through JOB-04 plus the separately owned Daily Stats repair; prove one placement-to-resolution cycle.
- Stop after scoped compatibility checks, short failure probes and the agreed real smoke. Do not expand this into a new trading framework, broad historical audit or model experiment.

**Status:** Work items recorded; backend plan scoped; Decisions 1–3 owner-approved. Implementation, migration execution, job launches and deployment remain separately gated. This update changes the plan only.
