# Railway daily NBA advanced stats

Status: deployed and native-verified. Runtime implementation committed/pushed as ec58708b; NBA_ADVANCED_STATS_ENABLED=true verified. Railway deployment 7e086120-20c4-4119-8a24-2b1567bc38e0 SUCCESS, exact commit verified in container. Native scheduler.run_job returned true/exit 0 at 2026-11-08T17:37:59Z; source already complete (0 provider requests), strict derived chain succeeded. Local main fast-forwarded with unrelated dirty changes preserved. Protected AGENTS.md update was not applied because its separate tool approval prompt timed out.

## Evidence
- One-time native Railway trial on 2026-11-08 fetched three games and independently verified persisted team/player completeness. This is source reachability evidence, not recurring reliability.
- Temporary wrapper had an import-path error and a false-negative post-save predicate override. Do not reuse that wrapper.
- Existing scheduler runs CDN/basic Daily Stats at 09:00 America/New_York with a 09:30 retry.
- Existing nba_advanced_stats_job.py runs CDN source -> bounded current-season advanced source -> strict derived Daily Stats with --skip-resolution --skip-linker -> coverage/readiness record. It currently blocks Railway.
- Inference requires successful nba_advanced_stats_job.py metrics through the latest completed source date plus a current raw completeness check.
- Scheduler has a pre-existing unrelated timeout edit (nba_schedule_job 180 -> 300 seconds). Preserve, do not publish it incidentally. Other dirty source/docs belong to concurrent lanes.

## Minimal implementation scope
- src/orchestration/nba_advanced_stats_job.py: explicit opt-in Railway mode while preserving the local CLI/default safeguards; bounded child runtime; shared database advisory exclusion for local/cloud invocations; retain strict derived chain and readiness metrics. Support skipping the redundant initial basic ingestion only when invoked after a successful daily basic run.
- src/orchestration/scheduler.py: gated advanced follow-up after successful 09:00 basic job and the 09:30 recovery path; never run advanced concurrently with that basic chain. Check current-run outcome, not yesterday's in-memory success. Preserve duplicate suppression and inference readiness gate. No separate 12:15 recurring timer.
- src/scrapers/nba_unified_scraper.py only if needed for a strict total request budget at the HTTP boundary. Preserve original exact-game post-save completeness predicate; no frozen-selection monkeypatch.
- AGENTS.md: record the owner's separately authorized recurring cloud lane while preserving daily_stats_job --cdn-only and no automatic betting/model promotion.
- This plan: validation/deployment receipt and backfill handoff.

## Runtime contract
- Daily cloud advanced source: current season, missing games only; proposed cap 30 games / 60 HTTP requests, 30-second request timeout, at most 2 attempts per game; bounded total job execution and a separate bounded derived-stage budget.
- Source blocking/rate-limit/timeout or incomplete outputs fail visibly; do not substitute a green CDN result for advanced success.
- Do not enable inference or record advanced readiness until raw and derived checks pass.
- Keep local producer available for owner-launched catch-up. Shared lock must prevent cloud/local wrappers overlapping; do not change or disable Windows scheduled tasks without separate permission.
- No bets, model changes, DDL, broad backfill launch, unrelated source edits, or temporary-container scheduler hack.

## Validation and stop rule
1. Inspect existing affected tests and run relevant checks with real DB/network access blocked; no new unit-test files without approval.
2. Exercise CLI help/dry-run and controlled success, incomplete-source and busy-lock paths; verify original per-game predicate remains live.
3. Review only scoped changes against baseline; preserve concurrent edits.
4. With publication permission, publish only the reviewed lane, deploy, and read back active Railway commit plus enabled schedule/chain logs.
5. Run one separately bounded native acceptance through the real entrypoint (not another temporary scraper implementation), then independent exact-scope DB readback. Do not execute the owner's full backfill.
6. Report deployment, first native chain, backlog completeness and next scheduled run as separate gates. Do not call future recurring reliability proven.

## Verified owner command available now
From the GameFlowData root, PowerShell:

.\venv\Scripts\python.exe -X utf8 src\orchestration\nba_advanced_stats_job.py --advanced-limit 120 --request-timeout 30 --max-retries 2 --http-client curl --no-proxy

The same invocation with --dry-run passed on 2026-11-08. It writes canonical DATABASE_URL, fills up to 120 current-season missing games that already have basic source rows, then runs strict derived catch-up and checks readiness. It does not imply all older seasons are repaired and does not sync the local mirror. If more than 120 games are missing, another bounded invocation may be needed. Updated local/cloud wrappers now share the overlap guard; old copies and direct low-level scraper commands do not. Chase's completed backfill is not being rerun.

## Evidence after implementation
- Actual CLI dry-run and offline real-entrypoint checks passed: ordering, today's retry status, overlap, cloud gate/caps, source failure, raw incompleteness, shared-lock refusal, curl/requests HTTP budgets and native child exit handling. Scoped Ruff and diff checks passed. No new unit-test files.
- Current-season canonical preflight: through 2026-11-07, zero missing advanced player/team pairs; successful local full-chain receipt ended 2026-11-08T17:24:47Z. User's completed backfill was not relaunched.
- This lane published only the advanced job, scheduler, unified scraper and docs/operations/nba-advanced-railway-daily.md. A later concurrent owner publication 963b426d included the separately owned scheduler timeout and other lanes; preserve that publication.
- Fresh persisted native readback at 17:39:58Z: producer_host=railway, through_date=2026-11-07, missing_advanced_pairs=0; check_nba_advanced_readiness returned true. Parent independently read receipt.json and db_readiness.json under /tmp/gameflow-advanced-daily-acceptance-20261108 in the active Railway container.
- Scheduled recurrence on November 9 remains future evidence; daily wiring and native execution are verified today.
- Separate inference persistence repair was explicitly approved and completed: only nullable JSONB quote_snapshot/model_snapshot were added, all expected prediction columns read back, and native --skip-bets --skip-discord inference succeeded at 17:53:14Z on deployment 8f277d3b-fae6-4bca-8a58-34b5e8599a80 / commit 963b426d. Independent readback: 1,078 predictions, 462 linked base sample arrays, no stored combo arrays, model snapshots on all rows and quote snapshots on all 251 lined rows. No paper_bets migration was applied. The run still logged 29 decimal.InvalidOperation feature-build errors; full player coverage remains unverified.
