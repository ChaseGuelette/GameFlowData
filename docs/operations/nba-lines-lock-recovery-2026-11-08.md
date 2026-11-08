# NBA lines checkpoint-lock recovery — 2026-11-08

## Incident and cause

The 22:00 UTC / 5:00 PM EST scheduled lines job scraped successfully, then failed at fixed-window linking because another process held the `nba_inference` checkpoint advisory lock. The scheduler's threading lock serializes its own children only; it cannot prevent competing hosts/containers/standalone commands. The lock state name identifies the source, not proof that the inference job owned it. Historical holder identity is unknown. The old `NBA deferred` alert label is not a root-cause classification.

The 22:05 run succeeded, but used a new capture window. Canonical SELECT-only verification found all 36,128 retained rows in `7310984 < staging_id <= 7370635` still missing both player/game IDs. Therefore later green schedules did not recover the failed capture.

## Repair

`src/processing/nba_linker_local.py` now shares `_acquire_linker_lock` across resume and fixed-window paths: retry only unsuccessful advisory-lock acquisition for up to 120 seconds using a monotonic deadline, commit before each sleep, preserve existing lock keys/unlocks, and propagate unrelated database errors. No lock bypass, holder termination or arbitrary-error retry.

Deployed isolated snapshot of pushed base `e2ad6710f2990de96e5ea20fb052f3f91230c220` plus this source change to Railway deployment `329b197b-3487-44d4-a9f8-6f23564c4580`. Deployment SUCCESS. Native read-only helper probe passed; deployed SHA256 `9cefec292b8d9f3ec4b7e1bce19a7df619354826cf007feb8b540c78781d8d3c`. Ten existing tests passed under the offline guard. Controlled immediate-acquire, contention-release, bounded-exhaustion and unrelated-error checks passed. No new unit tests added.

## Owner-approved fixed-window recovery

Executed inside Railway with a 300-second child cap:

    /app/venv/bin/python src/processing/nba_linker_local.py incremental --source-table nba_player_props_inference --cursor 7310984 --high-water 7370635 --batch-size 5000 --max-batches 8

No re-scrape, durable historical checkpoint reset, or non-null conflict repair. Child exited 0 and reported 36,128 rows scanned/updated, complete fixed window, identity_ready=true, zero conflicts and zero corrected non-null IDs. Remote execution receipt: `/app/logs/nba-linker-recovery-7310984-7370635.log`.

Independent post-recovery canonical Supabase readback: PASS. All 36,128 retained rows are fully linked; zero missing player IDs, game IDs, or either. Guards verified read-only transaction and 15-second statement timeout. SQL/result receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-recovery-verification-7310984-7370635-20261108T230349Z.md`.

Native scheduled lines jobs on the repair deployment completed successfully at 22:33:07, 22:37:56, 22:42:50, 22:47:57, 22:52:54, 22:57:56 and 23:03:10 UTC, with complete identity-ready linker summaries. These are logged outputs, distinct from independent database verification.

## Remaining boundaries

- Source change is deployed and local but uncommitted/unpushed. Preserve it in the next Git publication; no Git commit/push was inferred from recovery approval.
- The exact competing historical lock holder is unknown.
- Bounded waiting reduces transient failures; contention lasting longer than the budget still correctly fails. A durable failed-window retry queue is not implemented by this patch.
- Canonical GBrain promotion/sync is not claimed by this local receipt.
