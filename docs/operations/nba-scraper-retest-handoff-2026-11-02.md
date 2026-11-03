# NBA scraper retest handoff

The scraper changes are integrated into the main working checkout. After Chase explicitly authorized single-game smokes, the advanced scraper fetched/saved game `0022600001` with the direct/curl transport and exited zero. This is not a declaration that Railway or the full pipeline is healthy. No bulk, mirror, linker, derived or migration job was launched; the single-game exception does not authorize those jobs.

## Kick off immediately

- You: run the CDN source check, then the one-game advanced smoke below. Only start the bounded advanced catch-up if the smoke exits zero.
- This chat: source integration and offline code acceptance are complete for this bundle. Runtime/schema/deployment acceptance and automatic support-table transport are not complete; do not launch your jobs on your behalf.
- Other chat: keep historical policy/lifecycle work separate; no training/archive run is unlocked by this retest.
- Wiki: [how the pipeline fits together](../understanding/nba-production-data-pipeline.md).

## Verified on main

- Final integrated bundle: 139 existing scoped tests passed, including the follow-on mirror/opponent changes and backward-compatible schedule-fallback correction.
- Independent follow-up review approved closure of all four identified guard defects. This is code acceptance for bounded retesting, not a production-data readiness certificate.
- Ruff, Python compilation and scoped whitespace checks passed.
- No test files or fixtures were changed.
- Daily Stats basic catch-up now invokes basic-only population; player advanced averages and team advanced columns are excluded before the source gate. Advanced rolling outputs have their own post-gate catch-up.
- Advanced selection detects partially missing player possessions even if team advanced fields exist; player rows upsert idempotently and coverage is checked again.
- Production linking uses bounded durable resume; preview does not acquire checkpoint/advisory locks. Its small state migration/initialization is not applied.
- Source is uncommitted and not deployed. Existing remote schedules still use the deployed version until a separately approved deployment.

## Follow-on code now integrated, not executed

- Existing props copier now has explicit `--follow` with recent-ID overlap and a dedicated mode-tagged journal. It validates completed checkpoints before rotation, preserves legacy recovery journals, and refuses an empty local mirror or regressed source watermark. Exit 3 means the bounded invocation paused incomplete; rerun the same command to resume the frozen window.
- Existing generic sync has a new `--id-window` restricted to `raw_game_lines_staging`, capped at a 100,000-ID span. This mode bypasses initialization, schema creation and TRUNCATE, checks actual connected server/database identities as well as URL routing, and uses staged primary-key UPSERT. Its dry-run makes both DB sessions read-only; its diagnostic JSON file is still written.
- Opponent derivation now reads full relevant-season history through the requested completed end, writes only target dates, and rejects missing advanced rows or NULL possessions before deriving/writing. This does not repair missing upstream advanced data by itself.
- Advanced/source and mirror wrappers remain separate owner-launched steps. Windows redirected Python logging now explicitly uses UTF-8.

No live mirror or derived job was run. Automatic support-table mirroring is not yet wired/accepted; do not use legacy generic full/all-table modes as a substitute, because those retain their original destructive behavior.

After canonical source/linker acceptance, owner-only previews from the same PowerShell checkout:

    .\venv\Scripts\python.exe -X utf8 scripts\sync_nba_props_by_id.py --preflight --follow --batch-size 10000 --max-batches 50 --replay-id-span 200000 --state-file logs/sync/nba_scheduled_props.json

That props preflight checks connections/schema, not completed transfer or all historical linkage. The new follow journal must not replace the historical full-recovery journal. The separate `scripts\run_nba_local_sync.bat` is an execution entrypoint, not a scheduled/started task; it covers props only and replays recent IDs, not all historical corrections.

    .\venv\Scripts\python.exe -X utf8 scripts\sync_local_db.py --id-window --tables raw_game_lines_staging --replay-id-span 10000 --dry-run --state-file logs/sync/nba_lines_preview.json

Review the printed line bounds; use explicit `--start-after-id` and `--end-id` for the subsequently approved write so a later invocation does not silently preview a different interval. These previews are supplied for you to run, not a statement that they have passed against your live databases.

## Run in PowerShell

These source jobs write canonical `DATABASE_URL`, even though launched locally. They do not populate only `LOCAL_DATABASE_URL`.

    Set-Location 'C:\Users\Chase\Projects\GameFlowData'

1. CDN basic source check/catch-up (Railway-compatible source, executed here by you):

    .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_unified_scraper.py --cdn-only

Stop if it exits nonzero. Failed selected CDN games no longer become a successful job.

2. One-game LOCAL advanced smoke (now defaults to direct Chrome-compatible curl transport):

    .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_unified_scraper.py --season current --skip-team --skip-traditional --advanced-limit 1 --request-timeout 20 --max-retries 1

The connection repair and the successful authorized one-game CLI are documented in [advanced transport repair](../understanding/nba-advanced-scraper-connection-repair.md). A configured proxy now requires `--use-proxy`; `--no-proxy` explicitly selects the default direct route. Do not retry schema, write or coverage failures as if they were networking. Do not start the large run without the real smoke/readback gate.

3. Bounded current-season advanced catch-up, ONLY after the smoke succeeds:

    .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_unified_scraper.py --season current --skip-team --skip-traditional --advanced-limit 120 --request-timeout 30 --max-retries 2 --http-client curl --no-proxy

This explicitly uses the successful direct/curl route. The limit is selected games per invocation, not certification that the whole season is complete. Repeated runs use the actual missing/partial-game selector rather than redoing every complete game.

## Do not launch yet

- Do not run the legacy unbounded linker or a generic/full sync.
- Full Daily Stats/production resume requires the prepared state migration and an owner-approved replay floor. Schedule persistence additionally requires its existing schema and the ingest flag. No migration or OS task was applied/started/rescheduled here.
- Do not launch archive recovery, training, or historical replay: source/cohort/clock gates remain separate.

## Remaining runtime uncertainty

Local probes could fetch both official CDN schedule endpoints, but the earlier Railway logs show a CDN 403; local access is not cloud proof. Earlier advanced requests/SOCKS routes failed; the November 2 transport repair fetched and saved game `0022600001` via the real one-game CLI using direct curl, exit zero. See the linked connection-repair wiki for independent readback status. This does not clear all season, derived, mirror or Railway gates. No fabricated advanced values or zero-default replacements were added.
