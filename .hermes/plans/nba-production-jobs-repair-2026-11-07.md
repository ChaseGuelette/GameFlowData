# NBA production job recovery — November 7, 2026

## Authorization and acceptance
Chase authorized job repair, scoped publication/deployment and bounded non-overlapping recovery checks. No other session owns Daily Stats/scheduler/linker. Protect dirty historical harness/source files and separate paper-backend worktree. No migrations, DB session termination, broad backfills, new tests, model training or betting-policy changes. Verification jobs must skip betting/notifications where supported.

Done means actual current schedule, linked live quotes, predictions/samples and refreshed edges exist, with subsequent scheduled success. Code tests and green wrappers alone do not close this task.

## Observed causes
- Railway 914e2b9a: CDN native schedule canary HTTP403; persisted schedule observation 17:05 UTC then linker rejects under90-minute rule although scheduled refresh gaps exceed90minutes.
- No blocked SQL queries at19:18/19:19 UTC. Granted linker advisory guard protects an active writer; do not terminate.
- DailyStats still runs historical raw-props resume (267M staging IDs) while live lines use separate inference checkpoint (115k cursor,454k source max).
- Linker writes execute one UPDATE per row; repeated remote round trips consume its cadence. Frozen backlog and unresolved replay delay current quotes.
- Advanced producer completed17:57 UTC through Nov6, missing advanced pairs0. Earlier stale failures are superseded. Today has no prediction/sample outputs.
- Edge refresh uses old raw-props reader and spends20minutes creating retired DFS entries despite --skip-paper.

## Minimal repair lanes
1. Live source/linker: retain exact identity/preimage/lock guards; batch guarded updates in one bounded SQL statement; keep production basic ingestion independent of historical raw repair. Restore fresh official schedule acquisition rather than simply removing freshness. Ensure newly ingested current quotes are processed without historical starvation; preserve historical checkpoint evidence.
2. Edge consumer: stop retired DFS side effects; reuse current inference quote source/coherent pairing and current source game-time fallback; preserve empirical probabilities, BL parameters and --skip-paper. Missing samples/predictions must remain explicitly nonproductive.
3. Schedule/frontend: validate actual source in deployed runtime, correct only observed transport/consumer errors; distinguish unavailable from real empty slate. Preserve game IDs and 006 exclusion.
4. Verify current advanced readiness then no-bet inference, edges and subsequent scheduled lines. Coordinate with existing writers; no checkpoint reset or blind backlog launch.

## Validation
Existing scoped tests through skill offline guard, scoped Ruff/compile, controlled probes for changed boundaries, exact scoped diff review. Then bounded real read-only preview, approved current-data recovery, committed output readback and deployed scheduled-child verification. Source/deployment/data/runtime gates tracked separately.

## Status
Implemented and published: `4d3c1135`, `c999290c`, `a5cbd280`, `94b2f556`, `ef179f64`, `c2825ca3`. Latest Railway deployment `90fee6e7-7907-42c4-a489-6ca94fdfcf1a` is SUCCESS. Only this lane's commits were pushed from its isolated checkout; concurrent paper-backend commits were merged locally but were not published by this lane.

### Real execution evidence (November 7, UTC)
- Schedule: bounded emergency refresh verified 23 exact games for Nov6–8. Native Railway schedule job then completed at20:07:53, processing1206 official schedule rows. Recurrence is now30minutes, preserving90-minute consumer freshness.
- Lines: native deployed runs completed20:16:53 and20:21:54. Respectively40955 and41190 new quotes linked; each had0 conflicts,0 unverified references,0 unresolved identities. Both T60 materialization stages succeeded. Total runtimes113.3s/114.3s; linking27.1s/27.7s. Historical checkpoint preserved.
- Daily Stats: current-season anti-join predicates remove whole-history scans. Canonical recovery via Railway environment completed9.1s, then actual scheduler recording-wrapper retest completed5.9s at20:31:42. Both used basic-only/skip-linker/skip-resolution; zero missing basic-average dates. This was workstation execution against production, not a newly observed native morning trigger.
- Inference: advanced readiness throughNov6 passed. No-bet/no-Discord recovery at20:25:41 stored1036 predictions plus444 base sample arrays in29.4s. Earlier run stored1050 arrays including combos; existing records were not deleted. Future combo sample writes are now suppressed at the shared store boundary; consumers derive combos from base samples.
- Edge: scheduled native refresh completed20:22:25 in22.2s, updating229 predictions with edges and15 recommendations. No new paper placement in the frequent `--skip-paper` loop. Existing scheduled Discord behavior remains unchanged.
- Frontend: original deployed route returned502 from CDN. Official fallback and authenticated/RLS persisted-schedule fallback deployed. TypeScript/ESLint passed; genuine saved DB rows through controlled source403 path returned7 games; stale/missing DB evidence returns502. Actual signed-in browser readback is still open: no saved login and secure prompt unavailable.
- Automatic recovery: `ef179f64` adds half-hour no-bet/no-prediction-alert recovery after10:15 only when no recent successful inference exists and advanced inputs pass. Same-process inference duplicates are suppressed; original paper-entry windows are unchanged. Native Railway recovery ran20:35:07–20:35:48 and succeeded (child40.6s), reporting1043 predictions+447basearrays. `c2825ca3` adds the same guarded check on startup; deployed startup20:37:52–20:37:53 correctly read the persisted20:35:48 success and suppressed a duplicate. A second native schedule cycle completed20:37:53 with1206rows; native edge20:37:20 refreshed232edges in19.0s with12recommendations.
- Independent canonical readback around20:28: 1036 predictions across7games,232 with lines/probabilities/edges;444 fresh base arrays (148eachpts/reb/ast),all10000samples,0freshcombo writes. Six matching scheduled lines/edge executions after20:15 were allsuccess. Authenticated-role RLS read returned the same7current schedule rows. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_final_canonical_verification_20261107_7f3c9a.json`. These are timestamped snapshots, not asserted final totals after later scheduled runs.

### Validation and remaining distinctions
16 focused existing Python tests passed under the offline DB/network/model guard; scoped Ruff and frontend TypeScript/ESLint passed. Existing schedule-registration assertion expects the old three schedules and therefore needs an owner-approved fixture update. Pre-existing schedule-upsert mock and daily-runner max-age fixture failures were observed; no new tests or test edits were made. Short controlled probes passed capture bounds/failure barriers, no-DB dry-run, base-only sample persistence and recovery gates.

Railway MCP was unauthorized; CLI logs and deployment metadata worked. Native SSH initially ran the source canary (403) but later key authentication blocked ad-hoc execution. No DB session termination, migrations, historical checkpoint reset, broad backfill, model training or promotion. Advanced producer remains workstation-only and requires host availability.
