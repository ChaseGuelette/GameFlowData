# NBA props linkage recovery — 2026-11-03

Status: bounded read-only diagnosis; no repair jobs launched. Owner launches linking/sync jobs. No full-history completeness verdict.

## Verified distinctions

- Existing `raw_player_props_combined` is preserved locally/remotely, with matching 19-column schema and staging_id primary key.
- New remote `nba_props_linker_state` is control metadata, not replacement data; it remains uninitialized. Remote `nba_game_schedule` was created empty. Local `nba_game_schedule` is now created and fresh-readback verified on loopback `gameflow_local`, with 16 columns, keys/checks/FKs/date index and RLS; local anon/authenticated roles were absent, so their grant/policy was omitted. Schedule population has not run. Local linker state is unnecessary for fixed-window repair.
- Training consumes game_id/player_id-linked raw props for line features. A missing team_id alone is not equivalent to failure of that join.
- Added explicit `--local` routing to `LOCAL_DATABASE_URL` for fixed-window incremental mode only. Remote/default behavior is preserved. Local resume and legacy/unbounded commands are rejected. Scoped Ruff and 10 existing offline tests passed; controlled routing checks verified local/remote selection and local-resume rejection. Real-data repair has NOT run.
- Fixed-window incremental mode does not require a checkpoint row. It preserves existing nonnull IDs; conflicts remain reported rather than overwritten.
- Existing remote-to-local sync performs full-row upserts. Do not replay remote rows over stronger local identities until same-ID comparison proves the direction safe. Legacy CSV upload is not cleared.

## Sampled candidate envelopes (inclusive IDs)

These bracket affected samples, NOT exhaustively proven repair ranges or bad-row counts.

| DB | Candidate envelope | Observed affected dates |
|---|---|---|
| Local | 238374806–244072875 | May 19 (team-only in sample) |
| Local | 254330606–257748848 | June 3 (team-only in samples) |
| Local | 258889264–261167092 | June 8 (team-only in sample) |
| Local | 264587336–279401724 | October 20–November 2 (mixed team-only/game gaps) |
| Remote | 264933622–281154089 | October 20–November 5 |

Audit inspected at most 36 disjoint 1000-ID windows per DB. Remote minimum probe timed out; earlier remote history below local's 174552601 lower endpoint was not certified. Raw date filtering lacked a suitable observed date-index plan; no broad date scans were run.

Receipts: Hermes scratch `nba_props_linkage_audit_summary.json`, `nba_props_linkage_range_audit.json`, `nba_props_linkage_samples.csv`, `nba_props_linkage_exact_sql.sql`.

## Immediate safe diagnostic command

Run from project root in PowerShell. This runs on the workstation but reads REMOTE canonical data. It does not write. Exact sampled October window, not a historical repair:

    .\venv\Scripts\python.exe src\processing\nba_linker_local.py incremental --cursor 270284407 --high-water 270285407 --batch-size 1000 --max-batches 1 --dry-run

Read `LINKER_SUMMARY` for unresolved/contradictory references; scan completion does not imply linkage completion.

## Pending gates

1. Same-ID comparison complete: all 8000 inspected rows match in core identity and quote metadata after UTC normalization; identical linkage gaps. Local America/New_York versus remote UTC session rendering caused the initial false timestamp mismatches.
2. Explicit local-target bounded mode is implemented and offline-verified; start with the local dry-run below before any write.
3. Verify player/team/game reference resolution for affected windows before bulk repair. Missing/empty schedule and CDN access remain separate dependencies.
4. Choose null-only correction direction; do not overwrite conflicting nonnull IDs or blanket upload local data.
5. Owner-run bounded repair, then same-key readback and training-consumer acceptance. Sampling is insufficient to certify all historical props.
6. Initialize remote scheduled checkpoint only after selecting a justified replay floor. Schedule table ACL hardening remains separate outstanding work.

## Same-ID readback and local smoke

Eight exact 1000-ID windows were compared in both databases. All 8000 IDs exist in both, with identical game/player/team linkage values and no conflicting nonnull IDs. Five windows have missing team only; three windows lack game IDs (270284408–270285407, 274843066–274844065, 279400725–279401724). Copying rows between databases cannot repair those links. UTC-normalized comparison confirms zero core-identity or quote-metadata mismatches; initial timestamp differences were session-timezone formatting only. Corrected receipt: Hermes scratch `nba_props_same_id_compare_utc_corrected_compact_receipt.json`.

Local dry-run, October 20 sample:

    .\venv\Scripts\python.exe src\processing\nba_linker_local.py incremental --local --cursor 270284407 --high-water 270285407 --batch-size 1000 --max-batches 1 --dry-run

Only after preview establishes resolvable exact references, owner-run local write of the same sample:

    .\venv\Scripts\python.exe src\processing\nba_linker_local.py incremental --local --cursor 270284407 --high-water 270285407 --batch-size 1000 --max-batches 1

Re-run the dry-run after writing to inspect residual unresolved identities. Scan completion is not linkage completion. Do not expand to candidate envelopes until this actual consumer/reference path works. No existing nonnull ID is intentionally overwritten by bounded linking. Local reference completeness is a separate prerequisite.

## Owner-run local schema population and next batch

Owner's first local write completed; independent readback confirmed all 1000 rows fully linked, 14/14 player/game references and teams matched existing boxscores, no mismatches. This did not repair remaining ranges or remote data.

Read-only CDN canary passed with 1206 competitive games, October 20, 2026–April 11, 2027. Populate the newly created local schedule using the existing explicit engine API:

    .\venv\Scripts\python.exe -c "from src.db.client import get_engine; from src.scrapers.nba_schedule import ingest_schedule; print('Local schedule rows upserted:', ingest_schedule(engine=get_engine(local=True)))"

Only after successful schedule population, repair the next confirmed affected October 25 sample:

    .\venv\Scripts\python.exe src\processing\nba_linker_local.py incremental --local --cursor 274843065 --high-water 274844065 --batch-size 1000 --max-batches 1

This is a bounded next batch, not full-history completion. Current-season schedule cannot establish prior-season May/June references; validate those separately before running broad historical intervals.

## Verified October 25 correction

Owner populated 1206 local schedule rows and linked the next 1000-row window. Twenty-one conflicts were preexisting Keaton Wagler props carrying Keaton Wallace's player_id 1630811. An independent count verified exactly 21 approved IDs; owner explicitly approved their local-only correction. A predicate-guarded transaction changed only those rows to player_id 1643413 and team_id 1610612746, grounded in actual player/game stats for game 0022600116. Fresh committed readback verified all 21 corrected, zero old references, and the full window (274843065,274844065] contains 1000 rows with no NULL game/player/team IDs or mismatched boxscore references.

Receipt: Hermes scratch `nba_keaton_wagler_21row_local_correction_receipt.json`; exact IDs and before/after evidence are in its companion SQL/evidence files. The process that originally wrote the incorrect nonnull mapping has not been established, and other historical ranges remain uncertified.

## Approved remote repairs and deployment

Owner explicitly authorized all four remote repairs. Canonical schedule population committed 1206 rows. The production checkpoint was initialized at exclusive replay_floor/cursor 264587335 (June 14 boundary, before observed autumn gaps), without resetting any existing checkpoint. Remote Wagler correction committed exactly21 guarded rows, then both owner-tested1000-row windows were linked remotely. Fresh readback confirmed no missing IDs or exact boxscore-key conflicts in either window. Remote/local sample repairs now agree; broader history remains outstanding.

Railway NBA_SCHEDULE_INGEST_ENABLED=true was read back and the live-container CDN canary passed. Production resumed and exposed a full-backlog MIN/MAX fixture-date query timeout. Commit28755c3d bounds that query to the next batch_size*max_batches indexed rows on all three paths and reports healthy committed resume progress without claiming backlog completion. Conflicts/unverified identities retain failure behavior. Ten existing offline tests, Ruff and five exit-code checks passed; exact remote replacement query completed in2.507s. Git remote and Railway successful deployment of28755c3d were verified. New scheduler logs show all three schedule-refresh jobs registered.

Receipts: Hermes scratch `nba_remote_schedule_checkpoint_20261103_receipt.json`, `nba_remote_wagler_and_two_windows_final_receipt.json`. A first Railway SSH verification stalled during deployment and was stopped; it is not job-success evidence. Historical backlog is not fully repaired by these two sample windows.

### Subsequent production acceptance and remaining gate

Historical June quotes exposed wrong nonnull game IDs (June13 NYK@SAS quotes linked to June5) and wrong stored teams (e.g. Mikal Bridges assigned SAS rather than NYK). The explicit bounded `--repair-game-conflicts` mode now requires exact unique fixture/date/name evidence and an actual unique player-game-team boxscore relation before correcting a game/team pair. Existing player IDs and source/old-ID optimistic guards remain protected. Default mode remains null-only. Real3-row game-only and1-row joint game/team corrections passed fresh readback. A full Daily Stats attempt corrected4749 game IDs before reporting remaining team conflicts; that result is not whole-history completion.

Commit4482e873 deployed successfully. Railway deployment fdf77d14-97a1-427c-9f16-1e9b00c410d8 logged `Job completed successfully: lines_job.py` at2026-11-03 18:44:33UTC, following its18:35 start. This verifies an actual scheduled production lines run, not just registered jobs or local tests. Global unresolved history remains recorded separately from clean current-batch readiness.

Workstation Daily Stats completed ingestion/derived stages but another attempt failed acquiring the production linker lock. Commit029bdd1d adds bounded retries only for the exact lock-busy error; identity/SQL failures do not retry. Scoped pipeline tests25/25 and controlled retry/nonretry cases passed.

The subsequent real Daily Stats run against canonical remote completed successfully at2026-11-03 13:56:06ET (exit0,557.8s). The linker succeeded on attempt4 after bounded contention retries. Invocation: `venv/Scripts/python.exe -u src/orchestration/daily_stats_job.py --skip-resolution --max-catchup-dates 1`. Bet resolution was intentionally excluded. Railway deployment a878d9b0-1bf3-483c-b5f2-850e7d6d4989 successfully serves029bdd1d. Remaining acceptance is Railway-native Daily Stats execution and complete historical linkage/training readiness; neither is established by this workstation-to-remote run.
