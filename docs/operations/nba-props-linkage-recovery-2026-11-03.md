# NBA props linkage recovery — 2026-11-03

Status: bounded read-only diagnosis; no repair jobs launched. Owner launches linking/sync jobs. No full-history completeness verdict.

## Verified distinctions

- Existing `raw_player_props_combined` is preserved locally/remotely, with matching 19-column schema and staging_id primary key.
- New remote `nba_props_linker_state` is control metadata, not replacement data; it remains empty. `nba_game_schedule` also exists but is empty. Local copies of these new tables are absent.
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
