# NBA post-sync checks 1–2 — 2026-11-02 UTC

## Conclusion

The completed props transfer passed independent bounded verification. Historical coverage/linkage checks completed with blockers: the current combined table alone is not an adequate historical replay dataset. This is a data-readiness finding, not evidence that the new sync lost the sampled rows. No source repair, archive transfer, rescrape, pruning, schema change, training or deployment was performed.

## Check 1 — transfer integrity: PASS within bounded scope

- Journal completed at 2026-11-02 00:47:47 UTC, with 104,658,433 processed source rows in 10,466 batches. Cursor equals captured upper ID 279217733.
- Independently compared 24,000 rows in 12 deterministic, non-overlapping 2,000-ID windows across the captured range, including its beginning, middle and end.
- All 19 typed fields matched exactly; zero missing rows or value mismatches in the compared windows.
- Ordered schemas and `(staging_id)` primary keys match. Every inspected index is valid, ready and live.
- No relevant sync/COPY sessions remained. New remote rows beginning at 279217734 were explicitly excluded from comparison.
- This is distributed bounded verification plus the tool's per-page commit checks, not an exhaustive checksum of every transferred row.

## Check 2 — coverage, linkage and freshness: AUDIT COMPLETE; READINESS BLOCKED

### Reproducible diagnostic selection

Candidate diagnostic seasons were `22023`, `22024`, `22025`, not an approved training/calibration/protected-OOS split. Distinct regular-season games were sorted by `(game_date, game_id)`; two games were selected at each opening/middle/end phase per season. A separately classified playoff sample was available only for `22025` under the selector. Six play-in games were not mixed into those samples.

There were 19 diagnostic games: 18 regular-season games and one playoff game. Missing older playoff populations under this selector are a limitation, not proof that those games never existed in other source classifications.

### Quote coverage

- Current combined props were present for only **1/19** selected games: `0042500106` (2026-05-01). Its local journal-bounded count was 1,147,195 rows; quality analysis was explicitly capped to 2,631 rows.
- The cap contained 1,243 structural same-stored-snapshot Over/Under groups. Structural pairing does not certify historical decision-time eligibility or the entire game's coverage.
- A remote exact prop count timed out once at 10 seconds; it remains unknown. A narrower indexed existence probe confirmed the same sole present selected game. Timeout-derived zeros from an earlier attempt were superseded by `coverage_final.json`.
- Current game lines supplied both spreads and totals for **11/19** selected games; eight had none. Selected-game row totals matched locally/remotely at 1,492 rows. The bounded records contained 251 structural totals pairs and 251 spread pairs with team-label orientation metadata.
- Every inspected game-line row had insertion at/after commence. Snapshot/update fields did not show the same at-or-after indicators in these capped records. These are different clocks: insertion time must not be silently used as historical observation time.
- No quote was declared eligible. Producer-clock meaning, final decision policy and historical cohort remain unresolved S2 gates.

### Linkage

A separately distributed 10,000-row raw-ID sample found:

- 1,000 null `game_id` values;
- zero null `player_id` values;
- 2,000 null `team_id` values.

Six of the 19 selected games lacked `game_id_map` entries. A game-ID-conditioned sample alone cannot measure unlinked raw rows, which is why the separate raw-ID sample was used. These proportions describe this diagnostic sample, not the entire 104M-row table.

#### Same-ID follow-up

A later SELECT-only comparison checked exactly the same 600 source/local staging IDs in three 200-row windows (first, middle and completed-sync tail). All 600 IDs existed remotely; `game_id`, `player_id`, `team_id`, `api_game_id` and `commence_time` had zero differences or conflicting nonnull values. In the tail, all 200 rows already had missing game/team IDs on the canonical source as well as the mirror. This supports a source-linkage gap, not a copy failure. Comparing different newest-row windows would not establish that result.

A separate newest-1,000 remote sample found 23 game-linked rows with missing player/team IDs and no exact normalized player-name match in the inspected reference table. These are unresolved-reference/alias candidates, not permission to fuzzy-assign identities. Refresh authoritative source/player references and review remaining names before automatic writes. Detailed bounded evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/matched-tail.json`.

### Support-table state

All 14 requested support tables exist locally and remotely, with zero ordered schema mismatches. Small reference counts matched: players 2,015; teams 30; game map 7,249; league priors 147.

| Source | Local | Remote | Interpretation |
|---|---|---|---|
| `rapidapi_injuries.report_date` | 2026-10-30 | 2026-11-01 | Local refresh gap |
| Core player/team game stats and rolling averages | 2026-06-13 | 2026-06-13 | Mirror agrees with the observed source; no newer season stats established |
| `team_allowed_by_position.game_date` | 2026-05-25 | 2026-05-25 | Matching observed watermark; historical source-policy implications still open |
| `player_position_history.snapshot_date` | 2026-10-16 | 2026-10-16 | Matching observed watermark |
| `game_id_map.game_date_utc` | 2026-01-10 | 2026-01-10 | Mapping coverage requires investigation |
| `league_priors_history.snapshot_date` | 2026-01-01 | 2026-01-01 | Matching observed watermark |
| `raw_game_lines_staging` highest PK | 3,325,424 | 3,329,568 | Additional remote rows; PK endpoint dates are not exact max dates |

Advanced-game stats have no stat-date column; ten sampled remote game IDs were found locally through indexed membership. That is not whole-period certification. Future game-line fixtures were not interpreted as fresh completed-game stats.

### Archive limitation

- Remote `raw_player_props_archive` exists with approximately 171,185,488 estimated rows, 20 columns and no indexes. The local archive does not exist.
- An EXPLAIN-preflighted, nonrepresentative physical-prefix sample of 2,000 archived rows covered commence times from 2025-01-09 02:00 UTC through 2025-01-10 02:10 UTC.
- All 2,000 sampled archive staging IDs were absent from both remote current combined and local combined, verified with bounded PK membership queries.
- This proves the sampled retained archive history is excluded from this mirror. It does not prove where all 18 selected-game current-table misses reside. All-history coverage remains unknown; no unindexed archive date scan or blanket rescrape was attempted.

## Next actions and approval boundaries

1. Investigate current source/scheduler failures before treating local-only refreshes as a durable repair. The owner has supplied November 1 Discord failures and requested Railway log investigation plus a reconciliation script.
2. Prepare a dry-run-first reconciliation plan separating local catch-up, source-side repair, game/player linkage and retained archive recovery. A completed current-table sync is not approval to copy 171M archive rows or alter/delete history.
3. **Check 3 / historical S1:** run the isolated fail-closed implementation through a bounded real feature/artifact path; source integration remains separately approved.
4. **Check 4 / historical S2:** validate quote/injury/feature observation clocks and commence mapping, then obtain owner approval for decision time, missing-source handling and training/calibration/protected-OOS boundaries.

The owner requested a reminder of checks 2 and 4: check 2's audit is complete but its listed data blockers need reconciliation/reverification; check 4 is not complete. Check 3 is also still a separate real-data acceptance gate.

## Relevant prior lessons/invariants

Applied `operations/hard-facts`, `operations/critical-invariants`, `lessons/large-odds-table-audits-must-be-keyed-and-chunked`, and `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`. Older lesson-page MLB status paragraphs were not used as current NBA readiness evidence. SQL remained isolated/read-only, timeouts were not interpreted as missing data, and raw timestamp population was not treated as an end-to-end temporal-integrity pass.

## Reproducible evidence

Scratch files can expire. The key findings and boundaries above are durable; exact SQL, game selections and aggregate evidence were generated at:

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/transfer_evidence.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/transfer_audit.py`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/support_metadata.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/support_audit.py`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/coverage_final.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-post-sync-audit/coverage_samples.json`

See also [recovery tool and validation](nba-props-recovery-and-retention-2026-11-01.md), [historical readiness](nba-historical-readiness-2026-10-16.md), and [historical repair specification](nba-historical-repair-spec-2026-10-16.md).
