# NBA historical injury coverage audit — 2026-11-05

## Verdict

Canonical remote coverage is **not ready for historical injury-aware fitting or calibration** under the current ordinary training consumer.

- The authoritative `team_game_stats` cohorts contain **3,902 games** through 2026-11-05. The audit did not use the props fitting predicate or `raw_player_props_combined`.
- Only **123/3,902 games** currently have an effective consumer start: two historical odds-fallback games in `22024`, plus all 121 current-season games in `22026` from valid schedule rows.
- Across **67,193 injury rows** on cohort game dates, only the known 12-row `0022400875` pilot has consumer-valid publication evidence. All 12 are usable before start minus 60 minutes and are selected by the real consumer semantics.
- The other **67,181 existing rows** are an existing-row evidence-recovery candidate pool, not proven recoverable rows. Genuinely missing historical statuses remain **UNKNOWN** without comparison to official reports.
- Player and team linkage is mostly populated; game linkage is materially incomplete and is completely absent in the initial `22026` test window.

## Scope and guards

- Authority: canonical remote `DATABASE_URL`.
- Read-only: every completed metadata/data query ran in a transaction with verified `transaction_read_only=on` and `statement_timeout=15s` before the query.
- Budget: **17/35 queries spent**, including one timed-out query.
- Timeout handling: the first player-stat/`EXISTS` cohort aggregate timed out at 15 seconds. It was recorded and replaced with the narrower indexed `team_game_stats.season_id` population; the timeout was not treated as absence.
- Giant-table access: `raw_game_lines_staging` was accessed only through the indexed exact `nba_game_id` path for three controls, capped at 2,001 rows per game.
- Excluded: DB writes, external report retrieval, scraping, model jobs, application edits, broad props-table scans, and local-mirror claims.

## Cohort denominators

The exact cohort dates and games come from authoritative team game stats, bounded by season ID and `game_date <= 2026-11-05`.

| Season ID | Intended role | Games | Distinct dates | Date range | 002 regular | 004 playoff | 005 play-in |
|---|---|---:|---:|---|---:|---:|---:|
| `22023` | fit | 1,230 | 160 | 2023-10-24–2024-04-14 | 1,230 | 0 | 0 |
| `22024` | fit | 1,230 | 163 | 2024-10-22–2025-04-13 | 1,230 | 0 | 0 |
| `22025` | calibration | 1,321 | 211 | 2025-10-21–2026-06-13 | 1,230 | 85 | 6 |
| `22026` | initial test | 121 | 15 | 2026-10-20–2026-11-04 | 121 | 0 | 0 |

`22025` contains postseason game IDs in the authoritative stats population. Any regular-season-only calibration policy must explicitly exclude `004` and `005`; this audit did not silently remove them.

## Persisted/effective game-start coverage

“Effective start” applies the actual schedule-first consumer rules: a present schedule row must be valid; timing fallback is considered only when schedule is absent.

| Season | Games | Effective starts | Valid schedule | Resolved fallback | Reconciliation absent | Persisted unknown | Start coverage |
|---|---:|---:|---:|---:|---:|---:|---:|
| `22023` | 1,230 | 0 | 0 | 0 | 1,229 | 1 | 0.00% |
| `22024` | 1,230 | 2 | 0 | 2 | 1,228 | 0 | 0.16% |
| `22025` | 1,321 | 0 | 0 | 0 | 1,321 | 0 | 0.00% |
| `22026` | 121 | 121 | 121 | 0 | 120 | 0 | 100.00% |

Interpretation:

- **Absent reconciliation** means no row exists in `nba_injury_game_timing`; it does not prove source start absence.
- **Persisted unknown** means reconciliation ran and stored an unresolved result. The table does not retain the detailed resolver reason, so aggregate SQL cannot separate missing/null/overflow/date mismatch/conflict solely from persisted state.
- The one `22023` persisted unknown is control `0022300555`; the capped indexed raw-lines read confirms 130 rows with two conflicting starts (`17:00Z` and `18:10Z`), so it is correctly unresolved rather than an absent reconciliation.
- Current-season `22026` has complete effective schedule coverage even though only one timing row is persisted; schedule-first authority makes the other 120 timing absences non-blocking for start resolution.

## Injury identity and retained observation coverage

Rows are counted on the exact cohort game dates. These are row counts, not complete-report or healthy-by-omission claims.

| Season | Injury rows | Report dates | Player ID null | Team ID null | Game ID null | No observations |
|---|---:|---:|---:|---:|---:|---:|
| `22023` | 19,720 | 160 | 37 | 27 | 5,604 | 5,536 |
| `22024` | 21,487 | 163 | 11 | 9 | 6,110 | 6,030 |
| `22025` | 25,362 | 209 | 267 | 60 | 8,640 | 8,432 |
| `22026` | 624 | 8 | 3 | 0 | 624 | 624 |

Linked-row rates:

| Season | Player ID populated | Team ID populated | Game ID populated |
|---|---:|---:|---:|
| `22023` | 99.81% | 99.86% | 71.58% |
| `22024` | 99.95% | 99.96% | 71.56% |
| `22025` | 98.95% | 99.76% | 65.93% |
| `22026` | 99.52% | 100.00% | 0.00% |

## Publication evidence and start-minus-60 usability

The audit ran the existing pure consumer selection logic over retained SELECT data with an empty provider-label mapping. Therefore only observations accepted by current consumer semantics count; unsupported RapidAPI textual report labels do not become timestamps.

| Season | Team-game contexts | Contexts with cutoff | Consumer-valid publication rows | Valid before cutoff | Selected latest eligible rows | Selected team-games |
|---|---:|---:|---:|---:|---:|---:|
| `22023` | 2,460 | 0 | 0 | 0 | 0 | 0 |
| `22024` | 2,460 | 4 | 12 | 12 | 12 | 2 |
| `22025` | 2,642 | 0 | 0 | 0 | 0 | 0 |
| `22026` | 242 | 242 | 0 | 0 | 0 | 0 |

The 12 accepted rows are exactly injury IDs `69478`–`69489` for pilot game `0022400875`. This is the only observed historical publication-recovery success and must not be generalized to whole-history coverage.

Existing rows without consumer-valid publication evidence are candidate inputs to a future official-report comparison:

- `22023`: 19,720
- `22024`: 21,475
- `22025`: 25,362
- `22026`: 624
- Total: **67,181**

These counts do **not** prove that matching historical official reports exist or that reports contain all statuses. Previously unretained statuses and healthy omissions remain UNKNOWN.

## Control games

| Game | Result |
|---|---|
| `0022400875` | No schedule row; timing resolved by 136/136 agreeing raw-line starts at `2025-03-04T00:10:00Z`; 12 linked injury rows; all 12 official pilot observations pass the start-minus-60 consumer cutoff. |
| `0022300555` | No schedule row; timing persisted unknown; 130 capped indexed raw-line rows contain two starts (`17:00Z`, `18:10Z`); 15 linked injury rows. |
| `0022600160` | Valid final schedule and persisted timing both resolve to `2026-11-03T00:30:00Z`; no exact-game injury rows. |

## Limitations and next bounded use

- Identity figures count null IDs, not independently validated populated IDs. Injury-row denominators are based on report dates matching cohort dates, not a claim that every row belongs to a verified cohort game. Current-season null game IDs also block the existing exact-game recovery CLI; the ordinary selector itself uses team/date context, so missing publication evidence is independently decisive.
- No external official-report comparison was performed, so the audit cannot count genuinely missing statuses or certify the 67,181-row candidate pool as recoverable.
- `nba_injury_game_timing` persists basis/status but not the resolver reason; only exact controls can distinguish a known conflict from other unknown causes without rerunning the bounded resolver.
- The consumer-valid publication count is strict current behavior, not a claim that all unsupported source observations are false; their historical clock semantics remain unapproved.
- The initial `22026` test window has start coverage but zero retained source observations and zero game-linked injury rows, so start recovery alone does not make that cohort injury-usable.

## Receipts

Exact SQL, parameters, verified guards, and exact result rows are saved under:

`logs/receipts/nba_injury_coverage_audit_20261105/`

Key files:

- `manifest.json` — all 17 query receipts and paths.
- `summary.json` — reconciled audit totals.
- `01_schema_columns.json`, `02_schema_indexes.json`, `03_table_estimates.json` — preflight.
- `04_cohort_denominators_timed_out.json` — recorded timeout and narrowing action.
- `05_cohort_denominators.json` through `17_injury_rows_22026.json` — bounded data receipts.
- `consumer_semantics_22023.json` through `consumer_semantics_22026.json` — programmatically reconciled consumer results.

Mechanical reconciliation confirmed 17 numbered receipts, 17 queries spent, zero bad completed-query guards, 3,902 games, 123 effective starts, 67,193 injury rows, and 12 valid/pre-cutoff/selected injury rows.

Parent verification independently loaded the saved query receipts and reran `_select_training_injury_rows` for all four seasons without DB/network access. Selected counts and full consumer accounting matched the saved receipts: 0, 12, 0, 0. Every receipt path and read-only/15-second guard was also checked. This is an offline replay of real retained data, not a new live query or end-to-end training certification.

## Recovery command definition

The evidence-backed specification is `.hermes/plans/nba-injury-history-recovery.md`. It separates start inspection, identity repair, official publication matching, crash-safe remote execution and gated local mirroring. The orchestrator is defined, not implemented or launch-ready; current pilot commands do not have durable resume. No DB write, download, recovery or sync ran in this audit.
