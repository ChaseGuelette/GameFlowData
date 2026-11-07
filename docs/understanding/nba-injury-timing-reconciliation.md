# NBA injury timing reconciliation

## Kick off immediately

Current work: the historical injury recovery MVP passed the approved one-game remote write, local mirror, fresh readback, no-op resume and actual consumer checks. Delivery scope is `.hermes/plans/nba-injury-history-mvp-implementation.md`; this supersedes the hardened orchestrator scope for the first usable slice. Full historical recovery and training remain unstarted and owner-launched. Other chats: props coverage/linkage remains separate.

Status: migration and bounded pilot applied remotely and locally; actual public consumer verified. Source is uncommitted and undeployed. No new unit tests, broad backfills or training runs.

## Two clocks, not one

A game-start timestamp answers **when the game begins**. An injury observation timestamp answers **when the information became available**. Pregame eligibility requires the second instant to precede the applicable decision cutoff, which cannot be later than game start.

Existing game/schedule timestamps may solve missing cutoffs without another external scrape. They cannot by themselves establish the meaning of an injury provider's `08PM` label. Once both timestamps have known UTC offsets, comparison is straightforward; local display timezone no longer changes the result. A bare clock label still requires source timezone and date semantics, including daylight-saving behavior.

Do not substitute tipoff for injury publication time, scrape time for historical publication time, or midnight for a missing tipoff. Missing evidence stays unknown, and omitted players are not automatically healthy.

## MVP boundary

- Reuse authoritative game identity and existing schedule/start information before introducing new storage.
- Preserve raw source observations and explain the origin of any derived timestamp.
- Prefer idempotent reconciliation, including reruns after newly ingested or corrected game times.
- Keep existing strict replay requirements and non-injury feature behavior unchanged.
- Verify actual populated pregame consumer output, not only a successful migration or script exit.

## Evidence and delivery

Current baseline: `handoffs/handoff-143` and `docs/operations/nba-training-consumer-repair-2026-11-05.md`. The earlier consumer repair preserves unknown values; it did not establish usable historical injury timing.

Final execution evidence is summarized below. This is a working MVP and one-game publication-recovery pilot, not whole-history acceptance.

## Discovery: the actual missing pieces

The bounded remote/local database inspection found no `games` relation. Current start times live in `nba_game_schedule.commence_time_utc` (`timestamptz`). Historical `raw_game_lines_staging.commence_time`, indexed by `nba_game_id`, provides another source, but is not always consistent.

- `0022600160`: schedule start is `2026-11-03T00:30:00Z`, the same instant on both databases.
- `0022400875`: no schedule row; 136 historical game-line rows agree on `2025-03-04T00:10:00Z`.
- `0022300555`: historical game-line starts conflict; 84 rows say `18:10Z`, 46 say `17:00Z` on January 15, 2024. No arbitrary majority/minimum/maximum is approved as authority.

These are samples, not whole-history coverage claims. Exact SQL and receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_injury_timing_6b72ed3dd92f/report.md` and its sibling receipts.

`TrainingFeatureLoader.load()` already computes a cutoff of start minus 60 minutes. It reads starts from the schedule join, so historical games without that row get no cutoff. It also defaults `training_injury_report_times` to an empty mapping; therefore the normal path cannot interpret textual provider report labels. Fixing only start times will not populate historical injury features.

The provider is `nba-injuries-reports.p.rapidapi.com`, endpoint `/injuries/nba/YYYY-MM-DD`. Stored raw observations retain requested date and payload game date separately. Historical provider-label semantics remain unverified; no guessed mapping was enabled. Official evidence uses a separate source identity.

## Approved official-report recovery

Chase approved a bounded official NBA PDF recovery pilot. This is a separate evidence source, not a guessed reinterpretation of RapidAPI labels.

Direct retrieval of `https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-03-03_05PM.pdf` returned HTTP 200 and nine pages. The printed header says **03/03/25 05:30 PM**, not 5:00 PM. The NBA official report index labels publication links in ET: `https://official.nba.com/nba-injury-report-2025-26-season/`. The PDF also labels game times ET. The reader uses the printed header, America/New_York (including DST), original URL and document hash; filename hour tokens are not publication timestamps.

The pilot attached independently verified official observations only where game date, team, player, status and reason match existing injury rows after formatting normalization. Original API observations and flattened status fields remain intact. Unmatched or ambiguous rows remain unknown. This recovers evidence of what the official report said at publication, not proof of when GameFlow historically fetched it. Team completeness and healthy-by-omission remain unproven. Ambiguous/nonexistent DST header clocks are rejected rather than guessed.

Implemented:
- Small per-game timing reconciliation table and idempotent CLI; schedule ingestion refreshes it.
- Official PDF reader and bounded recovery CLI; explicit URLs and game/date scope, read-only by default.
- Existing training consumer accepts verified official publication instants while retaining the current cutoff and unknown-value behavior.

The parser dependency `pypdf==5.9.0` was installed in the existing local environment for real-document verification. Source implementation and database acceptance are separate gates.

## Game-start code checkpoint

Implemented and parent-reviewed: `database/migrations/20261105_nba_injury_game_timing.sql`, `src/processing/nba_injury_game_timing.py`, `scripts/reconcile_nba_injury_timing.py`, plus narrow joins/hooks in `training_loader.py` and `nba_schedule.py`. Parent compile, scoped Ruff and diff checks passed; no new unit tests were written.

The fallback examines at most 2,001 quote rows per exact game: at most 2,000 may be accepted only if all nonnull starts agree; overflow is unknown. No sorting of all historical quote rows is needed. Schedule ingestion refreshes only the submitted games using persisted schedule state, never an odds scan. Reruns update only changed timing state.

Migration and four-game execution passed on remote and local. Independent readback confirmed three resolved starts and one unresolved conflict. Each database inserted four rows; repeat execution changed zero. The schedule-only refresh helper also changed zero on its already-current sample. RLS is enabled and client access revoked; local missing Supabase roles are handled without creating roles.

## Official reader checkpoint

The official reader and bounded recovery CLI are implemented. Parent independently downloaded/parsed the March 3, 2025 report: nine pages, 130 rows, printed 17:30 ET -> 22:30 UTC. New files pass Ruff and compile; `injury_context.py` has three unchanged UP038 lint findings reproduced against HEAD, not introduced by this repair.

Remote/local dry-run receipts agreed: all 12 existing injury rows for game `0022400875` match this official report exactly after formatting normalization. The other 76 rows for March 3 belong outside this selected matching set; they are not evidence of 76 missing players in this game. Independent ID/count verification passed before the bounded update.

Official document SHA-256: `562291c9e3ce4bf72dbec389d70c684ce1f4cd29f667fd1fed7385be0acc9fd3`.

Dry-run receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-recovery-preflight-20261105T164000Z-1117/`.

The automatic start-time path is the existing `src/orchestration/nba_schedule_job.py` -> `ingest_schedule()` -> `upsert_schedule()` -> reconciliation hook. This is controlled by `NBA_SCHEDULE_INGEST_ENABLED`; deployment and current production gate state are not verified here. Official PDF recovery is an explicit bounded operator command, not a newly scheduled scraper.

## Final real-data verification

Official recovery ran remote first, then local. Each target changed and freshly verified 12 observations; repeated execution planned and changed zero. All original captures and flattened identity/status fields were preserved. Exact IDs: 69478–69489.

The actual public `load_training_injury_rows` consumer passed on both targets, with no report-time mapping:

| Cutoff | Accepted | Meaning |
|---|---:|---|
| 23:10 UTC (resolved start minus 60 minutes) | 12 | Official 22:30 UTC observations are eligible |
| 22:00 UTC | 0 | All 12 official observations rejected as after cutoff |
| Missing cutoff | 0 | Both team contexts remain unknown |

No raw-selector fallback or model fit was used. The odds-derived start remains labeled `odds_fallback`: consensus is not independent proof of official scheduled tipoff. The PDF lists this game's scheduled time as 7:00 ET, while odds commencement is 7:10 ET; the recovered 5:30 ET observation precedes either minus-60-minute cutoff. Do not silently generalize this pilot into exact historical tipoff authority.

Receipts:
- `logs/receipts/nba_injury_independent_count_gate_20261105.json`
- `logs/receipts/nba_injury_recovery_20261105_manifest.json` and its linked before/execute/readback/no-op receipts.

## Running the MVP

From the repository root, these PowerShell-safe commands are read-only by default:

```powershell
.\venv\Scripts\python.exe scripts\reconcile_nba_injury_timing.py --game-id 0022400875 --game-date 2025-03-03
.\venv\Scripts\python.exe scripts\recover_nba_injury_report_times.py --game-date 2025-03-03 --game-id 0022400875 --report-url https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-03-03_05PM.pdf
```

Add `--local` for the mirror; `--execute` explicitly writes. Inspect planned IDs/counts and follow the isolated SQL-runner count gate before expanding writes. Timing calls are capped at 100 games; official recovery at 10 games and four explicit PDF URLs. Broader historical recovery has not run.

Delivery scope: `.hermes/plans/nba-injury-timing-mvp.md`. Reusable workflow saved in `gameflow-minimal-source-integration`, reference `nba-injury-timing-recovery.md`. No application commit/push/deploy was performed. The separate props fitting blocker and strict replay requirements are unchanged.

## Next major target

Chase's wrap-up direction is **historical injury coverage audit plus a resumable recovery command**. First measure the intended seasons' start-time and pre-cutoff publication gaps; then produce a gap manifest and a checkpointed, bounded, remote-first CLI for owner-launched recovery. Verify interruption/resume and no-op behavior on a tiny cohort before handing off the broader command.

The existing explicit-URL script is a working pilot tool, not yet that whole-history runner. No audit or broad recovery was launched during wrap-up. Exact next-slice requirements are in the plan linked above; do not replace this target with a blanket injury rescrape or confuse it with the concurrent props lane.

## Historical recovery MVP — implemented and live dry-run verified

Audit evidence: `docs/operations/nba-injury-coverage-audit-2026-11-05.md`. Only 12 retained observations are currently usable; 67,181 additional rows are candidates, not proven recoverable statuses.

The first working slice will reuse the existing reader/resolver with explicit date windows, bounded report-slot URL candidates or explicit URLs, JSON resume state, remote-first additive writes and optional exact-row local mirroring. It will derive null game IDs only from unique exact date/team fixtures and an exact official status match. No guessed clocks, missing-status insertion, healthy omissions or start-conflict resolution.

The key distinction: code can recover supported evidence and expose unresolved gaps; it cannot make every historical row usable without matching sources. The public consumer remains unchanged. Actual recovery and broader acceptance follow code/dry-run validation, not vice versa.

Scope: new `scripts/recover_nba_injury_history.py` and an optional small processing module only. No new test suite, scheduling, generic manifest framework, destructive sync, props edits or deployment. Implementation is isolated from the dirty main checkout; the six existing injury-context tests passed under the no-DB/no-network guard before new source integration.

Implemented in one new CLI, `scripts/recover_nba_injury_history.py`, and integrated into the working checkout without changing inherited source. Parent review fixed checkpoint-directory creation, bounded resume ordering, fixture uniqueness outside a capped batch, multi-capture readback, and persisted timing verification.

The CLI defaults to remote read-only planning plus bounded official PDF retrieval. Input windows are at most 31 days; per-invocation limits default to 10 games and four HTTP attempts. `--report-slot` defaults to `05PM`, which is a candidate filename token, never a clock interpretation. Earlier games or missing report slots may remain blocked: supply additional supported slots/explicit URLs rather than guessing timestamps. This does not discover every historical report edition.

`--execute` performs additive remote recovery; `--mirror-local` additionally copies verified observations and timing into matching existing local rows. No broad sync or truncate is used. Missing or mismatched local rows remain pending; full local source population is not part of this command. State includes the mode, so dry-run and execution require separate state files. `--resume` preserves parsed captures, avoids redundant requests, rechecks database state, and advances past blocked games. Per-invocation budgets reset explicitly on each launch, with attempts retained in state.

Validation: scoped Ruff, compile and guarded `--help` passed; six existing tests passed. A controlled in-memory IO exercise using the actual retained 12-row pilot evidence passed execute/mirror, cached resume with zero new changes, null-game matching, pre-cutoff rejection and multiple-capture readback. This is not live write or mirror acceptance.

Actual remote dry-run verification passed on November 6, 2026:
- Game `0022400875`: 12 already present, zero planned, zero blocked; resume revalidated the DB and used zero new downloads.
- Two-game scope with `--max-games 1`: initial exit 3 (remaining budgeted work), then resume advanced to `0022400876` without fetching the PDF again.
- Game `0022400876`: **11 new observations planned**, four existing rows without an exact eligible match (IDs `69494`, `69497`, `69498`, `674523`), exit 4. These four are unmatched, not established bad player identities or genuinely missing source statuses.
- Public `load_training_injury_rows` returned 12 at the pilot cutoff, zero at an earlier cutoff and zero without a cutoff.
- 30 read queries and two official PDF downloads; guarded read-only transactions and zero DDL/DML. No actual recovery writes, local mirror writes, broad jobs, model runs, commits or deployments.

Receipts: `logs/receipts/nba_injury_history_mvp_20261106/verified_report.json`, `state/pilot_dry.json`, `state/two_game_dry.json`, and `public_consumer.json`. Parent checked the saved results and frozen source hash `91f965bad0f0e7a86ebd1be923d50d2288e4b7b3c638a0e136f6025fe658e9b5`.

### Operator commands (from the repository root)

Read-only one-game pilot:

`.\venv\Scripts\python.exe scripts\recover_nba_injury_history.py --start-date 2025-03-03 --end-date 2025-03-03 --game-id 0022400875 --state-file logs\receipts\injury_history_pilot_dry.json --max-games 1 --max-requests 1`

Resume the same scope by appending `--resume`. A 31-day or shorter date range without `--game-id` selects observed competitive games from canonical team stats, not the props fitting cohort. Use a fresh state path for each window; rerun with `--resume` to consume further bounded batches.

After reviewing the dry-run and approving writes, use a **new** state filename plus `--execute`; add `--mirror-local` only when local matching-row writes are also intended. Broad launches stay owner-run.

Exit codes: 0 means the selected scope was accounted without blockers (dry-run remains only a plan); 3 means bounded work remains; 4 means blocked/error outcomes, which can coexist with recoverable or already-written subsets; 2 means invalid inputs/state. Inspect per-game `planned`, `already_present`, `blocked`, `skipped` and mirror outcomes in the state, not only the process exit code.

### Approved real execution — 2026-11-06

Chase authorized the small real recovery and local mirror, not the full historical run. Independent preflight counts agreed on 11 exact eligible rows before writes. For March 3, 2025 game `0022400876`:

| Check | Remote | Local |
|---|---:|---:|
| Initial observations changed / verified | 11 / 11 | 11 / 11 |
| Resume changed / verified | 0 / 11 | 0 / 11 |
| Public consumer accepted at start minus 60 | 11 | 11 |
| Accepted before publication / without cutoff | 0 / 0 | 0 / 0 |
| Unmatched rows preserved | 4 | 4 |

One PDF was downloaded initially; resume used the cached report with zero additional requests. Fresh readback verified original API observations and flat fields were preserved, the exact new observation content matched, and the timing tuples matched across targets after UTC normalization. Each target's timing `updated_at` remained unchanged on resume. Exit 4 is expected for this partially recovered game because the four unmatched rows remain blocked; it is not evidence the 11 matching updates failed.

Evidence: `logs/receipts/nba_injury_history_live_pilot/final_summary.json`, `after_execute_verification.json`, `after_noop_verification.json`, `public_consumer_verification.json`, and the first/resumed state snapshots. Execution used 88 of the bounded 90 SQL statements (excluding guard statements). No application fix, full sync, DDL, model fit, commit, push or deployment was required. Whole-cohort training readiness remains unverified; the prior full audit is a pre-repair snapshot, not current coverage certification.

A separate final verifier used four exact-key SELECTs and confirmed all 11 repaired observations, preservation of original fields/maps, all four untouched blocked rows, unchanged per-route timing timestamps and UTC-normalized remote/local parity. Receipt: `logs/receipts/nba_injury_history_live_pilot/independent_final_readback.json`.

### Historical batch launcher — prepared, not launched

`scripts/run_nba_injury_history_recovery.ps1` is a thin Windows launcher around the same Python CLI. It plans only unless `-Run` is supplied. It uses at most seven-day windows, resumable per-window state, the existing game/request budgets and a per-window batch ceiling. Dry-run/execution and remote-only/mirror state filenames are distinct. A window whose inputs have all been visited can finish with gaps; gaps are retained in state and the overall launcher exits 4 rather than claiming complete recovery. An unexpected child failure or an exhausted batch ceiling stops before the next window.

The full source-recovery span below is the previously audited range, not a training-population or calibration-policy approval. It includes off-season windows, which are labeled `no_observed_games` if canonical stats contain no games. Missing source games are not inferred healthy or complete.

Plan only (verified: 159 windows, no child recovery launched):

`powershell -NoProfile -File scripts\run_nba_injury_history_recovery.ps1 -StartDate 2023-10-24 -EndDate 2026-11-04 -StateDirectory logs\receipts\injury_history_batches -Execute -MirrorLocal`

Owner launch, **not run by the agent**: append `-Run` to that same command after the one-game real execution/readback/no-op gate is accepted. Without `-Execute`, `-Run` performs source-fetching read-only dry-runs instead. A command containing `-Execute` but lacking `-Run` remains plan-only. Restart the same command to reuse window checkpoints; do not change report slots or mode inside an existing state identity.

The `05PM` filename slot is only a first supported source candidate. Gaps that need earlier publications, other report layouts, additional status history or ambiguous identity repair remain explicit follow-up work. Do not launch model training merely because the batch loop finishes.

Launcher verification passed on the actual PowerShell-to-Python path, without database execution: March 3, 2025 completed one read-only batch and returned `accounted_with_gaps` / exit 4 with one PDF request; July 15, 2025 returned `no_observed_games` / exit 0 without a PDF request. Neither produced stderr or a JSON-contract error. Receipt: `logs/receipts/nba_injury_history_live_pilot/launcher_smoke.json`. The one-game live gate and batch-launcher smoke are complete; the full historical launch is intentionally not run.
