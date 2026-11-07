# NBA injury timing MVP

## Kick off immediately

This chat owns implementation and verification, authorized by Chase's request to fully handle injury timing with a migration/reconciliation script. No new unit tests. No commit/push/deploy, training, broad backfill, or props edits. Source-clock interpretation remains under primary-source research; do not invent it.

## Slice A: authoritative game starts

Purpose: supply real start instants to the existing injury cutoff calculation without rewriting injury evidence or polluting the canonical CDN schedule with odds-derived rows.

Allowed source files:
- NEW database/migrations/20261105_nba_injury_game_timing.sql
- NEW src/processing/nba_injury_game_timing.py
- NEW scripts/reconcile_nba_injury_timing.py
- EDIT src/models/features/nba/training_loader.py (game start resolution only)
- EDIT src/scrapers/nba_schedule.py (schedule-ingest reconciliation hook only)

Implement a small per-game reconciliation table public.nba_injury_game_timing with primary key game_id text, game_date date, nullable game_start_utc timestamptz, basis/status text, updated_at timestamptz. Preserve one row per game; unresolved conflicts explicitly store null, never leave stale resolved fallback. Migration additive/idempotent, bounded lock/statement timeouts, no giant-table index. Restrict client access consistent with existing backend-only tables.

Resolver input: bounded exact game IDs and game dates. Validate real game IDs and coherent dates; reject duplicate input IDs with conflicting dates. Query schedule by exact ID first. Prefer schedule start when schedule.game_date matches and game status is not postponed/cancelled/TBD. An existing but invalid/conflicting schedule row must block fallback, not be bypassed. Only absent schedule rows may fall back to raw_game_lines_staging by indexed nba_game_id. Require ALL nonnull start values for that game agree, match supplied game date in America/New_York, and fit bounded rows/query timeout; null-only/missing/conflicting data stays unknown. Do not assume majority/min/max authority. Cap inspected quotes per game, mark overflow unknown. Retain basis/conflict reason in returned receipt. No raw_player_props_combined access.

CLI: requires explicit --game-id (repeatable, capped <=100) and matching date input or a bounded --start-date/--end-date selection of injury-linked games (max-games cap; refuse overflow). Dry-run read-only default; --execute persists upserts with IS DISTINCT FROM so repeat execution is no-op. Explicit --local route supported via existing get_engine(local=True); default canonical remote. No-argument invocation help/error without DB. Always verify transaction guards and short timeouts. Bounded chunking, no secrets in logs. JSON output compact counts/per-game resolution. Script does not apply migration implicitly.

Future schedule ingestion: after schedule upsert transaction, reconcile only submitted game IDs using schedule rows, no historical odds scans. Must not mark a stale rejected schedule input authoritative: resolve persisted schedule state. Missing timing migration should fail clearly rather than silently declare sync successful. Keep CDN-only unchanged.

Training loader: read reconciled starts for historical absent-schedule games through one-to-one table join, preserving schedule-first authority and game-date identity. Do not alter strict HistoricalFeatureLoader, props guard, cutoff minus60 minutes, injury imputation, provider clock mapping or feature ordering. Ensure an invalid present schedule does not silently use stale timing fallback. Existing direct schedule timestamp usage may require status/date validation consistent with resolver.

## Validation

No new unit tests. Writer: compile, scoped Ruff, CLI --help and argument checks without DB, review SQL and exact diff. Parent isolated runner: migration preflight local+remote, apply reviewed additive migration, run dry-run exact sampled games, bounded execute and fresh readback, repeat no-op, real training injury consumer sample after Slice B. Do not run model fits. Parent independently reviews source and verifies returned claims.

## Slice B: report availability clocks

Chase explicitly approved official NBA PDF recovery after discovery could not establish historical API label semantics. Implemented files: `src/scrapers/nba_official_injury_reports.py`, `scripts/recover_nba_injury_report_times.py`, the ordinary consumer parser in `injury_context.py`, and pinned `pypdf==5.9.0` in both dependency manifests. No new tests or strict-replay changes.

The reader derives publication from the printed header in America/New_York, preserves the official URL/hash/raw row, and marks lineage `official_reconstruction`. The bounded script attaches observations only to unique existing date/team/player/status/reason matches, preserving original API captures and flat fields. Dry-run is default; updates require explicit execution. This does not prove complete reports, historical local availability, or healthy omissions.

## Verified checkpoint

Slice A migration is applied remotely and locally. Independent readback confirms four target rows: three resolved, one unknown conflict. Repeat reconciliation changed zero rows and the schedule-only refresh helper was exercised. Source is not committed or deployed.

Slice B independent preflight and exact count gate agreed on 12 candidate injury rows (69478–69489) for `0022400875` / `2025-03-03` on both databases. Execution changed/read back all 12 per target, preserved prior captures and flat fields, and repeated with zero changes. The actual public consumer accepted 12 at23:10UTC, rejected all12 at22:00UTC, and accepted zero with missing cutoffs. No whole-history recovery or training acceptance is claimed. Odds-derived starts retain their lower-authority basis; the wiki records the pilot's scheduled-versus-odds ten-minute discrepancy.

Canonical educational/delivery record: `docs/understanding/nba-injury-timing-reconciliation.md`.

## Next major target — owner-directed at wrap-up

**Audit historical injury timing coverage, then prepare a resumable recovery command.** This is the next work package, not a backfill launched during wrap-up.

1. Audit the intended NBA cohorts (fit 2023–24/2024–25, calibration 2025–26, initial test 2026–27) with explicit date bounds and isolated read-only SQL. Report by season: game-start basis/coverage/conflicts; injury rows with valid publication evidence; before-cutoff usability; unresolved identities; and existing-row-only recovery versus genuinely missing earlier statuses. Inspect indexed access paths and bound work; do not scan giant props tables or reuse the empty props fitting predicate as the audit population.
2. Turn only evidenced gaps into a deterministic recovery manifest. Keep official scheduled starts distinct from odds commencement; the ten-minute pilot discrepancy must not be silently accepted as exact tipoff authority.
3. Build a real operator CLI around the current resolver/PDF reader: explicit date/season scope, dry-run default, remote-first execution, durable checkpoint state, bounded batches/request budget, resume after interruption, per-game/report outcomes and errors, idempotent reruns, and local mirror follow-through. Reuse existing readers rather than build another framework. Do not advertise a command as runnable until its parser and bounded live path exist and have been exercised.
4. Demonstrate interrupted/resumed execution on a tiny approved cohort, fresh readback and no-op repeat. Document one exact PowerShell-safe command for Chase to launch the broader recovery. No new unit tests; use the actual CLI/public consumer and lean data checks.

Owner launches the broad historical job after reviewing the audit and dry-run manifest. Full rescraping, inserting previously unretained statuses, resolving ambiguous start authority, and changing complete-team/omission policy are not silently authorized by an existing-row timestamp recovery.

The current `recover_nba_injury_report_times.py` accepts up to four explicit URLs; it is not yet a whole-history checkpointed recovery runner. The current targeted commands remain usable for small scopes.

## Historical audit and command-definition checkpoint — 2026-11-05

The bounded canonical remote audit is complete: `docs/operations/nba-injury-coverage-audit-2026-11-05.md`. Exact SQL/results are in `logs/receipts/nba_injury_coverage_audit_20261105/`. The parent independently replayed consumer selection from retained receipts. Only the known 12 pilot rows are currently usable; the remaining candidate rows are not proven recoverable and genuinely missing statuses remain unknown pending report comparison.

The requested resumable command is defined in `.hermes/plans/nba-injury-history-recovery.md`, including budgets, crash recovery, receipts, identity gates and remote-first mirror handling. Definition is complete; implementation and tiny write/resume acceptance remain pending. No broad job, DB write, sync, commit, push or deploy occurred.

**Newer checkpoint (2026-11-06):** Chase authorized the lean implementation. `scripts/recover_nba_injury_history.py` is implemented and its actual remote default-mode dry-run/resume is verified, including 11 newly planned observations for `0022400876`. Use `.hermes/plans/nba-injury-history-mvp-implementation.md` and the wiki for the current contract; the older hardened definition is not the implemented parser. Live execution/mirroring and broad history acceptance remain owner-run gates.
