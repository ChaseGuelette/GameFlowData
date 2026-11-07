# NBA injury history recovery command definition

**Superseded for the first implementation:** Chase requested a smaller working MVP. The implemented contract is `.hermes/plans/nba-injury-history-mvp-implementation.md` and the operator guide is `docs/understanding/nba-injury-timing-reconciliation.md`. The detailed manifest framework and flags below are retained design history, not the current CLI parser. Use `scripts/recover_nba_injury_history.py --help` for actual supported flags.

> **For Hermes:** Use subagent-driven-development and the GameFlow implementation-worker lane after implementation is authorized. This document defines the command; it does not claim a new CLI exists.

**Goal:** Recover supported publication evidence on existing NBA injury rows with bounded remote-first operations, crash-safe resume and separately approved local mirror follow-through.

**Architecture:** Reuse the existing game timing resolver and official PDF parser/matcher. Drive them from a deterministic, reviewed manifest; keep audit evidence, immutable input manifest, execution checkpoint and per-item receipts separate. Never turn a missing timestamp or unmatched status into a guessed observation.

**Tech stack:** Existing Python environment, SQLAlchemy/Postgres, existing official PDF reader, JSON manifests/checkpoints and content-addressed local captures. No new service, database table or dependency is proposed.

## Status and authority

- Completed: canonical remote SELECT-only audit on November 5, 2026. See `docs/operations/nba-injury-coverage-audit-2026-11-05.md` and `logs/receipts/nba_injury_coverage_audit_20261105/manifest.json`.
- Defined here: CLI contract, safety limits, checkpoint/receipt semantics, evidence gates and implementation/verification tasks.
- Not implemented here: the proposed `scripts/recover_nba_injury_history.py` orchestrator, URL discovery, checkpointed execution or mirror integration.
- Not authorized/run here: DB writes, recovery downloads, historical backfill, local sync, training, commit, push or deployment.
- Preserve all concurrent props and injury source changes. Do not modify the props fitting predicate or training semantics.

## Evidence that determines scope

| Cohort | Stats games | Effective starts | Injury rows on cohort dates | Valid publication / pre-cutoff rows | Null game IDs |
|---|---:|---:|---:|---:|---:|
| 22023 fit | 1,230 | 0 | 19,720 | 0 / 0 | 5,604 |
| 22024 fit | 1,230 | 2 | 21,487 | 12 / 12 | 6,110 |
| 22025 calibration | 1,321 | 0 | 25,362 | 0 / 0 | 8,640 |
| 22026 initial test | 121 | 121 | 624 | 0 / 0 | 624 |

Scope is the observed `team_game_stats` season population through 2026-11-05, not the eventual eligible fitting cohort. The 22025 population includes 85 playoff and six play-in games; freeze the intended game-type policy before generating an executable recovery manifest. Do not silently alter calibration scope.

67,181 existing rows lack consumer-valid publication evidence. They are a candidate pool, not 67,181 proven recoverable statuses. Null identity counts overlap: 318 player IDs, 96 team IDs and 20,978 game IDs. Populated IDs are not automatically validated IDs.

Only three games received raw start-source probes. Most absent timing rows have unexamined source recovery potential. Game `0022300555` has a verified start conflict; game `0022400875` has agreeing odds starts but the prior official PDF scheduled time differs by ten minutes. Preserve that distinction. The 12 pilot observations precede either cutoff; this does not certify odds time as exact scheduled tipoff.

## Recovery categories and ordering

1. **Start inspection/reconciliation:** absent effective starts become `start_uninspected`, not `source_start_missing`. Run bounded resolver dry-runs by exact game/date; retain schedule, odds fallback, no source, null-only, conflict, date mismatch and overflow outcomes. Never use majority/minimum/maximum to resolve conflict.
2. **Identity gate:** independently verify exact game/date/team/player identity before publication matching. Null, ambiguous or incoherent identity becomes `identity_blocked`. The current publication script selects exact game IDs and cannot recover null-game rows as-is. Linker repair is a separate reviewed scope, not an automatic step in this command.
3. **Report evidence:** use explicit verified official URLs first. No filename-clock interpretation and no guessed RapidAPI clock mapping. Printed header with America/New_York conversion is the publication authority; retain `official_reconstruction`, not historical local capture provenance.
4. **Existing-row match:** unique exact normalized date/team/player/status/reason matches may receive additive evidence. Preserve original observations and flattened fields.
5. **Unmatched report rows:** distinguish `identity_ambiguous`, `status_or_reason_differs`, `outside_target_game`, `no_existing_status_candidate` and parser failures. An unmatched PDF row alone is not proof of missing database status. Only a scoped identity/status comparison can classify a genuinely unretained status; insertion remains out of scope.
6. **Consumer verification:** evidence may be attached without claiming usability; missing/conflicting starts or after-cutoff reports remain explicitly unusable. Verify latest eligible edition through the existing public consumer after remote readback.

A complete execution accounting is not complete injury history, complete team reporting, healthy-by-omission permission or training acceptance.

## Proposed CLI contract — NOT runnable yet

Proposed file: `scripts/recover_nba_injury_history.py`.

Required input: `--manifest PATH --state-dir PATH`.

Modes:
- Default: offline manifest validation/planning. No network or DB mutation; may write only its own plan/receipts.
- `--probe`: bounded remote SELECT and approved official URL retrieval; no DB writes. Downloads count against request limits and are persisted by digest.
- `--execute`: apply only approved/revalidated manifest items remotely. Requires a prior successful probe with matching manifest and policy fingerprint; no implicit migration.
- `--resume`: resume the same run identity and immutable manifest; never infer progress from a date watermark alone.
- `--mirror-local`: separate explicit local-write approval, valid only with `--execute` or with `--resume` of a remotely verified execution. Without it, report `remote_verified_mirror_pending`.

Safety defaults (proposed, not existing parser behavior):
- `--max-games 10`: maximum distinct games processed per invocation, hard ceiling 100; work remains in small groups of at most 10 games on one date.
- `--max-requests 4`: maximum HTTP attempts per invocation, hard ceiling 20; retry attempts and index requests count, including failed requests. Cache hits do not.
- `--max-attempts 2`: per-report attempts per invocation, hard ceiling 3; backoff bounded by the invocation deadline.
- `--max-seconds 300`: total invocation wall-clock deadline, checked before each DB/network operation. Each DB statement remains <=15 seconds, writable lock timeout <=3 seconds, each HTTP attempt <=15 seconds.
- Retain existing PDF limits: <=5,000,000 bytes/document and <=25 pages/document. Retain <=2,000 injury rows per batch, with overflow refusal; exact raw-start probes <=2,001 rows/game, overflow unknown.
- Manifest must include a finite approved total request ceiling across resumes. Persist/reserve each request attempt before issuing it. A crash can consume a reservation without a response, but cannot reset the ceiling. Raising this limit requires a revised approved run, not a silent resume override.
- No-argument invocation and `--help` never connect, fetch or create a DB/schema.
- Default target is canonical remote; no local-only recovery mode. Verify masked target identity, refuse identical remote/local routes, and never print credentials.

Illustrative future PowerShell invocation (NOT available until implementation and validation):

` .\venv\Scripts\python.exe scripts\recover_nba_injury_history.py --manifest logs\receipts\nba_injury_recovery\approved_manifest.json --state-dir logs\receipts\nba_injury_recovery\run01 --probe --max-games 10 --max-requests 4 --max-seconds 300 `

The referenced approved manifest does not exist yet. Do not offer this as a launch-ready command. A broad execution command is issued only after the tiny-cohort crash/replay gate passes.

## Immutable manifest

Versioned JSON containing:
- Schema version, audit receipt paths/digests, creation instant, source revision plus dirty-source content fingerprint, selection policy and approval reference.
- Explicit season IDs, game-type allowlist, inclusive date bounds and fixed audit cutoff. Do not infer dates solely from a season label.
- Deterministically sorted unique `(game_date, game_id)` targets, exact team/player/row identifiers where established, and links to the supporting audit query/result.
- Per-game start state/basis and unresolved reason; expected game/team identity; explicit candidate row IDs. Date-only candidates remain non-executable until identity is established.
- Explicit report URLs with discovery evidence; expected digest/header when already known. Same URL yielding different bytes is a new evidence version, not permission to silently reuse an approved match plan.
- Match plan: `(injury_row_id, capture_sha256)`, intended observation digest and precondition fingerprint of identity/status/reason. Include proposed timing changes separately.
- Allowed stages, per-run request and row-update ceilings, remote target fingerprint, optional approved mirror tables/keys.
- Unresolved items with reasons. Zero verified report URLs is `discovery_blocked`, not a successful empty recovery.

Audit-only gap manifests may reference all candidate IDs. Execution manifests may include only reviewed, independently matched targets. Never expand an execution manifest automatically from unmatched PDF rows or a newer database snapshot.

## Checkpoints and receipts

Use an exclusive run-directory lock; reject concurrent writers. Persist checkpoints with same-directory temporary file, flush/fsync and atomic replace. Retain append-only sequenced attempt receipts; detect truncated/corrupt state rather than silently starting over.

Logical work identity is `(stage, game_id, game_date, report_url)`; capture evidence identity is its downloaded body SHA-256, and write identity is `(injury_row_id, capture_sha256)`. A document may cover several games: cache once but keep per-game outcomes. PDF bytes and parser output are content-addressed; verify their digest on reuse.

States:
- `pending`, `probe_complete`, `planned`, `write_prepared`, `commit_unknown`, `remote_committed_unverified`, `remote_verified`, `mirror_pending`, `mirror_verified`.
- Explicit non-success outcomes: `start_conflict`, `start_unavailable`, `identity_blocked`, `discovery_blocked`, `unsupported_report`, `ambiguous_match`, `after_cutoff`, `no_existing_status_candidate`, `retryable_error`, `terminal_error`.
- `already_present` requires readback of the exact intended observation and identity, not a trusted old checkpoint.

Every receipt records run/manifest/source fingerprints, stage and item IDs, attempt timestamps, target route, verified transaction guards, input bounds, request reservations, source URL/digest/header, counts/IDs planned/changed/read back, match/block reasons, before/after observation fingerprints and consumer acceptance. Counts of rows and counts of observations are separate. One row can acquire several captures; overlapping missing-identity categories must not be summed as distinct rows.

Dry-run and execution checkpoints are distinct: a planned item never becomes executed by resuming. Budget exhaustion/checkpointed interruption reports partial progress, not success. Suggested exit codes: 0 all selected work verified (or offline plan valid, clearly labeled); 2 invalid inputs/identity; 3 bounded stop with remaining work; 4 retryable failure; 5 blocked/unsupported items; 6 readback or integrity failure. Receipt mode always disambiguates planning from recovery.

## Crash-safe execution and idempotency

1. Revalidate immutable manifest, source fingerprint, DB route and expected identities/status/reason before each bounded batch. If data changed since the probe, stop that item for replanning.
2. Persist `write_prepared` with exact intended updates before opening a write transaction. Use narrow row locks or conditional updates covering the identity/status preconditions; the current pilot's ID-only write is insufficient protection for a long-running stale plan.
3. Merge only the approved observation into JSONB; preserve unrelated concurrent observations. Do not overwrite flat status/reason, replace the whole observation map or insert missing statuses.
4. Commit and independently read back exact IDs and observation contents from a fresh read transaction. Persist `remote_verified` only after this succeeds.
5. If interrupted between commit and checkpoint, resume with SELECT readback: matching observations are already present; missing ones are re-planned; changed or conflicting identity/status/capture is blocked. Never replay blindly or assume rollback.
6. Repeat execution with identical approved evidence must produce zero new changes and verify the same evidence. Separate `changed` from `already_present` so a safe concurrent no-op is not reported as corrupt merely because it differs from an old planned-change count.
7. Timing reconciliation follows its existing schedule-first/conflict-preserving rules and is separately receipt-linked. Persisted absence does not authorize 3,902 unrestricted raw-lines probes in one invocation.

## Local mirror follow-through

Remote verification must precede any local write. Do not refetch/reconstruct independently into local storage: copy the verified canonical rows and preserve source evidence exactly.

Inspected `scripts/sync_local_db.py` has a newer `--nba-support` path: explicit table allowlist, <=31-day window, row cap, schema/PK preflight and transactional UPSERT independent of the local watermark. `rapidapi_injuries` uses `report_date` in that path. It refuses an existing state-file rather than resuming it. Therefore use a new child receipt per mirror attempt and track retry state in the recovery orchestrator; re-read the current code at implementation because this script is concurrently modified.

Do NOT use generic `--full --tables rapidapi_injuries`: the inspected generic full path commits `TRUNCATE ... CASCADE` before copy. Generic dry-run is not assumed globally side-effect-free. Do not use incremental max-date sync for old changed rows.

For publication mirroring, prefer an approved exact-key copy if implemented and verified; otherwise the support-table date-window UPSERT is a broader local-write scope and requires explicit approval of that row set. Preflight schema/PKs and bounds, record that scope, then independently compare exact recovered IDs/observations with remote using UTC-normalized timestamps.

`nba_injury_game_timing` is not in the inspected NBA support registry. Timing-table mirroring therefore needs a separate bounded exact-key adapter or separately reviewed registry addition; do not advertise that both tables already sync through the support mode. Do not reconstruct timing independently on local stale sources. If either mirror stage is unsupported or fails, retain remote success and explicit mirror-pending status; never re-run remote writes merely to retry local work.

## Implementation slices and verification gates

1. **Manifest and safety parser** — create proposed `scripts/recover_nba_injury_history.py`; validate all arguments, source/manifest fingerprints, routing, limits and locks before IO. Exercise `--help`, no args, malformed IDs/dates, duplicate/conflicting targets and mode conflicts with DB/network access forbidden.
2. **Bounded probe and captures** — reuse `src/processing/nba_injury_game_timing.py`, `src/scrapers/nba_official_injury_reports.py` and current matching code in `scripts/recover_nba_injury_report_times.py`. Extract narrowly if required rather than duplicating matching semantics. Test cached-document hashes, request accounting, timeout/404/unsupported report, overflow and null identity. Exact URL discovery is initially an explicit reviewed input, not a guessed whole-history crawler.
3. **Checkpoint/remote write orchestration** — wrap existing additive behavior with identity preconditions, atomic receipts and commit-ambiguity readback. Offline failure injection at before request, after download, before commit, after commit/before checkpoint and before readback. Verify finite budgets survive restarts and dry-run never writes DB.
4. **Mirror adapter** — read current `scripts/sync_local_db.py` before any change; preserve its concurrent owner scope. Implement exact-key timing mirror separately unless an explicitly approved integration is smaller. Verify mirror failure/retry does not repeat remote effects.
5. **Tiny live acceptance (separate write approval)** — pilot `0022400875` as already-present/no-op control, `0022300555` as conflict control, `0022600160` as identity-blocked control. A positive new-update case requires an additionally approved unique match; do not fabricate it from the no-op pilot. Verify the actual public consumer before/after cutoff and without cutoff, original evidence preservation, interruption/resume and independent fresh readback.
6. **Owner launch gate** — report all blocked outcomes, exact manifest, proposed broader budget, source commit/fingerprint and mirror scope. Only then supply one PowerShell-safe owner-run execution command. No deployment or recurring task is included.

No new unit-test suite is required by the existing injury MVP scope; use scoped compile/Ruff, parser checks, deterministic failure-injection exercises and actual tiny-path verification. Do not mistake these planned checks for tests already run.

## Outstanding decisions

- Confirm calibration game types before execution manifest freeze; the audit intentionally included the observed postseason rows.
- Approve a bounded URL-discovery/comparison stage before claiming how many existing rows are recoverable or how many statuses are genuinely missing.
- Approve identity repair independently for the null-game population, especially current season.
- Approve start authority conflict resolution separately if new official evidence disagrees with odds; this command does not choose a new authority policy.
- Approve implementation and then tiny DB writes/local mirror scope separately. Broad jobs remain owner-launched.
