# NBA production data: jobs, linking and readiness

The objective is reliable scheduled production updates, not a collection of manual recovery commands. A successful deployment or scraper exit is not proof that usable model inputs were produced.

## Kick off immediately

- This chat: repair the remote Daily Stats pipeline, then scheduled props linkage, schedule ingestion, mirror sync and bounded historical-readiness checks.
- Other chat: keep the separate NBA evidence/lifecycle implementation lanes isolated; this repair does not authorize training or changes to those contracts.
- You: use the linked bounded source retest commands; approve deployment, schema/state initialization and broader data operations separately. Retests diagnose the repaired automation, not a replacement manual operating procedure.
- Non-goals: DFS-table repair, new unit tests, trading/model-policy changes, destructive refreshes and unbounded archive movement.
- Read next: [repair contract](../operations/nba-automation-repair-2026-11-02.md), [incident evidence](../operations/nba-reconciliation-incident-2026-11-02.md), [post-sync checks](../operations/nba-post-sync-checks-2026-11-02.md).
- Current owner-run commands: [scraper retest handoff](../operations/nba-scraper-retest-handoff-2026-11-02.md). Scraper/Daily Stats/linker and follow-on props/game-lines/opponent code is integrated and passed 139 existing tests; no production execution or deployment is implied. Support-table mirror scheduling remains a separate gate.

## The two data paths

For immediate advanced-request resets or SOCKS connection closures, see [the locally verified connection repair](nba-advanced-scraper-connection-repair.md): direct Chrome-compatible transport is now the local default, while Railway remains CDN-only.

    NBA CDN / odds feeds
        -> Railway scheduled ingestion
        -> canonical remote source tables
        -> source-derived rolling features and verified identity links
        -> model consumers

    Canonical remote tables
        -> downstream mirror sync
        -> local Postgres for offline work

`DATABASE_URL` is canonical remote production; `LOCAL_DATABASE_URL` is a downstream mirror. Running a script on the workstation does not necessarily make it local-data-only. Railway cannot be assumed to reach the workstation's Postgres: sync ownership, credentials, reachability and its actual schedule must be verified.

## Why rolling averages can be stale after a successful scrape

The existing Daily Stats wrapper invokes several child programs. A critical upstream failure stops later stages. Its first scraper failure can therefore prevent both linking and rolling-average population. Conversely, a successful source catch-up does not prove that the scheduled wrapper or downstream derivation recovered.

The inspected rolling-average implementation excludes prior appearances below eight minutes from stat values and uses shifted history. Repairing orchestration must preserve those temporal/minutes rules. A catch-up must discover completed source dates and produce the corresponding outputs; merely using the server's yesterday date can leave missed dates behind.

## What a linker does—and does not do

A prop has separate identities: player, game and team. Filling one must not unnecessarily block independent verified fills of another. A player's team must be verified for that particular game; assigning the home team by default is unsafe.

A safe linker freezes a primary-key window, pages within it and updates only missing identities while checking that source fields have not changed. Unmatched or ambiguous references remain unresolved; they are not a reason to guess with fuzzy names or nearby dates.

The initial local repair supports an explicitly bounded recent-window path. Production callers still need proper bounds, resume behavior and output verification. Repeatedly taking only the latest 1,000 rows cannot establish that a large ingestion batch or historical backlog was covered.

## Advanced-dependent outputs need a second source gate

CDN-only ingestion cannot supply player advanced stats. The existing position-history, league-position and opponent-allowed programs depend on those locally produced rows. Missing advanced coverage can therefore yield an empty successful process or partial aggregates; neither is a healthy refresh.

The inspected local advanced wrapper omits the unified scraper's required season and then invokes a broad mirror sync even after scraper failure. Repair must pass a current explicit season, bound the actual advanced selector, and prevent failure from cascading into an unrelated broad sync. Do not move this source onto Railway. Opponent-allowed history must cover actual prior games/full season, not assume 17 calendar days can supply L15 and season totals.

## Future schedule versus completed-game stats

Future games legitimately have no completed-game box scores. Their identity must come from a fresh authoritative schedule, with exact home/away/date agreement and literal ten-digit competitive game IDs. Cup Championship `006` is excluded. Current mutable schedule state is not historical point-in-time evidence.

The inspected schedule migration and ingestion program exist, but database application and real in-container access remain separate acceptance gates. No `stats.nba.com` request belongs on Railway.

## Five different meanings of “done”

1. Code verification: the reviewed code runs through the repaired path.
2. Deployment: Railway runs the reviewed commit, with the intended schedules.
3. Production output: source dates, derived outputs and linkage actually advance.
4. Mirror integrity: the selected canonical rows are faithfully present locally.
5. Historical readiness: required games, archives and timestamp semantics support the explicitly approved cohort and decision clock.

Passing one does not imply the others. Missing historical quotes cannot be repaired merely by relinking current rows. Historical provider times must not be silently treated as collection/availability times.

## How this work is verified

Use existing scoped tests, bounded real-path smoke/integration checks, SELECT-only audits, lint and differential type checks. Do not create unit tests. Keep production data writes, schema changes and deployment explicitly scoped, with exact readback of affected outputs. The linked repair contract records current status and unresolved approval gates; this page explains the stable boundaries.
