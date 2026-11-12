# NBA inference quote-volume reduction — source and backend deployed

**Current checkpoint — November 12, 2026:** Chase approved [the implementation scope](../../.hermes/plans/nba-quote-change-only-capture-2026-11-12.md). Source is published through `ccb292e1`: eight markets preserving active combos; no blocks/steals/double-double/triple-double or DFS capture; coordinated thirty-minute slots; change-only quote states with immutable capture receipts; current dashboard/backend/RPC, paper and T-60 readers updated. Edge refresh no longer restores withdrawn pregame offers, including an entirely empty quote result. Independent reviews, disposable-Postgres writes/permissions/cutoff checks, controlled public-path probes, Ruff/compile and integrated frontend production build passed. Existing suites remain 40 passed/4 unchanged baseline failures, plus 4 passing lines/resilience checks. The production migration is applied and backend deployment verified below; native scraper/real-slate volume measurements remain open. No agent-paid scrape, table deletion or model job occurred.

Owner follow-up: no new environment variable is required. `lines_job.py --live` now always passes change-only mode to its inference scraper; the optional wrapper flag remains only for command compatibility. Chase pushed this correction as `ccb292e1`, after initial code `65611ff6`. Railway automatically deployed the corrected push with no start-command change. Verify native capture output before claiming measured live row reduction. Coordinate receipt retention and producer/consumer rollback; do not delete authoritative receipts or duplicate the retention lane.

## Reviewed direction, superseding the initial sequence below

**Production checkpoint:** owner-pushed `ccb292e1` is deployed successfully as Railway `0bc6d98d-1f50-45ec-8993-c5fc8551ce5d`. Startup confirms the half-hour capture grid and existing 3 AM inference-prune registration. The production capture/current-view migration is applied with independent schema/grant readback. Native 11:42 ET edge refresh completed and logged persistence of 721 predictions; this is not proof of the next live scrape or measured unchanged-quote savings. See the retention runbook's November 12 follow-up before acting on handoff 155: preserve its completed historical backfill and combined-only dump decision, do not overwrite the new RPCs with the older cutover migration, and scope DFS/receipt retention separately.

- Recommended scheduled live scope: PTS/REB/AST/threes plus PRA/PR/PA/RA. Stop requesting blocks, steals, double-double and triple-double. A four-market base-only alternative also requires an explicit combo-product decision; combo quotes are not unused merely because forecasts derive from base samples.
- Remove DFS capture, preserve non-DFS book coverage, and skip started events before paid requests. Do not narrow execution books to the feature reader's DraftKings/Pinnacle pair.
- Capture every thirty minutes, merging the standalone 10:00 and full 12:01/16:01 scrapes into the half-hour grid. Preserve full-job injury/game-line behavior. Edge refresh is separate and remains unchanged unless separately requested.
- Change-only means line, either side's price, or offered-line membership changes. Write coherent state, not one independently changed side. Preserve alternate lines, reversions, restart safety and withdrawals.
- Add compact immutable event-capture receipts referencing existing quote rows. Unchanged successful polls append no inference quotes but retain genuine confirmation evidence. Update current readers, dashboard freshness, paper placement and T-60 selection together; do not mutate old timestamps or infer current availability from old history. This requires a coordinated producer/consumer slice, not only an insert filter.
- Existing T-60 row-cap/backlog recovery remains separate. No deletion, backfill or cap increase is authorized by this scope.

## Owner goal

Reduce new rows entering `nba_player_props_inference`; do not merely raise processing caps or prune old data. Code changes were approved and delivered; paid requests, production migration/deployment, deletion and existing-backlog recovery remain separately gated.

## Observed causes

- The Lines Job invokes the live scraper with `--combos`. The scraper resolves six core plus six combo markets, requests `us,us2,us_ex,us_dfs`, iterates all returned live/upcoming events and stores each capture with a fresh observation timestamp.
- The current paper policy selects PTS/REB/AST; the baseline also has prop-line threes inputs. T-60 compact features use those four markets. Both Over and Under prices remain necessary for coherent paired quotes and market-probability calculations even for UNDER-only betting.
- PostgreSQL live-row estimate was 27,474,620 at 2026-11-12 15:16:56 UTC; exact count hit a 15-second timeout. The table's actual date span has not been established.
- One affected game's materializer population was 11,484 repeated snapshots for 53 player/market combinations, not 53 market types. Increasing the 10,000 cap addresses this failure, not ingestion growth.

### Incident evidence retained for resume

Active deployment at diagnosis: `9384f8f6-57aa-4cc3-8b21-deb65e1064f8`, created November 9, CLI snapshot message `Fix Decimal NaN comparisons in NBA rolling features; preserve missingness`. November 12 native logs show the current-capture linker completing with `identity_ready=true`, zero conflicts and no unresolved ID; the following materializer returns `RuntimeError: per-event row cap exceeded`. The same cap-error class appears in November 10 logs. Scheduler's generic `NBA deferred — intermittent NBA lines linker failure is parked` label is misleading for this incident.

Canonical read-only query for exact event `f41d186dc7a7dcf01adab32579a82ca3` found 11,484 fully qualifying rows (bounded at 20,001, not capped), 53 unique player/market pairs, 15 players, four markets and one bookmaker. Game `0022600225` is Golden State at Houston on November 11. Compact feature rows for that game were zero. Schedule commence was November 12 02:30 UTC, while stored quote commence was 02:40 UTC; this separate discrepancy was observed, not resolved. Do not weaken timestamp/identity guards while reducing volume. Exact SQL was saved in Hermes scratch `nba_materializer_f41d186dc7a7dcf01adab32579a82ca3_sql_receipt.md` (temporary).

The recurring materializer loads all retained qualifying pre-T-60 snapshots, defaults to 10,000 rows/event and supports at most 50,000 by CLI. It reprocesses yesterday/today; `affected_rows=0` on successful events can be an idempotent no-op, not proof that all source data is absent. No cap increase or materializer repair was performed in this session.

## Initial proposal (historical; reviewed direction above takes precedence)

1. Restrict the scheduled NBA scraper to explicit `player_points player_rebounds player_assists player_threes`; check remaining active combo/dashboard consumers first. Drop blocks, steals, combo, double-double and triple-double requests from the paper pilot scope. Keep both sides.
2. Add explicit executable-book/region scope. Remove DFS capture for the retired DFS lane. Preserve Chase's actual execution books and the reference books required for pricing/features; do not silently narrow all books to DraftKings/Pinnacle based on the feature materializer's allowlist.
3. Bound capture to a useful pregame horizon and skip started games before fetching, rather than querying every returned upcoming event at every five-minute tick. Preserve observations needed before T-60 and prediction/entry times. Exact horizon and polling cadence require owner choice.
4. If repeated unchanged quotes still dominate, add state-aware capture suppression without losing paired-quote coherence or true observation/source timestamps. Never relabel an old snapshot as freshly observed; preserve enough availability evidence for T-60 and frozen entries. Do not introduce another odds table by default.
5. Separately finish/verify bounded retention after successful compact materialization. Local `archive_old_props_job.py` and scheduler already have concurrent uncommitted retention changes; coordinate that lane, do not overwrite it or claim deployed cleanup. Deletion needs independent counts and separate approval; keep unmaterialized core-market gaps.

## Acceptance

Compare actual inserted rows per completed capture before/after, not parser input counts (the delivered code now reports actual committed inserts). Verify required-market and paired-quote coverage, T-60 output, inference/recommendation/paper provenance and no unexpected API-credit increase. Report a measured row reduction on comparable slates; fewer configured market types do not imply a known percentage reduction in rows.
