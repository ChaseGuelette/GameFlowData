# NBA pretraining bounded data investigation — 2026-11-03

## Result

**BLOCKED for strict quote/injury acceptance.** Canonical remote Supabase was queried SELECT-only. Evidence is limited to eight exact games/dates: two each from seasons `22023`, `22024`, `22025`, and `22026` (`2026-10-26`, `2026-11-02`). This is triage, not whole-season coverage.

Receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-pretraining-investigation-cFuv2y/`

- `exact_sql.sql` — all SQL
- `compact_receipt.json` — exact compact results
- `aggregate_receipt.py` / `python_aggregate.json` — Python aggregation

**Query budget:** 11 SELECTs total; 2 metadata SELECTs; every query used `statement_timeout = 5s`. One broad representative-window lookup timed out and remains UNKNOWN; it was replaced by a cheaper exact-date indexed lookup. No DB writes, scraping, API calls, sync, jobs, training, source edits, or local-mirror claims.

## Bounded evidence

| Source | Sample result | Classification |
|---|---|---|
| Player props | Exact `game_id` lookup returned rows for only `0022600158` (`2026-11-02`): capped 200 rows, all with game/player/team IDs; 88 same-snapshot Over/Under keys and 24 one-sided keys. The other seven exact games returned zero linked rows. | **Linkage/source presence UNKNOWN**, not seven proven absences. Five sampled games have `game_id_map` rows, but `raw_player_props_combined.api_game_id` has no index, so an unlinked exact-external-game probe was not safely run. |
| Props clocks | In the 200-row `2026-11-02` sample, `snapshot_time` and `market_last_update` were populated and pre-T-60; all 200 lacked `bookmaker_last_update`. | Clock availability is partial. Historical-route authentication is **absent from retained DB evidence**; timestamps do not authenticate provenance. |
| Game lines | Four older exact games (`2023-11-01` through `2025-03-01`) returned 652 capped rows total. Samples had linked game/team IDs, populated clocks, and candidate same-snapshot multi-outcome keys. Spread keys grouped by absolute line and distinct outcome labels; the query did not prove opposite signs or correct team orientation. T-60 marginal counts were 498 for each clock column; this was not measured as a same-row intersection. The four later games returned zero linked rows. | Structural candidates only, not accepted coherent quotes; provenance remains unauthenticated. Later zeroes require linkage/source diagnosis before rescraping. |
| Injuries | Seven of eight exact dates returned rows (690 capped rows total); `2026-10-26` returned zero. Five historical dates through `2025-11-01` were inserted together on `2026-01-29`, so they are backfills, not contemporaneous availability evidence. `2026-11-02` showed three report labels and three scrape instants. | Rows exist, but **complete editions are not established**. `report_time` is text without timezone, and the unique key excludes report/scrape time, permitting edition collapse. |
| Schedule | `nba_game_schedule ... LIMIT 1` returned no rows. | Cannot serve as the current identity/commence authority. `player_game_stats` and `game_id_map` supplied sampled identities. |

## Required repair class

1. **Linkage first:** coordinate bounded, owner-controlled `staging_id` windows with the existing props linkage lane. Inspect/reuse the existing game-line linkage path before proposing any new implementation; this SQL investigation did not establish that a new linker is needed. Preserve nonnull conflicts. Do not treat remote-to-local copy as linkage repair.
2. **Sync second:** no sync-only gap was established because remote was canonical and local was not queried. After remote linkage/acceptance repair, perform only targeted remote-to-local table/window sync with same-key readback.
3. **Provenance/capture repair before replacement collection:** retain authenticated endpoint/route, requested snapshot, response status, raw-response digest or immutable receipt, inserted row-ID range/count, and market/bookmaker update clocks. Existing producer evidence has no row-bound payload receipts.
4. **Injury-edition reconstruction:** preserve an edition identifier, timezone-aware publication instant, raw document/response hash, expected/observed record counts, and explicit empty-edition receipts. Existing backfilled rows cannot reconstruct historical availability by themselves.
5. **Scoped rescraping only after the above checks:** the only sampled exact-date absence proven by an indexed injury lookup is `2026-10-26`. Re-acquire that date’s named editions after edition retention is fixed. For quote zeroes, first prove absence with bounded indexed/linker evidence; do not infer a three-season rescrape from these samples.

## Unknowns

Whole-cohort coverage, archive contribution, exact-row provenance, omitted-player meaning, complete injury editions, same-row three-clock T-60 eligibility, unlinked quote presence for external game IDs, local parity, and all unsampled games remain UNKNOWN. Missing records in these samples do not establish whole-history absence.

Coordinator review: capped samples used `LIMIT` without deterministic row ordering. Pair/one-sided counts describe only those captured samples; the cap itself may separate opposite sides. No same-row authenticity, price-conflict audit or exhaustive pairing acceptance is established by these counts. Existing records/archives may still avoid replacement collection; the October 26 absence is specific to the queried canonical injury table.
