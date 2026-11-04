# Injury linker reuse assessment — November 4, 2026

**Final code-review verdict: option 2 — retain the existing DB-table/linker workflow with narrow ingestion/date/identity-preservation adjustments. No replacement matching system is needed.** The bounded file-only cross-check completed and agreed that the matching functions are reusable. No live DB queries/writes or running-scrape changes were made.

**Owner correction:** Chase wants API data in the existing injury DB table, then the linker filling previously empty IDs on those same rows. Do not introduce a staging table, linking table or file-only linking pipeline as the primary workflow. The current recovery scraper actually writes files only; no staging/linking table was created. Those captures remain usable as the input to an import into `rapidapi_injuries`, without redownloading. The file-adapter proposal below is superseded as the primary delivery architecture; it documented compatibility, not the owner's preferred workflow.

**Revised MVP:** import the completed captures into the existing injury table while preserving request/game dates and distinct report observations; then reuse the existing player/team linker against those rows. Add date/team→game-ID enrichment where needed, independent of props. Check the actual schema before deciding whether narrow column/uniqueness changes are necessary, and scope writes to the imported population. Do not invoke unconditional CLI DDL during a read-only preview. Keep the running scraper and its outputs untouched. This is an ingestion compatibility change, not a reason to replace the identity-matching logic.

## Existing path and reusable owners

`src/processing/link_injury_data.py` is the RapidAPI linker, not the ESPN scraper/storage path.

- `load_reference_data` (114–148): reads `teams(team_id, team_name)` and `players(player_id, player_name)`.
- `match_team` (193–205): exact team-name lookup, then the existing alias normalizer.
- `match_player` (208–248): manual override, normalized exact match, then existing fuzzy suggestions/threshold logic.
- `build_injury_mappings` (251–320): reads distinct `(player, team)` pairs from `rapidapi_injuries` where player_id is NULL.
- `bulk_update_injuries` (325–394): writes player_id/nba_team_id for matching name/team groups where player_id remains NULL. This is identity linking, not game linking.
- `write_unmatched_report` (469–488): reusable unresolved-name report.

The old feature reader already derives the game association from date/team: `src/models/features/nba/historical_sources.py:241–247` joins game records to injuries on `report_date = game_date` and `nba_team_id` belonging to the game's teams. Props are not part of that join.

## Exact compatibility differences

| Difference | Minimum change | Why |
|---|---|---|
| New scraper produces normalized/quarantine files; old linker reads an existing DB table | Add a file-input mode or thin caller around the existing match functions; load reference lookups once | Running the old command now would link old DB rows, not the newly scraped files |
| New records preserve requested_date and payload game_date separately | Link by payload game_date; retain requested_date and capture lineage as separate fields | One response includes next-day games; the old `store_records` stamps every row with its request date |
| Old linker writes player/team IDs only | Resolve `game_date + nba_team_id` through historical game/stats records, requiring a unique game | Needed to associate injuries with actual NBA games, independent of props; an OUT player need not appear in the target game's played-player rows |
| Old DB writer deduplicates without capture/report-cycle identity | Keep linked file rows separate by capture SHA/record ordinal; do not reimport through the old writer unchanged | Otherwise observations from separate captures can disappear again |
| Old null-team fallback uses nearest player game within ±30 days | Do not use that fallback for this historical file import; unresolved teams remain unresolved | It can select a later game/team across a trade; the source file path already quarantines absent team fields |
| CLI runs `ensure_columns` before checking dry-run/report-only | New file mode branches before schema setup and legacy bulk updates | Existing --dry-run is not a guarantee of zero DDL; no schema operation is needed merely to build linked files |

Storage references: `src/scrapers/rapidapi_injury_backfill.py:236–278` (`store_records`) and 62–85 (table/unique key). Null-team fallback: `link_injury_data.py:399–460`. CLI ordering: `link_injury_data.py:539–584`.

## Smallest useful implementation

1. Add a file mode to `link_injury_data.py` that consumes the completed recovery partitions and calls the existing team/player matchers. Keep the legacy DB mode intact.
2. Read NBA reference identities and the date/team→game mapping; do not query props.
3. Add player_id, nba_team_id and game_id to each linked record while retaining original names, status, request/game dates and capture identity.
4. Write `linked_injuries.jsonl` plus a compact unresolved/conflict report. Quarantined statuses remain quarantined; successfully matching a name does not repair an invalid status.
5. For an MVP, automatically accept manual/unique exact mappings and retain fuzzy/ambiguous candidates for review using the existing suggestion output. Do not introduce another fuzzy-matching library or scoring framework. The current matcher picks the highest score above its threshold without a runner-up-gap check; static compatibility is not proof every fuzzy match is correct.

This first deliverable is linked evidence files using DB reference data. A subsequent approved DB import must retain the corrected dates and capture identity; it should not casually reuse `store_records` and its lossy unique key. No DB migration/import is authorized or run by this assessment.

Publication/edition/T-60 acceptance is separate from identity linking and is not solved by changing player/team IDs. It need not delay building this small linking path.

## What not to rebuild

Keep the existing team aliases, player normalization/manual mapping support, fuzzy suggestion code and unmatched report. Do not use `src/scrapers/injury_database.py` as an importer for this feed: that class serves ESPN injury fields/IDs, whereas these captures come from the existing RapidAPI feed.

## Verification boundary

This review establishes code reuse and identifies actual schema/IO differences. It does not establish the live match percentage, current reference-table completeness or successful DB ingestion. One completed partition/reference snapshot is sufficient for an initial populated probe after implementation approval; no broad investigation or hardening framework is proposed.
