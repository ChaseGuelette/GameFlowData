# NBA cached prop alias repair MVP

Owner request: investigate the unresolved aliases and provide a viable repair script before local sync. Scope is 649 player-alias tasks within the 657 unresolved events; quote/game-identity failures remain separate. No new odds requests, broad rescrape, schema migration, model work, sync or deployment.

Root cause established: all 13 required aliases are correctly mapped in the untracked `data/linker_data/player_mappings.csv`, but the new loaders read nonexistent root `linker_data/player_mappings.csv`. A separate legacy `src/processing/linker_data/player_mappings.csv` contains conflicting Lonnie Walker IDs. Promote only the 13 independently verified mappings into the shared code module; do not import either entire untracked mapping collection or create a second resolver. Preserve existing task API-cost metadata during zero-request cache replay.

Implementation (coordinator-owned):
- Add `src/processing/nba_player_aliases.py`: a small explicit alias map supported by canonical player/game evidence, applied only while the expected canonical ID/name still matches. Merge with existing manual aliases; conflicts remain ambiguous rather than overwritten.
- Wire that same map into the existing historical and daily linker context loaders in `nba_prop_snapshots.py` and `nba_linker_local.py`; no fuzzy resolver or new framework.
- Add `scripts/repair_nba_prop_aliases.py`: read only the unresolved player-alias task queue; consume already-cached responses; reuse existing parsing, exact linkage, team verification, T-60 selection and atomic importer. Default is read-only preview; `--execute` writes only existing compact rows/task status and diagnostic journal. A cache-only API object must prohibit HTTP even if a cache is absent. Explicit date/event bounds, resume from remote statuses, and per-event failure reporting.
- Update the existing T60 runbook with verified aliases, preview results and owner-run command.

Validation budget: canonical read-only identity lookup, affected existing linker tests, one real cached-data read-only preview of the repair queue. No new unit tests or recurring reviewer/writer rounds. Preserve unrelated injury work. Stop after the primary script path works; owner launches bulk writes.
