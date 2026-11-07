# NBA compact historical prop lines — investigation and proposal

Date: 2026-11-05. Status: owner approved implementation and remote migration; code/verification in progress. Broad backfill remains owner-launched.

## Owner-approved implementation update

Chase selected T-60, confirmed game-level eligibility (any linked player prop qualifies the game, not every player), and authorized both a compact historical feature table and a separate short-retention daily inference table. The one-time historical rescrape MUST be a new script; `src/scrapers/player_prop_scraper.py` remains unchanged. Reconcile historical and forward daily capture through shared selection semantics. Migration application does not authorize a deployment, old-table cleanup or an agent-launched full scrape. Implementation scope: `.hermes/plans/nba-props-t60-implementation.md`; operating instructions will be in `nba-props-t60-runbook.md` after integration.

## Decision summary

Chase requested ownership of props coverage/linkage, remote-table investigation, review of existing historical scrapers, and a compact historical training source instead of routinely querying enormous raw tables. Chase prefers considering a targeted rescrape over archive salvage. This document recommends that direction, conditional on a small source/linkage pilot; it does not claim rescraping is cheaper in dollars or that historical coverage is guaranteed.

Approved slice: a dedicated `nba_player_prop_feature_lines` table containing one selected line per canonical game/player/market under frozen `t60_v1`, populated directly from a new bounded historical scraper, plus `nba_player_props_inference` for forward daily quotes. The original proposal below is research context; the owner update and implementation runbook supersede its deferred-live-table sequencing.

## Remote evidence

Canonical remote Supabase was inspected in read-only transactions with a verified 15-second statement timeout. There were 16 SQL probes plus one table-inventory call. Large-table samples used indexed access; no archive scan or database write occurred.

| Relation | Planner row estimate | Reported total size | Finding |
|---|---:|---:|---|
| raw_player_props_combined | 107,050,112 | 63.59 GiB | Indexed staging ID and canonical game/player identities; not a trustworthy compact source without validation. |
| raw_player_props_archive | 171,185,488 | 43.52 GiB | No indexes discovered; planner statistics suggest old history but cannot certify exact coverage. |
| raw_game_lines_staging | 3,127,596 | 1.32 GiB | Team/game lines, not player props. |
| nba_game_schedule | 1,206 | — | Current statistics indicate the 2026–27 schedule; cannot assume full historical tipoff coverage. |
| game_id_map / game_id_map_staging | approximately 6,600 each | — | Existing indexed mapping candidates; identity/date correctness still needs inspection before reuse. |

No compact NBA historical feature-line table surfaced. An empty `raw_player_props_live` exists; its name is not proof of an implemented live-cache contract.

Six historical game IDs returned zero retained combined-table rows: `0022300777`, `0022300804`, `0022300821`, `0022400064`, `0022400413`, `0022400790`. This is sampled linked lookup evidence, not whole-season absence. Archive statistics span May 2023–April 2026; no indexed archive access was available, so exact archive coverage remains unknown.

Confirmed bad identity: staging row `246329485` has commence time May 24, 2026 but links to `0022500835`, whose player stats date is February 24, 2026. Prop and player-game team IDs also disagree. Latest 100 rows by staging ID had pregame clocks but all lacked canonical game/player IDs. Fresh ingestion therefore does not establish successful linking. The existing linker root cause has not been established; do not reuse mappings blindly.

Exact SQL and compact result receipts:

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_remote_investigation_2026-11-05/queries.sql`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_remote_investigation_2026-11-05/compact_results.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_remote_investigation_2026-11-05/summary.md`

These scratch receipts are temporary; the decisive findings are retained here. No full-season certification is implied.

## What the model actually needs

`src/models/features/nba/training_loader.py:452-469` loads numeric centering features for points, rebounds, assists and threes from exact player/game identities. It permits Pinnacle/DraftKings and picks the newest observation per market. The current date-level/pre-commence filters are not a full decision-timestamp policy; null commence times are allowed and timestamp ties lack a deterministic book tie-break.

`src/models/features/nba/line_sources.py:57-78` implements the equivalent compatibility feature query. Neither path requires a paired Over/Under quote merely to supply a numeric feature. Existing missing-line zero imputation is consumer behavior, not a valid source record; never insert fabricated zero lines into the new table.

Separately, `HistoricalFeatureLoader._filter_training_linkage` (`historical_loader.py:179-228`) keeps a GAME when any raw row has that game ID and a non-null player ID. It is not currently a per-player/per-stat line-coverage gate. Replacing its source is distinct from tightening its semantics. Preserve owner-approved cohort policy and explicit empty-population failure; report per-player/stat availability separately rather than silently excluding every missing-line player.

Live executable selection in `src/models/daily_runner.py:832-923` needs bookmaker-specific lines with Over and Under prices, then selects a paired quote. Historical feature storage must not silently replace or narrow live execution. A single directionless line loses the prices and provenance needed for execution.

## Proposed historical contract (not applied)

Table: `nba_player_prop_feature_lines`.

Grain: `(game_id, player_id, market_key, selection_policy_version)`; the active version yields exactly one row per player/game/stat. Consumers pin one version explicitly. A revised policy is a new version, not an unnoticed overwrite of a training dataset.

Proposed fields:

- Canonical `game_id`, `player_id`, `market_key`; `game_date` and `season_id` for bounded training exports.
- `line`, `bookmaker`; optional coherent `over_odds`/`under_odds` retained when returned, not required for the feature itself.
- Literal `api_game_id`, `api_player_name`, provider home/away teams; do not normalize an external ID into an NBA ID.
- `decision_time_utc`, `requested_snapshot_time`, actual `source_snapshot_time`, `market_last_update`, `bookmaker_last_update`, `fetched_at`.
- Provider `commence_time` plus independently reconciled canonical game-start cutoff and cutoff source. Record discrepancies; do not manufacture midnight tipoffs from dates.
- Selection-policy version, capture task ID, source payload reference/checksum, and mapping method/version.

New-table keys/indexes should support the unique grain and season/game-date exports. No new index on existing giant tables is proposed. Match canonical ID types to actual schema during implementation; do not introduce speculative foreign keys to a current-season-only schedule. Backend writes only; client access is denied unless separately required and reviewed.

Only validated rows enter the feature table. Preserve ambiguous mappings, unavailable markets and timing failures in a small capture-task/rejection record rather than pretending they are missing healthy/zero evidence. Retain bounded compressed source responses outside the feature table so selection can be reviewed without paying to fetch the same snapshot again. Storage location/retention is an explicit implementation decision.

### Selection policy to approve before spending credits

- Initial feature markets: `player_points`, `player_rebounds`, `player_assists`, `player_threes`.
- Use the existing allowed-book set, Pinnacle/DraftKings, with a deterministic rule. Proposed: newest valid market observation, then fixed book precedence for ties. Audit simultaneous line conflicts; no best-price/ROI-driven historical selection.
- Match historical timing to the intended live decision policy. T-60 is a candidate, not an approved default. A closing line is before tipoff but may still be unavailable to an earlier inference run. One fixed snapshot cannot support arbitrary earlier-time replay.
- Enforce actual source snapshot and applicable quote-update times no later than the decision cutoff, with decision cutoff strictly before the reconciled game start. Check returned event commence time again after the odds response. Reject unknown/conflicting timing under this compact contract.
- Do not use `fetched_at` (today's rescrape time) as historical availability.
- Resolve event by verified existing external mapping plus team/date/time consistency; otherwise unique team/date/time candidate. Resolve player by explicit alias/identity evidence with game/team consistency. Ambiguous or conflicting mappings remain rejected, never guessed.

A single historical row cannot serve as a mutable live latest-quote cache while preserving reproducible past decisions. Keep historical rows frozen/versioned; live feature queries may share selection logic without overwriting historical evidence.

## Existing scraper reuse

Primary: `src/scrapers/player_prop_scraper.py`:

- `get_events_for_date` (90–108): historical event discovery.
- `scrape_event_props` (110–157): per-event historical request and usage header.
- `parse_and_store` (159–217): bookmaker/market/outcome parsing.
- `_batch_insert` (219–236): currently writes directly to the huge combined table; do not use this sink for compact backfill.

Useful references: `src/scrapers/daily_player_props_scraper.py` for live/historical endpoint orchestration; `src/scrapers/probe_regions.py` for book/region coverage diagnostics; `scripts/sync_nba_props_by_id.py` for bounded execution/journal ergonomics, not historical scraping or canonical linking.

Required changes before reusing the bulk scraper:

1. Replace fixed UTC daily snapshots with the approved per-event decision cutoff.
2. Retain the response wrapper's actual snapshot timestamp; current fetch helpers discard it and label the requested timestamp as `snapshot_time`.
3. Request only required markets/books, then reduce in memory before compact-table writes; avoid all-book/all-outcome historical raw growth.
4. Distinguish successful empty response, permanent invalid request and transient failure. Current event progress handling can mark failed requests complete.
5. Task-level resume keyed by event/request time/markets/books/policy, including successful empty results; resume before API calls.
6. Hard request/credit budgets, conservative retry behavior and actual usage-header accounting, including discovery. Existing dry-run is an estimate, not proof of actual event coverage/cost.
7. Remote-first bounded upserts, atomic task completion and selected-row persistence, then targeted downstream local export/sync. No whole raw-table sync.

The Odds API official historical documentation supports player-prop history from May 3, 2023 and returns the closest snapshot at or before the requested timestamp. This establishes product availability, not coverage for each required book/player/game. Historical event cost depends on markets returned and region groups; use actual response headers and a bounded pilot before estimating the full run. References checked: official historical-data page and V4 guide, historical event odds section.

## Suggested sequence and approval gates

1. Approve selection timing, deterministic book rule, exact season/date cohort and pilot credit cap. Existing direction is fit 2023–24/2024–25, calibrate 2025–26, test 2026–27; preserve existing season resolution and do not broaden dates implicitly.
2. Prepare the additive compact schema and minimal scraper/selector adapter. Inspect existing mapping/linker code before writing a new resolver; no giant-table mutation or archive scan.
3. Execute only an explicitly authorized small pilot spanning fit/calibration seasons, an early tipoff and an identity edge case. Measure book/stat coverage, timestamp correctness, mapping acceptance, task resume and actual credit usage. Include a mismatched-date fixture that must be rejected.
4. Review pilot and whole-cohort task/credit estimate. Chase launches any broad population job.
5. Verify compact uniqueness, valid timing, deterministic reruns and season/player/stat coverage. Wire both training source/gate and model feature consumer deliberately; verify public loading path, not merely SQL existence.
6. Keep live raw ingestion intact during this slice. Later, optionally maintain a small current paired-quote cache keyed by game/player/market/book/line, updated in place with coherent prices and freshness metadata. Retention/pruning of existing raw/archive storage is a separate approval.

## Relevant prior lessons/invariants

- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: timeout is not absence; no unindexed archive salvage by default.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: enforce the cutoff end to end; a timestamp column alone is insufficient.
- `operations/critical-invariants`: isolated SQL runner, no non-concurrent index on the huge combined table, owner-launched long jobs, empirical-CDF probabilities unchanged.

No DB migration, paid request, source-code change, training, deployment or raw retention change occurred. Concurrent injury-timing work remains outside this lane.
