# NBA Injury API Recovery Pilot Implementation Plan

> **For Hermes:** Chase approved pilot implementation and execution on November 4, 2026, followed by scoping (not running) the full scrape. Execute Tasks A–C with lean validation; Task D remains evidence-gated. Do not repeatedly seek the same pilot approval.

**Goal:** Determine whether the existing basketball injury API can supply defensible historical T-60 injury inputs, and build the smallest non-destructive capture/repair pilot that exposes—not conceals—remaining source gaps.

**Architecture:** Keep the existing RapidAPI subscription/client as the primary source. First replay already captured responses into a file-only normalization/audit bundle; then make a capped adjacent-day capture if authorized. Change the historical feature reader only after report identity, publication availability and team completeness are supported by actual source evidence.

**Tech stack:** Existing Python venv, requests, stdlib JSON/hash/date handling, existing NBA injury aggregation and offline guard. No new dependency, database, service, scheduler or PDF ingestion pipeline for the first slice.

**Status:** TASKS A–C IMPLEMENTED AND EXERCISED; SOURCE AUDIT BLOCKED. Three live requests succeeded; combined replay retained 625 records with seven quarantined. The canonical target selection found zero eligible games among 17 bounded candidates, and the audit correctly returned exit 2. Native publication/edition integration (Task D), DB repair and training remain unimplemented. See [delivery report](../../docs/operations/nba-injury-pilot-delivery-2026-11-04.md) for actual commands/receipts and MVP deviations; see [full-scrape scope](2026-11-04-nba-injury-full-scrape.md) for the requested next-job plan. No full scrape was launched. Spark/OpenRouter canaries were unavailable; direct implementation left provider configuration unchanged.

---

## 1. Evidence and source conclusions

### What the provider actually documents

- RapidAPI's rendered endpoint page exposes one `GET /injuries/nba/{date}` operation, with a required date path parameter; no historical time/as-of/edition selector is documented on that page.[7]
- The provider overview describes the response `date` as game date and `reportTime` as a report-cycle label. It advertises ET refresh cycles and historical coverage; those claims do not establish an archive of every intraday revision.[1]
- The public marketplace OpenAPI mirror agrees on the single date-based operation and date-only parameter. It contains response examples but no response schema. It is corroborating marketplace metadata, not proof of an undisclosed upstream capability, and its gateway is not our configured provider host.[5]
- The rendered provider overview's quick-start hostname/path differs from the working client's route. Preserve the configured `nba-injuries-reports.p.rapidapi.com/injuries/nba/{date}`, already verified by successful API responses; do not copy the conflicting overview example.[1][7]
- NBA publishes ongoing injury updates. The current official page is not a historical guarantee for 2023–25. The old season-index URL redirected to the current season; this must not be cited as an intact old-season index.[8]

**Conclusion:** no documented request knob was found that retrieves an arbitrary historical T-60 edition. That is a bounded documentation finding, not proof that the provider cannot supply one. Do not guess `as_of`, `time`, `reportTime` or snapshot query parameters and spend quota probing unsupported combinations.

### What was measured directly

Earlier authenticated pilot: exactly three GETs, all HTTP 200. Parent verified raw digests, byte lengths and parsed totals:

| Request date | Rows | Payload game dates | Cycles | Invalid status rows |
|---|---:|---|---|---:|
| 2023-11-01 | 108 | Nov 1: 104; Nov 2: 4 | 07PM | 0 |
| 2024-11-01 | 125 | Nov 1: 98; Nov 2: 27 | 06PM, 07PM | 1 |
| 2025-11-01 | 91 | Nov 1: 47; Nov 2: 44 | 03PM, 05PM | 2 |

There are 324 records, including 75 with a game date different from the request date. All `gameTime`/`matchup` values are null. Raw payloads contain no exact publication instant, edition identifier, source-document reference or completeness marker. These are observations of the three responses, not provider-wide guarantees.

The final raw-data cross-check is decisive: **each payload game-date group contains only one cycle**, not multiple intraday editions. In 2024 the 98 Nov 1 rows are all `07PM`, while the 27 Nov 2 rows are all `06PM`; in 2025 the 47 Nov 1 rows are all `05PM` and the 44 Nov 2 rows all `03PM`. In 2023 both date groups use `07PM`. No team/player repeats within any of the three responses. Therefore the multiple labels in a response do not demonstrate intraday revisions for a single game date. Same-player revision/return-to-prior-status behavior remains untested by these real inputs, not passed.

Malformed raw examples are `Karl- Towns` / status `Anthony`, `Eli Ndiaye` / status `John`, and `Injury Surgery` / status `Recovery`. Do not repair these by guessing an injury status or silently discarding their possible impact on team totals.

The existing uniqueness key loses zero rows within each captured response. It can collapse editions across observations, but that risk must not be presented as the proven cause of these source gaps.

### Local defects and reuse boundaries

- `src/scrapers/rapidapi_injury_backfill.py:128-207`: reuse `RapidAPIInjuryClient`, auth and rate limiting. Existing `fetch_date()` retries and reduces responses to a list/None; some API/non-JSON errors become empty lists. A raw-capture pilot must distinguish those failures from an actual empty response.
- `rapidapi_injury_backfill.py:220-265`: the writer uses request date for `report_date` and ignores payload game date. Preserve both concepts; merely replacing the date field globally would change existing joins and resume semantics.
- `rapidapi_injury_backfill.py:84-85,212-217`: current uniqueness omits report time, and resume treats any stored row as a completed date. Neither is a complete-edition contract.
- Production callers include `src/orchestration/lines_job.py:287`, `run_daily.py:105`, and `edge_refresh_job.py:685-704`. Edge refresh also invokes the injury linker. Do not alter or execute these paths as part of the file-only pilot.
- `src/processing/link_injury_data.py` contains schema/index setup and DB mutations. Do not call it as a supposedly harmless ID-resolution helper.
- `src/models/features/nba/historical_sources.py:61-92,235-256,329-347` requires explicit metadata/edition membership and currently joins injuries by `report_date = game_date`.
- `src/models/features/nba/injury_context.py:397-486` requires complete asserted rosters, one edition/clock, explicit target status and valid OUT-player contribution evidence. `historical_observation_adapters.py:186-225` accepts local-observation clocks, not inferred historical publication.

A small date/status repair can improve retained records. It cannot manufacture missing editions, publication evidence or team completeness.

## 2. Source questions to resolve, with a stop condition

Prepare the following message for the provider through its documented support/discussion channel; **do not send it without approval**:

1. Does `/injuries/nba/{date}` select a report publication date, a game date, or the latest stored data associated with that date? Why do the measured responses also include next-day game dates?
2. For a historical game, does the endpoint return all retained intraday editions, the latest status per player, or another selection? Can older editions be selected by a supported route or parameter? Please provide a real example.
3. What date, timezone and minute precision belong to `reportTime`? For old `07PM` labels, is this 19:00, a 19:30 PDF, or merely a cycle name? Are label semantics different for newer `01_00PM` values?
4. Can responses identify the source PDF/edition, team submission state, and an explicitly complete/empty team report? What does an absent player/team mean?
5. Can the upstream parsing errors demonstrated in the three raw records be corrected without substituting guessed statuses?

**Stop condition:** if the provider offers no historical edition/availability/completeness contract and the bounded evidence comparison cannot establish one, report `SOURCE_BLOCKED`. Do not broaden scraping, switch providers or PDFs, relax the cutoff, synthesize healthy statuses, or claim training readiness. Code mechanics may still pass independently.

Adjacent-day comparisons below can test the date relationship; they cannot, by themselves, prove historical publication or completeness.

## 3. Exact pilot population and budgets

### Saved-response replay: zero network calls

Reuse the authenticated captures under:
`C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-api-pilot-20261103/`

Verify each `.raw` against its receipt before processing. Missing/expired scratch evidence is a blocker or an explicit refetch decision, not permission to invent replacement data. Copy approved evidence into the chosen retained pilot bundle before scratch expiry; never commit credentials or opaque generated bundles by default.

### Target-game selection: future bounded read-only preflight

- Target game dates remain November 1 of 2023, 2024 and 2025: fitting seasons 22023/22024 and calibration season 22025.
- Select at most two retained, prop-linked games per date (maximum six games), preferably differing tip-off times. Use the actual approved training cohort and authoritative pregame schedule/commence evidence; do not use dropped games merely because their IDs were in the earlier miss audit.
- A separate SQL runner prepares a target manifest, with at most ten SELECTs, five-second statement timeouts and exact date/game predicates. Inspect metadata/indexes first; no raw odds date scan, archive scan or DB writes. It must report observed candidate/selected counts and exact keys.
- Manifest fields: game ID, literal season ID, game date, team IDs, authoritative commence UTC and source reference, decision UTC = commence minus 60 minutes, and pregame roster/ID evidence if available. Missing roster/time evidence remains unknown. Never infer pregame roster completeness from target-game boxscore participation.
- Require at least one verifiable game per season to claim three-season pilot acceptance. If these dates cannot supply that, stop with a scoped replacement-date proposal; do not automatically expand the date window.
- No target manifest was fabricated or SQL preflight run during this planning pass.

### Optional capture: primary API only

- Compare saved D responses with previous-day requests: **2023-10-31, 2024-10-31, 2025-10-31**. Retain every returned record, then inspect rows whose payload game dates match the target November 1 dates.
- Default new capture is exactly these three requests; no automatic refetch of the already verified three originals.
- Hard ceiling: six HTTP attempts in one authorized run, including explicitly selected refetches and failures. No automatic retries. Single worker, minimum one-second spacing, connect timeout five seconds/read timeout fifteen seconds, maximum two MiB per response and twelve MiB total, overall run deadline three minutes. Bound streaming body reads as well as headers.
- Stop on 401/403, 429, budget exhaustion or ambiguous provider errors; save redacted failure receipts. Do not change subscriptions or authentication configuration.
- No unsupported query parameters, extra live-current experiment, full-season job, scheduler registration, production linker or local mirror sync.
- Official PDFs remain separately approved validation evidence, not automatic ingestion. Start with the two existing files. Any extra document comparison must be explicitly listed and bounded; publisher availability now is not proof of API edition selection then.

## 4. File-only pilot contracts

### Raw capture

Persist immutable response body plus receipt before parsing. Receipt includes requested date/URL, fetch start/end UTC, HTTP status, safe response headers/request ID, content type, byte count, SHA-256, client version and run ID. Never persist request auth headers or keys.

A successful HTTP response is `capture_complete`, not `source_accepted`. Preserve error/non-JSON bodies distinctly. An empty list means `empty_response_unverified`, not no injuries. Replaying a verified body uses no API key or network. A deliberate refetch creates a new capture identity; do not overwrite the earlier body.

### Normalization without destructive repair

Produce one output disposition per original record, preserving raw capture hash and record ordinal:
- `requested_date`, `game_date` from payload, `raw_report_time`, original team/player/status/reason and unused fields;
- source publication date/instant and availability bound **nullable until evidence resolves them**; retrieval UTC stays separate;
- recognized status or `quarantined_invalid_status`, invalid date/name/type or conflict reason;
- resolved game/team/player IDs only from explicit, evidence-backed reference mappings; unresolved and conflicting matches stay visible;
- possible edition key is provisional until authenticated; a response hash is a capture ID, not automatically a publication-edition ID.

Partition target-day and spillover records; do not discard next-day records. Do not drop invalid statuses and then certify the remaining team complete. Do not parse `07PM` into an invented publication instant, unconditionally add 30 minutes, or combine game date with report time as a publication date.

The documented API status vocabulary is `Out`, `Doubtful`, `Questionable`, `Probable`, `Available`.[1] Quarantine unknown tokens instead of mapping them to Healthy. A blank reason alone is not proof of a bad record: retain it as missing descriptive text when identity/status are valid, and let the source contract decide eligibility. The next-day tail ordering observed in these files is not a documented parsing contract. Likewise, other date offsets must be reported for review rather than silently normalized or discarded.

Across captures, keep the same player's observations separately. Within an explicitly identified edition, exact duplicates may be counted/coalesced with complete lineage; conflicting statuses block that edition. No fuzzy name/status repair without an explicit checked override and retained original.

Required accounting invariant: input records = retained target candidates + retained spillover + quarantined records, using mutually exclusive dispositions. Every row remains traceable. Report separate overlap/conflict counts without double-counting those dispositions.

### Source acceptance and cutoff

For each selected game/team, require:
1. An authenticated edition/selection rule with an availability instant or conservative bound proven at or before T-60 and strictly before commence.
2. A complete submitted team report, not merely the absence of error rows.
3. Game/team/player identity and pregame membership evidence; unresolved names or malformed records that could affect the team block it.
4. Explicit approved handling of an unlisted player in a verified complete team report, distinct from unknown/not submitted and from a medical assertion of health.
5. Compatible OUT-player contribution inputs for the existing feature aggregation.

A matching handful of PDF/API players does not prove full edition completeness. If a selected report is too late, exclude it as post-cutoff; do not move the decision time. Selecting an older report also requires its applicable game identity and the approved source policy, not an arbitrary carry-forward.

## 5. Approved baseline scope — A–C delivered; D remains gated

### Task A — add raw-response access without changing production behavior

Modify: `src/scrapers/rapidapi_injury_backfill.py` only to add a rate-limited single-attempt raw response method, proposed `RapidAPIInjuryClient.fetch_date_response`.

Reuse session auth and working host. The new method must not reduce malformed responses to `[]`, auto-retry, store records or initialize a DB. Preserve existing `fetch_date`, writer and CLI behavior for current callers. Any later production date/retention repair is a separate reviewed migration, not smuggled into this helper.

### Task B — pure normalizer and file receipts

Create (proposed): `src/scrapers/nba_injury_pilot.py`.

Own response/receipt validation, normalization/quarantine, grouping, duplicate/conflict diagnostics, bounded file persistence and report generation. No engine creation, DB imports, network on import or source-policy guessing. Reuse installed requests only through the existing client in capture mode. Keep replay import-safe and credential-free. Avoid a new generic source framework.

### Task C — standalone operator CLI

Create (proposed): `scripts/run_nba_injury_pilot.py`.

Proposed modes:
- `plan`: validate manifests and print exact requests/limits, no credential/network/DB access.
- `replay`: validate saved response receipts and generate repaired file views/diagnostics, zero HTTP/DB calls.
- `capture`: explicit `--execute`, exact request manifest, required output directory and budgets; capture only, never ingestion.
- `audit`: read a target manifest plus normalized captures and any verified source evidence; publish per-game/team PASS/BLOCKED reasons. Do not require a newly trained model.

No-argument invocation prints help and does nothing. Explicit output directory must be outside production artifact paths. Reuse existing verified captures unless a refetch was explicitly selected; do not equate presence of any DB date with successful capture.

Output bundle: `manifest.json`, immutable `raw/` and receipts, `normalized.jsonl`, `quarantine.jsonl`, `date_cycle_matrix.csv`, `game_team_readiness.csv`, `summary.json`, `summary.md`. JSON is authoritative for counts; Markdown is its projection. Final manifest is atomic; interrupted runs retain incomplete status and cannot be mistaken for accepted evidence.

Exit contract: 0 = requested mechanical mode completed; 2 = explicitly BLOCKED source audit; 1 = execution/parse/budget failure. Summary must separately state `mechanics_pass`, `injury_source_pass`, `native_consumer_pass` and `training_ready=false` unless the separate full training gate later proves readiness. Capture/replay exit 0 alone is never injury acceptance.

### Task D — conditional native injury integration, only after source acceptance

Potential later edits, **not included in Tasks A–C**:
- `src/models/features/nba/historical_observation_adapters.py`: distinct authenticated publication interpreter; retain local-observation behavior.
- `src/models/features/nba/injury_context.py`: explicit complete-report/not-listed semantics after owner approval and evidence; never silently replace unknown with Healthy.
- `src/models/features/nba/historical_sources.py`: read accepted pilot evidence with preserved game date and edition identity, rather than inserting misleading legacy rows solely to satisfy its current query.
- Corresponding policy validation in `src/models/features/nba/historical_policy.py` only if a new source interpreter requires it; inspect its exact accepted schema before writing.

Reuse existing feature math. First execute the actual injury adapter/aggregation with the accepted pilot. A full historical-loader check is conditional on other families' real metadata; do not fabricate rolling/quote evidence just to obtain a green end-to-end result. If this needs schema changes or wider APIs, return a follow-up scope rather than expanding automatically.

## 6. Verification and acceptance ladder

No new permanent unit tests or fixture edits are authorized by this plan. Use existing tests plus a few short controlled probes under the standard offline guard after implementation approval.

Existing suites inspected/discovered:
- `tests/test_rapidapi_injury_backfill.py`: dry-run is DB/key-free; schema bootstrap remains explicit; no runtime index drops.
- `tests/test_nba_source_boundaries.py`, `tests/test_nba_mode_loaders.py`: compatibility coverage, not historical source certification.

PowerShell-safe validation command (existing guard/suites; to run after implementation):

` .\venv\Scripts\python.exe C:\Users\Chase\AppData\Local\hermes\skills\gameflow\gameflow-implementation-worker\scripts\run_nba_offline_checks.py C:\Users\Chase\Projects\GameFlowData pytest tests/test_rapidapi_injury_backfill.py tests/test_nba_source_boundaries.py tests/test_nba_mode_loaders.py `

Validate in order:
1. Offline baseline and scoped Ruff/compile checks before/after implementation; no accidental DB or credential access in replay/plan paths.
2. Replay the genuine three saved captures: 324 inputs, 75 spillover records, three invalid-status records before mutually exclusive partitioning. Verify actual partition counts mechanically rather than subtracting overlapping flags by hand. Exact raw-key simulation remains zero lost duplicates for each capture.
3. Controlled material failures: malformed body/API error versus empty list, budget stop, tampered receipt, ambiguous name/status, conflicting edition, post-cutoff report, not-submitted team. No extensive new scratch test framework.
4. After explicit execution approval, capture only the three previous-day requests and compare date/cycle/status groups with the saved originals. Publish observations even if source acceptance remains BLOCKED.
   To exercise real intraday ordering, obtain at least one evidence-backed same-game-date pair of distinct editions through a documented API mechanism or separately approved source comparison. The saved cross-year repeated players do not meet that criterion. If no such evidence is available within the approved scope, report that acceptance item as BLOCKED rather than increasing the request budget.
5. Require at least one fully accepted game per selected season and both team reports per accepted game for a three-season injury pilot PASS. All rejected candidates and reasons remain visible. No whole-season coverage claim.
6. Only after Task D is independently reviewed and actual consumer verification passes should Chase consider wider collection. Historical DB repair and model training remain separately launched/approved.

Original proposed CLI syntax; use the delivery report for verified runnable commands. The provisional `--source-evidence` interface below was deferred with Task D and is not accepted by the MVP CLI:

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py replay --input-manifest <verified-captures.json> --output-dir <pilot-output> `

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py capture --request-manifest <three-previous-days.json> --max-requests 3 --execute --output-dir <pilot-output> `

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py audit --input-manifest <pilot-output/manifest.json> --target-manifest <verified-games.json> --source-evidence <verified-source-contract.json> --output-dir <audit-output> `

The placeholders are deliberately not launch commands. Manifest values must be real verified inputs, not invented authority metadata.

## 7. Decision and stop boundaries

- Research is complete for the available public docs and captured responses. Exact historical cycle/selection/completeness guarantees remain unanswered; the plan makes that visible instead of pretending more client code resolves it.
- Recommended first implementation slice: A–C only, file-based and non-destructive. It can validate ingestion mechanics while honestly reporting SOURCE_BLOCKED.
- Release Task D only after source evidence and the explicit omission policy are settled. Do not implement a permissive adapter in anticipation.
- Never rewrite/delete old injury history, perform broad relinking, alter daily production jobs, install a new provider, commit/push, or train from this plan alone.
- Keep the prior uncommitted missing-props training exclusion and concurrent NCAAB/linkage work protected.

## Relevant prior lessons/invariants

`lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: source meaning and the actual consuming cutoff both matter. `operations/critical-invariants`: strict pregame inputs, isolated bounded SQL, owner-launched broad jobs, unchanged probability/calibration math. Public response fields are evidence, not instructions.

## Local evidence and documentation

- Findings/approval context: `docs/operations/nba-injury-usability-investigation-2026-11-03.md`.
- Existing raw pilot/receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-api-pilot-20261103/`.
- Public docs, rendered RapidAPI pages, OpenAPI mirror, retrieval manifest and citation ledger: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-api-research-20261103/`.
- The main report and this plan retain durable conclusions; scratch paths can expire. Archive reviewed evidence into a chosen run bundle before implementation relies on it.

## Sources

[1] https://rapidapi.com/nichustm/api/nba-injuries-reports
[5] https://jojapi.com/hub/api/nba-injuries-reports/openapi.json
[7] https://rapidapi.com/nichustm/api/nba-injuries-reports/playground/Date
[8] https://official.nba.com/nba-injury-report-2026-27-season
