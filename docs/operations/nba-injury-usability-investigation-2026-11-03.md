# NBA historical injury usability — November 3, 2026

## Verdict

**November 4 execution update:** the [file-only pilot was implemented and run](nba-injury-pilot-delivery-2026-11-04.md), and the [full scrape was scoped](../../.hermes/plans/2026-11-04-nba-injury-full-scrape.md). Six preserved captures contain 625 records; seven were quarantined. Capture/replay works, but native source acceptance remains blocked. The bounded target-selection receipt had zero eligible games among 17 sampled candidates; no full-history inference or broad linkage repair followed.

**Insufficient as-is for the current strict T-60 injury-feature path.** The approved raw API pilot below confirms upstream malformed records and missing edition/availability guarantees; it did **not** reproduce deduplication loss within its three responses. Existing rows retain useful reported statuses, but do not establish complete pre-decision editions. Storage can discard repeated editions, but that alone is not the demonstrated cause of the sampled deficiencies. No production code or DB adjustment was made during this investigation.

The earlier training-only missing-props exclusion remains unchanged. No training, ingestion, injury repair, schema change, local sync or deployment was launched.

## Bounded canonical evidence

Remote Supabase; four SELECT attempts total including the first worker's metadata query before its provider error. The replacement completed three SELECT transactions, each with a five-second timeout, without SQL timeouts. Eight indexed date lookups used ordered ID samples with a 501-row overflow probe and a 500-row reporting cap. Every nonempty date was below the cap, so these are complete retained-row counts for the exact dates, not whole-season counts or proof of complete source reports.

| Report date | Retained rows | Stored report-time labels | Null player IDs |
|---|---:|---|---:|
| 2023-11-01 | 108 | 07PM | 0 |
| 2024-03-01 | 127 | 03PM, 07PM | 0 |
| 2024-11-01 | 125 | 06PM, 07PM | 0 |
| 2025-03-01 | 124 | 01PM, 06PM | 0 |
| 2025-11-01 | 90 | 03PM, 05PM | 3 |
| 2026-03-01 | 184 | 01PM, 07PM | 0 |
| 2026-10-26 | 0 | none | 0 |
| 2026-11-02 | 156 | eight labels from 10_00AM through 06_00PM | 1 |

Totals mechanically verified: 914 rows; 758 belong to the six dates before October 20, 2026. Four null player IDs, no null team IDs. The October 26 absence is a recent-season issue, not the historical verdict's basis.

Two concrete malformed retained records:
- ID 55987, 2024-11-01: player `Karl- Towns`, status `Anthony`, reason NULL. A name fragment occupies the status column.
- ID 76817, 2025-11-01: player `Eli Ndiaye`, status `John`, reason NULL and player ID NULL.

These are defects, not evidence that all other rows are invalid. Do not guess replacement statuses.

## Why a narrow parser adjustment is not sufficient

1. **Edition history is lossy.** The live unique key is `(report_date, COALESCE(team,''), player, status, COALESCE(reason,''))`; `report_time` is excluded. `store_records()` uses `DO NOTHING`. Repeated unchanged editions disappear, and a return to an earlier same-day status/reason can disappear too. This is a proven storage behavior, not a claim that a particular sampled player demonstrably made that lost transition. See `src/scrapers/rapidapi_injury_backfill.py:84-85,220-265`.
2. **Completeness is not retained.** The producer stores normalized rows, not raw responses, report manifests or explicit no-report receipts. A date with any row is skipped by resume; missing player/status records are dropped. There is no proof of complete per-team editions or meaningful omission. See `rapidapi_injury_backfill.py:149-184,212-217,242-265`.
3. **Scrape time is not publication time.** Five sampled historical dates were inserted on January 29, 2026. That does not invalidate authentic historical reports, but `scraped_at` cannot substitute for historical public availability. The native adapter currently uses only separately attested `local_observed_at`, treating `report_time` as display text: `src/models/features/nba/historical_observation_adapters.py:186-225`.
4. **The consumer contract cannot be fulfilled by these columns alone.** It requires edition row IDs, edition-linked provenance, exact roster membership, completeness authority and one observed instant; the target must have an explicit row. No such embedded metadata columns exist in the inspected table. The inspected producer does not create corresponding sidecars. Unexamined external backups/sidecars remain unknown, not proven absent. See `src/models/features/nba/historical_sources.py:61-92,329-347` and `src/models/features/nba/injury_context.py:397-472`.

## Publisher recovery route checked

Downloaded two actual official NBA PDFs successfully (HTTP 200), retaining bytes, SHA-256, URL and HTTP metadata separately from the database:
- `Injury-Report_2023-11-01_07PM.pdf`: header **11/01/23 07:30 PM**; 80,871 bytes.
- `Injury-Report_2024-11-01_06PM.pdf`: header **11/01/24 06:30 PM**; 86,631 bytes.

The filename hour is not the precise header minute in these examples. Do not blindly parse `07PM` as 19:00, assume every epoch is :30, or claim the API rows are authenticated to these files merely because labels match. These PDFs also distinguish team sections marked `NOT YET SUBMITTED`; a downloaded document is not automatically a submitted report for every team. Publication headers need validated timezone/availability semantics and must precede each game's T-60 cutoff. The sampled evening PDFs may be too late for early games.

The files prove a public recovery route exists for these two editions, not whole-season availability. Automated PDF text extraction merges table cells; it was used only to verify headers/report structure, not to manufacture or ingest player records.

## Approved recovery direction — existing basketball injury API first

Chase approved the recovery plan and clarified that the existing basketball injury API is the primary scraping source. Reuse `RapidAPIInjuryClient` in `src/scrapers/rapidapi_injury_backfill.py`, which calls `nba-injuries-reports.p.rapidapi.com/injuries/nba/{date}`. The official PDFs are independent validation/recovery candidates, not an approved replacement scraper. The three-request raw API pilot is complete; ingestion and schema changes have not run.

1. **Use the retained training cohort and inspect the API first.** After approved linkage exclusions, enumerate required game/team decision times under existing limits. Inspect raw API responses for a pilot date in each of 2023–24, 2024–25 and 2025–26 before passing through the current deduplicating writer. Establish whether the API already supplies the necessary report editions, exact publication semantics and team completeness indicators; distinguish provider omissions/malformed values from information lost by our ingestion. Do not blindly reacquire every daily edition or all raw odds history.
2. **Prove edition-preserving API capture on that pilot.** Retain raw response bytes, digest, requested date, source endpoint, retrieval time, report identities/times, team submission evidence where supplied and parse counts. Validate multiline/hyphenated names and status enums; reject malformed/ambiguous records. Use official PDFs only to independently check source meaning or investigate a demonstrated API gap, not to infer unsupported timestamps or completeness. If the API cannot supply required historical editions, report that limitation before changing the primary acquisition source. Keep original RapidAPI rows untouched.
3. **Add a separate historical-publication adapter where supported by verified API evidence.** Do not relabel historical publication as local observation. Reuse existing injury feature calculations, but give authenticated historical editions their own clock/source interpreter. Represent `not listed in a verified complete submitted team report` separately from `missing/not submitted/unknown` and from a medical assertion of healthy. The approved recovery direction does not turn unverified API omissions into complete reports or permit silently changing current `omitted_player=block` behavior without that source evidence. Include valid pregame team/roster and OUT-player contribution evidence.
4. **Accept before scaling.** Exercise the actual native consumer at T-60 on the pilot, including a missing team report, a post-cutoff edition and a malformed player row. Only after that passes should Chase launch bounded collection for the remaining retained cohort. Any DB schema/capture changes require scoped migration approval. Report exclusions rather than fabricate missing statuses.

This is acquisition/retention plus a source adapter, not a small label cleanup. Existing history remains useful for comparison and recovery; no bulk deletion or replacement is proposed. If rapid training without this work is preferred, a separately specified injury-free baseline would be a new feature policy, not permission to fill current injury features with zeros.

## Approved API pilot — completed November 3, 2026 ET

Exactly three GETs, no retries or DB connections, all HTTP 200. Parent independently verified raw byte lengths, SHA-256 digests and JSON-derived totals. Total: 324 records, three invalid statuses, 75 records with a payload date different from the requested date.

| Requested date | Rows | Payload dates/counts | Report cycles | Invalid statuses |
|---|---:|---|---|---:|
| 2023-11-01 | 108 | Nov 1: 104; Nov 2: 4 | 07PM: 108 | 0 |
| 2024-11-01 | 125 | Nov 1: 98; Nov 2: 27 | 06PM: 27; 07PM: 98 | 1 |
| 2025-11-01 | 91 | Nov 1: 47; Nov 2: 44 | 03PM: 44; 05PM: 47 | 2 |

Findings that refine the earlier diagnosis:
- The raw API already contains `Karl- Towns` / status `Anthony`, `Eli Ndiaye` / status `John`, and another row `Injury Surgery` / status `Recovery`. These are upstream payload defects, not introduced by our local parser. Do not guess their intended status.
- Fields are `date`, `team`, `player`, `status`, `reason`, `reportTime`, `gameTime`, `matchup`; gameTime/matchup are NULL throughout. No raw response edition ID, exact publication minute/timezone, source-document reference or report/team completeness indicator was supplied.
- Simulating our current uniqueness key drops **zero rows** from each response. Repeated-edition loss remains a storage-design risk, not the demonstrated explanation for this pilot. Do not promise that changing the unique key alone will recover missing intraday editions.
- Our writer ignores the payload `date` and stores the request date in `report_date` (`rapidapi_injury_backfill.py:251-257`). The provider's public response schema describes `date` as the game date. The native reader joins `report_date` to game date (`historical_sources.py:245-247`), so request date and game date must be retained separately before correct routing can be established. Do not blindly relabel existing rows: source publication/request dates and target game dates are distinct concepts.
- Provider listing schema was retrieved from `https://rapidapi.com/nichustm/api/nba-injuries-reports` via exact search excerpts. It documents ET refresh cycles and describes reportTime as a cycle, but the inspected text does not establish that this endpoint returns every historical intraday edition or exact original publication instants. Its quick-start hostname/path differs from the working configured endpoint; do not copy that example to change the client. Current pilot HTTP evidence establishes the actual working route.

**Next gate:** establish the provider's historical edition-selection and clock semantics (including whether another supported API parameter/route exposes the required pre-cutoff edition) before bulk collection or accepting complete T-60 snapshots. Retention repairs can preserve what arrives; they cannot manufacture absent source evidence. Capture raw bodies, preserve payload game date separately, and quarantine malformed statuses in the approved recovery implementation once that source boundary is resolved. Do not silently substitute PDFs or call the current pilot source-ready.

Receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-api-pilot-20261103/pilot-results.json`, three `.raw` bodies and matching `.receipt.json` files. Publication/retrieval timestamps are retained separately; UTC retrieval occurred on November 4 while local ET date was November 3.

## API research and pilot scope — research-only hold

The [scoped pilot implementation plan](../../.hermes/plans/2026-11-03-nba-injury-api-recovery-pilot.md) is complete as a research/design deliverable, not implemented. RapidAPI's actual rendered endpoint documentation exposes the date path parameter and one daily-report endpoint; the public OpenAPI mirror also has only that parameter and no response schema. No documented historical as-of/edition selector was found. The provider's refresh-cycle description does not resolve exact historical publication/selection/completeness. The plan retains those unanswered questions explicitly and includes a provider-message draft; nothing was sent.

Final raw-data cross-check: each `(request date, payload game date)` group has exactly one report cycle and no team/player repeats within a response. The two cycles in the 2024/2025 responses belong to different game dates, not demonstrated intraday editions of the same game's report. Cross-year repeated players are not revision-history evidence. This further limits what can be certified from the saved samples without assuming a provider-wide absence of older editions.

Recommended first slice is file-only raw capture, normalization/quarantine and audit using the existing client. Replay the saved three responses first; then, after execution approval, compare exactly three previous-day API responses (October 31 of 2023, 2024 and 2025) against the saved November 1 responses. Preserve request date, payload game date, retrieval time and raw cycle separately. This can establish ingestion mechanics and investigate date relationships but cannot manufacture missing publication evidence. The native feature adapter and any DB repair remain conditional on source acceptance. The user explicitly requested no implementation during this scoping pass.

Public documentation, rendered primary pages, OpenAPI mirror and citation ledger are retained under `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-api-research-20261103/`. The plan records exact proposed files, bounded requests, target-game selection rules, output contracts, failure states and verification gates. No new authenticated injury requests or SQL queries were made in this research pass.

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: verify producer meaning and actual consuming cutoff, not just timestamp presence.
- `operations/critical-invariants`: pre-decision inputs, isolated bounded SQL, no silent source defaults, owner-launched long jobs. Numerical model/calibration behavior remains unchanged.

## Reproducible receipts

- SQL/results: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-usability-20261103/exact_sql.sql` and `compact_results.json`.
- Publisher bytes/HTTP/hash receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-publisher-probe-20261103/`, including `publisher_receipts.json`.
- Policy/readiness context: [training readiness reconciliation](nba-training-readiness-reconciliation-2026-11-03.md).
