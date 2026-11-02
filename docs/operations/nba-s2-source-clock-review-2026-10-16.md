# S2 NBA source clocks — source-only review, October 16, 2026

## Kick off immediately

I’m setting the immediate ownership and stop conditions so S2 evidence work can continue without prematurely implementing a timestamp policy.

- This chat: source tracing and public timestamp-contract research below are complete; the data reconciliation portion remains open. After a successful stable NBA sync, use an isolated read-only runner for bounded source/quote samples and the separate S1 real-artifact gate.
- Other chat: no new source package is ready to launch. Preserve the delivered S1 worktree; do not change its files or implement these proposed policies yet.
- You: let the running sync finish. Read the conclusions below, then the [rebuild wiki](../understanding/nba-rebuild.md). Decision-time, ambiguous-quote and injury-history policies still require explicit approval after coverage evidence; no training/rescrape command is ready.

Status: source semantics partially resolved, not historical readiness certification. No new DB queries, paid API calls, source edits, migrations, training, integration or deployment were performed for this review. Existing remote metadata artifacts were read; prior stale-local samples are not reused as current coverage evidence.

## Conclusions

I’m summarizing what the source-clock review establishes, what remains uncertain, and why S1 does not resolve S2’s temporal contracts.

1. `snapshot_time` does not have one meaning across NBA ingestion paths. Historical request time, live response-parse time and live invocation-start time must not be interpreted interchangeably.
2. Historical requested time is not automatically unusable: provider documentation supplies a conservative snapshot-time bound, subject to row provenance and normal data-integrity checks.
3. Injury timing has a useful first-person vendor-claim lead, but no verified historical availability/version contract. A report cycle label is not proof that every stored status was available then.
4. S1 validates failure handling, not any of these clock contracts. Its [independent controlled checks passed](nba-s1-coordinator-review-2026-10-16.md); real-data/artifact acceptance is still separate.

## Clock map from current source

I’m mapping each ingestion path’s stored clock to its actual source-code meaning so downstream work does not treat unlike timestamps as interchangeable.

| Path | Stored clock meaning | What it does not prove |
|---|---|---|
| Historical game lines | `snapshot_time` is the requested historical timestamp; response-envelope timestamp is ignored | Exact returned snapshot time or unique pull identity |
| Historical player props | Same requested-time convention; historical event response is unwrapped without retaining its envelope timestamp | Exact per-event snapshot or whole-invocation atomicity |
| Live game lines | Local UTC timestamp generated after the bulk response is fetched, at parse start | Provider publication time |
| Live player props | One local UTC timestamp generated before iterating/fetching event odds; events commit separately | That later event quotes were already observable at that earlier invocation time |
| Injuries | Caller date plus raw textual `report_time`; first-insertion `scraped_at` | Complete ordered status history or original publication/availability time |

Source evidence: `src/scrapers/game_lines_scraper.py:26-40,64-109,134-185`; `src/scrapers/daily_player_props_scraper.py:63-116,126-245`; `src/scrapers/live_odds_scraper.py:55-147`; `src/scrapers/rapidapi_injury_backfill.py:62-85,220-262`. Existing delegated source traces are in scratch `nba-s2-20261016-173252/odds-source.md` and `injury-source.md`. The coordinator directly reread the historical endpoint/parameter and envelope-handling paths.

The saved canonical remote metadata confirms injury `report_time` is nullable text and `scraped_at` is nullable timestamp-with-time-zone. This verifies schema presence, not timestamp completeness or current row coverage.

## Historical odds: conservative bound, not exact identity

I’m explaining when historical odds request times can support a conservative as-of bound and which provenance and integrity questions remain open.

The Odds API's [official v4 documentation](https://the-odds-api.com/liveapi/guides/v4/#get-historical-odds) and [historical-data guide](https://the-odds-api.com/historical-odds-data/) say both historical bulk and per-event odds return a snapshot at or before the requested `date`; the response envelope includes its actual timestamp. The v4 documentation warns that historical snapshots may contain data errors and discusses future corrections. This is not an immutability guarantee.

Inference, conditional on a row actually coming from that historical route and faithfully retaining its request time: `provider_snapshot_time <= requested_time <= decision_time` supplies a conservative snapshot-time cutoff. The missing exact envelope timestamp alone does not establish future leakage or justify a broad rescrape.

Limits: this does not prove individual rows obey the provider contract, their market updates precede the decision, quotes were executable, duplicate pulls agree, or every mixed-table row has historical provenance. A fixed 17:00/23:00 timestamp alone is not authenticated source provenance. Preserve update-time and pre-commence checks; do not apply the inference to the live props invocation-start clock.

## Injury clock: stronger lead, gate remains open

I’m distinguishing the useful vendor timing lead from the unverified historical availability contract needed for injury features.

A [self-described API author's announcement](https://www.reddit.com/r/sportsanalytics/comments/1o11mry/nba_injuries_report_api/) links the exact RapidAPI listing used by this project and describes 11 AM / 3 PM / 5 PM Eastern refreshes and `reportTime` cycle labels; its example uses `05PM`. This is first-person vendor-claim evidence, not independently authenticated endpoint behavior. The [RapidAPI listing](https://rapidapi.com/nichustm/api/nba-injuries-reports) returned no usable contract text through the public extractor.

Still needed: confirm the format/timezone against retained payloads, whether each historical status retains its original edition, whether later revisions overwrite earlier editions, and what omitted players mean. Do not turn the announcement into a historical as-of guarantee or silently parse all text as a UTC datetime.

Storage itself loses observations: uniqueness excludes `report_time`, and insert-or-ignore discards repeat observations of the same date/team/player/status/reason. Distinct status/reason variants survive, but latest ID is not an edition sequence. `scraped_at` can mark first local insertion, including a later backfill; it cannot retroactively establish earlier source availability.

## Proposed acceptance rules — not approved implementation

I’m outlining candidate safeguards for owner review without presenting them as approved policy or implemented behavior.

- Use an explicit timezone-aware decision policy shared by training/replay; no accidental inheritance of fixed 23:30 UTC. Choose the supported candidate after timing/coverage evidence, not highest backtest ROI.
- Require quote observation/update eligibility and a verified pre-commence boundary. Do not invent commence times or silently label unknown-source rows historically safe.
- Pair actual compatible sides from one unambiguous quote identity. Identical duplicates and conflicting prices are different cases; independently selecting latest Over and Under is not proof of a coherent pair. Inspect conflicts before choosing tie-breaks.
- Do not treat missing/ambiguous injury history as healthy. Report the missing contract explicitly; any feature/cohort fallback is a separate owner-approved decision, not an S1 repair.

## Next evidence boundary

I’m defining the bounded read-only samples needed to turn the remaining S2 clock questions into an evidence-backed policy proposal.

After successful sync, inspect indexes/keys before selecting input-bounded event/time/ID windows. Sample injury cycle values, same-player variants and insertion-vs-game dates; sample quote timestamp ordering, nulls, linkage and conflicting same-identity sides. Compare a small canonical remote spot check where appropriate. No whole-table aggregates or destructive cleanup; a timeout is not missing-data evidence.

These are planned evidence questions, not executed data results. S2 remains open until reconciled samples support a policy proposal; S3-S5 must not be dispatched as though those choices were resolved.

## Relevant prior lessons/invariants

I’m preserving the project lessons and invariants that constrain source-clock decisions, model interpretation, and any later validation work.

- GBrain `operations/hard-facts` and `operations/critical-invariants`: explicit temporal availability, empirical CDF, Q10 preservation, no global conformal offsets, read-only SQL isolation and owner-launched long jobs. Hard-Facts review labels remain pending; current invariant text was read separately.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: audit the consumer query/artifact path before declaring cleanliness or buying a rescrape.
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: bound actual inputs and verify schema/indexes. These May lessons are past their review dates; their general procedures were checked against current NBA source, not treated as current MLB readiness claims.
- `decisions/model-architecture-decisions`: preserve the NBA minutes × rate architecture; no model/calibration redesign is needed to investigate source clocks.

Related: [repair specification](nba-historical-repair-spec-2026-10-16.md), [sync authority/preflight](nba-sync-inventory-2026-10-16.md), [initial evidence and its limits](nba-historical-readiness-2026-10-16.md).
