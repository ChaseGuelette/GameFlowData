# RapidAPI NBA injury clock validation — November 8, 2026

## Verdict

**The injury statuses are usable evidence in many sampled cases, but `reportTime` is not validated as a publication timestamp. Do not enable a global label-to-clock map.** This is an observed source-semantics problem, not merely a theoretical demand for perfect provenance.

Four sampled dates from 2023 through November 2025 had no status disagreements among name/team/date-matched players against the official 5:30 PM report, despite API labels of `07PM` or `03PM`. Two 2026 dates had differences against the early-evening reports. In particular, one day-of API status contradicts the official status at T-60. Thus neither `reportTime` interpreted literally nor a blanket “all historical responses are pre-T-60” rule passed validation.

A previous-day snapshot policy is a plausible simpler injury-enabled experiment. Two inspected previous-day response subsets matched their actual previous-day official reports for every unambiguously matched player. This is sample evidence, not approval to assign a previous-day timestamp to every archived response without checking that response-date contract. Keep this policy separate from the existing no-injury baseline and from a claimed complete same-day T-60 report.

## Scope and execution

- Canonical remote SELECT-only runner: 11 metadata/data queries; readback verified `transaction_read_only=on`, `statement_timeout=15s`, `lock_timeout=3s`.
- Exact payload dates: 2023-11-03, 2024-11-03, 2025-03-03, 2025-11-03, 2026-03-03, 2026-05-05.
- At most 500 table rows per exact indexed date. Actual fetched rows: 92, 101, 134, 113, 161, 11; no cap hit.
- Extracted 581 distinct retained RapidAPI raw records: 384 from same-day requests and 197 from preceding-day requests. These cover 12 request-date/capture/payload-date groups; 186 selected table rows had no retained observations.
- The 581 records are not 581 independent games, nor complete raw request bodies: exact payload-date selection excludes other dates in the same provider response.
- Downloaded and parsed 17 bounded official PDF samples using the existing repository parser unchanged. These were public-document research reads, not an application scraping/backfill job.
- No fresh paid RapidAPI requests, model training, source-code changes, application DB writes, or provider-support messages.

The method uses exact game date plus case/accent/punctuation-normalized team/player names. No fuzzy identity repair. Status is checked separately from reason; reason differences and unknown identities are retained rather than hidden. Comparing equal statuses across reports establishes agreement, not necessarily the unique source edition when a status remained unchanged.

## Documentation findings

The actual RapidAPI marketplace description calls `reportTime` a report cycle and lists 11 AM, 3 PM and 5 PM ET. Its example route differs from the configured route; the copy alone does not establish historical label semantics.[1]

The JoJ marketplace listing describes the same dated `/injuries/nba/{date}` route and says its data comes from official NBA PDFs.[2]

Its public OpenAPI exposes only the date parameter, not an as-of time or edition selector. This is marketplace schema evidence, not a private provider guarantee or proof that no undocumented endpoint exists.[4]

The observed captured raw records also contain `matchup: null` and `gameTime: null`, fields not explained by the earlier description. Across the six sampled payload dates, the `reportTime` label agrees with the hour of the first game block in the PDF: five date groups use `07PM`, while November 3, 2024 uses `03PM` and the first game starts at 3:30 PM ET. Other games that day inherit the same label. **Inference:** a game-time field may be getting carried across a date block by the provider parser. Provider internals were not inspected, so the proven conclusion is that the field is not a reliable publication clock—not a certified root-cause implementation diagnosis.

## Same-day comparison results

Counts below are recalculated from `pdf_api_comparison_final.json`, not agent recollection. “Agree/matched” excludes unresolved identities from the denominator and does not certify those excluded rows.

| API request/payload date | API label | Official reference header | API rows in scope | Status agree / identity-matched | Unmatched API identities |
|---|---|---|---:|---:|---:|
| 2023-11-03 | `07PM` | 5:30 PM ET | 61 | 59 / 59 | 2 |
| 2024-11-03 | `03PM` | 5:30 PM ET | 35 | 35 / 35 | 0 |
| 2025-03-03 | `07PM` | 5:30 PM ET | 88 | 87 / 87 | 1 |
| 2025-11-03 | `07PM` | 5:30 PM ET | 84 | 82 / 82 | 2 |
| 2026-03-03 | `07PM` | 5:00 PM ET | 112 | 103 / 105 | 7 |
| 2026-05-05 | `07PM` | 5:00 PM ET | 4 | 3 / 4 | 0 |

The older-date official references print 5:30 PM, not the hour encoded in their filename tokens.[5][6][7]

The other reference reports provide the November 2025 and March/May 2026 comparison evidence.[8][9][10]

Later-edition controls reject a simplistic “07PM means the 7 PM publication” interpretation: same-day API data disagrees with the later official status for 12 matched players on November 3, 2023; 6 on November 3, 2024; 22 on March 3, 2025; and 10 on November 3, 2025. Conversely, the March 2026 API does not exactly match either inspected 5:00/5:30 or 7:00 PM edition. Do not choose a universal source publication time from these results.

### Concrete early-label counterexample

On November 3, 2024, RapidAPI includes Zion Williamson as Questionable with `reportTime=03PM`. He is absent from the inspected 3:30 PM official report and present as Questionable in the 5:30 PM report. This invalidates treating the `03PM` label as sufficient proof that this record was published at 3 PM; it does not establish the exact first announcement minute.[18][6]

### Concrete T-60 counterexample

Canonical schedule game `0042500201` (Cleveland at Detroit), May 5, 2026:

- Scheduled start: 7:00 PM ET; computed T-60 cutoff: 6:00 PM ET.
- Official 6:00 PM report: Kevin Huerter **Questionable**.[17]
- Official 7:00 PM report: Kevin Huerter **Out**.[16]
- Retained day-of RapidAPI response: Huerter **Out**, `reportTime=07PM`.
- Retained previous-day RapidAPI response: Huerter **Questionable**, also `reportTime=07PM`.

The same label accompanies different statuses from different source dates. The day-of Out value cannot replace the Questionable value observed in the official report at the chosen cutoff. This is not evidence that all API records are late; it is a counterexample to a blanket pre-cutoff assumption.

## Previous-day shortcut: measured evidence and limitations

| API request date | Payload game date | Official previous-day reference | API rows | Agree / matched | Unresolved identity |
|---|---|---|---:|---:|---:|
| 2025-03-02 | 2025-03-03 | March 2, 5:30 PM ET | 33 | 32 / 32 | 1 |
| 2026-05-04 | 2026-05-05 | May 4, 7:00 PM ET | 4 | 4 / 4 | 0 |

These statuses are present in the corresponding actual previous-day NBA reports.[20][21]

This supports a practical experiment using a deliberately older information set: train and replay on the response requested for the preceding publication day containing the target game date; live inference uses an actually captured previous-day snapshot too. Ignore game-day status changes for that model's feature snapshot. New-bet eligibility and already-placed wager settlement remain separate policies.

Before treating the whole archive this way, check that the dated endpoint really preserves that publication day's information rather than a later rewritten status. The two verified subsets prove a useful path, not a whole-season contract. A bounded stratified check of the existing previous-day captures and day-of/back-to-back coverage is appropriate; no reason to require downloading every report edition. Missing previous-day team submissions remain unknown, and previous-day injury snapshots are not equivalent to complete T-60 coverage.

## Other recoverable issues found

- API reason fields sometimes include leaked PDF headers, e.g. `Injury Report: 03/03/25`.
- Some player names are truncated, e.g. `TyTy Jr.` or `Jalen Schifino`.
- A retained November 2025 record has player `Injury Surgery`, status `Recovery`, and empty reason: an obvious malformed parse, not an injury designation.

These defects should not justify rejecting all otherwise valid injuries. Preserve originals; quarantine malformed statuses/ambiguous identities; apply narrowly verified normalization/aliases. Do not weaken identity checks or alter actual medical status/reason to manufacture an exact match. A text-only reason-formatting difference should be distinguished from contradictory status evidence when designing the approved consumer repair.

## Recommended practical next step

1. **Stop treating `reportTime` as a publication timestamp.** The field-level validation failed; a global ET time-map would create both false rejections and false acceptance.
2. **Keep usable API statuses.** Existing matched official evidence can supply the timing for those observations; not every source field must be perfect for a supported status to be useful.
3. **Offer an explicitly previous-day injury-feature experiment** if Chase wants the simplest consistent train/live setup. First measure its team-game coverage and validate a modest stratified sample of the dated-response boundary. Keep absent reports unknown. This is a new policy proposal, not an implemented change or a replacement for the running no-injury baseline.
4. **For same-day T-60 injury features, recover an edition timestamp per response/date rather than demanding every PDF edition.** Ask the provider for original source URL/publication timestamp and whether older editions can be selected. Where unavailable, use a targeted eligible official report for the missing date/game cohort. Some earlier statuses genuinely are not in the archived day-of response.
5. Do not claim 100% consumer rate from this sample. Status usability, team-report completeness, identity mapping and actual feature-consumer acceptance are separate checks.

Suggested provider question (draft only, not sent):

> For `/injuries/nba/{date}`, what exactly does `reportTime` represent? In retained responses it appears to follow a game-time block rather than the printed PDF publication clock. Which original NBA PDF URL/header supplies each dated response, are historical responses immutable, and can we request an earlier intraday edition or receive `published_at` and the source URL?

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: actual consumer/source meaning matters more than a suggestive field name.
- `lessons/cheap-baseline-before-architecture`: keep the no-injury baseline moving; a bounded previous-day variant is a smaller hypothesis than rebuilding every historical edition.
- Unknown is not Healthy. Do not derive pregame injury labels from eventual box-score participation or retroactively erase earlier predictions/bets.
- Research did not change publication/cutoff rules, add a model feature variant, or authorize a backfill.

## Receipts and reusable comparison

Receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-rapidapi-clock-validation-20261108/`

- `rapidapi_clock_exact_sql.sql`
- `rapidapi_clock_raw_extraction.json`
- `rapidapi_clock_distributions.json`
- `pdf_api_comparison_final.json`
- `public/` contains original official PDFs, extracted text and retrieved public documentation.
- `ledger.json` records citation URLs.

Scratch may expire. The decisive counts, player example and dated source references are embedded here. Preserve approved raw evidence in the existing receipt system before implementing a whole-cohort acceptance rule.

Reusable, DB-free/no-network comparison helper: `gameflow-data-source-research/scripts/compare_nba_injury_report_samples.py`. It uses the repository parser unchanged, records parser failures, compares status separately from reason, and preserves unresolved identities. The helper was exercised on all 17 downloaded PDFs; all parsed successfully for this bounded comparison.

## Sources

[1] https://rapidapi.com/nichustm/api/nba-injuries-reports
[2] https://jojapi.com/hub/api/nba-injuries-reports
[4] https://jojapi.com/hub/api/nba-injuries-reports/openapi.json
[5] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2023-11-03_05PM.pdf
[6] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2024-11-03_05PM.pdf
[7] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-03-03_05PM.pdf
[8] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-11-03_05PM.pdf
[9] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-03-03_05_00PM.pdf
[10] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-05-05_05_00PM.pdf
[16] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-05-05_07_00PM.pdf
[17] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-05-05_06_00PM.pdf
[18] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2024-11-03_03PM.pdf
[20] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-03-02_05PM.pdf
[21] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-05-04_07_00PM.pdf
