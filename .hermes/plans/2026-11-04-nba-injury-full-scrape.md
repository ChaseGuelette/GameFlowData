# NBA Injury API Full Scrape Scope

> **Storage correction:** the file-only job was a design mistake for this DB/Railway system. Its completed downloads remain useful, but the normal recovery entrypoint now delegates to the existing DB scraper. See [DB-first implementation and pending activation](../../docs/operations/nba-injury-db-first-2026-11-04.md). No new staging/linking table is introduced; the old file-only launch flags are retired.

**Status:** RAW SCRAPE READY FOR OWNER LAUNCH. Chase corrected the design: acquisition must use historical NBA games, not prop-linked training eligibility. The corrected `historical_game_dates_v1` manifest contains 3,781 games, 534 target dates and 567 request dates; six captures are reusable and 561 new requests fit in 94 batches. The actual dry-run passed with no network/DB/model calls. See [verified launch command and receipts](../../docs/operations/nba-injury-recovery-launch-2026-11-04.md). No full scrape was launched by the agent.

## Recommendation

Keep the existing RapidAPI basketball injury source. Collect both the historical NBA game's date D and the preceding request date D−1, deduplicating request dates across games. Do not require props or training-player eligibility. Preserve responses independently rather than merging statuses into the legacy injury table during collection.

This is a raw-history recovery job, not automatic certification of T-60 features. Historical request-date/publication semantics and team completeness remain unresolved. Collection and training acceptance must remain distinct; collecting more dates does not by itself fix those contracts.

## What the actual pilot established

- The implemented CLI completed the three approved D−1 requests, all HTTP 200: October 31 of 2023, 2024 and 2025.
- New responses: 301 records, four quarantined invalid statuses. Original saved D responses: 324 records, three invalid statuses.
- Combined six-capture bundle: 625 records = 418 valid target candidates + 200 retained spillover + seven quarantined records. Digests and counts verified independently.
- For 37 raw `(game_date, team, player)` keys, status or reason differs between captures; 20 keys have differing statuses. These are raw-name comparisons, not certified canonical player-ID joins.
- Example: Precious Achiuwa for November 1, 2023 is `Doubtful` in the October 31 response and `Out` in the November 1 response; both have `07PM` labels. Therefore the cycle label alone cannot identify or order editions across request dates.
- All six bodies total 116,303 bytes. This small sample is not a season-wide volume estimate.
- Existing source/loader tests: 12 passed under the offline guard. The three live calls completed in approximately 2.45 seconds, but that is not a forecast for a full scrape.
- The bounded canonical target-selection query examined 17 candidate games on the three pilot dates and found no qualifying linked props in `raw_player_props_combined`, nor matching `nba_game_schedule` rows. Thus the pilot had zero eligible target games and could not certify T-60 features. This is an exact sampled current-table finding, not proof of a linker defect or empty fitting seasons. No broader recovery investigation was opened.

Evidence bundle: `C:/Users/Chase/GameFlowArtifacts/nba-injury-pilot-20261104/`. It is outside expiring Hermes scratch and contains the preserved originals, new captures and combined replay.

## Population

Fit: seasons 22023 and 22024. Calibration: 22025. Exclude 2026–27 from this job; its initial evaluation/live capture is a separate population.

1. Export distinct historical game IDs/dates for 22023, 22024 and 22025 from canonical schedule/game records. The actual export uses `player_game_stats`, without props joins or minimum-minute filtering.
2. Require a complete export and nonempty requested historical seasons. Season-role labels describe the intended model periods only; no training eligibility rule is applied during raw acquisition. Match injuries by payload game date/team, with player identity resolved separately. Do not make collection depend on already accepted injury features.
3. Form `request_dates = distinct(target_game_dates ∪ previous_day(target_game_dates))` and sort them chronologically.
4. Preserve all returned payload game dates; partition target versus spillover during normalization, not acquisition. Never shift a payload game date to the request date.
5. Remove request dates already captured successfully in the retained evidence store, after digest verification. HTTP-200 error objects and malformed/non-JSON bodies are not successful captures. Empty lists are captured evidence but remain unverified source coverage; inventory them explicitly.

The earlier props-qualified export had empty fitting cohorts, but that was the wrong acquisition population. The corrected game-only export completed with one read-only streaming SELECT and includes all 3,781 historical games. Earlier props-availability findings remain downstream training issues, not reasons to block raw injury collection.

### Hard planning ceiling, not expected work

Using the deliberately broad season envelope July 1, 2023 through June 30, 2026, plus June 30, 2023 for the prior-day boundary, there are at most **1,097 distinct request dates**. This is a calendar bound, not approved model date selection and not a recommendation to scrape offseason dates.

At six requests per batch that is at most **183 batches** before reuse of the six existing captures. Actual required dates should be determined from the cohort export. Maximum stored raw payload under the two-MiB per-response cap is 2,300,575,744 bytes for that ceiling; normalized reports/receipts need additional disk space. Use a disk/quota preflight before launch rather than relying on the tiny sample's average.

## Minimum implementation extension

Retain:
- `RapidAPIInjuryClient.fetch_date_response` in `src/scrapers/rapidapi_injury_backfill.py`.
- `src/scrapers/nba_injury_pilot.py` capture/normalization/receipt logic.
- `scripts/run_nba_injury_pilot.py` for actual per-batch capture and replay.

Implemented operator wrapper: `scripts/run_nba_injury_recovery.py`.

Implementation checkpoint: the pilot helper now exposes per-receipt `verified_capture`, explicit response-body completion markers and an optional per-response progress callback. These enable partial-batch reuse and journaling without changing existing production scraping. The same 12 guarded tests and genuine six-capture replay still pass; no new API requests were used for those checks.

Responsibilities only:
1. Read the approved target/cohort export and construct the exact date manifest; `--dry-run` prints new/reused/empty/failed dates and expected call count without auth/network/DB access.
2. Divide pending dates into at most six-request batches. Require explicit `--execute`, `--max-total-requests`, `--max-batches` and a new run/output directory. Default is dry-run.
3. Call the existing pilot capture sequentially, maintaining the one-second spacing across batch boundaries. Keep the pilot's no-retry behavior and per-batch byte/time limits.
4. Append a run journal after each response/batch. Resume from verified successful receipts, never from mere DB row/date existence. A partially failed batch may contain successful earlier receipts; inspect those individually rather than refetching them blindly.
5. Produce a date coverage inventory and partitioned normalized/quarantine files. Do not load all seasons into one giant in-memory frame. The pilot replay intentionally caps inputs at six captures; full-job normalization must operate batchwise and aggregate summaries, not remove that safety cap casually.
6. Record the exact launched cohort manifest hash, client/code version, request budget, previous-run references, per-date HTTP outcomes and reasons for skipping/stopping.

No new service, scheduler, library, generic ingestion framework or model feature redesign. Do not rewrite existing daily orchestration. Native injury adapter integration and any permanent DB migration are separate follow-up changes.

## Runtime and failure policy

- Owner-launched from the Windows workstation; sequential, existing configured RapidAPI subscription. No automatic billing/subscription changes.
- Per batch: maximum six attempts, no retries, five-second connect/fifteen-second read timeout, two MiB/response, twelve MiB total, three-minute deadline.
- Whole run: explicit approved total request ceiling and batch count, checked before every request; do not hide attempts in retries.
- Stop on authentication failure or 429. Stop on provider/non-JSON/oversized body failure and preserve receipts; an explicit later resume may retry only the recorded failures, within a new approved budget.
- Provider rate-limit headers are operational evidence only; they do not prove dollar pricing or available account credits. Verify the current account quota before launch.
- Wall time is request count plus provider latency and any operator pauses; no completion promise from three fast requests.
- File-only outputs initially. No calls to `store_records`, `ensure_table`, injury linker, local sync, migration, training or production deployment.

## Acceptance and downstream repair

Raw recovery PASS requires every requested date to have a verified response or an explicit unresolved outcome; no silently dropped dates, defaulted failures or untraceable rows. Report empty responses separately and do not certify full source coverage merely because capture mechanics passed.

Normalized repair is non-destructive: preserve request date, payload game date, raw cycle, raw identity/status/reason and retrieval time; quarantine invalid status/name alignment; never guess `Healthy` or overwrite the original API record.

Training acceptance is still conditional on an evidence-backed publication/edition-selection contract, explicit submitted/complete team semantics, pregame identity/roster evidence and a populated run through the real injury consumer. If the provider cannot support those requirements, retain useful raw history but report the exact source gap instead of claiming that the full scrape makes training ready.

A permanent DB write plan must name separate game-date/report/capture fields, edition-aware retention and affected legacy readers before approval. Do not globally repurpose `rapidapi_injuries.report_date` or change the live unique key as an incidental part of this collection job.

## Launch gate

Before Chase launches the full scrape:
- exact cohort/date manifest and required/new/reused request totals;
- current API quota and free disk check;
- reviewed thin wrapper and dry-run receipt;
- raw-only acquisition direction, now explicitly clarified by Chase; source acceptance remains a downstream gate;
- approved total/batch budget and resumable output location.

The corrected manifest passed the real dry-run and disk check. Launch ceilings are 600 requests / 100 batches for the 561 pending dates, with modest resume headroom. Use the launch report's `cohort-game-history.json` command, not either obsolete props-filtered/incomplete manifest.
