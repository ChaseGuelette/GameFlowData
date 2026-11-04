
# Layer 1 — Data ingestion

- Revision: 2026-11-03 / v1. Status: design, not implemented or authorized to run.
- Goal: acquire bounded, resumable facts and timestamped odds without embedding features or betting decisions in scrapers.
- Architecture: Python adapters return immutable receipts and normalized source-ID records; layer 2 resolves identity and persists them.
- Stack: retained requests/pandas/Pydantic; proposed `requirements-ncaab.txt` adds Parquet support. R and the SportsDataverse SDK are not required for the initial direct-file/direct-HTTP path.

[Summary](../README.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: database](02-database-and-identity-linking.md)

## 1. Scope and existing-code boundary

Initial products: ESPN-derived season schedules/team boxes/player boxes, live ESPN scoreboards/summaries, and The Odds API pregame spreads/totals. Moneylines are an optional later extension. KenPom, college props, direct NCAA scraping, CBBD, full PBP and injury vendors are not mandatory dependencies.

Historical files bootstrap the corpus; live requests maintain it. Fetch completed-game facts once and calculate rolling features internally. A release loader is not the live-query path. A future SDK implementation must pass the same normalized-record fixtures before replacing transport.

Existing `src/scrapers/daily_player_props_scraper.py` illustrates persistent sessions, distinct live/historical odds envelopes and credit headers. It is not a base class: it is NBA/player-prop-specific and has import-time engine/env behavior and failure-to-empty paths. Do not copy fixed UTC snapshot times, dynamic SQL table interpolation or silent exception handling.

## 2. Proposed files and owners

All `src/ncaab/...` paths are new proposals. Package `__init__.py` files must be import-safe.

| File | Main surface | Interaction |
|---|---|---|
| `src/ncaab/ingestion/http.py` | `HttpFetcher`, `RequestSpec`, `FetchResponse`, `RequestBudget` | Sessions; retry/rate/byte/deadline/credit limits; no domain SQL |
| `src/ncaab/ingestion/sportsdataverse.py` | `SportsDataverseAdapter`, `ReleaseAssetResolver` | Discover/stream exact season assets; normalize them |
| `src/ncaab/ingestion/espn.py` | `EspnLiveAdapter` | Discover scoreboard events; fetch completed-game summaries |
| `src/ncaab/ingestion/odds_api.py` | `OddsApiAdapter` | Current/historical sport-wide requests and snapshot envelopes |
| `src/ncaab/ingestion/normalize.py` | `normalize_schedule`, `normalize_team_box`, `normalize_player_box`, `normalize_odds` | Pure conversions to `NormalizedBatch` with explicit row issues |
| `src/ncaab/ingestion/reference.py` | `ReferenceManifestAdapter` | Import a reviewed season-team classification/alias manifest with provenance; no inferred division membership |
| `src/ncaab/ingestion/runner.py` | `IngestionSpec`, `IngestionPlan`, `IngestionResult`, `IngestionCoordinator` | Plan, resume, execute, reconcile task counts |
| `src/ncaab/cli.py` | `ingest` command | Lazy adapter/engine construction after config and authority checks |

The shared contracts/config/errors/time policy are defined in the overview. Layer 2 owns `PayloadStore`, source/fact/quote repositories and identity resolution; do not recreate these inside each scraper.

## 3. Public interfaces

```python
class HttpFetcher:
    def fetch(self, request: RequestSpec, *, budget: RequestBudget) -> FetchResponse: ...

class SportsDataverseAdapter:
    def plan(self, spec: IngestionSpec) -> list[RequestSpec]: ...
    def fetch(self, request: RequestSpec) -> SourceReceipt: ...
    def normalize(self, receipt: SourceReceipt) -> NormalizedBatch: ...

class EspnLiveAdapter:
    def discover(self, start_date: date, end_date: date) -> DiscoveryResult: ...
    def fetch_game(self, source_game_id: str) -> SourceReceipt: ...
    def normalize(self, receipt: SourceReceipt) -> NormalizedBatch: ...

class OddsApiAdapter:
    def plan(self, spec: IngestionSpec, games: Sequence[GameSchedule]) -> IngestionPlan: ...
    def fetch(self, request: RequestSpec) -> SourceReceipt: ...
    def normalize(self, receipt: SourceReceipt) -> NormalizedBatch: ...

class IngestionCoordinator:
    def plan(self, spec: IngestionSpec) -> IngestionPlan: ...
    def execute(self, plan: IngestionPlan, *, lease: LeaseToken) -> IngestionResult: ...
```

These are contracts to implement, not currently available imports.

`IngestionSpec`: provider/product, historical/live mode, season or date bounds, market/snapshot policy, source-policy version, DB target and explicit resource ceilings. Reject incompatible flags. Canonical DB is the default source destination; local source writes need an explicit approved experiment.

`IngestionPlan`: expanded stable task keys, config hash, already-complete tasks, expected bytes/credits, required mappings and blockers. Unknown paid cost is not zero. Dry run may perform bounded free metadata/SELECT reads, but cannot create schemas, write task rows or consume historical-odds credits.

`IngestionResult`: planned/attempted/completed/no-data/quarantined/failed/deferred counts, actual quota headers, receipts, normalized counts and watermarks. Counts must reconcile. Zero data, request failure and completed coverage are distinct outcomes.

## 4. Historical file workflow

1. Discover the exact dataset/season release asset; schedules and boxes need not have the same filename stem.
2. Record source URL/asset ID, observed ETag/length, retrieval time, parser version and SHA-256. A dataset refresh time is not each row's historical publication time.
3. Consult resume metadata before downloading. Reuse verified cached bytes; detect changed content rather than relying on a filename or maximum local date.
4. Stream to a temporary file under the durable data root, enforce limits, verify format/schema/digest, then atomically finalize.
5. Normalize in bounded batches. Do not load all seasons or all PBP into one dataframe by default.
6. Persist schedule/entities before dependent boxes; unresolved records remain retryable/quarantined.
7. Compare coverage against an independent schedule denominator and retain row failures. A listed season is not coverage certification.

Initial dataset names: `schedule`, `team_box`, `player_box`; `all_core` expands explicitly to these. Initial season-ending years are 2022-2026 plus current 2027 updates after approval. Earlier warm-up data is a separate bounded requirement.

The D1 cohort also requires a versioned season-specific team-classification reference. `ReferenceManifestAdapter` accepts a reviewed provider directory or owner-supplied manifest with source URLs, season/team source IDs, division/conference values and digest. It emits a normal receipt and `NormalizedBatch` into `ncaab_team_seasons`; missing classification is not guessed from a scoreboard group or current conference. This small reference input is distinct from optional NCAA PBP/lineup ingestion.

Downloaded historical rows carry `availability_basis=reconstructed_game_history` to flag their unproven original publication history, but their actual first receipt still establishes `known_at=received_at` for decisions after that observation. Never backdate `known_at` to a game date. A replay before receipt requires the explicitly reconstructed policy; a future live prediction may use those already-observed historical facts strictly. Scheduled maintenance and owner-launched historical acquisition are separately authorized paths.

## 5. Live workflow and normalization parity

Read scoreboards with explicit date/group/limit parameters. A group filter is not proof that all opponents are Division I. Preserve reschedules/status changes, prioritize newly final games, then revisit the correction lookback. One summary can contain both teams and both player groups; do not make redundant per-player/stat requests.

Normalize historical/live records to identical definitions. In-progress or incomplete final responses remain retryable. Future schedules are valid schedule facts, not missing results.

| Canonical field | Historical field | Live field | Rule |
|---|---|---|---|
| Source identities | `game_id`, `team_id`, `athlete_id` | Event/team-block/athlete IDs | Text-preserved identifiers; reject malformed literals rather than repairing them |
| Start | `date`, aware `game_date_time` | Event/competition date | Convert aware values to UTC; retain raw time and ET schedule date |
| Final/OT | `status_type_completed`, `status_period`, `format_regulation_periods` | Status and period metadata | Final flag alone does not prove populated boxes |
| Scores/shooting | Explicit made/attempted columns | Named stats and player labels | Parse names/labels, not fixed array positions |
| Minutes | Numeric `minutes` | `MIN` | Parse verified numeric/minute-second forms; distinguish DNP null from missing/invalid |
| Starter/DNP | `starter`, `did_not_play` | `starter`, `didNotPlay` | Completed-game outcomes, not target-game availability |
| Turnovers | `total_turnovers`, `turnovers`, `team_turnovers` | Corresponding named fields | Prefer verified total; never blindly add an already-included component |

A checked historical row had turnovers 11, team turnovers 1, and total turnovers 11. Adding the first two would fabricate a different value. Establish version-specific mapping fixtures before fallback. Validate reciprocal opponents, distinct teams, final score consistency, nonnegative counts, made <= attempted, and three-point makes <= all made field goals.

A missing athlete ID may quarantine that player without discarding valid team facts; neural readiness can still fail. Do not infer OT from rounded player-minute totals. Do not rewrite DNP source nulls as observed zero statistics; derived role calculations own that explicit interpretation.

## 6. Odds API workflow

Use `basketball_ncaab`; initial markets are `spreads` and `totals`. Send explicit `oddsFormat=american`, `dateFormat=iso`, markets and configured regions. Normalize American payouts with Decimal arithmetic while preserving the original integer price; reject format mismatches rather than guessing. Read credentials at execution only. Never save/log `apiKey`, including exception URLs, request fingerprints or retry messages.

Historical mode:

- Generate requested decision/closing times from the declared schedule policy; deduplicate common times so one sport-wide snapshot can serve multiple games.
- Preserve returned `timestamp`, `previous_timestamp`, `next_timestamp` and nested event/book/market groups.
- Require returned snapshot <= requested time and retain both clocks.
- Resume before paid requests using sport, requested time, regions, market set and policy version. Persist terminal empty tasks to avoid rebuying them.
- Estimate documented request cost and reconcile `x-requests-last`, `x-requests-used`, `x-requests-remaining`; never assume account entitlement/balance.

Current mode:

- Poll within the active-event window and quota budget.
- Live `received_at` is this system's observation time; old provider updates cannot backdate it.
- Preserve response-specific commencement time and reschedules.
- Keep each book/market capture coherent. Do not pair outcomes from different snapshots or incompatible points.

Emit `NormalizedMarketSnapshot` and outcomes. Store unlinked events if necessary, but never use them in inference before identity and orientation are resolved. Home/away reversal is a versioned mapping transformation, not an unchecked sign flip.

## 7. HTTP failures and task lifecycle

Use persistent sessions, connect/read timeouts, bounded jittered retries, and per-host rate/concurrency ceilings. Honor `Retry-After` within the run budget; otherwise defer. Stop before exceeding task/byte/credit/deadline limits.

- Retry selected transport errors, 429 and 5xx within the attempt ceiling.
- 401/403 are access/authentication failures; no bypassing controls or cycling identities.
- Classify 400/422 by documented body; not every 422 means healthy no-data.
- Decode/schema/oversize failures are explicit failures with receipt metadata.
- Empty expected-offseason results can be `no_data`; missing expected games or sections are `incomplete`.

Task states: `planned -> running -> complete | no_data | retryable_failed | terminal_failed | quarantined | deferred`. Reclaim crashed work only after lease expiry and retry checks. Global task keys support resume across runs, while each run separately records attempted/reused tasks.

## 8. Persistence boundary

Layer 1 creates no schema or dynamic table names. Layer-2 repositories own `ncaab_ingestion_runs`, `ncaab_ingestion_tasks`, `ncaab_source_payloads`, canonical facts, `ncaab_market_snapshots`, `ncaab_quotes` and `ncaab_link_issues`.

Perform network and file work outside DB transactions. Persist a bounded normalized batch and checkpoint atomically, checking the current fencing token. A completed task references durable bytes and committed records. Failed commits do not advance readiness/watermarks. Orphan temporary files are cleanup candidates, not valid receipts.

## 9. Planned CLI

These commands are unavailable until implementation; none was executed during design.

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli ingest --provider sdv_espn --dataset all_core --seasons 2022 2023 2024 2025 2026 --config configs/ncaab/research.yaml --dry-run
.\venv\Scripts\python.exe -m src.ncaab.cli ingest --provider espn_live --start-date 2026-11-02 --end-date 2026-11-03 --config configs/ncaab/research.yaml --dry-run
.\venv\Scripts\python.exe -m src.ncaab.cli ingest --provider odds_api --mode historical --start-date 2025-11-03 --end-date 2025-11-04 --config configs/ncaab/research.yaml --dry-run
```

Execution needs `--execute`, resource ceilings and approved paid-credit scope. Dry-run/execute are mutually exclusive; omission plans only. Config covers products, season/date bounds, DB/data root, time policy, parser version and all budgets. Help/import paths cannot load credentials, create an engine, initialize schemas or make requests.

## 10. Implementation order and acceptance

1. Pure request/receipt/config types and import-safe help.
2. Bounded HTTP and durable content-addressed payloads with controlled fixtures.
3. Historical/live normalizers with field-parity fixtures.
4. Planning/resume against repository fakes, then an isolated test database.
5. Odds envelopes/quota handling without paid requests in tests.
6. Separately approved small live canary on the actual deployment host.

Proposed tests: `tests/ncaab/test_ingestion_http.py`, `test_ingestion_normalizers.py`, `test_ingestion_resume.py`, `test_odds_ingestion.py`, `test_cli_import_safety.py`.

Acceptance proves secret redaction, bounded retries, unchanged IDs, timezone handling, DNP distinction, no turnover double count, resume of completed/empty paid tasks, coherent pairs, no task success after failed normalization, and identical normalization from matching historical/live fixtures. A local HTTP 200 is not production or all-season acceptance.

## References

[Source research](../data-sources.md); [Odds API v4](https://the-odds-api.com/liveapi/guides/v4/); [Python release loaders](https://sportsdataverse-py.sportsdataverse.org/docs/mbb/reference/loaders). Existing pattern reference: `src/scrapers/daily_player_props_scraper.py`.
