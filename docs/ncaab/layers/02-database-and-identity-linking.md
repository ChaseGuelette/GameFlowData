
# Layer 2 — Database and identity linking

- Revision: 2026-11-03 / v1. Status: proposed schema and repository contracts; no live schema inspection or migration has been performed.
- Goal: preserve identity, historical knowledge and reproducible results while isolating NCAAB from retained NBA/MLB tables.
- Architecture: PostgreSQL normalized/versioned facts plus content-addressed files; SQLAlchemy Core repositories own transactions and queries.

[Previous: ingestion](01-data-ingestion.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: features](03-feature-builders-and-feature-store.md)

## 1. Storage decisions and authority

Use `ncaab_`-prefixed tables in the existing application database, not a new database service. Before implementation, an isolated SELECT-only SQL runner must check whether any names/columns already exist; this document does not assert their live absence. Do not restore archived schemas blindly.

Canonical source writes target remote Supabase through the backend `postgres` role. A local training mirror is downstream and explicitly selected; an old local mirror is not production truth. No implicit cross-database writes or local-to-remote synchronization.

Keep large raw files, immutable feature datasets, models and reports under a configured durable `data_root`, outside Git. Store logical relative `blob_ref` values and digests in PostgreSQL, not machine-specific drive paths. Local filesystem/attached durable volume is the initial `PayloadStore` implementation; object storage is a later compatible implementation. An ephemeral container directory is not acceptable. Deployment must resolve durable storage and transfer/mirror access before jobs are enabled.

## 2. Proposed files and public APIs

| File | Owner / important methods | Used by |
|---|---|---|
| `src/ncaab/storage/connection.py` | `EngineProvider.get(target)`, `read_snapshot()`, `write_batch()` | Repositories; lazy bridge to existing engine |
| `src/ncaab/storage/payloads.py` | `PayloadStore.put_stream`, `open_verified`, `finalize_directory`, `resolve_ref` | Raw ingestion, feature export, artifacts, reports |
| `src/ncaab/storage/source_repository.py` | `SourceRepository.plan_tasks`, `claim_task`, `record_receipt`, `complete_task` | Layer 1 |
| `src/ncaab/storage/fact_repository.py` | `FactRepository.persist_batch`, `games_as_of`, `team_history`, `player_history`, `final_results` | Layers 1, 3, 6 |
| `src/ncaab/storage/identity.py` | `IdentityResolver.resolve_team/player/game`, `record_verified_mapping`, `quarantine` | Normalized source-ID records -> canonical IDs |
| `src/ncaab/storage/quote_repository.py` | `QuoteRepository.persist_snapshot`, `candidates_as_of`, `close_candidates` | Layers 1, 3, 5, 6 |
| `src/ncaab/storage/model_repository.py` | `ModelRepository.register_dataset`, `register_artifact`, `approve_paper_release`, `resolve_release` | Layers 3, 4, 7 |
| `src/ncaab/storage/prediction_repository.py` | `PredictionRepository.start_run`, `save_bundle`, `save_decisions` | Layer 5 |
| `src/ncaab/storage/evaluation_repository.py` | `EvaluationRepository.record_run`, `record_paper_bet`, `append_settlement` | Layer 6 |
| `src/ncaab/storage/ops_repository.py` | `OpsRepository.acquire/renew/release_lease`, `record_job`, `record_readiness` | Layer 7; fencing checks used by all writers |
| `src/ncaab/storage/research_files.py` | `FileResearchStore`, `FileRunSink` | Read frozen replay schedules/quotes/results and persist research outputs without canonical DB writes; same DTO/time-policy contracts |

Repositories accept an engine/transaction dependency, not a secret URL string, and use bound SQL. Table identifiers come from trusted constants, never request strings. Reads return typed records/dataframes with documented ordering, not ORM objects tied to a live session. File-only research uses `FileResearchStore` for `games_as_of`, `candidates_as_of`, `close_candidates` and `final_results` over verified exports; `FileRunSink` implements prediction/decision output methods using bounded run fragments. It does not emulate arbitrary SQL. Layer 6 finalizes these fragments into an immutable report. No file-only implementation is allowed for operational paper-bet idempotency.

Existing `src/db/client.py:get_engine(local: bool = False)` is the retained engine entry point. Its import loads environment and may create an engine; call it lazily after CLI validation. Do not rely on its local URL fallback. Require explicit local connection configuration before local execution. Retain its pool/timeout protections rather than independently calling `create_engine` in every scraper.

```python
class FactRepository:
    def persist_batch(self, batch: NormalizedBatch, receipt: SourceReceipt,
                      *, lease: LeaseToken) -> PersistResult: ...
    def games_as_of(self, context: AsOfContext, game_ids: Sequence[int]) -> list[GameSchedule]: ...
    def team_history(self, team_ids: Sequence[int], context: AsOfContext,
                     *, start_season: int) -> HistoryFrame: ...
    def final_results(self, game_ids: Sequence[int], *, label_as_of: datetime) -> list[GameResult]: ...

class QuoteRepository:
    def candidates_as_of(self, game_id: int, context: AsOfContext,
                         *, markets: Sequence[str], books: Sequence[str]) -> list[MarketSnapshot]: ...

class PayloadStore:
    def put_stream(self, chunks: Iterable[bytes], *, kind: str, max_bytes: int) -> BlobReceipt: ...
    def open_verified(self, ref: str, expected_sha256: str) -> BinaryIO: ...
```

## 3. Global schema conventions

- Canonical team/player/game IDs: `BIGINT GENERATED BY DEFAULT AS IDENTITY`. Provider identifiers remain exact `TEXT`; never reuse an ESPN integer as a cross-provider primary key.
- Run, receipt, artifact and prediction/decision IDs: application-generated UUIDs. Stable logical fingerprints are SHA-256 hex text with length validation, not random run IDs.
- All instants: `TIMESTAMPTZ`; ET schedule dates additionally use `DATE`. Naive timestamps are rejected at the boundary.
- Season: ending-year `SMALLINT`. Store source season/type values alongside normalized enums when interpretation matters.
- Points/counting stats: integers with nonnegative checks. Minutes: finite nonnegative numeric, nullable under an explicit missing/DNP policy.
- Line: `NUMERIC(9,3)`; decimal odds: `NUMERIC(18,8)` with `> 1`. Preserve source price format/value. Quarter-line contracts may be retained as raw offers but are ineligible for the initial binary settlement policy.
- Metadata/provenance/reason lists: JSONB with a schema version. Do not bury canonical IDs, primary clocks or query-critical price fields inside JSON.
- No cascading deletion of source facts or model evidence. Corrections append versions; soft retirement does not erase references.
- Enable RLS with no client-facing grants/policies initially. Backend access remains server-only; any future authenticated dashboard policy is a separate scope.

### Common revision columns

Versioned facts carry `id`, `source_receipt_id`, `source_family`, `parser_version`, `content_hash`, `first_observed_at`, `last_observed_at`, `known_at` nullable, `availability_basis`, `quality_flags`, and `supersedes_id` nullable. `known_at` uses a supported provider snapshot or actual first receipt, including for a historical file downloaded now; it is never invented from the old game date. It is nullable only when no supported availability evidence exists. `availability_basis=reconstructed_game_history` warns about use before that receipt, not about whether the file is known after receipt. `first_observed_at` and evidence content are immutable; `last_observed_at` may advance for a repeated identical observation.

Content deduplication is by canonical key + source family + normalized content hash. A duplicate does not create a fake correction or move the earliest observation forward. A changed revision does not overwrite the old revision. Quotes are different: an unchanged price observed in a new capture slot is still a new market observation.

## 4. Table catalog — source and identity

The following names and columns are the implementation contract. Add purely operational audit columns only if they do not change these semantics; record any substantive revision in the overview.

### `ncaab_schema_migrations`

`version INTEGER PK`, `name TEXT UNIQUE`, `checksum TEXT`, `applied_at TIMESTAMPTZ`, `applied_by TEXT`. Populated by approved migration execution, never by a normal application import. Startup checks required versions and expected columns without mutating schema.

### `ncaab_ingestion_runs`

`id UUID PK`, provider/product/mode, config/policy hashes, DB target, requested seasons/date bounds, start/finish timestamps, status, resource budget JSON, actual request/byte/credit counts, counters JSON, error summary. One execution, including a partial execution, remains auditable.

### `ncaab_ingestion_tasks`

`id UUID PK`, `task_key TEXT UNIQUE`, provider/product/request metadata without secrets, status, attempts, first/last run IDs, lease scope/fence, last receipt ID, retry-after timestamp, completion time and error code. A task key represents a source request/product identity, not a run. Reuse and terminal no-data prevent repeated paid fetches. Detailed per-run attempted task IDs are included in the run's saved task manifest.

### `ncaab_source_payloads`

`id UUID PK`, task/run IDs, source URL with secrets removed, request hash, HTTP status, capture-slot key, provider snapshot/update clocks, received timestamp, content SHA-256, blob ref/format/bytes, parser version, availability basis, sanitized quota headers, parent receipt nullable. Unique `(task_id, capture_slot_key, content_hash)`; preserve changed responses for the same task as separate receipts. A receipt cannot be accepted if its blob cannot be verified.

### `ncaab_teams`, `ncaab_players`

Each has `id BIGINT PK`, display name, creation/retirement timestamps and metadata. Names are presentation, not identity. Mutable player bios do not become historical feature fields. Initial records are established by authoritative ESPN schedule/box identities, not by fuzzy odds names.

### `ncaab_team_seasons`

`id BIGINT PK`, team ID, ending season, division, conference identifier/name, source receipt, validity/knowledge provenance, content hash and revision fields. Classification is season-specific. A missing conference is not proof of non-Division-I status; unverified classification blocks the D1 cohort rather than guessing. Unique team/season/source-family/content hash; index `(team_id, season, known_at DESC)`.

### `ncaab_games`

`id BIGINT PK`, sport fixed to `mens_college_basketball`, home/away team IDs, creation time, current version pointer nullable. Distinct teams required. This is stable identity, not the authoritative historical schedule/result row.

### `ncaab_team_source_ids`, `ncaab_player_source_ids`, `ncaab_game_source_ids`

Separate tables give real foreign keys instead of one polymorphic ID column. Each has `id`, provider, exact source ID, canonical entity ID, mapping version/method, evidence receipt IDs, verified timestamp and reviewer/mapping-policy metadata. Unique `(provider, source_id)` and index on canonical ID. Team aliases used for candidate matching are scoped in metadata by provider/season; never replace exact source IDs.

Game mappings additionally record `orientation` (`same` or `reversed`), source home/away identifiers, source start and the evidence supporting the match. On reversal, swap canonical side labels but keep a handicap attached to its original selected team; do not blindly negate it. A canonical home-handicap scalar must come from the mapped home outcome or a verified opposite-pair conversion. A provider reusing an identifier incompatibly is quarantined, not silently remapped.

### `ncaab_link_issues`

`id UUID PK`, entity type/provider/source ID, candidate IDs JSON, reason, evidence receipts, status (`open/resolved/rejected`), resolution mapping/version and resolution timestamp. Ambiguity remains explicit. Resolution can trigger bounded reprocessing of affected receipts; it does not require refetching paid data.

## 5. Table catalog — game facts

### `ncaab_game_versions`

`id BIGINT PK`, game ID, source receipt, scheduled start UTC, local game date, neutral-site flag nullable, venue ID/name, normalized/raw status, season/type, regulation periods, completed periods, overtime count nullable, home/away final points nullable, completion observation timestamp, source time fields and common revision columns. Index `(game_id, known_at DESC, id DESC)` and `(season, local_game_date)`.

Scores are nullable before final. A changed schedule, final score or status creates a version. Final ties or contradictory periods are quality issues, not automatically valid labels. Read labels through an explicit result query with `label_as_of`, not through the feature-history query.

### `ncaab_team_game_stats`

`id BIGINT PK`, game/team/opponent IDs, game version ID, home/away orientation, points, FGM/FGA, 3PM/3PA, FTM/FTA, ORB/DRB/TRB, assists, steals, blocks, total turnovers, optional team-turnover component, fouls, source statistic definitions, and common revision columns. Unique `(game_id, team_id, source_family, content_hash)`; index `(team_id, game_id)` plus the revision lookup. Two accepted team rows must agree with game orientation and final scores.

Store counting facts, not rolling efficiency columns. Possession estimates and Four Factors belong to layer 3 with a versioned formula. Preserve original total-turnover semantics; never infer a sum that the source already includes.

### `ncaab_player_game_stats`

`id BIGINT PK`, game/player/team IDs, minutes nullable, starter/DNP/ejected flags, points and shooting/rebounding/assist/turnover/steal/block/foul counts, source position label nullable, and common revision columns. Unique `(game_id, team_id, player_id, source_family, content_hash)`; index `(player_id, game_id)` and `(team_id, game_id)`.

Missing player IDs are quarantined before canonical insertion. DNP nulls remain null facts. A later role calculation may explicitly treat a reported DNP as zero minutes; missing/unreported is not interchangeable with DNP.

### `ncaab_roster_memberships`

`id BIGINT PK`, player/team IDs, season range, optional effective start/end, source receipt, membership basis (`published_roster/prior_game_participation/reconstructed_season_list`), known-at evidence, confidence/status and common revision fields. Membership ranges do not establish day-level eligibility. No target-game participant list is used as a pregame roster. Index player/team/season and evidence clocks; preserve transfers and contradictory observations for resolution.

## 6. Table catalog — quotes

### `ncaab_market_snapshots`

`id UUID PK`, source receipt, provider event ID, canonical game ID nullable until linked, mapping version/orientation, bookmaker key, market key, requested timestamp nullable, provider snapshot timestamp nullable, received timestamp, bookmaker/market last-update timestamps nullable, source commencement time, source mode (`historical/current`), normalized available-at clock, clock basis, pair-validation status, raw/normalized market hashes and quality flags.

Unique `(source_receipt_id, provider_event_id, bookmaker_key, market_key, market_hash)`. Index `(game_id, market_key, bookmaker_key, available_at DESC)` and an explicit unlinked lookup. Do not partition or add heavy indexes preemptively; establish volume first.

Historical available-at is the verified provider snapshot clock; current available-at is received-at. Provider update clocks are guards, not replacements. Reject a future last-update value relative to the observed snapshot unless a reviewed source-clock rule explains it. Missing update clocks remain explicit `snapshot_only` provenance, not fabricated values.

### `ncaab_quotes`

`id BIGINT PK`, market snapshot ID FK, selection (`home/away/over/under`), original outcome label, signed offered point nullable for moneyline, original price/format, normalized decimal odds, and parsing/eligibility flags. Use a unique partial index on `(market_snapshot_id, selection, offered_point)` for non-null points and another on `(market_snapshot_id, selection)` for null points. This prevents duplicate moneyline outcomes without assuming a particular PostgreSQL NULL-uniqueness extension. Index the parent snapshot; store exact numeric points rather than string-based comparisons.

The parent group is atomic. Spreads need opposite signed points; totals need equal points; initial supported groups have exactly two expected outcomes. No cross-book/cross-time pairing. Invalid offers remain auditable but cannot reach selection.

## 7. Table catalog — research and runtime results

### `ncaab_feature_builds`

`id UUID PK`, schema/policy/cohort/variant identifiers and hashes, mode/grade, requested date window/decision policy, source revision manifest ref/hash, feature/target metadata refs, row counts, missing/dropout counters, start/finish/status and dataset digest. Feature values and optional roster tensors live in immutable Parquet/array files, not a dynamically widening SQL table. Exact feature order is in the manifest.

### `ncaab_model_runs`

`id UUID PK`, experiment/fold/variant/architecture, feature build ID, train/selection/optional probability-fit cutoffs, feature/config/code/environment hashes, artifact ref/hash, metrics ref/hash, status, creation/finalization times and training-data grade. No row is loadable while the bundle is incomplete.

### `ncaab_model_releases`

`id UUID PK`, model run ID, stage (`research_evaluated/paper_approved/retired`), strategy/config hash, approved-by/at, validation receipt refs, effective time and revocation metadata. A paper approval is not a live-money permission. Resolve the applicable immutable release and record its ID on every prediction run; do not follow a mutable `latest` path mid-run.

### `ncaab_prediction_runs`

`id UUID PK`, purpose (`replay/paper/diagnostic`), `model_run_id` FK and optional approved `release_id` FK, config/policy hashes, input/label cutoffs as applicable, requested game range, start/finish/status, readiness summary and output refs/counters. One prediction run owns one artifact/variant; comparison orchestration creates separate constituent prediction runs.

### `ncaab_predictions`

`id UUID PK`, run ID, game ID, artifact ID/hash, intended and actual decision times, feature/source fingerprint, feature schema, availability grade, marginal quantile representation, sampler/version/seed metadata, prediction fingerprint, status/reasons and created-at. Unique `(run_id, game_id, artifact_id, actual_decision_at)`; index game/decision/artifact.

Resume within the same run reuses the same row. A deliberately new run may record another identical prediction; deterministic values derive from input identity, not the random run ID. This avoids needing a separate run-item junction solely for deduplication.

### `ncaab_decisions`

`id UUID PK`, run/prediction/game IDs, quote and reference snapshot IDs, strategy hash, market/selection/point/decimal odds, win/push/loss probabilities, conditional win probability, EV per unit, flat stake units, status (`bet/skip/blocked`), reasons, intended/actual decision clocks, book-routing evidence and candidate rank. Unique per run/prediction/quote/strategy. Save rejected valid candidates and coverage blockers, not just winners.

### `ncaab_evaluation_runs`

`id UUID PK`, `report_type` (`single/comparison`), prediction run ID nullable, experiment/fold IDs, frozen policy/config/artifact/data digests, replay and label cutoffs, source grade, result/report refs, counters, status and metrics JSON. Single reports require a prediction run; comparison reports require a verified input-evaluation-ID manifest instead. This is report identity, not a second model-fitting entry point. File-only research carries the same identities in manifests without pretending DB rows exist.

### `ncaab_paper_bets`

`id UUID PK`, experiment/strategy/release IDs, game/market/selection, intended decision slot, first decision ID, exact quoted contract/price, unit stake, placement observation time and status. Unique `(experiment_id, strategy_hash, game_id, market_key, intended_decision_at)` prevents duplicate paper bets across retries or new prediction run IDs. One market/slot does not create multiple entries because a refreshed price appeared.

### `ncaab_settlements`

`id UUID PK`, decision ID or paper-bet ID with exactly-one-target check, result version ID, ruleset ID/version, outcome (`win/loss/push/void/pending/manual_review`), returned/profit units, observed-at and supersedes ID. Versioned unique target/result/ruleset hash. Append corrected settlements; reports pin one eligible settlement version and never sum every revision.

## 8. Table catalog — operational control

### `ncaab_job_runs`

`id UUID PK`, job key, trigger (`manual/scheduled`), target DB, config hash, lease scope/fence, source/feature/prediction/evaluation run references, start/finish/status, counters and structured errors. Retained global `job_executions` remains the scheduler shell log; this table is the domain-level result.

### `ncaab_job_leases`

`scope TEXT PK`, owner token UUID, monotonic fence BIGINT, acquired/heartbeat/expiry timestamps and job-run ID. Claim/renew/release are conditional short transactions. A later owner increments the fence; every old writer must fail if its fence/owner is no longer current. This is cross-process protection, unlike the retained scheduler's thread locks.

### `ncaab_readiness_checks`

`id UUID PK`, job/prediction run ID nullable, game ID nullable, check key/version, model/product scope, decision time, status (`pass/warn/block/not_applicable`), expected/observed counters, source manifest hash, reason and observed-at. These are evidence records, not a mutable global ready boolean.

## 9. Identity and orientation resolution

1. Exact existing provider mapping wins only after validating source-format and season/event consistency.
2. Initial ESPN schedules establish canonical games/teams; boxes attach by exact IDs. Player IDs come from explicit athlete/game-team association, not current profile team.
3. For odds, generate candidates using verified team aliases/IDs, compatible home-away orientation and bounded start-time tolerance. A single candidate is not enough if a reschedule/doubleheader creates ambiguity.
4. Require exact corroborating metadata or an explicit reviewed mapping. Persist evidence and orientation. Do not choose the closest string merely to increase coverage.
5. Quarantine ambiguous/unmatched events and expose counts. Resolve/reprocess stored payloads without paying for another historical quote.
6. Crosswalks for optional NCAA/CBBD sources are separately versioned; joining source integer IDs directly is forbidden.

## 10. Time-travel and correction queries

`strict_point_in_time`: filter facts by supported `known_at <= decision_at` before selecting the latest eligible revision. For live facts and newly downloaded historical statistical files, our demonstrated knowledge is actual receipt time. Thus a bootstrap file is usable for future live decisions after it was received, but not as proof of availability before its receipt. Historical odds can use verified provider snapshot time even when downloaded later. Do not treat a later live response or undated historical statistical file as a historical snapshot.

`reconstructed_research`: historical game facts without original publication clocks may be used only from completed games on earlier ET calendar dates, excluding the target game, under a pinned dataset revision. Same-day facts require genuine qualifying observation evidence. Record the reconstruction assumption and possible later corrections. This mode supports research but is not certified historical observability.

Select labels separately using `label_as_of`; later outcomes are allowed as labels, never as predictors. A result correction invalidates/revisions dependent settlement reports. A source-stat correction invalidates downstream feature fingerprints; never use MAX(date) alone as a freshness check.

Use short, read-only repeatable-read snapshots to obtain a consistent bounded feature input set, then release the connection before training/inference. Writes use `with engine.begin()` and bound parameters. No network calls inside transactions. Roll back/close after timeout before issuing more SQL.

## 11. Migrations and dependency order

Proposed new migration files:

1. `migrations/ncaab_001_sources_and_facts.sql` — migration ledger, source/task metadata, identities/mappings, versions and memberships.
2. `migrations/ncaab_002_quotes.sql` — market snapshots/outcomes and quote indexes.
3. `migrations/ncaab_003_research_and_paper.sql` — feature/model/prediction/decision/evaluation/paper/settlement metadata.
4. `migrations/ncaab_004_operations.sql` — job runs, fenced leases, readiness records.

Apply only after schema discovery, explicit target/approval and bounded lock/statement timeouts through the approved SQL-runner workflow. Migration files are not created or executed by this design task. Do not modify or index `raw_player_props_combined`. For later indexes on populated large NCAAB tables, use an approved concurrent-index path outside transaction-wrapped DDL; initial empty-table indexes can be created with their tables.

## 12. Implementation and acceptance

Implement migration contracts and isolated-test-DB fixtures; then connection/payload handling, identity/fact revisions, atomic quote groups, research/result repositories, and fenced operational writes. Use a bounded sample transaction before a real source load.

Proposed tests: `tests/ncaab/test_storage_schema.py`, `test_identity_linking.py`, `test_fact_revisions.py`, `test_quote_repository.py`, `test_payload_store.py`, `test_repository_transactions.py`.

Acceptance covers unique/FK/check constraints, exact-ID preservation, home-away reversal, duplicate content, correction history, strict/reconstructed as-of selection, live-versus-historical quote clocks, crash recovery, lease fencing, source-key pagination, no dangling blob references and no credentials in stored receipts. Integration tests must use an isolated database; they must never default to the live application DB.

## References and retained interfaces

Existing `src/db/client.py:80` exposes `get_engine(local=False)`; lines 31-67 define pool/timeouts. Existing scheduler examples use `engine.begin()` for writes and `engine.connect()` for reads. These were inspected; source URLs/credentials were not printed. SQLAlchemy's [connection/transaction documentation](https://docs.sqlalchemy.org/en/20/core/connections.html) is supporting reference, not authorization to change database state.
