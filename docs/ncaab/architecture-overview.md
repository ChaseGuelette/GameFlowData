# NCAAB system architecture — design contract v1

- Updated: 2026-11-03.
- Status: complete design scope for implementation review, not implemented software or production acceptance.
- Goal: one reproducible path from basketball facts and historical/live quotes to game distributions, historical evaluation and forward-paper observation.
- Architecture: isolated `src/ncaab/` domain package, PostgreSQL source/result metadata, durable immutable files, and thin default-off integration with the retained scheduler.
- Stack: Python 3.11+, requests, pandas/NumPy, Pydantic, SQLAlchemy/PostgreSQL, XGBoost and APScheduler 3.x. PyArrow is a declared new NCAAB requirement; neural dependencies are later optional additions.

[Handoff summary](README.md) | [Model architecture decision](model-architecture-decision.md) | [Data-source evidence](data-sources.md)

## 1. How to use this specification

This overview owns cross-layer vocabulary, defaults and contracts. Each layer document owns its detailed files, algorithms, storage interactions, failure behavior, implementation sequence and tests. Do not independently redefine a contract in a downstream layer.

**New paths/classes/tables in this set are proposed implementation targets.** Existing code is explicitly labeled and supported by bounded inspection. No NCAAB source files, migrations, database writes, training, schedules, deployments, commits or pushes are authorized by the existence of this design.

Implement one complete baseline vertical slice before expanding model families. Do not restore archived NCAAB code by assuming old paths still exist. Do not broaden into NBA/MLB repairs, dashboard monetization, prediction-market execution or automatic wagering.

## 2. Seven-layer ownership

| Layer | Owns | Detailed specification |
|---|---|---|
| 1 | Provider adapters, pure normalization, task planning/resume and request budgets | [Data ingestion](layers/01-data-ingestion.md) |
| 2 | SQL schema, source/canonical identities, revisions, repositories and durable payload references | [Database and identity linking](layers/02-database-and-identity-linking.md) |
| 3 | Ordered feature contracts, prior-only transforms, state/roster inputs and immutable datasets | [Feature builders and feature store](layers/03-feature-builders-and-feature-store.md) |
| 4 | Chronological fits, model adapters, diagnostics, immutable artifact bundles and release records | [Training and artifact management](layers/04-training-and-artifact-management.md) |
| 5 | Deterministic marginal sampling, quote routing, probability/payout math and candidate decisions | [Prediction and betting decisions](layers/05-prediction-and-betting-decisions.md) |
| 6 | Historical replay, contractual settlement, metrics/CLV, paired comparisons and forward-paper tracking | [Evaluation and forward tracking](layers/06-evaluation-and-forward-tracking.md) |
| 7 | Job dependencies, fenced leases, readiness, correction recovery, logs and default-off scheduling | [Daily orchestration and data quality](layers/07-daily-orchestration-and-data-quality.md) |

```text
Historical files + live basketball APIs        The Odds API
                 \                             /
                  -> [1] adapters/receipts ----
                              |
                              v
                  [2] facts, IDs and quote groups
                              |
                              v
                    [3] as-of feature service
                         /              \
                        v                v
                [4] trainer         [5] prediction + decisions
                 | manifests ----------> |
                                         v
                              [6] replay / paper / settlement

[7] schedules approved bounded work, guards readiness and records outcomes.
```

Layer 6 invokes layer 5; it is not a second prediction implementation. Layer 3 may use layer 5's pure reference-market selector, but layer 5's quote-selection module must not import feature services. Keep this module-level dependency acyclic through shared DTOs.

## 3. Existing integration evidence and reuse limits

| Existing path | Verified capability | Boundary |
|---|---|---|
| `src/db/client.py` | `get_engine(local=False)`; pool/timeout protections; import-time env/engine behavior | Use lazily; validate target config; no credential/fallback assumptions |
| `src/scrapers/daily_player_props_scraper.py` | Sessions, live/historical odds envelopes, credit headers | Reference only; no NBA prop constants, fixed times or silent-error reuse |
| `src/models/quantile_trainer.py` | Separate XGBoost quantile models and monotonic repair | It also fits/applies conformal offsets; do not import unchanged |
| `src/models/nba_artifacts.py` | Digest manifests, manifest-last/finalization patterns | NBA-only validator stays strict; NCAAB has its own artifact owner |
| `src/models/features/nba/contracts.py` | Ordered-list/family contract pattern identified | Do not reuse NBA feature membership or player minutes/rate semantics |
| `src/backtesting/backtest_harness.py` | Phased replay/result lifecycle | NBA/player-prop/BL/sizing coupling prevents wholesale reuse |
| `src/orchestration/scheduler.py` | Script-based `run_job`, names, triggers, process-local locks and shell logs | Add only scoped default-off NCAAB wrappers; domain readiness is separate |
| `nixpacks.toml` | Installs `requirements.txt`; starts the scheduler through `/app/venv/bin/python` | Future dependency-overlay selection is an explicit deployment edit |
| `railway.toml` | Nixpacks builder and same scheduler start command | No separate always-on NCAAB service required by v1 |

Observed manifest differences already exist: `pyproject.toml` is a small core dependency set and its SQLAlchemy pin differs from `requirements.txt`; the latter contains the operational ML/scheduler packages. Do not claim dependency reconciliation has been done. The NCAAB overlay references the retained runtime requirements and adds `pyarrow==23.0.1`, the version exercised in source inspection. Future dependency changes must be tested rather than globally upgrading unrelated packages.

## 4. V1 design defaults and required operator inputs

These are explicit starting research controls, not empirical winners or live-money recommendations. A change increments the affected policy/config fingerprint and is reported in comparisons.

| Area | Default |
|---|---|
| Competition | Verified men's D1-versus-D1, no exhibitions; supported regular/postseason and neutral sites |
| Initial collection | Ending seasons 2022-2026, ongoing 2027; bounded earlier warm-up if required |
| Statistical source | SportsDataverse ESPN releases + direct live ESPN adapter |
| Additional source need | Verified season-specific team classification/reference manifest; never infer D1 from scoreboard group alone |
| Markets | Spreads and totals; moneyline wagers, props and parlays disabled |
| Historical mode | `reconstructed_research` for unversioned old statistical facts; provider snapshots retained for odds |
| Prospective mode | `strict_point_in_time` on observed inputs |
| Decision / close | `tminus60_v1` / `tminus5_v1`; actual paper cutoff and lateness recorded |
| Quote freshness / dispatch lateness | 10 minutes / 90 seconds |
| Independent / aware schemas | `team_box_v1` (31 fields) / `team_box_market_v1` (35 fields) |
| Models | `gbt_independent_v1`, `gbt_market_aware_v1`; same paired cohort/settings |
| Targets | Home-oriented final margin and final total, including supported OT |
| Quantiles | .01, .05, .10, .25, .50, .75, .90, .95, .99 |
| Distribution codec | `bounded_quantile_linear_v1`, 4,096 stratified samples/target; separate marginals |
| Candidate guard | Threshold within Q05-Q95; valid paired pregame quote |
| Selection / stakes | Raw EV >= .02/unit, flat one unit, one contract per market and at most two per game/variant |
| Feature selection / tuning / offsets | Off initially; no global conformal offsets; no BL/Kelly |
| Training windows | Two versus three prior seasons; discovery season 2025, independent season 2026 |
| Scheduling | Disabled until acceptance and owner enablement |

**Required operator inputs before affected execution:** `data_root`, canonical/local target configuration, verified team-classification evidence, ordered reference books, execution-book allowlist with settlement rulesets, regions, paid-credit ceiling, task/byte/runtime ceilings, and paper artifact/release approval. Empty lists/unknown budgets block the affected command rather than selecting all books or assuming free unlimited access.

Statistics-only planning need not wait for bookmaker configuration. Aware training, odds execution, betting replay and paper decisions do. Unknown operational values are explicit configuration requirements, not permission for an implementer to guess.

## Shared contracts

Shared files, owned by the cross-layer integration slice:

| Proposed file | Responsibility |
|---|---|
| `src/ncaab/contracts.py` | Frozen DTOs and enums; no IO or training |
| `src/ncaab/config.py` | Strict Pydantic configuration, expanded settings and command-specific validation |
| `src/ncaab/time_policy.py` | Source-clock eligibility, strict/reconstructed rules, intended/actual cutoff construction |
| `src/ncaab/fingerprints.py` | Canonical serialization and SHA-256 identity; explicit missingness/precision |
| `src/ncaab/errors.py` | Typed errors with stable reason codes and retryability |
| `src/ncaab/cli.py` | Import-safe argparse dispatcher; lazy domain/engine/optional-backend construction |
| `src/ncaab/__init__.py` | Package identity only; no env loading, connections or requests |

No module below the CLI loads `.env` on its own. Existing app DB imports are delayed until explicit target/config validation. HTTP credentials are injected at request time and stripped from saved configuration, URLs, fingerprints, headers and exception logs.

### Contract catalog

| Contract | Required fields / meaning | Producer -> consumer |
|---|---|---|
| `SourceReceipt` | UUID; provider/product/source family; safe request/task identity; received/provider clocks and basis; durable blob ref/hash/bytes; parser version; HTTP/quality/quota metadata | L1 -> L2 and provenance readers |
| `NormalizedBatch` | Receipt ID; typed records with original provider IDs; parsed values/units; row issues; parser/schema versions; no invented canonical mappings | L1 -> L2 |
| `GameSchedule` | Canonical game/home/away IDs; ending season; applicable start/date/status/neutral flag; version ID; classification evidence | L2 -> L3/L5/L6/L7 |
| `AsOfContext` | Aware UTC actual cutoff; intended slot; purpose; time-policy ID/hash; cohort ID/hash; `strict_point_in_time` or `reconstructed_research`; run ID | Caller -> every time-sensitive reader/transform |
| `FeatureBatch` | Ordered `(game_id, decision_at, schema_id)` rows and numeric columns; permitted null masks; source/schedule/state revision refs; per-row grade/reasons; fingerprint; optional roster tensor | L3 -> L4/L5 |
| `TrainingDataset` | Verified feature batch/file identity; separate margin/total labels with result-version/label cutoff; split metadata; train-fitted transforms only | L3 -> L4 |
| `ArtifactManifest` | Bundle type/version; architecture/variant/capabilities; ordered feature/grid/units; fit/selection cutoffs; actual creation; data/config/code/env hashes; file digests; sampling/repair policy | L4 -> L5/L6/L7 |
| `PredictionBundle` | Run/game/artifact/feature identities; actual/intended clocks; marginal quantiles/sample identity; grade/status/reasons; no hidden quote changes | L5 -> decisions/replay/paper |
| `QuoteSelection` | Canonical game/side/market; exact point/price; paired snapshot/outcome IDs; reference/execution role; all clocks and mapping/policy versions | L5 selector -> L3/L5/L6 |
| `BettingDecision` | Prediction and quote identities; win/push/loss/conditional probability; EV/stake; status/reasons; routing/selection rank; strategy identity | L5 -> L6/storage |
| `SettlementRecord` | Exactly one decision/paper target; observed result version; ruleset; outcome/returned/profit units; observation and supersession | L6 -> metrics/storage |
| `ReadinessReport` | Context/scope/version; named pass/warn/block checks; expected/observed counts; source identities; affected games/variants and reasons | L7 -> job/prediction gates |
| `LeaseToken` | Scope; owner UUID; monotonic fence; expiry; job-run identity | L7 -> all executing writers |
| `JobResult` | Run/job/config/lease IDs; executed/status; timing/counters/errors; output refs; requested versus achieved work | L7 -> CLI/scheduler/domain logs |

Batch calls share a context only when their cutoff/policy/slot semantics match. Different game decision times require distinct contexts; do not use one midnight/slate cutoff merely because games share a date.

### Clock rules

- `received_at`: when this process observed a response/file; always truthful UTC.
- `provider_snapshot_at`: provider's verified historical snapshot clock, not our download time.
- `provider_updated_at`: an upstream update guard; not automatically proof our system had the value.
- `known_at`: earliest supported availability for the revision, normally its actual first receipt even for an old season file; nullable only without supported evidence. An old file observed now is valid for future strict decisions, not retroactively for earlier ones.
- `decision_at`: the actual cutoff used to select inputs; separate from intended slot and inference completion.
- `label_as_of`: cutoff for selecting outcomes/corrections; labels are kept out of predictors.

Strict reads filter eligibility first, then choose the latest eligible revision. Historical statistical files without publication history may enter reconstructed research only from earlier ET game dates, unless same-day availability is genuinely supported. A mixture of strict odds and reconstructed stats is not a fully strict backtest. Physically creating a historical walk-forward artifact later is allowed, but its actual creation time is never backdated and its information cutoffs must precede evaluated decisions.

### Identity and fingerprint rules

Source identifiers are preserved exactly. Canonical IDs are separate. Provider aliases/orientation are evidence-backed and versioned; ambiguous joins are blocked. A handicap remains attached to its selected team when source home/away names are reversed.

Use distinct identities:

- `source_fingerprint`: ordered immutable source revisions and parser/availability policy.
- `feature_fingerprint`: schema/order, canonical values/null masks, source/state identity and decision context.
- `artifact_digest`: manifest and immutable model files, not directory name.
- `prediction_fingerprint`: artifact + feature/context + sampler settings.
- `strategy_hash`: logical variant, feature/decision/book/settlement/stake policies; excludes random run IDs and newly refitted weight digests. Exact weights are recorded separately.

Canonical feature exports use float64 values rounded to 12 decimal places with ties-to-even, normalize negative zero, and preserve explicit nulls. Hash and feed the same canonical values; do not round only the hash while predicting from different values. NaN/Infinity may not silently enter JSON hashes. Model matrices may represent allowed nulls as NaN only after contract validation. Decimal prices/points preserve source literals separately from normalized arithmetic. Never use Python's randomized `hash()` or traversal order for seeds.

## 6. File/module dependency rules

```text
contracts / errors / config / time_policy / fingerprints
             ^ imported by every layer, import no layer services

storage <- ingestion
storage <- features
storage + features <- training
storage + pure quote selection + features + model loading <- prediction
prediction + storage <- evaluation
all public services <- operations/jobs <- thin scheduler wrappers
```

The quote-selector module is pure orchestration over quote records/repositories and shared DTOs; it does not import `features.service` or `prediction.service`. Feature/state interfaces use protocols to avoid training/serving import cycles. Model creation uses a whitelist, not an arbitrary class import path taken from an artifact.

All proposed tests live under `tests/ncaab/`; bare test filenames in layer lists are relative to that directory. Add a shared `tests/ncaab/conftest.py` and clearly labeled sanitized/synthetic fixtures under `tests/ncaab/fixtures/`. Tests use dependency injection for clock, HTTP, repositories, file roots and models. No unit test imports a module that connects to the application DB or spends API credits. Optional neural imports occur only when that backend is explicitly selected.

## 7. Database inventory and migration ownership

Layer 2 is the authoritative column/key/index specification. All names below are proposed, with live existence still to be checked by an isolated SQL runner before migration:

```text
ncaab_schema_migrations
ncaab_ingestion_runs
ncaab_ingestion_tasks
ncaab_source_payloads
ncaab_teams
ncaab_players
ncaab_team_seasons
ncaab_games
ncaab_team_source_ids
ncaab_player_source_ids
ncaab_game_source_ids
ncaab_link_issues
ncaab_game_versions
ncaab_team_game_stats
ncaab_player_game_stats
ncaab_roster_memberships
ncaab_market_snapshots
ncaab_quotes
ncaab_feature_builds
ncaab_model_runs
ncaab_model_releases
ncaab_prediction_runs
ncaab_predictions
ncaab_decisions
ncaab_evaluation_runs
ncaab_paper_bets
ncaab_settlements
ncaab_job_runs
ncaab_job_leases
ncaab_readiness_checks
```

Migrations are `migrations/ncaab_001_sources_and_facts.sql`, `ncaab_002_quotes.sql`, `ncaab_003_research_and_paper.sql`, `ncaab_004_operations.sql`. Normal application startup never creates tables. Raw provider payloads/feature arrays/models/reports are stored as verified durable blobs; SQL holds query-critical facts and identity/provenance, not duplicated giant JSON or dynamically widening feature tables.

`artifact_id` in prediction contracts aliases the immutable training-run UUID (`ncaab_model_runs.id` when registered); there is not a second competing artifact-ID table. Research can use a verified file-only dataset/artifact/result bundle without registering results in the canonical DB. Paper requires the SQL repositories and a registered approved release. Backend writes use the canonical server role; RLS/client access remains closed initially. Local training mirrors are explicit and verified. Existing NBA/MLB tables, especially `raw_player_props_combined`, are outside the write/index scope.

## 8. Configuration and CLI surface

Proposed config files: `configs/ncaab/research.yaml` and `configs/ncaab/paper.yaml`. Store fully expanded validated configuration in every run manifest; credentials are never included. Initial section ownership:

| Section | Owner / required content |
|---|---|
| `storage` | Target and durable root; migration/schema version; local mirror provenance |
| `sources` | Products, parser versions, seasons/dates, correction window and reference manifest |
| `budgets` | Task/request/byte/time/credit ceilings, HTTP retry/rate/concurrency and DB batches |
| `cohort` | Season-specific D1 evidence and exhibition/postseason policy |
| `time` | Timezone, strict/reconstructed policy, decision/close offsets and lateness |
| `markets` | Explicit reference/execution books, regions, paired-quote/freshness and ruleset requirements |
| `features` | Exact schema/family versions, optional state/roster settings and null policy |
| `training` | Variant, fold, history window, quantiles, frozen hyperparameters and resources |
| `decision` | Sampler, supported core region, EV threshold, stakes and selection caps |
| `evaluation` | Artifact/dataset refs, label cutoff, report and bootstrap settings |
| `operations` | Enabled components, schedules, lease/heartbeat and readiness policies |

CLI subcommands: `ingest`, `build-dataset`, `train`, `validate-artifact`, `register-artifact`, `approve-paper`, `predict`, `replay`, `paper-settle`, `health`, `run-job`. Writing/long commands default to a dry plan and require `--execute`. Read-only validation/health must never repair or promote state. Help/imports do not load secrets, initialize a DB or make network calls.

## 9. Delivery order and cross-layer acceptance

1. Shared DTO/config/time/fingerprint contracts and fixture identities.
2. Layer-1 pure parsers/request planner plus layer-2 isolated schema/repository foundations.
3. Bounded source-to-canonical-data path, with quarantine/resume and exact evidence.
4. Layer-3 base/aware features and frozen dataset export, then layer-4 boosted artifacts.
5. Layer-5 deterministic probability/quote/decision engine.
6. Layer-6 same-engine replay/settlement/reporting and forward-paper storage.
7. Layer-7 leased services and disabled scheduler wrappers; deploy-host/source/storage acceptance before enablement.
8. State/roster/neural challengers behind the same contracts, with their own learning-curve/feature-family evidence.

A controlled fixture must pass the entire ingestion -> canonical facts -> as-of features -> artifact -> prediction -> decision -> settlement -> report chain before a broad real-data experiment. Fixture success is not coverage, model performance, deployment or daily automation acceptance. Only approved tiny fits belong in future unit/integration testing; this documentation task runs no training.

Each layer includes proposed tests and failure cases. Cross-layer tests additionally verify live/replay equivalence for the same context, exact ID orientation, later corrections, current versus historical snapshot clocks, order-independent probabilities, no-offset artifacts, duplicate-job fencing and complete dropout accounting.

## 10. Review/approval gates and non-goals

Design decisions above are implementable starting defaults. Remaining gates are evidence/operator inputs, not unspecified software behavior:

- Verify season-specific competition classification and historical/live field parity.
- Supply reference/execution books, settlement policies and approved API budgets.
- Check live schema/target state before applying the four migrations.
- Resolve durable production artifact/payload storage and dependency installation.
- Validate source access from the intended deployed host and record a bounded writing smoke separately.
- Select an artifact/release for forward paper explicitly; no automatic wagering or profitability certification.

No global conformal offsets, Gaussian-CDF replacement for sample probabilities, automatic Kelly/BL, arbitrary full-history scrapes, background training, generic framework expansion, or changes to protected NBA/MLB model behavior. Implementers must update these contracts when a substantive choice changes rather than hiding it in code.
