# Kalshi Deprecation and Project-Pruning Meta-Review

**Audit date:** 2026-07-18
**Mode:** read-only meta-review; this report is the only file written
**Product constraint:** Chase reports that Kalshi sports markets are unavailable to the project in Michigan. Kalshi sports trading and app functionality are therefore decommissioning targets, not repair or feature-hardening targets.

## Executive verdict

GameFlow should retire the Kalshi **sports** product as an ordered decommission, not continue the live-trading, paper/live-parity, dashboard, model-linkage, or refactor programs.

The current blast radius is larger than one integration:

- Railway registers ten Kalshi jobs unconditionally, including two sports refreshes and five high-frequency live-order lifecycle jobs (`src/orchestration/scheduler.py:1171-1262`). Their configured windows amount to approximately **1,532 scheduler triggers per day** before counting the two child runs inside live resolution. A disabled child can still appear as a successful no-op because gates are job-local, not registration-level (`04-scheduler-ingestion-operations.md:83-118,381-411`).
- Five dashboard API handlers expose Kalshi queue, approval, resume, and cancellation state; every handler is under-authorized before service-role access (`07-dashboard-product.md:84-107`; current routes under `dashboard/src/app/api/kalshi/`).
- The admin bot tracker is wholly Kalshi-specific: it imports approval, stale-order, circuit-breaker, order, P&L, and price-bucket surfaces (`dashboard/src/app/(protected)/bot-tracker/page.tsx:3-12,27-44,105-112`).
- Sports market ingestion, edge computation, paper trading, live queueing/execution/reconciliation/repricing/cancellation/settlement, alerting, analysis scripts, schema, tests, plans, and active knowledge pages remain present.
- Polymarket and arbitrage are coupled to Kalshi. The Polymarket scraper imports player-cache/linking functions from the Kalshi scraper (`src/scrapers/polymarket/polymarket_market_scraper.py:36-46`), while the arb contracts and UI encode paired Kalshi/Polymarket legs (`dashboard/src/types/arb-scanner.ts:1-24`).
- Non-sports Kalshi is a distinct product lane. It scrapes macro markets without edge computation or trading (`src/orchestration/kalshi_nonsports_refresh_job.py:1-16,42-69`) and feeds cross-market matching. Its future is not established by the Michigan sports-market decision.

Immediate policy:

1. Contain new sports orders and externally reachable control mutations.
2. Preserve only the minimum lifecycle needed to identify and close any existing exchange exposure.
3. Remove sports schedules and product surfaces before deleting shared or cross-market code.
4. Archive historical evidence and retain data read-only until legal, accounting, incident, and model-research retention decisions are explicit.
5. Do not spend on exactly-once execution, partial-fill state expansion, exchange-final settlement, paper/live parity, Kalshi dashboard test infrastructure, or Kalshi-specific MLOps. Those findings become removal and archival requirements.

## Scope, evidence, and constraints

Read in this review:

- `AGENTS.md` and `.hermes/audits/tech-debt/README.md`.
- Every completed audit report currently present: `00`, `01`, `02`, `04`, `05`, `06`, `07`, `08`, and `09`.
- Relevant live-trading, paper-trading, dashboard, arbitrage, and readiness plans.
- Current tracked source, tests, dashboard, migrations, scripts, manifests, active understanding docs, checked-in brain history, and command/config paths.

No `03-database-data-lifecycle.md`, `10-performance-storage-cost.md`, or `11-system-overhaul-adjudication.md` exists in the current audit directory. Database and storage dispositions below are therefore conservative and explicitly stop before live-schema or data action.

Prohibited and not performed:

- no DB, API, Railway, Vercel, Supabase, exchange, or live-gate inspection;
- no secret or credential-file inspection;
- no deploy, trade, cancellation, settlement, scrape, training, backtest, or card/plan/register creation;
- no code, config, docs, schema, existing plan, or canonical knowledge edit.

The working tree already contained unrelated modified/untracked work. This report does not interpret or alter it.

## Classification rules used

- **Remove now** — reachable or scheduled sports-Kalshi capability that creates trading, authorization, operational, cost, or misleading-product exposure. “Now” means the first approved decommission slice, not an action authorized by this audit.
- **Remove after dependency check** — Kalshi-specific capability whose deletion must wait for exposure closeout, callsite extraction, retained Polymarket/arb decisions, or historical-data retention.
- **Archive evidence** — incidents, plans, tests, analyses, and historical docs worth retaining outside active runtime/product instructions.
- **Retain shared primitive** — provider-neutral logic with demonstrated supported sportsbook, model, paper-trading, dashboard, or analytics consumers.
- **Needs product decision** — non-sports Kalshi, standalone Polymarket, broader cross-market research, or historical/model surfaces whose continuing product value is not established.

## Domain boundary: what is and is not being deprecated

| Domain | Current evidence | Disposition |
|---|---|---|
| Kalshi NBA/MLB sports ingestion, edge alerts, paper trading, approval, execution, lifecycle, settlement, dashboard | Sports series and stat maps feed a complete scheduled stack (`kalshi_utils.py:18-70,78-143`; `scheduler.py:1171-1205,1217-1262`) | **Remove now / Remove after dependency check** |
| Non-sports Kalshi macro/elections markets | Separate scraper stores `sport=NULL`; no edge/paper trading in its job (`kalshi_nonsports_refresh_job.py:4-16`) | **Needs product decision**; quarantine separately from sports |
| Sportsbook models, odds, normal NBA/MLB paper trading, DFS/user bets | Lane 06 explicitly identifies separate `PaperTrader`, `MLBPaperTrader`, DFS/user lifecycle families (`06-paper-trading-shared-primitives-migration.md:35-75`) | **Retain shared primitive/product** |
| Kalshi-only `batter_hrr` model plumbing | Stat config says no sportsbook prop lines and Kalshi-only availability (`src/models/mlb/mlb_stat_config.py:23-26`; `features/contracts.py:303-305`) | **Needs product decision**: remove as unsupported product surface or retain as research-only model |
| Standalone Polymarket collection/analysis | Polymarket has its own client/utils/scraper, but scraper reuses Kalshi player-linking (`polymarket_market_scraper.py:36-46`) | **Needs product decision**; can survive only after decoupling |
| Polymarket↔Kalshi sports/non-sports arbitrage | Matcher/scanner and paper records require both venues (`08-arbitrage-matcher-scanner-migration.md:1-9`; `arb-scanner.ts:1-24`) | Current product is nonviable without Kalshi; **Needs product decision**, with current privileged UI **Remove now** |
| Historical incidents, model-edge studies, P&L, paper bets, live order records | April 19 incident and later analysis are durable evidence (`brain/Operations/Kalshi-Live-Trading-Startup.md:3-8`; `scripts/fix_apr19_pnl.py:1-17`) | **Archive evidence**; do not treat as active readiness proof |

## Cross-report finding inventory and reclassification

### Report 00 — existing inventory reconciliation

| Finding/evidence | Reclassification | Deprecation interpretation |
|---|---|---|
| Lane 06 paper-trading shared primitives is documentation-only (`00-existing-inventory-reconciliation.md:280-285`) | **Retain shared primitive** for sportsbook/MLB/DFS/user primitives; **Archive evidence** for Kalshi parity scope | Do not implement Kalshi parity. Any future generic paper-trading work must exclude retired Kalshi contracts rather than use Kalshi as the unification anchor. |
| Lane 08 arb matcher/scanner is documentation-only and parked by default (`:283-285`) | **Needs product decision** | Do not execute the migration as written. Re-scope only if Chase retains standalone Polymarket or non-sports cross-market research. |
| TD-005/Lane 07 scheduler complexity and stale docs (`:373-376,402-415`) | **Retain shared primitive** for the generic scheduler; **Remove now** for Kalshi registrations | Removing ten Kalshi registrations reduces scheduler blast radius and must precede a broad registry refactor. |
| Product/deployment/gate truth remained a coverage gap (`:383-396`) | **Archive evidence / dependency check** | Later decommission verification must check deployed revision, schedule inventory, and zero reachable sports control paths; this audit did not inspect them. |

### Report 01 — Python architecture

Report 01 contains no direct Kalshi module finding. Its relevant indirect finding is PA-02: daily/backtest probability owners have divergent fallbacks, including a forbidden Gaussian branch (`01-python-architecture.md:83-112`).

- **Retain shared primitive:** empirical-CDF and model/backtest parity remain required for supported sportsbook/model products.
- **Remove after dependency check:** `src/models/kalshi_edge.py` is a Kalshi consumer and should disappear with the sports integration rather than receive a new shared probability-policy investment.
- **Archive evidence:** TMS-10’s Kalshi integer-contract comparator conflict should be preserved as historical contract-semantics evidence, not used to modify core model probability policy during decommission.

PA-03/04 artifact identity and PA-06 sweep parity are not Kalshi debt; they remain supported-model MLOps concerns and must not be pruned merely because Kalshi consumed model samples.

### Report 02 — testing, CI, and verification

| Finding/evidence | Reclassification | Requirement |
|---|---|---|
| TV-03: dashboard build-only verification cannot protect Kalshi approval/cancellation/auth semantics (`02-testing-ci-verification.md:109-136`) | **Remove now** for privileged routes; **Retain shared primitive** for dashboard test strategy | Do not build a Kalshi feature-hardening harness. Add only decommission/anti-regrowth coverage needed to prove routes/pages are absent or unreachable, while retaining generic dashboard test work for supported product paths. |
| TV-04: promotion/parity gates absent (`:138-168`) | **Retain shared primitive** for model promotion; **Archive evidence** for Kalshi paper/live linkage | Do not let removal of Kalshi tests erase model empirical-CDF, artifact, quote-clean, or scheduler invariant tests. |
| TV-05: god-class commands cite stale/future tests (`:170-199`) | **Archive evidence** | Retire Kalshi migration validation commands with their plans. Do not create missing Kalshi tests solely to satisfy obsolete plans. |
| Existing 22-file Kalshi-focused Python suite | **Remove after dependency check / Archive evidence** | Keep tests while closeout code exists. Then retain only tests for extracted generic primitives and anti-regrowth/import-absence guards; archive the historical behavior suite with the removed implementation revision. |

No dashboard test files currently exist (`02-testing-ci-verification.md:114-124`; `dashboard/package.json:5-10,26-34` as cited there). This increases removal risk but does not justify hardening a retired UI.

### Report 04 — scheduler, ingestion, and operations

| Finding/evidence | Reclassification | Requirement |
|---|---|---|
| Ten always-registered Kalshi schedules, with live gates inside jobs (`04-scheduler-ingestion-operations.md:76-90,104-118`) | **Remove now** | Remove sports refresh/new-order/reprice schedules first. Split non-sports decision before deleting its schedule. Keep only an explicitly bounded closeout mechanism if exposure exists; do not leave high-frequency polling as permanent “safety.” |
| E-07: telemetry collapses NBA/MLB/resolve modes into `kalshi_refresh_job.py` (`:318-347`) | **Archive evidence** for Kalshi; **Retain shared primitive** for scheduler identity | Do not redesign Kalshi telemetry. Preserve a final decommission record and improve generic schedule identity independently. |
| E-09: startup validation is advisory/job-unaware and Kalshi gates are distributed (`:381-411`) | **Remove now** for Kalshi env/gate ownership; **Retain shared primitive** for generic env schema | Remove retired names from active readiness/config docs only after callers are gone. Do not inspect or publish values. |
| E-10: schedule docs are stale (`:413-444`) | **Remove now / Archive evidence** | Active scheduler docs must stop listing Kalshi sports jobs after decommission; historical schedules move to an archive. |
| E-06 settlement after critical stats failures (`:287-316`) | **Retain shared primitive** | This finding concerns sportsbook/MLB resolver policy and should not be deleted with Kalshi. Kalshi internal-stat settlement is separately TMS-08. |

Current source confirms the blast radius: wrappers are at `scheduler.py:724-785`; registrations are at `:1171-1262`; optional arb gates are at `:1269-1308`.

### Report 05 — model/MLOps promotion

Report 05 has no Kalshi-specific model artifact finding. It references trading only to prevent paper evidence from standing in for live execution and to require lineage through selection (`05-model-mlops-promotion.md:91-93`).

- MMP-01 through MMP-09 remain **Retain shared primitive** findings for supported NBA/MLB model production.
- `docs/development_docs/kalshi_sportsbook_reference_ranker_notes_2026-05-24.md` is **Archive evidence**. It explicitly frames Kalshi as execution venue and sportsbooks as references (`:8-18,56-78`), so it is not an active sportsbook ranker specification after venue retirement.
- The generic sportsbook CLV/ranker pipeline remains retained. Remove only Kalshi-specific names, inputs, and interpretation; do not discard quote-clean, CLV, ranker, lineage, or rollback work.
- `scripts/verify_mlb_prediction_outputs.py` mixes supported MLB prediction-output checks with Kalshi table checks (`trading-readiness.../02-prediction-and-kalshi-linkage-verification.md:80-97`). **Remove after dependency check:** preserve the MLB prediction/sample verifier and delete or make optional the Kalshi market/queue/live checks.

### Report 06 — trading and market safety

All ten findings are relevant. Security/live-order/settlement findings are reclassified as containment/removal requirements, not hardening investments.

| Finding | Reclassification | Decommission requirement |
|---|---|---|
| TMS-01 no durable exactly-once submission boundary (`06-trading-market-safety.md:95-118`) | **Remove now** | Stop scheduled/new submissions and remove approval paths. Do not add an outbox/idempotency system for a retired venue. During closeout, assume accepted-but-unrecorded orders are possible and require an approved, read-only exchange-vs-local reconciliation before declaring zero exposure. |
| TMS-02 partial fills treated as full (`:120-142`) | **Remove after dependency check** | Do not trust local `filled` or P&L as proof of zero exposure. Preserve reconciliation code/data until outstanding requested/filled/remaining quantities are independently established; then remove. |
| TMS-03 missing API observation becomes cancellation (`:144-166`) | **Remove after dependency check** | Do not treat local `cancelled` as exchange-final. Closeout must use authoritative exchange state; no permanent unknown-state feature build. |
| TMS-04 exposure cap not execution-time invariant (`:168-189`) | **Remove now** | Stop new approval/execution rather than redesigning transactional reservations. |
| TMS-05 stale refresh can feed selection (`:191-213`) | **Remove now** | Remove sports refresh, edge-alert, paper-selection, and live-proposal paths; do not add Kalshi provenance plumbing. |
| TMS-06 non-equivalent arb contracts can be matched (`:215-237`) | **Needs product decision** | Current sports arb cannot be retained as trustworthy. If cross-market research survives, near/fuzzy results must be review-only and cannot be called executable; otherwise archive/remove the lane. |
| TMS-07 “pure arb” uses indicative prices (`:239-261`) | **Needs product decision / Archive evidence** | Remove “guaranteed/pure” operational claims and paper evidence from active product surfaces. No live two-leg executor exists (`:247,341`), which limits closeout risk. |
| TMS-08 internal stats substitute for exchange finality (`:263-285`) | **Remove after dependency check** | Do not run or trust local settlement as the final closeout authority. Preserve data and code only until authoritative final status is recorded; then archive/remove. |
| TMS-09 paper/live parity is overstated (`:287-309`) | **Archive evidence** | Cancel the Kalshi parity/refactor investment. Retain sportsbook paper-trading primitives, but do not route them through retired Kalshi strategy types. |
| TMS-10 integer contract semantics conflict with literal invariant wording (`:311-332`) | **Archive evidence** | Preserve the lesson that contract wording controls comparator semantics. Do not change core empirical-CDF behavior as part of removal. |

Current mitigations are not reasons to keep the feature. The global/sport gates, human approval, circuit breakers, cancellation path, and status enums (`06:50-69`) reduce accidental exposure but do not eliminate scheduled polling, under-authorized APIs, or uncertain exchange state.

### Report 07 — dashboard and product

| Finding/evidence | Reclassification | Requirement |
|---|---|---|
| F-01 unauthenticated service-role arb verification (`07-dashboard-product.md:60-80`) | **Remove now** | Remove/disable `/api/arb/verify` and the current review UI. If a future non-Kalshi product needs review, build a new authorized capability after a product decision; do not harden this retired cross-market route in place. |
| F-02 authenticated users can mutate Kalshi live control state (`:84-107`) | **Remove now** | Remove/disable queue, approve, and resume endpoints immediately in the decommission sequence. Cancellation endpoints must also leave the public dashboard; if closeout needs cancellation, use a separately approved, audited operations path. |
| F-11 privileged mutation observability is absent (`:301-321`) | **Archive evidence / removal requirement** | Preserve existing audit rows/log evidence and record the decommission actor/revision. Do not build a full Kalshi observability product. |
| F-12 no dashboard test harness (`:325-345`) | **Retain shared primitive** for dashboard; **Archive evidence** for Kalshi | Use minimal anti-regrowth/build checks for removal. Generic dashboard testing remains valuable for auth, billing, Ask, and supported pages. |

The bot tracker has no provider-neutral product content in its current shape: `useBotTracker` reads `get_kalshi_bot_summary`, live/paper Kalshi rows, and Kalshi daily logs (`dashboard/src/lib/hooks/useBotTracker.ts:29-97`). Remove its page, nav links, hooks, types, and components together after any archival export decision. The navbar exposes Bot and Arb links to admins (`dashboard/src/components/layout/Navbar.tsx:99-108,177-186`).

### Report 08 — infrastructure, security, and dependencies

| Finding/evidence | Reclassification | Requirement |
|---|---|---|
| I-04 env ownership is incomplete and includes live trading/arb names (`08-infra-security-dependencies.md:184-221`) | **Remove now / Remove after dependency check** | First remove active sports job references; after exposure closeout, remove Kalshi credential, gate, sizing, Discord, and sport-gate names from values-free contracts/docs/deploy settings through an approved secret-rotation lane. Never inspect or print values in source decommission work. |
| I-05 Railway readiness/rollback absent (`:225-260`) | **Retain shared primitive** | Generic worker rollback remains required. Kalshi decommission verification must prove the deployed scheduler revision no longer registers sports jobs, without running them. |
| I-08 tracked Railway log export (`08-infra-security-dependencies.md:9-20,47-55`) | **Remove now / Archive evidence** | Move operational evidence to an approved restricted archive or delete it after evidence review; do not keep runtime logs in the active repo. This is broader than Kalshi but may contain integration names. |
| `cryptography>=42.0.0` (`requirements.txt:21`) | **Remove after dependency check** | Current source imports `cryptography` only in `src/scrapers/kalshi/kalshi_client.py:43-45,84-85`. Remove the dependency after all retained/non-sports Kalshi client decisions and lockfile updates. |

The Kalshi client contains both read and write endpoints behind one authenticated client, loads path/base64 private keys, and retries requests (`kalshi_client.py:1-18,37-69,99-119,139-199`). Do not attempt to make this client safer for sports. Retain it only temporarily for approved exposure closeout or if Chase explicitly retains non-sports Kalshi.

### Report 09 — agent, GBrain, documentation, and knowledge workflow

| Finding/evidence | Reclassification | Requirement |
|---|---|---|
| AKW-01 canonical roadmap is stale (`09-agent-knowledge-workflow.md:95-126`) | **Remove now** from active knowledge after approval | Canonical roadmap must record the 2026-07-18 Michigan product closure and must not recommend Kalshi readiness, validation, or scaling. |
| AKW-02 transition-era authority pointers remain active (`:130-161`) | **Retain shared primitive** | Fix authority routing independently; do not write the checked-in `brain/` as canonical. Kalshi historical pages found there are evidence, not current truth. |
| AKW-03 audit evidence is untracked (`:165-194`) | **Retain shared primitive / Archive evidence** | This report and its source reports need durable review before code removal so the rationale is not lost. Tracking a report is not approval. |
| AKW-09 legacy trackers remain plausible authorities (`:366-396`) | **Archive evidence** | Mark historical Kalshi “ready/live/go-live/scale-up” commands and checked-in brain pages superseded; preserve incident chronology. |
| AKW-10 secondary routing docs are incomplete (`:400-429`) | **Retain shared primitive** | Knowledge-source routing is unrelated to the product cut and must remain intact. |

High-risk stale active instructions include:

- `brain/Decisions/Kalshi-Integration-Design.md:5-12` says fully implemented and ready to launch.
- `brain/Operations/Kalshi-Live-Trading-Startup.md:1-8,33-49,123-136` says LIVE, provides enablement/preflight, and recommends scale-up.
- `.claude/commands/check-kalshi.md:1-7,24-47,86-116` instructs go-live/scale-up analysis and direct Supabase delegation using retired agent roles.
- `brain/Product/Dashboard-Pages.md:43-46` correctly records the public prediction-markets page was already removed but says backend/bot-tracker and arb UI remain.

These are **Archive evidence**, not deletion-without-history. Canonical GBrain should receive a concise supersession decision during an approved wrap-up; this report does not perform that write.

## Current tracked implementation inventory and disposition

### A. Runtime gates and schedules

**Remove now (sports):**

- wrappers `run_kalshi_refresh`, `run_kalshi_refresh_mlb`, `run_kalshi_live_resolution`, `run_kalshi_daily_summary`, `run_kalshi_execute_approved`, and `run_kalshi_reprice_stale` (`scheduler.py:724-742,754-770`);
- registrations `kalshi_live_resolution`, `kalshi_daily_summary`, `kalshi_refresh_mlb`, `kalshi_refresh_nba`, `kalshi_execute_approved`, and `kalshi_reprice_stale` (`:1171-1205,1217-1236`);
- new-position work inside `kalshi_refresh_job.run` and `_run_live_trading`, currently gated at `src/orchestration/kalshi_refresh_job.py:182-218,343-362` as documented by report 06.

**Remove after exposure dependency check:**

- `kalshi_pending_fills`, `kalshi_stale_fills`, and `kalshi_execute_cancellations` schedules (`scheduler.py:1238-1262`);
- morning resolution/summary if any unsettled records require evidence preservation;
- closeout should not rely on these jobs’ current local-state semantics because TMS-02/03/08 show partial-fill, unknown-observation, and finality defects.

**Needs product decision:**

- `run_kalshi_nonsports_refresh` and its always-registered ten-minute schedule (`scheduler.py:745-751,1207-1215`);
- optional arb registrations and standalone Polymarket scrape (`:1269-1308`).

Default-off arb gates are containment, not a product decision. The non-sports Kalshi refresh is not registration-gated and should not be accidentally removed as if it were sports, or accidentally retained as if Chase approved it.

### B. Dashboard routes, pages, hooks, components, and types

**Remove now:**

- `/api/kalshi/queue`, `/approve`, `/resume`, `/cancel-queue`, `/cancel-approve` route files. Exact privilege evidence is in report 07 F-02 (`:88-105`).
- `/api/arb/verify` because it is unauthenticated service-role mutation (`07:60-80`).
- active navbar links for `/bot-tracker` and `/arb-scanner` (`Navbar.tsx:99-108,177-186`).

**Remove after archival/dependency check:**

- `dashboard/src/app/(protected)/bot-tracker/page.tsx`;
- `dashboard/src/lib/hooks/useBotTracker.ts`, `useTradeQueue.ts`;
- `dashboard/src/components/bot-tracker/` components;
- `dashboard/src/types/bot-tracker.ts` and `types/kalshi.ts`.

The page is an operational controller and historical viewer in one. If Chase wants historical P&L visibility, replace it later with a read-only archived report, not a retained live control UI.

**Needs product decision:**

- `/arb-scanner`, `useArbScanner`, arb components/types. Current records and UI are inherently paired-venue (`arb-scanner.ts:1-24,54-67`), and the verification route is unsafe. A future Polymarket-only analytics page would be a new product surface, not a reason to keep this one live.

### C. Backend clients, scrapers, services, jobs, and models

**Sports-specific Remove now / Remove after dependency check:**

- `src/models/kalshi_edge.py` — model-sample-to-Kalshi market edge owner;
- `src/paper_trading/kalshi_paper_trader.py`, `kalshi_analysis.py`;
- sports mode in `src/scrapers/kalshi/kalshi_market_scraper.py` and sports maps in `kalshi_utils.py`;
- sports refresh/daily summary and all live lifecycle entrypoints under `src/orchestration/kalshi_*.py`;
- the focused service package under `src/trading/kalshi/`: queue, selection, strategy, execution, risk, reconciliation, repricing, cancellation, settlement, actuals, daily ledger, status/state machine, events, and alert adapter.

Deletion order matters. `src/trading/kalshi/actuals_adapter.py` imports stat mappings from `kalshi_paper_trader.py`; lifecycle jobs compose the focused services; Discord alerts are shared in one large module. Remove callers before modules and extract any retained generic data first.

**Client closeout/non-sports decision:**

- `src/scrapers/kalshi/kalshi_client.py` has market reads and live write endpoints in the same class. Retain temporarily only for approved closeout or explicitly approved non-sports reads. If non-sports survives, create/read-only capability separation before removing write methods; do not preserve live sports writes “just in case.”
- `kalshi_discovery.py` is one-time/provider-specific discovery: **Archive evidence** unless non-sports is retained.

**Model-specific decision:**

- `batter_hrr` is documented as Kalshi-only/no sportsbook line (`mlb_stat_config.py:23-26`; `features/contracts.py:303-305`). Decide whether to remove its training/inference/artifacts or retain it as clearly research-only analytics. It must not remain a production-supported betting stat by inertia.

### D. Arbitrage and Polymarket dependency boundary

Current arb architecture is not a provider-neutral primitive:

- plan goal is explicitly Polymarket↔Kalshi (`08-arbitrage...md:1-9`);
- `MarketMatcher` loads Kalshi and Polymarket sources and performs sports/non-sports matching (`:65-94`);
- `ArbScanner` combines matching, opportunity math, storage, and review queue (`:164-185`);
- `ArbPaperTrader` records paired Kalshi and Polymarket legs (report 06 `:239-261`; dashboard type evidence above);
- no live two-leg executor exists (`06:334-342`).

Dependency order if Chase retains standalone Polymarket:

1. Remove/disable current arb mutation API and product claims.
2. Decide whether Polymarket collection/analytics has independent value.
3. Extract player cache/linking from Kalshi: current Polymarket scraper imports `build_player_cache` and `link_player` from the Kalshi scraper (`polymarket_market_scraper.py:36-46`).
4. Keep `src/scrapers/polymarket/{client,utils,market_scraper}.py` only if independently owned.
5. Archive/remove `src/arbitrage/`, `arb_scan_job.py`, `ArbPaperTrader`, arb tables/UI unless a new supported counterpart and contract-equivalence specification exists.

Do not implement the current Lane 08 refactor merely to make deletion prettier.

### E. Retain shared sportsbook/paper/model primitives

Do not remove:

- `src/paper_trading/paper_trader.py`, `mlb_paper_trader.py`, DFS/user resolvers, their supported tables/tests, or sportsbook odds/Kelly behavior merely because Lane 06 grouped them with Kalshi.
- NBA/MLB prediction, sample, backtest, CLV, ranker, feature, artifact, and promotion infrastructure. Kalshi-specific verifier checks and ranker interpretation can be cut while core MLOps remains.
- model invariants: empirical CDF, no global conformal offsets, and Q10 lesson.
- generic `src/utils/time_windows.py` only if a supported retained consumer remains. It is provider-neutral (`time_windows.py:1-21`) but current identified consumers are Kalshi/arb paths. If no retained Polymarket or other consumer remains, classify it and its tests as **Remove after dependency check**, not “shared” by name alone.
- generic scheduler run/telemetry machinery, dashboard auth/billing/Ask, Discord transport, and database client.

### F. Tests

Current focused coverage includes 22 `tests/test_kalshi_*.py` files. It characterizes:

- strategy/config/status/state transitions;
- queue/execution/risk/reconciliation/repricing/cancellation/settlement;
- direct-service orchestration and old-facade absence;
- paper/live Kelly parity;
- query sargability.

Disposition:

1. Keep tests while the corresponding closeout/runtime code exists.
2. Use them to prevent accidental behavior changes during containment, not to justify feature investment.
3. After implementation removal, delete implementation-coupled tests in the same slice.
4. Retain `test_kalshi_live_trader_removed.py` only until the entire Kalshi sports namespace is removed; replace it with a broader anti-regrowth inventory if Chase wants a durable product constraint.
5. Move provider-neutral `et_day_utc_bounds` coverage to a generic test only if a supported consumer remains.
6. Archive historical expected behavior at the removal revision. Do not preserve vulnerable dashboard routes just because no route harness exists.
7. Arb has no targeted `ArbScanner`, `MarketMatcher`, or `ArbPaperTrader` tests (`06:215-237,344-355`). That gap argues against retention, not for an unapproved hardening project.

### G. Config, dependencies, docs, plans, and commands

**Active config/env names to retire after callers/closeout:**

- credentials/base URL/private-key path or base64;
- global/sport live gates and force resume;
- live starting bankroll, Kelly fraction, min/max edge, contract, exposure, sweep, drawdown, daily-loss, and streak controls;
- YES-side/star-hits policy flags;
- Kalshi Discord channel names;
- arb scanner/scrape/alert/paper gates if the entire arb lane is retired.

Values were not inspected. Retirement should include approved provider credential revocation/secret removal after exposure closeout, not merely deleting source lookups.

**Remove after dependency check:** `cryptography>=42.0.0` is used only by the Kalshi client in current tracked source (`requirements.txt:21`; `kalshi_client.py:43-45,84-85`). Regenerate the appropriate lock/requirements artifacts in the implementation slice.

**Archive/supersede plans rather than execute:**

- `.hermes/plans/kalshi-live-trading-state-machine-refactor-2026-05-18.md`;
- `.hermes/plans/kalshi-live-trading-refactor-responsibility-plan-2026-05-18.md` (the old god class has already been removed; focused services now exist);
- `.hermes/plans/trading-readiness-fixes-2026-05-26/02-prediction-and-kalshi-linkage-verification.md` — retain generic MLB output verification only;
- `.hermes/plans/trading-readiness-fixes-2026-05-26/03-kalshi-query-timeouts.md` — archive the sargable-query lesson, no Kalshi index investment;
- Kalshi parts of dashboard and Lane 06 plans;
- Lane 08 unless Chase approves a re-scoped non-Kalshi product.

**Archive/supersede active docs/commands:** startup/go-live/scale-up docs, Kalshi design, `/check-kalshi`, bot-tracker/arb product pages, schedule entries, scraper docs, and ranker notes. Historical session logs and incident postmortems should remain immutable evidence with a prominent superseded status, not active instructions.

### H. Data and schema retention

Tracked DDL is incomplete relative to current code. `migrations/kalshi_live_trading.sql` creates only `kalshi_live_orders`, daily log, and singleton config (`:1-74`); current code also relies on market, orderbook, paper-bet, trade-queue, cancel-queue, arb, Polymarket, and verified-link tables. Several migrations were apparently applied through external/manual paths and are not represented in one tracked chain.

Disposition by data class:

| Data | Default audit disposition | Rationale |
|---|---|---|
| Live orders, fills, fees, queue, cancellations, settlement, config/halt history | **Archive evidence; retain read-only** | Needed for exposure closeout, accounting/tax, incident reconstruction, and TMS-01/02/03/08 uncertainty. |
| Paper bets, daily logs, edge/model snapshots, analysis outputs | **Archive evidence** | Valuable research/postmortem evidence but not active readiness proof. |
| `kalshi_markets` / orderbook snapshots | **Needs product/data-retention decision** | Potentially very large; report history estimated ~26.7M market rows, but no current DB/storage audit was run. Sports/non-sports are co-located via `sport`/NULL conventions, so blind table drop would destroy non-sports evidence. |
| Arb opportunities, paper bets/logs, verified links | **Needs product decision / Archive evidence** | Mixed sports/non-sports and paired-provider evidence. |
| Polymarket markets | **Needs product decision** | Can support independent analytics if retained; do not drop as “Kalshi data.” |
| RPCs/RLS/policies/indexes | **Remove after dependency check** | Live schema truth was not inspected. Drop only after app/backend consumers are gone and a separately approved migration preflight proves scope. |

No table, row, index, policy, RPC, or secret should be deleted in the first containment slice. A later DB plan must inventory live schema and counts through the isolated SQL-runner workflow; destructive counts require independent verification per `AGENTS.md:26-28`.

### I. Observability and operational evidence

Removal requirements:

- Record the last deployed revision containing Kalshi and the first deployed revision without sports registrations.
- Verify scheduler inventory/log registration only; do not trigger jobs as a smoke test.
- Distinguish `disabled`, `removed`, and `closeout-only` rather than accepting successful no-op rows (`04:381-411`).
- Preserve a bounded, sanitized final exposure/settlement reconciliation artifact through an approved operations lane; do not store secrets or raw provider payloads in this repo.
- Remove Kalshi alerts, Discord payload builders, metrics parsers, and dashboard polling only after callers are gone.
- Keep generic scheduler/job failure telemetry and Discord transport used by supported lanes.
- Remove tracked production-log exports from active source control after evidence retention review (`08:47-55`).

## Dependency-ordered decommission map

### Phase 0 — Decision record and containment boundary

1. Record Chase’s 2026-07-18 Michigan sports-market decision in canonical knowledge and mark old go-live/scale-up instructions superseded.
2. Define explicit scope: NBA/MLB sports Kalshi is retired; non-sports Kalshi and standalone Polymarket are unresolved.
3. Freeze Kalshi sports feature work, model-linkage work, dashboard refactors, and live-safety hardening.
4. Do not inspect current secrets/gates in the code-removal lane. Provider credential revocation is a separate approved operational action after closeout requirements are known.

### Phase 1 — Stop new exposure and privileged product control

1. Remove sports refresh/new-proposal, approved-execution, and stale-reprice registrations.
2. Remove/disable `/api/kalshi/approve`, `/queue`, and `/resume`; remove bot/arb nav links.
3. Remove/disable unauthenticated `/api/arb/verify` and stop presenting current arb output as executable/guaranteed.
4. Stop Kalshi sports paper betting, edge alerts, approval reminders, and daily product summaries.
5. Leave no alternate CLI/script that can submit new sports orders by default.

This phase addresses TMS-01/04/05 and dashboard F-01/F-02 through containment, not remediation.

### Phase 2 — Exposure and dependency closeout gate

Requires separate approval because it touches live provider/DB truth.

1. Determine whether any exchange positions, resting/partially filled orders, approved queue rows, pending cancellations, unresolved settlements, or accepted-but-unrecorded orders remain.
2. Treat local `filled`/`cancelled`/`won`/`lost` as non-authoritative because of TMS-02/03/08.
3. If exposure exists, use a bounded one-time closeout runbook with authoritative exchange state and explicit human review. Do not rely on public dashboard routes.
4. Once zero exposure/finality is independently verified, remove pending-fill, stale-fill, cancellation, and settlement schedules/services.
5. Preserve a sanitized closeout artifact and required accounting records.

### Phase 3 — Remove sports product surfaces

1. Delete bot-tracker page, Kalshi routes, hooks, types, and components.
2. Remove sports refresh, paper trader, edge calculator, daily summary, and associated Discord events/formatters.
3. Remove `src/trading/kalshi/` services after all closeout callers are gone.
4. Remove sports series/stat maps and sports scraping modes.
5. Remove Kalshi-only verifier checks while retaining generic MLB prediction/sample validation.
6. Decide `batter_hrr`; if no research owner exists, remove it from supported stat contracts, training/inference, docs, and artifacts in a separate model-safe slice.

### Phase 4 — Adjudicate non-sports Kalshi, Polymarket, and arb

Chase must choose one:

- **Retire all prediction-market integrations:** remove non-sports refresh, Kalshi client/scraper/utils, Polymarket collector, matcher/scanner, arb job/paper trader/dashboard, and their tables after archival decisions.
- **Retain standalone Polymarket analytics:** decouple player linking from the Kalshi scraper; remove all paired-leg Kalshi contracts and “pure arb” product claims; define a supported Polymarket-only consumer before retaining schedules/storage.
- **Retain non-sports cross-market research:** explicitly scope it as research/read-only, separate the Kalshi client’s read capability from order writes, keep arb alerts/paper trading off, require contract-equivalence fixtures before any product claim, and revisit Michigan/legal/access constraints separately. This is a new approved lane, not a continuation of sports trading.

### Phase 5 — Config, dependency, schema, and storage cleanup

1. Remove retired env-name references from code, values-free schema/docs, Railway/Vercel config, and alert routing.
2. Revoke/remove provider credentials through the approved operational secret-management path; never commit or print them.
3. Remove `cryptography` if no retained Kalshi client consumer remains; update locks and build validation.
4. Inventory live schema/RPC/RLS/index/table consumers using SQL isolation.
5. Archive or drop tables only after retention, legal/accounting, non-sports, and Polymarket decisions; independently verify destructive scope.
6. Remove generated/artifact storage tied only to retired Kalshi lanes after a bounded storage audit.

### Phase 6 — Tests, docs, observability, and anti-regrowth

1. Delete implementation-coupled tests with removed code; keep generic model/paper/dashboard/scheduler tests.
2. Add a small anti-regrowth inventory asserting no sports Kalshi schedules, routes, order-submit callsites, or active go-live instructions remain, if Chase wants the product decision mechanically enforced.
3. Update current scheduler/product/system docs and archive old plans/incidents with supersession metadata.
4. Sync the canonical GameFlowBrain decision and verify graph/orphan hygiene through the normal wrap-up process.
5. Verify scoped Python tests, dashboard build/lint, scheduler inventory, no prohibited imports, and deployment registration without executing provider jobs.

## Current blast radius by layer

| Layer | Current tracked surface | Main removal risk |
|---|---|---|
| Scheduler | 10 always-registered Kalshi jobs plus 3 optional arb registrations | High-frequency no-ops/API/DB work; closeout jobs mixed with new exposure |
| Dashboard | 5 Kalshi APIs, bot tracker, arb verification/API/page, hooks/components/types/nav | Under-authorized service-role mutations; history mixed with controls |
| Backend | Kalshi client/scraper/utils, edge model, paper trader, 8+ jobs, 14 focused trading modules | Order lifecycle/data dependencies and shared imports |
| Arbitrage | matcher/scanner/non-sports rules/team normalization/job/paper trader | Paired-provider contracts; false complementarity; no direct tests |
| Polymarket | independent client/utils/scraper but scraper imports Kalshi linking | Accidental deletion of potentially independent collector |
| Tests | 22 focused Kalshi files; no dashboard tests; no targeted arb tests | Removing guards too early or investing in obsolete behavior |
| Config/dependencies | many gate/sizing/credential/Discord names; cryptography | Secret lifecycle and stale readiness docs |
| Data/schema | live/paper/market/orderbook/queue/cancel/arb/Poly/verified-link families; incomplete tracked migration chain | Dropping mixed non-sports/history/accounting evidence |
| Knowledge | active go-live/startup/check command, historical brain, plans, scheduler docs | Agents recommend re-enablement or scaling after product closure |

## Archive and rollback requirements

Archive before deletion:

- this audit set and the exact reviewed removal revision;
- April 19 incident/postmortem evidence and the obsolete `fix_apr19_pnl.py` script, which still imports removed `KalshiLiveTrader` (`scripts/fix_apr19_pnl.py:1-17,30-33,137-165,225-226`);
- state-machine/refactor plans and focused behavior-test inventory;
- historical paper/live analysis and sportsbook-reference ranker note;
- values-free schema/table/RPC/policy inventory;
- sanitized final exposure/settlement reconciliation, if later approved.

Rollback does **not** mean keeping live order paths deployed. The safe rollback unit is the code revision and archived evidence. Re-enabling Kalshi sports after decommission would require a new product/legal/access decision and a new safety review; old gates, plans, and tests must not be treated as launch-ready rollback capability.

## Coverage gaps

1. Current exchange account, positions, resting/partial orders, and final settlements were not inspected.
2. Current Railway/Vercel deployed revision, registered jobs, environment names/values, and provider credentials were not inspected.
3. Current live DB schema, RLS, RPCs, table counts/sizes, mixed sports/non-sports distribution, and retention requirements were not inspected. The missing database/storage audit reports increase this gap.
4. Michigan unavailability was accepted as Chase’s product decision; this audit did not independently research legal/geographic scope or whether non-sports markets differ.
5. No product decision exists for non-sports Kalshi, standalone Polymarket, or read-only prediction-market research.
6. No accounting/tax/legal retention period is recorded for live orders, fills, fees, or statements.
7. No dashboard/browser build or route test was run; source evidence establishes current reachability/authorization shape only.
8. No Python tests were run; referenced tests are static coverage evidence.
9. Checked-in `brain/` is historical/non-canonical. Canonical remote GBrain Kalshi pages were represented through completed audit evidence, not edited or freshly queried in this read-only pass.
10. Some applied Kalshi/arb schema changes are not represented in the tracked migration file set; deletion planning requires live-schema inventory.
11. `batter_hrr` research/model value outside Kalshi execution is unresolved.
12. Whether any customer-facing billing/package promise includes prediction markets or arb was not established. The standalone public prediction-markets page is already documented as removed, but admin product surfaces remain.

## Decisions requiring Chase

1. **Non-sports Kalshi:** retire, retain as read-only research, or retain as a product candidate?
2. **Polymarket:** retire with arb, or retain standalone collection/analytics after Kalshi decoupling?
3. **Arbitrage:** archive all paired-venue work, or re-scope a non-live research lane? Current sports/non-sports scanner, paper P&L, and UI are not safe evidence of executable arb.
4. **Exposure closeout:** authorize a separate read-only provider/DB reconciliation to establish zero open/unknown exposure before lifecycle removal?
5. **Historical data retention:** required period and storage location for orders/fills/fees, paper bets, markets/orderbooks, arb, and incident artifacts?
6. **`batter_hrr`:** remove as Kalshi-only production plumbing or retain as explicitly research-only modeling?
7. **Historical UI:** is a static/read-only archived P&L report desired, or should bot/arb dashboard surfaces disappear completely?
8. **Credential retirement:** after closeout, authorize provider credential revocation/removal from deployed secret stores?
9. **Anti-regrowth policy:** should CI enforce “no Kalshi sports schedules/routes/order submission,” or is canonical decision documentation sufficient?
10. **Knowledge archive:** keep old Kalshi plans/startup docs in place with superseded banners, or move them to a dedicated archive path?

## Recommended review gate

Before any implementation card or plan is created, Chase should approve only these boundaries:

- sports Kalshi scope to remove;
- whether a one-time exposure closeout check is authorized;
- non-sports Kalshi / Polymarket / arb disposition;
- data retention and `batter_hrr` decisions.

After those decisions, decommission work should be split into small reviewed slices in the dependency order above. No slice should deploy, trade, cancel, settle, inspect secrets, or mutate schema merely because this audit identified it.

## Validation record

- Confirmed this report includes: every relevant completed-report finding; sports/non-sports/sportsbook/paper/Polymarket/historical boundaries; exact path/symbol/line evidence; blast radius; dependency order; retained rationale; coverage gaps; archive/rollback requirements; and Chase decisions.
- Confirmed absent audit reports `03`, `10`, and `11` are treated as coverage gaps rather than invented evidence.
- Report-only content and scoped `git diff --check` are the required final validations.
