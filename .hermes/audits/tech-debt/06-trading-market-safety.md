# Trading and Market-Safety Architecture Audit

> **Deprecation disposition (2026-07-18):** Chase reports that Kalshi sports markets are unavailable to the project in Michigan. Kalshi-specific findings in this report are preserved as safety evidence but are no longer feature-hardening recommendations. Their current disposition is defined by `12-kalshi-deprecation-and-project-pruning.md`: contain new exposure, establish any required closeout truth, then remove/archive the sports integration. Provider-neutral sportsbook/model/paper-trading findings remain active where explicitly retained by that report.

**Audit date:** 2026-07-18

**Mode:** read-only source/document audit; no API, DB, trade, test, deployment, training, or backtest execution

**Scope:** `src/paper_trading/`, `src/arbitrage/`, `src/trading/kalshi/`, `src/scrapers/kalshi/`, directly related orchestration adapters, targeted tests, god-class lanes 06 and 08, and existing audits 00/01/04.

## Executive verdict

The repository has materially better live-trading containment than the legacy god-class inventory suggests: new-trade creation is default-off behind global and sport gates, proposals require human approval, circuit breakers fail closed on balance failure, order cancellation remains available while new trading is disabled, and the live lifecycle now has focused services and state enums.

Those controls do **not** establish a safe live-money lifecycle. The most important unresolved boundaries are:

1. no durable exactly-once boundary around exchange order submission;
2. partial fills are collapsed into `filled`, while settlement uses requested contracts rather than proven filled quantity;
3. unavailable/incomplete exchange observations can be interpreted as cancellation;
4. the daily exposure cap is selection-time policy, not an execution-time invariant;
5. stale market/edge snapshots can survive refresh failures and reach paper/live selection;
6. arbitrage matching can treat non-equivalent contracts as complementary, and its “pure arb” math uses indicative rather than executable synchronized prices;
7. live settlement is based on internal stats, not authoritative exchange finality;
8. paper and live selection/lifecycle policy still have duplicated owners despite the parity claim.

This report does **not** recommend enabling live trading or Kelly sizing. It also does not recommend changing model architecture, calibration, Q10 behavior, or probability policy without contract-level evidence.

## Canonical prior lessons and invariants

Before interpreting betting behavior, the audit read the remote canonical GameFlowBrain over read-only SSH, including:

- `/home/chase/GameFlowBrain/Operations/Hard-Facts.md`
- `/home/chase/GameFlowBrain/Operations/Critical-Invariants.md`
- `/home/chase/GameFlowBrain/Operations/Kalshi-Live-Trading-Startup.md`
- `/home/chase/GameFlowBrain/Decisions/Kalshi-Integration-Design.md`

It also listed lesson pages and searched them for Kalshi, arbitrage, paper/live, quoted probability, settlement, bankroll, and Kelly terms. Relevant lesson pages surfaced included `Lessons/Quoted-Probability-Semantics.md`, `Lessons/Empirical-CDF-for-Probabilities.md`, `Lessons/Q10-Miscalibration-Is-Edge.md`, and `Lessons/Player-Props-Support-Both-Over-and-Under.md`.

Relevant prior lessons/invariants applied here:

- Live trading remains opt-in and must not be inferred safe from paper profitability alone. Human approval and explicit gates are safety boundaries, not strategy evidence.
- Do not deploy global conformal recalibration offsets; they repeatedly hurt ROI.
- The model's Q10 miscalibration is an edge and must not be blindly “fixed.”
- Probabilities are empirical-CDF probabilities, not Gaussian-CDF probabilities. Canonical critical-invariant wording records `(samples > line).mean()`.
- Quoted market semantics matter: the threshold represented by the contract must be established before interpreting an integer line as an over/under event.
- YES and NO are distinct tradable sides; NO support must not be removed merely because upstream sportsbook language is “over.” The current default remains NO-only unless explicitly enabled.
- Unsupported stats must remain blocked.
- Paper evidence must not silently stand in for executable order, fill, slippage, finality, or recovery evidence.

A current code/canonical wording conflict is recorded as TMS-10. It is deliberately **not** resolved in this report by proposing a probability-policy change.

## Current safety architecture

### New live positions

- `kalshi_refresh_job.run` gates new live work on `KALSHI_LIVE_TRADING_ENABLED` (`src/orchestration/kalshi_refresh_job.py:343-362`).
- `_run_live_trading` additionally requires an authenticated client, passes circuit breakers, and requires `<SPORT>_TRADING_ENABLED=true` (`src/orchestration/kalshi_refresh_job.py:182-203`).
- Selection proposes rows to `kalshi_trade_queue`; it does not directly submit an exchange order (`src/orchestration/kalshi_refresh_job.py:202-218`).
- `kalshi_execute_approved_job.main` rechecks the global live gate, auth, approved/expiry state, and circuit breakers before execution (`src/orchestration/kalshi_execute_approved_job.py:80-139`).

### Risk and lifecycle mitigations already present

- Balance API failure blocks new execution (`src/trading/kalshi/risk_service.py:107-113`; characterized at `tests/test_kalshi_risk_service.py:125-129`).
- Drawdown, daily loss, and consecutive-loss controls exist (`src/trading/kalshi/risk_service.py:114-163`).
- Candidate policy caps daily exposure and per-market contracts at selection (`src/trading/kalshi/strategy.py:288-328`).
- Selection deduplicates player/stat positions across existing orders and queue rows (`src/trading/kalshi/selection_loader.py:172-215`; `src/trading/kalshi/strategy.py:260-286`).
- Queue proposal uses `(game_date, ticker, side)` conflict handling and only renews eligible expired proposals (`src/trading/kalshi/queue_service.py:55-98`, `:156-208`).
- Reconciliation, cancellation, repricing, and settlement remain runnable when new trading is disabled; approved cancellation intentionally does not depend on the new-trade gate (`src/orchestration/kalshi_execute_cancellations_job.py:42-47`).
- Status vocabularies and allowed transitions are centralized (`src/trading/kalshi/statuses.py:6-31`; `src/trading/kalshi/state_machine.py:28-89`).

These are meaningful mitigations, but several services still mutate statuses directly and the state machine does not model `submitting`, `unknown`, `partially_filled`, or `settlement_pending` states needed by the failure modes below.

## Plan and prior-report reconciliation

### Lane 06 — paper-trading shared primitives

Status remains **documentation complete / implementation not started as a lane**, consistent with audit 00 (`.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md:280-285`). The plan explicitly identifies duplicated staking/lifecycle/status behavior and concentrated Kalshi parity tests (`.hermes/plans/god-class-migrations/06-paper-trading-shared-primitives-migration.md:69-75`, `:86-130`).

The current repository has nevertheless advanced adjacent live-Kalshi architecture: `src/trading/kalshi/` now owns strategy, selection loading, queueing, execution, reconciliation, risk, cancellation, repricing, settlement, and statuses. That work does not complete Lane 06. `KalshiPaperTrader.select_bets` remains an independent policy implementation (`src/paper_trading/kalshi_paper_trader.py:173-595`), and paper lifecycle/settlement semantics still differ from live.

Implication: refresh Lane 06's baseline before implementation. Preserve its “behavior changes are separate from extraction” constraint, but add the missing exchange-lifecycle states and golden paper/live decision fixtures from this report. Do not create a competing plan.

### Lane 08 — arbitrage matcher/scanner

Status remains **documentation complete / implementation not started**, also consistent with audit 00 (`.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md:283-285`). Current files still mix DB snapshot loading, fuzzy/near matching, verified links, stale filtering, opportunity math, persistence, and review-queue writes (`src/arbitrage/market_matcher.py:184-911`; `src/arbitrage/arb_scanner.py:76-455`). The plan already calls for pure match keys, matchers, opportunity math, fixture tests, and adapters (`.hermes/plans/god-class-migrations/08-arbitrage-matcher-scanner-migration.md:33-43`, `:81-84`, `:313-410`).

Implication: TMS-06 and TMS-07 are correctness characterization requirements for Lane 08, not reasons to mechanically extract current behavior as “safe.” Record current behavior first, then make semantic changes as separately approved fixes.

### Existing reports 00/01/04

- Audit 00 correctly records lanes 06 and 08 as shelved/documentation-only and notes that default-off arb gates reduce current urgency (`00-existing-inventory-reconciliation.md:280-285`). This report adds concrete latent safety defects; it does not change the lane register.
- Audit 01's broader warning about duplicate probability/edge owners and missing parity fixtures applies directly to TMS-09 and TMS-10 (`01-python-architecture.md:92-112`).
- Audit 04's scheduler map confirms frequent live lifecycle polling and default-off arb registration (`04-scheduler-ingestion-operations.md:76-90`). Its execution-identity finding also applies here: script-only `job_executions` records cannot distinguish Kalshi sport/mode or arb mode (`:325-341`). The order-specific audit trail required by TMS-01/TMS-03 must complement, not replace, scheduler telemetry.

## Findings

### TMS-01 — CRITICAL — Exchange submission has no durable exactly-once boundary

**Evidence**

- Approved rows are read without an atomic claim/lease transition (`KalshiQueueService.fetch_approved_rows`, `src/trading/kalshi/queue_service.py:210-245`).
- The executor then calls `execute_trades` and only afterward writes queue results (`src/orchestration/kalshi_execute_approved_job.py:127-139`).
- `_execute_single_trade` submits the exchange order before `_record_order` creates the local live-order row (`src/trading/kalshi/execution_service.py:64-149`, `:151-200`).
- `KalshiClient.create_order` sends no client-generated idempotency key (`src/scrapers/kalshi/kalshi_client.py:237-263`).
- `_request` retries POSTs after 429, timeout/request exceptions, and 5xx responses (`src/scrapers/kalshi/kalshi_client.py:92-153`). A response lost after exchange acceptance is therefore indistinguishable from a request that never arrived.
- The queue idempotency key protects proposal rows, not exchange submissions (`src/trading/kalshi/queue_service.py:55-98`).

**Failure mode**

An exchange accepts an order, the response times out, and the client retries or ultimately returns `None`. The retry can create a second exchange order; if all responses are lost, the local queue is marked failed and no `kalshi_live_orders` row identifies the accepted position. Concurrent/manual executors can also read the same `approved` row before either records completion. Reconciliation starts from local order IDs, so it cannot reliably discover an accepted unrecorded order.

**Confidence:** High. The missing claim state and idempotency token are directly visible. Whether Kalshi independently deduplicates identical POST bodies was not established and must not be assumed.

**Current mitigation:** Global/sport gates, human approval, queue proposal uniqueness, circuit breakers, per-market position cap, transport retries, and frequent fill reconciliation. None supplies a durable request identity spanning queue, API, and local order records.

**Plan interaction:** Lane 06's lifecycle/idempotency primitives should include a live execution-outbox contract, while audit 04/Lane 07 should persist scheduler attempt identity. Do not bury this correctness change inside a status-module extraction.

**Safe first evidence step:** Add no-network fault-injection tests with a fake exchange that (a) accepts then times out, (b) runs two executor workers against one approved row, and (c) returns a prior order for the same client token. Capture current duplicate/orphan behavior before changing code.

**Done condition:** A queue row is atomically claimed before submission; a stable client order/request ID is persisted before the POST and sent to the exchange; retries reuse that identity; restart recovery can query/reconcile by it; one approved queue row can produce at most one exchange position; and raw request/response outcome plus correlation IDs remain auditable without secrets.

### TMS-02 — CRITICAL — Partial fills are terminalized as filled and settled as full requested size

**Evidence**

- Any non-empty fill list causes reconciliation to set `status='filled'`, with no comparison between summed fill count and requested `contracts` (`src/trading/kalshi/reconciliation_service.py:68-87`).
- The service stores `fill_count`, but the status vocabulary has no `partially_filled` state (`src/trading/kalshi/statuses.py:15-21`).
- Repricing only joins exchange resting orders to DB rows with `status='pending'`; a partially filled order mislabeled `filled` drops out (`src/trading/kalshi/repricing_service.py:71-87`).
- Settlement loads both `contracts` and `fill_count` but computes payout/loss using `contracts`, not `fill_count` (`src/trading/kalshi/settlement_service.py:54-75`, `:102-116`).
- The reconciliation test explicitly expects four fills to become filled, but no targeted test covers `fill_count < contracts` (`tests/test_kalshi_reconciliation_service.py:152-168`).

**Failure mode**

A 10-contract order fills 2 and rests 8. Reconciliation marks the local order filled, lifecycle polling/repricing no longer owns the remainder, and settlement calculates winnings/losses for 10 contracts while cost/fee may represent only 2. This can overstate P&L, distort bankroll/high-water marks and loss streaks, leave an unmanaged resting remainder, and make later exchange fills invisible.

**Confidence:** High.

**Current mitigation:** Requested and filled counts are both persisted; API fills are aggregated with weighted prices; periodic reconciliation exists. Those facts make a correct implementation possible but do not enforce it.

**Plan interaction:** Lane 06's status/lifecycle primitive needs exchange-specific quantity states rather than forcing live orders into paper-style terminal labels. TMS-02 must be fixed before treating paper/live lifecycle parity as meaningful.

**Safe first evidence step:** Add a pure fake-client characterization test for requested 10 / filled 2 / still resting, followed by another fill. Assert current status, outstanding quantity, settlement input, and repricing visibility; do not call an integration.

**Done condition:** Requested, filled, cancelled, and remaining quantities are explicit; partial fills remain nonterminal and reconcilable; cancellation/repricing acts only on the remainder; settlement uses authoritative filled quantity/cost/fees; repeated fill events are idempotent; and quantity conservation is tested across submit, partial fill, cancel, replace, and settle.

### TMS-03 — HIGH — “Not observed as resting” can be misclassified as authoritative cancellation

**Evidence**

- `reconcile_fills` builds `resting_ids` from one `list_orders(status='resting')` result (`src/trading/kalshi/reconciliation_service.py:41-50`).
- A pending order absent from that set and without returned fills is updated to `cancelled` with zero P&L (`src/trading/kalshi/reconciliation_service.py:89-103`).
- `KalshiClient.list_orders` and `get_fills` collapse a failed `_request` to empty lists (`src/scrapers/kalshi/kalshi_client.py:291-318`).
- `list_orders` does not traverse a cursor, while other client methods expose cursor fields (`src/scrapers/kalshi/kalshi_client.py:291-300`, compared with `get_markets` at `:155-182`).
- The focused test locks in “not resting + no fills = cancelled” but does not distinguish authoritative terminal response from unavailable/incomplete observation (`tests/test_kalshi_reconciliation_service.py:138-149`).

**Failure mode**

An API timeout, failed request, pagination omission, or transient exchange inconsistency returns an incomplete resting set and no fills. The local order is terminalized as cancelled even though it may still be open or later fill. It then leaves pending-fill and repricing ownership, while local exposure/P&L/audit state diverges from the exchange.

**Confidence:** High for the collapse of unknown to empty; medium-high for pagination exposure because the API's actual default page size was not inspected.

**Current mitigation:** Client retries transient requests, reconciliation repeats frequently, and filled data already persisted can promote a pending row. The terminal cancellation update prevents later retries from repairing the unknown case automatically.

**Plan interaction:** This is a live order-state correctness fix adjacent to Lane 06 and audit 04 failure semantics. It should add an `unknown/reconcile_error` state and observation provenance, not merely rearrange service files.

**Safe first evidence step:** Fake `list_orders` and `get_fills` failures separately from genuine empty success, plus a truncated-page fixture. Record whether any local terminal mutation occurs.

**Done condition:** Transport failure, incomplete pagination, and authoritative exchange terminal status are distinct; no local terminal cancellation occurs without authoritative evidence; order lookup/list pagination is complete; unknown rows remain retryable and alertable; and recovery tests prove convergence after an outage.

### TMS-04 — HIGH — Daily exposure is not an execution-time invariant

**Evidence**

- Daily exposure is calculated and enforced during candidate selection (`src/trading/kalshi/selection_loader.py:79-170`; `src/trading/kalshi/strategy.py:288-328`).
- Queue rows retain the selected contract count until human approval (`src/trading/kalshi/queue_service.py:55-98`).
- Execution rechecks circuit breakers and recalculates Kelly/per-market count from current balance/orderbook (`src/orchestration/kalshi_execute_approved_job.py:100-139`; `src/trading/kalshi/execution_service.py:64-121`).
- Neither `KalshiRiskService.check_circuit_breakers` nor `KalshiExecutionService.execute_trades` reserves/rechecks the effective daily exposure cap against all currently filled, pending, and simultaneously executing orders (`src/trading/kalshi/risk_service.py:65-163`; `src/trading/kalshi/execution_service.py:52-149`).

**Failure mode**

Several proposals are sized under one exposure snapshot, remain pending approval, and execute after another order or process has consumed the cap. Each order can pass bankroll/per-market Kelly checks while aggregate daily exposure exceeds the intended boundary. Concurrent executors amplify this because there is no atomic day-level reservation.

**Confidence:** High.

**Current mitigation:** Human approval, proposal-time cap, current-balance Kelly recalculation, per-market max, existing-position capacity, drawdown/daily-loss/streak breakers. Balance limits insolvency but is not equivalent to the intended daily exposure policy.

**Plan interaction:** Lane 06 can own shared exposure arithmetic, but the live invariant requires a transactional reservation/execution adapter. Do not use this finding to change Kelly fractions or recommend Kelly/live activation.

**Safe first evidence step:** Add no-DB pure fixtures for stale approvals, then a fake transactional repository test with two executors competing for the last exposure capacity. Characterize whether pending/resting orders count toward exposure.

**Done condition:** Effective daily exposure is revalidated and atomically reserved at execution; all filled, partially filled, resting, submitting, and reserved quantities have explicit inclusion rules; reservations release on authoritative failure/cancel; and concurrent execution cannot exceed the configured cap.

### TMS-05 — HIGH — Refresh failures can fail open to stale market and edge snapshots

**Evidence**

- Market scrape failure is logged and the refresh continues (`src/orchestration/kalshi_refresh_job.py:258-269`).
- Orderbook failure is explicitly non-fatal (`src/orchestration/kalshi_refresh_job.py:270-278`).
- Edge-computation exception is logged and the refresh continues (`src/orchestration/kalshi_refresh_job.py:280-302`). Only a specific missing-samples result blocks downstream work (`:304-307`, `:343-346`).
- Paper and live candidate loaders choose the latest row anywhere in the target ET day and impose no maximum snapshot age (`src/paper_trading/kalshi_paper_trader.py:200-225`; `src/trading/kalshi/selection_loader.py:79-112`).
- Selection checks `close_time` but not source snapshot age or edge-computation run identity (`src/trading/kalshi/selection_loader.py:256-271`).

**Failure mode**

A prior same-day snapshot already has model/BL edges. A later scrape or edge refresh fails. The pipeline continues and can select the older row as still `open`, propose it for live approval, place paper bets, or send edge alerts even though price, spread, close status, and edge are stale. Human approval does not reveal the age/provenance because queue rows do not persist a source snapshot identity.

**Confidence:** High.

**Current mitigation:** Scheduled refresh frequency, same-ET-day filtering, stored market status, close-time expiry, execution-time orderbook/slippage checks, and missing-MC-sample blocking. Execution-time price checks reduce price drift but do not validate market identity, model-edge freshness, or stale approval semantics.

**Plan interaction:** This crosses Lane 06 selection inputs and audit 04 job dependency/failure telemetry. It is a correctness gate, not a scheduler-registry refactor.

**Safe first evidence step:** Use repository/fake-engine tests representing a successful 10:00 snapshot followed by 10:10 scrape/edge failure. Assert what paper/live selection and alerts see at 10:11, without API or DB integration.

**Done condition:** New proposals require a successful, bounded-age market snapshot and edge computation from the same declared refresh cycle (or an explicit, tested provenance contract); scrape/edge failure blocks only new positions while allowing reconciliation/cancellation/settlement; source timestamps/run IDs are persisted to queue/live audit records; and stale rows produce a visible fail-closed reason.

### TMS-06 — HIGH — Arbitrage matching can label non-equivalent contracts as complementary

**Evidence**

- Player-prop matching accepts lines within `NEAR_LINE_TOLERANCE = 0.5` and fuzzy names at score `>= 0.80` (`src/arbitrage/market_matcher.py:272-307`).
- The resulting `MatchedMarket` carries the Polymarket line while using prices from the potentially different Kalshi line (`src/arbitrage/market_matcher.py:314-334`).
- Game totals can match lines within the same tolerance (`src/arbitrage/market_matcher.py:472-481`).
- `ArbScanner.scan` sends every matched pair to opportunity detection; `match_type` and `match_confidence` are recorded but not used as eligibility gates (`src/arbitrage/arb_scanner.py:159-175`, `:317-348`).
- No targeted arbitrage test files were found under `tests/` for `ArbScanner`, `MarketMatcher`, or `ArbPaperTrader`; Lane 08 still proposes those fixture suites (`.hermes/plans/god-class-migrations/08-arbitrage-matcher-scanner-migration.md:313-410`).

**Failure mode**

Kalshi “player 1+ hits” and a Polymarket contract at a nearby but different threshold, or totals differing by 0.5, are treated as mutually exhaustive opposite legs. Both legs can lose or fail to produce the assumed $1 combined payout. A fuzzy player/date/title false positive creates the same semantic break. The scanner can then call this a `pure` guaranteed arb based only on prices.

**Confidence:** High for the eligibility path; high that different thresholds are not generally complementary.

**Current mitigation:** Same stat/player ID exact matching is preferred, game match keys include team/date/type, game start filtering exists for non-props, non-sports fuzzy matches are routed to review, liquidity/price sanity filters exist, and arb registration/alerts/paper trading default off. Player/game near matches do not receive equivalent review treatment.

**Plan interaction:** Lane 08 is the correct owner. Its pure match-key and fixture phases must define contract equivalence before extraction. Current behavior should not be canonized as a safe golden result.

**Safe first evidence step:** Build table-driven pure fixtures for exact, ±0.5, N+ versus over-N, wrong player, doubleheader/date, total, spread, and push-capable contracts. Assert an explicit `executable_complement` eligibility flag separately from similarity confidence.

**Done condition:** Arb eligibility requires a normalized outcome/settlement key proving complementary contracts, including participant, date/event, threshold, comparator, side, void/push rules, and resolution source; near/fuzzy matches are review-only; scanner math rejects non-equivalent pairs; and fixture tests cover known false-positive classes.

### TMS-07 — HIGH LATENT / MEDIUM CURRENT — “Pure arb” uses indicative prices and cannot prove paired execution

**Evidence**

- Opportunity math uses `pair.kalshi_yes_price` / derived NO and `poly_yes_price` / derived NO, not synchronized executable asks (`src/arbitrage/arb_scanner.py:257-289`).
- A Kalshi bid floor is used as a market-existence check, but the side's executable ask depth is not priced (`src/arbitrage/arb_scanner.py:262-306`).
- `min_fillable` is estimated from cumulative Kalshi volume and Polymarket liquidity, not level-by-level available quantity at the two required prices (`src/arbitrage/arb_scanner.py:296-315`).
- `ArbPaperTrader` inserts both legs at detected prices and states that pure-arb P&L is deterministic (`src/paper_trading/arb_paper_trader.py:85-123`, `:140-221`, `:268-298`).
- There is no live two-leg arb executor in the audited scope; current orchestration only scans, alerts, and optionally paper trades behind default-off gates (`src/orchestration/arb_scan_job.py:89-110`, `:144-203`).

**Failure mode**

Last/mid prices from unsynchronized snapshots show combined cost below $1 while one or both executable asks have moved or lack depth. A manual consumer acts on the “pure arb” alert and fills only one leg, or fills both above the modeled cost. Paper P&L remains positive by construction and therefore overstates executable evidence.

**Confidence:** High.

**Current mitigation:** Fees, volume/liquidity floors, low-price sanity checks, game-start filtering, pure-only paper selection, default-off registration/alerts/paper gates, and no audited live arb executor. Those controls materially reduce current exposure.

**Plan interaction:** Lane 08's opportunity-math extraction must separate indicative discovery from executable validation. Lane 06's arb paper adapter should preserve an explicit simulation-quality label rather than presenting paired fills as observed.

**Safe first evidence step:** Create pure in-memory fixtures with bid/ask/depth timestamps and partial-leg scenarios. Compare current indicative margin with an executable two-leg quote calculation; no API, DB, backtest, or trading action is needed.

**Done condition:** Discovery and executable qualification are distinct; executable qualification uses side-correct synchronized asks, fee/slippage rules, and depth for equal quantities; paper records quote ages and simulated fill assumptions; alerts never call an opportunity guaranteed without contract equivalence and executable paired depth; and no live executor is introduced by this remediation.

### TMS-08 — HIGH — Live settlement is internal-stat resolution, not exchange-final reconciliation

**Evidence**

- `KalshiSettlementService` resolves all local `filled` orders for dates before today from `fetch_actuals`, using `actual >= line`, and immediately cancels rows when actuals are absent (`src/trading/kalshi/settlement_service.py:54-116`).
- The injected exchange client is used only for a post-update balance alert, not to establish market settlement/finality (`src/trading/kalshi/settlement_service.py:150-189`).
- Actuals come from internal NBA/MLB stat tables and legacy paper mappings (`src/trading/kalshi/actuals_adapter.py:28-94`).
- Paper settlement waits more than 48 hours before cancelling missing-stat bets (`src/paper_trading/kalshi_paper_trader.py:714-739`); live settlement has no equivalent grace/finality state.
- The settlement tests explicitly expect missing actuals to become `cancelled` (`tests/test_kalshi_settlement_service.py:176-205`).

**Failure mode**

A delayed/postponed/corrected stat, DNP/void rule, market-specific wording, or exchange adjudication differs from the internal table. Local status/P&L becomes won, lost, or cancelled before authoritative exchange finality. Risk streaks, daily ledger, alerts, and reported bankroll then diverge from cash settlement; reruns cannot repair terminal rows because settlement only selects `filled`.

**Confidence:** High that exchange finality is not consulted; medium on the frequency of real adjudication divergence because no exchange/DB data was queried.

**Current mitigation:** Only prior dates are resolved, actual lookup is stat/sport specific, filled orders are reconciled first in refresh orchestration, DNP can map to `None`, and updates are transactional per settlement run. These are paper-resolution controls, not exchange-final controls.

**Plan interaction:** Lane 06 should distinguish paper outcome estimation from live exchange settlement. Audit 04's warning that resolution can run after upstream failures reinforces the need for explicit finality preconditions.

**Safe first evidence step:** Build fixture-only comparisons for normal final, delayed stats, DNP/void, postponement, correction, integer-threshold wording, and exchange result disagreement. Record current local transition and whether it can be corrected.

**Done condition:** Live orders enter a settlement-pending state; authoritative exchange market/order settlement determines terminal cash outcome and quantities; internal stats are explanatory/reconciliation evidence rather than sole finality; delayed/corrected/void cases remain retryable; discrepancies alert and remain auditable; and terminal updates are idempotent and repairable under a controlled reconciliation path.

### TMS-09 — MEDIUM — Paper/live parity is asserted more broadly than it is tested or owned

**Evidence**

- `KalshiPaperTrader.select_bets` says it mirrors live logic but independently implements gates, static filters, BL fallback, side choice, sportsbook-line preference, greedy Kelly allocation, overflow, persistence, and immediate fill assumptions (`src/paper_trading/kalshi_paper_trader.py:173-595`).
- Live selection has a separate typed implementation in `select_trade_intents` (`src/trading/kalshi/strategy.py:256-354`) plus DB input assembly (`src/trading/kalshi/selection_loader.py:79-300`).
- Some primitives are shared: supported-stat/config constants and Kelly sizing (`src/trading/kalshi/live_trading_config.py:1-33`; `src/paper_trading/kalshi_paper_trader.py:36-39`, `:143-154`).
- `tests/test_kalshi_paper_live_strategy_parity.py` checks only Kelly contract sizing and an explicit contract cap (`:18-54`). It does not compare selected tickers/sides, filters, exposure allocation, prices, queue dedup, fill lifecycle, or settlement.
- Live strategy tests cover live policy alone (`tests/test_kalshi_strategy_policy.py:12-152`).

**Failure mode**

A policy fix lands in only one owner. Paper evidence can select a different market/side/size than live, while the parity-named test remains green. Even identical selections assume immediate paper fill at stored price, whereas live uses orderbook checks, approval delay, resting/partial fills, cancellation, and repricing. Paper ROI can therefore be interpreted as live-executable evidence when it is not.

**Confidence:** High.

**Current mitigation:** Shared constants/Kelly function, comments documenting intended parity, same source table, NO-only default, stat whitelist, and focused tests for each live service. The architecture intentionally keeps paper and live persistence/lifecycle different; that difference needs explicit contracts rather than a broad parity label.

**Plan interaction:** This is the central refreshed requirement for Lane 06. Extract pure decision policy only after golden fixtures capture current divergences; keep paper fill simulation and live execution as separate adapters.

**Safe first evidence step:** Feed one in-memory candidate fixture matrix to thin paper/live decision adapters and record ticker, side, skip reason, contracts, exposure ordering, and source probability. Include pending/held positions, stale close time, unsupported stats, sportsbook alignment, YES disabled, and cap exhaustion.

**Done condition:** One pure, typed decision-policy owner produces selection/size intents for both modes; explicit adapters own paper simulation versus live queue/execution; golden tests enumerate intentional differences; parity tests cover decisions rather than only Kelly arithmetic; and reports clearly label simulated versus observed executable behavior.

### TMS-10 — NEEDS CANONICAL RECONCILIATION — Integer quoted-probability code conflicts with literal empirical-CDF invariant wording

**Evidence**

- Canonical `Operations/Critical-Invariants.md` records empirical CDF as `(samples > line).mean()` and prohibits Gaussian CDF.
- `KalshiEdgeCalculator.compute_edges` intentionally uses `>=` for integer Kalshi “N+” lines and `>` for non-integer lines (`src/models/kalshi_edge.py:298-309`).
- Paper and live settlement also treat YES as `actual >= line` (`src/paper_trading/kalshi_paper_trader.py:678-684`, `:740-752`; `src/trading/kalshi/settlement_service.py:100-116`).
- No targeted Kalshi edge test surfaced for integer N+ versus half-line semantics; the nearby refresh tests fake the edge calculator (`tests/test_kalshi_refresh_job_direct_services.py:102-164`).

**Failure mode**

Two unsafe responses are possible. Blindly enforcing literal `>` on a contract whose YES event is “N or more” understates YES probability; retaining `>=` without a canonical contract-normalization rule appears to violate a critical invariant and allows future paths to improvise comparator semantics. Different parsers can then compute or settle the same quoted contract differently.

**Confidence:** High that the wording/code conflict exists; low that current `>=` behavior is wrong without inspecting representative authoritative contract wording. This is a policy-clarification finding, not a recommendation to change calibration or probability behavior.

**Current mitigation:** Code comments document N+ semantics, settlement uses the same comparator, all paths use empirical samples rather than Gaussian CDF, and the quoted-probability lesson recognizes contract semantics as a separate concern.

**Plan interaction:** Audit 01's probability-owner parity concern applies. Lane 06 can consume a canonical threshold/comparator value object, but model/calibration policy remains out of scope.

**Safe first evidence step:** Use stored code fixtures or sanitized contract-title fixtures—no API/DB call—to map exact wording (`N+`, `over N`, `over N.5`) to normalized event/comparator, then compare edge and settlement outputs. Ask Chase to validate any canonical wording update.

**Done condition:** Canonical documentation explicitly distinguishes model sample CDF from quoted-contract threshold normalization; one tested comparator/threshold representation feeds edge, paper, live, and settlement paths; integer/half-line fixtures agree; no Gaussian path exists; and no Q10/global-calibration behavior changes as part of the reconciliation.

## Rejected suspicions and confirmed mitigations

1. **Rejected: new live orders can be created with only one env flag.** New proposal requires the global live gate and sport gate; approved execution rechecks the global gate, auth, approval/expiry, and circuit breakers (`kalshi_refresh_job.py:182-203`, `:343-362`; `kalshi_execute_approved_job.py:80-139`).
2. **Rejected: balance API failure fails open.** `check_circuit_breakers` returns false when balance is unavailable (`risk_service.py:107-113`; `test_kalshi_risk_service.py:125-129`).
3. **Rejected: disabling new trading prevents risk-reducing cancellation.** Approved cancellations intentionally run regardless of the new-trade gate (`kalshi_execute_cancellations_job.py:42-47`).
4. **Rejected: cancellation queue has no duplicate protection.** Enqueue uses `ON CONFLICT (live_order_id) WHERE status IN ('pending_review','approved') DO NOTHING` (`cancellation_service.py:67-113`). TMS-01/TMS-03 concern exchange-observation/submission identity, not this review-row dedup.
5. **Rejected: fuzzy non-sports matches always become executable arb candidates.** Fuzzy matches are stored for review while structured/verified matches proceed (`arb_scanner.py:140-155`, `:422-455`). The unresolved issue is that player/game near matching lacks an equivalent complementarity gate (TMS-06).
6. **Rejected: current audited code contains a live two-leg arb executor.** None surfaced in the allowed scope. Current arb risk is scanner/alert/manual interpretation and paper-evidence quality; live two-leg execution was not assessed or recommended.
7. **Mitigation, not full resolution: cancel-then-replace failure is fail-closed for new exposure.** If replacement creation fails, the old DB row is marked cancelled (`repricing_service.py:126-135`, `:221-227`; test at `test_kalshi_repricing_service.py:209-220`). However, response-loss idempotency remains covered by TMS-01, and the causal replacement failure is only logged rather than durably linked.

## Coverage gaps

- No API or DB calls were made, so current exchange account state, actual gate values, queue/order divergence, pagination behavior, and production scheduler overlap were not verified.
- No integration tests were executed by instruction. All test statements above are source-coverage observations, not pass claims.
- No targeted `ArbScanner`, `MarketMatcher`, or `ArbPaperTrader` tests surfaced under `tests/`; broad helper tests outside the filename/symbol searches may exist but were not found.
- No fault-injection test surfaced for accepted-order/response-timeout, duplicate executor, API-list failure, cursor truncation, or orphan exchange order.
- No partial-fill lifecycle test surfaced with `0 < fill_count < contracts` across reconciliation, repricing/cancellation, and settlement.
- No test surfaced proving the daily exposure cap under stale approvals or concurrent execution.
- No test surfaced requiring same-cycle/bounded-age market and edge provenance before paper/live selection.
- Settlement tests characterize internal-stat outcomes but do not compare authoritative exchange finality, voids, corrections, or postponements.
- The audit did not inspect dashboard mutation routes except through the service contracts they feed; authorization/RLS of approval endpoints is outside this file-scope audit.
- The audit did not inspect database DDL/migrations, so relied-on unique/partial constraints were inferred from SQL conflict clauses and code comments rather than verified against live schema.

## Safe evidence sequence

This is an evidence order, not an implementation or enablement recommendation:

1. TMS-01/TMS-03 no-network fault-injection characterization for submission identity and unknown exchange observations.
2. TMS-02 quantity-conservation fixtures across partial fill, cancel/reprice, and settlement.
3. TMS-04 concurrent exposure-reservation repository test.
4. TMS-05 stale refresh-cycle provenance fixtures.
5. TMS-06 contract-equivalence and false-match fixtures before any Lane 08 extraction.
6. TMS-07 indicative-versus-executable quote/depth fixtures, retaining all arb/live gates off.
7. TMS-08 exchange-finality disagreement fixtures.
8. TMS-09 paper/live golden decision fixtures before Lane 06 extraction.
9. TMS-10 canonical comparator reconciliation with Chase validation and no model/calibration change.

## Audit completion criteria

This audit is complete when the report itself:

- contains exact path/symbol/line evidence for each finding;
- states failure mode, confidence, current mitigation, plan interaction, safe first evidence step, and done condition;
- reconciles Lanes 06/08 and audits 00/01/04 without editing them;
- records relevant canonical lessons/invariants, rejected suspicions, and coverage gaps;
- changes no code, config, register, plan, deployment, model, or trading state;
- passes report-only content validation and `git diff --check` scoped to this file.
