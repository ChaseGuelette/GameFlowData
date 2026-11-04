
# Layer 6 — Evaluation and forward tracking

- Revision: 2026-11-03 / v1. Status: evaluation design; no backtest or performance result is claimed.
- Goal: determine what the models predict, what decisions they would make at eligible prices, and whether those results survive independent and forward observation.
- Architecture: chronological replay and forward-paper tracking call layer 5; settlement uses shared outcome math; reports pin every artifact, quote, source and result revision.

[Previous: prediction](05-prediction-and-betting-decisions.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: operations](07-daily-orchestration-and-data-quality.md)

## 1. Existing harness boundary

The inspected `src/backtesting/backtest_harness.py:BacktestHarness` is a retained NBA player-prop orchestrator. It owns player-stat defaults, a minutes/rate predictor, player-keyed outputs and BL/sizing behavior. Reuse its phased lifecycle and receipt discipline as design patterns, not the class as a sport-generic base. Do not inherit its default bookmakers, edge/Kelly settings or source-table queries.

No Kalshi, Polymarket, arbitrage, player-prop betting or dashboard implementation belongs to this layer. Evaluation is not training; a replay command must not silently refit an artifact or fetch a missing historical quote from a current endpoint.

## 2. Proposed files and APIs

| File | Owner | Main responsibility |
|---|---|---|
| `src/ncaab/evaluation/replay.py` | `ReplayRequest`, `ReplayEngine`, `ReplayResult` | Freeze inputs, walk decisions chronologically, call shared feature/prediction/decision services |
| `src/ncaab/evaluation/settlement.py` | `SettlementRuleset`, `SettlementService` | Result-version selection and contractual settlement using layer-5 outcome math |
| `src/ncaab/evaluation/metrics.py` | `PredictionMetrics`, `BettingMetrics`, `BlockBootstrap` | Proper scores, unit P&L, uncertainty and grouped comparisons |
| `src/ncaab/evaluation/clv.py` | `ClosingLineAnalyzer` | Same-book/same-contract closing comparisons and separately labeled line movement |
| `src/ncaab/evaluation/paper.py` | `PaperTracker` | Idempotent forward observation and later reconciliation; never places real wagers |
| `src/ncaab/evaluation/reports.py` | `EvaluationReportWriter`, `EvaluationManifest` | Immutable outputs and verdict-oriented reports |

```python
class ReplayEngine:
    def plan(self, request: ReplayRequest) -> ReplayPlan: ...
    def run(self, plan: ReplayPlan, *, execute: bool) -> ReplayResult: ...

class SettlementService:
    def settle(self, contract: BettingContract, result: GameResult,
               rules: SettlementRuleset) -> SettlementRecord: ...

class ClosingLineAnalyzer:
    def analyze(self, decisions: Sequence[BettingDecision],
                policy: ClosingPolicy) -> ClvReport: ...

class PaperTracker:
    def record(self, decisions: Sequence[BettingDecision], experiment: PaperExperiment) -> PaperReceipt: ...
    def reconcile(self, *, label_as_of: datetime, limit: int) -> SettlementBatch: ...
```

`ReplayRequest` includes frozen artifact identities, variant/strategy/config hashes, game/date cohort, intended decision policy, historical availability mode, quote/book policy, label cutoff, unit stakes and resource limits. All cutoffs are explicit. A result records both predictive coverage and decision coverage.

## 3. Replay algorithm

1. Resolve frozen dataset, artifact and policy manifests; reject incomplete/tampered bundles or fit/selection information reaching into the evaluated period.
2. Enumerate the independently defined eligible schedule cohort, not just games with successful predictions or convenient quotes.
3. Derive per-game decision contexts under the declared schedule/time mode. A reconstructed final schedule does not become proof of historical schedule knowledge.
4. Read bounded as-of facts/quotes and use layer 3. If a dynamic-state provider is enabled, parameters stay frozen while states update only from observations available before each subsequent decision.
5. Call `PredictionService` and `DecisionEngine` with the same inputs/types as daily paper operation. No alternate backtest-only probability, price-selection or missing-value implementation.
6. Save valid predictions and all block/dropout reasons, then candidate decisions and selected contracts.
7. Query outcomes separately at the explicit `label_as_of` cutoff and settle with the recorded ruleset.
8. Compute metrics, closing evidence and paired model comparisons; finalize reports only after all counts and references reconcile.

Batching/parallel prediction may accelerate pure inference, but ordering must not change seeds or feature histories. Persisted decisions and state updates have a deterministic chronological ordering with stable game-ID tie-breaks. Do not fit a model inside the replay loop.

## 4. Temporal grade and denominator rules

Report source availability as `strict_point_in_time` or `reconstructed_research` per prediction and for the run. Historical odds can have genuine provider snapshots while statistical facts are reconstructed; that does not make the whole evaluation strict.

Reconstructed history may support discovery and independent event-time testing, but reports must acknowledge later source corrections and unavailable original publication clocks. It does not prohibit a separately approved prospective paper trial using newly observed inputs. Never disguise reconstruction as an operational backtest with proven contemporaneous inputs.

Maintain these separate counts:

- Scheduled games in the requested season/date universe.
- Games passing sport/division/exhibition/result eligibility.
- Games with sufficient source/identity/feature evidence.
- Predictions by variant, including independent predictions without quotes.
- Quote-eligible game-market decisions and paired comparison cohort.
- Candidate contracts, selected bets, settled/pending/void/manual cases.
- Closing-evidence matches and reasons for missing CLV.

Compare independent/aware candidates on the same eligible game/decision set; also report each model's broader standalone coverage. Never describe a higher ROI caused by silently dropping difficult games as a predictive improvement.

## 5. Settlement contract

V1 supports approved full-game spreads/totals using final scores including overtime. Each allowed bookmaker must be mapped to a reviewed `SettlementRuleset`; no assumption that every book handles postponement/abandonment identically.

For a valid completed game, layer-5 `OutcomeEvaluator` supplies win/loss/push. With one-unit stake and decimal odds `d`, returned units are `d` for win, 0 for loss and 1 for push; profit is returned minus stake. Void returns stake with zero profit but is a separately classified case.

Cancelled, abandoned, postponed-after-decision, disputed or contradictory results require an explicit rule and source evidence. When the rule/evidence is missing, use `manual_review` or `pending`, not an automatic win/loss/void. Missing outcomes cannot be silently dropped from the denominator. A corrected score appends a settlement revision referencing the new result version; an immutable prior report is not overwritten.

`ncaab_settlements` references either a replay decision or a paper bet, not both. Select one eligible settlement version per target when calculating metrics; never sum superseded records. A new report after a correction has a new run ID and parent/report lineage.

## 6. Predictive and distributional metrics

Evaluate game predictions independently from the chosen bet threshold:

- Margin and total Q50 MAE, RMSE and signed bias.
- Pinball loss per quantile and target; declared aggregate across the fixed grid.
- Empirical quantile coverage, interval width and crossing/repair diagnostics.
- Sample-based CRPS where enabled, using an efficient sorted-sample implementation rather than an all-pairs matrix.
- Win/push/loss probability diagnostics at a fixed reference contract per game/market; integer and half-point lines are separate cohorts.
- Proper-score/coverage slices by season, neutral site, cold-start/history count, market mode and supported distribution region.

A three-outcome Brier/log-loss diagnostic must preserve push mass. If numerical clipping is needed to report log loss, record the metric-only epsilon and count actual zero-probability observed outcomes before clipping. Do not change betting probabilities just to make a diagnostic finite.

Do not treat every bookmaker's duplicate contract as an independent prediction sample. Reference-contract metrics use a predetermined route; game-level distribution metrics are one observation per game/target.

## 7. Betting metrics and uncertainty

Initial sizing is flat one-unit per selected contract. No compounding, Kelly or post-hoc stake optimization.

Report bets, unique games/dates, win/loss/push/void/pending/manual counts, placed stake, settled stake, profit, ROI, cumulative unit equity, maximum drawdown in units and drawdown duration. ROI uses profit divided by settled stake; pushes remain in settled stake, voids/pending/manual cases are separately excluded with their counts and placed capital shown. If denominator is zero, ROI is undefined, not zero or success.

A daily profitability/Sharpe-style diagnostic must state its denominator and be unannualized unless an explicit documented convention is selected. Do not mix dollar/percentage/unit drawdowns. Confidence intervals use game/date-block resampling retaining correlated markets/variants within a block, not independent resampling of individual bet rows. Initial bootstrap design is seven-calendar-day moving blocks and 2,000 replicates with a fixed seed; assess sensitivity rather than presenting that choice as a theorem.

Report full fixed-config outcomes before any parameter sensitivity. A discovered best cell is selection-inflated. Candidate choices belong to discovery, followed by a frozen independent test; the same holdout cannot be repeatedly used to invent a new family and still be called untouched.

### Evidence classification, not automatic deployment

Classify results as `invalid_methodology`, `insufficient_volume`, `unprofitable_fixed_policy`, `inconclusive`, or `research_candidate`. At least 100 selected settled bets is only an exploratory floor, not proof of profitability. A candidate claim also needs positive fixed-policy independent-window profit/ROI, adequate reported coverage, no unresolved temporal/quote/settlement defect, and a drawdown result against a risk cap declared before inspecting the holdout. If the profit interval includes zero, retain the uncertainty rather than calling the strategy certified profitable.

The acceptable drawdown cap and any higher volume/confidence target are explicit experiment inputs; absent thresholds disable automated candidate classification, not raw metric reporting. Paper experimentation can still be separately owner-approved to learn prospectively. No statistic automatically enables real wagering.

## 8. Closing-line evidence

Initial closing policy is `tminus5_v1`: latest eligible snapshot at/before the applicable start minus five minutes, with the same declared freshness guard. It is a defined pregame reference, not a claim to capture a bookmaker's exact final tick.

Require bet-quote time <= decision time < closing observation <= allowed pregame cutoff. Same-book, same-market, same-side, same-line pairs are primary. Recompute paired no-vig closing probability only from a coherent pair. Conditional closing-probability advantage is `p_close_novig - 1 / bet_decimal_odds`, explicitly conditional on non-push outcomes at that line.

If the main line moved and no same-contract price exists, report line movement separately: for a spread, selected-team handicap at bet minus selected-team handicap at close; for over, closing total minus bet total; for under, bet total minus closing total. Do not relabel price changes at a different line as probability CLV on the original contract.

Consensus close is an optional separately labeled fallback, never silently substituted into the primary metric. Missing same-book/line/time evidence has explicit dropout reasons. Positive mean CLV supports interpretation; it does not alone establish realized profitability or justify edge-proportional sizing. Edge-ranking diagnostics are optional for future ranking/sizing hypotheses, not an automatic blocker for all flat-stake research.

## 9. Paired comparisons and failure diagnosis

For each candidate versus the frozen baseline, report:

- Shared predictions and quote-eligible opportunities.
- Shared same-side bets, compressed-but-still-bet cases, dropped-below-threshold cases, flipped sides, and candidate-only bets.
- Candidate-only and baseline-only profit/volume under the same flat stakes and date blocks.
- Dropout differences by source, schema, cohort, reference market and timing.
- Book/side/line-band concentration and sensitivity, without promoting a small post-hoc winning slice.

If results look implausibly profitable or smooth, audit temporal selection, duplicate decisions, settlement revisions, missing losing labels and quote pairing before claiming model improvement. Zero bets with zero eligible quotes is a data-path failure, not evidence that the model found no edge.

## 10. Forward-paper lifecycle

Paper uses actual observed inputs/quotes and truthful decision timestamps. It never backfills a missed decision after the game. `PaperTracker` records the selected immutable contract/price and later settles it; it does not submit sportsbook orders.

Idempotency is experiment + stable strategy + game + market + intended slot. Strategy identity includes the logical variant and decision/feature policy, but not a random run ID or newly refitted weight digest. The exact artifact is recorded separately. A refreshed artifact in the same strategy cannot create a second paper bet for an already-recorded slot; materially changed policies start a new explicit experiment.

Report latency, source/feature freshness, skipped decisions, quote age, selected price/line, realized result and later closing evidence. Keep paper evidence separate from a reconstructed historical report. Paper approval is an owner gate and not a claim that a strategy has been certified profitable.

## 11. Output bundle and tables

Under `data_root/ncaab/evaluations/<run_id>/`, finalize:

```text
manifest.json
resolved_config.json
predictions.parquet
decisions.parquet
quote_ledger.parquet
bets.parquet
settlements.parquet
clv.parquet
metrics.json
coverage.json
comparison.json
report.md
```

Every file includes or joins to run/game/decision/artifact/policy IDs; the manifest inventories hashes, source/result revisions, time windows, mode/grade and execution assumptions. Empty files retain their schemas and counts. Write manifest last; a failed or incomplete run is not a finalized evaluation.

Use `ncaab_evaluation_runs`, `ncaab_paper_bets`, `ncaab_settlements` and layer-5 prediction/decision tables. Long replay can use a verified local mirror or a complete frozen export. The file-only path injects `FrozenFeatureProvider`, `FileResearchStore` and `FileRunSink`; it does not require canonical registry writes, fabricate DB foreign keys, or fetch missing historical rows from current services. A missing exact exported context is a blocker. Source freshness/identity must be stated. Registering reports in the canonical DB is explicit, not an automatic side effect of reading local artifacts.

## 12. CLI, sequence and acceptance

Planned commands only:

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli replay --config configs/ncaab/research.yaml --fold discovery --dry-run
.\venv\Scripts\python.exe -m src.ncaab.cli paper-settle --config configs/ncaab/paper.yaml --dry-run
```

The resolved config must provide artifact/dataset refs, label cutoff and resource ceilings before execution. Long replays remain owner-launched; no one-day smoke is permission for a full history sweep.

Implement settlement/outcome fixtures and immutable reports, then replay orchestration with dependency fakes, deterministic paired comparisons, CLV clock/contract matching, and paper idempotency. Only then approve a bounded real-source replay.

Proposed tests: `tests/ncaab/test_replay.py`, `test_settlement.py`, `test_evaluation_metrics.py`, `test_clv.py`, `test_paper_tracking.py`, `test_evaluation_reports.py`, `test_replay_serving_parity.py`.

Acceptance: identical replay/paper outputs for the same controlled context; no future data/close leakage; every universe game accounted for; correct push/void/revision math; paired comparison consistency; deterministic bootstrap; no duplicate paper bets; zero-denominator metrics truthful; and no report marked decision-grade merely because its process exited successfully.
