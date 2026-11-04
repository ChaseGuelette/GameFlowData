
# Layer 5 — Prediction and betting decisions

- Revision: 2026-11-03 / v1. Status: prediction/decision specification; not live wagering software.
- Goal: turn verified pregame inputs and a pinned artifact into reproducible probabilities and auditable flat-stake decisions.
- Architecture: network-free prediction and pure outcome math; repositories supply time-eligible quotes; replay and paper use the identical engine.

[Previous: training](04-training-and-artifact-management.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: evaluation](06-evaluation-and-forward-tracking.md)

## 1. Proposed files

| File | Owner | Consumers |
|---|---|---|
| `src/ncaab/prediction/service.py` | `PredictionService`, `PredictionRequest`, `PredictionRunResult` | Replay and daily jobs |
| `src/ncaab/prediction/sampling.py` | `QuantileSampler`, `SamplingSpec`, `MarginalSamples` | Probability calculation and diagnostics |
| `src/ncaab/prediction/outcomes.py` | `OutcomeEvaluator`, `OutcomeProbabilities`, `classify_outcomes` | Samples here and actual settlements in layer 6 |
| `src/ncaab/prediction/quote_selection.py` | `QuoteSelector`, `ReferenceMarketPolicy`, `ExecutionQuotePolicy` | Aware features, betting decisions and closing evaluation |
| `src/ncaab/prediction/decisions.py` | `DecisionEngine`, `DecisionPolicy`, `BettingDecision` | Both historical replay and forward paper |

Use layer-2 prediction/quote/model repositories and the layer-3 feature-provider contract. For file-only research, inject `FrozenFeatureProvider`, `FileResearchStore` and `FileRunSink`; their DTOs and eligibility rules are identical, not a second prediction algorithm. Paper uses registered SQL-backed repositories. No scraper calls, SQL string construction, artifact training or provider secrets belong inside the model/payout functions.

## 2. APIs and execution flow

```python
class PredictionService:
    def predict(self, request: PredictionRequest) -> PredictionBundle: ...

class QuoteSelector:
    def reference(self, game: GameSchedule, context: AsOfContext,
                  policy: ReferenceMarketPolicy) -> ReferenceMarkets: ...
    def candidates(self, game: GameSchedule, context: AsOfContext,
                   policy: ExecutionQuotePolicy) -> list[QuoteSelection]: ...

class QuantileSampler:
    def sample(self, prediction: QuantilePrediction, spec: SamplingSpec,
               *, input_identity: str) -> MarginalSamples: ...

class OutcomeEvaluator:
    def probabilities(self, samples: MarginalSamples,
                      contract: BettingContract) -> OutcomeProbabilities: ...

class DecisionEngine:
    def decide(self, prediction: PredictionBundle,
               candidates: Sequence[QuoteSelection], policy: DecisionPolicy) -> list[BettingDecision]: ...
```

Run order:

1. Resolve/pin the approved artifact or explicit research artifact and full config hash.
2. Validate capability, feature schema, chronology and readiness for each game.
3. Build features with an explicit context; reference markets are added only for aware schemas.
4. Predict quantiles and validate physical/monotonic/finite output.
5. Generate deterministic marginal samples once per game/target/input identity.
6. Retrieve eligible execution candidates, compute win/push/loss and EV, then apply the frozen selection policy.
7. Persist predictions, candidates, exclusions and selected decisions. Do not save only profitable-looking rows.

A source/feature/schema/artifact failure is `blocked`; a valid prediction with insufficient EV is `skip`; an eligible selected contract is `bet`. A missing quote does not retroactively mean the independent model failed to predict.

## 3. Decision-time policy

Initial research-control policy is `tminus60_v1`: intended decision time is the applicable start minus 60 minutes. Historical reconstructed schedules must be labeled as such; do not claim the final corrected start was known historically if no schedule history exists.

Prospective inference records both intended slot and actual cutoff. A dispatcher may run within 90 seconds of the intended slot; later runs are `missed_decision_window`, not retrospectively placed bets. All source/quote selection uses the actual recorded cutoff, and paper comparisons retain observed latency. Historical replay uses declared nominal cutoffs unless replaying actual recorded contexts.

A snapshot must precede the cutoff and source commencement, belong to the correct game, satisfy the configured maximum snapshot age (initially 10 minutes), and have no unexplained future update clock. Historical provider snapshots may be downloaded later; a later live observation cannot be backdated. Capture freshness does not guarantee an actual sportsbook fill; report the execution model as observed-snapshot pricing, not executed trading.

## 4. Reference markets versus execution markets

### Reference policy

Use an explicit ordered `reference_books` list. For each required spread/total, choose the highest-priority book with a valid latest coherent paired group at the cutoff. Do not choose the reference book because it makes a model look better. Missing required reference markets blocks the aware feature row.

The reference policy is shared with layer 3. Its chosen snapshot IDs are persisted in feature/prediction lineage. A moneyline reference is not part of the initial feature schema.

### Execution policy

Use a separate explicit `execution_books` allowlist. Evaluate the latest eligible group for each book/market, retaining both outcomes and their own price/line. Model EV may be compared across those contemporaneous contracts, but never across later snapshots.

A spread group has mapped home/away outcomes with opposite signed handicaps; a total group has over/under at one total. Missing counterparts, inconsistent points, duplicates, unsupported quarter lines and stale clocks make the group ineligible.

If provider home/away is reversed, swap the outcome's canonical side while keeping its handicap attached to the same team. Do **not** blindly negate a point just because its side label changed. A canonical home-spread scalar comes from the mapped home outcome; negation is only a verified conversion from the opposite member of a coherent pair.

Both book lists are owner-required configuration. Empty lists fail market-aware training/replay or decision execution; they do not fall back to every bookmaker. Neither list is copied from the NBA harness's historical bookmaker defaults.

## 5. Distribution-to-sample contract

Initial codec: `bounded_quantile_linear_v1`, with the layer-4 quantile grid and 4,096 samples per target. It is an approximate marginal representation, not a claim that tails or integer push mass are perfectly calibrated.

1. Validate/repair quantile ordering using the artifact's declared `cumulative_max_v1` policy; record any raw crossing and repair magnitude.
2. Derive a stable seed from SHA-256 of artifact digest, game ID, actual cutoff, ordered feature fingerprint, target and sampler version. Never seed from row order, Python's randomized `hash()`, book order or random run UUID.
3. Draw one uniform position within each of 4,096 equal-probability strata. Use piecewise-linear inverse interpolation between quantile knots.
4. Clamp the unspecified outer probability intervals to the endpoint quantiles; record the endpoint convention and mass. Do not silently extrapolate a Gaussian tail.
5. Convert to integer scoring outcomes with `numpy.rint` (nearest integer, ties-to-even), then integer dtype. Require finite quantiles, nonnegative total quantiles, a positive total median, and Q95 > Q05 for each actionable target. Violations block rather than trigger hidden calibration.
6. Treat margin/total arrays as separate marginals. `supports_joint=False`; elementwise combination, same-game parlay pricing and implied score correlations are unsupported.

Initial betting eligibility additionally requires the contract threshold to lie within the relevant predicted Q05-Q95 region. This avoids claiming precise tail edges from endpoint clamping. Evaluate quantile/line calibration and integer-versus-half-line behavior explicitly; more samples do not repair a misspecified distribution.

The same identity/config yields exactly the same samples/probabilities regardless of batching or candidate order. Changes in sampler/grid/repair policy create a new strategy/artifact identity and require replay comparison.

## 6. Outcome and payout math

Let `M` be home-minus-away final margin and `T` final total. Canonical signed handicap belongs to the selected team.

| Contract | Win condition | Push condition |
|---|---|---|
| Home spread with handicap `s` | `M + s > 0` | `M + s == 0` |
| Away spread with handicap `s` | `-M + s > 0` | `-M + s == 0` |
| Over at `L` | `T > L` | `T == L` |
| Under at `L` | `T < L` | `T == L` |

Loss is the remaining valid outcome. Compare integer samples and Decimal-compatible points without an arbitrary float equality tolerance. Probability is the empirical fraction in each class; the three probabilities must sum to one.

At decimal odds `d`, expected net profit per unit is `p_win * (d - 1) - p_loss`. Conditional win probability is `p_win / (p_win + p_loss)` when the denominator is nonzero; use it, not unconditional win probability, when comparing to paired no-push implied probabilities.

Illustrative arithmetic, not measured model performance: `p_win=.52`, `p_push=.03`, `p_loss=.45`, `d=1.91` gives EV `0.0232` units per unit staked. A push returns the stake, not a profit or loss.

Moneyline betting remains disabled in v1. Final college ties are not an ordinary moneyline push, but rounded marginal samples may generate zero margins. Enabling moneylines requires a separately validated nonzero-margin/OT-support probability policy; do not silently count those samples as refundable bets.

Actual settlement in layer 6 calls the same outcome classifier with observed outcomes, then applies the recorded ruleset. No duplicate hand-written sign logic in the backtester.

## 7. Initial selection policy

The following are explicit research controls, not optimal or live-approved settings:

- Spreads/totals only; no props, parlays, arbitrage or automated wagering.
- `min_ev_per_unit=0.02`, raw model probabilities, flat one-unit stakes.
- At most one selected side/contract per market per game/variant/decision slot.
- Choose highest eligible EV; tie-break by configured book priority, freshest eligible snapshot, then stable quote ID.
- At most two selected bets per game/variant with the two initial markets. Correlation is handled in reporting, not by inventing a joint distribution.
- No BL blending, Kelly, edge-proportional staking, or post-hoc book/side filtering in the initial comparison.

Save every candidate's probability, price, line, EV, source IDs and exclusion reason. If two opposite sides appear positive across books, the one-per-market rule still applies; this is not an arbitrage execution system. Candidate volume changing after a feature/model update is a finding to analyze, not automatically improved quality.

## 8. Persistence and idempotency

`PredictionRepository` maintains `ncaab_prediction_runs`, `ncaab_predictions`, `ncaab_decisions`. Every decision references the exact artifact/feature identity and quote row; reference and execution snapshots are separate references.

Retries in one run upsert the same logical game/decision/artifact row. New diagnostic runs can persist identical values under distinct run IDs. Paper duplication is prevented by layer 6's experiment/strategy/game/market/intended-slot key, not by random prediction IDs.

Blocked records include reasons such as `missing_identity`, `unverified_cohort`, `incomplete_history`, `unsupported_clock`, `missing_reference_market`, `stale_quote`, `unpaired_market`, `unsupported_line`, `artifact_mismatch`, `invalid_distribution`, `outside_supported_quantiles`, `missed_decision_window`. A no-bet result carries counts distinguishing missing inputs from below-threshold opportunities.

## 9. CLI, implementation and acceptance

Planned command only:

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli predict --config configs/ncaab/paper.yaml --game-id 123 --decision-at 2026-11-03T22:00:00Z --dry-run
```

Game ID/time are illustrative caller-supplied values, not an identified production prediction. Planning validates configuration/identity without writing predictions; execution requires explicit authority. No model training or live fetching happens inside this command's core prediction service.

Implement quote/time selectors and outcome math first; then sampler; artifact/feature integration; candidate policy; persistence/idempotency; and shared replay/paper calls.

Proposed tests: `tests/ncaab/test_quote_selection.py`, `test_outcome_math.py`, `test_sampling.py`, `test_prediction_service.py`, `test_decision_policy.py`.

Acceptance includes home/away reversal, integer pushes, half-lines, stale/future/mixed snapshots, exact bookmaker priority, no lookahead, deterministic reordering/chunking, blind prediction invariance to price mutation, tail guard, no Gaussian probability fallback, unsupported moneyline behavior, one-per-market selection, and truthful zero-bet/dropout reporting.
