# NCAAB model architecture decision

- Updated: 2026-11-03
- Status: initial research design; effectiveness unvalidated; no training or implementation launch authorized.
- Scope: men's college basketball game-level predictions for private research and betting.

[Handoff summary](README.md) | [Data sources](data-sources.md) | [System architecture](architecture-overview.md)

## Decision summary

Start with a direct distributional boosted-tree baseline for game margin and total. Fit market-independent and market-aware variants using the same non-market feature foundation, prediction targets, model family, and evaluation games. Train the variants separately; do not assume their fitted parameters are identical.

Use the first complete ingestion-to-replay path to test incremental hypotheses. The dynamic team-state model and the roster-aware neural model are substantive challengers, not prerequisites for obtaining a baseline. A baseline need not already be profitable before neural research is justified.

KenPom is excluded from the initial setup. No model in this document requires its ratings or predictions.

## Separate the design choices

| Choice | Question |
|---|---|
| Team representation | Use explicit team statistics, estimated latent team states, learned roster representations, or a tested combination? |
| Matchup predictor | How do the two teams and venue/context become an outcome distribution? |
| Market inclusion | Does the predictor use only basketball information, or also quotes available at the decision time? |
| Output design | Direct margin/total distributions, or a richer joint scoring/simulation model? |
| Training history | Which past examples and recency weights improve later decisions? |

These are different experimental axes. Market-aware versus market-independent is not itself a choice between Bayesian models, trees, and neural networks.

## Proposal inventory

### 1. Dynamic hierarchical Bayesian/state-space team strength

Estimate evolving offensive strength, defensive strength, pace, and uncertainty from prior games. State-space modeling addresses evolution over time; hierarchical modeling addresses sharing information across teams or groups. They can be combined in one model.

Proposed role in the hybrid: provide team-state estimates and uncertainty summaries to the downstream matchup predictor. The same **market-independent** state estimator should feed both market variants. A shared state estimator that consumes odds would contaminate the market-independent branch.

Across seasons, examine partial resets and priors informed by verified roster/role/coaching continuity rather than assuming a school is the same team indefinitely. Separate uncertainty about team strength from game-to-game outcome variability. Passing uncertainty summaries into trees does not automatically produce a fully Bayesian posterior predictive distribution.

**Alternative retained:** an integrated Bayesian outcome model could generate its own predictive distributions without a tree downstream. It is a separate challenger, not another mandatory layer. Layer 4 scopes an inexpensive empirical-Bayes/MAP state estimator with forward filtering first; that approximation must not be described as a full joint Bayesian posterior.

### 2. Direct distributional boosted-tree predictor

Use pregame team/state/style/context inputs to predict distributions of home-oriented scoring margin and game total, rather than only a point forecast. The v1 design now specifies direct XGBoost quantile regressors, a fixed nine-quantile grid, no conformal offsets, and the versioned marginal sampler in [layer 4](layers/04-training-and-artifact-management.md) and [layer 5](layers/05-prediction-and-betting-decisions.md). These are starting research controls, not empirically selected winners.

Initially, separate margin and total prediction heads are acceptable research baselines. They do not by themselves establish a coherent joint distribution of both scores. Do not use them as if a joint simulation or correlated-market model has already been validated.

### 3. Matchup-interaction hypotheses

Test whether opponent pairings add information beyond overall team quality. Candidate families include ball security versus turnover pressure, offensive versus defensive rebounding, offensive shot profile versus defensive tendencies, foul generation versus foul tendency, and conflicting tempo preferences.

These are basketball hypotheses, not a separate required architecture. Trees can test them through paired attributes and explicit interaction features; a neural matchup network can test learned representations. Run family-level inclusion/exclusion comparisons instead of declaring success from selector importance alone.

### 4. Market-independent versus market-aware variants

- **Independent:** no odds or line-derived information in its predictor or upstream shared features. Its probabilities can still be compared with prices when evaluating a bet.
- **Aware:** the same basketball foundation plus eligible spread/total/price information from a declared pregame decision time.
- **Residual alternative:** predict a correction to the market's expectation instead of directly appending market inputs. Keep this as a separate later experiment; do not silently change the baseline target and feature set together.

Disagreement is diagnostic. It is not automatically a bet, an edge, or a reason to average the models. The market itself is a reference baseline, and improvements must survive actual offered prices and chronological testing.

### 5. Neural predictor on the same team features

A compact neural matchup/distribution predictor can replace the boosted-tree head while consuming the same inputs. This is the simplest comparison for whether the predictor family helps independently of changing the team representation.

### 6. Roster encoder plus neural matchup model

Use a shared encoder for both teams. Its inputs describe the players and their expected roles using only information available before the target game. A set encoder or attention-based set model is a candidate; arbitrary player ordering should not define the team representation.

```text
Team A player/role inputs -> shared roster encoder -> A representation
Team B player/role inputs -> shared roster encoder -> B representation

A + B + game context + optional shared Bayesian states
    -> neural matchup predictor
    -> margin/total distribution outputs
```

The encoder and matchup predictor can be trained together. This branch competes with the tree predictor; it is not another prediction model stacked after the trees. Market inputs belong only in its market-aware variant, not in any encoder shared with a market-independent variant.

A smaller neural head without the roster encoder isolates the benefit of the encoder. IDs are needed for linkage, but the model should not rely solely on memorizing athlete or school identities.

### 7. Opponent-aware temporal encoder

A later representation experiment could encode recent games, their order, opponent strength, and changing roles instead of relying only on hand-built rolling summaries. This was discussed as a candidate, not selected as an initial requirement.

### 8. Joint scoring / possession-efficiency simulation

A richer alternative would model shared possessions/pace and each team's scoring efficiency, then generate joint game outcomes. This could express score dependence and game-shape variability and derive multiple market probabilities from the same simulated games.

Compare it against direct margin/total prediction rather than assuming a more realistic decomposition is more profitable. Detailed possession, endgame, and lineup simulation is deferred.

## Initial setup and experiment order

| Stage | Setup | What it establishes |
|---|---|---|
| Initial baseline | Simple pregame team summaries and game context; direct distributional trees; independent and aware versions | A working shared feature/inference/replay path and the effect of market inclusion |
| Dynamic-state challenger | Add Bayesian team-state estimates while keeping the downstream setup comparable | Whether better estimation of current team condition adds value |
| Matchup-family tests | Add predeclared style/interaction families under fixed model settings | Which relationships improve predictions and betting decisions |
| Neural challenger | Same-feature neural head, then roster encoder plus matchup head | Predictor contribution versus learned roster representation contribution |
| Later alternatives | Integrated Bayesian outcomes, temporal encoders, residual-market models, joint game simulation | Specific remaining failure modes, not an unrestricted architecture sweep |

Do not require the entire Bayesian branch to be completed before a bounded neural comparison can begin. Avoid a full combinatorial experiment across every representation, predictor, output family, market mode, and history length.

## Data history and temporal evaluation

The collection target is 2021-22 through 2025-26 plus current 2026-27 updates, subject to source coverage. Acquisition depth is not the training window. Current team state should react to recent evidence and roster change; older games may still provide examples of matchup/outcome relationships.

Compare two- and three-season training windows on the **same later games**, with fixed features and model settings. Treat recency weighting as another controlled experiment. Reserve an independent later test after choosing a candidate; no random game splits that allow future information into earlier forecasts. Add earlier warm-up history where feature initialization requires it.

Generate historical Bayesian states and other learned intermediate features using prior-only fitting/filtering. Do not use full-season smoothing, future outcomes, final-season averages, actual target-game minutes, or later roster/quote updates. Offline and daily inputs must follow the same definitions.

## Evaluation and decision policy

- Assess distribution quality as well as mean/median error; a more accurate point forecast is not automatically a better bet.
- Use historically available, executable quote snapshots and actual offered payouts. Keep market-independent and aware results comparable.
- Start model/feature discovery with raw predictions and flat stakes. Market blending, ranking, and sizing are separate experiments, not a way to disguise a weak predictor.
- Report profit/ROI, drawdown, volume, uncertainty, timing/dropout, and supporting closing-line evidence. A small or highly selected winning slice is not promotion evidence.
- Keep historical selection separate from forward-paper tracking and any live-money decision. No automatic wagering is authorized.

## Relevant prior lessons / invariants

- **Cheap baseline before architecture:** validate the intended signal with the simplest controlled comparison before paying for complexity.
- **Feature selection is not an ablation:** compare feature families directly, especially correlated roster/state summaries.
- **Temporal integrity is end-to-end:** timestamps alone are insufficient if consumers ignore cutoffs.
- **Empirical probabilities:** when using samples, count outcomes directly; do not replace the sample distribution with a Gaussian-CDF approximation. Model explicit push probability for integer lines and preserve quantile monotonicity where applicable.
- **Existing NBA behavior is not a college assumption:** do not presume the NBA Q10 edge transfers to NCAAB, and do not deploy global conformal offsets.

These lessons were retrieved earlier in the planning discussion from GameFlow's critical-invariants and atomic modeling-lesson pages. They constrain the experiments; they do not establish that any NCAAB architecture is already validated.

## Design defaults and remaining gates

The [shared architecture contract](architecture-overview.md) now specifies D1-versus-D1 eligibility, T-60 decisions/T-5 closing reference, the 31/35-field base/aware schemas, fixed quantile/model controls, deterministic sampling, chronological discovery/independent folds, flat-unit decision policy and immutable artifact format. Layer specifications define implementation ownership and tests.

Execution still requires verified classification/source coverage, explicit reference/execution books and book rules, resource/API budgets, schema preflight, durable storage and deployment-host acceptance. These are required operator/evidence inputs, not permission to guess. Moneyline wagers remain disabled until generated zero-margin samples and actual no-tie final outcomes have a validated policy. No layer is implemented or launched by this document.
