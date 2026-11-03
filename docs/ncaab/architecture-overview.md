# NCAAB system architecture overview

- Updated: 2026-11-03
- Status: logical architecture only; no implementation, schema, deployment, or job launch authorized.

[Handoff summary](README.md) | [Model architecture decision](model-architecture-decision.md) | [Data sources](data-sources.md)

## Purpose and boundary

Build one reusable path from historical/live basketball facts and time-stamped odds to reproducible game-level predictions and evaluation. The first complete baseline should exercise this path before larger architecture experiments are added.

This is not a claim that new NCAAB classes, tables, scripts, or services already exist. Reuse retained project capabilities only after inspecting their actual interfaces. Do not restore the archived NCAAB lane by assuming historical paths are current.

## Seven-layer map

Each linked layer document exists but is **completely empty by design**. The descriptions here establish responsibilities only; detailed contracts, file scopes and implementation plans are deferred.

| Layer | Responsibility | Layer document |
|---|---|---|
| 1. Data ingestion | Historical statistical bootstrap, live game refresh, and separate historical/current odds acquisition | [Data ingestion](layers/01-data-ingestion.md) |
| 2. Database and identity linking | Persist source/canonical records, map provider identities, preserve temporal/provenance fields | [Database and identity linking](layers/02-database-and-identity-linking.md) |
| 3. Feature builders and feature store | Build pregame team/matchup/roster inputs using shared as-of semantics | [Feature builders and feature store](layers/03-feature-builders-and-feature-store.md) |
| 4. Training and artifact management | Fit candidate models and preserve reproducible model/feature/configuration artifacts | [Training and artifact management](layers/04-training-and-artifact-management.md) |
| 5. Prediction and betting decisions | Load artifacts, generate distributions/probabilities, compare eligible quotes and record decisions | [Prediction and betting decisions](layers/05-prediction-and-betting-decisions.md) |
| 6. Evaluation and forward tracking | Replay historical decisions, evaluate predictive/betting results, and track forward-paper behavior | [Evaluation and forward tracking](layers/06-evaluation-and-forward-tracking.md) |
| 7. Daily orchestration and data quality | Run dependencies in order, handle corrections/retries, enforce freshness and expose failures | [Daily orchestration and data quality](layers/07-daily-orchestration-and-data-quality.md) |

## Data and control flow

```text
SportsDataverse historical files + live basketball APIs     The Odds API
                         \                                  /
                          -> 1. Ingestion adapters --------
                                      |
                                      v
                        2. Database + identity linking
                                      |
                                      v
                       3. Shared as-of feature construction
                              /                    \
                             v                      v
                 4. Model training             5. Prediction
                    + saved artifacts ---------> + quote decisions
                                                     |
                                                     v
                                  6. Historical replay / forward tracking

7. Orchestration and data quality coordinate and check the full path.
```

Historical replay supplies a historical decision time and eligible source/quote inputs to the same feature and prediction path used for daily operation. It must not implement a second drifting probability or bet-selection engine. Layer 7 schedules work; it does not substitute for successful source, feature, and prediction output checks.

## Responsibility clarifications

### Ingestion is not feature engineering

Separate the basketball and odds adapters and their historical/live modes. Fetching a completed-game box score does not itself produce ready-to-use pregame rolling averages. Provider wrappers do not guarantee completeness, uptime, deployment-host access, or identical historical/live schemas.

### Database design is not one table per scraper

Organize canonical records around games, teams, players, team/player game facts, membership information, odds snapshots and prediction/evaluation records. Preserve source IDs and lineage. Exact tables, keys, migrations and storage decisions remain unspecified until layer 2 is written.

### Feature builders are not feature selectors

Layer 3 loads eligible information and constructs candidate inputs. Feature selection belongs to layer 4 as an optional training operation. Inference must use the saved feature/preprocessing contract, not rerun an unconstrained selector.

A fitted Bayesian team-state component spans these responsibilities: layer 4 fits/version-controls its parameters, layer 3 generates or retrieves prior-only state features, and layer 7 updates required states before inference. Do not bury a separately trained component inside an unversioned SQL aggregate.

### Training is not daily prediction

Layer 4 produces artifacts; layer 5 consumes them. Daily inference normally updates inputs/states and predicts without implying a full model retrain. Artifacts must identify features, preprocessing, temporal cutoffs, architecture, market mode and relevant data/configuration versions.

### Predictions are not automatically bets

The decision layer translates outcome distributions into probabilities at particular lines and compares them with actual offered payouts. Keep push outcomes, price validity and settlement rules explicit. The independent model may inspect prices for a decision without using them as model inputs.

### Evaluation is not production readiness

Predictive quality, historical flat-stake results, forward-paper behavior and live-money authorization are separate gates. Historical quote timing and data dropout must be visible, not hidden by successful process exit. Automated wagering is outside this initial architecture scope.

## Cross-layer contracts to preserve

- **Canonical identity:** map the same game/team/player across providers explicitly; do not match on display names alone.
- **Time:** distinguish event time, source update/publication evidence, retrieval time, decision time and closing observation. Preserve time zones and reschedules.
- **Historical/live parity:** one definition and feature order across training, replay and serving; no silent defaults for missing required inputs.
- **Reproducibility:** version data selections, transformations, fitted components and artifacts; record exact experiment cutoffs and configuration.
- **Probability semantics:** use empirical probabilities from samples, retain pushes, and do not substitute Gaussian-CDF approximations.
- **Execution safety:** bounded/resumable acquisition, idempotent writes, visible retries and corrections, and affected-game blocking on failed readiness checks.
- **Scope:** KenPom is not an initial dependency. Optional NCAA/CBBD enrichment must justify its additional ingestion and identity complexity.

## Delivery sequence

First fill the layer specifications, then implement one minimal vertical path through ingestion, linking/storage, as-of features, a tree baseline, shared prediction, and chronological replay. Add forward-paper operation and daily checks without claiming production or betting readiness from a single local smoke test.

Bayesian-state and neural challengers should reuse those contracts. They may be investigated in bounded experiments once a comparison baseline exists; completing every other candidate architecture is not a prerequisite.

No SQL migrations, source backfills, training runs, scheduler changes, deployments, commits, or pushes are authorized by this overview. The [handoff summary](README.md) is the next-session entry point.
