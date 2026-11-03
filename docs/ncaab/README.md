# NCAAB research and implementation handoff

- Updated: 2026-11-03
- Owner: Chase
- Status: documentation-only planning; implementation and production activation are not authorized by this handoff.

## Purpose

Develop men's college basketball game-level probability models for spreads and totals, with moneyline probabilities available from a validated margin distribution. The goal is to investigate a repeatable betting edge and diversify beyond player props. Profitability, capacity, and freedom from account limits are not established outcomes.

This is a new research/design lane. It does not restore the previously archived NCAAB implementation, change the active NBA/MLB runtime, or authorize a scraper, migration, backfill, training run, deployment, or live-money action.

## Read in this order

| Document | Role |
|---|---|
| [Model architecture decision](model-architecture-decision.md) | Model proposals, how the components fit together, and the initial controlled comparison |
| [Data sources](data-sources.md) | Historical bootstrap, live refresh, odds, coverage evidence, and remaining source questions |
| [System architecture overview](architecture-overview.md) | Responsibilities and boundaries of the seven system layers |

## Working direction

- **Sport:** men's college basketball only; not college football or college player props.
- **Statistical foundation:** SportsDataverse's ESPN-derived schedules and team/player game histories, consumed from Python. hoopR is the R interface, not a separate hosted database.
- **Daily refresh:** live basketball-provider requests for new/corrected games, followed by internally calculated features. Historical season files are not the only update path.
- **Markets:** the existing The Odds API account; separate historical and current quote acquisition from basketball-stat ingestion.
- **Excluded for now:** KenPom. It is not a required adapter, benchmark, feature source, or subscription for this initial setup. Kaggle is also outside the initial build.
- **Optional later sources:** NCAA-derived lineup/possession data and targeted CBBD gap-filling, only for a demonstrated modeling or coverage need.
- **Initial collection target:** the five completed seasons 2021-22 through 2025-26, plus ongoing 2026-27 updates. This is a planning target, not a completed acquisition or a claim that five seasons is optimal.
- **Initial experiment:** comparable market-independent and market-aware distributional boosted-tree models. Add dynamic Bayesian team-state and neural challengers as controlled extensions rather than making them prerequisites for the first end-to-end path.

## Seven layer documents

These files exist as **intentionally empty placeholders**, at Chase's request. Their responsibilities are summarized only in the [architecture overview](architecture-overview.md); none is an implementation specification yet.

1. [Data ingestion](layers/01-data-ingestion.md)
2. [Database and identity linking](layers/02-database-and-identity-linking.md)
3. [Feature builders and feature store](layers/03-feature-builders-and-feature-store.md)
4. [Training and artifact management](layers/04-training-and-artifact-management.md)
5. [Prediction and betting decisions](layers/05-prediction-and-betting-decisions.md)
6. [Evaluation and forward tracking](layers/06-evaluation-and-forward-tracking.md)
7. [Daily orchestration and data quality](layers/07-daily-orchestration-and-data-quality.md)

## What is established versus still open

Established by the discussion/research: the separation of raw statistics, market data, features, model training, prediction, and evaluation; a small historical sample and local live-source feasibility were checked. See [data sources](data-sources.md) for the exact scope and limitations.

Still open: the precise competition universe, initial bookmaker/quote-time policy, schema and IDs, model-library/objective details, deploy-host access, full-period coverage, and architecture winners. The model document records a proposed starting configuration, not measured comparative performance.

The next handoff should fill the individual layer specifications before implementation. Keep exact repository paths, migrations, interfaces, acceptance checks, and ownership out of this overview until they have been inspected and agreed. Use existing verified project capabilities where appropriate; do not assume archived NCAAB paths still exist.

## Safety and continuity

- Historical features, generated team states, rosters, and quotes must be available before each decision. Training and daily inference should share feature and prediction logic.
- Main-context agents must not call Supabase directly; use the isolated SQL-runner pattern for any future DB work.
- Long training/backfill jobs remain owner-launched. Database writes, deployment, and live-money activity require their own explicit scope/approval.
- Preserve the active NBA/MLB lanes and unrelated working-tree changes. This documentation does not authorize commits or pushes.
- This is a repository-local handoff set; canonical GBrain publication/readback has not been performed.

Return to the [documentation index](../README.md).
