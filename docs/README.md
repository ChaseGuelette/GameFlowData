# Documentation index

Status: authoritative map of every tracked Markdown surface. Reconciled 2026-08-24.

This repository documents the active NBA/MLB sportsbook product plus the separately deployed read-only Engineering OS. Canonical decisions, lessons, roadmap state, and handoffs live in remote GBrain (`source=gameflow`). Repository documentation explains the checked-in implementation and must not revive retired product lanes.

## Status classes

- **Active:** source-backed setup, operation, or contract documentation.
- **Canonical mirror:** checked-in mirror of an active remote GBrain page.
- **Historical evidence:** preserved result/plan context; missing archived paths are intentional and its commands are not current operating instructions.
- **Agent contract:** always-loaded safety/workflow policy, not a product runbook.

## Root and subsystem guides

| File | Status | Purpose |
|---|---|---|
| [`../README.md`](../README.md) | Active | Product boundary, setup, validation, and retained surfaces |
| [`../AGENTS.md`](../AGENTS.md) | Agent contract | GameFlow safety and context-routing rules |
| [`../CLAUDE.md`](../CLAUDE.md) | Agent contract | Slim mirror for Claude Code users |
| [`../dashboard/README.md`](../dashboard/README.md) | Active | Next.js dashboard setup, env, routes, and validation |
| [`../migrations/README.md`](../migrations/README.md) | Active | Retained schema contracts and archive boundary |
| [`../ops/engineering_os/README.md`](../ops/engineering_os/README.md) | Active | Engineering OS MVP 0 setup, validation, services, and rollback |
| [`../configs/mlb/examples/USAGE_GUIDE.md`](../configs/mlb/examples/USAGE_GUIDE.md) | Active | MLB lifecycle YAML operator workflow |
| [`../configs/mlb/examples/FEATURE_FAMILIES.md`](../configs/mlb/examples/FEATURE_FAMILIES.md) | Active | Test-covered lifecycle family inventory |

## Active operations

- [NBA 2026–27 core-product restart plan](operations/nba-core-product-restart-plan.md) — current session-resume anchor, scope, sequencing, and approval boundaries.
- [NBA data-readiness audit](operations/nba-data-readiness-audit-2026-09-24.md) — combined source, feature, odds/linkage, and temporal-integrity assessment.
- [NBA audit lane evidence](operations/audits/) — detailed read-only reports with exact queries, paths, tests, and caveats.
- [Daily pipeline automation](daily_pipeline_automation.md) — APScheduler timing and job boundaries.
- [Railway deployment](railway_deployment.md) — one always-on Python scheduler worker.
- [Discord integration](discord_bot_development.md) — retained NBA/MLB alerts and optional commands.
- [Database client](db_client_documentation.md) — remote/local engines and pooling behavior.
- [Historical NBA prop scraper](player_prop_scraper_documentation.md) — manual Odds API backfill.
- [NBA rolling averages](populate_average_stats_documentation.md) — full/incremental point-in-time summaries.
- [MLB FanGraphs scraper](mlb_fangraphs_scraper_documentation.md) — local-only point-in-time advanced snapshots.
- [MLB processing pipeline](mlb_processing_pipeline_documentation.md) — linking, averages, matchup context, and active production profiles.

## Active model references

- [NBA feature store](feature_store_documentation.md)
- [NBA feature-contract catalog](nba_feature_catalog.md)
- [MLB feature-contract catalog](mlb_feature_catalog.md)
- [NBA model pipeline runbook](model_pipeline_runbook.md)
- [MLB lifecycle operator guide](development_docs/mlb_model_lifecycle_usage_guide.md)

Exact contract membership lives in source constants and artifact manifests. Lifecycle docs govern workflow but never authorize long training/sweeps; Chase launches those jobs.

## Engineering OS canonical mirrors

- [Roadmap and usage](operations/gameflow-engineering-os-roadmap-and-usage.md)
- [Technical implementation](operations/gameflow-engineering-os-technical-implementation.md)

MVP 0 remains deployed read-only, but Engineering OS work is paused while NBA core-product readiness is the active priority. Do not resume certification or later MVPs without a new owner decision.

## Historical evidence

- [Engineering OS MVP 0 baseline](operations/evidence/engineering-os-mvp0-baseline.md)
- [Engineering OS MVP 0 certification log](operations/evidence/engineering-os-mvp0-certification.md)
- [Frozen Batter Hits baselines](development_docs/mlb_batter_hits_frozen_baselines.md)
- [Frozen Pitcher K baselines](development_docs/mlb_pitcher_k_frozen_baselines.md)
- [Repository reduction plan](../.hermes/plans/2026-08-24_142813-gameflow-repository-archival-and-reduction.md)

These pages preserve evidence. Paths removed by the reduction are not broken clean-checkout dependencies.

## Retired scope

The active repository does not contain or operate:

- Kalshi or Polymarket trading;
- cross-platform arbitrage;
- NCAAB;
- Bot Tracker, Arb Scanner, or Data Vault dashboard surfaces;
- unsupported MLB stat lanes removed during repository reduction.

Historical GBrain handoffs may discuss those systems. They are archive context, not build instructions.

## Verification contract

For documentation-only changes:

1. every tracked Markdown link must resolve or be an explicit external/GBrain reference;
2. referenced repository paths in active docs must exist;
3. command help/dry-runs should be exercised where safe;
4. `git diff --check` must pass;
5. source-backed counts and route lists must be generated from current code/build output rather than old handoffs.

For a full clean-checkout verification, follow [`../README.md`](../README.md).
