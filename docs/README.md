# Documentation index

This repository documents the active NBA/MLB product only. Remote GBrain is the authority for durable decisions, modeling lessons, current handoffs, and the Engineering OS roadmap. Git documentation is limited to commands and contracts that must stay aligned with the checked-in code.

## Start here

- [`../README.md`](../README.md) — supported product boundary, repository map, verification, and archive location.
- [`daily_pipeline_automation.md`](daily_pipeline_automation.md) — current APScheduler jobs and operator checks.
- [`railway_deployment.md`](railway_deployment.md) — single-worker Railway deployment contract.
- [`model_pipeline_runbook.md`](model_pipeline_runbook.md) — NBA train/backtest/inference workflow and safety gates.
- [`development_docs/mlb_model_lifecycle_usage_guide.md`](development_docs/mlb_model_lifecycle_usage_guide.md) — YAML-driven MLB experiment workflow.
- [`discord_bot_development.md`](discord_bot_development.md) — retained Discord commands and alert transport.
- [`../migrations/README.md`](../migrations/README.md) — active schema contracts and migration policy.

## Data and feature references

- [`feature_store_documentation.md`](feature_store_documentation.md)
- [`nba_feature_catalog.md`](nba_feature_catalog.md)
- [`mlb_feature_catalog.md`](mlb_feature_catalog.md)
- [`mlb_processing_pipeline_documentation.md`](mlb_processing_pipeline_documentation.md)
- [`player_prop_scraper_documentation.md`](player_prop_scraper_documentation.md)
- [`populate_average_stats_documentation.md`](populate_average_stats_documentation.md)
- [`db_client_documentation.md`](db_client_documentation.md)
- [`mlb_fangraphs_scraper_documentation.md`](mlb_fangraphs_scraper_documentation.md)

These references describe retained implementation areas but are subordinate to current source and tests. If a command or path disagrees with the code, treat the code as current and correct the document.

## Engineering OS

`operations/` documents the separately deployed, private read-only Engineering OS. It is not part of customer production and is not proof of current runtime health. MVP 0 is implemented; seven-day, phone, host-reboot, and owner-acceptance gates remain open.

## Historical evidence

`development_docs/*_frozen_baselines.md` and `operations/evidence/` are retained evidence. Paths to removed artifacts or reports refer to the verified pre-reduction archive, not to files expected in a clean checkout.

## Retired scope

Kalshi, Polymarket, cross-platform arbitrage, NCAAB, Bot Tracker, Arb Scanner, Data Vault, and their runtime/UI documentation are not active repository scope. Historical context remains in GBrain and the verified source bundle under `C:\Users\Chase\Archives\GameFlowData\2026-08-24-pre-prune\`.

## Documentation rules

1. Do not add session logs, generated reports, local brain mirrors, or copied handoffs to Git.
2. Keep executable commands PowerShell-safe when they are intended for Chase to run locally.
3. Never document global conformal offsets as deployable; Q10 behavior is edge-bearing.
4. Keep Railway advanced-stat collection CDN-only; no `stats.nba.com` calls from Railway.
5. Treat training, sweeps, broad backfills, DB changes, deployment, and promotion as separately approved actions.
